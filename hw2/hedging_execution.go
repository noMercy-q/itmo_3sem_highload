package resilience

import (
	"context"
	"time"
)

type hedgeResult struct {
	response    Response
	outcome     Outcome
	index       int
	completedAt time.Time
}

type hedgeExecution struct {
	callerCtx context.Context
	totalCtx  context.Context
	budget    Budget
	results   chan hedgeResult
	launched  int
	last      hedgeResult

	hedgeAt      time.Time
	firstReplied bool
}

func (e *hedgeExecution) run(senders []SubrequestSender, delay time.Duration) (Response, error) {
	if err := e.stopError(); err != nil {
		return Response{}, err
	}

	started := make(chan time.Time, 1)
	e.launch(0, &hedgeStartSender{sender: senders[0], started: started})

	select {
	case start := <-started:
		e.hedgeAt = start.Add(delay)
	case <-e.totalCtx.Done():
		return Response{}, e.stopError()
	}

	timer := time.NewTimer(time.Until(e.hedgeAt))
	defer timer.Stop()

	var hedge <-chan time.Time
	if len(senders) > 1 {
		hedge = timer.C
	}

	for {
		if err := e.stopError(); err != nil {
			return e.last.response, err
		}

		select {
		case result := <-e.results:
			if response, err, done := e.accept(result); done {
				return response, err
			}

			if e.firstReplied {
				timer.Stop()
				hedge = nil
			}
		case <-hedge:
			hedge = nil

			// select may pick the timer while an early reply is already queued.
			select {
			case result := <-e.results:
				if response, err, done := e.accept(result); done {
					return response, err
				}
			default:
			}

			if !e.firstReplied {
				for index := 1; index < len(senders); index++ {
					if err := e.stopError(); err != nil {
						return e.last.response, err
					}

					e.launch(index, senders[index])
				}
			}
		case <-e.totalCtx.Done():
			return e.last.response, e.stopError()
		}
	}
}

func (e *hedgeExecution) launch(index int, sender SubrequestSender) {
	e.launched++
	go func() {
		response, err := (DefaultStrategy{}).Execute(e.totalCtx, sender, e.budget)
		outcome := Classify(response, err)
		if strategyErr, ok := err.(*StrategyError); ok {
			outcome = strategyErr.LastOutcome
		}

		e.results <- hedgeResult{response: response, outcome: outcome, index: index, completedAt: time.Now()}
	}()
}

func (e *hedgeExecution) accept(result hedgeResult) (Response, error, bool) {
	e.last = result
	if result.index == 0 && !result.completedAt.After(e.hedgeAt) {
		switch result.outcome {
		case OutcomeSuccess, OutcomeFastError:
			e.firstReplied = true
		case OutcomeFailure:
			e.firstReplied = result.response.StatusCode >= 500 && result.response.StatusCode < 600
		}
	}

	if err := e.stopError(); err != nil {
		return result.response, err, true
	}

	return result.response, nil, result.outcome == OutcomeSuccess
}

func (e *hedgeExecution) stopError() error {
	if reason, cause, stopped := contextStop(e.callerCtx, e.totalCtx); stopped {
		return &StrategyError{Reason: reason, Attempts: e.launched, LastOutcome: e.last.outcome, Cause: cause}
	}

	return nil
}

type hedgeStartSender struct {
	sender  SubrequestSender
	started chan<- time.Time
}

func (s *hedgeStartSender) Send(ctx context.Context) (Response, error) {
	s.started <- time.Now()
	return s.sender.Send(ctx)
}
