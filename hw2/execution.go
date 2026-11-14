package resilience

import (
	"context"
	"time"
)

type retryState struct {
	attempts   int
	fastErrors int
	failures   int
	timeouts   int
}

func executeAttempt(ctx context.Context, sender SubrequestSender, perAttempt time.Duration) (Response, Outcome, error) {
	if err := contextError(ctx); err != nil {
		return Response{}, Classify(Response{}, err), err
	}

	attemptCtx, cancel := context.WithTimeout(ctx, perAttempt)
	defer cancel()

	response, err := sender.Send(attemptCtx)

	// Cancellation takes priority over a response.
	if contextErr := contextError(attemptCtx); contextErr != nil {
		err = contextErr
	}

	return response, Classify(response, err), err
}

// The deadline may expire before its timer callback runs.
func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}

	return nil
}

func contextStop(ctx, totalCtx context.Context) (StopReason, error, bool) {
	if err := contextError(ctx); err != nil {
		return StopCanceled, err, true
	}

	if err := contextError(totalCtx); err != nil {
		return StopLatencyBudget, err, true
	}

	return "", nil, false
}

func (s *retryState) consume(outcome Outcome, budget RetryBudget) (StopReason, bool) {
	switch outcome {
	case OutcomeFastError:
		s.fastErrors++
		return StopFastErrorBudget, s.fastErrors >= budget.FastErrors
	case OutcomeTimeout:
		s.timeouts++
		return StopTimeoutBudget, s.timeouts >= budget.Timeouts
	default:
		s.failures++
		return StopFailureBudget, s.failures >= budget.Failures
	}
}

func strategyError(reason StopReason, state retryState, outcome Outcome, cause error) error {
	return &StrategyError{Reason: reason, Attempts: state.attempts, LastOutcome: outcome, Cause: cause}
}

func wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
