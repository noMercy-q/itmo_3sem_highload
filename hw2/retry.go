package resilience

import (
	"context"
	"time"
)

type delayPolicy interface {
	Delay(Outcome) time.Duration
}

type noDelay struct{}

func (noDelay) Delay(Outcome) time.Duration { return 0 }

type RetryStrategy struct{}

func (RetryStrategy) Execute(ctx context.Context, sender SubrequestSender, budget Budget) (Response, error) {
	return executeSequential(ctx, sender, budget, noDelay{})
}

func executeSequential(ctx context.Context, sender SubrequestSender, budget Budget, delays delayPolicy) (Response, error) {
	if sender == nil {
		return Response{}, &StrategyError{Reason: StopAttemptsExhausted, Cause: ErrNilSender}
	}

	if err := budget.validateRetry(); err != nil {
		return Response{}, err
	}

	totalCtx, cancel := context.WithTimeout(ctx, budget.Latency.Total)
	defer cancel()

	state := retryState{}
	var last Response
	var lastOutcome Outcome
	var lastCause error

	for state.attempts < budget.Retry.MaxAttempts {
		if reason, cause, stopped := contextStop(ctx, totalCtx); stopped {
			return last, strategyError(reason, state, lastOutcome, cause)
		}

		state.attempts++
		last, lastOutcome, lastCause = executeAttempt(totalCtx, sender, budget.Latency.PerAttempt)

		if reason, cause, stopped := contextStop(ctx, totalCtx); stopped {
			return last, strategyError(reason, state, lastOutcome, cause)
		}

		if isControlError(lastCause) {
			return last, lastCause
		}

		if lastOutcome == OutcomeSuccess {
			return last, nil
		}

		if reason, exhausted := state.consume(lastOutcome, budget.Retry); exhausted {
			return last, strategyError(reason, state, lastOutcome, lastCause)
		}

		if state.attempts >= budget.Retry.MaxAttempts {
			return last, strategyError(StopAttemptsExhausted, state, lastOutcome, lastCause)
		}

		if err := wait(totalCtx, delays.Delay(lastOutcome)); err != nil {
			reason := StopLatencyBudget
			if ctx.Err() != nil {
				reason = StopCanceled
			}

			return last, strategyError(reason, state, lastOutcome, err)
		}
	}

	return last, strategyError(StopAttemptsExhausted, state, lastOutcome, lastCause)
}
