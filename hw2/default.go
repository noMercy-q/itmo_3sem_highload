package resilience

import "context"

type DefaultStrategy struct{}

func (DefaultStrategy) Execute(ctx context.Context, sender SubrequestSender, budget Budget) (Response, error) {
	if sender == nil {
		return Response{}, &StrategyError{Reason: StopAttemptsExhausted, Cause: ErrNilSender}
	}

	if err := budget.validateLatency(); err != nil {
		return Response{}, err
	}

	totalCtx, cancel := context.WithTimeout(ctx, budget.Latency.Total)
	defer cancel()

	if reason, cause, stopped := contextStop(ctx, totalCtx); stopped {
		return Response{}, strategyError(reason, retryState{}, 0, cause)
	}

	response, outcome, cause := executeAttempt(totalCtx, sender, budget.Latency.PerAttempt)
	state := retryState{attempts: 1}

	if reason, ctxErr, stopped := contextStop(ctx, totalCtx); stopped {
		return response, strategyError(reason, state, outcome, ctxErr)
	}

	if isControlError(cause) {
		return response, cause
	}

	if outcome == OutcomeSuccess {
		return response, nil
	}

	return response, strategyError(StopAttemptsExhausted, state, outcome, cause)
}
