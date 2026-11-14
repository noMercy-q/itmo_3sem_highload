package resilience

import (
	"context"
	"errors"
	"time"
)

type HedgingConfig struct {
	Delay time.Duration
}

// An early 4xx/5xx disables hedging; a timeout without a reply does not.
type HedgingStrategy struct {
	delay time.Duration
}

func NewHedgingStrategy(config HedgingConfig) (*HedgingStrategy, error) {
	if config.Delay < 0 {
		return nil, errors.New("hedging delay must not be negative")
	}

	return &HedgingStrategy{delay: config.Delay}, nil
}

func (s *HedgingStrategy) Execute(ctx context.Context, senders []SubrequestSender, budget Budget) (Response, error) {
	if s == nil {
		return Response{}, errors.New("hedging strategy is nil")
	}

	if err := validateSenders(senders); err != nil {
		return Response{}, err
	}

	if err := budget.validateLatency(); err != nil {
		return Response{}, err
	}

	totalCtx, cancel := context.WithTimeout(ctx, budget.Latency.Total)
	defer cancel()

	execution := hedgeExecution{
		callerCtx: ctx,
		totalCtx:  totalCtx,
		budget:    budget,
		results:   make(chan hedgeResult, len(senders)),
	}
	return execution.run(senders, s.delay)
}
