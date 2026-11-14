package resilience

import (
	"context"
	"errors"
	"math"
	"time"
)

type BackoffConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func (c BackoffConfig) validate() error {
	if c.InitialDelay <= 0 {
		return errors.New("initial backoff delay must be positive")
	}

	if c.MaxDelay < c.InitialDelay {
		return errors.New("max backoff delay must be at least initial delay")
	}

	if math.IsNaN(c.Multiplier) || math.IsInf(c.Multiplier, 0) || c.Multiplier <= 1 {
		return errors.New("backoff multiplier must be finite and greater than one")
	}

	return nil
}

type ExponentialBackoffStrategy struct {
	config BackoffConfig
}

func NewExponentialBackoffStrategy(config BackoffConfig) (*ExponentialBackoffStrategy, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &ExponentialBackoffStrategy{config: config}, nil
}

func (s *ExponentialBackoffStrategy) Execute(ctx context.Context, sender SubrequestSender, budget Budget) (Response, error) {
	if s == nil {
		return Response{}, errors.New("exponential backoff strategy is nil")
	}

	return executeSequential(ctx, sender, budget, &exponentialDelays{config: s.config})
}

type exponentialDelays struct {
	config BackoffConfig
	heavy  int
}

func (d *exponentialDelays) Delay(outcome Outcome) time.Duration {
	if outcome == OutcomeFastError {
		return 0
	}

	if outcome != OutcomeFailure && outcome != OutcomeTimeout {
		return 0
	}

	power := math.Pow(d.config.Multiplier, float64(d.heavy))
	d.heavy++
	if power >= float64(d.config.MaxDelay)/float64(d.config.InitialDelay) {
		return d.config.MaxDelay
	}

	delay := time.Duration(float64(d.config.InitialDelay) * power)
	if delay > d.config.MaxDelay || delay < 0 {
		return d.config.MaxDelay
	}

	return delay
}
