package resilience

import (
	"errors"
	"time"
)

// Limits include the initial attempt: one stops on the first matching error.
type RetryBudget struct {
	MaxAttempts int
	FastErrors  int
	Failures    int
	Timeouts    int
}

type LatencyBudget struct {
	Total      time.Duration
	PerAttempt time.Duration
}

type Budget struct {
	Retry   RetryBudget
	Latency LatencyBudget
}

func (b Budget) validateLatency() error {
	if b.Latency.Total <= 0 {
		return errors.New("total latency budget must be positive")
	}

	if b.Latency.PerAttempt <= 0 {
		return errors.New("subrequest latency budget must be positive")
	}

	return nil
}

func (b Budget) validateRetry() error {
	if err := b.validateLatency(); err != nil {
		return err
	}

	if b.Retry.MaxAttempts <= 0 {
		return errors.New("max attempts must be positive")
	}

	if b.Retry.FastErrors <= 0 || b.Retry.Failures <= 0 || b.Retry.Timeouts <= 0 {
		return errors.New("per-outcome retry budgets must be positive")
	}

	return nil
}
