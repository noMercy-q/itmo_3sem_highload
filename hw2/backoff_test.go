package resilience

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func newTestBackoff(t *testing.T, initial, max time.Duration) *ExponentialBackoffStrategy {
	t.Helper()

	strategy, err := NewExponentialBackoffStrategy(BackoffConfig{
		InitialDelay: initial,
		MaxDelay:     max,
		Multiplier:   2,
	})
	if err != nil {
		t.Fatalf("NewExponentialBackoffStrategy(): %v", err)
	}

	return strategy
}

func TestExponentialDelaySequenceAndCap(t *testing.T) {
	delays := &exponentialDelays{config: BackoffConfig{InitialDelay: 10 * time.Millisecond, MaxDelay: 25 * time.Millisecond, Multiplier: 2}}
	wants := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}

	for index, want := range wants {
		if got := delays.Delay(OutcomeFailure); got != want {
			t.Fatalf("delay %d = %s, want %s", index, got, want)
		}
	}

	if got := delays.Delay(OutcomeFastError); got != 0 {
		t.Fatalf("fast-error delay = %s, want zero", got)
	}
}

func TestExponentialBackoffEventuallySucceeds(t *testing.T) {
	strategy := newTestBackoff(t, 5*time.Millisecond, 20*time.Millisecond)
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 503}},
		{response: Response{StatusCode: 200}},
	}}
	started := time.Now()

	response, err := strategy.Execute(context.Background(), sender, standardBudget())

	if err != nil || response.StatusCode != 200 {
		t.Fatalf("response=%+v error=%v", response, err)
	}

	if elapsed := time.Since(started); elapsed < 14*time.Millisecond {
		t.Fatalf("elapsed=%s, expected two backoff delays", elapsed)
	}
}

func TestExponentialBackoffDoesNotDelayFastErrors(t *testing.T) {
	strategy := newTestBackoff(t, 100*time.Millisecond, time.Second)
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 400}},
		{response: Response{StatusCode: 200}},
	}}
	started := time.Now()

	_, err := strategy.Execute(context.Background(), sender, standardBudget())

	if err != nil {
		t.Fatalf("Execute(): %v", err)
	}

	if elapsed := time.Since(started); elapsed >= 50*time.Millisecond {
		t.Fatalf("fast error unexpectedly delayed retry by %s", elapsed)
	}
}

func TestExponentialBackoffDelaysRetryAfterTimeout(t *testing.T) {
	const initialDelay = 10 * time.Millisecond
	strategy := newTestBackoff(t, initialDelay, 40*time.Millisecond)
	sender := &scriptedSender{steps: []scriptedStep{
		{block: true},
		{response: Response{StatusCode: 200}},
	}}
	budget := standardBudget()
	budget.Latency.PerAttempt = 2 * time.Millisecond
	budget.Latency.Total = 100 * time.Millisecond
	started := time.Now()

	response, err := strategy.Execute(context.Background(), sender, budget)

	if err != nil || response.StatusCode != 200 || sender.callCount() != 2 {
		t.Fatalf("response=%+v error=%v calls=%d", response, err, sender.callCount())
	}

	if elapsed := time.Since(started); elapsed < initialDelay {
		t.Fatalf("elapsed=%s, want timeout followed by at least %s backoff", elapsed, initialDelay)
	}
}

func TestExponentialBackoffRespectsTotalLatencyDuringDelay(t *testing.T) {
	strategy := newTestBackoff(t, 100*time.Millisecond, time.Second)
	sender := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 500}}}}
	budget := standardBudget()
	budget.Latency.Total = 15 * time.Millisecond

	_, err := strategy.Execute(context.Background(), sender, budget)

	requireStrategyError(t, err, StopLatencyBudget, 1, OutcomeFailure)
	if sender.callCount() != 1 {
		t.Fatalf("calls=%d, want 1", sender.callCount())
	}
}

func TestBackoffConfigRejectsNonFiniteMultiplier(t *testing.T) {
	for _, multiplier := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := NewExponentialBackoffStrategy(BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Second,
			Multiplier:   multiplier,
		})
		if err == nil {
			t.Fatalf("multiplier=%v unexpectedly accepted", multiplier)
		}
	}
}

func TestBackoffFailsFastWhenCircuitHasNoAvailableReplica(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 500}}}}
	backoff := newTestBackoff(t, 100*time.Millisecond, time.Second)
	strategy := newTestCircuit(t, backoff, circuitConfig(1), clock)

	openingBudget := standardBudget()
	openingBudget.Retry.Failures = 1

	_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender}, openingBudget)

	started := time.Now()

	_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

	if !errors.Is(err, ErrAllCircuitsOpen) {
		t.Fatalf("error=%v, want ErrAllCircuitsOpen", err)
	}

	if elapsed := time.Since(started); elapsed >= 50*time.Millisecond {
		t.Fatalf("control error incorrectly waited for backoff: %s", elapsed)
	}

	if sender.callCount() != 1 {
		t.Fatalf("calls=%d, want no subrequest while circuit is open", sender.callCount())
	}
}
