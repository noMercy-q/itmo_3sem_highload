package resilience

import (
	"context"
	"sync"
	"testing"
	"time"
)

type scriptedStep struct {
	response Response
	err      error
	delay    time.Duration
	block    bool
}

type scriptedSender struct {
	mu       sync.Mutex
	steps    []scriptedStep
	calls    int
	canceled int
}

func (s *scriptedSender) Send(ctx context.Context) (Response, error) {
	s.mu.Lock()
	index := s.calls
	s.calls++
	var step scriptedStep
	if index < len(s.steps) {
		step = s.steps[index]
	} else {
		step = scriptedStep{response: Response{StatusCode: 500}}
	}

	s.mu.Unlock()

	if step.block {
		<-ctx.Done()
		s.mu.Lock()
		s.canceled++
		s.mu.Unlock()

		return Response{}, ctx.Err()
	}

	if step.delay > 0 {
		timer := time.NewTimer(step.delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			s.mu.Lock()
			s.canceled++
			s.mu.Unlock()

			return Response{}, ctx.Err()
		}
	}

	return step.response, step.err
}

func (s *scriptedSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

func standardBudget() Budget {
	return Budget{
		Retry:   RetryBudget{MaxAttempts: 5, FastErrors: 5, Failures: 5, Timeouts: 5},
		Latency: LatencyBudget{Total: time.Second, PerAttempt: 100 * time.Millisecond},
	}
}

func requireStrategyError(t *testing.T, err error, reason StopReason, attempts int, outcome Outcome) *StrategyError {
	t.Helper()

	strategyErr, ok := err.(*StrategyError)
	if !ok {
		t.Fatalf("error = %T %v, want *StrategyError", err, err)
	}

	if strategyErr.Reason != reason || strategyErr.Attempts != attempts || strategyErr.LastOutcome != outcome {
		t.Fatalf("strategy error = %+v, want reason=%s attempts=%d outcome=%s", strategyErr, reason, attempts, outcome)
	}

	return strategyErr
}

type senderFunc func(context.Context) (Response, error)

func (f senderFunc) Send(ctx context.Context) (Response, error) { return f(ctx) }

// Simulates a delayed context deadline callback.
type deadlineWithoutNotification struct {
	context.Context
	deadline time.Time
}

func (c deadlineWithoutNotification) Deadline() (time.Time, bool) { return c.deadline, true }
