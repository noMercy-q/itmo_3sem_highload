package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultReturnsSuccessfulPayloadWithOneCall(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200, Body: []byte("ok")}}}}

	response, err := (DefaultStrategy{}).Execute(context.Background(), sender, standardBudget())

	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if string(response.Body) != "ok" || sender.callCount() != 1 {
		t.Fatalf("response=%+v calls=%d", response, sender.callCount())
	}
}

func TestDefaultReturnsFastErrorWithoutRetry(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 401, Body: []byte("denied")}}}}

	response, err := (DefaultStrategy{}).Execute(context.Background(), sender, standardBudget())

	if response.StatusCode != 401 || sender.callCount() != 1 {
		t.Fatalf("response=%+v calls=%d", response, sender.callCount())
	}

	requireStrategyError(t, err, StopAttemptsExhausted, 1, OutcomeFastError)
}

func TestDefaultAppliesPerAttemptTimeout(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{{block: true}}}
	budget := standardBudget()
	budget.Latency.PerAttempt = 10 * time.Millisecond

	_, err := (DefaultStrategy{}).Execute(context.Background(), sender, budget)

	strategyErr := requireStrategyError(t, err, StopAttemptsExhausted, 1, OutcomeTimeout)
	if !errors.Is(strategyErr, context.DeadlineExceeded) {
		t.Fatalf("error %v does not wrap context deadline", strategyErr)
	}
}

func TestDefaultHonorsTotalLatencyBudget(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{{block: true}}}
	budget := standardBudget()
	budget.Latency.Total = 10 * time.Millisecond
	budget.Latency.PerAttempt = time.Second

	_, err := (DefaultStrategy{}).Execute(context.Background(), sender, budget)

	requireStrategyError(t, err, StopLatencyBudget, 1, OutcomeTimeout)
}

func TestDefaultHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sender := &scriptedSender{steps: []scriptedStep{{block: true}}}

	_, err := (DefaultStrategy{}).Execute(ctx, sender, standardBudget())

	requireStrategyError(t, err, StopCanceled, 0, 0)
	if sender.callCount() != 0 {
		t.Fatalf("calls=%d, want no send after prior cancellation", sender.callCount())
	}
}

type cancelThenSucceedSender struct {
	cancel context.CancelFunc
	calls  int
}

func (s *cancelThenSucceedSender) Send(context.Context) (Response, error) {
	s.calls++
	s.cancel()
	return Response{StatusCode: 200}, nil
}

func TestDefaultCancellationWinsOverConcurrentSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sender := &cancelThenSucceedSender{cancel: cancel}

	_, err := (DefaultStrategy{}).Execute(ctx, sender, standardBudget())

	requireStrategyError(t, err, StopCanceled, 1, OutcomeFailure)
	if sender.calls != 1 {
		t.Fatalf("calls=%d, want 1", sender.calls)
	}
}
