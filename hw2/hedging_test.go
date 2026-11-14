package resilience

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func newTestHedging(t *testing.T, delay time.Duration) *HedgingStrategy {
	t.Helper()

	strategy, err := NewHedgingStrategy(HedgingConfig{Delay: delay})
	if err != nil {
		t.Fatal(err)
	}

	return strategy
}

func TestHedgingReturnsEarlyFirstSuccessWithoutHedge(t *testing.T) {
	first := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200, Body: []byte("first")}}}}
	second := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200, Body: []byte("second")}}}}
	strategy := newTestHedging(t, 30*time.Millisecond)

	response, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, standardBudget())

	if err != nil || string(response.Body) != "first" {
		t.Fatalf("response=%+v err=%v", response, err)
	}

	if first.callCount() != 1 || second.callCount() != 0 {
		t.Fatalf("calls first=%d second=%d, want 1/0", first.callCount(), second.callCount())
	}
}

func TestHedgingLaunchesRemainingAndFastestSuccessWins(t *testing.T) {
	first := &scriptedSender{steps: []scriptedStep{{block: true}}}
	second := &scriptedSender{steps: []scriptedStep{{delay: 20 * time.Millisecond, response: Response{StatusCode: 200, Body: []byte("second")}}}}
	third := &scriptedSender{steps: []scriptedStep{{delay: 2 * time.Millisecond, response: Response{StatusCode: 200, Body: []byte("third")}}}}
	strategy := newTestHedging(t, 5*time.Millisecond)
	started := time.Now()

	response, err := strategy.Execute(context.Background(), []SubrequestSender{first, second, third}, standardBudget())

	if err != nil || string(response.Body) != "third" {
		t.Fatalf("response=%+v err=%v", response, err)
	}

	if elapsed := time.Since(started); elapsed < 5*time.Millisecond {
		t.Fatalf("hedge returned after %s, before configured delay", elapsed)
	}

	if first.callCount() != 1 || second.callCount() != 1 || third.callCount() != 1 {
		t.Fatalf("calls=%d/%d/%d, want 1/1/1", first.callCount(), second.callCount(), third.callCount())
	}

	requireEventuallyCanceled(t, first)
	requireEventuallyCanceled(t, second)
}

func TestHedgingEarlyErrorResponseDisablesHedge(t *testing.T) {
	for _, status := range []int{401, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			first := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: status}}}}
			second := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200}}}}
			strategy := newTestHedging(t, 20*time.Millisecond)
			budget := standardBudget()
			budget.Latency.Total = 40 * time.Millisecond

			_, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, budget)

			var strategyErr *StrategyError
			if !errors.As(err, &strategyErr) || strategyErr.Reason != StopLatencyBudget {
				t.Fatalf("error=%v, want latency budget exhaustion", err)
			}

			if first.callCount() != 1 || second.callCount() != 0 {
				t.Fatalf("calls first/second=%d/%d, want 1/0", first.callCount(), second.callCount())
			}
		})
	}
}

func TestHedgingAllFailuresWaitForTotalLatency(t *testing.T) {
	senders := []SubrequestSender{
		&scriptedSender{steps: []scriptedStep{{delay: 10 * time.Millisecond, response: Response{StatusCode: 500}}}},
		&scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 401}}}},
		&scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 503}}}},
	}
	budget := standardBudget()
	budget.Latency.Total = 25 * time.Millisecond
	strategy := newTestHedging(t, 2*time.Millisecond)
	started := time.Now()

	_, err := strategy.Execute(context.Background(), senders, budget)

	var strategyErr *StrategyError
	if !errors.As(err, &strategyErr) || strategyErr.Reason != StopLatencyBudget || strategyErr.Attempts != 3 || !errors.Is(strategyErr, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want latency exhaustion after three attempts", err)
	}

	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("all-failed returned after %s, before total latency budget", elapsed)
	}
}

func TestHedgingStopsAtTotalLatencyAndCancelsCalls(t *testing.T) {
	first := &scriptedSender{steps: []scriptedStep{{block: true}}}
	second := &scriptedSender{steps: []scriptedStep{{block: true}}}
	budget := standardBudget()
	budget.Latency.Total = 20 * time.Millisecond
	budget.Latency.PerAttempt = time.Second
	strategy := newTestHedging(t, 2*time.Millisecond)

	_, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, budget)

	var strategyErr *StrategyError
	if !errors.As(err, &strategyErr) || strategyErr.Reason != StopLatencyBudget || strategyErr.Attempts != 2 || !errors.Is(strategyErr, context.DeadlineExceeded) {
		t.Fatalf("strategy error=%+v", strategyErr)
	}

	requireEventuallyCanceled(t, first)
	requireEventuallyCanceled(t, second)
}

func requireEventuallyCanceled(t *testing.T, sender *scriptedSender) {
	t.Helper()

	deadline := time.Now().Add(100 * time.Millisecond)

	for time.Now().Before(deadline) {
		sender.mu.Lock()
		canceled := sender.canceled
		sender.mu.Unlock()

		if canceled > 0 {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("sender did not observe cancellation")
}

func TestHedgingStartsFirstSenderBeforeZeroDelayHedges(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)

	starts := make(chan int, 2)
	sender := func(index int) SubrequestSender {
		return senderFunc(func(ctx context.Context) (Response, error) {
			starts <- index
			<-ctx.Done()
			return Response{}, ctx.Err()
		})
	}
	strategy := newTestHedging(t, 0)
	budget := standardBudget()
	budget.Latency.Total = 10 * time.Millisecond

	_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender(0), sender(1)}, budget)

	select {
	case first := <-starts:
		if first != 0 {
			t.Fatalf("first sender=%d, want 0", first)
		}
	default:
		t.Fatal("no sender was called")
	}
}

func TestHedgingTimeoutWithoutResponseStillAllowsHedge(t *testing.T) {
	first := &scriptedSender{steps: []scriptedStep{{block: true}}}
	second := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200}}}}
	strategy := newTestHedging(t, 10*time.Millisecond)
	budget := standardBudget()
	budget.Latency.PerAttempt = 2 * time.Millisecond

	response, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, budget)

	if err != nil || response.StatusCode != 200 || first.callCount() != 1 || second.callCount() != 1 {
		t.Fatalf("response=%+v error=%v calls=%d/%d", response, err, first.callCount(), second.callCount())
	}
}

func TestHedgingHonorsPriorCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sender := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200}}}}
	strategy := newTestHedging(t, 0)

	_, err := strategy.Execute(ctx, []SubrequestSender{sender}, standardBudget())

	requireStrategyError(t, err, StopCanceled, 0, 0)
	if sender.callCount() != 0 {
		t.Fatalf("calls=%d, want 0", sender.callCount())
	}
}

func TestHedgingCancellationWinsOverConcurrentSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sender := &cancelThenSucceedSender{cancel: cancel}
	strategy := newTestHedging(t, time.Second)

	_, err := strategy.Execute(ctx, []SubrequestSender{sender}, standardBudget())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want cancellation", err)
	}
}

func TestHedgingRejectsQueuedSuccessAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	execution := hedgeExecution{callerCtx: ctx, totalCtx: ctx, launched: 1}
	result := hedgeResult{response: Response{StatusCode: 200}, outcome: OutcomeSuccess}
	cancel()

	_, err, done := execution.accept(result)

	if !done || !errors.Is(err, context.Canceled) {
		t.Fatalf("done=%v error=%v, want cancellation", done, err)
	}
}

func TestHedgingRejectsQueuedSuccessAfterDeadline(t *testing.T) {
	execution := hedgeExecution{
		callerCtx: context.Background(),
		totalCtx:  deadlineWithoutNotification{Context: context.Background(), deadline: time.Now().Add(-time.Second)},
		launched:  1,
	}

	_, err, done := execution.accept(hedgeResult{response: Response{StatusCode: 200}, outcome: OutcomeSuccess})

	if !done {
		t.Fatal("expired execution continued")
	}

	requireStrategyError(t, err, StopLatencyBudget, 1, OutcomeSuccess)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want deadline exceeded", err)
	}
}
