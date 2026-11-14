package resilience

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

type fakeCircuitClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeCircuitClock() *fakeCircuitClock {
	return &fakeCircuitClock{now: time.Unix(1_000, 0)}
}

func (c *fakeCircuitClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeCircuitClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func circuitConfig(window int) CircuitBreakerConfig {
	return CircuitBreakerConfig{WindowSize: window, FailureThreshold: 1, Cooldown: time.Minute}
}

func newTestCircuit(t *testing.T, client ClientStrategy, config CircuitBreakerConfig, clock timeSource) *CircuitBreakerStrategy {
	t.Helper()

	strategy, err := newCircuitBreakerStrategy(client, config, clock)
	if err != nil {
		t.Fatal(err)
	}

	return strategy
}

func TestCircuitBreakerUsesRoundRobinWhileClosed(t *testing.T) {
	clock := newFakeCircuitClock()
	first := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
	}}
	second := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(2), clock)

	for range 4 {
		if _, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, standardBudget()); err != nil {
			t.Fatal(err)
		}
	}

	if first.callCount() != 2 || second.callCount() != 2 {
		t.Fatalf("calls=%d/%d, want 2/2", first.callCount(), second.callCount())
	}
}

func TestCircuitBreakerRotatesComposedRetryAttempts(t *testing.T) {
	clock := newFakeCircuitClock()
	first := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 500}}}}
	second := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 503}}}}
	third := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 200, Body: []byte("ok")}}}}
	strategy := newTestCircuit(t, RetryStrategy{}, circuitConfig(1), clock)

	response, err := strategy.Execute(
		context.Background(),
		[]SubrequestSender{first, second, third},
		standardBudget(),
	)

	if err != nil || string(response.Body) != "ok" {
		t.Fatalf("response=%+v error=%v", response, err)
	}

	if first.callCount() != 1 || second.callCount() != 1 || third.callCount() != 1 {
		t.Fatalf("calls=%d/%d/%d, want 1/1/1", first.callCount(), second.callCount(), third.callCount())
	}
}

func TestCircuitBreakerOpensAfterFullFailureWindow(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 503}},
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(2), clock)

	for range 2 {
		if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err == nil {
			t.Fatal("failure request unexpectedly succeeded")
		}
	}

	_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

	if !errors.Is(err, ErrAllCircuitsOpen) {
		t.Fatalf("error=%v, want ErrAllCircuitsOpen", err)
	}

	if sender.callCount() != 2 {
		t.Fatalf("calls=%d, want 2", sender.callCount())
	}
}

func TestCircuitBreakerSkipsOpenReplica(t *testing.T) {
	clock := newFakeCircuitClock()
	failing := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 500}},
	}}
	healthy := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(2), clock)

	for range 5 {
		_, _ = strategy.Execute(context.Background(), []SubrequestSender{failing, healthy}, standardBudget())
	}

	if failing.callCount() != 2 || healthy.callCount() != 3 {
		t.Fatalf("calls failing/healthy=%d/%d, want 2/3", failing.callCount(), healthy.callCount())
	}
}

func TestCircuitBreakerSuccessfulHalfOpenProbeClosesCircuit(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(2), clock)

	for range 2 {
		_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())
	}

	clock.Advance(time.Minute)

	for range 2 {
		if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err != nil {
			t.Fatalf("request after cooldown failed: %v", err)
		}
	}

	if sender.callCount() != 4 {
		t.Fatalf("calls=%d, want 4", sender.callCount())
	}
}

func TestCircuitBreakerFailedProbeReopensCircuit(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 503}},
		{response: Response{StatusCode: 200}},
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(2), clock)

	for range 2 {
		_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())
	}

	clock.Advance(time.Minute)

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err == nil {
		t.Fatal("failed half-open probe unexpectedly succeeded")
	}

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); !errors.Is(err, ErrAllCircuitsOpen) {
		t.Fatalf("error=%v, want reopened circuit", err)
	}

	clock.Advance(time.Minute)

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err != nil {
		t.Fatalf("second half-open probe failed: %v", err)
	}
}

func TestCircuitBreakerReturnsTypedErrorWhenAllOpen(t *testing.T) {
	clock := newFakeCircuitClock()
	first := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 500}}}}
	second := &scriptedSender{steps: []scriptedStep{{response: Response{StatusCode: 500}}}}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(1), clock)

	_, _ = strategy.Execute(context.Background(), []SubrequestSender{first, second}, standardBudget())

	_, _ = strategy.Execute(context.Background(), []SubrequestSender{first, second}, standardBudget())

	_, err := strategy.Execute(context.Background(), []SubrequestSender{first, second}, standardBudget())

	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) || openErr.Replicas != 2 {
		t.Fatalf("error=%T %+v, want CircuitOpenError for 2 replicas", err, err)
	}
}

type controlledProbeSender struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (s *controlledProbeSender) Send(ctx context.Context) (Response, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	if call == 1 {
		return Response{StatusCode: 500}, nil
	}

	if call == 2 {
		close(s.started)

		select {
		case <-s.release:
			return Response{StatusCode: 200}, nil
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	}

	return Response{StatusCode: 200}, nil
}

func (s *controlledProbeSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

func TestCircuitBreakerAllowsOnlyOneConcurrentHalfOpenProbe(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &controlledProbeSender{started: make(chan struct{}), release: make(chan struct{})}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(1), clock)

	_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

	clock.Advance(time.Minute)

	firstDone := make(chan error, 1)
	go func() {
		_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

		firstDone <- err
	}()
	<-sender.started

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); !errors.Is(err, ErrAllCircuitsOpen) {
		t.Fatalf("concurrent request error=%v, want ErrAllCircuitsOpen", err)
	}

	if sender.callCount() != 2 {
		t.Fatalf("calls=%d, want only one half-open probe", sender.callCount())
	}

	close(sender.release)

	if err := <-firstDone; err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}
}

type staleFailureSender struct {
	mu           sync.Mutex
	calls        int
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

func (s *staleFailureSender) Send(ctx context.Context) (Response, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	switch call {
	case 1:
		close(s.firstStarted)

		select {
		case <-s.releaseFirst:
			return Response{StatusCode: 500}, nil
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	case 2:
		return Response{StatusCode: 500}, nil
	default:
		return Response{StatusCode: 200}, nil
	}
}

func TestCircuitBreakerIgnoresResultFromPreviousGeneration(t *testing.T) {
	clock := newFakeCircuitClock()
	sender := &staleFailureSender{firstStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	strategy := newTestCircuit(t, DefaultStrategy{}, circuitConfig(1), clock)

	staleDone := make(chan error, 1)
	go func() {
		_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

		staleDone <- err
	}()
	<-sender.firstStarted

	// Recover the circuit before the older request completes.
	_, _ = strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

	clock.Advance(time.Minute)

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err != nil {
		t.Fatalf("half-open probe failed: %v", err)
	}

	close(sender.releaseFirst)

	if err := <-staleDone; err == nil {
		t.Fatal("stale failing request unexpectedly succeeded")
	}

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget()); err != nil {
		t.Fatalf("stale result reopened recovered circuit: %v", err)
	}
}

func TestCircuitBreakerConfigRejectsNonFiniteThreshold(t *testing.T) {
	for _, threshold := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := NewCircuitBreakerStrategy(DefaultStrategy{}, CircuitBreakerConfig{
			WindowSize:       1,
			FailureThreshold: threshold,
			Cooldown:         time.Second,
		})
		if err == nil {
			t.Fatalf("threshold=%v unexpectedly accepted", threshold)
		}
	}
}

func TestCircuitBreakerUsesOnlyTheLatestWindow(t *testing.T) {
	clock := newFakeCircuitClock()
	config := CircuitBreakerConfig{WindowSize: 4, FailureThreshold: 0.75, Cooldown: time.Minute}
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}},
		{response: Response{StatusCode: 200}}, // Evicts the initial failure.
		{response: Response{StatusCode: 401}},
		{err: ErrTimeout},
		{response: Response{StatusCode: 503}}, // Three failures in the latest four.
	}}
	strategy := newTestCircuit(t, DefaultStrategy{}, config, clock)

	for attempt := range 8 {
		_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

		if errors.Is(err, ErrAllCircuitsOpen) {
			t.Fatalf("circuit opened before attempt %d: %v", attempt+1, err)
		}
	}

	_, err := strategy.Execute(context.Background(), []SubrequestSender{sender}, standardBudget())

	if !errors.Is(err, ErrAllCircuitsOpen) || sender.callCount() != 8 {
		t.Fatalf("error=%v calls=%d, want open circuit after eight sends", err, sender.callCount())
	}
}
