package resilience

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrAllCircuitsOpen       = errors.New("all circuits are open")
	ErrSenderPoolSizeChanged = errors.New("sender pool size changed")
)

// The error ratio is checked only after the window is full.
type CircuitBreakerConfig struct {
	WindowSize       int
	FailureThreshold float64
	Cooldown         time.Duration
}

func (c CircuitBreakerConfig) validate() error {
	if c.WindowSize <= 0 {
		return errors.New("circuit breaker window size must be positive")
	}

	if math.IsNaN(c.FailureThreshold) || math.IsInf(c.FailureThreshold, 0) || c.FailureThreshold <= 0 || c.FailureThreshold > 1 {
		return errors.New("circuit breaker failure threshold must be finite and in (0, 1]")
	}

	if c.Cooldown <= 0 {
		return errors.New("circuit breaker cooldown must be positive")
	}

	return nil
}

// All replicas are open or already running a half-open probe.
type CircuitOpenError struct {
	Replicas int
}

func (e *CircuitOpenError) Error() string {
	return fmt.Sprintf("%s (%d replica(s) unavailable)", ErrAllCircuitsOpen, e.Replicas)
}

func (e *CircuitOpenError) Is(target error) bool { return target == ErrAllCircuitsOpen }

func (e *CircuitOpenError) IsExecutionControlError() bool { return true }

// State is keyed by replica index; keep the pool order stable.
type CircuitBreakerStrategy struct {
	client ClientStrategy
	config CircuitBreakerConfig
	clock  timeSource

	mu       sync.Mutex
	next     uint64
	poolSize int
	states   []circuitState
}

func NewCircuitBreakerStrategy(client ClientStrategy, config CircuitBreakerConfig) (*CircuitBreakerStrategy, error) {
	return newCircuitBreakerStrategy(client, config, realTimeSource{})
}

func newCircuitBreakerStrategy(client ClientStrategy, config CircuitBreakerConfig, clock timeSource) (*CircuitBreakerStrategy, error) {
	if client == nil {
		return nil, ErrNilClientStrategy
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	if clock == nil {
		return nil, errors.New("circuit breaker clock is nil")
	}

	return &CircuitBreakerStrategy{client: client, config: config, clock: clock}, nil
}

func (s *CircuitBreakerStrategy) Execute(ctx context.Context, senders []SubrequestSender, budget Budget) (Response, error) {
	if s == nil || s.client == nil {
		return Response{}, ErrNilClientStrategy
	}

	if err := validateSenders(senders); err != nil {
		return Response{}, err
	}

	if err := s.ensurePool(len(senders)); err != nil {
		return Response{}, err
	}

	response, err := s.client.Execute(ctx, &circuitPoolSender{owner: s, senders: senders}, budget)
	if err != nil {
		var openErr *CircuitOpenError
		if errors.As(err, &openErr) {
			return response, openErr
		}
	}

	return response, err
}

func (s *CircuitBreakerStrategy) ensurePool(size int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poolSize == 0 {
		s.poolSize = size
		s.states = make([]circuitState, size)
		return nil
	}

	if s.poolSize != size {
		return fmt.Errorf("%w: configured=%d, received=%d", ErrSenderPoolSizeChanged, s.poolSize, size)
	}

	return nil
}

type circuitReservation struct {
	index      int
	probe      bool
	generation uint64
}

func (s *CircuitBreakerStrategy) acquire() (circuitReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	start := s.next % uint64(s.poolSize)

	for offset := 0; offset < s.poolSize; offset++ {
		index := int((start + uint64(offset)) % uint64(s.poolSize))
		state := &s.states[index]

		switch state.phase {
		case circuitClosed:
			s.next = uint64(index + 1)
			return circuitReservation{index: index, generation: state.generation}, nil
		case circuitOpen:
			if now.Before(state.openUntil) {
				continue
			}

			state.phase = circuitHalfOpen
			s.next = uint64(index + 1)
			return circuitReservation{index: index, probe: true, generation: state.generation}, nil
		case circuitHalfOpen:
			continue
		}
	}

	return circuitReservation{}, &CircuitOpenError{Replicas: s.poolSize}
}

func (s *CircuitBreakerStrategy) complete(reservation circuitReservation, outcome Outcome, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := &s.states[reservation.index]
	if reservation.generation != state.generation {
		return
	}

	// Caller cancellation must not affect replica health.
	if errors.Is(err, context.Canceled) {
		if reservation.probe && state.phase == circuitHalfOpen {
			state.phase = circuitOpen
			state.openUntil = s.clock.Now()
			state.generation++
		}

		return
	}

	failed := outcome != OutcomeSuccess
	if reservation.probe {
		if state.phase != circuitHalfOpen {
			return
		}

		if failed {
			state.phase = circuitOpen
			state.openUntil = s.clock.Now().Add(s.config.Cooldown)
			state.generation++
			return
		}

		state.reset()
		return
	}

	// Ignore late results from a circuit that has already opened.
	if state.phase != circuitClosed {
		return
	}

	state.add(failed, s.config.WindowSize)
	if len(state.history) == s.config.WindowSize && float64(state.failures)/float64(s.config.WindowSize) >= s.config.FailureThreshold {
		state.phase = circuitOpen
		state.openUntil = s.clock.Now().Add(s.config.Cooldown)
		state.generation++
	}
}

type circuitPoolSender struct {
	owner   *CircuitBreakerStrategy
	senders []SubrequestSender
}

func (s *circuitPoolSender) Send(ctx context.Context) (Response, error) {
	reservation, err := s.owner.acquire()
	if err != nil {
		return Response{}, err
	}

	response, sendErr := s.senders[reservation.index].Send(ctx)
	if sendErr == nil && ctx.Err() != nil {
		sendErr = ctx.Err()
	}

	outcome := Classify(response, sendErr)
	s.owner.complete(reservation, outcome, sendErr)
	return response, sendErr
}
