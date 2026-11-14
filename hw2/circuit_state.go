package resilience

import "time"

type circuitPhase uint8

const (
	circuitClosed circuitPhase = iota
	circuitOpen
	circuitHalfOpen
)

type circuitState struct {
	phase      circuitPhase
	openUntil  time.Time
	history    []bool
	next       int
	failures   int
	generation uint64
}

func (s *circuitState) reset() {
	s.generation++
	s.phase = circuitClosed
	s.openUntil = time.Time{}
	s.history = nil
	s.next = 0
	s.failures = 0
}

func (s *circuitState) add(failed bool, windowSize int) {
	if len(s.history) < windowSize {
		s.history = append(s.history, failed)
		if failed {
			s.failures++
		}

		return
	}

	if s.history[s.next] {
		s.failures--
	}

	s.history[s.next] = failed
	if failed {
		s.failures++
	}

	s.next = (s.next + 1) % windowSize
}

type timeSource interface {
	Now() time.Time
}

type realTimeSource struct{}

func (realTimeSource) Now() time.Time { return time.Now() }
