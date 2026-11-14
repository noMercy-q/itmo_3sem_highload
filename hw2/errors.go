package resilience

import (
	"errors"
	"fmt"
)

var (
	ErrTimeout           = errors.New("subrequest timeout")
	ErrNilSender         = errors.New("subrequest sender is nil")
	ErrNoSenders         = errors.New("no subrequest senders configured")
	ErrNilClientStrategy = errors.New("client strategy is nil")
)

// ControlError means no replica received the request; retries must stop.
type ControlError interface {
	error
	IsExecutionControlError() bool
}

func isControlError(err error) bool {
	var controlErr ControlError
	return errors.As(err, &controlErr) && controlErr.IsExecutionControlError()
}

type StopReason string

const (
	StopAttemptsExhausted StopReason = "attempts_exhausted"
	StopFastErrorBudget   StopReason = "fast_error_budget_exhausted"
	StopFailureBudget     StopReason = "failure_budget_exhausted"
	StopTimeoutBudget     StopReason = "timeout_budget_exhausted"
	StopLatencyBudget     StopReason = "latency_budget_exhausted"
	StopCanceled          StopReason = "canceled"
)

type StrategyError struct {
	Reason      StopReason
	Attempts    int
	LastOutcome Outcome
	Cause       error
}

func (e *StrategyError) Error() string {
	if e == nil {
		return "<nil>"
	}

	message := fmt.Sprintf("resilience strategy stopped: %s after %d attempt(s), last outcome %s", e.Reason, e.Attempts, e.LastOutcome)
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}

	return message
}

func (e *StrategyError) Unwrap() error { return e.Cause }
