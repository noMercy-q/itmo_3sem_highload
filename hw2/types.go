package resilience

import (
	"context"
	"errors"
	"net/http"
)

type Response struct {
	StatusCode int
	Body       []byte
}

type Outcome uint8

const (
	OutcomeSuccess Outcome = iota + 1
	OutcomeFastError
	OutcomeFailure
	OutcomeTimeout
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFastError:
		return "fast_error"
	case OutcomeFailure:
		return "failure"
	case OutcomeTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Transport errors other than timeouts count as failures.
func Classify(response Response, err error) Outcome {
	if errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return OutcomeTimeout
	}

	if err != nil {
		return OutcomeFailure
	}

	switch {
	case response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices:
		return OutcomeSuccess
	case response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError:
		return OutcomeFastError
	default:
		return OutcomeFailure
	}
}

// Send must return promptly when ctx is canceled.
// One instance serves one logical request, including its retries.
type SubrequestSender interface {
	Send(context.Context) (Response, error)
}

type ClientStrategy interface {
	Execute(context.Context, SubrequestSender, Budget) (Response, error)
}

type MultiClientStrategy interface {
	Execute(context.Context, []SubrequestSender, Budget) (Response, error)
}
