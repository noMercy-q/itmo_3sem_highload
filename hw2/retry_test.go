package resilience

import (
	"context"
	"testing"
	"time"
)

func TestRetryEventuallySucceedsAcrossErrorClasses(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 503}},
		{response: Response{StatusCode: 429}},
		{response: Response{StatusCode: 200, Body: []byte("done")}},
	}}

	response, err := (RetryStrategy{}).Execute(context.Background(), sender, standardBudget())

	if err != nil || string(response.Body) != "done" || sender.callCount() != 3 {
		t.Fatalf("response=%+v err=%v calls=%d", response, err, sender.callCount())
	}
}

func TestRetryStopsAtFastErrorBudget(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 400}},
		{response: Response{StatusCode: 404}},
		{response: Response{StatusCode: 200}},
	}}
	budget := standardBudget()
	budget.Retry.FastErrors = 2

	_, err := (RetryStrategy{}).Execute(context.Background(), sender, budget)

	requireStrategyError(t, err, StopFastErrorBudget, 2, OutcomeFastError)
}

func TestRetryStopsAtHeavyOutcomeBudgets(t *testing.T) {
	tests := []struct {
		name    string
		steps   []scriptedStep
		prepare func(*Budget)
		reason  StopReason
		outcome Outcome
	}{
		{
			name: "failure budget",
			steps: []scriptedStep{
				{response: Response{StatusCode: 500}},
				{response: Response{StatusCode: 503}},
				{response: Response{StatusCode: 200}},
			},
			prepare: func(budget *Budget) { budget.Retry.Failures = 2 },
			reason:  StopFailureBudget,
			outcome: OutcomeFailure,
		},
		{
			name:  "timeout budget",
			steps: []scriptedStep{{block: true}, {block: true}, {response: Response{StatusCode: 200}}},
			prepare: func(budget *Budget) {
				budget.Retry.Timeouts = 2
				budget.Latency.PerAttempt = 2 * time.Millisecond
			},
			reason:  StopTimeoutBudget,
			outcome: OutcomeTimeout,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender := &scriptedSender{steps: test.steps}
			budget := standardBudget()
			test.prepare(&budget)

			_, err := (RetryStrategy{}).Execute(context.Background(), sender, budget)

			requireStrategyError(t, err, test.reason, 2, test.outcome)
			if sender.callCount() != 2 {
				t.Fatalf("calls=%d, want 2", sender.callCount())
			}
		})
	}
}

func TestRetryStopsAtTotalAttemptBudget(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{
		{response: Response{StatusCode: 500}},
		{response: Response{StatusCode: 502}},
		{response: Response{StatusCode: 200}},
	}}
	budget := standardBudget()
	budget.Retry.MaxAttempts = 2

	_, err := (RetryStrategy{}).Execute(context.Background(), sender, budget)

	requireStrategyError(t, err, StopAttemptsExhausted, 2, OutcomeFailure)
}

func TestRetryCanRecoverAfterSubrequestTimeout(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{
		{block: true},
		{response: Response{StatusCode: 200}},
	}}
	budget := standardBudget()
	budget.Latency.PerAttempt = 5 * time.Millisecond

	response, err := (RetryStrategy{}).Execute(context.Background(), sender, budget)

	if err != nil || response.StatusCode != 200 || sender.callCount() != 2 {
		t.Fatalf("response=%+v err=%v calls=%d", response, err, sender.callCount())
	}
}

func TestRetryStopsWhenTotalLatencyExpires(t *testing.T) {
	sender := &scriptedSender{steps: []scriptedStep{{block: true}, {block: true}}}
	budget := standardBudget()
	budget.Latency.Total = 15 * time.Millisecond
	budget.Latency.PerAttempt = 10 * time.Millisecond

	_, err := (RetryStrategy{}).Execute(context.Background(), sender, budget)

	strategyErr := err.(*StrategyError)
	if strategyErr.Reason != StopLatencyBudget || strategyErr.Attempts < 1 || strategyErr.Attempts > 2 {
		t.Fatalf("strategy error = %+v", strategyErr)
	}
}

func TestRetryCancellationWinsOverConcurrentSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sender := &cancelThenSucceedSender{cancel: cancel}

	_, err := (RetryStrategy{}).Execute(ctx, sender, standardBudget())

	requireStrategyError(t, err, StopCanceled, 1, OutcomeFailure)
	if sender.calls != 1 {
		t.Fatalf("calls=%d, want 1", sender.calls)
	}
}
