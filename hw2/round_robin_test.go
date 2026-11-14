package resilience

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type orderedSender struct {
	id       string
	response Response
	mu       *sync.Mutex
	order    *[]string
	calls    int
}

func (s *orderedSender) Send(context.Context) (Response, error) {
	s.mu.Lock()
	*s.order = append(*s.order, s.id)
	s.calls++
	s.mu.Unlock()

	return s.response, nil
}

func TestRoundRobinRotatesRetryAttempts(t *testing.T) {
	var mu sync.Mutex
	var order []string
	senders := []SubrequestSender{
		&orderedSender{id: "a", response: Response{StatusCode: 500}, mu: &mu, order: &order},
		&orderedSender{id: "b", response: Response{StatusCode: 503}, mu: &mu, order: &order},
		&orderedSender{id: "c", response: Response{StatusCode: 200, Body: []byte("ok")}, mu: &mu, order: &order},
	}
	strategy, err := NewRoundRobinStrategy(RetryStrategy{})
	if err != nil {
		t.Fatal(err)
	}

	response, err := strategy.Execute(context.Background(), senders, standardBudget())

	if err != nil || string(response.Body) != "ok" {
		t.Fatalf("response=%+v err=%v", response, err)
	}

	mu.Lock()
	defer mu.Unlock()

	if got := strings.Join(order, ""); got != "abc" {
		t.Fatalf("order=%q, want abc", got)
	}
}

func TestRoundRobinWrapsAcrossLogicalRequests(t *testing.T) {
	var mu sync.Mutex
	var order []string
	senders := []SubrequestSender{
		&orderedSender{id: "a", response: Response{StatusCode: 200}, mu: &mu, order: &order},
		&orderedSender{id: "b", response: Response{StatusCode: 200}, mu: &mu, order: &order},
		&orderedSender{id: "c", response: Response{StatusCode: 200}, mu: &mu, order: &order},
	}
	strategy, _ := NewRoundRobinStrategy(DefaultStrategy{})

	for range 5 {
		if _, err := strategy.Execute(context.Background(), senders, standardBudget()); err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if got := strings.Join(order, ""); got != "abcab" {
		t.Fatalf("order=%q, want abcab", got)
	}
}

func TestRoundRobinIsConcurrentAndBalanced(t *testing.T) {
	var mu sync.Mutex
	var order []string
	replicas := []*orderedSender{
		{id: "a", response: Response{StatusCode: 200}, mu: &mu, order: &order},
		{id: "b", response: Response{StatusCode: 200}, mu: &mu, order: &order},
		{id: "c", response: Response{StatusCode: 200}, mu: &mu, order: &order},
	}
	senders := []SubrequestSender{replicas[0], replicas[1], replicas[2]}
	strategy, _ := NewRoundRobinStrategy(DefaultStrategy{})
	var workers sync.WaitGroup

	for range 300 {
		workers.Add(1)
		go func() {
			defer workers.Done()

			if _, err := strategy.Execute(context.Background(), senders, standardBudget()); err != nil {
				t.Errorf("Execute() error: %v", err)
			}
		}()
	}

	workers.Wait()

	mu.Lock()
	defer mu.Unlock()

	for _, replica := range replicas {
		if replica.calls != 100 {
			t.Fatalf("sender %s calls=%d, want 100", replica.id, replica.calls)
		}
	}
}

func TestRoundRobinRejectsInvalidPoolsAndStrategy(t *testing.T) {
	if _, err := NewRoundRobinStrategy(nil); !errors.Is(err, ErrNilClientStrategy) {
		t.Fatalf("constructor error=%v, want ErrNilClientStrategy", err)
	}

	strategy, _ := NewRoundRobinStrategy(DefaultStrategy{})

	if _, err := strategy.Execute(context.Background(), nil, standardBudget()); !errors.Is(err, ErrNoSenders) {
		t.Fatalf("empty pool error=%v, want ErrNoSenders", err)
	}

	if _, err := strategy.Execute(context.Background(), []SubrequestSender{nil}, standardBudget()); !errors.Is(err, ErrNilSender) {
		t.Fatalf("nil sender error=%v, want ErrNilSender", err)
	}
}
