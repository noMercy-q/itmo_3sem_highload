package resilience

import (
	"context"
	"sync"
)

// The cursor is shared between Execute calls.
type RoundRobinStrategy struct {
	client ClientStrategy

	mu   sync.Mutex
	next uint64
}

func NewRoundRobinStrategy(client ClientStrategy) (*RoundRobinStrategy, error) {
	if client == nil {
		return nil, ErrNilClientStrategy
	}

	return &RoundRobinStrategy{client: client}, nil
}

func (s *RoundRobinStrategy) Execute(ctx context.Context, senders []SubrequestSender, budget Budget) (Response, error) {
	if s == nil || s.client == nil {
		return Response{}, ErrNilClientStrategy
	}

	if err := validateSenders(senders); err != nil {
		return Response{}, err
	}

	return s.client.Execute(ctx, &roundRobinSender{owner: s, senders: senders}, budget)
}

type roundRobinSender struct {
	owner   *RoundRobinStrategy
	senders []SubrequestSender
}

func (s *roundRobinSender) Send(ctx context.Context) (Response, error) {
	s.owner.mu.Lock()
	index := s.owner.next % uint64(len(s.senders))
	s.owner.next++
	s.owner.mu.Unlock()

	return s.senders[index].Send(ctx)
}
