package resilience

import "fmt"

func validateSenders(senders []SubrequestSender) error {
	if len(senders) == 0 {
		return ErrNoSenders
	}

	for index, sender := range senders {
		if sender == nil {
			return fmt.Errorf("sender at index %d: %w", index, ErrNilSender)
		}
	}

	return nil
}
