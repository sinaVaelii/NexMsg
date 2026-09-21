package pubsub

import "fmt"

const defaultBufferSize = 64

// Subscriber receives messages from topics it's subscribed to.
// Each subscriber has its own buffered channel — the broker writes to it,
// the consumer reads from it.
type Subscriber struct {
	id   string
	ch   chan Message
	done chan struct{} // closed by broker on shutdown or unsubscribe
}

// NewSubscriber creates a subscriber with the given ID and a default buffer size.
func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		id:   id,
		ch:   make(chan Message, defaultBufferSize),
		done: make(chan struct{}),
	}
}

// Messages returns the read-only channel the consumer should range over.
func (s *Subscriber) Messages() <-chan Message {
	return s.ch
}

// Done returns a channel that's closed when this subscriber is shut down.
func (s *Subscriber) Done() <-chan struct{} {
	return s.done
}

// ID returns the subscriber's identifier.
func (s *Subscriber) ID() string {
	return s.id
}

func (s *Subscriber) String() string {
	return fmt.Sprintf("subscriber(%s)", s.id)
}
