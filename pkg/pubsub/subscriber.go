package pubsub

import (
	"fmt"
	"sync/atomic"
)

const defaultBufferSize = 64

// Subscriber receives messages from topics it's subscribed to.
// Each subscriber has its own buffered channel — the broker writes to it,
// the consumer reads from it.
type Subscriber struct {
	id     string
	ch     chan Message
	done   chan struct{} // closed by broker on shutdown or unsubscribe
	closed atomic.Bool
}

// NewSubscriber creates a subscriber with the given ID and a default buffer size.
func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		id:   id,
		ch:   make(chan Message, defaultBufferSize),
		done: make(chan struct{}),
	}
}

// closeCh idempotently closes the subscriber's message and done channels.
// Only the broker's run loop calls this — safe because the atomic flag
// guarantees exactly-once semantics even if the subscriber appears in
// multiple topics.
func (s *Subscriber) closeCh() {
	if !s.closed.CompareAndSwap(false, true) {
		return // already closed
	}
	close(s.ch)
	close(s.done)
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

