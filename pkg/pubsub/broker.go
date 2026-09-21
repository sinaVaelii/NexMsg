package pubsub

import (
	"fmt"
	"log"
	"time"
)

type commandType int

const (
	cmdSubscribe commandType = iota
	cmdUnsubscribe
	cmdPublish
	cmdShutdown
)

// command is a serialized operation sent to the broker's run loop.
// Using a command channel instead of mutexes makes all state mutations
// sequential — no locks, no race conditions, idiomatic Go.
type command struct {
	typ        commandType
	topic      Topic
	subscriber *Subscriber
	msg        Message
	result     chan error
}

// Broker is the central message router. It runs a single goroutine that
// owns all subscription state — external callers communicate via commands.
type Broker struct {
	cmdCh chan command
	quit  chan struct{} // closed when the run loop exits
}

// NewBroker creates and starts a broker. The broker's run loop is active
// until Shutdown is called.
func NewBroker() *Broker {
	b := &Broker{
		cmdCh: make(chan command, 256),
		quit:  make(chan struct{}),
	}
	go b.run()
	return b
}

// Subscribe registers a subscriber for the given topic.
// Safe to call from any goroutine.
func (b *Broker) Subscribe(topic Topic, sub *Subscriber) error {
	result := make(chan error, 1)
	b.cmdCh <- command{
		typ:        cmdSubscribe,
		topic:      topic,
		subscriber: sub,
		result:     result,
	}
	return <-result
}

// Unsubscribe removes a subscriber from a topic and closes its done channel.
// Safe to call from any goroutine.
func (b *Broker) Unsubscribe(topic Topic, sub *Subscriber) error {
	result := make(chan error, 1)
	b.cmdCh <- command{
		typ:        cmdUnsubscribe,
		topic:      topic,
		subscriber: sub,
		result:     result,
	}
	return <-result
}

// Publish sends a message to all subscribers of the given topic.
// Safe to call from any goroutine.
func (b *Broker) Publish(topic Topic, payload []byte) error {
	result := make(chan error, 1)
	b.cmdCh <- command{
		typ: cmdPublish,
		msg: Message{
			Topic:     topic,
			Payload:   payload,
			CreatedAt: time.Now(),
		},
		result: result,
	}
	return <-result
}

// Shutdown gracefully stops the broker. It drains pending commands,
// closes all subscriber channels, and returns when fully stopped.
func (b *Broker) Shutdown() {
	result := make(chan error, 1)
	b.cmdCh <- command{
		typ:    cmdShutdown,
		result: result,
	}
	<-result  // wait for run loop to finish processing
	<-b.quit  // wait for run loop goroutine to exit
}

// run is the broker's main loop — the ONLY goroutine that touches the
// subscribers map. Everything else sends commands.
func (b *Broker) run() {
	defer close(b.quit)

	subs := make(map[Topic]map[*Subscriber]struct{})

	for cmd := range b.cmdCh {
		switch cmd.typ {
		case cmdSubscribe:
			b.handleSubscribe(subs, cmd)
		case cmdUnsubscribe:
			b.handleUnsubscribe(subs, cmd)
		case cmdPublish:
			b.handlePublish(subs, cmd)
		case cmdShutdown:
			b.handleShutdown(subs, cmd)
			return
		}
	}
}

func (b *Broker) handleSubscribe(subs map[Topic]map[*Subscriber]struct{}, cmd command) {
	if _, ok := subs[cmd.topic]; !ok {
		subs[cmd.topic] = make(map[*Subscriber]struct{})
	}
	subs[cmd.topic][cmd.subscriber] = struct{}{}
	log.Printf("[broker] subscribed %s to %s", cmd.subscriber, cmd.topic)
	cmd.result <- nil
}

func (b *Broker) handleUnsubscribe(subs map[Topic]map[*Subscriber]struct{}, cmd command) {
	topicSubs, ok := subs[cmd.topic]
	if !ok {
		cmd.result <- fmt.Errorf("topic %s has no subscribers", cmd.topic)
		return
	}
	if _, exists := topicSubs[cmd.subscriber]; !exists {
		cmd.result <- fmt.Errorf("%s is not subscribed to %s", cmd.subscriber, cmd.topic)
		return
	}

	delete(topicSubs, cmd.subscriber)
	if len(topicSubs) == 0 {
		delete(subs, cmd.topic)
	}

	close(cmd.subscriber.done)
	log.Printf("[broker] unsubscribed %s from %s", cmd.subscriber, cmd.topic)
	cmd.result <- nil
}

func (b *Broker) handlePublish(subs map[Topic]map[*Subscriber]struct{}, cmd command) {
	topicSubs, ok := subs[cmd.msg.Topic]
	if !ok {
		cmd.result <- nil // publishing to a topic with no subscribers is not an error
		return
	}

	for sub := range topicSubs {
		select {
		case sub.ch <- cmd.msg:
			// delivered
		default:
			// subscriber channel full — drop message, don't block the broker
			log.Printf("[broker] dropped message for slow %s on %s", sub, cmd.msg.Topic)
		}
	}
	cmd.result <- nil
}

func (b *Broker) handleShutdown(subs map[Topic]map[*Subscriber]struct{}, cmd command) {
	for topic, topicSubs := range subs {
		for sub := range topicSubs {
			close(sub.ch)
			close(sub.done)
		}
		delete(subs, topic)
	}
	close(b.cmdCh)
	log.Printf("[broker] shutdown complete")
	cmd.result <- nil
}
