package pubsub_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sinavaelii/NexMsg/pkg/pubsub"
)

func TestSubscribeAndPublish(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	sub := pubsub.NewSubscriber("test-sub-1")
	if err := broker.Subscribe(pubsub.TopicChat, sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	payload := []byte("hello NexMsg")
	if err := broker.Publish(pubsub.TopicChat, payload); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-sub.Messages():
		if string(msg.Payload) != string(payload) {
			t.Errorf("got payload %q, want %q", msg.Payload, payload)
		}
		if msg.Topic != pubsub.TopicChat {
			t.Errorf("got topic %v, want %v", msg.Topic, pubsub.TopicChat)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	sub := pubsub.NewSubscriber("unsub-test")
	if err := broker.Subscribe(pubsub.TopicNotification, sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := broker.Unsubscribe(pubsub.TopicNotification, sub); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	// done channel should be closed after unsubscribe
	select {
	case <-sub.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("done channel not closed after unsubscribe")
	}

	// publish after unsubscribe — subscriber should NOT receive
	if err := broker.Publish(pubsub.TopicNotification, []byte("ghost")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg, ok := <-sub.Messages():
		if ok {
			t.Fatalf("received message after unsubscribe: %q", msg.Payload)
		}
		// ok == false means channel is closed — expected
	case <-time.After(100 * time.Millisecond):
		// also acceptable — no message
	}
}

func TestFanOutToMultipleSubscribers(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	const numSubs = 5
	subs := make([]*pubsub.Subscriber, numSubs)
	for i := range subs {
		subs[i] = pubsub.NewSubscriber(fmt.Sprintf("fan-out-%d", i))
		if err := broker.Subscribe(pubsub.TopicPresence, subs[i]); err != nil {
			t.Fatalf("subscribe failed for sub %d: %v", i, err)
		}
	}

	payload := []byte("user-online")
	if err := broker.Publish(pubsub.TopicPresence, payload); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	for i, sub := range subs {
		select {
		case msg := <-sub.Messages():
			if string(msg.Payload) != string(payload) {
				t.Errorf("sub %d: got %q, want %q", i, msg.Payload, payload)
			}
		case <-time.After(time.Second):
			t.Errorf("sub %d: timed out", i)
		}
	}
}

func TestConcurrentPublish(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	sub := pubsub.NewSubscriber("concurrent-receiver")
	if err := broker.Subscribe(pubsub.TopicChat, sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	const numPublishers = 10
	const msgsPerPublisher = 50

	var wg sync.WaitGroup
	wg.Add(numPublishers)

	for p := 0; p < numPublishers; p++ {
		go func(publisherID int) {
			defer wg.Done()
			for m := 0; m < msgsPerPublisher; m++ {
				payload := fmt.Appendf(nil, "p%d-m%d", publisherID, m)
				if err := broker.Publish(pubsub.TopicChat, payload); err != nil {
					t.Errorf("publish failed from publisher %d: %v", publisherID, err)
				}
			}
		}(p)
	}

	// collect messages in a separate goroutine
	received := make(chan int, 1)
	go func() {
		count := 0
		for count < numPublishers*msgsPerPublisher {
			select {
			case <-sub.Messages():
				count++
			case <-time.After(5 * time.Second):
				received <- count
				return
			}
		}
		received <- count
	}()

	wg.Wait() // all publishers done

	got := <-received
	want := numPublishers * msgsPerPublisher
	if got != want {
		t.Errorf("received %d messages, want %d", got, want)
	}
}

func TestPublishToEmptyTopic(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	// publishing to a topic with no subscribers should not error
	if err := broker.Publish(pubsub.TopicSystem, []byte("no-one-listening")); err != nil {
		t.Fatalf("publish to empty topic should not error, got: %v", err)
	}
}

func TestMultipleTopicsIsolation(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	chatSub := pubsub.NewSubscriber("chat-only")
	notifSub := pubsub.NewSubscriber("notif-only")

	if err := broker.Subscribe(pubsub.TopicChat, chatSub); err != nil {
		t.Fatalf("subscribe chat failed: %v", err)
	}
	if err := broker.Subscribe(pubsub.TopicNotification, notifSub); err != nil {
		t.Fatalf("subscribe notif failed: %v", err)
	}

	if err := broker.Publish(pubsub.TopicChat, []byte("chat-msg")); err != nil {
		t.Fatalf("publish chat failed: %v", err)
	}
	if err := broker.Publish(pubsub.TopicNotification, []byte("notif-msg")); err != nil {
		t.Fatalf("publish notif failed: %v", err)
	}

	// chat subscriber should only get chat messages
	select {
	case msg := <-chatSub.Messages():
		if string(msg.Payload) != "chat-msg" {
			t.Errorf("chatSub got %q, want %q", msg.Payload, "chat-msg")
		}
	case <-time.After(time.Second):
		t.Fatal("chatSub timed out")
	}

	// chat subscriber should NOT receive notification messages
	select {
	case msg := <-chatSub.Messages():
		t.Fatalf("chatSub received cross-topic message: %q", msg.Payload)
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	// notif subscriber should only get notification messages
	select {
	case msg := <-notifSub.Messages():
		if string(msg.Payload) != "notif-msg" {
			t.Errorf("notifSub got %q, want %q", msg.Payload, "notif-msg")
		}
	case <-time.After(time.Second):
		t.Fatal("notifSub timed out")
	}
}

func TestShutdownClosesChannels(t *testing.T) {
	broker := pubsub.NewBroker()

	sub := pubsub.NewSubscriber("shutdown-test")
	if err := broker.Subscribe(pubsub.TopicSystem, sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	broker.Shutdown()

	// after shutdown, Messages channel should be closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("Messages channel should be closed after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("Messages channel not closed after shutdown")
	}

	// done channel should be closed
	select {
	case <-sub.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("Done channel not closed after shutdown")
	}
}
