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
	if err := broker.Subscribe(pubsub.RoomTopic("general"), sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	payload := []byte("hello NexMsg")
	if err := broker.Publish(pubsub.RoomTopic("general"), payload); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg := <-sub.Messages():
		if string(msg.Payload) != string(payload) {
			t.Errorf("got payload %q, want %q", msg.Payload, payload)
		}
		if msg.Topic != pubsub.RoomTopic("general") {
			t.Errorf("got topic %v, want %v", msg.Topic, pubsub.RoomTopic("general"))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	sub := pubsub.NewSubscriber("unsub-test")
	topic := pubsub.RoomTopic("notifications")
	if err := broker.Subscribe(topic, sub); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := broker.Unsubscribe(topic, sub); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	select {
	case <-sub.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("done channel not closed after unsubscribe")
	}

	if err := broker.Publish(topic, []byte("ghost")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case msg, ok := <-sub.Messages():
		if ok {
			t.Fatalf("received message after unsubscribe: %q", msg.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		// also acceptable
	}
}

func TestFanOutToMultipleSubscribers(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	topic := pubsub.RoomTopic("presence")
	const numSubs = 5
	subs := make([]*pubsub.Subscriber, numSubs)
	for i := range subs {
		subs[i] = pubsub.NewSubscriber(fmt.Sprintf("fan-out-%d", i))
		if err := broker.Subscribe(topic, subs[i]); err != nil {
			t.Fatalf("subscribe failed for sub %d: %v", i, err)
		}
	}

	payload := []byte("user-online")
	if err := broker.Publish(topic, payload); err != nil {
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

	topic := pubsub.RoomTopic("chat")
	sub := pubsub.NewSubscriber("concurrent-receiver")
	if err := broker.Subscribe(topic, sub); err != nil {
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
				if err := broker.Publish(topic, payload); err != nil {
					t.Errorf("publish failed from publisher %d: %v", publisherID, err)
				}
			}
		}(p)
	}

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

	wg.Wait()

	got := <-received
	want := numPublishers * msgsPerPublisher
	if got != want {
		t.Errorf("received %d messages, want %d", got, want)
	}
}

func TestPublishToEmptyTopic(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	if err := broker.Publish(pubsub.TopicSystem, []byte("no-one-listening")); err != nil {
		t.Fatalf("publish to empty topic should not error, got: %v", err)
	}
}

func TestMultipleTopicsIsolation(t *testing.T) {
	broker := pubsub.NewBroker()
	defer broker.Shutdown()

	chatTopic := pubsub.RoomTopic("chat")
	notifTopic := pubsub.RoomTopic("notif")

	chatSub := pubsub.NewSubscriber("chat-only")
	notifSub := pubsub.NewSubscriber("notif-only")

	if err := broker.Subscribe(chatTopic, chatSub); err != nil {
		t.Fatalf("subscribe chat failed: %v", err)
	}
	if err := broker.Subscribe(notifTopic, notifSub); err != nil {
		t.Fatalf("subscribe notif failed: %v", err)
	}

	if err := broker.Publish(chatTopic, []byte("chat-msg")); err != nil {
		t.Fatalf("publish chat failed: %v", err)
	}
	if err := broker.Publish(notifTopic, []byte("notif-msg")); err != nil {
		t.Fatalf("publish notif failed: %v", err)
	}

	select {
	case msg := <-chatSub.Messages():
		if string(msg.Payload) != "chat-msg" {
			t.Errorf("chatSub got %q, want %q", msg.Payload, "chat-msg")
		}
	case <-time.After(time.Second):
		t.Fatal("chatSub timed out")
	}

	select {
	case msg, ok := <-chatSub.Messages():
		if ok {
			t.Fatalf("chatSub received cross-topic message: %q", msg.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		// expected
	}

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

	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("Messages channel should be closed after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("Messages channel not closed after shutdown")
	}

	select {
	case <-sub.Done():
		// expected
	case <-time.After(time.Second):
		t.Fatal("Done channel not closed after shutdown")
	}
}
