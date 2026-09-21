package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sinavaelii/NexMsg/pkg/pubsub"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	broker := pubsub.NewBroker()

	// --- create subscribers ---
	alice := pubsub.NewSubscriber("alice")
	bob := pubsub.NewSubscriber("bob")
	sysMonitor := pubsub.NewSubscriber("sys-monitor")

	// --- subscribe to topics ---
	must(broker.Subscribe(pubsub.TopicChat, alice))
	must(broker.Subscribe(pubsub.TopicChat, bob))
	must(broker.Subscribe(pubsub.TopicNotification, alice))
	must(broker.Subscribe(pubsub.TopicSystem, sysMonitor))

	// --- start consumer goroutines ---
	var wg sync.WaitGroup
	consume := func(sub *pubsub.Subscriber) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range sub.Messages() {
				fmt.Printf("  📨 [%s] %s → %q (at %s)\n",
					sub.ID(), msg.Topic, msg.Payload,
					msg.CreatedAt.Format(time.TimeOnly))
			}
			fmt.Printf("  🔌 [%s] channel closed\n", sub.ID())
		}()
	}

	consume(alice)
	consume(bob)
	consume(sysMonitor)

	// --- publish messages ---
	fmt.Println("\n🚀 Publishing messages...")

	must(broker.Publish(pubsub.TopicChat, []byte("Hey everyone!")))
	must(broker.Publish(pubsub.TopicChat, []byte("NexMsg is alive!")))
	must(broker.Publish(pubsub.TopicNotification, []byte("You have 3 new messages")))
	must(broker.Publish(pubsub.TopicSystem, []byte("health-check: OK")))

	// let messages propagate
	time.Sleep(100 * time.Millisecond)

	// --- unsubscribe bob from chat ---
	fmt.Println("\n🔕 Unsubscribing bob from chat...")
	must(broker.Unsubscribe(pubsub.TopicChat, bob))

	must(broker.Publish(pubsub.TopicChat, []byte("Bob won't see this")))
	time.Sleep(100 * time.Millisecond)

	// --- shutdown ---
	fmt.Println("\n⏹ Shutting down broker...")
	broker.Shutdown()

	wg.Wait()
	fmt.Println("\n✅ All done. No goroutine leaks.")
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
