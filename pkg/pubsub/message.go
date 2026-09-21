package pubsub

import "time"

// Message is a single unit of data flowing through the pub/sub system.
type Message struct {
	Topic     Topic
	Payload   []byte
	CreatedAt time.Time
}
