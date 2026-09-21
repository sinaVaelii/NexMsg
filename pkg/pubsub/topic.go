package pubsub

import "fmt"

// Topic represents a message category in the pub/sub system.
type Topic int

const (
	TopicChat         Topic = iota // Real-time chat messages
	TopicNotification              // Push notifications
	TopicPresence                  // Online/offline status
	TopicSystem                    // Internal system events
)

var topicNames = [...]string{
	TopicChat:         "chat",
	TopicNotification: "notification",
	TopicPresence:     "presence",
	TopicSystem:       "system",
}

func (t Topic) String() string {
	if int(t) < len(topicNames) {
		return topicNames[t]
	}
	return fmt.Sprintf("topic(%d)", t)
}
