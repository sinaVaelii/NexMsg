package pubsub

// Topic represents a message category in the pub/sub system.
// String-based to support dynamic room topics.
type Topic string

const (
	TopicSystem Topic = "system" // Internal system events
)

// RoomTopic creates a topic for a chat room.
func RoomTopic(room string) Topic {
	return Topic("room:" + room)
}
