package store

import "time"

// ChatMessage represents a persisted chat message.
type ChatMessage struct {
	ID        string    `json:"id"`
	Room      string    `json:"room"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}
