package hub

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/sinavaelii/NexMsg/internal/store"
	"github.com/sinavaelii/NexMsg/pkg/pubsub"
)

// Hub manages all connected clients and bridges them to the pub/sub broker.
type Hub struct {
	broker   *pubsub.Broker
	store    *store.Store
	clients  map[*Client]struct{}
	register chan *Client
	remove   chan *Client
	roomMsg  chan roomMessage
}

type roomMessage struct {
	room     string
	username string
	content  string
}

// New creates a Hub wired to the given broker and store.
func New(broker *pubsub.Broker, store *store.Store) *Hub {
	return &Hub{
		broker:   broker,
		store:    store,
		clients:  make(map[*Client]struct{}),
		register: make(chan *Client),
		remove:   make(chan *Client),
		roomMsg:  make(chan roomMessage, 256),
	}
}

// Run starts the hub's event loop. Call this in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = struct{}{}
			log.Printf("[hub] client registered: %s", c.username)

		case c := <-h.remove:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				h.cleanupClient(c)
				log.Printf("[hub] client removed: %s", c.username)
			}

		case msg := <-h.roomMsg:
			h.handleRoomMessage(msg)
		}
	}
}

// Register adds a client to the hub.
func (h *Hub) Register(c *Client) {
	h.register <- c
}

// Remove removes a client from the hub.
func (h *Hub) Remove(c *Client) {
	h.remove <- c
}

// JoinRoom subscribes a client to a room's topic.
func (h *Hub) JoinRoom(c *Client, room string) error {
	ctx := context.Background()

	// ensure room exists
	if err := h.store.AddRoom(ctx, room); err != nil {
		log.Printf("[hub] failed to add room %s: %v", room, err)
	}

	topic := pubsub.RoomTopic(room)
	sub := pubsub.NewSubscriber(c.username + ":" + room)
	if err := h.broker.Subscribe(topic, sub); err != nil {
		return err
	}

	c.mu.Lock()
	c.rooms[room] = sub
	c.mu.Unlock()

	// track membership
	_ = h.store.AddRoomMember(ctx, room, c.username)

	// send join notification to room
	h.broadcastSystem(room, "user_joined", c.username)

	// pump messages from this subscriber to the client's send channel
	go h.pumpSubscriber(c, sub, room)

	return nil
}

// LeaveRoom unsubscribes a client from a room.
func (h *Hub) LeaveRoom(c *Client, room string) error {
	c.mu.Lock()
	sub, ok := c.rooms[room]
	if ok {
		delete(c.rooms, room)
	}
	c.mu.Unlock()

	if !ok {
		return nil
	}

	topic := pubsub.RoomTopic(room)
	_ = h.broker.Unsubscribe(topic, sub)
	_ = h.store.RemoveRoomMember(context.Background(), room, c.username)

	h.broadcastSystem(room, "user_left", c.username)
	return nil
}

// SendMessage publishes a chat message to a room and persists it.
func (h *Hub) SendMessage(username, room, content string) {
	h.roomMsg <- roomMessage{room: room, username: username, content: content}
}

func (h *Hub) handleRoomMessage(msg roomMessage) {
	ctx := context.Background()

	chatMsg := store.ChatMessage{
		ID:        uuid.New().String(),
		Room:      msg.room,
		Username:  msg.username,
		Content:   msg.content,
		Timestamp: time.Now(),
	}

	// persist
	if err := h.store.SaveMessage(ctx, chatMsg); err != nil {
		log.Printf("[hub] failed to save message: %v", err)
	}

	// build JSON payload for pub/sub
	payload, err := json.Marshal(map[string]any{
		"type":      "message",
		"id":        chatMsg.ID,
		"room":      chatMsg.Room,
		"username":  chatMsg.Username,
		"content":   chatMsg.Content,
		"timestamp": chatMsg.Timestamp.UnixMilli(),
	})
	if err != nil {
		log.Printf("[hub] marshal error: %v", err)
		return
	}

	topic := pubsub.RoomTopic(msg.room)
	if err := h.broker.Publish(topic, payload); err != nil {
		log.Printf("[hub] publish error: %v", err)
	}
}

func (h *Hub) broadcastSystem(room, eventType, username string) {
	payload, _ := json.Marshal(map[string]any{
		"type":     eventType,
		"room":     room,
		"username": username,
	})
	topic := pubsub.RoomTopic(room)
	_ = h.broker.Publish(topic, payload)
}

// pumpSubscriber forwards messages from a pub/sub subscriber to the client's
// WebSocket send channel.
func (h *Hub) pumpSubscriber(c *Client, sub *pubsub.Subscriber, room string) {
	for msg := range sub.Messages() {
		select {
		case c.send <- msg.Payload:
		default:
			log.Printf("[hub] dropping message for slow client %s in %s", c.username, room)
		}
	}
}

func (h *Hub) cleanupClient(c *Client) {
	c.mu.Lock()
	rooms := make([]string, 0, len(c.rooms))
	for room := range c.rooms {
		rooms = append(rooms, room)
	}
	c.mu.Unlock()

	for _, room := range rooms {
		_ = h.LeaveRoom(c, room)
	}

	_ = h.store.SetOffline(context.Background(), c.username)
	close(c.send)
}

// Broker returns the hub's broker — used by the gRPC server.
func (h *Hub) Broker() *pubsub.Broker {
	return h.broker
}

// Store returns the hub's store — used by the gRPC server.
func (h *Hub) Store() *store.Store {
	return h.store
}
