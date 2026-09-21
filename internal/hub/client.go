package hub

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sinavaelii/NexMsg/pkg/pubsub"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
	sendBufSize    = 256
)

// Client represents a single WebSocket connection.
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	username string
	send     chan []byte

	mu    sync.Mutex
	rooms map[string]*pubsub.Subscriber
}

// NewClient creates a client for a WebSocket connection.
func NewClient(hub *Hub, conn *websocket.Conn, username string) *Client {
	return &Client{
		hub:      hub,
		conn:     conn,
		username: username,
		send:     make(chan []byte, sendBufSize),
		rooms:    make(map[string]*pubsub.Subscriber),
	}
}

// Username returns the client's username.
func (c *Client) Username() string {
	return c.username
}

// wsEvent is the JSON envelope for WebSocket messages from the client.
type wsEvent struct {
	Type    string `json:"type"`
	Room    string `json:"room,omitempty"`
	Content string `json:"content,omitempty"`
}

// ReadPump reads messages from the WebSocket and routes them.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.Remove(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error for %s: %v", c.username, err)
			}
			return
		}

		var evt wsEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			log.Printf("[ws] invalid JSON from %s: %v", c.username, err)
			continue
		}

		switch evt.Type {
		case "join":
			if err := c.hub.JoinRoom(c, evt.Room); err != nil {
				log.Printf("[ws] join error: %v", err)
			}
		case "leave":
			if err := c.hub.LeaveRoom(c, evt.Room); err != nil {
				log.Printf("[ws] leave error: %v", err)
			}
		case "message":
			if evt.Room != "" && evt.Content != "" {
				c.hub.SendMessage(c.username, evt.Room, evt.Content)
			}
		case "typing":
			c.broadcastTyping(evt.Room)
		default:
			log.Printf("[ws] unknown event type %q from %s", evt.Type, c.username)
		}
	}
}

// WritePump writes messages from the send channel to the WebSocket.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) broadcastTyping(room string) {
	payload, _ := json.Marshal(map[string]any{
		"type":     "typing",
		"room":     room,
		"username": c.username,
	})
	topic := pubsub.RoomTopic(room)
	_ = c.hub.Broker().Publish(topic, payload)
}
