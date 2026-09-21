package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store wraps Redis operations for NexMsg persistence.
type Store struct {
	rdb *redis.Client
}

// New creates a new Store connected to the given Redis address.
func New(addr string) *Store {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &Store{rdb: rdb}
}

// Ping checks the Redis connection.
func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// Close closes the Redis connection.
func (s *Store) Close() error {
	return s.rdb.Close()
}

// --- Rooms ---

func roomsKey() string { return "rooms" }

// AddRoom registers a room name.
func (s *Store) AddRoom(ctx context.Context, name string) error {
	return s.rdb.SAdd(ctx, roomsKey(), name).Err()
}

// ListRooms returns all room names.
func (s *Store) ListRooms(ctx context.Context) ([]string, error) {
	return s.rdb.SMembers(ctx, roomsKey()).Result()
}

// RoomExists checks if a room exists.
func (s *Store) RoomExists(ctx context.Context, name string) (bool, error) {
	return s.rdb.SIsMember(ctx, roomsKey(), name).Result()
}

// --- Messages ---

func messagesKey(room string) string {
	return fmt.Sprintf("room:%s:messages", room)
}

// SaveMessage persists a chat message in a sorted set (scored by timestamp).
func (s *Store) SaveMessage(ctx context.Context, msg ChatMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	return s.rdb.ZAdd(ctx, messagesKey(msg.Room), redis.Z{
		Score:  float64(msg.Timestamp.UnixMilli()),
		Member: string(data),
	}).Err()
}

// GetMessages returns the last `limit` messages for a room, oldest first.
func (s *Store) GetMessages(ctx context.Context, room string, limit int64) ([]ChatMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	results, err := s.rdb.ZRevRange(ctx, messagesKey(room), 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("get messages: %w", err)
	}

	msgs := make([]ChatMessage, 0, len(results))
	// reverse so oldest comes first
	for i := len(results) - 1; i >= 0; i-- {
		var m ChatMessage
		if err := json.Unmarshal([]byte(results[i]), &m); err != nil {
			log.Printf("[store] skip corrupt message: %v", err)
			continue
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// --- Presence ---

func onlineKey(username string) string {
	return fmt.Sprintf("user:%s:online", username)
}

func roomMembersKey(room string) string {
	return fmt.Sprintf("room:%s:members", room)
}

// SetOnline marks a user as online with a TTL.
func (s *Store) SetOnline(ctx context.Context, username string, ttl time.Duration) error {
	return s.rdb.Set(ctx, onlineKey(username), "1", ttl).Err()
}

// SetOffline removes a user's online status.
func (s *Store) SetOffline(ctx context.Context, username string) error {
	return s.rdb.Del(ctx, onlineKey(username)).Err()
}

// AddRoomMember adds a user to a room's member set.
func (s *Store) AddRoomMember(ctx context.Context, room, username string) error {
	return s.rdb.SAdd(ctx, roomMembersKey(room), username).Err()
}

// RemoveRoomMember removes a user from a room's member set.
func (s *Store) RemoveRoomMember(ctx context.Context, room, username string) error {
	return s.rdb.SRem(ctx, roomMembersKey(room), username).Err()
}

// GetRoomMembers returns all members of a room.
func (s *Store) GetRoomMembers(ctx context.Context, room string) ([]string, error) {
	return s.rdb.SMembers(ctx, roomMembersKey(room)).Result()
}
