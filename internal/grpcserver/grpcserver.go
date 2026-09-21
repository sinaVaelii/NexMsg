package grpcserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/sinavaelii/NexMsg/internal/hub"
	"github.com/sinavaelii/NexMsg/internal/store"
	pb "github.com/sinavaelii/NexMsg/proto/chat"
	"github.com/sinavaelii/NexMsg/pkg/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ChatServer implements the gRPC ChatService.
type ChatServer struct {
	pb.UnimplementedChatServiceServer
	hub   *hub.Hub
	store *store.Store
}

// NewChatServer creates a gRPC chat server.
func NewChatServer(h *hub.Hub, s *store.Store) *ChatServer {
	return &ChatServer{hub: h, store: s}
}

// RegisterTo registers the service on a gRPC server.
func (s *ChatServer) RegisterTo(srv *grpc.Server) {
	pb.RegisterChatServiceServer(srv, s)
}

// CreateRoom creates a new chat room.
func (s *ChatServer) CreateRoom(ctx context.Context, req *pb.CreateRoomRequest) (*pb.CreateRoomResponse, error) {
	if err := s.store.AddRoom(ctx, req.Name); err != nil {
		return nil, err
	}
	return &pb.CreateRoomResponse{Name: req.Name}, nil
}

// ListRooms returns all rooms.
func (s *ChatServer) ListRooms(ctx context.Context, _ *pb.ListRoomsRequest) (*pb.ListRoomsResponse, error) {
	rooms, err := s.store.ListRooms(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ListRoomsResponse{Rooms: rooms}, nil
}

// GetMessages returns message history for a room.
func (s *ChatServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	limit := int64(req.Limit)
	if limit <= 0 {
		limit = 50
	}
	msgs, err := s.store.GetMessages(ctx, req.Room, limit)
	if err != nil {
		return nil, err
	}

	pbMsgs := make([]*pb.ChatMessage, len(msgs))
	for i, m := range msgs {
		pbMsgs[i] = &pb.ChatMessage{
			Id:        m.ID,
			Room:      m.Room,
			Username:  m.Username,
			Content:   m.Content,
			Timestamp: timestamppb.New(m.Timestamp),
		}
	}
	return &pb.GetMessagesResponse{Messages: pbMsgs}, nil
}

// Stream handles bidirectional streaming for real-time chat.
func (s *ChatServer) Stream(stream pb.ChatService_StreamServer) error {
	var username string
	var rooms []string
	broker := s.hub.Broker()

	// subscriber per room → maps room name to subscriber
	subs := make(map[string]*pubsub.Subscriber)

	// cleanup on exit
	defer func() {
		for room, sub := range subs {
			_ = broker.Unsubscribe(pubsub.RoomTopic(room), sub)
			_ = s.store.RemoveRoomMember(context.Background(), room, username)
		}
		if username != "" {
			_ = s.store.SetOffline(context.Background(), username)
		}
	}()

	// channel for outgoing events
	outCh := make(chan *pb.ServerEvent, 256)

	// sender goroutine
	go func() {
		for evt := range outCh {
			if err := stream.Send(evt); err != nil {
				return
			}
		}
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			close(outCh)
			return nil
		}
		if err != nil {
			close(outCh)
			return err
		}

		switch evt := in.Event.(type) {
		case *pb.ClientEvent_Join:
			username = evt.Join.Username
			room := evt.Join.Room

			_ = s.store.SetOnline(context.Background(), username, 5*time.Minute)
			_ = s.store.AddRoom(context.Background(), room)
			_ = s.store.AddRoomMember(context.Background(), room, username)

			sub := pubsub.NewSubscriber(username + ":grpc:" + room)
			if err := broker.Subscribe(pubsub.RoomTopic(room), sub); err != nil {
				outCh <- &pb.ServerEvent{Event: &pb.ServerEvent_Error{
					Error: &pb.ErrorEvent{Message: err.Error()},
				}}
				continue
			}
			subs[room] = sub
			rooms = append(rooms, room)

			// pump subscriber → outCh
			go func(sub *pubsub.Subscriber, room string) {
				for msg := range sub.Messages() {
					var parsed map[string]any
					if err := json.Unmarshal(msg.Payload, &parsed); err != nil {
						continue
					}
					msgType, _ := parsed["type"].(string)
					switch msgType {
					case "message":
						ts, _ := parsed["timestamp"].(float64)
						outCh <- &pb.ServerEvent{Event: &pb.ServerEvent_Message{
							Message: &pb.ChatMessage{
								Id:        strVal(parsed, "id"),
								Room:      strVal(parsed, "room"),
								Username:  strVal(parsed, "username"),
								Content:   strVal(parsed, "content"),
								Timestamp: timestamppb.New(time.UnixMilli(int64(ts))),
							},
						}}
					case "user_joined":
						outCh <- &pb.ServerEvent{Event: &pb.ServerEvent_UserJoined{
							UserJoined: &pb.UserJoined{
								Room:     strVal(parsed, "room"),
								Username: strVal(parsed, "username"),
							},
						}}
					case "user_left":
						outCh <- &pb.ServerEvent{Event: &pb.ServerEvent_UserLeft{
							UserLeft: &pb.UserLeft{
								Room:     strVal(parsed, "room"),
								Username: strVal(parsed, "username"),
							},
						}}
					case "typing":
						outCh <- &pb.ServerEvent{Event: &pb.ServerEvent_UserTyping{
							UserTyping: &pb.UserTyping{
								Room:     strVal(parsed, "room"),
								Username: strVal(parsed, "username"),
							},
						}}
					}
				}
			}(sub, room)

			log.Printf("[grpc] %s joined %s", username, room)

		case *pb.ClientEvent_Leave:
			room := evt.Leave.Room
			if sub, ok := subs[room]; ok {
				_ = broker.Unsubscribe(pubsub.RoomTopic(room), sub)
				_ = s.store.RemoveRoomMember(context.Background(), room, username)
				delete(subs, room)
			}
			log.Printf("[grpc] %s left %s", username, room)

		case *pb.ClientEvent_Send:
			room := evt.Send.Room
			content := evt.Send.Content
			if username == "" || room == "" || content == "" {
				continue
			}

			chatMsg := store.ChatMessage{
				ID:        uuid.New().String(),
				Room:      room,
				Username:  username,
				Content:   content,
				Timestamp: time.Now(),
			}
			_ = s.store.SaveMessage(context.Background(), chatMsg)

			payload, _ := json.Marshal(map[string]any{
				"type":      "message",
				"id":        chatMsg.ID,
				"room":      chatMsg.Room,
				"username":  chatMsg.Username,
				"content":   chatMsg.Content,
				"timestamp": chatMsg.Timestamp.UnixMilli(),
			})
			_ = broker.Publish(pubsub.RoomTopic(room), payload)

		case *pb.ClientEvent_Typing:
			if username != "" {
				payload, _ := json.Marshal(map[string]any{
					"type":     "typing",
					"room":     evt.Typing.Room,
					"username": username,
				})
				_ = broker.Publish(pubsub.RoomTopic(evt.Typing.Room), payload)
			}
		}
	}
}

func strVal(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
