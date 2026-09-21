package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sinavaelii/NexMsg/internal/hub"
	"github.com/sinavaelii/NexMsg/internal/store"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Server handles HTTP and WebSocket requests.
type Server struct {
	hub   *hub.Hub
	store *store.Store
	mux   *http.ServeMux
}

// New creates an HTTP server wired to the hub and store.
func New(h *hub.Hub, s *store.Store) *Server {
	srv := &Server{hub: h, store: s, mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) routes() {
	s.mux.HandleFunc("/ws", s.handleWebSocket)
	s.mux.HandleFunc("/api/rooms", s.handleRooms)
	s.mux.HandleFunc("/api/rooms/", s.handleRoomMessages)
	s.mux.Handle("/", http.FileServer(http.Dir("web")))
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	log.Printf("[http] listening on %s", addr)
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "username required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	client := hub.NewClient(s.hub, conn, username)
	s.hub.Register(client)

	// mark online
	_ = s.store.SetOnline(context.Background(), username, 5*time.Minute)

	go client.WritePump()
	go client.ReadPump()
}

func (s *Server) handleRooms(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		rooms, err := s.store.ListRooms(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if rooms == nil {
			rooms = []string{}
		}
		json.NewEncoder(w).Encode(map[string]any{"rooms": rooms})

	case http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
			http.Error(w, "name required", http.StatusBadRequest)
			return
		}
		if err := s.store.AddRoom(ctx, body.Name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"name": body.Name})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRoomMessages(w http.ResponseWriter, r *http.Request) {
	// expect: /api/rooms/{name}/messages
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/rooms/"), "/")
	if len(parts) < 2 || parts[1] != "messages" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	roomName := parts[0]

	limitStr := r.URL.Query().Get("limit")
	limit := int64(50)
	if n, err := strconv.ParseInt(limitStr, 10, 64); err == nil && n > 0 {
		limit = n
	}

	msgs, err := s.store.GetMessages(r.Context(), roomName, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if msgs == nil {
		msgs = []store.ChatMessage{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"messages": msgs})
}
