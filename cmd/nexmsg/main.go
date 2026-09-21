package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sinavaelii/NexMsg/internal/grpcserver"
	"github.com/sinavaelii/NexMsg/internal/hub"
	"github.com/sinavaelii/NexMsg/internal/server"
	"github.com/sinavaelii/NexMsg/internal/store"
	"github.com/sinavaelii/NexMsg/pkg/pubsub"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// --- Redis ---
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	st := store.New(redisAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := st.Ping(ctx); err != nil {
		log.Fatalf("redis connection failed (%s): %v", redisAddr, err)
	}
	log.Printf("[main] connected to redis at %s", redisAddr)

	// seed default rooms
	for _, room := range []string{"general", "random", "tech"} {
		_ = st.AddRoom(context.Background(), room)
	}

	// --- Pub/Sub Broker ---
	broker := pubsub.NewBroker()

	// --- Hub ---
	h := hub.New(broker, st)
	go h.Run()

	// --- gRPC Server ---
	grpcLis, err := net.Listen("tcp", envOr("GRPC_ADDR", ":9090"))
	if err != nil {
		log.Fatalf("grpc listen failed: %v", err)
	}
	grpcSrv := grpc.NewServer()
	chatSrv := grpcserver.NewChatServer(h, st)
	chatSrv.RegisterTo(grpcSrv)

	go func() {
		log.Printf("[main] gRPC server listening on %s", grpcLis.Addr())
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("grpc serve failed: %v", err)
		}
	}()

	// --- HTTP + WebSocket Server ---
	httpAddr := envOr("HTTP_ADDR", ":8080")
	srv := server.New(h, st)

	go func() {
		log.Printf("[main] HTTP server listening on %s", httpAddr)
		if err := srv.ListenAndServe(httpAddr); err != nil {
			log.Fatalf("http serve failed: %v", err)
		}
	}()

	log.Println("[main] NexMsg is running!")
	log.Printf("[main] Frontend: http://localhost%s", httpAddr)
	log.Printf("[main] WebSocket: ws://localhost%s/ws?username=YOUR_NAME", httpAddr)
	log.Printf("[main] gRPC: localhost%s", grpcLis.Addr())

	// --- Graceful Shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] shutting down...")
	grpcSrv.GracefulStop()
	broker.Shutdown()
	_ = st.Close()
	log.Println("[main] goodbye!")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
