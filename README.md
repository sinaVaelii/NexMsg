# NexMsg

Real-time messaging engine built in Go.

## Features & Architecture

NexMsg is designed as a multi-tier messaging architecture:
1. **Core Pub/Sub Engine**: A thread-safe, non-blocking broker system built entirely with goroutines and channels (no mutexes).
2. **gRPC API**: Fast and scalable RPC communication defined via Protocol Buffers.
3. **HTTP & WebSocket API**: Real-time frontend connectivity and basic static file serving.
4. **Redis Integration**: Distributed state and caching support for horizontal scaling.

---

## Phase 1 — Core Pub/Sub Engine

A thread-safe pub/sub system using goroutines and channels. No mutexes — all state is serialized through a single broker goroutine with a command channel.

### Core Engine Features
- **Typed topics** — `TopicChat`, `TopicNotification`, `TopicPresence`, `TopicSystem`
- **Fan-out delivery** — one publish reaches all subscribers of that topic
- **Non-blocking publish** — slow subscribers get dropped, broker never blocks
- **Graceful shutdown** — all channels closed cleanly, no goroutine leaks
- **Fully thread-safe** — subscribe, unsubscribe, and publish from any goroutine

### Architecture

```text
Publisher ──► Broker (single goroutine) ──► Subscriber 1
                                        ──► Subscriber 2
                                        ──► Subscriber N
```

The broker owns all subscription state. External callers send commands through a buffered channel. The broker's `run()` loop processes them sequentially — no locks needed.

---

## Phase 2 — Server, gRPC, WebSocket & Redis

Expanded the core engine into a fully-fledged server exposing multiple interfaces.

### Components
- **gRPC Server** (Port `9090`): For fast internal service-to-service communication.
- **HTTP/WebSocket Server** (Port `8080`): Handles real-time client connections and serves the web frontend.
- **Redis Store**: Connects to `localhost:6379` to manage persistent/distributed data.
- **Protobuf Definitions**: Located in `proto/chat/chat.proto`, compiled into Go interfaces.

---

## Getting Started

### Prerequisites
- [Go 1.22+](https://go.dev/)
- Redis Server (e.g. `sudo apt-get install redis-server`)
- Protobuf Compiler (`protoc`) and Go plugins (`protoc-gen-go`, `protoc-gen-go-grpc`)

### Run the Server

1. **Start Redis:**
   ```bash
   sudo systemctl start redis-server
   ```

2. **Download dependencies:**
   ```bash
   go mod tidy
   ```

3. **Start NexMsg:**
   ```bash
   go run ./cmd/nexmsg/
   ```

When started, the application will be accessible at:
- **HTTP / Web UI**: `http://localhost:8080`
- **WebSocket Endpoint**: `ws://localhost:8080/ws?username=YOUR_NAME`
- **gRPC API**: `localhost:9090`

### Run Tests

Run the Pub/Sub concurrency and race detector tests:
```bash
go test ./pkg/pubsub/ -v -race
```
