# NexMsg

Real-time messaging engine built in Go.

## Phase 1 — Pub/Sub Engine

A thread-safe pub/sub system using goroutines and channels. No mutexes — all state is serialized through a single broker goroutine with a command channel.

### Features

- **Typed topics** — `TopicChat`, `TopicNotification`, `TopicPresence`, `TopicSystem`
- **Fan-out delivery** — one publish reaches all subscribers of that topic
- **Non-blocking publish** — slow subscribers get dropped, broker never blocks
- **Graceful shutdown** — all channels closed cleanly, no goroutine leaks
- **Fully thread-safe** — subscribe, unsubscribe, and publish from any goroutine

### Quick Start

```bash
go run ./cmd/nexmsg/
```

### Run Tests

```bash
go test ./pkg/pubsub/ -v -race
```

### Architecture

```
Publisher ──► Broker (single goroutine) ──► Subscriber 1
                                        ──► Subscriber 2
                                        ──► Subscriber N
```

The broker owns all subscription state. External callers send commands through a buffered channel. The broker's `run()` loop processes them sequentially — no locks needed.
