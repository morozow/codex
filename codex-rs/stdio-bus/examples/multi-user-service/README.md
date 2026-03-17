# Multi-User Codex Service via stdio_bus

This example demonstrates a real-world scenario: a web service that allows multiple users
to interact with Codex simultaneously, with stdio_bus managing the worker pool.

## Why This Matters

**Without stdio_bus**, to serve multiple users you would need to:
- Spawn a separate Codex process for each user
- Implement session affinity yourself
- Handle process crashes and restarts manually
- Build your own load balancing

**With stdio_bus**, all of this is handled automatically:
- Worker pool management
- Session affinity (same user → same worker)
- Automatic crash recovery with backoff
- Load balancing across workers

## Test Results ✅

Successfully tested on 2026-03-17 with 5 concurrent users and 3 workers:

```
=== Multi-User Codex Test ===

Starting 5 concurrent users...

[Alice] Got session: thread:user-72a7ba-f3db98d1
[Bob] Got session: thread:user-cd764d-bd8ea9e4
[Charlie] Got session: thread:user-3ea38a-09880c3c
[Diana] Got session: thread:user-dd19ff-e6f18f70
[Eve] Got session: thread:user-159c59-5bf806c1

=== All users completed ===
```

**stdio_bus logs showing session distribution:**

```
[INFO] stdio Bus kernel v0.1.0 starting on macos
[INFO] Process manager created with 3 workers across 1 pools
[INFO] [worker=0] Worker started (pid=86270)
[INFO] [worker=1] Worker started (pid=86271)
[INFO] [worker=2] Worker started (pid=86272)
[INFO] All 3 workers started successfully
[INFO] Listening on TCP 127.0.0.1:9999

[INFO] [session=thread:user-159c59] New session assigned to worker 0
[INFO] [session=thread:user-cd764d] New session assigned to worker 1
[INFO] [session=thread:user-dd19ff] New session assigned to worker 2
[INFO] [session=thread:user-72a7ba] New session assigned to worker 0
[INFO] [session=thread:user-3ea38a] New session assigned to worker 1
```

**Session distribution across 3 workers:**

| User    | Assigned Worker |
|---------|-----------------|
| Eve     | worker 0        |
| Bob     | worker 1        |
| Diana   | worker 2        |
| Alice   | worker 0        |
| Charlie | worker 1        |

**Key observations:**
- 5 users distributed across 3 workers (load balancing works)
- Each user's subsequent requests go to the same worker (session affinity works)
- All 20 requests (5 users × 4 requests each) completed successfully


## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web Browser                               │
│  User A: "List files"    User B: "Write code"    User C: ...    │
└──────────┬─────────────────────┬─────────────────────┬──────────┘
           │                     │                     │
           ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                     HTTP API Server                              │
│                   (Python FastAPI)                               │
│                     localhost:8000                               │
│                                                                  │
│  - Receives HTTP requests from users                             │
│  - Maintains single TCP connection to stdio_bus                  │
│  - Tracks user sessions                                          │
│  - Multiplexes requests/responses                                │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                    TCP Connection (persistent)
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      stdio_bus daemon                            │
│                     localhost:9999                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Session Router                         │    │
│  │  session:user-A → worker 0  (affinity maintained)        │    │
│  │  session:user-B → worker 1  (affinity maintained)        │    │
│  │  session:user-C → worker 2  (load balanced)              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Features: auto-restart, backoff, graceful shutdown, monitoring  │
└──────────┬─────────────────────┬─────────────────────┬──────────┘
           │                     │                     │
           ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ codex-app-server│   │ codex-app-server│   │ codex-app-server│
│   --worker      │   │   --worker      │   │   --worker      │
│   (worker 0)    │   │   (worker 1)    │   │   (worker 2)    │
│                 │   │                 │   │                 │
│ Handles:        │   │ Handles:        │   │ Handles:        │
│ - Eve, Alice    │   │ - Bob, Charlie  │   │ - Diana         │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

## What This Demonstrates

1. **Multiple concurrent users** - 5 users sending requests simultaneously
2. **Session affinity** - Each user's requests always go to the same worker
3. **Load balancing** - New sessions distributed across available workers
4. **Stateful conversations** - Each user maintains their own context
5. **Horizontal scaling** - Add more workers by changing `instances` in config

## Quick Start

```bash
# Terminal 1: Start stdio_bus with Codex workers
./start_stdiobus.sh

# Terminal 2: Start the web API server
./start_api.sh

# Terminal 3: Test with multiple users
./test_multi_user.sh

# Or open http://localhost:8000 in browser for interactive UI
```

## Files

| File | Description |
|------|-------------|
| `start_stdiobus.sh` | Starts stdio_bus daemon with 3 Codex workers |
| `api_server.py` | FastAPI server that proxies requests to stdio_bus |
| `start_api.sh` | Starts the API server with dependencies |
| `test_multi_user.sh` | Simulates 5 concurrent users |

## How Session Affinity Works

```
Request 1: {sessionId: null}           → stdio_bus assigns worker 0
Response:  {sessionId: "thread:abc"}   → client saves session ID

Request 2: {sessionId: "thread:abc"}   → stdio_bus routes to worker 0
Request 3: {sessionId: "thread:abc"}   → stdio_bus routes to worker 0
```

## Scaling

To handle more users, increase `instances` in config:

```json
"instances": 10  // 10 Codex workers instead of 3
```

## Comparison: With vs Without stdio_bus

| Aspect | Without stdio_bus | With stdio_bus |
|--------|-------------------|----------------|
| Process per user | 1 process per user | N workers for M users |
| Session affinity | Manual implementation | Built-in |
| Crash recovery | Manual/systemd | Automatic with backoff |
| Load balancing | Manual | Built-in round-robin |
| Memory usage | High | Low (shared pool) |
| Scaling | Spawn more processes | Change config |
