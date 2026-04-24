# ACP M3 — Design

Generated: 2026-04-24
Branch: `feature/acp-daemon-m3`
ADR reference: ADR-012

---

## 1. Postgres Schema (DDL)

The `acp` database lives in the existing Postgres 16 instance. The init script
is placed in `compose/fase-0/postgres/init/acp.sql` (or the equivalent init-d
path in the compose stack). It follows the pattern of `litellm.sql` and
`openwebui.sql`.

```sql
-- acp.sql — idempotent init script for the acp database
-- Runs once at container startup via /docker-entrypoint-initdb.d/

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'acp') THEN
    CREATE ROLE acp WITH LOGIN PASSWORD 'acp_local_dev';
  END IF;
END
$$;

SELECT 'CREATE DATABASE acp OWNER acp'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'acp') \gexec

\c acp

SET ROLE acp;

CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT        PRIMARY KEY,
    workspace_root  TEXT        NOT NULL,
    model           TEXT,
    created_at_ms   BIGINT      NOT NULL,
    updated_at_ms   BIGINT      NOT NULL,
    compaction      JSONB,
    fork            JSONB,
    version         INTEGER     NOT NULL DEFAULT 1,
    closed_at_ms    BIGINT      -- NULL while session is open
);

CREATE INDEX IF NOT EXISTS sessions_workspace_updated
    ON sessions (workspace_root, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS session_events (
    event_id        BIGSERIAL   PRIMARY KEY,
    session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
    seq             INTEGER     NOT NULL,
    event_type      TEXT        NOT NULL,
    -- 'message' | 'compaction' | 'permission_request' | 'permission_response' | 'meta'
    role            TEXT,
    -- 'user' | 'assistant' | 'tool' | 'system' | NULL
    payload         JSONB       NOT NULL,
    created_at_ms   BIGINT      NOT NULL,
    UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS session_events_session_seq
    ON session_events (session_id, seq);

CREATE TABLE IF NOT EXISTS session_clients (
    client_id       TEXT        NOT NULL,
    session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
    transport       TEXT        NOT NULL DEFAULT 'websocket',
    -- 'websocket' | 'stdio'
    attached_at_ms  BIGINT      NOT NULL,
    last_seen_ms    BIGINT      NOT NULL,
    last_seq        INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (client_id, session_id)
);

CREATE INDEX IF NOT EXISTS session_clients_session
    ON session_clients (session_id);
```

### Design decisions for the schema

**seq is per-session monotonic, starting at 1.** The BIGSERIAL `event_id` is
for global ordering across sessions (PK); `seq` is the per-session ordering
field used by replay queries and the catch-up algorithm. These are separate
because `event_id` can have gaps between sessions; `seq` must be gapless within
a session for the catch-up algorithm to work correctly.

**FLAGGED FOR USER REVIEW (A)**: The `seq` counter needs to be generated
application-side (not by Postgres sequences) to avoid race conditions in the
following scenario: two concurrent writes to the same session must yield
consecutive `seq` values. The application-side approach: the session's driver
task is the single writer (only one `session/prompt` at a time per session due
to `SessionBusy` enforcement), so `seq` is simply an `AtomicI32` on `SessionSlot`
incremented before each DB insert. This is safe because only one writer exists
per session. Alternative: use `SELECT COALESCE(MAX(seq), 0) + 1 FROM session_events
WHERE session_id = $1 FOR UPDATE` inside a transaction, which Postgres guarantees
but adds a round-trip per write. The `AtomicI32` approach is chosen for simplicity
given the single-writer constraint.

**`closed_at_ms` on sessions:** Used to distinguish open vs closed sessions in
`session/list` queries. Set on `session/close`. Never deleted (append-only
sessions table).

**`session_clients` is soft presence:** Rows are inserted on `session/resume`
or `session/new` attach and deleted on clean close. A reaper task runs on daemon
startup and every 5 minutes: it deletes rows where `last_seen_ms < now - 300_000`
(5 min heartbeat window). The heartbeat is updated by any inbound message from
the client task, including `session/prompt` and `session/permission_response`.

---

## 2. Rust Trait Signatures

### `SessionBackend` trait (in `runtime::session_control`)

```rust
/// Abstracts session storage so unit tests can use FileSessionBackend
/// while production uses PostgresSessionBackend.
#[async_trait]
pub trait SessionBackend: Send + Sync {
    /// Create a new session record. Returns error if session_id already exists.
    async fn create_session(&self, session: &Session) -> Result<(), BackendError>;

    /// Load a session by id. Returns None if not found.
    async fn load_session(&self, session_id: &str) -> Result<Option<Session>, BackendError>;

    /// Append one event to the session's event log. `seq` must be provided
    /// by the caller (atomically incremented per session slot).
    async fn append_event(
        &self,
        session_id: &str,
        seq: i32,
        event: &StoredEvent,
    ) -> Result<(), BackendError>;

    /// Load all events for a session in seq order.
    async fn load_events(
        &self,
        session_id: &str,
        since_seq: i32,  // exclusive lower bound; 0 = all events
    ) -> Result<Vec<StoredEvent>, BackendError>;

    /// Mark a session as closed (sets closed_at_ms).
    async fn close_session(&self, session_id: &str) -> Result<(), BackendError>;

    /// List all open sessions (closed_at_ms IS NULL) for the workspace root.
    async fn list_open_sessions(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionSummaryRow>, BackendError>;

    /// Upsert presence row for a client. Called on attach and every heartbeat.
    async fn upsert_client_presence(
        &self,
        client_id: &str,
        session_id: &str,
        transport: &str,
    ) -> Result<(), BackendError>;

    /// Remove presence row for a client. Called on clean disconnect.
    async fn remove_client_presence(
        &self,
        client_id: &str,
        session_id: &str,
    ) -> Result<(), BackendError>;
}
```

### `StoredEvent` (serializable, lives in `runtime::session_control`)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub seq: i32,
    pub event_type: StoredEventType,
    pub role: Option<String>,
    pub payload: serde_json::Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredEventType {
    Message,
    Compaction,
    PermissionRequest,
    PermissionResponse,
    Meta,
}
```

### `FileSessionBackend` (keeps existing JSONL behavior)

Wraps the existing `SessionStore`. `append_event` translates `StoredEvent` back
into the existing JSONL append. `load_events` replays from the JSONL file.
`list_open_sessions` scans the sessions directory.

**FLAGGED FOR USER REVIEW (B)**: The existing `Session::push_message` and
`Session::save_to_path` API writes directly to the JSONL file and is not
async. The `FileSessionBackend::append_event` will need to either:
(a) call `tokio::task::spawn_blocking` to wrap the synchronous I/O, or
(b) accept that file writes are blocking (acceptable since file backend is for
tests only, not production). Decision: **option (b)** — `FileSessionBackend` calls
`spawn_blocking` only for the file I/O operations that would block. This avoids
changing the synchronous `Session` API while keeping the async trait contract.

### `PostgresSessionBackend`

```rust
pub struct PostgresSessionBackend {
    pool: sqlx::PgPool,
}

impl PostgresSessionBackend {
    pub async fn connect(url: &str) -> Result<Self, BackendError>;
    pub async fn run_migrations(&self) -> Result<(), BackendError>;
    // ... implements SessionBackend
}
```

Uses `sqlx` 0.8 with `postgres` + `json` + `chrono` + `runtime-tokio-rustls` features.
`sqlx::query!` macros with `DATABASE_URL` set at compile time (or `sqlx::query`
at runtime if compile-time checking is not feasible in the worktree).

---

## 3. ACP Protocol Extensions

All new methods and notifications follow JSON-RPC 2.0. The protocol version
is bumped from `"0.1"` to `"0.2"` in `ACP_PROTOCOL_VERSION` once Phase 2 lands.

### New method: `session/prompt`

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "method": "session/prompt",
  "params": {
    "session_id": "session-1714000000000-0",
    "text": "list all rust files",
    "turn_id": "turn-abc123"
  }
}
```

`turn_id` is optional; if omitted the server generates one (UUID4).

**Result (returned immediately, before streaming completes):**
```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "result": {
    "session_id": "session-1714000000000-0",
    "turn_id": "turn-abc123",
    "status": "started"
  }
}
```

**Errors:**
- `-32001` `UnknownSession`
- `-32003` `SessionBusy` — turn already in progress

### New notification: `session/update`

Sent from server → all attached clients. No `id` field (notification).

```json
{
  "jsonrpc": "2.0",
  "method": "session/update",
  "params": {
    "session_id": "session-1714000000000-0",
    "turn_id": "turn-abc123",
    "seq": 5,
    "event": { ... }
  }
}
```

The `seq` field in the notification payload matches the `seq` written to
`session_events`. Clients can use this to detect gaps (if they track the
last seen `seq`).

### New method: `session/permission_response` (Phase 4)

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 55,
  "method": "session/permission_response",
  "params": {
    "session_id": "session-1714000000000-0",
    "request_id": "perm-xyz789",
    "decision": "allow",
    "reason": null
  }
}
```

**Result:**
```json
{"jsonrpc": "2.0", "id": 55, "result": {"ok": true}}
```

**Errors:**
- `-32004` `RequestAlreadyResolved` — another client already responded
- `-32005` `NoSuchPermissionRequest` — request_id unknown or expired

### New notification: `session/permission_request` (Phase 4)

```json
{
  "jsonrpc": "2.0",
  "method": "session/permission_request",
  "params": {
    "session_id": "session-1714000000000-0",
    "request_id": "perm-xyz789",
    "tool_name": "Bash",
    "input_preview": "rm -rf /tmp/work_dir",
    "required_mode": "danger-full-access",
    "current_mode": "workspace-write",
    "reason": "Bash requires danger-full-access in this context"
  }
}
```

### New notification: `session/permission_timeout` (Phase 4)

```json
{
  "jsonrpc": "2.0",
  "method": "session/permission_timeout",
  "params": {
    "session_id": "session-1714000000000-0",
    "request_id": "perm-xyz789"
  }
}
```

### Updated `ServerCapabilities`

```rust
pub struct ServerCapabilities {
    pub sessions: bool,
    pub streaming: bool,      // true from Phase 2
    pub tools: bool,          // true from Phase 2
    pub permissions: bool,    // true from Phase 4
    pub protocol_version: String,  // "0.1" → "0.2" (Phase 2) → "0.3" (Phase 4)
}
```

### Error code table

| Code   | Name                   | When                                      |
|--------|------------------------|-------------------------------------------|
| -32001 | UnknownSession         | session_id not found                      |
| -32002 | SessionLoadFailed      | storage backend error on load             |
| -32003 | SessionBusy            | turn already in progress                  |
| -32004 | RequestAlreadyResolved | second permission response                |
| -32005 | NoSuchPermissionRequest| request_id unknown or timed out           |

---

## 4. `SessionEvent` Enum (ACP event schema)

`SessionEvent` is the in-process type flowing through `broadcast::Sender` AND
the JSON shape stored in `session_events.payload`. It must be `Serialize +
Deserialize + Clone + Send + Sync`.

```rust
/// All events that can occur within an ACP session turn.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    /// Partial assistant text chunk.
    TextDelta {
        turn_id: String,
        text: String,
    },
    /// Partial reasoning/thinking chunk (extended-thinking providers).
    ThinkingDelta {
        turn_id: String,
        text: String,
    },
    /// The model has requested a tool call.
    ToolUseStart {
        turn_id: String,
        tool_use_id: String,
        tool_name: String,
        input: String,
    },
    /// The tool has returned a result.
    ToolResult {
        turn_id: String,
        tool_use_id: String,
        tool_name: String,
        output: String,
        is_error: bool,
    },
    /// Token usage stats for the turn.
    Usage {
        turn_id: String,
        input_tokens: u32,
        output_tokens: u32,
        cache_read_input_tokens: u32,
        cache_creation_input_tokens: u32,
    },
    /// A compaction event fired during the turn.
    Compaction {
        turn_id: String,
        summary: String,
        removed_message_count: usize,
    },
    /// The turn has ended (assistant message complete).
    TurnEnd {
        turn_id: String,
    },
    /// The turn ended with an error.
    TurnError {
        turn_id: String,
        code: i64,
        message: String,
    },
    /// A permission prompt is pending; all clients must consider responding.
    PermissionRequest {
        request_id: String,
        tool_name: String,
        input_preview: String,
        required_mode: String,
        current_mode: String,
        reason: Option<String>,
    },
    /// A client's lag exceeded BROADCAST_CAPACITY; the client is disconnected.
    ClientLagged {
        dropped: usize,
    },
}
```

### JSON wire format for `session_events.payload`

The `payload` JSONB column stores the `SessionEvent` serialized via
`serde_json::to_value`. Because the enum uses `#[serde(tag = "type")]`, the
stored JSON always has a `"type"` discriminant field, e.g.:

```json
{"type": "text_delta", "turn_id": "turn-abc", "text": "Hello"}
{"type": "tool_use_start", "turn_id": "turn-abc", "tool_use_id": "tool-1", "tool_name": "Bash", "input": "ls /tmp"}
{"type": "turn_end", "turn_id": "turn-abc"}
```

`StoredEvent.role` is set as follows:
- `TextDelta`, `ThinkingDelta`, `ToolUseStart` → `"assistant"`
- `ToolResult` → `"tool"`
- `TurnEnd`, `TurnError`, `Usage`, `Compaction` → `null`
- `PermissionRequest`, `PermissionResponse` → `null`

---

## 5. Broadcast Fan-out Topology

```
┌──────────────────────────────────────────────────────────────┐
│  SessionSlot (per session, behind Arc<Mutex<_>>)             │
│                                                              │
│  session: Session                                            │
│  path: PathBuf  (file backend)                               │
│  broadcast_tx: Arc<broadcast::Sender<SessionEvent>>          │
│  turn_in_progress: Arc<AtomicBool>                           │
│  next_seq: Arc<AtomicI32>                                    │
│  pending_permission: Option<oneshot::Sender<Decision>>       │
└──────────────────────────────────────────────────────────────┘
        │ Arc clone
        ▼
┌───────────────────────────────┐
│  TurnDriver task              │
│  (spawned per session/prompt) │
│                               │
│  1. lock SessionSlot          │
│  2. set turn_in_progress=true │
│  3. for each AssistantEvent:  │
│     a. convert → SessionEvent │
│     b. inc next_seq           │
│     c. backend.append_event() │
│     d. broadcast_tx.send()    │
│  4. set turn_in_progress=false│
└───────────────────────────────┘
        │ broadcast::Sender
        │ (capacity=256)
        ├──────────────────────────────┐
        ▼                              ▼
┌─────────────────────┐   ┌─────────────────────┐
│ WS client task A    │   │ WS client task B     │
│ broadcast::Receiver │   │ broadcast::Receiver  │
│                     │   │                      │
│ on recv:            │   │ on recv:             │
│  send notification  │   │  send notification   │
│  via transport      │   │  via transport       │
└─────────────────────┘   └─────────────────────┘
```

**Key invariant**: the TurnDriver writes to storage BEFORE calling
`broadcast_tx.send()`. This means any event visible in the broadcast channel
is already durable. A client that attaches after the driver has started is
guaranteed to find at least the events it missed in storage.

**Broadcast capacity**: `BROADCAST_CAPACITY = 256` events. This allows a
0-latency subscriber (e.g., a fast local CLI) to lag up to 256 events behind
without being dropped. For slow clients (e.g., a mobile client on a weak
connection) the lag budget is consumed faster. The `ClientLagged` event is
sent BEFORE disconnecting so the client can reconnect and resume via storage
replay.

**SessionSlot lock discipline**: The `Mutex<SessionSlot>` is held:
1. During `session/new` creation (brief write).
2. During `session/resume` registry insertion (brief write).
3. During `session/close` removal (brief write).
4. During `session/prompt` to check `turn_in_progress` and start the TurnDriver
   (the driver itself does NOT hold the slot mutex during the turn — it uses
   the `Arc<AtomicBool>` and `Arc<AtomicI32>` for turn state and seq tracking).

This avoids a deadlock where the slot mutex blocks the TurnDriver while client
tasks are waiting for notifications that would only arrive once the driver
proceeds.

---

## 6. Catch-up Replay Algorithm

When client C attaches to session S that already has events:

```
1. C sends session/resume(session_id=S)
2. Handler acquires read lock on session registry → finds slot for S
3. Handler acquires session slot mutex briefly to get broadcast_tx
4. Handler calls broadcast_tx.subscribe() → gets Receiver R
   (subscribe happens BEFORE the DB query to avoid missing live events)
5. Handler queries backend.load_events(session_id=S, since_seq=0)
   → returns stored_events [e1, e2, ..., eN] (seq 1..N)
6. Handler sends stored_events to client C as burst of session/update notifications
7. Handler checks if any events arrived on R while DB query was running:
   - R.try_recv() until empty or error
   - For each received event e: if e.seq > N (not already sent), send to C
8. Handler enters live relay loop: for each event from R, forward to C

Gap safety proof:
- step 4 subscribes before step 5 queries
- TurnDriver writes to storage, THEN broadcasts
- Therefore: any event that entered the broadcast channel before step 4 is
  already in storage and captured in step 5
- Any event that enters the channel between step 5 and step 7 is caught in step 7
- No event can enter the channel but NOT storage (invariant: write before broadcast)
- Therefore no gaps are possible
```

**Implementation note**: The `broadcast::Receiver` has a history buffer of size
`BROADCAST_CAPACITY`. If the DB query in step 5 takes longer than the time to
fill 256 events, the receiver lags and events are lost. In practice, a DB query
should complete in < 50 ms; filling 256 events at typical LLM token rates takes
many seconds. This is an acceptable trade-off for the single-host Fase 0 topology.

---

## 7. Open WebUI Pipe — Structure and Pseudocode

File: `integrations/openwebui/claw_acp_pipe.py`

```python
"""
claw_acp_pipe.py — Open WebUI Pipe Function for claw ACP daemon.

Installation:
1. Open WebUI → Settings → Functions → Create Function
2. Paste this file content.
3. Configure Valves: daemon_url, workspace_root.
4. Select "claw" as model in any conversation.

Requirements: websockets (pre-installed in Open WebUI container >= 0.3.x)
"""
import asyncio, json, uuid
from typing import AsyncGenerator
import websockets  # type: ignore

class Pipe:
    class Valves(BaseModel):
        daemon_url: str = "ws://claw-daemon:7800"
        workspace_root: str = "/workspace"
        session_ttl_hours: int = 24
        connect_timeout_s: float = 30.0
        turn_timeout_s: float = 300.0

    def __init__(self):
        self.valves = self.Valves()
        self._sessions: dict[str, str] = {}  # conversation_id → session_id

    async def pipe(self, body: dict) -> AsyncGenerator[str, None]:
        conversation_id = body.get("metadata", {}).get("conversation_id")
        model = body.get("model", "default")
        user_text = body["messages"][-1]["content"]

        async with websockets.connect(
            self.valves.daemon_url,
            open_timeout=self.valves.connect_timeout_s,
        ) as ws:
            await self._rpc(ws, "initialize", {
                "protocol_version": "0.2",
                "client_info": {"name": "openwebui-pipe", "version": "1.0"},
            })

            session_id = self._sessions.get(conversation_id)
            if session_id:
                try:
                    await self._rpc(ws, "session/resume", {"session_id": session_id})
                except AcpError:
                    session_id = None

            if not session_id:
                result = await self._rpc(ws, "session/new", {
                    "workspace_root": self.valves.workspace_root,
                    "model": model,
                })
                session_id = result["session_id"]
                if conversation_id:
                    self._sessions[conversation_id] = session_id

            turn_id = str(uuid.uuid4())
            await self._rpc(ws, "session/prompt", {
                "session_id": session_id,
                "text": user_text,
                "turn_id": turn_id,
            })

            # Stream session/update notifications until TurnEnd
            async for raw in ws:
                msg = json.loads(raw)
                if "method" not in msg:
                    continue  # skip RPC responses
                if msg["method"] != "session/update":
                    continue
                event = msg["params"]["event"]
                etype = event.get("type")
                if etype == "text_delta":
                    yield self._sse_chunk(event["text"])
                elif etype == "tool_use_start":
                    yield self._sse_comment(f"tool: {event['tool_name']}({event['input'][:80]})")
                elif etype == "tool_result":
                    yield self._sse_comment(f"result: {event['output'][:80]}")
                elif etype == "permission_request":
                    yield self._sse_comment(
                        f"PERMISSION_REQUEST:{json.dumps(event)}"
                    )
                elif etype == "turn_end":
                    yield "data: [DONE]\n\n"
                    break
                elif etype == "turn_error":
                    yield self._sse_chunk(f"[Error {event['code']}]: {event['message']}")
                    break

    def _sse_chunk(self, text: str) -> str:
        payload = json.dumps({"choices": [{"delta": {"content": text}}]})
        return f"data: {payload}\n\n"

    def _sse_comment(self, text: str) -> str:
        return f"data: [COMMENT:{text}]\n\n"

    async def _rpc(self, ws, method: str, params: dict) -> dict:
        req_id = str(uuid.uuid4())
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}))
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("id") == req_id:
                if "error" in msg:
                    raise AcpError(msg["error"]["code"], msg["error"]["message"])
                return msg.get("result", {})

class AcpError(Exception):
    def __init__(self, code: int, message: str):
        self.code = code
        super().__init__(f"ACP error {code}: {message}")
```

### Valves configuration table

| Valve             | Type   | Default                   | Description                        |
|-------------------|--------|---------------------------|------------------------------------|
| `daemon_url`      | str    | `ws://claw-daemon:7800`   | ACP daemon WebSocket address       |
| `workspace_root`  | str    | `/workspace`              | Workspace root for new sessions    |
| `session_ttl_hours` | int  | 24                        | Not enforced by daemon today (docs)|
| `connect_timeout_s` | float | 30.0                     | WS connect timeout in seconds      |
| `turn_timeout_s`  | float  | 300.0                     | Max seconds to wait for TurnEnd    |

**FLAGGED FOR USER REVIEW (C)**: The `daemon_url` default `ws://claw-daemon:7800`
assumes the daemon runs as a container named `claw-daemon` in the compose network.
If the daemon runs on the host (not containerized), the URL must be
`ws://host.docker.internal:7800` (Docker for Mac/Windows) or the host's bridge
IP on Linux. This must be documented in the compose stack README.

---

## 8. M4 Permission Prompt Sequence Diagram

```
Client A              Daemon (SessionSlot)          TurnDriver        Client B
   │                         │                          │                 │
   │ session/prompt ─────────►                          │                 │
   │                         │──── spawn TurnDriver ───►│                 │
   │ result: {turn_id} ◄─────│                          │                 │
   │                         │                          │                 │
   │                         │◄─── ToolUseStart ────────│                 │
   │ session/update ◄────────│                          │                 │
   │  (tool_use_start)       │──── session/update ─────────────────────►│
   │                         │                          │                 │
   │                         │◄─── (needs permission)───│                 │
   │                         │  create oneshot channel  │                 │
   │                         │  pending_permission=Some(tx)               │
   │                         │                          │                 │
   │ session/update ◄────────│  broadcast perm_request  │                 │
   │  (permission_request)   │──── session/update ─────────────────────►│
   │                         │                          │(driver blocks   │
   │                         │                          │ on rx)          │
   │                         │                          │                 │
   │ session/permission ─────►                          │                 │
   │  _response {allow}      │  tx.send(Allow)          │                 │
   │                         │──────────────────────────►│                │
   │ result: {ok:true} ◄─────│                          │                 │
   │                         │                          │                 │
   │                         │              (driver unblocks)             │
   │                         │◄─── ToolResult ───────────│                │
   │ session/update ◄────────│                           │                │
   │  (tool_result)          │──── session/update ─────────────────────►│
   │                         │                           │                │
   │                         │◄─── TurnEnd ──────────────│                │
   │ session/update ◄────────│                           │                │
   │  (turn_end)             │──── session/update ─────────────────────►│
   │                         │                           │                │
   
   [If Client B tries to respond after Client A:]
   
   Client B ─ session/permission_response ──────────────────────────────►
   Daemon ──────────────────────────── result: error -32004 ────────────►
```

### AcpPermissionPrompter struct design

```rust
/// Implements PermissionPrompter by blocking on a oneshot channel.
/// Lives inside SessionSlot as Option<PendingPermissionRequest>.
pub struct PendingPermissionRequest {
    pub request_id: String,
    pub tx: oneshot::Sender<PermissionPromptDecision>,
}

/// The async variant used by TurnDriver. Not PermissionPrompter directly
/// (that trait is sync) — TurnDriver calls decide_async instead.
impl SessionSlot {
    pub async fn request_permission(
        &mut self,
        request: &PermissionRequest,
        broadcast_tx: &broadcast::Sender<SessionEvent>,
        timeout_secs: u64,
    ) -> PermissionPromptDecision;
}
```

**Design rationale**: `PermissionPrompter` (from `runtime::permissions`) is a
synchronous trait (`fn decide(&mut self, ...) -> PermissionPromptDecision`). The
existing `ConversationRuntime` calls it synchronously. To avoid changing the
`ConversationRuntime` API, the `AcpPermissionPrompter` wraps a
`tokio::runtime::Handle` and uses `handle.block_on(rx.await)` to block the
runtime-thread call while the async daemon task awaits the response. This is
safe because `ConversationRuntime` runs inside a `tokio::task::spawn_blocking`
call in the TurnDriver (the runtime is not async-native). This avoids changing
the `runtime` crate's trait signatures.

**FLAGGED FOR USER REVIEW (D)**: This means `ConversationRuntime` MUST run in
`spawn_blocking` in the TurnDriver, not in an `async` block. If it already runs
in an async context (I could not find `spawn_blocking` calls in the current
TurnDriver scaffold since it doesn't exist yet), the permission prompt architecture
needs revision. The APPLY agent must verify the exact calling convention before
implementing.

---

## 9. M5 Go/No-Go Decision

**Decision: CONDITIONAL GO** — proceed with Phase 5 only after Phases 1-4 are
complete and stable, and only if the protocol delta (see below) is < 200 LOC.

### Research findings (preliminary, based on public repo analysis)

The `slopus/happy` client targets the `zed-industries/agent-client-protocol`
spec. As of early 2026, the ACP spec defines:
- `initialize` / `initialized` handshake
- Session lifecycle methods (subset of what M2 implements)
- Tool call streaming (M3 spec)
- Permission prompts (M4 spec)

The primary unknowns for compatibility:
1. **Message framing**: Does happy use Content-Length framing (stdio), raw WebSocket
   text frames, or its own framing? The claw daemon already supports both; no
   adapter needed if happy uses standard WebSocket text frames.
2. **Method name alignment**: The ACP spec method names (`session/new`,
   `session/prompt`, etc.) should match if both implementations follow the spec.
   Drift is possible because the spec is still evolving.
3. **Capability negotiation**: Does happy check `capabilities.streaming` before
   sending prompts? Unknown.

**Recommendation**: After Phase 4 is stable, attempt a manual interop test:
1. Run `claw acp serve --addr 0.0.0.0:7800`.
2. Point happy's ACP server URL to the daemon.
3. Record any JSON-RPC errors or missing method errors.
4. If errors are < 5 protocol deviations, implement the adapter. If > 5, file
   upstream issues and defer.

**If deferred**: Document in `docs/acp-m3/happy-interop-blockers.md` with
specific issue numbers. The overall M3 implementation is not blocked by M5.

---

## 10. Open Design Questions (Ambiguities Extended from ADR-012)

These were not fully specified in ADR-012 and were resolved here. Flag for
user review before APPLY starts.

| ID | Question | Decision Made | Risk |
|----|----------|---------------|------|
| A | How is `seq` generated to avoid races? | AtomicI32 on SessionSlot; single-writer per session | Low — enforced by SessionBusy |
| B | How does FileSessionBackend bridge sync session API to async trait? | spawn_blocking for file I/O | Low — file backend is test-only |
| C | What is the default daemon URL for the Pipe in Docker compose? | ws://claw-daemon:7800; host variant needs README | Medium — must configure correctly |
| D | Does ConversationRuntime run in spawn_blocking in TurnDriver? | Assumed yes; APPLY must verify | High — if no, permission prompt arch changes |
| E | When does the session_clients reaper run? | Daemon startup + every 5 min via tokio interval | Low |
| F | What is client_id for WebSocket clients? | UUID4 generated at WS accept time, not from ACP handshake | Low |
| G | Does session/list show dormant sessions from Postgres AND live in-memory? | Yes, merged and deduplicated by session_id | Low |
| H | Is sqlx compile-time query checking feasible in this worktree? | Prefer sqlx::query! with DATABASE_URL at compile time; fallback to runtime if CI DB not available | Medium |
