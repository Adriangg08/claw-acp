# ACP M3 — Specification

Generated: 2026-04-24
Branch: `feature/acp-daemon-m3`
ADR reference: ADR-012 (session storage, fan-out, pipe)

---

## Overview

This specification covers the five implementation phases required to bring the
ACP daemon from M2 (session lifecycle) to full bidirectional multi-client
support. Each phase is independently testable; phases must be applied in order
because each one is a dependency of the next.

---

## Phase 1 — Postgres Session Backend

### Objective

Replace the JSONL file store with a Postgres-backed event log while keeping
the `FileSessionBackend` active for unit tests and backward compat.

### Functional Requirements

1. **F1.1** A `SessionBackend` trait in `runtime::session_control` abstracts
   create/load/append/list/close operations over sessions. Existing code that
   calls `SessionStore` methods directly continues to compile via an adapter
   that implements `SessionBackend` on top of the existing file logic.

2. **F1.2** A `PostgresSessionBackend` struct implements `SessionBackend` using
   three tables in a dedicated `acp` Postgres database (see DESIGN.md for DDL).
   The implementation uses `sqlx` with the `postgres` feature and compile-time
   query checking via `sqlx::query!` macros.

3. **F1.3** The `acp` database is created by a one-time Docker init script
   (`/docker-entrypoint-initdb.d/acp.sql`) following the same pattern as
   `litellm` and `openwebui` databases in the compose stack. The init script
   creates the `acp` user, `acp` database, and runs the three-table DDL.
   It must be idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE USER IF NOT EXISTS`).

4. **F1.4** Runtime selection is done via environment variable `CLAW_SESSION_BACKEND`:
   - `file` (default) — existing JSONL behavior, no Postgres dependency.
   - `postgres` — requires `DATABASE_URL` or `CLAW_PG_URL` to be set.

5. **F1.5** A one-shot CLI sub-command `claw acp migrate` reads all existing JSONL
   session files from the configured `SessionStore` root, converts them to
   `session_events` rows (each `ConversationMessage` → one row with `event_type='message'`,
   each `SessionCompaction` → one row with `event_type='compaction'`), and writes
   a migration report to stdout.

6. **F1.6** `session/list` is updated to query both live (in-memory) and dormant
   (Postgres only, `closed_at_ms IS NULL`) sessions, merging the two sets
   and deduplicating by `session_id`.

7. **F1.7** `session/resume` loads from Postgres if the session is not live,
   populating the `SessionSlot` from the stored rows.

### Non-functional Requirements

1. **NF1.1** Each write to `session_events` must complete within 50 ms under
   normal Postgres load. Writes block the current turn's async task; use
   `sqlx::query!` with a pool of max 10 connections.

2. **NF1.2** The `acp` Postgres database must not be created or connected to
   unless `CLAW_SESSION_BACKEND=postgres` is set. Existing CLI users with
   only `file` backend see zero new dependencies or startup latency.

3. **NF1.3** Connection failures during a turn are surfaced as a JSON-RPC
   `-32603 InternalError` response, not a panic. The daemon continues serving
   other sessions.

4. **NF1.4** The migration script is idempotent: re-running after partial
   migration inserts only rows not yet present (check by `(session_id, seq)` unique key).

### Out of Scope

- Multi-host Postgres replication or connection pooling via PgBouncer.
- Schema versioning / migration tooling beyond the initial create script.
- Read replicas.

### Acceptance Tests

- AT1.1: Create a session with `CLAW_SESSION_BACKEND=postgres`, push 3 messages,
  restart the daemon, resume the session — all 3 messages are present.
- AT1.2: `session/list` returns both live (in-memory) and dormant (Postgres,
  `closed_at_ms IS NULL`) sessions without duplicates.
- AT1.3: Running `claw acp migrate` against a workspace with 2 JSONL files
  inserts the correct number of rows and is idempotent on second run.
- AT1.4: With `CLAW_SESSION_BACKEND=file`, the daemon starts without a DB
  connection and behaves exactly as M2.
- AT1.5: Postgres connection failure during `session/new` returns JSON-RPC error
  `-32603` with a descriptive message; the daemon remains alive.
- AT1.6: Integration test using `testcontainers-rs` spawns a real Postgres 16
  container, runs all CRUD operations via `PostgresSessionBackend`, and verifies
  ordering by `seq`.

---

## Phase 2 — M3 Tool-call Streaming + Fan-out

### Objective

Enable `session/prompt`: a client sends user input, the daemon runs the model
turn end-to-end, and every client attached to that session receives the stream
as `session/update` notifications in real time.

### Functional Requirements

1. **F2.1** A new ACP method `session/prompt` accepts `{session_id, text}` and
   starts a model turn. The method returns a single JSON-RPC result
   `{session_id, turn_id}` immediately (the turn runs asynchronously). Stream
   events arrive as notifications via `session/update`.

2. **F2.2** `session/update` is a JSON-RPC notification (no `id` field) pushed
   to all clients attached to the given session. Its payload carries a
   `SessionEvent` (see DESIGN.md for variants).

3. **F2.3** Each `SessionEvent` is persisted to `session_events` (Postgres) or
   appended to the JSONL file (file backend) BEFORE it is broadcast in-memory.
   This guarantees that a client that attaches mid-turn can catch up from storage
   without missing events.

4. **F2.4** `SessionSlot` gains an `Arc<broadcast::Sender<SessionEvent>>` field.
   Each WebSocket client task subscribes a `broadcast::Receiver` on attach. The
   `ConversationRuntime` output is consumed by a dedicated driver task per
   session that writes to storage, then sends to the broadcast channel.

5. **F2.5** Catch-up replay on client attach:
   - New client sends `session/resume`.
   - The handler reads all events for the session from storage ordered by `seq`.
   - Subscribes to the broadcast channel BEFORE completing the DB read (record
     the high-watermark seq from Postgres before the channel subscribe, then
     re-read any events with seq > hwm from the channel's history buffer).
   - The handler streams stored events to the client as a burst of `session/update`
     notifications, then switches to the live channel.
   - No gaps: the daemon writes to storage first, so any event that arrived
     after the DB query started is in the broadcast buffer or in Postgres.

6. **F2.6** Backpressure: if a `broadcast::Receiver` falls `BROADCAST_CAPACITY`
   events behind, it is considered lagged. The client task receives a
   `SessionEvent::ClientLagged { dropped: usize }` notification and is
   disconnected. `BROADCAST_CAPACITY` is 256 by default, configurable via
   `CLAW_BROADCAST_CAPACITY` env var.

7. **F2.7** A model turn in progress when a second client attaches: the new client
   gets replay of completed events from storage, then joins the live broadcast
   mid-stream. This is valid — clients must handle receiving partial turns on
   attach.

8. **F2.8** Tool calls and tool results both appear in the stream as separate
   `SessionEvent` variants, in the exact order the runtime emits them. The full
   tool input and output are included (not truncated) so clients can render
   complete tool invocations.

9. **F2.9** `ServerCapabilities` is updated: `streaming: true, tools: true` once
   Phase 2 is deployed.

10. **F2.10** The `session/prompt` method must reject new turns if a turn is
    already in progress for the session (return `-32003 SessionBusy`).

### Non-functional Requirements

1. **NF2.1** Fan-out latency from broadcast to all attached clients is target
   < 5 ms on the local loopback (single host).

2. **NF2.2** A session with 0 attached clients runs normally; events are stored
   to Postgres for future replay. The broadcast send is a no-op when there are
   no receivers (broadcast allows 0 receivers).

3. **NF2.3** The daemon must not deadlock when a `ConversationRuntime` tool
   call blocks waiting for external I/O. The runtime runs in its own tokio
   task; it sends events via a channel, not directly to any transport.

4. **NF2.4** At turn end, the runtime's driver task drops the turn's sender
   half cleanly so all receivers see `SessionEvent::TurnEnd`.

### Out of Scope

- Flow control / back-pressure above the broadcast lag mechanism.
- Mid-turn cancel by client (planned for M4+).
- Streaming to stdio transport clients (stdio is single-client; fan-out applies
  to WebSocket multi-client only, but the broadcast infrastructure is shared).

### Acceptance Tests

- AT2.1: A single client sends `session/prompt` on a read-file task; the client
  receives (in order) `TextDelta`, `ToolUseStart`, `ToolResult`, `TextDelta`,
  `TurnEnd` notifications.
- AT2.2: Two clients attach to the same session; client A sends `session/prompt`;
  both A and B receive identical `session/update` notification streams.
- AT2.3: Client B attaches mid-turn (after the turn starts); B receives catch-up
  events from storage for the completed portion, then joins live. No events are
  duplicated or missing.
- AT2.4: Client B connects 200 ms after the turn ends; B receives the full turn
  via storage replay without any live channel subscription.
- AT2.5: Sending `session/prompt` while a turn is in progress returns `-32003`.
- AT2.6: A client that lags beyond `BROADCAST_CAPACITY` receives
  `ClientLagged` and is disconnected cleanly.
- AT2.7: A multi-tool turn (`grep` + `read` + `edit`) produces events in the
  correct order matching the runtime emission order.

---

## Phase 3 — Open WebUI Pipe Function

### Objective

A single Python file installable as an Open WebUI Function that translates
Open WebUI chat completions requests into ACP sessions, runs turns through the
daemon, and returns the response as an SSE generator.

### Functional Requirements

1. **F3.1** The Pipe file is a self-contained Python module `claw_acp_pipe.py`
   located at `integrations/openwebui/claw_acp_pipe.py` in the worktree.
   It has zero dependencies beyond the Python standard library and `websockets`
   (available in Open WebUI's container).

2. **F3.2** The Pipe class is named `Pipe` with a nested `Valves` dataclass
   configuring: `daemon_url` (str, default `ws://claw-daemon:7800`),
   `workspace_root` (str, default `/workspace`), `session_ttl_hours` (int,
   default 24).

3. **F3.3** The `pipe(body: dict) -> AsyncGenerator` method:
   - Reads `body["model"]` to determine the target model (passed as `model`
     in `session/new` params).
   - Reads `body["messages"]` (OpenAI format). Uses `messages[-1]["content"]`
     as the prompt text for `session/prompt`. Earlier messages are NOT resent
     as individual turns — session history lives in the daemon.
   - Looks up or creates a session keyed by the Open WebUI conversation ID
     (`body.get("metadata", {}).get("conversation_id", None)`). If no
     conversation ID or no cached session ID, calls `session/new`. Otherwise
     calls `session/resume`.
   - Sends `session/prompt` with the user text.
   - Yields `session/update` notification payloads as SSE chunks, converting
     `TextDelta` events to `data: {"choices": [{"delta": {"content": "..."}}]}`
     format. All other event types are yielded as `data: [COMMENT: ...]` chunks
     (invisible to the user, visible in browser devtools for debugging).
   - Sends `data: [DONE]` on `TurnEnd`.

4. **F3.4** Session ID is persisted in Open WebUI's in-memory Pipe state
   (`self._sessions: dict[str, str]`) mapping `conversation_id → session_id`.
   This survives for the lifetime of the Open WebUI server process. On restart,
   the Pipe calls `session/resume` and falls back to `session/new` on error.

5. **F3.5** If Phase 2 streaming is not yet enabled (`capabilities.streaming = false`),
   the Pipe falls back to sending `session/prompt` as a blocking call (via the
   non-streaming path) and returns the accumulated text as a single string.
   This allows the Pipe to work with Phase 1 (non-streaming) without modification.

6. **F3.6** ACP error responses (JSON-RPC `error` field) are surfaced to the user
   as an error message in the chat UI: `"[ACP error {code}]: {message}"`.

7. **F3.7** WebSocket connection per `pipe()` call: open, send `initialize`,
   send `session/resume` or `session/new`, send `session/prompt`, read until
   `TurnEnd`, close. No persistent connection is kept between calls.

### Non-functional Requirements

1. **NF3.1** The Pipe must not import any package not available in the standard
   Open WebUI container image. If `websockets` is not available, fail gracefully
   with a clear error in the UI.

2. **NF3.2** The WebSocket connection timeout is 30 s for connect and 5 min for
   turn completion. On timeout, the Pipe returns an error message to the UI
   and closes the WS.

3. **NF3.3** The Pipe is tested manually (not in CI, as Open WebUI cannot be
   spawned in CI). The test procedure is documented in the file's docstring.

### Out of Scope

- Persistent WebSocket connection pooling across calls.
- Image or file upload forwarding.
- Rendering tool calls as structured UI elements (they appear as text narration
  in the delta stream).
- Multi-model routing (the Pipe targets one daemon; model selection is passed
  as a param to `session/new`).

### Acceptance Tests

- AT3.1: Install `claw_acp_pipe.py` as a Function in Open WebUI. Select "claw"
  as the model. Send "hello". Receive a response in the chat UI.
- AT3.2: Send a prompt that triggers a tool call (e.g., "list files in /tmp").
  The tool call text narration appears in the chat as part of the response.
- AT3.3: Open two browser tabs pointing to the same Open WebUI conversation.
  Both see the same response (shared session replay).
- AT3.4: Restart the Open WebUI container. The next message correctly resumes
  the session or falls back to a new session gracefully.
- AT3.5: With daemon unreachable, the Pipe returns a descriptive error in the
  chat UI within the configured timeout.

---

## Phase 4 — M4 Permission Prompt Broadcast

### Objective

When the runtime requires interactive approval for a tool call, broadcast the
permission request to all attached clients. Accept the first valid response;
reject subsequent responses with a "session locked" error.

### Functional Requirements

1. **F4.1** A new ACP notification `session/permission_request` is sent to all
   attached clients when `PermissionMode::Prompt` is triggered. Payload:
   `{session_id, request_id, tool_name, input_preview, required_mode, reason}`.

2. **F4.2** Clients respond by sending ACP method `session/permission_response`
   with `{session_id, request_id, decision: "allow" | "deny", reason?: string}`.

3. **F4.3** The daemon's `AcpPermissionPrompter` (a struct implementing
   `PermissionPrompter`) blocks the tool execution task on a
   `tokio::sync::oneshot::Receiver<PermissionPromptDecision>`. The first
   `session/permission_response` that arrives sends on the sender half; all
   subsequent responses receive `-32004 RequestAlreadyResolved`.

4. **F4.4** If no client responds within `PERMISSION_TIMEOUT_SECS` (default 60 s,
   configurable via env var), the prompter returns `PermissionPromptDecision::Deny`
   with reason `"permission prompt timed out"`. The timeout fires a
   `session/permission_timeout` notification to all clients.

5. **F4.5** The Open WebUI Pipe handles `session/permission_request` by yielding a
   structured prompt in the SSE stream (as a `data: [PERMISSION: ...]` comment
   chunk) and waiting for the user to respond via a follow-up message. The
   follow-up message is detected if its text matches `allow` / `deny` and a
   pending `request_id` is in progress.

6. **F4.6** `ServerCapabilities` is updated: `permissions: true` once Phase 4 is
   deployed.

7. **F4.7** The permission outcome (allow/deny, who responded, timestamp) is
   recorded as a `session_events` row with `event_type='permission_response'`.

### Non-functional Requirements

1. **NF4.1** The `AcpPermissionPrompter` is async-compatible: the decision
   future is awaited without blocking the Tokio thread pool.

2. **NF4.2** Only one concurrent permission prompt per session at a time.
   A second tool call requiring permission while the first prompt is pending
   queues until the first is resolved. If the first times out, the queued
   prompt fires immediately.

3. **NF4.3** The permission prompt and response payloads must carry enough
   information for a mobile client to render without additional RPC calls:
   tool name, truncated input preview (max 512 chars), required permission
   level, and human-readable reason.

### Out of Scope

- "Remember this decision for this session" persistence.
- Policy engine changes (the existing `PermissionPolicy` rules are unchanged).
- M5 (happy) integration for permission UI — that is Phase 5.

### Acceptance Tests

- AT4.1: Run `claw` in Prompt mode against `Bash`. The first tool call sends
  a `session/permission_request` notification to all attached clients.
- AT4.2: Client A sends `session/permission_response` with `decision: "allow"`.
  The tool executes. Client B's subsequent response returns `-32004`.
- AT4.3: No client responds within 60 s (simulated with short timeout). The
  tool is denied; a `session/permission_timeout` notification is received.
- AT4.4: Two concurrent tool calls requiring permission: the second fires only
  after the first is resolved.
- AT4.5: The permission response is recorded in `session_events` and visible
  on session replay.

---

## Phase 5 — M5 slopus/happy Interop (Stretch)

### Objective

Research the `slopus/happy` relay protocol and determine whether ACP ↔ happy
interop is feasible within the current scope. If feasible, document the
connection procedure and any wire-format adaptations required.

### Functional Requirements

1. **F5.1** Research the `slopus/happy` public repository protocol documentation
   and any open issues describing ACP compatibility.

2. **F5.2** Document the delta between happy's expected wire format and claw's
   ACP JSON-RPC 2.0 format, specifically: message framing, session lifecycle
   method names, streaming event names.

3. **F5.3** If the delta is small (< 200 LOC adapter), implement an
   `AcpHappyAdapter` that translates between the two protocols. The adapter
   lives in `rust/crates/acp/src/happy.rs` behind a `happy` feature flag so
   it is not compiled by default.

4. **F5.4** If the delta is large or the protocol is undocumented, open a
   GitHub issue against `slopus/happy` and document the blocker in
   `docs/acp-m3/happy-interop-blockers.md`.

5. **F5.5** A manual interop test (not automated CI) is run and its result is
   recorded in `docs/acp-m3/happy-interop-test-log.md`.

### Non-functional Requirements

1. **NF5.1** Any happy-specific adaptation must be isolated behind a feature flag.
   The default build must not be affected.

2. **NF5.2** No modifications to the ACP wire format visible to non-happy clients
   are allowed for the sake of happy compatibility.

### Out of Scope

- Automated CI for happy interop.
- Forking or modifying the `slopus/happy` source.

### Acceptance Tests

- AT5.1 (manual): Connect `happy` mobile client to `claw acp serve --addr`.
  Send a prompt. Receive a response.
- AT5.2 (manual): Permission prompt appears in the happy UI and can be answered.

---

## Cross-cutting Non-functional Requirements

- **Observability**: All new ACP handler methods must emit `tracing::info!` /
  `tracing::debug!` spans with `session_id` and `client_id` as structured
  fields. No `println!` or `eprintln!` in production paths.
- **Error propagation**: All internal failures return structured JSON-RPC errors,
  never panic. Reserve panics for truly unrecoverable invariant violations.
- **Test coverage**: Each phase must include unit tests for the core logic and
  at least one integration test exercising the full stack (transport → handler →
  storage). See TASKS.md for per-task test requirements.
- **Backward compatibility**: The `file` backend path (CLAW_SESSION_BACKEND=file)
  must remain fully functional and pass the existing M2 test suite unchanged
  after each phase lands.
