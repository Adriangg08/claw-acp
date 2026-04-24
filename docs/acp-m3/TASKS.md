# ACP M3 — Task Breakdown

Generated: 2026-04-24
Branch: `feature/acp-daemon-m3`

All tasks are ordered so each depends only on tasks with lower numbers.
"Files touched" paths are relative to the worktree root.

---

## Legend

- **LOC** = Lines of Rust (or Python) excluding comments and blank lines
- **Test** = required test coverage (unit = in same file, integration = separate test binary)
- **Dep** = tasks that must be complete before this task starts

---

## Phase 1 — Postgres Session Backend

### T1.1 — Add `sqlx` dependency to `acp` crate

**Phase**: 1
**Dep**: none
**Files touched**:
- `rust/crates/acp/Cargo.toml`
- `rust/Cargo.toml` (workspace deps if using workspace dep table)

**LOC estimate**: 10
**Test coverage**: none (build-only change)
**Notes**: Add `sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "json", "macros"] }`. Also add `uuid = { version = "1", features = ["v4"] }` and `tokio-util` if not already present.

---

### T1.2 — Define `StoredEvent` and `SessionBackend` trait in `runtime::session_control`

**Phase**: 1
**Dep**: none
**Files touched**:
- `rust/crates/runtime/src/session_control.rs`

**LOC estimate**: 80
**Test coverage**: none (trait definition only)
**Notes**: Add `StoredEvent`, `StoredEventType`, `SessionSummaryRow`, `BackendError` types. Add `SessionBackend` trait with 8 async methods (see DESIGN.md §2). Mark the trait `#[async_trait]`. Export from `runtime::session_control` module.

---

### T1.3 — Implement `FileSessionBackend`

**Phase**: 1
**Dep**: T1.2
**Files touched**:
- `rust/crates/runtime/src/session_control.rs` (or new `src/backend_file.rs`)

**LOC estimate**: 120
**Test coverage**:
- Unit: `create_session` + `load_session` round-trip using `TempDir`
- Unit: `append_event` writes correct seq, `load_events` replays in order
- Unit: `close_session` sets closed_at_ms equivalent (for file: move to closed subdir or mark in JSONL meta record)

---

### T1.4 — Implement `PostgresSessionBackend`

**Phase**: 1
**Dep**: T1.2, T1.3
**Files touched**:
- `rust/crates/acp/src/backend_postgres.rs` (new file)
- `rust/crates/acp/src/lib.rs` (pub mod backend_postgres)

**LOC estimate**: 200
**Test coverage**:
- Integration test using `testcontainers` crate (dev-dependency): spawn Postgres 16, create schema, run all `SessionBackend` methods, assert ordering and idempotency.
- File: `rust/crates/acp/tests/postgres_backend.rs`

**Notes**: Use `sqlx::PgPool::connect()`. All SQL uses `sqlx::query!` macros with `DATABASE_URL` env set in `.cargo/config.toml` for development. For CI without a DB, gate with `#[cfg(feature = "postgres-tests")]`.

---

### T1.5 — Postgres init script for `acp` database

**Phase**: 1
**Dep**: T1.4
**Files touched**:
- `compose/fase-0/postgres/init/30-acp.sql` (new file; naming with prefix 30 to control order)

**LOC estimate**: 40 (SQL, not Rust)
**Test coverage**: Manual — run `docker compose up postgres`, verify `acp` DB and tables exist.
**Notes**: Must be idempotent. Follow the pattern of existing init scripts in the compose stack.

---

### T1.6 — Wire `CLAW_SESSION_BACKEND` env var into `acp::serve`

**Phase**: 1
**Dep**: T1.4
**Files touched**:
- `rust/crates/acp/src/lib.rs`
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 50
**Test coverage**:
- Unit: `serve` with `CLAW_SESSION_BACKEND=file` builds a `FileSessionBackend`.
- Unit: with `CLAW_SESSION_BACKEND=postgres` and no DB URL, returns error before listen.

**Notes**: `build_session_store()` becomes `build_session_backend()` returning `Arc<dyn SessionBackend>`. `SessionHandler` gains a `backend: Arc<dyn SessionBackend>` field. `SessionStore` is kept for the `file` path (wraps existing logic).

---

### T1.7 — Update `session/new`, `session/resume`, `session/close`, `session/list` to use `SessionBackend`

**Phase**: 1
**Dep**: T1.6
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 80
**Test coverage**:
- Existing unit tests in `session.rs` must still pass (they use `FileSessionBackend`).
- New unit test: `handle_list` returns dormant Postgres sessions (mock backend).

---

### T1.8 — One-shot JSONL migration CLI command

**Phase**: 1
**Dep**: T1.4, T1.7
**Files touched**:
- `rust/crates/acp/src/migrate.rs` (new file)
- CLI main (wherever `claw acp` subcommands are registered — likely `rust/src/main.rs` or equivalent)

**LOC estimate**: 100
**Test coverage**:
- Unit: `migrate_session_to_postgres()` with a temp JSONL file and a `PostgresSessionBackend` (via testcontainer) inserts correct rows.
- Idempotency test: run migration twice, assert row count unchanged.

---

### T1.9 — Session client presence upsert and reaper task

**Phase**: 1
**Dep**: T1.4
**Files touched**:
- `rust/crates/acp/src/session.rs`
- `rust/crates/acp/src/lib.rs` (spawn reaper on serve start)

**LOC estimate**: 60
**Test coverage**:
- Unit: reaper deletes stale rows (rows with `last_seen_ms` > 5 min old) and keeps fresh rows.

---

**Phase 1 total LOC estimate**: ~740 Rust + 40 SQL = ~780 LOC

---

## Phase 2 — M3 Tool-call Streaming + Fan-out

### T2.1 — Add `broadcast` and `AtomicI32` fields to `SessionSlot`

**Phase**: 2
**Dep**: T1.7
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 30
**Test coverage**: none (struct change)
**Notes**: `broadcast_tx: Arc<broadcast::Sender<SessionEvent>>`, `turn_in_progress: Arc<AtomicBool>`, `next_seq: Arc<AtomicI32>`. Import `tokio::sync::broadcast`. Add `use tokio::sync::broadcast::Sender as BroadcastSender`.

---

### T2.2 — Define `SessionEvent` enum

**Phase**: 2
**Dep**: T1.2
**Files touched**:
- `rust/crates/acp/src/stream.rs` (was empty — fill it in)

**LOC estimate**: 60
**Test coverage**:
- Unit: each `SessionEvent` variant round-trips through `serde_json::to_value` / `from_value`.

---

### T2.3 — Implement `AssistantEvent` → `SessionEvent` conversion

**Phase**: 2
**Dep**: T2.2
**Files touched**:
- `rust/crates/acp/src/stream.rs`

**LOC estimate**: 50
**Test coverage**:
- Unit: `AssistantEvent::TextDelta("hello")` → `SessionEvent::TextDelta { text: "hello", turn_id: ... }`.
- Unit: `AssistantEvent::ToolUse { id, name, input }` → `SessionEvent::ToolUseStart { ... }`.

---

### T2.4 — TurnDriver task

**Phase**: 2
**Dep**: T2.1, T2.3
**Files touched**:
- `rust/crates/acp/src/turn_driver.rs` (new file)
- `rust/crates/acp/src/lib.rs`

**LOC estimate**: 150
**Test coverage**:
- Unit: mock `ApiClient` that emits 3 events; assert TurnDriver broadcasts all 3 and writes them to a mock `SessionBackend`.
- Unit: TurnDriver sets `turn_in_progress=false` on both success and error.

**Notes**: TurnDriver runs in `tokio::spawn`. It takes `Arc<Mutex<SessionSlot>>`, `Arc<dyn SessionBackend>`, the prompt text, and a `turn_id`. It acquires the slot mutex only for slot state reads/writes, not during the model call.

---

### T2.5 — Implement `session/prompt` handler

**Phase**: 2
**Dep**: T2.4
**Files touched**:
- `rust/crates/acp/src/session.rs`
- `rust/crates/acp/src/lib.rs` (route `session/prompt` in dispatch)

**LOC estimate**: 60
**Test coverage**:
- Unit: `session/prompt` returns `-32003` if `turn_in_progress=true`.
- Integration: `session/prompt` on a live session triggers TurnDriver and returns `{status: "started"}`.

---

### T2.6 — Client task broadcast relay

**Phase**: 2
**Dep**: T2.5
**Files touched**:
- `rust/crates/acp/src/lib.rs` (update `run_dispatch_loop`)
- `rust/crates/acp/src/session.rs` (`handle_resume` subscribes to broadcast channel)

**LOC estimate**: 80
**Test coverage**:
- Integration: two `StdioTransport` pairs (duplex) attached to the same session; client A sends `session/prompt`; both A and B receive `session/update` notifications with matching events.

---

### T2.7 — Catch-up replay on `session/resume`

**Phase**: 2
**Dep**: T2.6, T1.4
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 70
**Test coverage**:
- Integration: client B attaches after client A has received 5 events; B receives all 5 events from storage replay, then 0 duplicates from the live channel.
- Integration (mid-turn): client B attaches while a turn is in progress after 3 events; B receives 3 from replay + remaining events from live channel.

---

### T2.8 — Update `ServerCapabilities` to M3

**Phase**: 2
**Dep**: T2.5
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 10
**Test coverage**: Unit — `handle_initialize` returns `streaming: true, tools: true`.

---

**Phase 2 total LOC estimate**: ~510 Rust

---

## Phase 3 — Open WebUI Pipe Function

### T3.1 — Write `claw_acp_pipe.py`

**Phase**: 3
**Dep**: T2.5 (needs session/prompt to work end-to-end)
**Files touched**:
- `integrations/openwebui/claw_acp_pipe.py` (new file)

**LOC estimate**: 120 (Python)
**Test coverage**:
- Manual test procedure documented in the file's module docstring.
- No automated CI test (Open WebUI cannot be spawned in CI).

---

### T3.2 — Pipe integration runbook

**Phase**: 3
**Dep**: T3.1
**Files touched**:
- `docs/acp-m3/pipe-install-runbook.md` (new file)

**LOC estimate**: 0 Rust (docs only)
**Test coverage**: n/a

---

**Phase 3 total LOC estimate**: ~120 Python

---

## Phase 4 — M4 Permission Prompt Broadcast

### T4.1 — Add `PendingPermissionRequest` to `SessionSlot`

**Phase**: 4
**Dep**: T2.4
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 30
**Test coverage**: none (struct change)

---

### T4.2 — Implement `AcpPermissionPrompter`

**Phase**: 4
**Dep**: T4.1
**Files touched**:
- `rust/crates/acp/src/tools.rs` (was empty — fill it in)

**LOC estimate**: 100
**Test coverage**:
- Unit: `request_permission()` resolves immediately when `tx.send()` is called.
- Unit: `request_permission()` resolves with `Deny` after timeout.
- Unit: second call to `request_permission()` while first is pending queues correctly.

---

### T4.3 — Wire permission request into TurnDriver

**Phase**: 4
**Dep**: T4.2, T2.4
**Files touched**:
- `rust/crates/acp/src/turn_driver.rs`

**LOC estimate**: 60
**Test coverage**:
- Integration: tool call that triggers `PermissionMode::Prompt` sends `session/permission_request` notification; providing `session/permission_response` allows the tool to proceed.

---

### T4.4 — Implement `session/permission_response` handler

**Phase**: 4
**Dep**: T4.1
**Files touched**:
- `rust/crates/acp/src/session.rs`
- `rust/crates/acp/src/lib.rs` (route in dispatch)

**LOC estimate**: 50
**Test coverage**:
- Unit: second response returns `-32004`.
- Unit: response to unknown `request_id` returns `-32005`.

---

### T4.5 — Add permission events to `SessionEvent` and storage

**Phase**: 4
**Dep**: T4.3, T1.4
**Files touched**:
- `rust/crates/acp/src/stream.rs` (add `PermissionRequest` variant)
- `rust/crates/acp/src/turn_driver.rs`
- `rust/crates/acp/src/backend_postgres.rs` (no changes needed — `StoredEventType::PermissionRequest` already defined in T1.2)

**LOC estimate**: 30
**Test coverage**:
- Integration: permission request and response both appear in `session_events` with correct `event_type`.

---

### T4.6 — Update `ServerCapabilities` to M4

**Phase**: 4
**Dep**: T4.4
**Files touched**:
- `rust/crates/acp/src/session.rs`

**LOC estimate**: 5
**Test coverage**: Unit — `handle_initialize` returns `permissions: true`.

---

**Phase 4 total LOC estimate**: ~275 Rust

---

## Phase 5 — M5 slopus/happy Interop (Stretch)

### T5.1 — Research happy protocol compatibility

**Phase**: 5
**Dep**: T4.6 (all prior phases stable)
**Files touched**:
- `docs/acp-m3/happy-interop-blockers.md` (new — may be empty if no blockers)

**LOC estimate**: 0 Rust (research + docs)
**Test coverage**: Manual interop test (see SPEC AT5.1, AT5.2)

---

### T5.2 — Implement `AcpHappyAdapter` (if feasible, see M5 go/no-go)

**Phase**: 5
**Dep**: T5.1
**Files touched**:
- `rust/crates/acp/src/happy.rs` (new file, feature-gated)
- `rust/crates/acp/Cargo.toml` (`[features] happy = []`)

**LOC estimate**: 0–200 Rust (depends on protocol delta; may be 0 if no adapter needed)
**Test coverage**: Manual only (cannot spawn happy in CI)

---

**Phase 5 total LOC estimate**: 0–200 Rust

---

## Task Summary

| Phase | Task count | LOC estimate | Key deliverable               |
|-------|------------|--------------|-------------------------------|
| 1     | 9          | ~780         | Postgres backend + migration  |
| 2     | 8          | ~510         | Streaming + fan-out           |
| 3     | 2          | ~120 Python  | Open WebUI Pipe               |
| 4     | 6          | ~275         | Permission prompts            |
| 5     | 2          | 0-200        | happy interop (stretch)       |
| **Total** | **27** | **~1685–1885** | —                         |

---

## Suggested APPLY Batches

Apply agents should work in these batches. Each batch is independently
committable (all tests pass at the end of the batch).

| Batch | Tasks       | Description                                  |
|-------|-------------|----------------------------------------------|
| B1    | T1.1–T1.5   | Trait + FileBackend + PostgresBackend + SQL  |
| B2    | T1.6–T1.9   | Wire backend into serve + migration + reaper |
| B3    | T2.1–T2.4   | SessionSlot + SessionEvent + TurnDriver      |
| B4    | T2.5–T2.8   | session/prompt + relay + catch-up + caps     |
| B5    | T3.1–T3.2   | Python Pipe + install runbook                |
| B6    | T4.1–T4.4   | Permission slot + prompter + handler         |
| B7    | T4.5–T4.6   | Permission events + storage + caps           |
| B8    | T5.1–T5.2   | happy research + adapter (stretch)           |
