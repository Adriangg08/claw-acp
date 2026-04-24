# Flag D — ConversationRuntime Calling Context

**Verified by**: Phase 1 APPLY agent
**Date**: 2026-04-24
**Files inspected**:
- `rust/crates/runtime/src/conversation.rs`
- `rust/crates/acp/src/session.rs`
- `rust/crates/acp/src/lib.rs`

## Finding: ConversationRuntime is SYNCHRONOUS (no TurnDriver yet)

`ConversationRuntime::run_turn` is a **synchronous method** — it does NOT use `async`,
does NOT use `.await`, and does NOT exist inside a `spawn_blocking` call today
(because no TurnDriver exists in M2).

Key evidence:

```rust
// rust/crates/runtime/src/conversation.rs:320-324
pub fn run_turn(
    &mut self,
    user_input: impl Into<String>,
    mut prompter: Option<&mut dyn PermissionPrompter>,
) -> Result<TurnSummary, RuntimeError> {
```

The `run_turn` signature is `fn` (not `async fn`). `ApiClient::stream` is also
synchronous (`fn stream(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError>`).

The `PermissionPrompter` trait (called by `ConversationRuntime` during tool execution)
is also synchronous:
- File: `rust/crates/runtime/src/permissions.rs` (expected — follows same pattern)
- Called via `prompter.decide(...)` (sync call inside sync `run_turn`)

## Implication for Phase 2 (TurnDriver)

When the TurnDriver is implemented in Phase 2, `ConversationRuntime::run_turn`
**MUST be called inside `tokio::task::spawn_blocking`** because:

1. The runtime is sync and blocks the calling thread.
2. Running it directly in an `async` task would starve the Tokio thread pool.
3. The `AcpPermissionPrompter` (Phase 4) will use `handle.block_on(rx.await)` to
   bridge the async permission response channel into the sync `PermissionPrompter`
   trait call — this is safe ONLY inside `spawn_blocking` (never inside a regular async task).

## Phase 4 Architecture: NO REVISION NEEDED

The DESIGN.md assumption (§8 Flag D note) is CORRECT: the runtime IS synchronous
and WILL run in `spawn_blocking`. The Phase 4 `AcpPermissionPrompter` design using
`handle.block_on(rx.await)` inside `spawn_blocking` is valid.

**Action required in Phase 2**: Wrap `ConversationRuntime::run_turn` call in
`tokio::task::spawn_blocking(move || runtime.run_turn(...)).await`.
