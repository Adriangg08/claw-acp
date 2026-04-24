# Phase 2 — Streaming Fidelity Blocker

**Status**: Documented / Deferred to Phase 2.5  
**Date**: 2026-04-24  
**Author**: Phase 2 APPLY agent  

---

## Decision: Turn-level streaming (not delta-level)

### Finding

`ConversationRuntime::run_turn` (in `rust/crates/runtime/src/conversation.rs:320`)
is a **synchronous, non-streaming** API:

```rust
pub fn run_turn(
    &mut self,
    user_input: impl Into<String>,
    mut prompter: Option<&mut dyn PermissionPrompter>,
) -> Result<TurnSummary, RuntimeError> {
```

It accumulates all model streaming events (text deltas, tool calls) internally and
returns a single `TurnSummary` struct at the end. There is no callback mechanism
for receiving events as they are emitted by the underlying `ApiClient::stream`.

The `ApiClient::stream` method is also synchronous:
```rust
pub trait ApiClient {
    fn stream(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError>;
}
```

It returns a `Vec<AssistantEvent>` (already accumulated), not a stream.

### Impact

The TurnDriver in `rust/crates/acp/src/turn_driver.rs` runs `ConversationRuntime::run_turn`
inside `tokio::task::spawn_blocking` (correct per Flag D finding), but emits events
**turn-level**: text is emitted as a single `TextDelta` event per `ContentBlock::Text`,
not as streaming tokens.

### Emitted event sequence (current — turn-level)

For a turn with one tool call:

```
TurnStart
TextDelta     (all assistant text as one chunk, not streaming tokens)
ThinkingDelta (if present, also full block)
ToolUseStart  (tool begins)
ToolResult    (tool result)
TextDelta     (assistant follow-up, full block)
Usage
TurnEnd
```

### Desired sequence (delta-level — requires Phase 2.5)

```
TurnStart
TextDelta("I'll")
TextDelta(" check")
TextDelta(" the")
...
ToolUseStart
ToolResult
TextDelta("Found")
...
TurnEnd
```

### Resolution path (Phase 2.5)

Option A: Refactor `ApiClient::stream` to return an `impl Iterator<Item = AssistantEvent>`
or a channel, allowing the TurnDriver to emit events as they arrive from the model.

Option B: Add a callback/visitor pattern to `ConversationRuntime`:
```rust
pub fn run_turn_streaming(
    &mut self,
    user_input: impl Into<String>,
    on_event: &mut dyn FnMut(AssistantEvent),
    prompter: Option<&mut dyn PermissionPrompter>,
) -> Result<TurnSummary, RuntimeError>
```

Option C: Make `ApiClient::stream` async and return a `tokio::sync::mpsc::Receiver`
or `futures::Stream`. This requires making `ConversationRuntime` async-aware.

**Recommendation**: Option B is the least invasive (doesn't require changing all
callers of `ApiClient::stream`). Option C provides the cleanest API but requires
broader refactoring.

### Current impact on clients

- **SPEC AT2.1**: The spec requires `TextDelta, ToolUseStart, ToolResult, TextDelta, TurnEnd`
  — this IS satisfied (turn-level, not delta-level, but same sequence).
- **Open WebUI Pipe (Phase 3)**: Text arrives as a single chunk per turn rather than
  streaming tokens. UX impact: text appears all at once rather than progressively.
  The pipe still works correctly; the streaming illusion is absent until Phase 2.5.
- **Fan-out (AT2.2, AT2.3)**: Fully working — all clients receive events via the
  broadcast channel correctly.

### Files affected

- `rust/crates/acp/src/turn_driver.rs` — TurnDriver::run_inner, run_turn_sync
- `rust/crates/runtime/src/conversation.rs` — will need refactoring
- `rust/crates/runtime/src/conversation.rs` — ApiClient::stream API
