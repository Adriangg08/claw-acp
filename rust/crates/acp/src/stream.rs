//! Streaming bridge: ACP `session/update` notifications from runtime events.
//!
//! Will translate `AssistantEvent`, `ToolCall`, and usage/compaction events
//! emitted by `ConversationRuntime` into ACP stream messages. Filled in
//! across milestones M2 and M3.
