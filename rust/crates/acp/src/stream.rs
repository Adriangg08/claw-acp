//! Streaming bridge: ACP `session/update` notifications from runtime events.
//!
//! [`SessionEvent`] is the canonical in-process type that flows through
//! `tokio::sync::broadcast` AND is serialised as `session_events.payload` in
//! the backend. It must be `Serialize + Deserialize + Clone + Send + Sync`.
//!
//! See DESIGN.md §4 for the JSON wire format and role-assignment rules.

use runtime::AssistantEvent;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// SessionEvent enum — all variants per DESIGN.md §4
// ---------------------------------------------------------------------------

/// All events that can occur within an ACP session turn.
///
/// Serialised with `#[serde(tag = "type", rename_all = "snake_case")]` so the
/// stored JSON always has a `"type"` discriminant, e.g.
/// `{"type": "text_delta", "turn_id": "…", "text": "…"}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    /// Partial assistant text chunk.
    TextDelta {
        turn_id: String,
        text: String,
    },
    /// Chain-of-thought / reasoning delta (extended-thinking providers).
    ThinkingDelta {
        turn_id: String,
        text: String,
    },
    /// The model has requested a tool call (execution is about to start).
    ToolUseStart {
        turn_id: String,
        tool_use_id: String,
        tool_name: String,
        /// Full input JSON string as emitted by the model.
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
    /// Token usage stats for the turn (emitted once at turn end).
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
    /// The turn started.
    TurnStart {
        turn_id: String,
    },
    /// The turn completed successfully.
    TurnEnd {
        turn_id: String,
    },
    /// The turn ended with an error.
    TurnError {
        turn_id: String,
        code: i64,
        message: String,
    },
    /// A permission prompt is pending; all clients may respond.
    PermissionRequest {
        request_id: String,
        tool_name: String,
        input_preview: String,
        required_mode: String,
        current_mode: String,
        reason: Option<String>,
    },
    /// A client's lag exceeded `BROADCAST_CAPACITY`; the client is disconnected.
    ClientLagged {
        dropped: usize,
    },
}

impl SessionEvent {
    /// The `role` value stored alongside the event in `session_events`.
    ///
    /// Follows the role-assignment rules from DESIGN.md §4.
    #[must_use]
    pub fn role(&self) -> Option<&'static str> {
        match self {
            Self::TextDelta { .. }
            | Self::ThinkingDelta { .. }
            | Self::ToolUseStart { .. } => Some("assistant"),
            Self::ToolResult { .. } => Some("tool"),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// T2.3 — AssistantEvent → SessionEvent conversion
// ---------------------------------------------------------------------------

/// Context needed to convert a runtime [`AssistantEvent`] into a
/// [`SessionEvent`].  The `turn_id` is fixed for the lifetime of one turn;
/// the driver supplies it at call time.
pub struct EventConverter {
    pub turn_id: String,
}

impl EventConverter {
    #[must_use]
    pub fn new(turn_id: impl Into<String>) -> Self {
        Self {
            turn_id: turn_id.into(),
        }
    }

    /// Convert a runtime [`AssistantEvent`] into zero or one [`SessionEvent`]s.
    ///
    /// Returns `None` for events that do not have a direct ACP representation
    /// (e.g. `MessageStop`, `PromptCache`). The driver handles `ToolUse` specially
    /// — it emits a `ToolUseStart` here and a `ToolResult` after execution.
    #[must_use]
    pub fn convert(&self, event: &AssistantEvent) -> Option<SessionEvent> {
        match event {
            AssistantEvent::TextDelta(text) => Some(SessionEvent::TextDelta {
                turn_id: self.turn_id.clone(),
                text: text.clone(),
            }),
            AssistantEvent::ThinkingDelta(text) => Some(SessionEvent::ThinkingDelta {
                turn_id: self.turn_id.clone(),
                text: text.clone(),
            }),
            AssistantEvent::ToolUse { id, name, input } => Some(SessionEvent::ToolUseStart {
                turn_id: self.turn_id.clone(),
                tool_use_id: id.clone(),
                tool_name: name.clone(),
                input: input.clone(),
            }),
            AssistantEvent::Usage(usage) => Some(SessionEvent::Usage {
                turn_id: self.turn_id.clone(),
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
            }),
            // PromptCache and MessageStop are telemetry / protocol artefacts —
            // they have no ACP wire representation.
            AssistantEvent::PromptCache(_) | AssistantEvent::MessageStop => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::TokenUsage;
    use serde_json;

    fn round_trip(event: &SessionEvent) -> SessionEvent {
        let v = serde_json::to_value(event).expect("serialize");
        serde_json::from_value(v).expect("deserialize")
    }

    #[test]
    fn text_delta_round_trips() {
        let ev = SessionEvent::TextDelta {
            turn_id: "t1".to_string(),
            text: "hello world".to_string(),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn thinking_delta_round_trips() {
        let ev = SessionEvent::ThinkingDelta {
            turn_id: "t1".to_string(),
            text: "step 1".to_string(),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn tool_use_start_round_trips() {
        let ev = SessionEvent::ToolUseStart {
            turn_id: "t2".to_string(),
            tool_use_id: "tool-123".to_string(),
            tool_name: "Bash".to_string(),
            input: r#"{"cmd":"ls"}"#.to_string(),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn tool_result_round_trips() {
        let ev = SessionEvent::ToolResult {
            turn_id: "t2".to_string(),
            tool_use_id: "tool-123".to_string(),
            tool_name: "Bash".to_string(),
            output: "file1\nfile2".to_string(),
            is_error: false,
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn turn_end_round_trips() {
        let ev = SessionEvent::TurnEnd {
            turn_id: "t3".to_string(),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn turn_error_round_trips() {
        let ev = SessionEvent::TurnError {
            turn_id: "t3".to_string(),
            code: -32603,
            message: "internal error".to_string(),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn client_lagged_round_trips() {
        let ev = SessionEvent::ClientLagged { dropped: 5 };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn permission_request_round_trips() {
        let ev = SessionEvent::PermissionRequest {
            request_id: "perm-1".to_string(),
            tool_name: "Bash".to_string(),
            input_preview: "rm -rf /".to_string(),
            required_mode: "danger".to_string(),
            current_mode: "workspace".to_string(),
            reason: Some("needs full access".to_string()),
        };
        assert_eq!(round_trip(&ev), ev);
    }

    #[test]
    fn type_discriminant_is_snake_case() {
        let ev = SessionEvent::TextDelta {
            turn_id: "t".to_string(),
            text: "x".to_string(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "text_delta");
    }

    #[test]
    fn tool_use_start_type_discriminant() {
        let ev = SessionEvent::ToolUseStart {
            turn_id: "t".to_string(),
            tool_use_id: "id".to_string(),
            tool_name: "N".to_string(),
            input: "{}".to_string(),
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "tool_use_start");
    }

    // T2.3 conversion tests

    #[test]
    fn convert_text_delta() {
        let conv = EventConverter::new("turn-1");
        let ae = AssistantEvent::TextDelta("hello".to_string());
        let se = conv.convert(&ae).unwrap();
        assert!(matches!(se, SessionEvent::TextDelta { text, .. } if text == "hello"));
    }

    #[test]
    fn convert_thinking_delta() {
        let conv = EventConverter::new("turn-1");
        let ae = AssistantEvent::ThinkingDelta("step".to_string());
        let se = conv.convert(&ae).unwrap();
        assert!(matches!(se, SessionEvent::ThinkingDelta { text, .. } if text == "step"));
    }

    #[test]
    fn convert_tool_use_emits_tool_use_start() {
        let conv = EventConverter::new("turn-1");
        let ae = AssistantEvent::ToolUse {
            id: "tool-xyz".to_string(),
            name: "Bash".to_string(),
            input: r#"{"cmd":"ls"}"#.to_string(),
        };
        let se = conv.convert(&ae).unwrap();
        match se {
            SessionEvent::ToolUseStart {
                tool_use_id,
                tool_name,
                input,
                ..
            } => {
                assert_eq!(tool_use_id, "tool-xyz");
                assert_eq!(tool_name, "Bash");
                assert_eq!(input, r#"{"cmd":"ls"}"#);
            }
            other => panic!("expected ToolUseStart, got {other:?}"),
        }
    }

    #[test]
    fn convert_usage() {
        let conv = EventConverter::new("turn-1");
        let usage = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cache_read_input_tokens: 10,
            cache_creation_input_tokens: 5,
        };
        let ae = AssistantEvent::Usage(usage);
        let se = conv.convert(&ae).unwrap();
        match se {
            SessionEvent::Usage {
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(input_tokens, 100);
                assert_eq!(output_tokens, 50);
            }
            other => panic!("expected Usage, got {other:?}"),
        }
    }

    #[test]
    fn convert_message_stop_returns_none() {
        let conv = EventConverter::new("turn-1");
        let ae = AssistantEvent::MessageStop;
        assert!(conv.convert(&ae).is_none());
    }
}
