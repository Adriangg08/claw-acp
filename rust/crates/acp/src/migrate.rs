//! One-shot JSONL → Postgres migration helper.
//!
//! Reads all JSONL session files from a [`SessionStore`] root, converts each
//! [`ConversationMessage`] to a `session_events` row (with
//! `event_type='message'`), and each `SessionCompaction` to a row with
//! `event_type='compaction'`.
//!
//! Idempotent: `ON CONFLICT (session_id, seq) DO NOTHING` means re-running
//! after a partial migration inserts only new rows.
//!
//! CLI: `claw acp migrate`  — see SPEC.md F1.5 and TASKS.md T1.8.

use std::time::{SystemTime, UNIX_EPOCH};

use runtime::session_control::{SessionStore, StoredEvent, StoredEventType};
use runtime::{BackendError, SessionBackend};
use tracing::{info, warn};

use crate::backend_postgres::PostgresSessionBackend;

/// Report produced by a migration run.
#[derive(Debug, Default)]
pub struct MigrationReport {
    /// Sessions successfully migrated (new rows inserted).
    pub sessions_migrated: usize,
    /// Sessions already present (skipped).
    pub sessions_skipped: usize,
    /// Total event rows inserted.
    pub events_inserted: usize,
    /// Sessions that failed to migrate (I/O or parse errors).
    pub sessions_failed: usize,
}

impl std::fmt::Display for MigrationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== ACP Migration Report ===")?;
        writeln!(f, "Sessions migrated : {}", self.sessions_migrated)?;
        writeln!(f, "Sessions skipped  : {}", self.sessions_skipped)?;
        writeln!(f, "Events inserted   : {}", self.events_inserted)?;
        writeln!(f, "Sessions failed   : {}", self.sessions_failed)?;
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Migrate all JSONL sessions in `store` to `pg`.
///
/// Idempotent — safe to call multiple times; only inserts rows that don't
/// already exist (keyed by `(session_id, seq)`).
pub async fn migrate_store_to_postgres(
    store: &SessionStore,
    pg: &PostgresSessionBackend,
) -> Result<MigrationReport, BackendError> {
    let mut report = MigrationReport::default();

    let summaries = store
        .list_sessions()
        .map_err(|e| BackendError::Io(std::io::Error::other(e.to_string())))?;

    info!(
        count = summaries.len(),
        "starting JSONL → Postgres migration"
    );

    for summary in &summaries {
        let loaded = match store.load_session(&summary.id) {
            Ok(l) => l,
            Err(e) => {
                warn!(session_id = %summary.id, error = %e, "failed to load session — skipping");
                report.sessions_failed += 1;
                continue;
            }
        };

        let session = &loaded.session;

        // Try to create the session record; skip if it already exists.
        match pg.create_session(session).await {
            Ok(()) => {
                report.sessions_migrated += 1;
            }
            Err(BackendError::Database(ref msg)) if msg.contains("duplicate") || msg.contains("unique") => {
                report.sessions_skipped += 1;
                // Still process events — they use ON CONFLICT DO NOTHING.
            }
            Err(e) => {
                warn!(session_id = %session.session_id, error = %e, "failed to create session row");
                report.sessions_failed += 1;
                continue;
            }
        }

        // Emit one event per compaction (seq=1 if compaction is present).
        let mut seq: i32 = 1;
        if let Some(compaction) = &session.compaction {
            let event = StoredEvent {
                seq,
                event_type: StoredEventType::Compaction,
                role: None,
                payload: serde_json::json!({
                    "type": "compaction",
                    "count": compaction.count,
                    "removed_message_count": compaction.removed_message_count,
                    "summary": compaction.summary,
                }),
                created_at_ms: now_ms(),
            };
            if let Err(e) = pg.append_event(&session.session_id, seq, &event).await {
                warn!(session_id = %session.session_id, seq, error = %e, "failed to insert compaction event");
            } else {
                report.events_inserted += 1;
            }
            seq += 1;
        }

        // Emit one event per message.
        for msg in &session.messages {
            use runtime::{ContentBlock, MessageRole};

            let role = match msg.role {
                MessageRole::User => "user",
                MessageRole::Assistant => "assistant",
                MessageRole::Tool => "tool",
                MessageRole::System => "system",
            };

            // Serialize the message blocks as a JSON array.
            let blocks: Vec<serde_json::Value> = msg
                .blocks
                .iter()
                .map(|b| match b {
                    ContentBlock::Text { text } => {
                        serde_json::json!({"type": "text", "text": text})
                    }
                    ContentBlock::Thinking { reasoning } => {
                        serde_json::json!({"type": "thinking", "reasoning": reasoning})
                    }
                    ContentBlock::ToolUse { id, name, input } => {
                        serde_json::json!({
                            "type": "tool_use",
                            "id": id,
                            "name": name,
                            "input": input,
                        })
                    }
                    ContentBlock::ToolResult { tool_use_id, tool_name, output, is_error } => {
                        serde_json::json!({
                            "type": "tool_result",
                            "tool_use_id": tool_use_id,
                            "tool_name": tool_name,
                            "output": output,
                            "is_error": is_error,
                        })
                    }
                })
                .collect();

            let event = StoredEvent {
                seq,
                event_type: StoredEventType::Message,
                role: Some(role.to_string()),
                payload: serde_json::json!({
                    "type": "message",
                    "role": role,
                    "blocks": blocks,
                }),
                created_at_ms: now_ms(),
            };

            if let Err(e) = pg.append_event(&session.session_id, seq, &event).await {
                warn!(session_id = %session.session_id, seq, error = %e, "failed to insert message event");
            } else {
                report.events_inserted += 1;
            }

            seq += 1;
        }
    }

    info!(
        migrated = report.sessions_migrated,
        skipped = report.sessions_skipped,
        events = report.events_inserted,
        failed = report.sessions_failed,
        "JSONL → Postgres migration complete"
    );

    Ok(report)
}
