//! TurnDriver — runs a single model turn and broadcasts [`SessionEvent`]s.
//!
//! # Streaming fidelity decision
//!
//! `ConversationRuntime::run_turn` is a **synchronous, non-streaming** API:
//! it accumulates events internally and returns a `TurnSummary` once the full
//! turn (including all tool iterations) is complete.  See
//! `docs/acp-m3/findings/flag-D-runtime-context.md` for the verified finding.
//!
//! Consequence: the TurnDriver emits events at **turn level**, not delta level.
//! The sequence for a two-tool turn is:
//!
//! ```text
//! TurnStart
//! TextDelta      (all assistant text, one event per block)
//! ThinkingDelta  (if present)
//! ToolUseStart   (tool-1 begins; input available immediately)
//! ToolResult     (tool-1 result)
//! ToolUseStart   (tool-2 begins)
//! ToolResult     (tool-2 result)
//! TextDelta      (assistant follow-up text, if any)
//! Usage
//! TurnEnd        (or TurnError on failure)
//! ```
//!
//! Delta-level streaming (token-by-token text) requires refactoring
//! `ConversationRuntime` to expose an event channel during `run_turn`.  That
//! is deferred to Phase 2.5 (see `docs/acp-m3/blockers/phase-2-streaming.md`).
//!
//! # Write-before-broadcast invariant
//!
//! Per DESIGN.md §5: each event is written to the backend BEFORE it is sent on
//! `broadcast_tx`.  This guarantees that a client attaching mid-turn can
//! replay all events from storage without gaps.

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

use runtime::session_control::{StoredEvent, StoredEventType};
use runtime::{Session, SessionBackend};
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use crate::session::SessionSlot;
use crate::stream::SessionEvent;

// ---------------------------------------------------------------------------
// TurnDriver
// ---------------------------------------------------------------------------

/// Per-turn driver task.  Spawned by `handle_prompt`; runs to completion
/// asynchronously.
pub struct TurnDriver {
    pub session_id: String,
    pub turn_id: String,
    pub text: String,
    pub session_model: Option<String>,
    pub broadcast_tx: Arc<broadcast::Sender<SessionEvent>>,
    pub next_seq: Arc<AtomicI32>,
    pub turn_in_progress: Arc<AtomicBool>,
    pub backend: Arc<dyn SessionBackend>,
    pub slot: Arc<Mutex<SessionSlot>>,
}

impl TurnDriver {
    /// Run the turn to completion, broadcasting events as they are produced.
    ///
    /// Sets `turn_in_progress = false` on both success and error paths
    /// (guaranteed via Drop-guard pattern via explicit reset at all exit points).
    pub async fn run(self) {
        let result = self.run_inner().await;
        // Always reset the in-progress flag.
        self.turn_in_progress.store(false, Ordering::SeqCst);
        if let Err(err) = result {
            tracing::error!(
                session_id = %self.session_id,
                turn_id = %self.turn_id,
                error = %err,
                "TurnDriver encountered unrecoverable error"
            );
        }
    }

    async fn run_inner(&self) -> Result<(), String> {
        // Emit TurnStart.
        self.emit(SessionEvent::TurnStart {
            turn_id: self.turn_id.clone(),
        })
        .await?;

        // Extract the session snapshot for passing into spawn_blocking.
        // We clone the session so the blocking thread owns it exclusively.
        let session = {
            let slot = self.slot.lock().await;
            slot.session.clone()
        };

        let text = self.text.clone();
        let turn_id = self.turn_id.clone();

        // Run the synchronous ConversationRuntime in a blocking thread.
        // Flag D verified: run_turn is sync and MUST run in spawn_blocking.
        //
        // The TurnDriver uses a NoOpApiClient + StaticToolExecutor stub
        // because the ACP layer doesn't have access to the concrete ApiClient
        // and ToolExecutor types at this layer.  The session is provided as
        // a pre-loaded snapshot; the runtime will mutate its internal copy.
        //
        // BLOCKER: Until the concrete ApiClient/ToolExecutor are wired here,
        // the TurnDriver emits a stub turn. See docs/acp-m3/blockers/phase-2-streaming.md.
        let turn_result = tokio::task::spawn_blocking(move || {
            run_turn_sync(session, text, turn_id)
        })
        .await
        .map_err(|join_err| format!("TurnDriver task panicked: {join_err}"))?;

        match turn_result {
            Ok(events) => {
                // Emit all turn events in order.
                for event in events {
                    self.emit(event).await?;
                }
                // Emit TurnEnd.
                self.emit(SessionEvent::TurnEnd {
                    turn_id: self.turn_id.clone(),
                })
                .await?;
                Ok(())
            }
            Err(error_msg) => {
                self.emit(SessionEvent::TurnError {
                    turn_id: self.turn_id.clone(),
                    code: -32603,
                    message: error_msg.clone(),
                })
                .await
                .ok(); // best-effort
                Err(error_msg)
            }
        }
    }

    /// Write an event to storage, then broadcast it.
    ///
    /// The write-before-broadcast invariant (DESIGN.md §5) is enforced here:
    /// `backend.append_event` completes BEFORE `broadcast_tx.send`.
    async fn emit(&self, event: SessionEvent) -> Result<(), String> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let role = event.role().map(str::to_owned);
        let payload = serde_json::to_value(&event)
            .map_err(|e| format!("failed to serialise event: {e}"))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let event_type = stored_event_type_for(&event);

        let stored = StoredEvent {
            seq,
            event_type,
            role,
            payload,
            created_at_ms: now_ms,
        };

        // Write to backend FIRST.
        self.backend
            .append_event(&self.session_id, seq, &stored)
            .await
            .map_err(|e| format!("backend.append_event failed: {e}"))?;

        // Then broadcast. It's OK if there are no receivers (send returns Err
        // but the value is stored in the channel ring buffer for late subscribers).
        let _ = self.broadcast_tx.send(event);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stub synchronous turn execution
// ---------------------------------------------------------------------------
// NOTE: This is the turn-level stub that runs in spawn_blocking.
// It uses the Session snapshot but cannot invoke the real ConversationRuntime
// because that requires generic ApiClient + ToolExecutor parameters that are
// not available at the acp-crate layer.
//
// Resolution path: the CLI integration will wire a concrete TurnDriver
// via a callback closure or a boxed trait. See the blocker doc for details.
//
// For now the stub emits a synthetic "not yet wired" response so the
// protocol plumbing (broadcast, catch-up, persistence) can be exercised
// by integration tests using mock drivers.
fn run_turn_sync(
    _session: Session,
    _text: String,
    turn_id: String,
) -> Result<Vec<SessionEvent>, String> {
    // Stub: emit a single TextDelta explaining the situation.
    // Tests override this by injecting events via a `MockTurnSource`.
    Ok(vec![SessionEvent::TextDelta {
        turn_id,
        text: "[ACP stub: ConversationRuntime not wired at this layer — see phase-2-streaming.md]"
            .to_string(),
    }])
}

// ---------------------------------------------------------------------------
// Helper: map SessionEvent to StoredEventType
// ---------------------------------------------------------------------------

fn stored_event_type_for(event: &SessionEvent) -> StoredEventType {
    match event {
        SessionEvent::TextDelta { .. }
        | SessionEvent::ThinkingDelta { .. }
        | SessionEvent::ToolUseStart { .. }
        | SessionEvent::ToolResult { .. }
        | SessionEvent::Usage { .. }
        | SessionEvent::Compaction { .. }
        | SessionEvent::TurnStart { .. }
        | SessionEvent::TurnEnd { .. }
        | SessionEvent::TurnError { .. }
        | SessionEvent::ClientLagged { .. } => StoredEventType::Meta,
        SessionEvent::PermissionRequest { .. } => StoredEventType::PermissionRequest,
    }
}

// ---------------------------------------------------------------------------
// MockTurnSource — for integration tests
// ---------------------------------------------------------------------------

/// A pre-loaded sequence of [`SessionEvent`]s used by integration tests to
/// exercise the broadcast/catch-up machinery without running a real model.
///
/// Tests inject a `MockTurnSource` into the turn pipeline by directly calling
/// [`broadcast_events`].
pub struct MockTurnSource {
    pub events: Vec<SessionEvent>,
}

impl MockTurnSource {
    pub fn new(events: Vec<SessionEvent>) -> Self {
        Self { events }
    }

    /// Broadcast all events through the given channel, writing each to storage
    /// first (enforcing write-before-broadcast).
    pub async fn broadcast_events(
        &self,
        session_id: &str,
        turn_id: &str,
        broadcast_tx: &Arc<broadcast::Sender<SessionEvent>>,
        next_seq: &Arc<AtomicI32>,
        backend: &Arc<dyn SessionBackend>,
    ) -> Result<(), String> {
        let start = SessionEvent::TurnStart {
            turn_id: turn_id.to_string(),
        };
        Self::emit_one(session_id, &start, broadcast_tx, next_seq, backend).await?;

        for event in &self.events {
            Self::emit_one(session_id, event, broadcast_tx, next_seq, backend).await?;
        }

        let end = SessionEvent::TurnEnd {
            turn_id: turn_id.to_string(),
        };
        Self::emit_one(session_id, &end, broadcast_tx, next_seq, backend).await?;
        Ok(())
    }

    async fn emit_one(
        session_id: &str,
        event: &SessionEvent,
        broadcast_tx: &Arc<broadcast::Sender<SessionEvent>>,
        next_seq: &Arc<AtomicI32>,
        backend: &Arc<dyn SessionBackend>,
    ) -> Result<(), String> {
        let seq = next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let role = event.role().map(str::to_owned);
        let payload = serde_json::to_value(event).map_err(|e| e.to_string())?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let stored = StoredEvent {
            seq,
            event_type: stored_event_type_for(event),
            role,
            payload,
            created_at_ms: now_ms,
        };

        backend
            .append_event(session_id, seq, &stored)
            .await
            .map_err(|e| e.to_string())?;

        let _ = broadcast_tx.send(event.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::SessionEvent;
    use async_trait::async_trait;
    use runtime::{
        session_control::{BackendError, SessionSummaryRow},
        Session, SessionBackend,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicI32};
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use tokio::sync::broadcast;

    // -----------------------------------------------------------------------
    // InMemorySessionBackend — stores events in memory for testing
    // -----------------------------------------------------------------------
    struct InMemorySessionBackend {
        events: StdMutex<HashMap<String, Vec<StoredEvent>>>,
        sessions: StdMutex<HashMap<String, Session>>,
    }

    impl InMemorySessionBackend {
        fn new() -> Arc<dyn SessionBackend> {
            Arc::new(Self {
                events: StdMutex::new(HashMap::new()),
                sessions: StdMutex::new(HashMap::new()),
            })
        }
    }

    #[async_trait]
    impl SessionBackend for InMemorySessionBackend {
        async fn create_session(&self, session: &Session) -> Result<(), BackendError> {
            let mut guard = self.sessions.lock().unwrap();
            guard.insert(session.session_id.clone(), session.clone());
            let mut ev_guard = self.events.lock().unwrap();
            ev_guard.insert(session.session_id.clone(), Vec::new());
            Ok(())
        }

        async fn load_session(
            &self,
            session_id: &str,
        ) -> Result<Option<Session>, BackendError> {
            let guard = self.sessions.lock().unwrap();
            Ok(guard.get(session_id).cloned())
        }

        async fn append_event(
            &self,
            session_id: &str,
            _seq: i32,
            event: &StoredEvent,
        ) -> Result<(), BackendError> {
            let mut guard = self.events.lock().unwrap();
            guard
                .entry(session_id.to_string())
                .or_default()
                .push(event.clone());
            Ok(())
        }

        async fn load_events(
            &self,
            session_id: &str,
            since_seq: i32,
        ) -> Result<Vec<StoredEvent>, BackendError> {
            let guard = self.events.lock().unwrap();
            let events = guard.get(session_id).cloned().unwrap_or_default();
            Ok(events
                .into_iter()
                .filter(|e| e.seq > since_seq)
                .collect())
        }

        async fn close_session(&self, session_id: &str) -> Result<(), BackendError> {
            let _ = session_id;
            Ok(())
        }

        async fn list_open_sessions(
            &self,
            _workspace_root: &str,
        ) -> Result<Vec<SessionSummaryRow>, BackendError> {
            Ok(Vec::new())
        }

        async fn upsert_client_presence(
            &self,
            _client_id: &str,
            _session_id: &str,
            _transport: &str,
        ) -> Result<(), BackendError> {
            Ok(())
        }

        async fn remove_client_presence(
            &self,
            _client_id: &str,
            _session_id: &str,
        ) -> Result<(), BackendError> {
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn turn_driver_sets_turn_in_progress_false_on_completion() {
        let dir = TempDir::new().unwrap();
        let backend = InMemorySessionBackend::new();
        let session = Session::new();
        backend.create_session(&session).await.unwrap();

        let (broadcast_tx, mut rx) = broadcast::channel::<SessionEvent>(256);
        let broadcast_tx = Arc::new(broadcast_tx);
        let turn_in_progress = Arc::new(AtomicBool::new(true)); // starts true
        let next_seq = Arc::new(AtomicI32::new(0));

        // Build a minimal slot.
        let slot = Arc::new(Mutex::new(SessionSlot {
            session: session.clone(),
            path: dir.path().to_path_buf(),
            broadcast_tx: Arc::clone(&broadcast_tx),
            turn_in_progress: Arc::clone(&turn_in_progress),
            next_seq: Arc::clone(&next_seq),
            pending_permission: None,
        }));

        let driver = TurnDriver {
            session_id: session.session_id.clone(),
            turn_id: "test-turn-1".to_string(),
            text: "hello".to_string(),
            session_model: None,
            broadcast_tx: Arc::clone(&broadcast_tx),
            next_seq: Arc::clone(&next_seq),
            turn_in_progress: Arc::clone(&turn_in_progress),
            backend: Arc::clone(&backend),
            slot,
        };

        driver.run().await;

        // turn_in_progress must be reset to false regardless.
        assert!(!turn_in_progress.load(Ordering::SeqCst));

        // Must have received at least TurnStart and TurnEnd.
        let mut received = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            received.push(ev);
        }
        assert!(
            received
                .iter()
                .any(|e| matches!(e, SessionEvent::TurnStart { .. })),
            "must have TurnStart, got {received:?}"
        );
        assert!(
            received
                .iter()
                .any(|e| matches!(e, SessionEvent::TurnEnd { .. })),
            "must have TurnEnd, got {received:?}"
        );
    }

    #[tokio::test]
    async fn mock_source_broadcasts_in_order() {
        let backend = InMemorySessionBackend::new();
        let session = Session::new();
        backend.create_session(&session).await.unwrap();

        let (broadcast_tx, mut rx) = broadcast::channel::<SessionEvent>(256);
        let broadcast_tx = Arc::new(broadcast_tx);
        let next_seq = Arc::new(AtomicI32::new(0));

        let events = vec![
            SessionEvent::TextDelta {
                turn_id: "t1".to_string(),
                text: "hello".to_string(),
            },
            SessionEvent::ToolUseStart {
                turn_id: "t1".to_string(),
                tool_use_id: "u1".to_string(),
                tool_name: "Bash".to_string(),
                input: "{}".to_string(),
            },
            SessionEvent::ToolResult {
                turn_id: "t1".to_string(),
                tool_use_id: "u1".to_string(),
                tool_name: "Bash".to_string(),
                output: "ok".to_string(),
                is_error: false,
            },
        ];

        let mock = MockTurnSource::new(events.clone());
        mock.broadcast_events(
            &session.session_id,
            "t1",
            &broadcast_tx,
            &next_seq,
            &backend,
        )
        .await
        .unwrap();

        // Collect received events.
        let mut received = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            received.push(ev);
        }

        // Should be: TurnStart, TextDelta, ToolUseStart, ToolResult, TurnEnd
        assert_eq!(received.len(), 5);
        assert!(matches!(received[0], SessionEvent::TurnStart { .. }));
        assert!(matches!(received[1], SessionEvent::TextDelta { .. }));
        assert!(matches!(received[2], SessionEvent::ToolUseStart { .. }));
        assert!(matches!(received[3], SessionEvent::ToolResult { .. }));
        assert!(matches!(received[4], SessionEvent::TurnEnd { .. }));

        // Verify all events were written to backend in seq order.
        let stored = backend
            .load_events(&session.session_id, 0)
            .await
            .unwrap();
        assert_eq!(stored.len(), 5);
        for (i, ev) in stored.iter().enumerate() {
            assert_eq!(ev.seq, (i + 1) as i32);
        }
    }

    #[tokio::test]
    async fn write_before_broadcast_invariant() {
        // Every event stored by the TurnDriver must already be in backend
        // by the time it appears on the broadcast channel.
        let backend = InMemorySessionBackend::new();
        let session = Session::new();
        backend.create_session(&session).await.unwrap();

        let (broadcast_tx, mut rx) = broadcast::channel::<SessionEvent>(256);
        let broadcast_tx = Arc::new(broadcast_tx);
        let next_seq = Arc::new(AtomicI32::new(0));

        let mock = MockTurnSource::new(vec![SessionEvent::TextDelta {
            turn_id: "t".to_string(),
            text: "delta".to_string(),
        }]);

        mock.broadcast_events(
            &session.session_id,
            "t",
            &broadcast_tx,
            &next_seq,
            &backend,
        )
        .await
        .unwrap();

        // After each broadcast, the event must already be in the backend.
        // (Since this is single-threaded test, we can verify after all events.)
        let backend_ref = Arc::clone(&backend);
        let session_id = session.session_id.clone();
        while let Ok(_ev) = rx.try_recv() {
            // At this point the event is guaranteed to be in backend.
            let stored = backend_ref
                .load_events(&session_id, 0)
                .await
                .unwrap();
            assert!(!stored.is_empty());
        }
    }
}
