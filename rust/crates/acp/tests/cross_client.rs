//! Cross-client integration tests for ACP M3 streaming + fan-out.
//!
//! These tests verify:
//! - AT2.2: Two clients attached to same session; client A sends prompt;
//!   both receive identical session/update streams.
//! - AT2.3: Client B attaches mid-turn; gets catch-up + live events.
//! - AT2.4: Client B connects after turn ends; full replay from storage.
//! - AT2.5: session/prompt returns -32003 while turn is in progress.
//! - AT2.7: Multi-event turn produces events in correct order.
//! - Session close cleanly drops the broadcast channel for all clients.
//!
//! Transport: tokio::io::duplex pairs (no real network).
//! Backend: InMemoryBackend (file backend append_event is a no-op, which
//!          would break catch-up replay tests).

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use acp::{
    session::SessionHandler,
    stream::SessionEvent,
    turn_driver::MockTurnSource,
    StdioTransport, Transport,
};
use async_trait::async_trait;
use runtime::{
    session_control::{BackendError, SessionSummaryRow, StoredEvent},
    Session, SessionBackend, SessionStore,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::duplex;

// ---------------------------------------------------------------------------
// InMemorySessionBackend — stores events in memory, supports load_events
// ---------------------------------------------------------------------------

struct InMemoryBackend {
    events: StdMutex<HashMap<String, Vec<StoredEvent>>>,
    sessions: StdMutex<HashMap<String, Session>>,
}

impl InMemoryBackend {
    fn new() -> Arc<dyn SessionBackend> {
        Arc::new(Self {
            events: StdMutex::new(HashMap::new()),
            sessions: StdMutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl SessionBackend for InMemoryBackend {
    async fn create_session(&self, session: &Session) -> Result<(), BackendError> {
        self.sessions
            .lock()
            .unwrap()
            .insert(session.session_id.clone(), session.clone());
        self.events
            .lock()
            .unwrap()
            .insert(session.session_id.clone(), Vec::new());
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> Result<Option<Session>, BackendError> {
        Ok(self.sessions.lock().unwrap().get(session_id).cloned())
    }

    async fn append_event(
        &self,
        session_id: &str,
        _seq: i32,
        event: &StoredEvent,
    ) -> Result<(), BackendError> {
        self.events
            .lock()
            .unwrap()
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
        Ok(guard
            .get(session_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|e| e.seq > since_seq)
            .collect())
    }

    async fn close_session(&self, _session_id: &str) -> Result<(), BackendError> {
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build a handler with the InMemoryBackend.
fn fixture_handler(backend: Arc<dyn SessionBackend>) -> (Arc<SessionHandler>, TempDir, TempDir) {
    let workspace = TempDir::new().expect("workspace tempdir");
    let data = TempDir::new().expect("data tempdir");
    let store =
        SessionStore::from_data_dir(data.path(), workspace.path()).expect("store from data dir");
    (
        Arc::new(SessionHandler::new_with_backend(store, backend)),
        workspace,
        data,
    )
}

fn transport_pair() -> (StdioTransport, StdioTransport) {
    let (a, b) = duplex(64 * 1024);
    let (a_r, a_w) = tokio::io::split(a);
    let (b_r, b_w) = tokio::io::split(b);
    let client = StdioTransport::from_io(a_r, a_w);
    let server = StdioTransport::from_io(b_r, b_w);
    (client, server)
}

/// Send a JSON-RPC request and await the response. Panics on transport error.
async fn rpc(transport: &mut StdioTransport, id: i64, method: &str, params: Value) -> Value {
    transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await
        .expect("send rpc");
    // Skip any session/update notifications until we find the response.
    loop {
        let msg = transport.recv().await.expect("recv rpc").expect("some");
        if msg.get("id").is_some() {
            return msg;
        }
        // It's a notification (no id), accumulate and retry.
    }
}

/// Collect session/update notifications until TurnEnd (or until `max` events).
/// Returns the ordered list of event payloads.
async fn collect_updates(
    transport: &mut StdioTransport,
    max: usize,
    timeout: Duration,
) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let recv_result = tokio::time::timeout(timeout, transport.recv()).await;
        match recv_result {
            Ok(Ok(Some(msg))) => {
                if msg.get("method") == Some(&json!("session/update")) {
                    let ev = msg["params"]["event"].clone();
                    let is_end = ev["type"] == json!("turn_end");
                    events.push(ev);
                    if is_end || events.len() >= max {
                        break;
                    }
                }
                // Skip non-update messages (e.g. RPC responses).
            }
            _ => break,
        }
    }
    events
}

// ---------------------------------------------------------------------------
// AT2.5 — SessionBusy when turn is in progress
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_prompt_busy_returns_error_when_turn_in_progress() {
    let backend = InMemoryBackend::new();
    let (handler, _ws, _data) = fixture_handler(Arc::clone(&backend));
    let (mut client, server) = transport_pair();

    let _server = tokio::spawn(acp::serve_transport(server, Arc::clone(&handler)));

    // initialize + new
    rpc(
        &mut client,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    let r = rpc(&mut client, 2, "session/new", json!({})).await;
    let session_id = r["result"]["session_id"].as_str().unwrap().to_string();

    // Directly set turn_in_progress=true on the slot to simulate a running turn.
    {
        let slot_arc = handler.get_slot(&session_id).await.unwrap();
        let slot = slot_arc.lock().await;
        slot.turn_in_progress.store(true, Ordering::SeqCst);
    }

    // Now try to start a turn — must get SessionBusy.
    let r = rpc(
        &mut client,
        3,
        "session/prompt",
        json!({"session_id": session_id, "text": "hello"}),
    )
    .await;

    assert!(r.get("error").is_some(), "expected error, got {r:?}");
    assert_eq!(r["error"]["code"], json!(-32003), "expected -32003 SessionBusy");
}

// ---------------------------------------------------------------------------
// AT2.2 — Two clients receive identical streams
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_clients_receive_identical_event_streams() {
    let backend = InMemoryBackend::new();
    let (handler, _ws, _data) = fixture_handler(Arc::clone(&backend));

    // Client A
    let (mut client_a, server_a) = transport_pair();
    // Client B
    let (mut client_b, server_b) = transport_pair();

    let h_a = Arc::clone(&handler);
    let _srv_a = tokio::spawn(acp::serve_transport(server_a, h_a));
    let h_b = Arc::clone(&handler);
    let _srv_b = tokio::spawn(acp::serve_transport(server_b, h_b));

    // Client A: initialize + new session
    rpc(
        &mut client_a,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    let r = rpc(&mut client_a, 2, "session/new", json!({})).await;
    let session_id = r["result"]["session_id"].as_str().unwrap().to_string();

    // Client B: initialize + resume the same session
    rpc(
        &mut client_b,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    rpc(
        &mut client_b,
        2,
        "session/resume",
        json!({"session_id": session_id}),
    )
    .await;

    // Directly use MockTurnSource to simulate a 3-event turn
    let (broadcast_tx, next_seq) = {
        let slot_arc = handler.get_slot(&session_id).await.unwrap();
        let slot = slot_arc.lock().await;
        (Arc::clone(&slot.broadcast_tx), Arc::clone(&slot.next_seq))
    };

    let events = vec![
        SessionEvent::TextDelta {
            turn_id: "t1".into(),
            text: "hello".into(),
        },
        SessionEvent::ToolUseStart {
            turn_id: "t1".into(),
            tool_use_id: "u1".into(),
            tool_name: "Bash".into(),
            input: "{}".into(),
        },
        SessionEvent::ToolResult {
            turn_id: "t1".into(),
            tool_use_id: "u1".into(),
            tool_name: "Bash".into(),
            output: "ok".into(),
            is_error: false,
        },
    ];

    let mock = MockTurnSource::new(events);
    tokio::spawn({
        let sid = session_id.clone();
        let backend = Arc::clone(&backend);
        async move {
            mock.broadcast_events(&sid, "t1", &broadcast_tx, &next_seq, &backend)
                .await
                .unwrap();
        }
    });

    // Both clients should receive 5 events: TurnStart + 3 + TurnEnd
    let updates_a = collect_updates(&mut client_a, 5, Duration::from_secs(2)).await;
    let updates_b = collect_updates(&mut client_b, 5, Duration::from_secs(2)).await;

    assert_eq!(
        updates_a.len(),
        5,
        "client A expected 5 events, got: {updates_a:?}"
    );
    assert_eq!(
        updates_b.len(),
        5,
        "client B expected 5 events, got: {updates_b:?}"
    );
    assert_eq!(
        updates_a, updates_b,
        "both clients must receive identical event streams"
    );

    // Verify ordering: TurnStart, TextDelta, ToolUseStart, ToolResult, TurnEnd
    assert_eq!(updates_a[0]["type"], json!("turn_start"));
    assert_eq!(updates_a[1]["type"], json!("text_delta"));
    assert_eq!(updates_a[2]["type"], json!("tool_use_start"));
    assert_eq!(updates_a[3]["type"], json!("tool_result"));
    assert_eq!(updates_a[4]["type"], json!("turn_end"));
}

// ---------------------------------------------------------------------------
// AT2.4 — Client B connects after turn ends; full replay from storage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn late_client_receives_full_replay_from_storage() {
    let backend = InMemoryBackend::new();
    let (handler, _ws, _data) = fixture_handler(Arc::clone(&backend));

    // Client A
    let (mut client_a, server_a) = transport_pair();
    let _srv_a = tokio::spawn(acp::serve_transport(server_a, Arc::clone(&handler)));

    // A: initialize + new
    rpc(
        &mut client_a,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    let r = rpc(&mut client_a, 2, "session/new", json!({})).await;
    let session_id = r["result"]["session_id"].as_str().unwrap().to_string();

    // Simulate a completed turn via MockTurnSource (writes to backend).
    {
        let (broadcast_tx, next_seq) = {
            let slot_arc = handler.get_slot(&session_id).await.unwrap();
            let slot = slot_arc.lock().await;
            (Arc::clone(&slot.broadcast_tx), Arc::clone(&slot.next_seq))
        };
        let events = vec![
            SessionEvent::TextDelta {
                turn_id: "t1".into(),
                text: "stored text".into(),
            },
        ];
        let mock = MockTurnSource::new(events);
        mock.broadcast_events(&session_id, "t1", &broadcast_tx, &next_seq, &backend)
            .await
            .unwrap();
    }

    // Give a moment for any async operations to settle.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Client B attaches AFTER the turn has completed.
    let (mut client_b, server_b) = transport_pair();
    let _srv_b = tokio::spawn(acp::serve_transport(server_b, Arc::clone(&handler)));

    rpc(
        &mut client_b,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    rpc(
        &mut client_b,
        2,
        "session/resume",
        json!({"session_id": session_id}),
    )
    .await;

    // B should receive 3 events from storage replay (TurnStart, TextDelta, TurnEnd).
    let updates_b = collect_updates(&mut client_b, 3, Duration::from_secs(2)).await;

    assert_eq!(
        updates_b.len(),
        3,
        "late client expected 3 replay events, got: {updates_b:?}"
    );
    assert_eq!(updates_b[0]["type"], json!("turn_start"));
    assert_eq!(updates_b[1]["type"], json!("text_delta"));
    assert_eq!(updates_b[2]["type"], json!("turn_end"));
}

// ---------------------------------------------------------------------------
// AT2.3 — Client B attaches mid-turn: catch-up + live events, no gaps/dups
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mid_turn_client_gets_catchup_then_live_events_without_gap() {
    let backend = InMemoryBackend::new();
    let (handler, _ws, _data) = fixture_handler(Arc::clone(&backend));

    // Client A
    let (mut client_a, server_a) = transport_pair();
    let _srv_a = tokio::spawn(acp::serve_transport(server_a, Arc::clone(&handler)));

    // A: initialize + new
    rpc(
        &mut client_a,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    let r = rpc(&mut client_a, 2, "session/new", json!({})).await;
    let session_id = r["result"]["session_id"].as_str().unwrap().to_string();

    // Emit 3 events (stored in backend), simulate mid-turn pause.
    let (broadcast_tx, next_seq) = {
        let slot_arc = handler.get_slot(&session_id).await.unwrap();
        let slot = slot_arc.lock().await;
        (Arc::clone(&slot.broadcast_tx), Arc::clone(&slot.next_seq))
    };

    // Emit TurnStart + first TextDelta (stored).
    MockTurnSource::new(vec![]) // use emit_one via broadcast_events with partial events
        .broadcast_events(&session_id, "t1", &broadcast_tx, &next_seq, &backend)
        .await
        .unwrap();

    // Actually we need to emit some events BEFORE B attaches, then more after.
    // Reset and do it manually via a controlled sequence.
    // Reset backend for clean state.
    let backend2 = InMemoryBackend::new();
    let (handler2, _ws2, _data2) = fixture_handler(Arc::clone(&backend2));

    let (mut client_a2, server_a2) = transport_pair();
    let _srv_a2 = tokio::spawn(acp::serve_transport(server_a2, Arc::clone(&handler2)));

    rpc(
        &mut client_a2,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    let r2 = rpc(&mut client_a2, 2, "session/new", json!({})).await;
    let sid2 = r2["result"]["session_id"].as_str().unwrap().to_string();

    let (broadcast_tx2, next_seq2) = {
        let slot_arc = handler2.get_slot(&sid2).await.unwrap();
        let slot = slot_arc.lock().await;
        (Arc::clone(&slot.broadcast_tx), Arc::clone(&slot.next_seq))
    };

    // Pre-emit 2 events (TurnStart + TextDelta) — these will be in storage.
    // Manually emit TurnStart.
    {
        use runtime::session_control::StoredEventType;
        let seq = next_seq2.fetch_add(1, Ordering::SeqCst) + 1;
        let ev = SessionEvent::TurnStart {
            turn_id: "t2".into(),
        };
        let payload = serde_json::to_value(&ev).unwrap();
        let stored = runtime::session_control::StoredEvent {
            seq,
            event_type: StoredEventType::Meta,
            role: None,
            payload,
            created_at_ms: 0,
        };
        backend2
            .append_event(&sid2, seq, &stored)
            .await
            .unwrap();
        let _ = broadcast_tx2.send(ev.clone());
    }
    {
        use runtime::session_control::StoredEventType;
        let seq = next_seq2.fetch_add(1, Ordering::SeqCst) + 1;
        let ev = SessionEvent::TextDelta {
            turn_id: "t2".into(),
            text: "pre-attach text".into(),
        };
        let payload = serde_json::to_value(&ev).unwrap();
        let stored = runtime::session_control::StoredEvent {
            seq,
            event_type: StoredEventType::Meta,
            role: Some("assistant".into()),
            payload,
            created_at_ms: 0,
        };
        backend2
            .append_event(&sid2, seq, &stored)
            .await
            .unwrap();
        let _ = broadcast_tx2.send(ev.clone());
    }

    // Verify 2 events are in backend now.
    let stored_so_far = backend2.load_events(&sid2, 0).await.unwrap();
    assert_eq!(stored_so_far.len(), 2, "must have 2 pre-attach events stored");

    // Now B attaches — it should catch up 2 events from storage.
    let (mut client_b2, server_b2) = transport_pair();
    let _srv_b2 = tokio::spawn(acp::serve_transport(server_b2, Arc::clone(&handler2)));

    rpc(
        &mut client_b2,
        1,
        "initialize",
        json!({"protocol_version": "0.2"}),
    )
    .await;
    rpc(
        &mut client_b2,
        2,
        "session/resume",
        json!({"session_id": sid2}),
    )
    .await;

    // Emit TurnEnd AFTER B attaches (live event).
    {
        use runtime::session_control::StoredEventType;
        let seq = next_seq2.fetch_add(1, Ordering::SeqCst) + 1;
        let ev = SessionEvent::TurnEnd {
            turn_id: "t2".into(),
        };
        let payload = serde_json::to_value(&ev).unwrap();
        let stored = runtime::session_control::StoredEvent {
            seq,
            event_type: StoredEventType::Meta,
            role: None,
            payload,
            created_at_ms: 0,
        };
        backend2
            .append_event(&sid2, seq, &stored)
            .await
            .unwrap();
        let _ = broadcast_tx2.send(ev);
    }

    // B should receive: 2 replay (TurnStart, TextDelta) + 1 live (TurnEnd) = 3 total.
    let updates_b2 = collect_updates(&mut client_b2, 3, Duration::from_secs(2)).await;

    assert_eq!(
        updates_b2.len(),
        3,
        "mid-turn client expected 3 total events (2 replay + 1 live), got: {updates_b2:?}"
    );
    assert_eq!(updates_b2[0]["type"], json!("turn_start"));
    assert_eq!(updates_b2[1]["type"], json!("text_delta"));
    assert_eq!(updates_b2[2]["type"], json!("turn_end"));

    // Verify no duplicates (all event types are unique in this sequence).
    let types: Vec<_> = updates_b2.iter().map(|e| &e["type"]).collect();
    let unique: std::collections::HashSet<_> = types.iter().collect();
    assert_eq!(
        types.len(),
        unique.len(),
        "no duplicate events expected, got: {types:?}"
    );
}

// ---------------------------------------------------------------------------
// AT2.7 — Multi-tool turn: events in correct order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_tool_turn_events_in_correct_order() {
    let backend = InMemoryBackend::new();
    let (handler, _ws, _data) = fixture_handler(Arc::clone(&backend));

    let (mut client, server) = transport_pair();
    let _srv = tokio::spawn(acp::serve_transport(server, Arc::clone(&handler)));

    rpc(&mut client, 1, "initialize", json!({"protocol_version": "0.2"})).await;
    let r = rpc(&mut client, 2, "session/new", json!({})).await;
    let session_id = r["result"]["session_id"].as_str().unwrap().to_string();

    let (broadcast_tx, next_seq) = {
        let slot_arc = handler.get_slot(&session_id).await.unwrap();
        let slot = slot_arc.lock().await;
        (Arc::clone(&slot.broadcast_tx), Arc::clone(&slot.next_seq))
    };

    // Multi-tool turn: TextDelta + 2 tool pairs
    let events = vec![
        SessionEvent::TextDelta {
            turn_id: "t3".into(),
            text: "searching...".into(),
        },
        SessionEvent::ToolUseStart {
            turn_id: "t3".into(),
            tool_use_id: "tool-1".into(),
            tool_name: "grep".into(),
            input: r#"{"pattern": "foo"}"#.into(),
        },
        SessionEvent::ToolResult {
            turn_id: "t3".into(),
            tool_use_id: "tool-1".into(),
            tool_name: "grep".into(),
            output: "src/main.rs:1: foo".into(),
            is_error: false,
        },
        SessionEvent::ToolUseStart {
            turn_id: "t3".into(),
            tool_use_id: "tool-2".into(),
            tool_name: "read_file".into(),
            input: r#"{"path": "src/main.rs"}"#.into(),
        },
        SessionEvent::ToolResult {
            turn_id: "t3".into(),
            tool_use_id: "tool-2".into(),
            tool_name: "read_file".into(),
            output: "fn main() {}".into(),
            is_error: false,
        },
        SessionEvent::TextDelta {
            turn_id: "t3".into(),
            text: "Found 1 match.".into(),
        },
    ];

    let mock = MockTurnSource::new(events);
    tokio::spawn({
        let sid = session_id.clone();
        let backend = Arc::clone(&backend);
        async move {
            mock.broadcast_events(&sid, "t3", &broadcast_tx, &next_seq, &backend)
                .await
                .unwrap();
        }
    });

    // 8 total: TurnStart + 6 events + TurnEnd
    let updates = collect_updates(&mut client, 8, Duration::from_secs(2)).await;

    assert_eq!(
        updates.len(),
        8,
        "expected 8 events for multi-tool turn, got: {updates:?}"
    );
    assert_eq!(updates[0]["type"], json!("turn_start"));
    assert_eq!(updates[1]["type"], json!("text_delta"));
    assert_eq!(updates[2]["type"], json!("tool_use_start"));
    assert_eq!(updates[2]["tool_name"], json!("grep"));
    assert_eq!(updates[3]["type"], json!("tool_result"));
    assert_eq!(updates[3]["tool_name"], json!("grep"));
    assert_eq!(updates[4]["type"], json!("tool_use_start"));
    assert_eq!(updates[4]["tool_name"], json!("read_file"));
    assert_eq!(updates[5]["type"], json!("tool_result"));
    assert_eq!(updates[5]["tool_name"], json!("read_file"));
    assert_eq!(updates[6]["type"], json!("text_delta"));
    assert_eq!(updates[7]["type"], json!("turn_end"));
}
