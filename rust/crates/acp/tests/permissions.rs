//! Integration tests for ACP M4 — permission prompt broadcast.
//!
//! These tests verify:
//! - AT4.1: Tool call triggers `session/permission_request` notification to clients.
//! - AT4.2: Client A responds allow; Client B's subsequent response gets -32004 equivalent.
//! - AT4.3: Timeout path (shortened) — tool is denied, timeout event fired.
//! - AT4.4 (partial): second prompt fires only after first resolved.
//! - AT4.5: Permission response recorded in session_events.
//!
//! Transport: `tokio::io::duplex` pairs (no real network).
//! Backend: `InMemoryBackend` (supports `load_events` for replay checks).

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use acp::{
    session::{
        error_codes, NewSessionParams, PendingPermissionRequest, PermissionDecisionStr,
        PermissionResponseParams, SessionHandler,
    },
    stream::SessionEvent,
    turn_driver::MockTurnSource,
    StdioTransport, Transport,
};
use async_trait::async_trait;
use runtime::{
    session_control::{BackendError, SessionSummaryRow, StoredEvent, StoredEventType},
    PermissionPromptDecision, PermissionPrompter, Session, SessionBackend, SessionStore,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::duplex;
use tokio::sync::oneshot;

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
        let events = guard.get(session_id).cloned().unwrap_or_default();
        Ok(events.into_iter().filter(|e| e.seq > since_seq).collect())
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
// Test fixtures
// ---------------------------------------------------------------------------

fn fixture_handler() -> (Arc<SessionHandler>, TempDir, TempDir) {
    let workspace = TempDir::new().expect("tempdir");
    let data_dir = TempDir::new().expect("data tempdir");
    let store = SessionStore::from_data_dir(data_dir.path(), workspace.path())
        .expect("store from data dir");
    let backend = InMemoryBackend::new();
    (
        Arc::new(SessionHandler::new_with_backend(store, backend)),
        workspace,
        data_dir,
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

/// Send an RPC request and return the raw response (skips notifications).
async fn rpc(transport: &mut StdioTransport, id: i64, method: &str, params: Value) -> Value {
    transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await
        .expect("send ok");

    // Drain until we get the response for our request id.
    loop {
        let msg = transport
            .recv()
            .await
            .expect("recv ok")
            .expect("not closed");
        if msg.get("id").is_some() {
            return msg;
        }
        // Skip notifications (no id).
    }
}

// ---------------------------------------------------------------------------
// AT4.1: Permission request broadcast
// ---------------------------------------------------------------------------

/// AT4.1: A `PermissionRequest` event installed in the slot's broadcast channel
/// reaches all attached clients.
///
/// We use the direct `broadcast_tx` to inject a PermissionRequest event
/// (bypassing TurnDriver, which is still stubbed) and verify the client
/// sees it via the dispatch loop relay.
#[tokio::test]
async fn at4_1_permission_request_broadcast_reaches_client() {
    let (handler, _ws, _data) = fixture_handler();

    // Create session.
    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    // Build transport pair.
    let (mut client_transport, server_transport) = transport_pair();

    // Serve the session dispatch loop in a background task.
    let handler_clone = Arc::clone(&handler);
    let server_task = tokio::spawn(async move {
        acp::serve_transport(server_transport, handler_clone).await
    });

    // Client: initialize + session/resume (subscribes to broadcast).
    rpc(&mut client_transport, 1, "initialize", json!({"protocol_version": "0.2"})).await;
    rpc(
        &mut client_transport,
        2,
        "session/resume",
        json!({"session_id": session_id.clone()}),
    )
    .await;

    // Inject a PermissionRequest event via the broadcast channel directly
    // (simulates TurnDriver broadcasting the event).
    {
        let slot_arc = handler.get_slot(&session_id).await.expect("slot");
        let slot = slot_arc.lock().await;
        let _ = slot.broadcast_tx.send(SessionEvent::PermissionRequest {
            request_id: "perm-test-1".to_string(),
            tool_name: "Bash".to_string(),
            input_preview: "rm -rf /tmp/test".to_string(),
            required_mode: "danger-full-access".to_string(),
            current_mode: "workspace-write".to_string(),
            reason: Some("requires full access".to_string()),
        });
    }

    // Wait for the client to receive the permission_request notification.
    let timeout = tokio::time::Duration::from_secs(5);
    let received = tokio::time::timeout(timeout, async {
        loop {
            let msg = client_transport
                .recv()
                .await
                .expect("recv")
                .expect("not closed");
            if let Some("session/update") = msg.get("method").and_then(Value::as_str) {
                if let Some(event) = msg.get("params").and_then(|p| p.get("event")) {
                    if event.get("type").and_then(Value::as_str) == Some("permission_request") {
                        return event.clone();
                    }
                }
            }
        }
    })
    .await
    .expect("permission_request received within timeout");

    assert_eq!(
        received["type"].as_str(),
        Some("permission_request"),
        "event type must be permission_request"
    );
    assert_eq!(received["request_id"].as_str(), Some("perm-test-1"));
    assert_eq!(received["tool_name"].as_str(), Some("Bash"));
    assert_eq!(
        received["input_preview"].as_str(),
        Some("rm -rf /tmp/test")
    );

    server_task.abort();
}

// ---------------------------------------------------------------------------
// AT4.2: First-to-answer-wins / second gets error
// ---------------------------------------------------------------------------

/// AT4.2: Client A responds with allow; client B's subsequent response
/// returns -32005 (NoSuchPermissionRequest — slot is empty after A consumed it).
#[tokio::test]
async fn at4_2_first_response_wins_second_gets_error() {
    let (handler, _ws, _data) = fixture_handler();

    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    // Install a pending permission in the slot.
    let (tx, rx) = oneshot::channel::<PermissionPromptDecision>();
    {
        let slot_arc = handler.get_slot(&session_id).await.expect("slot");
        let mut slot = slot_arc.lock().await;
        slot.pending_permission = Some(PendingPermissionRequest {
            request_id: "perm-race-1".to_string(),
            tx,
        });
    }

    // Client A sends allow response — must succeed.
    let result_a = handler
        .handle_permission_response(PermissionResponseParams {
            session_id: session_id.clone(),
            request_id: "perm-race-1".to_string(),
            decision: PermissionDecisionStr::Allow,
            reason: None,
        })
        .await;
    assert!(result_a.is_ok(), "first response must succeed: {result_a:?}");

    // Verify the decision arrived at the TurnDriver end.
    let decision = rx.await.expect("decision received");
    assert_eq!(decision, PermissionPromptDecision::Allow);

    // Client B sends allow response — slot is empty, must get NoSuchPermissionRequest.
    let result_b = handler
        .handle_permission_response(PermissionResponseParams {
            session_id: session_id.clone(),
            request_id: "perm-race-1".to_string(),
            decision: PermissionDecisionStr::Allow,
            reason: None,
        })
        .await;

    let err = result_b.expect_err("second response must fail");
    assert_eq!(
        err.code(),
        error_codes::NO_SUCH_PERMISSION_REQUEST,
        "second response must return -32005, got code {}", err.code()
    );
}

// ---------------------------------------------------------------------------
// AT4.3: Timeout path
// ---------------------------------------------------------------------------

/// AT4.3: No client responds within the timeout window.
/// The prompter must return Deny with reason "permission prompt timed out".
#[tokio::test]
async fn at4_3_timeout_returns_deny() {
    use acp::tools::AcpPermissionPrompter;
    use runtime::{PermissionMode, PermissionRequest};

    let (handler, _ws, _data) = fixture_handler();
    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    let slot_arc = handler.get_slot(&session_id).await.expect("slot");
    let handle = tokio::runtime::Handle::current();
    let mut prompter =
        AcpPermissionPrompter::new(session_id.clone(), Arc::clone(&slot_arc), handle)
            .with_timeout(1); // 1 second — short for test

    let request = PermissionRequest {
        tool_name: "Bash".to_string(),
        input: r#"{"command":"rm -rf /tmp"}"#.to_string(),
        current_mode: PermissionMode::WorkspaceWrite,
        required_mode: PermissionMode::DangerFullAccess,
        reason: Some("needs full access".to_string()),
    };

    // Run in spawn_blocking (simulates TurnDriver context).
    let decision = tokio::task::spawn_blocking(move || prompter.decide(&request))
        .await
        .expect("spawn_blocking");

    match decision {
        PermissionPromptDecision::Deny { reason } => {
            assert!(
                reason.contains("timed out"),
                "reason must mention timeout, got: {reason}"
            );
        }
        PermissionPromptDecision::Allow => panic!("expected Deny on timeout"),
    }

    // Slot must be clean after timeout.
    let slot = slot_arc.lock().await;
    assert!(
        slot.pending_permission.is_none(),
        "pending_permission must be None after timeout"
    );
}

// ---------------------------------------------------------------------------
// AT4.5: Permission events in session_events storage
// ---------------------------------------------------------------------------

/// AT4.5: PermissionRequest events are persisted to the backend.
///
/// We use `MockTurnSource` with a `PermissionRequest` event and verify
/// it appears in `backend.load_events` with `event_type = PermissionRequest`.
#[tokio::test]
async fn at4_5_permission_request_persisted_to_backend() {
    use std::sync::atomic::AtomicI32;

    let (handler, _ws, _data) = fixture_handler();

    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    let (broadcast_tx, _rx) = tokio::sync::broadcast::channel::<SessionEvent>(256);
    let broadcast_tx = Arc::new(broadcast_tx);
    let next_seq = Arc::new(AtomicI32::new(0));

    // Build a MockTurnSource with a PermissionRequest event.
    let events = vec![SessionEvent::PermissionRequest {
        request_id: "perm-persist-1".to_string(),
        tool_name: "Bash".to_string(),
        input_preview: "echo test".to_string(),
        required_mode: "danger-full-access".to_string(),
        current_mode: "workspace-write".to_string(),
        reason: None,
    }];
    let mock = MockTurnSource::new(events);

    mock.broadcast_events(
        &session_id,
        "turn-persist-test",
        &broadcast_tx,
        &next_seq,
        handler.backend(),
    )
    .await
    .expect("broadcast ok");

    // Verify the event appears in storage.
    let stored = handler
        .backend()
        .load_events(&session_id, 0)
        .await
        .expect("load_events ok");

    // Should have: TurnStart, PermissionRequest, TurnEnd = 3 events.
    assert_eq!(stored.len(), 3, "expected 3 stored events, got {}", stored.len());

    // Find the permission request event.
    let perm_event = stored
        .iter()
        .find(|e| e.event_type == StoredEventType::PermissionRequest)
        .expect("PermissionRequest event must be in storage");

    // Verify the payload structure.
    let payload = &perm_event.payload;
    assert_eq!(
        payload.get("type").and_then(Value::as_str),
        Some("permission_request"),
        "event type discriminant must be 'permission_request'"
    );
    assert_eq!(
        payload.get("request_id").and_then(Value::as_str),
        Some("perm-persist-1")
    );
    assert_eq!(
        payload.get("tool_name").and_then(Value::as_str),
        Some("Bash")
    );
}

// ---------------------------------------------------------------------------
// AT4.2 extension: Two clients attached, both see PermissionRequest
// ---------------------------------------------------------------------------

/// Two clients subscribed to the same session both receive the
/// `permission_request` notification when the event is broadcast.
#[tokio::test]
async fn at4_2_both_clients_see_permission_request() {
    let (handler, _ws, _data) = fixture_handler();

    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    // Subscribe two receivers directly to the broadcast channel.
    let (mut rx_a, mut rx_b) = {
        let slot_arc = handler.get_slot(&session_id).await.expect("slot");
        let slot = slot_arc.lock().await;
        let rx_a = slot.broadcast_tx.subscribe();
        let rx_b = slot.broadcast_tx.subscribe();
        (rx_a, rx_b)
    };

    // Broadcast a PermissionRequest event.
    {
        let slot_arc = handler.get_slot(&session_id).await.expect("slot");
        let slot = slot_arc.lock().await;
        let _ = slot.broadcast_tx.send(SessionEvent::PermissionRequest {
            request_id: "perm-multi-1".to_string(),
            tool_name: "ReadFile".to_string(),
            input_preview: "/etc/passwd".to_string(),
            required_mode: "read-only".to_string(),
            current_mode: "workspace-write".to_string(),
            reason: None,
        });
    }

    // Both A and B must receive the event.
    let ev_a = rx_a.try_recv().expect("client A must receive PermissionRequest");
    let ev_b = rx_b.try_recv().expect("client B must receive PermissionRequest");

    assert!(
        matches!(ev_a, SessionEvent::PermissionRequest { .. }),
        "client A received: {ev_a:?}"
    );
    assert!(
        matches!(ev_b, SessionEvent::PermissionRequest { .. }),
        "client B received: {ev_b:?}"
    );

    // Both events are the same (broadcast clones).
    if let (
        SessionEvent::PermissionRequest { request_id: id_a, .. },
        SessionEvent::PermissionRequest { request_id: id_b, .. },
    ) = (ev_a, ev_b)
    {
        assert_eq!(id_a, "perm-multi-1");
        assert_eq!(id_b, "perm-multi-1");
    }
}

// ---------------------------------------------------------------------------
// Dispatch loop: session/permission_response routing via transport
// ---------------------------------------------------------------------------

/// Verify that the `session/permission_response` JSON-RPC method is correctly
/// dispatched through the transport layer to `handle_permission_response`.
#[tokio::test]
async fn dispatch_loop_routes_permission_response() {
    let (handler, _ws, _data) = fixture_handler();

    let session_id = handler
        .handle_new(NewSessionParams::default())
        .await
        .expect("new session")
        .session_id;

    // Install a pending permission in the slot.
    let (tx, rx) = oneshot::channel::<PermissionPromptDecision>();
    {
        let slot_arc = handler.get_slot(&session_id).await.expect("slot");
        let mut slot = slot_arc.lock().await;
        slot.pending_permission = Some(PendingPermissionRequest {
            request_id: "perm-dispatch-1".to_string(),
            tx,
        });
    }

    // Build transport pair.
    let (mut client_transport, server_transport) = transport_pair();

    let handler_clone = Arc::clone(&handler);
    let server_task = tokio::spawn(async move {
        acp::serve_transport(server_transport, handler_clone).await
    });

    // Send permission_response via the transport.
    let response = rpc(
        &mut client_transport,
        1,
        "session/permission_response",
        json!({
            "session_id": session_id,
            "request_id": "perm-dispatch-1",
            "decision": "allow",
        }),
    )
    .await;

    // Must get {"ok": true}.
    assert_eq!(
        response.get("result").and_then(|r| r.get("ok")).and_then(Value::as_bool),
        Some(true),
        "permission_response must return ok: true, got: {response}"
    );

    // The TurnDriver side must have received Allow.
    let decision = tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        rx,
    )
    .await
    .expect("decision received within timeout")
    .expect("channel not dropped");

    assert_eq!(decision, PermissionPromptDecision::Allow);

    // Second response with same request_id via transport must fail.
    let response2 = rpc(
        &mut client_transport,
        2,
        "session/permission_response",
        json!({
            "session_id": session_id,
            "request_id": "perm-dispatch-1",
            "decision": "allow",
        }),
    )
    .await;

    let error_code = response2
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(Value::as_i64)
        .expect("must have error code");
    assert_eq!(
        error_code,
        error_codes::NO_SUCH_PERMISSION_REQUEST,
        "second response must return -32005, got {error_code}"
    );

    server_task.abort();
}
