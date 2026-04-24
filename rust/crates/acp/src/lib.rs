//! Agent Client Protocol (ACP) server for claw-code.
//!
//! This crate hosts the ACP daemon that `claw acp serve` launches,
//! bridging ACP clients (e.g. Zed, `slopus/happy`) with the existing
//! claw-code runtime (`Session`, `ConversationRuntime`, tool executor,
//! `PermissionEnforcer`).
//!
//! Milestone status:
//! - **M1** — transport (stdio + websocket framing): done.
//! - **M2** — session lifecycle (`initialize`, `session/new`, `session/resume`,
//!   `session/close`, `session/list`): done (this module).
//! - **M3** — tool call streaming: pending.
//! - **M4** — permission prompts: pending.
//! - **M5** — `slopus/happy` interop: pending.
//!
//! Tracked upstream as ROADMAP #76. Spec:
//! <https://github.com/zed-industries/agent-client-protocol>.

pub mod backend_postgres;
pub mod migrate;
pub mod session;
pub mod stream;
pub mod tools;
pub mod transport;
pub mod turn_driver;

pub use session::{
    AcpError as SessionError, CloseSessionParams, InitializeParams, InitializeResult,
    ListSessionsResult, NewSessionParams, NewSessionResult, PendingPermissionRequest,
    PermissionDecisionStr, PermissionResponseParams, PromptParams, PromptResult,
    ResumeSessionParams, ResumeSessionResult, ServerCapabilities, ServerInfo, SessionHandler,
    SessionSummary, ACP_PROTOCOL_VERSION, ACP_SERVER_NAME, ACP_SERVER_VERSION,
};
pub use stream::SessionEvent;
pub use transport::{StdioTransport, Transport, TransportError, WebSocketTransport};

use std::path::PathBuf;
use std::sync::Arc;

use runtime::{FileSessionBackend, SessionBackend, SessionStore};
use serde_json::{json, Value};

/// Options passed from `claw acp serve` into the server entrypoint.
///
/// Starts minimal on purpose. Transport selection lives here so the CLI
/// can keep parsing flags without depending on module internals.
#[derive(Debug, Clone)]
pub enum ServeOptions {
    /// Speak ACP over stdio — the default for editor/agent spawn.
    Stdio {
        /// Workspace root the session store partitions against. Defaults
        /// to the server's current working directory when `None`.
        workspace_root: Option<PathBuf>,
        /// Explicit data directory for the session store. When `None`,
        /// the store is derived from `workspace_root` via
        /// [`SessionStore::from_cwd`]-style layout
        /// (`<workspace>/.claw/sessions/<fingerprint>/`).
        data_dir: Option<PathBuf>,
    },
    /// Speak ACP over a WebSocket bound to `addr`. Format: `host:port`.
    WebSocket {
        addr: String,
        workspace_root: Option<PathBuf>,
        data_dir: Option<PathBuf>,
    },
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self::Stdio {
            workspace_root: None,
            data_dir: None,
        }
    }
}

/// Error type surfaced by the ACP server.
#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    /// The requested ACP feature is not yet implemented.
    #[error("ACP feature not implemented: {0}")]
    NotImplemented(&'static str),
    /// Transport-level failure (framing, I/O, protocol).
    #[error("ACP transport error: {0}")]
    Transport(#[from] TransportError),
    /// I/O failure outside the transport layer (listener accept, bind, ...).
    #[error("ACP I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Session store failed to initialize (bad workspace root, perms, ...).
    #[error("ACP session store error: {0}")]
    SessionStore(String),
    /// Backend configuration error (missing DATABASE_URL, connection refused, ...).
    #[error("ACP backend error: {0}")]
    Backend(String),
}

/// Launch the ACP server with the given options.
///
/// Blocks the current task until the transport closes or an unrecoverable
/// error occurs. M2 wires a [`SessionHandler`] into the dispatch loop so
/// `initialize` and `session/*` calls are answered with real semantics;
/// tool execution (M3) and permissions (M4) still return a structured
/// JSON-RPC `-32601` error.
///
/// Backend is selected via `CLAW_SESSION_BACKEND`:
/// - `file` (default) — existing JSONL behavior, no Postgres dependency.
/// - `postgres` — requires `CLAW_PG_URL` or `DATABASE_URL` to be set.
///
/// This is the single integration point used by `rusty-claude-cli` so the
/// CLI does not need to depend on internal module layout.
pub async fn serve(options: ServeOptions) -> Result<(), AcpError> {
    match options {
        ServeOptions::Stdio {
            workspace_root,
            data_dir,
        } => {
            let (store, backend) = build_session_backend(workspace_root, data_dir).await?;
            let handler = Arc::new(SessionHandler::new_with_backend(store, backend));
            tracing::info!("ACP server listening on stdio (Content-Length framing)");
            let transport = StdioTransport::new();
            run_dispatch_loop(transport, handler).await
        }
        ServeOptions::WebSocket {
            addr,
            workspace_root,
            data_dir,
        } => {
            let (store, backend) = build_session_backend(workspace_root, data_dir).await?;
            // Share one handler across all websocket sessions so `session/list`
            // reflects every connected client in this process.
            let handler = Arc::new(SessionHandler::new_with_backend(store, backend));
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let bound = listener.local_addr()?;
            tracing::info!(%bound, "ACP server listening on websocket");
            loop {
                let (stream, peer) = listener.accept().await?;
                tracing::info!(%peer, "ACP websocket client connected");
                let ws = match tokio_tungstenite::accept_async(
                    tokio_tungstenite::MaybeTlsStream::Plain(stream),
                )
                .await
                {
                    Ok(ws) => ws,
                    Err(err) => {
                        tracing::warn!(error = %err, "websocket handshake failed");
                        continue;
                    }
                };
                let transport = WebSocketTransport::from_stream(ws);
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    if let Err(err) = run_dispatch_loop(transport, handler).await {
                        tracing::warn!(error = %err, "session loop ended with error");
                    }
                });
            }
        }
    }
}

/// Resolve the session store and backend based on `CLAW_SESSION_BACKEND`.
///
/// Returns `(SessionStore, Arc<dyn SessionBackend>)`.
/// The `SessionStore` is still needed for the file backend's JSONL persistence.
async fn build_session_backend(
    workspace_root: Option<PathBuf>,
    data_dir: Option<PathBuf>,
) -> Result<(SessionStore, Arc<dyn SessionBackend>), AcpError> {
    let store = build_session_store(workspace_root, data_dir)?;
    let backend_env = std::env::var("CLAW_SESSION_BACKEND").unwrap_or_else(|_| "file".to_string());

    match backend_env.to_lowercase().as_str() {
        "postgres" => {
            let url = std::env::var("CLAW_PG_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .map_err(|_| {
                    AcpError::Backend(
                        "CLAW_SESSION_BACKEND=postgres requires CLAW_PG_URL or DATABASE_URL"
                            .to_string(),
                    )
                })?;
            let pg = crate::backend_postgres::PostgresSessionBackend::connect(&url)
                .await
                .map_err(|e| AcpError::Backend(e.to_string()))?;
            pg.run_migrations()
                .await
                .map_err(|e| AcpError::Backend(e.to_string()))?;

            // T1.9: spawn the stale-client reaper task.
            // Runs immediately on startup and then every 5 minutes.
            // Heartbeat window: 5 minutes = 300_000 ms.
            let reaper_pool = pg.pool().clone();
            tokio::spawn(async move {
                const HEARTBEAT_TTL_MS: i64 = 300_000;
                const REAP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
                loop {
                    if let Err(err) =
                        crate::backend_postgres::reap_stale_clients(&reaper_pool, HEARTBEAT_TTL_MS)
                            .await
                    {
                        tracing::warn!(error = %err, "reaper task failed");
                    }
                    tokio::time::sleep(REAP_INTERVAL).await;
                }
            });

            tracing::info!("ACP using Postgres session backend");
            Ok((store, Arc::new(pg) as Arc<dyn SessionBackend>))
        }
        "file" | _ => {
            let backend = FileSessionBackend::new(store.clone());
            tracing::info!("ACP using file session backend");
            Ok((store, Arc::new(backend) as Arc<dyn SessionBackend>))
        }
    }
}

/// Resolve a `SessionStore` from the (optional) workspace root + data
/// directory. `workspace_root = None` defaults to the process cwd.
fn build_session_store(
    workspace_root: Option<PathBuf>,
    data_dir: Option<PathBuf>,
) -> Result<SessionStore, AcpError> {
    let workspace = match workspace_root {
        Some(p) => p,
        None => std::env::current_dir()
            .map_err(|err| AcpError::SessionStore(format!("cwd unavailable: {err}")))?,
    };
    let store = match data_dir {
        Some(data) => SessionStore::from_data_dir(data, &workspace)
            .map_err(|err| AcpError::SessionStore(err.to_string()))?,
        None => SessionStore::from_cwd(&workspace)
            .map_err(|err| AcpError::SessionStore(err.to_string()))?,
    };
    Ok(store)
}

/// Drive the M2 dispatch loop against an arbitrary transport.
///
/// Public so integration tests can drive the server over `duplex`
/// streams without touching the real stdio handles. Production callers
/// go through [`serve`].
pub async fn serve_transport<T: Transport>(
    transport: T,
    handler: Arc<SessionHandler>,
) -> Result<(), AcpError> {
    run_dispatch_loop(transport, handler).await
}

/// Dispatch loop — M3 version with broadcast relay.
///
/// Handles incoming JSON-RPC requests AND relays `session/update`
/// notifications from the broadcast channel concurrently using `tokio::select!`.
///
/// When a client does `session/resume` or `session/new`, the handler subscribes
/// to the session's broadcast channel. Subsequent `session/update` notifications
/// are forwarded from that receiver to the transport without blocking request
/// processing.
async fn run_dispatch_loop<T: Transport>(
    mut transport: T,
    handler: Arc<SessionHandler>,
) -> Result<(), AcpError> {
    // Active session_id for this client connection (set on session/resume or session/new).
    let mut active_session_id: Option<String> = None;
    // Broadcast receiver for the active session (set when session is attached).
    let mut broadcast_rx: Option<tokio::sync::broadcast::Receiver<SessionEvent>> = None;

    loop {
        // Decide what to select on: if we have a broadcast receiver, select on both.
        let event_opt = if let Some(rx) = broadcast_rx.as_mut() {
            tokio::select! {
                biased;
                // Prefer incoming requests (so prompt/close are handled promptly).
                msg = transport.recv() => {
                    match msg {
                        Ok(Some(m)) => Either::Inbound(m),
                        Ok(None) => {
                            tracing::info!("ACP peer closed transport");
                            transport.close().await.ok();
                            return Ok(());
                        }
                        Err(TransportError::Closed) => return Ok(()),
                        Err(err) => {
                            tracing::warn!(error = %err, "inbound transport error");
                            return Err(err.into());
                        }
                    }
                }
                // Relay broadcast events as session/update notifications.
                broadcast_result = rx.recv() => {
                    match broadcast_result {
                        Ok(ev) => Either::Broadcast(ev),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Client fell too far behind (F2.6). Send ClientLagged and close.
                            let lagged = SessionEvent::ClientLagged { dropped: n as usize };
                            let notif = build_update_notification(
                                active_session_id.as_deref().unwrap_or(""),
                                "",
                                0,
                                &lagged,
                            );
                            transport.send(notif).await.ok();
                            tracing::warn!(
                                session_id = ?active_session_id,
                                dropped = n,
                                "broadcast receiver lagged — disconnecting client"
                            );
                            transport.close().await.ok();
                            return Ok(());
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Sender dropped (session closed). Relay is done.
                            broadcast_rx = None;
                            continue;
                        }
                    }
                }
            }
        } else {
            // No broadcast receiver — only handle inbound.
            match transport.recv().await {
                Ok(Some(m)) => Either::Inbound(m),
                Ok(None) => {
                    tracing::info!("ACP peer closed transport");
                    transport.close().await.ok();
                    return Ok(());
                }
                Err(TransportError::Closed) => return Ok(()),
                Err(err) => {
                    tracing::warn!(error = %err, "inbound transport error");
                    return Err(err.into());
                }
            }
        };

        match event_opt {
            Either::Broadcast(ev) => {
                // Extract turn_id and seq from the event for the notification envelope.
                let turn_id = event_turn_id(&ev);
                // seq: we don't track per-event seq in the relay — use 0 as sentinel.
                // The client can use the seq from storage replay; live events are
                // delivered in order. A proper seq requires querying the backend which
                // would add latency. TODO: thread seq through broadcast message.
                let notif = build_update_notification(
                    active_session_id.as_deref().unwrap_or(""),
                    &turn_id,
                    0,
                    &ev,
                );
                if let Err(err) = transport.send(notif).await {
                    tracing::warn!(error = %err, "failed to relay session/update");
                    return Err(err.into());
                }
            }
            Either::Inbound(msg) => {
                tracing::debug!(?msg, "received ACP message");

                // JSON-RPC 2.0: only requests (those carrying an `id`) demand a reply.
                // Notifications are logged and dropped.
                let Some(id) = msg.get("id").cloned() else {
                    tracing::debug!("dropping ACP notification (no id)");
                    continue;
                };

                let method = msg
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>")
                    .to_string();
                let params = msg.get("params").cloned().unwrap_or(Value::Null);

                // Handle session/resume and session/new specially so we can subscribe to broadcast.
                let (response, new_session_id) = dispatch_with_subscription(
                    handler.as_ref(),
                    &id,
                    &method,
                    params,
                )
                .await;

                // Send the RPC response FIRST so the client's request-response cycle
                // completes before catch-up notifications arrive. This matches the
                // JSON-RPC 2.0 model: the response closes the request, and subsequent
                // session/update notifications are independent server pushes.
                if let Err(err) = transport.send(response).await {
                    tracing::warn!(error = %err, "failed to send ACP response");
                    return Err(err.into());
                }

                if let Some(sid) = new_session_id {
                    // Subscribe to the session's broadcast channel BEFORE any DB read
                    // (catch-up algorithm step 4 per DESIGN.md §6).
                    if let Some((rx, hwm)) = handler.subscribe_to_session(&sid).await {
                        // Perform catch-up replay: load all events from storage.
                        let stored_events = handler
                            .backend()
                            .load_events(&sid, 0)
                            .await
                            .unwrap_or_default();

                        tracing::debug!(
                            session_id = %sid,
                            event_count = stored_events.len(),
                            "replaying stored events to client"
                        );

                        // Send stored events to this client only (not broadcast).
                        for ev in &stored_events {
                            if let Ok(session_ev) =
                                serde_json::from_value::<SessionEvent>(ev.payload.clone())
                            {
                                let turn_id = event_turn_id(&session_ev);
                                let notif = build_update_notification(
                                    &sid,
                                    &turn_id,
                                    ev.seq,
                                    &session_ev,
                                );
                                if let Err(err) = transport.send(notif).await {
                                    tracing::warn!(error = %err, "failed to send replay event");
                                    return Err(err.into());
                                }
                            }
                        }

                        let _ = hwm; // hwm is captured implicitly: all stored events sent above
                        active_session_id = Some(sid);
                        broadcast_rx = Some(rx);
                    } else {
                        active_session_id = Some(sid);
                    }
                }
            }
        }
    }
}

/// Dispatch result that also carries the subscribed session id when a session
/// attach method (`session/resume`, `session/new`) is called.
async fn dispatch_with_subscription(
    handler: &SessionHandler,
    id: &Value,
    method: &str,
    params: Value,
) -> (Value, Option<String>) {
    match method {
        "session/resume" => match parse_params::<ResumeSessionParams>(params, id) {
            Ok(p) => {
                let session_id = p.session_id.clone();
                match handler.handle_resume(p).await {
                    Ok(result) => {
                        let resp =
                            ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null));
                        (resp, Some(session_id))
                    }
                    Err(err) => (session_error_response(id, &err), None),
                }
            }
            Err(resp) => (resp, None),
        },
        "session/new" => match parse_params::<NewSessionParams>(params, id) {
            Ok(p) => match handler.handle_new(p).await {
                Ok(result) => {
                    let session_id = result.session_id.clone();
                    let resp =
                        ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null));
                    (resp, Some(session_id))
                }
                Err(err) => (session_error_response(id, &err), None),
            },
            Err(resp) => (resp, None),
        },
        other => (dispatch(handler, id, other, params).await, None),
    }
}

/// Translate one JSON-RPC request into a JSON-RPC response value.
///
/// Extracted so future milestones can unit-test dispatch without
/// standing up a transport pair.
async fn dispatch(handler: &SessionHandler, id: &Value, method: &str, params: Value) -> Value {
    match method {
        "initialize" => {
            let params = parse_params::<InitializeParams>(params, id);
            match params {
                Ok(p) => {
                    let result = handler.handle_initialize(p).await;
                    ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null))
                }
                Err(resp) => resp,
            }
        }
        "session/new" => match parse_params::<NewSessionParams>(params, id) {
            Ok(p) => match handler.handle_new(p).await {
                Ok(result) => ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null)),
                Err(err) => session_error_response(id, &err),
            },
            Err(resp) => resp,
        },
        "session/resume" => match parse_params::<ResumeSessionParams>(params, id) {
            Ok(p) => match handler.handle_resume(p).await {
                Ok(result) => ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null)),
                Err(err) => session_error_response(id, &err),
            },
            Err(resp) => resp,
        },
        "session/close" => match parse_params::<CloseSessionParams>(params, id) {
            Ok(p) => match handler.handle_close(p).await {
                Ok(()) => ok_response(id, json!({"ok": true})),
                Err(err) => session_error_response(id, &err),
            },
            Err(resp) => resp,
        },
        "session/list" => {
            let result = handler.handle_list().await;
            ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null))
        }
        "session/prompt" => match parse_params::<PromptParams>(params, id) {
            Ok(p) => match handler.handle_prompt(p).await {
                Ok(result) => ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null)),
                Err(err) => session_error_response(id, &err),
            },
            Err(resp) => resp,
        },
        // Phase 4 — permission prompt response (SPEC F4.2, DESIGN.md §3).
        "session/permission_response" => {
            match parse_params::<PermissionResponseParams>(params, id) {
                Ok(p) => match handler.handle_permission_response(p).await {
                    Ok(()) => ok_response(id, json!({"ok": true})),
                    Err(err) => session_error_response(id, &err),
                },
                Err(resp) => resp,
            }
        }
        other => method_not_found(id, other),
    }
}

// ---------------------------------------------------------------------------
// Helper: Either discriminant for select! arms
// ---------------------------------------------------------------------------

enum Either {
    Inbound(Value),
    Broadcast(SessionEvent),
}

// ---------------------------------------------------------------------------
// Helper: extract turn_id from SessionEvent for notification envelope
// ---------------------------------------------------------------------------

fn event_turn_id(event: &SessionEvent) -> String {
    match event {
        SessionEvent::TextDelta { turn_id, .. }
        | SessionEvent::ThinkingDelta { turn_id, .. }
        | SessionEvent::ToolUseStart { turn_id, .. }
        | SessionEvent::ToolResult { turn_id, .. }
        | SessionEvent::Usage { turn_id, .. }
        | SessionEvent::Compaction { turn_id, .. }
        | SessionEvent::TurnStart { turn_id }
        | SessionEvent::TurnEnd { turn_id }
        | SessionEvent::TurnError { turn_id, .. } => turn_id.clone(),
        SessionEvent::PermissionRequest { .. } | SessionEvent::ClientLagged { .. } => {
            String::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: build a session/update notification
// ---------------------------------------------------------------------------

fn build_update_notification(
    session_id: &str,
    turn_id: &str,
    seq: i32,
    event: &SessionEvent,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "session_id": session_id,
            "turn_id": turn_id,
            "seq": seq,
            "event": serde_json::to_value(event).unwrap_or(Value::Null),
        }
    })
}

fn parse_params<P: serde::de::DeserializeOwned>(params: Value, id: &Value) -> Result<P, Value> {
    // `null` is treated as "no params", which is valid for any
    // `#[derive(Default)]` params type.
    if params.is_null() {
        return serde_json::from_value(json!({})).map_err(|err| invalid_params(id, err));
    }
    serde_json::from_value(params).map_err(|err| invalid_params(id, err))
}

fn ok_response(id: &Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn invalid_params(id: &Value, err: serde_json::Error) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": session::error_codes::INVALID_PARAMS,
            "message": format!("invalid params: {err}"),
        }
    })
}

fn session_error_response(id: &Value, err: &session::AcpError) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": err.code(),
            "message": err.to_string(),
        }
    })
}

fn method_not_found(id: &Value, method: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32601,
            "message": format!("method '{method}' not yet implemented — tool streaming is M3, permissions M4"),
        }
    })
}
