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

pub mod session;
pub mod stream;
pub mod tools;
pub mod transport;

pub use session::{
    AcpError as SessionError, CloseSessionParams, InitializeParams, InitializeResult,
    ListSessionsResult, NewSessionParams, NewSessionResult, ResumeSessionParams,
    ResumeSessionResult, ServerCapabilities, ServerInfo, SessionHandler, SessionSummary,
    ACP_PROTOCOL_VERSION, ACP_SERVER_NAME, ACP_SERVER_VERSION,
};
pub use transport::{StdioTransport, Transport, TransportError, WebSocketTransport};

use std::path::PathBuf;
use std::sync::Arc;

use runtime::SessionStore;
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
}

/// Launch the ACP server with the given options.
///
/// Blocks the current task until the transport closes or an unrecoverable
/// error occurs. M2 wires a [`SessionHandler`] into the dispatch loop so
/// `initialize` and `session/*` calls are answered with real semantics;
/// tool execution (M3) and permissions (M4) still return a structured
/// JSON-RPC `-32601` error.
///
/// This is the single integration point used by `rusty-claude-cli` so the
/// CLI does not need to depend on internal module layout.
pub async fn serve(options: ServeOptions) -> Result<(), AcpError> {
    match options {
        ServeOptions::Stdio {
            workspace_root,
            data_dir,
        } => {
            let store = build_session_store(workspace_root, data_dir)?;
            let handler = Arc::new(SessionHandler::new(store));
            tracing::info!("ACP server listening on stdio (Content-Length framing)");
            let transport = StdioTransport::new();
            run_dispatch_loop(transport, handler).await
        }
        ServeOptions::WebSocket {
            addr,
            workspace_root,
            data_dir,
        } => {
            let store = build_session_store(workspace_root, data_dir)?;
            // Share one handler across all websocket sessions so `session/list`
            // reflects every connected client in this process.
            let handler = Arc::new(SessionHandler::new(store));
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

/// Dispatch loop used once M2 is wired. Routes `initialize` and
/// `session/*` methods to the handler; everything else still returns
/// JSON-RPC `-32601` until subsequent milestones land.
async fn run_dispatch_loop<T: Transport>(
    mut transport: T,
    handler: Arc<SessionHandler>,
) -> Result<(), AcpError> {
    loop {
        let msg = match transport.recv().await {
            Ok(Some(msg)) => msg,
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
        };

        tracing::debug!(?msg, "received ACP message");

        // JSON-RPC 2.0: only requests (those carrying an `id`) demand a
        // reply. Notifications are logged and dropped.
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

        let response = dispatch(handler.as_ref(), &id, &method, params).await;

        if let Err(err) = transport.send(response).await {
            tracing::warn!(error = %err, "failed to send ACP response");
            return Err(err.into());
        }
    }
}

/// Translate one JSON-RPC request into a JSON-RPC response value.
///
/// Extracted so future milestones can unit-test dispatch without
/// standing up a transport pair.
async fn dispatch(
    handler: &SessionHandler,
    id: &Value,
    method: &str,
    params: Value,
) -> Value {
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
                Ok(result) => {
                    ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null))
                }
                Err(err) => session_error_response(id, &err),
            },
            Err(resp) => resp,
        },
        "session/resume" => match parse_params::<ResumeSessionParams>(params, id) {
            Ok(p) => match handler.handle_resume(p).await {
                Ok(result) => {
                    ok_response(id, serde_json::to_value(result).unwrap_or(Value::Null))
                }
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
        other => method_not_found(id, other),
    }
}

fn parse_params<P: serde::de::DeserializeOwned>(
    params: Value,
    id: &Value,
) -> Result<P, Value> {
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
