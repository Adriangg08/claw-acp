//! Agent Client Protocol (ACP) server for claw-code.
//!
//! This crate hosts the ACP daemon that `claw acp serve` launches,
//! bridging ACP clients (e.g. Zed, `slopus/happy`) with the existing
//! claw-code runtime (`Session`, `ConversationRuntime`, tool executor,
//! `PermissionEnforcer`).
//!
//! M1 (transport layer) is landed: stdio + websocket framing is real.
//! Session, streaming, tool and permission surfaces are still stubbed —
//! see module docs for which milestone fills each in.
//!
//! Tracked upstream as ROADMAP #76. Spec:
//! <https://github.com/zed-industries/agent-client-protocol>.

pub mod session;
pub mod stream;
pub mod tools;
pub mod transport;

pub use transport::{StdioTransport, Transport, TransportError, WebSocketTransport};

use serde_json::{json, Value};

/// Options passed from `claw acp serve` into the server entrypoint.
///
/// Starts minimal on purpose. Transport selection lives here so the CLI
/// can keep parsing flags without depending on module internals.
#[derive(Debug, Clone)]
pub enum ServeOptions {
    /// Speak ACP over stdio — the default for editor/agent spawn.
    Stdio,
    /// Speak ACP over a WebSocket bound to `addr`. Format: `host:port`.
    WebSocket { addr: String },
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self::Stdio
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
}

/// Launch the ACP server with the given options.
///
/// Blocks the current task until the transport closes or an unrecoverable
/// error occurs. The session/dispatch layer is stubbed in M1: every
/// inbound JSON-RPC request is answered with a structured "not yet
/// implemented" error so remote clients can observe the server is alive
/// even though higher-level semantics are pending.
///
/// This is the single integration point used by `rusty-claude-cli` so the
/// CLI does not need to depend on internal module layout.
pub async fn serve(options: ServeOptions) -> Result<(), AcpError> {
    match options {
        ServeOptions::Stdio => {
            tracing::info!("ACP server listening on stdio (Content-Length framing)");
            let transport = StdioTransport::new();
            run_stub_loop(transport).await
        }
        ServeOptions::WebSocket { addr } => {
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            let bound = listener.local_addr()?;
            tracing::info!(%bound, "ACP server listening on websocket");
            // Accept one client at a time for M1. M2+ will spawn per-session
            // tasks as the session layer lands.
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
                if let Err(err) = run_stub_loop(transport).await {
                    tracing::warn!(error = %err, "session loop ended with error");
                }
            }
        }
    }
}

/// Placeholder dispatch loop used until the session layer (M2) lands.
///
/// For every inbound JSON-RPC-ish request we reply with error `-32601`
/// ("method not found"). Notifications are logged and dropped.
async fn run_stub_loop<T: Transport>(mut transport: T) -> Result<(), AcpError> {
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

        // Only JSON-RPC requests (those carrying an `id`) demand a reply.
        let Some(id) = msg.get("id").cloned() else {
            tracing::debug!("dropping ACP notification (no id)");
            continue;
        };

        let method = msg
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");

        let response = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("method '{method}' not yet implemented — ACP M1 transport only"),
            }
        });

        if let Err(err) = transport.send(response).await {
            tracing::warn!(error = %err, "failed to send ACP error response");
            return Err(err.into());
        }
    }
}
