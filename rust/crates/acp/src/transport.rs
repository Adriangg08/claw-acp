//! ACP wire transport: stdio (default) and websocket (optional).
//!
//! The transport layer is intentionally message-agnostic. Its only job is
//! to frame JSON values in and out. Session and dispatch logic lives in
//! sibling modules (`session`, `stream`). Milestone M1.
//!
//! - [`StdioTransport`] uses LSP-style `Content-Length` framing over any
//!   `AsyncRead + AsyncWrite` pair. In production it wraps
//!   `tokio::io::stdin` / `tokio::io::stdout`; tests wire `tokio::io::duplex`
//!   so no real stdio is touched.
//! - [`WebSocketTransport`] wraps a `tokio-tungstenite` stream. Each text
//!   frame is one JSON message; binary frames are rejected as a protocol
//!   error so mis-wired clients fail loudly instead of hanging.

use std::io;
use std::pin::Pin;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Error surface for all ACP transports.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Underlying I/O failed (socket closed mid-frame, disk error, etc.).
    #[error("transport I/O error: {0}")]
    Io(#[from] io::Error),
    /// JSON encode/decode failed on a frame payload.
    #[error("transport serde error: {0}")]
    Serde(#[from] serde_json::Error),
    /// The transport is closed and cannot be used.
    #[error("transport is closed")]
    Closed,
    /// Peer violated the framing contract (bad header, unexpected frame
    /// type, invalid length, ...). Recoverable at the caller's discretion.
    #[error("transport protocol violation: {0}")]
    Protocol(String),
}

/// Common contract for ACP transports.
///
/// Implementations must be safe to move across tokio tasks (`Send + Sync`).
/// Cancellation semantics follow tokio's — dropping a future pending on
/// [`Transport::recv`] is safe but may lose framing state, so callers
/// typically drive one task per transport.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Await the next framed JSON message. `Ok(None)` means the peer
    /// closed the stream cleanly.
    async fn recv(&mut self) -> Result<Option<Value>, TransportError>;

    /// Serialize `msg` and emit it as a single frame.
    async fn send(&mut self, msg: Value) -> Result<(), TransportError>;

    /// Shut down the transport. Idempotent: calling twice returns `Ok`.
    async fn close(&mut self) -> Result<(), TransportError>;
}

// ---------------------------------------------------------------------------
// Stdio transport
// ---------------------------------------------------------------------------

/// LSP-style `Content-Length` framed transport over any async byte streams.
///
/// Wire format per message:
///
/// ```text
/// Content-Length: <N>\r\n
/// \r\n
/// <N bytes of UTF-8 JSON>
/// ```
///
/// Additional headers before the blank line are tolerated and ignored, so
/// the transport remains forward-compatible with clients that add e.g.
/// `Content-Type`.
pub struct StdioTransport {
    reader: Pin<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    writer: Pin<Box<dyn AsyncWrite + Send + Sync + Unpin>>,
    closed: bool,
}

impl StdioTransport {
    /// Build a stdio transport speaking to the process's real stdin/stdout.
    ///
    /// Use [`StdioTransport::from_io`] in tests to avoid touching the real
    /// terminal.
    #[must_use]
    pub fn new() -> Self {
        Self::from_io(tokio::io::stdin(), tokio::io::stdout())
    }

    /// Build a stdio transport over arbitrary async I/O handles.
    ///
    /// Primary use cases: tests (pair with `tokio::io::duplex`) and
    /// embedding the transport in harnesses that proxy stdio through
    /// another process.
    pub fn from_io<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        Self {
            reader: Box::pin(reader),
            writer: Box::pin(writer),
            closed: false,
        }
    }

    /// Read one `Content-Length: N\r\n\r\n` framed JSON value.
    async fn read_frame(&mut self) -> Result<Option<Value>, TransportError> {
        // Parse headers: read bytes one at a time until the CRLFCRLF
        // terminator. This is cheap — ACP headers are tiny (tens of bytes)
        // — and keeps us from over-reading into the next frame's body.
        let mut header_buf: Vec<u8> = Vec::with_capacity(64);
        loop {
            let mut byte = [0u8; 1];
            match self.reader.read_exact(&mut byte).await {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    if header_buf.is_empty() {
                        return Ok(None);
                    }
                    return Err(TransportError::Protocol(
                        "unexpected EOF while reading header".to_string(),
                    ));
                }
                Err(err) => return Err(TransportError::Io(err)),
            }
            header_buf.push(byte[0]);
            if header_buf.ends_with(b"\r\n\r\n") {
                break;
            }
            // Guard against a peer shipping gigabytes of garbage as a
            // "header". 8 KiB is far above any legitimate ACP header set.
            if header_buf.len() > 8 * 1024 {
                return Err(TransportError::Protocol(
                    "header section exceeded 8 KiB without terminator".to_string(),
                ));
            }
        }

        let header_str = std::str::from_utf8(&header_buf)
            .map_err(|err| TransportError::Protocol(format!("header is not utf-8: {err}")))?;

        let mut content_length: Option<usize> = None;
        for line in header_str.split("\r\n") {
            if line.is_empty() {
                continue;
            }
            let Some((name, value)) = line.split_once(':') else {
                return Err(TransportError::Protocol(format!(
                    "malformed header line: {line:?}"
                )));
            };
            if name.trim().eq_ignore_ascii_case("content-length") {
                let parsed: usize = value.trim().parse().map_err(|err| {
                    TransportError::Protocol(format!("invalid Content-Length {value:?}: {err}"))
                })?;
                content_length = Some(parsed);
            }
            // Unknown headers are ignored for forward compatibility.
        }

        let length = content_length
            .ok_or_else(|| TransportError::Protocol("missing Content-Length header".to_string()))?;

        let mut body = vec![0u8; length];
        self.reader
            .read_exact(&mut body)
            .await
            .map_err(|err| match err.kind() {
                io::ErrorKind::UnexpectedEof => {
                    TransportError::Protocol("unexpected EOF while reading body".to_string())
                }
                _ => TransportError::Io(err),
            })?;

        let value: Value = serde_json::from_slice(&body)?;
        Ok(Some(value))
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for StdioTransport {
    async fn recv(&mut self) -> Result<Option<Value>, TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        self.read_frame().await
    }

    async fn send(&mut self, msg: Value) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        let body = serde_json::to_vec(&msg)?;
        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(&body).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Best-effort shutdown of the writer. The reader will observe EOF
        // on next poll; we don't own stdin so we can't close it further.
        let _ = self.writer.shutdown().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WebSocket transport
// ---------------------------------------------------------------------------

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// WebSocket-backed ACP transport.
///
/// Each outbound message becomes a single text frame. Inbound binary
/// frames are rejected with a protocol error so that a misconfigured
/// client fails fast instead of silently dropping data.
pub struct WebSocketTransport {
    ws: WsStream,
    closed: bool,
}

impl WebSocketTransport {
    /// Connect to a WebSocket URL and wrap the stream.
    ///
    /// The URL must be `ws://` or `wss://`.
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        let (ws, _response) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(Self::map_ws_error)?;
        Ok(Self { ws, closed: false })
    }

    /// Wrap an already-established websocket stream. Primarily used by
    /// tests and embedding harnesses that accept server-side connections.
    #[must_use]
    pub fn from_stream(ws: WsStream) -> Self {
        Self { ws, closed: false }
    }

    fn map_ws_error(err: tokio_tungstenite::tungstenite::Error) -> TransportError {
        use tokio_tungstenite::tungstenite::Error as WsError;
        match err {
            WsError::Io(io_err) => TransportError::Io(io_err),
            WsError::ConnectionClosed | WsError::AlreadyClosed => TransportError::Closed,
            other => TransportError::Protocol(other.to_string()),
        }
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn recv(&mut self) -> Result<Option<Value>, TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        loop {
            match self.ws.next().await {
                None => return Ok(None),
                Some(Err(err)) => return Err(Self::map_ws_error(err)),
                Some(Ok(WsMessage::Text(text))) => {
                    let value: Value = serde_json::from_str(text.as_str())?;
                    return Ok(Some(value));
                }
                Some(Ok(WsMessage::Binary(_))) => {
                    return Err(TransportError::Protocol(
                        "binary frames are not accepted on ACP websocket transport".to_string(),
                    ));
                }
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => {
                    // tungstenite auto-replies to pings; loop for the next payload.
                    continue;
                }
                Some(Ok(WsMessage::Close(_))) => {
                    self.closed = true;
                    return Ok(None);
                }
                Some(Ok(WsMessage::Frame(_))) => {
                    // Raw frames aren't produced by the high-level API; treat
                    // defensively as a protocol error.
                    return Err(TransportError::Protocol(
                        "unexpected raw frame on ACP websocket transport".to_string(),
                    ));
                }
            }
        }
    }

    async fn send(&mut self, msg: Value) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        let text = serde_json::to_string(&msg)?;
        self.ws
            .send(WsMessage::Text(text))
            .await
            .map_err(Self::map_ws_error)
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Best-effort close: ignore errors from an already-dead peer.
        let _ = self.ws.close(None).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn test_stdio_framing_encode() {
        let (client, mut server) = duplex(1024);
        // Split client into read/write halves so we can drive StdioTransport
        // as a full-duplex peer.
        let (client_r, client_w) = tokio::io::split(client);
        let mut transport = StdioTransport::from_io(client_r, client_w);

        transport
            .send(json!({"hello": "world"}))
            .await
            .expect("send must succeed");
        transport.close().await.expect("close must succeed");

        // Read everything the transport emitted.
        let mut buf = Vec::new();
        server.read_to_end(&mut buf).await.expect("read to end");
        let text = String::from_utf8(buf).expect("valid utf-8");

        // Body JSON has no guaranteed key order beyond serde's preservation of
        // insertion order, so we re-parse to compare semantically.
        let (header, body) = text.split_once("\r\n\r\n").expect("has header/body split");
        assert!(header.starts_with("Content-Length: "));
        let declared_len: usize = header
            .trim_start_matches("Content-Length: ")
            .parse()
            .expect("numeric content length");
        assert_eq!(declared_len, body.len());
        let value: Value = serde_json::from_str(body).expect("body is valid json");
        assert_eq!(value, json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn test_stdio_framing_decode() {
        let (mut client, server) = duplex(1024);
        let (server_r, server_w) = tokio::io::split(server);
        let mut transport = StdioTransport::from_io(server_r, server_w);

        let payload = br#"{"method":"ping","id":1}"#;
        let header = format!("Content-Length: {}\r\n\r\n", payload.len());
        client.write_all(header.as_bytes()).await.unwrap();
        client.write_all(payload).await.unwrap();
        client.flush().await.unwrap();

        let msg = transport.recv().await.expect("recv ok").expect("some msg");
        assert_eq!(msg, json!({"method": "ping", "id": 1}));
    }

    #[tokio::test]
    async fn test_stdio_framing_multiple() {
        let (mut client, server) = duplex(1024);
        let (server_r, server_w) = tokio::io::split(server);
        let mut transport = StdioTransport::from_io(server_r, server_w);

        for (idx, body) in [br#"{"n":1}"# as &[u8], br#"{"n":2}"#].iter().enumerate() {
            let header = format!("Content-Length: {}\r\n\r\n", body.len());
            client.write_all(header.as_bytes()).await.unwrap();
            client.write_all(body).await.unwrap();
            client.flush().await.unwrap();

            let msg = transport.recv().await.unwrap().unwrap();
            assert_eq!(msg, json!({"n": idx + 1}));
        }
    }

    #[tokio::test]
    async fn test_stdio_framing_malformed_header() {
        let (mut client, server) = duplex(1024);
        let (server_r, server_w) = tokio::io::split(server);
        let mut transport = StdioTransport::from_io(server_r, server_w);

        // Garbage header block terminated correctly — should yield a
        // Protocol error, not a panic or an Io error.
        client
            .write_all(b"not-a-header-line\r\n\r\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let err = transport.recv().await.expect_err("must fail");
        match err {
            TransportError::Protocol(_) => {}
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_stdio_close_idempotent() {
        let (client, _server) = duplex(64);
        let (client_r, client_w) = tokio::io::split(client);
        let mut transport = StdioTransport::from_io(client_r, client_w);

        transport.close().await.expect("first close ok");
        transport.close().await.expect("second close ok");

        // After close, send/recv must surface Closed rather than panic.
        let err = transport
            .send(json!({}))
            .await
            .expect_err("send after close");
        assert!(matches!(err, TransportError::Closed));
    }

    #[tokio::test]
    async fn test_websocket_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server: accept one connection, echo the first JSON message back.
        // We wrap the raw TcpStream in MaybeTlsStream::Plain BEFORE the
        // websocket handshake so the resulting stream type matches what
        // WebSocketTransport::from_stream expects.
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = accept_async(MaybeTlsStream::Plain(stream)).await.unwrap();
            let mut server_transport = WebSocketTransport::from_stream(ws);
            let msg = server_transport.recv().await.unwrap().unwrap();
            server_transport.send(msg).await.unwrap();
            server_transport.close().await.unwrap();
        });

        let url = format!("ws://{addr}");
        let mut client = WebSocketTransport::connect(&url).await.unwrap();
        client.send(json!({"echo": 42})).await.unwrap();
        let reply = client.recv().await.unwrap().unwrap();
        assert_eq!(reply, json!({"echo": 42}));
        client.close().await.unwrap();

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_websocket_rejects_binary() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends a binary frame as soon as the client connects.
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(MaybeTlsStream::Plain(stream)).await.unwrap();
            ws.send(WsMessage::Binary(vec![1, 2, 3]))
                .await
                .unwrap();
            // Keep the connection open until the client closes it.
            let _ = ws.next().await;
        });

        let url = format!("ws://{addr}");
        let mut client = WebSocketTransport::connect(&url).await.unwrap();
        let err = client.recv().await.expect_err("binary must be rejected");
        match err {
            TransportError::Protocol(msg) => assert!(msg.contains("binary")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
        client.close().await.unwrap();

        let _ = server_task.await;
    }
}
