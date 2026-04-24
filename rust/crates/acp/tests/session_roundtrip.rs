//! Integration test for ACP M2 session lifecycle over stdio transport.
//!
//! Drives a full `initialize` → `session/new` → `session/list` →
//! `session/close` → `session/list` round-trip using `tokio::io::duplex`
//! so no real stdio is touched. Runs against the public
//! [`acp::serve_transport`] entrypoint so it exercises the same dispatch
//! path the production `claw acp serve` uses.

use std::sync::Arc;

use acp::{SessionHandler, StdioTransport, Transport};
use runtime::SessionStore;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::duplex;

fn fixture_handler() -> (Arc<SessionHandler>, TempDir, TempDir) {
    let workspace = TempDir::new().expect("workspace tempdir");
    let data = TempDir::new().expect("data tempdir");
    let store = SessionStore::from_data_dir(data.path(), workspace.path()).expect("store");
    (Arc::new(SessionHandler::new(store)), workspace, data)
}

/// Build an `StdioTransport` pair that speak to each other over
/// `tokio::io::duplex` — no real stdin/stdout involved.
fn transport_pair() -> (StdioTransport, StdioTransport) {
    let (a, b) = duplex(16 * 1024);
    let (a_r, a_w) = tokio::io::split(a);
    let (b_r, b_w) = tokio::io::split(b);
    // Each transport operates on one end of the duplex. Client owns `a`
    // (reads from a_r, writes through a_w); server owns `b`. Writing
    // through `a_w` is read by `b_r` (what the server sees), and vice
    // versa — exactly the client/server topology the tests need.
    let client = StdioTransport::from_io(a_r, a_w);
    let server = StdioTransport::from_io(b_r, b_w);
    (client, server)
}

#[tokio::test]
async fn test_session_roundtrip_initialize_new_list_close_list() {
    let (handler, _ws, _data) = fixture_handler();
    let (mut client, server) = transport_pair();

    let server_task = tokio::spawn(acp::serve_transport(server, Arc::clone(&handler)));

    // 1. initialize
    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocol_version": "0.1", "client_info": {"name": "it-test"}},
        }))
        .await
        .expect("send initialize");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(1));
    assert_eq!(resp["result"]["server_info"]["name"], json!("claw-code"));
    assert_eq!(resp["result"]["capabilities"]["sessions"], json!(true));
    // M3: streaming is now true (Phase 2 landed).
    assert_eq!(resp["result"]["capabilities"]["streaming"], json!(true));
    assert_eq!(resp["result"]["capabilities"]["tools"], json!(true));

    // 2. session/new
    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {"model": "sonnet-4"},
        }))
        .await
        .expect("send new");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(2));
    let session_id = resp["result"]["session_id"]
        .as_str()
        .expect("session_id string")
        .to_string();
    assert!(!session_id.is_empty(), "session id must be non-empty");

    // 3. session/list — should contain the new session
    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/list",
        }))
        .await
        .expect("send list");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(3));
    let sessions = resp["result"]["sessions"].as_array().expect("array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], json!(session_id));
    assert_eq!(sessions[0]["model"], json!("sonnet-4"));

    // 4. session/close
    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/close",
            "params": {"session_id": session_id.clone()},
        }))
        .await
        .expect("send close");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(4));
    assert_eq!(resp["result"], json!({"ok": true}));

    // 5. session/list — now empty
    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "session/list",
        }))
        .await
        .expect("send list 2");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(5));
    let sessions = resp["result"]["sessions"].as_array().expect("array");
    assert!(sessions.is_empty(), "sessions must be empty after close");

    // Close the client so the server sees EOF and exits cleanly.
    client.close().await.ok();
    drop(client);
    // Give the server task at most a second to notice EOF.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), server_task).await;
}

#[tokio::test]
async fn test_unknown_method_returns_method_not_found() {
    let (handler, _ws, _data) = fixture_handler();
    let (mut client, server) = transport_pair();
    let server_task = tokio::spawn(acp::serve_transport(server, handler));

    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {},
        }))
        .await
        .expect("send");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(42));
    assert_eq!(resp["error"]["code"], json!(-32601));

    client.close().await.ok();
    drop(client);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), server_task).await;
}

#[tokio::test]
async fn test_unknown_session_close_returns_error() {
    let (handler, _ws, _data) = fixture_handler();
    let (mut client, server) = transport_pair();
    let server_task = tokio::spawn(acp::serve_transport(server, handler));

    client
        .send(json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "session/close",
            "params": {"session_id": "no-such-session"},
        }))
        .await
        .expect("send");
    let resp = client.recv().await.expect("recv").expect("some");
    assert_eq!(resp["id"], json!(7));
    // -32001 is the ACP-specific UNKNOWN_SESSION code (see
    // session::error_codes::UNKNOWN_SESSION).
    assert_eq!(resp["error"]["code"], json!(-32001));

    client.close().await.ok();
    drop(client);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), server_task).await;
}
