//! Integration tests for `PostgresSessionBackend` using `testcontainers`.
//!
//! These tests spin up a real ephemeral Postgres 16 container and validate the
//! full `SessionBackend` trait contract, including ordering and idempotency.
//!
//! Gate: `#[cfg(feature = "postgres-tests")]` so they do NOT run in CI unless
//! the feature is explicitly enabled. Run locally with:
//!
//! ```bash
//! cargo test -p acp --test postgres_backend --features postgres-tests
//! ```
//!
//! The `testcontainers` crate manages container lifecycle. Docker must be
//! running. The container is ephemeral — removed on test process exit.

#![cfg(feature = "postgres-tests")]

use acp::backend_postgres::PostgresSessionBackend;
use runtime::session_control::{StoredEvent, StoredEventType};
use runtime::{Session, SessionBackend};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

/// Helper: start an ephemeral Postgres 16 container and return a connected backend.
async fn start_backend() -> (ContainerAsync<Postgres>, PostgresSessionBackend) {
    let container = Postgres::default()
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("port");

    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let backend = PostgresSessionBackend::connect(&url)
        .await
        .expect("connect to ephemeral Postgres");

    backend
        .run_migrations()
        .await
        .expect("run migrations");

    (container, backend)
}

/// Helper: build a minimal session with a unique workspace root.
fn make_session(workspace_root: &str) -> Session {
    Session::new().with_workspace_root(std::path::PathBuf::from(workspace_root))
}

fn make_event(seq: i32, event_type: StoredEventType, role: Option<&str>, text: &str) -> StoredEvent {
    StoredEvent {
        seq,
        event_type,
        role: role.map(String::from),
        payload: serde_json::json!({"type": "message", "text": text}),
        created_at_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_and_load_session_round_trip() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-round-trip");
    backend.create_session(&session).await.expect("create");

    let loaded = backend
        .load_session(&session.session_id)
        .await
        .expect("load ok")
        .expect("session must exist");

    assert_eq!(loaded.session_id, session.session_id);
    assert_eq!(
        loaded.workspace_root(),
        Some(std::path::Path::new("/workspace/test-round-trip"))
    );
}

#[tokio::test]
async fn load_nonexistent_session_returns_none() {
    let (_container, backend) = start_backend().await;

    let result = backend
        .load_session("nonexistent-session-id-xyz")
        .await
        .expect("query ok");

    assert!(result.is_none());
}

#[tokio::test]
async fn append_and_load_events_in_seq_order() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-events");
    backend.create_session(&session).await.expect("create");

    // Append events in order.
    for seq in 1..=5_i32 {
        let event = make_event(seq, StoredEventType::Message, Some("user"), &format!("msg-{seq}"));
        backend
            .append_event(&session.session_id, seq, &event)
            .await
            .expect("append");
    }

    let events = backend
        .load_events(&session.session_id, 0)
        .await
        .expect("load events");

    assert_eq!(events.len(), 5, "must have 5 events");
    for (i, ev) in events.iter().enumerate() {
        assert_eq!(ev.seq, (i + 1) as i32, "events must be ordered by seq");
    }
}

#[tokio::test]
async fn load_events_since_seq_is_exclusive() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-since-seq");
    backend.create_session(&session).await.expect("create");

    for seq in 1..=3_i32 {
        let event = make_event(seq, StoredEventType::Message, Some("assistant"), &format!("chunk-{seq}"));
        backend
            .append_event(&session.session_id, seq, &event)
            .await
            .expect("append");
    }

    // since_seq=2 should return only seq=3
    let events = backend
        .load_events(&session.session_id, 2)
        .await
        .expect("load events since 2");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq, 3);
}

#[tokio::test]
async fn append_event_is_idempotent_on_conflict() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-idempotent");
    backend.create_session(&session).await.expect("create");

    let event = make_event(1, StoredEventType::Message, Some("user"), "hello");

    // First append
    backend
        .append_event(&session.session_id, 1, &event)
        .await
        .expect("first append");

    // Second append with same seq — must not error (ON CONFLICT DO NOTHING)
    backend
        .append_event(&session.session_id, 1, &event)
        .await
        .expect("second append must be idempotent");

    let events = backend
        .load_events(&session.session_id, 0)
        .await
        .expect("load");
    assert_eq!(events.len(), 1, "duplicate insert must be ignored");
}

#[tokio::test]
async fn close_session_marks_closed_at_ms() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-close");
    backend.create_session(&session).await.expect("create");

    // Before close: appears in open list.
    let before = backend
        .list_open_sessions("/workspace/test-close")
        .await
        .expect("list before");
    assert!(before.iter().any(|r| r.session_id == session.session_id));

    backend
        .close_session(&session.session_id)
        .await
        .expect("close");

    // After close: NOT in open list.
    let after = backend
        .list_open_sessions("/workspace/test-close")
        .await
        .expect("list after");
    assert!(!after.iter().any(|r| r.session_id == session.session_id));
}

#[tokio::test]
async fn list_open_sessions_returns_correct_workspace() {
    let (_container, backend) = start_backend().await;

    let session_a = make_session("/workspace/ws-a");
    let session_b = make_session("/workspace/ws-b");

    backend.create_session(&session_a).await.expect("create a");
    backend.create_session(&session_b).await.expect("create b");

    let list_a = backend
        .list_open_sessions("/workspace/ws-a")
        .await
        .expect("list a");
    let list_b = backend
        .list_open_sessions("/workspace/ws-b")
        .await
        .expect("list b");

    assert_eq!(list_a.len(), 1);
    assert_eq!(list_a[0].session_id, session_a.session_id);

    assert_eq!(list_b.len(), 1);
    assert_eq!(list_b[0].session_id, session_b.session_id);
}

#[tokio::test]
async fn upsert_and_remove_client_presence() {
    let (_container, backend) = start_backend().await;

    let session = make_session("/workspace/test-presence");
    backend.create_session(&session).await.expect("create");

    // Upsert presence.
    backend
        .upsert_client_presence("client-1", &session.session_id, "websocket")
        .await
        .expect("upsert");

    // Upsert again (idempotent).
    backend
        .upsert_client_presence("client-1", &session.session_id, "websocket")
        .await
        .expect("re-upsert");

    // Remove.
    backend
        .remove_client_presence("client-1", &session.session_id)
        .await
        .expect("remove");
}
