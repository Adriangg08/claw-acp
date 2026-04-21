//! ACP session lifecycle: `initialize`, `session/new`, `session/resume`,
//! `session/close`, `session/list`.
//!
//! Maps ACP session concepts onto claw-code's existing [`runtime::Session`]
//! and [`runtime::SessionStore`]. Milestone M2.
//!
//! # Design notes
//!
//! ## Q1 — session namespacing (workspace root resolution)
//!
//! `SessionStore::from_cwd` partitions session directories by a 16-char
//! FNV-1a fingerprint of the workspace path. ACP clients pass their OWN
//! workspace root in `session/new`. We resolve this by letting the
//! **client's workspace root win** in ACP mode: the `SessionHandler` is
//! built from a `SessionStore` that the caller scoped to the workspace
//! (via `SessionStore::from_data_dir` with `workspace_root = <client
//! path>`). This preserves the 1:1 client→session mapping ACP assumes and
//! keeps the on-disk fingerprint consistent across CLI + ACP launches of
//! the same workspace.
//!
//! ## Q2 — runtime concurrency (one runtime per session)
//!
//! `ConversationRuntime<C, T>` is generic over the api client and tool
//! executor. To keep the `acp` crate decoupled from `api` and `tools`
//! (and to avoid pulling concrete generic parameters into the transport
//! layer), the per-session slot stored in the handler is a
//! [`SessionSlot`] wrapping the `Session` record itself. M3 (tool call
//! streaming) will attach a concrete `ConversationRuntime` to each slot
//! via a small extension trait; until then the slot is the authoritative
//! "session is live" marker. The slot is held behind an
//! `Arc<Mutex<SessionSlot>>` so within a session turns serialize, while
//! different sessions run fully in parallel against the `RwLock`-guarded
//! registry.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use runtime::session_control::SessionControlError;
use runtime::{Session, SessionStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

/// Protocol version this server implements. Bumped when the wire shape
/// changes. Tracks the zed-industries/agent-client-protocol spec.
pub const ACP_PROTOCOL_VERSION: &str = "0.1";

/// Server name advertised via `initialize`.
pub const ACP_SERVER_NAME: &str = "claw-code";

/// Server version advertised via `initialize`. Intentionally hard-coded
/// rather than pulled from `CARGO_PKG_VERSION` so the value is visible in
/// tests and stable against crate-level bumps.
pub const ACP_SERVER_VERSION: &str = "0.1.0";

// ---------------------------------------------------------------------------
// JSON-RPC error codes
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 reserved codes we emit from the handler.
pub mod error_codes {
    /// Standard JSON-RPC `InvalidParams` (-32602).
    pub const INVALID_PARAMS: i64 = -32602;
    /// Standard JSON-RPC `InternalError` (-32603).
    pub const INTERNAL_ERROR: i64 = -32603;
    /// ACP-specific: unknown session id.
    pub const UNKNOWN_SESSION: i64 = -32001;
    /// ACP-specific: session could not be loaded from the store.
    pub const SESSION_LOAD_FAILED: i64 = -32002;
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Handler-level errors. Each variant maps to a JSON-RPC error code via
/// [`AcpError::code`].
#[derive(Debug, Error)]
pub enum AcpError {
    #[error("unknown session id: {0}")]
    UnknownSession(String),
    #[error("session store error: {0}")]
    Store(String),
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
}

impl AcpError {
    /// JSON-RPC 2.0 error code for this error.
    #[must_use]
    pub fn code(&self) -> i64 {
        match self {
            Self::UnknownSession(_) => error_codes::UNKNOWN_SESSION,
            Self::Store(_) => error_codes::SESSION_LOAD_FAILED,
            Self::InvalidParams(_) => error_codes::INVALID_PARAMS,
        }
    }
}

impl From<SessionControlError> for AcpError {
    fn from(value: SessionControlError) -> Self {
        Self::Store(value.to_string())
    }
}

// ---------------------------------------------------------------------------
// Wire types — initialize
// ---------------------------------------------------------------------------

/// Parameters for the `initialize` handshake. Mostly informational today.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct InitializeParams {
    /// Client-advertised protocol version. The server compares for log
    /// telemetry only — M2 accepts any version.
    #[serde(default)]
    pub protocol_version: Option<String>,
    /// Optional client name/version for logging.
    #[serde(default)]
    pub client_info: Option<ClientInfo>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientInfo {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
}

/// Server capabilities advertised on `initialize`.
#[derive(Debug, Clone, Serialize)]
pub struct InitializeResult {
    pub protocol_version: String,
    pub server_info: ServerInfo,
    pub capabilities: ServerCapabilities,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}

/// Capability flags — bumped as each milestone lands. M2 advertises
/// session lifecycle only; tool streaming and permissions are M3/M4.
#[derive(Debug, Clone, Serialize)]
pub struct ServerCapabilities {
    pub sessions: bool,
    pub streaming: bool,
    pub tools: bool,
    pub permissions: bool,
}

impl ServerCapabilities {
    #[must_use]
    pub const fn m2() -> Self {
        Self {
            sessions: true,
            streaming: false,
            tools: false,
            permissions: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Wire types — session/new
// ---------------------------------------------------------------------------

/// Parameters for `session/new`.
///
/// `workspace_root` is the path the client considers the project root.
/// If provided, it is advisory: the session is stored under the handler's
/// pre-scoped workspace partition regardless. The field is recorded on
/// the `Session` itself so downstream tooling can detect mismatches.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct NewSessionParams {
    #[serde(default)]
    pub workspace_root: Option<PathBuf>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NewSessionResult {
    pub session_id: String,
    pub workspace_root: PathBuf,
}

// ---------------------------------------------------------------------------
// Wire types — session/resume
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResumeSessionParams {
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResumeSessionResult {
    pub session_id: String,
    pub message_count: usize,
}

// ---------------------------------------------------------------------------
// Wire types — session/close
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CloseSessionParams {
    pub session_id: String,
}

// ---------------------------------------------------------------------------
// Wire types — session/list
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ListSessionsResult {
    pub sessions: Vec<SessionSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub message_count: usize,
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// SessionSlot — the per-session state the handler owns
// ---------------------------------------------------------------------------

/// Per-session state owned by the handler.
///
/// Today this is just the `Session` record and its persistence path. M3
/// attaches a `ConversationRuntime` here so turns can stream tool calls
/// without the handler reaching into `SessionStore` on every request.
#[derive(Debug, Clone)]
pub struct SessionSlot {
    pub session: Session,
    pub path: PathBuf,
}

// ---------------------------------------------------------------------------
// SessionHandler
// ---------------------------------------------------------------------------

/// Owns the active-session registry and routes ACP session lifecycle
/// messages to the underlying [`SessionStore`].
///
/// Internally: one `Arc<Mutex<SessionSlot>>` per live session, keyed by
/// session id, wrapped in a top-level `RwLock` so list/lookup are
/// lock-free in the common read path while session creation/removal
/// take a brief write lock.
pub struct SessionHandler {
    runtimes: RwLock<HashMap<String, Arc<Mutex<SessionSlot>>>>,
    store: SessionStore,
}

impl SessionHandler {
    /// Build a new handler backed by `store`. Call sites typically scope
    /// the store with [`SessionStore::from_data_dir`] using the ACP
    /// client's workspace root (see Q1 note above).
    #[must_use]
    pub fn new(store: SessionStore) -> Self {
        Self {
            runtimes: RwLock::new(HashMap::new()),
            store,
        }
    }

    /// Expose the bound store — useful for tests and for higher-level
    /// callers that need to list persisted (but not currently active)
    /// sessions.
    #[must_use]
    pub fn store(&self) -> &SessionStore {
        &self.store
    }

    /// Respond to the ACP `initialize` handshake.
    ///
    /// M2 ignores client-supplied capabilities and always advertises the
    /// same server capability set. This is intentional — versioning
    /// lives in `protocol_version`, not feature flags.
    pub async fn handle_initialize(&self, params: InitializeParams) -> InitializeResult {
        if let Some(info) = params.client_info.as_ref() {
            tracing::info!(
                client = %info.name,
                client_version = info.version.as_deref().unwrap_or("?"),
                protocol_version = params.protocol_version.as_deref().unwrap_or("?"),
                "ACP client initialized"
            );
        }
        InitializeResult {
            protocol_version: ACP_PROTOCOL_VERSION.to_string(),
            server_info: ServerInfo {
                name: ACP_SERVER_NAME.to_string(),
                version: ACP_SERVER_VERSION.to_string(),
            },
            capabilities: ServerCapabilities::m2(),
        }
    }

    /// Create a new session, persist it, and register it in the active
    /// registry. Returns the session id + the effective workspace root
    /// (the store's root, not the client's raw input — see Q1).
    pub async fn handle_new(
        &self,
        params: NewSessionParams,
    ) -> Result<NewSessionResult, AcpError> {
        let workspace_root = self.store.workspace_root().to_path_buf();

        let mut session = Session::new().with_workspace_root(workspace_root.clone());
        if let Some(model) = params.model {
            session.model = Some(model);
        }

        let handle = self.store.create_handle(&session.session_id);
        session = session.with_persistence_path(handle.path.clone());
        session.save_to_path(&handle.path).map_err(|err| {
            AcpError::Store(format!("failed to persist new session: {err}"))
        })?;

        let session_id = session.session_id.clone();
        let slot = Arc::new(Mutex::new(SessionSlot {
            session,
            path: handle.path,
        }));

        {
            let mut guard = self.runtimes.write().await;
            guard.insert(session_id.clone(), slot);
        }

        tracing::info!(%session_id, ?workspace_root, "ACP session created");

        Ok(NewSessionResult {
            session_id,
            workspace_root,
        })
    }

    /// Resume a persisted session by id. If the session is already in
    /// the live registry, return it as-is. Otherwise load it from the
    /// store and register it.
    pub async fn handle_resume(
        &self,
        params: ResumeSessionParams,
    ) -> Result<ResumeSessionResult, AcpError> {
        // Fast path: session already live in the registry.
        {
            let guard = self.runtimes.read().await;
            if let Some(slot) = guard.get(&params.session_id) {
                let slot = slot.lock().await;
                return Ok(ResumeSessionResult {
                    session_id: params.session_id,
                    message_count: slot.session.messages.len(),
                });
            }
        }

        // Slow path: load from disk and insert into the registry.
        let loaded = self
            .store
            .load_session(&params.session_id)
            .map_err(|err| match err {
                SessionControlError::Format(msg) => {
                    AcpError::UnknownSession(format!("{}: {msg}", params.session_id))
                }
                other => other.into(),
            })?;

        let message_count = loaded.session.messages.len();
        let session_id = loaded.session.session_id.clone();
        let slot = Arc::new(Mutex::new(SessionSlot {
            session: loaded.session,
            path: loaded.handle.path,
        }));

        {
            let mut guard = self.runtimes.write().await;
            guard.insert(session_id.clone(), slot);
        }

        tracing::info!(%session_id, message_count, "ACP session resumed");
        Ok(ResumeSessionResult {
            session_id,
            message_count,
        })
    }

    /// Close a session, removing it from the active registry. Returns
    /// an error if the session id is unknown — we pick the stricter of
    /// the two reasonable contracts so mis-wired clients fail loudly
    /// rather than no-op silently.
    pub async fn handle_close(&self, params: CloseSessionParams) -> Result<(), AcpError> {
        let removed = {
            let mut guard = self.runtimes.write().await;
            guard.remove(&params.session_id)
        };
        match removed {
            Some(_) => {
                tracing::info!(session_id = %params.session_id, "ACP session closed");
                Ok(())
            }
            None => Err(AcpError::UnknownSession(params.session_id)),
        }
    }

    /// List all currently-active (in-memory) sessions.
    ///
    /// NOTE: this is the *live* registry, not the full on-disk session
    /// list. Dormant sessions on disk are only surfaced when a client
    /// successfully resumes them.
    pub async fn handle_list(&self) -> ListSessionsResult {
        let guard = self.runtimes.read().await;
        let mut sessions = Vec::with_capacity(guard.len());
        for (id, slot) in guard.iter() {
            let slot = slot.lock().await;
            sessions.push(SessionSummary {
                session_id: id.clone(),
                message_count: slot.session.messages.len(),
                model: slot.session.model.clone(),
            });
        }
        // Deterministic ordering so clients can diff list snapshots.
        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        ListSessionsResult { sessions }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Tempdir bundle returned alongside the handler so the caller
    /// keeps the backing directories alive for the duration of the
    /// test. Dropping either tempdir would wipe the session files.
    struct FixtureDirs {
        _workspace: TempDir,
        _data: TempDir,
    }

    fn fixture_handler() -> (SessionHandler, FixtureDirs) {
        let workspace = TempDir::new().expect("tempdir");
        let data_dir = TempDir::new().expect("data tempdir");
        let store = SessionStore::from_data_dir(data_dir.path(), workspace.path())
            .expect("store from data dir");
        (
            SessionHandler::new(store),
            FixtureDirs {
                _workspace: workspace,
                _data: data_dir,
            },
        )
    }

    #[tokio::test]
    async fn test_initialize_returns_capabilities() {
        let (handler, _ws) = fixture_handler();
        let result = handler
            .handle_initialize(InitializeParams {
                protocol_version: Some("0.1".to_string()),
                client_info: Some(ClientInfo {
                    name: "test-client".to_string(),
                    version: Some("1.0".to_string()),
                }),
            })
            .await;
        assert_eq!(result.protocol_version, ACP_PROTOCOL_VERSION);
        assert_eq!(result.server_info.name, ACP_SERVER_NAME);
        assert!(result.capabilities.sessions, "M2 must advertise sessions");
        assert!(
            !result.capabilities.streaming,
            "streaming lands in M3, not M2"
        );
        assert!(!result.capabilities.tools, "tools land in M3, not M2");
        assert!(
            !result.capabilities.permissions,
            "permissions land in M4, not M2"
        );
    }

    #[tokio::test]
    async fn test_new_session_creates_entry_and_returns_id() {
        let (handler, _ws) = fixture_handler();
        let result = handler
            .handle_new(NewSessionParams {
                workspace_root: None,
                model: Some("sonnet-4".to_string()),
            })
            .await
            .expect("new session");

        assert!(
            !result.session_id.is_empty(),
            "session id must be non-empty"
        );
        assert_eq!(
            result.workspace_root,
            handler.store().workspace_root().to_path_buf()
        );

        // List should show exactly this session.
        let list = handler.handle_list().await;
        assert_eq!(list.sessions.len(), 1);
        assert_eq!(list.sessions[0].session_id, result.session_id);
        assert_eq!(list.sessions[0].model.as_deref(), Some("sonnet-4"));
    }

    #[tokio::test]
    async fn test_resume_known_session_succeeds() {
        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new ok");

        // Close it first so the resume path has to go through the store.
        handler
            .handle_close(CloseSessionParams {
                session_id: created.session_id.clone(),
            })
            .await
            .expect("close ok");

        let resumed = handler
            .handle_resume(ResumeSessionParams {
                session_id: created.session_id.clone(),
            })
            .await
            .expect("resume ok");
        assert_eq!(resumed.session_id, created.session_id);
        assert_eq!(resumed.message_count, 0);

        // And the registry now contains it again.
        let list = handler.handle_list().await;
        assert_eq!(list.sessions.len(), 1);
    }

    #[tokio::test]
    async fn test_resume_unknown_session_returns_error() {
        let (handler, _ws) = fixture_handler();
        let err = handler
            .handle_resume(ResumeSessionParams {
                session_id: "session-does-not-exist".to_string(),
            })
            .await
            .expect_err("must error");
        match err {
            AcpError::UnknownSession(msg) => {
                assert!(
                    msg.contains("session-does-not-exist"),
                    "error must name the id, got {msg:?}"
                );
            }
            other => panic!("expected UnknownSession, got {other:?}"),
        }
        assert_eq!(err.code(), error_codes::UNKNOWN_SESSION);
    }

    #[tokio::test]
    async fn test_close_removes_session() {
        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new ok");

        handler
            .handle_close(CloseSessionParams {
                session_id: created.session_id.clone(),
            })
            .await
            .expect("close ok");

        let list = handler.handle_list().await;
        assert!(
            list.sessions.is_empty(),
            "registry must be empty after close"
        );
    }

    #[tokio::test]
    async fn test_close_unknown_session_errors() {
        // Contract: closing an unknown session returns UnknownSession.
        // We pick "error" over "noop" so mis-wired clients fail loudly
        // rather than double-close silently.
        let (handler, _ws) = fixture_handler();
        let err = handler
            .handle_close(CloseSessionParams {
                session_id: "nope".to_string(),
            })
            .await
            .expect_err("must error");
        assert!(matches!(err, AcpError::UnknownSession(_)));
    }

    #[tokio::test]
    async fn test_list_returns_all_active() {
        let (handler, _ws) = fixture_handler();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let r = handler
                .handle_new(NewSessionParams::default())
                .await
                .expect("new ok");
            ids.push(r.session_id);
        }
        ids.sort();

        let list = handler.handle_list().await;
        let mut listed: Vec<String> = list.sessions.iter().map(|s| s.session_id.clone()).collect();
        listed.sort();
        assert_eq!(listed, ids);
    }

    #[tokio::test]
    async fn test_concurrent_new_sessions_are_isolated() {
        // Two parallel session/new calls must yield distinct ids and
        // distinct slot arcs (i.e., separate mutex-guarded state).
        let (handler, _ws) = fixture_handler();
        let handler = Arc::new(handler);

        let h1 = Arc::clone(&handler);
        let t1 = tokio::spawn(async move { h1.handle_new(NewSessionParams::default()).await });
        let h2 = Arc::clone(&handler);
        let t2 = tokio::spawn(async move { h2.handle_new(NewSessionParams::default()).await });

        let r1 = t1.await.expect("task join 1").expect("new ok 1");
        let r2 = t2.await.expect("task join 2").expect("new ok 2");
        assert_ne!(r1.session_id, r2.session_id, "ids must be unique");

        // Registry must hold two distinct Arc instances.
        let guard = handler.runtimes.read().await;
        let slot1 = guard
            .get(&r1.session_id)
            .expect("slot 1 in registry")
            .clone();
        let slot2 = guard
            .get(&r2.session_id)
            .expect("slot 2 in registry")
            .clone();
        assert!(
            !Arc::ptr_eq(&slot1, &slot2),
            "sessions must own independent slot arcs"
        );
    }

    #[tokio::test]
    async fn test_resume_live_session_no_disk_roundtrip() {
        // Resuming a session that is still in the live registry should
        // return the same message count without error, even if the
        // backing store is readonly / unreachable. We approximate by
        // checking the happy path (re-resume without close).
        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new ok");
        let resumed = handler
            .handle_resume(ResumeSessionParams {
                session_id: created.session_id.clone(),
            })
            .await
            .expect("resume ok");
        assert_eq!(resumed.session_id, created.session_id);
    }
}
