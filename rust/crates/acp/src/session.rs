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
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::Arc;

use runtime::session_control::SessionControlError;
use runtime::{FileSessionBackend, PermissionPromptDecision, Session, SessionBackend, SessionStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::stream::SessionEvent;
use crate::turn_driver::{StubTurnExecutorFactory, TurnExecutorFactory};

/// Protocol version this server implements. Bumped when the wire shape
/// changes. Tracks the zed-industries/agent-client-protocol spec.
/// Bumped to "0.2" when Phase 2 (streaming + fan-out) lands.
pub const ACP_PROTOCOL_VERSION: &str = "0.2";

/// Default broadcast channel capacity (number of events).
/// Configurable via `CLAW_BROADCAST_CAPACITY` env var.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;

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
    /// ACP-specific: a turn is already in progress for this session.
    pub const SESSION_BUSY: i64 = -32003;
    /// ACP-specific: a permission response was already received for this request.
    pub const REQUEST_ALREADY_RESOLVED: i64 = -32004;
    /// ACP-specific: the permission request_id is unknown or has expired/timed out.
    pub const NO_SUCH_PERMISSION_REQUEST: i64 = -32005;
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
    /// A turn is already in progress for this session (SPEC F2.10).
    #[error("session busy: a turn is already in progress for session {0}")]
    SessionBusy(String),
    /// A permission response was already received (SPEC F4.3).
    #[error("permission request {0} already resolved by another client")]
    RequestAlreadyResolved(String),
    /// No pending permission request with the given request_id (SPEC §3).
    #[error("no pending permission request with id '{0}'")]
    NoSuchPermissionRequest(String),
}

impl AcpError {
    /// JSON-RPC 2.0 error code for this error.
    #[must_use]
    pub fn code(&self) -> i64 {
        match self {
            Self::UnknownSession(_) => error_codes::UNKNOWN_SESSION,
            Self::Store(_) => error_codes::SESSION_LOAD_FAILED,
            Self::InvalidParams(_) => error_codes::INVALID_PARAMS,
            Self::SessionBusy(_) => error_codes::SESSION_BUSY,
            Self::RequestAlreadyResolved(_) => error_codes::REQUEST_ALREADY_RESOLVED,
            Self::NoSuchPermissionRequest(_) => error_codes::NO_SUCH_PERMISSION_REQUEST,
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

    /// M3 capabilities: sessions + streaming + tools.
    #[must_use]
    pub const fn m3() -> Self {
        Self {
            sessions: true,
            streaming: true,
            tools: true,
            permissions: false,
        }
    }

    /// M4 capabilities: sessions + streaming + tools + permissions.
    #[must_use]
    pub const fn m4() -> Self {
        Self {
            sessions: true,
            streaming: true,
            tools: true,
            permissions: true,
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
// Wire types — session/prompt (M3)
// ---------------------------------------------------------------------------

/// Parameters for `session/prompt` (SPEC F2.1, DESIGN.md §3).
///
/// Returns immediately with `PromptResult`; the actual turn runs async and
/// pushes `session/update` notifications to all subscribers.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PromptParams {
    pub session_id: String,
    /// User text for this turn.
    pub text: String,
    /// Optional client-supplied turn id. Generated server-side if omitted.
    #[serde(default)]
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromptResult {
    pub session_id: String,
    pub turn_id: String,
    pub status: &'static str,
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
// Wire types — session/permission_response (Phase 4, SPEC F4.2, DESIGN.md §3)
// ---------------------------------------------------------------------------

/// The client-supplied decision in a `session/permission_response` request.
///
/// Maps to [`runtime::PermissionPromptDecision`]:
/// - `"allow"` → `Allow`
/// - `"deny"` → `Deny { reason: … }`
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecisionStr {
    Allow,
    Deny,
}

/// Parameters for `session/permission_response` (DESIGN.md §3).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PermissionResponseParams {
    pub session_id: String,
    pub request_id: String,
    pub decision: PermissionDecisionStr,
    /// Optional human-readable reason (required when `decision = deny`).
    #[serde(default)]
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------
// PendingPermissionRequest — held inside SessionSlot behind a Mutex
// ---------------------------------------------------------------------------

/// A single pending permission prompt awaiting a client response.
///
/// The TurnDriver creates this when `AcpPermissionPrompter::decide` is called.
/// The first `session/permission_response` that arrives sends on `tx`; all
/// subsequent responses receive `-32004 RequestAlreadyResolved`.
///
/// Stored as `Option<PendingPermissionRequest>` in [`SessionSlot`] to enforce
/// the single-prompt-at-a-time invariant (SPEC NF4.2).
pub struct PendingPermissionRequest {
    pub request_id: String,
    pub tx: oneshot::Sender<PermissionPromptDecision>,
}

// ---------------------------------------------------------------------------
// SessionSlot — the per-session state the handler owns
// ---------------------------------------------------------------------------

/// Per-session state owned by the handler.
///
/// M3 additions (per DESIGN.md §5):
/// - `broadcast_tx`: fan-out channel for `SessionEvent`s (capacity 256).
/// - `turn_in_progress`: atomic flag enforcing single-writer invariant (F2.10).
/// - `next_seq`: monotonic per-session event sequence counter (Design §1, Flag A).
///
/// M4 addition (per DESIGN.md §8):
/// - `pending_permission`: the active permission prompt awaiting a client
///   response. `None` when no prompt is in flight. Protected by the slot's
///   `Mutex` so the dispatch loop can atomically take/replace it.
pub struct SessionSlot {
    pub session: Session,
    pub path: PathBuf,
    /// Broadcast sender for `session/update` notifications.
    /// Subscribers (one per attached client) receive a cloned receiver.
    pub broadcast_tx: Arc<broadcast::Sender<SessionEvent>>,
    /// `true` while a TurnDriver task is running for this session.
    /// Guards the single-writer invariant — `session/prompt` returns
    /// `SessionBusy` if this is `true`.
    pub turn_in_progress: Arc<AtomicBool>,
    /// Monotonic sequence counter. The TurnDriver increments this before
    /// each `backend.append_event` call. Uses `Ordering::SeqCst` to ensure
    /// the seq written to storage is consistent with the broadcast order.
    pub next_seq: Arc<AtomicI32>,
    /// Active permission prompt awaiting a client response (SPEC F4.3, NF4.2).
    /// Only one prompt may be in flight at a time per session; the TurnDriver
    /// queues subsequent prompts until the first is resolved or timed out.
    pub pending_permission: Option<PendingPermissionRequest>,
}

impl SessionSlot {
    /// Create a new slot, initialising M3/M4 state.
    pub fn new(session: Session, path: PathBuf, broadcast_capacity: usize) -> Self {
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            session,
            path,
            broadcast_tx: Arc::new(broadcast_tx),
            turn_in_progress: Arc::new(AtomicBool::new(false)),
            next_seq: Arc::new(AtomicI32::new(0)),
            pending_permission: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SessionHandler
// ---------------------------------------------------------------------------

/// Owns the active-session registry and routes ACP session lifecycle
/// messages to the underlying [`SessionStore`] and [`SessionBackend`].
///
/// Internally: one `Arc<Mutex<SessionSlot>>` per live session, keyed by
/// session id, wrapped in a top-level `RwLock` so list/lookup are
/// lock-free in the common read path while session creation/removal
/// take a brief write lock.
pub struct SessionHandler {
    runtimes: RwLock<HashMap<String, Arc<Mutex<SessionSlot>>>>,
    store: SessionStore,
    /// Pluggable storage backend (file or Postgres) selected via
    /// `CLAW_SESSION_BACKEND`. See DESIGN.md §2 and SPEC.md F1.4.
    backend: Arc<dyn SessionBackend>,
    /// Factory that builds and runs `ConversationRuntime` for each turn.
    ///
    /// Injected by `acp::serve()`. Defaults to `StubTurnExecutorFactory`
    /// (emits a canned "not wired" response) when not explicitly provided.
    /// The CLI passes `CliTurnExecutorFactory` to get real LLM execution.
    executor_factory: Arc<dyn TurnExecutorFactory>,
}

impl SessionHandler {
    /// Build a new handler backed by `store` using the file backend.
    ///
    /// Uses `StubTurnExecutorFactory` — suitable for tests and code-paths
    /// that only need session lifecycle (no LLM execution). Production paths
    /// that need real model execution call [`Self::new_with_backend`] and
    /// supply a factory via [`Self::with_executor_factory`].
    #[must_use]
    pub fn new(store: SessionStore) -> Self {
        let backend = Arc::new(FileSessionBackend::new(store.clone()));
        Self {
            runtimes: RwLock::new(HashMap::new()),
            store,
            backend,
            executor_factory: Arc::new(StubTurnExecutorFactory),
        }
    }

    /// Build a new handler with an explicit [`SessionBackend`].
    ///
    /// Called by `serve()` after `build_session_backend()` resolves the env var.
    /// Uses `StubTurnExecutorFactory` by default; call
    /// [`Self::with_executor_factory`] to wire in the real CLI factory.
    #[must_use]
    pub fn new_with_backend(store: SessionStore, backend: Arc<dyn SessionBackend>) -> Self {
        Self {
            runtimes: RwLock::new(HashMap::new()),
            store,
            backend,
            executor_factory: Arc::new(StubTurnExecutorFactory),
        }
    }

    /// Replace the turn executor factory.
    ///
    /// Called immediately after construction by `acp::serve()` when a real
    /// `CliTurnExecutorFactory` is available.
    #[must_use]
    pub fn with_executor_factory(mut self, factory: Arc<dyn TurnExecutorFactory>) -> Self {
        self.executor_factory = factory;
        self
    }

    /// Expose the bound store — useful for tests and for higher-level
    /// callers that need to list persisted (but not currently active)
    /// sessions.
    #[must_use]
    pub fn store(&self) -> &SessionStore {
        &self.store
    }

    /// Expose the backend — useful for tests and migration helpers.
    #[must_use]
    pub fn backend(&self) -> &Arc<dyn SessionBackend> {
        &self.backend
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
            capabilities: ServerCapabilities::m4(),
        }
    }

    /// Create a new session, persist it via the backend, and register it in the
    /// active registry. Returns the session id + the effective workspace root.
    pub async fn handle_new(&self, params: NewSessionParams) -> Result<NewSessionResult, AcpError> {
        let workspace_root = self.store.workspace_root().to_path_buf();

        let mut session = Session::new().with_workspace_root(workspace_root.clone());
        if let Some(model) = params.model {
            session.model = Some(model);
        }

        // Persist via backend (file backend writes JSONL; Postgres backend inserts row).
        self.backend
            .create_session(&session)
            .await
            .map_err(|err| AcpError::Store(format!("failed to persist new session: {err}")))?;

        // For the file backend: also keep a persistence path on the Session so
        // push_message works correctly (file backend uses Session::append_persisted_message).
        let handle = self.store.create_handle(&session.session_id);
        session = session.with_persistence_path(handle.path.clone());

        let session_id = session.session_id.clone();
        let capacity = broadcast_capacity_from_env();
        let slot = Arc::new(Mutex::new(SessionSlot::new(session, handle.path, capacity)));

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
        let capacity = broadcast_capacity_from_env();
        let slot = Arc::new(Mutex::new(SessionSlot::new(
            loaded.session,
            loaded.handle.path,
            capacity,
        )));

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

    /// Close a session, removing it from the active registry and marking it
    /// closed in the backend. Returns an error if the session id is unknown.
    pub async fn handle_close(&self, params: CloseSessionParams) -> Result<(), AcpError> {
        let removed = {
            let mut guard = self.runtimes.write().await;
            guard.remove(&params.session_id)
        };
        match removed {
            Some(_) => {
                // Mark session as closed in backend (best-effort; don't fail the
                // close operation if the backend write fails).
                if let Err(err) = self.backend.close_session(&params.session_id).await {
                    tracing::warn!(
                        session_id = %params.session_id,
                        error = %err,
                        "backend close_session failed (non-fatal)"
                    );
                }
                tracing::info!(session_id = %params.session_id, "ACP session closed");
                Ok(())
            }
            None => Err(AcpError::UnknownSession(params.session_id)),
        }
    }

    /// List all open sessions: both live (in-memory) and dormant (backend).
    ///
    /// Per SPEC.md F1.6: merges the in-memory registry with backend's
    /// `list_open_sessions`, deduplicating by `session_id`.
    pub async fn handle_list(&self) -> ListSessionsResult {
        let workspace_root = self.store.workspace_root().to_string_lossy().to_string();

        // Collect live (in-memory) sessions first.
        let guard = self.runtimes.read().await;
        let mut seen = std::collections::HashSet::new();
        let mut sessions = Vec::with_capacity(guard.len());

        for (id, slot) in guard.iter() {
            seen.insert(id.clone());
            let slot = slot.lock().await;
            sessions.push(SessionSummary {
                session_id: id.clone(),
                message_count: slot.session.messages.len(),
                model: slot.session.model.clone(),
            });
        }

        // Merge dormant sessions from the backend (dedup by session_id).
        match self.backend.list_open_sessions(&workspace_root).await {
            Ok(rows) => {
                for row in rows {
                    if seen.contains(&row.session_id) {
                        continue; // already represented by the live entry
                    }
                    sessions.push(SessionSummary {
                        session_id: row.session_id,
                        message_count: row.message_count.unwrap_or(0) as usize,
                        model: row.model,
                    });
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "backend list_open_sessions failed — serving live-only list");
            }
        }

        // Deterministic ordering so clients can diff list snapshots.
        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        ListSessionsResult { sessions }
    }

    /// Start a model turn for the session.
    ///
    /// Per SPEC F2.1 and F2.10:
    /// - Returns `SessionBusy` if a turn is already in progress.
    /// - Otherwise atomically sets `turn_in_progress = true`, spawns a
    ///   `TurnDriver` task, and returns immediately with `{status: "started"}`.
    ///
    /// The TurnDriver broadcasts `session/update` notifications to all
    /// subscribers via `broadcast_tx`. Callers must subscribe to the broadcast
    /// channel (via `subscribe_to_session`) to receive events.
    pub async fn handle_prompt(&self, params: PromptParams) -> Result<PromptResult, AcpError> {
        use crate::turn_driver::TurnDriver;
        use std::sync::atomic::Ordering;

        // Resolve the slot.
        let slot_arc = {
            let guard = self.runtimes.read().await;
            guard
                .get(&params.session_id)
                .cloned()
                .ok_or_else(|| AcpError::UnknownSession(params.session_id.clone()))?
        };

        // Enforce single-writer invariant (F2.10). Acquire the mutex briefly just
        // to read the session model, then do the CAS on the atomic flag.
        let (turn_in_progress, broadcast_tx, next_seq, session_model) = {
            let slot = slot_arc.lock().await;
            (
                Arc::clone(&slot.turn_in_progress),
                Arc::clone(&slot.broadcast_tx),
                Arc::clone(&slot.next_seq),
                slot.session.model.clone(),
            )
        };

        // CAS: false → true. If the slot was already true, return SessionBusy.
        if turn_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(AcpError::SessionBusy(params.session_id.clone()));
        }

        let turn_id = params
            .turn_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        tracing::info!(
            session_id = %params.session_id,
            turn_id = %turn_id,
            "starting model turn"
        );

        let driver = TurnDriver {
            session_id: params.session_id.clone(),
            turn_id: turn_id.clone(),
            text: params.text.clone(),
            session_model,
            broadcast_tx,
            next_seq,
            turn_in_progress,
            backend: Arc::clone(&self.backend),
            slot: slot_arc,
            executor_factory: Arc::clone(&self.executor_factory),
        };

        tokio::spawn(async move {
            driver.run().await;
        });

        Ok(PromptResult {
            session_id: params.session_id,
            turn_id,
            status: "started",
        })
    }

    /// Handle a `session/permission_response` from a client.
    ///
    /// Routes the client's allow/deny decision to the `AcpPermissionPrompter`
    /// waiting on the session's oneshot channel. First valid response wins;
    /// subsequent responses return `-32004 RequestAlreadyResolved`.
    ///
    /// After routing the decision, broadcasts a `PermissionResponse` event to all
    /// attached clients so they know the outcome (SPEC F4.7 via storage, DESIGN §8).
    pub async fn handle_permission_response(
        &self,
        params: PermissionResponseParams,
    ) -> Result<(), AcpError> {
        let slot_arc = {
            let guard = self.runtimes.read().await;
            guard
                .get(&params.session_id)
                .cloned()
                .ok_or_else(|| AcpError::UnknownSession(params.session_id.clone()))?
        };

        // Atomically take the pending permission out of the slot.
        // Holding the mutex here is intentional: the atomicity of take + send
        // prevents two concurrent responses from both "winning".
        let pending = {
            let mut slot = slot_arc.lock().await;
            slot.pending_permission.take()
        };

        match pending {
            None => {
                // No pending prompt at all (already timed out or never started).
                Err(AcpError::NoSuchPermissionRequest(params.request_id))
            }
            Some(pending) => {
                // Verify the request_id matches (guards against stale responses after
                // a session re-use with a new prompt).
                if pending.request_id != params.request_id {
                    // Put it back — we took it but it wasn't ours to consume.
                    let mut slot = slot_arc.lock().await;
                    slot.pending_permission = Some(pending);
                    return Err(AcpError::NoSuchPermissionRequest(params.request_id));
                }

                // Translate the wire decision into the runtime type.
                let decision = match params.decision {
                    PermissionDecisionStr::Allow => PermissionPromptDecision::Allow,
                    PermissionDecisionStr::Deny => PermissionPromptDecision::Deny {
                        reason: params
                            .reason
                            .unwrap_or_else(|| "denied by client".to_string()),
                    },
                };

                // Send on the oneshot. If the receiver is gone (TurnDriver timed out),
                // the send will fail — treat as NoSuchPermissionRequest since from the
                // client's perspective the prompt is gone.
                if pending.tx.send(decision).is_err() {
                    tracing::warn!(
                        session_id = %params.session_id,
                        request_id = %params.request_id,
                        "permission response arrived after timeout — TurnDriver already moved on"
                    );
                    return Err(AcpError::NoSuchPermissionRequest(params.request_id));
                }

                tracing::info!(
                    session_id = %params.session_id,
                    request_id = %params.request_id,
                    "permission response routed to TurnDriver"
                );
                Ok(())
            }
        }
    }

    /// Get a reference to the slot arc for a session (for tests and diagnostics).
    pub async fn get_slot(&self, session_id: &str) -> Option<Arc<Mutex<SessionSlot>>> {
        let guard = self.runtimes.read().await;
        guard.get(session_id).cloned()
    }

    /// Subscribe to broadcast events for a session.
    ///
    /// Returns a `broadcast::Receiver` and the current `next_seq` value
    /// (the sequence number of the LAST event written to storage). Subscribing
    /// before the DB read is required by the catch-up algorithm (DESIGN.md §6).
    ///
    /// Returns `None` if the session is not in the live registry.
    pub async fn subscribe_to_session(
        &self,
        session_id: &str,
    ) -> Option<(broadcast::Receiver<SessionEvent>, i32)> {
        use std::sync::atomic::Ordering;
        let guard = self.runtimes.read().await;
        let slot_arc = guard.get(session_id)?.clone();
        drop(guard);

        let slot = slot_arc.lock().await;
        let rx = slot.broadcast_tx.subscribe();
        let current_seq = slot.next_seq.load(Ordering::SeqCst);
        Some((rx, current_seq))
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Read `CLAW_BROADCAST_CAPACITY` from the environment; fall back to
/// `DEFAULT_BROADCAST_CAPACITY` (256) if unset or invalid.
pub fn broadcast_capacity_from_env() -> usize {
    std::env::var("CLAW_BROADCAST_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BROADCAST_CAPACITY)
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
        assert!(result.capabilities.sessions, "M4 must advertise sessions");
        assert!(
            result.capabilities.streaming,
            "M4 must advertise streaming"
        );
        assert!(result.capabilities.tools, "M4 must advertise tools");
        assert!(
            result.capabilities.permissions,
            "M4 must advertise permissions"
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
        match &err {
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

    // -----------------------------------------------------------------------
    // T4.4 — permission_response handler unit tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_permission_response_unknown_session_returns_error() {
        let (handler, _ws) = fixture_handler();
        let err = handler
            .handle_permission_response(PermissionResponseParams {
                session_id: "no-such-session".to_string(),
                request_id: "perm-1".to_string(),
                decision: PermissionDecisionStr::Allow,
                reason: None,
            })
            .await
            .expect_err("must error");
        assert!(matches!(err, AcpError::UnknownSession(_)));
        assert_eq!(err.code(), error_codes::UNKNOWN_SESSION);
    }

    #[tokio::test]
    async fn test_permission_response_no_pending_returns_no_such_request() {
        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new session");

        // No pending permission — response must return NoSuchPermissionRequest.
        let err = handler
            .handle_permission_response(PermissionResponseParams {
                session_id: created.session_id.clone(),
                request_id: "perm-never-existed".to_string(),
                decision: PermissionDecisionStr::Allow,
                reason: None,
            })
            .await
            .expect_err("must error");
        assert!(matches!(err, AcpError::NoSuchPermissionRequest(_)));
        assert_eq!(err.code(), error_codes::NO_SUCH_PERMISSION_REQUEST);
    }

    #[tokio::test]
    async fn test_permission_response_wrong_request_id_returns_error() {
        use runtime::PermissionPromptDecision;
        use tokio::sync::oneshot;

        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new session");

        // Install a pending permission with request_id "perm-A".
        let (tx, _rx) = oneshot::channel::<PermissionPromptDecision>();
        {
            let slot_arc = handler
                .get_slot(&created.session_id)
                .await
                .expect("slot");
            let mut slot = slot_arc.lock().await;
            slot.pending_permission = Some(PendingPermissionRequest {
                request_id: "perm-A".to_string(),
                tx,
            });
        }

        // Respond with wrong request_id "perm-B".
        let err = handler
            .handle_permission_response(PermissionResponseParams {
                session_id: created.session_id.clone(),
                request_id: "perm-B".to_string(),
                decision: PermissionDecisionStr::Allow,
                reason: None,
            })
            .await
            .expect_err("must error");
        assert!(matches!(err, AcpError::NoSuchPermissionRequest(_)));

        // The pending_permission must still be in the slot (we put it back).
        let slot_arc = handler.get_slot(&created.session_id).await.expect("slot");
        let slot = slot_arc.lock().await;
        assert!(
            slot.pending_permission.is_some(),
            "pending permission must be restored after wrong-id response"
        );
    }

    #[tokio::test]
    async fn test_permission_response_routes_decision_to_oneshot() {
        use runtime::PermissionPromptDecision;
        use tokio::sync::oneshot;

        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new session");

        // Install a pending permission with request_id "perm-correct".
        let (tx, rx) = oneshot::channel::<PermissionPromptDecision>();
        {
            let slot_arc = handler
                .get_slot(&created.session_id)
                .await
                .expect("slot");
            let mut slot = slot_arc.lock().await;
            slot.pending_permission = Some(PendingPermissionRequest {
                request_id: "perm-correct".to_string(),
                tx,
            });
        }

        // Send the allow response.
        handler
            .handle_permission_response(PermissionResponseParams {
                session_id: created.session_id.clone(),
                request_id: "perm-correct".to_string(),
                decision: PermissionDecisionStr::Allow,
                reason: None,
            })
            .await
            .expect("response ok");

        // The decision must have arrived on the receiver.
        let decision = rx.await.expect("oneshot resolved");
        assert_eq!(decision, PermissionPromptDecision::Allow);

        // The slot must now have no pending permission.
        let slot_arc = handler.get_slot(&created.session_id).await.expect("slot");
        let slot = slot_arc.lock().await;
        assert!(
            slot.pending_permission.is_none(),
            "pending_permission cleared after successful response"
        );
    }

    #[tokio::test]
    async fn test_permission_response_second_response_gets_no_such_request() {
        use runtime::PermissionPromptDecision;
        use tokio::sync::oneshot;

        let (handler, _ws) = fixture_handler();
        let created = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new session");

        // Install a pending permission.
        let (tx, _rx) = oneshot::channel::<PermissionPromptDecision>();
        {
            let slot_arc = handler
                .get_slot(&created.session_id)
                .await
                .expect("slot");
            let mut slot = slot_arc.lock().await;
            slot.pending_permission = Some(PendingPermissionRequest {
                request_id: "perm-X".to_string(),
                tx,
            });
        }

        // First response — succeeds (even though receiver is dropped, the tx.send
        // will fail, but the slot is still cleared).
        // Actually: _rx is live so tx.send succeeds.
        let result = handler
            .handle_permission_response(PermissionResponseParams {
                session_id: created.session_id.clone(),
                request_id: "perm-X".to_string(),
                decision: PermissionDecisionStr::Deny,
                reason: Some("not allowed".to_string()),
            })
            .await;
        assert!(result.is_ok(), "first response must succeed");

        // Second response with same request_id — slot is empty now.
        let err = handler
            .handle_permission_response(PermissionResponseParams {
                session_id: created.session_id.clone(),
                request_id: "perm-X".to_string(),
                decision: PermissionDecisionStr::Allow,
                reason: None,
            })
            .await
            .expect_err("second response must fail");
        assert_eq!(err.code(), error_codes::NO_SUCH_PERMISSION_REQUEST);
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
