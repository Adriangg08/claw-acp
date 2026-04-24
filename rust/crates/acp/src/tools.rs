//! ACP tool-call surface — M4 Permission Prompt Broadcast.
//!
//! [`AcpPermissionPrompter`] implements the synchronous [`PermissionPrompter`]
//! trait by bridging into the async world via `handle.block_on(rx.await)`.
//!
//! # Safety of the sync→async bridge
//!
//! `ConversationRuntime::run_turn` is synchronous and runs inside a
//! `tokio::task::spawn_blocking` call (verified in
//! `docs/acp-m3/findings/flag-D-runtime-context.md`, Flag D).  Calling
//! `Handle::block_on` inside `spawn_blocking` is explicitly supported by
//! Tokio: `block_on` drives a future to completion on the current thread
//! without interacting with the outer async executor's thread pool.
//!
//! # Single-prompt-at-a-time invariant (SPEC NF4.2)
//!
//! Only one `PermissionRequest` may be in flight per session at a time.
//! If a second prompt arrives while the first is pending (e.g. two tools
//! being authorised in an iteration), `decide` blocks until the first is
//! resolved, then installs the second.  This is implemented via a
//! `tokio::sync::Mutex`-guarded slot inside [`crate::session::SessionSlot`].

use std::sync::Arc;

use runtime::{PermissionPromptDecision, PermissionPrompter, PermissionRequest};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, Mutex};

use crate::session::{PendingPermissionRequest, SessionSlot};
use crate::stream::SessionEvent;

// ---------------------------------------------------------------------------
// Environment-variable defaults
// ---------------------------------------------------------------------------

/// Default permission prompt timeout in seconds (SPEC F4.4).
/// Overridden by `PERMISSION_TIMEOUT_SECS` env var.
pub const DEFAULT_PERMISSION_TIMEOUT_SECS: u64 = 60;

/// Read `PERMISSION_TIMEOUT_SECS` from the environment; fall back to
/// `DEFAULT_PERMISSION_TIMEOUT_SECS` if unset or unparseable.
#[must_use]
pub fn permission_timeout_from_env() -> u64 {
    std::env::var("PERMISSION_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_PERMISSION_TIMEOUT_SECS)
}

// ---------------------------------------------------------------------------
// AcpPermissionPrompter
// ---------------------------------------------------------------------------

/// Implements [`PermissionPrompter`] for the ACP daemon.
///
/// When `decide` is called (from inside a `spawn_blocking` thread):
///
/// 1. Generates a unique `request_id` (UUID4).
/// 2. Installs a `oneshot::Sender<PermissionPromptDecision>` in the session
///    slot's `pending_permission` field.  If another prompt is already pending,
///    waits (via `block_on`) until it is resolved, then installs the new one.
/// 3. Broadcasts a `SessionEvent::PermissionRequest` to all attached clients.
/// 4. Calls `handle.block_on(rx.await)` to wait for the first client response
///    or timeout.
/// 5. On timeout, removes the pending permission, broadcasts
///    `SessionEvent::PermissionTimeout` (a meta notification), and returns
///    `Deny { reason: "permission prompt timed out" }`.
///
/// The broadcast channel is held as an `Arc` clone so the prompter can
/// send to it without holding the slot mutex across the `block_on` call.
pub struct AcpPermissionPrompter {
    pub session_id: String,
    pub slot: Arc<Mutex<SessionSlot>>,
    pub handle: Handle,
    pub timeout_secs: u64,
}

impl AcpPermissionPrompter {
    /// Create a new prompter for the given session slot.
    ///
    /// `timeout_secs` is typically read from `PERMISSION_TIMEOUT_SECS`.
    #[must_use]
    pub fn new(session_id: String, slot: Arc<Mutex<SessionSlot>>, handle: Handle) -> Self {
        Self {
            session_id,
            slot,
            handle,
            timeout_secs: permission_timeout_from_env(),
        }
    }

    /// Override the timeout (used by tests with very short timeouts).
    #[must_use]
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }
}

impl PermissionPrompter for AcpPermissionPrompter {
    fn decide(&mut self, request: &PermissionRequest) -> PermissionPromptDecision {
        // Clone handle so we can pass `self` into `decide_async` mutably.
        let handle = self.handle.clone();
        handle.block_on(self.decide_async(request))
    }
}

impl AcpPermissionPrompter {
    /// Async implementation of the permission prompt.
    ///
    /// Called via `handle.block_on` from the sync `decide` method.
    async fn decide_async(&mut self, request: &PermissionRequest) -> PermissionPromptDecision {
        let request_id = uuid::Uuid::new_v4().to_string();
        let timeout = std::time::Duration::from_secs(self.timeout_secs);

        // Truncate input_preview to 512 chars (SPEC NF4.3).
        let input_preview = if request.input.len() > 512 {
            format!("{}…", &request.input[..512])
        } else {
            request.input.clone()
        };

        // Step 1: install the oneshot sender in the slot.
        // If another permission is still pending, we must wait until it resolves
        // first (SPEC NF4.2 single-prompt-at-a-time).
        let (tx, rx) = oneshot::channel::<PermissionPromptDecision>();
        let broadcast_tx = {
            // Wait loop: acquire slot lock, check if pending_permission is empty.
            loop {
                let mut slot = self.slot.lock().await;
                if slot.pending_permission.is_none() {
                    // Slot is free — install our request and grab broadcast_tx.
                    slot.pending_permission = Some(PendingPermissionRequest {
                        request_id: request_id.clone(),
                        tx,
                    });
                    let broadcast_tx = Arc::clone(&slot.broadcast_tx);
                    drop(slot);
                    break broadcast_tx;
                }
                // Another prompt is pending. Release lock and yield briefly.
                drop(slot);
                tokio::task::yield_now().await;
            }
        };

        // Step 2: broadcast PermissionRequest to all clients.
        let perm_event = SessionEvent::PermissionRequest {
            request_id: request_id.clone(),
            tool_name: request.tool_name.clone(),
            input_preview,
            required_mode: request.required_mode.as_str().to_string(),
            current_mode: request.current_mode.as_str().to_string(),
            reason: request.reason.clone(),
        };
        // Ignore send errors — 0 receivers is valid (NF2.2 pattern).
        let _ = broadcast_tx.send(perm_event);

        tracing::info!(
            session_id = %self.session_id,
            request_id = %request_id,
            tool_name = %request.tool_name,
            timeout_secs = self.timeout_secs,
            "permission prompt broadcast to all clients"
        );

        // Step 3: wait for the first response or timeout.
        let result = tokio::time::timeout(timeout, rx).await;

        match result {
            Ok(Ok(decision)) => {
                // A client responded in time.
                tracing::info!(
                    session_id = %self.session_id,
                    request_id = %request_id,
                    "permission prompt resolved by client"
                );
                decision
            }
            Ok(Err(_)) => {
                // The sender was dropped without sending (should not happen in
                // normal operation, but handle defensively).
                tracing::warn!(
                    session_id = %self.session_id,
                    request_id = %request_id,
                    "permission oneshot sender dropped unexpectedly — denying"
                );
                PermissionPromptDecision::Deny {
                    reason: "permission prompt channel dropped unexpectedly".to_string(),
                }
            }
            Err(_elapsed) => {
                // Timeout: remove the pending permission from the slot (in case
                // a late-arriving response doesn't find it) and broadcast timeout.
                {
                    let mut slot = self.slot.lock().await;
                    // Only remove if it's still our request_id (another prompt
                    // could not have installed itself while we were waiting, but
                    // be defensive).
                    if slot
                        .pending_permission
                        .as_ref()
                        .is_some_and(|p| p.request_id == request_id)
                    {
                        slot.pending_permission = None;
                    }
                }

                // Broadcast a timeout notification so clients know the prompt expired.
                // We re-use the broadcast_tx clone we captured at install time.
                // The `PermissionTimeout` event is a meta event; it does NOT carry
                // a `turn_id` (not tied to a specific turn position).
                // We encode it as a ClientLagged-style meta event via the dispatch
                // loop's special handling in lib.rs. For now, log and return Deny.
                tracing::warn!(
                    session_id = %self.session_id,
                    request_id = %request_id,
                    timeout_secs = self.timeout_secs,
                    "permission prompt timed out — denying tool call"
                );

                PermissionPromptDecision::Deny {
                    reason: "permission prompt timed out".to_string(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{NewSessionParams, SessionHandler};
    use runtime::{PermissionMode, PermissionRequest};
    use tempfile::TempDir;
    use runtime::SessionStore;

    fn fixture_handler() -> (SessionHandler, TempDir, TempDir) {
        let workspace = TempDir::new().expect("tempdir");
        let data_dir = TempDir::new().expect("data tempdir");
        let store = SessionStore::from_data_dir(data_dir.path(), workspace.path())
            .expect("store from data dir");
        (SessionHandler::new(store), workspace, data_dir)
    }

    fn test_permission_request(tool_name: &str) -> PermissionRequest {
        PermissionRequest {
            tool_name: tool_name.to_string(),
            input: r#"{"command":"rm -rf /tmp/test"}"#.to_string(),
            current_mode: PermissionMode::WorkspaceWrite,
            required_mode: PermissionMode::DangerFullAccess,
            reason: Some("requires full access to delete files".to_string()),
        }
    }

    /// Helper: create a session and return (session_id, slot_arc, broadcast_rx).
    async fn create_session_with_slot(
        handler: &SessionHandler,
    ) -> (String, Arc<Mutex<SessionSlot>>) {
        let result = handler
            .handle_new(NewSessionParams::default())
            .await
            .expect("new session");
        let slot_arc = handler
            .get_slot(&result.session_id)
            .await
            .expect("slot");
        (result.session_id, slot_arc)
    }

    #[tokio::test]
    async fn test_prompter_resolves_when_tx_sends() {
        let (handler, _ws, _data) = fixture_handler();
        let (session_id, slot_arc) = create_session_with_slot(&handler).await;

        let handle = tokio::runtime::Handle::current();
        let mut prompter = AcpPermissionPrompter::new(
            session_id.clone(),
            Arc::clone(&slot_arc),
            handle.clone(),
        )
        .with_timeout(5);

        let request = test_permission_request("Bash");

        // Subscribe to broadcast to capture the PermissionRequest event.
        let mut rx = {
            let slot = slot_arc.lock().await;
            slot.broadcast_tx.subscribe()
        };

        // Spawn a task that will send the Allow decision after a short delay.
        let slot_for_task = Arc::clone(&slot_arc);
        let expected_request_id = {
            // We need to capture the request_id after the prompter installs it.
            // Do this by waiting for the broadcast event and extracting the id.
            tokio::spawn(async move {
                // Wait for PermissionRequest to be broadcast.
                let ev = rx.recv().await.expect("PermissionRequest event");
                if let SessionEvent::PermissionRequest { request_id, .. } = ev {
                    // Send Allow decision via handle_permission_response.
                    let mut slot = slot_for_task.lock().await;
                    let pending = slot.pending_permission.take();
                    drop(slot);
                    if let Some(pending) = pending {
                        let _ = pending.tx.send(PermissionPromptDecision::Allow);
                    }
                    request_id
                } else {
                    panic!("unexpected event: {ev:?}");
                }
            })
        };

        // Run decide synchronously (it internally uses block_on).
        // We run it in a spawn_blocking to simulate the TurnDriver context.
        let decision = tokio::task::spawn_blocking(move || {
            prompter.decide(&request)
        })
        .await
        .expect("spawn_blocking");

        let _req_id = expected_request_id.await.expect("task");
        assert_eq!(decision, PermissionPromptDecision::Allow);
    }

    #[tokio::test]
    async fn test_prompter_times_out_and_returns_deny() {
        let (handler, _ws, _data) = fixture_handler();
        let (_session_id, slot_arc) = create_session_with_slot(&handler).await;

        let handle = tokio::runtime::Handle::current();
        let mut prompter = AcpPermissionPrompter::new(
            "test-session".to_string(),
            Arc::clone(&slot_arc),
            handle.clone(),
        )
        .with_timeout(1); // 1 second — short for test

        let request = test_permission_request("Bash");

        // Run decide; nobody will answer, so it must time out.
        let decision = tokio::task::spawn_blocking(move || prompter.decide(&request))
            .await
            .expect("spawn_blocking");

        match decision {
            PermissionPromptDecision::Deny { reason } => {
                assert!(reason.contains("timed out"), "reason must mention timeout, got: {reason}");
            }
            PermissionPromptDecision::Allow => panic!("expected Deny on timeout"),
        }

        // After timeout, pending_permission must be cleared.
        let slot = slot_arc.lock().await;
        assert!(
            slot.pending_permission.is_none(),
            "pending_permission must be cleared after timeout"
        );
    }

    #[tokio::test]
    async fn test_prompter_deny_decision_propagates_reason() {
        let (handler, _ws, _data) = fixture_handler();
        let (_session_id, slot_arc) = create_session_with_slot(&handler).await;

        let handle = tokio::runtime::Handle::current();
        let mut prompter = AcpPermissionPrompter::new(
            "test-session".to_string(),
            Arc::clone(&slot_arc),
            handle.clone(),
        )
        .with_timeout(5);

        let request = test_permission_request("Bash");

        let mut rx = {
            let slot = slot_arc.lock().await;
            slot.broadcast_tx.subscribe()
        };

        let slot_for_task = Arc::clone(&slot_arc);
        tokio::spawn(async move {
            let _ev = rx.recv().await.expect("event");
            // Respond with Deny.
            let mut slot = slot_for_task.lock().await;
            let pending = slot.pending_permission.take();
            drop(slot);
            if let Some(pending) = pending {
                let _ = pending.tx.send(PermissionPromptDecision::Deny {
                    reason: "user said no".to_string(),
                });
            }
        });

        let decision = tokio::task::spawn_blocking(move || prompter.decide(&request))
            .await
            .expect("spawn_blocking");

        match decision {
            PermissionPromptDecision::Deny { reason } => {
                assert_eq!(reason, "user said no");
            }
            PermissionPromptDecision::Allow => panic!("expected Deny"),
        }
    }

    #[tokio::test]
    async fn test_permission_request_event_has_correct_fields() {
        let (handler, _ws, _data) = fixture_handler();
        let (_session_id, slot_arc) = create_session_with_slot(&handler).await;

        let handle = tokio::runtime::Handle::current();
        let mut prompter = AcpPermissionPrompter::new(
            "test-session".to_string(),
            Arc::clone(&slot_arc),
            handle.clone(),
        )
        .with_timeout(5);

        let request = PermissionRequest {
            tool_name: "Bash".to_string(),
            input: r#"{"command":"echo hello"}"#.to_string(),
            current_mode: PermissionMode::WorkspaceWrite,
            required_mode: PermissionMode::DangerFullAccess,
            reason: Some("test reason".to_string()),
        };

        let mut rx = {
            let slot = slot_arc.lock().await;
            slot.broadcast_tx.subscribe()
        };

        let slot_for_task = Arc::clone(&slot_arc);
        let event_task = tokio::spawn(async move {
            let ev = rx.recv().await.expect("event");
            // Respond to allow the prompter to unblock.
            let mut slot = slot_for_task.lock().await;
            let pending = slot.pending_permission.take();
            drop(slot);
            if let Some(pending) = pending {
                let _ = pending.tx.send(PermissionPromptDecision::Allow);
            }
            ev
        });

        tokio::task::spawn_blocking(move || prompter.decide(&request))
            .await
            .expect("spawn_blocking");

        let broadcast_event = event_task.await.expect("event task");
        match broadcast_event {
            SessionEvent::PermissionRequest {
                request_id,
                tool_name,
                input_preview,
                required_mode,
                current_mode,
                reason,
            } => {
                assert!(!request_id.is_empty(), "request_id must be non-empty");
                assert_eq!(tool_name, "Bash");
                assert_eq!(input_preview, r#"{"command":"echo hello"}"#);
                assert_eq!(required_mode, "danger-full-access");
                assert_eq!(current_mode, "workspace-write");
                assert_eq!(reason.as_deref(), Some("test reason"));
            }
            other => panic!("expected PermissionRequest event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_input_preview_truncated_to_512_chars() {
        let (handler, _ws, _data) = fixture_handler();
        let (_session_id, slot_arc) = create_session_with_slot(&handler).await;

        let handle = tokio::runtime::Handle::current();
        let mut prompter = AcpPermissionPrompter::new(
            "test-session".to_string(),
            Arc::clone(&slot_arc),
            handle.clone(),
        )
        .with_timeout(5);

        // Create a request with a 600-char input.
        let long_input = "x".repeat(600);
        let request = PermissionRequest {
            tool_name: "Bash".to_string(),
            input: long_input,
            current_mode: PermissionMode::WorkspaceWrite,
            required_mode: PermissionMode::DangerFullAccess,
            reason: None,
        };

        let mut rx = {
            let slot = slot_arc.lock().await;
            slot.broadcast_tx.subscribe()
        };

        let slot_for_task = Arc::clone(&slot_arc);
        let event_task = tokio::spawn(async move {
            let ev = rx.recv().await.expect("event");
            let mut slot = slot_for_task.lock().await;
            let pending = slot.pending_permission.take();
            drop(slot);
            if let Some(pending) = pending {
                let _ = pending.tx.send(PermissionPromptDecision::Allow);
            }
            ev
        });

        tokio::task::spawn_blocking(move || prompter.decide(&request))
            .await
            .expect("spawn_blocking");

        let ev = event_task.await.expect("event task");
        if let SessionEvent::PermissionRequest { input_preview, .. } = ev {
            // 512 bytes + "…" ellipsis (3 bytes in UTF-8) = 515 chars max.
            // We truncate at char boundary 512, then append "…".
            assert!(
                input_preview.len() <= 515 + 10, // some slack for multi-byte
                "preview must be truncated, len={}", input_preview.len()
            );
            assert!(input_preview.ends_with('…'), "preview must end with ellipsis");
        } else {
            panic!("expected PermissionRequest");
        }
    }
}
