//! Agent Client Protocol (ACP) server for claw-code.
//!
//! This crate will eventually host the ACP daemon that `claw acp serve`
//! launches, bridging ACP clients (e.g. Zed, `slopus/happy`) with the
//! existing claw-code runtime (`Session`, `ConversationRuntime`, tool
//! executor, `PermissionEnforcer`).
//!
//! Today this crate is a scaffold: the module layout and public entrypoint
//! exist so subsequent PRs can land protocol surfaces incrementally without
//! having to rewire the CLI each time. The real server is not implemented.
//!
//! Tracked upstream as ROADMAP #76. Spec:
//! <https://github.com/zed-industries/agent-client-protocol>.

pub mod session;
pub mod stream;
pub mod tools;
pub mod transport;

/// Options passed from `claw acp serve` into the server entrypoint.
///
/// Intentionally minimal — fields will be added as transports/flags land.
#[derive(Debug, Default, Clone)]
pub struct ServeOptions {
    /// When true, speak ACP over stdio (default for editor spawn).
    pub stdio: bool,
}

/// Error type surfaced by the ACP server.
///
/// Kept opaque on purpose for the scaffold; concrete variants will arrive
/// alongside the transport + session work.
#[derive(Debug)]
pub enum AcpError {
    /// The requested ACP feature is not yet implemented.
    NotImplemented(&'static str),
}

impl std::fmt::Display for AcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotImplemented(what) => write!(f, "ACP feature not implemented: {what}"),
        }
    }
}

impl std::error::Error for AcpError {}

/// Launch the ACP server with the given options.
///
/// This is the single integration point used by `rusty-claude-cli` so the
/// CLI does not need to depend on internal module layout. Today it returns
/// a `NotImplemented` error; downstream milestones will replace the body
/// without touching the CLI wiring.
pub fn serve(_options: ServeOptions) -> Result<(), AcpError> {
    Err(AcpError::NotImplemented(
        "acp::serve — server is scaffolded but not yet implemented (ROADMAP #76)",
    ))
}
