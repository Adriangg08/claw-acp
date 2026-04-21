//! ACP wire transport: stdio (default) and websocket (optional).
//!
//! Responsible for framing JSON-RPC-style ACP messages and dispatching them
//! to the session + stream modules. Milestone M1.
