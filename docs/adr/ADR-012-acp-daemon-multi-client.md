# ADR-012 — ACP Daemon Multi-Client Support

**Status**: Accepted
**Date**: 2026-04-24
**Branch**: `feature/acp-daemon-m3`

---

## Context

The claw ACP daemon (introduced in M2) supports a single local WebSocket client.
M3 adds Postgres-backed session storage, multi-client fan-out, tool-call streaming,
permission broadcast, and an Open WebUI pipe function. See `docs/acp-m3/SPEC.md`
and `docs/acp-m3/DESIGN.md` for the full specification and design.

## Decision

Implement ACP M3 as five phases (P1–P5) on `feature/acp-daemon-m3`:

- **P1**: Postgres session backend (`SessionBackend` trait + `PostgresSessionBackend`)
- **P2**: Tool-call streaming + broadcast fan-out (`session/update` notifications)
- **P3**: Open WebUI pipe function (`integrations/openwebui/claw_acp_pipe.py`)
- **P4**: Permission prompt broadcast (`session/permission_request` + response)
- **P5** (stretch): Evaluate slopus/happy mobile client interop

## Consequences

- Multi-client sessions are now first-class. Any number of WebSocket clients can
  attach to a session and receive real-time event streams.
- Postgres is an optional dependency gated behind `CLAW_SESSION_BACKEND=postgres`.
  File backend remains default and test-only.
- Open WebUI gains a native pipe function for streaming ACP sessions.
- Permission prompts are broadcast to all attached clients; first responder wins.

## Alternatives Considered / Deferred

### M5 — slopus/happy mobile client interop (DEFERRED)

**Decision**: NO-GO. After full protocol research (2026-04-24), the happy relay
protocol differs from ACP on 8 axes — well above the < 5 deviation threshold —
primarily because happy mandates Socket.IO transport, per-user OAuth auth, and
end-to-end NaCl/AES-256-GCM encryption at the relay boundary, all of which ACP
assumes absent. A correct adapter is estimated at ~750 LOC of non-trivial crypto
and Socket.IO bridging code, exceeding the < 200 LOC fit criterion by ~3.75x.
Full analysis in `docs/acp-m3/m5-happy-deferral.md`.

Recommended paths forward: (a) propose an ACP-compatible unencrypted connection
mode upstream to `slopus/happy`, or (b) build a minimal own mobile PWA that
speaks ACP JSON-RPC 2.0 directly over WebSocket (separate ADR).
