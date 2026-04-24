# M5 — slopus/happy Interop: Deferral Report

**Date**: 2026-04-24
**Branch**: `feature/acp-daemon-m3`
**Decision**: NO-GO — defer to future ADR
**Authored during**: Phase 5 (stretch) research pass

---

## 1. What Was Researched

Sources consulted (public, unauthenticated reads via GitHub API):

- `slopus/happy` — mobile/web client monorepo
  - `packages/happy-wire/src/sessionProtocol.ts` — canonical event schema (Zod)
  - `packages/happy-wire/src/messages.ts` — encrypted wire container schema
  - `packages/happy-wire/src/legacyProtocol.ts` — active legacy protocol
  - `packages/happy-wire/README.md` — full wire spec with normative examples
  - `docs/protocol.md` — transport, WebSocket model, Socket.IO event catalog
  - `docs/encryption.md` — E2E encryption design (NaCl secretbox + AES-256-GCM)
  - `docs/session-protocol.md` — unified session event protocol, comparison with ACP
  - `docs/session-protocol-claude.md` — Claude-specific adapter notes
  - `docs/realtime-sync-and-rpc.md` — Socket.IO RPC model

- `slopus/happy-server` — relay server
  - `sources/app/api/socket/sessionUpdateHandler.ts` — server-side Socket.IO handlers
  - `sources/types.ts` — server-side artifact types
  - `sources/modules/encrypt.ts` — server encryption module

---

## 2. Protocol Summary

Happy uses a **proprietary relay protocol** that is fundamentally different from
ACP JSON-RPC 2.0. Key characteristics:

**Transport**: Socket.IO (not raw WebSocket) at path `/v1/updates` with three
connection scopes (`user-scoped`, `session-scoped`, `machine-scoped`). Happy
clients never connect to the AI backend directly — the happy-relay mediates all
content as opaque encrypted blobs.

**Message model**: All session content (messages, tool calls, agent state) is
end-to-end encrypted client-side before hitting the relay. The wire container is:

```json
{
  "id": "msg-db-id",
  "seq": 101,
  "content": { "t": "encrypted", "c": "BASE64_CIPHERTEXT" },
  "createdAt": 1739347200000,
  "updatedAt": 1739347200000
}
```

After decryption, the payload is a session envelope:

```json
{
  "id": "cuid2",
  "time": 1739347200000,
  "role": "user" | "agent",
  "turn": "cuid2",
  "ev": { "t": "text" | "tool-call-start" | "tool-call-end" | "turn-start" | "turn-end" | ... }
}
```

**Session protocol note (critical)**: The `docs/session-protocol.md` explicitly
states under "Comparison with ACP":

> **Why not ACP directly?**
> 1. **Encryption** — ACP assumes plaintext REST. Our payloads are end-to-end encrypted.
> 2. **Tool calls are UI-visible** — ACP models tools as metadata for debugging. We render them with spinners, descriptions, and permission dialogs.

The happy team is aware of the real ACP spec (`agentcommunicationprotocol.dev`)
and explicitly chose NOT to use it for the above reasons.

**Active protocol**: The `sessionProtocol.ts` is marked `⚠️ UNDER REVIEW — LIKELY
NEEDS MORE CAREFUL DESIGN` and `NOT used in production`. The production path uses
the legacy protocol (`role: 'user' | role: 'agent'`). The session protocol with
typed events (`tool-call-start`, etc.) is frozen pending redesign.

---

## 3. Gap Analysis — Deviations Count

| # | Deviation | Category | Notes |
|---|-----------|----------|-------|
| 1 | **Transport layer**: Happy requires Socket.IO, not raw WebSocket JSON-RPC | Fundamental | Socket.IO uses its own framing, rooms, ack model — not compatible with our `axum`-based raw WS handler |
| 2 | **Authentication model**: Happy requires Bearer token OAuth session per user; our ACP is unauthenticated localhost | Fundamental | Our ACP has no auth layer at all; adding one changes the security model |
| 3 | **E2E encryption**: Every message payload is NaCl/AES-256-GCM encrypted by the client; server never sees plaintext | Fundamental | Our ACP daemon would need to implement the full key exchange and encryption handshake — this alone is > 200 LOC of crypto code in Rust |
| 4 | **Data encryption key (DEK) model**: Sessions use per-session DEKs wrapped with ephemeral Curve25519 keypairs | Fundamental | Not expressible in ACP's plaintext JSON-RPC model without a separate key exchange layer |
| 5 | **Session identity model**: Happy sessions are user-account-scoped objects living in happy-server's Postgres; our sessions are workspace-scoped local processes | Semantic | A happy `sessionId` is a DB row in their cloud relay; our `session_id` identifies a local Claude process. The mapping is M:N (one user, many machines) |
| 6 | **Message submission**: Client emits `socket.emit("message", {sid, message: "<base64-ciphertext>"})` — a Socket.IO event, not a JSON-RPC method call | Protocol | Our `session/prompt` takes plaintext; happy's equivalent takes encrypted blobs |
| 7 | **Tool call model mismatch**: Happy's `tool-call-start` has `title`, `description`, `args: Record<string,unknown>`; our `ToolUseStart` has `input: String` (raw JSON string) and no title/description | Semantic | Format translation needed, but only one of 8 deviations |
| 8 | **Turn lifecycle**: Happy uses `turn-start` / `turn-end(status)` where status is `completed|failed|cancelled`; ACP uses `TurnEnd` (no status on the event, status inferred from `TurnError`) | Semantic | Minor, but requires mapping logic |

**Total deviations: 8** (GO threshold was < 5)

**Critical deviations**: #1 (Socket.IO), #2 (auth), #3 (E2E crypto), #4 (DEK model)
— any single one of these makes a clean adapter infeasible. All four together mean
the adapter is not a thin shim; it is effectively a new relay implementation.

---

## 4. Why < 200 LOC Is Not Achievable

The spec's < 200 LOC criterion assumes the two protocols share transport and
security model and differ only in method names or field shapes. Happy's protocol
differs at the transport + security layer:

1. **Socket.IO client in Rust**: No production-quality async Socket.IO client
   exists in the Rust ecosystem as of 2026-04 that matches Socket.IO v4's
   handshake, polling fallback, and ack model. `rust-socketio` exists but is
   alpha-quality. Implementing a minimal compliant client: ~300 LOC minimum.

2. **NaCl secretbox + AES-256-GCM decrypt in Rust**: Using `sodiumoxide` or
   `chacha20poly1305` + `aes-gcm` crates, implementing the dataKey variant
   (version byte + nonce + ciphertext + auth tag) plus the legacy secretbox
   variant: ~150 LOC of crypto handling.

3. **DEK key exchange (ephemeral Curve25519)**: Decrypting the per-session DEK
   bundle (ephPubKey 32B + nonce 24B + ciphertext) using `tweetnacl.box`
   semantics: ~80 LOC.

4. **Session envelope routing**: Mapping from the `CoreUpdateContainer` →
   `SessionEnvelope` → `SessionEvent` → ACP `SessionEvent`: ~100 LOC.

5. **OAuth token management**: Acquiring and refreshing the Bearer token to
   authenticate against happy-relay: ~120 LOC.

Minimum credible estimate: **750+ LOC** for a correct, non-hacky adapter.
The > 200 LOC threshold is exceeded by ~3.75x even on a generous count.

---

## 5. What Would Need to Change for Interop to Be Feasible

### Option A — Happy adopts a plaintext ACP mode

Happy would need to add an alternative connection type (e.g., `acp-scoped`) that
accepts JSON-RPC 2.0 over raw WebSocket without E2E encryption, routing directly
to the daemon instead of through the relay. This is a design change that must be
proposed upstream. Upstream have explicitly documented why they don't use ACP
(`docs/session-protocol.md` §"Why not ACP directly?").

**File upstream**: https://github.com/slopus/happy/issues (open a feature request
for ACP-over-WebSocket mode — no issue was filed during this research pass because
the upstream team's stated position makes it unlikely to be accepted without prior
alignment discussion).

### Option B — We implement the happy-server relay protocol

We run a local instance of `slopus/happy-server` (TypeScript/Node) and implement
a machine-scoped Socket.IO client in the claw daemon that speaks the happy-relay
protocol and bridges to ACP. This is architecturally correct but:

- Requires operating the happy-server relay (cloud or self-hosted) + managing
  OAuth credentials per user.
- The bridge is a full project, not a stretch phase.
- No E2E encryption without the key management infrastructure.

### Option C — Build our own minimal mobile client

Implement a small PWA or React Native app that speaks ACP JSON-RPC 2.0 directly
over WebSocket to the local daemon (via Tailscale or SSH tunnel). This bypasses
happy entirely. Simpler than adapting happy; gives us full control of UX. The
mobile client would be trivial (< 500 LOC JS) because ACP is already documented.

**Recommendation**: Option C is the most pragmatic path if mobile client access
is a real user need. Option A is the correct long-term interop path but requires
upstream alignment first.

---

## 6. Summary Recommendation

1. Do NOT implement `AcpHappyAdapter`. The protocol delta is 8 deviations (vs
   threshold of < 5), and the adapter would be ~750 LOC of non-trivial crypto
   + Socket.IO code — not a clean abstraction boundary.

2. File an upstream issue on `slopus/happy` requesting an ACP-compatible connection
   mode if mobile interop is a stated product goal.

3. If immediate mobile access is needed, evaluate Option C (own minimal mobile
   client over ACP WebSocket) in a separate ADR.

4. Revisit this decision after: (a) happy's `sessionProtocol.ts` exits its
   "UNDER REVIEW" state, or (b) happy officially supports an ACP-compatible mode.
