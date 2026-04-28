# claw ACP Pipe — Open WebUI Installation Guide

`acp_pipe.py` is an Open WebUI **Pipe Function** that turns Open WebUI into an
ACP client. Every chat message is forwarded to the claw daemon over WebSocket
(ACP JSON-RPC 2.0, protocol `"0.2"`), and the response streams back as SSE.

---

## Prerequisites

| Component | Requirement |
|-----------|-------------|
| Open WebUI | >= 0.3.x (includes `websockets` Python package) |
| claw daemon | Running with `claw acp serve --addr <host>:7800` |
| ACP protocol | Phase 2 complete (streaming + tools). Phase 4 optional (permissions). |

---

## Installation

1. **Open Admin Panel** → **Functions** → **(+) Create Function**
2. Paste the entire content of `acp_pipe.py` into the editor.
3. Click **Save**.
4. Toggle the function **ON** (enable icon next to the function name).
5. In any chat conversation, click the model selector and choose **"claw"**
   (or the name you gave the function).

---

## Valve Configuration

Open the gear icon next to the function to configure:

| Valve | Type | Default | Description |
|-------|------|---------|-------------|
| `daemon_url` | str | `ws://claw-daemon:7800` | WebSocket address of the ACP daemon |
| `workspace_root` | str | `/workspace` | Root path passed to `session/new` |
| `model` | str | `default` | Model alias forwarded to the daemon |
| `permission_timeout_seconds` | int | `300` | Seconds before auto-deny on permission prompts |
| `pg_url` | str | `postgres://acp:acp_local_dev@postgres:5432/acp` | Postgres DSN for persistent session mapping |

### `pg_url` — Postgres-backed session mapping (B1)

The `pg_url` valve enables **persistent** `chat_id → session_id` mapping stored
in the `chat_session_mapping` table of the ACP Postgres database.

When set:
- Every Open WebUI chat is permanently linked to exactly one ACP session.
- Restarting Open WebUI or the Pipe does **not** lose the link — the next
  message in the same chat resumes the original ACP session with full history.
- Two browser tabs pointing at the same chat share the same ACP session
  (same context, same history).

When `pg_url` is empty or `psycopg2-binary` is not installed:
- Falls back to the v1.0.0 in-memory dict behaviour (mapping lost on restart).

The Postgres URL must be reachable from inside the Open WebUI container.
If Open WebUI and Postgres are in the same Docker Compose network, the default
`postgres://acp:acp_local_dev@postgres:5432/acp` works without changes.

### Choosing `daemon_url`

The correct URL depends on how you run the daemon relative to Open WebUI:

#### Case 1 — Daemon and Open WebUI in the same Docker Compose network

```
daemon_url = ws://claw-daemon:7800
```

The daemon service must be named `claw-daemon` in your `compose.yml`, or
substitute whatever service name you use.

#### Case 2 — Daemon on the host, Open WebUI in Docker (Docker Desktop / Mac / Windows)

```
daemon_url = ws://host.docker.internal:7800
```

`host.docker.internal` is resolved by Docker Desktop to the host machine.
Also set `extra_hosts: ["host.docker.internal:host-gateway"]` in the Open WebUI
service if you are on Linux with Docker Engine (not Docker Desktop).

#### Case 3 — Daemon on the WSL host, Open WebUI in Docker (native Linux/WSL2)

Find the host bridge IP from inside the container:

```bash
ip route | grep default   # shows something like: default via 172.17.0.1 dev eth0
```

Use that IP:

```
daemon_url = ws://172.17.0.1:7800
```

#### Case 4 — Everything on the same host (no Docker)

```
daemon_url = ws://localhost:7800
```

---

## Starting the Daemon

The claw daemon does NOT start automatically. You must launch it:

```bash
claw acp serve --addr 0.0.0.0:7800
```

- `0.0.0.0` — listen on all interfaces (required when Open WebUI runs in Docker).
- `7800` — the ACP WebSocket port (matches the default `daemon_url` in the Pipe).
- The session backend defaults to `file` (JSONL). Set `CLAW_SESSION_BACKEND=postgres`
  + `CLAW_PG_URL=...` to use the Postgres backend (Phase 1).

---

## How Sessions Work

The Pipe links each Open WebUI `chat_id` (UUID) to one ACP `session_id` via the
`chat_session_mapping` Postgres table.

### Mapping lifecycle

1. **New chat** — on the first message, the Pipe calls `session/new`, receives
   a `session_id`, and `INSERT`s a row into `chat_session_mapping`.
2. **Existing chat** — on subsequent messages, the Pipe does
   `SELECT session_id FROM chat_session_mapping WHERE chat_id = $1`, calls
   `session/resume` with the stored `session_id`, and updates `last_accessed_ms`.
3. **Session gone** (daemon restarted, session evicted) — `session/resume`
   returns `AcpError -32001`. The Pipe clears the stale in-memory entry and
   creates a new session, inserting a fresh mapping row.
4. **DB unreachable** — falls back to the in-memory dict silently. The chat
   continues working; history resumption across Open WebUI restarts is degraded
   until Postgres is available again.

### Resume across page reloads and Pipe restarts

Because the mapping is in Postgres, not in memory:
- **Page reload** — same chat_id is sent with the next message; the Pipe looks
  it up in the DB and resumes the ACP session. No history is lost.
- **Pipe restart** (OWUI container restart / new Pipe version deployed) — same
  behaviour. The DB survives the restart.
- **Daemon restart** — if the daemon loses the session (no Postgres backend),
  `session/resume` will fail with `-32001`. The Pipe gracefully creates a new
  session and updates the mapping. If the daemon uses the Postgres backend,
  sessions survive daemon restarts too.

### Bootstrap script (next step)

`scripts/owui-bootstrap-chats.py` pre-populates `chat_session_mapping` for
existing ACP sessions created via the CLI before this Pipe version was deployed.
Run it once after generating your Open WebUI API key:

```bash
python3 scripts/owui-bootstrap-chats.py \
    --owui-token <your-key> \
    --owui-url https://chat.homelab.local \
    --min-events 3 \
    --dry-run      # preview first, then remove --dry-run to apply
```

The script is idempotent: sessions that already have a mapping are skipped.

---

## Permission Prompts (Phase 4)

When the daemon needs user approval for a tool call (e.g. `Bash` in
`danger-full-access` mode), the Pipe displays a block like:

```
---
PERMISSION REQUEST

Tool: `Bash`
Command: `rm -rf /tmp/old_build`
Required mode: `danger-full-access` (current: `workspace-write`)
Reason: Bash requires danger-full-access in this context

Reply **allow** to permit this action, or **deny** to block it.
(Auto-deny in 300s if no response)
---
```

The user then types `allow` or `deny` as their next message. The Pipe detects
the keyword, sends `session/permission_response` to the daemon, and continues
streaming the turn result.

If the user sends an unrelated message while a permission prompt is pending,
the Pipe auto-denies the pending prompt and processes the new message normally.

### Error codes you may see

| Code | Meaning |
|------|---------|
| `-32001` | `UnknownSession` — session_id not found (daemon restarted) |
| `-32003` | `SessionBusy` — a turn is already in progress |
| `-32005` | `NoSuchPermissionRequest` — prompt already resolved or timed out |

---

## Streaming Fidelity

Current limitation: **turn-level streaming**, not token-level.

The daemon emits one `TextDelta` event per `ContentBlock::Text` (i.e. one chunk
per assistant text block). This means the text appears all at once at the end of
each content block rather than word-by-word.

True per-token streaming requires Phase 2.5 refactoring of the `ApiClient`
stream trait in the Rust daemon. See
`docs/acp-m3/blockers/phase-2-streaming.md` for details.

---

## Visible vs. Hidden SSE Content

| Event type | What user sees | Purpose |
|------------|---------------|---------|
| `TextDelta` | Full assistant text | Main response |
| `tool_use_start` | `[tool: Bash(ls /tmp...)]` inline | Tool call narration |
| `permission_request` | Full PERMISSION REQUEST block | User action required |
| `client_lagged` | Warning message | Broadcast buffer overflow |
| `turn_error` | `[ACP error N]: message` | Error narration |
| `thinking_delta` | — (hidden) | COMMENT in devtools |
| `tool_result` | — (hidden) | COMMENT in devtools |
| `usage` | — (hidden) | Token stats in devtools |
| `compaction` | — (hidden) | Session compaction notice |

Hidden items appear in browser devtools (Network tab → EventStream) prefixed
with `[COMMENT:...]` for debugging.

---

## Future Improvements

- **Per-token delta streaming** — after Phase 2.5 ApiClient refactor.
- **Richer permission UX** — structured UI elements instead of text blocks.
- **Docker Compose expose** — `claw-daemon` service with port 7800 exposed so
  Open WebUI can reach it without extra Valve configuration.
- **Model routing** — multiple daemon URLs keyed by model name.
