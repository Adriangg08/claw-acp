"""
claw_acp_pipe.py — Open WebUI Pipe Function for the claw ACP daemon.

Version: 2.0.0
Protocol: ACP JSON-RPC 2.0, version "0.2"
Daemon capabilities required: streaming=true, tools=true (Phase 2+)
                              permissions=true (Phase 4+)

────────────────────────────────────────────────────────────────
SESSION PERSISTENCE (B1)
────────────────────────────────────────────────────────────────
Each Open WebUI chat_id is permanently linked to one ACP session_id via the
`chat_session_mapping` table in Postgres.  On every pipe() call:

  • chat_id found in DB → session/resume the linked ACP session, update
    last_accessed_ms.  History and context are fully preserved across Open
    WebUI restarts and page reloads.

  • chat_id NOT in DB → session/new, then INSERT mapping row.

  • DB unreachable → fall back to in-memory dict (same behaviour as v1.0.0).
    The Pipe never crashes the chat due to a DB error.

────────────────────────────────────────────────────────────────
INSTALLATION
────────────────────────────────────────────────────────────────
1. Admin Panel → Functions → (+) Create Function
2. Paste the full content of this file.
3. Click Save, then toggle the function ON (enable icon).
4. Configure Valves (gear icon next to the function):
   - daemon_url: WebSocket address of the claw daemon
   - workspace_root: path the daemon uses for new sessions
   - model: model alias passed to session/new (e.g. "sonnet")
   - permission_timeout_seconds: seconds to wait for user allow/deny
   - pg_url: Postgres connection string for the ACP database

────────────────────────────────────────────────────────────────
MANUAL TEST PROCEDURE (no CI — Open WebUI cannot be spawned)
────────────────────────────────────────────────────────────────
Prereqs: daemon running via `claw acp serve --addr 0.0.0.0:7800`

T1 — Basic response:
  Send "hello". Expect a text reply in the chat UI.

T2 — Tool call narration:
  Send "list files in /tmp". Expect the response text to include
  a [tool] marker showing which tool was called.

T3 — Session resume:
  Send a second message in the same conversation. The daemon must
  resume the existing session (no new session/new call on the wire).

T4 — Daemon unreachable:
  Stop the daemon, send a message. Expect:
  "[ACP connect error] ..." within 30 s.

T5 — Permission prompt (requires PermissionMode=Prompt on daemon):
  Send a prompt that triggers a tool requiring approval (e.g. a
  Bash command in danger mode). Expect the Pipe to yield a
  PERMISSION REQUEST block in the response. Reply "allow" or "deny"
  in the next message.

T6 — Session resume across restart:
  Note the chat_id. Restart Open WebUI. Send a follow-up message in
  the same chat. Expect the daemon to resume the original ACP session
  (verify via daemon logs: "session/resume <session_id>").

────────────────────────────────────────────────────────────────
LIMITATIONS (current)
────────────────────────────────────────────────────────────────
- Turn-level streaming only: the daemon emits one TextDelta per
  ContentBlock (not per token). True delta streaming requires
  Phase 2.5 refactor of ApiClient (see docs/acp-m3/blockers/).
- Permission UX is text-based: user types "allow" or "deny" as
  their next message to resolve a pending permission prompt.
- No image/file forwarding.
- No multi-model routing (one daemon URL per Valve config).

────────────────────────────────────────────────────────────────
REQUIREMENTS
────────────────────────────────────────────────────────────────
Python stdlib + websockets (pre-installed in Open WebUI >= 0.3.x).
psycopg2-binary (pre-installed in Open WebUI >= 0.3.x).
If websockets is missing the Pipe returns a clear error in the UI.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import AsyncGenerator, Optional

try:
    import websockets  # type: ignore
    from websockets.exceptions import WebSocketException  # type: ignore
    _WEBSOCKETS_AVAILABLE = True
except ImportError:
    _WEBSOCKETS_AVAILABLE = False

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

from pydantic import BaseModel

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# ACP error
# ──────────────────────────────────────────────────────────────

class AcpError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        super().__init__(f"ACP error {code}: {message}")


# ──────────────────────────────────────────────────────────────
# DB helpers (synchronous — psycopg2, connection-per-call)
# ──────────────────────────────────────────────────────────────

def _db_lookup(pg_url: str, chat_id: str) -> Optional[tuple[str, str]]:
    """
    Look up a chat_id in chat_session_mapping.

    Returns (session_id, workspace_root) if found, None otherwise.
    Raises on connection/query errors — caller must catch.
    """
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT session_id, workspace_root "
                "FROM chat_session_mapping "
                "WHERE chat_id = %s",
                (chat_id,),
            )
            row = cur.fetchone()
    return (row[0], row[1]) if row else None


def _db_touch(pg_url: str, chat_id: str) -> None:
    """
    Update last_accessed_ms for an existing mapping row.
    Best-effort: caller should not surface errors to the user.
    """
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE chat_session_mapping "
                "SET last_accessed_ms = (extract(epoch from now()) * 1000)::BIGINT "
                "WHERE chat_id = %s",
                (chat_id,),
            )
        conn.commit()


def _db_insert(pg_url: str, chat_id: str, session_id: str, workspace_root: str) -> None:
    """
    Insert a new chat_id → session_id mapping.
    Uses INSERT … ON CONFLICT DO NOTHING so concurrent Pipe calls are safe.
    """
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_session_mapping "
                "(chat_id, session_id, workspace_root) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (chat_id) DO NOTHING",
                (chat_id, session_id, workspace_root),
            )
        conn.commit()


# ──────────────────────────────────────────────────────────────
# Pipe
# ──────────────────────────────────────────────────────────────

class Pipe:
    """
    Open WebUI Pipe Function: bridges chat completions to the claw ACP daemon.

    Each pipe() call opens a fresh WebSocket connection, runs one full turn,
    then closes.  Session continuity is maintained via the chat_session_mapping
    Postgres table (B1).  The in-memory dict (_sessions) is kept as a fallback
    when Postgres is unreachable.
    """

    class Valves(BaseModel):
        """User-configurable settings, editable in the Open WebUI Admin Panel."""

        daemon_url: str = "ws://claw-daemon:7800"
        """ACP daemon WebSocket URL.
        Docker Desktop / Docker on Mac/Windows: ws://host.docker.internal:7800
        Native Linux host (daemon on host, Open WebUI in docker):
          ws://<host-bridge-ip>:7800  (find with: ip route | grep default)
        Daemon and Open WebUI in same compose network: ws://claw-daemon:7800
        """

        workspace_root: str = "/workspace"
        """Workspace root passed to session/new. Must be a path the daemon
        can access (i.e. the daemon's filesystem, not Open WebUI's)."""

        model: str = "default"
        """Model alias passed to session/new params. The daemon resolves this
        against its configured model aliases (e.g. 'sonnet', 'opus')."""

        permission_timeout_seconds: int = 300
        """Seconds to wait for the user to respond to a permission prompt
        before auto-denying the tool call."""

        pg_url: str = "postgres://acp:acp_local_dev@postgres:5432/acp"
        """Postgres connection string for the ACP database.
        Used to persist the chat_id → session_id mapping across Open WebUI
        restarts.  Must be reachable from inside the Open WebUI container.
        Format: postgres://<user>:<password>@<host>:<port>/<dbname>
        Set to an empty string to disable Postgres persistence (falls back
        to in-memory mapping, same as Pipe v1.0.0 behaviour)."""

    def __init__(self) -> None:
        self.valves = self.Valves()
        # Fallback in-memory map used when Postgres is unavailable.
        # chat_id → session_id; survives for the lifetime of the OWUI process.
        self._sessions: dict[str, str] = {}
        # Pending permission request_id per chat.
        # Set when a permission_request event arrives; cleared when resolved.
        self._pending_permissions: dict[str, str] = {}  # chat_id → request_id

    # ──────────────────────────────────────────────────────────────────────────
    # Session resolution — Postgres-backed with in-memory fallback
    # ──────────────────────────────────────────────────────────────────────────

    def _resolve_session(self, chat_id: str) -> Optional[str]:
        """
        Return the ACP session_id for chat_id, or None if not found.

        Lookup order:
        1. Postgres chat_session_mapping (persistent, survives restarts).
        2. In-memory _sessions dict (fallback when PG is unavailable).

        Side-effect: updates last_accessed_ms in Postgres on a hit.
        """
        if chat_id and self.valves.pg_url and _PSYCOPG2_AVAILABLE:
            try:
                row = _db_lookup(self.valves.pg_url, chat_id)
                if row:
                    session_id, _workspace = row
                    try:
                        _db_touch(self.valves.pg_url, chat_id)
                    except Exception:  # noqa: BLE001
                        log.warning("acp_pipe: failed to update last_accessed_ms for chat_id=%s", chat_id)
                    # Sync in-memory cache so _resolve_permission can find it.
                    self._sessions[chat_id] = session_id
                    return session_id
            except Exception as exc:  # noqa: BLE001
                log.warning("acp_pipe: DB lookup failed, falling back to memory: %s", exc)

        # Fallback: in-memory dict.
        return self._sessions.get(chat_id)

    def _persist_session(self, chat_id: str, session_id: str, workspace_root: str) -> None:
        """
        Save a new chat_id → session_id mapping.

        Writes to Postgres (idempotent INSERT … ON CONFLICT DO NOTHING) and
        also updates the in-memory cache.  DB errors are logged but not raised
        so a DB outage never crashes the chat.
        """
        self._sessions[chat_id] = session_id
        if self.valves.pg_url and _PSYCOPG2_AVAILABLE:
            try:
                _db_insert(self.valves.pg_url, chat_id, session_id, workspace_root)
            except Exception as exc:  # noqa: BLE001
                log.warning("acp_pipe: failed to persist mapping chat_id=%s → %s: %s", chat_id, session_id, exc)

    # ──────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────────

    async def pipe(self, body: dict) -> AsyncGenerator[str, None]:
        """
        Main entry point called by Open WebUI for each user message.

        Receives OpenAI chat completions body, converts it to ACP session/prompt,
        and yields SSE chunks in OpenAI streaming format.

        chat_id extraction:
          Open WebUI injects the chat UUID into body["metadata"]["chat_id"].
          Some older versions also expose it at body["chat_id"]; we check both.
        """
        if not _WEBSOCKETS_AVAILABLE:
            yield self._sse_chunk(
                "[ACP Pipe error] 'websockets' package not found in Open WebUI container. "
                "Install it or upgrade Open WebUI to >= 0.3.x."
            )
            return

        metadata: dict = body.get("metadata") or {}
        chat_id: Optional[str] = (
            metadata.get("chat_id")
            or body.get("chat_id")
            or None
        )
        model: str = body.get("model", self.valves.model) or self.valves.model
        messages: list = body.get("messages", [])

        if not messages:
            yield self._sse_chunk("[ACP Pipe error] no messages in request body")
            return

        user_text: str = messages[-1].get("content", "")

        # ── Permission resolution shortcut ──────────────────────────────────
        # If the user's message is "allow" or "deny" and there is a pending
        # permission prompt for this chat, resolve it via a dedicated WS call
        # rather than starting a new prompt turn.
        if chat_id and chat_id in self._pending_permissions:
            normalized = user_text.strip().lower()
            if normalized in ("allow", "deny"):
                async for chunk in self._resolve_permission(
                    chat_id, normalized, user_text
                ):
                    yield chunk
                return
            else:
                # User sent an unrelated message while permission is pending.
                # Auto-deny to unblock the daemon, then proceed with new prompt.
                async for chunk in self._resolve_permission(
                    chat_id, "deny", "auto-denied: user sent unrelated message"
                ):
                    yield chunk
                # Fall through to process the actual user message.

        # ── Normal turn ─────────────────────────────────────────────────────
        async for chunk in self._run_turn(chat_id, model, user_text):
            yield chunk

    # ──────────────────────────────────────────────────────────────────────────
    # Internal: resolve a pending permission prompt
    # ──────────────────────────────────────────────────────────────────────────

    async def _resolve_permission(
        self,
        chat_id: str,
        decision: str,
        reason: str,
    ) -> AsyncGenerator[str, None]:
        """Send session/permission_response for the pending request."""
        request_id = self._pending_permissions.pop(chat_id, None)
        if not request_id:
            return

        session_id = self._sessions.get(chat_id)
        if not session_id:
            yield self._sse_chunk(
                f"[ACP Pipe warning] no active session for chat; "
                f"permission {decision} dropped"
            )
            return

        try:
            async with websockets.connect(
                self.valves.daemon_url,
                open_timeout=30.0,
            ) as ws:
                await self._rpc(ws, "initialize", {
                    "protocol_version": "0.2",
                    "client_info": {"name": "openwebui-pipe", "version": "2.0"},
                })
                await self._rpc(ws, "session/resume", {"session_id": session_id})
                await self._rpc(ws, "session/permission_response", {
                    "session_id": session_id,
                    "request_id": request_id,
                    "decision": decision,
                    "reason": reason,
                })
                yield self._sse_chunk(
                    f"\n\n[Permission {decision.upper()}] Tool call {decision}d."
                )
        except AcpError as exc:
            # -32005 = NoSuchPermissionRequest (already resolved or timed out)
            if exc.code == -32005:
                yield self._sse_chunk(
                    "\n\n[Permission already resolved — daemon timed out or another client responded]"
                )
            else:
                yield self._sse_chunk(f"\n\n[ACP error {exc.code}]: {exc}")
        except Exception as exc:  # noqa: BLE001
            yield self._sse_chunk(f"\n\n[ACP permission error]: {exc}")

    # ──────────────────────────────────────────────────────────────────────────
    # Internal: run a full prompt turn
    # ──────────────────────────────────────────────────────────────────────────

    async def _run_turn(
        self,
        chat_id: Optional[str],
        model: str,
        user_text: str,
    ) -> AsyncGenerator[str, None]:
        """Open WS, initialize, attach/create session, send prompt, stream events."""
        try:
            async with websockets.connect(
                self.valves.daemon_url,
                open_timeout=30.0,
            ) as ws:
                # 1. Handshake
                await self._rpc(ws, "initialize", {
                    "protocol_version": "0.2",
                    "client_info": {"name": "openwebui-pipe", "version": "2.0"},
                })

                # 2. Attach to session (resume or new)
                session_id: Optional[str] = (
                    self._resolve_session(chat_id) if chat_id else None
                )
                if session_id:
                    try:
                        await self._rpc(ws, "session/resume", {"session_id": session_id})
                    except AcpError:
                        # Session gone (daemon restarted, session closed, etc.)
                        # Clear stale mapping so we create a fresh one below.
                        self._sessions.pop(chat_id, None)
                        session_id = None

                if not session_id:
                    result = await self._rpc(ws, "session/new", {
                        "workspace_root": self.valves.workspace_root,
                        "model": model,
                    })
                    session_id = result["session_id"]
                    if chat_id:
                        self._persist_session(chat_id, session_id, self.valves.workspace_root)

                # 3. Send prompt
                turn_id = str(uuid.uuid4())
                await self._rpc(ws, "session/prompt", {
                    "session_id": session_id,
                    "text": user_text,
                    "turn_id": turn_id,
                })

                # 4. Stream session/update notifications until TurnEnd or TurnError
                turn_timeout = self.valves.permission_timeout_seconds + 60
                deadline = asyncio.get_event_loop().time() + turn_timeout
                async for chunk in self._stream_turn(ws, session_id, chat_id):
                    yield chunk
                    if asyncio.get_event_loop().time() > deadline:
                        yield self._sse_chunk(
                            f"\n\n[ACP Pipe error] Turn exceeded {turn_timeout}s. "
                            "Check daemon health."
                        )
                        break

        except AcpError as exc:
            yield self._sse_chunk(f"[ACP error {exc.code}]: {exc}")
        except (OSError, WebSocketException if _WEBSOCKETS_AVAILABLE else OSError) as exc:
            yield self._sse_chunk(
                f"[ACP connect error] Cannot reach daemon at {self.valves.daemon_url}: {exc}. "
                "Check daemon_url Valve and ensure `claw acp serve --addr ...` is running."
            )
        except Exception as exc:  # noqa: BLE001
            yield self._sse_chunk(f"[ACP Pipe unexpected error]: {exc}")

        yield "data: [DONE]\n\n"

    async def _stream_turn(
        self,
        ws,
        session_id: str,
        chat_id: Optional[str],
    ) -> AsyncGenerator[str, None]:
        """
        Consume session/update notifications from the WebSocket until TurnEnd.

        SessionEvent type mapping:
          text_delta        → OpenAI delta chunk (visible text)
          thinking_delta    → COMMENT (reasoning, not displayed by default)
          turn_start        → COMMENT (turn started marker)
          tool_use_start    → inline narration text + COMMENT
          tool_result       → COMMENT
          permission_request→ inline narration + stores pending request_id
          usage             → COMMENT (token stats)
          compaction        → COMMENT
          client_lagged     → error text
          turn_end          → [DONE] signal, break
          turn_error        → error text, break
        """
        async for raw in ws:
            msg = json.loads(raw)

            # Skip anything that is not a notification (no "method" field
            # means it's an RPC response; _rpc() already consumed those).
            if "method" not in msg:
                continue
            if msg["method"] != "session/update":
                # session/permission_request would appear here if the daemon
                # ever sends it as a top-level notification instead of via
                # session/update. Handle defensively.
                if msg["method"] == "session/permission_request":
                    params = msg.get("params", {})
                    async for chunk in self._handle_permission_request(
                        params, session_id, chat_id
                    ):
                        yield chunk
                continue

            params = msg.get("params", {})
            event = params.get("event", {})
            etype = event.get("type", "")

            if etype == "text_delta":
                yield self._sse_chunk(event.get("text", ""))

            elif etype == "thinking_delta":
                yield self._sse_comment(f"thinking: {event.get('text', '')[:120]}")

            elif etype == "turn_start":
                yield self._sse_comment(f"turn_start: {event.get('turn_id', '')}")

            elif etype == "tool_use_start":
                tool_name = event.get("tool_name", "?")
                raw_input = event.get("input", "")
                # Show a brief narration inline so the user sees the tool call.
                preview = raw_input[:80] + ("..." if len(raw_input) > 80 else "")
                yield self._sse_chunk(f"\n\n[tool: {tool_name}({preview})]\n\n")
                yield self._sse_comment(
                    f"TOOL_CALL:{json.dumps({'tool': tool_name, 'input': raw_input[:512]})}"
                )

            elif etype == "tool_result":
                tool_name = event.get("tool_name", "?")
                output = event.get("output", "")
                is_error = event.get("is_error", False)
                status = "error" if is_error else "ok"
                yield self._sse_comment(
                    f"TOOL_RESULT:{json.dumps({'tool': tool_name, 'status': status, 'output': output[:256]})}"
                )

            elif etype == "permission_request":
                async for chunk in self._handle_permission_request(
                    event, session_id, chat_id
                ):
                    yield chunk

            elif etype == "usage":
                yield self._sse_comment(
                    f"usage: in={event.get('input_tokens')} "
                    f"out={event.get('output_tokens')} "
                    f"cache_read={event.get('cache_read_input_tokens')} "
                    f"cache_create={event.get('cache_creation_input_tokens')}"
                )

            elif etype == "compaction":
                summary = event.get("summary", "")
                removed = event.get("removed_message_count", 0)
                yield self._sse_comment(
                    f"compaction: removed {removed} messages — {summary[:80]}"
                )

            elif etype == "client_lagged":
                dropped = event.get("dropped", 0)
                yield self._sse_chunk(
                    f"\n\n[ACP Pipe warning] Broadcast buffer overflow — "
                    f"{dropped} events dropped. Response may be incomplete."
                )

            elif etype == "turn_end":
                # TurnEnd signals that the assistant message is complete.
                break

            elif etype == "turn_error":
                code = event.get("code", 0)
                message = event.get("message", "unknown error")
                yield self._sse_chunk(f"\n\n[ACP error {code}]: {message}")
                break

            else:
                # Unknown future event type — log as comment, do not crash.
                yield self._sse_comment(f"unknown_event: {etype}")

    def _handle_permission_request(
        self,
        event: dict,
        session_id: str,
        chat_id: Optional[str],
    ) -> AsyncGenerator[str, None]:
        """
        Handle an incoming permission_request event.

        Yields a visible narration block + stores the request_id so the next
        user message of "allow" or "deny" can resolve it.
        """
        async def _gen():
            request_id = event.get("request_id", "")
            tool_name = event.get("tool_name", "?")
            input_preview = event.get("input_preview", "")
            required_mode = event.get("required_mode", "")
            current_mode = event.get("current_mode", "")
            reason = event.get("reason") or ""

            # Store pending request so next user message can resolve it.
            if chat_id:
                self._pending_permissions[chat_id] = request_id

            # Yield a PERMISSION REQUEST block as visible text.
            block = (
                f"\n\n---\n"
                f"**PERMISSION REQUEST**\n\n"
                f"Tool: `{tool_name}`\n"
                f"Command: `{input_preview}`\n"
                f"Required mode: `{required_mode}` (current: `{current_mode}`)\n"
            )
            if reason:
                block += f"Reason: {reason}\n"
            block += (
                f"\nReply **allow** to permit this action, or **deny** to block it.\n"
                f"(Auto-deny in {self.valves.permission_timeout_seconds}s if no response)\n"
                f"---\n\n"
            )
            yield self._sse_chunk(block)
            yield self._sse_comment(
                f"PERMISSION_REQUEST:{json.dumps(event)}"
            )

        return _gen()

    # ──────────────────────────────────────────────────────────────────────────
    # Internal: JSON-RPC helper
    # ──────────────────────────────────────────────────────────────────────────

    async def _rpc(self, ws, method: str, params: dict) -> dict:
        """
        Send a JSON-RPC request and return the result dict.

        Skips notifications (no `id` field) while waiting for the matching
        response. Raises AcpError on JSON-RPC error responses.
        """
        req_id = str(uuid.uuid4())
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        }))
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("id") != req_id:
                # Not our response — could be a notification or another client's
                # response. Skip it; the dispatch loop handles notifications.
                continue
            if "error" in msg:
                err = msg["error"]
                raise AcpError(err.get("code", -32603), err.get("message", "unknown"))
            return msg.get("result") or {}
        # WebSocket closed before response arrived.
        raise AcpError(-32603, f"connection closed while waiting for '{method}' response")

    # ──────────────────────────────────────────────────────────────────────────
    # SSE helpers
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sse_chunk(text: str) -> str:
        """Yield an OpenAI-format SSE data chunk with visible text content."""
        payload = json.dumps({"choices": [{"delta": {"content": text}}]})
        return f"data: {payload}\n\n"

    @staticmethod
    def _sse_comment(text: str) -> str:
        """Yield a non-visible SSE comment (visible in browser devtools, not in UI)."""
        return f"data: [COMMENT:{text}]\n\n"
