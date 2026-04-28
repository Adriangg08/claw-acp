#!/usr/bin/env python3
"""
owui-bootstrap-chats.py — Backfill Open WebUI chats for existing ACP sessions.

PURPOSE
-------
When you have existing ACP sessions in Postgres (created via the CLI before the
Pipe had Postgres-backed session mapping) this script creates a matching OWUI
chat for each session and inserts a row into chat_session_mapping so the Pipe
can resume the session from the chat UI.

The script is idempotent: if a session already has a mapping row in
chat_session_mapping, it is skipped (no duplicate OWUI chat is created).

DO NOT RUN THIS SCRIPT until you have your Open WebUI API key.
Use --dry-run to preview what would happen without making any changes.

USAGE
-----
    python3 scripts/owui-bootstrap-chats.py \\
        --owui-token <api-key> \\
        --owui-url https://chat.homelab.local \\
        [--pg-url postgres://acp:acp_local_dev@localhost:5432/acp] \\
        [--min-events 3] \\
        [--dry-run]

REQUIREMENTS
------------
    pip install psycopg2-binary requests

ARGUMENTS
---------
    --owui-token     (required) Open WebUI API key.  Generate in OWUI:
                     Settings → Account → API Keys → Create new secret key.
    --owui-url       (required) Base URL of your Open WebUI instance
                     (no trailing slash).
    --pg-url         Postgres DSN for the ACP database.
                     Default: postgres://acp:acp_local_dev@localhost:5432/acp
    --min-events     Minimum number of session_events a session must have to be
                     bootstrapped.  Sessions with fewer events are trivial
                     (e.g. abandoned on first message) and are skipped.
                     Default: 3
    --dry-run        Preview what would happen without writing anything.

OUTPUT (per session)
--------------------
    [SKIP]    session <id> already has mapping (chat_id=<uuid>)
    [SKIP]    session <id> has only <n> events (< min_events) — skipped
    [DRY-RUN] would create OWUI chat for session <id> (workspace=..., events=..., title=...)
    [CREATE]  created OWUI chat <chat_id> for ACP session <id>
              (workspace=<root>, events=<n>, title=<title>)
    [ERROR]   session <id>: <reason>
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_PG_URL = "postgres://acp:acp_local_dev@localhost:5432/acp"
DEFAULT_MIN_EVENTS = 3


# ──────────────────────────────────────────────────────────────────────────────
# Postgres helpers
# ──────────────────────────────────────────────────────────────────────────────

def fetch_sessions(conn, min_events: int) -> list[dict]:
    """
    Return all ACP sessions that have at least min_events events, annotated with:
      - session_id, workspace_root, model, created_at_ms
      - event_count  (total events in session_events)
      - title        (derived from first user message, fallback to session_id[:8])
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                s.session_id,
                s.workspace_root,
                s.model,
                s.created_at_ms,
                COUNT(e.event_id)   AS event_count,
                (
                    SELECT COALESCE(
                        e2.payload->>'text',
                        e2.payload->'blocks'->0->>'text',
                        e2.payload->'content'->0->>'text'
                    )
                    FROM   session_events e2
                    WHERE  e2.session_id = s.session_id
                      AND  e2.role       = 'user'
                    ORDER BY e2.seq
                    LIMIT 1
                )                   AS first_user_message
            FROM   sessions s
            JOIN   session_events e USING (session_id)
            GROUP  BY s.session_id
            HAVING COUNT(e.event_id) >= %s
            ORDER  BY s.created_at_ms DESC
            """,
            (min_events,),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_existing_mappings(conn) -> dict[str, str]:
    """Return {session_id: chat_id} for all rows in chat_session_mapping."""
    with conn.cursor() as cur:
        cur.execute("SELECT session_id, chat_id FROM chat_session_mapping")
        return {row[0]: row[1] for row in cur.fetchall()}


def insert_mapping(conn, chat_id: str, session_id: str, workspace_root: str) -> None:
    """Insert a mapping row.  Uses ON CONFLICT DO NOTHING for safety."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO chat_session_mapping (chat_id, session_id, workspace_root)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id) DO NOTHING
            """,
            (chat_id, session_id, workspace_root),
        )
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Open WebUI helpers
# ──────────────────────────────────────────────────────────────────────────────

def _owui_headers(token: str, host_header: Optional[str] = None) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if host_header:
        headers["Host"] = host_header
    return headers


def derive_title(session: dict) -> str:
    """Derive a chat title from the session's first user message, or workspace + date."""
    msg: Optional[str] = session.get("first_user_message")
    if msg:
        msg_clean = msg.strip().replace("\n", " ")
        return msg_clean[:80] + ("..." if len(msg_clean) > 80 else "")
    # Fallback: [CLAW] <last-segment-of-workspace> · <YYYY-MM-DD>
    import datetime as _dt
    ws = (session.get("workspace_root") or "").rstrip("/")
    leaf = ws.rsplit("/", 1)[-1] if ws else "unknown"
    created = session.get("created_at_ms") or 0
    date = _dt.datetime.fromtimestamp(created / 1000).strftime("%Y-%m-%d") if created else "?"
    return f"[CLAW] {leaf} · {date}"


def create_owui_chat(owui_url: str, token: str, title: str, host_header: Optional[str] = None) -> str:
    """
    Create a new chat in Open WebUI via POST /api/v1/chats/new.

    Returns the new chat_id (UUID string).
    Raises requests.HTTPError on non-2xx responses.
    """
    url = f"{owui_url.rstrip('/')}/api/v1/chats/new"
    payload = {
        "chat": {
            "title": title,
            "models": ["claw"],   # must match the Pipe function name in OWUI
            "messages": [],
            "history": {"messages": {}, "currentId": None},
            "tags": [],
            "params": {},
        }
    }
    resp = requests.post(url, headers=_owui_headers(token, host_header), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    chat_id: Optional[str] = data.get("id")
    if not chat_id:
        raise ValueError(f"OWUI /api/v1/chats/new returned no 'id': {data!r}")
    return chat_id


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill Open WebUI chats for existing ACP sessions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--owui-token", required=True, metavar="KEY",
                   help="Open WebUI API key (Settings → Account → API Keys)")
    p.add_argument("--owui-url", required=True, metavar="URL",
                   help="Base URL of your Open WebUI instance (no trailing slash)")
    p.add_argument("--pg-url", default=DEFAULT_PG_URL, metavar="DSN",
                   help=f"Postgres DSN for the ACP database (default: {DEFAULT_PG_URL})")
    p.add_argument("--min-events", type=int, default=DEFAULT_MIN_EVENTS, metavar="N",
                   help=f"Skip sessions with fewer than N events (default: {DEFAULT_MIN_EVENTS})")
    p.add_argument("--dry-run", action="store_true",
                   help="Preview actions without writing anything")
    p.add_argument("--host-header", metavar="HOST", default=None,
                   help="Override the Host header (needed when --owui-url is "
                        "an internal docker name but OWUI checks the public host)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    log.info("Connecting to Postgres at %s", args.pg_url)
    try:
        conn = psycopg2.connect(args.pg_url)
    except Exception as exc:
        log.error("Cannot connect to Postgres: %s", exc)
        return 1

    try:
        sessions = fetch_sessions(conn, args.min_events)
        existing = fetch_existing_mappings(conn)
    except Exception as exc:
        log.error("Failed to read from Postgres: %s", exc)
        conn.close()
        return 1

    log.info(
        "Found %d sessions with >= %d events; %d already have mappings",
        len(sessions), args.min_events, len(existing),
    )

    created = 0
    skipped = 0
    errors = 0

    for session in sessions:
        sid = session["session_id"]
        workspace = session["workspace_root"]
        n_events = int(session["event_count"])
        title = derive_title(session)

        # Idempotency: skip if mapping already exists.
        if sid in existing:
            log.info("[SKIP]   session %s already has mapping (chat_id=%s)", sid, existing[sid])
            skipped += 1
            continue

        if args.dry_run:
            log.info(
                "[DRY-RUN] would create OWUI chat for session %s "
                "(workspace=%s, events=%d, title=%r)",
                sid, workspace, n_events, title,
            )
            created += 1
            continue

        try:
            chat_id = create_owui_chat(args.owui_url, args.owui_token, title, args.host_header)
            insert_mapping(conn, chat_id, sid, workspace)
            log.info(
                "[CREATE]  created OWUI chat %s for ACP session %s "
                "(workspace=%s, events=%d, title=%r)",
                chat_id, sid, workspace, n_events, title,
            )
            created += 1
        except Exception as exc:  # noqa: BLE001
            log.error("[ERROR]   session %s: %s", sid, exc)
            errors += 1

    conn.close()

    action = "would create" if args.dry_run else "created"
    log.info(
        "Done. %s %d chat(s), skipped %d, %d error(s).",
        action, created, skipped, errors,
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
