-- 31-acp-chat-mapping.sql — idempotent DDL for the OWUI chat → ACP session mapping
--
-- Applied after 30-acp.sql (which creates the role, database, and core tables).
-- Run via: docker exec -i postgres psql -U sovereign -d acp < 31-acp-chat-mapping.sql
--
-- Design note on FK:
--   chat_session_mapping.session_id references ACP sessions by string ID.
--   A hard FK to sessions(session_id) is intentionally omitted because:
--   1. The bootstrap script inserts mappings for sessions that already exist,
--      so FK direction is fine — but the Pipe also creates mappings at first
--      message, immediately after session/new returns, so timing is safe.
--   2. However, ACP may evolve to support session IDs generated externally
--      (e.g. pre-allocated UUIDs) before they are persisted in sessions.
--      Keeping a soft reference avoids hard FK violations during such windows.
--   3. The sessions table lives in the same DB and can always be joined for
--      integrity checks; we trade hard enforcement for operational flexibility.
--
-- All DDL is idempotent (IF NOT EXISTS / IF NOT EXISTS on indexes).

\set ON_ERROR_STOP on

\c acp

SET ROLE acp;

-- ---------------------------------------------------------------------------
-- chat_session_mapping: one row per Open WebUI chat.
-- Maintains the stable link between an OWUI chat UUID and an ACP session_id
-- so the Pipe can resume the correct session across restarts and reloads.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS chat_session_mapping (
    chat_id           TEXT    PRIMARY KEY,
    -- Open WebUI chat UUID (body["metadata"]["chat_id"] in Pipe calls).

    session_id        TEXT    NOT NULL,
    -- ACP session_id returned by session/new. Soft reference — see design note
    -- above for rationale on omitting a hard FK to sessions(session_id).

    workspace_root    TEXT    NOT NULL,
    -- Denormalized for fast lookup without joining sessions; avoids an extra
    -- round-trip when the Pipe needs workspace_root to pass to session/resume.

    created_at_ms     BIGINT  NOT NULL
                              DEFAULT (extract(epoch from now()) * 1000)::BIGINT,
    -- Unix epoch milliseconds when this mapping was first created.

    last_accessed_ms  BIGINT  NOT NULL
                              DEFAULT (extract(epoch from now()) * 1000)::BIGINT
    -- Updated on every pipe() call that finds an existing mapping.
);

-- Index for reverse lookup: given a session_id, find all linked OWUI chats.
-- Useful for the bootstrap script and operational queries.
CREATE INDEX IF NOT EXISTS chat_session_mapping_session_id
    ON chat_session_mapping (session_id);

-- ---------------------------------------------------------------------------
-- Permissions: the `acp` role owns the table (SET ROLE acp above), so SELECT,
-- INSERT, UPDATE are already granted. Explicit GRANT makes intention clear and
-- survives if the table is ever recreated under a different ownership context.
-- ---------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE ON TABLE chat_session_mapping TO acp;
