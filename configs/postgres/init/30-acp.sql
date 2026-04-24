-- 30-acp.sql — idempotent init script for the ACP database
-- Placed in /docker-entrypoint-initdb.d/ via the compose stack.
-- Follows the pattern of 001-databases.sql (litellm, langfuse, openwebui).
--
-- Runs ONCE on first boot (empty data dir). All DDL uses IF NOT EXISTS so
-- re-running against an existing database is safe.
--
-- Role password: acp_local_dev (rotate via Infisical in production).

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- Role (idempotent via DO block)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'acp') THEN
    CREATE ROLE acp WITH LOGIN PASSWORD 'acp_local_dev';
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Database (direct CREATE; cannot use DO block — Postgres transaction restriction)
-- ---------------------------------------------------------------------------

SELECT 'CREATE DATABASE acp OWNER acp'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'acp') \gexec

GRANT ALL PRIVILEGES ON DATABASE acp TO acp;

-- ---------------------------------------------------------------------------
-- Schema — connect to the acp database and create tables
-- ---------------------------------------------------------------------------

\c acp

SET ROLE acp;

-- sessions: one row per ACP session.
-- closed_at_ms IS NULL  →  session is open / resumable.
-- closed_at_ms IS NOT NULL  →  session is closed (never deleted; append-only).
CREATE TABLE IF NOT EXISTS sessions (
    session_id      TEXT        PRIMARY KEY,
    workspace_root  TEXT        NOT NULL,
    model           TEXT,
    created_at_ms   BIGINT      NOT NULL,
    updated_at_ms   BIGINT      NOT NULL,
    compaction      JSONB,
    fork            JSONB,
    version         INTEGER     NOT NULL DEFAULT 1,
    closed_at_ms    BIGINT              -- NULL while session is open
);

CREATE INDEX IF NOT EXISTS sessions_workspace_updated
    ON sessions (workspace_root, updated_at_ms DESC);

-- session_events: append-only event log.
-- seq is per-session monotonic starting at 1 (generated application-side via
-- AtomicI32 on SessionSlot — see DESIGN.md §1 point A).
-- event_id is global ordering PK (BIGSERIAL may have gaps between sessions).
CREATE TABLE IF NOT EXISTS session_events (
    event_id        BIGSERIAL   PRIMARY KEY,
    session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
    seq             INTEGER     NOT NULL,
    event_type      TEXT        NOT NULL,
    -- 'message' | 'compaction' | 'permission_request' | 'permission_response' | 'meta'
    role            TEXT,
    -- 'user' | 'assistant' | 'tool' | 'system' | NULL
    payload         JSONB       NOT NULL,
    created_at_ms   BIGINT      NOT NULL,
    UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS session_events_session_seq
    ON session_events (session_id, seq);

-- session_clients: soft client-presence tracking.
-- Rows are upserted on attach and deleted on clean close or by the reaper task.
-- Reaper runs on daemon startup and every 5 minutes, deleting rows where
-- last_seen_ms < now - 300_000 (5-minute heartbeat window).
CREATE TABLE IF NOT EXISTS session_clients (
    client_id       TEXT        NOT NULL,
    session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
    transport       TEXT        NOT NULL DEFAULT 'websocket',
    -- 'websocket' | 'stdio'
    attached_at_ms  BIGINT      NOT NULL,
    last_seen_ms    BIGINT      NOT NULL,
    last_seq        INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (client_id, session_id)
);

CREATE INDEX IF NOT EXISTS session_clients_session
    ON session_clients (session_id);
