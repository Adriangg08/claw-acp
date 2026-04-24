//! Postgres-backed [`SessionBackend`] implementation.
//!
//! Uses `sqlx` 0.8 with the `postgres` feature. SQL is written using
//! `sqlx::query` (runtime-checked) rather than `sqlx::query!` macros because
//! compile-time checking requires `DATABASE_URL` at build time, which is not
//! available in CI. See DESIGN.md §2 and TASKS.md T1.4.
//!
//! Schema is created by `configs/postgres/init/30-acp.sql`.
//! Run migrations via `claw acp migrate` or call `run_migrations()` before
//! first use.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use runtime::session_control::{BackendError, SessionSummaryRow, StoredEvent, StoredEventType};
use runtime::{Session, SessionBackend};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

/// Returns the current wall-clock time in milliseconds since the UNIX epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Postgres-backed session storage.
///
/// Connect via [`PostgresSessionBackend::connect`] and then optionally call
/// [`PostgresSessionBackend::run_migrations`] to create the `acp` schema
/// (idempotent, uses `CREATE TABLE IF NOT EXISTS`).
///
/// Connection pool: max 10 connections per DESIGN.md §2 NF1.1.
pub struct PostgresSessionBackend {
    pool: PgPool,
}

impl PostgresSessionBackend {
    /// Connect to a Postgres instance and build the pool.
    ///
    /// `url` must be a valid libpq connection string or URL, e.g.
    /// `postgres://acp:acp_local_dev@localhost:5432/acp`.
    pub async fn connect(url: &str) -> Result<Self, BackendError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(|e| BackendError::Database(e.to_string()))?;

        info!("PostgresSessionBackend connected to database");
        Ok(Self { pool })
    }

    /// Run the DDL migrations (idempotent — uses `CREATE TABLE IF NOT EXISTS`).
    ///
    /// Creates the three ACP tables if they do not already exist.
    /// Safe to call on every daemon start-up.
    pub async fn run_migrations(&self) -> Result<(), BackendError> {
        // sessions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                session_id      TEXT        PRIMARY KEY,
                workspace_root  TEXT        NOT NULL,
                model           TEXT,
                created_at_ms   BIGINT      NOT NULL,
                updated_at_ms   BIGINT      NOT NULL,
                compaction      JSONB,
                fork            JSONB,
                version         INTEGER     NOT NULL DEFAULT 1,
                closed_at_ms    BIGINT
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS sessions_workspace_updated
                ON sessions (workspace_root, updated_at_ms DESC)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        // session_events table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_events (
                event_id        BIGSERIAL   PRIMARY KEY,
                session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
                seq             INTEGER     NOT NULL,
                event_type      TEXT        NOT NULL,
                role            TEXT,
                payload         JSONB       NOT NULL,
                created_at_ms   BIGINT      NOT NULL,
                UNIQUE (session_id, seq)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS session_events_session_seq
                ON session_events (session_id, seq)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        // session_clients table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS session_clients (
                client_id       TEXT        NOT NULL,
                session_id      TEXT        NOT NULL REFERENCES sessions (session_id),
                transport       TEXT        NOT NULL DEFAULT 'websocket',
                attached_at_ms  BIGINT      NOT NULL,
                last_seen_ms    BIGINT      NOT NULL,
                last_seq        INTEGER     NOT NULL DEFAULT 0,
                PRIMARY KEY (client_id, session_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS session_clients_session
                ON session_clients (session_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(e.to_string()))?;

        info!("PostgresSessionBackend migrations complete");
        Ok(())
    }

    /// Expose the underlying pool for the migration helper and tests.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Reconstruct a [`Session`] from Postgres metadata rows.
    ///
    /// We use the existing [`Session::load_from_path`] path by writing a
    /// minimal JSONL to a tempfile.  This avoids duplicating Session's internal
    /// deserialization logic while keeping the Postgres backend crate-private.
    fn reconstruct_session(
        session_id: &str,
        workspace_root: &str,
        model: Option<&str>,
        created_at_ms: i64,
        updated_at_ms: i64,
    ) -> Result<Session, BackendError> {
        // Write a minimal JSONL snapshot that Session::load_from_path can parse.
        let model_field = match model {
            Some(m) => format!(r#","model":"{}""#, m.replace('"', r#"\""#)),
            None => String::new(),
        };

        let jsonl = format!(
            r#"{{"type":"session_meta","version":1,"session_id":"{session_id}","created_at_ms":{created_at_ms},"updated_at_ms":{updated_at_ms},"workspace_root":"{workspace_root}"{model_field}}}"#,
        );

        // Write to tempfile then load.
        let tmp = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .map_err(BackendError::Io)?;

        std::fs::write(tmp.path(), jsonl.as_bytes()).map_err(BackendError::Io)?;

        let session = Session::load_from_path(tmp.path())
            .map_err(|e| BackendError::Serde(e.to_string()))?;

        Ok(session)
    }
}

#[async_trait]
impl SessionBackend for PostgresSessionBackend {
    async fn create_session(&self, session: &Session) -> Result<(), BackendError> {
        let workspace_root = session
            .workspace_root()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();

        let compaction_json: Option<serde_json::Value> = session.compaction.as_ref().map(|c| {
            serde_json::json!({
                "count": c.count,
                "removed_message_count": c.removed_message_count,
                "summary": c.summary,
            })
        });

        let fork_json: Option<serde_json::Value> = session.fork.as_ref().map(|f| {
            serde_json::json!({
                "parent_session_id": f.parent_session_id,
                "branch_name": f.branch_name,
            })
        });

        sqlx::query(
            r#"
            INSERT INTO sessions
                (session_id, workspace_root, model, created_at_ms, updated_at_ms,
                 compaction, fork, version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(&session.session_id)
        .bind(&workspace_root)
        .bind(&session.model)
        .bind(session.created_at_ms as i64)
        .bind(session.updated_at_ms as i64)
        .bind(compaction_json.map(sqlx::types::Json))
        .bind(fork_json.map(sqlx::types::Json))
        .bind(session.version as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("create_session failed: {e}")))?;

        debug!(session_id = %session.session_id, "session created in Postgres");
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> Result<Option<Session>, BackendError> {
        let row = sqlx::query(
            r#"
            SELECT session_id, workspace_root, model, created_at_ms, updated_at_ms
            FROM sessions
            WHERE session_id = $1
            "#,
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("load_session query failed: {e}")))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let sid: String = row.get("session_id");
        let workspace_root: String = row.get("workspace_root");
        let model: Option<String> = row.get("model");
        let created_at_ms: i64 = row.get("created_at_ms");
        let updated_at_ms: i64 = row.get("updated_at_ms");

        let session = Self::reconstruct_session(
            &sid,
            &workspace_root,
            model.as_deref(),
            created_at_ms,
            updated_at_ms,
        )?;

        debug!(session_id = %sid, "session loaded from Postgres");
        Ok(Some(session))
    }

    async fn append_event(
        &self,
        session_id: &str,
        seq: i32,
        event: &StoredEvent,
    ) -> Result<(), BackendError> {
        let ts = now_ms();

        sqlx::query(
            r#"
            INSERT INTO session_events (session_id, seq, event_type, role, payload, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (session_id, seq) DO NOTHING
            "#,
        )
        .bind(session_id)
        .bind(seq)
        .bind(event.event_type.as_str())
        .bind(&event.role)
        .bind(sqlx::types::Json(&event.payload))
        .bind(ts)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("append_event failed: {e}")))?;

        // Update the session's updated_at_ms timestamp.
        sqlx::query("UPDATE sessions SET updated_at_ms = $1 WHERE session_id = $2")
            .bind(ts)
            .bind(session_id)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Database(format!("append_event update_ts failed: {e}")))?;

        debug!(session_id, seq, "event appended to Postgres");
        Ok(())
    }

    async fn load_events(
        &self,
        session_id: &str,
        since_seq: i32,
    ) -> Result<Vec<StoredEvent>, BackendError> {
        let rows = sqlx::query(
            r#"
            SELECT seq, event_type, role, payload, created_at_ms
            FROM session_events
            WHERE session_id = $1 AND seq > $2
            ORDER BY seq ASC
            "#,
        )
        .bind(session_id)
        .bind(since_seq)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("load_events query failed: {e}")))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i32 = row.get("seq");
            let event_type_str: String = row.get("event_type");
            let event_type = event_type_str
                .parse::<StoredEventType>()
                .unwrap_or(StoredEventType::Meta);
            let role: Option<String> = row.get("role");
            let payload: serde_json::Value = row
                .try_get::<sqlx::types::Json<serde_json::Value>, _>("payload")
                .map(|j| j.0)
                .unwrap_or(serde_json::Value::Null);
            let created_at_ms: i64 = row.get("created_at_ms");

            events.push(StoredEvent {
                seq,
                event_type,
                role,
                payload,
                created_at_ms,
            });
        }

        debug!(session_id, count = events.len(), "events loaded from Postgres");
        Ok(events)
    }

    async fn close_session(&self, session_id: &str) -> Result<(), BackendError> {
        let ts = now_ms();
        let result = sqlx::query(
            "UPDATE sessions SET closed_at_ms = $1 WHERE session_id = $2 AND closed_at_ms IS NULL",
        )
        .bind(ts)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("close_session failed: {e}")))?;

        if result.rows_affected() == 0 {
            warn!(session_id, "close_session: session not found or already closed");
        } else {
            info!(session_id, "session closed in Postgres");
        }
        Ok(())
    }

    async fn list_open_sessions(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionSummaryRow>, BackendError> {
        let rows = sqlx::query(
            r#"
            SELECT s.session_id, s.workspace_root, s.model, s.created_at_ms, s.updated_at_ms,
                   (SELECT COUNT(*)::BIGINT FROM session_events e WHERE e.session_id = s.session_id)
                       AS message_count
            FROM sessions s
            WHERE s.workspace_root = $1 AND s.closed_at_ms IS NULL
            ORDER BY s.updated_at_ms DESC
            "#,
        )
        .bind(workspace_root)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("list_open_sessions query failed: {e}")))?;

        let summaries = rows
            .into_iter()
            .map(|row| SessionSummaryRow {
                session_id: row.get("session_id"),
                workspace_root: row.get("workspace_root"),
                model: row.get("model"),
                created_at_ms: row.get("created_at_ms"),
                updated_at_ms: row.get("updated_at_ms"),
                message_count: row.get("message_count"),
            })
            .collect();

        Ok(summaries)
    }

    async fn upsert_client_presence(
        &self,
        client_id: &str,
        session_id: &str,
        transport: &str,
    ) -> Result<(), BackendError> {
        let ts = now_ms();
        sqlx::query(
            r#"
            INSERT INTO session_clients (client_id, session_id, transport, attached_at_ms, last_seen_ms)
            VALUES ($1, $2, $3, $4, $4)
            ON CONFLICT (client_id, session_id)
            DO UPDATE SET last_seen_ms = EXCLUDED.last_seen_ms, transport = EXCLUDED.transport
            "#,
        )
        .bind(client_id)
        .bind(session_id)
        .bind(transport)
        .bind(ts)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("upsert_client_presence failed: {e}")))?;

        debug!(client_id, session_id, "client presence upserted");
        Ok(())
    }

    async fn remove_client_presence(
        &self,
        client_id: &str,
        session_id: &str,
    ) -> Result<(), BackendError> {
        sqlx::query(
            "DELETE FROM session_clients WHERE client_id = $1 AND session_id = $2",
        )
        .bind(client_id)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Database(format!("remove_client_presence failed: {e}")))?;

        debug!(client_id, session_id, "client presence removed");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stale client reaper (T1.9) — exposed so the daemon can spawn a periodic task
// ---------------------------------------------------------------------------

/// Delete client-presence rows where `last_seen_ms` is older than `ttl_ms`.
///
/// Called from the daemon startup reaper task (see DESIGN.md §1).
pub async fn reap_stale_clients(pool: &PgPool, ttl_ms: i64) -> Result<u64, BackendError> {
    let cutoff = now_ms() - ttl_ms;
    let result = sqlx::query("DELETE FROM session_clients WHERE last_seen_ms < $1")
        .bind(cutoff)
        .execute(pool)
        .await
        .map_err(|e| BackendError::Database(format!("reap_stale_clients failed: {e}")))?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(deleted, "reaped stale client presence rows");
    }
    Ok(deleted)
}
