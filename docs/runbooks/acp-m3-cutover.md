# ACP M3+ Cutover Runbook

**Branch**: `feature/acp-daemon-m3`
**Date authored**: 2026-04-24
**Commits**: 10 (7defd80…f0e49e0) — 10,119 lines across 28 files
**Tests**: 551 passing (acp + runtime crates)

This runbook is the authoritative procedure for merging the ACP M3+ branch into
`feature/acp`, deploying the ACP daemon, and enabling the Open WebUI Pipe. Every
step includes exact commands, expected output, and a rollback. Read it top to
bottom before starting. The procedure is reversible at every step.

---

## Pre-flight Checklist

Complete every item before issuing the first command. If any item is not true,
stop and fix it first.

- [ ] **No active claw sessions on the other project.** You told me to leave you
  alone until you're done — finish your current claw session first. The merge
  does not affect the running binary, but rebuilding and installing a new binary
  mid-session will confuse in-flight conversations.

- [ ] **Postgres container is running.**
  ```
  docker ps --filter name=postgres --format '{{.Names}} {{.Status}}'
  ```
  Expected: a line containing `postgres` and `Up`.

- [ ] **`compose/fase-0/.env` is accessible and has `POSTGRES_PASSWORD`.**
  The superuser password is needed only if you want to re-run the init SQL
  manually as `postgres`. The ACP role's own password (`acp_local_dev`) is
  hard-coded in `30-acp.sql` and does not require the env file at runtime.

- [ ] **Working tree is clean on `feature/acp` in `claw-code-fork`.**
  ```
  cd /home/adr/development/sovereign-agents/claw-code-fork
  git status
  ```
  Expected: `nothing to commit, working tree clean`.

- [ ] **You are on the right worktree for the merge source.**
  The ACP M3 work lives at `/home/adr/development/sovereign-agents-acp-m3`
  (git worktree), branch `feature/acp-daemon-m3`. This is NOT the same path
  as `claw-code-fork`.

---

## Step-by-Step Procedure

### Step 1 — Verify worktree branch and commit history

```bash
cd /home/adr/development/sovereign-agents-acp-m3
git log --oneline -10
```

Expected output (oldest at bottom):

```
f0e49e0 docs(acp-m3): M5 happy interop NO-GO deferral + ADR-012
bf0514b feat(acp): wire real ConversationRuntime into TurnDriver via TurnExecutorFactory
d31afaa feat(acp/m3): Open WebUI Pipe function for ACP daemon (B5)
dfd1205 test(acp/m4): integration tests for permission prompt broadcast (B7)
d84d247 feat(acp/m4): permission slot + prompter + handler + dispatch (B6)
95d86bf feat(acp/m3): session/prompt handler + broadcast relay + catch-up replay (B4)
28b1e11 feat(acp/m3): SessionEvent enum + SessionSlot fan-out fields + TurnDriver (B3)
aac4cf0 docs(acp/m3): add SPEC, DESIGN, TASKS docs for ACP M3 implementation
50e05da feat(acp/m3): wire CLAW_SESSION_BACKEND env var + backend into session handler (B2)
7defd80 feat(acp/m3): add SessionBackend trait + FileSessionBackend + PostgresSessionBackend (B1)
```

If the output matches, proceed. If not, you are on the wrong branch or worktree.

**Rollback**: N/A — read-only step.

---

### Step 2 — Create the ACP Postgres database and role

The SQL init script at `configs/postgres/init/30-acp.sql` is idempotent
(`IF NOT EXISTS` everywhere). It creates:

- Role `acp` with password `acp_local_dev`
- Database `acp` owned by `acp`
- Three tables: `sessions`, `session_events`, `session_clients`

**Option A — Run directly via docker exec (recommended)**

```bash
docker exec -i postgres psql -U postgres \
  < /home/adr/development/sovereign-agents-acp-m3/configs/postgres/init/30-acp.sql
```

Expected output (condensed):

```
DO
CREATE DATABASE
GRANT
SET
CREATE TABLE
CREATE INDEX
CREATE TABLE
CREATE INDEX
CREATE TABLE
CREATE INDEX
```

If you see `NOTICE: role "acp" already exists` or
`NOTICE: database "acp" already exists` — those are fine; the script handles
them with the `IF NOT EXISTS` path.

**Verify**

```bash
docker exec postgres psql -U postgres -c "\l" | grep acp
```

Expected: `acp | acp | UTF8 | ...`

**Option B — Copy the file into the container first**

If the bind-mount is not in place:

```bash
docker cp /home/adr/development/sovereign-agents-acp-m3/configs/postgres/init/30-acp.sql \
  postgres:/tmp/30-acp.sql
docker exec postgres psql -U postgres -f /tmp/30-acp.sql
```

**Rollback**: Drop the `acp` database and role. This is isolated — it does not
touch `litellm`, `langfuse`, or `openwebui` databases.

```bash
docker exec postgres psql -U postgres -c "DROP DATABASE IF EXISTS acp;"
docker exec postgres psql -U postgres -c "DROP ROLE IF EXISTS acp;"
```

---

### Step 3 — Merge `feature/acp-daemon-m3` into `feature/acp`

```bash
cd /home/adr/development/sovereign-agents/claw-code-fork
git fetch --all
git checkout feature/acp
git merge --ff-only feature/acp-daemon-m3
```

The `--ff-only` flag is intentional. The worktree branch was started from the tip
of `feature/acp`, so the merge should always be a fast-forward. If it fails with
`fatal: Not possible to fast-forward, aborting`:

- The `feature/acp` branch has diverged (someone added commits to it after the
  worktree was created).
- Recovery: `git rebase feature/acp feature/acp-daemon-m3` and resolve any
  conflicts, then retry the merge. Alternatively, cherry-pick the 10 commits
  individually: `git cherry-pick 7defd80^..f0e49e0`.

**Expected output on success**:

```
Updating <base-sha>..f0e49e0
Fast-forward
 configs/postgres/init/30-acp.sql  |   95 ++
 ...
 28 files changed, 10119 insertions(+), 143 deletions(-)
```

**Rollback**: Reset `feature/acp` to the pre-merge SHA.

```bash
# Find the pre-merge commit (ORIG_HEAD is set automatically by git merge)
git reset --hard ORIG_HEAD
```

If `ORIG_HEAD` is gone, use `git reflog` to find the SHA before the merge.

---

### Step 4 — Rebuild and install the claw binary

The binary must be rebuilt from the merged `feature/acp` branch in `claw-code-fork`,
not from the worktree.

```bash
cd /home/adr/development/sovereign-agents/claw-code-fork/rust
cargo build --release -p rusty-claude-cli
```

Expected: `Finished release profile [optimized] target(s) in N s`

**Install option A — cargo install (installs to `~/.cargo/bin/claw`)**

```bash
cargo install --path crates/rusty-claude-cli --force
```

Check that the new version is active:

```bash
claw --version
```

Expected: the git SHA shown should be `f0e49e0` or the merge commit SHA.

**Install option B — copy binary manually**

```bash
cp /home/adr/development/sovereign-agents/claw-code-fork/rust/target/release/claw \
   ~/.local/bin/claw
```

(Adjust destination to wherever your `PATH` picks up `claw`. Current install:
`/home/adr/.local/bin/claw`.)

**Verify**:

```bash
claw --version
# Should show git SHA matching f0e49e0 or later
claw acp --help 2>&1 | head -5
# Should show ACP subcommand help
```

**Rollback**: Reinstall the old binary from its prior release build, or revert
`feature/acp` (Step 3 rollback) and rebuild.

---

### Step 5 — (Optional) Migrate existing JSONL sessions to Postgres

This step is reversible and non-destructive. JSONL files are **never deleted** by
the migration — they remain at their original paths as backup.

**What it does**: Reads all JSONL session files from the session store root,
converts each `ConversationMessage` to a `session_events` row and each
`SessionCompaction` to a compaction row. The migration is idempotent
(`ON CONFLICT DO NOTHING`), so re-running is safe.

**Command**:

```bash
claw acp migrate --pg-url postgresql://acp:acp_local_dev@localhost:5432/acp
```

Alternatively, if you have the env vars set:

```bash
export CLAW_PG_URL=postgresql://acp:acp_local_dev@localhost:5432/acp
claw acp migrate
```

For more details: `claw acp migrate --help`

If migration is not critical right now, skip this step. The Postgres backend
will start fresh and sessions will accumulate from first use.

**How long it takes**: Typically under 1 second per session for local JSONL
files. Hundreds of sessions complete in a few seconds.

**Rollback**: Nothing to do. JSONL files are untouched. To discard migrated
Postgres data: `docker exec postgres psql -U postgres -c "DELETE FROM acp.session_events; DELETE FROM acp.sessions;"`.

---

### Step 6 — Start the ACP daemon

The daemon is `claw acp serve`. It listens on a WebSocket port and speaks
ACP JSON-RPC 2.0 to connected clients.

**Configure Postgres backend (recommended for multi-client)**:

```bash
export CLAW_SESSION_BACKEND=postgres
export CLAW_PG_URL=postgresql://acp:acp_local_dev@localhost:5432/acp
```

Without these env vars, the daemon falls back to the JSONL `FileSessionBackend`
(single-client mode, no cross-client fan-out).

**Start in foreground (for debugging)**:

```bash
claw acp serve --addr 0.0.0.0:7800
```

**Start in background with nohup**:

```bash
nohup claw acp serve --addr 0.0.0.0:7800 \
  > ~/logs/claw-acp.log 2>&1 &
echo "Daemon PID: $!"
```

**Start via systemd user unit** (optional, for auto-start on login):

Create `~/.config/systemd/user/claw-acp.service`:

```ini
[Unit]
Description=claw ACP daemon
After=network.target

[Service]
ExecStart=/home/adr/.local/bin/claw acp serve --addr 0.0.0.0:7800
Environment=CLAW_SESSION_BACKEND=postgres
Environment=CLAW_PG_URL=postgresql://acp:acp_local_dev@localhost:5432/acp
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now claw-acp.service
systemctl --user status claw-acp.service
```

**Verify daemon is up**:

```bash
# Option A: HTTP probe (if the daemon exposes one — check logs)
curl -s http://localhost:7800 2>&1 | head -5

# Option B: WebSocket handshake with wscat (install: npm i -g wscat)
wscat -c ws://localhost:7800
# Type the initialize message:
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"0.2","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}
# Expected response contains: "capabilities":{"streaming":true,"tools":true,"permissions":true}
```

**Rollback**: Kill the daemon process.

```bash
# If running in background:
pkill -f "claw acp serve"
# Or if using systemd:
systemctl --user stop claw-acp.service
```

---

### Step 7 — Verify via CLI client (smoke test)

Open a new terminal with the daemon running:

```bash
# Set env vars if not in shell profile
export CLAW_SESSION_BACKEND=postgres
export CLAW_PG_URL=postgresql://acp:acp_local_dev@localhost:5432/acp

# Start a normal claw session (uses stdio ACP internally)
claw
```

Send a test message. The session ID is shown in the footer. Note it:

```
> list the files in /tmp
```

Expected: claw responds and shows file listing. The session is written to the
Postgres `sessions` and `session_events` tables.

**Verify in Postgres**:

```bash
docker exec postgres psql -U acp -d acp -c \
  "SELECT session_id, created_at_ms, updated_at_ms FROM sessions ORDER BY created_at_ms DESC LIMIT 3;"
```

---

### Step 8 — Deploy the Open WebUI Pipe

The pipe file is at `docs/open-webui-pipe/acp_pipe.py` in the worktree.
It requires the daemon to be running (Step 6).

1. Open **https://chat.homelab.local** (or your Open WebUI URL).
2. Go to **Admin Panel → Functions → + Create Function**.
3. Paste the full content of
   `/home/adr/development/sovereign-agents-acp-m3/docs/open-webui-pipe/acp_pipe.py`.
4. Click **Save**, then click the **toggle** to enable the function.
5. Click the **gear icon** (Valves) next to the function and set:

   | Valve | Value | Notes |
   |-------|-------|-------|
   | `daemon_url` | `ws://localhost:7800` | Use `ws://host.docker.internal:7800` if WebUI is in a container and daemon is on the host |
   | `workspace_root` | `/home/adr` | Path the daemon uses for new sessions |
   | `model` | `sonnet` | Model alias per your LiteLLM config |
   | `permission_timeout_seconds` | `60` | Keep this BELOW the daemon's 60-second default — see Known Limitations |

6. Save the Valves.

**Verify**:

- Create a new chat in Open WebUI.
- Select **claw** (or the function name you saved) from the model dropdown.
- Send: `hello`
- Expected: a text reply appears. Turn-level streaming — text arrives as one
  block, not token-by-token. This is expected behavior (see Known Limitations).

**Rollback**: Disable or delete the function in Admin Panel → Functions.

---

### Step 9 — Cross-client smoke test

This step verifies that two clients share the same session via the daemon.

**Terminal A** (CLI):

```bash
claw
# Send: "my test phrase for cross-client verification"
# Note the session_id from the footer
```

**Open WebUI**:

If the pipe supports session resume by conversation ID (current behavior: the
pipe creates a new session per Open WebUI conversation thread), you can verify
fan-out by connecting a second WebSocket manually using the session_id from
the CLI:

```bash
wscat -c ws://localhost:7800
# Send:
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"0.2","capabilities":{},"clientInfo":{"name":"watcher","version":"0"}}}
# Then resume:
{"jsonrpc":"2.0","id":2,"method":"session/resume","params":{"session_id":"<SESSION_ID_FROM_STEP_7>"}}
# Send a turn from this second client:
{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"session_id":"<SESSION_ID>","text":"what did I just say?"}}
```

Expected: both the CLI and the wscat connection receive the same stream of
events (`TurnStart`, `TextDelta`, `TurnEnd`). The CLI displays the response;
the wscat raw JSON shows the same events.

For **permission prompt cross-client** (requires a tool that triggers approval):

- Send a prompt that triggers a dangerous tool from one client.
- The `PermissionRequest` event fans out to ALL attached clients.
- The first client to respond `session/permission_response` with `allow` or
  `deny` wins. Remaining clients receive the same `PermissionResponse` event.
- In Open WebUI: type `allow` or `deny` as your next message when you see the
  `PERMISSION REQUEST` block in the response.

---

## Rollback Procedure

If anything goes wrong after the merge, these steps fully revert the cutover.

### 1. Stop the daemon

```bash
pkill -f "claw acp serve"
# or
systemctl --user stop claw-acp.service
```

### 2. Disable the Open WebUI Pipe

Admin Panel → Functions → toggle the claw function OFF (or delete it).

### 3. Revert the merge on `feature/acp`

```bash
cd /home/adr/development/sovereign-agents/claw-code-fork
# If the branch has NOT been pushed to remote:
git reset --hard ORIG_HEAD
# If the branch HAS been pushed (and you need to preserve remote history):
git revert -m 1 <merge-commit-sha>
git push
```

### 4. Reinstall the old binary

```bash
# Rebuild from the reverted branch
cd /home/adr/development/sovereign-agents/claw-code-fork/rust
cargo build --release -p rusty-claude-cli
cp target/release/claw ~/.local/bin/claw
```

### 5. Postgres data (if needed)

The `acp` database is fully isolated from `litellm`, `langfuse`, and `openwebui`.
Dropping it has no effect on anything else:

```bash
docker exec postgres psql -U postgres -c "DROP DATABASE IF EXISTS acp;"
docker exec postgres psql -U postgres -c "DROP ROLE IF EXISTS acp;"
```

JSONL session files (the original session store) are never modified or deleted
by any step in this runbook. They remain at their original paths under
`~/.claw/sessions/` and are immediately usable if you revert to the
`FileSessionBackend`.

---

## Known Limitations

### Turn-level streaming (not delta-level)

Text appears in the Open WebUI chat as one block per turn, not token-by-token.
This is because `ConversationRuntime::run_turn` is a synchronous, accumulating
API — there is no callback for individual tokens.

Full detail: `docs/acp-m3/blockers/phase-2-streaming.md`

Resolution path: Option B from that doc (callback/visitor on `run_turn`) is
recommended as the least invasive change.

### Permission UX is text-based in Open WebUI

When a tool requires permission, the Pipe emits a `PERMISSION REQUEST` block in
the chat response. The user must type `allow` or `deny` as their next message.
There is no button UI — Open WebUI's function API does not support injecting
interactive widgets mid-response.

### slopus/happy mobile client: not supported

Eight protocol-level deviations (Socket.IO transport, E2E encryption, DEK key
model, OAuth auth, and four semantic mismatches) make a clean adapter
infeasible. The estimated implementation is ~750 LOC of non-trivial crypto +
Socket.IO code, exceeding the < 200 LOC feasibility threshold by 3.75x.

Full detail: `docs/acp-m3/m5-happy-deferral.md`

Options if mobile access is needed: open a `slopus/happy` upstream issue
requesting an ACP-compatible WebSocket mode (Option A in that doc), or build
a minimal PWA client that speaks ACP JSON-RPC 2.0 directly (Option C).

### Daemon vs. Pipe permission timeout mismatch

The ACP daemon's internal permission prompt slot expires after **60 seconds** by
default. The Open WebUI Pipe Valve `permission_timeout_seconds` defaults to
**300 seconds**. If the Pipe waits longer than the daemon's slot TTL, the
daemon will close the slot and return a `PermissionError::Timeout` while the
Pipe is still waiting.

**Recommendation**: Set the Pipe Valve `permission_timeout_seconds` to **50**
(10 seconds below the daemon's 60-second hard limit) to ensure the Pipe gets a
clean error before the WebSocket connection times out.

---

## Troubleshooting

### `Connection refused` on `ws://localhost:7800`

The daemon is not running. Start it per Step 6. Check:

```bash
ps aux | grep "claw acp serve"
```

If using systemd: `systemctl --user status claw-acp.service` and
`journalctl --user -u claw-acp.service -n 50`.

### `401 Unauthorized` from LiteLLM

The daemon passes model requests through the configured `ApiClient`. If LiteLLM
returns 401, the API key is missing or wrong:

```bash
# Check what key claw is using
grep -r "ANTHROPIC_API_KEY\|LITELLM" ~/.claw/settings.json ~/.config/claw/ 2>/dev/null
```

Set the correct key in your environment or in claw's settings.

### `Postgres permission denied` when starting daemon with Postgres backend

The `acp` role can only access the `acp` database. Verify the connection URL:

```bash
docker exec postgres psql -U acp -d acp -c "SELECT current_user, current_database();"
```

Expected: `acp | acp`. If it fails, re-run the init SQL from Step 2.

### Stale session slots / `PermissionError::SlotNotFound`

The daemon reaps stale client rows (where `last_seen_ms` is > 5 minutes old)
on startup and every 5 minutes. If you see slot-not-found errors, the session
was likely detached and the slot was reaped. Resume the session with a new
`session/resume` call.

### Session not visible in Postgres after CLI use

Check that `CLAW_SESSION_BACKEND=postgres` is set in the daemon's environment,
not just in the shell running `claw`. If the daemon was started without the
env var, it defaults to `FileSessionBackend` (JSONL). Restart the daemon with
the env var set.

### Where to look in logs

| Source | Command |
|--------|---------|
| LiteLLM proxy | `docker logs litellm --tail 100 -f` |
| claw daemon stderr | Check your nohup log or `journalctl --user -u claw-acp.service` |
| Langfuse per-request traces | https://langfuse.homelab.local — filter by session tag |
| Postgres event history | `docker exec postgres psql -U acp -d acp -c "SELECT session_id, seq, event_type, created_at_ms FROM session_events ORDER BY event_id DESC LIMIT 20;"` |
| Open WebUI Pipe logs | Admin Panel → Logs (if available), or Open WebUI container: `docker logs open-webui --tail 50` |
