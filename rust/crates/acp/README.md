# acp — Agent Client Protocol server for claw-code

Scaffold crate that will host the ACP daemon launched by `claw acp serve`.

## Status

**Scaffold only.** `acp::serve` currently returns `NotImplemented`. The CLI
wires through to this crate so that follow-up PRs can land protocol surfaces
incrementally without re-touching the CLI each time.

Tracked upstream as **ROADMAP #76**. Today, `claw acp` (no subcommand) remains
the discoverability alias that prints status — it does not touch this crate.
`claw acp serve` is the new handoff point into `acp::serve`.

## What ACP is

The Agent Client Protocol is a JSON-RPC-style protocol defined by Zed
Industries for decoupling coding agents from editor/client UIs. Spec:
<https://github.com/zed-industries/agent-client-protocol>.

Known consumers:

- **Zed** — native editor integration.
- **`slopus/happy`** — mobile-first ACP client; primary target for our fork so
  the user can talk to `claw` from a phone without bespoke plumbing.

## Target surface

Minimum viable ACP server to make `happy` work end-to-end:

1. **Transport** — stdio framing (required), websocket (nice to have).
2. **Session lifecycle** — `session/new`, `session/resume`, `session/close`
   mapped to `runtime::Session` + `ConversationRuntime`.
3. **Streaming** — translate `AssistantEvent`, tool call events, and usage
   deltas into ACP `session/update` notifications.
4. **Tool calls** — surface `ToolCall` events and accept client-side tool
   results where the protocol allows it.
5. **Permission prompts** — bridge `PermissionEnforcer` outcomes to ACP
   user-confirmation requests so a mobile client can approve `Bash`, `Edit`,
   etc.

## Module layout

```
src/
  lib.rs        — public entrypoint (`serve`, `ServeOptions`, `AcpError`)
  transport.rs  — stdio / websocket framing (M1)
  session.rs    — new/resume/close session handlers (M2)
  stream.rs     — runtime-event -> ACP notification bridge (M2/M3)
  tools.rs      — tool call + permission bridges (M3/M4)
```

## Integration point

`rusty-claude-cli` calls `acp::serve(options)` when the user runs
`claw acp serve`. That wiring is present today behind a `todo!()` — the goal
is that incremental PRs can fill in the body without touching CLI parsing.

## Milestones

See `/docs/acp-implementation-plan.md` in the sovereign-agents monorepo for
the full M1–M5 breakdown. This crate does not track milestones inline to
keep the upstream-PR diff tight.
