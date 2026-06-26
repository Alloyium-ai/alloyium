# Codex Realtime Role Spec

## Summary

Keep `codex-gw` as the stable batch/job gateway and add a separate Codex app-server
role for long-lived interactive sessions. The new role is a signed A2A peer, tentatively
`codex-rt-gw`, that owns session lifecycle, active-turn steering, context injection, and
streaming session events.

This does not expose Codex app-server directly. Codex app-server remains a local
JSON-RPC worker behind the Alloyium gateway. A2A, portal chat, optional loopback HTTP,
SSE, or WebSocket clients talk to the gateway role, and the gateway translates to
`thread/start`, `thread/resume`, `turn/start`, `turn/steer`, `turn/interrupt`, and
`thread/inject_items`.

## Why Split The Role

The current `codex_gateway.ts` can process both `codex.job.request.v1` and the realtime
`codex.session.*`/`codex.turn.*` schemas. That hybrid path is useful for compatibility,
but it mixes two different operating models:

- Jobs are request/reply work units. They need durable accepted/completed/failed replies,
  claim-checked output, long turn timeouts, and queue-like semantics.
- Realtime sessions are live conversations. They need stable session IDs, persisted
  thread IDs, monotonic event streams, active-turn IDs, steering, interrupt, replay, and
  lower-latency UI feedback.

Running a second identity lets us evaluate realtime behavior without destabilizing the
job system or changing existing fleet dispatch assumptions.

## Roles

### `codex-gw`

Purpose: batch/one-off Codex execution.

Default contracts:

- Accept `codex.job.request.v1`.
- Emit `codex.job.accepted.v1`, `codex.job.delta.v1`, `codex.job.completed.v1`,
  `codex.job.failed.v1`, and `codex.job.rejected.v1`.
- Continue to serve fleet dispatch, fusion/panel calls, CI-style work, and portal
  `one-off` sends.

Recommended defaults:

- Keep current `CODEX_GW_ALLOW_WRITE` and workspace-write allowlist behavior.
- Keep long job timeout defaults.
- Keep claim-checked large output behavior.
- Do not make portal chat depend on this role during the realtime eval.

### `codex-rt-gw`

Purpose: long-lived Codex app-server session router.

Default contracts:

- Accept `codex.session.create.v1`.
- Accept `codex.session.input.v1`.
- Accept `codex.session.state.v1`.
- Accept `codex.turn.start.v1`.
- Accept `codex.turn.steer.v1`.
- Accept `codex.turn.interrupt.v1`.
- Accept `codex.thread.inject_items.v1`.
- Emit `codex.session.ready.v1`, `codex.turn.started.v1`,
  `codex.session.event.v1`, `codex.turn.completed.v1`,
  `codex.turn.interrupted.v1`, `codex.turn.failed.v1`, and
  `codex.thread.injected.v1`.

Recommended defaults for eval:

- `A2A_AGENT_ID=codex-rt-gw`.
- `CODEX_GW_ROLE=session`.
- `CODEX_GW_ALLOW_WRITE=0` for the first eval.
- `CODEX_GW_CODEX_SANDBOX=read-only`.
- `CODEX_GW_THREAD_STORE_PREFIX=alloyium:codex:rt:thread:`.
- `CODEX_GW_HTTP_PORT=0` by default.
- If HTTP/SSE is enabled, bind loopback unless `CODEX_GW_HTTP_TOKEN` is set.
- Use app-server stdio or Unix socket transport; do not expose app-server TCP directly.

## Routing

Portal and service routing should become explicit:

- Portal `one-off` to Codex targets `codex-gw` and wraps plain text as
  `codex.job.request.v1`.
- Portal `chat` to Codex targets `codex-rt-gw` and wraps plain text as
  `codex.session.input.v1`.
- Fusion, fleet orchestration, build work, and CI-style dispatch keep targeting
  `codex-gw`.
- Direct A2A clients may still target either role with the canonical schema for that
  role.

Suggested portal env:

```text
A2A_PORTAL_CODEX_JOB_TARGET=codex-gw
A2A_PORTAL_CODEX_SESSION_TARGET=codex-rt-gw
```

The current recipient detection already treats `codex-*` as a Codex-like peer, so the
main behavior change is choosing the default target for chat and making the UI label
clear.

## Session Model

Session identity:

- `session_id` is the external stable UI/client session ID.
- `thread_key` is the durable lookup key for app-server thread reuse.
- `thread_id` is the Codex app-server thread ID.
- `turn_id` is the active Codex turn ID.

For portal chat:

```text
session_id = portal:chat:<portal-agent>:<codex-agent>[:chat-context]
thread_key = same value
stream_topic = portal-rt-<normalized-thread-key>
```

Required persisted state:

- `thread_key -> { threadId, sandbox, cwd, updated_at }` in Redis.
- `session_id -> { thread_id, thread_key, cwd, sandbox, approval_policy, status,
  active_turn_id, seq, updated_at }` for restart recovery.
- Event replay buffer keyed by session, preferably a Redis Stream:
  `alloyium:codex:rt:events:<session_id>`.

Current implementation already has thread-key persistence and in-process session state.
The next production step is persisting session registry state and replayable events.

## Event Semantics

Inbound client modes:

- `auto`: if a turn is active, use `turn/steer`; otherwise use `turn/start` for
  actionable user input.
- `start`: force `turn/start`; if another turn is active, reject with
  `active_turn_exists` unless an explicit queue policy is added.
- `steer`: force `turn/steer`; reject if there is no active turn.
- `inject`: use `thread/inject_items` only; do not wake the model.
- `interrupt`: use `turn/interrupt`.

Outbound normalized events:

- `turn_started`
- `agent_text_delta`
- `item_started`
- `item_completed`
- `thread_status_changed`
- `context_compacted`
- `turn_completed`
- `turn_failed`
- `turn_interrupted`

Every outbound event must include:

- `schema`
- `session_id`
- `thread_id`
- `seq`
- `event`
- `ts`

When available, include:

- `turn_id`
- `item_id`
- `text`
- `status`
- `payload`

Ordering rules:

- `seq` is monotonic per `session_id`.
- Events from unrelated `thread_id` values must be ignored.
- Once `turn_id` is known, events from unrelated turns must be ignored.
- `item/completed` assistant text is a fallback only when no delta for that item was
  already emitted.

## HTTP/SSE/WS Control Plane

The session role may expose HTTP for local tools, but A2A remains the primary fabric
contract.

Required endpoints:

```text
POST /v1/codex/sessions
GET  /v1/codex/sessions
GET  /v1/codex/sessions/{session_id}
POST /v1/codex/sessions/{session_id}/turns
POST /v1/codex/sessions/{session_id}/events
GET  /v1/codex/sessions/{session_id}/events
POST /v1/codex/sessions/{session_id}/interrupt
```

Transport requirements:

- Loopback bind is allowed without a token.
- Non-loopback bind requires `Authorization: Bearer <CODEX_GW_HTTP_TOKEN>`.
- SSE should support replay from `Last-Event-ID` or `?after_seq=`.
- WebSocket can be added after SSE if the portal needs bidirectional browser streaming.

## Implementation Plan

1. Add gateway role mode.
   - Implemented: `CODEX_GW_ROLE=hybrid|job|session`, default `hybrid` for
     compatibility.
   - Implemented: in `job` mode, reject realtime schemas with
     `unsupported_schema_for_role`.
   - Implemented: in `session` mode, reject `codex.job.request.v1` with
     `unsupported_schema_for_role`.
   - Add role/mode to startup logs and presence metadata if presence supports it.

2. Add `codex-rt-gw` service wiring.
   - Implemented: add `onboard-codex-rt-gw`.
   - Implemented: add a `codex-rt-gw` Compose service using the same image and
     command under the `realtime` profile.
   - Implemented: set session-role defaults and a separate thread store prefix.
   - Implemented: keep it read-only for the first eval.

3. Make portal Codex routing explicit.
   - Implemented: add `A2A_PORTAL_CODEX_JOB_TARGET`.
   - Implemented: add `A2A_PORTAL_CODEX_SESSION_TARGET`.
   - Implemented: keep `send_mode=one-off` on the selected/job target.
   - Implemented: route chat from the configured job target to the session target
     when `A2A_PORTAL_CODEX_SESSION_TARGET` is set.
   - Keep explicit user-selected recipients respected.

4. Persist session registry and event replay.
   - Extend the realtime registry with a Redis-backed store.
   - Restore sessions on gateway restart using persisted session records.
   - Store normalized events in a capped Redis Stream per session.
   - Allow SSE replay by sequence.

5. Tighten realtime backpressure.
   - Add per-session max active turn count of one.
   - Add bounded event buffer limits and metrics.
   - Decide whether extra `start` requests are rejected or queued.

6. Add tests.
   - Role-mode schema rejection tests.
   - Compose/config shape tests for `codex-rt-gw`.
   - Portal default target tests for chat vs one-off.
   - Session restore tests after process-local registry loss.
   - SSE replay tests with `after_seq`.
   - Mock app-server tests for interleaved thread/turn notifications.

7. Run eval.
   - Start both roles in the same fabric.
   - Send portal one-off requests to `codex-gw`.
   - Send portal chat requests to `codex-rt-gw`.
   - Kill/restart `codex-rt-gw` during idle and active sessions.
   - Verify thread reuse, event replay, active-turn cleanup, and no cross-session
     event leakage.

## Eval Acceptance Criteria

- Existing `codex.job.request.v1` workflows continue to pass against `codex-gw`.
- Portal chat creates or resumes a `codex-rt-gw` session and returns
  `codex.session.ready.v1`.
- While a turn is active, follow-up portal chat input uses `turn/steer`.
- When idle, portal chat input uses `turn/start`.
- Passive context can be injected with `codex.thread.inject_items.v1`.
- Interrupt stops the active turn and emits `codex.turn.interrupted.v1`.
- Restarting `codex-rt-gw` preserves thread mapping and can resume a session.
- Event stream `seq` is monotonic and replayable.
- Events from another thread or turn are ignored.
- Non-loopback HTTP cannot start without a token.

## Non-Goals

- Do not replace the job contract.
- Do not expose Codex app-server directly to the LAN.
- Do not make `codex-rt-gw` write-enabled during the first eval.
- Do not use app-server TCP WebSocket as the public protocol.
- Do not require fusion/fleet/build clients to migrate to realtime.

## Open Questions

- Should session-role writes be enabled later with a separate allowlist, or should
  write-enabled interactive work stay on launched per-task peers?
- Should `start` during an active turn reject, queue, or interrupt-and-replace?
- How long should session event replay be retained?
- Should portal chat default to one shared `codex-rt-gw` or create per-user/session
  launched realtime peers for stronger isolation?
- Should app-server lifecycle be one app-server process per gateway role, per tenant,
  or eventually per session?
