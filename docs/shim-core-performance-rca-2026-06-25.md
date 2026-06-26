# Shim/Core Performance RCA - 2026-06-25

## Scope

Reviewed the host-level A2A shim and central MCP core paths for scaling risks under many peers and concurrent tool sessions.

## Runtime Observations

- A deployed `a2a-core` container was healthy during the check, with modest memory use.
- Delivery logs showed direct notification registration and ack pairs completing normally.
- The core had a high session count after startup/reconnects, with many `tool_only=true` sessions plus standing agent sessions. This is not an immediate crash symptom, but it is a capacity risk if stale sockets or stalled MCP handshakes accumulate.

## Existing Safeguards Found

- Shim queues are bounded (`CTRL`, `MCP`, stdin, UDS).
- MCP/UDS frames are capped at 16 MiB.
- UDS acceptor already caps pre-hello CTRL frames, pre-session MCP frames/bytes, direct-delivery inflight, and incomplete-frame slowloris behavior.
- Central core uses a shared NATS/Redis pool, shared key cache, and shared brain/Kai/vault tool fronts instead of per-container full bridge resources.

## Changes

- Cached MCP tool surfaces per session so repeated `tools/list` requests do not rebuild identical channel/brain/Kai/vault/launcher schemas.
- Added `A2A_MCP_PENDING_INJECT_CAP` with default `256` for pre-initialize `notifications/claude/channel` buffering. Overflow rejects the inject promise so upstream durable delivery can redeliver instead of retaining unbounded in-memory work.
- Added `A2A_UDS_MAX_CONNECTIONS` with default `512` for the central UDS acceptor. Excess local socket connections are refused before allocating session state.
- Reused the per-session launcher instance across MCP server wiring and core tool-list bookkeeping.
- Pinned the Compose `a2a-core` container inbox DB to `/app/data/a2a-inbox.sqlite3`. The host path remains controlled by `A2A_INBOX_HOST_PATH`; the container path must not inherit a host-local `A2A_INBOX_DB` value because that can make UDS session starts fail with `SQLITE_CANTOPEN`.

## Config Knobs

- `A2A_MCP_PENDING_INJECT_CAP`: max pending channel notifications held before MCP initialized. Default: `256`.
- `A2A_UDS_MAX_CONNECTIONS`: max live UDS connections in the core acceptor. Default: `512`.

The `a2a-core` Compose service exposes both knobs with those defaults.

## Remaining Follow-Ups

- Add a lightweight core metrics endpoint or status-plane fields for live sessions, tool-only sessions, UDS live connections, pending pre-init inject count, and p95 tool-call latency.
- Run a synthetic load test with many shim clients repeatedly performing initialize, `tools/list`, `a2a_peers`, and direct notification delivery.
- Review launcher/tool-only session lifecycle if live session count continues to climb without corresponding socket count.
