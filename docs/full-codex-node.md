# Full Codex A2A Node

This preset brings up the local fabric with a write-enabled Codex peer named
`codex-a2a-full-1`. It is a normal signed A2A peer with the shared `/workspace` mount,
the A2A MCP tools, and launch authority so it can create additional Codex peers through
`a2a_launch_codex_agent`.

## Fresh Clone

Log in to the host CLIs first:

```bash
claude
codex
```

Then run:

```bash
git clone https://github.com/Alloyium-ai/alloyium.git
cd alloyium
bin/alloyium init
bin/alloyium up full-codex
bin/alloyium verify
```

`bin/alloyium init` creates `.env`, generates `A2A_LAUNCHER_TOKEN`, sets `CC_UID` and
`CC_GID` to the current host user, records `A2A_LAUNCH_PROJECT_DIR`, and prepares
`data/workspace`.

`bin/alloyium up full-codex` starts the default stack plus the `launcher`, `fleet`, and
`fusion` profiles, then asks the launcher to start `codex-a2a-full-1`.

## What The Preset Adds

`compose.presets/full-codex.yaml` is layered on top of `compose.yaml`.

- `a2a-core` gets `A2A_LAUNCHER_URL`, `A2A_LAUNCHER_TOKEN`, and
  `A2A_AGENT_LAUNCH_ALLOWED_IDS=codex-gw,codex-a2a-full-1`.
- `a2a-launcher` gets `A2A_LAUNCHER_TOKEN` and
  `A2A_LAUNCH_ALLOWED_IDS=codex-gw,claude-gw,agent-1,core,pm-design,pm-dev,pm-ops,codex-a2a-full-1`.
- Launcher-spawned Codex peers inherit the shared workspace mount at `/workspace` and
  `CODEX_BUILD_CWD_ROOTS=/workspace,<legacy host git root>`.

Both allowlists matter. `A2A_AGENT_LAUNCH_ALLOWED_IDS` controls whether a peer sees the
launch tool in its MCP surface. `A2A_LAUNCH_ALLOWED_IDS` controls whether the launcher
accepts the API request.

## Portal And Brain

The portal is published at `http://127.0.0.1:8901` by default. To use it from another
machine on the LAN:

```bash
A2A_PORTAL_BIND=0.0.0.0 bin/alloyium up full-codex
```

To connect the fabric to an external agent brain:

```bash
BRAIN_URL=http://brain-host:8787 bin/alloyium up full-codex --replace
```

The `--replace` flag recreates `cc-agent-codex-a2a-full-1` so the launched peer picks up
new launcher-provided environment.

## Codex Jobs And Realtime Sessions

Alloyium exposes two Codex execution shapes:

- `codex.job.request.v1` remains the batch/one-off contract for fleet dispatch,
  CI-style work, and portal one-off sends.
- Portal chat sends to Codex-like peers use `codex.session.input.v1`, which creates
  or reuses a long-lived app-server thread, starts a turn when idle, steers an active
  turn when possible, and streams normalized `codex.session.event.v1` events to a
  deterministic portal topic.

The gateway also accepts the lower-level realtime A2A schemas:

- `codex.session.create.v1`
- `codex.session.state.v1`
- `codex.turn.start.v1`
- `codex.turn.steer.v1`
- `codex.thread.inject_items.v1`
- `codex.turn.interrupt.v1`

For local tools that need HTTP instead of A2A, a Codex gateway can expose an optional
loopback-only HTTP/SSE control plane:

```bash
CODEX_GW_HTTP_PORT=8995 bun codex_gateway.ts
```

Useful endpoints:

```text
POST /v1/codex/sessions
GET  /v1/codex/sessions
GET  /v1/codex/sessions/{session_id}
POST /v1/codex/sessions/{session_id}/turns
POST /v1/codex/sessions/{session_id}/events
GET  /v1/codex/sessions/{session_id}/events
POST /v1/codex/sessions/{session_id}/interrupt
```

Do not expose the HTTP gateway publicly. Non-loopback binds require
`CODEX_GW_HTTP_TOKEN`; clients must send `Authorization: Bearer <token>`.

## Useful Commands

```bash
bin/alloyium ps full-codex
bin/alloyium verify
bin/alloyium launch full-codex --replace
bin/alloyium down full-codex
```

`down full-codex` removes the launcher-spawned `cc-agent-codex-a2a-full-1` container
before running Compose down. This avoids leaving a dynamic container attached to the
Compose network.

## Host-Layer Peer

For host operations, you can run a Codex gateway directly on the machine in tmux while
joining the Docker e2e bus. This is separate from launcher-spawned container peers and
uses a host identity such as `host-ops-gw-e2e-gpubox`.

See [`docs/host-codex-peer.md`](host-codex-peer.md).
