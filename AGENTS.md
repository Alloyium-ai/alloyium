# AGENTS.md

Guidance for coding agents working in this repository. Keep setup instructions practical:
new users should be able to clone the repo, start the full local fabric, see the portal,
and verify a fully A2A-enabled Codex peer without reconstructing Compose profiles by hand.

## Primary Entry Points

- `bin/alloyium` - local setup wrapper for init/up/launch/verify/down.
- `compose.yaml` - base Docker Compose stack.
- `compose.presets/full-codex.yaml` - preset for launcher + fleet + fusion + full Codex node.
- `docs/full-codex-node.md` - user-facing full-node setup notes.
- `docs/host-codex-peer.md` - host-level Codex peer setup for e2e bus access.
- `docs/shared-workspace.md` - `/workspace` mount behavior.
- `GETTING_STARTED.md` - longer product walkthrough.
- `a2a_portal.ts` - web portal and send surface.
- `a2a_launcher.ts` - Docker-backed runtime peer launcher.
- `a2a_core.ts` - shared MCP-over-UDS A2A tool host.
- `codex_gateway.ts` / `claude_gateway.ts` - model gateway peers.

Alloyium uses the host user's logged-in CLI sessions. Before running the gateways, the
host should have working CLI logins:

```bash
claude
codex
```

The stack mounts host CLI state into containers:

- Claude: `~/.claude` and `~/.claude.json`
- Codex: `~/.codex`
- SSH keys: `~/.ssh`, read-only

Do not put API keys, OAuth tokens, private keys, `.env`, generated `a2a/`, or `data/`
contents into commits.

## Fresh Clone Startup

Preferred full local setup:

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

`bin/alloyium up full-codex` layers `compose.presets/full-codex.yaml` over
`compose.yaml`, starts the `launcher`, `fleet`, and `fusion` profiles, and launches
`codex-a2a-full-1`.

Expected long-running services include:

- `redis`
- `nats`
- `brain`
- `tei`
- `vault`
- `a2a-core`
- `a2a-portal`
- `a2a-launcher`
- `codex-gw`
- `claude-gw`
- `claude-agent`
- `claude-agent-b`
- `codex-gw-b`
- `alloyium-cortex`
- launcher-spawned container `cc-agent-codex-a2a-full-1`

The `onboard-*` containers are one-shot setup jobs. It is normal for them to show
`Exited (0)` after the stack is healthy.

The base stack owns the default Agent Brain and Vault services. Fresh clones should
not require a manual clone of `alloyium-brain` or a host KeePassXC install. Override
`ALLOYIUM_BRAIN_BUILD_CONTEXT`, `ALLOYIUM_VAULT_BUILD_CONTEXT`, `BRAIN_URL`, or
`VAULT_URL` only for local development or external service deployments.

## Full Codex A2A Node

`codex-a2a-full-1` is the standard full-node peer for local/e2e testing. It is launched
by `a2a-launcher`, not managed directly by Compose.

Expected properties:

- A2A tools enabled through the shared-core shim.
- Shared workspace mounted at `/workspace`.
- Write-enabled policy (`CODEX_GW_ALLOW_WRITE=1`).
- `CODEX_BUILD_CWD_ROOTS` includes `/workspace`.
- Launch authority for `a2a_launch_codex_agent`.

Two allowlists must stay in sync:

- `A2A_AGENT_LAUNCH_ALLOWED_IDS` on `a2a-core`: controls which peers see the launch tool.
- `A2A_LAUNCH_ALLOWED_IDS` on `a2a-launcher`: controls which `created_by` values the
  launcher API accepts.

The full-codex preset defaults both to include `codex-a2a-full-1`. If you rename the
full node with `ALLOYIUM_FULL_CODEX_AGENT_ID`, update both allowlists in `.env`.

To recreate the dynamic node after changing launcher-provided environment:

```bash
bin/alloyium launch full-codex --replace
```

## Host-Layer Codex Peer

Use a host-layer peer only when the agent needs direct host access for Docker, tmux,
filesystem, or LAN diagnostics. It should use a distinct identity such as
`host-ops-gw-e2e-gpubox`, not an identity from another LAN mesh.

The host peer has two A2A paths that must point at the same e2e stack:

- outer `codex_gateway.ts`: connects to the e2e NATS/Redis container bridge IPs.
- inner `codex app-server`: gets A2A tools through the e2e `/run/a2a-core/core.sock`.

Follow `docs/host-codex-peer.md`. Generated host identity files live under
`a2a-host/` and must never be committed. To run multiple host peers, repeat onboarding
with a unique `AGENT` value, for example `host-ops-gw-e2e-gpubox-2`, and launch it in
a separate tmux session.

Remote host peers use the same pattern, but connect their outer gateway to the gpubox
NATS/Redis proxies and run their own local `a2a_core.ts` for the shim socket. On hosts
that already run a legacy A2A stack, use separate e2e paths such as
`~/alloyium-e2e-remote`, `~/.run/alloyium-e2e/a2a-core/core.sock`, and unique tmux
session names so the legacy `/run/a2a-core` or `~/.run/a2a-core` stack is untouched.

Remote hosts can also launch their own container peers onto the gpubox bus. Run a
separate remote `a2a_launcher.ts` bound to localhost, build a distinct Codex gateway
image on that host, and set the remote core's `A2A_LAUNCHER_URL`,
`A2A_LAUNCHER_TOKEN`, and `A2A_AGENT_LAUNCH_ALLOWED_IDS`. Keep launcher network,
secrets, workspace, image tags, and tmux sessions separate from any legacy stack. See
`docs/host-codex-peer.md#remote-host-launcher`.

## Host-Layer Claude Peer

The Claude counterpart of the codex host peer — a real `claude` session on the host with
host-level access, joined to the bus. Use a distinct identity such as
`host-claude-<hostname>`.

Unlike the codex host peer, the Claude peer joins the bus **only** through the a2a-shim →
`/run/a2a-core/core.sock` (a2a-core holds the shared NATS/Redis connections and registers
its presence/inbox), so the launcher needs no bus IPs. Onboard the identity, then run the
committed wrapper:

```bash
bin/host-claude-peer
```

Follow `docs/host-claude-peer.md`. Generated host identity files live under `a2a-host/`
and must never be committed. To run multiple peers, onboard a unique `AGENT` and launch
each with a distinct `TMUX_SESSION`.

## Profiles

Compose starts services with no `profiles:` by default. Anything with a profile must be
requested explicitly.

Important profiles:

| Profile | Services | Purpose |
| --- | --- | --- |
| `launcher` | `a2a-launcher` | Enables runtime worker launch endpoints. Requires `A2A_LAUNCHER_TOKEN`. |
| `fleet` | `claude-agent-b`, `codex-gw-b`, plus onboarding jobs | Adds second Claude/Codex peers for multi-agent tests. |
| `fusion` | `alloyium-cortex`, plus onboarding job | Fans a prompt to Claude and Codex and synthesizes a result. |
| `taskboard` | taskboard bridge/dispatcher | Optional taskboard integration. |
| `remote-bus` | Redis LAN proxy | Exposes Redis for remote bus scenarios. |
| `legacy-nats-proxy` | NATS LAN proxy | Exposes NATS for legacy remote scenarios. |
| `kai` / `gateways` | Kai token materialization | Optional local secrets path. |
| `test` | test container | Runs repository selftest container. |

Use the wrapper for the standard full stack:

```bash
bin/alloyium ps full-codex
bin/alloyium config full-codex
```

Raw Compose equivalent:

```bash
docker compose -f compose.yaml -f compose.presets/full-codex.yaml \
  --profile launcher --profile fleet --profile fusion up -d --build
```

## Portal UI

Default portal URL:

```text
http://127.0.0.1:8901
```

For a LAN browser:

```bash
A2A_PORTAL_BIND=0.0.0.0 bin/alloyium up full-codex
```

To bind only to one LAN address:

```bash
A2A_PORTAL_BIND=<server-ip> bin/alloyium up full-codex
```

The portal has send controls. Do not expose it on an untrusted network without
firewalling or other access control.

## Agent Brain

Brain tools are default-on against the in-stack `brain` service and fail soft if the
service is unavailable. To wire the stack to an external agent-brain service:

```bash
BRAIN_URL=http://brain-host:8787 bin/alloyium up full-codex --replace
```

The effective brain URL for MCP tools is the `BRAIN_URL` on `a2a-core`, because
`a2a-shim` connects to the shared `/run/a2a-core/core.sock` server and the core owns
the `BrainTools` instance. A gateway or host peer may also pass `BRAIN_URL` into its
shim env, but that does not override a running core. If `BRAIN_URL` changes on a live
stack, restart `a2a-core` during an idle window so existing shim sessions reconnect to
the core with the new brain configuration.

Use `--replace` when changing `BRAIN_URL` because launcher-spawned containers inherit
that value only when they are created.

Read-only probe from inside the stack:

```bash
docker compose -f compose.yaml -f compose.presets/full-codex.yaml \
  --profile launcher --profile fleet --profile fusion \
  exec -T a2a-core bun -e \
  'import { BrainTools } from "./brain_tools.ts"; const bt = new BrainTools({ timeoutMs: 5000 }); console.log(JSON.stringify(await bt.callTool("a2a_recall", { query: "alloyium", limit: 1 })));'
```

## Verify The Stack

Use:

```bash
bin/alloyium verify
```

Manual checks:

```bash
curl -fsS http://127.0.0.1:8901/api/send-status
curl -fsS http://127.0.0.1:8901/api/presence
docker inspect cc-agent-codex-a2a-full-1
```

Expected presence should include at least:

- `a2a-portal`
- `a2a-core-*`
- `codex-gw`
- `claude-gw`
- `claude-agent`
- `claude-agent-b`
- `codex-gw-b`
- `alloyium-cortex`
- `codex-a2a-full-1`

## Shutdown

Use the wrapper for the full stack:

```bash
bin/alloyium down full-codex
```

This removes `cc-agent-codex-a2a-full-1` before `docker compose down`, which prevents a
launcher-spawned container from keeping the Compose network in use.

If doing raw Compose shutdown, include the same profiles used at startup and remove any
launcher-spawned containers:

```bash
docker rm -f cc-agent-codex-a2a-full-1
docker compose -f compose.yaml -f compose.presets/full-codex.yaml \
  --profile launcher --profile fleet --profile fusion down
```

If `docker compose down` reports that `alloyium_a2a-net` is still in use:

```bash
docker ps -a --filter network=alloyium_a2a-net \
  --format '{{.Names}} {{.Status}} service={{.Label "com.docker.compose.service"}}'
```

Remove launcher-spawned `cc-agent-*` containers or rerun `down` with the missing
profiles.

## Common Gotchas

- `docker compose up` alone does not start `launcher`, `fleet`, or `fusion`.
- `docker compose ps` must be run from the repo directory, or with explicit `-f` files.
- If a custom project name was used, include `-p <project>` in later `ps`, `up`, and
  `down` commands.
- The portal default bind is loopback-only.
- `codex-gw` and `claude-gw` depend on host CLI login state.
- Launcher-spawned containers do not automatically pick up changed environment; recreate
  them with `bin/alloyium launch full-codex --replace`.
- Do not delete named volumes casually; they contain generated bus/secrets/state. Use
  `down -v` only when intentionally resetting the stack.
