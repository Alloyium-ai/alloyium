# Host-Layer Codex A2A Peer

This runbook starts a Codex gateway directly on the host in tmux while connecting it to
the local Docker e2e A2A fabric. Use this when the agent needs host-level access for
Docker, tmux, filesystem, or LAN diagnostics that should not run inside a container.

Prefer container peers for normal development. A host-layer peer is intentionally more
powerful and should use a distinct identity from any existing LAN mesh peer.

## Prerequisites

Start the full local stack first:

```bash
bin/alloyium up full-codex
bin/alloyium verify
```

Confirm the e2e core socket is host-visible:

```bash
test -S /run/a2a-core/core.sock
```

The host user also needs a working Codex CLI login:

```bash
codex
```

## Onboard The Host Peer

Use a unique agent id. Do not reuse an existing host identity from another mesh.

```bash
AGENT=host-ops-gw-e2e-gpubox
A2A_DIR="$PWD/a2a-host"
SHIM="$HOME/.local/bin/a2a-shim-alloyium-e2e"

mkdir -p "$A2A_DIR" "$HOME/.local/bin" "$HOME/logs"

docker cp alloyium-codex-gw-1:/usr/local/bin/a2a-shim "$SHIM"
chmod +x "$SHIM"

NATS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-nats-1)
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)

bun onboard.ts "$AGENT" \
  --transport none \
  --dir "$A2A_DIR" \
  --redis "redis://$REDIS_IP:6379" \
  --nats "nats://$NATS_IP:4222"
```

`a2a-host/` contains private seed material and is ignored by git.

## Start In Tmux

Create a host wrapper outside the repo. It resolves Docker bridge IPs at each start so a
Compose restart does not require editing the script.

```bash
cat > "$HOME/restart-host-ops-gw-e2e.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT="${ALLOYIUM_ROOT:-$HOME/alloyium}"
AGENT="${AGENT:-host-ops-gw-e2e-gpubox}"

cd "$ROOT"

export PATH="$HOME/.npm-global/bin:$HOME/.bun/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
mkdir -p "$HOME/.local/state/alloyium/$AGENT"

set -a
source "$ROOT/a2a-host/$AGENT.a2a.env"
set +a

NATS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-nats-1)
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)

export NATS_URL="nats://$NATS_IP:4222"
export REDIS_URL="redis://$REDIS_IP:6379"
export A2A_INBOX_DB="$HOME/.local/state/alloyium/$AGENT/a2a-inbox.sqlite3"

export A2A_CORE_SOCK="/run/a2a-core/core.sock"
export CODEX_GW_A2A_SHIM_BIN="$HOME/.local/bin/a2a-shim-alloyium-e2e"
export CODEX_GW_ENABLE_A2A_TOOLS=1
export CODEX_GW_A2A_TOOLS_MODE=shim
export CODEX_GW_A2A_TOOLS_REQUIRED=1
export CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC=20
export CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC=600

export BRAIN_URL="${BRAIN_URL:-}"
export CODEX_GW_EFFORT="${CODEX_GW_EFFORT:-xhigh}"
export CODEX_GW_BUDGET_MAX_PCT="${CODEX_GW_BUDGET_MAX_PCT:-92}"
export CODEX_GW_ALLOW_WRITE="${CODEX_GW_ALLOW_WRITE:-1}"
export CODEX_GW_WRITE_ALLOWLIST="${CODEX_GW_WRITE_ALLOWLIST:-dev-pm,agent-1,a2a-portal}"
export CODEX_GW_CODEX_SANDBOX="${CODEX_GW_CODEX_SANDBOX:-danger-full-access}"
export CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX="${CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX:-workspace-write}"
export CODEX_BUILD_CWD_ROOTS="${CODEX_BUILD_CWD_ROOTS:-$HOME/git,$ROOT,$HOME}"

export CODEX_GW_AGENT_PREAMBLE="You are $AGENT, a host-layer Codex A2A peer for the Alloyium e2e stack. Use A2A tools for peer coordination. Prefer bounded host diagnostics and Docker/Compose operations. Never reveal secrets or private key material."

exec bun codex_gateway.ts 2>&1 | tee -a "$HOME/logs/$AGENT.log"
EOF

chmod +x "$HOME/restart-host-ops-gw-e2e.sh"
tmux new-session -d -s e2e-host-ops-1 "$HOME/restart-host-ops-gw-e2e.sh"
```

If your clone is not at `~/alloyium`, change `ROOT` in the wrapper.

## Verify

```bash
tmux capture-pane -p -t e2e-host-ops-1:0.0 -S -80

REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)
redis-cli -u "redis://$REDIS_IP:6379" GET alloyium:a2a:presence:host-ops-gw-e2e-gpubox
```

The tmux pane should show:

```text
bus joined as 'host-ops-gw-e2e-gpubox'
```

Stop it with:

```bash
tmux kill-session -t e2e-host-ops-1
```

## Run More Than One Host Peer

Each host peer needs its own A2A identity. Repeat onboarding with a new `AGENT`, then
start the same wrapper with that `AGENT` value:

```bash
AGENT=host-ops-gw-e2e-gpubox-2

NATS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-nats-1)
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)

bun onboard.ts "$AGENT" \
  --transport none \
  --dir "$PWD/a2a-host" \
  --redis "redis://$REDIS_IP:6379" \
  --nats "nats://$NATS_IP:4222"

tmux new-session -d -s e2e-host-ops-2 \
  "AGENT=$AGENT $HOME/restart-host-ops-gw-e2e.sh"
```

Verify both:

```bash
tmux ls | grep e2e-host-ops

REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)
redis-cli -u "redis://$REDIS_IP:6379" GET alloyium:a2a:presence:host-ops-gw-e2e-gpubox
redis-cli -u "redis://$REDIS_IP:6379" GET alloyium:a2a:presence:host-ops-gw-e2e-gpubox-2
```

## Remote Host Peer

A peer on another server can join the Docker e2e bus by connecting to the gpubox NATS
and Redis proxies over TCP. This is useful when the remote machine has host resources
that should participate in the same A2A fabric.

On gpubox, expose only the bus ports needed by remote peers:

```bash
A2A_REMOTE_BUS_BIND=<gpubox-lan-ip> \
A2A_REMOTE_NATS_PORT=4222 \
A2A_REMOTE_REDIS_PORT=16379 \
docker compose -f compose.yaml -f compose.presets/full-codex.yaml \
  --profile legacy-nats-proxy --profile remote-bus \
  up -d nats-lan-proxy redis-lan-proxy
```

If the stack was started with extra override files, use the same `-f` file list and
environment used for the running stack.

The remote host then uses:

```bash
NATS_URL=nats://<gpubox-lan-ip>:4222
REDIS_URL=redis://<gpubox-lan-ip>:16379
```

Do not point a remote shim at gpubox's `/run/a2a-core/core.sock`; Unix sockets are local
to the machine. Start a separate `a2a_core.ts` on the remote host with its own socket,
then point that remote Codex gateway's inner shim at the remote socket.

For a server that already runs a legacy Alloyium/A2A stack, keep all e2e paths separate:

- repo/root: `~/alloyium-e2e-remote`
- core socket: `~/.run/alloyium-e2e/a2a-core/core.sock`
- runtime state: `~/.local/state/alloyium/<agent-id>/`
- shim binary: `~/.local/bin/a2a-shim-alloyium-e2e`
- agent id: a unique value such as `host-ops-gw-e2e-srv01`
- tmux sessions: unique names such as `e2e-gpubox-core-srv01` and `e2e-host-ops-srv01-1`

Remote core wrapper:

```bash
cat > "$HOME/restart-alloyium-e2e-core-gpubox.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT="${ALLOYIUM_E2E_ROOT:-$HOME/alloyium-e2e-remote}"
SOCK="${A2A_CORE_SOCK:-$HOME/.run/alloyium-e2e/a2a-core/core.sock}"

mkdir -p "$(dirname "$SOCK")" "$HOME/.local/state/alloyium/e2e-core-gpubox" "$HOME/logs"
cd "$ROOT"

export PATH="$HOME/.bun/bin:$HOME/.npm-global/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
export NATS_URL="${NATS_URL:-nats://<gpubox-lan-ip>:4222}"
export REDIS_URL="${REDIS_URL:-redis://<gpubox-lan-ip>:16379}"
export A2A_UDS_SOCKET_PATH="$SOCK"
export A2A_TRANSPORT_AUTH=none
export A2A_INBOX_DB="$HOME/.local/state/alloyium/e2e-core-gpubox/a2a-inbox.sqlite3"
export BRAIN_URL="${BRAIN_URL:-}"
export KAI_HTTP_URL="${KAI_HTTP_URL:-}"
export KAI_WS_URL="${KAI_WS_URL:-}"
export KAI_TOKEN_PATH="${KAI_TOKEN_PATH:-}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

exec bun a2a_core.ts 2>&1 | tee -a "$HOME/logs/alloyium-e2e-core-gpubox.log"
EOF

chmod +x "$HOME/restart-alloyium-e2e-core-gpubox.sh"
tmux new-session -d -s e2e-gpubox-core-srv01 "$HOME/restart-alloyium-e2e-core-gpubox.sh"
```

Remote Codex peer wrapper:

```bash
cat > "$HOME/restart-host-ops-gw-e2e-srv01.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT="${ALLOYIUM_E2E_ROOT:-$HOME/alloyium-e2e-remote}"
AGENT="${AGENT:-host-ops-gw-e2e-srv01}"
SOCK="${A2A_CORE_SOCK:-$HOME/.run/alloyium-e2e/a2a-core/core.sock}"

mkdir -p "$HOME/.local/state/alloyium/$AGENT" "$HOME/logs"
cd "$ROOT"

export PATH="$HOME/.bun/bin:$HOME/.npm-global/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"

set -a
source "$ROOT/a2a-host/$AGENT.a2a.env"
set +a

export NATS_URL="${NATS_URL:-nats://<gpubox-lan-ip>:4222}"
export REDIS_URL="${REDIS_URL:-redis://<gpubox-lan-ip>:16379}"
export A2A_INBOX_DB="$HOME/.local/state/alloyium/$AGENT/a2a-inbox.sqlite3"

export A2A_CORE_SOCK="$SOCK"
export CODEX_GW_A2A_SHIM_BIN="$HOME/.local/bin/a2a-shim-alloyium-e2e"
export CODEX_GW_ENABLE_A2A_TOOLS=1
export CODEX_GW_A2A_TOOLS_MODE=shim
export CODEX_GW_A2A_TOOLS_REQUIRED=1
export CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC=20
export CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC=600

export BRAIN_URL="${BRAIN_URL:-}"
export CODEX_GW_EFFORT="${CODEX_GW_EFFORT:-xhigh}"
export CODEX_GW_BUDGET_MAX_PCT="${CODEX_GW_BUDGET_MAX_PCT:-92}"
export CODEX_GW_ALLOW_WRITE="${CODEX_GW_ALLOW_WRITE:-1}"
export CODEX_GW_WRITE_ALLOWLIST="${CODEX_GW_WRITE_ALLOWLIST:-dev-pm,agent-1,a2a-portal}"
export CODEX_GW_CODEX_SANDBOX="${CODEX_GW_CODEX_SANDBOX:-danger-full-access}"
export CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX="${CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX:-workspace-write}"
export CODEX_BUILD_CWD_ROOTS="${CODEX_BUILD_CWD_ROOTS:-$HOME/git,$ROOT,$HOME}"

export CODEX_GW_AGENT_PREAMBLE="You are $AGENT, a host-layer Codex A2A peer connected to the gpubox Alloyium e2e bus. Use A2A tools for peer coordination. Prefer bounded host diagnostics and Docker/tmux checks. Never reveal secrets or private key material."

exec bun codex_gateway.ts 2>&1 | tee -a "$HOME/logs/$AGENT.log"
EOF

chmod +x "$HOME/restart-host-ops-gw-e2e-srv01.sh"
```

Onboard the remote identity against gpubox Redis, then start it:

```bash
AGENT=host-ops-gw-e2e-srv01
mkdir -p "$PWD/a2a-host"

bun onboard.ts "$AGENT" \
  --transport none \
  --dir "$PWD/a2a-host" \
  --redis redis://<gpubox-lan-ip>:16379 \
  --nats nats://<gpubox-lan-ip>:4222

tmux new-session -d -s e2e-host-ops-srv01-1 "$HOME/restart-host-ops-gw-e2e-srv01.sh"
```

Verify from gpubox:

```bash
curl -fsS http://127.0.0.1:8901/api/presence | grep host-ops-gw-e2e-srv01
```

For a live request test:

```bash
curl -fsS http://127.0.0.1:8901/api/send \
  -H 'content-type: application/json' \
  -d '{"to":"host-ops-gw-e2e-srv01","type":"request","send_mode":"one-off","body":"Reply with exactly: PONG_SRV01_E2E"}'
```

Then check the DM:

```bash
curl -fsS http://127.0.0.1:8901/api/dm/host-ops-gw-e2e-srv01 | grep PONG_SRV01_E2E
```

## Remote Host Launcher

A remote host peer can also launch container peers on that remote host while those peers
join the gpubox A2A bus. Keep this isolated from any legacy stack on the remote server:
use a separate Docker image tag, network, secrets path, workspace volume, core socket,
and tmux session names.

Only gpubox needs LAN-exposed bus ports:

- NATS: `<gpubox-lan-ip>:4222`
- Redis: `<gpubox-lan-ip>:16379`

The remote launcher's HTTP API should stay local to the remote host, for example
`127.0.0.1:18910`. Do not expose it on the LAN unless you add an outer access-control
layer.

On the remote host, build a distinct Codex gateway image from the e2e clone:

```bash
cd "$HOME/alloyium-e2e-remote"

bun install --frozen-lockfile

docker build \
  -f docker/gateway-codex/Dockerfile \
  -t alloyium-e2e-remote-codex-gw:latest \
  --build-arg CC_UID="$(id -u)" \
  --build-arg CC_GID="$(id -g)" \
  .
```

Prepare isolated runtime resources:

```bash
mkdir -p \
  "$HOME/alloyium-e2e-remote/a2a-launch-secrets" \
  "$HOME/alloyium-e2e-remote/data/workspace" \
  "$HOME/.local/state/alloyium/e2e-launcher-gpubox" \
  "$HOME/logs"

chmod 700 "$HOME/alloyium-e2e-remote/a2a-launch-secrets"

docker network inspect alloyium-e2e-remote_a2a-net >/dev/null 2>&1 \
  || docker network create alloyium-e2e-remote_a2a-net >/dev/null

docker volume inspect alloyium-e2e-remote_codex_workspaces >/dev/null 2>&1 \
  || docker volume create alloyium-e2e-remote_codex_workspaces >/dev/null
```

Create the remote launcher wrapper:

```bash
cat > "$HOME/restart-alloyium-e2e-launcher-gpubox.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT="${ALLOYIUM_E2E_ROOT:-$HOME/alloyium-e2e-remote}"
TOKEN_FILE="${A2A_LAUNCHER_TOKEN_FILE:-$HOME/.local/state/alloyium/e2e-launcher-gpubox/token}"
CORE_DIR="${A2A_CORE_DIR:-$HOME/.run/alloyium-e2e/a2a-core}"
WORKSPACE_DIR="${A2A_WORKSPACE_HOST_PATH:-$ROOT/data/workspace}"
SECRETS_DIR="${A2A_LAUNCH_SECRETS_DIR:-$ROOT/a2a-launch-secrets}"

mkdir -p "$(dirname "$TOKEN_FILE")" "$SECRETS_DIR" "$WORKSPACE_DIR" "$HOME/logs"
chmod 700 "$SECRETS_DIR"
if [ ! -s "$TOKEN_FILE" ]; then
  umask 077
  openssl rand -hex 32 > "$TOKEN_FILE"
fi

cd "$ROOT"

export PATH="$HOME/.bun/bin:$HOME/.npm-global/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
export NATS_URL="${NATS_URL:-nats://<gpubox-lan-ip>:4222}"
export REDIS_URL="${REDIS_URL:-redis://<gpubox-lan-ip>:16379}"
export A2A_LAUNCHER_HOST="${A2A_LAUNCHER_HOST:-127.0.0.1}"
export A2A_LAUNCHER_PORT="${A2A_LAUNCHER_PORT:-18910}"
export A2A_LAUNCHER_TOKEN="$(tr -d '\n' < "$TOKEN_FILE")"
export A2A_LAUNCH_PROVIDER="${A2A_LAUNCH_PROVIDER:-docker}"
export A2A_LAUNCH_ALLOWED_IDS="${A2A_LAUNCH_ALLOWED_IDS:-host-ops-gw-e2e-srv01}"
export A2A_LAUNCH_COMPOSE_PROJECT="${A2A_LAUNCH_COMPOSE_PROJECT:-alloyium-e2e-remote}"
export A2A_LAUNCH_CODEX_IMAGE="${A2A_LAUNCH_CODEX_IMAGE:-alloyium-e2e-remote-codex-gw:latest}"
export A2A_LAUNCH_NETWORK="${A2A_LAUNCH_NETWORK:-alloyium-e2e-remote_a2a-net}"
export A2A_LAUNCH_SECRETS_DIR="$SECRETS_DIR"
export A2A_LAUNCH_SECRETS_VOLUME="$SECRETS_DIR"
export A2A_LAUNCH_CORE_SOURCE="$CORE_DIR"
export A2A_LAUNCH_WORKSPACES_VOLUME="${A2A_LAUNCH_WORKSPACES_VOLUME:-alloyium-e2e-remote_codex_workspaces}"
export A2A_LAUNCH_PROJECT_DIR="$ROOT"
export A2A_LAUNCH_WORKSPACE_HOST_PATH="$WORKSPACE_DIR"
export A2A_LAUNCH_WORKSPACE_CONTAINER_PATH="${A2A_LAUNCH_WORKSPACE_CONTAINER_PATH:-/workspace}"
export A2A_LAUNCH_PRESENCE_WAIT_MS="${A2A_LAUNCH_PRESENCE_WAIT_MS:-30000}"
export A2A_LAUNCH_CONTAINER_USER="${A2A_LAUNCH_CONTAINER_USER:-$(id -u):$(id -g)}"
export CODEX_HOST_HOME="${CODEX_HOST_HOME:-$HOME/.codex}"
export CODEX_SSH_DIR="${CODEX_SSH_DIR:-$HOME/.ssh}"
export CODEX_WORKSPACE_ROOT="${CODEX_WORKSPACE_ROOT:-$HOME/git}"
export CODEX_BUILD_CWD_ROOTS="${CODEX_BUILD_CWD_ROOTS:-/workspace,$HOME/git,$ROOT}"
export CODEX_GW_ALLOW_WRITE="${CODEX_GW_ALLOW_WRITE:-1}"
export CODEX_GW_WRITE_ALLOWLIST="${CODEX_GW_WRITE_ALLOWLIST:-dev-pm,agent-1,a2a-portal,host-ops-gw-e2e-srv01}"
export CODEX_GW_CODEX_SANDBOX="${CODEX_GW_CODEX_SANDBOX:-danger-full-access}"
export CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX="${CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX:-workspace-write}"
export BRAIN_URL="${BRAIN_URL:-}"
export KAI_HTTP_URL="${KAI_HTTP_URL:-}"
export KAI_WS_URL="${KAI_WS_URL:-}"
export KAI_TOKEN_PATH="${KAI_TOKEN_PATH:-}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

docker network inspect "$A2A_LAUNCH_NETWORK" >/dev/null 2>&1 || docker network create "$A2A_LAUNCH_NETWORK" >/dev/null
docker volume inspect "$A2A_LAUNCH_WORKSPACES_VOLUME" >/dev/null 2>&1 || docker volume create "$A2A_LAUNCH_WORKSPACES_VOLUME" >/dev/null

exec bun a2a_launcher.ts 2>&1 | tee -a "$HOME/logs/alloyium-e2e-launcher-gpubox.log"
EOF

chmod 700 "$HOME/restart-alloyium-e2e-launcher-gpubox.sh"
tmux new-session -d -s e2e-gpubox-launcher-srv01 "$HOME/restart-alloyium-e2e-launcher-gpubox.sh"
curl -fsS http://127.0.0.1:18910/readyz
```

Expose the launcher tool through the remote core. Add these lines to the remote core
wrapper before `exec bun a2a_core.ts`:

```bash
TOKEN_FILE="${A2A_LAUNCHER_TOKEN_FILE:-$HOME/.local/state/alloyium/e2e-launcher-gpubox/token}"
mkdir -p "$(dirname "$TOKEN_FILE")"
if [ ! -s "$TOKEN_FILE" ]; then
  umask 077
  openssl rand -hex 32 > "$TOKEN_FILE"
fi

export A2A_LAUNCHER_URL="${A2A_LAUNCHER_URL:-http://127.0.0.1:18910}"
export A2A_LAUNCHER_TOKEN="$(tr -d '\n' < "$TOKEN_FILE")"
export A2A_AGENT_LAUNCH_ALLOWED_IDS="${A2A_AGENT_LAUNCH_ALLOWED_IDS:-host-ops-gw-e2e-srv01}"
export A2A_AGENT_LAUNCH_MODE="${A2A_AGENT_LAUNCH_MODE:-shim}"
export A2A_AGENT_LAUNCH_TIMEOUT_MS="${A2A_AGENT_LAUNCH_TIMEOUT_MS:-120000}"
```

Then restart the remote e2e core and host peer:

```bash
tmux kill-session -t e2e-gpubox-core-srv01 2>/dev/null || true
rm -f "$HOME/.run/alloyium-e2e/a2a-core/core.sock"
tmux new-session -d -s e2e-gpubox-core-srv01 "$HOME/restart-alloyium-e2e-core-gpubox.sh"

tmux kill-session -t e2e-host-ops-srv01-1 2>/dev/null || true
tmux new-session -d -s e2e-host-ops-srv01-1 "$HOME/restart-host-ops-gw-e2e-srv01.sh"
```

If the peer was restarted abruptly and Redis still has stale presence, wait for the TTL
or delete only that exact presence key:

```bash
redis-cli -h <gpubox-lan-ip> -p 16379 TTL alloyium:a2a:presence:host-ops-gw-e2e-srv01
redis-cli -h <gpubox-lan-ip> -p 16379 DEL alloyium:a2a:presence:host-ops-gw-e2e-srv01
```

Verify launch from gpubox through the portal:

```bash
curl -fsS http://127.0.0.1:8901/api/send \
  -H 'content-type: application/json' \
  -d '{"to":"host-ops-gw-e2e-srv01","type":"request","send_mode":"one-off","body":"Use your a2a_launch_codex_agent tool now. Launch a persistent Codex peer with agent_id exactly codex-srv01-launched-1, label srv01-launch, mode shim. Reply with JSON containing ok, agent_id, status, and runtime_id."}'

curl -fsS http://127.0.0.1:8901/api/presence | grep codex-srv01-launched-1
```

Verify the remote-created node replies on the gpubox bus:

```bash
curl -fsS http://127.0.0.1:8901/api/send \
  -H 'content-type: application/json' \
  -d '{"to":"codex-srv01-launched-1","type":"request","send_mode":"one-off","body":"Reply with exactly: PONG_FROM_SRV01_LAUNCHED_1"}'

curl -fsS http://127.0.0.1:8901/api/dm/codex-srv01-launched-1 | grep PONG_FROM_SRV01_LAUNCHED_1
```

## Notes

- The outer `codex_gateway.ts` joins the bus through NATS/Redis.
- The inner `codex app-server` gets A2A tools through `a2a-shim` and
  `/run/a2a-core/core.sock`.
- Both paths must point at the same e2e stack.
- The host peer uses `transport none`; NATS transport is unauthenticated, but A2A message
  signing remains enabled and the Redis pubkey registration is still required.
