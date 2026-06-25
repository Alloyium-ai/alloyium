# Host-Layer Claude A2A Peer

This runbook starts a Claude Code gateway directly on the host in tmux while connecting it
to the local Docker e2e A2A fabric. Use this when the agent needs host-level access for
Docker, tmux, filesystem, or LAN diagnostics that should not run inside a container. It is
the Claude counterpart of [`docs/host-codex-peer.md`](host-codex-peer.md).

Prefer container peers (`claude-agent`) for normal development. A host-layer peer is
intentionally more powerful and should use a distinct identity from any existing LAN mesh
peer.

Unlike the codex host peer (whose outer `codex_gateway.ts` dials NATS/Redis directly), the
Claude peer joins the bus **entirely through the a2a-shim → `/run/a2a-core/core.sock`** —
the same path the containerized `claude-agent` uses. So the launch wrapper never needs the
bus NATS/Redis IPs; only onboarding does (to register the pubkey in Redis).

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

The host also needs a logged-in Claude CLI and a host Bun runtime with the repo deps
installed (the peer runs `claude` on the host, driven by your OAuth subscription — no API
key):

```bash
claude                      # log in once
curl -fsSL https://bun.sh/install | bash   # if bun is not already installed
bun install --frozen-lockfile              # repo deps on the host (for onboard.ts)
```

## Onboard The Host Peer

Use a unique agent id. Do not reuse an existing host identity from another mesh.

```bash
AGENT="host-claude-$(hostname -s)"
SHIM="$HOME/.local/bin/a2a-shim-alloyium-e2e"

mkdir -p "$PWD/a2a-host" "$HOME/.local/bin"

# The shim binary the Claude session will use as its MCP server (UDS -> a2a-core).
docker cp alloyium-codex-gw-1:/usr/local/bin/a2a-shim "$SHIM"
chmod +x "$SHIM"

NATS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-nats-1)
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)

bun onboard.ts "$AGENT" \
  --transport none \
  --dir "$PWD/a2a-host" \
  --redis "redis://$REDIS_IP:6379" \
  --nats "nats://$NATS_IP:4222"
```

`a2a-host/` contains private seed material and is ignored by git.

## Start In Tmux

The launcher is committed at `bin/host-claude-peer`. It writes the session's control files
(system prompt, `.mcp.json`, launch script) under `~/.local/state/alloyium/<AGENT>` —
deliberately **off** the shared workspace — sets the Claude session's working directory to
the shared host workspace (`data/workspace`) so a bare `git clone` lands there, starts the
session in tmux, and auto-dismisses the one-time trust / dev-channels confirmations.

```bash
bin/host-claude-peer
# or, to override the identity / working dir:
AGENT="host-claude-$(hostname -s)" CLAUDE_AGENT_CWD=/abs/work/dir bin/host-claude-peer
```

The wrapper fails fast with a clear message if the stack is down, the shim is missing, or
the peer has not been onboarded.

## Verify

```bash
tmux capture-pane -p -t host-claude -S -80
```

A working session shows the alloyium MCP server in use and live channel injection from
peers, for example:

```text
← alloyium: <message text from a peer>
  Called alloyium (ctrl+o to expand)
```

> Note: at startup Claude may print `server:alloyium · no MCP server configured with that
> name` once — that is a harmless race where `--dangerously-load-development-channels` runs
> a beat before the shim's MCP server finishes connecting. Once `Called alloyium` and
> `← alloyium:` lines appear, channel injection and the a2a tools are live.

Confirm presence on the bus:

```bash
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)
redis-cli -u "redis://$REDIS_IP:6379" GET "alloyium:a2a:presence:host-claude-$(hostname -s)"
```

The presence entry's `host` field will be the a2a-core container id (not the machine
hostname), because the Claude peer registers presence *through* a2a-core over the shim —
this is expected and differs from the codex host peer.

Stop it with:

```bash
tmux kill-session -t host-claude
```

## Run More Than One Host Peer

Each host peer needs its own A2A identity and tmux session. Onboard a new `AGENT`, then
launch with a distinct session name:

```bash
AGENT=host-claude-$(hostname -s)-2

NATS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-nats-1)
REDIS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' alloyium-redis-1)

bun onboard.ts "$AGENT" --transport none --dir "$PWD/a2a-host" \
  --redis "redis://$REDIS_IP:6379" --nats "nats://$NATS_IP:4222"

AGENT="$AGENT" TMUX_SESSION="host-claude-2" bin/host-claude-peer
```

## Notes

- The Claude session reaches the bus only via `a2a-shim` → `/run/a2a-core/core.sock`;
  a2a-core (holding the shared NATS/Redis connections) registers the peer's presence,
  durable inbox, and signing-verify on its behalf.
- The session uses `transport none`; NATS transport is unauthenticated, but A2A message
  signing remains enabled and the Redis pubkey registration is still required.
- The peer drives your logged-in `claude` OAuth session; `ANTHROPIC_API_KEY` /
  `ANTHROPIC_AUTH_TOKEN` are stripped before spawn.
- A host-layer peer has host-level access. Anything it does in git still flows through your
  normal branch-protection + PR review; but its shell/Docker/filesystem reach is broad —
  run it deliberately.
