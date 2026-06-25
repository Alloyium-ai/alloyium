# Spec: Dynamic container UID/GID

## Problem

Every container that runs as the `bun` user is built on `oven/bun:1.3.14-debian`,
which bakes the `bun` user, `/home/bun`, and `/app` at **uid/gid 1000**. Compose
already runs the agent services as `user: "${CC_UID:-1000}:${CC_GID:-1000}"` and
bind-mounts the host user's credential dirs (`~/.claude`, `~/.codex`, `~/.ssh`) into
those containers.

This only works when the **host user is uid 1000**. On any other host (here the host
user is **1003**, and **uid 1000 belongs to a different real user**), the two identities
collide:

- Run as 1000 → cannot read/write the host's 1003-owned creds (e.g. `~/.claude.json`
  is `0600`), and the OAuth/codex state cannot be read or refreshed.
- Run as 1003 → cannot write the image's 1000-owned `/home/bun`, `/app`, the inbox
  SQLite, or the shared `a2a_secrets` seeds (observed: `mkdir /home/bun: permission
  denied`, `SQLITE_CANTOPEN`, codex `failed to initialize sqlite state runtime`).

`CC_UID`/`CC_GID` are currently consumed only at **runtime** (`user:`). The image's
baked ownership is fixed at 1000, so changing `CC_UID` alone cannot fix it.

## Goal

Make the baked uid/gid **dynamic**: derive from the host user, default to 1000 (so
upstream behaviour is unchanged when unset), and apply at **build time** so the image's
`bun` user, `/home/bun`, `/app`, and all `bun`-owned files match `CC_UID`/`CC_GID`.
Then build-time and runtime identities agree, and the host bind mounts line up because
`CC_UID = id -u` of the host user.

## Single source of truth

`CC_UID` / `CC_GID` in `.env` (already the runtime knob) become the **one** source for
both the runtime `user:` and the new build args. `.env` is generated from `id -u` /
`id -g` so it tracks the host user automatically. Default remains `1000`.

## Design

A small idempotent script remaps the image's `bun` user at build time:

`docker/lib/uidremap.sh` (run as root, early in each `bun`-based final stage):
```sh
gid=${CC_GID:-1000}; uid=${CC_UID:-1000}
[ "$gid" != "$(id -g bun)" ] && groupmod -g "$gid" bun
[ "$uid" != "$(id -u bun)" ] && usermod  -u "$uid" bun
chown -R "$uid:$gid" /home/bun
```
`usermod`/`groupmod` are present in the debian base. Defaults (1000) make it a no-op.

Each affected Dockerfile gets, in its **final** (oven/bun) stage, **before** the first
`USER bun` / `COPY --chown=bun` / `RUN chown ...bun` and while still `USER root`:
```dockerfile
ARG CC_UID=1000
ARG CC_GID=1000
COPY docker/lib/uidremap.sh /usr/local/sbin/uidremap
RUN sh /usr/local/sbin/uidremap
```
After the remap, the existing `chown bun:bun /app`, `COPY --chown=bun:bun ...`, and
`bun install` (run as `USER bun`) all retarget to the new uid/gid automatically, because
they reference `bun` by name. The block is placed **after** the heavy apt/npm layers so
changing `CC_UID` does not bust those caches.

### Per-Dockerfile insertion anchors (insert the block immediately AFTER each line)

| Dockerfile | base? | insert after line | first bun-ownership line it must precede |
|---|---|---|---|
| `docker/bun/Dockerfile` | yes | `  && rm -rf /var/lib/apt/lists/*` | `RUN chown bun:bun /app` |
| `docker/a2a-core/Dockerfile` | yes (final stage) | `  && rm -rf /var/lib/apt/lists/*` | `RUN chown bun:bun /app` |
| `docker/gateway-claude/Dockerfile` | yes | `WORKDIR /app` | `RUN chown bun:bun /app` |
| `docker/gateway-codex/Dockerfile` | yes (final stage) | `  && rm -rf /var/lib/apt/lists/*` (the apt block ending before `WORKDIR /app`) | `chown -R bun:bun /app /ms-playwright` |
| `docker/agent-claude/Dockerfile` | yes | `WORKDIR /app` | `RUN chown bun:bun /app` |
| `docker/test/Dockerfile` | yes | `  && rm -rf /var/lib/apt/lists/*` | `RUN chown bun:bun /app` |
| `docker/socat/Dockerfile` | **no** (alpine, no bun user) | — skip — | — |

## compose.yaml

Add two top-level anchors:
```yaml
x-cc-build-args: &cc-build-args
  CC_UID: ${CC_UID:-1000}
  CC_GID: ${CC_GID:-1000}

x-build-bun: &build-bun
  context: .
  dockerfile: docker/bun/Dockerfile
  args:
    <<: *cc-build-args
```
Wire the args into **every service that builds a bun-derived image** (all except the
two `socat` proxies):

- All `docker/bun/Dockerfile` services (onboard-*, a2a-portal, a2a-launcher,
  taskboard-event-bridge, a2a-taskboard-dispatcher, materialize-kai-token, alloyium-cortex):
  collapse their `build:` block to `build: *build-bun`.
- `a2a-core`, `test`: add `args: { <<: *cc-build-args }` to their existing build block.
- `claude-gw` (gateway-claude), `claude-agent`/`claude-agent-b` (agent-claude),
  `codex-gw`/`codex-gw-b` (gateway-codex): add `<<: *cc-build-args` to their existing
  `args:` (alongside the CLI-version arg).

Why every bun-image service (not just the `user: ${CC_UID}` ones): the `onboard-*`
one-shots write each agent's signing seed into the shared `a2a_secrets` volume as `bun`
(`chown bun:bun` + `su bun`). If the writer's `bun` stays 1000 while the reader gateway
runs as 1003, the seed is unreadable. Remapping every bun image keeps the whole fabric
on one uid.

## .env

Generated to match the host user:
```
CC_UID=<id -u>
CC_GID=<id -g>
```

## Rollout / verification

1. `docker compose down` (the existing `a2a_secrets` seeds and `./data/a2a-inbox` were
   written by the old uid; `a2a-core`/`onboard-*` re-`chown` them as root on next up, so
   no manual cleanup is required, but a `-v` reset is cleanest for an eval).
2. `docker compose up -d --build`.
3. Verify, per gateway container:
   - `id` reports the host uid/gid.
   - reads **and writes** its mounted creds (`~/.claude.json`, `~/.codex`).
   - `claude-gw` joins the bus; `codex-gw` initializes its sqlite state (no `CANTOPEN`).
   - `./data/a2a-inbox` and `a2a_secrets` seeds owned by the host uid.
   - portal answers `200` on http://localhost:8901.

## Non-goals / safety

- No credential permissions are loosened; creds stay `0600` owned by the host user. The
  container simply *is* that user.
- Defaults stay 1000 → no behaviour change for hosts where the user is already 1000.
- `socat` proxies and pulled images (`nats`, `redis`) are untouched.
