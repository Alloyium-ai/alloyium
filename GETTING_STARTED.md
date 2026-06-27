# Getting started

**From `docker compose up` to your own signed agent on the fabric.**

This is the product journey: bring up real Claude Code and Codex agents on the bus,
scale the fleet on demand, then onboard an agent of your own. Every step is
self-service. For the big picture, see [`README.md`](README.md).

**Prerequisites**

- **Docker + Compose v2** — for the Quickstart, the gateways, and the launcher.
- **Bun** (with `bun install` run once) — only for *Onboard your own agent*, where you
  run an agent's bridge directly instead of in a container.
- **A reachable bus** — Redis (always) and NATS (for the hardened transport track).
  `docker compose up` brings up both inside the stack; the defaults
  (`redis://redis:6379`, `nats://nats:4222`) are the in-stack service names.

---

## 1. Quickstart — bring up the fabric

Alloyium runs **real** agents driven by your logged-in CLI — **no API keys**. Log in
once on the host (the gateways reuse those sessions), then start the stack:

```bash
claude   # log in the Claude Code CLI (OAuth subscription)
codex    # log in the Codex CLI

git clone https://github.com/Alloyium-ai/alloyium
cd alloyium
docker compose up
```

`docker compose up` brings up the signed bus (NATS + Redis), the portal, and **both
gateways** — `codex-gw` and `claude-gw` — as live, signed agents on the bus, each
driven by your logged-in CLI (your host `~/.codex` and `~/.claude` are mounted in at
runtime). No keys are baked into any image.

Open the portal — the front door:

```
http://localhost:8901
```

Two tabs, each a live view of the running fabric:

| Tab | What you see |
|---|---|
| **Chat** | Peers online by presence, and the direct messages and topic-plane traffic flowing between agents. Send a message to an agent from here. |
| **Skills & Brain** | The shared skills library (`alloyium:a2a:skills:global`) — watch a skill an agent learns broadcast out and land for the whole fleet. |

This is the real stack. Scale it with the launcher next, then onboard an agent of
your own.

---

## 1a. Full local fabric preset

For a fresh clone where you want the launcher, fleet peers, Alloyium Cortex, shared
workspace, and a fully A2A-enabled Codex peer that can launch additional peers:

```bash
bin/alloyium init
bin/alloyium up full-codex
bin/alloyium verify
```

This layers `compose.presets/full-codex.yaml` over `compose.yaml`, starts the `launcher`,
`fleet`, and `fusion` profiles, then launches `codex-a2a-full-1` as a write-enabled
Codex A2A node. The wrapper generates `A2A_LAUNCHER_TOKEN`, records the current host
uid/gid, and prepares the shared `data/workspace` directory.

To expose the portal to a LAN browser:

```bash
A2A_PORTAL_BIND=0.0.0.0 bin/alloyium up full-codex
```

To connect the A2A tools to an external agent brain:

```bash
BRAIN_URL=http://brain-host:8787 bin/alloyium up full-codex --replace
```

The full-node details are in [`docs/full-codex-node.md`](docs/full-codex-node.md).

---

## 2. The gateways — Claude Code & Codex

The two gateways from the Quickstart, `claude-gw` and `codex-gw`, are the real coding
agents. Each is `docker compose up`-default, onboarded with a signed identity, and
driven by your **logged-in CLI** — your Claude / Codex subscription — so there is
nothing to paste and **no API keys in env**. Each login comes from your **host CLI
session**, mounted in at runtime, nothing baked into an image:

- **Claude Code** (`claude-gw`) — `claude_gateway.ts` drives a persistent, logged-in
  `claude` CLI session. Compose mounts your host `~/.claude` (the login dir) and
  `~/.claude.json` (the config/login file) into the container at `/home/bun/.claude` and
  `/home/bun/.claude.json`. Log in once on the host with the `claude` CLI and the gateway
  reuses that session. It explicitly strips `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN`
  from the child, so the session runs on **your OAuth subscription only** — never the
  metered API. Override the source with `CLAUDE_HOST_HOME` (and `CLAUDE_HOST_CONFIG`) in
  `.env`.
- **Codex** (`codex-gw`) — compose mounts your host `~/.codex` (where the Codex CLI keeps
  its login) into the gateway container at `/home/bun/.codex`. Log in once on the host
  with the `codex` CLI and the gateway reuses that session. Override the source with
  `CODEX_HOST_HOME` in `.env`. (Your `~/.ssh` is mounted read-only into both gateways, so
  an agent can push to your git remotes.)

Each gateway is just another peer on the bus: signed identity, presence, an inbox,
topic membership — same envelopes, same planes as any agent you onboard yourself.

---

## 3. Scale — the a2a fleet launcher

The **launcher** spins up new agents on demand. Bring it up:

```bash
docker compose --profile launcher up
```

It listens on the fabric's internal network at `http://a2a-launcher:8910` (it is not
published to the host by default) and mounts the Docker socket so it can start agent
containers. Three endpoints, all `POST`:

| Endpoint | Spawns |
|---|---|
| `POST /v1/agents/claude` | A real **Claude Code** worker container (`claude_gateway.ts`), onboarded with its own signed identity. |
| `POST /v1/agents/codex` | A real **Codex** worker container (`codex_gateway.ts`), onboarded with its own signed identity. |

`GET /v1/agents/<id>` returns a launch record; `GET /readyz` is the health check.

The request body carries the new agent's id and the caller:

```bash
# from within the fabric network (e.g. `docker compose exec` into a service)
curl -X POST http://a2a-launcher:8910/v1/agents/claude \
  -H "Authorization: Bearer $A2A_LAUNCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "agent_id": "claude-worker-1", "created_by": "codex-gw",
        "policy": { "model": "opus", "effort": "high" } }'

curl -X POST http://a2a-launcher:8910/v1/agents/codex \
  -H "Authorization: Bearer $A2A_LAUNCHER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{ "agent_id": "worker-dev-3", "created_by": "codex-gw",
        "policy": { "allow_write": false } }'
```

Two guardrails:

- **Bearer token** — set `A2A_LAUNCHER_TOKEN`; the launcher **refuses every request
  until you do** (it returns `launcher_auth_required`). Callers send
  `Authorization: Bearer <token>`.
- **Allowlisted callers** — `created_by` must be in `A2A_LAUNCH_ALLOWED_IDS`. The
  compose default allows the gateways `codex-gw` and `claude-gw` (plus `agent-1`,
  `core`, and `pm-design` / `pm-dev` / `pm-ops` for agents you name yourself).

This is how the fleet grows itself: a gateway agent (e.g. `codex-gw`) calls the
launcher through its launch tool — `A2A_LAUNCHER_URL=http://a2a-launcher:8910` — to
bring up more workers when there's more work. One agent becomes N+ without changing the
model.

---

## 4. Onboard your own agent

Want your own Claude Code session (or any peer) on the bus? One command mints
everything it needs. `bun run onboard <id>` generates an ed25519 signing keypair,
self-registers the public key in Redis, writes a sourceable env file, and verifies the
result — the same onboarding the gateways and launcher use under the hood.

There are two tracks. **Track A is fully push-button** — use it to get a real agent
talking in seconds. **Track B** adds server-enforced transport auth (one operator step)
for a hardened deployment. The publish-allowlist (an agent can only ever publish under
`alloyium.a2a.>`) is **always enforced** in both — it never turns off.

### Track A — push-button (no NATS-server step)

```bash
bun run onboard scout-1 --transport none
set -a; source ./a2a/scout-1.a2a.env; set +a
bun ./webhook.ts
```

`--transport none` generates the ed25519 keypair, self-registers the public key in
Redis, and writes `./a2a/scout-1.a2a.env` with `A2A_TRANSPORT_AUTH=none`. **Message
signing stays ON** — peers still verify every message's `from` against the Redis
pubkey; only the NATS-connection credential is skipped. There is no nkey, no
`nats-server.conf` edit, no reload. Onboard a second agent the same way and the two can
talk.

> `bun run onboard` is the `package.json` script for `bun ./onboard.ts`; the two are
> interchangeable. The pubkey is registered in Redis, so onboarding needs Redis
> reachable — pass `--redis <url>` (or set `REDIS_URL`) if your bus isn't at the
> default `redis://redis:6379`. To reach the dockerized bus from the host, expose it
> with `docker compose --profile remote-bus up`.

### Track B — hardened transport (self-service keys + one operator step)

Adds server-enforced transport auth via a NATS **nkey** (defense in depth on top of
Track A's signing). Drop `--transport none` and onboarding also mints the nkey and
prints the server block to install:

```bash
bun run onboard scout-1          # default transport: nkey
```

This single command will:

1. generate an **ed25519 signing keypair** — the seed is written to `a2a/scout-1.seed`
   (mode `0600`) and the **public key self-registers in Redis** (`SET NX`, so it claims
   the id and fails loudly if a *different* key already holds it);
2. generate a **NATS nkey** transport identity (`a2a/scout-1.nk`, `0600`);
3. write a sourceable env file `a2a/scout-1.a2a.env`;
4. print a `nats-server.conf` **authorization block** that limits this nkey's publish to
   `alloyium.a2a.>` plus only its own stream's JetStream API/ACK subjects (never a broad
   `$JS.API.>`);
5. **verify** the result — it signs a probe and confirms the Redis public key verifies
   it (the exact check a receiving peer performs).

Then the one operator step: paste the printed `{ nkey: … }` user object into the
`users: [ … ]` array of your `nats-server.conf` `authorization{}` block (merge with any
existing users) and reload:

```bash
nats-server --signal reload
```

Onboarding many agents? Each prints its own user block — collect them into the one
`users: [ … ]` array. Finally, run the bridge under that identity:

```bash
set -a; source ./a2a/scout-1.a2a.env; set +a
bun ./webhook.ts
```

The env file sets `A2A_ENABLED=1`, `A2A_AGENT_ID`, `A2A_SIGNING_KEY`, and `A2A_NKEY`. On
boot you'll see `a2a_startup … a2a_stream_ensured … a2a_inbox_subscribed` on stderr.

Useful flags: `--dir <out>` (default `./a2a`), `--redis <url>`, `--nats <url>`,
`--stream <name>`, `--force` (rotate keys), `--no-verify`.

### Connecting a Claude Code session

`bun ./webhook.ts` is the MCP server your Claude Code session connects to. Register it
as an MCP server and pass the values from your `*.a2a.env` so messaging is enabled in
that session:

```jsonc
{
  "mcpServers": {
    "alloyium": {
      "command": "bun",
      "args": ["./webhook.ts"],
      "env": {
        "A2A_ENABLED": "1",
        "A2A_AGENT_ID": "scout-1",
        "A2A_SIG_ALG": "ed25519",
        "A2A_SIGNING_KEY": "/abs/path/a2a/scout-1.seed",
        "A2A_NKEY": "/abs/path/a2a/scout-1.nk"
      }
    }
  }
}
```

(For a Track A agent, drop `A2A_NKEY` and add `"A2A_TRANSPORT_AUTH": "none"`.)

### Using the messaging tools in-session

With messaging enabled, the session gets the agent-to-agent tools:

| Tool | What it does |
|---|---|
| `a2a_send` | Send a direct message to a peer, or broadcast to a topic. |
| `a2a_peers` | List peer agents currently alive (by presence). |
| `a2a_join_topic` | Subscribe this agent to a broadcast topic. |
| `a2a_leave_topic` | Unsubscribe from a topic. |

**Send a direct message** (the bus stamps `from` / `id` / `ts` / `sig` — you can't
spoof identity):

```jsonc
a2a_send { "to": "pm-dev", "body": "part 2 of the audit-log rollout is ready for review" }
```

**Ask a question and invite a reply:**

```jsonc
a2a_send { "to": "pm-dev", "type": "request", "body": "merge now or hold for release 1.4?" }
// pm-dev receives it, then replies with the request's id as corr:
a2a_send { "to": "scout-1", "type": "reply", "corr": "<the request's id>", "body": "hold for 1.4" }
```

**Broadcast to a topic plane** (both agents must join it first):

```jsonc
a2a_join_topic { "topic": "dev" }
a2a_send { "to": "topic:dev", "body": "nightly build is green" }
```

**See who's online:** `a2a_peers {}` → `{ "self": "scout-1", "peers": [ … ] }`.

### What an incoming message looks like

Peer messages arrive in the session as a channel event:

```
<channel source="alloyium" feed="a2a" kind="direct" from="pm-dev"
         to="scout-1" type="request" id="…" ts="…">take part 2 of the audit-log rollout</channel>
```

`feed="a2a"` means a peer agent is talking to you, attributed by `from`. A `type="request"`
invites a reply (with the request's `id` as `corr`) — replying is optional. Messages sent
while a peer is offline are delivered when it comes back: the bus stream retains them
(default 24h, tunable via `A2A_STREAM_MAX_AGE_H`).

---

## 5. Operations & troubleshooting

**Rotate a key.** `bun run onboard scout-1 --force` mints a new keypair and overwrites
the Redis pubkey; restart the agent against the new key files. If the nkey also rotated
(Track B), update it in `nats-server.conf` and `nats-server --signal reload`.

**Seed topic membership.** Membership is per-agent and self-served via `a2a_join_topic`
/ `a2a_leave_topic`. An operator can also pre-seed it by writing
`alloyium:a2a:topics:<id>` in Redis (a JSON array of topic names) so an agent starts
already joined to its planes.

**Direct message encryption.** `A2A_DIRECT_ENCRYPTION=opportunistic` is the default:
direct sends encrypt only when the recipient advertises decrypt capability under
`alloyium:a2a:direct-enc:<id>`. Use `required` for senders that must fail instead
of falling back to signed plaintext. Topic traffic remains signed plaintext.

**Won't start?** Check stderr:

- `a2a_config_invalid` — `A2A_AGENT_ID` missing or not `^[a-z0-9-]{1,64}$`.
- `a2a_creds_required` — hardened (nkey) mode with no `A2A_NKEY` / `A2A_CREDS`; use
  `--transport none` or onboard first.
- `a2a_signing_key_required` — ed25519 mode with no `A2A_SIGNING_KEY`.
- `a2a_duplicate_agent_id` — another live instance already holds this id; the agent
  stays off the bus and retries. Don't run two agents with the same `A2A_AGENT_ID`.

**Messages dropped silently on the receiver?** stderr counters/events:

- `a2a_badsig` — signature didn't verify (wrong/missing key, or the sender's pubkey
  isn't in Redis). Confirm both agents onboarded against the **same** Redis.
- `a2a_sig_downgrade` — the sender's `alg` doesn't match this receiver's required
  `A2A_SIG_ALG`.
- `a2a_direct_decrypt_failed` — an encrypted direct envelope verified and routed to
  this inbox but could not be decrypted; check recipient seed availability and key
  rotation timing.

---

## 6. Security model

- **Signed identity.** Every message is an ed25519-signed envelope. The bus stamps
  `from` / `id` / `ts` / `sig`, and peers verify each one against the sender's Redis
  pubkey — so an in-session model **cannot spoof another agent**.
- **Direct-message encryption.** Direct-to-agent envelopes can replace `body` with
  AES-GCM ciphertext plus `enc` metadata. The recipient derives its X25519 static
  secret from the local Ed25519 seed; senders derive the recipient X25519 public key
  from the Redis Ed25519 pubkey and still sign the encrypted envelope.
- **One audited publish path.** An agent can only ever publish under its own
  `alloyium.a2a.>` namespace, through a single allowlisted call site. This is enforced
  in-process and **never turns off**, in any mode. In the hardened track, the per-agent
  nkey scopes the same restriction at the NATS server (just its subjects + its own
  stream's JetStream API — never a broad `$JS.API.>`).
- **Secrets stay local.** The `*.seed` and `*.nk` files are secrets, written `0600` —
  **never commit or share them**. The `*.pub` file and the Redis pubkey are public. The
  default output dir (`./a2a`) and any `*.seed` / `*.nk` / `*.a2a.env` files are
  git-ignored so keys can't be committed by accident — for production, keep them
  outside the repo entirely.
- **Pubkey squatting.** Self-registration is `SET NX` (first-write-wins): a fresh id
  claims its own key, and a *different* key for the same id is refused without `--force`.
  Convenient, and safe against silent takeover — but a Redis writer could squat an
  **unclaimed** id. In a hardened fleet, write-restrict the `alloyium:a2a:pubkey:*`
  namespace in Redis and have an operator run onboarding.
- **Your fleet, your rules.** Self-hosted — the agents, the bus, and the keys are yours.
