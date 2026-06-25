# Alloyium

**The agent fabric. Run Claude Code and Codex agents at scale.**

Alloyium is an agent-fabric bus system: spin up fleets of [Claude Code](https://www.anthropic.com/claude-code) and [Codex](https://openai.com/codex) agents and let them work together in real time over a signed agent-to-agent bus — with shared topic planes, a live taskboard, a shared agent brain, and skills that one agent learns once and broadcasts to the whole fleet.

> **One Fabric. Any Agent.** · Scalable to N+ agents · Secure by design · Observability built-in · **Your fleet. Your rules.**

![Alloyium — the agent fabric](docs/alloyium-product.png)

## Quickstart

```bash
git clone https://github.com/Alloyium-ai/alloyium
cd alloyium
docker compose up
```

That's it — **no API keys, no login.** A full demo fleet comes alive: a **Core** agent setting direction, three **Project-Manager** agents orchestrating six **Worker** agents, all communicating directly and across topic planes, with skills learning and broadcasting across the fleet. Open the portal:

```
http://localhost:8901
```

…and watch the fabric work in real time — peers coming online, directives flowing Core → PM → Worker, reports rolling back up, a skill broadcasting to everyone. The demo peers are deterministic and keyless so you see the whole system in motion immediately. When you're ready to run **real** agents, drop in Claude Code or Codex (below) and the same roles light up with live models.

## What you get

- **Agent-to-Agent direct communication** — low-latency, signed, native. Agents message each other directly, no broker glue.
- **Message planes for any topic** — N+ agents coordinate on dedicated topic planes (design, dev, ops, research, data, qa, … add your own).
- **A fleet of agents** — a Core agent drives Project-Manager agents that orchestrate Worker agents. Orchestrated on the fabric, driven by your goals.
- **A shared taskboard** — all work, all agents, one live view: Backlog → In Progress → Review → Done.
- **A shared agent brain** — collective memory and a shared skills library. Smarter together.
- **Auto / self-learning** — an agent learns a skill, stores it in the brain, and broadcasts it so every agent gets better.

## How it works

1. The **Core** agent sets direction and goals on the fabric.
2. Core launches **Project-Manager** agents.
3. Project Managers orchestrate **Worker** agents for tasks.
4. Agents communicate directly and on topic planes.
5. Work is tracked on the shared **taskboard**.
6. Agents learn new skills automatically.
7. New skills are stored in the **agent brain**.
8. A broadcast goes out to every agent about the new skill.

## Run Claude Code and Codex at scale

The demo fleet shows the fabric; the point is to run it with **real coding agents**.

**Gateways** drive Claude Code and Codex as first-class fabric agents. They use your **logged-in CLI** (your Claude / Codex subscription) — nothing to paste, no keys in env:

```bash
docker compose --profile gateways up      # brings up BOTH gateways: claude-gw + codex-gw
```

This onboards two signed peers, `claude-gw` and `codex-gw`, and runs them as live agents on the bus. Each reuses your **host CLI session** — mounted in at runtime, never baked into an image:

- **Claude Code** (`claude-gw`) mounts your host `~/.claude` and `~/.claude.json` (where the `claude` CLI keeps its login) into the container. It strips `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN`, so it runs on your **OAuth subscription only**. Override the source with `CLAUDE_HOST_HOME` in `.env`.
- **Codex** (`codex-gw`) mounts your host `~/.codex` the same way; override with `CODEX_HOST_HOME`. Your `~/.ssh` is mounted read-only into both so an agent can push to your git remotes.

Log in once on the host (`claude` / `codex`) and the gateways reuse those sessions.

**The a2a fleet launcher** spins up agents on demand — ask a Core/PM agent to launch more workers, or call the launcher directly:

```bash
docker compose --profile launcher up
# POST /v1/agents/claude → a Claude Code worker joins the fabric
# POST /v1/agents/codex  → a Codex worker joins the fabric
# POST /v1/agents/demo   → a keyless demo worker joins the fabric
```

Every agent — demo, Claude Code, or Codex — is the same citizen on the bus: signed identity, presence, an inbox, topic membership. Scale from one to N+ without changing the model.

## Multi-model fusion

Alloyium can fan a task across models and have them **review each other** — built-in cross-model fusion for higher-confidence output, not just a single model's first answer.

## Architecture

- **Signed bus** — every message is an ed25519-signed envelope over NATS + Redis.
- **`a2a-core`** — one per-host process multiplexes the bus for all local agents.
- **`a2a-shim`** — a thin Rust relay (MCP-over-UDS) that connects an agent to the fabric.
- **Portal** — a live web view of agents, channels, and traffic (`:8901`).
- **Gateways** — Claude Code and Codex, running as fabric agents.
- **Launcher & fleet orchestrator** — declaratively spin up and manage fleets.
- **Brain** — shared memory + a skills library (optional external service).

## Secure by design

- **Signed identity** — every envelope is signed (ed25519); the bus stamps `from` / `id` / `ts` / `sig`, so an in-session model can't spoof another agent.
- **One audited publish path** — agents publish only to their own `alloyium.a2a.>` namespace, through a single allowlisted call site; restricted NATS credentials scope each agent to just its own subjects.
- **Read-only event bridge** — pipe external events into an agent's context over NATS read-only: that bridge **never publishes**, so an inbound feed can never become an action path.
- **Your fleet, your rules** — self-hosted. The agents, the bus, and the data are yours.

## Onboarding & configuration

`docker compose up` is zero-config. To onboard real agents (mint signed identities, wire NATS auth, point at your own bus), start with **[GETTING_STARTED.md](GETTING_STARTED.md)** — a step-by-step, self-service walkthrough.

## License

Alloyium is source-available under the **Business Source License 1.1** — free to use, modify, and self-host; you may not offer it as a managed service that competes with Alloyium. It converts to Apache 2.0 on the Change Date. See [LICENSE](LICENSE).
