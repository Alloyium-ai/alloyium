# syntax=docker/dockerfile:1.7

# ── a2a-shim builder ─────────────────────────────────────────────────────────
# The thin Rust relay (MCP-over-UDS) that connects this agent to the shared
# a2a-core. Same binary the codex gateway and a2a-core build.
FROM rust:1-slim-bookworm AS a2a-shim-builder
WORKDIR /src/a2a-shim
COPY a2a-shim/Cargo.toml a2a-shim/Cargo.lock ./
COPY a2a-shim/src ./src
RUN cargo build --release --locked

# ── conversational Claude agent ──────────────────────────────────────────────
# A FIRST-CLASS fabric peer (not the claude-gw inference gateway): a real `claude`
# session that holds the a2a tools AND receives channel injection in-session, so it
# talks to other agents. Drives the user's OWN logged-in OAuth session — no API key.
FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/home/bun
# Headless image: silence the background autoupdater (and its "~/.local/bin/claude
# missing" warning) — the version is pinned via npm. Official knob: DISABLE_AUTOUPDATER=1.
ENV DISABLE_AUTOUPDATER=1
ARG CLAUDE_CLI_VERSION=2.1.191
# Claude Code needs a current Node LTS (NodeSource 20 — Debian's apt node is older).
# tmux gives the headless `claude` session its pty; redis-tools for the presence check.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    openssh-client \
    procps \
    redis-tools \
    tmux \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && npm install -g "@anthropic-ai/claude-code@${CLAUDE_CLI_VERSION}" \
  && npm cache clean --force \
  && rm -rf /var/lib/apt/lists/*

COPY --from=a2a-shim-builder /src/a2a-shim/target/release/a2a-shim /usr/local/bin/a2a-shim

WORKDIR /app
RUN chown bun:bun /app

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile
USER root

COPY --chown=bun:bun . .
# No baked credentials: the agent drives the user's OWN logged-in `claude` CLI session,
# mounted at runtime (see compose CLAUDE_HOST_HOME / ~/.claude.json / CLAUDE_SSH_DIR). It
# runs on the OAuth subscription only (ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN are stripped
# before spawn). Mark mounted repos git-safe so git never warns on a foreign-owned checkout.
RUN git config --global --add safe.directory '*'
COPY --chown=bun:bun docker/agent-claude/entrypoint.sh /usr/local/bin/claude-agent-entrypoint
RUN chmod 755 /usr/local/bin/claude-agent-entrypoint

USER bun
ENTRYPOINT ["claude-agent-entrypoint"]
