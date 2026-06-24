# syntax=docker/dockerfile:1.7

FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/home/bun
ARG CLAUDE_CLI_VERSION=2.1.177
# Claude Code needs a current Node LTS — install Node 20 from NodeSource (Debian's apt node is older).
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    openssh-client \
    procps \
    redis-tools \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && npm install -g "@anthropic-ai/claude-code@${CLAUDE_CLI_VERSION}" \
  && npm cache clean --force \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN chown bun:bun /app

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile
USER root

COPY --chown=bun:bun . .
# No baked credentials: the gateway drives the user's OWN logged-in `claude` CLI session,
# mounted at runtime (see compose CLAUDE_HOST_HOME / CLAUDE_SSH_DIR). The child runs with NO
# tools and NO MCP (pure inference), so there is no browser/codex tooling to bake in. Mark
# mounted repos as git-safe so git never warns on a foreign-owned checkout.
RUN git config --global --add safe.directory '*'
COPY --chown=bun:bun docker/gateway-claude/entrypoint.sh /usr/local/bin/claude-gw-entrypoint
RUN chmod 755 /usr/local/bin/claude-gw-entrypoint

ENTRYPOINT ["claude-gw-entrypoint"]
CMD ["bun", "claude_gateway.ts"]
