# syntax=docker/dockerfile:1.7

FROM rust:1-slim-bookworm AS a2a-shim-builder

WORKDIR /src/a2a-shim
COPY a2a-shim/Cargo.toml a2a-shim/Cargo.lock ./
COPY a2a-shim/src ./src
RUN cargo build --release --locked

FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/home/bun
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ARG CODEX_CLI_VERSION=0.141.0
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    nodejs \
    npm \
    openssh-client \
    procps \
    redis-tools \
  && npm install -g "@openai/codex@${CODEX_CLI_VERSION}" \
  && npm cache clean --force \
  && rm -rf /var/lib/apt/lists/*

# Remap the baked `bun` user to the host uid/gid (build args; default 1000 = no-op).
ARG CC_UID=1000
ARG CC_GID=1000
COPY docker/lib/uidremap.sh /usr/local/sbin/uidremap
RUN sh /usr/local/sbin/uidremap

WORKDIR /app
RUN mkdir -p /ms-playwright \
  && chown -R bun:bun /app /ms-playwright
COPY --from=a2a-shim-builder /src/a2a-shim/target/release/a2a-shim /usr/local/bin/a2a-shim

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile
USER root
RUN bunx playwright install-deps chromium \
  && rm -rf /var/lib/apt/lists/*
USER bun
RUN bunx playwright install chromium

COPY --chown=bun:bun . .
# No baked credentials: the gateway uses the user's own codex/model auth + SSH keys mounted at
# runtime (see compose CODEX_HOST_HOME / CODEX_SSH_DIR). Mark mounted repos as git-safe so codex
# can run git in them without ownership warnings.
RUN git config --global --add safe.directory '*'
COPY --chown=bun:bun docker/gateway-codex/entrypoint.sh /usr/local/bin/codex-gw-entrypoint
RUN chmod 755 /usr/local/bin/codex-gw-entrypoint

ENTRYPOINT ["codex-gw-entrypoint"]
CMD ["bun", "codex_gateway.ts"]
