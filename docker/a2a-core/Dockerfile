FROM rust:1-slim-bookworm AS a2a-shim-builder

WORKDIR /src/a2a-shim
COPY a2a-shim/Cargo.toml a2a-shim/Cargo.lock ./
COPY a2a-shim/src ./src
RUN cargo build --release --locked

FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    gosu \
    git \
    procps \
    redis-tools \
  && rm -rf /var/lib/apt/lists/*

# Remap the baked `bun` user to the host uid/gid (build args; default 1000 = no-op).
ARG CC_UID=1000
ARG CC_GID=1000
COPY docker/lib/uidremap.sh /usr/local/sbin/uidremap
RUN sh /usr/local/sbin/uidremap

WORKDIR /app
RUN chown bun:bun /app
COPY --from=a2a-shim-builder /src/a2a-shim/target/release/a2a-shim /usr/local/bin/a2a-shim

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile

COPY --chown=bun:bun . .

CMD ["bun", "a2a_core.ts"]
