FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
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

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile

COPY --chown=bun:bun . .

CMD ["bun", "run", "selftest"]
