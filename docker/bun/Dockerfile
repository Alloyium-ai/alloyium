FROM oven/bun:1.3.14-debian

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    gosu \
    git \
    procps \
    redis-tools \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN chown bun:bun /app

COPY --chown=bun:bun package.json bun.lock ./
USER bun
RUN bun install --frozen-lockfile

COPY --chown=bun:bun . .

CMD ["bun", "--version"]
