#!/bin/sh
# Remap the image's built-in `bun` user/group to the host user's uid/gid.
#
# The base image (oven/bun:*-debian) bakes `bun`, /home/bun and /app at uid/gid 1000.
# When the host user is not 1000, bind-mounted host creds and the baked image dirs end up
# owned by different identities and the agent cannot read/write them. Running this once at
# build time (as root, before any `chown bun` / `COPY --chown=bun` / `bun install`) makes
# the whole image agree with the host user.
#
# Driven by the CC_UID / CC_GID build args. Defaults (1000) are a no-op, so upstream
# behaviour is unchanged unless a non-1000 host uid/gid is supplied.
set -eu

uid="${CC_UID:-1000}"
gid="${CC_GID:-1000}"

if [ "${gid}" != "$(id -g bun)" ]; then
  groupmod -g "${gid}" bun
fi
if [ "${uid}" != "$(id -u bun)" ]; then
  usermod -u "${uid}" bun
fi

# usermod -u does not re-own existing files; ensure the remapped user owns its HOME.
# /app and other bun-owned paths are (re)chowned by the Dockerfile's own `bun`-named
# steps that run after this script, so they retarget automatically.
chown -R "${uid}:${gid}" /home/bun
