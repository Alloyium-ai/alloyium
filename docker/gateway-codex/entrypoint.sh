#!/bin/sh
set -eu

# Thin entrypoint. The gateway authenticates with the user's OWN codex/model credentials
# provided at runtime (mount ~/.codex via CODEX_HOST_HOME, ~/.ssh via CODEX_SSH_DIR).
# Nothing is baked into the image and no internal git remote is configured.

exec "$@"
