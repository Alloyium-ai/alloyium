#!/bin/sh
set -eu

# Thin entrypoint. The gateway authenticates with the user's OWN logged-in `claude` CLI
# session provided at runtime (mount ~/.claude via CLAUDE_HOST_HOME, ~/.claude.json, and
# ~/.ssh via CLAUDE_SSH_DIR). Nothing is baked into the image: the child drives the OAuth
# subscription only (ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN are stripped before spawn).

exec "$@"
