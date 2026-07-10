# Codex app-server protocol bindings (generated — ground truth)

TypeScript bindings for the `codex app-server` JSON-RPC protocol, the canonical
ground truth for any codex-gateway work. Regenerate with:

    codex app-server generate-ts --out codex-protocol

Key methods (verified to exist): `initialize`, `thread/start`, `thread/resume`,
`thread/fork`, `turn/start`, `turn/steer`, `turn/interrupt`, `account/rateLimits/read`,
`account/usage/read`. Streaming notifications: `thread/started`, `turn/started`,
`item/started`, `item/agentMessage/delta`, `item/completed`, `turn/completed`.
