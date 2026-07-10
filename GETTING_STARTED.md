# Getting Started

This public export is a localhost-first developer tree. It is not wired to any
private fleet or hosted control plane.

## Prerequisites

- Bun
- Rust/Cargo for `a2a-shim`
- Redis
- NATS with JetStream enabled

## Basic Flow

1. Start local Redis and NATS.
2. Install dependencies with `bun install`.
3. Generate or onboard a local test agent with `bun onboard.ts`.
4. Run the TypeScript services or tests against local endpoints.

Use environment variables to point at non-default local services:

```bash
NATS_URL=nats://127.0.0.1:4222
REDIS_URL=redis://127.0.0.1:6379
```

Do not commit generated keys, `.env` files, local databases, or runtime logs.
