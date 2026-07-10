# Getting Started

This public export is a localhost-first developer tree. It is not wired to any
private fleet or hosted control plane.

## Prerequisites

- Bun
- Rust/Cargo for `a2a-shim`
- Docker, unless you already have Redis and NATS with JetStream enabled

## Basic Flow

Fastest path:

```bash
make init
make deps
make demo
```

`make demo` starts local Redis and NATS containers, generates throwaway
identities under `.alloyium/demo`, starts a PM, three team peers, and a
fusion-panel peer, then runs a signed request/reply workflow across the A2A bus.

To keep the local fleet up:

```bash
make fleet-up
```

Use `make fleet-status` to print live peers and advertised features. Use
`make fleet-down` to remove the demo bus containers and `make demo-clean` to
remove generated demo state.

Manual flow:

1. Start local Redis and NATS with JetStream.
2. Install dependencies with `bun install`.
3. Generate or onboard a local test agent with `bun onboard.ts`.
4. Run the TypeScript services or tests against local endpoints.

Use environment variables to point at non-default local services:

```bash
NATS_URL=nats://127.0.0.1:4222
REDIS_URL=redis://127.0.0.1:6379
```

Do not commit generated keys, `.env` files, local databases, or runtime logs.
