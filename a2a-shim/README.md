# a2a-shim

`a2a-shim` is a thin per-agent MCP-over-UDS relay between a stdio MCP client and
`a2a-core`.

It replaces `bun webhook.ts` in the path:

```text
claude <-> stdio MCP <-> a2a-shim <-> UDS <-> a2a-core
```

The shim is advisory/relay only. It holds zero bus connections, zero database
connections, and no tool logic. It has no capital or fire authority.

It builds to the frozen §A wire contract from
`brain ops-specs/specs/2026-06-17-mcp-shim-ph2-3-acceptor-protocol`.

## Build

```sh
cargo build --release --target x86_64-unknown-linux-musl
```

The release profile is tuned for a small static-musl binary.

## Environment

Required:

- `A2A_AGENT_ID`: agent id advertised during hello.
- `A2A_SIGNING_KEY`: path to ed25519 seed material.

Optional:

- `A2A_SIG_ALG`: signature algorithm, default `ed25519`.
- `A2A_CORE_SOCK`: core Unix socket, default `/run/a2a-core/core.sock`.
- `SUBS_KEY`: subscription key, default `alloyium:subscriptions`.

## Precondition Probe

```sh
a2a-shim --ping [sock]
```

When `sock` is omitted, the shim uses `A2A_CORE_SOCK` from `Config::from_env()`.
The probe is the launcher-pm §A.7 framed CTRL `ping` -> `pong` health check.

## Status

This package is scaffold only. Wire logic, framing bytes, handshake execution,
signing, pumps, reconnect, and conformance vector execution are intentionally
stubbed for later implementation phases.

## License

`a2a-shim` is part of Alloyium core and is licensed under AGPL-3.0-or-later. See
[`../LICENSE`](../LICENSE).
