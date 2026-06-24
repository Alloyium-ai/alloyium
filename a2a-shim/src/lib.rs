#![forbid(unsafe_code)]
//! `a2a-shim` library — the modules shared by the binary and the conformance tests.
//!
//! Thin per-agent MCP-over-UDS relay (replaces `bun webhook.ts`):
//! `claude ⇄ stdio MCP ⇄ shim ⇄ UDS ⇄ a2a-core`. Holds zero bus/DB connections and no
//! tool logic; advisory/relay only, no capital/fire authority. Builds to the FROZEN §A-v3
//! wire contract: brain `ops-specs/specs/2026-06-17-mcp-shim-ph2-3-acceptor-protocol`.

pub mod app;
pub mod config;
pub mod ctrl;
pub mod framing;
pub mod hello;
pub mod mcp_pump;
pub mod resilience;
pub mod signer;
pub mod transport;
