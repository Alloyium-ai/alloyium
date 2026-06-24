//! Shim configuration from the environment (the entire shim config; see §F / a2a-launch.sh A2A_MODE=shim).
use std::env;
use std::time::Duration;

use thiserror::Error;

// Default timer values (production). Each is overridable via the matching
// `A2A_*_MS` environment variable; the defaults equal the §A-v3 wire behavior,
// so production is unchanged when the vars are absent. The launcher contract
// (A2A_AGENT_ID / A2A_SIGNING_KEY / A2A_SIG_ALG / A2A_CORE_SOCK / SUBS_KEY) is
// untouched — these are optional tuning knobs (also used by the conformance
// harness to drive the timers fast).
const DEFAULT_HELLO_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MCP_REQUEST_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_RECONNECT_MIN_MS: u64 = 100;
const DEFAULT_RECONNECT_MAX_MS: u64 = 2_000;
const DEFAULT_PING_INTERVAL_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub struct Config {
    pub agent_id: String,
    pub signing_key_path: String,
    pub sig_alg: String,
    pub core_sock: String,
    pub subs_key: String,
    pub tool_only: bool,
    pub hello_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub mcp_request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub reconnect_min_ms: u64,
    pub reconnect_max_ms: u64,
    pub ping_interval_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Config, ConfigError> {
        let agent_id =
            env::var("A2A_AGENT_ID").map_err(|_| ConfigError::MissingVar("A2A_AGENT_ID"))?;
        let signing_key_path =
            env::var("A2A_SIGNING_KEY").map_err(|_| ConfigError::MissingVar("A2A_SIGNING_KEY"))?;
        let sig_alg = env::var("A2A_SIG_ALG").unwrap_or_else(|_| "ed25519".to_owned());
        let core_sock =
            env::var("A2A_CORE_SOCK").unwrap_or_else(|_| "/run/a2a-core/core.sock".to_owned());
        let subs_key =
            env::var("SUBS_KEY").unwrap_or_else(|_| "alloyium:subscriptions".to_owned());
        let tool_only = env_bool("A2A_TOOL_ONLY") || env_bool("A2A_SHIM_TOOL_ONLY");

        Ok(Config {
            agent_id,
            signing_key_path,
            sig_alg,
            core_sock,
            subs_key,
            tool_only,
            hello_timeout_ms: env_ms("A2A_HELLO_TIMEOUT_MS", DEFAULT_HELLO_TIMEOUT_MS),
            request_timeout_ms: env_ms("A2A_REQUEST_TIMEOUT_MS", DEFAULT_REQUEST_TIMEOUT_MS),
            mcp_request_timeout_ms: env_ms(
                "A2A_MCP_REQUEST_TIMEOUT_MS",
                DEFAULT_MCP_REQUEST_TIMEOUT_MS,
            ),
            connect_timeout_ms: env_ms("A2A_CONNECT_TIMEOUT_MS", DEFAULT_CONNECT_TIMEOUT_MS),
            reconnect_min_ms: env_ms("A2A_RECONNECT_MIN_MS", DEFAULT_RECONNECT_MIN_MS),
            reconnect_max_ms: env_ms("A2A_RECONNECT_MAX_MS", DEFAULT_RECONNECT_MAX_MS),
            ping_interval_ms: env_ms("A2A_PING_INTERVAL_MS", DEFAULT_PING_INTERVAL_MS),
        })
    }

    pub fn hello_timeout(&self) -> Duration {
        Duration::from_millis(self.hello_timeout_ms)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn mcp_request_timeout(&self) -> Duration {
        Duration::from_millis(self.mcp_request_timeout_ms)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    pub fn reconnect_min(&self) -> Duration {
        Duration::from_millis(self.reconnect_min_ms)
    }

    pub fn reconnect_max(&self) -> Duration {
        Duration::from_millis(self.reconnect_max_ms)
    }

    /// Ping cadence. Floored at 1ms: `tokio::time::interval(0)` panics and a
    /// zero ping interval is never useful.
    pub fn ping_interval(&self) -> Duration {
        Duration::from_millis(self.ping_interval_ms.max(1))
    }
}

/// Parse an unsigned-millis env var, falling back to `default` when unset or
/// unparseable.
fn env_ms(var: &str, default: u64) -> u64 {
    match env::var(var) {
        Ok(raw) => raw.trim().parse::<u64>().unwrap_or(default),
        Err(_) => default,
    }
}

fn env_bool(var: &str) -> bool {
    match env::var(var) {
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable {0}")]
    MissingVar(&'static str),
}
