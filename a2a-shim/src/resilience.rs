use tokio::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u32);

/// Exponential reconnect backoff: `min * 2^attempt`, capped at `max`. Defaults
/// (A2A_RECONNECT_MIN_MS / A2A_RECONNECT_MAX_MS) are 100ms → 2s per §A.6.
pub fn backoff(attempt: u32, min: Duration, max: Duration) -> Duration {
    let shift = attempt.min(5);
    let base = min.as_millis() as u64;
    let millis = base.saturating_mul(1_u64 << shift);
    Duration::from_millis(millis).min(max)
}

pub fn should_drop_late_reply(current: Epoch, reply: Epoch) -> bool {
    reply.0 < current.0
}

/// Timeout classes are intentionally simple: normal MCP requests get the
/// `normal` budget, while known streaming/RAG surfaces get the longer `long`
/// budget. Both are configurable (A2A_REQUEST_TIMEOUT_MS /
/// A2A_MCP_REQUEST_TIMEOUT_MS); production defaults are 30s / 120s.
pub fn timeout_budget(method: &str, normal: Duration, long: Duration) -> Duration {
    let method = method.to_ascii_lowercase();
    if method.contains("kai_send") || method.contains("brain") || method.contains("recall") {
        long
    } else {
        normal
    }
}
