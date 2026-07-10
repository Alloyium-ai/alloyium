use tokio::time::Duration;

const INBOX_WAIT_DEFAULT_TIMEOUT_MS: i64 = 300_000;
const INBOX_WAIT_MAX_TIMEOUT_MS: i64 = 600_000;

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

pub fn inbox_wait_budget(
    action: &str,
    timeout_ms: Option<i64>,
    grace: Duration,
    max: Duration,
) -> Option<Duration> {
    if action != "wait" {
        return None;
    }

    let timeout_ms = timeout_ms.unwrap_or(INBOX_WAIT_DEFAULT_TIMEOUT_MS);
    if !(1..=INBOX_WAIT_MAX_TIMEOUT_MS).contains(&timeout_ms) {
        return None;
    }

    let timeout = Duration::from_millis(timeout_ms as u64);
    Some(timeout.saturating_add(grace).min(max))
}

#[cfg(test)]
mod tests {
    use super::inbox_wait_budget;
    use tokio::time::Duration;

    #[test]
    fn inbox_wait_budget_uses_requested_timeout_plus_grace() {
        assert_eq!(
            inbox_wait_budget(
                "wait",
                Some(150_000),
                Duration::from_millis(5_000),
                Duration::from_millis(605_000),
            ),
            Some(Duration::from_millis(155_000))
        );
    }

    #[test]
    fn inbox_wait_budget_defaults_missing_timeout() {
        assert_eq!(
            inbox_wait_budget(
                "wait",
                None,
                Duration::from_millis(5_000),
                Duration::from_millis(605_000),
            ),
            Some(Duration::from_millis(305_000))
        );
    }

    #[test]
    fn inbox_wait_budget_rejects_invalid_or_non_wait_inputs() {
        let grace = Duration::from_millis(5_000);
        let max = Duration::from_millis(605_000);

        assert_eq!(inbox_wait_budget("wait", Some(600_001), grace, max), None);
        assert_eq!(inbox_wait_budget("wait", Some(0), grace, max), None);
        assert_eq!(inbox_wait_budget("wait", Some(-1), grace, max), None);
        assert_eq!(inbox_wait_budget("list", Some(150_000), grace, max), None);
        assert_eq!(inbox_wait_budget("read", None, grace, max), None);
        assert_eq!(inbox_wait_budget("ack", None, grace, max), None);
    }

    #[test]
    fn inbox_wait_budget_clamps_to_max() {
        assert_eq!(
            inbox_wait_budget(
                "wait",
                Some(600_000),
                Duration::from_millis(5_000),
                Duration::from_millis(602_000),
            ),
            Some(Duration::from_millis(602_000))
        );
    }
}
