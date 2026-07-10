// a2a_portal_reply_watch — detached reply-watch loop for the portal's async send path.
//
// Extracted from a2a_portal.ts so the poll/timeout loop is unit-testable with an injected
// clock and no bus. Preserves the old waitForPortalReply semantics exactly, minus the lock:
// check-before-sleep (a reply already present returns with waited_ms ≈ 0 and zero sleeps),
// a poll floor of max(50, pollMs), and a 'disabled' short-circuit when timeoutMs <= 0.
// The caller owns event emission; this module has no side effects beyond findReply/sleep.

export type DeliveryWait =
  | { status: 'reply'; reply_id: string; waited_ms: number }
  | { status: 'timeout'; timeout_ms: number }
  | { status: 'disabled' }

export type WatchForReplyOpts = {
  findReply: () => { id: string } | null
  timeoutMs: number
  pollMs: number
  sleep?: (ms: number) => Promise<void> // default Bun.sleep
  now?: () => number // default Date.now
}

export async function watchForReply(opts: WatchForReplyOpts): Promise<DeliveryWait> {
  const { findReply, timeoutMs, pollMs } = opts
  if (timeoutMs <= 0) return { status: 'disabled' }
  const now = opts.now ?? Date.now
  const sleep = opts.sleep ?? Bun.sleep
  const interval = Math.max(50, pollMs) // poll floor preserved from waitForPortalReply
  const started = now()
  const deadline = started + timeoutMs
  while (now() < deadline) {
    const reply = findReply() // checked before every sleep, so an existing reply returns instantly
    if (reply) return { status: 'reply', reply_id: reply.id, waited_ms: now() - started }
    await sleep(interval)
  }
  return { status: 'timeout', timeout_ms: timeoutMs }
}
