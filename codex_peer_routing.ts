// Shared Codex peer routing predicates.
//
// Batch/one-off codex.job.request.v1 traffic should target full gateway peers,
// not codex realtime-session identities. Realtime peers accept the codex.session.*
// contract instead.

const CODEX_JOB_RECIPIENTS = [
  /^codex-gw(?:-\d+)?$/,
  /^codex-[a-z0-9-]+$/,
  /^codex-gw-sub-[a-z0-9-]+$/,
  /^host-ops-gw(?:-[a-z0-9-]+)?$/,
]

const CODEX_REALTIME_SESSION_RECIPIENTS = [
  /^codex-rt(?:-|$)/,
  /(?:^|-)realtime-session(?:-|$)/,
  /(?:^|-)rt-session(?:-|$)/,
]

export function isCodexRealtimeSessionPeer(agentId: string): boolean {
  const id = agentId.trim().toLowerCase()
  return !!id && CODEX_REALTIME_SESSION_RECIPIENTS.some((re) => re.test(id))
}

export function isCodexJobRecipient(agentId: string): boolean {
  const id = agentId.trim().toLowerCase()
  if (!id || id.startsWith('topic:') || isCodexRealtimeSessionPeer(id)) return false
  return CODEX_JOB_RECIPIENTS.some((re) => re.test(id))
}
