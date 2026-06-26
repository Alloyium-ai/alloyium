import {
  CODEX_SESSION_EVENT_SCHEMA,
  CODEX_SESSION_INPUT_SCHEMA,
  CODEX_SESSION_READY_SCHEMA,
  CODEX_TURN_COMPLETED_SCHEMA,
  CODEX_TURN_FAILED_SCHEMA,
  CODEX_TURN_INTERRUPTED_SCHEMA,
  CODEX_TURN_STARTED_SCHEMA,
} from './codex_realtime.ts'

export type PortalSendType = 'msg' | 'request' | 'reply'
export type PortalSendMode = 'chat' | 'one-off'

export type PortalSendArgs =
  & { to: string; body: string; type: PortalSendType }
  & Partial<{ thread: string; corr: string; ttl_ms: number }>

export type PortalSendBuildResult =
  | { ok: true; args: PortalSendArgs; sendMode: PortalSendMode; chatContext: string | null }
  | { ok: false; error: string }

const SEND_TYPES = new Set<PortalSendType>(['msg', 'request', 'reply'])
const SEND_MODES = new Set<PortalSendMode>(['chat', 'one-off'])
const CODEX_JOB_RECIPIENTS = [
  /^codex-gw(?:-\d+)?$/,
  /^codex-[a-z0-9-]+$/,
  /^codex-gw-sub-[a-z0-9-]+$/,
  /^host-ops-gw(?:-[a-z0-9-]+)?$/,
]
const HOST_OPS_RE = /^host-ops-gw(?:-[a-z0-9-]+)?$/
const LOCAL_HOST_OPS_RE = /^host-ops-gw$/

export function normalizePortalRecipient(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const raw = value.trim()
  if (!raw) return null
  if (raw.startsWith('@')) return raw.slice(1).trim().toLowerCase() || null
  if (raw.startsWith('#')) {
    const topic = raw.slice(1).trim().toLowerCase()
    return topic ? `topic:${topic}` : null
  }
  if (raw.toLowerCase().startsWith('topic:')) {
    const topic = raw.slice('topic:'.length).trim().toLowerCase()
    return topic ? `topic:${topic}` : null
  }
  return raw.toLowerCase()
}

export function isSelfPortalRecipient(recipient: unknown, portalAgentId: string): boolean {
  const to = normalizePortalRecipient(recipient)
  const self = normalizePortalRecipient(portalAgentId)
  return !!to && !!self && to === self
}

export function buildPortalSendArgs(input: unknown): PortalSendBuildResult {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return { ok: false, error: 'bad_request' }
  const o = input as Record<string, unknown>
  const to = normalizePortalRecipient(o.to)
  if (!to) return { ok: false, error: 'bad_recipient' }

  const body = typeof o.body === 'string' ? o.body : ''
  if (!body.trim()) return { ok: false, error: 'empty_body' }

  const type = (typeof o.type === 'string' ? o.type : 'msg') as PortalSendType
  if (!SEND_TYPES.has(type)) return { ok: false, error: 'bad_type' }

  const sendMode = normalizePortalSendMode(o.send_mode ?? o.sendMode)
  if (!sendMode) return { ok: false, error: 'bad_send_mode' }

  const chatContext = normalizeChatContext(o.chat_context ?? o.chatContext)
  if (chatContext === undefined) return { ok: false, error: 'bad_chat_context' }

  const args: PortalSendArgs = { to, body, type }
  if (typeof o.thread === 'string' && o.thread.trim()) args.thread = o.thread.trim()

  if (type === 'reply') {
    if (typeof o.corr !== 'string' || !o.corr.trim()) return { ok: false, error: 'bad_corr' }
    args.corr = o.corr.trim()
  } else if (o.corr != null) {
    return { ok: false, error: 'bad_corr' }
  }

  if (o.ttl_ms != null) {
    if (!Number.isInteger(o.ttl_ms)) return { ok: false, error: 'bad_ttl' }
    args.ttl_ms = o.ttl_ms
  }

  return { ok: true, args, sendMode, chatContext }
}

export function wrapPlainCodexRequest(args: PortalSendArgs, jobId: string, opts: { threadKey?: string | null; cwd?: string | null } = {}): PortalSendArgs {
  if (args.type !== 'request' || !isCodexJobRecipient(args.to) || isJsonObjectWithSchema(args.body, 'codex.job.request.v1')) return args
  return {
    ...args,
    body: JSON.stringify({
      schema: 'codex.job.request.v1',
      job_id: jobId,
      ...(opts.threadKey ? { thread_key: opts.threadKey } : {}),
      input: [{ type: 'text', text: args.body }],
      sandbox: 'read-only',
      approval_policy: 'never',
      cwd: opts.cwd || '/tmp',
      budget_policy: { max_primary_used_percent: 99 },
    }),
  }
}

export function wrapPlainCodexRealtimeInput(args: PortalSendArgs, opts: { sessionId?: string | null; threadKey?: string | null; cwd?: string | null; streamTopic?: string | null } = {}): PortalSendArgs {
  if (args.type !== 'request' || !isCodexJobRecipient(args.to) || isJsonObjectWithCodexSchema(args.body)) return args
  const sessionId = opts.sessionId || opts.threadKey
  if (!sessionId) return args
  return {
    ...args,
    body: JSON.stringify({
      schema: CODEX_SESSION_INPUT_SCHEMA,
      session_id: sessionId,
      thread_key: opts.threadKey ?? sessionId,
      input: [{ type: 'text', text: args.body }],
      mode: 'auto',
      sandbox: 'read-only',
      approval_policy: 'never',
      cwd: opts.cwd || '/tmp',
      ...(opts.streamTopic ? { stream_topic: opts.streamTopic } : {}),
    }),
  }
}

export function routePortalCodexTarget(
  args: PortalSendArgs,
  sendMode: PortalSendMode,
  opts: { jobTarget?: string | null; sessionTarget?: string | null } = {},
): PortalSendArgs {
  if (args.type !== 'request' || sendMode !== 'chat' || !isCodexJobRecipient(args.to)) return args
  const jobTarget = normalizePortalRecipient(opts.jobTarget ?? 'codex-gw')
  const sessionTarget = normalizePortalRecipient(opts.sessionTarget)
  if (!jobTarget || !sessionTarget || args.to !== jobTarget || sessionTarget === args.to) return args
  return { ...args, to: sessionTarget }
}

export function buildPortalDefaultCwd(
  args: PortalSendArgs,
  sendMode: PortalSendMode,
  opts: { hostOpsCwd?: string; remoteHostOpsCwd?: string; codexCwd?: string; oneOffCwd?: string } = {},
): string | null {
  if (args.type !== 'request' || !isCodexJobRecipient(args.to)) return null
  if (sendMode !== 'chat') return normalizeCwd(opts.oneOffCwd) ?? '/tmp'
  if (LOCAL_HOST_OPS_RE.test(args.to)) return normalizeCwd(opts.hostOpsCwd) ?? '/srv/git/alloyium'
  if (HOST_OPS_RE.test(args.to)) return normalizeCwd(opts.remoteHostOpsCwd) ?? '/srv/remote/alloyium'
  return normalizeCwd(opts.codexCwd) ?? '/app'
}

export function buildPortalThreadKey(args: PortalSendArgs, portalAgentId: string, sendMode: PortalSendMode, opts: { chatContext?: string | null } = {}): string | null {
  if (sendMode !== 'chat' || args.type !== 'request' || !isCodexJobRecipient(args.to)) return null
  const self = normalizeThreadKeyPart(portalAgentId)
  const target = normalizeThreadKeyPart(args.to)
  if (!self || !target) return null
  return opts.chatContext ? `portal:chat:${self}:${target}:${opts.chatContext}` : `portal:chat:${self}:${target}`
}

export function buildPortalRealtimeStreamTopic(threadKey: string | null): string | null {
  if (!threadKey) return null
  const token = threadKey
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
  return token ? `portal-rt-${token}`.slice(0, 64).replace(/-$/g, '') : null
}

function normalizePortalSendMode(value: unknown): PortalSendMode | null {
  if (value == null || value === '') return 'one-off'
  if (typeof value !== 'string') return null
  const mode = value.trim().toLowerCase().replace('_', '-')
  if (mode === 'oneoff') return 'one-off'
  return SEND_MODES.has(mode as PortalSendMode) ? mode as PortalSendMode : null
}

function normalizeThreadKeyPart(value: string): string | null {
  const v = normalizePortalRecipient(value)
  if (!v || v.startsWith('topic:')) return null
  return v.replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '') || null
}

function normalizeChatContext(value: unknown): string | null | undefined {
  if (value == null || value === '') return null
  if (typeof value !== 'string') return undefined
  const normalized = value.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '')
  if (!normalized || normalized.length > 64) return undefined
  return normalized
}

function normalizeCwd(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const cwd = value.trim()
  if (!cwd || !cwd.startsWith('/')) return null
  return cwd
}

export function isCodexJobRecipient(agentId: string): boolean {
  return CODEX_JOB_RECIPIENTS.some((re) => re.test(agentId))
}

function isJsonObjectWithSchema(text: string, schema: string): boolean {
  try {
    const parsed = JSON.parse(text)
    return !!parsed && typeof parsed === 'object' && !Array.isArray(parsed) && (parsed as any).schema === schema
  } catch {
    return false
  }
}

function isJsonObjectWithCodexSchema(text: string): boolean {
  try {
    const parsed = JSON.parse(text)
    return !!parsed && typeof parsed === 'object' && !Array.isArray(parsed) && typeof (parsed as any).schema === 'string' && (parsed as any).schema.startsWith('codex.')
  } catch {
    return false
  }
}

export function formatPortalRenderedBody(body: string): string | null {
  let parsed: any
  try { parsed = JSON.parse(body) } catch { return null }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null

  if (parsed.schema === 'codex.job.completed.v1' && typeof parsed.output === 'string') {
    const status = typeof parsed.status === 'string' && parsed.status ? parsed.status : 'completed'
    const header = `Codex job completed: ${status}`
    const notes: string[] = []
    if (parsed.output_preview === true) notes.push('preview only')
    if (parsed.truncated === true) notes.push('truncated')
    if (typeof parsed.blob_error === 'string' && parsed.blob_error) notes.push(`blob ${parsed.blob_error}`)
    if (typeof parsed.result_ref === 'string' && parsed.result_ref) notes.push(`result_ref: ${parsed.result_ref}`)
    const body = parsed.output ? `${header}\n\n${parsed.output}` : header
    if (!notes.length) return body
    return `${body}\n\n[${notes.join('; ')}]`
  }

  if (parsed.schema === 'codex.job.accepted.v1') {
    const notes: string[] = []
    if (typeof parsed.primary_used_pct === 'number') notes.push(`primary ${parsed.primary_used_pct}%`)
    if (typeof parsed.stream_topic === 'string' && parsed.stream_topic) notes.push(`stream ${parsed.stream_topic}`)
    return notes.length ? `Codex job accepted (${notes.join('; ')})` : 'Codex job accepted'
  }

  if (parsed.schema === 'codex.job.rejected.v1') {
    const reason = typeof parsed.reason === 'string' && parsed.reason ? parsed.reason : 'rejected'
    const detail = typeof parsed.detail === 'string' && parsed.detail ? `: ${parsed.detail}` : ''
    return `Codex job rejected: ${reason}${detail}`
  }

  if (parsed.schema === 'codex.job.failed.v1' && typeof parsed.error === 'string') {
    return `Codex job failed: ${parsed.error}`
  }

  if (parsed.schema === CODEX_SESSION_READY_SCHEMA) {
    return `Codex session ready: ${parsed.session_id ?? '?'}`
  }

  if (parsed.schema === CODEX_TURN_STARTED_SCHEMA) {
    return `Codex turn started: ${parsed.turn_id ?? '?'}`
  }

  if (parsed.schema === CODEX_TURN_COMPLETED_SCHEMA) {
    const header = `Codex turn completed: ${parsed.status ?? 'completed'}`
    return typeof parsed.output === 'string' && parsed.output ? `${header}\n\n${parsed.output}` : header
  }

  if (parsed.schema === CODEX_TURN_INTERRUPTED_SCHEMA) {
    return `Codex turn interrupted: ${parsed.turn_id ?? '?'}`
  }

  if (parsed.schema === CODEX_TURN_FAILED_SCHEMA && typeof parsed.error === 'string') {
    return `Codex turn failed: ${parsed.error}`
  }

  if (parsed.schema === CODEX_SESSION_EVENT_SCHEMA) {
    if (parsed.event === 'agent_text_delta' && typeof parsed.text === 'string') return parsed.text
    const event = typeof parsed.event === 'string' ? parsed.event : 'event'
    return `Codex realtime ${event}`
  }

  return null
}
