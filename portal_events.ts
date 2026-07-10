export type PortalEventSource = 'a2a' | 'portal' | 'redis' | 'taskboard' | 'langchain'
export type PortalResourceKind = 'channel' | 'peer' | 'presence' | 'inbox' | 'send' | 'taskboard' | 'health' | 'session'

export type PortalEventType =
  | 'portal.snapshot.v1'
  | 'a2a.message.v1'
  | 'a2a.stream.delta.v1'
  | 'portal.channel.updated.v1'
  | 'portal.fleet.updated.v1'
  | 'portal.presence.updated.v1'
  | 'portal.inbox.attention.updated.v1'
  | 'portal.inbox.counts.updated.v1'
  | 'portal.send.queued.v1'
  | 'portal.send.accepted.v1'
  | 'portal.send.progress.v1'
  | 'portal.send.completed.v1'
  | 'portal.send.timeout.v1'
  | 'portal.send.failed.v1'
  | 'portal.langchain.status.v1'
  | 'portal.health.updated.v1'
  | 'portal.heartbeat.v1'
  | 'portal.resync.required.v1'
  | 'portal.error.v1'

export type PortalEventScope = {
  tenant: string
  channels: string[]
  recipients: string[]
}

export type PortalEventEnvelope<P = unknown> = {
  schema: 'portal.event.v1'
  seq: number
  ts: string
  source: PortalEventSource
  event_type: PortalEventType
  resource: { kind: PortalResourceKind; id: string }
  actor: { operator_id: string | null; agent_id: string | null }
  scope: PortalEventScope
  payload: P
  redaction: { applied: boolean; policy: 'portal-default-v1' }
}

export type PortalEventInput<P = unknown> =
  Omit<PortalEventEnvelope<P>, 'schema' | 'seq' | 'ts' | 'actor' | 'scope' | 'redaction'> & {
    seq?: number
    ts?: string
    actor?: Partial<PortalEventEnvelope['actor']>
    scope?: Partial<PortalEventScope>
    redaction?: Partial<PortalEventEnvelope['redaction']>
    dedup_key?: string | null
  }

export type PortalMessageLike = {
  id: string
  channel: string
  kind: 'topic' | 'dm' | 'local'
  from: string
  to: string
  type: string
  thread?: string
  corr?: string
  ts: string
  body: string
  t: number
}

export type PortalEventFilters = {
  tenant?: string | null
  channels?: string[]
  recipients?: string[]
  resources?: string[]
  eventTypes?: string[]
  includeSnapshots?: boolean
  limit?: number
}

const SECRET_KEY_RE = /(?:^|[_-])(authorization|bearer|token|secret|password|passwd|private[_-]?key|api[_-]?key|signature|signing[_-]?key)(?:$|[_-])/i
const SECRET_VALUE_PATTERNS: RegExp[] = [
  /\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
  /\b(?:sk|pk|ghp|github_pat|xox[baprs])_[A-Za-z0-9_=-]{16,}\b/g,
]

export function createPortalEvent<P = unknown>(input: PortalEventInput<P>): PortalEventEnvelope {
  return {
    schema: 'portal.event.v1',
    seq: normalizeSeq(input.seq),
    ts: input.ts || new Date().toISOString(),
    source: input.source,
    event_type: input.event_type,
    resource: { kind: input.resource.kind, id: String(input.resource.id || '') },
    actor: {
      operator_id: stringOrNull(input.actor?.operator_id),
      agent_id: stringOrNull(input.actor?.agent_id),
    },
    scope: normalizePortalScope(input.scope),
    payload: redactPortalPayload(input.payload),
    redaction: { applied: true, policy: 'portal-default-v1', ...input.redaction },
  }
}

export function portalEventFromMessage(message: PortalMessageLike, portalAgentId: string, opts: { subject?: string } = {}): PortalEventInput {
  const parsed = parseJsonObject(message.body)
  const eventType = parsed && typeof parsed.schema === 'string' && parsed.schema.includes('.delta.')
    ? 'a2a.stream.delta.v1'
    : 'a2a.message.v1'
  const channel = message.channel || (message.kind === 'dm' ? `@${message.to}` : '')
  const recipients = normalizeTokenList([message.from, message.to, portalAgentId].filter(Boolean))
  return {
    source: 'a2a',
    event_type: eventType,
    resource: { kind: message.kind === 'topic' ? 'channel' : 'peer', id: channel },
    actor: { agent_id: message.from || null },
    scope: {
      tenant: 'default',
      channels: [channel],
      recipients,
    },
    payload: {
      id: message.id,
      channel,
      kind: message.kind,
      from: message.from,
      to: message.to,
      type: message.type,
      thread: message.thread ?? null,
      corr: message.corr ?? null,
      ts: message.ts,
      body: message.body,
      t: message.t,
      subject: opts.subject ?? null,
    },
    dedup_key: `a2a:${eventType}:${channel}:${message.id}`,
  }
}

export function portalEventFromSendLifecycle(
  eventType: Extract<PortalEventType, `portal.send.${string}`>,
  payload: Record<string, unknown>,
  portalAgentId: string,
): PortalEventInput {
  const sendId = String(payload.send_id || payload.id || payload.corr || 'unknown')
  const routedTo = typeof payload.routed_to === 'string' ? payload.routed_to : typeof payload.to === 'string' ? payload.to : ''
  return {
    source: 'portal',
    event_type: eventType,
    resource: { kind: 'send', id: sendId },
    actor: { agent_id: portalAgentId },
    scope: { tenant: 'default', channels: [], recipients: normalizeTokenList([routedTo, portalAgentId].filter(Boolean)) },
    payload,
    dedup_key: `portal:${eventType}:${sendId}:${payload.status ?? ''}:${payload.bus_id ?? ''}:${payload.attempts ?? ''}:${payload.retry_in_ms ?? ''}:${payload.settle_ms ?? ''}`,
  }
}

export function makeTransientPortalEvent(input: PortalEventInput, seq: number): PortalEventEnvelope {
  return createPortalEvent({ ...input, seq })
}

export function portalSnapshotEvent(payload: unknown, seq: number, scope?: Partial<PortalEventScope>): PortalEventEnvelope {
  return createPortalEvent({
    seq,
    source: 'portal',
    event_type: 'portal.snapshot.v1',
    resource: { kind: 'health', id: 'snapshot' },
    scope,
    payload,
  })
}

export function portalHeartbeatEvent(seq: number, payload: unknown = {}): PortalEventEnvelope {
  return createPortalEvent({
    seq,
    source: 'portal',
    event_type: 'portal.heartbeat.v1',
    resource: { kind: 'health', id: 'heartbeat' },
    payload,
  })
}

export function portalResyncRequiredEvent(seq: number, payload: unknown, scope?: Partial<PortalEventScope>): PortalEventEnvelope {
  return createPortalEvent({
    seq,
    source: 'portal',
    event_type: 'portal.resync.required.v1',
    resource: { kind: 'health', id: 'resync' },
    scope,
    payload,
  })
}

export function isPortalEventEnvelope(value: unknown): value is PortalEventEnvelope {
  const o = value as PortalEventEnvelope
  return !!o
    && typeof o === 'object'
    && o.schema === 'portal.event.v1'
    && Number.isInteger(o.seq)
    && typeof o.ts === 'string'
    && typeof o.source === 'string'
    && typeof o.event_type === 'string'
    && !!o.resource
    && typeof o.resource.kind === 'string'
    && typeof o.resource.id === 'string'
    && !!o.scope
    && typeof o.scope.tenant === 'string'
    && Array.isArray(o.scope.channels)
    && Array.isArray(o.scope.recipients)
}

export function isPortalEventType(value: string): value is PortalEventType {
  return PORTAL_EVENT_TYPES.has(value as PortalEventType)
}

export function portalEventMatchesScope(event: PortalEventEnvelope, filters: PortalEventFilters = {}): boolean {
  const tenant = normalizeToken(filters.tenant)
  if (tenant && event.scope.tenant !== tenant) return false
  if (filters.includeSnapshots === false && event.event_type === 'portal.snapshot.v1') return false
  const eventTypes = normalizeTokenList(filters.eventTypes)
  if (eventTypes.length && !eventTypes.includes(event.event_type)) return false
  const resources = normalizeTokenList(filters.resources)
  if (resources.length) {
    const id = normalizeToken(event.resource.id)
    const kind = normalizeToken(event.resource.kind)
    const qualified = `${kind}:${id}`
    if (!resources.includes(id) && !resources.includes(kind) && !resources.includes(qualified)) return false
  }
  const channels = normalizeTokenList(filters.channels)
  if (channels.length && !intersects(event.scope.channels, channels)) return false
  const recipients = normalizeTokenList(filters.recipients)
  if (recipients.length && !intersects(event.scope.recipients, recipients)) return false
  return true
}

export function serializePortalSseEvent(event: PortalEventEnvelope): string {
  return `id: ${event.seq}\nevent: ${event.event_type}\ndata: ${JSON.stringify(event)}\n\n`
}

export function serializePortalSseComment(comment: string): string {
  return `: ${comment.replace(/[\r\n]+/g, ' ')}\n\n`
}

export function parsePortalSeq(value: unknown): number | null {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) return null
  return Math.trunc(n)
}

export function redactPortalPayload<T>(value: T): T {
  return redactValue(value, new WeakSet()) as T
}

function normalizePortalScope(scope: Partial<PortalEventScope> | undefined): PortalEventScope {
  return {
    tenant: normalizeToken(scope?.tenant) || 'default',
    channels: normalizeTokenList(scope?.channels),
    recipients: normalizeTokenList(scope?.recipients),
  }
}

function redactValue(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === 'string') return redactString(value)
  if (value == null || typeof value !== 'object') return value
  if (seen.has(value)) return '[Circular]'
  seen.add(value)
  if (Array.isArray(value)) return value.map((v) => redactValue(v, seen))
  const out: Record<string, unknown> = {}
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    out[key] = SECRET_KEY_RE.test(key) ? '[REDACTED]' : redactValue(child, seen)
  }
  return out
}

function redactString(value: string): string {
  let out = value
  for (const pattern of SECRET_VALUE_PATTERNS) out = out.replace(pattern, '[REDACTED]')
  return out
}

function normalizeToken(value: unknown): string {
  return typeof value === 'string' ? value.trim().toLowerCase() : ''
}

function normalizeTokenList(values: unknown): string[] {
  const raw = Array.isArray(values) ? values : typeof values === 'string' ? values.split(',') : []
  const seen = new Set<string>()
  const out: string[] = []
  for (const value of raw) {
    const token = normalizeToken(value)
    if (token && !seen.has(token)) {
      seen.add(token)
      out.push(token)
    }
  }
  return out
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null
}

function normalizeSeq(value: unknown): number {
  const n = Number(value)
  return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : 0
}

function intersects(a: string[], b: string[]): boolean {
  const set = new Set(a.map(normalizeToken))
  return b.some((value) => set.has(normalizeToken(value)))
}

function parseJsonObject(value: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null
  } catch {
    return null
  }
}

const PORTAL_EVENT_TYPES = new Set<PortalEventType>([
  'portal.snapshot.v1',
  'a2a.message.v1',
  'a2a.stream.delta.v1',
  'portal.channel.updated.v1',
  'portal.fleet.updated.v1',
  'portal.presence.updated.v1',
  'portal.inbox.attention.updated.v1',
  'portal.inbox.counts.updated.v1',
  'portal.send.queued.v1',
  'portal.send.accepted.v1',
  'portal.send.progress.v1',
  'portal.send.completed.v1',
  'portal.send.timeout.v1',
  'portal.send.failed.v1',
  'portal.langchain.status.v1',
  'portal.health.updated.v1',
  'portal.heartbeat.v1',
  'portal.resync.required.v1',
  'portal.error.v1',
])
