import { createHmac, timingSafeEqual } from 'node:crypto'

export const TASKBOARD_EVENT_SCHEMA = 'taskboard.event.v1'
export const TASKBOARD_EVENT_STREAM = 'TASKBOARD_EVENTS'
export const TASKBOARD_EVENT_SUBJECT = 'taskboard.v1.>'
export const DEFAULT_TASKBOARD_WEBHOOK_MAX_SKEW_MS = 300_000

export const TASKBOARD_SIGNATURE_HEADER = 'X-Taskboard-Signature'
export const TASKBOARD_TIMESTAMP_HEADER = 'X-Taskboard-Timestamp'
export const TASKBOARD_DELIVERY_HEADER = 'X-Taskboard-Delivery'
export const TASKBOARD_EVENT_HEADER = 'X-Taskboard-Event'

const enc = new TextEncoder()

export type TaskboardWebhookHeaders = {
  signature: string | null
  timestamp: string | null
  deliveryId: string | null
  eventType: string | null
}

export type TaskboardVerifyFailure =
  | 'missing_secret'
  | 'missing_signature'
  | 'missing_timestamp'
  | 'missing_delivery'
  | 'bad_timestamp'
  | 'stale_timestamp'
  | 'bad_signature_format'
  | 'signature_mismatch'

export type TaskboardVerifyResult =
  | { ok: true }
  | { ok: false; reason: TaskboardVerifyFailure; status: number }

export type TaskboardActor = {
  type?: string | null
  agent?: string | null
  principal_id?: string | number | null
  [key: string]: unknown
}

export type TaskboardEventEnvelope = {
  schema: typeof TASKBOARD_EVENT_SCHEMA
  event_id: string
  event_type: string
  occurred_at: string
  source: {
    system: 'openclawdev-taskboard'
    taskboard_url: string | null
    db_event_id: number | null
  }
  scope: {
    org_id: number
    project_id: number
    epic_id: number | null
    task_id: number | null
  }
  idempotency: {
    task_id: number | null
    fire_generation: number | null
    event_id: string
  }
  actor: TaskboardActor
  payload: unknown
}

export type NormalizeTaskboardPayloadOptions = {
  taskboardUrl?: string | null
  defaultOrgId?: number
  defaultProjectId?: number
  nowMs?: () => number
}

export type ResolveSecretEnv = Pick<NodeJS.ProcessEnv, 'TASKBOARD_EVENT_BRIDGE_HMAC_SECRET' | 'TASKBOARD_EVENT_BRIDGE_HMAC_SECRET_FILE'>

export function taskboardWebhookHeaders(headers: Headers | Record<string, string | undefined | null>): TaskboardWebhookHeaders {
  const get = (name: string): string | null => {
    if (headers instanceof Headers) return headers.get(name)
    const lower = name.toLowerCase()
    for (const [k, v] of Object.entries(headers)) {
      if (k.toLowerCase() === lower) return v == null ? null : String(v)
    }
    return null
  }
  return {
    signature: get(TASKBOARD_SIGNATURE_HEADER),
    timestamp: get(TASKBOARD_TIMESTAMP_HEADER),
    deliveryId: get(TASKBOARD_DELIVERY_HEADER),
    eventType: get(TASKBOARD_EVENT_HEADER),
  }
}

export function computeTaskboardWebhookSignature(
  secret: string,
  timestamp: string,
  deliveryId: string,
  rawBody: Uint8Array | string,
): string {
  const body = typeof rawBody === 'string' ? enc.encode(rawBody) : rawBody
  const prefix = enc.encode(`${timestamp}.${deliveryId}.`)
  const digest = createHmac('sha256', enc.encode(secret)).update(prefix).update(body).digest('hex')
  return `sha256=${digest}`
}

export function parseTaskboardTimestampMs(timestamp: string): number | null {
  const trimmed = timestamp.trim()
  if (!trimmed) return null
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    const n = Number(trimmed)
    if (!Number.isFinite(n) || n <= 0) return null
    return n >= 1_000_000_000_000 ? Math.trunc(n) : Math.trunc(n * 1000)
  }
  const parsed = Date.parse(trimmed)
  return Number.isFinite(parsed) ? parsed : null
}

export function verifyTaskboardWebhookSignature(opts: {
  secret?: string | null
  signature?: string | null
  timestamp?: string | null
  deliveryId?: string | null
  rawBody: Uint8Array | string
  nowMs?: () => number
  maxSkewMs?: number
}): TaskboardVerifyResult {
  const secret = opts.secret?.trim()
  if (!secret) return { ok: false, reason: 'missing_secret', status: 500 }
  const signature = opts.signature?.trim()
  if (!signature) return { ok: false, reason: 'missing_signature', status: 401 }
  const timestamp = opts.timestamp?.trim()
  if (!timestamp) return { ok: false, reason: 'missing_timestamp', status: 401 }
  const deliveryId = opts.deliveryId?.trim()
  if (!deliveryId) return { ok: false, reason: 'missing_delivery', status: 401 }

  const timestampMs = parseTaskboardTimestampMs(timestamp)
  if (timestampMs == null) return { ok: false, reason: 'bad_timestamp', status: 401 }
  const skewMs = Math.abs((opts.nowMs ?? Date.now)() - timestampMs)
  const maxSkewMs = opts.maxSkewMs ?? DEFAULT_TASKBOARD_WEBHOOK_MAX_SKEW_MS
  if (skewMs > maxSkewMs) return { ok: false, reason: 'stale_timestamp', status: 401 }

  if (!/^sha256=[0-9a-f]{64}$/i.test(signature)) {
    return { ok: false, reason: 'bad_signature_format', status: 401 }
  }
  const expected = computeTaskboardWebhookSignature(secret, timestamp, deliveryId, opts.rawBody)
  const got = Buffer.from(signature.toLowerCase(), 'utf8')
  const want = Buffer.from(expected, 'utf8')
  if (got.length !== want.length || !timingSafeEqual(got, want)) {
    return { ok: false, reason: 'signature_mismatch', status: 401 }
  }
  return { ok: true }
}

export async function resolveTaskboardEventBridgeSecret(env: ResolveSecretEnv = process.env): Promise<string | null> {
  const direct = env.TASKBOARD_EVENT_BRIDGE_HMAC_SECRET?.trim()
  if (direct) return direct
  const file = env.TASKBOARD_EVENT_BRIDGE_HMAC_SECRET_FILE?.trim()
  if (!file) return null
  const value = await Bun.file(file).text()
  const trimmed = value.trim()
  return trimmed || null
}

export function normalizeTaskboardPayload(input: unknown, opts: NormalizeTaskboardPayloadOptions = {}): TaskboardEventEnvelope {
  if (!isRecord(input)) throw new Error('webhook payload must be a JSON object')
  const task = recordOrNull(input.task)
  const source = recordOrNull(input.source)
  const scope = recordOrNull(input.scope)
  const idempotency = recordOrNull(input.idempotency)

  const eventId = stringValue(input.event_id)
  if (!eventId) throw new Error('webhook payload is missing event_id')
  const eventType = stringValue(input.event_type)
  if (!eventType) throw new Error('webhook payload is missing event_type')
  const occurredAt = stringValue(input.occurred_at) ?? new Date((opts.nowMs ?? Date.now)()).toISOString()

  const orgId = firstNumber(
    scope?.org_id,
    input.org_id,
    task?.org_id,
    recordOrNull(task?.project)?.org_id,
    recordOrNull(task?.epic)?.org_id,
    opts.defaultOrgId ?? 1,
  )
  const projectId = firstNumber(
    scope?.project_id,
    input.project_id,
    task?.project_id,
    recordOrNull(task?.project)?.id,
    recordOrNull(task?.epic)?.project_id,
    opts.defaultProjectId ?? 1,
  )
  if (orgId == null) throw new Error('webhook payload is missing org_id')
  if (projectId == null) throw new Error('webhook payload is missing project_id')

  const taskId = firstNumber(scope?.task_id, input.task_id, task?.id)
  const epicId = firstNumber(scope?.epic_id, input.epic_id, task?.epic_id, recordOrNull(task?.epic)?.id)
  const fireGeneration = firstNumber(idempotency?.fire_generation, input.fire_generation, task?.fire_generation)
  const actor = recordOrNull(input.actor) ?? {}

  return {
    schema: TASKBOARD_EVENT_SCHEMA,
    event_id: eventId,
    event_type: eventType,
    occurred_at: occurredAt,
    source: {
      system: 'openclawdev-taskboard',
      taskboard_url: stringValue(source?.taskboard_url) ?? opts.taskboardUrl ?? null,
      db_event_id: firstNumber(source?.db_event_id, input.db_event_id, input.outbox_id),
    },
    scope: {
      org_id: orgId,
      project_id: projectId,
      epic_id: epicId,
      task_id: taskId,
    },
    idempotency: {
      task_id: taskId,
      fire_generation: fireGeneration,
      event_id: eventId,
    },
    actor: sanitizeEventPayload(actor) as TaskboardActor,
    payload: sanitizeEventPayload(isRecord(input.payload) ? input.payload : eventSpecificPayload(input)),
  }
}

export function buildTaskboardSubject(envelope: Pick<TaskboardEventEnvelope, 'scope' | 'event_type'>): string {
  const org = subjectToken(envelope.scope.org_id)
  const project = subjectToken(envelope.scope.project_id)
  const eventTokens = eventTypeTokens(envelope.event_type)
  return `taskboard.v1.org.${org}.project.${project}.${eventTokens}`
}

export function eventTypeTokens(eventType: string): string {
  return eventType
    .split('.')
    .map((part) => subjectToken(part))
    .join('.')
}

export function subjectToken(value: unknown): string {
  const raw = String(value ?? '').trim().toLowerCase()
  const token = raw.replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '')
  return token || 'unknown'
}

export function sanitizeEventPayload(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => sanitizeEventPayload(item))
  if (!isRecord(value)) return value
  const out: Record<string, unknown> = {}
  for (const [key, item] of Object.entries(value)) {
    if (sensitiveKey(key)) continue
    const sanitized = sanitizeEventPayload(item)
    if (sanitized !== undefined) out[key] = sanitized
  }
  return out
}

function eventSpecificPayload(input: Record<string, unknown>): Record<string, unknown> {
  const omit = new Set([
    'schema',
    'event_id',
    'event_type',
    'occurred_at',
    'source',
    'scope',
    'idempotency',
    'actor',
    'task_id',
    'fire_generation',
    'db_event_id',
    'outbox_id',
    'org_id',
    'project_id',
    'epic_id',
  ])
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(input)) {
    if (!omit.has(key)) out[key] = value
  }
  return out
}

function sensitiveKey(key: string): boolean {
  const k = key.toLowerCase().replace(/[-\s]+/g, '_')
  return (
    /(^|_)(token|secret|password|hmac|authorization|bearer)($|_)/.test(k) ||
    /private_?key/.test(k) ||
    /^vault_.*path$/.test(k) ||
    /_vault_.*path$/.test(k)
  )
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function recordOrNull(value: unknown): Record<string, unknown> | null {
  return isRecord(value) ? value : null
}

function stringValue(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  return trimmed || null
}

function numberValue(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.trunc(value)
  if (typeof value === 'string' && value.trim() && /^-?\d+$/.test(value.trim())) return Number(value.trim())
  return null
}

function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    const n = numberValue(value)
    if (n != null) return n
  }
  return null
}
