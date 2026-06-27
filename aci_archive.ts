import { Database } from 'bun:sqlite'
import { createHash } from 'node:crypto'
import type { InboxMessageRow } from './a2a_inbox_store.ts'

export type AciEnvelopeLike = {
  id: string
  from: string
  to: string
  type: string
  thread?: string
  corr?: string
  ts: string
  ttl_ms?: number
  body: string
  attrs?: Record<string, string>
  alg?: string
  sig?: string
}

export type AciIngestInput = {
  env: AciEnvelopeLike
  rawEnvelope?: string
  subject?: string
  sourceKind?: 'direct' | 'topic' | 'portal' | 'backfill'
  routeKind?: 'direct' | 'topic'
  observedAt?: string
  deliveredAt?: string | null
  streamSeq?: number | null
  trustStatus?: string
  routeStatus?: string
  recipient?: string | null
  handled?: boolean | number | null
  handledAt?: string | null
}

export type AciBackfillResult = {
  scanned: number
  ingested: number
  skipped: number
}

export type AciQueryFilters = {
  from?: string | null
  to?: string | null
  agents?: string[]
  topics?: string[]
  types?: string[]
  schemas?: string[]
  thread?: string | null
  jobId?: string | null
  sessionId?: string | null
  limit?: number
  cursor?: string | null
  order?: 'asc' | 'desc'
}

export type AciEvent = {
  event_id: string
  env_id: string
  source_kind: string
  route_kind: string
  from_agent: string | null
  to_agent: string | null
  topic: string | null
  msg_type: string
  schema: string | null
  thread: string | null
  corr: string | null
  job_id: string | null
  task_id: string | null
  agent_run_id: string | null
  thread_key: string | null
  stream_topic: string | null
  result_ref: string | null
  status: string | null
  error_class: string | null
  sent_at: string
  observed_at: string
  delivered_at: string | null
  latency_ms: number | null
  trust_status: string
  route_status: string
  body_available: boolean
  body_redaction_state: string
  can_view_body: false
  raw_sha256: string | null
  size_bytes: number
  metadata_json: Record<string, unknown>
}

export type AciNode = {
  id: string
  kind: 'agent' | 'topic' | 'job' | 'session' | 'external'
  label: string
  first_seen_at: string | null
  last_seen_at: string | null
  counts: Record<string, number>
  metadata_json?: Record<string, unknown>
}

export type AciEdgeAggregate = {
  id: string
  from: string
  to: string
  kind: string
  count: number
  first_seen_at: string | null
  last_seen_at: string | null
  p50_latency_ms: number | null
  p95_latency_ms: number | null
  request_count: number
  reply_count: number
  failure_count: number
  thread_count: number
  job_count: number
  schemas: string[]
  msg_types: string[]
}

type EventRow = {
  event_id: string
  env_id: string
  source_kind: string
  route_kind: string
  from_agent: string | null
  to_agent: string | null
  topic: string | null
  msg_type: string
  schema: string | null
  thread: string | null
  corr: string | null
  sent_at: string
  observed_at: string
  delivered_at: string | null
  raw_sha256: string | null
  body_size_bytes: number
  body_redaction_state: string
  job_id: string | null
  task_id: string | null
  agent_run_id: string | null
  event_ref_id: string | null
  thread_key: string | null
  stream_topic: string | null
  result_ref: string | null
  status: string | null
  error_class: string | null
  trust_status: string
  route_status: string
  metadata_json: string | null
}

const MAX_LIMIT = 500
const DEFAULT_LIMIT = 100
const SAFE_SCALAR_KEYS = [
  'schema',
  'job_id',
  'task_id',
  'agent_run_id',
  'event_id',
  'triage_id',
  'thread_key',
  'stream_topic',
  'result_ref',
  'session_id',
  'status',
  'failure_class',
  'error_class',
]

function nowIso(): string { return new Date().toISOString() }

function sha256(s: string): string {
  return createHash('sha256').update(s).digest('hex')
}

function byteLen(s: string): number {
  return Buffer.byteLength(s, 'utf8')
}

function cleanScalar(v: unknown, max = 256): string | null {
  if (typeof v !== 'string' && typeof v !== 'number' && typeof v !== 'boolean') return null
  const s = String(v).trim()
  if (!s) return null
  return s.slice(0, max)
}

function cleanToken(v: unknown, max = 256): string | null {
  const s = cleanScalar(v, max)
  if (!s || !/^[a-zA-Z0-9_.:@/\-]+$/.test(s)) return s
  return s
}

function classifyError(v: unknown): string | null {
  const explicit = cleanScalar(v, 128)
  if (!explicit) return null
  const s = explicit.toLowerCase()
  if (/timeout|deadline|timed out/.test(s)) return 'timeout'
  if (/auth|credential|permission|forbidden|unauthor/.test(s)) return 'auth'
  if (/not[_ -]?found|missing|enoent/.test(s)) return 'not_found'
  if (/rate|quota|limit/.test(s)) return 'rate_limit'
  if (/invalid|bad request|schema|parse/.test(s)) return 'invalid'
  return /^[a-z0-9_.:-]{1,64}$/i.test(explicit) ? explicit : 'error'
}

function statusFromSchema(schema: string | null): string | null {
  if (!schema) return null
  if (schema.includes('.job.request')) return 'requested'
  if (schema.includes('.job.accepted')) return 'accepted'
  if (schema.includes('.job.completed')) return 'completed'
  if (schema.includes('.job.failed')) return 'failed'
  if (schema.includes('.job.rejected')) return 'rejected'
  return null
}

function parseBodyMetadata(body: string): {
  schema: string | null
  jobId: string | null
  taskId: string | null
  agentRunId: string | null
  eventRefId: string | null
  threadKey: string | null
  streamTopic: string | null
  resultRef: string | null
  sessionId: string | null
  status: string | null
  errorClass: string | null
  metadata: Record<string, unknown>
} {
  let parsed: any = null
  try { parsed = JSON.parse(body) } catch {}
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return {
      schema: null,
      jobId: null,
      taskId: null,
      agentRunId: null,
      eventRefId: null,
      threadKey: null,
      streamTopic: null,
      resultRef: null,
      sessionId: null,
      status: null,
      errorClass: null,
      metadata: { body_json: false },
    }
  }
  const metadata: Record<string, unknown> = { body_json: true }
  for (const key of SAFE_SCALAR_KEYS) {
    const v = cleanScalar(parsed[key], key.endsWith('_ref') || key === 'result_ref' ? 512 : 256)
    if (v != null) metadata[key] = v
  }
  const schema = cleanScalar(parsed.schema, 160)
  const status = cleanScalar(parsed.status, 64) ?? statusFromSchema(schema)
  const errorClass = classifyError(parsed.error_class ?? parsed.failure_class ?? parsed.error ?? parsed.reason)
  if (status) metadata.status = status
  if (errorClass) metadata.error_class = errorClass
  return {
    schema,
    jobId: cleanScalar(parsed.job_id, 160),
    taskId: cleanScalar(parsed.task_id, 80),
    agentRunId: cleanScalar(parsed.agent_run_id, 80),
    eventRefId: cleanScalar(parsed.event_id ?? parsed.triage_id, 160),
    threadKey: cleanScalar(parsed.thread_key, 256),
    streamTopic: cleanScalar(parsed.stream_topic, 256),
    resultRef: cleanScalar(parsed.result_ref, 512),
    sessionId: cleanScalar(parsed.session_id, 160),
    status,
    errorClass,
    metadata,
  }
}

function normalizeLimit(n: number | undefined, fallback = DEFAULT_LIMIT): number {
  return Math.min(Math.max(Math.trunc(n ?? fallback), 1), MAX_LIMIT)
}

function encodeCursor(row: Pick<EventRow, 'observed_at' | 'event_id'>): string {
  return Buffer.from(JSON.stringify({ observed_at: row.observed_at, event_id: row.event_id })).toString('base64url')
}

function decodeCursor(cursor: string | null | undefined): { observed_at: string; event_id: string } | null {
  if (!cursor) return null
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'))
    if (typeof parsed?.observed_at === 'string' && typeof parsed?.event_id === 'string') return parsed
  } catch {}
  return null
}

function safeJsonParseObject(raw: string | null): Record<string, unknown> {
  if (!raw) return {}
  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

function splitCsv(s: string | null): string[] {
  if (!s) return []
  return [...new Set(s.split(',').map(x => x.trim()).filter(Boolean))].slice(0, 40)
}

function nodeKind(id: string): AciNode['kind'] {
  if (id.startsWith('topic:')) return 'topic'
  if (id.startsWith('job:')) return 'job'
  if (id.startsWith('session:')) return 'session'
  return 'agent'
}

function nodeLabel(id: string): string {
  if (id.startsWith('topic:')) return '#' + id.slice(6)
  if (id.startsWith('job:')) return id.slice(4)
  if (id.startsWith('session:')) return id.slice(8)
  return id
}

function routeFrom(env: AciEnvelopeLike, subject: string | null | undefined, routeKind?: 'direct' | 'topic'): {
  routeKind: 'direct' | 'topic'
  sourceKind: 'direct' | 'topic'
  topic: string | null
  toAgent: string | null
} {
  const subjectTopic = subject?.match(/\.topic\.([a-z0-9-]+)$/)?.[1] ?? null
  const bodyTopic = env.to?.startsWith('topic:') ? env.to.slice('topic:'.length) : null
  const topic = subjectTopic ?? bodyTopic
  const kind = routeKind ?? (topic ? 'topic' : 'direct')
  return {
    routeKind: kind,
    sourceKind: kind,
    topic: kind === 'topic' ? topic : null,
    toAgent: kind === 'direct' ? env.to : null,
  }
}

function buildSubject(env: AciEnvelopeLike, routeKind: 'direct' | 'topic', topic: string | null): string {
  if (routeKind === 'topic') return `alloyium.a2a.topic.${topic ?? env.to.replace(/^topic:/, '')}`
  return `alloyium.a2a.agent.${env.to}.inbox`
}

function latencyMs(sentAt: string, observedAt: string): number | null {
  const a = Date.parse(sentAt)
  const b = Date.parse(observedAt)
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null
  return Math.max(0, b - a)
}

function eventToApi(row: EventRow): AciEvent {
  return {
    event_id: row.event_id,
    env_id: row.env_id,
    source_kind: row.source_kind,
    route_kind: row.route_kind,
    from_agent: row.from_agent,
    to_agent: row.to_agent,
    topic: row.topic,
    msg_type: row.msg_type,
    schema: row.schema,
    thread: row.thread,
    corr: row.corr,
    job_id: row.job_id,
    task_id: row.task_id,
    agent_run_id: row.agent_run_id,
    thread_key: row.thread_key,
    stream_topic: row.stream_topic,
    result_ref: row.result_ref,
    status: row.status,
    error_class: row.error_class,
    sent_at: row.sent_at,
    observed_at: row.observed_at,
    delivered_at: row.delivered_at,
    latency_ms: latencyMs(row.sent_at, row.observed_at),
    trust_status: row.trust_status,
    route_status: row.route_status,
    body_available: row.body_size_bytes > 0,
    body_redaction_state: row.body_redaction_state,
    can_view_body: false,
    raw_sha256: row.raw_sha256,
    size_bytes: row.body_size_bytes,
    metadata_json: safeJsonParseObject(row.metadata_json),
  }
}

function eventWhere(alias: string, filters: AciQueryFilters = {}, args: unknown[] = []): string[] {
  const where: string[] = []
  if (filters.from) {
    where.push(`${alias}.observed_at >= ?`)
    args.push(filters.from)
  }
  if (filters.to) {
    where.push(`${alias}.observed_at <= ?`)
    args.push(filters.to)
  }
  if (filters.thread) {
    where.push(`${alias}.thread = ?`)
    args.push(filters.thread)
  }
  if (filters.jobId) {
    where.push(`${alias}.job_id = ?`)
    args.push(filters.jobId)
  }
  if (filters.sessionId) {
    where.push(`${alias}.thread_key = ?`)
    args.push(filters.sessionId)
  }
  if (filters.agents?.length) {
    const qs = filters.agents.map(() => '?').join(',')
    where.push(`(${alias}.from_agent IN (${qs}) OR ${alias}.to_agent IN (${qs}))`)
    args.push(...filters.agents, ...filters.agents)
  }
  if (filters.topics?.length) {
    const qs = filters.topics.map(() => '?').join(',')
    where.push(`${alias}.topic IN (${qs})`)
    args.push(...filters.topics.map(t => t.replace(/^topic:/, '').replace(/^#/, '')))
  }
  if (filters.types?.length) {
    const qs = filters.types.map(() => '?').join(',')
    where.push(`${alias}.msg_type IN (${qs})`)
    args.push(...filters.types)
  }
  if (filters.schemas?.length) {
    const qs = filters.schemas.map(() => '?').join(',')
    where.push(`${alias}.schema IN (${qs})`)
    args.push(...filters.schemas)
  }
  return where
}

export function initializeAciArchiveDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

export function migrateAciArchiveDb(db: Database): void {
  initializeAciArchiveDb(db)
  db.exec(`
    CREATE TABLE IF NOT EXISTS aci_schema_migrations (
      name TEXT PRIMARY KEY,
      version INTEGER NOT NULL,
      applied_at TEXT NOT NULL
    );

    INSERT OR IGNORE INTO aci_schema_migrations(name, version, applied_at)
      VALUES ('aci_archive', 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

    CREATE TABLE IF NOT EXISTS aci_agent (
      agent_id TEXT PRIMARY KEY,
      kind TEXT NOT NULL DEFAULT 'agent',
      display_name TEXT,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      last_host TEXT,
      presence_json TEXT,
      metadata_json TEXT
    );

    CREATE TABLE IF NOT EXISTS aci_raw_envelope (
      raw_sha256 TEXT PRIMARY KEY,
      first_seen_at TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      encoding TEXT NOT NULL DEFAULT 'utf8-json',
      body_sha256 TEXT NOT NULL,
      body_size_bytes INTEGER NOT NULL,
      body_redaction_state TEXT NOT NULL DEFAULT 'metadata_only',
      encrypted_state TEXT NOT NULL DEFAULT 'unknown',
      retention_class TEXT NOT NULL DEFAULT 'metadata_default'
    );

    CREATE TABLE IF NOT EXISTS aci_message_event (
      event_id TEXT PRIMARY KEY,
      env_id TEXT NOT NULL,
      source_kind TEXT NOT NULL,
      source_stream TEXT,
      subject TEXT NOT NULL,
      stream_seq INTEGER,
      route_kind TEXT NOT NULL,
      from_agent TEXT,
      to_agent TEXT,
      topic TEXT,
      msg_type TEXT NOT NULL,
      schema TEXT,
      thread TEXT,
      corr TEXT,
      sent_at TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      delivered_at TEXT,
      ttl_ms INTEGER,
      alg TEXT,
      sig_present INTEGER NOT NULL DEFAULT 0 CHECK (sig_present IN (0,1)),
      trust_status TEXT NOT NULL,
      route_status TEXT NOT NULL,
      raw_sha256 TEXT,
      body_sha256 TEXT,
      body_size_bytes INTEGER NOT NULL DEFAULT 0,
      body_redaction_state TEXT NOT NULL DEFAULT 'metadata_only',
      job_id TEXT,
      task_id TEXT,
      agent_run_id TEXT,
      event_ref_id TEXT,
      thread_key TEXT,
      stream_topic TEXT,
      result_ref TEXT,
      status TEXT,
      error_class TEXT,
      metadata_json TEXT,
      FOREIGN KEY(raw_sha256) REFERENCES aci_raw_envelope(raw_sha256)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS aci_message_event_source_env_idx
      ON aci_message_event(source_kind, subject, env_id);
    CREATE INDEX IF NOT EXISTS aci_message_event_time_idx
      ON aci_message_event(observed_at DESC, event_id DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_agents_idx
      ON aci_message_event(from_agent, to_agent, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_topic_idx
      ON aci_message_event(topic, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_thread_idx
      ON aci_message_event(thread, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_job_idx
      ON aci_message_event(job_id, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_schema_idx
      ON aci_message_event(schema, observed_at DESC);

    CREATE TABLE IF NOT EXISTS aci_message_delivery (
      event_id TEXT NOT NULL,
      recipient_agent TEXT NOT NULL,
      durable_name TEXT,
      delivered_at TEXT,
      injected_at TEXT,
      acked_at TEXT,
      handled INTEGER NOT NULL DEFAULT 0 CHECK (handled IN (0,1)),
      handled_at TEXT,
      inject_status TEXT,
      PRIMARY KEY(event_id, recipient_agent),
      FOREIGN KEY(event_id) REFERENCES aci_message_event(event_id)
    );
    CREATE INDEX IF NOT EXISTS aci_message_delivery_recipient_idx
      ON aci_message_delivery(recipient_agent, delivered_at DESC);

    CREATE TABLE IF NOT EXISTS aci_message_edge (
      edge_id TEXT PRIMARY KEY,
      event_id TEXT NOT NULL,
      edge_kind TEXT NOT NULL,
      from_node_type TEXT NOT NULL,
      from_node_id TEXT NOT NULL,
      to_node_type TEXT NOT NULL,
      to_node_id TEXT NOT NULL,
      thread TEXT,
      corr TEXT,
      job_id TEXT,
      session_id TEXT,
      occurred_at TEXT NOT NULL,
      latency_ms INTEGER,
      metadata_json TEXT,
      FOREIGN KEY(event_id) REFERENCES aci_message_event(event_id)
    );
    CREATE INDEX IF NOT EXISTS aci_message_edge_from_idx
      ON aci_message_edge(from_node_id, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_edge_to_idx
      ON aci_message_edge(to_node_id, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_edge_kind_idx
      ON aci_message_edge(edge_kind, occurred_at DESC);

    CREATE TABLE IF NOT EXISTS aci_job (
      job_id TEXT PRIMARY KEY,
      job_system TEXT,
      requester TEXT,
      assignee TEXT,
      request_event_id TEXT,
      request_env_id TEXT,
      corr TEXT,
      thread TEXT,
      thread_key TEXT,
      stream_topic TEXT,
      taskboard_event_id TEXT,
      task_id TEXT,
      agent_run_id TEXT,
      status TEXT,
      accepted_at TEXT,
      completed_at TEXT,
      terminal_event_id TEXT,
      failure_class TEXT,
      metadata_json TEXT
    );

    CREATE TABLE IF NOT EXISTS aci_conversation_session (
      session_id TEXT PRIMARY KEY,
      source_kind TEXT,
      root_thread TEXT,
      thread_key TEXT,
      requester TEXT,
      target_agent TEXT,
      stream_topic TEXT,
      started_at TEXT,
      ended_at TEXT,
      status TEXT,
      metadata_json TEXT
    );
  `)
}

export class AciArchiveStore {
  readonly db: Database
  private readonly ownsDb: boolean

  constructor(pathOrDb: string | Database = ':memory:') {
    if (typeof pathOrDb === 'string') {
      this.db = new Database(pathOrDb, { create: true })
      this.ownsDb = true
    } else {
      this.db = pathOrDb
      this.ownsDb = false
    }
    migrateAciArchiveDb(this.db)
  }

  close(): void {
    if (this.ownsDb) this.db.close()
  }

  ingestEnvelope(input: AciIngestInput): { inserted: boolean; event_id: string } {
    const env = input.env
    const observedAt = input.observedAt ?? nowIso()
    const route = routeFrom(env, input.subject, input.routeKind)
    const sourceKind = input.sourceKind === 'backfill' ? route.sourceKind : (input.sourceKind ?? route.sourceKind)
    const subject = input.subject ?? buildSubject(env, route.routeKind, route.topic)
    const rawEnvelope = input.rawEnvelope ?? JSON.stringify(env)
    const rawHash = sha256(rawEnvelope)
    const bodyHash = sha256(env.body)
    const bodySize = byteLen(env.body)
    const rawSize = byteLen(rawEnvelope)
    const bodyMeta = parseBodyMetadata(env.body)
    const eventId = env.id
    const routeTarget = route.toAgent ?? (route.topic ? `topic:${route.topic}` : env.to)
    const metadata = {
      ...bodyMeta.metadata,
      source_kind: sourceKind,
      route_kind: route.routeKind,
      body_available: bodySize > 0,
      body_redaction_state: 'metadata_only',
    }

    this.db.transaction(() => {
      this.db.prepare(`
        INSERT INTO aci_raw_envelope (
          raw_sha256, first_seen_at, size_bytes, body_sha256, body_size_bytes
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(raw_sha256) DO NOTHING
      `).run(rawHash, observedAt, rawSize, bodyHash, bodySize)

      if (env.from) this.upsertAgent(env.from, observedAt)
      if (route.toAgent) this.upsertAgent(route.toAgent, observedAt)

      this.db.prepare(`
        INSERT INTO aci_message_event (
          event_id, env_id, source_kind, source_stream, subject, stream_seq,
          route_kind, from_agent, to_agent, topic, msg_type, schema, thread, corr,
          sent_at, observed_at, delivered_at, ttl_ms, alg, sig_present,
          trust_status, route_status, raw_sha256, body_sha256, body_size_bytes,
          body_redaction_state, job_id, task_id, agent_run_id, event_ref_id,
          thread_key, stream_topic, result_ref, status, error_class, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
          observed_at = MIN(aci_message_event.observed_at, excluded.observed_at),
          delivered_at = COALESCE(aci_message_event.delivered_at, excluded.delivered_at),
          status = COALESCE(excluded.status, aci_message_event.status),
          error_class = COALESCE(excluded.error_class, aci_message_event.error_class),
          metadata_json = COALESCE(excluded.metadata_json, aci_message_event.metadata_json)
      `).run(
        eventId,
        env.id,
        sourceKind,
        'ALLOYIUM_A2A',
        subject,
        input.streamSeq ?? null,
        route.routeKind,
        env.from,
        route.toAgent,
        route.topic,
        env.type,
        bodyMeta.schema,
        env.thread ?? null,
        env.corr ?? null,
        env.ts,
        observedAt,
        input.deliveredAt ?? null,
        env.ttl_ms ?? null,
        env.alg ?? null,
        env.sig ? 1 : 0,
        input.trustStatus ?? (env.sig ? 'verified' : 'metadata_only'),
        input.routeStatus ?? 'accepted',
        rawHash,
        bodyHash,
        bodySize,
        'metadata_only',
        bodyMeta.jobId,
        bodyMeta.taskId,
        bodyMeta.agentRunId,
        bodyMeta.eventRefId,
        bodyMeta.threadKey ?? bodyMeta.sessionId,
        bodyMeta.streamTopic,
        bodyMeta.resultRef,
        bodyMeta.status,
        bodyMeta.errorClass,
        JSON.stringify(metadata),
      )

      const recipient = input.recipient ?? route.toAgent
      if (recipient) {
        this.db.prepare(`
          INSERT INTO aci_message_delivery (
            event_id, recipient_agent, delivered_at, handled, handled_at, inject_status
          ) VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(event_id, recipient_agent) DO UPDATE SET
            delivered_at = COALESCE(aci_message_delivery.delivered_at, excluded.delivered_at),
            handled = MAX(aci_message_delivery.handled, excluded.handled),
            handled_at = COALESCE(aci_message_delivery.handled_at, excluded.handled_at),
            inject_status = COALESCE(excluded.inject_status, aci_message_delivery.inject_status)
        `).run(
          eventId,
          recipient,
          input.deliveredAt ?? observedAt,
          input.handled ? 1 : 0,
          input.handledAt ?? null,
          'stored',
        )
      }

      if (env.from && routeTarget) {
        this.insertEdge({
          eventId,
          kind: route.routeKind,
          from: env.from,
          to: routeTarget,
          occurredAt: observedAt,
          thread: env.thread ?? null,
          corr: env.corr ?? null,
          jobId: bodyMeta.jobId,
          sessionId: bodyMeta.threadKey ?? bodyMeta.sessionId ?? env.thread ?? null,
          latency: latencyMs(env.ts, observedAt),
          metadata: { schema: bodyMeta.schema, msg_type: env.type },
        })
      }

      if (bodyMeta.jobId) {
        this.upsertJob({
          jobId: bodyMeta.jobId,
          eventId,
          envId: env.id,
          schema: bodyMeta.schema,
          from: env.from,
          to: route.toAgent,
          corr: env.corr ?? null,
          thread: env.thread ?? null,
          threadKey: bodyMeta.threadKey,
          streamTopic: bodyMeta.streamTopic,
          taskId: bodyMeta.taskId,
          agentRunId: bodyMeta.agentRunId,
          status: bodyMeta.status,
          failureClass: bodyMeta.errorClass,
          observedAt,
        })
        if (env.from) this.insertEdge({
          eventId,
          kind: 'job',
          from: env.from,
          to: `job:${bodyMeta.jobId}`,
          occurredAt: observedAt,
          thread: env.thread ?? null,
          corr: env.corr ?? null,
          jobId: bodyMeta.jobId,
          sessionId: bodyMeta.threadKey ?? env.thread ?? null,
          latency: null,
          metadata: { schema: bodyMeta.schema, role: 'job-event' },
        })
        if (route.toAgent) this.insertEdge({
          eventId,
          kind: 'job',
          from: `job:${bodyMeta.jobId}`,
          to: route.toAgent,
          occurredAt: observedAt,
          thread: env.thread ?? null,
          corr: env.corr ?? null,
          jobId: bodyMeta.jobId,
          sessionId: bodyMeta.threadKey ?? env.thread ?? null,
          latency: null,
          metadata: { schema: bodyMeta.schema, role: 'job-target' },
        })
      }

      const sessionId = bodyMeta.threadKey ?? bodyMeta.sessionId ?? env.thread ?? null
      if (sessionId) this.upsertSession(sessionId, {
        sourceKind,
        rootThread: env.thread ?? null,
        threadKey: bodyMeta.threadKey,
        requester: env.from,
        targetAgent: route.toAgent,
        streamTopic: bodyMeta.streamTopic,
        observedAt,
        status: bodyMeta.status,
      })
    })()

    const exists = this.db.prepare('SELECT 1 AS ok FROM aci_message_event WHERE event_id = ?').get(eventId)
    return { inserted: !!exists, event_id: eventId }
  }

  ingestInboxRow(row: InboxMessageRow): { inserted: boolean; event_id: string } {
    let env: AciEnvelopeLike | null = null
    try { env = JSON.parse(row.raw_envelope) } catch {}
    if (!env || typeof env !== 'object') {
      env = {
        id: row.env_id,
        from: row.from_agent,
        to: row.to_agent,
        type: row.msg_type,
        thread: row.thread ?? undefined,
        corr: row.corr ?? undefined,
        ts: row.ts,
        body: row.body,
        attrs: row.attrs_json ? safeJsonParseObject(row.attrs_json) as Record<string, string> : undefined,
      }
    }
    return this.ingestEnvelope({
      env,
      rawEnvelope: row.raw_envelope,
      subject: row.subject ?? undefined,
      sourceKind: 'backfill',
      routeKind: row.to_agent.startsWith('topic:') ? 'topic' : 'direct',
      observedAt: row.delivered_at,
      deliveredAt: row.delivered_at,
      recipient: row.recipient,
      handled: row.handled,
      handledAt: row.handled_at,
      trustStatus: 'legacy_unverified',
      routeStatus: 'stored',
    })
  }

  backfillInboxMessages(opts: { recipient?: string | null; limit?: number } = {}): AciBackfillResult {
    const table = this.db.prepare(`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'a2a_inbox_message'
    `).get() as { name: string } | null
    if (!table) return { scanned: 0, ingested: 0, skipped: 0 }
    const args: unknown[] = []
    let where = ''
    if (opts.recipient) {
      where = 'WHERE recipient = ?'
      args.push(opts.recipient)
    }
    const limit = normalizeLimit(opts.limit, MAX_LIMIT)
    const rows = this.db.prepare(`
      SELECT * FROM a2a_inbox_message
      ${where}
      ORDER BY delivered_at ASC, env_id ASC
      LIMIT ?
    `).all(...args, limit) as InboxMessageRow[]
    let ingested = 0
    let skipped = 0
    for (const row of rows) {
      try {
        this.ingestInboxRow(row)
        ingested += 1
      } catch {
        skipped += 1
      }
    }
    return { scanned: rows.length, ingested, skipped }
  }

  listAgents(filters: AciQueryFilters & { q?: string | null } = {}): { agents: AciNode[] } {
    const args: unknown[] = []
    const where: string[] = []
    if (filters.q) {
      where.push('agent_id LIKE ?')
      args.push(`%${filters.q}%`)
    }
    const rows = this.db.prepare(`
      SELECT
        agent_id,
        first_seen_at,
        last_seen_at,
        COALESCE((SELECT COUNT(*) FROM aci_message_event e WHERE e.from_agent = a.agent_id), 0) AS sent_count,
        COALESCE((SELECT COUNT(*) FROM aci_message_event e WHERE e.to_agent = a.agent_id), 0) AS received_count
      FROM aci_agent a
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY last_seen_at DESC, agent_id ASC
      LIMIT ?
    `).all(...args, normalizeLimit(filters.limit, 200)) as Array<{
      agent_id: string
      first_seen_at: string
      last_seen_at: string
      sent_count: number
      received_count: number
    }>
    return {
      agents: rows.map(row => ({
        id: row.agent_id,
        kind: 'agent',
        label: row.agent_id,
        first_seen_at: row.first_seen_at,
        last_seen_at: row.last_seen_at,
        counts: { sent: Number(row.sent_count) || 0, received: Number(row.received_count) || 0 },
      })),
    }
  }

  graph(filters: AciQueryFilters = {}): { window: { from: string | null; to: string | null }; nodes: AciNode[]; edges: AciEdgeAggregate[]; truncated: boolean } {
    const args: unknown[] = []
    const where = eventWhere('me', filters, args)
    const limit = normalizeLimit(filters.limit, 100)
    const rows = this.db.prepare(`
      SELECT
        e.from_node_id,
        e.to_node_id,
        e.edge_kind,
        COUNT(*) AS count,
        MIN(e.occurred_at) AS first_seen_at,
        MAX(e.occurred_at) AS last_seen_at,
        AVG(e.latency_ms) AS avg_latency_ms,
        SUM(CASE WHEN me.msg_type = 'request' THEN 1 ELSE 0 END) AS request_count,
        SUM(CASE WHEN me.msg_type = 'reply' THEN 1 ELSE 0 END) AS reply_count,
        SUM(CASE WHEN me.status IN ('failed', 'rejected') OR me.error_class IS NOT NULL THEN 1 ELSE 0 END) AS failure_count,
        COUNT(DISTINCT me.thread) AS thread_count,
        COUNT(DISTINCT me.job_id) AS job_count,
        GROUP_CONCAT(DISTINCT me.schema) AS schemas,
        GROUP_CONCAT(DISTINCT me.msg_type) AS msg_types
      FROM aci_message_edge e
      JOIN aci_message_event me ON me.event_id = e.event_id
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      GROUP BY e.from_node_id, e.to_node_id, e.edge_kind
      ORDER BY count DESC, last_seen_at DESC
      LIMIT ?
    `).all(...args, limit + 1) as any[]
    const page = rows.slice(0, limit)
    const nodes = new Map<string, AciNode>()
    const addNode = (id: string, count: number) => {
      const prev = nodes.get(id)
      if (prev) {
        prev.counts.events += count
        return
      }
      nodes.set(id, {
        id,
        kind: nodeKind(id),
        label: nodeLabel(id),
        first_seen_at: null,
        last_seen_at: null,
        counts: { events: count },
      })
    }
    const edges = page.map(row => {
      addNode(row.from_node_id, Number(row.count) || 0)
      addNode(row.to_node_id, Number(row.count) || 0)
      return {
        id: `${row.edge_kind}:${row.from_node_id}->${row.to_node_id}`,
        from: row.from_node_id,
        to: row.to_node_id,
        kind: row.edge_kind,
        count: Number(row.count) || 0,
        first_seen_at: row.first_seen_at,
        last_seen_at: row.last_seen_at,
        p50_latency_ms: row.avg_latency_ms == null ? null : Math.round(Number(row.avg_latency_ms)),
        p95_latency_ms: row.avg_latency_ms == null ? null : Math.round(Number(row.avg_latency_ms)),
        request_count: Number(row.request_count) || 0,
        reply_count: Number(row.reply_count) || 0,
        failure_count: Number(row.failure_count) || 0,
        thread_count: Number(row.thread_count) || 0,
        job_count: Number(row.job_count) || 0,
        schemas: splitCsv(row.schemas),
        msg_types: splitCsv(row.msg_types),
      }
    })
    return {
      window: { from: filters.from ?? null, to: filters.to ?? null },
      nodes: [...nodes.values()],
      edges,
      truncated: rows.length > limit,
    }
  }

  matrix(filters: AciQueryFilters & { metric?: string | null; includeTopics?: boolean } = {}): {
    window: { from: string | null; to: string | null }
    metric: string
    rows: AciNode[]
    cols: AciNode[]
    cells: Array<{ row: string; col: string; count: number; last_seen_at: string | null; failure_count: number; score: number }>
    truncated: boolean
  } {
    const args: unknown[] = []
    const where = eventWhere('me', filters, args)
    const edgeFilter = filters.includeTopics === false ? "e.edge_kind = 'direct'" : "e.edge_kind IN ('direct', 'topic')"
    where.push(edgeFilter)
    const limit = normalizeLimit(filters.limit, 200)
    const rows = this.db.prepare(`
      SELECT
        e.from_node_id AS row_id,
        e.to_node_id AS col_id,
        COUNT(*) AS count,
        MAX(e.occurred_at) AS last_seen_at,
        SUM(CASE WHEN me.status IN ('failed', 'rejected') OR me.error_class IS NOT NULL THEN 1 ELSE 0 END) AS failure_count
      FROM aci_message_edge e
      JOIN aci_message_event me ON me.event_id = e.event_id
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      GROUP BY e.from_node_id, e.to_node_id
      ORDER BY count DESC, last_seen_at DESC
      LIMIT ?
    `).all(...args, limit + 1) as any[]
    const page = rows.slice(0, limit)
    const rowNodes = new Map<string, AciNode>()
    const colNodes = new Map<string, AciNode>()
    const maxCount = Math.max(1, ...page.map(row => Number(row.count) || 0))
    const metric = filters.metric ?? 'count'
    const cells = page.map(row => {
      if (!rowNodes.has(row.row_id)) rowNodes.set(row.row_id, { id: row.row_id, kind: nodeKind(row.row_id), label: nodeLabel(row.row_id), first_seen_at: null, last_seen_at: row.last_seen_at, counts: {} })
      if (!colNodes.has(row.col_id)) colNodes.set(row.col_id, { id: row.col_id, kind: nodeKind(row.col_id), label: nodeLabel(row.col_id), first_seen_at: null, last_seen_at: row.last_seen_at, counts: {} })
      const count = Number(row.count) || 0
      const failures = Number(row.failure_count) || 0
      return {
        row: row.row_id,
        col: row.col_id,
        count,
        last_seen_at: row.last_seen_at,
        failure_count: failures,
        score: metric === 'failures' ? failures : count / maxCount,
      }
    })
    return {
      window: { from: filters.from ?? null, to: filters.to ?? null },
      metric,
      rows: [...rowNodes.values()],
      cols: [...colNodes.values()],
      cells,
      truncated: rows.length > limit,
    }
  }

  timeline(filters: AciQueryFilters = {}): { window: { from: string | null; to: string | null }; lanes: AciNode[]; events: AciEvent[]; next_cursor: string | null; truncated: boolean } {
    const args: unknown[] = []
    const order = filters.order === 'asc' ? 'ASC' : 'DESC'
    const where = eventWhere('e', filters, args)
    const cursor = decodeCursor(filters.cursor)
    if (cursor) {
      if (order === 'ASC') where.push('(e.observed_at > ? OR (e.observed_at = ? AND e.event_id > ?))')
      else where.push('(e.observed_at < ? OR (e.observed_at = ? AND e.event_id < ?))')
      args.push(cursor.observed_at, cursor.observed_at, cursor.event_id)
    }
    const limit = normalizeLimit(filters.limit, DEFAULT_LIMIT)
    const rows = this.db.prepare(`
      SELECT * FROM aci_message_event e
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY e.observed_at ${order}, e.event_id ${order}
      LIMIT ?
    `).all(...args, limit + 1) as EventRow[]
    const page = rows.slice(0, limit)
    const laneIds = new Set<string>()
    for (const row of page) {
      if (row.from_agent) laneIds.add(row.from_agent)
      if (row.to_agent) laneIds.add(row.to_agent)
      if (row.topic) laneIds.add(`topic:${row.topic}`)
    }
    return {
      window: { from: filters.from ?? null, to: filters.to ?? null },
      lanes: [...laneIds].map(id => ({ id, kind: nodeKind(id), label: nodeLabel(id), first_seen_at: null, last_seen_at: null, counts: {} })),
      events: page.map(eventToApi),
      next_cursor: rows.length > limit && page.length > 0 ? encodeCursor(page[page.length - 1]) : null,
      truncated: rows.length > limit,
    }
  }

  ego(agentId: string, filters: AciQueryFilters = {}): { center: AciNode; nodes: AciNode[]; edges: AciEdgeAggregate[]; events: AciEvent[]; recent_threads: string[]; recent_jobs: string[] } {
    const graph = this.graph({ ...filters, agents: [agentId], limit: filters.limit ?? 80 })
    const timeline = this.timeline({ ...filters, agents: [agentId], limit: 50 })
    return {
      center: { id: agentId, kind: nodeKind(agentId), label: nodeLabel(agentId), first_seen_at: null, last_seen_at: null, counts: {} },
      nodes: graph.nodes,
      edges: graph.edges,
      events: timeline.events,
      recent_threads: [...new Set(timeline.events.map(e => e.thread).filter(Boolean) as string[])].slice(0, 20),
      recent_jobs: [...new Set(timeline.events.map(e => e.job_id).filter(Boolean) as string[])].slice(0, 20),
    }
  }

  thread(thread: string, filters: AciQueryFilters = {}): ReturnType<AciArchiveStore['timeline']> {
    return this.timeline({ ...filters, thread })
  }

  job(jobId: string, filters: AciQueryFilters = {}): ReturnType<AciArchiveStore['timeline']> {
    return this.timeline({ ...filters, jobId })
  }

  stats(): { events: number; agents: number; edges: number; jobs: number; sessions: number } {
    const one = (sql: string) => Number((this.db.prepare(sql).get() as { n: number } | null)?.n ?? 0)
    return {
      events: one('SELECT COUNT(*) AS n FROM aci_message_event'),
      agents: one('SELECT COUNT(*) AS n FROM aci_agent'),
      edges: one('SELECT COUNT(*) AS n FROM aci_message_edge'),
      jobs: one('SELECT COUNT(*) AS n FROM aci_job'),
      sessions: one('SELECT COUNT(*) AS n FROM aci_conversation_session'),
    }
  }

  private upsertAgent(agentId: string, seenAt: string): void {
    this.db.prepare(`
      INSERT INTO aci_agent(agent_id, first_seen_at, last_seen_at)
      VALUES (?, ?, ?)
      ON CONFLICT(agent_id) DO UPDATE SET
        first_seen_at = MIN(aci_agent.first_seen_at, excluded.first_seen_at),
        last_seen_at = MAX(aci_agent.last_seen_at, excluded.last_seen_at)
    `).run(agentId, seenAt, seenAt)
  }

  private insertEdge(input: {
    eventId: string
    kind: string
    from: string
    to: string
    occurredAt: string
    thread: string | null
    corr: string | null
    jobId: string | null
    sessionId: string | null
    latency: number | null
    metadata: Record<string, unknown>
  }): void {
    const fromKind = nodeKind(input.from)
    const toKind = nodeKind(input.to)
    const edgeId = `${input.eventId}:${input.kind}:${input.from}->${input.to}`
    this.db.prepare(`
      INSERT INTO aci_message_edge (
        edge_id, event_id, edge_kind, from_node_type, from_node_id, to_node_type,
        to_node_id, thread, corr, job_id, session_id, occurred_at, latency_ms,
        metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(edge_id) DO NOTHING
    `).run(
      edgeId,
      input.eventId,
      input.kind,
      fromKind,
      input.from,
      toKind,
      input.to,
      input.thread,
      input.corr,
      input.jobId,
      input.sessionId,
      input.occurredAt,
      input.latency,
      JSON.stringify(input.metadata),
    )
  }

  private upsertJob(input: {
    jobId: string
    eventId: string
    envId: string
    schema: string | null
    from: string
    to: string | null
    corr: string | null
    thread: string | null
    threadKey: string | null
    streamTopic: string | null
    taskId: string | null
    agentRunId: string | null
    status: string | null
    failureClass: string | null
    observedAt: string
  }): void {
    const terminal = input.status === 'completed' || input.status === 'failed' || input.status === 'rejected'
    this.db.prepare(`
      INSERT INTO aci_job (
        job_id, job_system, requester, assignee, request_event_id, request_env_id,
        corr, thread, thread_key, stream_topic, task_id, agent_run_id, status,
        accepted_at, completed_at, terminal_event_id, failure_class, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(job_id) DO UPDATE SET
        assignee = COALESCE(aci_job.assignee, excluded.assignee),
        corr = COALESCE(aci_job.corr, excluded.corr),
        thread = COALESCE(aci_job.thread, excluded.thread),
        thread_key = COALESCE(aci_job.thread_key, excluded.thread_key),
        stream_topic = COALESCE(aci_job.stream_topic, excluded.stream_topic),
        task_id = COALESCE(aci_job.task_id, excluded.task_id),
        agent_run_id = COALESCE(aci_job.agent_run_id, excluded.agent_run_id),
        status = COALESCE(excluded.status, aci_job.status),
        accepted_at = COALESCE(aci_job.accepted_at, excluded.accepted_at),
        completed_at = COALESCE(aci_job.completed_at, excluded.completed_at),
        terminal_event_id = COALESCE(excluded.terminal_event_id, aci_job.terminal_event_id),
        failure_class = COALESCE(excluded.failure_class, aci_job.failure_class),
        metadata_json = COALESCE(excluded.metadata_json, aci_job.metadata_json)
    `).run(
      input.jobId,
      input.schema?.split('.')[0] ?? null,
      input.from,
      input.to,
      input.schema?.includes('.job.request') ? input.eventId : null,
      input.schema?.includes('.job.request') ? input.envId : null,
      input.corr,
      input.thread,
      input.threadKey,
      input.streamTopic,
      input.taskId,
      input.agentRunId,
      input.status,
      input.status === 'accepted' ? input.observedAt : null,
      terminal ? input.observedAt : null,
      terminal ? input.eventId : null,
      input.failureClass,
      JSON.stringify({ latest_schema: input.schema }),
    )
  }

  private upsertSession(sessionId: string, input: {
    sourceKind: string
    rootThread: string | null
    threadKey: string | null
    requester: string | null
    targetAgent: string | null
    streamTopic: string | null
    observedAt: string
    status: string | null
  }): void {
    this.db.prepare(`
      INSERT INTO aci_conversation_session (
        session_id, source_kind, root_thread, thread_key, requester, target_agent,
        stream_topic, started_at, ended_at, status, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(session_id) DO UPDATE SET
        ended_at = MAX(COALESCE(aci_conversation_session.ended_at, excluded.ended_at), excluded.ended_at),
        status = COALESCE(excluded.status, aci_conversation_session.status),
        target_agent = COALESCE(aci_conversation_session.target_agent, excluded.target_agent),
        stream_topic = COALESCE(aci_conversation_session.stream_topic, excluded.stream_topic)
    `).run(
      sessionId,
      input.sourceKind,
      input.rootThread,
      input.threadKey,
      input.requester,
      input.targetAgent,
      input.streamTopic,
      input.observedAt,
      input.observedAt,
      input.status,
      JSON.stringify({ metadata_only: true }),
    )
  }
}
