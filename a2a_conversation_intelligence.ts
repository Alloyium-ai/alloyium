import { Database } from 'bun:sqlite'
import { createHash } from 'node:crypto'
import type { InboxMessageRow } from './a2a_inbox_store.ts'

type RouteKind = 'direct' | 'topic' | 'unknown'

export type ACIEnvelopeLike = {
  v?: number
  id?: string
  from?: string
  to?: string
  type?: string
  thread?: string
  corr?: string
  ts?: string
  ttl_ms?: number
  body?: unknown
  attrs?: Record<string, unknown>
  alg?: string
  sig?: string
}

export type ACIIngestInput = {
  envelope: ACIEnvelopeLike
  rawEnvelope?: string
  subject?: string | null
  sourceKind?: string
  sourceStream?: string | null
  streamSeq?: number | null
  routeKind?: RouteKind
  observedAt?: string
  deliveredAt?: string | null
  trustStatus?: string | null
  routeStatus?: string | null
  delivery?: {
    recipientAgent?: string | null
    durableName?: string | null
    deliveredAt?: string | null
    injectedAt?: string | null
    ackedAt?: string | null
    handled?: boolean | number | null
    handledAt?: string | null
    injectStatus?: string | null
  }
}

export type ACITimeFilter = {
  since?: string | null
  until?: string | null
}

export type ACITimelineFilters = ACITimeFilter & {
  thread?: string | null
  threadKey?: string | null
  sessionId?: string | null
  agent?: string | null
  jobId?: string | null
  topic?: string | null
  corr?: string | null
  limit?: number
}

export type ACIIngestResult = {
  ok: true
  event_id: string
  inserted: boolean
  raw_sha256: string
  body_sha256: string
}

const MAX_LIMIT = 500
const DEFAULT_LIMIT = 100

function nowIso(): string { return new Date().toISOString() }

function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex')
}

function bytes(text: string): number {
  return Buffer.byteLength(text, 'utf8')
}

const SECRETISH_RE: RegExp[] = [
  /-----BEGIN[\s\S]*?-----END[A-Z0-9 ]*-----/i,
  /\b(?:sk|rk|pk|ghp|gho|github_pat|xox[baprs])[-_][A-Za-z0-9-]{12,}\b/i,
  /\bAKIA[0-9A-Z]{16}\b/,
  /\b[A-Za-z][A-Za-z0-9+.-]*:\/\/[^/\s:@]+:[^@\s]+@/,
  /\b(?:api[_-]?key|token|secret|password|passwd|pwd)=\S+/i,
]

function looksSecretish(s: string): boolean {
  return SECRETISH_RE.some((re) => re.test(s))
}

function asString(v: unknown, maxLen = 256): string | null {
  if (typeof v !== 'string') return null
  const s = v.trim()
  if (!s || s.length > maxLen || /[\u0000-\u001f]/.test(s) || looksSecretish(s)) return null
  return s
}

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null
}

function normalizeLimit(limit: number | undefined, fallback = DEFAULT_LIMIT): number {
  return Math.min(Math.max(Math.trunc(limit ?? fallback), 1), MAX_LIMIT)
}

function safeJson(value: unknown): string {
  return JSON.stringify(value ?? {})
}

function parseJsonObject(text: string | null | undefined): Record<string, any> | null {
  if (!text) return null
  try {
    const parsed = JSON.parse(text)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null
  } catch {
    return null
  }
}

function envelopeBodyText(env: ACIEnvelopeLike): string {
  if (typeof env.body === 'string') return env.body
  if (env.body == null) return ''
  try { return JSON.stringify(env.body) } catch { return String(env.body) }
}

function topicFromSubject(subject: string | null | undefined): string | null {
  const m = String(subject ?? '').match(/^alloyium\.a2a\.topic\.([a-z0-9-]+)$/)
  return m ? m[1] : null
}

function inboxRecipientFromSubject(subject: string | null | undefined): string | null {
  const m = String(subject ?? '').match(/^alloyium\.a2a\.agent\.([a-z0-9-]+)\.inbox$/)
  return m ? m[1] : null
}

function routeKindFromSubject(subject: string | null | undefined, to: string | null): RouteKind {
  if (topicFromSubject(subject) || to?.startsWith('topic:')) return 'topic'
  if (inboxRecipientFromSubject(subject)) return 'direct'
  return 'unknown'
}

function topicFromRoute(subject: string | null | undefined, to: string | null): string | null {
  return topicFromSubject(subject) ?? (to?.startsWith('topic:') ? to.slice('topic:'.length) : null)
}

function eventIdFor(sourceKind: string, subject: string | null, envId: string | null, rawHash: string): string {
  return `aci-${sha256(`${sourceKind}\n${subject ?? ''}\n${envId ?? rawHash}`).slice(0, 32)}`
}

function edgeIdFor(eventId: string, edgeKind: string, fromNode: string, toNode: string): string {
  return `edge-${sha256(`${eventId}\n${edgeKind}\n${fromNode}\n${toNode}`).slice(0, 32)}`
}

function statusFromBody(schema: string | null, body: Record<string, any> | null): string | null {
  const explicit = normalizedClass(body?.status)
  if (explicit) return explicit
  if (!schema) return null
  if (schema.endsWith('.request.v1')) return 'requested'
  if (schema.endsWith('.accepted.v1')) return 'accepted'
  if (schema.endsWith('.completed.v1')) return 'completed'
  if (schema.endsWith('.failed.v1')) return 'failed'
  if (schema.endsWith('.rejected.v1')) return 'rejected'
  if (schema.endsWith('.delta.v1')) return 'delta'
  if (schema.includes('.turn_failed.')) return 'failed'
  if (schema.includes('.turn_completed.')) return 'completed'
  return null
}

function jobSystemFromSchema(schema: string | null): string | null {
  if (!schema) return null
  const m = schema.match(/^([a-z0-9_-]+)\.job\./)
  return m ? m[1] : null
}

function statusIsTerminal(status: string | null): boolean {
  return status === 'completed' || status === 'failed' || status === 'rejected'
}

function normalizedClass(v: unknown): string | null {
  const s = asString(v)
  if (!s) return null
  const out = s.toLowerCase().replace(/[^a-z0-9_.-]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 64)
  return out || null
}

function errorClassFromBody(status: string | null, body: Record<string, any> | null): string | null {
  const reason = normalizedClass(body?.reason ?? body?.detail)
  if (reason) return reason
  if (body?.error != null) return 'error'
  return status === 'failed' || status === 'rejected' ? status : null
}

function safeBodyMetadata(body: Record<string, any> | null): Record<string, unknown> {
  if (!body) return { parsed_body: false }
  const out: Record<string, unknown> = {
    parsed_body: true,
    schema: asString(body.schema),
    output_preview: typeof body.output_preview === 'boolean' ? body.output_preview : undefined,
    truncated: typeof body.truncated === 'boolean' ? body.truncated : undefined,
    encoding: asString(body.encoding),
    len: asNumber(body.len),
    body_keys: Object.keys(body).filter(k => !['output', 'partial_output', 'input', 'messages', 'body', 'text', 'error', 'detail'].includes(k)).sort(),
  }
  for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k]
  return out
}

function encryptedState(body: Record<string, any> | null): string {
  if (!body) return 'unknown'
  if (body.encrypted === true || typeof body.ciphertext === 'string' || String(body.schema ?? '').includes('encrypted')) return 'encrypted'
  return 'plaintext'
}

function numberOrNull(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v
  if (typeof v === 'string' && v.trim() && Number.isFinite(Number(v))) return Number(v)
  return null
}

function taskIdFromBody(body: Record<string, any> | null): number | null {
  return numberOrNull(body?.task_id) ?? numberOrNull(body?.task_context?.task_id)
}

function agentRunIdFromBody(body: Record<string, any> | null): number | null {
  return numberOrNull(body?.agent_run_id) ?? numberOrNull(body?.task_context?.agent_run_id)
}

function taskboardEventIdFromBody(body: Record<string, any> | null): string | null {
  return asString(body?.taskboard_event_id) ?? asString(body?.event_id) ?? asString(body?.task_context?.event_id)
}

function queryTimeWhere(prefix: string, filters: ACITimeFilter = {}): { where: string[]; args: unknown[] } {
  const where: string[] = []
  const args: unknown[] = []
  if (filters.since) { where.push(`${prefix} >= ?`); args.push(filters.since) }
  if (filters.until) { where.push(`${prefix} <= ?`); args.push(filters.until) }
  return { where, args }
}

export function initializeACIDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

export function migrateACIDb(db: Database): void {
  initializeACIDb(db)
  db.exec(`
    CREATE TABLE IF NOT EXISTS aci_schema_migrations (
      name TEXT PRIMARY KEY,
      version INTEGER NOT NULL,
      applied_at TEXT NOT NULL
    );
    INSERT OR IGNORE INTO aci_schema_migrations(name, version, applied_at)
      VALUES ('agent-conversation-intelligence', 1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));

    CREATE TABLE IF NOT EXISTS aci_agent (
      agent_id TEXT PRIMARY KEY,
      kind TEXT,
      display_name TEXT,
      first_seen_at TEXT,
      last_seen_at TEXT,
      last_host TEXT,
      presence_json TEXT,
      metadata_json TEXT
    );

    CREATE TABLE IF NOT EXISTS aci_raw_envelope (
      raw_sha256 TEXT PRIMARY KEY,
      first_seen_at TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      encoding TEXT NOT NULL,
      body_sha256 TEXT,
      body_size_bytes INTEGER,
      body_redaction_state TEXT NOT NULL,
      encrypted_state TEXT NOT NULL,
      retention_class TEXT NOT NULL,
      raw_ref TEXT,
      metadata_json TEXT
    );

    CREATE TABLE IF NOT EXISTS aci_message_event (
      event_id TEXT PRIMARY KEY,
      env_id TEXT,
      source_kind TEXT NOT NULL,
      source_stream TEXT,
      subject TEXT,
      stream_seq INTEGER,
      route_kind TEXT NOT NULL,
      from_agent TEXT,
      to_agent TEXT,
      topic TEXT,
      msg_type TEXT,
      thread TEXT,
      corr TEXT,
      sent_at TEXT,
      observed_at TEXT NOT NULL,
      delivered_at TEXT,
      ttl_ms INTEGER,
      alg TEXT,
      sig_present INTEGER NOT NULL DEFAULT 0 CHECK (sig_present IN (0,1)),
      trust_status TEXT,
      route_status TEXT,
      raw_sha256 TEXT,
      body_schema TEXT,
      job_id TEXT,
      task_id INTEGER,
      agent_run_id INTEGER,
      event_ref_id TEXT,
      triage_id TEXT,
      thread_key TEXT,
      stream_topic TEXT,
      result_ref TEXT,
      status TEXT,
      error_class TEXT,
      metadata_json TEXT,
      FOREIGN KEY(raw_sha256) REFERENCES aci_raw_envelope(raw_sha256)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS aci_message_event_source_env_idx
      ON aci_message_event(source_kind, subject, env_id)
      WHERE env_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS aci_message_event_observed_idx
      ON aci_message_event(observed_at DESC, event_id DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_agent_idx
      ON aci_message_event(from_agent, to_agent, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_thread_idx
      ON aci_message_event(thread, thread_key, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_job_idx
      ON aci_message_event(job_id, observed_at DESC);
    CREATE INDEX IF NOT EXISTS aci_message_event_topic_idx
      ON aci_message_event(topic, observed_at DESC);

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
    CREATE INDEX IF NOT EXISTS aci_message_edge_graph_idx
      ON aci_message_edge(edge_kind, from_node_id, to_node_id, occurred_at DESC);

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
      task_id INTEGER,
      agent_run_id INTEGER,
      status TEXT,
      accepted_at TEXT,
      completed_at TEXT,
      terminal_event_id TEXT,
      failure_class TEXT,
      metadata_json TEXT
    );
    CREATE INDEX IF NOT EXISTS aci_job_status_idx ON aci_job(status, job_id);
    CREATE INDEX IF NOT EXISTS aci_job_task_idx ON aci_job(task_id, agent_run_id);

    CREATE TABLE IF NOT EXISTS aci_external_event (
      source_kind TEXT NOT NULL,
      external_id TEXT NOT NULL,
      subject TEXT,
      occurred_at TEXT,
      raw_sha256 TEXT,
      schema TEXT,
      task_id INTEGER,
      agent_run_id INTEGER,
      metadata_json TEXT,
      PRIMARY KEY(source_kind, external_id)
    );
  `)
}

export class ACIStore {
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
    migrateACIDb(this.db)
  }

  close(): void {
    if (this.ownsDb) this.db.close()
  }

  ingestRawEnvelope(input: Omit<ACIIngestInput, 'envelope'> & { rawEnvelope: string }): ACIIngestResult {
    try {
      const parsed = JSON.parse(input.rawEnvelope)
      return this.ingestA2AEnvelope({ ...input, envelope: parsed })
    } catch {
      const observedAt = input.observedAt ?? nowIso()
      const rawHash = sha256(input.rawEnvelope)
      const eventId = eventIdFor(input.sourceKind ?? 'a2a', input.subject ?? null, null, rawHash)
      const existed = !!this.db.prepare('SELECT 1 FROM aci_message_event WHERE event_id = ?').get(eventId)
      this.db.prepare(`
        INSERT INTO aci_raw_envelope (
          raw_sha256, first_seen_at, size_bytes, encoding, body_sha256, body_size_bytes,
          body_redaction_state, encrypted_state, retention_class, raw_ref, metadata_json
        ) VALUES (?, ?, ?, 'utf8', NULL, NULL, 'metadata_only', 'unknown', 'hash_only', NULL, ?)
        ON CONFLICT(raw_sha256) DO NOTHING
      `).run(rawHash, observedAt, bytes(input.rawEnvelope), safeJson({ parsed_envelope: false }))
      this.db.prepare(`
        INSERT INTO aci_message_event (
          event_id, source_kind, source_stream, subject, route_kind, topic, observed_at,
          trust_status, route_status, raw_sha256, error_class, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'malformed_json', ?, 'malformed_json', ?)
        ON CONFLICT(event_id) DO NOTHING
      `).run(
        eventId,
        input.sourceKind ?? 'a2a',
        input.sourceStream ?? null,
        input.subject ?? null,
        input.routeKind ?? routeKindFromSubject(input.subject, null),
        topicFromSubject(input.subject),
        observedAt,
        input.trustStatus ?? 'unverified',
        rawHash,
        safeJson({ parsed_envelope: false }),
      )
      return { ok: true, event_id: eventId, inserted: !existed, raw_sha256: rawHash, body_sha256: '' }
    }
  }

  ingestA2AEnvelope(input: ACIIngestInput): ACIIngestResult {
    const env = input.envelope
    const bodyText = envelopeBodyText(env)
    const body = parseJsonObject(bodyText)
    const rawEnvelope = input.rawEnvelope ?? JSON.stringify(env)
    const rawHash = sha256(rawEnvelope)
    const bodyHash = sha256(bodyText)
    const observedAt = input.observedAt ?? nowIso()
    const sourceKind = input.sourceKind ?? 'a2a'
    const subject = input.subject ?? null
    const envId = asString(env.id)
    const eventId = eventIdFor(sourceKind, subject, envId, rawHash)
    const existed = !!this.db.prepare('SELECT 1 FROM aci_message_event WHERE event_id = ?').get(eventId)

    const fromAgent = asString(env.from)
    const rawTo = asString(env.to)
    const routeKind = input.routeKind ?? routeKindFromSubject(subject, rawTo)
    const topic = topicFromRoute(subject, rawTo)
    const toAgent = routeKind === 'topic' ? null : rawTo
    const recipient = input.delivery?.recipientAgent ?? inboxRecipientFromSubject(subject) ?? toAgent
    const schema = asString(body?.schema)
    const status = statusFromBody(schema, body)
    const jobId = asString(body?.job_id)
    const threadKey = asString(body?.thread_key) ?? asString(body?.session_id)
    const streamTopic = asString(body?.stream_topic)
    const taskId = taskIdFromBody(body)
    const agentRunId = agentRunIdFromBody(body)
    const errorClass = errorClassFromBody(status, body)
    const sessionId = threadKey ?? asString(env.thread) ?? asString(env.corr) ?? jobId

    this.db.prepare(`
      INSERT INTO aci_raw_envelope (
        raw_sha256, first_seen_at, size_bytes, encoding, body_sha256, body_size_bytes,
        body_redaction_state, encrypted_state, retention_class, raw_ref, metadata_json
      ) VALUES (?, ?, ?, 'utf8', ?, ?, 'metadata_only', ?, 'hash_only', NULL, ?)
      ON CONFLICT(raw_sha256) DO NOTHING
    `).run(
      rawHash,
      observedAt,
      bytes(rawEnvelope),
      bodyHash,
      bytes(bodyText),
      encryptedState(body),
      safeJson(safeBodyMetadata(body)),
    )

    this.upsertAgent(fromAgent, observedAt)
    if (toAgent) this.upsertAgent(toAgent, observedAt)

    this.db.prepare(`
      INSERT INTO aci_message_event (
        event_id, env_id, source_kind, source_stream, subject, stream_seq, route_kind,
        from_agent, to_agent, topic, msg_type, thread, corr, sent_at, observed_at, delivered_at,
        ttl_ms, alg, sig_present, trust_status, route_status, raw_sha256, body_schema,
        job_id, task_id, agent_run_id, event_ref_id, triage_id, thread_key, stream_topic,
        result_ref, status, error_class, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        observed_at = excluded.observed_at,
        delivered_at = COALESCE(aci_message_event.delivered_at, excluded.delivered_at),
        trust_status = COALESCE(excluded.trust_status, aci_message_event.trust_status),
        route_status = COALESCE(excluded.route_status, aci_message_event.route_status),
        status = COALESCE(excluded.status, aci_message_event.status),
        error_class = COALESCE(excluded.error_class, aci_message_event.error_class),
        metadata_json = excluded.metadata_json
    `).run(
      eventId,
      envId,
      sourceKind,
      input.sourceStream ?? null,
      subject,
      input.streamSeq ?? null,
      routeKind,
      fromAgent,
      toAgent,
      topic,
      asString(env.type),
      asString(env.thread),
      asString(env.corr),
      asString(env.ts) ?? observedAt,
      observedAt,
      input.deliveredAt ?? input.delivery?.deliveredAt ?? null,
      asNumber(env.ttl_ms),
      asString(env.alg),
      env.sig ? 1 : 0,
      input.trustStatus ?? (env.sig ? 'signed_observed' : 'unsigned_observed'),
      input.routeStatus ?? 'observed',
      rawHash,
      schema,
      jobId,
      taskId,
      agentRunId,
      taskboardEventIdFromBody(body),
      asString(body?.triage_id),
      threadKey,
      streamTopic,
      asString(body?.result_ref),
      status,
      errorClass,
      safeJson(safeBodyMetadata(body)),
    )

    if (recipient) {
      this.db.prepare(`
        INSERT INTO aci_message_delivery (
          event_id, recipient_agent, durable_name, delivered_at, injected_at, acked_at,
          handled, handled_at, inject_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, recipient_agent) DO UPDATE SET
          durable_name = COALESCE(excluded.durable_name, aci_message_delivery.durable_name),
          delivered_at = COALESCE(excluded.delivered_at, aci_message_delivery.delivered_at),
          injected_at = COALESCE(excluded.injected_at, aci_message_delivery.injected_at),
          acked_at = COALESCE(excluded.acked_at, aci_message_delivery.acked_at),
          handled = MAX(aci_message_delivery.handled, excluded.handled),
          handled_at = COALESCE(excluded.handled_at, aci_message_delivery.handled_at),
          inject_status = COALESCE(excluded.inject_status, aci_message_delivery.inject_status)
      `).run(
        eventId,
        recipient,
        input.delivery?.durableName ?? null,
        input.delivery?.deliveredAt ?? input.deliveredAt ?? null,
        input.delivery?.injectedAt ?? null,
        input.delivery?.ackedAt ?? null,
        input.delivery?.handled ? 1 : 0,
        input.delivery?.handledAt ?? null,
        input.delivery?.injectStatus ?? null,
      )
    }

    if (fromAgent && (toAgent || topic)) {
      const edgeKind = routeKind === 'topic'
        ? 'a2a_topic_publish'
        : env.type === 'request' ? 'a2a_request'
          : env.type === 'reply' ? 'a2a_reply'
            : 'a2a_msg'
      const toNodeType = routeKind === 'topic' ? 'topic' : 'agent'
      const toNodeId = routeKind === 'topic' ? topic! : toAgent!
      this.upsertEdge(eventId, edgeKind, 'agent', fromAgent, toNodeType, toNodeId, {
        thread: asString(env.thread),
        corr: asString(env.corr),
        jobId,
        sessionId,
        occurredAt: observedAt,
        metadata: { body_schema: schema },
      })
      if (jobId) {
        this.upsertEdge(eventId, 'job_lifecycle', 'agent', fromAgent, 'job', jobId, {
          thread: asString(env.thread),
          corr: asString(env.corr),
          jobId,
          sessionId,
          occurredAt: observedAt,
          metadata: { status, body_schema: schema },
        })
      }
    }

    if (sessionId) {
      this.upsertSession({
        sessionId,
        sourceKind,
        rootThread: asString(env.thread),
        threadKey,
        requester: env.type === 'request' ? fromAgent : null,
        targetAgent: env.type === 'request' ? toAgent : null,
        streamTopic,
        observedAt,
        status,
      })
    }
    if (jobId) {
      this.upsertJob({
        jobId,
        schema,
        status,
        fromAgent,
        toAgent,
        eventId,
        envId,
        corr: asString(env.corr),
        thread: asString(env.thread),
        threadKey,
        streamTopic,
        taskboardEventId: taskboardEventIdFromBody(body),
        taskId,
        agentRunId,
        observedAt,
        errorClass,
      })
    }

    return { ok: true, event_id: eventId, inserted: !existed, raw_sha256: rawHash, body_sha256: bodyHash }
  }

  backfillFromInboxRows(rows: InboxMessageRow[], opts: { sourceKind?: string; sourceStream?: string } = {}): { scanned: number; ingested: number } {
    let ingested = 0
    for (const row of rows) {
      const raw = row.raw_envelope || JSON.stringify({
        id: row.env_id,
        from: row.from_agent,
        to: row.to_agent,
        type: row.msg_type,
        thread: row.thread ?? undefined,
        corr: row.corr ?? undefined,
        ts: row.ts,
        body: row.body,
      })
      const before = this.db.query('SELECT COUNT(*) AS n FROM aci_message_event').get() as { n: number }
      const res = this.ingestRawEnvelope({
        rawEnvelope: raw,
        subject: row.subject,
        sourceKind: opts.sourceKind ?? 'a2a',
        sourceStream: opts.sourceStream ?? 'a2a_inbox_message',
        routeKind: row.subject ? routeKindFromSubject(row.subject, row.to_agent) : 'direct',
        observedAt: row.delivered_at,
        deliveredAt: row.delivered_at,
        delivery: {
          recipientAgent: row.recipient,
          deliveredAt: row.delivered_at,
          handled: !!row.handled,
          handledAt: row.handled_at,
        },
      })
      const after = this.db.query('SELECT COUNT(*) AS n FROM aci_message_event').get() as { n: number }
      if (res.inserted || after.n > before.n) ingested++
    }
    return { scanned: rows.length, ingested }
  }

  backfillFromInboxDb(pathOrDb: string | Database, opts: { limit?: number; since?: string | null } = {}): { scanned: number; ingested: number } {
    const db = typeof pathOrDb === 'string' ? new Database(pathOrDb, { readonly: true }) : pathOrDb
    try {
      const where: string[] = []
      const args: unknown[] = []
      if (opts.since) { where.push('delivered_at >= ?'); args.push(opts.since) }
      const rows = db.prepare(`
        SELECT * FROM a2a_inbox_message
        ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY delivered_at ASC, env_id ASC
        LIMIT ?
      `).all(...args, normalizeLimit(opts.limit, MAX_LIMIT)) as InboxMessageRow[]
      return this.backfillFromInboxRows(rows)
    } finally {
      if (typeof pathOrDb === 'string') db.close()
    }
  }

  summary(filters: ACITimeFilter = {}): Record<string, unknown> {
    const q = queryTimeWhere('observed_at', filters)
    const where = q.where.length ? `WHERE ${q.where.join(' AND ')}` : ''
    const one = (sql: string, args: unknown[] = q.args) => (this.db.prepare(sql).get(...args) as any)?.n ?? 0
    const last = this.db.prepare(`SELECT MAX(observed_at) AS t FROM aci_message_event ${where}`).get(...q.args) as { t: string | null }
    return {
      ok: true,
      events: one(`SELECT COUNT(*) AS n FROM aci_message_event ${where}`),
      agents: one('SELECT COUNT(*) AS n FROM aci_agent', []),
      edges: one(`SELECT COUNT(*) AS n FROM aci_message_edge ${where.replaceAll('observed_at', 'occurred_at')}`),
      jobs: one('SELECT COUNT(*) AS n FROM aci_job', []),
      topics: one(`SELECT COUNT(DISTINCT topic) AS n FROM aci_message_event ${where}${where ? ' AND' : ' WHERE'} topic IS NOT NULL`),
      requests: one(`SELECT COUNT(*) AS n FROM aci_message_event ${where}${where ? ' AND' : ' WHERE'} msg_type = 'request'`),
      replies: one(`SELECT COUNT(*) AS n FROM aci_message_event ${where}${where ? ' AND' : ' WHERE'} msg_type = 'reply'`),
      failures: one(`SELECT COUNT(*) AS n FROM aci_message_event ${where}${where ? ' AND' : ' WHERE'} (status IN ('failed','rejected') OR error_class IS NOT NULL)`),
      last_observed_at: last?.t ?? null,
    }
  }

  agents(filters: ACITimeFilter & { limit?: number } = {}): Record<string, unknown> {
    const args: unknown[] = []
    const agentWhere: string[] = []
    if (filters.since) { agentWhere.push('a.last_seen_at >= ?'); args.push(filters.since) }
    if (filters.until) { agentWhere.push('a.first_seen_at <= ?'); args.push(filters.until) }
    const where = agentWhere.length ? `WHERE ${agentWhere.join(' AND ')}` : ''
    const rows = this.db.prepare(`
      SELECT
        a.agent_id, a.kind, a.display_name, a.first_seen_at, a.last_seen_at,
        (SELECT COUNT(*) FROM aci_message_event e WHERE e.from_agent = a.agent_id) AS sent_count,
        (SELECT COUNT(*) FROM aci_message_event e WHERE e.to_agent = a.agent_id) AS received_count
      FROM aci_agent a
      ${where}
      ORDER BY a.last_seen_at DESC, a.agent_id ASC
      LIMIT ?
    `).all(...args, normalizeLimit(filters.limit)) as any[]
    return { ok: true, agents: rows }
  }

  graph(filters: ACITimeFilter & { edgeKind?: string | null; limit?: number } = {}): Record<string, unknown> {
    const q = queryTimeWhere('occurred_at', filters)
    if (filters.edgeKind) { q.where.push('edge_kind = ?'); q.args.push(filters.edgeKind) }
    const where = q.where.length ? `WHERE ${q.where.join(' AND ')}` : ''
    const edges = this.db.prepare(`
      SELECT edge_kind, from_node_type, from_node_id, to_node_type, to_node_id,
             COUNT(*) AS count, MAX(occurred_at) AS last_seen_at
      FROM aci_message_edge
      ${where}
      GROUP BY edge_kind, from_node_type, from_node_id, to_node_type, to_node_id
      ORDER BY count DESC, last_seen_at DESC
      LIMIT ?
    `).all(...q.args, normalizeLimit(filters.limit)) as any[]
    const nodes = new Map<string, any>()
    const touch = (type: string, id: string, dir: 'out' | 'in', count: number) => {
      const key = `${type}:${id}`
      const n = nodes.get(key) ?? { id, type, label: id, in_count: 0, out_count: 0 }
      if (dir === 'out') n.out_count += count
      else n.in_count += count
      nodes.set(key, n)
    }
    for (const e of edges) {
      touch(e.from_node_type, e.from_node_id, 'out', Number(e.count) || 0)
      touch(e.to_node_type, e.to_node_id, 'in', Number(e.count) || 0)
    }
    return { ok: true, nodes: [...nodes.values()], edges }
  }

  timeline(filters: ACITimelineFilters = {}): Record<string, unknown> {
    const q = queryTimeWhere('observed_at', filters)
    if (filters.thread) { q.where.push('thread = ?'); q.args.push(filters.thread) }
    if (filters.threadKey) { q.where.push('thread_key = ?'); q.args.push(filters.threadKey) }
    if (filters.sessionId) { q.where.push('(thread_key = ? OR thread = ? OR corr = ? OR job_id = ?)'); q.args.push(filters.sessionId, filters.sessionId, filters.sessionId, filters.sessionId) }
    if (filters.agent) { q.where.push('(from_agent = ? OR to_agent = ?)'); q.args.push(filters.agent, filters.agent) }
    if (filters.jobId) { q.where.push('job_id = ?'); q.args.push(filters.jobId) }
    if (filters.topic) { q.where.push('topic = ?'); q.args.push(filters.topic) }
    if (filters.corr) { q.where.push('corr = ?'); q.args.push(filters.corr) }
    const where = q.where.length ? `WHERE ${q.where.join(' AND ')}` : ''
    const events = this.db.prepare(`
      SELECT event_id, env_id, source_kind, source_stream, subject, route_kind,
             from_agent, to_agent, topic, msg_type, thread, corr, sent_at, observed_at,
             delivered_at, trust_status, route_status, body_schema, job_id, task_id,
             agent_run_id, event_ref_id, thread_key, stream_topic, result_ref, status, error_class
      FROM aci_message_event
      ${where}
      ORDER BY observed_at DESC, event_id DESC
      LIMIT ?
    `).all(...q.args, normalizeLimit(filters.limit)) as any[]
    return { ok: true, events }
  }

  jobs(filters: ACITimeFilter & { jobId?: string | null; limit?: number } = {}): Record<string, unknown> {
    const where: string[] = []
    const args: unknown[] = []
    if (filters.jobId) { where.push('job_id = ?'); args.push(filters.jobId) }
    if (filters.since) { where.push('(accepted_at >= ? OR completed_at >= ?)'); args.push(filters.since, filters.since) }
    const sqlWhere = where.length ? `WHERE ${where.join(' AND ')}` : ''
    const rows = this.db.prepare(`
      SELECT job_id, job_system, requester, assignee, request_event_id, request_env_id,
             corr, thread, thread_key, stream_topic, taskboard_event_id, task_id,
             agent_run_id, status, accepted_at, completed_at, terminal_event_id, failure_class
      FROM aci_job
      ${sqlWhere}
      ORDER BY COALESCE(completed_at, accepted_at, job_id) DESC
      LIMIT ?
    `).all(...args, normalizeLimit(filters.limit)) as any[]
    return { ok: true, jobs: rows }
  }

  private upsertAgent(agentId: string | null, seenAt: string): void {
    if (!agentId) return
    this.db.prepare(`
      INSERT INTO aci_agent(agent_id, kind, display_name, first_seen_at, last_seen_at, metadata_json)
      VALUES (?, 'agent', ?, ?, ?, '{}')
      ON CONFLICT(agent_id) DO UPDATE SET
        first_seen_at = CASE
          WHEN aci_agent.first_seen_at IS NULL OR excluded.first_seen_at < aci_agent.first_seen_at THEN excluded.first_seen_at
          ELSE aci_agent.first_seen_at
        END,
        last_seen_at = CASE
          WHEN aci_agent.last_seen_at IS NULL OR excluded.last_seen_at > aci_agent.last_seen_at THEN excluded.last_seen_at
          ELSE aci_agent.last_seen_at
        END
    `).run(agentId, agentId, seenAt, seenAt)
  }

  private upsertEdge(eventId: string, edgeKind: string, fromType: string, fromId: string, toType: string, toId: string, opts: {
    thread: string | null
    corr: string | null
    jobId: string | null
    sessionId: string | null
    occurredAt: string
    latencyMs?: number | null
    metadata?: Record<string, unknown>
  }): void {
    this.db.prepare(`
      INSERT INTO aci_message_edge (
        edge_id, event_id, edge_kind, from_node_type, from_node_id, to_node_type,
        to_node_id, thread, corr, job_id, session_id, occurred_at, latency_ms, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(edge_id) DO NOTHING
    `).run(
      edgeIdFor(eventId, edgeKind, fromId, toId),
      eventId,
      edgeKind,
      fromType,
      fromId,
      toType,
      toId,
      opts.thread,
      opts.corr,
      opts.jobId,
      opts.sessionId,
      opts.occurredAt,
      opts.latencyMs ?? null,
      safeJson(opts.metadata ?? {}),
    )
  }

  private upsertSession(input: {
    sessionId: string
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
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
      ON CONFLICT(session_id) DO UPDATE SET
        ended_at = CASE
          WHEN aci_conversation_session.ended_at IS NULL OR excluded.ended_at > aci_conversation_session.ended_at THEN excluded.ended_at
          ELSE aci_conversation_session.ended_at
        END,
        requester = COALESCE(aci_conversation_session.requester, excluded.requester),
        target_agent = COALESCE(aci_conversation_session.target_agent, excluded.target_agent),
        stream_topic = COALESCE(excluded.stream_topic, aci_conversation_session.stream_topic),
        status = COALESCE(excluded.status, aci_conversation_session.status)
    `).run(
      input.sessionId,
      input.sourceKind,
      input.rootThread,
      input.threadKey,
      input.requester,
      input.targetAgent,
      input.streamTopic,
      input.observedAt,
      input.observedAt,
      input.status,
    )
  }

  private upsertJob(input: {
    jobId: string
    schema: string | null
    status: string | null
    fromAgent: string | null
    toAgent: string | null
    eventId: string
    envId: string | null
    corr: string | null
    thread: string | null
    threadKey: string | null
    streamTopic: string | null
    taskboardEventId: string | null
    taskId: number | null
    agentRunId: number | null
    observedAt: string
    errorClass: string | null
  }): void {
    const isRequest = input.schema?.endsWith('.request.v1') ?? false
    const requester = isRequest ? input.fromAgent : input.toAgent
    const assignee = isRequest ? input.toAgent : input.fromAgent
    this.db.prepare(`
      INSERT INTO aci_job (
        job_id, job_system, requester, assignee, request_event_id, request_env_id,
        corr, thread, thread_key, stream_topic, taskboard_event_id, task_id, agent_run_id,
        status, accepted_at, completed_at, terminal_event_id, failure_class, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(job_id) DO UPDATE SET
        job_system = COALESCE(excluded.job_system, aci_job.job_system),
        requester = COALESCE(aci_job.requester, excluded.requester),
        assignee = COALESCE(aci_job.assignee, excluded.assignee),
        request_event_id = COALESCE(aci_job.request_event_id, excluded.request_event_id),
        request_env_id = COALESCE(aci_job.request_env_id, excluded.request_env_id),
        corr = COALESCE(excluded.corr, aci_job.corr),
        thread = COALESCE(excluded.thread, aci_job.thread),
        thread_key = COALESCE(excluded.thread_key, aci_job.thread_key),
        stream_topic = COALESCE(excluded.stream_topic, aci_job.stream_topic),
        taskboard_event_id = COALESCE(excluded.taskboard_event_id, aci_job.taskboard_event_id),
        task_id = COALESCE(excluded.task_id, aci_job.task_id),
        agent_run_id = COALESCE(excluded.agent_run_id, aci_job.agent_run_id),
        status = COALESCE(excluded.status, aci_job.status),
        accepted_at = COALESCE(aci_job.accepted_at, excluded.accepted_at),
        completed_at = COALESCE(aci_job.completed_at, excluded.completed_at),
        terminal_event_id = COALESCE(excluded.terminal_event_id, aci_job.terminal_event_id),
        failure_class = COALESCE(excluded.failure_class, aci_job.failure_class),
        metadata_json = excluded.metadata_json
    `).run(
      input.jobId,
      jobSystemFromSchema(input.schema),
      requester,
      assignee,
      isRequest ? input.eventId : null,
      isRequest ? input.envId : null,
      input.corr,
      input.thread,
      input.threadKey,
      input.streamTopic,
      input.taskboardEventId,
      input.taskId,
      input.agentRunId,
      input.status,
      input.status === 'accepted' ? input.observedAt : null,
      statusIsTerminal(input.status) ? input.observedAt : null,
      statusIsTerminal(input.status) ? input.eventId : null,
      input.errorClass,
      safeJson({ body_schema: input.schema }),
    )
  }
}
