import { Database } from 'bun:sqlite'
import { mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import {
  createPortalEvent,
  isPortalEventEnvelope,
  portalEventMatchesScope,
  type PortalEventEnvelope,
  type PortalEventFilters,
  type PortalEventInput,
} from './portal_events.ts'

export type PortalEventAppendResult = {
  event: PortalEventEnvelope
  inserted: boolean
}

export type PortalEventListResult = {
  events: PortalEventEnvelope[]
  lastSeq: number
  compacted: boolean
}

export type PortalEventSnapshot = {
  last_seq: number
  compacted: boolean
  events: PortalEventEnvelope[]
}

const MAX_LIMIT = 1000

export function initializePortalEventDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

export function migratePortalEventDb(db: Database): void {
  initializePortalEventDb(db)
  db.exec(`
    CREATE TABLE IF NOT EXISTS portal_event (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      schema TEXT NOT NULL,
      ts TEXT NOT NULL,
      source TEXT NOT NULL,
      event_type TEXT NOT NULL,
      resource_kind TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      actor_operator_id TEXT,
      actor_agent_id TEXT,
      tenant TEXT NOT NULL,
      channels_json TEXT NOT NULL,
      recipients_json TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      redaction_json TEXT NOT NULL,
      dedup_key TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS portal_event_dedup_idx
      ON portal_event(dedup_key)
      WHERE dedup_key IS NOT NULL;
    CREATE INDEX IF NOT EXISTS portal_event_ts_idx
      ON portal_event(ts);
    CREATE INDEX IF NOT EXISTS portal_event_type_seq_idx
      ON portal_event(event_type, seq);
    CREATE INDEX IF NOT EXISTS portal_event_resource_seq_idx
      ON portal_event(resource_kind, resource_id, seq);
    CREATE INDEX IF NOT EXISTS portal_event_tenant_seq_idx
      ON portal_event(tenant, seq);
  `)
}

export class PortalEventStore {
  readonly db: Database
  private readonly ownsDb: boolean

  constructor(pathOrDb: string | Database = ':memory:') {
    if (typeof pathOrDb === 'string') {
      if (pathOrDb !== ':memory:') mkdirSync(dirname(pathOrDb), { recursive: true })
      this.db = new Database(pathOrDb, { create: true })
      this.ownsDb = true
    } else {
      this.db = pathOrDb
      this.ownsDb = false
    }
    migratePortalEventDb(this.db)
  }

  close(): void {
    if (this.ownsDb) this.db.close()
  }

  append(input: PortalEventInput): PortalEventAppendResult {
    const event = createPortalEvent(input)
    const dedupKey = input.dedup_key || null
    const result = this.db.prepare(`
      INSERT OR IGNORE INTO portal_event (
        schema, ts, source, event_type, resource_kind, resource_id,
        actor_operator_id, actor_agent_id, tenant, channels_json, recipients_json,
        payload_json, redaction_json, dedup_key
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      event.schema,
      event.ts,
      event.source,
      event.event_type,
      event.resource.kind,
      event.resource.id,
      event.actor.operator_id,
      event.actor.agent_id,
      event.scope.tenant,
      JSON.stringify(event.scope.channels),
      JSON.stringify(event.scope.recipients),
      JSON.stringify(event.payload),
      JSON.stringify(event.redaction),
      dedupKey,
    )
    const row = dedupKey ? this.readByDedupKey(dedupKey) : this.read(this.latestSeq())
    if (!row) throw new Error('portal_event_store_append_missing')
    return { event: row, inserted: result.changes > 0 }
  }

  read(seq: number): PortalEventEnvelope | null {
    const row = this.db.prepare('SELECT * FROM portal_event WHERE seq = ?').get(Math.trunc(seq))
    return row ? rowToEvent(row as PortalEventRow) : null
  }

  readByDedupKey(dedupKey: string): PortalEventEnvelope | null {
    const row = this.db.prepare('SELECT * FROM portal_event WHERE dedup_key = ?').get(dedupKey)
    return row ? rowToEvent(row as PortalEventRow) : null
  }

  getAfter(afterSeq = 0, filters: PortalEventFilters = {}): PortalEventListResult {
    const limit = normalizeLimit(filters.limit)
    const where = ['seq > ?']
    const args: unknown[] = [Math.max(0, Math.trunc(afterSeq))]
    if (filters.tenant) {
      where.push('tenant = ?')
      args.push(String(filters.tenant).trim().toLowerCase())
    }
    if (filters.eventTypes?.length) {
      where.push(`event_type IN (${filters.eventTypes.map(() => '?').join(', ')})`)
      args.push(...filters.eventTypes)
    }
    const rows = this.db.prepare(`
      SELECT * FROM portal_event
      WHERE ${where.join(' AND ')}
      ORDER BY seq ASC
      LIMIT ?
    `).all(...args, Math.min(MAX_LIMIT, limit * 5)) as PortalEventRow[]
    const events = rows
      .map(rowToEvent)
      .filter((event) => portalEventMatchesScope(event, filters))
      .slice(0, limit)
    return { events, lastSeq: this.latestSeq(), compacted: this.hasCompactionGap(afterSeq) }
  }

  snapshot(filters: PortalEventFilters = {}): PortalEventSnapshot {
    const afterSeq = Math.max(0, this.latestSeq() - normalizeLimit(filters.limit))
    const result = this.getAfter(afterSeq, filters)
    return { last_seq: result.lastSeq, compacted: result.compacted, events: result.events }
  }

  compactBefore(seq: number): { deleted: number } {
    const result = this.db.prepare('DELETE FROM portal_event WHERE seq < ?').run(Math.max(0, Math.trunc(seq)))
    return { deleted: result.changes }
  }

  latestSeq(): number {
    const row = this.db.prepare('SELECT COALESCE(MAX(seq), 0) AS seq FROM portal_event').get() as { seq: number }
    return Number(row?.seq || 0)
  }

  oldestSeq(): number {
    const row = this.db.prepare('SELECT COALESCE(MIN(seq), 0) AS seq FROM portal_event').get() as { seq: number }
    return Number(row?.seq || 0)
  }

  hasCompactionGap(afterSeq: number): boolean {
    const oldest = this.oldestSeq()
    return oldest > 0 && afterSeq > 0 && Math.trunc(afterSeq) < oldest - 1
  }
}

type PortalEventRow = {
  seq: number
  schema: string
  ts: string
  source: PortalEventEnvelope['source']
  event_type: PortalEventEnvelope['event_type']
  resource_kind: PortalEventEnvelope['resource']['kind']
  resource_id: string
  actor_operator_id: string | null
  actor_agent_id: string | null
  tenant: string
  channels_json: string
  recipients_json: string
  payload_json: string
  redaction_json: string
}

function rowToEvent(row: PortalEventRow): PortalEventEnvelope {
  const event = {
    schema: row.schema,
    seq: row.seq,
    ts: row.ts,
    source: row.source,
    event_type: row.event_type,
    resource: { kind: row.resource_kind, id: row.resource_id },
    actor: { operator_id: row.actor_operator_id, agent_id: row.actor_agent_id },
    scope: {
      tenant: row.tenant,
      channels: parseJsonArray(row.channels_json),
      recipients: parseJsonArray(row.recipients_json),
    },
    payload: parseJson(row.payload_json),
    redaction: parseJson(row.redaction_json),
  }
  if (!isPortalEventEnvelope(event)) throw new Error('portal_event_store_bad_row')
  return event
}

function parseJson(value: string): unknown {
  try { return JSON.parse(value) } catch { return null }
}

function parseJsonArray(value: string): string[] {
  const parsed = parseJson(value)
  return Array.isArray(parsed) ? parsed.filter((v): v is string => typeof v === 'string') : []
}

function normalizeLimit(limit: number | undefined): number {
  return Math.min(Math.max(Math.trunc(limit ?? 200), 1), MAX_LIMIT)
}
