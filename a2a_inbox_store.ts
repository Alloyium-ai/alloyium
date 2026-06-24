import { Database } from 'bun:sqlite'

export type InboxEnvelopeLike = {
  id: string
  from: string
  to: string
  type: string
  thread?: string
  corr?: string
  ts: string
  body: string
  attrs?: Record<string, string>
}

export type InboxMessageRow = {
  env_id: string
  recipient: string
  from_agent: string
  to_agent: string
  msg_type: string
  thread: string | null
  corr: string | null
  ts: string
  delivered_at: string
  subject: string | null
  body: string
  attrs_json: string | null
  raw_envelope: string
  handled: number
  handled_at: string | null
}

export type InboxListFilters = {
  recipient: string
  handled?: boolean
  from?: string | null
  thread?: string | null
  since?: string | null
  limit?: number
  cursor?: string | null
}

export type InboxListResult = {
  messages: InboxMessageRow[]
  nextCursor: string | null
}

export type StoreInboxMessageInput = {
  recipient: string
  envelope: InboxEnvelopeLike
  subject?: string | null
  rawEnvelope?: string
  deliveredAt?: string
}

const MAX_LIMIT = 200

function nowIso(): string { return new Date().toISOString() }

function encodeCursor(row: InboxMessageRow): string {
  return Buffer.from(JSON.stringify({ delivered_at: row.delivered_at, env_id: row.env_id })).toString('base64url')
}

function decodeCursor(cursor: string | null | undefined): { delivered_at: string; env_id: string } | null {
  if (!cursor) return null
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'))
    if (typeof parsed?.delivered_at === 'string' && typeof parsed?.env_id === 'string') return parsed
  } catch {}
  return null
}

function normalizeLimit(limit: number | undefined): number {
  return Math.min(Math.max(Math.trunc(limit ?? 50), 1), MAX_LIMIT)
}

export function initializeA2AInboxDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

export function migrateA2AInboxDb(db: Database): void {
  initializeA2AInboxDb(db)
  db.exec(`
    CREATE TABLE IF NOT EXISTS a2a_inbox_message (
      env_id TEXT NOT NULL,
      recipient TEXT NOT NULL,
      from_agent TEXT NOT NULL,
      to_agent TEXT NOT NULL,
      msg_type TEXT NOT NULL,
      thread TEXT,
      corr TEXT,
      ts TEXT NOT NULL,
      delivered_at TEXT NOT NULL,
      subject TEXT,
      body TEXT NOT NULL,
      attrs_json TEXT,
      raw_envelope TEXT NOT NULL,
      handled INTEGER NOT NULL DEFAULT 0 CHECK (handled IN (0,1)),
      handled_at TEXT,
      PRIMARY KEY (recipient, env_id)
    );
    CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_delivery_idx
      ON a2a_inbox_message(recipient, delivered_at DESC, env_id DESC);
    CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_handled_idx
      ON a2a_inbox_message(recipient, handled, delivered_at DESC, env_id DESC);
    CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_from_idx
      ON a2a_inbox_message(recipient, from_agent, delivered_at DESC, env_id DESC);
    CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_thread_idx
      ON a2a_inbox_message(recipient, thread, delivered_at DESC, env_id DESC);
  `)
}

export class A2AInboxStore {
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
    migrateA2AInboxDb(this.db)
  }

  close(): void {
    if (this.ownsDb) this.db.close()
  }

  store(input: StoreInboxMessageInput): { inserted: boolean; message: InboxMessageRow } {
    const env = input.envelope
    const deliveredAt = input.deliveredAt ?? nowIso()
    const attrsJson = env.attrs == null ? null : JSON.stringify(env.attrs)
    const rawEnvelope = input.rawEnvelope ?? JSON.stringify(env)
    const inserted = this.db.prepare(`
      INSERT INTO a2a_inbox_message (
        env_id, recipient, from_agent, to_agent, msg_type, thread, corr, ts,
        delivered_at, subject, body, attrs_json, raw_envelope
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(recipient, env_id) DO NOTHING
    `).run(
      env.id,
      input.recipient,
      env.from,
      env.to,
      env.type,
      env.thread ?? null,
      env.corr ?? null,
      env.ts,
      deliveredAt,
      input.subject ?? null,
      env.body,
      attrsJson,
      rawEnvelope,
    ).changes === 1
    const message = this.read(input.recipient, env.id)
    if (!message) throw new Error('inbox_store_insert_missing')
    return { inserted, message }
  }

  list(filters: InboxListFilters): InboxListResult {
    const limit = normalizeLimit(filters.limit)
    const where = ['recipient = ?']
    const args: unknown[] = [filters.recipient]
    if (filters.handled !== undefined) {
      where.push('handled = ?')
      args.push(filters.handled ? 1 : 0)
    }
    if (filters.from) {
      where.push('from_agent = ?')
      args.push(filters.from)
    }
    if (filters.thread) {
      where.push('thread = ?')
      args.push(filters.thread)
    }
    if (filters.since) {
      where.push('ts >= ?')
      args.push(filters.since)
    }
    const cursor = decodeCursor(filters.cursor)
    if (cursor) {
      where.push('(delivered_at < ? OR (delivered_at = ? AND env_id < ?))')
      args.push(cursor.delivered_at, cursor.delivered_at, cursor.env_id)
    }
    const rows = this.db.prepare(`
      SELECT * FROM a2a_inbox_message
      WHERE ${where.join(' AND ')}
      ORDER BY delivered_at DESC, env_id DESC
      LIMIT ?
    `).all(...args, limit + 1) as InboxMessageRow[]
    const page = rows.slice(0, limit)
    return {
      messages: page,
      nextCursor: rows.length > limit && page.length > 0 ? encodeCursor(page[page.length - 1]) : null,
    }
  }

  read(recipient: string, envId: string): InboxMessageRow | null {
    return this.db.prepare(`
      SELECT * FROM a2a_inbox_message
      WHERE recipient = ? AND env_id = ?
    `).get(recipient, envId) as InboxMessageRow | null
  }

  ack(recipient: string, envId: string, handledAt = nowIso()): InboxMessageRow | null {
    this.db.prepare(`
      UPDATE a2a_inbox_message
      SET handled = 1,
          handled_at = COALESCE(handled_at, ?)
      WHERE recipient = ? AND env_id = ?
    `).run(handledAt, recipient, envId)
    return this.read(recipient, envId)
  }
}
