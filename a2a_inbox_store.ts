import { Database } from 'bun:sqlite'

export type InboxEnvelopeLike = {
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
  ttl_ms: number | null
  delivered_at: string
  subject: string | null
  body: string
  body_schema: string | null
  body_job_id: string | null
  attrs_json: string | null
  raw_envelope: string
  handled: number
  handled_at: string | null
  injected: number
  injected_at: string | null
  injected_assumed: number
  inject_attempts: number
  last_inject_error: string | null
  inject_dead: number
  inject_dead_at: string | null
}

export type InboxListFilters = {
  recipient: string
  handled?: boolean
  from?: string | null
  type?: string | null
  corr?: string | null
  thread?: string | null
  since?: string | null
  bodyContains?: string | null
  jobId?: string | null
  schema?: string | null
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
const MAX_BODY_METADATA_LENGTH = 128
// Bump when a new one-shot data migration is added. Data migrations are gated on
// PRAGMA user_version so a reopen of an already-migrated DB re-touches zero rows
// (no WAL churn on startup).
const METADATA_BACKFILL_USER_VERSION = 1
const INJECTED_STATE_USER_VERSION = 2
const INJECTED_ASSUMED_USER_VERSION = 3
const INJECT_DEADLETTER_USER_VERSION = 4

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

function escapeLike(value: string): string {
  return value.replace(/[\\%_]/g, (m) => `\\${m}`)
}

function cleanBodyMetadataValue(value: unknown): string | null {
  if (typeof value !== 'string') return null
  if (value.length < 1 || value.length > MAX_BODY_METADATA_LENGTH) return null
  return value
}

function extractInboxBodyMetadata(body: string): { schema: string | null; jobId: string | null } {
  try {
    const parsed = JSON.parse(body)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { schema: null, jobId: null }
    const obj = parsed as Record<string, unknown>
    return {
      schema: cleanBodyMetadataValue(obj.schema),
      jobId: cleanBodyMetadataValue(obj.job_id),
    }
  } catch {
    return { schema: null, jobId: null }
  }
}

function ensureColumn(db: Database, table: string, column: string, definition: string): void {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>
  if (rows.some((row) => row.name === column)) return
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`)
}

function jsonTextMetadataExpr(path: '$.schema' | '$.job_id'): string {
  return `
    CASE WHEN json_valid(body) THEN
      CASE
        WHEN json_type(body, '${path}') = 'text'
          AND length(json_extract(body, '${path}')) BETWEEN 1 AND ${MAX_BODY_METADATA_LENGTH}
        THEN json_extract(body, '${path}')
      END
    END
  `
}

export function initializeA2AInboxDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

function inboxUserVersion(db: Database): number {
  const row = db.prepare('PRAGMA user_version').get() as { user_version?: number } | undefined
  return row?.user_version ?? 0
}

/**
 * Idempotent migration. Schema/index DDL is IF-NOT-EXISTS and cheap to re-run on
 * every open. The one-time body_schema/body_job_id backfill is gated on
 * PRAGMA user_version so it runs exactly once per database: on an already-migrated
 * DB the backfill UPDATEs are skipped entirely, so a reopen re-updates zero rows
 * (rows whose metadata legitimately stays NULL are never re-touched).
 *
 * @returns metadataBackfill — number of rows the one-shot backfill updated this
 *   call (0 on any reopen of an already-migrated DB).
 */
export function migrateA2AInboxDb(db: Database): { metadataBackfill: number } {
  initializeA2AInboxDb(db)
  let metadataBackfill = 0
  let inTransaction = false
  db.exec('BEGIN IMMEDIATE')
  inTransaction = true
  try {
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
        ttl_ms INTEGER,
        delivered_at TEXT NOT NULL,
        subject TEXT,
        body TEXT NOT NULL,
        body_schema TEXT,
        body_job_id TEXT,
        attrs_json TEXT,
        raw_envelope TEXT NOT NULL,
        handled INTEGER NOT NULL DEFAULT 0 CHECK (handled IN (0,1)),
        handled_at TEXT,
        injected INTEGER NOT NULL DEFAULT 0 CHECK (injected IN (0,1)),
        injected_at TEXT,
        injected_assumed INTEGER NOT NULL DEFAULT 0 CHECK (injected_assumed IN (0,1)),
        inject_attempts INTEGER NOT NULL DEFAULT 0 CHECK (inject_attempts >= 0),
        last_inject_error TEXT,
        inject_dead INTEGER NOT NULL DEFAULT 0 CHECK (inject_dead IN (0,1)),
        inject_dead_at TEXT,
        PRIMARY KEY (recipient, env_id)
      );
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_delivery_idx
        ON a2a_inbox_message(recipient, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_handled_idx
        ON a2a_inbox_message(recipient, handled, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_from_idx
        ON a2a_inbox_message(recipient, from_agent, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_type_idx
        ON a2a_inbox_message(recipient, msg_type, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_corr_idx
        ON a2a_inbox_message(recipient, corr, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_thread_idx
        ON a2a_inbox_message(recipient, thread, delivered_at DESC, env_id DESC);
    `)
    ensureColumn(db, 'a2a_inbox_message', 'body_schema', 'TEXT')
    ensureColumn(db, 'a2a_inbox_message', 'body_job_id', 'TEXT')
    ensureColumn(db, 'a2a_inbox_message', 'ttl_ms', 'INTEGER')
    ensureColumn(db, 'a2a_inbox_message', 'injected', 'INTEGER NOT NULL DEFAULT 0 CHECK (injected IN (0,1))')
    ensureColumn(db, 'a2a_inbox_message', 'injected_at', 'TEXT')
    ensureColumn(db, 'a2a_inbox_message', 'injected_assumed', 'INTEGER NOT NULL DEFAULT 0 CHECK (injected_assumed IN (0,1))')
    ensureColumn(db, 'a2a_inbox_message', 'inject_attempts', 'INTEGER NOT NULL DEFAULT 0 CHECK (inject_attempts >= 0)')
    ensureColumn(db, 'a2a_inbox_message', 'last_inject_error', 'TEXT')
    ensureColumn(db, 'a2a_inbox_message', 'inject_dead', 'INTEGER NOT NULL DEFAULT 0 CHECK (inject_dead IN (0,1))')
    ensureColumn(db, 'a2a_inbox_message', 'inject_dead_at', 'TEXT')
    db.exec(`
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_body_job_id_idx
        ON a2a_inbox_message(recipient, body_job_id, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_body_schema_idx
        ON a2a_inbox_message(recipient, body_schema, delivered_at DESC, env_id DESC);
      CREATE INDEX IF NOT EXISTS a2a_inbox_recipient_injected_idx
        ON a2a_inbox_message(recipient, injected, delivered_at ASC, env_id ASC);
    `)

    // One-shot backfills and their version bump are serialized with writers.
    // A concurrent startup cannot insert a fresh pending row between reading
    // user_version and the legacy "assume injected" update.
    const userVersion = inboxUserVersion(db)
    if (userVersion < METADATA_BACKFILL_USER_VERSION) {
      const schemaBackfill = db.prepare(`
        UPDATE a2a_inbox_message
        SET body_schema = ${jsonTextMetadataExpr('$.schema')}
        WHERE body_schema IS NULL
      `).run()
      const jobIdBackfill = db.prepare(`
        UPDATE a2a_inbox_message
        SET body_job_id = ${jsonTextMetadataExpr('$.job_id')}
        WHERE body_job_id IS NULL
      `).run()
      metadataBackfill = schemaBackfill.changes + jobIdBackfill.changes
    }
    if (userVersion < INJECTED_STATE_USER_VERSION) {
      db.prepare(`
        UPDATE a2a_inbox_message
        SET injected = 1,
            injected_at = COALESCE(injected_at, delivered_at),
            injected_assumed = 1
        WHERE injected = 0 OR injected_at IS NULL
      `).run()
    }
    if (userVersion < INJECTED_ASSUMED_USER_VERSION) {
      db.prepare(`
        UPDATE a2a_inbox_message
        SET ttl_ms = json_extract(raw_envelope, '$.ttl_ms')
        WHERE ttl_ms IS NULL
          AND json_valid(raw_envelope)
          AND json_type(raw_envelope, '$.ttl_ms') = 'integer'
      `).run()
      db.prepare(`
        UPDATE a2a_inbox_message
        SET injected_assumed = 1
        WHERE injected = 1
          AND injected_at = delivered_at
      `).run()
    }
    if (userVersion < INJECT_DEADLETTER_USER_VERSION) {
      db.exec(`PRAGMA user_version = ${INJECT_DEADLETTER_USER_VERSION}`)
    }
    db.exec('COMMIT')
    inTransaction = false
  } catch (err) {
    if (inTransaction) {
      try { db.exec('ROLLBACK') } catch {}
    }
    throw err
  }
  return { metadataBackfill }
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
    const bodyMetadata = extractInboxBodyMetadata(env.body)
    const attrsJson = env.attrs == null ? null : JSON.stringify(env.attrs)
    const rawEnvelope = input.rawEnvelope ?? JSON.stringify(env)
    const inserted = this.db.prepare(`
      INSERT INTO a2a_inbox_message (
        env_id, recipient, from_agent, to_agent, msg_type, thread, corr, ts,
        ttl_ms, delivered_at, subject, body, body_schema, body_job_id, attrs_json, raw_envelope
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
      env.ttl_ms ?? null,
      deliveredAt,
      input.subject ?? null,
      env.body,
      bodyMetadata.schema,
      bodyMetadata.jobId,
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
    if (filters.type) {
      where.push('msg_type = ?')
      args.push(filters.type)
    }
    if (filters.corr) {
      where.push('corr = ?')
      args.push(filters.corr)
    }
    if (filters.thread) {
      where.push('thread = ?')
      args.push(filters.thread)
    }
    if (filters.since) {
      where.push('delivered_at >= ?')
      args.push(filters.since)
    }
    if (filters.bodyContains) {
      where.push("body LIKE ? ESCAPE '\\'")
      args.push(`%${escapeLike(filters.bodyContains)}%`)
    }
    if (filters.jobId) {
      where.push('body_job_id = ?')
      args.push(filters.jobId)
    }
    if (filters.schema) {
      where.push('body_schema = ?')
      args.push(filters.schema)
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

  markInjected(recipient: string, envId: string, injectedAt = nowIso()): InboxMessageRow | null {
    this.db.prepare(`
      UPDATE a2a_inbox_message
      SET injected = 1,
          injected_at = COALESCE(injected_at, ?),
          injected_assumed = 0,
          inject_attempts = 0,
          last_inject_error = NULL,
          inject_dead = 0,
          inject_dead_at = NULL
      WHERE recipient = ? AND env_id = ?
    `).run(injectedAt, recipient, envId)
    return this.read(recipient, envId)
  }

  recordInjectFailure(recipient: string, envId: string, error: string): InboxMessageRow | null {
    this.db.prepare(`
      UPDATE a2a_inbox_message
      SET inject_attempts = inject_attempts + 1,
          last_inject_error = ?,
          injected_assumed = 0
      WHERE recipient = ?
        AND env_id = ?
        AND injected = 0
    `).run(error, recipient, envId)
    return this.read(recipient, envId)
  }

  deadLetterInject(recipient: string, envId: string, deadAt = nowIso(), error?: string): InboxMessageRow | null {
    this.db.prepare(`
      UPDATE a2a_inbox_message
      SET injected = 1,
          injected_at = COALESCE(injected_at, ?),
          injected_assumed = 0,
          inject_dead = 1,
          inject_dead_at = COALESCE(inject_dead_at, ?),
          last_inject_error = COALESCE(?, last_inject_error)
      WHERE recipient = ? AND env_id = ?
    `).run(deadAt, deadAt, error ?? null, recipient, envId)
    return this.read(recipient, envId)
  }

  reopenAssumedInjected(recipient: string, envelope: InboxEnvelopeLike, rawEnvelope: string): InboxMessageRow | null {
    const attrsJson = envelope.attrs == null ? null : JSON.stringify(envelope.attrs)
    this.db.prepare(`
      UPDATE a2a_inbox_message
      SET injected = 0,
          injected_at = NULL,
          injected_assumed = 0,
          inject_attempts = 0,
          last_inject_error = NULL,
          inject_dead = 0,
          inject_dead_at = NULL
      WHERE recipient = ?
        AND env_id = ?
        AND injected = 1
        AND injected_assumed = 1
        AND (
          raw_envelope = ?
          OR (
            from_agent = ?
            AND to_agent = ?
            AND msg_type = ?
            AND thread IS ?
            AND corr IS ?
            AND ts = ?
            AND ttl_ms IS ?
            AND body = ?
            AND attrs_json IS ?
          )
        )
    `).run(
      recipient,
      envelope.id,
      rawEnvelope,
      envelope.from,
      envelope.to,
      envelope.type,
      envelope.thread ?? null,
      envelope.corr ?? null,
      envelope.ts,
      envelope.ttl_ms ?? null,
      envelope.body,
      attrsJson,
    )
    return this.read(recipient, envelope.id)
  }

  pendingInjection(recipient: string, limit = 50): InboxMessageRow[] {
    const n = normalizeLimit(limit)
    return this.db.prepare(`
      SELECT * FROM a2a_inbox_message
      WHERE recipient = ? AND injected = 0 AND inject_dead = 0
      ORDER BY delivered_at ASC, rowid ASC
      LIMIT ?
    `).all(recipient, n) as InboxMessageRow[]
  }
}
