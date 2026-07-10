import { Database } from 'bun:sqlite'
import { mkdirSync } from 'node:fs'
import { dirname } from 'node:path'

export type PortalSendOutboxStatus = 'queued' | 'publishing' | 'published' | 'failed'

export type PortalSendOutboxPayload = {
  schema: 'portal.send.outbox.v1'
  send_id: string
  args: Record<string, unknown>
  send_mode: string
  chat_context: string | null
  thread_key: string | null
  cwd: string | null
  routed_to: string
  reply_tracking: boolean
  queued_at: string
}

export type PortalSendOutboxRow = {
  id: string
  status: PortalSendOutboxStatus
  attempts: number
  next_attempt_at: string
  locked_at: string | null
  created_at: string
  updated_at: string
  payload: PortalSendOutboxPayload
  bus_id: string | null
  response: unknown
  last_error: string | null
}

type PortalSendOutboxDbRow = {
  id: string
  status: PortalSendOutboxStatus
  attempts: number
  next_attempt_at: string
  locked_at: string | null
  created_at: string
  updated_at: string
  payload_json: string
  bus_id: string | null
  response_json: string | null
  last_error: string | null
}

export function initializePortalSendOutboxDb(db: Database): void {
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA synchronous = NORMAL')
  db.exec('PRAGMA wal_autocheckpoint = 1000')
}

export function migratePortalSendOutboxDb(db: Database): void {
  initializePortalSendOutboxDb(db)
  db.exec(`
    CREATE TABLE IF NOT EXISTS portal_send_outbox (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at TEXT NOT NULL,
      locked_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      bus_id TEXT,
      response_json TEXT,
      last_error TEXT
    );
    CREATE INDEX IF NOT EXISTS portal_send_outbox_due_idx
      ON portal_send_outbox(status, next_attempt_at, created_at);
    CREATE INDEX IF NOT EXISTS portal_send_outbox_locked_idx
      ON portal_send_outbox(status, locked_at);
    CREATE INDEX IF NOT EXISTS portal_send_outbox_created_idx
      ON portal_send_outbox(created_at);
  `)
}

export class PortalSendOutboxStore {
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
    migratePortalSendOutboxDb(this.db)
  }

  close(): void {
    if (this.ownsDb) this.db.close()
  }

  enqueue(payload: PortalSendOutboxPayload): PortalSendOutboxRow {
    const now = payload.queued_at || new Date().toISOString()
    this.db.prepare(`
      INSERT INTO portal_send_outbox (
        id, status, attempts, next_attempt_at, locked_at, created_at, updated_at,
        payload_json, bus_id, response_json, last_error
      )
      VALUES (?, 'queued', 0, ?, NULL, ?, ?, ?, NULL, NULL, NULL)
    `).run(payload.send_id, now, now, now, JSON.stringify(payload))
    const row = this.read(payload.send_id)
    if (!row) throw new Error('portal_send_outbox_enqueue_missing')
    return row
  }

  read(id: string): PortalSendOutboxRow | null {
    const row = this.db.prepare('SELECT * FROM portal_send_outbox WHERE id = ?').get(id) as PortalSendOutboxDbRow | null
    return row ? rowToOutboxRow(row) : null
  }

  claimDue(opts: { now?: string; limit?: number; staleMs?: number } = {}): PortalSendOutboxRow[] {
    const now = opts.now ?? new Date().toISOString()
    const limit = Math.min(Math.max(Math.trunc(opts.limit ?? 5), 1), 100)
    const staleMs = Math.max(1, Math.trunc(opts.staleMs ?? 120_000))
    const staleBefore = new Date(Date.parse(now) - staleMs).toISOString()
    const candidates = this.db.prepare(`
      SELECT id FROM portal_send_outbox
      WHERE
        (status = 'queued' AND next_attempt_at <= ?)
        OR (status = 'publishing' AND locked_at IS NOT NULL AND locked_at <= ?)
      ORDER BY next_attempt_at ASC, created_at ASC
      LIMIT ?
    `).all(now, staleBefore, limit) as Array<{ id: string }>

    const rows: PortalSendOutboxRow[] = []
    for (const candidate of candidates) {
      const result = this.db.prepare(`
        UPDATE portal_send_outbox
        SET status = 'publishing',
            attempts = attempts + 1,
            locked_at = ?,
            updated_at = ?
        WHERE id = ?
          AND (
            (status = 'queued' AND next_attempt_at <= ?)
            OR (status = 'publishing' AND locked_at IS NOT NULL AND locked_at <= ?)
          )
      `).run(now, now, candidate.id, now, staleBefore)
      if (result.changes > 0) {
        const row = this.read(candidate.id)
        if (row) rows.push(row)
      }
    }
    return rows
  }

  markPublished(id: string, busId: string, response: unknown, now = new Date().toISOString()): PortalSendOutboxRow {
    this.db.prepare(`
      UPDATE portal_send_outbox
      SET status = 'published',
          bus_id = ?,
          response_json = ?,
          locked_at = NULL,
          last_error = NULL,
          updated_at = ?
      WHERE id = ?
    `).run(busId, JSON.stringify(response ?? null), now, id)
    return this.mustRead(id)
  }

  markRetry(id: string, error: string, nextAttemptAt: string, now = new Date().toISOString()): PortalSendOutboxRow {
    this.db.prepare(`
      UPDATE portal_send_outbox
      SET status = 'queued',
          locked_at = NULL,
          last_error = ?,
          next_attempt_at = ?,
          updated_at = ?
      WHERE id = ?
    `).run(error, nextAttemptAt, now, id)
    return this.mustRead(id)
  }

  markFailed(id: string, error: string, now = new Date().toISOString()): PortalSendOutboxRow {
    this.db.prepare(`
      UPDATE portal_send_outbox
      SET status = 'failed',
          locked_at = NULL,
          last_error = ?,
          updated_at = ?
      WHERE id = ?
    `).run(error, now, id)
    return this.mustRead(id)
  }

  counts(): Record<PortalSendOutboxStatus, number> {
    const out: Record<PortalSendOutboxStatus, number> = { queued: 0, publishing: 0, published: 0, failed: 0 }
    const rows = this.db.prepare('SELECT status, COUNT(*) AS n FROM portal_send_outbox GROUP BY status').all() as Array<{ status: PortalSendOutboxStatus; n: number }>
    for (const row of rows) {
      if (row.status in out) out[row.status] = Number(row.n) || 0
    }
    return out
  }

  private mustRead(id: string): PortalSendOutboxRow {
    const row = this.read(id)
    if (!row) throw new Error('portal_send_outbox_missing')
    return row
  }
}

function rowToOutboxRow(row: PortalSendOutboxDbRow): PortalSendOutboxRow {
  return {
    id: row.id,
    status: row.status,
    attempts: Number(row.attempts) || 0,
    next_attempt_at: row.next_attempt_at,
    locked_at: row.locked_at,
    created_at: row.created_at,
    updated_at: row.updated_at,
    payload: parsePayload(row.payload_json),
    bus_id: row.bus_id,
    response: parseJson(row.response_json),
    last_error: row.last_error,
  }
}

function parsePayload(value: string): PortalSendOutboxPayload {
  const parsed = parseJson(value)
  if (!parsed || typeof parsed !== 'object' || (parsed as any).schema !== 'portal.send.outbox.v1') {
    throw new Error('portal_send_outbox_bad_payload')
  }
  return parsed as PortalSendOutboxPayload
}

function parseJson(value: string | null): unknown {
  if (!value) return null
  try { return JSON.parse(value) } catch { return null }
}
