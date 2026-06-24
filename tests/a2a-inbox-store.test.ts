import { describe, expect, test } from 'bun:test'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { A2AChannel } from '../a2a-channel.ts'
import { A2AInboxStore } from '../a2a_inbox_store.ts'

const env = (id: string, to: string, over: Record<string, unknown> = {}) => ({
  id,
  from: 'sender-a',
  to,
  type: 'request',
  ts: '2026-06-23T12:00:00.000Z',
  thread: 'thread-1',
  body: 'hello',
  attrs: { k: 'v' },
  ...over,
})

describe('A2AInboxStore', () => {
  test('stores direct inbox messages idempotently, scoped by recipient, and acks', () => {
    const store = new A2AInboxStore(':memory:')
    expect(store.store({ recipient: 'agent-b', envelope: env('m1', 'agent-b') }).inserted).toBe(true)
    expect(store.store({ recipient: 'agent-b', envelope: env('m1', 'agent-b') }).inserted).toBe(false)
    expect(store.store({ recipient: 'agent-c', envelope: env('m1', 'agent-c') }).inserted).toBe(true)

    expect(store.list({ recipient: 'agent-b' }).messages).toHaveLength(1)
    expect(store.list({ recipient: 'agent-c' }).messages).toHaveLength(1)
    expect(store.read('agent-b', 'm1')?.body).toBe('hello')

    const acked = store.ack('agent-b', 'm1', '2026-06-23T12:01:00.000Z')
    expect(acked?.handled).toBe(1)
    expect(acked?.handled_at).toBe('2026-06-23T12:01:00.000Z')
    expect(store.list({ recipient: 'agent-b', handled: false }).messages).toHaveLength(0)
    expect(store.list({ recipient: 'agent-b', handled: true }).messages).toHaveLength(1)
    store.close()
  })

  test('persists messages across store restart', () => {
    const dir = mkdtempSync(join(tmpdir(), 'a2a-inbox-store-'))
    const dbPath = join(dir, 'inbox.sqlite3')
    try {
      const first = new A2AInboxStore(dbPath)
      first.store({ recipient: 'agent-b', envelope: env('m2', 'agent-b', { body: 'durable' }) })
      first.close()

      const second = new A2AInboxStore(dbPath)
      expect(second.read('agent-b', 'm2')?.body).toBe('durable')
      second.close()
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test('disabled channel does not create a default inbox database', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'a2a-inbox-lazy-'))
    const dbPath = join(dir, 'a2a-inbox.sqlite3')
    try {
      const ch = new A2AChannel(async () => {}, { enabled: false, inboxDbPath: dbPath })
      expect(existsSync(dbPath)).toBe(false)
      await ch.start()
      expect(existsSync(dbPath)).toBe(false)
      await ch.stop()
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
