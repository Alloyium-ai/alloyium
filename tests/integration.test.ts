// Integration tests — live NATS + Redis, fully isolated. Every case uses a
// throwaway Redis key, the self-owned CLAUDE_SELFTEST stream, and subjects under
// alloyium.channels.selftest.* — NEVER a production subject. Benign defaults are
// injected so no fallback path can ever bind production RAMP_ALERTS.
import { test, expect, describe, beforeAll, afterAll, afterEach } from 'bun:test'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { NatsChannel, type SubSpec } from '../nats-channel.ts'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const STREAM = 'CLAUDE_SELFTEST'
const JS_WILDCARD = 'alloyium.channels.selftest.js.>'
const BENIGN_DEFAULTS: SubSpec[] = [{ subject: 'alloyium.channels.selftest.never', mode: 'core' }]
const enc = (o: unknown) => new TextEncoder().encode(JSON.stringify(o))

// Probe infra at import so we can skip cleanly where it is unavailable.
let available = true
let tnc: NatsConnection
let redis: RedisClient
try {
  tnc = await connect({ servers: NATS_URL, name: 'it-probe' })
  redis = new RedisClient(REDIS_URL)
  await redis.set('alloyium:test:probe', '1')
  await redis.del('alloyium:test:probe')
} catch {
  available = false
}

type Rec = { subject: string; mode: string; content: string }
const channels: NatsChannel[] = []
const createdKeys: string[] = []
let uid = 0

async function startChannel(specs: SubSpec[], received: Rec[], extra: Partial<{ redisUrl: string }> = {}) {
  uid++
  const key = `alloyium:test:${uid}`
  const control = `alloyium.channels.selftest.ctrl.${uid}`
  createdKeys.push(key)
  await redis.set(key, JSON.stringify(specs))
  const ch = new NatsChannel(
    async (content, attrs) => { received.push({ subject: attrs.subject, mode: attrs.mode, content }) },
    { subsKey: key, controlSubject: control, defaults: BENIGN_DEFAULTS, deleteOnDrop: true, ...extra },
  )
  channels.push(ch)
  await ch.start()
  await Bun.sleep(450) // let subscriptions attach
  return { ch, key, control }
}

async function waitFor(fn: () => boolean, ms = 3000) {
  const start = Date.now()
  while (Date.now() - start < ms) { if (fn()) return true; await Bun.sleep(40) }
  return fn()
}

beforeAll(async () => {
  if (!available) return
  const jsm = await tnc.jetstreamManager()
  try { await jsm.streams.add({ name: STREAM, subjects: [JS_WILDCARD] }) } catch {}
})

afterEach(async () => {
  for (const ch of channels) { try { await ch.stop() } catch {} }
  channels.length = 0
})

afterAll(async () => {
  if (!available) return
  try { const jsm = await tnc.jetstreamManager(); await jsm.streams.delete(STREAM) } catch {}
  for (const k of createdKeys) { try { await redis.del(k) } catch {} }
  try { await tnc.drain() } catch {}
  try { (redis as any)?.close?.() } catch {}
})

describe.skipIf(!available)('integration', () => {
  test('IT-1 core-path delivery', async () => {
    const subj = `alloyium.channels.selftest.core.${uid + 1}`
    const rec: Rec[] = []
    await startChannel([{ subject: subj, mode: 'core' }], rec)
    tnc.publish(subj, enc({ kind: 'CORE', n: 1 }))
    expect(await waitFor(() => rec.some((r) => r.subject === subj && r.mode === 'core'))).toBe(true)
  })

  test('IT-2 jetstream durable delivery, acked (no redelivery in-window)', async () => {
    const subj = `alloyium.channels.selftest.js.${uid + 1}`
    const rec: Rec[] = []
    await startChannel([{ subject: subj, mode: 'jetstream', stream: STREAM, durable: `d${uid + 1}`, filter_subject: subj }], rec)
    await tnc.jetstream().publish(subj, enc({ kind: 'JS', n: 1 }))
    expect(await waitFor(() => rec.filter((r) => r.subject === subj).length === 1)).toBe(true)
    await Bun.sleep(700)
    expect(rec.filter((r) => r.subject === subj).length).toBe(1) // acked → not redelivered
  })

  test('IT-3 reload adds a new subscription', async () => {
    const a = `alloyium.channels.selftest.core.a${uid + 1}`
    const b = `alloyium.channels.selftest.core.b${uid + 1}`
    const rec: Rec[] = []
    const { ch, key } = await startChannel([{ subject: a, mode: 'core' }], rec)
    await redis.set(key, JSON.stringify([{ subject: a, mode: 'core' }, { subject: b, mode: 'core' }]))
    await ch.reload(); await Bun.sleep(300)
    tnc.publish(b, enc({ n: 1 }))
    expect(await waitFor(() => rec.some((r) => r.subject === b))).toBe(true)
  })

  test('IT-4 reload removes a subscription (delivery stops)', async () => {
    const subj = `alloyium.channels.selftest.core.r${uid + 1}`
    const rec: Rec[] = []
    const { ch, key } = await startChannel([{ subject: subj, mode: 'core' }], rec)
    await redis.set(key, JSON.stringify([]))
    await ch.reload(); await Bun.sleep(300)
    const before = rec.length
    tnc.publish(subj, enc({ n: 1 }))
    await Bun.sleep(600)
    expect(rec.length).toBe(before) // nothing new
  })

  test('IT-5 control-subject nudge triggers a reload', async () => {
    const a = `alloyium.channels.selftest.core.n${uid + 1}`
    const rec: Rec[] = []
    const { key, control } = await startChannel([{ subject: 'alloyium.channels.selftest.core.placeholder' }].map((s) => ({ ...s, mode: 'core' as const })), rec)
    await redis.set(key, JSON.stringify([{ subject: a, mode: 'core' }]))
    tnc.publish(control, enc({ op: 'reload' }))
    await Bun.sleep(400)
    tnc.publish(a, enc({ n: 1 }))
    expect(await waitFor(() => rec.some((r) => r.subject === a))).toBe(true)
  })

  test('IT-6 editing a spec in place actually applies the new throttle (H2 regression)', async () => {
    const subj = `alloyium.channels.selftest.core.e${uid + 1}`
    const rec: Rec[] = []
    const { ch, key } = await startChannel([{ subject: subj, mode: 'core' }], rec)
    // same id, add a huge min_interval — must take effect on reload
    await redis.set(key, JSON.stringify([{ subject: subj, mode: 'core', min_interval_ms: 100_000 }]))
    await ch.reload(); await Bun.sleep(300)
    const before = rec.length
    for (let i = 0; i < 5; i++) tnc.publish(subj, enc({ n: i }))
    await Bun.sleep(700)
    expect(rec.length - before).toBe(1) // first passes, rest throttled — NOT 5
  })

  test('IT-7 malformed Redis JSON keeps current subs (does NOT revert to defaults)', async () => {
    const subj = `alloyium.channels.selftest.core.m${uid + 1}`
    const rec: Rec[] = []
    const { ch, key } = await startChannel([{ subject: subj, mode: 'core' }], rec)
    await redis.set(key, '{ this is : not json')
    await ch.reload(); await Bun.sleep(300)
    tnc.publish(subj, enc({ n: 1 })) // original sub must still be live
    expect(await waitFor(() => rec.some((r) => r.subject === subj))).toBe(true)
  })

  test('IT-8 unreachable Redis does not hang startup (bounded by timeout)', async () => {
    const rec: Rec[] = []
    const t0 = Date.now()
    await startChannel([{ subject: 'alloyium.channels.selftest.core.x', mode: 'core' }], rec, { redisUrl: 'redis://127.0.0.1:65011' })
    expect(Date.now() - t0).toBeLessThan(8000) // REDIS_TIMEOUT_MS(2500) + attach(450), well under a hang
  })

  test('IT-9 sample throttle drops messages (1st + every Nth)', async () => {
    const subj = `alloyium.channels.selftest.core.s${uid + 1}`
    const rec: Rec[] = []
    await startChannel([{ subject: subj, mode: 'core', sample: 3 }], rec)
    for (let i = 0; i < 9; i++) tnc.publish(subj, enc({ n: i }))
    await Bun.sleep(800)
    expect(rec.filter((r) => r.subject === subj).length).toBe(3) // 9 / 3
  })

  test('IT-11 in-place throttle edit on a JS sub applies the gate WITHOUT dropping the durable/backlog', async () => {
    const subj = `alloyium.channels.selftest.js.e${uid + 1}`
    const dur = `dedit${uid + 1}`
    const base: SubSpec = { subject: subj, mode: 'jetstream', stream: STREAM, durable: dur, filter_subject: subj }
    const rec: Rec[] = []
    const { ch, key } = await startChannel([base], rec)
    const js = tnc.jetstream()
    const jsm = await tnc.jetstreamManager()
    await js.publish(subj, enc({ id: 'pre' }))
    expect(await waitFor(() => rec.some((r) => r.content.includes('pre')))).toBe(true)

    // edit in place: add a huge min_interval (local-only change)
    await redis.set(key, JSON.stringify([{ ...base, min_interval_ms: 100_000 }]))
    await ch.reload(); await Bun.sleep(300)
    // durable must STILL exist (not deleted by a throttle-only edit)
    let stillThere = true
    try { await jsm.consumers.info(STREAM, dur) } catch { stillThere = false }
    expect(stillThere).toBe(true)

    // throttle now in effect: a burst yields exactly one delivery
    const before = rec.length
    for (let i = 0; i < 4; i++) await js.publish(subj, enc({ id: `post${i}` }))
    await Bun.sleep(700)
    expect(rec.length - before).toBe(1)
  })

  test('IT-12 concurrent reload() calls do not double-subscribe (serialized + coalesced)', async () => {
    const subj = `alloyium.channels.selftest.core.c${uid + 1}`
    const rec: Rec[] = []
    const { ch } = await startChannel([{ subject: subj, mode: 'core' }], rec)
    await Promise.all([ch.reload(), ch.reload(), ch.reload()]) // fire concurrently
    await Bun.sleep(200)
    tnc.publish(subj, enc({ n: 1 }))
    await Bun.sleep(500)
    // exactly one delivery — no duplicate subscription from interleaved reloads
    expect(rec.filter((r) => r.subject === subj).length).toBe(1)
  })

  test('IT-10 dropping a JS sub deletes the durable; re-add does NOT replay the backlog', async () => {
    const subj = `alloyium.channels.selftest.js.b${uid + 1}`
    const dur = `dback${uid + 1}`
    const spec: SubSpec = { subject: subj, mode: 'jetstream', stream: STREAM, durable: dur, filter_subject: subj }
    const rec: Rec[] = []
    const { ch, key } = await startChannel([spec], rec)
    const js = tnc.jetstream()
    const jsm = await tnc.jetstreamManager()

    await js.publish(subj, enc({ id: 'msg1' }))
    expect(await waitFor(() => rec.some((r) => r.content.includes('msg1')))).toBe(true)

    // remove → durable deleted
    await redis.set(key, JSON.stringify([]))
    await ch.reload(); await Bun.sleep(300)
    let durableGone = false
    try { await jsm.consumers.info(STREAM, dur) } catch { durableGone = true }
    expect(durableGone).toBe(true)

    // publish backlog while gone (persisted in the stream)
    await js.publish(subj, enc({ id: 'msg2' }))
    await js.publish(subj, enc({ id: 'msg3' }))

    // re-add (fresh durable, deliver NEW) → backlog must NOT replay
    await redis.set(key, JSON.stringify([spec]))
    await ch.reload(); await Bun.sleep(400)
    await js.publish(subj, enc({ id: 'msg4' }))
    expect(await waitFor(() => rec.some((r) => r.content.includes('msg4')))).toBe(true)
    expect(rec.some((r) => r.content.includes('msg2'))).toBe(false)
    expect(rec.some((r) => r.content.includes('msg3'))).toBe(false)
  })
})
