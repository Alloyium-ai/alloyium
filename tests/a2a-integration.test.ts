// A2A integration tests — live NATS + Redis, fully isolated. Each channel uses a
// UNIQUE prefix (alloyium.a2a.it<uid>.) so its inbox subject space and throwaway
// JetStream stream never collide with another test or with production. devNoAuth
// keeps these free of creds/signing key material. Streams + Redis keys are
// cleaned up in afterAll.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { A2AChannel, DIRECT_ENC_ALG, type A2AChannelOpts, signEnvelope, signCanonical, inboxSubject, type Envelope, type VerifyKey, encryptDirectEnvelopeBody } from '../a2a-channel.ts'
import { requireBus } from './_require_bus.ts'
import { mkdtempSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'

let available = true
let probe: NatsConnection
let redis: RedisClient
try {
  probe = await connect({ servers: NATS_URL, name: 'a2a-it-probe' })
  redis = new RedisClient(REDIS_URL)
  await redis.set('alloyium:a2a:it:probe', '1')
  await redis.del('alloyium:a2a:it:probe')
} catch { available = false }
// Gate job (A2A_TEST_REQUIRE_BUS=1) must FAIL — not skip — if the bus is unreachable.
requireBus(available, 'a2a-integration', { NATS_URL, REDIS_URL })

let uid = Math.floor(Math.random() * 1e6)
const channels: A2AChannel[] = []
const streams: string[] = []
const redisKeys: string[] = []
const tempDirs: string[] = []

type Rec = { feed?: string; kind?: string; from?: string; type?: string; corr?: string; subject?: string; id?: string; content: string }
type Fleet = { prefix: string; stream: string }
type ImmediateInboxCheck = { id: string; phase: string; listed: boolean; listedUnhandled: boolean; read: boolean }

function newFleet(): Fleet {
  uid++
  const f = { prefix: `alloyium.a2a.it${uid}.`, stream: `ALLOYIUM_A2A_IT_${uid}` }
  streams.push(f.stream)
  return f
}

async function startChannel(agentId: string, extra: Partial<A2AChannelOpts> = {}, received: Rec[] = [], fleet = newFleet()) {
  const ch = new A2AChannel(
    async (content, attrs) => { received.push({ ...attrs, content } as Rec) },
    { enabled: true, agentId, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: fleet.prefix, stream: fleet.stream, ...extra },
  )
  channels.push(ch)
  await ch.start()
  return { ch, prefix: fleet.prefix, stream: fleet.stream, received, fleet }
}

// Provision an ed25519 keypair: publish the raw pubkey (base64) to Redis under
// the sender's id, return private material for signing and optional direct decrypt.
async function provisionEd25519Material(agentId: string): Promise<{ privateKey: CryptoKey; seed: Uint8Array; pubB64: string }> {
  const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
  const rawPub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
  const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', kp.privateKey))
  const seed = pkcs8.slice(pkcs8.length - 32)
  const key = `alloyium:a2a:pubkey:${agentId}`
  const pubB64 = Buffer.from(rawPub).toString('base64')
  await redis.set(key, pubB64); redisKeys.push(key)
  return { privateKey: kp.privateKey, seed, pubB64 }
}
async function provisionEd25519(agentId: string): Promise<CryptoKey> {
  return (await provisionEd25519Material(agentId)).privateKey
}
async function provisionHmac(agentId: string, secret: string): Promise<string> {
  const key = `alloyium:a2a:secret:${agentId}`
  await redis.set(key, secret); redisKeys.push(key)
  return secret
}
// Signed-mode options for a channel that uses a NATS without auth (skipCreds).
const signed = (signingKey: CryptoKey | string, sigAlg: 'ed25519' | 'hmac' = 'ed25519'): Partial<A2AChannelOpts> =>
  ({ devNoAuth: false, skipCreds: true, sigAlg, signingKey })

// Generous default — these files run in parallel against one NATS/Redis, so
// under contention an operation that takes ~100ms solo can take much longer.
async function waitFor(fn: () => boolean, ms = 12000, step = 40): Promise<boolean> {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) { if (fn()) return true; await Bun.sleep(step) }
  return fn()
}

// Publish an envelope straight to a peer's inbox via the probe connection —
// lets a test forge/replay/tamper without going through a real sender channel.
async function rawPublishInbox(fleet: Fleet, toId: string, env: Partial<Envelope> & { _raw?: string }) {
  const js = probe.jetstream()
  const data = env._raw != null ? env._raw : JSON.stringify(env)
  await js.publish(inboxSubject(fleet.prefix, toId), new TextEncoder().encode(data), {})
}
async function captureNextInboxEnvelope(fleet: Fleet, toId: string): Promise<any> {
  const sub = probe.subscribe(inboxSubject(fleet.prefix, toId))
  try {
    for await (const m of sub) {
      sub.unsubscribe()
      return JSON.parse(Buffer.from(m.data).toString('utf8'))
    }
  } finally {
    try { sub.unsubscribe() } catch {}
  }
}
function mkEnv(from: string, to: string, over: Partial<Envelope> = {}): Envelope {
  return { v: 1, id: crypto.randomUUID(), from, to, type: 'msg', ts: new Date().toISOString(), body: 'x', ...over }
}
function parseToolResult(result: any): any {
  return JSON.parse(result.content[0].text)
}

afterAll(async () => {
  for (const ch of channels) { try { await ch.stop() } catch {} }
  if (available) {
    const jsm = await probe.jetstreamManager()
    for (const s of streams) { try { await jsm.streams.delete(s) } catch {} }
    for (const k of redisKeys) { try { await redis.del(k) } catch {} }
    try { await probe.drain() } catch {}
  }
  for (const d of tempDirs) rmSync(d, { recursive: true, force: true })
})

describe.skipIf(!available)('A2A send path (P3)', () => {
  test('IT-A7 — per-peer rate limit: burst over the bucket is rejected, refusal is reported', async () => {
    const { ch } = await startChannel(`sender-${uid}`, { ratePerPeerPerMin: 2, ratePerMin: 100 })
    const out: any[] = []
    for (let i = 0; i < 3; i++) {
      const r = await ch.callTool('a2a_send', { to: 'topic:rate-room', body: `m${i}` })
      out.push(JSON.parse(r.content[0].text))
    }
    expect(out[0].ok).toBe(true)
    expect(out[1].ok).toBe(true)
    expect(out[2]).toMatchObject({ ok: false, error: 'rate_limited' })
  }, 30_000)

  test('a topic broadcast publishes via core NATS (mode=core, no seq)', async () => {
    const { ch } = await startChannel(`topic-sender-${uid}`)
    const r = JSON.parse((await ch.callTool('a2a_send', { to: 'topic:hello', body: 'hi' })).content[0].text)
    expect(r).toMatchObject({ ok: true, mode: 'core' })
    expect(r.seq).toBeUndefined()
    expect(r.subject.endsWith('topic.hello')).toBe(true)
  }, 30_000)

  test('a direct send to a (not-yet-listening) peer returns a JetStream PubAck seq', async () => {
    const { ch } = await startChannel(`direct-sender-${uid}`)
    const r = JSON.parse((await ch.callTool('a2a_send', { to: 'nobody-home', body: 'parked' })).content[0].text)
    expect(r).toMatchObject({ ok: true, mode: 'jetstream' })
    expect(typeof r.seq).toBe('number')
  }, 30_000)

  test('tool-only channel can share an agent id with its runtime owner and still send', async () => {
    const f = newFleet(); const A = `tool-owner-${uid}`, B = `tool-peer-${uid}`
    const rb: Rec[] = []
    await startChannel(B, {}, rb, f)
    const { ch: owner } = await startChannel(A, {}, [], f)
    expect(owner.isStarted()).toBe(true)

    const { ch: toolOnly } = await startChannel(A, { toolOnly: true }, [], f)
    expect(toolOnly.isStarted()).toBe(true)

    const peers = JSON.parse((await toolOnly.callTool('a2a_peers', {})).content[0].text)
    expect(peers.peers.map((p: any) => p.id)).toContain(B)

    const r = JSON.parse((await toolOnly.callTool('a2a_send', { to: B, body: 'tool-only-hi', type: 'request' })).content[0].text)
    expect(r.ok).toBe(true)
    expect(await waitFor(() => rb.some((m) => m.from === A && m.content === 'tool-only-hi'))).toBe(true)
  }, 30_000)
})

describe.skipIf(!available)('A2A receive path (P4)', () => {
  test('IT-A1 — signed direct send is delivered, attributed feed=a2a, and verified', async () => {
    const f = newFleet(); const A = `a1s-${uid}`, B = `a1r-${uid}`
    const ka = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    await startChannel(B, signed(kb), rb, f)
    const { ch: chA } = await startChannel(A, signed(ka), [], f)
    const r = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'hello', type: 'request' })).content[0].text)
    expect(r.ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0]).toMatchObject({ feed: 'a2a', kind: 'direct', from: A, type: 'request', content: 'hello' })
  }, 35_000)

  test('IT-ENC1 — capable signed direct send is encrypted on the bus and plaintext after delivery', async () => {
    const f = newFleet(); const A = `enc1s-${uid}`, B = `enc1r-${uid}`
    const ma = await provisionEd25519Material(A); const mb = await provisionEd25519Material(B)
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, { ...signed(mb.privateKey), signingSeed: mb.seed }, rb, f)
    const { ch: chA } = await startChannel(A, { ...signed(ma.privateKey), signingSeed: ma.seed }, [], f)
    const rawP = Promise.race([
      captureNextInboxEnvelope(f, B),
      Bun.sleep(8000).then(() => { throw new Error('timed out waiting for raw encrypted envelope') }),
    ])
    await Bun.sleep(50)
    const sent = parseToolResult(await chA.callTool('a2a_send', { to: B, body: 'encrypt-me', type: 'request', thread: 'enc-thread' }))
    expect(sent).toMatchObject({ ok: true, encrypted: true, enc_alg: DIRECT_ENC_ALG })
    const raw = await rawP
    expect(raw.enc).toMatchObject({ alg: DIRECT_ENC_ALG })
    expect(raw.body).not.toBe('encrypt-me')
    expect(await waitFor(() => rb.some((m) => m.content === 'encrypt-me'))).toBe(true)
    const listed = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'list', thread: 'enc-thread' }))
    expect(listed.messages[0]).toMatchObject({ body: 'encrypt-me', from: A, to: B, thread: 'enc-thread' })
  }, 45_000)

  test('IT-ENC2 — required mode refuses direct send when the recipient lacks encryption capability', async () => {
    const f = newFleet(); const A = `enc2s-${uid}`, B = `enc2r-${uid}`
    const ma = await provisionEd25519Material(A)
    const { ch: chA } = await startChannel(A, { ...signed(ma.privateKey), signingSeed: ma.seed, directEncryption: 'required' }, [], f)
    const sent = parseToolResult(await chA.callTool('a2a_send', { to: B, body: 'must-encrypt' }))
    expect(sent).toMatchObject({ ok: false, error: 'direct_encryption_unavailable' })
  }, 35_000)

  test('IT-ENC3 — encrypted direct message for the wrong recipient is dropped after signature verification', async () => {
    const f = newFleet(); const A = `enc3s-${uid}`, B = `enc3r-${uid}`, C = `enc3c-${uid}`
    const ma = await provisionEd25519Material(A); const mb = await provisionEd25519Material(B); const mc = await provisionEd25519Material(C)
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, { ...signed(mb.privateKey), signingSeed: mb.seed }, rb, f)
    const env = mkEnv(A, B, { body: 'wrong-recipient', alg: 'ed25519' })
    const encrypted = await encryptDirectEnvelopeBody(env, env.body, mc.pubB64)
    env.body = encrypted.body
    env.enc = encrypted.enc
    env.sig = await signEnvelope(env, 'ed25519', ma.privateKey)
    await rawPublishInbox(f, B, env)
    await Bun.sleep(900)
    expect(rb.some((m) => m.content === 'wrong-recipient')).toBe(false)
    expect(chB.counts().decryptfail).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A2 — a message sent while the peer is offline is delivered when it starts', async () => {
    const f = newFleet(); const A = `a2s-${uid}`, B = `a2r-${uid}`
    const ka = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const { ch: chA } = await startChannel(A, signed(ka), [], f) // ensures the stream
    expect(JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'parked' })).content[0].text).ok).toBe(true)
    const rb: Rec[] = []
    await startChannel(B, signed(kb), rb, f) // comes online → replays the backlog
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0].content).toBe('parked')
  }, 35_000)

  test('IT-A3 — request/reply correlates via corr', async () => {
    const f = newFleet(); const A = `a3a-${uid}`, B = `a3b-${uid}`
    const ka = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const ra: Rec[] = []; const rb: Rec[] = []
    const { ch: chA } = await startChannel(A, signed(ka), ra, f)
    const { ch: chB } = await startChannel(B, signed(kb), rb, f)
    const sent = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'ping', type: 'request' })).content[0].text)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0].id).toBe(sent.id)
    const rep = JSON.parse((await chB.callTool('a2a_send', { to: A, body: 'pong', type: 'reply', corr: sent.id })).content[0].text)
    expect(rep.ok).toBe(true)
    expect(await waitFor(() => ra.length > 0)).toBe(true)
    expect(ra[0]).toMatchObject({ type: 'reply', corr: sent.id, content: 'pong' })
  }, 35_000)

  test('direct delivery persists to the recipient inbox and survives channel restart', async () => {
    const f = newFleet(); const A = `inbox-s-${uid}`, B = `inbox-r-${uid}`
    const dir = mkdtempSync(join(tmpdir(), 'a2a-inbox-it-')); tempDirs.push(dir)
    const aDb = join(dir, 'a.sqlite3')
    const bDb = join(dir, 'b.sqlite3')
    const ra: Rec[] = []; const rb: Rec[] = []
    const { ch: chA } = await startChannel(A, { inboxDbPath: aDb }, ra, f)
    const { ch: chB } = await startChannel(B, { inboxDbPath: bDb }, rb, f)

    const sent = JSON.parse((await chA.callTool('a2a_send', {
      to: B,
      type: 'request',
      thread: 'persist-thread',
      body: 'persist me',
      attrs: { scope: 'inbox-test' },
    })).content[0].text)
    expect(sent.ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)

    const bList = JSON.parse((await chB.callTool('a2a-inbox-messages', { action: 'list' })).content[0].text)
    expect(bList.ok).toBe(true)
    expect(bList.self).toBe(B)
    expect(bList.messages).toHaveLength(1)
    expect(bList.messages[0]).toMatchObject({ id: sent.id, from: A, to: B, type: 'request', thread: 'persist-thread', body: 'persist me', handled: false })

    const aList = JSON.parse((await chA.callTool('a2a-inbox-messages', { action: 'list' })).content[0].text)
    expect(aList.messages).toHaveLength(0)

    const read = JSON.parse((await chB.callTool('a2a-inbox-messages', { action: 'read', id: sent.id })).content[0].text)
    expect(read.message.attrs).toMatchObject({ scope: 'inbox-test' })
    const acked = JSON.parse((await chB.callTool('a2a-inbox-messages', { action: 'ack', id: sent.id })).content[0].text)
    expect(acked.message.handled).toBe(true)

    await rawPublishInbox(f, B, mkEnv(A, B, { id: sent.id, body: 'persist me redelivered', thread: 'persist-thread' }))
    await Bun.sleep(800)
    const afterDupe = JSON.parse((await chB.callTool('a2a-inbox-messages', { action: 'list' })).content[0].text)
    expect(afterDupe.messages).toHaveLength(1)
    expect(afterDupe.messages[0].body).toBe('persist me')

    await chB.stop()
    const rb2: Rec[] = []
    const { ch: chB2 } = await startChannel(B, { inboxDbPath: bDb }, rb2, f)
    const afterRestart = JSON.parse((await chB2.callTool('a2a-inbox-messages', { action: 'list', handled: true })).content[0].text)
    expect(afterRestart.messages).toHaveLength(1)
    expect(afterRestart.messages[0]).toMatchObject({ id: sent.id, handled: true, body: 'persist me' })
  }, 45_000)

  test('direct delivery is immediately listable from recipient inject path, including startup catch-up', async () => {
    const f = newFleet(); const A = `inbox-stress-s-${uid}`, B = `inbox-stress-r-${uid}`, C = `inbox-stress-c-${uid}`
    const dir = mkdtempSync(join(tmpdir(), 'a2a-inbox-stress-')); tempDirs.push(dir)
    const aDb = join(dir, 'a.sqlite3')
    const bDb = join(dir, 'b.sqlite3')
    const immediateChecks: ImmediateInboxCheck[] = []
    let phase = 'online'
    let chB: A2AChannel
    const recipientInject = async (_content: string, attrs: Record<string, string>) => {
      const id = attrs.id
      const listed = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'list', limit: 200 }))
      const listedUnhandled = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'list', handled: false, limit: 200 }))
      const read = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'read', id }))
      immediateChecks.push({
        id,
        phase,
        listed: listed.messages.some((m: any) => m.id === id),
        listedUnhandled: listedUnhandled.messages.some((m: any) => m.id === id),
        read: read.message?.id === id,
      })
    }
    const chA = new A2AChannel(
      async () => {},
      { enabled: true, agentId: A, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, inboxDbPath: aDb, ratePerMin: 500, ratePerPeerPerMin: 500 },
    )
    channels.push(chA)
    await chA.start()
    chB = new A2AChannel(
      recipientInject,
      { enabled: true, agentId: B, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, inboxDbPath: bDb },
    )
    channels.push(chB)
    await chB.start()

    const onlineN = 100
    for (let i = 0; i < onlineN; i++) {
      const sent = parseToolResult(await chA.callTool('a2a_send', {
        to: B,
        type: 'request',
        thread: 'stress-online',
        body: `online-${i}`,
      }))
      expect(sent.ok).toBe(true)
      expect(await waitFor(() => immediateChecks.some((c) => c.id === sent.id), 8000, 10)).toBe(true)
      const check = immediateChecks.find((c) => c.id === sent.id)
      expect(check).toMatchObject({ phase: 'online', listed: true, listedUnhandled: true, read: true })
    }

    phase = 'catchup'
    const catchupIds: string[] = []
    const catchupN = 100
    for (let i = 0; i < catchupN; i++) {
      const sent = parseToolResult(await chA.callTool('a2a_send', {
        to: C,
        type: 'request',
        thread: 'stress-catchup',
        body: `catchup-${i}`,
      }))
      expect(sent.ok).toBe(true)
      catchupIds.push(sent.id)
    }
    chB = new A2AChannel(
      recipientInject,
      { enabled: true, agentId: C, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, inboxDbPath: bDb },
    )
    channels.push(chB)
    await chB.start()
    expect(await waitFor(() => catchupIds.every((id) => immediateChecks.some((c) => c.id === id)), 15000, 10)).toBe(true)
    for (const id of catchupIds) {
      const check = immediateChecks.find((c) => c.id === id)
      expect(check).toMatchObject({ phase: 'catchup', listed: true, listedUnhandled: true, read: true })
    }
    expect(immediateChecks.filter((c) => c.phase === 'online')).toHaveLength(onlineN)
    expect(immediateChecks.filter((c) => c.phase === 'catchup')).toHaveLength(catchupN)
  }, 120_000)

  test('direct delivery remains listable when inject fails after persist and redelivers idempotently', async () => {
    const prevNakBackoff = process.env.JS_NAK_BACKOFF_MS
    process.env.JS_NAK_BACKOFF_MS = '50'
    try {
      const f = newFleet(); const A = `inbox-fail-s-${uid}`, B = `inbox-fail-r-${uid}`
      const dir = mkdtempSync(join(tmpdir(), 'a2a-inbox-inject-fail-')); tempDirs.push(dir)
      const aDb = join(dir, 'a.sqlite3')
      const bDb = join(dir, 'b.sqlite3')
      let chB: A2AChannel
      const attemptsById = new Map<string, number>()
      const firstFailureChecks: Array<{ id: string; listed: boolean; read: boolean; count: number }> = []
      const recipientInject = async (_content: string, attrs: Record<string, string>) => {
        const id = attrs.id
        const attempt = (attemptsById.get(id) ?? 0) + 1
        attemptsById.set(id, attempt)
        if (attempt === 1) {
          const listed = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'list', limit: 20 }))
          const read = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'read', id }))
          firstFailureChecks.push({
            id,
            listed: listed.messages.some((m: any) => m.id === id),
            read: read.message?.id === id,
            count: listed.messages.filter((m: any) => m.id === id).length,
          })
          throw new Error('INJECT-FAIL-STILL-LISTABLE')
        }
      }
      const chA = new A2AChannel(
        async () => {},
        { enabled: true, agentId: A, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, inboxDbPath: aDb },
      )
      channels.push(chA)
      await chA.start()
      chB = new A2AChannel(
        recipientInject,
        { enabled: true, agentId: B, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, inboxDbPath: bDb, inboxAckWaitMs: 500 },
      )
      channels.push(chB)
      await chB.start()

      const sent = parseToolResult(await chA.callTool('a2a_send', {
        to: B,
        type: 'request',
        thread: 'inject-fail-still-listable',
        body: 'persist before inject failure',
      }))
      expect(sent.ok).toBe(true)
      expect(await waitFor(() => (attemptsById.get(sent.id) ?? 0) >= 2, 15000, 20)).toBe(true)

      expect(firstFailureChecks).toEqual([{ id: sent.id, listed: true, read: true, count: 1 }])
      const afterRedelivery = parseToolResult(await chB.callTool('a2a-inbox-messages', { action: 'list', limit: 20 }))
      const rows = afterRedelivery.messages.filter((m: any) => m.id === sent.id)
      expect(rows).toHaveLength(1)
      expect(rows[0]).toMatchObject({ id: sent.id, from: A, to: B, handled: false, body: 'persist before inject failure' })
      expect(attemptsById.get(sent.id)).toBe(2)
    } finally {
      if (prevNakBackoff == null) delete process.env.JS_NAK_BACKOFF_MS
      else process.env.JS_NAK_BACKOFF_MS = prevNakBackoff
    }
  }, 45_000)

  test('IT-A5 — a duplicate envelope id is injected exactly once', async () => {
    const f = newFleet(); const B = `a5r-${uid}`
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, {}, rb, f) // devNoAuth
    const env = mkEnv('peer-x', B, { body: 'dupe' })
    await rawPublishInbox(f, B, env)
    await rawPublishInbox(f, B, env)
    await Bun.sleep(900)
    expect(rb.length).toBe(1)
    expect(chB.counts().dup).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A6 — an expired (ttl) message is dropped, never injected', async () => {
    const f = newFleet(); const B = `a6r-${uid}`
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, {}, rb, f)
    await rawPublishInbox(f, B, mkEnv('peer-x', B, { body: 'stale', ts: new Date(Date.now() - 10_000).toISOString(), ttl_ms: 1000 }))
    await Bun.sleep(700)
    expect(rb.length).toBe(0)
    expect(chB.counts().expired).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A9 — malformed inbound is dropped+acked, never injected raw', async () => {
    const f = newFleet(); const B = `a9r-${uid}`
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, {}, rb, f)
    await rawPublishInbox(f, B, { _raw: 'not json at all {{{' } as any)
    await Bun.sleep(700)
    expect(rb.length).toBe(0)
    expect(chB.counts().mal).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A13 — a bad signature is dropped (badsig), never injected', async () => {
    const f = newFleet(); const A = `a13s-${uid}`, B = `a13r-${uid}`
    await provisionEd25519(A); const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, signed(kb), rb, f)
    await rawPublishInbox(f, B, mkEnv(A, B, { alg: 'ed25519', sig: Buffer.from(new Uint8Array(64)).toString('base64') }))
    await Bun.sleep(800)
    expect(rb.length).toBe(0)
    expect(chB.counts().badsig).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A18 — an alg downgrade (hmac on an ed25519 receiver) is rejected', async () => {
    const f = newFleet(); const A = `a18s-${uid}`, B = `a18r-${uid}`
    await provisionEd25519(A); const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    const { ch: chB } = await startChannel(B, signed(kb), rb, f)
    await rawPublishInbox(f, B, mkEnv(A, B, { alg: 'hmac', sig: 'AAAA' }))
    await Bun.sleep(800)
    expect(rb.length).toBe(0)
    expect(chB.counts().downgrade).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A19 — pubkey rotation: new-key message delivered, old-key message then rejected', async () => {
    const f = newFleet(); const A = `a19s-${uid}`, B = `a19r-${uid}`
    const k1 = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    await startChannel(B, signed(kb), rb, f)
    const e1 = mkEnv(A, B, { alg: 'ed25519', body: 'one' }); e1.sig = await signEnvelope(e1, 'ed25519', k1)
    await rawPublishInbox(f, B, e1)
    expect(await waitFor(() => rb.some((r) => r.content === 'one'))).toBe(true)
    // rotate the sender's key in Redis
    const kp2 = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
    const rawPub2 = new Uint8Array(await crypto.subtle.exportKey('raw', kp2.publicKey))
    await redis.set(`alloyium:a2a:pubkey:${A}`, Buffer.from(rawPub2).toString('base64'))
    const e2 = mkEnv(A, B, { alg: 'ed25519', body: 'two' }); e2.sig = await signEnvelope(e2, 'ed25519', kp2.privateKey)
    await rawPublishInbox(f, B, e2)
    expect(await waitFor(() => rb.some((r) => r.content === 'two'))).toBe(true) // refetch absorbs rotation
    // a message signed with the OLD key is now rejected
    const e3 = mkEnv(A, B, { alg: 'ed25519', body: 'three' }); e3.sig = await signEnvelope(e3, 'ed25519', k1)
    await rawPublishInbox(f, B, e3)
    await Bun.sleep(800)
    expect(rb.some((r) => r.content === 'three')).toBe(false)
  }, 45_000)

  test('IT-A21 — HMAC mode: a signed message round-trips', async () => {
    const f = newFleet(); const A = `a21s-${uid}`, B = `a21r-${uid}`
    await provisionHmac(A, 'secret-A'); await provisionHmac(B, 'secret-B')
    const rb: Rec[] = []
    await startChannel(B, signed('secret-B', 'hmac'), rb, f)
    const { ch: chA } = await startChannel(A, signed('secret-A', 'hmac'), [], f)
    expect(JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'hmac-hi' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0].content).toBe('hmac-hi')
  }, 35_000)

  test('IT-XALG — a SHARED verify-key cache namespaces by alg: ed25519 + hmac receivers from one sender never cross', async () => {
    // The a2a-core injects ONE verify-key cache across all its sessions. If two sessions
    // use different sig algs, the cache MUST NOT return an ed25519 CryptoKey where an hmac
    // secret is expected. Sender S has BOTH key types provisioned; two receivers share ONE
    // keyCache. (Closes gate review P2-2c.)
    const f = newFleet(); const S = `xas-${uid}`, RE = `xare-${uid}`, RH = `xarh-${uid}`
    const kS = await provisionEd25519(S)         // S's ed25519 keypair (pub → Redis)
    await provisionHmac(S, 'xalg-secret-S')        // S's hmac secret → Redis
    const kRE = await provisionEd25519(RE)         // RE's own ed25519 key (needed to start)
    const sharedKC = new Map<string, { key: VerifyKey; exp: number }>() // ONE cache, both receivers
    const re: Rec[] = []; const rh: Rec[] = []
    await startChannel(RE, { ...signed(kRE, 'ed25519'), sharedKeyCache: sharedKC }, re, f)
    await startChannel(RH, { ...signed('xalg-secret-RH', 'hmac'), sharedKeyCache: sharedKC }, rh, f)
    // ed25519-signed S→RE → caches under 'ed25519:S'
    const eEnv = mkEnv(S, RE, { alg: 'ed25519', body: 'ed-from-S' }); eEnv.sig = await signEnvelope(eEnv, 'ed25519', kS)
    await rawPublishInbox(f, RE, eEnv)
    expect(await waitFor(() => re.some((r) => r.content === 'ed-from-S'))).toBe(true)
    // hmac-signed S→RH on the SAME shared cache. Under the OLD `from`-only cache key this
    // would collide with the cached ed25519 CryptoKey and FAIL hmac verify; namespacing keeps
    // 'ed25519:S' and 'hmac:S' distinct, so RH still verifies + delivers.
    const hEnv = mkEnv(S, RH, { alg: 'hmac', body: 'hmac-from-S' }); hEnv.sig = await signEnvelope(hEnv, 'hmac', 'xalg-secret-S')
    await rawPublishInbox(f, RH, hEnv)
    expect(await waitFor(() => rh.some((r) => r.content === 'hmac-from-S'))).toBe(true)
    // both namespaced entries coexist in the one shared cache → proven no cross-return
    expect(sharedKC.has('ed25519:' + S)).toBe(true)
    expect(sharedKC.has('hmac:' + S)).toBe(true)
  }, 45_000)

  test('IT-SHIMSIGN — externalSign (shim-signs Q2) delegates signing; the envelope verifies at the peer', async () => {
    // The sender holds NO own key — signing is delegated to an EXTERNAL signer (the shim that
    // holds the seed). The wire signature must be identical to core-signs, so a normal receiver
    // verifies it under the sender's pubkey. Proves the core need not hold seeds.
    const f = newFleet(); const A = `ssa-${uid}`, B = `ssb-${uid}`
    const kA = await provisionEd25519(A)  // A's keypair: pub→Redis, privateKey = the "shim's seed"
    const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    await startChannel(B, signed(kb, 'ed25519'), rb, f)
    let extCalls = 0
    const externalSign = async (canon: string) => { extCalls++; return signCanonical('ed25519', kA, canon) }
    // NO signingKey, and signingKeyPath:'' to DEFEAT ambient A2A_SIGNING_KEY env (?? only falls
    // through on null/undefined, so '' wins) — this makes the new `&& !this.externalSign` gate
    // clause LOAD-BEARING regardless of the shell. Only externalSign satisfies the gate.
    const { ch: chA } = await startChannel(A, { devNoAuth: false, skipCreds: true, sigAlg: 'ed25519', signingKeyPath: '', externalSign }, [], f)
    expect(chA.isStarted()).toBe(true)
    const r = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'shim-signed' })).content[0].text)
    expect(r.ok).toBe(true)
    expect(extCalls).toBeGreaterThanOrEqual(1) // signing was DELEGATED, not done in-process
    expect(await waitFor(() => rb.some((x) => x.content === 'shim-signed'))).toBe(true) // verifies under A's pubkey
  }, 35_000)

  test('IT-SHIMSIGN-GATE — without externalSign (and no key), the ed25519 gate still fires (clause is load-bearing)', async () => {
    const f = newFleet(); const A = `ssg-${uid}`
    // Same hermetic config as IT-SHIMSIGN but NO externalSign → a2a_signing_key_required → not started.
    const ch = new A2AChannel(async () => {}, {
      enabled: true, agentId: A, devNoAuth: false, skipCreds: true, sigAlg: 'ed25519', signingKeyPath: '',
      natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream,
    })
    channels.push(ch)
    await ch.start()
    expect(ch.isStarted()).toBe(false) // gated; externalSign is the ONLY thing that would have let it start
    await ch.stop()
  }, 35_000)

  test('IT-SHIMSIGN-FAIL — a throwing/hung externalSign returns {ok:false, sign_failed}, never escapes send()', async () => {
    const f = newFleet(); const A = `ssf-${uid}`, B = `ssf2-${uid}`
    await provisionEd25519(A)
    const externalSign = async (_canon: string) => { throw new Error('shim down') }
    const { ch: chA } = await startChannel(A, { devNoAuth: false, skipCreds: true, sigAlg: 'ed25519', signingKeyPath: '', externalSign }, [], f)
    expect(chA.isStarted()).toBe(true)
    const r = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'will-not-sign' })).content[0].text)
    expect(r).toMatchObject({ ok: false, error: 'sign_failed' }) // structured error, not an unhandled throw
  }, 35_000)

  test('IT-SHIMSIGN-NOSEED — with externalSign the core never reads a local seed (even a bad signingKeyPath)', async () => {
    const f = newFleet(); const A = `ssn-${uid}`
    await provisionEd25519(A)
    const externalSign = async (_canon: string) => Buffer.from(new Uint8Array(64)).toString('base64')
    // signingKeyPath points at a nonexistent file: if start() tried to load it, start would
    // fail. externalSign must short-circuit loadOwnSignKey → started, the seed never read.
    const { ch } = await startChannel(A, { devNoAuth: false, skipCreds: true, sigAlg: 'ed25519', signingKeyPath: '/nonexistent/shim.seed', externalSign }, [], f)
    expect(ch.isStarted()).toBe(true)
  }, 35_000)

  test('IT-TA1 — transport=none (Option A): two agents exchange a SIGNED message over anonymous NATS', async () => {
    const f = newFleet(); const A = `ta-a-${uid}`, B = `ta-b-${uid}`
    const ka = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const none = (k: CryptoKey): Partial<A2AChannelOpts> => ({ devNoAuth: false, transportAuth: 'none', sigAlg: 'ed25519', signingKey: k })
    const rb: Rec[] = []
    await startChannel(B, none(kb), rb, f)
    const { ch: chA } = await startChannel(A, none(ka), [], f)
    expect(JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'optA-hi' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0]).toMatchObject({ feed: 'a2a', from: A, content: 'optA-hi' }) // verified under A's pubkey
  }, 35_000)

  test('IT-A8 — duplicate agent-id is refused to start; a2a_peers lists live peers', async () => {
    const f = newFleet(); const A = `a8a-${uid}`, B = `a8b-${uid}`
    const { ch: chA } = await startChannel(A, {}, [], f)
    const { ch: chB } = await startChannel(B, {}, [], f)
    const pr = JSON.parse((await chA.callTool('a2a_peers', {})).content[0].text)
    expect(pr.ok).toBe(true)
    expect(pr.self).toBe(A)
    expect(pr.peers.map((p: any) => p.id)).toContain(B)
    // a second instance of B (same id) must refuse to start — would split the durable
    const chB2 = new A2AChannel(async () => {}, { enabled: true, agentId: B, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream })
    channels.push(chB2)
    await chB2.start()
    expect(chB2.isStarted()).toBe(false)
    expect(chB.isStarted()).toBe(true)
  }, 35_000)

  test('IT-RB1 — route binding: a direct envelope addressed elsewhere is dropped (replay guard)', async () => {
    const f = newFleet(); const C = `rbc-${uid}`
    const rc: Rec[] = []
    const { ch } = await startChannel(C, {}, rc, f) // devNoAuth
    await rawPublishInbox(f, C, mkEnv('peer-x', 'other-agent', { body: 'misrouted' })) // to != C, onto C's inbox
    await Bun.sleep(800)
    expect(rc.length).toBe(0)
    expect(ch.counts().misroute).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-RB2 — a VALIDLY SIGNED envelope for B replayed onto C is dropped (the H1 attack)', async () => {
    const f = newFleet(); const A = `rba-${uid}`, B = `rbb-${uid}`, C = `rbc2-${uid}`
    const ka = await provisionEd25519(A); const kc = await provisionEd25519(C)
    const rc: Rec[] = []
    const { ch } = await startChannel(C, signed(kc), rc, f)
    // A genuinely signs a message for B; an attacker replays it onto C's inbox.
    const e = mkEnv(A, B, { alg: 'ed25519', body: 'for-B-only' }); e.sig = await signEnvelope(e, 'ed25519', ka)
    await rawPublishInbox(f, C, e)
    await Bun.sleep(900)
    expect(rc.length).toBe(0) // signature verifies, but the route does not — must NOT inject
    expect(ch.counts().misroute).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-RB3 — alg downgrade: an ed25519-signed envelope with alg stripped is dropped', async () => {
    const f = newFleet(); const A = `dga-${uid}`, B = `dgb-${uid}`
    const ka = await provisionEd25519(A); const kb = await provisionEd25519(B)
    const rb: Rec[] = []
    const { ch } = await startChannel(B, signed(kb), rb, f)
    const e = mkEnv(A, B, { body: 'no-alg' }); e.sig = await signEnvelope(e, 'ed25519', ka) // sign but DON'T set alg
    await rawPublishInbox(f, B, e)
    await Bun.sleep(800)
    expect(rb.length).toBe(0)
    expect(ch.counts().downgrade).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-PR1 — presence is released when start fails after the claim (no dup lockout)', async () => {
    const f = newFleet(); const id = `prel-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${id}`)
    // live Redis (claim succeeds) but dead NATS (connect throws after the claim)
    const bad = new A2AChannel(async () => {}, { enabled: true, agentId: id, devNoAuth: true, natsUrl: 'nats://127.0.0.1:65999', redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream })
    channels.push(bad)
    await bad.start()
    expect(bad.isStarted()).toBe(false)
    await bad.stop() // clears the retry timer so it can't re-claim mid-test
    // the presence key was released → a fresh instance of the SAME id starts cleanly
    const { ch } = await startChannel(id, {}, [], f)
    expect(ch.isStarted()).toBe(true)
  }, 35_000)

  test('IT-A10 — stop does not delete the durable; restart resumes without replaying acked msgs', async () => {
    const f = newFleet(); const B = `a10r-${uid}`
    const rb1: Rec[] = []
    const { ch: chB } = await startChannel(B, {}, rb1, f)
    await rawPublishInbox(f, B, mkEnv('peer-x', B, { body: 'first' }))
    expect(await waitFor(() => rb1.length > 0)).toBe(true)
    await chB.stop()
    const rb2: Rec[] = []
    await startChannel(B, {}, rb2, f) // same agent/stream/prefix → same durable
    await Bun.sleep(800)
    expect(rb2.length).toBe(0) // 'first' was acked, not replayed
    await rawPublishInbox(f, B, mkEnv('peer-x', B, { body: 'second' }))
    expect(await waitFor(() => rb2.length > 0)).toBe(true)
    expect(rb2[0].content).toBe('second')
  }, 45_000)
})

describe.skipIf(!available)('A2A topics — self-serve join/leave (P6)', () => {
  test('IT-A4 — topic broadcast reaches a joined peer; the sender does not re-inject its own echo', async () => {
    const f = newFleet(); const S = `a4s-${uid}`, C = `a4c-${uid}`
    const rs: Rec[] = []; const rc: Rec[] = []
    const { ch: chS } = await startChannel(S, {}, rs, f)
    const { ch: chC } = await startChannel(C, {}, rc, f)
    redisKeys.push(`alloyium:a2a:topics:${S}`, `alloyium:a2a:topics:${C}`)
    await chC.callTool('a2a_join_topic', { topic: 'bcast' })
    await chS.callTool('a2a_join_topic', { topic: 'bcast' })
    await Bun.sleep(800) // let the core subscriptions establish (no replay)
    await chS.callTool('a2a_send', { to: 'topic:bcast', body: 'yo' })
    expect(await waitFor(() => rc.some((r) => r.content === 'yo'))).toBe(true)
    expect(rc[0]).toMatchObject({ feed: 'a2a', kind: 'topic', from: S })
    expect(rs.some((r) => r.content === 'yo')).toBe(false) // self-echo dropped
    expect(chS.counts().self).toBeGreaterThanOrEqual(1)
  }, 35_000)

  test('IT-A16 — join/leave is per-agent: a non-member never receives, leaving stops delivery', async () => {
    const f = newFleet(); const A = `a16a-${uid}`, B = `a16b-${uid}`, Sx = `a16s-${uid}`
    const ra: Rec[] = []; const rb: Rec[] = []
    const { ch: chA } = await startChannel(A, {}, ra, f)
    await startChannel(B, {}, rb, f)
    const { ch: chS } = await startChannel(Sx, {}, [], f)
    redisKeys.push(`alloyium:a2a:topics:${A}`)
    const j = JSON.parse((await chA.callTool('a2a_join_topic', { topic: 'room' })).content[0].text)
    expect(j).toMatchObject({ ok: true })
    expect(j.topics).toContain('room')
    await Bun.sleep(800)
    await chS.callTool('a2a_send', { to: 'topic:room', body: 'hi-room' })
    expect(await waitFor(() => ra.some((r) => r.content === 'hi-room'))).toBe(true)
    expect(rb.some((r) => r.content === 'hi-room')).toBe(false) // B never joined
    const l = JSON.parse((await chA.callTool('a2a_leave_topic', { topic: 'room' })).content[0].text)
    expect(l.topics).not.toContain('room')
    await Bun.sleep(800)
    ra.length = 0
    await chS.callTool('a2a_send', { to: 'topic:room', body: 'after-leave' })
    await Bun.sleep(600)
    expect(ra.some((r) => r.content === 'after-leave')).toBe(false) // A left
  }, 45_000)

  test('join is idempotent; leaving the last topic writes a valid []', async () => {
    const f = newFleet(); const A = `a16i-${uid}`
    const { ch } = await startChannel(A, {}, [], f)
    const key = `alloyium:a2a:topics:${A}`; redisKeys.push(key)
    const j1 = JSON.parse((await ch.callTool('a2a_join_topic', { topic: 'r1' })).content[0].text)
    expect(j1.topics).toEqual(['r1'])
    const j2 = JSON.parse((await ch.callTool('a2a_join_topic', { topic: 'r1' })).content[0].text)
    expect(j2.topics).toEqual(['r1']) // idempotent
    const j3 = JSON.parse((await ch.callTool('a2a_join_topic', { topic: 'r2' })).content[0].text)
    expect(j3.topics.slice().sort()).toEqual(['r1', 'r2'])
    JSON.parse((await ch.callTool('a2a_leave_topic', { topic: 'r1' })).content[0].text)
    const l2 = JSON.parse((await ch.callTool('a2a_leave_topic', { topic: 'r2' })).content[0].text)
    expect(l2.topics).toEqual([])
    expect(await redis.get(key)).toBe('[]')
  }, 35_000)

  test('bad token is rejected; a malformed topics key is refused without a write', async () => {
    const f = newFleet(); const A = `a16x-${uid}`
    const { ch } = await startChannel(A, {}, [], f)
    expect(JSON.parse((await ch.callTool('a2a_join_topic', { topic: 'BAD TOPIC' })).content[0].text)).toMatchObject({ ok: false, error: 'bad_topic' })
    const key = `alloyium:a2a:topics:${A}`; redisKeys.push(key)
    await redis.set(key, '{not json')
    const r = JSON.parse((await ch.callTool('a2a_join_topic', { topic: 'ok' })).content[0].text)
    expect(r).toMatchObject({ ok: false, error: 'topics_bad_json' })
    expect(await redis.get(key)).toBe('{not json') // unchanged
  }, 35_000)
})
