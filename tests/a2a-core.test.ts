// a2a-core / shared-connection-injection tests — live NATS + Redis, fully isolated.
//
// Covers PHASE 1 of the mcp-shim/core-app design (brain ops-specs/specs/2026-06-16-
// mcp-shim-core-app): (A) the A2AChannel shared-connection refactor — start() REUSES
// injected NATS+Redis, stop() does NOT close them, the creds gate is bypassed when a
// shared NATS is injected, and shared conns are both-or-neither; (B) the A2ACore
// skeleton — boots owning ONE shared NATS+Redis, multiplexes one A2AChannel per agent
// session keyed by identity, routes inbound to the right session, and tears the shared
// conns down exactly once on stop; (C) the no-injection DEFAULT is unchanged (a
// non-injected channel opens AND closes its OWN conns — the single-agent bridge path).
//
// Isolation mirrors a2a-integration.test.ts: each "fleet" gets a UNIQUE prefix +
// throwaway JetStream stream so subject spaces never collide. devNoAuth keeps these
// free of creds/signing material. Streams + Redis keys are cleaned up in afterAll.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, type NatsConnection, type JetStreamClient } from 'nats'
import { RedisClient } from 'bun'
import { A2AChannel, type A2AChannelOpts, inboxSubject, type Envelope } from '../a2a-channel.ts'
import { A2ACore } from '../a2a_core.ts'
import { requireBus } from './_require_bus.ts'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const TEST_STREAM_LIMITS = {
  streamMaxMsgSize: 64 * 1024,
  streamMaxBytes: 2 * 1024 * 1024,
}

let available = true
// The shared NATS+Redis the INJECTION tests ride — ONE connection set carrying every
// session, exactly as the core will (proves the multiplex, not just that it compiles).
let sharedNats: NatsConnection
let sharedJs: JetStreamClient
let sharedRedis: RedisClient
try {
  sharedNats = await connect({ servers: NATS_URL, name: 'a2a-core-test-shared' })
  sharedJs = sharedNats.jetstream()
  sharedRedis = new RedisClient(REDIS_URL)
  await sharedRedis.set('alloyium:a2a:coretest:probe', '1')
  await sharedRedis.del('alloyium:a2a:coretest:probe')
} catch { available = false }
// Gate job (A2A_TEST_REQUIRE_BUS=1) must FAIL — not skip — if the bus is unreachable.
requireBus(available, 'a2a-core', { NATS_URL, REDIS_URL })

let uid = Math.floor(Math.random() * 1e6)
const channels: A2AChannel[] = []
const cores: A2ACore[] = []
const streams: string[] = []
const redisKeys: string[] = []

type Rec = { feed?: string; kind?: string; from?: string; type?: string; corr?: string; id?: string; content: string }
type Fleet = { prefix: string; stream: string }

function newFleet(): Fleet {
  uid++
  const f = { prefix: `alloyium.a2a.ct${uid}.`, stream: `ALLOYIUM_A2A_CT_${uid}` }
  streams.push(f.stream)
  return f
}

// Start an A2AChannel over the SHARED (test-owned) NATS+Redis — the injected path.
async function startShared(agentId: string, fleet: Fleet, received: Rec[] = [], extra: Partial<A2AChannelOpts> = {}) {
  const ch = new A2AChannel(
    async (content, attrs) => { received.push({ ...attrs, content } as Rec) },
    { enabled: true, agentId, devNoAuth: true, prefix: fleet.prefix, stream: fleet.stream, sharedNats, sharedJs, sharedRedis, ...TEST_STREAM_LIMITS, ...extra },
  )
  channels.push(ch)
  await ch.start()
  return { ch, received }
}

async function waitFor(fn: () => boolean, ms = 12000, step = 40): Promise<boolean> {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) { if (fn()) return true; await Bun.sleep(step) }
  return fn()
}

// Publish straight to a peer's inbox via the shared js — lets a test deliver without a
// second sender channel (used to prove the shared conn is still live after a stop()).
async function rawPublishInbox(fleet: Fleet, toId: string, env: Partial<Envelope>) {
  await sharedJs.publish(inboxSubject(fleet.prefix, toId), new TextEncoder().encode(JSON.stringify(env)), {})
}
function mkEnv(from: string, to: string, over: Partial<Envelope> = {}): Envelope {
  return { v: 1, id: crypto.randomUUID(), from, to, type: 'msg', ts: new Date().toISOString(), body: 'x', ...over }
}

afterAll(async () => {
  for (const c of cores) { try { await c.stop() } catch {} }
  for (const ch of channels) { try { await ch.stop() } catch {} }
  if (available) {
    try {
      const jsm = await sharedNats.jetstreamManager()
      for (const s of streams) { try { await jsm.streams.delete(s) } catch {} }
    } catch {}
    for (const k of redisKeys) { try { await sharedRedis.del(k) } catch {} }
    try { await sharedNats.drain() } catch {}
    try { (sharedRedis as any).close?.() } catch {}
  }
})

// ── (A) A2AChannel shared-connection refactor ──────────────────────────────────
describe.skipIf(!available)('A2AChannel — shared-connection injection (spec §7.4)', () => {
  test('two sessions multiplex over ONE shared NATS+Redis and exchange a direct message', async () => {
    const f = newFleet(); const A = `cta-${uid}`, B = `ctb-${uid}`
    const rb: Rec[] = []
    await startShared(B, f, rb)
    const { ch: chA } = await startShared(A, f)
    const r = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'shared-hi', type: 'request' })).content[0].text)
    expect(r.ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0]).toMatchObject({ feed: 'a2a', kind: 'direct', from: A, type: 'request', content: 'shared-hi' })
  }, 35_000)

  test('CRITICAL — stop() on one session does NOT close the shared NATS/Redis (others survive)', async () => {
    const f = newFleet(); const A = `ctsa-${uid}`, B = `ctsb-${uid}`
    const rb: Rec[] = []
    const { ch: chA } = await startShared(A, f)
    await startShared(B, f, rb)
    // exchange once so both consumers are live
    expect(JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'one' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)

    await chA.stop() // stop ONE session

    // the shared connections must remain open and usable…
    expect(sharedNats.isClosed()).toBe(false)
    const probeKey = `alloyium:a2a:coretest:live-${uid}`; redisKeys.push(probeKey)
    await sharedRedis.set(probeKey, 'still-alive')
    expect(await sharedRedis.get(probeKey)).toBe('still-alive')
    // …and session B (same shared conns) must keep receiving on the still-open connection.
    rb.length = 0
    await rawPublishInbox(f, B, mkEnv('peer-x', B, { body: 'after-stop' }))
    expect(await waitFor(() => rb.some((r) => r.content === 'after-stop'))).toBe(true)
  }, 40_000)

  test('the creds gate is bypassed when a shared NATS is injected (core owns transport auth)', async () => {
    // hmac receiver, OWN signing key injected, NO creds, NOT devNoAuth, transportAuth unset.
    // Without the shared-conn gate bypass this would fail closed as a2a_creds_required.
    const f = newFleet(); const id = `ctgate-${uid}`
    const ch = new A2AChannel(async () => {}, {
      enabled: true, agentId: id, sigAlg: 'hmac', signingKey: 'own-secret',
      prefix: f.prefix, stream: f.stream, sharedNats, sharedJs, sharedRedis, ...TEST_STREAM_LIMITS,
    })
    channels.push(ch)
    await ch.start()
    expect(ch.isStarted()).toBe(true)
  }, 35_000)

  test('shared conns are both-or-neither — injecting only one throws', () => {
    expect(() => new A2AChannel(async () => {}, { enabled: true, agentId: 'x', sharedNats } as any)).toThrow(/both-or-neither/)
    expect(() => new A2AChannel(async () => {}, { enabled: true, agentId: 'x', sharedRedis } as any)).toThrow(/both-or-neither/)
  })

  test('2C pool — sharedPool + sharedNats is rejected (mutually exclusive); pool needs all 3 conns', () => {
    const pool = { consume: sharedNats, publish: sharedNats, control: sharedNats }
    expect(() => new A2AChannel(async () => {}, { enabled: true, agentId: 'x', sharedRedis, sharedPool: pool, sharedNats } as any)).toThrow(/mutually exclusive/)
    expect(() => new A2AChannel(async () => {}, { enabled: true, agentId: 'x', sharedRedis, sharedPool: { consume: sharedNats } } as any)).toThrow(/consume \+ publish \+ control/)
    expect(() => new A2AChannel(async () => {}, { enabled: true, agentId: 'x', sharedPool: pool } as any)).toThrow(/both-or-neither/) // pool still needs sharedRedis
  })

  test('a FAILED injected start() does NOT drain/close the shared conns (start-catch path) — gate P2-2a', async () => {
    const f = newFleet(); const id = `ctfail-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${id}`)
    // ed25519 + a signing-key PATH that does not exist + NOT devNoAuth → start() passes the
    // gate (creds bypassed by sharedConns; signingKeyPath is truthy), reuses the shared nc,
    // then THROWS in loadOwnSignKey → the start-catch path runs with shared conns injected.
    const ch = new A2AChannel(async () => {}, {
      enabled: true, agentId: id, sigAlg: 'ed25519', signingKeyPath: '/nonexistent/a2a-core-test.seed',
      prefix: f.prefix, stream: f.stream, sharedNats, sharedJs, sharedRedis, ...TEST_STREAM_LIMITS,
    })
    channels.push(ch)
    await ch.start()
    expect(ch.isStarted()).toBe(false)         // the key load threw → not started
    expect(sharedNats.isClosed()).toBe(false)  // the catch path must NOT drain the core's nc
    const k = `alloyium:a2a:coretest:catch-${uid}`; redisKeys.push(k)
    await sharedRedis.set(k, 'ok'); expect(await sharedRedis.get(k)).toBe('ok') // nor close the core's Redis
    await ch.stop() // clear the self-heal retry timer the failed start scheduled
  }, 35_000)

  test('a presence-DUP on an injected session leaves the shared Redis OPEN — gate P2-2b', async () => {
    const f = newFleet(); const D = `ctdup-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${D}`)
    const mk = () => new A2AChannel(async () => {}, { enabled: true, agentId: D, devNoAuth: true, prefix: f.prefix, stream: f.stream, sharedNats, sharedJs, sharedRedis, ...TEST_STREAM_LIMITS })
    const ch1 = mk(); channels.push(ch1); await ch1.start()
    expect(ch1.isStarted()).toBe(true)
    // a SECOND instance of the same id over the SAME shared conns → claimPresence returns
    // 'dup'; the claim-fail cleanup must NOT close the shared Redis (it is the core's).
    const ch2 = mk(); channels.push(ch2); await ch2.start()
    expect(ch2.isStarted()).toBe(false)        // refused as duplicate
    expect(sharedNats.isClosed()).toBe(false)
    const k = `alloyium:a2a:coretest:dup-${uid}`; redisKeys.push(k)
    await sharedRedis.set(k, 'ok'); expect(await sharedRedis.get(k)).toBe('ok') // shared Redis still open
    expect(ch1.isStarted()).toBe(true)         // the holder is unaffected
    await ch2.stop() // clear the dup-retry timer
  }, 35_000)
})

// ── (B) A2ACore skeleton ────────────────────────────────────────────────────────
describe.skipIf(!available)('A2ACore skeleton — multiplex sessions over owned shared conns', () => {
  // The core opens its OWN shared NATS+Redis internally (the real connect path).
  function newCore(fleet: Fleet): A2ACore {
    const core = new A2ACore({ devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: fleet.prefix, stream: fleet.stream, sessionDefaults: { devNoAuth: true, ...TEST_STREAM_LIMITS } })
    cores.push(core)
    return core
  }

  test('boots owning one NATS+Redis; addSession multiplexes two agents and routes inbound to the right session', async () => {
    const f = newFleet(); const A = `coa-${uid}`, B = `cob-${uid}`
    const core = newCore(f)
    await core.start()
    expect(core.isStarted()).toBe(true)
    expect(core.natsUp()).toBe(true)

    const ra: Rec[] = []; const rb: Rec[] = []
    expect(await core.addSession(A, async (content, attrs) => { ra.push({ ...attrs, content } as Rec) })).toMatchObject({ ok: true, agentId: A })
    expect(await core.addSession(B, async (content, attrs) => { rb.push({ ...attrs, content } as Rec) })).toMatchObject({ ok: true, agentId: B })
    expect(core.sessionCount()).toBe(2)

    // A sends to B THROUGH the core → routed to B's sink only.
    const r = JSON.parse((await core.callTool(A, 'a2a_send', { to: B, body: 'core-routed', type: 'request' })).content[0].text)
    expect(r.ok).toBe(true)
    expect(await waitFor(() => rb.length > 0)).toBe(true)
    expect(rb[0]).toMatchObject({ feed: 'a2a', kind: 'direct', from: A, content: 'core-routed' })
    expect(ra.length).toBe(0) // not delivered to the wrong session
  }, 40_000)

  test('tool surface + routing: listTools spans a2a+brain+kai+vault; a2a_peers runs on the session; unknown session errors', async () => {
    const f = newFleet(); const A = `cpa-${uid}`, B = `cpb-${uid}`
    const core = newCore(f)
    await core.start()
    await core.addSession(A, async () => {})
    await core.addSession(B, async () => {})

    const names = core.listTools(A).map((t: any) => t.name)
    expect(names).toEqual(expect.arrayContaining(['a2a_send', 'a2a_peers', 'a2a_join_topic', 'a2a_remember', 'a2a_recall', 'kai_send', 'vault_howto']))

    // a2a_peers is dispatched to A's own channel → sees B as a live peer.
    const pr = JSON.parse((await core.callTool(A, 'a2a_peers', {})).content[0].text)
    expect(pr.ok).toBe(true)
    expect(pr.self).toBe(A)
    expect(pr.peers.map((p: any) => p.id)).toContain(B)

    const miss = JSON.parse((await core.callTool('no-such-agent', 'a2a_peers', {})).content[0].text)
    expect(miss).toMatchObject({ ok: false, error: 'unknown_session' })
  }, 40_000)

  test('duplicate session refused; removeSession leaves the shared conns + other sessions alive', async () => {
    const f = newFleet(); const A = `cda-${uid}`, B = `cdb-${uid}`
    const core = newCore(f)
    await core.start()
    await core.addSession(A, async () => {})
    await core.addSession(B, async () => {})
    expect(await core.addSession(A, async () => {})).toMatchObject({ ok: false, error: 'session_exists' })

    expect(await core.removeSession(A)).toBe(true)
    expect(core.hasSession(A)).toBe(false)
    expect(core.natsUp()).toBe(true) // shared conns untouched by a single removeSession
    // B still works on the shared conns
    const pr = JSON.parse((await core.callTool(B, 'a2a_peers', {})).content[0].text)
    expect(pr.ok).toBe(true)
    expect(pr.self).toBe(B)
  }, 40_000)

  test('core.stop() tears the shared conns down once and drops all sessions', async () => {
    const f = newFleet(); const A = `cza-${uid}`
    const core = newCore(f)
    await core.start()
    await core.addSession(A, async () => {})
    expect(core.natsUp()).toBe(true)
    await core.stop()
    expect(core.isStarted()).toBe(false)
    expect(core.natsUp()).toBe(false)
    expect(core.sessionCount()).toBe(0)
  }, 35_000)

  // ── 2C: traffic-class pool + per-session backpressure ──
  function newPoolCore(fleet: Fleet, natsPoolSize: number): A2ACore {
    const core = new A2ACore({ devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: fleet.prefix, stream: fleet.stream, sessionDefaults: { devNoAuth: true, ...TEST_STREAM_LIMITS }, natsPoolSize })
    cores.push(core); return core
  }

  test('2C pool — natsPoolSize:3 (consume/publish/control split) — two sessions exchange end-to-end', async () => {
    const f = newFleet(); const A = `cp3a-${uid}`, B = `cp3b-${uid}`
    const core = newPoolCore(f, 3)
    await core.start()
    expect(core.natsUp()).toBe(true)
    const rb: Rec[] = []
    await core.addSession(B, async (c, a) => { rb.push({ ...a, content: c } as Rec) })
    await core.addSession(A, async () => {})
    // send rides the PUBLISH conn, B's inbox consumer rides the CONSUME conn — both must work.
    expect(JSON.parse((await core.callTool(A, 'a2a_send', { to: B, body: 'pooled' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.some((x) => x.content === 'pooled'))).toBe(true)
  }, 40_000)

  test('2C pool — natsPoolSize:1 (single conn) still works (backward-compat)', async () => {
    const f = newFleet(); const A = `cp1a-${uid}`, B = `cp1b-${uid}`
    const core = newPoolCore(f, 1)
    await core.start()
    const rb: Rec[] = []
    await core.addSession(B, async (c, a) => { rb.push({ ...a, content: c } as Rec) })
    await core.addSession(A, async () => {})
    expect(JSON.parse((await core.callTool(A, 'a2a_send', { to: B, body: 'single' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.some((x) => x.content === 'single'))).toBe(true)
  }, 40_000)

  test('2C backpressure — a SLOW inject on session A does NOT delay delivery to session B (per-session isolation)', async () => {
    const f = newFleet(); const A = `cbpa-${uid}`, B = `cbpb-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${A}`, `alloyium:a2a:presence:${B}`)
    const core = newPoolCore(f, 3)
    await core.start()
    // A's inject simulates a slow/wedged claude: each delivery blocks ~500ms.
    await core.addSession(A, async () => { await Bun.sleep(500) })
    const rb: Rec[] = []
    await core.addSession(B, async (c, a) => { rb.push({ ...a, content: c } as Rec) })
    // Flood A's inbox so A's bounded consumer fills + backpressures (12 × ~500ms ≈ 6s of drain).
    for (let i = 0; i < 12; i++) await rawPublishInbox(f, A, mkEnv('peer-x', A, { body: `a${i}` }))
    // Deliver to B; it must arrive PROMPTLY — B's consumer is independent, never HOL-blocked by A.
    const t0 = Date.now()
    await rawPublishInbox(f, B, mkEnv('peer-y', B, { body: 'b-fast' }))
    expect(await waitFor(() => rb.some((x) => x.content === 'b-fast'), 4000)).toBe(true)
    expect(Date.now() - t0).toBeLessThan(3000) // not serialized behind A's ~6s backlog
  }, 45_000)

  test('2C pool — natsPoolSize:2 (consume separate, publish+control aliased) — exchange works', async () => {
    const f = newFleet(); const A = `cp2a-${uid}`, B = `cp2b-${uid}`
    const core = newPoolCore(f, 2)
    await core.start()
    const rb: Rec[] = []
    await core.addSession(B, async (c, a) => { rb.push({ ...a, content: c } as Rec) })
    await core.addSession(A, async () => {})
    expect(JSON.parse((await core.callTool(A, 'a2a_send', { to: B, body: 'pool2' })).content[0].text).ok).toBe(true)
    expect(await waitFor(() => rb.some((x) => x.content === 'pool2'))).toBe(true)
  }, 40_000)

  test('2C bound — the per-session in-flight bound (max_ack_pending) caps un-acked under a wedged inject (folds P1)', async () => {
    const f = newFleet(); const A = `cbnd-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${A}`)
    // tiny bound (4) so a modest flood proves the cap; wedged inject (never completes in-test).
    const core = new A2ACore({ devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, natsPoolSize: 3, sessionDefaults: { devNoAuth: true, ...TEST_STREAM_LIMITS, inboxQueueMax: 4, inboxMaxAckPending: 4, inboxAckWaitMs: 120000 } })
    cores.push(core); await core.start()
    await core.addSession(A, async () => { await Bun.sleep(120000) }) // wedged: never acks
    for (let i = 0; i < 20; i++) await rawPublishInbox(f, A, mkEnv('peer-x', A, { body: `a${i}` }))
    await Bun.sleep(1500) // let JS deliver up to the ceiling
    const info: any = await (await sharedNats.jetstreamManager()).consumers.info(f.stream, `alloyium-a2a-${A}`)
    // delivered-but-unacked is capped by max_ack_pending (4), NOT 20 — the bound holds (per consume.js).
    expect(info.num_ack_pending).toBeLessThanOrEqual(4)
    expect(info.num_pending).toBeGreaterThan(0) // the rest stay parked in the stream, undelivered
  }, 40_000)

  test('2C tuning — an EXISTING durable is tuned IN PLACE (no recreate/replay) — folds P1-1', async () => {
    const f = newFleet(); const A = `ctun-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${A}`)
    // 1st channel creates the durable with the default tuning…
    const own = (extra: Partial<A2AChannelOpts>) => new A2AChannel(async () => {}, { enabled: true, agentId: A, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, ...TEST_STREAM_LIMITS, ...extra })
    const ch1 = own({}); channels.push(ch1); await ch1.start(); expect(ch1.isStarted()).toBe(true)
    await ch1.stop()
    // …2nd channel (same id/durable) with a DIFFERENT max_ack_pending → must UPDATE in place,
    // NOT delete+recreate (which would replay the backlog). Core fields match → no drift recreate.
    const ch2 = own({ inboxMaxAckPending: 7, inboxAckWaitMs: 90000 }); channels.push(ch2); await ch2.start()
    expect(ch2.isStarted()).toBe(true)
    const info: any = await (await sharedNats.jetstreamManager()).consumers.info(f.stream, `alloyium-a2a-${A}`)
    expect(info.config.max_ack_pending).toBe(7) // applied in place
    await ch2.stop()
  }, 40_000)

  test('core fields are authoritative — a caller cannot override agentId/identity via opts', async () => {
    const f = newFleet(); const A = `cova-${uid}`, B = `covb-${uid}`
    const core = newCore(f)
    await core.start()
    // Attempt to smuggle a different identity through opts — must be ignored: the session
    // registers AND signs/claims/consumes as the registry key 'A', never 'B'.
    expect(await core.addSession(A, async () => {}, { agentId: B } as any)).toMatchObject({ ok: true, agentId: A })
    expect(core.hasSession(A)).toBe(true)
    expect(core.hasSession(B)).toBe(false)
    const pr = JSON.parse((await core.callTool(A, 'a2a_peers', {})).content[0].text)
    expect(pr.self).toBe(A) // the channel bound to A, not the smuggled B
  }, 35_000)

  test('core fails closed: no creds + auth not explicitly disabled → start() rejects', async () => {
    // transportAuth:'creds' with no credsPath and devNoAuth off → the core must refuse to
    // connect rather than silently run the fleet unauthenticated (folded cross-model P1).
    const core = new A2ACore({ natsUrl: NATS_URL, redisUrl: REDIS_URL, devNoAuth: false, transportAuth: 'creds' })
    cores.push(core)
    await expect(core.start()).rejects.toThrow(/creds required/)
    expect(core.isStarted()).toBe(false)
  }, 15_000)

  // ── CF-1: addSession↔stop() lifecycle race (cross-model-convergent must-fix) ──
  test('CF-1 — addSession racing stop() rolls back: returns core_not_started, no leaked session/presence', async () => {
    const f = newFleet(); const A = `cracea-${uid}`
    const presKey = `alloyium:a2a:presence:${A}`; redisKeys.push(presKey)
    const core = newCore(f)
    await core.start()
    // Kick off addSession WITHOUT awaiting, then immediately stop() — the add's start()
    // overlaps the shutdown window. stop() waits the add out; the add re-checks `stopping`
    // after its await and rolls itself back over the still-live bus.
    const addP = core.addSession(A, async () => {})
    await core.stop()
    const r = await addP
    expect(r.ok).toBe(false)
    expect(r.error).toBe('core_not_started') // not {ok:true} onto a torn-down core
    expect(core.sessionCount()).toBe(0)
    expect(core.natsUp()).toBe(false)
    // the rolled-back session left NO stranded presence claim (released on the live bus
    // before stop() drained) — verified via the test's own Redis connection.
    expect(await sharedRedis.get(presKey)).toBeFalsy()
  }, 35_000)

  test('double-stop() / double-signal is idempotent — one teardown, same promise', async () => {
    const f = newFleet(); const A = `cstopa-${uid}`
    const core = newCore(f)
    await core.start()
    await core.addSession(A, async () => {})
    const p1 = core.stop(); const p2 = core.stop() // e.g. SIGTERM then SIGINT
    expect(p1).toBe(p2)                            // same in-flight stop promise (no double drain)
    await Promise.all([p1, p2])
    expect(core.natsUp()).toBe(false)
    expect(core.sessionCount()).toBe(0)
    await core.stop() // a third stop after completion still resolves cleanly (memoized)
  }, 35_000)

  test('concurrent start() awaits ONE start — never a second shared connection set', async () => {
    const f = newFleet(); const A = `cstarta-${uid}`
    const core = newCore(f)
    const a = core.start(); const b = core.start()
    expect(a).toBe(b) // both callers await the SAME in-flight start
    await Promise.all([a, b])
    expect(core.isStarted()).toBe(true)
    expect(core.natsUp()).toBe(true)
    expect(await core.addSession(A, async () => {})).toMatchObject({ ok: true, agentId: A }) // usable after
  }, 35_000)

  test('start() during/after stop() rejects (stopping wins over started) — folds GPT-5.5 P1', async () => {
    const f = newFleet()
    const core = newCore(f)
    await core.start()
    const sp = core.stop() // latches stopping+stopPromise synchronously; _doStop runs async
    // Even in the window before _doStop flips `started` false, a concurrent start() must reject.
    await expect(core.start()).rejects.toThrow(/stopping|stopped/)
    await sp
    await expect(core.start()).rejects.toThrow(/stopping|stopped/) // and after, still terminal
  }, 35_000)

  test('two concurrent addSession(same id) — exactly one wins, the other rolls back', async () => {
    const f = newFleet(); const A = `cdup-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${A}`)
    const core = newCore(f)
    await core.start()
    const [r1, r2] = await Promise.all([core.addSession(A, async () => {}), core.addSession(A, async () => {})])
    expect([r1.ok, r2.ok].filter(Boolean).length).toBe(1) // exactly one ok:true
    expect(core.sessionCount()).toBe(1)                    // no double-register / clobber
    expect(core.hasSession(A)).toBe(true)
    // the WINNER survives the loser's rollback (loser failed the presence SET NX → never
    // claimed → can't clobber): the registered session is fully functional. (Opus P2.)
    const pr = JSON.parse((await core.callTool(A, 'a2a_peers', {})).content[0].text)
    expect(pr.ok).toBe(true)
    expect(pr.self).toBe(A)
  }, 35_000)
})

// ── (C) no-injection DEFAULT unchanged (the single-agent bridge path) ───────────
describe.skipIf(!available)('A2AChannel — no-injection default is unchanged', () => {
  test('a non-injected channel opens its OWN conns, sends via own JetStream, and releases on stop (restart-clean)', async () => {
    const f = newFleet(); const id = `defr-${uid}`
    redisKeys.push(`alloyium:a2a:presence:${id}`)
    // OWN conns (no shared* injected) — the exact single-agent bridge path.
    const own = (received: Rec[] = []) => new A2AChannel(
      async (content, attrs) => { received.push({ ...attrs, content } as Rec) },
      { enabled: true, agentId: id, devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix: f.prefix, stream: f.stream, ...TEST_STREAM_LIMITS },
    )
    const ch1 = own(); channels.push(ch1)
    await ch1.start()
    expect(ch1.isStarted()).toBe(true)
    // proves it opened its OWN NATS/JetStream: a direct send to a parked peer returns a PubAck seq.
    const r = JSON.parse((await ch1.callTool('a2a_send', { to: `nobody-${uid}`, body: 'own-parked' })).content[0].text)
    expect(r).toMatchObject({ ok: true, mode: 'jetstream' })
    expect(typeof r.seq).toBe('number')

    await ch1.stop() // must close its OWN conns + release presence (token-guarded)

    // A FRESH instance of the SAME id starts cleanly → presence was released by stop()
    // (i.e. its own Redis path ran and tore down), exactly as before the refactor.
    const ch2 = own(); channels.push(ch2)
    await ch2.start()
    expect(ch2.isStarted()).toBe(true)
  }, 40_000)
})

// ── (D) authoritative session-opts LOCK — PURE (no bus; runs even without NATS/Redis) ──────
// buildSessionOpts() is the exact merge addSession() feeds A2AChannel; testing it directly
// drives the PROD condition (this.prefix === undefined) that a bus-backed test cannot reach
// in isolation (a default-prefix stream would overlap the prod ALLOYIUM_A2A subject space).
describe('A2ACore.buildSessionOpts — caller opts cannot override identity/routing (P2-1)', () => {
  test('with NO core prefix (prod), opts.prefix/agentId/stream are all locked out', () => {
    // PROD: a core constructed with no prefix → this.prefix === undefined. The OLD conditional
    // spread `...(this.prefix ? {prefix} : {})` would let opts.prefix leak here (o.prefix==='zzbad');
    // the unconditional `prefix: this.prefix` shadows it to undefined (A2AChannel then defaults).
    const core = new A2ACore({ devNoAuth: true, natsUrl: 'nats://127.0.0.1:4222', redisUrl: 'redis://127.0.0.1:6379', stream: 'ALLOYIUM_A2A_LOCKED' })
    const o = core.buildSessionOpts('alice', { prefix: 'alloyium.a2a.zzbad.', agentId: 'bob', stream: 'EVIL_STREAM' } as any)
    expect(o.prefix).toBeUndefined()          // ← would be 'alloyium.a2a.zzbad.' under the pre-fix code
    expect(o.agentId).toBe('alice')           // identity locked (not the smuggled 'bob')
    expect(o.stream).toBe('ALLOYIUM_A2A_LOCKED') // routing locked (not 'EVIL_STREAM')
    expect(o.enabled).toBe(true)
  })

  test('with a core prefix set (test isolation), it is authoritative over opts.prefix too', () => {
    const core = new A2ACore({ devNoAuth: true, natsUrl: 'nats://127.0.0.1:4222', redisUrl: 'redis://127.0.0.1:6379', prefix: 'alloyium.a2a.it99.', stream: 'S' })
    const o = core.buildSessionOpts('alice', { prefix: 'alloyium.a2a.zzbad.' } as any)
    expect(o.prefix).toBe('alloyium.a2a.it99.')
  })
})
