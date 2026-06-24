// SLICE 3 — beat/status plane schema module + core StatusPlane. The PURE schema tests run with
// no bus; the StatusPlane + A2ACore tests use live NATS (skip/require per the gate flag).
import { test, expect, describe, afterAll } from 'bun:test'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import {
  buildBeat, buildStatus, isValidBeat, isValidStatus, beatSubject, statusSubject,
  BEAT_SCHEMA, STATUS_SCHEMA, type AgentBeat,
} from '../plane_schemas.ts'
import { StatusPlane } from '../status_plane.ts'
import { A2ACore } from '../a2a_core.ts'
import { signEnvelope, verifyEnvelope, isValidInbound, type Envelope } from '../a2a-channel.ts'
import { requireBus } from './_require_bus.ts'

// Build a SIGNED a2a beat envelope (what dev-pm's agent emits): the beat is env.body, env signed.
async function signedBeatEnvelope(fromId: string, beat: any, privKey: CryptoKey): Promise<{ env: Envelope; raw: Uint8Array }> {
  const env: Envelope = { v: 1, id: crypto.randomUUID(), from: fromId, to: 'topic:agent-beat', type: 'msg', ts: new Date().toISOString(), body: JSON.stringify(beat) }
  env.alg = 'ed25519'; env.sig = await signEnvelope(env, 'ed25519', privKey)
  return { env, raw: new TextEncoder().encode(JSON.stringify(env)) }
}
const genKp = () => crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as Promise<CryptoKeyPair>

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'

// ── (A) PURE schema module — runs without a bus (single source of truth, dev-pm imports it) ──
describe('plane_schemas — buildBeat/buildStatus + validators (pure)', () => {
  const at = () => 1_700_000_000_000 // fixed clock

  test('buildBeat fills authoritative fields + defaults; caller can never spoof v/schema/agent_id/ts', () => {
    const b = buildBeat('agent-x', { loop_seq: 5, state: 'in_task', boot_id: 'boot1', session_id: 'sess1', host: 'h', schema: 'evil' as any, agent_id: 'spoof' as any }, at)
    expect(b.schema).toBe(BEAT_SCHEMA)   // authoritative, not 'evil'
    expect(b.agent_id).toBe('agent-x')   // authoritative, not 'spoof'
    expect(b.v).toBe(1)
    expect(b.ts).toBe(new Date(at()).toISOString())
    expect(b).toMatchObject({ loop_seq: 5, state: 'in_task', driver_mode: 'loop', stop_hook_active: false, inbox_depth: 0, task_ids: [] })
    expect(isValidBeat(b)).toBe(true)
  })

  test('buildStatus fills authoritative + rich fields', () => {
    const s = buildStatus('agent-y', { phase: 'SLICE 3', progress: '3/4', state: 'in_task', boot_id: 'b', session_id: 's', host: 'h', attrs: { k: 'v' } }, at)
    expect(s.schema).toBe(STATUS_SCHEMA)
    expect(s.agent_id).toBe('agent-y')
    expect(s).toMatchObject({ phase: 'SLICE 3', progress: '3/4', state: 'in_task', attrs: { k: 'v' } })
    expect(isValidStatus(s)).toBe(true)
  })

  test('isValidBeat is fail-closed on malformed input', () => {
    const ok = buildBeat('a', { boot_id: 'b', session_id: 's', host: 'h' }, at)
    expect(isValidBeat(ok)).toBe(true)
    expect(isValidBeat(null)).toBe(false)
    expect(isValidBeat({ ...ok, schema: 'agent.beat.v2' })).toBe(false)       // wrong schema
    expect(isValidBeat({ ...ok, v: 2 })).toBe(false)                          // wrong version
    expect(isValidBeat({ ...ok, agent_id: 'BAD ID' })).toBe(false)           // off-token id
    expect(isValidBeat({ ...ok, agent_id: 123 })).toBe(false)               // non-string id (TOKEN_RE must not coerce)
    expect(isValidBeat({ ...ok, host: 'x'.repeat(300) })).toBe(false)        // size bound
    expect(isValidBeat({ ...ok, loop_seq: 'x' })).toBe(false)                // non-numeric seq
    expect(isValidBeat({ ...ok, driver_mode: 'nope' })).toBe(false)          // bad driver
    expect(isValidBeat({ ...ok, state: 'nope' })).toBe(false)                // bad state
    expect(isValidBeat({ ...ok, ts: 'not-a-date' })).toBe(false)             // bad ts
    expect(isValidBeat({ ...ok, task_ids: [1, 2] })).toBe(false)             // non-string task ids
  })

  test('isValidStatus is fail-closed; subjects ride the topic.<token> allowlist shape', () => {
    const ok = buildStatus('a', { boot_id: 'b', session_id: 's', host: 'h' }, at)
    expect(isValidStatus(ok)).toBe(true)
    expect(isValidStatus({ ...ok, schema: 'x' })).toBe(false)
    expect(isValidStatus({ ...ok, attrs: { k: 5 } })).toBe(false)            // non-string attr value
    expect(beatSubject()).toBe('alloyium.a2a.topic.agent-beat')
    expect(statusSubject()).toBe('alloyium.a2a.topic.agent-status')
    expect(beatSubject('alloyium.a2a.it7.')).toBe('alloyium.a2a.it7.topic.agent-beat') // prefix substitutes
  })
})

// ── (B) live: StatusPlane round-trips + A2ACore core-beat ───────────────────────────────────
let available = true
let nc: NatsConnection
let redis: RedisClient
try {
  nc = await connect({ servers: NATS_URL, name: 'plane-test' })
  redis = new RedisClient(REDIS_URL)
  await redis.set('alloyium:a2a:planetest:probe', '1'); await redis.del('alloyium:a2a:planetest:probe')
} catch { available = false }
requireBus(available, 'plane-schemas', { NATS_URL, REDIS_URL })

let uid = Math.floor(Math.random() * 1e6)
const cores: A2ACore[] = []
const planes: StatusPlane[] = []
afterAll(async () => {
  for (const p of planes) { try { p.stop() } catch {} }
  for (const c of cores) { try { await c.stop() } catch {} }
  if (available) { try { await nc.drain() } catch {}; try { (redis as any).close?.() } catch {} }
})

// Collect messages on a subject into an array (decoded JSON).
function collect(subject: string): { msgs: any[] } {
  const out: any[] = []
  const sub = nc.subscribe(subject)
  ;(async () => { for await (const m of sub) { try { out.push(JSON.parse(new TextDecoder().decode(m.data))) } catch {} } })().catch(() => {})
  return { msgs: out }
}
async function waitFor(fn: () => boolean, ms = 8000, step = 30): Promise<boolean> {
  const t0 = Date.now(); while (Date.now() - t0 < ms) { if (fn()) return true; await Bun.sleep(step) }
  return fn()
}

describe.skipIf(!available)('StatusPlane — relay/publish on the (control) connection', () => {
  test('relayBeat publishes the agent beat UNCHANGED; publishStatus publishes a valid status', async () => {
    const prefix = `alloyium.a2a.pl${uid++}.`
    const plane = new StatusPlane(nc, { prefix, coreBeatMs: 0 }); planes.push(plane) // no core-beat timer
    const beats = collect(`${prefix}topic.agent-beat`)
    const stats = collect(`${prefix}topic.agent-status`)
    await Bun.sleep(150) // let the subs establish
    const beat = buildBeat('agent-z', { boot_id: 'b1', session_id: 's1', host: 'h', loop_seq: 9, sig: 'AAAA', alg: 'ed25519' })
    expect(plane.relayBeat(beat)).toBe(true)
    expect(plane.publishStatus(buildStatus('agent-z', { boot_id: 'b1', session_id: 's1', host: 'h', phase: 'p' }))).toBe(true)
    expect(await waitFor(() => beats.msgs.length > 0 && stats.msgs.length > 0)).toBe(true)
    expect(beats.msgs[0]).toMatchObject({ schema: BEAT_SCHEMA, agent_id: 'agent-z', loop_seq: 9, sig: 'AAAA', alg: 'ed25519' }) // UNCHANGED (sig+alg preserved)
    expect(stats.msgs[0]).toMatchObject({ schema: STATUS_SCHEMA, agent_id: 'agent-z', phase: 'p' })
  }, 20_000)

  test('relayBeatBytes forwards a SIGNED a2a beat ENVELOPE byte-identical so env.sig survives + still verifies (SLICE 3.1)', async () => {
    const prefix = `alloyium.a2a.pl${uid++}.`
    const plane = new StatusPlane(nc, { prefix, coreBeatMs: 0 }); planes.push(plane)
    const got: Uint8Array[] = []
    const sub = nc.subscribe(`${prefix}topic.agent-beat`)
    ;(async () => { for await (const m of sub) got.push(m.data) })().catch(() => {})
    await Bun.sleep(150)
    const kp = await genKp()
    const beat = buildBeat('agent-beat-x', { boot_id: 'b', session_id: 's', host: 'h', loop_seq: 3 })
    const { raw } = await signedBeatEnvelope('agent-beat-x', beat, kp.privateKey) // the wire form: beat = env.body, signed
    expect(plane.relayBeatBytes(raw)).toBe(true)
    expect(await waitFor(() => got.length > 0)).toBe(true)
    expect(Buffer.from(got[0]).equals(Buffer.from(raw))).toBe(true) // BYTE-identical → env.sig intact
    const relayed = JSON.parse(new TextDecoder().decode(got[0]))
    expect(await verifyEnvelope(relayed as Envelope, kp.publicKey, 'ed25519')).toBe(true) // the relayed bytes STILL verify
    // fail-closed: a BARE beat (not an envelope) is dropped (proves envelope-awareness), as is garbage
    expect(plane.relayBeatBytes(new TextEncoder().encode(JSON.stringify(beat)))).toBe(false)
    expect(plane.relayBeatBytes(new TextEncoder().encode('not json {{{'))).toBe(false)
    // misrouted: a structurally-valid SIGNED envelope whose beat body is fine but env.to is NOT the beat
    // topic is dropped — the relay won't launder a misroute onto agent-beat (folds GPT-5.5 conditional-P1).
    const mis: Envelope = { v: 1, id: crypto.randomUUID(), from: 'agent-beat-x', to: 'topic:agent-other', type: 'msg', ts: new Date().toISOString(), body: JSON.stringify(beat) }
    mis.alg = 'ed25519'; mis.sig = await signEnvelope(mis, 'ed25519', kp.privateKey)
    expect(isValidInbound(mis)).toBe(true)                                              // it IS a valid envelope…
    expect(plane.relayBeatBytes(new TextEncoder().encode(JSON.stringify(mis)))).toBe(false) // …but wrong-to → relay drops it
  }, 20_000)

  test('relayBeat / publishStatus are fail-closed — off-token id + bad schema dropped, nothing published', async () => {
    const prefix = `alloyium.a2a.pl${uid++}.`
    const plane = new StatusPlane(nc, { prefix, coreBeatMs: 0 }); planes.push(plane)
    const beats = collect(`${prefix}topic.agent-beat`)
    await Bun.sleep(150)
    const offToken = { ...buildBeat('valid-id', { boot_id: 'b', session_id: 's', host: 'h' }), agent_id: 'BAD ID' } as any // full valid beat w/ a space in the id
    expect(plane.relayBeat(offToken)).toBe(false)                                  // rejected on TOKEN_RE, not a missing field
    expect(plane.relayBeat({ ...buildBeat('valid-id', { boot_id: 'b', session_id: 's', host: 'h' }), agent_id: 123 } as any)).toBe(false) // non-string id (no coercion)
    expect(plane.publishStatus({ schema: 'nope' } as any)).toBe(false)
    await Bun.sleep(400)
    expect(beats.msgs.length).toBe(0)
    expect(plane.counts().dropped).toBeGreaterThanOrEqual(3)
  }, 20_000)

  test('StatusPlane confines to alloyium.a2a.> — an off-namespace prefix is rejected at construction', () => {
    expect(() => new StatusPlane(nc, { prefix: 'trades.' })).toThrow()       // deny-prefix
    expect(() => new StatusPlane(nc, { prefix: 'alloyium.a2a.ok.' })).not.toThrow()
  })
})

describe.skipIf(!available)('A2ACore — emits its OWN beat+status as SIGNED a2a envelopes (SLICE 3.1)', () => {
  test('with a per-host core signer, the core beat is a SIGNED envelope that verifies (detector-parity round-trip)', async () => {
    const prefix = `alloyium.a2a.pl${uid++}.`
    const beats = collect(`${prefix}topic.agent-beat`)
    const stats = collect(`${prefix}topic.agent-status`)
    await Bun.sleep(150)
    const kp = await genKp()            // the per-host core identity keypair (deploy onboards this in prod)
    const coreAgentId = `a2a-core-test-${uid}` // charset-safe (no '@'); unique per run so the core OWNS presence → self-beat emits (SLICE 3.2 gates emit on ownership)
    const core = new A2ACore({ devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix, stream: `ALLOYIUM_A2A_PL_${uid}`, natsPoolSize: 3, statusBeatMs: 250, coreAgentId, coreSigningKey: kp.privateKey })
    cores.push(core); await core.start()
    expect(await waitFor(() => beats.msgs.length > 0 && stats.msgs.length > 0)).toBe(true)
    // the wire payload is a SIGNED a2a ENVELOPE — parse + verify it the SAME way dev-pm's detector does.
    const env = beats.msgs.find((m) => m.from === coreAgentId) as Envelope
    expect(env).toBeDefined()
    expect(isValidInbound(env)).toBe(true)
    expect(await verifyEnvelope(env, kp.publicKey, 'ed25519')).toBe(true) // ← VERIFIES under the core pubkey (detector parity)
    const beat = JSON.parse(env.body)
    expect(beat).toMatchObject({ schema: BEAT_SCHEMA, agent_id: coreAgentId, driver_mode: 'service' })
    expect(env.from).toBe(beat.agent_id) // dev-pm's allowedBeatEmitter requirement (from === beat.agent_id)
    expect(beat.loop_seq).toBeGreaterThanOrEqual(1)
    const statusEnv = stats.msgs.find((m) => m.from === coreAgentId) as Envelope // status is also a signed envelope
    expect(await verifyEnvelope(statusEnv, kp.publicKey, 'ed25519')).toBe(true)
    expect(JSON.parse(statusEnv.body)).toMatchObject({ schema: STATUS_SCHEMA, agent_id: coreAgentId })
    await core.stop()
    const seen = beats.msgs.length; await Bun.sleep(500) // timer cleared on stop → no new beats
    expect(beats.msgs.length).toBe(seen)
  }, 25_000)

  test('with NO core signer, the core does NOT self-emit (an unsigned beat would be dropped on verify)', async () => {
    const prefix = `alloyium.a2a.pl${uid++}.`
    const beats = collect(`${prefix}topic.agent-beat`)
    await Bun.sleep(150)
    const core = new A2ACore({ devNoAuth: true, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix, stream: `ALLOYIUM_A2A_PL_${uid}`, natsPoolSize: 3, statusBeatMs: 200, coreSigningKeyPath: '' }) // no signer; coreSigningKeyPath:'' (falsy) defeats any A2A_CORE_SIGNING_KEY env leak → hermetic no-signer assertion
    cores.push(core); await core.start()
    await Bun.sleep(700)
    expect(beats.msgs.length).toBe(0) // no signer → no core beat (vs a useless unsigned one)
    await core.stop()
  }, 20_000)
})
