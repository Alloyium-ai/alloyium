// Onboarding tests — key generation, NX self-registration, file output, the
// verify round-trip, the nkey transport-auth path, and a full onboard→talk proof.
// Live Redis + NATS; isolated agent-ids / streams / a throwaway temp dir.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { rmSync, statSync, readFileSync, existsSync } from 'node:fs'
import { join } from 'node:path'

const REPO = new URL('..', import.meta.url).pathname
import {
  generateEd25519Keypair, generateNkeyUser, registerPubkey, PubkeyConflict,
  natsUserBlock, natsAuthSnippet, verifyRoundTrip, onboard, nkeyPublicOf,
} from '../onboard.ts'
import { A2AChannel } from '../a2a-channel.ts'

type Rec = { feed?: string; kind?: string; from?: string; content: string }

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const DIR = '/tmp/a2a-onboard-test'

let available = true
let probe: NatsConnection
let redis: RedisClient
try {
  probe = await connect({ servers: NATS_URL, name: 'onboard-probe' })
  redis = new RedisClient(REDIS_URL)
  await redis.set('alloyium:a2a:onb:probe', '1'); await redis.del('alloyium:a2a:onb:probe')
} catch { available = false }

let uid = Math.floor(Math.random() * 1e6)
const redisKeys: string[] = []
const streams: string[] = []
const channels: A2AChannel[] = []

afterAll(async () => {
  for (const ch of channels) { try { await ch.stop() } catch {} }
  if (available) {
    const jsm = await probe.jetstreamManager()
    for (const s of streams) { try { await jsm.streams.delete(s) } catch {} }
    for (const k of redisKeys) { try { await redis.del(k) } catch {} }
    try { await probe.drain() } catch {}
  }
  try { rmSync(DIR, { recursive: true, force: true }) } catch {}
})

const waitFor = async (fn: () => boolean, ms = 12000, step = 40) => {
  const t0 = Date.now(); while (Date.now() - t0 < ms) { if (fn()) return true; await Bun.sleep(step) } return fn()
}

describe('key generation (pure)', () => {
  test('ed25519 keypair: 32-byte seed + 32-byte pubkey, base64', async () => {
    const { seedB64, pubB64 } = await generateEd25519Keypair()
    expect(Buffer.from(seedB64, 'base64').length).toBe(32)
    expect(Buffer.from(pubB64, 'base64').length).toBe(32)
  })
  test('nkey user: SU… seed, U… public, derivable from seed', () => {
    const { seed, publicKey } = generateNkeyUser()
    expect(seed.startsWith('SU')).toBe(true)
    expect(publicKey.startsWith('U')).toBe(true)
    expect(nkeyPublicOf(seed)).toBe(publicKey)
  })
  test('nats authorization snippet restricts publish to a2a + stream-scoped JS API', () => {
    const snip = natsAuthSnippet([natsUserBlock('agent-x', 'UABC', 'ALLOYIUM_A2A')])
    expect(snip).toContain('nkey: UABC')
    expect(snip).toContain('"alloyium.a2a.>"')
    expect(snip).toContain('$JS.API.STREAM.INFO.ALLOYIUM_A2A')
    expect(snip).toContain('$JS.ACK.ALLOYIUM_A2A.>')
    expect(snip).not.toContain('"$JS.API.>"') // no broad JS API grant — can't touch other streams
    expect(snip).not.toContain('trades.')
  })
})

describe.skipIf(!available)('pubkey self-registration (NX)', () => {
  test('create → idempotent exists → conflict (different key) → force overwrites', async () => {
    const id = `onbk-${uid}`; const key = `alloyium:a2a:pubkey:${id}`; redisKeys.push(key)
    const a = await generateEd25519Keypair(); const b = await generateEd25519Keypair()
    expect(await registerPubkey(redis, id, a.pubB64)).toBe('created')
    expect(await registerPubkey(redis, id, a.pubB64)).toBe('exists') // same key, idempotent
    await expect(registerPubkey(redis, id, b.pubB64)).rejects.toBeInstanceOf(PubkeyConflict)
    expect(await registerPubkey(redis, id, b.pubB64, true)).toBe('forced') // rotation
    expect(await redis.get(key)).toBe(b.pubB64)
  })
})

describe.skipIf(!available)('onboard orchestrator + files + verify', () => {
  test('onboard writes 0600 secrets / 0644 pub / env, registers pubkey, verifies', async () => {
    const id = `onbf-${uid}`; redisKeys.push(`alloyium:a2a:pubkey:${id}`)
    const r = await onboard({ id, dir: DIR, redis, natsUrl: NATS_URL, redisUrl: REDIS_URL })
    expect(r.pubkeyStatus).toBe('created')
    expect(r.verified).toBe(true)
    expect(r.nkeyPublic.startsWith('U')).toBe(true)
    expect(statSync(r.files.seedPath).mode & 0o777).toBe(0o600)
    expect(statSync(r.files.nkeyPath).mode & 0o777).toBe(0o600)
    expect(statSync(r.files.pubPath).mode & 0o777).toBe(0o644)
    const env = readFileSync(r.files.envPath, 'utf8')
    expect(env).toContain(`A2A_AGENT_ID=${id}`)
    expect(env).toContain(`A2A_SIGNING_KEY=${r.files.seedPath}`)
    expect(env).toContain(`A2A_NKEY=${r.files.nkeyPath}`)
  })
  test('re-onboard reuses keys (idempotent); --force rotates', async () => {
    const id = `onbr-${uid}`; redisKeys.push(`alloyium:a2a:pubkey:${id}`)
    const r1 = await onboard({ id, dir: DIR, redis })
    const r2 = await onboard({ id, dir: DIR, redis })
    expect(r2.reusedKeys).toBe(true)
    expect(r2.pubkeyB64).toBe(r1.pubkeyB64)
    expect(r2.pubkeyStatus).toBe('exists')
    const r3 = await onboard({ id, dir: DIR, redis, force: true })
    expect(r3.pubkeyB64).not.toBe(r1.pubkeyB64)
    expect(r3.pubkeyStatus).toBe('forced')
  })
  test('transport=none (Option A): anonymous-transport env, no nkey, signing key present', async () => {
    const id = `onbn0-${uid}`; redisKeys.push(`alloyium:a2a:pubkey:${id}`)
    const r = await onboard({ id, dir: DIR, redis, transport: 'none' })
    expect(r.transport).toBe('none')
    expect(r.nkeyPublic).toBeNull()
    expect(r.natsUserBlock).toBeNull()       // no server step
    expect(r.files.nkeyPath).toBeNull()
    expect(r.verified).toBe(true)
    expect(existsSync(`${DIR}/${id}.nk`)).toBe(false)
    const env = readFileSync(r.files.envPath, 'utf8')
    expect(env).toContain('A2A_TRANSPORT_AUTH=none')
    expect(env).not.toContain('A2A_NKEY')
    expect(env).toContain('A2A_SIGNING_KEY=')   // signing stays ON
  })
  test('verifyRoundTrip fails if the Redis pubkey is tampered', async () => {
    const id = `onbv-${uid}`; const key = `alloyium:a2a:pubkey:${id}`; redisKeys.push(key)
    const r = await onboard({ id, dir: DIR, redis })
    expect(r.verified).toBe(true)
    const other = await generateEd25519Keypair()
    await redis.set(key, other.pubB64)
    expect(await verifyRoundTrip(redis, id, readFileSync(r.files.seedPath, 'utf8').trim())).toBe(false)
  })
})

describe.skipIf(!available)('onboarded identities work on the live bridge', () => {
  test('a channel starts using A2A_NKEY transport auth (no devNoAuth, no skipCreds)', async () => {
    uid++; const id = `onbn-${uid}`; const stream = `ALLOYIUM_A2A_ONB_${uid}`; const prefix = `alloyium.a2a.onb${uid}.`
    streams.push(stream); redisKeys.push(`alloyium:a2a:pubkey:${id}`, `alloyium:a2a:presence:${id}`)
    const r = await onboard({ id, dir: DIR, redis })
    const ch = new A2AChannel(async () => {}, {
      enabled: true, agentId: id, sigAlg: 'ed25519',
      signingKeyPath: r.files.seedPath, nkeyPath: r.files.nkeyPath, // transport via nkey, signing via seed
      natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix, stream,
    })
    channels.push(ch)
    await ch.start()
    expect(ch.isStarted()).toBe(true) // nkeyAuthenticator path connected + signing key loaded
  }, 35_000)

  test('CLI parses option/value pairs before the positional id (--dir before <id>)', async () => {
    uid++; const id = `cliord-${uid}`; const dir = `${DIR}/cliord`
    redisKeys.push(`alloyium:a2a:pubkey:${id}`)
    const proc = Bun.spawn([process.execPath, 'onboard.ts', '--dir', dir, '--no-verify', id], {
      cwd: REPO, env: { ...process.env, REDIS_URL, NATS_URL } as Record<string, string>, stdout: 'pipe', stderr: 'pipe',
    })
    expect(await proc.exited).toBe(0)
    expect(existsSync(join(dir, `${id}.seed`))).toBe(true)        // <id> used as agent-id, not the --dir value
    expect(existsSync(join(dir, 'a2a.seed'))).toBe(false)
    expect(await redis.get(`alloyium:a2a:pubkey:${id}`)).not.toBeNull()
  }, 20_000)

  test('two onboarded agents exchange a signed, verified message end-to-end', async () => {
    uid++; const stream = `ALLOYIUM_A2A_ONB_${uid}`; const prefix = `alloyium.a2a.onb${uid}.`
    streams.push(stream)
    const A = `onb-a-${uid}`, B = `onb-b-${uid}`
    for (const x of [A, B]) redisKeys.push(`alloyium:a2a:pubkey:${x}`, `alloyium:a2a:presence:${x}`)
    const ra = await onboard({ id: A, dir: DIR, redis })
    const rb = await onboard({ id: B, dir: DIR, redis })
    const mk = (id: string, files: any, received: Rec[]) => new A2AChannel(
      async (content, attrs) => { received.push({ ...attrs, content } as Rec) },
      { enabled: true, agentId: id, sigAlg: 'ed25519', signingKeyPath: files.seedPath, nkeyPath: files.nkeyPath, natsUrl: NATS_URL, redisUrl: REDIS_URL, prefix, stream },
    )
    const got: Rec[] = []
    const chB = mk(B, rb.files, got); channels.push(chB); await chB.start()
    const chA = mk(A, ra.files, []); channels.push(chA); await chA.start()
    const sent = JSON.parse((await chA.callTool('a2a_send', { to: B, body: 'onboarded-hello', type: 'request' })).content[0].text)
    expect(sent.ok).toBe(true)
    expect(await waitFor(() => got.length > 0)).toBe(true)
    expect(got[0]).toMatchObject({ feed: 'a2a', kind: 'direct', from: A, content: 'onboarded-hello' })
  }, 45_000)
})
