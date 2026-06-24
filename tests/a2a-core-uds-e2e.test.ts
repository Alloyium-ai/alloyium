import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { Buffer } from 'node:buffer'
import { mkdir, rm } from 'node:fs/promises'
import { hostname } from 'node:os'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { A2ACore } from '../a2a_core.ts'
import { startUdsAcceptor } from '../uds_acceptor.ts'
import { FrameDecoder, FrameWriter, FRAME_TYPE_CTRL, FRAME_TYPE_MCP } from '../uds_frame.ts'
import { requireBus } from './_require_bus.ts'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const PROBE_KEY = 'alloyium:a2a:e2e:probe'

let available = true
let probeNats: NatsConnection | undefined
let redis: RedisClient | undefined

try {
  probeNats = await connect({ servers: NATS_URL, name: 'a2a-core-uds-e2e-probe' })
  redis = new RedisClient(REDIS_URL)
  await redis.set(PROBE_KEY, '1')
  await redis.del(PROBE_KEY)
} catch {
  available = false
  try { await redis?.del(PROBE_KEY) } catch {}
  try { await probeNats?.drain() } catch {}
  try { (redis as any)?.close?.() } catch {}
}

requireBus(available, 'a2a-core-uds-e2e', { NATS_URL, REDIS_URL })

const enc = new TextEncoder()
const dec = new TextDecoder()
const runId = `${Date.now()}${Math.floor(Math.random() * 1_000_000)}`
const agentId = `ztest-e2e-${runId}`
const prefix = `alloyium.a2a.e2e${runId}.`
const stream = `ALLOYIUM_A2A_E2E_${runId}`
const tempDir = `/tmp/a2a-core-e2e-${process.pid}-${runId}`
const socketDir = `${tempDir}/sock`
const socketPath = `${socketDir}/core.sock`

const pubkeyKey = `alloyium:a2a:pubkey:${agentId}`
const presenceKey = `alloyium:a2a:presence:${agentId}`
const epochKey = `alloyium:a2a:org:core-epoch:${agentId}`
const redisKeys = [pubkeyKey, presenceKey, epochKey]

const PKCS8_ED25519_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06,
  0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
])

let core: A2ACore | undefined
let acceptor: Awaited<ReturnType<typeof startUdsAcceptor>> | undefined
let signKey: CryptoKey | undefined
const clients: Array<{ close(): void }> = []

function b64(u8: Uint8Array): string {
  return Buffer.from(u8).toString('base64')
}

function fromB64(s: string): Uint8Array {
  return new Uint8Array(Buffer.from(s, 'base64'))
}

function hexToBytes(hex: string): Uint8Array {
  const h = hex.trim().replace(/^0x/i, '')
  if (h.length % 2 !== 0) throw new Error(`odd hex length: ${h.length}`)
  const out = new Uint8Array(h.length / 2)
  for (let i = 0; i < out.length; i++) {
    const n = Number.parseInt(h.slice(i * 2, i * 2 + 2), 16)
    if (!Number.isFinite(n)) throw new Error(`bad hex at byte ${i}`)
    out[i] = n
  }
  return out
}

async function importEd25519Seed(seed: Uint8Array): Promise<CryptoKey> {
  if (seed.length !== 32) throw new Error(`ed25519 seed must be 32 bytes, got ${seed.length}`)
  const pkcs8 = new Uint8Array(PKCS8_ED25519_PREFIX.length + seed.length)
  pkcs8.set(PKCS8_ED25519_PREFIX)
  pkcs8.set(seed, PKCS8_ED25519_PREFIX.length)
  return crypto.subtle.importKey('pkcs8', pkcs8, { name: 'Ed25519' }, false, ['sign'])
}

async function waitFor(fn: () => boolean | Promise<boolean>, ms = 12_000, step = 40): Promise<boolean> {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    if (await fn()) return true
    await Bun.sleep(step)
  }
  return await fn()
}

function jsonBytes(obj: unknown): Uint8Array {
  return enc.encode(JSON.stringify(obj))
}

async function connectClient(path: string) {
  const ctrlQ: any[] = []
  const mcpQ: any[] = []
  const errors: Error[] = []
  const decoder = new FrameDecoder()
  let closed = false

  const socket = await Bun.connect({
    unix: path,
    socket: {
      data(_socket: any, chunk: Uint8Array) {
        try {
          for (const frame of decoder.push(chunk)) {
            if (frame.type === FRAME_TYPE_CTRL) ctrlQ.push(JSON.parse(dec.decode(frame.payload)))
            else if (frame.type === FRAME_TYPE_MCP) mcpQ.push(JSON.parse(dec.decode(frame.payload)))
            else errors.push(new Error(`unexpected frame type ${frame.type}`))
          }
        } catch (e) {
          errors.push(e instanceof Error ? e : new Error(String(e)))
        }
      },
      close() {
        closed = true
      },
      error(_socket: any, error: Error) {
        errors.push(error instanceof Error ? error : new Error(String(error)))
      },
    },
  } as any)

  let overflow: Error | undefined
  const writer = new FrameWriter(
    (chunk: Uint8Array) => { socket.write(chunk) },
    { highWater: 1 << 20, onOverflow: () => { overflow = new Error('client frame writer overflow') } },
  )

  async function flush() {
    if (overflow) throw overflow
    await Promise.resolve(writer.drain())
    if (overflow) throw overflow
  }

  async function nextFrom(q: any[], label: string, ms = 12_000): Promise<any> {
    const ok = await waitFor(() => q.length > 0 || errors.length > 0 || closed, ms, 20)
    if (!ok) throw new Error(`timeout waiting for ${label}`)
    if (errors.length) throw errors.shift()!
    const msg = q.shift()
    if (msg === undefined) throw new Error(`socket closed while waiting for ${label}`)
    return msg
  }

  const api = {
    async sendCtrl(obj: unknown) {
      writer.enqueueCtrl(jsonBytes(obj))
      await flush()
    },
    async sendMcp(obj: unknown) {
      writer.enqueueMcp(jsonBytes(obj))
      await flush()
    },
    nextCtrl(ms?: number) {
      return nextFrom(ctrlQ, 'CTRL frame', ms)
    },
    nextMcp(ms?: number) {
      return nextFrom(mcpQ, 'MCP frame', ms)
    },
    close() {
      if (closed) return
      closed = true
      try { socket.terminate() } catch { try { socket.end?.() } catch {} }
    },
  }

  clients.push(api)
  return api
}

async function nextMcpResponse(client: Awaited<ReturnType<typeof connectClient>>, id: number, ms = 12_000): Promise<any> {
  const deadline = Date.now() + ms
  for (;;) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error(`timeout waiting for MCP response id=${id}`)
    const msg = await client.nextMcp(remaining)
    if (msg?.id === id) return msg
  }
}

async function completeHello(client: Awaited<ReturnType<typeof connectClient>>): Promise<{ t: 'ok'; session: string; epoch: number }> {
  await client.sendCtrl({
    t: 'hello',
    v: 1,
    agentId,
    host: hostname(),
    pid: process.pid,
    subsKey: `subs-${agentId}-${crypto.randomUUID()}`,
  })

  const challenge = await client.nextCtrl()
  expect(challenge.t).toBe('challenge')
  expect(typeof challenge.nonce).toBe('string')

  const nonce = fromB64(challenge.nonce)
  expect(nonce.byteLength).toBe(32)

  const sig = b64(new Uint8Array(await crypto.subtle.sign({ name: 'Ed25519' }, signKey!, nonce)))
  await client.sendCtrl({ t: 'auth', alg: 'ed25519', sig })

  const ok = await client.nextCtrl()
  expect(ok.t).toBe('ok')
  expect(typeof ok.session).toBe('string')
  expect(typeof ok.epoch).toBe('number')
  expect(ok.epoch).toBeGreaterThanOrEqual(1)
  return ok
}

describe.skipIf(!available)('A2ACore + UDS acceptor E2E', () => {
  beforeAll(async () => {
    if (!redis) throw new Error('redis unavailable')

    const vectors = await Bun.file(new URL('./shim-conformance/vectors.json', import.meta.url)).json() as {
      seed_hex: string
      pubkey_raw_b64: string
    }
    if (!vectors.seed_hex || !vectors.pubkey_raw_b64) throw new Error('missing shim conformance seed/pubkey vector')
    expect(fromB64(vectors.pubkey_raw_b64).byteLength).toBe(32)

    signKey = await importEd25519Seed(hexToBytes(vectors.seed_hex))
    await redis.set(pubkeyKey, vectors.pubkey_raw_b64)

    core = new A2ACore({
      devNoAuth: true,
      natsUrl: NATS_URL,
      redisUrl: REDIS_URL,
      prefix,
      stream,
      sessionDefaults: { devNoAuth: true },
    })
    await core.start()

    await mkdir(socketDir, { recursive: true })
    acceptor = await startUdsAcceptor({ core, redis, socketPath, socketDir })
  }, 35_000)

  afterAll(async () => {
    for (const client of clients) { try { client.close() } catch {} }
    try { await acceptor?.close() } catch {}
    try { await core?.stop() } catch {}

    try {
      const jsm = await probeNats?.jetstreamManager()
      if (jsm) { try { await jsm.streams.delete(stream) } catch {} }
    } catch {}

    if (redis) {
      for (const key of redisKeys) { try { await redis.del(key) } catch {} }
    }

    try { await probeNats?.drain() } catch {}
    try { (redis as any)?.close?.() } catch {}
    try { await rm(tempDir, { recursive: true, force: true }) } catch {}
  }, 35_000)

  test('end-to-end: hello -> session -> initialize/tools.list -> close removes session; second hello raises epoch', async () => {
    expect(core).toBeDefined()
    expect(acceptor).toBeDefined()
    expect(signKey).toBeDefined()

    const requiredTools = ['a2a_send', 'a2a_peers', 'a2a_remember', 'a2a_recall', 'kai_send']

    const client1 = await connectClient(acceptor!.socketPath)
    let firstEpoch = 0
    try {
      const ok = await completeHello(client1)
      firstEpoch = ok.epoch

      expect(await waitFor(() => core!.hasSession(agentId))).toBe(true)
      expect(core!.sessionCount()).toBeGreaterThanOrEqual(1)

      await client1.sendMcp({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-06-18',
          capabilities: {},
          clientInfo: { name: 'e2e', version: '1' },
        },
      })

      const init = await nextMcpResponse(client1, 1)
      expect(init.error).toBeUndefined()
      expect(init.result?.serverInfo).toEqual({ name: 'alloyium', version: '0.1.0' })
      expect(init.result?.capabilities?.experimental?.['claude/channel']).toEqual({})

      await client1.sendMcp({ jsonrpc: '2.0', method: 'notifications/initialized' })
      await client1.sendMcp({ jsonrpc: '2.0', id: 2, method: 'tools/list' })

      const toolsList = await nextMcpResponse(client1, 2)
      expect(toolsList.error).toBeUndefined()
      expect(Array.isArray(toolsList.result?.tools)).toBe(true)

      const names = new Set((toolsList.result.tools as any[]).map((t) => t.name))
      const coreNames = new Set(core!.listTools(agentId).map((t: any) => t.name))
      for (const name of requiredTools) {
        expect(names.has(name)).toBe(true)
        expect(coreNames.has(name)).toBe(true)
      }
    } finally {
      client1.close()
    }

    expect(await waitFor(async () => !core!.hasSession(agentId) && (await redis!.get(presenceKey)) == null, 15_000)).toBe(true)
    expect(core!.sessionCount()).toBe(0)

    const client2 = await connectClient(acceptor!.socketPath)
    try {
      const ok2 = await completeHello(client2)
      expect(ok2.epoch).toBeGreaterThan(firstEpoch)
      expect(await waitFor(() => core!.hasSession(agentId))).toBe(true)
    } finally {
      client2.close()
    }

    expect(await waitFor(() => !core!.hasSession(agentId), 15_000)).toBe(true)
    expect(core!.sessionCount()).toBe(0)
  }, 45_000)
})
