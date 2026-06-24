// a2a-core UDS session tests — live NATS + Redis, isolated by prefix/stream.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { A2ACore, type UdsSessionWiring } from '../a2a_core.ts'
import { UdsServerTransport, makeUdsInject, makeUdsSign, type SignReply } from '../uds_transport.ts'
import { PendingMap } from '../uds_frame.ts'
import { requireBus } from './_require_bus.ts'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'

let available = true
let probeNats: NatsConnection | undefined
let probeRedis: RedisClient | undefined
try {
  probeNats = await connect({ servers: NATS_URL, name: 'a2a-core-uds-test-probe' })
  probeRedis = new RedisClient(REDIS_URL)
  await probeRedis.set('alloyium:a2a:coreudstest:probe', '1')
  await probeRedis.del('alloyium:a2a:coreudstest:probe')
} catch { available = false }

requireBus(available, 'a2a-core-uds', { NATS_URL, REDIS_URL })

let uid = Math.floor(Math.random() * 1e6)
const cores: A2ACore[] = []
const streams: string[] = []
const redisKeys: string[] = []

type Fleet = { prefix: string; stream: string }

class FakeWriter {
  ctrlFrames: unknown[] = []
  mcpFrames: unknown[] = []
  rawFrames: unknown[] = []
  closed = false

  async writeCtrl(frame: unknown): Promise<void> { this.ctrlFrames.push(frame) }
  async enqueueCtrl(frame: unknown): Promise<void> { await this.writeCtrl(frame) }
  async sendCtrl(frame: unknown): Promise<void> { await this.writeCtrl(frame) }

  async writeMcp(frame: unknown): Promise<void> { this.mcpFrames.push(frame) }
  async enqueueMcp(frame: unknown): Promise<void> { await this.writeMcp(frame) }
  async sendMcp(frame: unknown): Promise<void> { await this.writeMcp(frame) }

  async writeFrame(frame: unknown): Promise<void> { this.record(frame) }
  async enqueue(frame: unknown): Promise<void> { this.record(frame) }
  async send(frame: unknown): Promise<void> { this.record(frame) }
  write(frame: unknown): true { this.record(frame); return true }

  async flush(): Promise<void> {}
  async drain(): Promise<void> {}
  async close(): Promise<void> { this.closed = true }

  private record(frame: any): void {
    const tag = String(frame?.kind ?? frame?.type ?? frame?.channel ?? '')
    if (tag === 'mcp') this.mcpFrames.push(frame)
    else if (tag === 'ctrl' || tag === 'control') this.ctrlFrames.push(frame)
    else this.rawFrames.push(frame)
  }
}

function newFleet(): Fleet {
  uid++
  const f = { prefix: `alloyium.a2a.cu${uid}.`, stream: `ALLOYIUM_A2A_CU_${uid}` }
  streams.push(f.stream)
  return f
}

function newCore(fleet: Fleet): A2ACore {
  const core = new A2ACore({
    devNoAuth: true,
    natsUrl: NATS_URL,
    redisUrl: REDIS_URL,
    prefix: fleet.prefix,
    stream: fleet.stream,
    sessionDefaults: { devNoAuth: true },
  })
  cores.push(core)
  return core
}

function trackAgent(agentId: string): string {
  redisKeys.push(`alloyium:a2a:presence:${agentId}`)
  return agentId
}

function makeWiring(epoch: number): { fakeWriter: FakeWriter; wiring: UdsSessionWiring } {
  const fakeWriter = new FakeWriter()
  const transport = new UdsServerTransport(fakeWriter as any)
  return {
    fakeWriter,
    wiring: {
      epoch,
      transport: transport as UdsSessionWiring['transport'],
      ctxInject: makeUdsInject(fakeWriter as any),
      externalSign: makeUdsSign({ writer: fakeWriter as any, pending: new PendingMap<number, SignReply>(), timeoutMs: 1000 }),
    },
  }
}

afterAll(async () => {
  for (const core of cores) { try { await core.stop() } catch {} }
  if (available) {
    try {
      const jsm = await probeNats!.jetstreamManager()
      for (const stream of streams) { try { await jsm.streams.delete(stream) } catch {} }
    } catch {}
    for (const key of redisKeys) { try { await probeRedis!.del(key) } catch {} }
    try { await probeNats?.drain() } catch {}
    try { (probeRedis as any)?.close?.() } catch {}
  }
})

describe.skipIf(!available)('A2ACore UDS sessions', () => {
  test('addUdsSession stands up a session', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-add-${uid}`)
    const { wiring } = makeWiring(1)
    expect(await core.addUdsSession(agentId, wiring)).toMatchObject({ ok: true, agentId, epoch: 1 })
    expect(core.sessionCount()).toBe(1)
    expect(core.hasSession(agentId)).toBe(true)
  }, 35_000)

  test('UDS session exposes the a2a + brain + kai + vault tool surface', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-tools-${uid}`)
    const { wiring } = makeWiring(1)
    expect(await core.addUdsSession(agentId, wiring)).toMatchObject({ ok: true, agentId, epoch: 1 })

    const names = core.listTools(agentId).map((t: any) => t.name)
    expect(names).toEqual(expect.arrayContaining(['a2a_send', 'a2a_peers', 'a2a_remember', 'a2a_recall', 'kai_send', 'vault_howto']))
  }, 35_000)

  test('removeSession CAS refuses a stale older epoch', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-cas-${uid}`)
    const { wiring } = makeWiring(2)
    expect(await core.addUdsSession(agentId, wiring)).toMatchObject({ ok: true, agentId, epoch: 2 })

    expect(await core.removeSession(agentId, 1)).toBe(false)
    expect(core.hasSession(agentId)).toBe(true)
    expect(await core.removeSession(agentId, 2)).toBe(true)
    expect(core.hasSession(agentId)).toBe(false)
  }, 35_000)

  test('removeSession without epoch still removes a plain addSession session', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-plain-${uid}`)
    expect(await core.addSession(agentId, async () => {})).toMatchObject({ ok: true, agentId })
    expect(core.hasSession(agentId)).toBe(true)

    expect(await core.removeSession(agentId)).toBe(true)
    expect(core.hasSession(agentId)).toBe(false)
  }, 35_000)

  test('duplicate addUdsSession for the same agentId is refused', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-dup-${uid}`)
    expect(await core.addUdsSession(agentId, makeWiring(1).wiring)).toMatchObject({ ok: true, agentId, epoch: 1 })
    expect(await core.addUdsSession(agentId, makeWiring(2).wiring)).toMatchObject({ ok: false, agentId, error: 'session_exists' })
    expect(core.sessionCount()).toBe(1)
  }, 35_000)

  test('multiple toolOnly UDS sessions may share one agent identity', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-toolonly-many-${uid}`)
    await probeRedis!.set(`alloyium:a2a:presence:${agentId}`, JSON.stringify({ last_seen: Date.now() }))
    expect(await core.addUdsSession(agentId, makeWiring(1).wiring, { toolOnly: true })).toMatchObject({ ok: true, agentId, epoch: 1 })
    expect(await core.addUdsSession(agentId, makeWiring(2).wiring, { toolOnly: true })).toMatchObject({ ok: true, agentId, epoch: 2 })
    expect(core.sessionCount()).toBe(2)
    expect(core.hasSession(agentId)).toBe(true)

    const names = core.listTools(agentId).map((t: any) => t.name)
    expect(names).toEqual(expect.arrayContaining(['a2a_send', 'a2a_peers', 'a2a_remember', 'vault_howto']))

    expect(await core.removeSession(agentId, 1)).toBe(true)
    expect(core.sessionCount()).toBe(1)
    expect(core.hasSession(agentId)).toBe(true)
    expect(await core.removeSession(agentId, 2)).toBe(true)
    expect(core.sessionCount()).toBe(0)
    expect(core.hasSession(agentId)).toBe(false)
  }, 35_000)

  test('toolOnly UDS session skips presence ownership and still exposes tools', async () => {
    const fleet = newFleet()
    const core = newCore(fleet)
    await core.start()

    const agentId = trackAgent(`uds-toolonly-${uid}`)
    await probeRedis!.set(`alloyium:a2a:presence:${agentId}`, JSON.stringify({ last_seen: Date.now() }))
    const { wiring } = makeWiring(1)
    expect(await core.addUdsSession(agentId, wiring, { toolOnly: true })).toMatchObject({ ok: true, agentId, epoch: 1 })

    const names = core.listTools(agentId).map((t: any) => t.name)
    expect(names).toEqual(expect.arrayContaining(['a2a_send', 'a2a_peers', 'a2a_remember', 'vault_howto']))
  }, 35_000)
})
