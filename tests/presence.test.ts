import { RedisClient } from 'bun'
import { afterAll, describe, expect, test } from 'bun:test'
import { randomUUID } from 'node:crypto'
import { PresenceClaimer } from '../presence.ts'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const PRESENCE_PREFIX = 'alloyium:a2a:presence:'
const TEST_HOST = 'presence-test-host'

const redis = new RedisClient(REDIS_URL)
const cleanupKeys = new Set<string>()

function keyFor(agentId: string): string {
  return `${PRESENCE_PREFIX}${agentId}`
}

function makeAgentId(): string {
  const agentId = `presence-test-${randomUUID()}`
  cleanupKeys.add(keyFor(agentId))
  return agentId
}

async function delKey(agentId: string): Promise<void> {
  await redis.send('DEL', [keyFor(agentId)])
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

afterAll(async () => {
  try {
    if (cleanupKeys.size > 0) {
      await redis.send('DEL', [...cleanupKeys])
    }
  } finally {
    redis.close()
  }
})

describe('PresenceClaimer', () => {
  test('claims presence with detector-compatible JSON value', async () => {
    const agentId = makeAgentId()
    await delKey(agentId)

    const claimer = new PresenceClaimer(redis, { agentId, host: TEST_HOST })
    try {
      expect(await claimer.start()).toBe('ok')

      const raw = await redis.get(keyFor(agentId))
      expect(raw).not.toBeNull()

      const value = JSON.parse(raw!) as Record<string, unknown>
      expect(Object.keys(value).sort()).toEqual(['host', 'last_seen', 'started_at', 'token'])
      expect(typeof value.token).toBe('string')
      expect(value.host).toBe(TEST_HOST)
      expect(typeof value.started_at).toBe('string')
      expect(typeof value.last_seen).toBe('string')
      expect(value.started_at).toBe(value.last_seen)
    } finally {
      await claimer.stop()
    }
  })

  test('refreshes last_seen on heartbeat before TTL expiry', async () => {
    const agentId = makeAgentId()
    await delKey(agentId)

    const claimer = new PresenceClaimer(redis, {
      agentId,
      host: TEST_HOST,
      heartbeatMs: 1000,
      ttlS: 5,
    })

    try {
      expect(await claimer.start()).toBe('ok')

      const claimedRaw = await redis.get(keyFor(agentId))
      expect(claimedRaw).not.toBeNull()
      const claimed = JSON.parse(claimedRaw!) as { started_at: string; last_seen: string }

      await sleep(1300)

      const heartbeatRaw = await redis.get(keyFor(agentId))
      expect(heartbeatRaw).not.toBeNull()
      const heartbeat = JSON.parse(heartbeatRaw!) as { started_at: string; last_seen: string }

      expect(heartbeat.started_at).toBe(claimed.started_at)
      expect(Date.parse(heartbeat.last_seen)).toBeGreaterThan(Date.parse(claimed.last_seen))

      const ttl = Number(await redis.send('TTL', [keyFor(agentId)]))
      expect(ttl).toBeGreaterThan(0)
    } finally {
      await claimer.stop()
    }
  })

  test('releases presence on stop', async () => {
    const agentId = makeAgentId()
    await delKey(agentId)

    const claimer = new PresenceClaimer(redis, { agentId, host: TEST_HOST })

    expect(await claimer.start()).toBe('ok')
    await claimer.stop()

    expect(await redis.get(keyFor(agentId))).toBeNull()
  })

  test('guards duplicate claim and foreign-token release', async () => {
    const agentId = makeAgentId()
    await delKey(agentId)

    const claimerA = new PresenceClaimer(redis, { agentId, host: TEST_HOST })
    const claimerB = new PresenceClaimer(redis, { agentId, host: TEST_HOST })

    try {
      expect(await claimerA.start()).toBe('ok')
      expect(await claimerB.start()).toBe('dup')

      await claimerB.stop()
      expect(await redis.get(keyFor(agentId))).not.toBeNull()

      await claimerA.stop()
      expect(await redis.get(keyFor(agentId))).toBeNull()
    } finally {
      await claimerB.stop()
      await claimerA.stop()
    }
  })

  test('rejects heartbeat intervals that cannot refresh before TTL expiry', () => {
    const agentId = makeAgentId()

    expect(() => new PresenceClaimer(redis, {
      agentId,
      heartbeatMs: 10_000,
      ttlS: 5,
    })).toThrow(/heartbeatMs/)
  })

  test('retries claim until owned after a stale predecessor key expires (folds Opus P0)', async () => {
    const agentId = makeAgentId()
    const key = keyFor(agentId)
    // simulate a crashed predecessor: a FOREIGN-token record still present with a short TTL (no clean release)
    await redis.send('SET', [key, JSON.stringify({ token: `predecessor-${randomUUID()}`, host: 'predecessor-host', started_at: new Date().toISOString(), last_seen: new Date().toISOString() }), 'EX', '2'])
    const claimer = new PresenceClaimer(redis, { agentId, host: TEST_HOST, heartbeatMs: 1000, ttlS: 5 })
    try {
      expect(await claimer.start()).toBe('dup')      // predecessor still holds it
      expect(claimer.isOwned()).toBe(false)
      await sleep(3600)                              // predecessor's 2s TTL expires, then a maintain tick re-claims
      expect(claimer.isOwned()).toBe(true)           // CONVERGED to ownership — not presence-less forever
      const raw = await redis.get(key)
      expect(raw).not.toBeNull()
      expect(JSON.parse(raw!).host).toBe(TEST_HOST)  // OUR record replaced the predecessor's
    } finally {
      await claimer.stop()
    }
  }, 15_000)

  test('isOwned() reflects claim/stop lifecycle (gates the core self-beat)', async () => {
    const agentId = makeAgentId()
    await delKey(agentId)
    const claimer = new PresenceClaimer(redis, { agentId, host: TEST_HOST })
    expect(claimer.isOwned()).toBe(false)            // before start
    expect(await claimer.start()).toBe('ok')
    expect(claimer.isOwned()).toBe(true)             // owned after a clean claim
    await claimer.stop()
    expect(claimer.isOwned()).toBe(false)            // released
  })

  test('heartbeat failure is TTL-aware: keeps ownership through a transient blip, drops it past ttlS (folds GPT-5.5 P1)', async () => {
    // Fake Redis: the initial claim SET succeeds, then it goes "down" → every op throws (Redis unreachable).
    // An injected clock drives the staleness logic deterministically (the real maintain timer just triggers ticks).
    const fake: any = {
      down: false,
      async send(cmd: string) { if (this.down) throw new Error('redis_down'); return cmd === 'SET' ? 'OK' : null },
      async get() { if (this.down) throw new Error('redis_down'); return null },
    }
    let clock = 1_700_000_000_000
    const claimer = new PresenceClaimer(fake, { agentId: `ttl-aware-${randomUUID()}`, host: TEST_HOST, ttlS: 5, heartbeatMs: 1000, now: () => clock })
    try {
      expect(await claimer.start()).toBe('ok')         // claim wins; lastOwnedAt = clock(T0)
      expect(claimer.isOwned()).toBe(true)
      fake.down = true                                  // Redis becomes unreachable
      clock += 2000                                     // < ttlS since claim → a TRANSIENT blip
      await sleep(1200)                                 // a maintain tick fires the (throwing) heartbeat
      expect(claimer.isOwned()).toBe(true)              // ownership KEPT (beats continue, no false-wedge)
      clock += 4000                                     // total +6s > ttlS(5s) → key has certainly expired
      await sleep(1200)                                 // next tick: heartbeat throws → staleness → fail closed
      expect(claimer.isOwned()).toBe(false)             // dropped ownership BEFORE another core could also emit
    } finally {
      await claimer.stop()
    }
  }, 15_000)
})
