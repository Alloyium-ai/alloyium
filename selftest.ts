#!/usr/bin/env bun
// Isolated smoke test for the NATS→channel bridge. Uses a throwaway Redis key,
// a temporary self-owned JetStream stream, and benign alloyium.channels.selftest*
// subjects — it NEVER publishes to a production subject. Cleans up after itself.
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const TEST_KEY = 'alloyium:selftest'
const enc = (o: unknown) => new TextEncoder().encode(JSON.stringify(o))

// Point the bridge at our throwaway key BEFORE importing it (env read at import).
process.env.SUBS_KEY = TEST_KEY
process.env.CONTROL_SUBJECT = 'alloyium.channels.selftest.control'

const specs = [
  { subject: 'alloyium.channels.selftest', mode: 'core' },
  { subject: 'alloyium.channels.selftest.js', mode: 'jetstream', stream: 'CLAUDE_SELFTEST', durable: 'claude-selftest-consumer', filter_subject: 'alloyium.channels.selftest.js' },
]

let nc: NatsConnection | undefined
const redis = new RedisClient(REDIS_URL)

async function cleanup() {
  try { await new RedisClient(REDIS_URL).del(TEST_KEY) } catch {}
  try {
    if (nc) { const jsm = await nc.jetstreamManager(); await jsm.streams.delete('CLAUDE_SELFTEST') }
  } catch {}
  try { await nc?.drain() } catch {}
}

try {
  await redis.set(TEST_KEY, JSON.stringify(specs))
  nc = await connect({ servers: NATS_URL, name: 'alloyium-selftest' })
  const jsm = await nc.jetstreamManager()
  try { await jsm.streams.add({ name: 'CLAUDE_SELFTEST', subjects: ['alloyium.channels.selftest.js'] }) } catch {}

  const received: Array<{ subject: string; mode: string; content: string }> = []
  const { NatsChannel } = await import('./nats-channel.ts')
  const ch = new NatsChannel(async (content, attrs) => {
    received.push({ subject: attrs.subject, mode: attrs.mode, content })
    console.log(`  ↳ INJECT feed=${attrs.feed} subject=${attrs.subject} mode=${attrs.mode} :: ${content}`)
  })
  await ch.start()
  await Bun.sleep(600) // let subscriptions attach

  console.log('publishing test messages...')
  nc.publish('alloyium.channels.selftest', enc({ kind: 'CORE_TEST', hello: 'core-nats' }))
  await nc.jetstream().publish('alloyium.channels.selftest.js', enc({ kind: 'JS_TEST', hello: 'jetstream' }))
  await Bun.sleep(1200)

  const gotCore = received.some((r) => r.subject === 'alloyium.channels.selftest' && r.mode === 'core')
  const gotJs = received.some((r) => r.subject === 'alloyium.channels.selftest.js' && r.mode === 'jetstream')
  console.log(`\nresult: core=${gotCore ? 'OK' : 'FAIL'}  jetstream=${gotJs ? 'OK' : 'FAIL'}  (received ${received.length})`)

  await cleanup()
  process.exit(gotCore && gotJs ? 0 : 1)
} catch (e) {
  console.error('selftest error:', e)
  await cleanup()
  process.exit(2)
}
