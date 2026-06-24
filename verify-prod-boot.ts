#!/usr/bin/env bun
// One-shot verification that the bridge seeds the PRODUCTION Redis key on boot
// and attaches the Tier-A subscriptions. Read-only (never publishes). Leaves the
// seeded key + durables in place — this is the actual wiring step.
import './preamble.ts'
import { NatsChannel } from './nats-channel.ts'

let n = 0
const ch = new NatsChannel(async (content, attrs) => {
  n++
  const body = content.length > 200 ? content.slice(0, 200) + '…' : content
  console.error(`  ↳ LIVE feed=${attrs.feed} subject=${attrs.subject} mode=${attrs.mode} :: ${body}`)
})

await ch.start()
console.error('--- listening ~12s for live Tier-A alerts (read-only) ---')
await Bun.sleep(12_000)
console.error(`--- received ${n} live message(s) in the window ---`)
await ch.stop()
process.exit(0)
