#!/usr/bin/env bun

import { existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { RedisClient } from 'bun'
import { connect } from 'nats'
import { A2AChannel } from '../a2a-channel.ts'
import { AGENT_PROFILE_FEATURE_TOKENS } from '../agent_profile_refs.ts'
import { onboard } from '../onboard.ts'

type Mode = 'once' | 'serve' | 'status' | 'check-bus'
type ToolResult<T> = { content?: Array<{ text?: string }>; isError?: boolean } & T
type InboundEvent = { body: string; attrs: Record<string, string> }
type DemoAgent = {
  id: string
  role: string
  label: string
  features: string[]
}
type RuntimeAgent = DemoAgent & {
  channel: A2AChannel
  events: InboundEvent[]
}

type DemoConfig = {
  natsUrl: string
  redisUrl: string
  stream: string
  subjectPrefix: string
  stateDir: string
  a2aDir: string
  runDir: string
  topicsKeyPrefix: string
  secretKeyPrefix: string
  pubkeyKeyPrefix: string
  directEncCapKeyPrefix: string
  peerProtocolKeyPrefix: string
  presenceKeyPrefix: string
}

const DEMO_AGENTS: DemoAgent[] = [
  {
    id: 'pm',
    role: 'pm',
    label: 'PM',
    features: ['alloyium.demo.pm.v1', 'alloyium.demo.routing.v1', 'alloyium.demo.status.v1'],
  },
  {
    id: 'engineer',
    role: 'engineer',
    label: 'Engineer',
    features: ['alloyium.demo.team.v1', 'alloyium.demo.implementation.v1'],
  },
  {
    id: 'reviewer',
    role: 'reviewer',
    label: 'Reviewer',
    features: ['alloyium.demo.team.v1', 'alloyium.demo.review.v1'],
  },
  {
    id: 'researcher',
    role: 'researcher',
    label: 'Researcher',
    features: ['alloyium.demo.team.v1', 'alloyium.demo.research.v1'],
  },
  {
    id: 'fusion-panel',
    role: 'fusion',
    label: 'Fusion Panel',
    features: ['alloyium.demo.fusion-panel.v1', 'alloyium.demo.synthesis.v1'],
  },
]

const RESPONSE_BY_AGENT: Record<string, { summary: string; bullets: string[] }> = {
  engineer: {
    summary: 'Build path is Makefile automation plus generated local identities on a signed A2A bus.',
    bullets: [
      'Start Redis and NATS with JetStream.',
      'Register each demo peer pubkey in Redis.',
      'Run request and reply traffic through A2AChannel.',
    ],
  },
  reviewer: {
    summary: 'Review focus is keeping public defaults useful without restoring private topology.',
    bullets: [
      'Generated identities and inbox stores stay under ignored runtime paths.',
      'The demo namespace is isolated from the default local bus.',
      'Peer features are advertised through normal presence and protocol metadata.',
    ],
  },
  researcher: {
    summary: 'Developer onboarding should prove a PM, team, and fusion workflow in one command.',
    bullets: [
      'No hosted control plane is required.',
      'Local overrides stay in .env.',
      'The same demo can be left running for manual peer experiments.',
    ],
  },
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function usage(exitCode = 0): never {
  console.log(`usage: bun scripts/demo-fleet.ts [--once|--serve|--status|--check-bus]

Modes:
  --once    start the demo fleet, run one workflow, then stop
  --serve   start the demo fleet, run one workflow, and keep peers alive
  --status  print live demo peers from Redis presence
  --check-bus  verify Redis and NATS are reachable, then exit`)
  process.exit(exitCode)
}

function parseMode(): Mode {
  const args = process.argv.slice(2)
  if (args.includes('--help') || args.includes('-h')) usage(0)
  const selected = args.filter((arg) => ['--once', '--serve', '--status', '--check-bus'].includes(arg))
  if (selected.length > 1) usage(2)
  if (selected[0] === '--serve') return 'serve'
  if (selected[0] === '--status') return 'status'
  if (selected[0] === '--check-bus') return 'check-bus'
  return 'once'
}

function env(name: string, fallback: string): string {
  const value = process.env[name]
  return value && value.trim() ? value.trim() : fallback
}

function buildConfig(): DemoConfig {
  const baseKeyPrefix = env('ALLOYIUM_DEMO_KEY_PREFIX', 'alloyium:demo:a2a:').replace(/:+$/, '') + ':'
  const stateDir = resolve(env('ALLOYIUM_DEMO_STATE_DIR', '.alloyium/demo'))
  return {
    natsUrl: env('NATS_URL', 'nats://127.0.0.1:4222'),
    redisUrl: env('REDIS_URL', 'redis://127.0.0.1:6379'),
    stream: env('ALLOYIUM_DEMO_STREAM', 'ALLOYIUM_A2A_DEMO'),
    subjectPrefix: env('ALLOYIUM_DEMO_SUBJECT_PREFIX', 'alloyium.a2a.demo.'),
    stateDir,
    a2aDir: join(stateDir, 'a2a'),
    runDir: join(stateDir, 'run'),
    topicsKeyPrefix: `${baseKeyPrefix}topics:`,
    secretKeyPrefix: `${baseKeyPrefix}secret:`,
    pubkeyKeyPrefix: `${baseKeyPrefix}pubkey:`,
    directEncCapKeyPrefix: `${baseKeyPrefix}direct-enc:`,
    peerProtocolKeyPrefix: `${baseKeyPrefix}peer-protocol:`,
    presenceKeyPrefix: `${baseKeyPrefix}presence:`,
  }
}

async function waitForBus(config: DemoConfig, timeoutMs = 12_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastError = ''
  while (Date.now() < deadline) {
    let redis: RedisClient | null = null
    try {
      redis = new RedisClient(config.redisUrl)
      await redis.send('PING', [])
      const nc = await connect({ servers: config.natsUrl, name: 'alloyium-demo-probe', timeout: 1000 } as any)
      await nc.drain()
      return
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error)
      await sleep(250)
    } finally {
      try { (redis as any)?.close?.() } catch {}
    }
  }
  throw new Error(`local bus did not become ready: ${lastError}`)
}

function parseToolText<T>(result: ToolResult<T>): T {
  const text = result?.content?.[0]?.text
  if (!text) throw new Error('tool returned no JSON text')
  return JSON.parse(text) as T
}

function tryJson(value: string): any {
  try { return JSON.parse(value) } catch { return null }
}

async function ensureAgentMaterial(config: DemoConfig): Promise<void> {
  mkdirSync(config.a2aDir, { recursive: true })
  mkdirSync(config.runDir, { recursive: true })
  process.env.A2A_PUBKEY_KEY_PREFIX = config.pubkeyKeyPrefix
  const redis = new RedisClient(config.redisUrl)
  try {
    for (const agent of DEMO_AGENTS) {
      const missingLocalKeys = !existsSync(join(config.a2aDir, `${agent.id}.seed`)) || !existsSync(join(config.a2aDir, `${agent.id}.pub`))
      await onboard({
        id: agent.id,
        dir: config.a2aDir,
        redis,
        force: process.env.ALLOYIUM_DEMO_FORCE_KEYS === '1' || missingLocalKeys,
        verify: true,
        natsUrl: config.natsUrl,
        redisUrl: config.redisUrl,
        stream: config.stream,
        subjectPrefix: config.subjectPrefix,
        transport: 'none',
      })
    }
  } finally {
    try { (redis as any).close?.() } catch {}
  }
}

function responseFor(agent: RuntimeAgent, body: string): Record<string, unknown> {
  const payload = tryJson(body)
  if (agent.id === 'fusion-panel') {
    const inputs = Array.isArray(payload?.inputs) ? payload.inputs : []
    return {
      schema: 'alloyium.demo.reply.v1',
      agent: agent.id,
      role: agent.role,
      summary: 'Fusion panel combined the PM route and team replies into a startup checklist.',
      bullets: [
        'Local bus is reachable.',
        'PM, team, and fusion peers are live with signed A2A request/reply.',
        'Feature metadata is visible through the normal peer list.',
      ],
      inputs_seen: inputs.length,
    }
  }
  if (agent.id === 'pm') {
    return {
      schema: 'alloyium.demo.reply.v1',
      agent: agent.id,
      role: agent.role,
      summary: 'PM is online and can route work to the local demo team.',
      bullets: ['Ask the PM to route a task, or address a team peer directly.'],
    }
  }
  const response = RESPONSE_BY_AGENT[agent.id] ?? {
    summary: `${agent.label} handled the request.`,
    bullets: ['Demo peer received and replied over A2A.'],
  }
  return {
    schema: 'alloyium.demo.reply.v1',
    agent: agent.id,
    role: agent.role,
    request: payload?.task ?? payload?.ask ?? body,
    ...response,
  }
}

async function handleInbound(agent: RuntimeAgent, body: string, attrs: Record<string, string>): Promise<void> {
  agent.events.push({ body, attrs })
  const bodyLine = body.length > 120 ? `${body.slice(0, 117)}...` : body
  console.log(`[${agent.id}] <- ${attrs.from} ${attrs.type}${attrs.corr ? ` corr=${attrs.corr}` : ''}: ${bodyLine}`)
  if (attrs.type !== 'request' || !attrs.from || !attrs.id) return
  const replyBody = JSON.stringify(responseFor(agent, body))
  const result = await agent.channel.callTool('a2a_send', {
    to: attrs.from,
    type: 'reply',
    corr: attrs.id,
    body: replyBody,
    direct_encryption: 'off',
  })
  const parsed = parseToolText<{ ok: boolean; error?: string }>(result)
  if (!parsed.ok) throw new Error(`${agent.id} failed to reply: ${parsed.error ?? 'unknown_error'}`)
}

async function startFleet(config: DemoConfig): Promise<Map<string, RuntimeAgent>> {
  const runtimes = new Map<string, RuntimeAgent>()
  for (const definition of DEMO_AGENTS) {
    const runtime: RuntimeAgent = {
      ...definition,
      events: [],
      channel: null as unknown as A2AChannel,
    }
    runtime.channel = new A2AChannel(
      (body, attrs) => handleInbound(runtime, body, attrs),
      {
        enabled: true,
        agentId: definition.id,
        sigAlg: 'ed25519',
        signingKeyPath: join(config.a2aDir, `${definition.id}.seed`),
        transportAuth: 'none',
        natsUrl: config.natsUrl,
        redisUrl: config.redisUrl,
        stream: config.stream,
        prefix: config.subjectPrefix,
        topicsKeyPrefix: config.topicsKeyPrefix,
        secretKeyPrefix: config.secretKeyPrefix,
        pubkeyKeyPrefix: config.pubkeyKeyPrefix,
        directEncCapKeyPrefix: config.directEncCapKeyPrefix,
        peerProtocolKeyPrefix: config.peerProtocolKeyPrefix,
        presenceKeyPrefix: config.presenceKeyPrefix,
        inboxDbPath: join(config.runDir, `${definition.id}-inbox.sqlite3`),
        profilePresence: {
          features: [...AGENT_PROFILE_FEATURE_TOKENS],
          profile: {
            role: definition.role,
            profile_id: definition.id,
            source: 'demo-fleet',
          },
        },
        app: { name: 'alloyium-demo-fleet', version: '0.1.0' },
        runtimeKind: 'alloyium-demo-agent',
        mcpPath: 'local-demo',
        featureTokens: definition.features,
        directEncryption: 'off',
        presenceTtlS: 30,
        heartbeatMs: 5_000,
        ratePerMin: 120,
        ratePerPeerPerMin: 60,
        maxSendBytes: 16_384,
      },
    )
    await runtime.channel.start()
    if (!runtime.channel.isStarted()) {
      throw new Error(`agent ${definition.id} did not start; check for duplicate live demo peers`)
    }
    runtimes.set(definition.id, runtime)
  }
  await sleep(500)
  return runtimes
}

async function sendRequest(from: RuntimeAgent, to: string, body: Record<string, unknown>): Promise<string> {
  const result = await from.channel.callTool('a2a_send', {
    to,
    type: 'request',
    body: JSON.stringify(body),
    direct_encryption: 'off',
  })
  const parsed = parseToolText<{ ok: boolean; id?: string; error?: string; detail?: string }>(result)
  if (!parsed.ok || !parsed.id) {
    throw new Error(`send to ${to} failed: ${parsed.error ?? 'unknown_error'}${parsed.detail ? `: ${parsed.detail}` : ''}`)
  }
  return parsed.id
}

async function waitForReplies(agent: RuntimeAgent, corrIds: string[], timeoutMs: number): Promise<InboundEvent[]> {
  const wanted = new Set(corrIds)
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const found = new Map<string, InboundEvent>()
    for (const event of agent.events) {
      if (event.attrs.type === 'reply' && event.attrs.corr && wanted.has(event.attrs.corr)) {
        found.set(event.attrs.corr, event)
      }
    }
    if (found.size === wanted.size) return corrIds.map((id) => found.get(id)!)
    await sleep(100)
  }
  throw new Error(`timed out waiting for ${corrIds.length} replies`)
}

async function listPeers(agent: RuntimeAgent): Promise<Array<Record<string, unknown>>> {
  const result = await agent.channel.callTool('a2a_peers', {})
  const parsed = parseToolText<{ ok: boolean; peers?: Array<Record<string, unknown>>; error?: string }>(result)
  if (!parsed.ok) throw new Error(`a2a_peers failed: ${parsed.error ?? 'unknown_error'}`)
  return parsed.peers ?? []
}

function featureSummary(value: unknown): string {
  const features = Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
  const highlighted = features.filter((feature) => feature.startsWith('alloyium.demo.') || feature.startsWith('a2a.profile.'))
  return (highlighted.length ? highlighted : features).slice(0, 8).join(', ')
}

function printPeerTable(peers: Array<Record<string, unknown>>): void {
  const rows = peers
    .map((peer) => {
      const profile = peer.profile && typeof peer.profile === 'object' ? peer.profile as Record<string, unknown> : {}
      return {
        id: String(peer.id ?? ''),
        role: String(profile.role ?? ''),
        runtime: String((peer.runtime as any)?.kind ?? ''),
        features: featureSummary(peer.features),
      }
    })
    .sort((a, b) => a.id.localeCompare(b.id))
  const header = ['peer', 'role', 'runtime', 'features']
  const data = rows.map((row) => [row.id, row.role, row.runtime, row.features])
  const widths = header.map((name, index) => Math.max(name.length, ...data.map((row) => row[index].length)))
  const line = (columns: string[]) => columns.map((column, index) => column.padEnd(widths[index])).join('  ')
  console.log(line(header))
  console.log(line(widths.map((width) => '-'.repeat(width))))
  for (const row of data) console.log(line(row))
}

async function runWorkflow(runtimes: Map<string, RuntimeAgent>): Promise<void> {
  const pm = runtimes.get('pm')
  if (!pm) throw new Error('pm runtime missing')
  console.log('\nDemo workflow: PM asks the team for a local startup plan.')
  const teamIds = ['engineer', 'reviewer', 'researcher']
  const requestIds: string[] = []
  for (const teamId of teamIds) {
    const id = await sendRequest(pm, teamId, {
      schema: 'alloyium.demo.request.v1',
      task: 'Validate a quick local Alloyium fleet startup path.',
      assigned_to: teamId,
    })
    requestIds.push(id)
  }
  const teamReplies = await waitForReplies(pm, requestIds, 8_000)
  console.log('\nTeam replies:')
  for (const reply of teamReplies) {
    const parsed = tryJson(reply.body)
    console.log(`- ${reply.attrs.from}: ${parsed?.summary ?? reply.body}`)
  }

  const fusionRequestId = await sendRequest(pm, 'fusion-panel', {
    schema: 'alloyium.demo.fusion-request.v1',
    task: 'Synthesize the team replies into a startup status.',
    inputs: teamReplies.map((reply) => tryJson(reply.body) ?? reply.body),
  })
  const [fusionReply] = await waitForReplies(pm, [fusionRequestId], 8_000)
  const fusionBody = tryJson(fusionReply.body)
  console.log(`\nFusion panel: ${fusionBody?.summary ?? fusionReply.body}`)
  if (Array.isArray(fusionBody?.bullets)) {
    for (const bullet of fusionBody.bullets) console.log(`- ${bullet}`)
  }

  console.log('\nLive peer feature matrix:')
  printPeerTable(await listPeers(pm))
}

async function status(config: DemoConfig): Promise<void> {
  const redis = new RedisClient(config.redisUrl)
  try {
    const peers: Array<Record<string, unknown>> = []
    let cursor = '0'
    do {
      const result: any = await redis.send('SCAN', [cursor, 'MATCH', `${config.presenceKeyPrefix}*`, 'COUNT', '200'])
      cursor = String(result[0])
      for (const key of (result[1] ?? []) as string[]) {
        const raw = await redis.get(key)
        if (!raw) continue
        try {
          const presence = JSON.parse(raw)
          peers.push({
            id: key.slice(config.presenceKeyPrefix.length),
            runtime: presence.runtime,
            profile: presence.profile,
            features: presence.features,
          })
        } catch {}
      }
    } while (cursor !== '0')
    if (peers.length === 0) {
      console.log('no live demo peers found')
      return
    }
    printPeerTable(peers)
  } finally {
    try { (redis as any).close?.() } catch {}
  }
}

async function stopFleet(runtimes: Map<string, RuntimeAgent>): Promise<void> {
  for (const runtime of [...runtimes.values()].reverse()) {
    try { await runtime.channel.stop() } catch (error) {
      console.error(`failed to stop ${runtime.id}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
}

async function main(): Promise<void> {
  const mode = parseMode()
  const config = buildConfig()
  if (mode === 'check-bus') {
    await waitForBus(config)
    console.log('local bus ready')
    return
  }
  if (mode === 'status') {
    await status(config)
    return
  }

  await waitForBus(config)
  await ensureAgentMaterial(config)
  const runtimes = await startFleet(config)
  let stopping = false
  const shutdown = async () => {
    if (stopping) return
    stopping = true
    await stopFleet(runtimes)
  }
  process.once('SIGINT', () => { void shutdown().finally(() => process.exit(0)) })
  process.once('SIGTERM', () => { void shutdown().finally(() => process.exit(0)) })

  try {
    console.log(`Demo fleet started on ${config.subjectPrefix} (${config.stream}).`)
    await runWorkflow(runtimes)
    if (mode === 'serve') {
      console.log('\nFleet is running. Press Ctrl-C to stop.')
      await new Promise(() => {})
    }
  } finally {
    if (mode === 'once') await shutdown()
  }
}

main().catch((error) => {
  console.error(`demo fleet failed: ${error instanceof Error ? error.message : String(error)}`)
  process.exit(1)
})
