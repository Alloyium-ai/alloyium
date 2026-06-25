#!/usr/bin/env bun
// a2a-portal — a live Slack/Discord-style web view of the A2A bus.
//
// Captures every message on alloyium.a2a.> (topics + agent inboxes), tracks presence
// from Redis, and serves a single-page UI: channels in the sidebar, click one to
// read the agents' messages, live-updated over SSE. LAN-bound so a laptop can hit it.
//
//   bun a2a_portal.ts            # serves on 0.0.0.0:8900 (override A2A_PORTAL_PORT)
//
// Monitor + operator send surface. Reads the full A2A namespace directly, and can
// publish through a normal signed A2AChannel when an a2a-portal identity is configured.
import { connect, type NatsConnection } from 'nats'
import { RedisClient } from 'bun'
import { randomUUID } from 'node:crypto'
import { A2AChannel } from './a2a-channel.ts'
import { buildPortalDefaultCwd, buildPortalSendArgs, buildPortalThreadKey, formatPortalRenderedBody, isCodexJobRecipient, isSelfPortalRecipient, wrapPlainCodexRequest, type PortalSendBuildResult } from './a2a_portal_send.ts'
import { PortalLangChainAgent, PORTAL_LANGCHAIN_CHANNEL } from './portal_langchain_agent.ts'
import { resolveRef } from './output_transport.ts'

const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const PORT = Number(process.env.A2A_PORTAL_PORT ?? 8900)
const STORE = process.env.A2A_PORTAL_STORE ?? '/srv/git/alloyium/logs/a2a_portal.jsonl'
const PORTAL_AGENT_ID = process.env.A2A_PORTAL_AGENT_ID ?? process.env.A2A_AGENT_ID ?? 'a2a-portal'
const SEND_ENABLED = (process.env.A2A_PORTAL_SEND_ENABLED ?? '0') === '1' || process.env.A2A_PORTAL_SEND_ENABLED?.toLowerCase() === 'true'
const HOST_OPS_CHAT_CWD = process.env.A2A_PORTAL_HOST_OPS_CHAT_CWD ?? '/srv/git/alloyium'
const REMOTE_HOST_OPS_CHAT_CWD = process.env.A2A_PORTAL_REMOTE_HOST_OPS_CHAT_CWD ?? '/srv/remote/alloyium'
const CODEX_CHAT_CWD = process.env.A2A_PORTAL_CODEX_CHAT_CWD ?? '/app'
const ONE_OFF_CWD = process.env.A2A_PORTAL_ONE_OFF_CWD ?? '/tmp'
const INTERACTIVE_ACK_TIMEOUT_MS = envMs('A2A_PORTAL_INTERACTIVE_ACK_TIMEOUT_MS', 120_000)
const INTERACTIVE_ACK_POLL_MS = envMs('A2A_PORTAL_INTERACTIVE_ACK_POLL_MS', 250)
const INTERACTIVE_REPLY_SETTLE_MS = envMs('A2A_PORTAL_INTERACTIVE_REPLY_SETTLE_MS', 15_000)
const INTERACTIVE_TIMEOUT_SETTLE_MS = envMs('A2A_PORTAL_INTERACTIVE_TIMEOUT_SETTLE_MS', 30_000)
const CAP = 800 // messages kept per channel in memory

type ChannelKind = 'topic' | 'dm' | 'local'
type Msg = { id: string; channel: string; kind: ChannelKind; from: string; to: string; type: string; thread?: string; corr?: string; ts: string; body: string; t: number }
type MsgView = Msg & { rendered_body?: string }
const channels = new Map<string, { name: string; kind: ChannelKind; msgs: Msg[]; lastT: number; lastFrom: string }>()
const sse = new Set<ReadableStreamDefaultController>()
const dec = new TextDecoder()
const enc = new TextEncoder()
const interactiveSendQueues = new Map<string, Promise<void>>()

function envMs(name: string, fallback: number): number {
  const n = Number(process.env[name])
  return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : fallback
}

function channelOf(subject: string): { name: string; kind: 'topic' | 'dm' } | null {
  // alloyium.a2a.topic.<name>  |  alloyium.a2a.agent.<id>.inbox
  const m1 = subject.match(/^alloyium\.a2a\.topic\.([a-z0-9-]+)$/)
  if (m1) return { name: m1[1], kind: 'topic' }
  const m2 = subject.match(/^alloyium\.a2a\.agent\.([a-z0-9-]+)\.inbox$/)
  if (m2) return { name: '@' + m2[1], kind: 'dm' }
  return null
}

function record(m: Msg, persist = true) {
  let ch = channels.get(m.channel)
  if (!ch) { ch = { name: m.channel, kind: m.kind, msgs: [], lastT: 0, lastFrom: '' }; channels.set(m.channel, ch) }
  // de-dup by id within the channel (a message can hit both topic + inbox subs)
  if (ch.msgs.length && ch.msgs.some((x) => x.id === m.id)) return
  ch.msgs.push(m); if (ch.msgs.length > CAP) ch.msgs.shift()
  ch.lastT = m.t; ch.lastFrom = m.from
  if (persist) { try { Bun.write(Bun.file(STORE), '', {}); } catch {} }
  for (const c of sse) { try { c.enqueue(enc.encode(`data: ${JSON.stringify({ kind: 'msg', channel: m.channel, message: m })}\n\n`)) } catch {} }
}

function dmMessages(peer: string): Msg[] {
  const name = peer.startsWith('@') ? peer : `@${peer}`
  const peerId = name.slice(1)
  const byId = new Map<string, Msg>()
  for (const m of channels.get(name)?.msgs ?? []) byId.set(m.id, m)
  for (const m of channels.get(`@${PORTAL_AGENT_ID}`)?.msgs ?? []) {
    if ((m.from === peerId && m.to === PORTAL_AGENT_ID) || (m.from === PORTAL_AGENT_ID && m.to === peerId)) byId.set(m.id, m)
  }
  return [...byId.values()].sort((a, b) => a.t - b.t)
}

function listChannels(): Array<{ name: string; kind: ChannelKind; count: number; lastT: number; lastFrom: string }> {
  const out: Array<{ name: string; kind: ChannelKind; count: number; lastT: number; lastFrom: string }> = []
  const peers = new Set<string>()

  const localMessages = langchainAgent?.messages() ?? []
  const localLast = localMessages.at(-1)
  out.push({
    name: PORTAL_LANGCHAIN_CHANNEL,
    kind: 'local',
    count: localMessages.length,
    lastT: localLast?.t ?? 0,
    lastFrom: localLast?.from ?? '',
  })

  for (const c of channels.values()) {
    if (c.kind === 'topic' || c.kind === 'local') {
      out.push({ name: c.name, kind: c.kind, count: c.msgs.length, lastT: c.lastT, lastFrom: c.lastFrom })
      continue
    }

    const rawPeer = c.name.startsWith('@') ? c.name.slice(1) : c.name
    if (rawPeer && rawPeer !== PORTAL_AGENT_ID) peers.add(rawPeer)
    for (const m of c.msgs) {
      if (m.from && m.from !== PORTAL_AGENT_ID && m.to === PORTAL_AGENT_ID) peers.add(m.from)
      if (m.to && m.to !== PORTAL_AGENT_ID && !m.to.startsWith('topic:') && m.from === PORTAL_AGENT_ID) peers.add(m.to)
    }
  }

  for (const peer of peers) {
    const msgs = dmMessages(peer)
    const last = msgs.at(-1)
    out.push({ name: `@${peer}`, kind: 'dm', count: msgs.length, lastT: last?.t ?? 0, lastFrom: last?.from ?? '' })
  }

  return out.sort((a, b) => b.lastT - a.lastT)
}

async function langchainChannelMessages(): Promise<MsgView[]> {
  return viewMessages((langchainAgent?.messages() ?? []) as Msg[])
}

async function readPortalChannel(name: string, limit: number): Promise<Array<{ from: string; to: string; type: string; ts: string; body: string }>> {
  const raw = String(name || '').trim()
  const normalized = raw.startsWith('#') ? raw.slice(1) : raw
  const count = Math.min(50, Math.max(1, Math.floor(limit || 20)))
  let msgs: Msg[] = []
  if (normalized === PORTAL_LANGCHAIN_CHANNEL) msgs = (langchainAgent?.messages() ?? []) as Msg[]
  else if (normalized.startsWith('@')) msgs = dmMessages(normalized)
  else if (channels.has(normalized)) msgs = channels.get(normalized)?.msgs ?? []
  else if (channels.has(`@${normalized}`)) msgs = dmMessages(normalized)
  return msgs.slice(-count).map((m) => ({
    from: m.from,
    to: m.to,
    type: m.type,
    ts: m.ts,
    body: m.body.length > 4000 ? m.body.slice(0, 3984) + '\n[truncated]' : m.body,
  }))
}

async function renderBody(body: string): Promise<string | null> {
  let parsed: any
  try { parsed = JSON.parse(body) } catch { return formatPortalRenderedBody(body) }
  if (parsed?.schema === 'codex.job.completed.v1' && typeof parsed.result_ref === 'string' && parsed.result_ref) {
    let miss = ''
    const output = await resolveRef(redis, parsed, (reason) => { miss = reason })
    parsed = { ...parsed, output }
    if (miss) parsed.blob_error = miss
    else parsed.output_preview = false
    return formatPortalRenderedBody(JSON.stringify(parsed))
  }
  return formatPortalRenderedBody(body)
}

async function viewMessages(msgs: Msg[]): Promise<MsgView[]> {
  return Promise.all(msgs.map(async (m) => {
    const rendered = await renderBody(m.body)
    return rendered == null ? m : { ...m, rendered_body: rendered }
  }))
}

// append-only persistence (best effort)
let storeFd: any = null
async function persistInit() {
  try { await Bun.write(STORE, (await Bun.file(STORE).exists()) ? await Bun.file(STORE).text() : '') } catch {}
}
function append(line: string) { try { require('node:fs').appendFileSync(STORE, line + '\n') } catch {} }

async function loadHistory() {
  try {
    if (!(await Bun.file(STORE).exists())) return
    const lines = (await Bun.file(STORE).text()).split('\n').filter(Boolean).slice(-CAP * 4)
    for (const l of lines) { try { record(JSON.parse(l), false) } catch {} }
    let n = 0; for (const c of channels.values()) n += c.msgs.length
    console.error(`[portal] loaded ${n} messages from history across ${channels.size} channels`)
  } catch {}
}

async function onMessage(subject: string, data: Uint8Array) {
  const ch = channelOf(subject); if (!ch) return
  let env: any; try { env = JSON.parse(dec.decode(data)) } catch { return }
  if (!env || typeof env !== 'object' || !env.id) return
  const m: Msg = {
    id: env.id, channel: ch.name, kind: ch.kind, from: env.from ?? '?', to: env.to ?? '',
    type: env.type ?? 'msg', thread: env.thread, corr: env.corr,
    ts: env.ts ?? new Date().toISOString(), body: typeof env.body === 'string' ? env.body : JSON.stringify(env.body ?? ''),
    t: Date.parse(env.ts ?? '') || Date.now(),
  }
  // de-dup before persisting
  const existing = channels.get(m.channel)
  if (existing && existing.msgs.some((x) => x.id === m.id)) return
  record(m, false)
  append(JSON.stringify(m))
  projectSkillEvent(m)
}

let redis: RedisClient
let sendChannel: A2AChannel | null = null
let sendStatus: { enabled: boolean; agent_id: string; error: string } = { enabled: false, agent_id: PORTAL_AGENT_ID, error: SEND_ENABLED ? 'not_started' : 'disabled' }
let langchainAgent: PortalLangChainAgent | null = null

async function presence(): Promise<any[]> {
  const out: any[] = []
  try {
    const keys: string[] = []
    // bun RedisClient has no scan helper; use SCAN via send
    let cursor = '0'
    do {
      const res: any = await redis.send('SCAN', [cursor, 'MATCH', 'alloyium:a2a:presence:*', 'COUNT', '200'])
      cursor = res[0]; for (const k of res[1]) keys.push(k)
    } while (cursor !== '0')
    for (const k of keys) {
      const id = k.split(':').pop()!
      const ttl = Number(await redis.send('TTL', [k]))
      let host = ''; try { host = JSON.parse((await redis.get(k)) ?? '{}').host ?? '' } catch {}
      out.push({ id, host, ttl })
    }
  } catch {}
  return out.sort((a, b) => a.id.localeCompare(b.id))
}

async function startSendChannel() {
  if (!SEND_ENABLED) return
  try {
    sendChannel = new A2AChannel(async () => {}, {
      enabled: true,
      agentId: PORTAL_AGENT_ID,
      sigAlg: 'ed25519',
      transportAuth: 'none',
      natsUrl: NATS_URL,
      redisUrl: REDIS_URL,
      signingKeyPath: process.env.A2A_SIGNING_KEY,
    })
    await sendChannel.start()
    if (!sendChannel.isStarted()) {
      sendStatus = { enabled: false, agent_id: PORTAL_AGENT_ID, error: 'a2a_channel_not_started' }
      sendChannel = null
      return
    }
    sendStatus = { enabled: true, agent_id: PORTAL_AGENT_ID, error: '' }
    console.error(`[portal] outbound send enabled as '${PORTAL_AGENT_ID}'`)
  } catch (e) {
    sendStatus = { enabled: false, agent_id: PORTAL_AGENT_ID, error: e instanceof Error ? e.message : String(e) }
    sendChannel = null
    console.error('[portal] outbound send disabled', sendStatus.error)
  }
}

type SendOk = Record<string, any>
type DeliveryWait =
  | { status: 'reply'; reply_id: string; waited_ms: number }
  | { status: 'timeout'; timeout_ms: number }
  | { status: 'disabled' }

function shouldSerializeInteractiveSend(built: Extract<PortalSendBuildResult, { ok: true }>): boolean {
  const args = built.args
  return built.sendMode === 'chat' && args.type === 'request' && !args.to.startsWith('topic:') && !isCodexJobRecipient(args.to)
}

async function withInteractiveSendQueue<T>(target: string, fn: () => Promise<T>): Promise<T> {
  if (interactiveSendQueues.has(target)) {
    throw { status: 409, body: { ok: false, error: 'send_busy', detail: `waiting for ${target} to reply or settle` } }
  }
  const run = fn()
  const done = run.then(() => {}, () => {})
  interactiveSendQueues.set(target, done)
  try {
    return await run
  } finally {
    if (interactiveSendQueues.get(target) === done) interactiveSendQueues.delete(target)
  }
}

function abortError(): { status: number; body: { ok: false; error: string; detail: string } } {
  return { status: 499, body: { ok: false, error: 'client_closed', detail: 'send was canceled before it reached the bus' } }
}

async function withAbortableInteractiveSendQueue<T>(target: string, signal: AbortSignal | undefined, fn: () => Promise<T>): Promise<T> {
  return withInteractiveSendQueue(target, async () => {
    if (signal?.aborted) throw abortError()
    return fn()
  })
}

function findPortalReply(fromPeer: string, corr: string, sinceT: number): Msg | null {
  const msgs = channels.get(`@${PORTAL_AGENT_ID}`)?.msgs ?? []
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i]
    if (m.t < sinceT - 1000) break
    if (m.from === fromPeer && m.to === PORTAL_AGENT_ID && m.corr === corr) return m
  }
  return null
}

async function waitForPortalReply(fromPeer: string, corr: string, sinceT: number): Promise<DeliveryWait> {
  if (INTERACTIVE_ACK_TIMEOUT_MS <= 0) return { status: 'disabled' }
  const started = Date.now()
  const deadline = started + INTERACTIVE_ACK_TIMEOUT_MS
  while (Date.now() < deadline) {
    const reply = findPortalReply(fromPeer, corr, sinceT)
    if (reply) return { status: 'reply', reply_id: reply.id, waited_ms: Date.now() - started }
    await Bun.sleep(Math.max(50, INTERACTIVE_ACK_POLL_MS))
  }
  return { status: 'timeout', timeout_ms: INTERACTIVE_ACK_TIMEOUT_MS }
}

async function publishPortalSend(built: Extract<PortalSendBuildResult, { ok: true }>): Promise<SendOk> {
  const threadKey = buildPortalThreadKey(built.args, PORTAL_AGENT_ID, built.sendMode, { chatContext: built.chatContext })
  const cwd = buildPortalDefaultCwd(built.args, built.sendMode, { hostOpsCwd: HOST_OPS_CHAT_CWD, remoteHostOpsCwd: REMOTE_HOST_OPS_CHAT_CWD, codexCwd: CODEX_CHAT_CWD, oneOffCwd: ONE_OFF_CWD })
  const args = wrapPlainCodexRequest(built.args, `portal-codex-${randomUUID()}`, { threadKey, cwd })
  const sentAt = Date.now()
  const res = await sendChannel!.callTool('a2a_send', args)
  let parsed: any = null
  try { parsed = JSON.parse(res?.content?.[0]?.text ?? '') } catch {}
  if (!parsed?.ok) throw { status: 400, body: { ok: false, error: parsed?.error ?? 'send_failed', detail: parsed?.detail } }
  const out = { ok: true, send_mode: built.sendMode, chat_context: built.chatContext, thread_key: threadKey, cwd, ...parsed }
  if (!shouldSerializeInteractiveSend(built)) return out
  const deliveryWait = await waitForPortalReply(built.args.to, parsed.id, sentAt)
  const settleMs = deliveryWait.status === 'reply' ? INTERACTIVE_REPLY_SETTLE_MS : deliveryWait.status === 'timeout' ? INTERACTIVE_TIMEOUT_SETTLE_MS : 0
  if (settleMs > 0) await Bun.sleep(settleMs)
  return { ...out, serialized: true, delivery_wait: deliveryWait, settle_ms: settleMs }
}

// ── skills panel: read-only projection of real skill broadcasts over the bus ────
//
// Real agents broadcast skill.created.v1 on the skills-global plane; the channel
// persists every GLOBAL skill into the shared registry (alloyium:a2a:skills:global).
// The portal reads that registry for the Skills Library and folds each live
// broadcast into an activity feed, pushed to the SPA over the SSE stream.
const SKILLS_REGISTRY_KEY = process.env.A2A_SKILLS_GLOBAL_KEY ?? 'alloyium:a2a:skills:global'
const BRAIN_CAP = 60

type FeedItem = { id: string; kind: 'learned'; plane: string; directive: string; source?: string; t: number }

const brainFeed: FeedItem[] = []

function sseSend(obj: unknown): void {
  const frame = enc.encode(`data: ${JSON.stringify(obj)}\n\n`)
  for (const c of sse) { try { c.enqueue(frame) } catch {} }
}

function feedItem(kind: FeedItem['kind'], plane: string, directive: string, t: number, source?: string): FeedItem {
  return { id: randomUUID(), kind, plane, directive, source, t: t || Date.now() }
}
function pushFeed(item: FeedItem): void {
  brainFeed.unshift(item)
  if (brainFeed.length > BRAIN_CAP) brainFeed.length = BRAIN_CAP
  sseSend({ kind: 'brain', item })
}

async function onSkillCreated(ev: any, t: number): Promise<void> {
  const rec = {
    name: String(ev.name ?? ''),
    source: String(ev.source ?? ev.by ?? ''),
    slug: String(ev.slug ?? ev.name ?? ''),
    summary: String(ev.summary ?? ''),
    tags: Array.isArray(ev.tags) ? ev.tags.map(String) : [],
    scope: String(ev.scope ?? 'tagged'),
    ts: String(ev.ts ?? new Date(t || Date.now()).toISOString()),
  }
  if (!rec.name) return
  sseSend({ kind: 'skill', skill: rec })
  pushFeed(feedItem('learned', rec.tags[0] ?? '', rec.name, t, rec.source))
}

function projectSkillEvent(m: Msg): void {
  if (m.body.indexOf('skill.created') < 0) return
  let body: any
  try { body = JSON.parse(m.body) } catch { return }
  if (!body || typeof body !== 'object') return
  const t = m.t || Date.now()
  try {
    if (body.schema === 'skill.created.v1' && body.name) void onSkillCreated(body, t)
  } catch {}
}

// HGETALL shape-agnostic read (Bun may return a flat [f,v,…] array or a map).
async function hgetallSafe(key: string): Promise<Record<string, string>> {
  let res: any
  try { res = await redis.send('HGETALL', [key]) } catch { return {} }
  const out: Record<string, string> = {}
  if (Array.isArray(res)) {
    for (let i = 0; i + 1 < res.length; i += 2) out[String(res[i])] = String(res[i + 1])
  } else if (res && typeof res === 'object') {
    for (const [k, v] of Object.entries(res)) out[k] = typeof v === 'string' ? v : String(v)
  }
  return out
}

// Skills Library = the shared global skills registry (real agents broadcast there).
async function readSkills(): Promise<any[]> {
  const merged = new Map<string, any>()
  const ingest = (h: Record<string, string>, fallbackScope: string) => {
    for (const [name, raw] of Object.entries(h)) {
      let v: any = {}
      try { v = JSON.parse(raw) } catch {}
      const prev = merged.get(name) ?? {}
      merged.set(name, {
        name,
        source: v.source ?? prev.source ?? '',
        slug: v.slug ?? prev.slug ?? name,
        summary: v.summary ?? prev.summary ?? '',
        tags: Array.isArray(v.tags) ? v.tags : (prev.tags ?? []),
        scope: v.scope ?? prev.scope ?? fallbackScope,
        ts: v.ts ?? prev.ts ?? '',
      })
    }
  }
  ingest(await hgetallSafe(SKILLS_REGISTRY_KEY), 'global')
  return [...merged.values()].sort((a, b) => String(b.ts).localeCompare(String(a.ts)))
}

// ── HTTP ──────────────────────────────────────────────────────────────────────
const json = (o: any, init: ResponseInit = {}) => new Response(JSON.stringify(o), { ...init, headers: { 'content-type': 'application/json', ...(init.headers ?? {}) } })

Bun.serve({
  port: PORT, hostname: '0.0.0.0',
  idleTimeout: 255,
  async fetch(req) {
    const url = new URL(req.url)
    const p = url.pathname
    if (p === '/') return new Response(HTML, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } })
    if (p === '/api/send-status') return json({ send: sendStatus })
    if (p === '/api/send' && req.method === 'POST') {
      if (!sendChannel || !sendStatus.enabled) return json({ ok: false, error: 'send_disabled', detail: sendStatus.error }, { status: 503 })
      let body: unknown
      try { body = await req.json() } catch { return json({ ok: false, error: 'bad_json' }, { status: 400 }) }
      const built = buildPortalSendArgs(body)
      if (!built.ok) return json({ ok: false, error: built.error }, { status: 400 })
      if (isSelfPortalRecipient(built.args.to, PORTAL_AGENT_ID)) return json({ ok: false, error: 'self_send', detail: `choose another agent; this portal sends as ${PORTAL_AGENT_ID}` }, { status: 400 })
      try {
        const out = shouldSerializeInteractiveSend(built)
          ? await withAbortableInteractiveSendQueue(built.args.to, req.signal, () => publishPortalSend(built))
          : await publishPortalSend(built)
        return json(out)
      } catch (e) {
        const err = e as { status?: number; body?: any }
        if (err?.body) return json(err.body, { status: err.status ?? 500 })
        return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, { status: 500 })
      }
    }
    if (p === '/api/langchain/status') {
      return json({ langchain: langchainAgent?.status() ?? { enabled: false, ready: false, channel: PORTAL_LANGCHAIN_CHANNEL, model: '', error: 'not_started', send_ready: false, send_error: 'not_started' } })
    }
    if (p === '/api/langchain/messages') {
      return json({ name: PORTAL_LANGCHAIN_CHANNEL, messages: await langchainChannelMessages() })
    }
    if (p === '/api/langchain/chat' && req.method === 'POST') {
      if (!langchainAgent) return json({ ok: false, error: 'langchain_not_started' }, { status: 503 })
      let body: any
      try { body = await req.json() } catch { return json({ ok: false, error: 'bad_json' }, { status: 400 }) }
      const result = await langchainAgent.chat(body?.body)
      return json(result, { status: result.ok ? 200 : 503 })
    }
    if (p === '/api/channels') {
      return json({ channels: listChannels() })
    }
    if (p === '/api/presence') return json({ presence: await presence() })
    if (p === '/api/skills') return json({ skills: await readSkills(), feed: brainFeed })
    const mc = p.match(/^\/api\/channels\/(.+)$/)
    if (mc) {
      const name = decodeURIComponent(mc[1])
      if (name === PORTAL_LANGCHAIN_CHANNEL) return json({ name, messages: await langchainChannelMessages() })
      const c = channels.get(name)
      return json({ name, messages: c ? await viewMessages(c.msgs) : [] })
    }
    const mdm = p.match(/^\/api\/dm\/([a-z0-9-]+)$/)
    if (mdm) {
      const peer = decodeURIComponent(mdm[1])
      return json({ name: `@${peer}`, peer, self: PORTAL_AGENT_ID, messages: await viewMessages(dmMessages(peer)) })
    }
    if (p === '/api/stream') {
      let controllerRef: ReadableStreamDefaultController | null = null
      let keepalive: ReturnType<typeof setInterval> | null = null
      const stream = new ReadableStream({
        start(controller) {
          controllerRef = controller
          sse.add(controller)
          controller.enqueue(enc.encode(`: connected\n\n`))
          keepalive = setInterval(() => {
            try { controller.enqueue(enc.encode(`: keepalive\n\n`)) } catch {}
          }, 25_000)
        },
        cancel() {
          if (keepalive) clearInterval(keepalive)
          if (controllerRef) sse.delete(controllerRef)
        },
      })
      return new Response(stream, { headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' } })
    }
    return new Response('not found', { status: 404 })
  },
})

const HTML = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>A2A Portal</title><style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0b0d12;--bg2:#12151c;--bg3:#1a1e27;--line:#222733;--tx:#d7dce5;--mut:#7b8494;--acc:#5b8cff;--ok:#36d399;--bad:#ff6b8a;--side-w:310px}
html,body{height:100%;-webkit-text-size-adjust:100%}body{font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--tx);display:flex;height:100vh;overflow:hidden}
.side{width:var(--side-w);flex:0 0 var(--side-w);min-width:240px;max-width:min(520px,45vw);min-height:0;background:var(--bg2);display:flex;flex-direction:column;overflow:hidden}
.resizer{width:7px;flex:0 0 7px;background:var(--bg2);border-left:1px solid var(--line);border-right:1px solid var(--line);cursor:col-resize;touch-action:none}
.resizer:hover,.resizer.dragging{background:var(--bg3);border-left-color:var(--acc)}
body.resizing{cursor:col-resize;user-select:none}
.scrim{display:none}
.brand{padding:14px 16px;font-weight:700;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:8px}
.brand .dot{width:9px;height:9px;border-radius:50%;background:#36d399;box-shadow:0 0 8px #36d399}
.sec{padding:12px 12px 4px;color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.08em}
.list{overflow:auto;flex:1 1 0;min-height:120px}
#presence{flex:0 1 auto;max-height:min(42vh,360px);min-height:88px;overflow:auto;border-bottom:1px solid var(--line)}
.ch{padding:7px 14px;cursor:pointer;display:flex;align-items:center;gap:7px;color:var(--tx);border-left:2px solid transparent}
.ch:hover{background:var(--bg3)}.ch.active{background:var(--bg3);border-left-color:var(--acc)}
.ch .h{color:var(--mut)}.ch .n{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ch .c{background:var(--bg);color:var(--mut);border-radius:9px;padding:0 7px;font-size:11px}
.pr{padding:6px 14px;display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer}
  .pr:hover{background:var(--bg3)}.pr.active{background:var(--bg3)}.pr.self{cursor:default;color:var(--mut)}.pr.self:hover{background:transparent}
.pr .d{width:8px;height:8px;border-radius:50%;background:var(--ok)}.pr .d.off{background:#555}
.pr .agent-name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pr .host{color:var(--mut);font-size:11px;margin-left:auto}
.hostgrp{padding-bottom:4px}
.hosthead{padding:9px 14px 4px;color:#a8b1c1;font-size:12px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.primary-placeholder{padding:5px 14px 3px 30px;color:var(--mut);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pr.placeholder{cursor:default;color:var(--mut)}
.pr.placeholder:hover{background:transparent}
.pr.sub{padding-left:34px;font-size:12px}
.pr.sub .d{width:7px;height:7px}
.main{flex:1;display:flex;flex-direction:column;min-width:0}
.top{padding:12px 18px;border-bottom:1px solid var(--line);background:var(--bg2);display:flex;align-items:center;gap:10px}
.navbtn{display:none}
.top h2{font-size:15px;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.top .meta{color:var(--mut);font-size:12px;white-space:nowrap}
.viewtoggle{margin-left:auto;display:flex;align-items:center;gap:6px;color:var(--mut);font-size:12px;white-space:nowrap;user-select:none}
.viewtoggle input{accent-color:var(--acc)}
.msgs{flex:1;overflow:auto;padding:14px 18px}
.m{display:flex;gap:10px;padding:7px 0;border-top:1px solid #161a22}
.av{width:34px;height:34px;border-radius:8px;flex:0 0 34px;display:flex;align-items:center;justify-content:center;font-weight:700;color:#0b0d12;font-size:13px}
.mb{flex:1;min-width:0}
.mh{display:flex;align-items:baseline;gap:8px}
.mh .who{font-weight:600}.mh .ts{color:var(--mut);font-size:11px}
.badge{font-size:10px;padding:1px 6px;border-radius:6px;text-transform:uppercase;letter-spacing:.04em}
.badge.request{background:#3a2f12;color:#f6c453}.badge.reply{background:#10301f;color:#48d597}.badge.msg{background:#1b2233;color:#7d9bff}
.arrow{color:var(--mut);font-size:11px}
.body{white-space:pre-wrap;word-break:break-word;margin-top:2px;color:#cdd3dd}
.body.code{background:var(--bg2);border:1px solid var(--line);border-radius:7px;padding:8px 10px;font:12px ui-monospace,Menlo,Consolas,monospace;color:#a8b6cc;overflow:auto}
	.body.rendered{font:13px/1.55 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:var(--tx);max-width:980px;white-space:normal;overflow-wrap:anywhere}
	.body.rendered p{margin:0 0 8px}.body.rendered p:last-child{margin-bottom:0}
	.body.rendered h1,.body.rendered h2,.body.rendered h3,.body.rendered h4{margin:10px 0 6px;line-height:1.25;color:#eef2f8}
	.body.rendered h1{font-size:19px}.body.rendered h2{font-size:17px}.body.rendered h3{font-size:15px}.body.rendered h4{font-size:14px}
	.body.rendered ul,.body.rendered ol{margin:4px 0 10px 22px;padding:0}.body.rendered li{margin:2px 0}
	.body.rendered blockquote{margin:6px 0 10px;padding:4px 0 4px 12px;border-left:3px solid var(--line);color:#aeb8c7}
	.body.rendered pre{margin:8px 0 10px;padding:10px 12px;border:1px solid var(--line);border-radius:7px;background:var(--bg2);overflow:auto;white-space:pre;color:#a8b6cc}
	.body.rendered code{font:12px ui-monospace,Menlo,Consolas,monospace;background:var(--bg2);border:1px solid var(--line);border-radius:4px;padding:1px 4px;color:#a8b6cc}.body.rendered pre code{background:transparent;border:0;padding:0}
	.body.rendered a{color:#8fb0ff;text-decoration:none}.body.rendered a:hover{text-decoration:underline}
	.body.rendered table{border-collapse:collapse;margin:8px 0 10px;display:block;max-width:100%;overflow:auto}.body.rendered th,.body.rendered td{border:1px solid var(--line);padding:5px 8px;text-align:left}.body.rendered th{background:var(--bg2)}
.empty{color:var(--mut);text-align:center;margin-top:80px}
.thr{color:var(--mut);font-size:11px}
.composer{border-top:1px solid var(--line);background:var(--bg2);padding:10px 12px;display:grid;grid-template-columns:180px 104px 112px 96px 1fr 80px;gap:8px;align-items:end;flex:0 0 auto}
.composer input,.composer textarea,.composer select{width:100%;background:var(--bg);border:1px solid var(--line);color:var(--tx);border-radius:7px;padding:9px 10px;font:13px ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
.composer textarea{resize:none;min-height:38px;max-height:120px}
.composer button{height:38px;border:0;border-radius:7px;background:var(--acc);color:white;font-weight:700;cursor:pointer}
.composer button.secondary{background:var(--bg);border:1px solid var(--line);color:var(--tx)}
.composer button:disabled{background:#343946;color:var(--mut);cursor:not-allowed}
.sendstate{grid-column:1/-1;color:var(--mut);font-size:11px;min-height:16px}
.sendstate.err{color:var(--bad)}.sendstate.ok{color:var(--ok)}
@media(max-width:760px){
html,body{height:100dvh}body{font-size:15px;height:100dvh}
.side{position:fixed;inset:0 auto 0 0;width:min(86vw,320px);min-width:0;max-width:320px;z-index:20;transform:translateX(-100%);transition:transform .18s ease;box-shadow:18px 0 34px rgba(0,0,0,.3)}
.resizer{display:none}
body.nav-open .side{transform:translateX(0)}
.scrim{display:block;position:fixed;inset:0;background:rgba(0,0,0,.48);opacity:0;pointer-events:none;transition:opacity .18s ease;z-index:15}
body.nav-open .scrim{opacity:1;pointer-events:auto}
.brand{padding:15px 16px}.sec{padding:14px 16px 6px}
#presence{max-height:32dvh;min-height:0;overflow:auto;border-bottom:1px solid var(--line)}
.list{min-height:0}
.ch,.pr{min-height:44px;padding:10px 16px;font-size:14px}.pr .host{font-size:12px}.ch .c{font-size:12px;padding:1px 8px}
.main{width:100%;flex:1 1 auto}
.top{min-height:54px;padding:10px 12px;gap:8px;flex-wrap:wrap;align-content:center}
.navbtn{display:flex;align-items:center;justify-content:center;width:44px;height:44px;flex:0 0 44px;padding:0;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--tx);font:20px/1 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;cursor:pointer}
.top h2{font-size:16px;flex:1 1 0;max-width:100%}.top .meta{font-size:12px;margin-left:auto}.viewtoggle{order:3;width:100%;margin-left:44px;font-size:12px}
.msgs{padding:8px 12px 12px}.m{gap:8px;padding:10px 0}.av{width:30px;height:30px;flex-basis:30px;border-radius:7px;font-size:12px}.mh{gap:6px;line-height:1.3;flex-wrap:wrap}.mh .who{max-width:100%;overflow-wrap:anywhere}.mh .ts{font-size:11px}.arrow,.thr{max-width:100%;overflow-wrap:anywhere}.body{font-size:14px;line-height:1.55;margin-top:6px}.body.code{font-size:12px;max-height:45dvh}.body.rendered{font-size:14px;max-width:none}
.composer{padding:10px 12px calc(10px + env(safe-area-inset-bottom));grid-template-columns:minmax(0,1fr) minmax(0,1fr) 76px 84px;gap:8px;align-items:stretch}
.composer input,.composer textarea{grid-column:1/-1}.composer input,.composer textarea,.composer select{font-size:16px}.composer textarea{min-height:84px}.composer select,.composer button{height:42px}.sendstate{grid-column:1/-1;font-size:12px;overflow-wrap:anywhere}
}
@media(max-width:420px){.top .meta{display:none}.viewtoggle{margin-left:52px}.msgs{padding-left:10px;padding-right:10px}.composer{padding-left:10px;padding-right:10px}}
/* ── tabs + skills/brain panel ─────────────────────────────────── */
.tabs{display:flex;gap:4px}
.tab{background:transparent;border:1px solid var(--line);color:var(--mut);border-radius:7px;padding:6px 12px;font:13px ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;cursor:pointer;white-space:nowrap}
.tab:hover{color:var(--tx);background:var(--bg3)}
.tab.active{color:var(--tx);background:var(--bg3);border-color:var(--acc)}
.tabpanel{display:none;flex:1;min-height:0;overflow:auto}
body:not(.tab-chat) #msgs,body:not(.tab-chat) .composer,body:not(.tab-chat) .chat-only{display:none}
body.tab-brain #brainPanel{display:block;overflow:auto}
.brain-head{display:flex;align-items:baseline;gap:10px;margin-bottom:12px;flex:0 0 auto}
.brain-head h2{font-size:16px}
.panel-sub{color:var(--mut);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.col-empty{color:var(--mut);font-size:12px;text-align:center;padding:18px 0}
#brainPanel{padding:14px 18px}
.brain-grid{display:grid;grid-template-columns:1.5fr 1fr;gap:16px;align-items:start}
.sec2{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;display:flex;align-items:center;gap:8px}
.cnt2{background:var(--bg2);color:var(--mut);border-radius:9px;padding:0 7px;font-size:11px}
.skills{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
.skill{background:var(--bg2);border:1px solid var(--line);border-radius:9px;padding:11px 12px;position:relative;transition:border-color .4s,box-shadow .4s}
.skill.fresh{border-color:var(--ok);box-shadow:0 0 0 1px var(--ok),0 0 14px rgba(54,211,153,.25)}
.skill .nm{font-weight:600;color:var(--tx);margin-bottom:3px;word-break:break-word}
.skill .src{color:var(--mut);font-size:11px;margin-bottom:6px}
.skill .sum{font-size:12px;color:#c3cad6;line-height:1.45;margin-bottom:8px}
.skill .tags{display:flex;flex-wrap:wrap;gap:4px}
.skill .tag{background:var(--bg);color:var(--acc);border:1px solid var(--line);border-radius:5px;padding:0 6px;font-size:10px}
.skill .bcast{position:absolute;top:9px;right:10px;font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:var(--ok)}
.feed{display:flex;flex-direction:column;gap:8px}
.fi{display:flex;gap:9px;padding:8px 10px;background:var(--bg2);border:1px solid var(--line);border-radius:8px}
.fi .ic{width:8px;height:8px;border-radius:50%;margin-top:5px;flex:0 0 8px}
.fi .ft{flex:1;min-width:0}.fi .ft .h{font-size:12px;color:var(--tx)}.fi .ft .s{font-size:11px;color:var(--mut);margin-top:2px;word-break:break-word}
.fi .ts2{color:var(--mut);font-size:10px;white-space:nowrap}
@media(max-width:760px){.tab{padding:8px 12px}.brain-grid{grid-template-columns:1fr}#brainPanel{padding:10px 12px}}
</style></head><body class="tab-chat">
<div class="side">
  <div class="brand"><span class="dot"></span> A2A Portal</div>
  <div class="sec">Agents</div><div id="presence"></div>
  <div class="sec">Channels</div><div class="list" id="channels"></div>
</div>
<div class="resizer" id="sideResizer" role="separator" aria-label="Resize sidebar" aria-orientation="vertical" tabindex="0"></div>
<div class="scrim" id="scrim"></div>
<div class="main">
  <div class="top"><button class="navbtn" id="navBtn" type="button" aria-label="Open navigation" aria-expanded="false">☰</button><div class="tabs" id="tabs"><button class="tab active" type="button" data-tab="chat">Chat</button><button class="tab" type="button" data-tab="brain">Skills &amp; Brain</button></div><h2 id="title" class="chat-only"># —</h2><label class="viewtoggle chat-only"><input id="renderOutput" type="checkbox"> Render output</label><span class="meta chat-only" id="meta"></span></div>
  <div class="msgs" id="msgs"><div class="empty">Select a channel</div></div>
  <div class="composer">
    <input id="sendTo" autocomplete="off" spellcheck="false" aria-label="Recipient" placeholder="@agent or #topic">
    <select id="sendType" aria-label="Message type"><option value="request">request</option><option value="msg">msg</option></select>
    <select id="sendMode" aria-label="Conversation mode"><option value="chat">chat</option><option value="one-off">one-off</option></select>
    <button id="resetContextBtn" class="secondary" type="button" title="Clear chat context">Reset</button>
    <textarea id="sendBody" spellcheck="true" aria-label="Message body" placeholder="Message"></textarea>
    <button id="sendBtn">Send</button>
    <div id="sendState" class="sendstate"></div>
  </div>
  <div class="tabpanel" id="brainPanel">
    <div class="brain-head"><h2>Skills &amp; Brain</h2><span class="panel-sub">the shared skills library — learned once by an agent, broadcast to the whole fleet</span></div>
    <div class="brain-grid">
      <div><div class="sec2">Skills Library <span class="cnt2" id="skillCount">0</span></div><div class="skills" id="skills"></div></div>
      <div><div class="sec2">Memory &amp; Activity</div><div class="feed" id="feed"></div></div>
    </div>
  </div>
</div>
  <script>
  const $=s=>document.querySelector(s)
  const LANGCHAIN_CHANNEL='${PORTAL_LANGCHAIN_CHANNEL}'
  let active=null, chans=[], presenceList=[], sendReady=false, portalAgent='', currentMsgs=[]
  let activeTab='chat', skillsData=[], feedData=[], freshSkills={}
  let langchainReady=false, langchainError='', langchainModel=''
  let renderOutput=localStorage.getItem('a2a.portal.renderOutput')==='1'
  let sendMode=localStorage.getItem('a2a.portal.sendMode')||'chat'
  let chatContexts=loadChatContexts()
const colors=['#5b8cff','#f6c453','#48d597','#ff6b8a','#b48cff','#3fd0d4','#ffa657','#9ece6a']
const color=s=>{let h=0;for(const c of s)h=(h*31+c.charCodeAt(0))>>>0;return colors[h%colors.length]}
const initials=s=>s.replace('@','').split('-').map(x=>x[0]).join('').slice(0,3).toUpperCase()
const ago=t=>{if(!t)return '';const d=(Date.now()-t)/1000;if(d<60)return Math.floor(d)+'s';if(d<3600)return Math.floor(d/60)+'m';return Math.floor(d/3600)+'h'}
	function fmtBody(m){if(renderOutput&&typeof m.rendered_body==='string')return {text:m.rendered_body,code:false,rendered:true};
	  let o;try{o=JSON.parse(m.body)}catch{return {text:m.body,code:false,rendered:renderOutput}}; if(o&&typeof o==='object')return {text:JSON.stringify(o,null,2),code:true,rendered:false};return {text:m.body,code:false,rendered:renderOutput}}
function setNav(open){document.body.classList.toggle('nav-open',open);const b=$('#navBtn');if(b)b.setAttribute('aria-expanded',open?'true':'false')}
function closeNavOnMobile(){if(matchMedia('(max-width:760px)').matches)setNav(false)}
function setupNav(){const b=$('#navBtn'),s=$('#scrim');if(b)b.onclick=()=>setNav(!document.body.classList.contains('nav-open'));if(s)s.onclick=()=>setNav(false);document.addEventListener('keydown',e=>{if(e.key==='Escape')setNav(false)})}
function sideBounds(){return {min:240,max:Math.min(520,Math.max(240,Math.floor(window.innerWidth*.45)))}}
function clampSideWidth(w){const b=sideBounds();return Math.max(b.min,Math.min(b.max,Math.round(Number(w)||310)))}
function storedSideWidth(){const v=Number(localStorage.getItem('a2a.portal.sidebarWidth'));return Number.isFinite(v)&&v>0?v:310}
function currentSideWidth(){const s=$('.side');return s?s.getBoundingClientRect().width:storedSideWidth()}
function applySideWidth(w,persist=true){const next=clampSideWidth(w);document.documentElement.style.setProperty('--side-w',next+'px');if(persist)localStorage.setItem('a2a.portal.sidebarWidth',String(next));return next}
function setupSidebarResize(){const r=$('#sideResizer');if(!r)return;const isMobile=()=>matchMedia('(max-width:760px)').matches
  if(!isMobile())applySideWidth(storedSideWidth(),!!localStorage.getItem('a2a.portal.sidebarWidth'))
  let startX=0,startW=0,pointer=null
  const move=e=>{if(pointer===null)return;applySideWidth(startW+e.clientX-startX)}
  const stop=e=>{if(pointer===null)return;try{r.releasePointerCapture?.(pointer)}catch{};pointer=null;r.classList.remove('dragging');document.body.classList.remove('resizing');document.removeEventListener('pointermove',move);document.removeEventListener('pointerup',stop);document.removeEventListener('pointercancel',stop)}
  r.addEventListener('pointerdown',e=>{if(isMobile())return;e.preventDefault();pointer=e.pointerId;startX=e.clientX;startW=currentSideWidth();r.classList.add('dragging');document.body.classList.add('resizing');r.setPointerCapture?.(e.pointerId);document.addEventListener('pointermove',move);document.addEventListener('pointerup',stop);document.addEventListener('pointercancel',stop)})
  r.addEventListener('keydown',e=>{if(isMobile())return;if(e.key==='Home'||e.key==='End'||e.key==='ArrowLeft'||e.key==='ArrowRight'){e.preventDefault();const b=sideBounds();const cur=storedSideWidth();const next=e.key==='Home'?b.min:e.key==='End'?b.max:cur+(e.key==='ArrowRight'?16:-16);applySideWidth(next)}})
  window.addEventListener('resize',()=>{if(!isMobile())applySideWidth(storedSideWidth())})}
function setupPresenceClicks(){const el=$('#presence');if(!el)return;el.addEventListener('click',e=>{const target=e.target&&e.target.closest?e.target:null;const row=target&&target.closest('.pr[data-agent]');if(row&&row.dataset.agent)open_('@'+row.dataset.agent)})}
function setupRenderToggle(){const cb=$('#renderOutput');if(!cb)return;cb.checked=renderOutput;cb.onchange=()=>{renderOutput=cb.checked;localStorage.setItem('a2a.portal.renderOutput',renderOutput?'1':'0');render(currentMsgs)}}
  function setupSendMode(){const el=$('#sendMode');if(!el)return;el.value=sendMode==='one-off'?'one-off':'chat';el.onchange=()=>{sendMode=el.value==='one-off'?'one-off':'chat';localStorage.setItem('a2a.portal.sendMode',sendMode)}}
  function loadChatContexts(){try{const v=JSON.parse(localStorage.getItem('a2a.portal.chatContexts')||'{}');return v&&typeof v==='object'?v:{}}catch{return {}}}
  function saveChatContexts(){localStorage.setItem('a2a.portal.chatContexts',JSON.stringify(chatContexts))}
  function newChatContextId(){return 'c-'+Date.now().toString(36)+'-'+Math.random().toString(36).slice(2,8)}
  function isLangChainChannel(name=active){return name===LANGCHAIN_CHANNEL}
  function chatContextTarget(v){const t=normalizedTarget(v);if(!t||t===portalAgent||t.startsWith('topic:'))return '';return t}
  function resetChatContext(){const state=$('#sendState');const target=chatContextTarget($('#sendTo').value);if(!target){state.className='sendstate err';state.textContent='choose an agent chat to reset';return}
    chatContexts[target]=newChatContextId();saveChatContexts();state.className='sendstate ok';state.textContent='new chat context for '+target}
async function loadChannels(){const r=await(await fetch('/api/channels')).json();chans=r.channels;renderSidebar();if(!active&&chans.length){const first=preferredInitialChannel();if(first)open_(first.name)}}
function primaryForAgent(id){const s=String(id||''),i=s.indexOf('-sub-');return i>0?s.slice(0,i):s}
function isOnlinePresence(p){return p&&Number(p.ttl)>0}
function sortedPresenceItems(items){return [...items].sort((a,b)=>{const ao=isOnlinePresence(a)?0:1,bo=isOnlinePresence(b)?0:1;if(ao!==bo)return ao-bo;return String(a.id).localeCompare(String(b.id))})}
function presenceGroupOnline(g){return isOnlinePresence(g.primary)||g.subs.some(isOnlinePresence)}
function renderAgentRow(p,cls=''){const id=String(p.id||''),self=id===portalAgent;return '<div class="pr'+(cls?' '+cls:'')+(active==='@'+id?' active':'')+(self?' self':'')+'" '+(self?'':'data-agent="'+escAttr(id)+'"')+'><span class="d'+(isOnlinePresence(p)?'':' off')+'"></span><span class="agent-name" style="color:'+color(id)+'">'+esc(id)+'</span><span class="host">'+(self?'you':'')+'</span></div>'}
function renderPresenceGroups(items){if(!items.length)return '<div class="pr" style="color:#7b8494">none</div>'
  const hosts=new Map();for(const p of items){const h=String(p.host||'').trim()||'unknown host';if(!hosts.has(h))hosts.set(h,[]);hosts.get(h).push(p)}
  return [...hosts.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([host,agents])=>{
    const byPrimary=new Map();for(const p of sortedPresenceItems(agents)){const primary=primaryForAgent(p.id);if(!byPrimary.has(primary))byPrimary.set(primary,{primary:null,subs:[]});const g=byPrimary.get(primary);if(primary===p.id)g.primary=p;else g.subs.push(p)}
    const rows=[...byPrimary.entries()].sort((a,b)=>{const ao=presenceGroupOnline(a[1])?0:1,bo=presenceGroupOnline(b[1])?0:1;if(ao!==bo)return ao-bo;return a[0].localeCompare(b[0])}).map(([primary,g])=>{
      const parts=[];if(g.primary)parts.push(renderAgentRow(g.primary));else parts.push('<div class="pr placeholder"><span class="d off"></span><span class="agent-name">'+esc(primary)+'</span><span class="host">primary</span></div>')
      for(const sub of sortedPresenceItems(g.subs))parts.push(renderAgentRow(sub,'sub'))
      return parts.join('')
    }).join('')
    return '<div class="hostgrp"><div class="hosthead">'+esc(host)+'</div>'+rows+'</div>'
  }).join('')}
async function loadPresence(){const r=await(await fetch('/api/presence')).json();presenceList=r.presence||[];const el=$('#presence');el.innerHTML=renderPresenceGroups(presenceList)}
  async function loadSendStatus(){let r;try{r=await(await fetch('/api/send-status')).json()}catch{r={send:{enabled:false,error:'offline'}}}
    sendReady=!!r.send?.enabled;portalAgent=r.send?.agent_id||'';updateComposerState(r.send?.error||'send disabled')}
  async function loadLangChainStatus(){let r;try{r=await(await fetch('/api/langchain/status')).json()}catch{r={langchain:{ready:false,error:'offline',model:''}}}
    langchainReady=!!r.langchain?.ready;langchainError=r.langchain?.error||'';langchainModel=r.langchain?.model||'';updateComposerState()}
  function updateComposerState(sendError='send disabled'){const b=$('#sendBtn'),s=$('#sendState');if(!b||!s)return;
    if(isLangChainChannel()){b.disabled=!langchainReady;s.className='sendstate'+(langchainReady?' ok':' err');s.textContent=langchainReady?'langchain '+langchainModel:(langchainError||'langchain disabled');return}
    b.disabled=!sendReady;s.className='sendstate'+(sendReady?' ok':' err');s.textContent=sendReady?'sending as '+portalAgent:sendError}
  function preferredInitialChannel(){return chans.find(c=>c.kind==='dm'&&c.name!=='@'+portalAgent)||chans.find(c=>c.kind==='topic')||chans.find(c=>c.name!=='@'+portalAgent)||chans[0]}
  function renderSidebar(){$('#channels').innerHTML=chans.map(c=>{
    const marker=c.kind==='topic'?'#':(c.kind==='local'?'ai':'');return '<div class="ch'+(c.name===active?' active':'')+'" onclick="open_(\\''+c.name+'\\')">'+
    '<span class="h">'+marker+'</span><span class="n">'+c.name+'</span><span class="c">'+c.count+'</span></div>'}).join('')}
  async function open_(name){active=name;closeNavOnMobile();renderSidebar();const path=isLangChainChannel(name)?'/api/langchain/messages':(name[0]==='@'?'/api/dm/'+encodeURIComponent(name.slice(1)):'/api/channels/'+encodeURIComponent(name));const r=await(await fetch(path)).json();
    $('#title').textContent=isLangChainChannel(name)?'LangChain Agent':((name[0]==='@'?'':'# ')+name);setRecipientForActive(name,r.messages||[]);loadPresence();updateComposerState();render(r.messages);}
  function setRecipientForActive(name=active,msgs=[]){if(!name)return;let target=name[0]==='@'?name:'#'+name;
    const sendTo=$('#sendTo'),sendType=$('#sendType'),mode=$('#sendMode'),reset=$('#resetContextBtn'),body=$('#sendBody')
    const local=isLangChainChannel(name);sendTo.disabled=local;sendType.disabled=local;mode.disabled=local;reset.disabled=local;if(body)body.placeholder=local?'Message LangChain agent':'Message'
    if(local){sendTo.value='langchain-agent';return}
    if(name==='@'+portalAgent){const m=[...msgs].reverse().find(x=>(x.from&&x.from!==portalAgent)||(x.to&&x.to!==portalAgent));const peer=m?(m.from!==portalAgent?m.from:m.to):'';target=peer?'@'+peer:''}
    sendTo.value=target}
function render(msgs){currentMsgs=msgs||[];const el=$('#msgs');if(!currentMsgs.length){$('#meta').textContent='0 messages';el.innerHTML='<div class="empty">No messages yet</div>';return}
  $('#meta').textContent=currentMsgs.length+' messages'
  el.innerHTML=currentMsgs.map(m=>{const fb=fmtBody(m);const type=String(m.type||'msg');const cls=type.replace(/[^a-z0-9_-]/gi,'')||'msg';return '<div class="m">'+
   '<div class="av" style="background:'+color(m.from)+'">'+esc(initials(m.from))+'</div>'+
   '<div class="mb"><div class="mh"><span class="who" style="color:'+color(m.from)+'">'+esc(m.from)+'</span>'+
   '<span class="badge '+cls+'">'+esc(type)+'</span>'+(m.to&&m.to[0]!=='t'&&m.kind==='dm'?'<span class="arrow">→ '+esc(m.to)+'</span>':'')+
   (m.thread?'<span class="thr">#'+esc(m.thread)+'</span>':'')+'<span class="ts">'+new Date(m.t).toLocaleTimeString()+'</span></div>'+
	   '<div class="body'+(fb.code?' code':'')+(fb.rendered?' rendered':'')+'">'+(fb.rendered?markdown(fb.text):esc(fb.text))+'</div></div></div>'}).join('')
	  el.scrollTop=el.scrollHeight}
	function esc(s){return String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))}
	function escAttr(s){return esc(s).replace(/"/g,'&quot;')}
	function safeHref(raw){try{const u=new URL(String(raw||'').trim().replace(/[\\u0000-\\u001f\\s]/g,''),location.origin);return /^(https?:|mailto:)$/i.test(u.protocol)?u.href:''}catch{return ''}}
	function inlineMd(s){const slots=[];let text=String(s)
	  const put=h=>'\\u0000'+(slots.push(h)-1)+'\\u0000'
	  text=text.replace(/\`([^\`]+)\`/g,(_,c)=>put('<code>'+esc(c)+'</code>'))
	  text=text.replace(/\\[([^\\]\\n]+)\\]\\(([^)\\s]+)(?:\\s+["'][^"']*["'])?\\)/g,(_,label,href)=>{const safe=safeHref(href);return safe?put('<a href="'+escAttr(safe)+'" target="_blank" rel="noopener noreferrer">'+inlineMd(label)+'</a>'):label})
	  text=esc(text)
	    .replace(/\\*\\*([^*]+)\\*\\*/g,'<strong>$1</strong>')
	    .replace(/__([^_]+)__/g,'<strong>$1</strong>')
	    .replace(/(^|[^*])\\*([^*\\n]+)\\*/g,'$1<em>$2</em>')
	    .replace(/(^|[^_])_([^_\\n]+)_/g,'$1<em>$2</em>')
	  return text.replace(/\\u0000(\\d+)\\u0000/g,(_,i)=>slots[Number(i)]||'')}
	function markdown(src){const lines=String(src||'').replace(/\\r\\n?/g,'\\n').split('\\n');let out=[],para=[],list='',code=null,quote=[]
	  const closeList=()=>{if(list){out.push('</'+list+'>');list=''}}
	  const flushPara=()=>{if(!para.length)return;closeList();out.push('<p>'+inlineMd(para.join('\\n')).replace(/\\n/g,'<br>')+'</p>');para=[]}
	  const flushQuote=()=>{if(!quote.length)return;flushPara();closeList();out.push('<blockquote>'+inlineMd(quote.join('\\n')).replace(/\\n/g,'<br>')+'</blockquote>');quote=[]}
	  const openList=t=>{flushPara();flushQuote();if(list&&list!==t)closeList();if(!list){list=t;out.push('<'+t+'>')}}
	  const tableAt=i=>i+1<lines.length&&/\\|/.test(lines[i]||'')&&/^\\s*\\|?\\s*:?-{3,}:?\\s*(\\|\\s*:?-{3,}:?\\s*)+\\|?\\s*$/.test(lines[i+1]||'')
	  const cells=line=>line.trim().replace(/^\\||\\|$/g,'').split('|').map(x=>x.trim())
	  for(let i=0;i<lines.length;i++){const line=lines[i]
	    let m=line.match(/^\\s*(\\x60\\x60\\x60|~~~)\\s*([\\w.-]+)?\\s*$/);if(m){if(code){out.push('<pre><code>'+esc(code.lines.join('\\n'))+'</code></pre>');code=null}else{flushPara();flushQuote();closeList();code={lines:[]}};continue}
	    if(code){code.lines.push(line);continue}
	    if(!line.trim()){flushPara();flushQuote();closeList();continue}
	    if(tableAt(i)){flushPara();flushQuote();closeList();const head=cells(lines[i]),rows=[];i+=2;while(i<lines.length&&/\\|/.test(lines[i])&&lines[i].trim()){rows.push(cells(lines[i]));i++}i--;out.push('<table><thead><tr>'+head.map(c=>'<th>'+inlineMd(c)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+r.map(c=>'<td>'+inlineMd(c)+'</td>').join('')+'</tr>').join('')+'</tbody></table>');continue}
	    m=line.match(/^(#{1,4})\\s+(.+)$/);if(m){flushPara();flushQuote();closeList();out.push('<h'+m[1].length+'>'+inlineMd(m[2].trim())+'</h'+m[1].length+'>');continue}
	    m=line.match(/^>\\s?(.*)$/);if(m){flushPara();closeList();quote.push(m[1]);continue}
	    m=line.match(/^\\s*[-*+]\\s+(.+)$/);if(m){openList('ul');out.push('<li>'+inlineMd(m[1])+'</li>');continue}
	    m=line.match(/^\\s*\\d+[.)]\\s+(.+)$/);if(m){openList('ol');out.push('<li>'+inlineMd(m[1])+'</li>');continue}
	    if(quote.length)flushQuote();para.push(line)}
	  if(code)out.push('<pre><code>'+esc(code.lines.join('\\n'))+'</code></pre>');flushPara();flushQuote();closeList();return out.join('')}
	function normalizedTarget(v){const raw=(v||'').trim().toLowerCase();if(!raw)return '';if(raw[0]==='@')return raw.slice(1);if(raw[0]==='#')return 'topic:'+raw.slice(1);return raw}
function channelForTarget(v){const t=normalizedTarget(v);if(!t||t===portalAgent)return '';return t.startsWith('topic:')?t.slice(6):'@'+t}
  async function sendNow(){if(isLangChainChannel())return sendLangChainNow();if(!sendReady)return;const body=$('#sendBody').value;if(!body.trim())return;
    const target=chatContextTarget($('#sendTo').value)
    const payload={to:$('#sendTo').value,type:$('#sendType').value,send_mode:$('#sendMode').value,chat_context:target?chatContexts[target]:undefined,body}
  const btn=$('#sendBtn'),state=$('#sendState');btn.disabled=true;state.className='sendstate';state.textContent='sending';
  const next=channelForTarget(payload.to);if(!next){state.className='sendstate err';state.textContent='choose another agent; this portal sends as '+portalAgent;btn.disabled=!sendReady;return}
  let out;try{const r=await fetch('/api/send',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});out=await r.json()}catch(e){out={ok:false,error:String(e)}}
  if(out.ok){$('#sendBody').value='';state.className='sendstate ok';const dw=out.delivery_wait;const ds=dw&&dw.status==='reply'?' · replied':(dw&&dw.status==='timeout'?' · no reply yet':'');state.textContent='sent '+out.id+(out.thread_key?' · chat':'')+ds;setTimeout(()=>open_(next),300);setTimeout(loadChannels,700)}
    else{state.className='sendstate err';state.textContent=out.error+(out.detail?': '+out.detail:'')}
    btn.disabled=!sendReady}
  async function sendLangChainNow(){const body=$('#sendBody').value;if(!body.trim())return;const btn=$('#sendBtn'),state=$('#sendState');btn.disabled=true;state.className='sendstate';state.textContent='thinking';
    let out;try{const r=await fetch('/api/langchain/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({body})});out=await r.json()}catch(e){out={ok:false,error:String(e)}}
    if(out.messages)render(out.messages);if(out.ok){$('#sendBody').value='';state.className='sendstate ok';state.textContent='answered';setTimeout(loadChannels,300)}
    else{state.className='sendstate err';state.textContent=out.error||out.reply||'langchain failed';if(out.status){langchainReady=!!out.status.ready;langchainError=out.status.error||langchainError}}
    btn.disabled=!langchainReady}
$('#sendBtn').onclick=sendNow
$('#resetContextBtn').onclick=resetChatContext
$('#sendBody').addEventListener('keydown',e=>{if((e.metaKey||e.ctrlKey)&&e.key==='Enter'){e.preventDefault();sendNow()}})
  function activePath(){return isLangChainChannel()?'/api/langchain/messages':(active&&active[0]==='@'?'/api/dm/'+encodeURIComponent(active.slice(1)):'/api/channels/'+encodeURIComponent(active))}
  function messageTouchesActive(d){if(!active)return false;if(d.channel===active)return true;if(active[0]!=='@')return false;const peer=active.slice(1);const m=d.message||{};return d.channel==='@'+portalAgent&&m.from===peer}
  function setupTabs(){document.body.classList.add('tab-chat');var bs=document.querySelectorAll('#tabs .tab');for(var i=0;i<bs.length;i++){(function(b){b.onclick=function(){setTab(b.dataset.tab)}})(bs[i])}}
  function setTab(name){activeTab=name;document.body.classList.remove('tab-chat','tab-brain');document.body.classList.add('tab-'+name);
    var bs=document.querySelectorAll('#tabs .tab');for(var i=0;i<bs.length;i++)bs[i].classList.toggle('active',bs[i].dataset.tab===name);
    closeNavOnMobile();if(name==='brain')loadBrain()}
  async function loadBrain(){try{var r=await(await fetch('/api/skills')).json();if(r){skillsData=r.skills||[];feedData=r.feed||[]}}catch(e){}renderSkills();renderFeed()}
  function renderSkills(){var el=$('#skills');if(!el)return;var cnt=$('#skillCount');if(cnt)cnt.textContent=skillsData.length;
    if(!skillsData.length){el.innerHTML='<div class="col-empty">No skills learned yet</div>';return}
    el.innerHTML=skillsData.map(function(s){var fresh=freshSkills[s.name]&&freshSkills[s.name]>Date.now();
      var tags=(s.tags||[]).map(function(t){return '<span class="tag">'+esc(t)+'</span>'}).join('');
      var badge=fresh?'<span class="bcast">new · broadcast</span>':(s.scope==='global'?'<span class="bcast">global</span>':'');
      return '<div class="skill'+(fresh?' fresh':'')+'">'+badge+'<div class="nm">'+esc(s.name)+'</div><div class="src">from '+esc(s.source||'?')+'</div>'+
        (s.summary?'<div class="sum">'+esc(s.summary)+'</div>':'')+'<div class="tags">'+tags+'</div></div>'}).join('')}
  function upsertSkill(s){if(!s||!s.name)return;var i=-1;for(var k=0;k<skillsData.length;k++){if(skillsData[k].name===s.name){i=k;break}}
    if(i>=0)skillsData[i]=s;else skillsData.unshift(s);freshSkills[s.name]=Date.now()+9000;
    if(activeTab==='brain')renderSkills();setTimeout(function(){if(activeTab==='brain')renderSkills()},9200)}
  function feedLabel(it){return {h:'Skill learned &amp; broadcast',s:esc(it.directive||'')+(it.source?' · from '+esc(it.source):''),ic:'#f6c453'}}
  function renderFeed(){var el=$('#feed');if(!el)return;if(!feedData.length){el.innerHTML='<div class="col-empty">No activity yet</div>';return}
    el.innerHTML=feedData.slice(0,60).map(function(it){var l=feedLabel(it);
      return '<div class="fi"><span class="ic" style="background:'+l.ic+'"></span><div class="ft"><div class="h">'+l.h+'</div><div class="s">'+l.s+'</div></div><span class="ts2">'+ago(it.t)+'</span></div>'}).join('')}
const es=new EventSource('/api/stream')
es.onmessage=e=>{const d=JSON.parse(e.data);
  if(d.kind==='skill'){upsertSkill(d.skill);return}
  if(d.kind==='brain'){if(d.item){feedData.unshift(d.item);if(feedData.length>80)feedData.length=80}if(activeTab==='brain')renderFeed();return}
  if(d.kind!=='msg')return;
  const c=chans.find(x=>x.name===d.channel);if(c){c.count++;c.lastT=d.message.t}else loadChannels();renderSidebar();
  if(messageTouchesActive(d)){const el=$('#msgs');const stick=el.scrollTop+el.clientHeight>el.scrollHeight-40;
    fetch(activePath()).then(r=>r.json()).then(r=>{render(r.messages);if(!stick)el.scrollTop=el.scrollTop})}}
  async function init(){setupNav();setupTabs();setupSidebarResize();setupPresenceClicks();setupRenderToggle();setupSendMode();await loadSendStatus();await loadLangChainStatus();await loadChannels();await loadPresence();setInterval(loadPresence,5000);setInterval(loadChannels,8000);setInterval(loadSendStatus,10000);setInterval(loadLangChainStatus,10000)}
init()
</script></body></html>`

// ── boot ──────────────────────────────────────────────────────────────────────
let nc: NatsConnection | undefined
let shutdownPromise: Promise<void> | null = null

async function shutdown(code = 0) {
  if (!shutdownPromise) {
    shutdownPromise = (async () => {
      await sendChannel?.stop().catch(() => {})
      await nc?.drain().catch(() => {})
    })()
  }
  await shutdownPromise
  process.exit(code)
}

for (const sig of ['SIGTERM', 'SIGINT'] as const) {
  process.on(sig, () => { void shutdown(0) })
}

;(async () => {
  redis = new RedisClient(REDIS_URL)
  await persistInit(); await loadHistory()
  await startSendChannel()
  langchainAgent = new PortalLangChainAgent({
    portalAgentId: PORTAL_AGENT_ID,
    getSendChannel: () => sendChannel,
    getSendStatus: () => sendStatus,
    listPeers: presence,
    listChannels,
    readChannel: readPortalChannel,
  })
  nc = await connect({ servers: NATS_URL, name: 'a2a-portal' })
  const sub = nc.subscribe('alloyium.a2a.>')
  console.error(`[portal] serving http://0.0.0.0:${PORT}  (NATS ${NATS_URL})`)
  ;(async () => { for await (const m of sub) onMessage(m.subject, m.data) })()
})().catch((e) => { console.error('[portal] boot failed', e); process.exit(1) })
