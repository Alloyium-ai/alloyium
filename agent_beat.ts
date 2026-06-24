#!/usr/bin/env bun
import { randomUUID } from 'node:crypto'
import { hostname as osHostname } from 'node:os'
import { dirname, join } from 'node:path'
import { mkdir, readFile, rename, writeFile, readdir, stat, unlink } from 'node:fs/promises'
import { connect, credsAuthenticator, nkeyAuthenticator } from 'nats'
import { importEd25519Seed, signEnvelope, topicSubject, type Envelope, type SigAlg, type SignKey } from './a2a-channel.ts'

/** Frozen heartbeat schema emitted to `topic:agent-beat`. */
export const AGENT_BEAT_SCHEMA = 'agent.beat.v1' as const

/** A2A topic token for ephemeral loop liveness beats. */
export const AGENT_BEAT_TOPIC = 'agent-beat' as const

/**
 * Agent driving mode reported by a beat. Agents emit 'loop' | 'goal'; the
 * a2a-core's OWN service beat emits 'service' (per the SLICE-3 shared SoT
 * plane_schemas.ts — imported at merge). The wedge detector accepts any mode
 * and classifies mode-agnostically (live-by-loop_seq).
 */
export type AgentDriverMode = 'loop' | 'goal' | 'service'

/** Coarse loop state reported by a beat. */
export type AgentBeatState = 'booting' | 'idle' | 'in_task' | 'paused' | 'draining'

/** Lifecycle point represented by this emit. */
export type AgentBeatPhase = 'turn-start' | 'turn-end' | 'still-working' | 'progress' | 'keepalive'

/** Frozen `agent.beat.v1` core contract. */
export interface AgentBeat {
  schema: typeof AGENT_BEAT_SCHEMA
  agent_id: string
  host: string
  boot_id: string
  session_id: string
  loop_seq: number
  turn_start_ts: string
  last_turn_ts: string
  last_progress_ts: string
  driver_mode: AgentDriverMode
  stop_hook_active: boolean
  goal_id?: string
  goal_epoch?: number
  state: AgentBeatState
  inbox_depth: number
  task_ids: string[]
}

/** Partial state supplied by a hook or loop driver. */
export interface EmitBeatPartial {
  agent_id?: string
  host?: string
  boot_id?: string
  session_id?: string
  driver_mode?: AgentDriverMode
  stop_hook_active?: boolean
  goal_id?: string
  goal_epoch?: number
  state?: AgentBeatState
  inbox_depth?: number
  task_ids?: string[]
  phase?: AgentBeatPhase
  progress?: boolean
  now?: number
  state_dir?: string
  publish?: boolean
  emitter_id?: string
  signing_key_path?: string
  /** Keepalive-only throttle (ms): skip entirely — no state write, no loop_seq advance, no
   *  publish — if a beat (any phase) already landed within this window. Gap-filler semantics so a
   *  per-tool-call PostToolUse keepalive can't spam the bus / add NATS latency to every tool call.
   *  Ignored for non-keepalive phases (turn-start / turn-end are never throttled). */
  min_interval_ms?: number
}

/** Persisted monotonic beat state for one `(agent, session, boot)`. */
export interface PersistedBeatState {
  agent_id: string
  boot_id: string
  session_id: string
  loop_seq: number
  turn_start_ts: string
  last_turn_ts: string
  last_progress_ts: string
  updated_ts: string
}

/** Result returned by `emitBeat`; publish failures are values, not thrown errors.
 *  A throttled keepalive returns `{ ok: true, skipped: 'throttled' }` with no `beat`. */
export type EmitBeatResult =
  | { ok: true; beat: AgentBeat; state_file: string; send?: unknown }
  | { ok: true; skipped: 'throttled'; state_file: string; reason: string; beat?: undefined }
  | { ok: false; error: string; beat?: AgentBeat; state_file?: string; send?: unknown }

const TOKEN_RE = /^[a-z0-9-]{1,64}$/
const EPOCH_TS = new Date(0).toISOString()

/** Return the default dedicated signed beat-emitter id for an agent. */
export function defaultBeatEmitterId(agentId: string): string {
  const suffix = '-beat'
  return `${agentId.slice(0, Math.max(1, 64 - suffix.length))}${suffix}`
}

/** Compute the next beat and persisted state without side effects. */
export function buildNextBeat(
  prev: PersistedBeatState | null,
  input: EmitBeatPartial,
  identity: { agent_id: string; boot_id: string; session_id: string },
): { beat: AgentBeat; next: PersistedBeatState } {
  const nowMs = input.now ?? Date.now()
  const nowIso = new Date(nowMs).toISOString()
  const phase = input.phase ?? 'turn-end'
  const prevSeq = Number.isFinite(prev?.loop_seq) ? Number(prev!.loop_seq) : 0

  let loopSeq = prevSeq
  let turnStart = prev?.turn_start_ts || nowIso
  let lastTurn = prev?.last_turn_ts || nowIso
  let lastProgress = prev?.last_progress_ts || EPOCH_TS

  if (phase === 'turn-start') {
    // A turn-start beat ADVANCES loop_seq (so the detector ingests it — claimHighWater needs
    // loop_seq > last) and sets turn_start_ts=now (the current turn starts now → the long-turn
    // clock measures THIS turn). last_turn_ts + last_progress_ts are PRESERVED (no turn completed,
    // no progress yet) so it can't mask a genuine long-turn / stale-goal-spin. Fixes the
    // resumed-after-long-idle false long-turn (panel P1a/P2). Used by the boot beat + UserPromptSubmit hook.
    loopSeq = prevSeq + 1
    turnStart = nowIso
  } else if (phase === 'turn-end') {
    loopSeq = prevSeq + 1
    if (!prev?.turn_start_ts) turnStart = nowIso
    lastTurn = nowIso
  } else if (phase === 'still-working') {
    if (!prev?.turn_start_ts) turnStart = nowIso
  } else if (phase === 'keepalive') {
    // Keepalive = loop-tied proof-of-life that ADVANCES loop_seq (defeats false loop-dead)
    // but PRESERVES last_turn_ts + last_progress_ts (a keepalive must NEVER bump the progress
    // clocks, else it masks wedged:long-turn / wedged:stale-goal-spin on a genuinely-active agent).
    loopSeq = prevSeq + 1
    if (!prev?.turn_start_ts) turnStart = nowIso
  }

  // progress bumps last_progress_ts — EXCEPT for a keepalive or a turn-start (neither is progress).
  if (phase === 'progress' || (input.progress === true && phase !== 'keepalive' && phase !== 'turn-start')) lastProgress = nowIso

  const beat: AgentBeat = {
    schema: AGENT_BEAT_SCHEMA,
    agent_id: identity.agent_id,
    host: input.host || osHostname(),
    boot_id: identity.boot_id,
    session_id: identity.session_id,
    loop_seq: loopSeq,
    turn_start_ts: turnStart,
    last_turn_ts: lastTurn,
    last_progress_ts: lastProgress,
    driver_mode: input.driver_mode ?? envDriverMode(),
    stop_hook_active: input.stop_hook_active ?? false,
    ...(input.goal_id ? { goal_id: input.goal_id } : {}),
    ...(typeof input.goal_epoch === 'number' ? { goal_epoch: input.goal_epoch } : {}),
    state: input.state ?? (phase === 'turn-start' || phase === 'still-working' || phase === 'progress' ? 'in_task' : 'idle'),
    inbox_depth: Math.max(0, Math.floor(input.inbox_depth ?? 0)),
    task_ids: Array.isArray(input.task_ids) ? input.task_ids.map(String).slice(0, 128) : [],
  }

  return {
    beat,
    next: {
      agent_id: identity.agent_id,
      boot_id: identity.boot_id,
      session_id: identity.session_id,
      loop_seq: loopSeq,
      turn_start_ts: turnStart,
      last_turn_ts: lastTurn,
      last_progress_ts: lastProgress,
      updated_ts: nowIso,
    },
  }
}

/**
 * Emit one `agent.beat.v1`.
 *
 * This uses a cheap signed publish-only NATS path. It does not construct
 * `A2AChannel`, claim presence, create an inbox durable, or subscribe to topics.
 * Under signed auth the emitter id must be onboarded separately, typically
 * `<agent-id>-beat`, with its own Redis pubkey.
 */
export async function emitBeat(input: EmitBeatPartial = {}): Promise<EmitBeatResult> {
  try {
    const identity = await resolveIdentity(input)
    if (!TOKEN_RE.test(identity.agent_id)) return { ok: false, error: `bad_agent_id:${identity.agent_id}` }

    const stateFile = beatStateFile(input.state_dir, identity.agent_id, identity.session_id, identity.boot_id)
    const prev = await readJson<PersistedBeatState>(stateFile)

    // Keepalive throttle (gap-filler): if any beat already landed within min_interval_ms, skip
    // entirely — no state write, no loop_seq advance, no publish — so a per-tool-call PostToolUse
    // keepalive can't spam the bus, while still filling the gaps of a long turn well inside the
    // loop-dead window. Only keepalives are throttled; turn-start/turn-end always emit. Clock skew /
    // unparseable prev never throttles (emit-rather-than-wrongly-skip). #32 residual-(b).
    if (input.phase === 'keepalive' && input.min_interval_ms && input.min_interval_ms > 0 && prev?.updated_ts) {
      const sinceMs = (input.now ?? Date.now()) - Date.parse(prev.updated_ts)
      if (Number.isFinite(sinceMs) && sinceMs >= 0 && sinceMs < input.min_interval_ms) {
        return { ok: true, skipped: 'throttled', state_file: stateFile, reason: `keepalive ${Math.round(sinceMs)}ms < min_interval ${input.min_interval_ms}ms` }
      }
    }

    const { beat, next } = buildNextBeat(prev, input, identity)
    await writeJsonAtomic(stateFile, next)
    await pruneBeatStateFiles(dirname(stateFile), stateFile).catch(() => {})

    if (input.publish === false) return { ok: true, beat, state_file: stateFile }

    const send = await publishBeat(beat, input)
    if (!send.ok) return { ok: false, error: send.error, beat, state_file: stateFile, send }
    return { ok: true, beat, state_file: stateFile, send }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) }
  }
}

/** Publish an already-built beat as a signed A2A topic envelope over core NATS. */
export async function publishBeat(
  beat: AgentBeat,
  input: Pick<EmitBeatPartial, 'emitter_id' | 'signing_key_path'> = {},
): Promise<{ ok: true; id: string; subject: string } | { ok: false; error: string }> {
  const emitterId = input.emitter_id || process.env.A2A_BEAT_EMITTER_ID || defaultBeatEmitterId(beat.agent_id)
  if (!TOKEN_RE.test(emitterId)) return { ok: false, error: `bad_emitter_id:${emitterId}` }

  const sigAlg = ((process.env.A2A_BEAT_SIG_ALG || process.env.A2A_SIG_ALG || 'ed25519') as SigAlg)
  if (sigAlg !== 'ed25519' && sigAlg !== 'hmac') return { ok: false, error: `bad_sig_alg:${sigAlg}` }

  const signKey = await loadBeatSignKey(sigAlg, beat.agent_id, emitterId, input.signing_key_path)
  const subject = topicSubject('alloyium.a2a.', AGENT_BEAT_TOPIC)
  const env: Envelope = {
    v: 1,
    id: randomUUID(),
    from: emitterId,
    to: `topic:${AGENT_BEAT_TOPIC}`,
    type: 'msg',
    ts: new Date().toISOString(),
    body: JSON.stringify(beat),
    attrs: { schema: AGENT_BEAT_SCHEMA, agent_id: beat.agent_id, boot_id: beat.boot_id, session_id: beat.session_id },
    alg: sigAlg,
  }
  env.sig = await signEnvelope(env, sigAlg, signKey)

  let nc: Awaited<ReturnType<typeof connect>> | null = null
  try {
    nc = await connect(await natsConnectOpts(emitterId))
    nc.publish(subject, new TextEncoder().encode(JSON.stringify(env)))
    await withTimeout(nc.flush(), Number(process.env.A2A_BEAT_PUB_TIMEOUT_MS ?? 5000), 'nats.flush')
    return { ok: true, id: env.id, subject }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) }
  } finally {
    if (nc) {
      try { await withTimeout(nc.drain(), 1000, 'nats.drain') } catch { try { nc.close() } catch {} }
    }
  }
}

async function loadBeatSignKey(sigAlg: SigAlg, agentId: string, emitterId: string, explicit?: string): Promise<SignKey> {
  if (sigAlg === 'hmac') {
    const secret = process.env.A2A_BEAT_HMAC_SECRET
    if (!secret) throw new Error('A2A_BEAT_HMAC_SECRET required for hmac beat signing')
    return secret
  }

  const path = explicit || process.env.A2A_BEAT_SIGNING_KEY || inferredSidecarSeedPath(agentId, emitterId)
  if (!path) throw new Error('A2A_BEAT_SIGNING_KEY required; do not sign sidecar beats with the agent seed')
  const buf = await readFile(path)
  return importEd25519Seed(parseSeed(buf))
}

function inferredSidecarSeedPath(agentId: string, emitterId: string): string | undefined {
  const agentSeed = process.env.A2A_SIGNING_KEY
  if (!agentSeed) return undefined
  if (emitterId === agentId) return agentSeed
  return join(dirname(agentSeed), `${emitterId}.seed`)
}

function parseSeed(buf: Uint8Array): Uint8Array {
  if (buf.length === 32) return new Uint8Array(buf)
  const s = Buffer.from(buf).toString('utf8').trim()
  if (/^[0-9a-fA-F]{64}$/.test(s)) return new Uint8Array(Buffer.from(s, 'hex'))
  return new Uint8Array(Buffer.from(s, 'base64'))
}

async function natsConnectOpts(name: string): Promise<Record<string, unknown>> {
  const transportAuth = process.env.A2A_BEAT_TRANSPORT_AUTH || process.env.A2A_TRANSPORT_AUTH
  // Bound connection establishment: nats.js defaults to ~20s, which would stall the caller on a
  // down/misconfigured bus. Critical now that the PostToolUse keepalive puts emitBeat on the
  // per-tool-call path. #32 residual-(b) fusion gate (Opus 4.8 + GPT-5.5 P1). Env-overridable.
  const ct = Number(process.env.A2A_BEAT_CONNECT_TIMEOUT_MS)
  const opts: Record<string, unknown> = {
    servers: process.env.NATS_URL || 'nats://nats:4222',
    name: `a2a-beat-${name}`,
    reconnect: true,
    maxReconnectAttempts: 2,
    timeout: Number.isFinite(ct) && ct > 0 ? ct : 3000,
  }
  if (transportAuth === 'none') return opts

  const nkeyPath = process.env.A2A_BEAT_NKEY || process.env.A2A_NKEY
  const credsPath = process.env.A2A_BEAT_CREDS || process.env.A2A_CREDS
  if (nkeyPath) opts.authenticator = nkeyAuthenticator(new TextEncoder().encode((await readFile(nkeyPath, 'utf8')).trim()))
  else if (credsPath) opts.authenticator = credsAuthenticator(await readFile(credsPath))
  return opts
}

async function resolveIdentity(input: EmitBeatPartial): Promise<{ agent_id: string; boot_id: string; session_id: string }> {
  const agent_id = input.agent_id || process.env.A2A_WATCH_AGENT_ID || process.env.A2A_AGENT_ID || ''
  const session_id = input.session_id || process.env.A2A_SESSION_ID || process.env.CLAUDE_SESSION_ID || process.env.TMUX_PANE || 'default'
  const boot_id = input.boot_id || process.env.A2A_BOOT_ID || await persistedBootId(input.state_dir, agent_id, session_id)
  return { agent_id, boot_id, session_id }
}

async function persistedBootId(dir: string | undefined, agentId: string, sessionId: string): Promise<string> {
  const slot = safeSegment(process.env.A2A_BOOT_PROCESS_ID || String(process.ppid || process.pid))
  const p = join(baseStateDir(dir), safeSegment(agentId), safeSegment(sessionId), `current-boot-${slot}.json`)
  const cur = await readJson<{ boot_id?: string }>(p)
  const boot_id = cur?.boot_id || randomUUID()
  await writeJsonAtomic(p, { boot_id })
  return boot_id
}

function baseStateDir(override?: string): string {
  return override || process.env.A2A_BEAT_STATE_DIR || join(process.env.XDG_RUNTIME_DIR || '/tmp', 'alloyium', 'agent-beat')
}

function beatStateFile(dir: string | undefined, agentId: string, sessionId: string, bootId: string): string {
  return join(baseStateDir(dir), safeSegment(agentId), safeSegment(sessionId), `${safeSegment(bootId)}.json`)
}

function envDriverMode(): AgentDriverMode {
  return process.env.A2A_DRIVER_MODE === 'goal' ? 'goal' : 'loop'
}

function safeSegment(s: string): string {
  const out = String(s || 'unknown').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 64)
  return out || 'unknown'
}

async function readJson<T>(path: string): Promise<T | null> {
  try { return JSON.parse(await readFile(path, 'utf8')) as T } catch { return null }
}

async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`
  await writeFile(tmp, JSON.stringify(value, null, 2), 'utf8')
  await rename(tmp, path)
}

async function pruneBeatStateFiles(dir: string, keep: string, maxFiles = Number(process.env.A2A_BEAT_MAX_STATE_FILES ?? 32)): Promise<void> {
  const entries = await readdir(dir)
  const files = await Promise.all(entries.filter((f) => f.endsWith('.json') && !f.startsWith('current-boot-')).map(async (f) => {
    const p = join(dir, f)
    return { p, mtime: (await stat(p)).mtimeMs }
  }))
  files.sort((a, b) => b.mtime - a.mtime)
  for (const f of files.slice(Math.max(1, maxFiles))) if (f.p !== keep) await unlink(f.p).catch(() => {})
}

function withTimeout<T>(p: Promise<T> | T, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((_, rej) => { t = setTimeout(() => rej(new Error(`${label} timed out`)), ms) })
  return Promise.race([Promise.resolve(p).finally(() => clearTimeout(t)), timeout])
}

function parseArgs(argv: string[]): EmitBeatPartial {
  const out: EmitBeatPartial = {}
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    const v = argv[i + 1]
    if (a === '--phase') out.phase = v as AgentBeatPhase, i++
    else if (a === '--state') out.state = v as AgentBeatState, i++
    else if (a === '--driver-mode') out.driver_mode = v as AgentDriverMode, i++
    else if (a === '--goal-id') out.goal_id = v, i++
    else if (a === '--goal-epoch') out.goal_epoch = Number(v), i++
    else if (a === '--inbox-depth') out.inbox_depth = Number(v), i++
    else if (a === '--task-ids') out.task_ids = v ? v.split(',').filter(Boolean) : [], i++
    else if (a === '--progress') out.progress = true
    else if (a === '--stop-hook-active') out.stop_hook_active = true
    else if (a === '--no-publish') out.publish = false
    else if (a === '--min-interval-ms') out.min_interval_ms = Number(v), i++
  }
  return out
}

if (import.meta.main) {
  const r = await emitBeat(parseArgs(Bun.argv.slice(2)))
  console.error(JSON.stringify(r.ok && r.beat ? { ok: true, beat: r.beat, state_file: r.state_file } : r))
  process.exit(0)
}
