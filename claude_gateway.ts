#!/usr/bin/env bun
// claude-gateway — the CANONICAL full-duplex Claude (Opus) agent on the A2A bus.
//
// The Claude analog of `codex_gateway.ts`: a persistent inference peer that owns a
// long-lived Claude CLI session and bridges the A2A `*.job.*` contract. NO Anthropic
// API key — it drives the logged-in `claude` CLI (OAuth subscription) exactly as the
// launched agents do; NO one-shot `claude -p` per request.
//
// Implements the EXACT mirror of codex_gateway's contract (code = spec):
//   inbound  direct request: claude.job.request.v1 { job_id, input[], stream_topic?, thread_key?, sandbox? }
//   reply    direct:         claude.job.accepted.v1, then claude.job.completed.v1 | claude.job.failed.v1
//   stream   topic:<stream_topic>: claude.job.delta.v1 { job_id, seq, text }   (non-durable; requester JOINs first)
// Back-compat: a plain-text request body (no schema) is treated as a one-shot prompt and answered with plain text.
//
// ── persistence model (investigated empirically, cc 2.1.177) ───────────────────
// `claude -p --input-format stream-json --output-format stream-json --verbose` is a
// LONG-LIVED multi-turn process: write one user-message JSON per turn to stdin, read
// `assistant`/`result` events from stdout, the process stays warm and RETAINS context
// across turns (verified). This is the analog of `codex app-server`. Three measured
// constraints shape the design: (1) a single process == a single conversation —
// per-message session_id is ignored; (2) `/clear` is unreliable in -p mode; (3) NO
// `system/init` is emitted until the FIRST user message, so warming can't wait on it.
// → thread ISOLATION (codex's thread/start per no-thread-key job) is provided by
// SEPARATE warm processes from a pool, not by multiplexing one process. thread_key →
// a sticky warm session (true multi-turn reuse); no thread_key → a fresh warm session
// leased from a pre-warmed pool and retired after the job (no cross-job leakage; no
// cold-start on the request path). Warming = spawn + a brief liveness window (catches
// an immediate exit) since init can't be awaited. The faithful mirror given the CLI.
//
// Turns run in the BACKGROUND of onInbound (ack-fast): the request is accepted, then
// the turn + reply are dispatched off the inbox consume loop. This (a) lets the warm
// pool actually serve concurrent jobs, and (b) keeps an 8h turn from holding the
// consume loop or piling up ack_wait redeliveries. Duplicate requests are deduped by
// the channel's id LRU, so ack-fast loses no job-once semantics.
//
// ADVISORY-ONLY: the child runs with NO built-in tools (`--tools ""`) and NO MCP
// (`--strict-mcp-config`) in /tmp — there is no tool the model could call, the
// read-only-equivalent of codex sandbox=read-only. Capital fire is never on the bus.
// Env from the agent's *.a2a.env (A2A_AGENT_ID, keys, NATS/REDIS, SUBS_KEY).
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { planReply, fitTruncate, putBlob, delBlob, truncateToBytes, utf8ByteLength, sentOk, resolveJobInput, type WrapFn, type BlobRef } from './output_transport.ts'

const AGENT_ID = process.env.A2A_AGENT_ID ?? 'claude-gw'
const MODEL = process.env.CLAUDE_GW_MODEL ?? 'opus'
const EFFORT = process.env.CLAUDE_GW_EFFORT ?? 'high' // low|medium|high|xhigh|max — spec suggests max; high is the shared-endpoint default, env-overridable up
const CWD = process.env.CLAUDE_GW_CWD ?? '/tmp' // clean inference cwd: no project CLAUDE.md/settings bleed into context

// Detached-autonomous guard: this gateway drives `claude` with NO human attached. An
// agent that self-raises an interactive prompt (AskUserQuestion / plan approval) blocks
// until the turn timeout — there is no one to answer. Enforced two ways in CLAUDE_CLI_ARGS:
// `--disallowedTools AskUserQuestion` (hard tool block) + this directive appended to the
// system prompt (so the model never tries).
export const CLAUDE_DETACHED_DIRECTIVE =
  'You are a detached, autonomous agent: there is NO interactive human to answer prompts. ' +
  'Never use AskUserQuestion or any interactive/approval prompt — no one can respond and you will hang indefinitely. ' +
  'If you would ask, instead make the best-justified decision and state it, or include the question in your normal text output.'

// The exact `claude` CLI argv — extracted + exported so the AskUserQuestion block is
// audit-verifiable and regression-tested (see tests/claude_gateway.test.ts).
export const CLAUDE_CLI_ARGS: string[] = [
  'claude', '-p',
  '--input-format', 'stream-json',
  '--output-format', 'stream-json',
  '--verbose',                            // required for stream-json output under -p
  '--model', MODEL,
  '--effort', EFFORT,
  '--tools', '',                          // NO built-in tools → read-only/advisory inference
  '--disallowedTools', 'AskUserQuestion', // HARD-block the interactive prompt — a detached agent has no human to answer it (else an 8h turn-timeout hang)
  '--append-system-prompt', CLAUDE_DETACHED_DIRECTIVE, // + tell the model never to raise one
  '--strict-mcp-config',                  // NO MCP servers → pure inference, no side channels
  '--dangerously-skip-permissions',       // skip tool-permission gates (the tool set is already empty)
]
const POOL = Math.max(1, Number(process.env.CLAUDE_GW_POOL ?? process.env.CLAUDE_GW_CONCURRENCY ?? 2)) // warm idle sessions kept ready for no-thread-key jobs (CLAUDE_GW_CONCURRENCY = provisioned alias)
// A no-thread-key session is retired (killed) once it has served this many turns —
// default 1 ⇒ strict per-job isolation. Raise to enable warm multi-turn reuse of
// pooled sessions (trades isolation for fewer respawns). thread_key sessions are
// sticky and exempt (recycled only at THREAD_MAX_TURNS to bound runaway context).
const SESSION_MAX_TURNS = Math.max(1, Number(process.env.CLAUDE_GW_SESSION_MAX_TURNS ?? 1))
const THREAD_MAX_TURNS = Math.max(1, Number(process.env.CLAUDE_GW_THREAD_MAX_TURNS ?? 200))
const MAX_ACTIVE = Math.max(1, Number(process.env.CLAUDE_GW_MAX_ACTIVE ?? Math.max(POOL + 2, 4))) // cap concurrent in-flight turns → bounds live `claude` processes under burst
const MAX_THREADS = Math.max(1, Number(process.env.CLAUDE_GW_MAX_THREADS ?? 64)) // bound the sticky thread_key map (evict + kill the oldest)
const SPAWN_LIVENESS_MS = Number(process.env.CLAUDE_GW_SPAWN_LIVENESS_MS ?? 3_000) // claude emits no init until the first turn; this brief window only catches an immediate exit (bad flags/auth/login)
// Generous turn cap: the GATEWAY ceiling so a long ML/deep-analysis turn is never
// prematurely killed (8h). Per-job/client timeouts stay overridable DOWNWARD.
const TURN_TIMEOUT_MS = Number(process.env.CLAUDE_GW_TURN_TIMEOUT_MS ?? 28_800_000)
// Output transport (RCA Issue 1): the completed reply is byte-sized to THIS agent's
// A2A send cap (a2a.getMaxSendBytes()). Large output is delivered LOSSLESSLY by
// claim-check (TTL'd Redis blob + ref + marked preview) or, if Redis is down, by
// explicit byte-truncation — never a silent char-slice. (Reuses output_transport.ts.)
const log = (...a: any[]) => console.error(`[claude-gw ${AGENT_ID}]`, ...a)
let jobseq = 0
const jid = () => `clj-${AGENT_ID}-${(jobseq++).toString(36)}-${(Date.now() % 1e7).toString(36)}`

// Lazy Redis for claim-check blobs; opened on first large reply, reused after.
let _redis: RedisClient | null = null
const getRedis = (): RedisClient => (_redis ??= new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379'))

// Child env: inherit ours but STRIP any API-key auth so the `claude` child uses ONLY
// the logged-in OAuth subscription (spec: NO Anthropic API key — never bill the API).
const CHILD_ENV: Record<string, string> = (() => {
  const e: Record<string, string> = {}
  for (const [k, v] of Object.entries(process.env)) if (typeof v === 'string') e[k] = v
  delete e.ANTHROPIC_API_KEY; delete e.ANTHROPIC_AUTH_TOKEN; delete e.ANTHROPIC_BASE_URL
  return e
})()

export type ParsedClaudeStreamEvent =
  | { kind: 'init'; sessionId?: string }
  | { kind: 'rate_limit'; usedPct: number }
  | { kind: 'assistant'; text: string }
  | { kind: 'result'; text: string; status: string; isError: boolean }
  | { kind: 'error'; message: string }
  | { kind: 'other' }

export function parseStreamEvent(m: unknown): ParsedClaudeStreamEvent {
  if (!m || typeof m !== 'object') return { kind: 'other' }
  const ev = m as any
  const t = ev.type
  if (t === 'system' && ev.subtype === 'init') {
    return { kind: 'init', sessionId: typeof ev.session_id === 'string' ? ev.session_id : undefined }
  }
  if (t === 'rate_limit_event') {
    const u = ev.rate_limit_info?.utilization
    return typeof u === 'number' ? { kind: 'rate_limit', usedPct: Math.round(u * 100) } : { kind: 'other' }
  }
  if (t === 'assistant') {
    let text = ''
    const content = ev.message?.content
    if (Array.isArray(content)) {
      for (const c of content) if (c?.type === 'text' && typeof c.text === 'string') text += c.text
    }
    return { kind: 'assistant', text }
  }
  if (t === 'result') {
    const isError = ev.is_error === true
    return {
      kind: 'result',
      text: typeof ev.result === 'string' ? ev.result : '',
      status: typeof ev.subtype === 'string' ? ev.subtype : (isError ? 'unknown' : 'success'),
      isError,
    }
  }
  if (t === 'error') {
    const message = typeof ev.message === 'string' ? ev.message : (typeof ev.error === 'string' ? ev.error : 'error')
    return { kind: 'error', message }
  }
  return { kind: 'other' }
}

// ── persistent Claude CLI session (one process == one conversation) ────────────
// Mirrors CodexAppServer's read-loop/turn machinery, adapted to the Claude CLI's
// newline-delimited stream-json protocol. Turns are SERIALIZED (one in flight per
// process) via a promise queue, matching the CLI's serial turn handling.
class ClaudeSession {
  private proc = Bun.spawn(
    CLAUDE_CLI_ARGS,
    { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe', cwd: CWD, env: CHILD_ENV },
  )
  sessionId = ''
  turns = 0
  dead = false
  usedPct: number | null = null // latest rate-limit utilization (×100), best-effort budget signal
  private inflight: { resolve: (v: { text: string; status: string }) => void; text: string; onDelta?: (s: string) => void } | null = null
  private q: Promise<unknown> = Promise.resolve() // serialize turns
  constructor() { void this.readLoop(); void this.drainStderr() }

  private write(o: any) { this.proc.stdin.write(JSON.stringify(o) + '\n'); this.proc.stdin.flush() }

  private async readLoop() {
    const dec = new TextDecoder(); let buf = ''
    try {
      for await (const chunk of this.proc.stdout) {
        buf += dec.decode(chunk); let nl: number
        while ((nl = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1)
          if (!line) continue
          let m: any; try { m = JSON.parse(line) } catch { continue }
          this.onEvent(m)
        }
      }
    } catch { /* stdout closed */ }
    // process ended: fail any in-flight turn and mark dead so the pool drops us.
    this.dead = true
    if (this.inflight) { const f = this.inflight; this.inflight = null; f.resolve({ text: f.text.trim(), status: 'process_exited' }) }
  }

  // Drain stderr so a verbose long-lived child never blocks on a full stderr pipe;
  // log a bounded slice for diagnosability.
  private async drainStderr() {
    const dec = new TextDecoder()
    try { for await (const chunk of this.proc.stderr) { const s = dec.decode(chunk).trim(); if (s) log(`[child ${this.sessionId.slice(0, 8) || '?'}] ${s.slice(0, 500)}`) } }
    catch { /* stderr closed */ }
  }

  private onEvent(m: any) {
    const ev = parseStreamEvent(m)
    if (ev.kind === 'init') { if (ev.sessionId) this.sessionId = ev.sessionId; return }
    if (ev.kind === 'rate_limit') { this.usedPct = ev.usedPct; return }
    if (!this.inflight) return
    if (ev.kind === 'assistant') {
      if (ev.text) { this.inflight.text += ev.text; this.inflight.onDelta?.(ev.text) }
      return
    }
    if (ev.kind === 'result') {
      const f = this.inflight; this.inflight = null
      const status = ev.isError ? `error:${ev.status || 'unknown'}` : (ev.status || 'success')
      const text = (ev.text.length ? ev.text : f.text).trim()
      f.resolve({ text, status })
    }
  }

  // One turn: write the user message, await the matching `result` (or the turn cap).
  // Serialized behind `q` so concurrent callers on a sticky thread queue cleanly.
  runTurn(prompt: string, onDelta?: (s: string) => void): Promise<{ text: string; status: string }> {
    const run = async (): Promise<{ text: string; status: string }> => {
      if (this.dead) return { text: '', status: 'process_exited' }
      const done = new Promise<{ text: string; status: string }>((resolve) => { this.inflight = { resolve, text: '', onDelta } })
      this.turns++
      this.write({ type: 'user', message: { role: 'user', content: prompt } }) // buffers in the stdin pipe if the child is still booting
      const t0 = Date.now()
      const timeout = new Promise<{ text: string; status: string }>((resolve) => {
        const tick = () => {
          if (!this.inflight) return // already resolved (result event, or process exit via readLoop)
          if (Date.now() - t0 >= TURN_TIMEOUT_MS) {
            const f = this.inflight; this.inflight = null
            this.kill() // a timed-out turn leaves the CLI mid-work — reap it; never reuse a dirty session
            resolve({ text: f.text.trim(), status: 'timeout' })
            return
          }
          setTimeout(tick, 50)
        }
        setTimeout(tick, 50)
      })
      return Promise.race([done, timeout])
    }
    const r = this.q.then(run, run)
    this.q = r.catch(() => {})
    return r
  }

  kill() { this.dead = true; try { this.proc.stdin.end() } catch {} ; try { this.proc.kill() } catch {} }
}

// ── pool: thread_key → sticky warm session; no key → pre-warmed pool, retire-per-job ──
class ClaudePool {
  private threads = new Map<string, ClaudeSession>() // thread_key → sticky warm conversation (insertion-ordered → oldest-first eviction)
  private idle: ClaudeSession[] = []                  // pre-warmed sessions for no-thread-key jobs
  private active = 0                                  // in-flight turns; bounded by MAX_ACTIVE

  private async spawnSession(): Promise<ClaudeSession> {
    const s = new ClaudeSession()
    // `claude -p` stream-json emits no system/init until the FIRST user message, so we
    // can't gate readiness on init. The process is warm once spawned; this brief window
    // only catches an immediate exit (bad flags / not logged in / auth).
    await Bun.sleep(SPAWN_LIVENESS_MS)
    if (s.dead) { s.kill(); throw new Error('claude session died on spawn (flags/auth/login?)') }
    return s
  }

  // Pre-warm the idle pool to POOL (best-effort, off the request path).
  private warming = false
  private async topUp() {
    if (this.warming) return
    this.warming = true
    try { while (this.idle.length < POOL) { try { this.idle.push(await this.spawnSession()) } catch (e) { log(`warm spawn failed: ${e}`); break } } }
    finally { this.warming = false }
  }
  async init() { await this.topUp(); log(`pool warmed (${this.idle.length}/${POOL} idle)`) }

  // Concurrency gate: bound concurrent in-flight turns → bounds live `claude` processes.
  private async acquire() { while (this.active >= MAX_ACTIVE) await Bun.sleep(25); this.active++ }
  private release() { if (this.active > 0) this.active-- }

  private evictThreads() {
    while (this.threads.size > MAX_THREADS) {
      const k = this.threads.keys().next().value as string | undefined
      if (k === undefined) break
      const old = this.threads.get(k); this.threads.delete(k); old?.kill()
    }
  }

  async runTurn(prompt: string, opts: { threadKey?: string; onDelta?: (s: string) => void } = {}): Promise<{ text: string; status: string }> {
    await this.acquire()
    try {
      if (opts.threadKey) {
        // sticky warm thread (true multi-turn reuse — the codex thread analog)
        let s = this.threads.get(opts.threadKey)
        if (!s || s.dead || s.turns >= THREAD_MAX_TURNS) {
          if (s) { this.threads.delete(opts.threadKey); s.kill() }
          s = await this.spawnSession(); this.threads.set(opts.threadKey, s); this.evictThreads()
        }
        return await s.runTurn(prompt, opts.onDelta)
      }
      // no thread_key: lease a pre-warmed session (or spawn one), run, then retire/recycle.
      let s = this.idle.pop()
      if (!s || s.dead) s = await this.spawnSession()
      void this.topUp() // refill in the background so the next job stays warm
      try {
        return await s.runTurn(prompt, opts.onDelta)
      } finally {
        if (s.dead || s.turns >= SESSION_MAX_TURNS) s.kill() // strict per-job isolation by default
        else this.idle.push(s)                               // reuse enabled when SESSION_MAX_TURNS > 1
      }
    } finally {
      this.release()
    }
  }

  usedPct(): number | null {
    for (const s of [...this.idle, ...this.threads.values()]) if (typeof s.usedPct === 'number') return s.usedPct
    return null
  }
  stop() { for (const s of [...this.idle, ...this.threads.values()]) s.kill() }
}

// ── A2A bridge (EXACT mirror of codex_gateway's onInbound) ─────────────────────
const pool = new ClaudePool()
let a2a: A2AChannel

const sendMsg = (to: string, body: any, extra: any = {}) =>
  a2a.callTool('a2a_send', { to, body: typeof body === 'string' ? body : JSON.stringify(body), ...extra })

async function onInbound(content: string, attrs: any) {
  if (attrs?.feed !== 'a2a' || attrs?.kind !== 'direct') return
  if (attrs.type !== 'request') return // jobs arrive as requests
  const from = attrs.from, corr = attrs.id

  // parse the canonical job, or fall back to a plain-text prompt
  let parsed: any = null
  try { parsed = JSON.parse(content) } catch {}
  const isJob = parsed?.schema === 'claude.job.request.v1'
  const job = isJob ? parsed : { job_id: jid(), input: [{ type: 'text', text: content }], stream_topic: null }
  const job_id = job.job_id || jid()
  // Resolve a possibly claim-checked input: a large prompt (e.g. the fusion judge
  // fan-in or opus-leg input) rides a Redis blob + tiny `input_ref` because the bus
  // caps `args.body`; recover the FULL prompt here. No `input_ref` → the inline join,
  // unchanged. Redis is opened lazily, so a ref-less job touches it not at all.
  const inputMiss = { reason: '' } // set iff input_ref was present but the blob couldn't be recovered
  const prompt = (await resolveJobInput(getRedis(), job, (r) => { inputMiss.reason = r; log(`input blob ${r} (job ${job_id})`) })) || String(content)
  const tid = typeof job.triage_id === 'string' ? { triage_id: job.triage_id } : {}
  const threadKey = typeof job.thread_key === 'string' ? job.thread_key : undefined
  const stream_topic = job.stream_topic ?? null

  // admission: always accept (Claude CLI exposes no cheap pre-turn budget gate; we
  // report the latest rate-limit utilization, like codex's primary_used_pct, but never
  // reject — budget admission was spec'd optional). MUST be a correlated reply.
  const used = pool.usedPct()
  await sendMsg(from, { schema: 'claude.job.accepted.v1', job_id, ...tid, stream_topic, primary_used_pct: used }, { type: 'reply', corr })
  log(`ACCEPT ${job_id} from ${from} (util ${used ?? '?'}%, thread ${threadKey ?? '-'})`)

  // FAIL-CLOSED on a claim-checked-input miss: if `input_ref` was present but the blob
  // couldn't be recovered (miss/expiry/sha/len), REFUSE to run on the truncated inline
  // preview — that silent partial-prompt is exactly what claim-check exists to prevent.
  // Loud failure with the real reason instead. (Cross-model review, P1.)
  if (inputMiss.reason && isJob) {
    await sendMsg(from, { schema: 'claude.job.failed.v1', job_id, ...tid, error: `input_blob_${inputMiss.reason}` }, { type: 'reply', corr }).catch(() => {})
    log(`FAIL ${job_id}: input blob ${inputMiss.reason} — refusing to run on a partial prompt`)
    return
  }

  // Run the turn in the BACKGROUND so the inbox consume loop keeps flowing: this lets
  // the warm pool serve concurrent jobs and keeps an 8h turn from holding the loop /
  // piling up ack_wait redeliveries. (Channel id-dedup preserves job-once semantics.)
  void (async () => {
    try {
      let seq = 0
      const { text, status } = await pool.runTurn(prompt, {
        threadKey,
        onDelta: stream_topic ? (d) => { void sendMsg(`topic:${stream_topic}`, { schema: 'claude.job.delta.v1', job_id, ...tid, seq: seq++, text: d }) } : undefined,
      })
      // final delta marker so streaming consumers know the stream ended (lossy-tolerant)
      if (stream_topic) void sendMsg(`topic:${stream_topic}`, { schema: 'claude.job.delta.v1', job_id, ...tid, seq: seq++, text: '', final: true })

      const bodyCap = a2a.getMaxSendBytes()
      if (isJob) {
        // byte-safe completed reply: inline if it fits; else claim-check (lossless blob+ref
        // + marked preview); else (Redis down) explicit byte-truncation. Never silent.
        const wrap: WrapFn = (t, extra) => ({ schema: 'claude.job.completed.v1', job_id, ...tid, status, output: t, ...(extra ?? {}) })
        const plan = planReply(text, bodyCap, wrap)
        let reply: any
        let putRef: BlobRef | null = null
        if (plan.kind === 'inline') {
          reply = wrap(plan.output)
        } else if (plan.kind === 'claimcheck') {
          try {
            putRef = await putBlob(getRedis(), { text })
            reply = wrap(plan.preview, { output_preview: true, result_ref: putRef.result_ref, sha256: putRef.sha256, len: putRef.len, encoding: putRef.encoding, expires_at: putRef.expires_at, truncated: false })
          } catch (be) {
            const tp = fitTruncate(text, bodyCap, wrap)
            reply = wrap(tp.output, { truncated: true, original_bytes: tp.original_bytes, emitted_bytes: tp.emitted_bytes })
            log(`BLOB-FAIL ${job_id}: ${be} — byte-truncated ${tp.original_bytes}->${tp.emitted_bytes}B`)
          }
        } else {
          reply = wrap(plan.output, { truncated: true, original_bytes: plan.original_bytes, emitted_bytes: plan.emitted_bytes })
          log(`TRUNCATE ${job_id}: ${plan.original_bytes}->${plan.emitted_bytes}B (cap ${bodyCap})`)
        }
        const res = await sendMsg(from, reply, { type: 'reply', corr })
        if (!sentOk(res)) {
          if (putRef) await delBlob(getRedis(), putRef.result_ref).catch(() => {})
          await sendMsg(from, { schema: 'claude.job.failed.v1', job_id, ...tid, error: 'reply_too_large' }, { type: 'reply', corr }).catch(() => {})
          log(`SEND-FAIL ${job_id}: completed reply rejected — sent failed.v1 (no DONE)`)
        } else {
          log(`DONE ${job_id} status=${status} ${text.length}c${plan.kind !== 'inline' ? ` [${plan.kind}]` : ''}`)
        }
      } else {
        // plain-text form: body IS the string (no schema to carry a ref) → inline or byte-truncate.
        const full = text || `(no output, status=${status})`
        let out = full
        if (utf8ByteLength(full) > bodyCap) {
          const marker = '\n…[truncated]'
          const mb = utf8ByteLength(marker)
          out = bodyCap >= mb ? truncateToBytes(full, bodyCap - mb).text + marker : truncateToBytes(full, bodyCap).text
          log(`TRUNCATE ${job_id} (plain): ${utf8ByteLength(full)}->${utf8ByteLength(out)}B`)
        }
        const res = await sendMsg(from, out, { type: 'reply', corr })
        if (!sentOk(res)) { await sendMsg(from, '(error: reply_too_large)', { type: 'reply', corr }).catch(() => {}); log(`SEND-FAIL ${job_id} (plain): rejected`) }
        else log(`DONE ${job_id} status=${status} ${text.length}c`)
      }
    } catch (e) {
      await sendMsg(from, { schema: 'claude.job.failed.v1', job_id, ...tid, error: e instanceof Error ? e.message : String(e) }, { type: 'reply', corr }).catch(() => {})
      log(`FAIL ${job_id}: ${e}`)
    }
  })()
}

export async function main() {
  a2a = new A2AChannel(onInbound, { enabled: true, agentId: AGENT_ID })
  await pool.init()
  await a2a.start()
  if (!a2a.isStarted()) {
    log(`FATAL: could not join the bus as '${AGENT_ID}' (duplicate agent-id? another instance alive, or stale presence). Exiting.`)
    pool.stop(); process.exit(1)
  }
  log(`bus joined as '${AGENT_ID}' (Opus via Claude CLI, util ${pool.usedPct() ?? '?'}%). canonical claude.job.* contract ready.`)

  for (const sig of ['SIGTERM', 'SIGINT'] as const) process.on(sig, async () => { await a2a.stop().catch(() => {}); pool.stop(); process.exit(0) })
}

if (import.meta.main) await main()
