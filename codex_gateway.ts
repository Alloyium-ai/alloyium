#!/usr/bin/env bun
// codex-gateway — the CANONICAL full-duplex codex agent on the A2A bus.
//
// Implements the codex-gateway spec's contract (code = spec):
//   inbound  direct request: codex.job.request.v1 { job_id, input[], stream_topic?, thread_key?, sandbox?, budget_policy? }
//   reply    direct:         codex.job.accepted.v1 | codex.job.rejected.v1 (admission), then codex.job.completed.v1 | codex.job.failed.v1
//   stream   topic:<stream_topic>: codex.job.delta.v1 { job_id, seq, text }   (non-durable; requester JOINs first)
//   approval direct:         codex.approval.request.v1 -> APPROVER, awaits codex.approval.decision.v1
// Back-compat: a plain-text request body (no schema) is treated as a one-shot prompt and answered with plain text.
//
// Owns a `codex app-server`. ADVISORY-ONLY: codex defaults to sandbox=read-only; any
// command/edit approval is ROUTED to a human approver over the bus (never auto-accepted).
// Capital fire is never on the bus. Env from the agent's *.a2a.env (A2A_AGENT_ID, keys, NATS/REDIS, SUBS_KEY).
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { planReply, fitTruncate, putBlob, delBlob, truncateToBytes, utf8ByteLength, sentOk, resolveJobInput, type WrapFn, type BlobRef } from './output_transport.ts'
import { authorizeWriteJob } from './codex_build_authz.ts'
import { CODEX_DETACHED_NO_INTERACTIVE_PROMPT_DIRECTIVE, buildCodexA2AToolsConfigArgs, buildCodexAgentPrompt, codexA2AToolsEnabled, codexA2AToolsMode } from './codex_agent_tools.ts'

const AGENT_ID = process.env.A2A_AGENT_ID ?? 'codex-gw'
const APPROVER = process.env.CODEX_GW_APPROVER ?? '' // who approves command/edit escalations; '' => the requester
const BUDGET_MAX = Number(process.env.CODEX_GW_BUDGET_MAX_PCT ?? 92) // reject admission above this primary used%
const MODEL = process.env.CODEX_GW_MODEL
const EFFORT = process.env.CODEX_GW_EFFORT
const APPROVAL_TIMEOUT_MS = Number(process.env.CODEX_GW_APPROVAL_TIMEOUT_MS ?? 120_000)
const WRITE_ENABLED = process.env.CODEX_GW_ALLOW_WRITE === '1' || process.env.CODEX_GW_ALLOW_WRITE?.toLowerCase() === 'true'
const WRITE_ALLOWLIST = (process.env.CODEX_GW_WRITE_ALLOWLIST ?? 'dev-pm').split(',').map((s) => s.trim()).filter(Boolean)
const CWD_ROOTS = (process.env.CODEX_BUILD_CWD_ROOTS ?? '').split(',').map((s) => s.trim()).filter(Boolean)
const CODEX_EXEC_SANDBOX = normalizeCodexSandboxMode(process.env.CODEX_GW_CODEX_SANDBOX)
const CODEX_WORKSPACE_WRITE_EXEC_SANDBOX = normalizeCodexSandboxMode(process.env.CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX)
const THREAD_CACHE_MAX = Math.max(1, Number(process.env.CODEX_GW_THREAD_CACHE_MAX ?? 256) || 256)
const THREAD_CACHE_TTL_MS = Math.max(1, Number(process.env.CODEX_GW_THREAD_CACHE_TTL_MS ?? 2 * 60 * 60 * 1000) || 2 * 60 * 60 * 1000)
const PEER_INBOX_MAX = Math.max(0, Number(process.env.CODEX_GW_PEER_INBOX_MAX ?? 20) || 20)
const PEER_INBOX_ITEM_MAX_BYTES = Math.max(256, Number(process.env.CODEX_GW_PEER_INBOX_ITEM_MAX_BYTES ?? 2048) || 2048)
const CHANNELS_DIR = process.env.CHANNELS ?? import.meta.dir
const A2A_TOOLS_ENABLED = codexA2AToolsEnabled()
const A2A_TOOLS_MODE = codexA2AToolsMode()
const A2A_TOOLS_STARTUP_TIMEOUT_SEC = positiveNumber(process.env.CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC)
const A2A_TOOLS_TOOL_TIMEOUT_SEC = positiveNumber(process.env.CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC)
const AGENT_PREAMBLE = process.env.CODEX_GW_AGENT_PREAMBLE?.trim()
// Output transport (RCA Issue 1): the completed reply is byte-sized to THIS agent's
// A2A send cap (a2a.getMaxSendBytes()). Large output is delivered LOSSLESSLY by
// claim-check (TTL'd Redis blob + ref + marked preview) or, if Redis is down, by
// explicit byte-truncation — never the old silent char-slice. The deprecated
// CODEX_GW_MAX_OUTPUT knob is retired.
const log = (...a: any[]) => console.error(`[codex-gw ${AGENT_ID}]`, ...a)
let jobseq = 0
const jid = () => `cj-${AGENT_ID}-${(jobseq++).toString(36)}-${(Date.now() % 1e7).toString(36)}`

function positiveNumber(value: unknown): number | undefined {
  if (value == null || value === '') return undefined
  const n = Number(value)
  return Number.isFinite(n) && n > 0 ? n : undefined
}

export function describeGatewayError(e: unknown): string {
  if (e instanceof Error) return e.message
  if (typeof e === 'string') return e
  if (e && typeof e === 'object') {
    const rec = e as Record<string, unknown>
    if (typeof rec.message === 'string' && rec.message) return rec.message
    try { return JSON.stringify(e) } catch {}
  }
  return String(e)
}

export type CodexSandboxMode = 'read-only' | 'workspace-write' | 'danger-full-access'

/**
 * Normalize Codex sandbox names plus the common "yolo"/"no sandbox" aliases.
 *
 * The public A2A job sandbox remains `read-only` or `workspace-write`; this only
 * controls what the gateway sends to `codex app-server` after its own admission
 * and cwd authorization checks have passed.
 */
export function normalizeCodexSandboxMode(value: unknown): CodexSandboxMode | null {
  if (typeof value !== 'string') return null
  const v = value.trim().toLowerCase()
  if (!v) return null
  if (v === 'read-only' || v === 'readonly') return 'read-only'
  if (v === 'workspace-write' || v === 'workspace' || v === 'write') return 'workspace-write'
  if (['danger-full-access', 'full-access', 'danger', 'dangerous', 'yolo', 'no-sandbox', 'none'].includes(v)) return 'danger-full-access'
  return null
}

export function resolveCodexExecutionSandbox(
  requestSandbox: string,
  opts: { defaultSandbox?: string | null; workspaceWriteSandbox?: string | null } = {},
): CodexSandboxMode {
  const requested = normalizeCodexSandboxMode(requestSandbox) ?? 'read-only'
  const defaultSandbox = normalizeCodexSandboxMode(opts.defaultSandbox)
  const workspaceWriteSandbox = normalizeCodexSandboxMode(opts.workspaceWriteSandbox)
  if (requested === 'workspace-write') return workspaceWriteSandbox ?? defaultSandbox ?? 'workspace-write'
  return defaultSandbox ?? 'read-only'
}

// Lazy Redis for claim-check blobs; opened on first large reply, reused after.
let _redis: RedisClient | null = null
const getRedis = (): RedisClient => (_redis ??= new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379'))

/** Execution context that a warm codex thread is allowed to be reused under. */
export interface GatewayThreadContext {
  sandbox: string
  cwd: string
}

/** Options for the in-process codex thread cache. */
export interface GatewayThreadCacheOptions {
  maxEntries?: number
  ttlMs?: number
  now?: () => number
}

type CachedThread = GatewayThreadContext & { threadId: string; lastUsedMs: number }

/**
 * In-process thread_key cache with fail-closed reuse semantics.
 *
 * A cached thread is reused only when the requested `{sandbox,cwd}` exactly matches
 * the context used to create it. A read-only job therefore cannot inherit an old
 * workspace-write thread, and a workspace-write job must start a fresh thread when
 * its authorized cwd changes or the prior entry expired/evicted.
 */
export class GatewayThreadCache {
  private entries = new Map<string, CachedThread>()
  private readonly maxEntries: number
  private readonly ttlMs: number
  private readonly now: () => number

  constructor(opts: GatewayThreadCacheOptions = {}) {
    const maxEntries = opts.maxEntries ?? THREAD_CACHE_MAX
    const ttlMs = opts.ttlMs ?? THREAD_CACHE_TTL_MS
    this.maxEntries = Number.isFinite(maxEntries) && maxEntries > 0 ? Math.floor(maxEntries) : THREAD_CACHE_MAX
    this.ttlMs = Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : THREAD_CACHE_TTL_MS
    this.now = opts.now ?? Date.now
  }

  /** Remove expired entries and enforce bounded cache growth. */
  cleanup(): void {
    const now = this.now()
    for (const [key, entry] of this.entries) {
      if (now - entry.lastUsedMs > this.ttlMs) this.entries.delete(key)
    }
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value
      if (oldest === undefined) break
      this.entries.delete(oldest)
    }
  }

  /** Return a cached thread id only if sandbox and cwd match exactly. */
  get(threadKey: string | undefined, ctx: GatewayThreadContext): string | null {
    if (!threadKey) return null
    this.cleanup()
    const entry = this.entries.get(threadKey)
    if (!entry) return null
    if (entry.sandbox !== ctx.sandbox || entry.cwd !== ctx.cwd) {
      this.entries.delete(threadKey)
      return null
    }
    this.entries.delete(threadKey)
    this.entries.set(threadKey, { ...entry, lastUsedMs: this.now() })
    return entry.threadId
  }

  /** Cache a thread id for one exact sandbox/cwd context. */
  set(threadKey: string | undefined, threadId: string, ctx: GatewayThreadContext): void {
    if (!threadKey) return
    this.cleanup()
    this.entries.delete(threadKey)
    this.entries.set(threadKey, { threadId, sandbox: ctx.sandbox, cwd: ctx.cwd, lastUsedMs: this.now() })
    this.cleanup()
  }

  /** Remove one cached thread key, for example after a timed-out turn. */
  delete(threadKey: string | undefined): boolean {
    if (!threadKey) return false
    return this.entries.delete(threadKey)
  }

  /** Current live cache size after cleanup. */
  size(): number {
    this.cleanup()
    return this.entries.size
  }
}

/** Gateway-local workspace-write preflight result before Redis authz. */
export type WorkspaceWriteJobValidation =
  | { ok: true; threadKey: string }
  | { ok: false; reason: 'write-unauthorized'; detail: 'write-approval-policy-not-never' | 'write-thread-key-required' | 'signing-off' }

/**
 * Validate gateway-only invariants for workspace-write jobs.
 *
 * Write jobs require A2A signing to be on so the requester identity is
 * cryptographically verified (env.from is attacker-spoofable under signing-off),
 * must run with `approval_policy:'never'` so requesters cannot approve their own
 * escalations, and must carry a non-empty thread_key so Redis ownership binding is
 * always applied by authorizeWriteJob.
 */
export function validateWorkspaceWriteJob(job: { thread_key?: unknown }, approvalPolicy: string, signingEnabled: boolean): WorkspaceWriteJobValidation {
  if (!signingEnabled) return { ok: false, reason: 'write-unauthorized', detail: 'signing-off' }
  if (approvalPolicy !== 'never') return { ok: false, reason: 'write-unauthorized', detail: 'write-approval-policy-not-never' }
  if (typeof job.thread_key !== 'string' || job.thread_key.trim().length === 0) return { ok: false, reason: 'write-unauthorized', detail: 'write-thread-key-required' }
  return { ok: true, threadKey: job.thread_key.trim() }
}

/**
 * A write-enabled build gateway must run EVERY job with `approval_policy:'never'`.
 *
 * approval_policy is requester-controlled and codex approvals route back to the requester
 * (`APPROVER || lastRequester`), and the sandbox normalization sends any non-'workspace-write'
 * value to 'read-only' WITHOUT the workspace-write signing/authz gate. So an 'on-request' job —
 * even one normalized to read-only — lets a requester SELF-APPROVE a codex escalation to write,
 * bypassing the signing + allowlist + cwd-authz checks (#38 P1, GPT-5.5 gate leg). The build
 * gateway is autonomous Model-B (no human-in-the-loop), so non-'never' is never legitimate there.
 * Read-only gateways (writeEnabled=false) may honor the requested policy.
 */
export function approvalPolicyAllowed(approvalPolicy: string, writeEnabled: boolean): boolean {
  return !writeEnabled || approvalPolicy === 'never'
}

export type CodexMcpElicitationResult =
  | { action: 'accept'; content: Record<string, never> }
  | { action: 'decline' }

/**
 * Codex app-server asks for MCP tool permission via `mcpServer/elicitation/request`.
 *
 * The gateway-owned `a2a_tools` server is the intentionally exposed A2A/brain/kai/vault
 * tool surface for first-class agents, so approve those tool-call elicitations in-process.
 * Unknown MCP servers stay fail-closed; do not auto-approve Codex's built-in app plugins
 * or future server classes by accident.
 */
export function codexMcpElicitationResult(m: unknown): CodexMcpElicitationResult {
  const req = m && typeof m === 'object' ? m as Record<string, any> : {}
  const params = req.params && typeof req.params === 'object' ? req.params as Record<string, any> : {}
  const meta = params._meta && typeof params._meta === 'object' ? params._meta as Record<string, any> : {}
  if (params.serverName === 'a2a_tools' && meta.codex_approval_kind === 'mcp_tool_call') {
    return { action: 'accept', content: {} }
  }
  return { action: 'decline' }
}

/**
 * Back-compat plain-text requests are always read-only. On a write-enabled
 * gateway, they still must use `never` so the global no-self-approval invariant
 * holds; read-only gateways can keep the older interactive default.
 */
export function defaultPlainRequestApprovalPolicy(writeEnabled: boolean): 'never' | 'on-request' {
  return writeEnabled ? 'never' : 'on-request'
}

export function codexDetachedThreadOverrides(): { developerInstructions: string; config: { tools: { request_user_input: false } } } {
  return {
    developerInstructions: CODEX_DETACHED_NO_INTERACTIVE_PROMPT_DIRECTIVE,
    config: {
      tools: { request_user_input: false },
    },
  }
}

export type PeerInboxEvent = {
  from: string
  type: string
  body: string
  id?: string
  corr?: string
  ts?: string
}

export function buildPeerInboxContext(events: PeerInboxEvent[], maxBodyBytes = PEER_INBOX_ITEM_MAX_BYTES): string {
  if (!events.length) return ''
  const lines = events.map((ev) => {
    const body = truncateToBytes(ev.body, maxBodyBytes)
    const meta = [
      `from=${ev.from}`,
      `type=${ev.type}`,
      ev.id ? `id=${ev.id}` : '',
      ev.corr ? `corr=${ev.corr}` : '',
      ev.ts ? `ts=${ev.ts}` : '',
    ].filter(Boolean).join(' ')
    const suffix = body.truncated ? `\n  [body truncated ${body.originalBytes}->${body.emittedBytes}B]` : ''
    return `- ${meta}\n  body: ${body.text}${suffix}`
  })
  return [
    'Recent A2A inbox events received asynchronously since the previous turn.',
    'Treat them as advisory peer context; do not mechanically reply.',
    ...lines,
  ].join('\n')
}

// ── codex app-server JSON-RPC client ──────────────────────────────────────────
type ApprovalFn = (m: any) => Promise<'accept' | 'decline'>
/** Pull a human error string out of a codex app-server turn event (shape varies by codex version). */
function extractTurnError(params: any): string {
  if (!params) return ''
  const e = params.error ?? params.failure ?? params.turn?.error ?? params.message ?? params.reason
  if (!e) return ''
  if (typeof e === 'string') return e.slice(0, 400)
  return String(e.message ?? e.detail ?? e.code ?? JSON.stringify(e)).slice(0, 400)
}
class CodexAppServer {
  private proc: ReturnType<typeof Bun.spawn>
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: any) => void }>()
  private idc = 0
  private handlers = new Set<(m: any) => void>()
  private threads = new GatewayThreadCache()
  private stderrRing: string[] = []
  onApproval: ApprovalFn = async () => 'decline'
  constructor() {
    const args = ['codex', 'app-server', ...buildCodexA2AToolsConfigArgs({
      enabled: A2A_TOOLS_ENABLED,
      channelsDir: CHANNELS_DIR,
      toolsMode: A2A_TOOLS_MODE,
      agentId: AGENT_ID,
      signingKeyPath: process.env.A2A_SIGNING_KEY,
      shimCommand: process.env.CODEX_GW_A2A_SHIM_BIN ?? process.env.A2A_SHIM_BIN,
      coreSock: process.env.A2A_CORE_SOCK,
      sigAlg: process.env.A2A_SIG_ALG ?? 'ed25519',
      transportAuth: process.env.A2A_TRANSPORT_AUTH,
      natsUrl: process.env.NATS_URL,
      redisUrl: process.env.REDIS_URL,
      brainUrl: process.env.BRAIN_URL,
      kaiHttpUrl: process.env.KAI_HTTP_URL,
      kaiWsUrl: process.env.KAI_WS_URL,
      kaiTokenPath: process.env.KAI_TOKEN_PATH,
      maxSendBytes: process.env.A2A_MAX_SEND_BYTES,
      inheritEnvVars: process.env.KAI_TOKEN ? ['KAI_TOKEN'] : [],
      startupTimeoutSec: A2A_TOOLS_STARTUP_TIMEOUT_SEC,
      toolTimeoutSec: A2A_TOOLS_TOOL_TIMEOUT_SEC,
      required: process.env.CODEX_GW_A2A_TOOLS_REQUIRED === '1' || process.env.CODEX_GW_A2A_TOOLS_REQUIRED?.toLowerCase() === 'true',
    })]
    this.proc = Bun.spawn(args, { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' })
    log(`app-server spawned (a2a_tools=${A2A_TOOLS_ENABLED ? 'on' : 'off'}, mode=${A2A_TOOLS_MODE})`)
    void this.readLoop(); void this.stderrLoop()
  }
  private send(o: any) { this.proc.stdin.write(JSON.stringify(o) + '\n'); this.proc.stdin.flush() }
  private req(method: string, params?: any): Promise<any> {
    const id = this.idc++; this.send({ id, method, params })
    return new Promise((res, rej) => this.pending.set(id, { resolve: res, reject: rej }))
  }
  private notify(method: string, params?: any) { this.send({ method, params }) }
  private async readLoop() {
    const dec = new TextDecoder(); let buf = ''
    for await (const chunk of this.proc.stdout) {
      buf += dec.decode(chunk); let nl: number
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1)
        if (!line) continue
        let m: any; try { m = JSON.parse(line) } catch { continue }
        if (m.id !== undefined && (m.result !== undefined || m.error !== undefined)) {
          const p = this.pending.get(m.id); if (p) { this.pending.delete(m.id); m.error ? p.reject(new Error(describeGatewayError(m.error))) : p.resolve(m.result) }
        } else if (typeof m.method === 'string' && m.method.includes('requestApproval') && m.id !== undefined) {
          // route to the human approver over the bus; default decline on any failure
          this.onApproval(m).then((d) => this.send({ id: m.id, result: { decision: d } })).catch(() => this.send({ id: m.id, result: { decision: 'decline' } }))
        } else if (m.method === 'mcpServer/elicitation/request' && m.id !== undefined) {
          const result = codexMcpElicitationResult(m)
          const params = m.params ?? {}
          log(`mcp elicitation ${result.action}: server=${params.serverName ?? '?'} kind=${params._meta?.codex_approval_kind ?? '?'} tool=${params._meta?.tool_description ?? '?'}`)
          this.send({ id: m.id, result })
        } else {
          for (const h of this.handlers) h(m)
        }
      }
    }
  }
  private async stderrLoop() {
    const dec = new TextDecoder()
    for await (const chunk of this.proc.stderr) {
      const text = dec.decode(chunk).trim()
      if (!text) continue
      for (const line of text.split(/\r?\n/)) { log(`app-server stderr: ${line}`); this.stderrRing.push(line); if (this.stderrRing.length > 40) this.stderrRing.shift() }
    }
  }
  /** Recent app-server stderr (error-ish lines preferred) — surfaced on an in-band turn failure
   *  (e.g. usage-limit/quota) that codex reports as status=failed with no JSON-RPC error + 0 output. */
  recentErrorContext(): string {
    const errish = this.stderrRing.filter((l) => /error|usage limit|rate.?limit|quota|denied|unauthoriz|forbidden|fail/i.test(l))
    const lines = (errish.length ? errish : this.stderrRing).slice(-4)
    return lines.join(' | ').slice(0, 400)
  }
  async init() {
    await this.req('initialize', { clientInfo: { name: 'codex-gateway', title: 'A2A Codex Gateway', version: '0.2.0' }, capabilities: { experimentalApi: true, requestAttestation: false } })
    this.notify('initialized'); log('app-server initialized')
  }
  private async thread(threadKey: string | undefined, opts: { sandbox: string; approvalPolicy: string; cwd: string }): Promise<string> {
    const codexSandbox = resolveCodexExecutionSandbox(opts.sandbox, {
      defaultSandbox: CODEX_EXEC_SANDBOX,
      workspaceWriteSandbox: CODEX_WORKSPACE_WRITE_EXEC_SANDBOX,
    })
    const ctx = { sandbox: `${opts.sandbox}->${codexSandbox}`, cwd: opts.cwd }
    const cached = this.threads.get(threadKey, ctx)
    if (cached) return cached
    if (codexSandbox !== opts.sandbox) log(`codex sandbox override for thread: request=${opts.sandbox} app-server=${codexSandbox} cwd=${opts.cwd}`)
    const start: any = {
      approvalPolicy: opts.approvalPolicy,
      sandbox: codexSandbox,
      cwd: opts.cwd,
      ...codexDetachedThreadOverrides(),
    }
    if (MODEL) start.model = MODEL
    const th = await this.req('thread/start', start)
    const id = th.threadId ?? th.thread?.id ?? th.id
    this.threads.set(threadKey, id, ctx)
    return id
  }
  async runTurn(prompt: string, opts: { threadKey?: string; sandbox?: string; approvalPolicy?: string; cwd?: string; onDelta?: (s: string) => void } = {}): Promise<{ text: string; status: string; timedOut: boolean; timeoutMs: number; error?: string }> {
    const threadId = await this.thread(opts.threadKey, { sandbox: opts.sandbox ?? 'read-only', approvalPolicy: opts.approvalPolicy ?? 'never', cwd: opts.cwd ?? '/tmp' })
    let text = ''; let status = 'unknown'; let done = false; let errorDetail = ''
    const h = (m: any) => {
      const mm: string = m.method ?? ''
      if (mm.includes('agentMessage')) { const d = m.params?.delta ?? m.params?.text; if (typeof d === 'string') { text += d; opts.onDelta?.(d) } }
      else if (mm === 'turn/completed') { status = m.params?.status ?? m.params?.turn?.status ?? 'completed'; errorDetail ||= extractTurnError(m.params); done = true }
      else if (mm === 'turn/failed') { errorDetail ||= extractTurnError(m.params); status = (m.params?.status as string) || 'failed'; done = true }
    }
    this.handlers.add(h)
    try {
      const turn: any = { threadId, input: [{ type: 'text', text: prompt, text_elements: [] }] }
      if (EFFORT) turn.effort = EFFORT
      await this.req('turn/start', turn)
      const TURN_TIMEOUT_MS = Number(process.env.CODEX_GW_TURN_TIMEOUT_MS ?? 28_800_000) // 8h ceiling: ML/deep-analysis turns must never be prematurely killed; per-job/client bounds override DOWNWARD
      const t0 = Date.now(); while (!done && Date.now() - t0 < TURN_TIMEOUT_MS) await Bun.sleep(50)
      if (!done) status = 'timeout'
      if (!done) this.threads.delete(opts.threadKey)
      // codex sometimes reports an in-band failure (e.g. usage-limit/quota) as status=failed with no
      // JSON-RPC error + zero output — recover the reason from recent app-server stderr so the failure
      // is never silent.
      if (done && status !== 'completed' && !errorDetail) errorDetail = this.recentErrorContext()
      return { text: text.trim(), status, timedOut: !done, timeoutMs: TURN_TIMEOUT_MS, error: errorDetail || undefined }
    } finally { this.handlers.delete(h) }
  }
  async primaryUsedPct(): Promise<number | null> { try { const r = await this.req('account/rateLimits/read'); return r?.rateLimits?.primary?.usedPercent ?? null } catch { return null } }
  stop() { try { this.proc.kill() } catch {} }
}

// ── A2A bridge ────────────────────────────────────────────────────────────────
let codex: CodexAppServer
const pendingApprovals = new Map<string, (d: 'accept' | 'decline') => void>()
let a2a: A2AChannel
const peerInbox: PeerInboxEvent[] = []

const sendMsg = (to: string, body: any, extra: any = {}) =>
  a2a.callTool('a2a_send', { to, body: typeof body === 'string' ? body : JSON.stringify(body), ...extra })

let lastRequester = ''

function recordPeerInboxEvent(content: string, attrs: any): void {
  if (PEER_INBOX_MAX <= 0) return
  const ev: PeerInboxEvent = {
    from: String(attrs.from ?? 'unknown'),
    type: String(attrs.type ?? 'msg'),
    body: content,
    ...(attrs.id ? { id: String(attrs.id) } : {}),
    ...(attrs.corr ? { corr: String(attrs.corr) } : {}),
    ...(attrs.ts ? { ts: String(attrs.ts) } : {}),
  }
  peerInbox.push(ev)
  while (peerInbox.length > PEER_INBOX_MAX) peerInbox.shift()
  log(`A2A-INBOX queued ${ev.type} from ${ev.from}${ev.corr ? ` corr=${ev.corr}` : ''}`)
}

function drainPeerInboxContext(): string {
  if (!peerInbox.length) return ''
  return buildPeerInboxContext(peerInbox.splice(0), PEER_INBOX_ITEM_MAX_BYTES)
}

function configureApprovals() {
  // route a codex command/edit approval to a human over the bus
  codex.onApproval = async (m: any) => {
    const approval_id = `ap-${(Date.now() % 1e7).toString(36)}-${Math.floor(jobseq).toString(36)}`
    const approver = APPROVER || lastRequester || AGENT_ID
    log(`approval needed (${m.method}) -> routing to ${approver}`)
    const decided = new Promise<'accept' | 'decline'>((resolve) => {
      pendingApprovals.set(approval_id, resolve)
      setTimeout(() => { if (pendingApprovals.delete(approval_id)) resolve('decline') }, APPROVAL_TIMEOUT_MS)
    })
    await sendMsg(approver, { schema: 'codex.approval.request.v1', approval_id, method: m.method, params: m.params }, { type: 'request' }).catch(() => {})
    return decided
  }
}

async function onInbound(content: string, attrs: any) {
  if (attrs?.feed !== 'a2a' || attrs?.kind !== 'direct') return
  // approval decisions come back as direct replies/msgs
  let parsed: any = null
  try { parsed = JSON.parse(content) } catch {}
  if (parsed?.schema === 'codex.approval.decision.v1' && parsed.approval_id) {
    const r = pendingApprovals.get(parsed.approval_id)
    if (r) { pendingApprovals.delete(parsed.approval_id); r(parsed.decision === 'accept' ? 'accept' : 'decline') }
    return
  }
  if (attrs.type !== 'request') {
    recordPeerInboxEvent(content, attrs)
    return
  }
  const from = attrs.from, corr = attrs.id
  lastRequester = from

  // parse the canonical job, or fall back to a plain-text prompt
  const isJob = parsed?.schema === 'codex.job.request.v1'
  const job = isJob ? parsed : { job_id: jid(), input: [{ type: 'text', text: content }], stream_topic: null, sandbox: 'read-only' }
  const job_id = job.job_id || jid()
  // Resolve a possibly claim-checked input: a large prompt rides a Redis blob + tiny
  // `input_ref` (the bus caps `args.body`); recover the FULL prompt here. No `input_ref`
  // → the inline join, unchanged (triage / eval callers are byte-for-byte identical).
  const inputMiss = { reason: '' } // set iff input_ref was present but the blob couldn't be recovered
  const prompt = (await resolveJobInput(getRedis(), job, (r) => { inputMiss.reason = r; log(`input blob ${r} (job ${job_id})`) })) || String(content)
  const sandbox = job.sandbox === 'workspace-write' ? 'workspace-write' : 'read-only'
  // §5 triage-path contract: approval_policy + cwd come from the REQUEST (a
  // read-only triage job carries approval_policy:'never'). A bare plain-text
  // prompt is read-only; on write-enabled gateways it must also use 'never'
  // because approvals route back to the requester.
  const approvalPolicy = isJob ? (typeof job.approval_policy === 'string' ? job.approval_policy : 'never') : defaultPlainRequestApprovalPolicy(WRITE_ENABLED)
  const cwd = isJob && typeof job.cwd === 'string' && job.cwd ? job.cwd : '/tmp'
  const tid = typeof job.triage_id === 'string' ? { triage_id: job.triage_id } : {}
  let effectiveCwd = cwd
  let effectiveThreadKey = job.thread_key

  // #38 P1: on a write-enabled gateway, refuse any job whose approval_policy is not 'never' — for ALL
  // sandboxes (a non-'workspace-write' value normalizes to read-only and skips the write gate below, but
  // an 'on-request' policy still lets the requester self-approve a codex escalation to write).
  if (!approvalPolicyAllowed(approvalPolicy, WRITE_ENABLED)) {
    log(`REJECT ${job_id}: approval_policy '${approvalPolicy}' not allowed on a write-enabled gateway (self-approval escalation risk) from ${from}`)
    await sendMsg(from, { schema: 'codex.job.rejected.v1', job_id, ...tid, reason: 'write-unauthorized', detail: 'approval-policy-not-never' }, { type: 'reply', corr })
    return
  }

  if (sandbox === 'workspace-write') {
    const writeJob = validateWorkspaceWriteJob(job, approvalPolicy, a2a.signingEnabled)
    if (!writeJob.ok) {
      log(`REJECT ${job_id}: workspace-write unauthorized (${writeJob.detail}) from ${from}`)
      await sendMsg(from, { schema: 'codex.job.rejected.v1', job_id, ...tid, reason: writeJob.reason, detail: writeJob.detail }, { type: 'reply', corr })
      return
    }
    effectiveThreadKey = writeJob.threadKey

    const authz = await authorizeWriteJob(getRedis(), { requesterId: from, cwd, threadKey: writeJob.threadKey }, {
      allowWrite: WRITE_ENABLED, allowlist: WRITE_ALLOWLIST, cwdRoots: CWD_ROOTS,
    })
    if (!authz.ok) {
      log(`REJECT ${job_id}: workspace-write unauthorized (${authz.reason}) from ${from}`)
      await sendMsg(from, { schema: 'codex.job.rejected.v1', job_id, ...tid, reason: 'write-unauthorized', detail: authz.reason }, { type: 'reply', corr })
      return
    }
    effectiveCwd = authz.realpath
  }

  // budget-aware admission: the job's budget_policy governs the shed threshold;
  // env BUDGET_MAX is only the fallback for a contract-less plain-text prompt.
  const budgetMax = typeof job.budget_policy?.max_primary_used_percent === 'number' ? job.budget_policy.max_primary_used_percent : BUDGET_MAX
  const used = await codex.primaryUsedPct()
  if (used !== null && used >= budgetMax) {
    log(`REJECT ${job_id}: budget ${used}% >= ${budgetMax}%`)
    await sendMsg(from, { schema: 'codex.job.rejected.v1', job_id, ...tid, reason: 'budget-shed', retry_after_ms: 1_800_000, primary_used_pct: used }, { type: 'reply', corr })
    return
  }
  // #4: admission MUST be a correlated reply (type:reply + corr). A real requester
  // (pitcher-analysis) matches acceptance on corr; a bare msg leaves ctx.admitted
  // unset and the degraded/deadline path mislabels gateway_status.
  await sendMsg(from, { schema: 'codex.job.accepted.v1', job_id, ...tid, stream_topic: job.stream_topic ?? null, primary_used_pct: used }, { type: 'reply', corr })
  log(`ACCEPT ${job_id} from ${from} (budget ${used ?? '?'}%/${budgetMax}%, sandbox ${sandbox}, approval ${approvalPolicy})`)

  // FAIL-CLOSED on a claim-checked-input miss: if `input_ref` was present but the blob
  // couldn't be recovered, REFUSE to run on the truncated inline preview (the silent
  // partial-prompt claim-check exists to prevent). Loud failure instead. (X-model review, P1.)
  if (inputMiss.reason && isJob) {
    await sendMsg(from, { schema: 'codex.job.failed.v1', job_id, ...tid, error: `input_blob_${inputMiss.reason}` }, { type: 'reply', corr }).catch(() => {})
    log(`FAIL ${job_id}: input blob ${inputMiss.reason} — refusing to run on a partial prompt`)
    return
  }

  // run, streaming deltas to the job's topic if provided
  try {
    let seq = 0
    const effectivePrompt = [AGENT_PREAMBLE, drainPeerInboxContext(), prompt].filter(Boolean).join('\n\n')
    const agentPrompt = buildCodexAgentPrompt(effectivePrompt, {
      agentId: AGENT_ID,
      requester: from,
      jobId: job_id,
      streamTopic: typeof job.stream_topic === 'string' ? job.stream_topic : undefined,
      toolsEnabled: A2A_TOOLS_ENABLED,
    })
    const { text, status, timedOut, timeoutMs, error: turnError } = await codex.runTurn(agentPrompt, {
      threadKey: effectiveThreadKey, sandbox, approvalPolicy, cwd: effectiveCwd,
      onDelta: job.stream_topic ? (d) => { void sendMsg(`topic:${job.stream_topic}`, { schema: 'codex.job.delta.v1', job_id, ...tid, seq: seq++, text: d }) } : undefined,
    })
    // final delta marker so streaming consumers know the stream ended (lossy-tolerant)
    if (job.stream_topic) void sendMsg(`topic:${job.stream_topic}`, { schema: 'codex.job.delta.v1', job_id, ...tid, seq: seq++, text: '', final: true })

    const bodyCap = a2a.getMaxSendBytes()
    if (timedOut) {
      const partialBudget = Math.max(0, Math.min(2048, bodyCap - 512))
      const partial = partialBudget > 0 ? truncateToBytes(text, partialBudget).text : ''
      if (isJob) {
        await sendMsg(from, {
          schema: 'codex.job.failed.v1',
          job_id,
          ...tid,
          error: 'turn_timeout',
          timeout_ms: timeoutMs,
          ...(partial ? { partial_output: partial } : {}),
        }, { type: 'reply', corr }).catch(() => {})
      } else {
        await sendMsg(from, `(error: turn_timeout after ${timeoutMs}ms)${partial ? `\n\n${partial}` : ''}`, { type: 'reply', corr }).catch(() => {})
      }
      log(`TIMEOUT ${job_id}: turn exceeded ${timeoutMs}ms${partial ? ` partial=${partial.length}c` : ''}`)
      return
    }
    // in-band codex turn failure (status!=completed, e.g. usage-limit/quota) — surface the reason as
    // codex.job.failed.v1 instead of a silent completed/0c reply. (observability: no more silent-0c hunts.)
    if (status !== 'completed') {
      const detail = turnError || `codex_turn_${status}`
      if (isJob) {
        const partialBudget = Math.max(0, Math.min(2048, bodyCap - 512))
        const partial = text && partialBudget > 0 ? truncateToBytes(text, partialBudget).text : ''
        await sendMsg(from, { schema: 'codex.job.failed.v1', job_id, ...tid, error: detail, status, ...(partial ? { partial_output: partial } : {}) }, { type: 'reply', corr }).catch(() => {})
      } else {
        await sendMsg(from, `(error: ${detail})${text ? `\n\n${text}` : ''}`, { type: 'reply', corr }).catch(() => {})
      }
      log(`FAIL ${job_id}: status=${status} codex_error=${detail.slice(0, 200)}`)
      return
    }
    if (isJob) {
      // byte-safe completed reply: inline if it fits; else claim-check (lossless blob+ref
      // + marked preview); else (Redis down) explicit byte-truncation. Never silent.
      const wrap: WrapFn = (t, extra) => ({ schema: 'codex.job.completed.v1', job_id, ...tid, status, output: t, ...(extra ?? {}) })
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
        await sendMsg(from, { schema: 'codex.job.failed.v1', job_id, ...tid, error: 'reply_too_large' }, { type: 'reply', corr }).catch(() => {})
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
        // reserve marker space; if the cap is smaller than the marker itself
        // (pathological), drop the marker and hard byte-truncate to the cap.
        out = bodyCap >= mb ? truncateToBytes(full, bodyCap - mb).text + marker : truncateToBytes(full, bodyCap).text
        log(`TRUNCATE ${job_id} (plain): ${utf8ByteLength(full)}->${utf8ByteLength(out)}B`)
      }
      const res = await sendMsg(from, out, { type: 'reply', corr })
      if (!sentOk(res)) { await sendMsg(from, '(error: reply_too_large)', { type: 'reply', corr }).catch(() => {}); log(`SEND-FAIL ${job_id} (plain): rejected`) }
      else log(`DONE ${job_id} status=${status} ${text.length}c`)
    }
  } catch (e) {
    const error = describeGatewayError(e)
    await sendMsg(from, { schema: 'codex.job.failed.v1', job_id, ...tid, error }, { type: 'reply', corr }).catch(() => {})
    log(`FAIL ${job_id}: ${error}`)
  }
}

async function main() {
  codex = new CodexAppServer()
  configureApprovals()
  a2a = new A2AChannel(onInbound, { enabled: true, agentId: AGENT_ID })
  if (WRITE_ENABLED && !a2a.signingEnabled) {
    log(`FATAL: CODEX_GW_ALLOW_WRITE is set but the A2A channel is running with signing OFF (A2A_DEV_NO_AUTH). workspace-write requires a cryptographically verified sender — env.from is spoofable under signing-off (write + allowlist bypass). Refusing to start.`)
    process.exit(1)
  }
  await codex.init()
  await a2a.start()
  if (!a2a.isStarted()) {
    log(`FATAL: could not join the bus as '${AGENT_ID}' (duplicate agent-id? another instance alive, or stale presence). Exiting.`)
    codex.stop(); process.exit(1)
  }
  const used = await codex.primaryUsedPct()
  log(`bus joined as '${AGENT_ID}' (codex primary budget ${used ?? '?'}%). canonical codex.job.* contract ready.`)

  for (const sig of ['SIGTERM', 'SIGINT'] as const) process.on(sig, async () => { await a2a.stop().catch(() => {}); codex.stop(); process.exit(0) })
}

if (import.meta.main) await main()
