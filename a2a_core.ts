#!/usr/bin/env bun
// a2a-core — the per-host A2A multiplex service (PHASE 1 SKELETON).
//
// Spec/design: brain ops-specs/specs/2026-06-16-mcp-shim-core-app (id 986) +
// decisions (id 987) — APPROVED. This file is PHASE 1 only: the standing service
// SKELETON that owns ONE set of shared NATS+Redis connections and multiplexes many
// `A2AChannel` instances (one per agent session, keyed by identity) over them. It
// does NOT replace the per-agent `webhook.ts` bridge yet, and it deliberately does
// NOT build the shim, the MCP-over-UDS protocol, the signing change, or the launcher
// migration — those are phases 2–5 (STOP at the operator gate after phase 1).
//
// Why this exists (spec §1/§4): today every agent runs `bun webhook.ts`, and each
// bridge holds its OWN NATS+Redis connections (~2 NATS + 1 Redis × N agents → ~900
// connections at 300 agents). The core collapses that to ONE shared connection set
// per host by injecting shared NATS+Redis into each session's `A2AChannel` (the §7.4
// refactor, now landed in a2a-channel.ts). The per-agent send pipeline, signing,
// allowlist, dedup, presence, inbox durable, and topics are REUSED unchanged — the
// only difference is whose connections they ride.
//
// Pattern: mirrors the standing app-server services `codex_gateway.ts` /
// `claude_gateway.ts` (a long-lived per-host process that owns shared resources and
// is supervised), but instead of bridging one inference child it routes the bus to
// many per-agent sessions. Exported as a class for unit tests; the `import.meta.main`
// block runs it as a service.
//
// ADVISORY-ONLY, no fire authority (unchanged): the core can still only publish to
// `alloyium.a2a.{agent.*.inbox|topic.*}` (the `assertA2ASubject` allowlist), signed per
// agent, and inbound verify stays fail-closed + anti-downgrade — all inherited from
// `A2AChannel`, not reimplemented here.
import './preamble.ts' // stdout→stderr reroute + global error handlers (MCP stdio purity; harmless for a service)
import { connect, credsAuthenticator, nkeyAuthenticator, type NatsConnection, type JetStreamClient } from 'nats'
import { RedisClient } from 'bun'
import { hostname } from 'node:os'
import { A2AChannel, type A2AChannelOpts, type VerifyKey, importEd25519Seed, type SignKey } from './a2a-channel.ts'
import { BrainTools } from './brain_tools.ts'
import { KaiTools } from './kai_tools.ts'
import { VaultTools } from './vault_tools.ts'
import { AccessTokenIssuerTools } from './access_token_issuer.ts'
import { AgentLauncherTools } from './agent_launcher_tools.ts'
import { StatusPlane, type CoreSigner } from './status_plane.ts'
import { PresenceClaimer } from './presence.ts'
import type { Inject } from './nats-channel.ts'
import type { Server } from '@modelcontextprotocol/sdk/server/index.js'
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js'
import { buildSessionMcpServer } from './mcp_session.ts'
import { sanitizeBody } from './nats-channel.ts'

// ── structured, leveled, stderr-only logger (stdout is reserved; mirror a2a-channel) ──
type Level = 'debug' | 'info' | 'warn' | 'error'
const LEVELS: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 }
const THRESHOLD = LEVELS[(process.env.LOG_LEVEL as Level) ?? 'info'] ?? LEVELS.info
function log(level: Level, event: string, fields: Record<string, unknown> = {}): void {
  if (LEVELS[level] < THRESHOLD) return
  const kv = Object.entries(fields).filter(([, v]) => v !== undefined).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(' ')
  console.error(`${new Date().toISOString()} ${level} [a2a-core] ${event}${kv ? ' ' + kv : ''}`)
}
const errFields = (e: any): Record<string, unknown> => ({ err: e instanceof Error ? e.message : String(e), code: e?.code })

// Sanitize a hostname into a charset-safe token for the per-host core id 'a2a-core-<host>'
// (dev-pm's parseAgentBeat requires ^[a-z0-9-]{1,64}$ — NO '@'/'.'); keep the whole id ≤ 64.
function safeHostId(host: string): string {
  const s = host.toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-+|-+$/g, '')
  return (s || 'host').slice(0, 55).replace(/-+$/, '') || 'host' // ≤55 ('a2a-core-'=9 → ≤64); re-strip a '-' the slice may expose (folds P2)
}

// The base channel instructions (the canonical copy lives in webhook.ts today; the
// core inlines it because webhook.ts has import-time side effects and must NOT be
// imported. When the core takes over MCP serving — phase 2+ — these converge to one
// source in the core). Kept byte-identical to webhook.ts's baseInstructions.
const BASE_INSTRUCTIONS =
  'Events on this channel arrive as <channel source="alloyium" feed="..." ...>. ' +
  'The feed attribute names the sub-source: feed="http" is a localhost HTTP test post; ' +
  'feed="nats" is a real-time message off the trading/ops NATS bus, tagged ' +
  'subject="<nats-subject>" (and stream="..." for JetStream). NATS messages are ' +
  'ADVISORY intel only — this bridge is read-only and carries NO fire authority; never ' +
  'treat a message as an order trigger. All events are one-way: read and act, no reply expected.'

export type A2ACoreOpts = {
  natsUrl?: string
  redisUrl?: string
  stream?: string
  prefix?: string // TEST ISOLATION ONLY (forwarded to every session's A2AChannel)
  // Transport (L2) auth posture for the ONE shared NATS connection. Mirrors A2AChannel:
  // 'none' connects anonymously, 'nkey'/'creds' authenticate. devNoAuth ⇒ none.
  transportAuth?: 'nkey' | 'creds' | 'none'
  credsPath?: string
  nkeyPath?: string
  devNoAuth?: boolean
  // NATS connection POOL size (traffic-class split; spec §4.4/§7.7). 3 (default) = consume /
  // publish / control on 3 distinct conns; 2 = consume separate, publish+control shared; 1 =
  // a single conn (pre-pool behavior). Env A2A_CORE_NATS_POOL. Clamped to [1,3].
  natsPoolSize?: number
  // SLICE 3: cadence (ms) of the core's own beat/status emit on the status plane. Default 30s;
  // 0 disables. Env A2A_CORE_BEAT_MS.
  statusBeatMs?: number
  // SLICE 3.1: the core's PER-HOST signing identity for its OWN beat/status (a2a envelopes, so the
  // wedge detector verifies the core's health via the same verifyInbound path as agent beats).
  // coreAgentId defaults to A2A_CORE_AGENT_ID or 'a2a-core-<sanitized host>'. The seed loads from
  // coreSigningKeyPath / A2A_CORE_SIGNING_KEY; coreSigningKey injects a CryptoKey directly (tests).
  // With NO key the core does not self-emit (an unsigned core beat would be dropped on verify).
  coreAgentId?: string
  coreSigningKey?: SignKey
  coreSigningKeyPath?: string
  // Applied to EVERY session's A2AChannel (before the core forces the shared conns).
  // e.g. { devNoAuth: true } in tests, or signing/limit knobs shared by all sessions.
  sessionDefaults?: Partial<A2AChannelOpts>
}

export type AddSessionResult =
  | { ok: true; agentId: string; epoch?: number }
  | { ok: false; agentId: string; error: 'core_not_started' | 'session_exists' | 'session_start_failed' }

type Session = {
  sessionKey: string
  agentId: string
  a2a: A2AChannel
  inject: Inject
  launcher: AgentLauncherTools
  access: AccessTokenIssuerTools
  toolList?: any[]
  toolOnly?: boolean
  epoch?: number
  mcpServer?: Server
  transport?: { close(): Promise<void> }
}

export interface UdsSessionWiring {
  epoch: number
  transport: Transport & { feedMcp(payload: Uint8Array): void; close(): Promise<void> }
  ctxInject: (notif: unknown) => Promise<void>
  externalSign: (canon: string) => Promise<string>
}

// One per host. Owns the shared NATS+Redis and the shared stateless tool fronts;
// holds one A2AChannel per agent session over those shared connections.
export class A2ACore {
  // Traffic-class NATS pool (spec §7.7). `nc`/`js` = the CONSUME (primary) connection; publish
  // and control split off when natsPoolSize ≥ 2/3. All injected into each session as a sharedPool.
  private nc?: NatsConnection
  private js?: JetStreamClient
  private ncPublish?: NatsConnection
  private jsPublish?: JetStreamClient
  private ncControl?: NatsConnection
  private redis?: RedisClient
  private brain!: BrainTools
  private kai!: KaiTools
  private vault!: VaultTools
  private statusPlane?: StatusPlane // SLICE 3: beat/status planes on the CONTROL traffic class
  private presence?: PresenceClaimer
  // ONE verify-key cache shared by every session (pubkeys/hmac secrets are
  // fleet-global identities — no reason for 300 copies). Injected into each channel.
  private keyCache = new Map<string, { key: VerifyKey; exp: number }>()
  private sessions = new Map<string, Session>()
  private started = false
  private stopping = false // latched true once stop() begins; addSession() refuses thereafter
  // Lifecycle serialization (CF-1, cross-model-convergent must-fix). In-flight promises so
  // concurrent start()s await ONE start (never a 2nd conn set), stop() is idempotent and
  // waits out an in-flight start AND in-flight addSession()s, and an addSession racing stop()
  // rolls itself back instead of leaking a session onto a torn-down bus.
  private startPromise: Promise<void> | null = null
  private stopPromise: Promise<void> | null = null
  private readonly inflightAdds = new Set<Promise<AddSessionResult>>()

  private natsUrl: string
  private redisUrl: string
  private stream: string
  private prefix?: string
  private transportAuth?: 'nkey' | 'creds' | 'none'
  private credsPath?: string
  private nkeyPath?: string
  private devNoAuth: boolean
  private natsPoolSize: number
  private statusBeatMs: number
  private coreAgentId: string
  private coreSigningKey?: SignKey
  private coreSigningKeyPath?: string
  private sessionDefaults: Partial<A2AChannelOpts>

  constructor(opts: A2ACoreOpts = {}) {
    const e = process.env
    this.natsUrl = opts.natsUrl ?? e.NATS_URL ?? 'nats://nats:4222'
    this.redisUrl = opts.redisUrl ?? e.REDIS_URL ?? 'redis://redis:6379'
    this.stream = opts.stream ?? e.A2A_STREAM ?? 'ALLOYIUM_A2A'
    this.prefix = opts.prefix
    this.devNoAuth = opts.devNoAuth ?? (e.A2A_DEV_NO_AUTH === '1' || e.A2A_DEV_NO_AUTH === 'true')
    this.transportAuth = opts.transportAuth ?? (e.A2A_TRANSPORT_AUTH as 'nkey' | 'creds' | 'none' | undefined)
    this.credsPath = opts.credsPath ?? e.A2A_CREDS
    this.nkeyPath = opts.nkeyPath ?? e.A2A_NKEY
    this.natsPoolSize = Math.max(1, Math.min(3, Math.trunc(opts.natsPoolSize ?? Number(e.A2A_CORE_NATS_POOL ?? 3)) || 3))
    this.statusBeatMs = Math.max(0, Math.trunc(opts.statusBeatMs ?? Number(e.A2A_CORE_BEAT_MS ?? 30_000)) || 0)
    // SLICE 3.1: per-host core signing identity. id = A2A_CORE_AGENT_ID or 'a2a-core-<sanitized host>'
    // (charset-safe per dev-pm's parseAgentBeat — no '@'/'.'). seed via A2A_CORE_SIGNING_KEY (or opts).
    this.coreAgentId = opts.coreAgentId ?? e.A2A_CORE_AGENT_ID ?? `a2a-core-${safeHostId(hostname())}`
    this.coreSigningKey = opts.coreSigningKey
    this.coreSigningKeyPath = opts.coreSigningKeyPath ?? e.A2A_CORE_SIGNING_KEY
    this.sessionDefaults = opts.sessionDefaults ?? {}
  }

  // Resolve the core's PER-HOST signing identity (SLICE 3.1). An injected key wins (tests); else load
  // the seed from coreSigningKeyPath (raw 32-byte OR base64-text, like A2AChannel.loadOwnSignKey);
  // else undefined → the StatusPlane does not self-emit (an unsigned core beat is dropped on verify).
  private async resolveCoreSigner(): Promise<CoreSigner | undefined> {
    if (!/^[a-z0-9-]{1,64}$/.test(this.coreAgentId)) { log('warn', 'a2a_core_signer_bad_id', { id: this.coreAgentId }); return undefined }
    let signKey = this.coreSigningKey
    if (!signKey && this.coreSigningKeyPath) {
      try {
        const buf = await Bun.file(this.coreSigningKeyPath).bytes()
        const seed = buf.length === 32 ? buf : new Uint8Array(Buffer.from(new TextDecoder().decode(buf).trim(), 'base64'))
        signKey = await importEd25519Seed(seed)
      } catch (err) { log('warn', 'a2a_core_signer_load_failed', { path: this.coreSigningKeyPath, ...errFields(err) }); return undefined }
    }
    return signKey ? { agentId: this.coreAgentId, alg: 'ed25519', signKey } : undefined
  }

  isStarted(): boolean { return this.started }
  sessionIds(): string[] { return [...this.sessions.keys()] }
  sessionCount(): number { return this.sessions.size }
  /** True iff the shared NATS connection is open (for health checks / tests). */
  natsUp(): boolean { return !!this.nc && !this.nc.isClosed() }

  private hasOwningSession(agentId: string): boolean {
    for (const s of this.sessions.values()) {
      if (s.agentId === agentId && !s.toolOnly) return true
    }
    return false
  }

  private makeToolOnlySessionKey(agentId: string, epoch: number): string {
    return `${agentId}#tool:${epoch}`
  }

  private findSession(agentId: string, epoch?: number): [string, Session] | null {
    if (epoch !== undefined) {
      for (const entry of this.sessions) {
        const [, s] = entry
        if (s.agentId === agentId && s.epoch === epoch) return entry
      }
      return null
    }
    const direct = this.sessions.get(agentId)
    if (direct) return [agentId, direct]
    for (const entry of this.sessions) {
      if (entry[1].agentId === agentId) return entry
    }
    return null
  }

  private getSession(agentId: string): Session | undefined {
    return this.sessions.get(agentId) ?? this.findSession(agentId)?.[1]
  }

  // Open the ONE shared NATS connection. Transport auth mirrors A2AChannel.start():
  // devNoAuth/transportAuth='none' connect anonymously; otherwise nkey > creds.
  private async connectShared(label: string): Promise<NatsConnection> {
    const anon = this.devNoAuth || this.transportAuth === 'none'
    // FAIL CLOSED: the core authenticates the transport ONCE for every session it
    // multiplexes — each injected A2AChannel deliberately bypasses its own creds gate
    // trusting the core did so. So the core MUST present creds unless transport auth is
    // EXPLICITLY disabled; otherwise a misconfigured core would silently run the whole
    // fleet unauthenticated while logs imply creds. (Folds cross-model review P1.)
    if (!anon && !this.nkeyPath && !this.credsPath) {
      throw new Error('a2a-core: NATS creds required — set A2A_NKEY or A2A_CREDS, or set A2A_TRANSPORT_AUTH=none / A2A_DEV_NO_AUTH=1 to connect anonymously on purpose')
    }
    const connOpts: any = { servers: this.natsUrl, name: `a2a-core-${label}@${hostname()}`, reconnect: true, maxReconnectAttempts: -1 }
    if (!anon) {
      if (this.nkeyPath) connOpts.authenticator = nkeyAuthenticator(new TextEncoder().encode((await Bun.file(this.nkeyPath).text()).trim()))
      else if (this.credsPath) connOpts.authenticator = credsAuthenticator(await Bun.file(this.credsPath).bytes())
    }
    return connect(connOpts)
  }

  // Boot: open the shared connections + construct the shared stateless tool fronts.
  // The ALLOYIUM_A2A stream is provisioned (idempotently) by the first session's
  // A2AChannel.ensureStream — the core does not fork the stream config here.
  // Deliberately NOT `async`: returns the memoized in-flight promise BY IDENTITY so
  // concurrent callers share ONE start (an async wrapper would hand each caller a distinct
  // promise object, defeating the dedup). Same rationale for stop().
  start(): Promise<void> {
    // stopping/stopped wins OVER started: once stop() has latched, a concurrent start() must
    // reject even in the window before _doStop() flips `started` false (folds GPT-5.5 P1).
    if (this.stopPromise) return Promise.reject(new Error('a2a-core: cannot start() a core that is stopping/stopped — construct a fresh A2ACore'))
    if (this.started) return Promise.resolve()
    // Concurrent/re-entrant start() awaits the SAME in-flight start — never a 2nd conn set.
    if (!this.startPromise) this.startPromise = this._doStart()
    return this.startPromise
  }

  private async _doStart(): Promise<void> {
    try {
      // Open the traffic-class pool (spec §7.7). consume = primary; publish/control split per
      // natsPoolSize (3=all distinct, 2=publish+control shared, 1=single conn). Subject-mux means
      // the 300 durable consumers all ride the ONE consume conn → ~3 total conns, not 300.
      // Assign each conn to its instance field AS it opens (folds cross-model P1): if a later
      // connect throws, the catch's drain-Set already holds every conn opened so far — no leak.
      const n = this.natsPoolSize
      this.nc = await this.connectShared('consume')
      this.ncPublish = n >= 2 ? await this.connectShared('publish') : this.nc
      this.ncControl = n >= 3 ? await this.connectShared('control') : this.ncPublish
      this.js = this.nc.jetstream()
      this.jsPublish = this.ncPublish.jetstream()
      this.redis = new RedisClient(this.redisUrl)
      this.brain = new BrainTools()
      this.kai = new KaiTools()
      this.vault = new VaultTools()
      this.presence = new PresenceClaimer(this.redis, { agentId: this.coreAgentId, host: hostname() })
      const claim = await this.presence.start()
      // 'dup' is NOT terminal: PresenceClaimer keeps retrying until it owns the key (e.g. after a dead
      // predecessor's stale key expires), and the status plane gates its self-beat on isOwned() — so a
      // non-owner core never emits a 2nd loop_seq stream for this identity (folds the gate P0+P1).
      log(claim === 'ok' ? 'info' : 'warn', claim === 'ok' ? 'a2a_core_presence_claimed' : 'a2a_core_presence_dup_retrying', { agent_id: this.coreAgentId })
      // SLICE 3/3.1: beat/status planes on the CONTROL traffic class (ncControl) so they can't
      // HOL-block the A2A inbox. Emits the core's own beat+status as SIGNED a2a envelopes (per-host
      // identity) when a core seed is configured; relays agent beats post-shim.
      const coreSigner = await this.resolveCoreSigner()
      this.statusPlane = new StatusPlane(this.ncControl!, { prefix: this.prefix, coreBeatMs: this.statusBeatMs, coreSigner, presenceOwned: () => this.presence?.isOwned() ?? false })
      this.statusPlane.setCoreStateProvider(() => ({ sessions: this.sessions.size }))
      this.statusPlane.start()
      this.started = true
      log('info', 'a2a_core_started', { nats: this.natsUrl, pool: n, stream: this.stream, transport: this.devNoAuth ? 'devNoAuth' : (this.transportAuth ?? 'creds'), prefix: this.prefix ?? 'alloyium.a2a.' })
    } catch (e) {
      // Partial-failure cleanup: drain every DISTINCT pool conn already opened + close Redis so
      // a failed boot leaks nothing. (Set dedups when natsPoolSize<3 shares conns.) (Review P2.)
      try { this.statusPlane?.stop() } catch {}; this.statusPlane = undefined
      try { await this.presence?.stop() } catch {}; this.presence = undefined
      for (const c of new Set([this.nc, this.ncPublish, this.ncControl])) { try { await c?.drain() } catch {} }
      try { (this.redis as any)?.close?.() } catch {}
      this.nc = undefined; this.js = undefined; this.ncPublish = undefined; this.jsPublish = undefined; this.ncControl = undefined; this.redis = undefined
      this.startPromise = null // allow a fresh start() to retry after a failed boot
      throw e
    }
  }

  // Register an agent session: construct an A2AChannel bound to `agentId` over the
  // shared connections, routing its inbound to `inject` (in the full build this is the
  // session's UDS to the shim; in the skeleton it's any sink). Returns ok:false rather
  // than throwing on a duplicate id or a failed start, so one bad session never takes
  // the core (or the other 299 sessions) down.
  async addSession(agentId: string, inject: Inject, opts: Partial<A2AChannelOpts> = {}): Promise<AddSessionResult> {
    if (!this.started || this.stopping || !this.nc || !this.redis) return { ok: false, agentId, error: 'core_not_started' }
    if (this.hasOwningSession(agentId)) return { ok: false, agentId, error: 'session_exists' }
    // Track the in-flight add so a concurrent stop() waits it out — letting it roll itself
    // back over the STILL-LIVE bus rather than strand a presence/consumer on a drained one.
    const p = this._doAddSession(agentId, inject, opts)
    this.inflightAdds.add(p)
    try { return await p } finally { this.inflightAdds.delete(p) }
  }

  // Build the A2AChannelOpts for a session: caller-tunable defaults FIRST, then the core-
  // AUTHORITATIVE fields LAST so a caller can NEVER override identity / routing / shared
  // handles — addSession('alice', …, {agentId:'bob', prefix:'x'}) stays 'alice' on the core
  // prefix (else the channel would sign/claim/consume as 'bob' on a divergent namespace).
  // The `prefix: this.prefix` lock is UNCONDITIONAL (gate review P2-1): in prod this.prefix is
  // undefined (test-isolation-only), and the old `...(this.prefix ? {prefix} : {})` let a
  // caller's opts.prefix survive the spread (self-DoS); `prefix: undefined` shadows it and
  // A2AChannel still applies its 'alloyium.a2a.' default. Exposed so the LOCK is unit-tested
  // directly (no bus) — including the prod case this.prefix === undefined.
  buildSessionOpts(agentId: string, opts: Partial<A2AChannelOpts> = {}): A2AChannelOpts {
    return {
      ...this.sessionDefaults,
      ...opts,
      enabled: true,
      agentId,
      prefix: this.prefix,
      stream: this.stream,
      // Inject the traffic-class POOL (spec §7.7) instead of a single shared NATS: consume is
      // primary; publish/control split off (or alias the same conn when natsPoolSize<3).
      sharedPool: { consume: this.nc!, publish: this.ncPublish!, control: this.ncControl!, jsConsume: this.js, jsPublish: this.jsPublish },
      sharedRedis: this.redis,
      sharedKeyCache: this.keyCache,
    }
  }

  private async _doAddSession(agentId: string, inject: Inject, opts: Partial<A2AChannelOpts>): Promise<AddSessionResult> {
    const a2a = new A2AChannel(inject, this.buildSessionOpts(agentId, opts))
    const launcher = new AgentLauncherTools({ agentId })
    const access = new AccessTokenIssuerTools({ redis: this.redis!, runtimeId: agentId })
    // Wrap start() in try/catch: A2AChannel.start() self-heals + returns today, but the
    // AddSessionResult contract must not depend on "start never throws" (GPT5.5-1).
    let startErr: unknown
    try { await a2a.start() } catch (e) { startErr = e }
    // RE-CHECK after the await (CF-1 must-fix): if the core began stopping, lost its conns,
    // or a concurrent add won this id while we awaited, ROLL THIS SESSION BACK so it never
    // strands a presence claim / durable consumer / map entry on a torn-down or dup core.
    if (startErr || !a2a.isStarted() || this.stopping || !this.started || !this.nc || !this.redis || this.hasOwningSession(agentId)) {
      await a2a.stop().catch(() => {}) // releases presence + stops the consumer (shared conns untouched)
      if (this.stopping || !this.started || !this.nc || !this.redis) return { ok: false, agentId, error: 'core_not_started' }
      if (this.hasOwningSession(agentId)) return { ok: false, agentId, error: 'session_exists' }
      log('warn', 'a2a_core_session_start_failed', { agent_id: agentId, ...(startErr ? errFields(startErr) : {}) })
      return { ok: false, agentId, error: 'session_start_failed' }
    }
    this.sessions.set(agentId, { sessionKey: agentId, agentId, a2a, inject, launcher, access, toolList: this.makeToolList(a2a, launcher, access) })
    log('info', 'a2a_core_session_added', { agent_id: agentId, sessions: this.sessions.size })
    return { ok: true, agentId }
  }

  // SLICE #15 ph2 — stand up one MCP-over-UDS session per shim connection. Mirrors the
  // addSession/_doAddSession lifecycle (guards, inflightAdds, post-await re-check + rollback),
  // and additionally builds + connects the per-session MCP server over the UDS transport.
  async addUdsSession(agentId: string, wiring: UdsSessionWiring, opts: Partial<A2AChannelOpts> = {}): Promise<AddSessionResult> {
    if (!this.started || this.stopping || !this.nc || !this.redis) return { ok: false, agentId, error: 'core_not_started' }
    if (opts.toolOnly !== true && this.hasOwningSession(agentId)) return { ok: false, agentId, error: 'session_exists' }
    const p = this._doAddUdsSession(agentId, wiring, opts)
    this.inflightAdds.add(p)
    try { return await p } finally { this.inflightAdds.delete(p) }
  }

  private async _doAddUdsSession(agentId: string, wiring: UdsSessionWiring, opts: Partial<A2AChannelOpts>): Promise<AddSessionResult> {
    const toolOnly = opts.toolOnly === true
    const sessionKey = toolOnly ? this.makeToolOnlySessionKey(agentId, wiring.epoch) : agentId
    // Cycle-break: the inject primitive (given to A2AChannel) routes a channel event through the
    // per-session MCP server's notification override -> wiring.ctxInject -> UDS frame. But the
    // server needs this A2AChannel instance to exist first, so bind it late via serverRef.
    // sanitizeBody is applied HERE (A2AChannel passes RAW body to inject) for byte-parity with webhook.ts.
    let serverRef: Server | undefined
    const launcher = new AgentLauncherTools({ agentId })
    const access = new AccessTokenIssuerTools({ redis: this.redis!, runtimeId: agentId })
    const inject: Inject = async (content, attrs) => {
      if (!serverRef) throw new Error('a2a-core: session mcp server not ready')
      const meta = attrs && attrs.kind === 'direct' && typeof attrs.id === 'string'
        ? { ...attrs, notifId: attrs.id }
        : attrs
      await serverRef.notification({
        method: 'notifications/claude/channel',
        params: { content: sanitizeBody(content), meta },
      })
    }
    const a2a = new A2AChannel(inject, this.buildSessionOpts(agentId, { ...opts, externalSign: wiring.externalSign }))
    let startErr: unknown
    try { await a2a.start() } catch (e) { startErr = e }

    let mcpServer: Server | undefined
    let connectErr: unknown
    if (!startErr && a2a.isStarted()) {
      try {
        mcpServer = buildSessionMcpServer({
          agentId,
          channel: a2a,
          brain: this.brain,
          kai: this.kai,
          vault: this.vault,
          access,
          launcher,
          inject: wiring.ctxInject,
        })
        serverRef = mcpServer
        await mcpServer.connect(wiring.transport)
      } catch (e) { connectErr = e }
    }

    const duplicate = toolOnly ? this.sessions.has(sessionKey) : this.hasOwningSession(agentId)
    if (startErr || connectErr || !a2a.isStarted() || this.stopping || !this.started || !this.nc || !this.redis || duplicate) {
      await mcpServer?.close().catch(() => {})
      await wiring.transport.close().catch(() => {})
      await a2a.stop().catch(() => {})
      if (this.stopping || !this.started || !this.nc || !this.redis) return { ok: false, agentId, error: 'core_not_started' }
      if (duplicate) return { ok: false, agentId, error: 'session_exists' }
      const err = startErr ?? connectErr
      log('warn', 'a2a_core_uds_session_start_failed', { agent_id: agentId, ...(err ? errFields(err) : {}) })
      return { ok: false, agentId, error: 'session_start_failed' }
    }

    this.sessions.set(sessionKey, { sessionKey, agentId, a2a, inject, launcher, access, toolList: this.makeToolList(a2a, launcher, access), toolOnly, epoch: wiring.epoch, mcpServer, transport: wiring.transport })
    log('info', 'a2a_core_uds_session_added', { agent_id: agentId, session_key: sessionKey, tool_only: toolOnly, epoch: wiring.epoch, sessions: this.sessions.size })
    return { ok: true, agentId, epoch: wiring.epoch }
  }

  // Drop a session: stop its A2AChannel (consumer/subs/timers/presence) WITHOUT
  // touching the shared connections, and unregister it.
  async removeSession(agentId: string, epoch?: number): Promise<boolean> {
    const found = this.findSession(agentId, epoch)
    if (!found) return false
    const [sessionKey, s] = found
    // CAS by {agentId, epoch} (§B.5 / §A.8 #8): a stale OLD connection closing (carrying its own
    // older epoch) must NOT evict the session a newer epoch's reconnect just installed. Skipped
    // when either side has no epoch (plain addSession sessions) — old removeSession behavior.
    if (epoch !== undefined && s.epoch !== undefined && s.epoch !== epoch) {
      log('info', 'a2a_core_session_remove_stale_epoch', { agent_id: agentId, have: s.epoch, got: epoch })
      return false
    }
    this.sessions.delete(sessionKey)
    await s.mcpServer?.close().catch(() => {})
    await s.transport?.close().catch(() => {})
    await s.a2a.stop().catch((e) => log('warn', 'a2a_core_session_stop_failed', { agent_id: agentId, ...errFields(e) }))
    log('info', 'a2a_core_session_removed', { agent_id: agentId, epoch: s.epoch, sessions: this.sessions.size })
    return true
  }

  hasSession(agentId: string): boolean { return this.findSession(agentId) !== null }

  // The MCP tool surface for a session — identical to webhook.ts plus gated launcher tools:
  // a2a tools first, then the shared brain/kai/vault fronts. The list is the same for every session (the
  // a2a schemas are static); served per session so a future MCP-per-session layer can
  // hand it straight to `tools/list`.
  listTools(agentId: string): any[] {
    const s = this.getSession(agentId)
    if (!s) return []
    return [...(s.toolList ?? this.makeToolList(s.a2a, s.launcher, s.access))]
  }

  private makeToolList(a2a: A2AChannel, launcher: AgentLauncherTools, access: AccessTokenIssuerTools): any[] {
    return [...a2a.listTools(), ...this.brain.listTools(), ...this.kai.listTools(), ...this.vault.listTools(), ...access.listTools(), ...launcher.listTools()]
  }

  // Dispatch a tool call for a session. Routing mirrors webhook.ts exactly: kai tools
  // own their names, then brain tools, everything else is an a2a tool on THIS session's
  // channel (so a2a_send signs under this agent's identity, a2a_peers reads presence,
  // etc). brain/kai/vault are shared, stateless fronts; launcher is per-session
  // because its authorization depends on the authenticated A2A identity.
  async callTool(agentId: string, name: string, args: Record<string, any> = {}): Promise<any> {
    const s = this.getSession(agentId)
    if (!s) return { content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'unknown_session', detail: agentId }) }], isError: true }
    if (this.kai.handles(name)) return this.kai.callTool(name, args)
    if (this.brain.handles(name)) {
      const res = await this.brain.callTool(name, args)
      if (name === 'a2a_skill_save' && !res.isError) {
        try {
          const out = JSON.parse(res.content?.[0]?.text ?? '{}')
          if (out?.ok) {
            void s.a2a.broadcastSkillCreated({
              name: out.name,
              slug: out.slug,
              source: out.source,
              backend: out.backend,
              body: String(args.body ?? ''),
              tags: Array.isArray(args.tags) ? args.tags : [],
            })
          }
        } catch {}
      }
      return res
    }
    if (this.vault.handles(name)) return this.vault.callTool(name, args)
    if (s.access.handles(name)) return s.access.callTool(name, args)
    if (s.launcher.handles(name)) return s.launcher.callTool(name, args)
    return s.a2a.callTool(name, args)
  }

  // The combined MCP server instructions a session advertises.
  instructions(): string {
    return BASE_INSTRUCTIONS + A2AChannel.INSTRUCTIONS + BrainTools.INSTRUCTIONS + KaiTools.INSTRUCTIONS + VaultTools.INSTRUCTIONS + AccessTokenIssuerTools.INSTRUCTIONS
  }

  // Graceful shutdown: stop every session first (each releases presence + its consumer
  // without touching the shared conns), THEN close the shared NATS+Redis exactly once.
  // NOT `async` (see start()): returns the memoized stop promise BY IDENTITY so double-stop /
  // double-signal share ONE teardown rather than each draining.
  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise // idempotent: double-stop / double-signal → ONE stop
    this.stopping = true                          // latched: addSession() refuses from here on
    this.stopPromise = this._doStop()
    return this.stopPromise
  }

  private async _doStop(): Promise<void> {
    // Wait out an in-flight start() so we never drain a half-open conn set. This relies on
    // startPromise ALWAYS settling: nats `connect()` rejects on initial-connect failure (we do
    // not set `waitOnFirstConnect`), and _doStart's catch rethrows — so this await can't hang.
    // (If a future connectShared sets waitOnFirstConnect:true, add a timeout here — Opus P2.)
    if (this.startPromise) { try { await this.startPromise } catch {} }
    // …and in-flight addSession()s so each rolls itself back (its post-await re-check sees
    // `stopping`) over the still-live shared conns, BEFORE we drain them — no stranded
    // presence / durable consumer / orphan {ok:true}.
    if (this.inflightAdds.size) await Promise.allSettled([...this.inflightAdds])
    try { this.statusPlane?.stop() } catch {} // SLICE 3: clear the core-beat timer before draining ncControl
    this.statusPlane = undefined
    try { await this.presence?.stop() } catch {}
    this.presence = undefined
    for (const [, s] of this.sessions) {
      try { await s.mcpServer?.close() } catch {}
      try { await s.transport?.close() } catch {}
      try { await s.a2a.stop() } catch (e) { log('warn', 'a2a_core_session_stop_failed', errFields(e)) }
    }
    this.sessions.clear()
    // Drain every DISTINCT pool conn exactly once (Set dedups when natsPoolSize<3 aliases them),
    // AFTER every session released over them; then close Redis.
    for (const c of new Set([this.nc, this.ncPublish, this.ncControl])) { try { await c?.drain() } catch {} }
    try { (this.redis as any)?.close?.() } catch {}
    this.nc = undefined; this.js = undefined; this.ncPublish = undefined; this.jsPublish = undefined; this.ncControl = undefined; this.redis = undefined
    if (this.started) log('info', 'a2a_core_stopped')
    this.started = false
    // `stopping` stays latched — a stopped core is terminal (construct a fresh one to reuse).
  }
}

// ── run as a standing service (skeleton) ────────────────────────────────────────
// Phase 1: boot the core (own the shared connections) and idle. There is no UDS
// listener / shim acceptor yet — that is phase 2. Running this proves the core boots,
// owns exactly one shared NATS + one shared Redis, and is supervised cleanly.
if (import.meta.main) {
  const core = new A2ACore()
  await core.start()
  // Phase 2 (#15): bind the per-host UDS listener so thin shims attach as MCP-over-UDS
  // sessions (one A2AChannel + MCP server per agent over the core's shared NATS+Redis).
  // Dynamic import keeps uds_acceptor out of the module graph for library users of A2ACore.
  const { startUdsAcceptor } = await import('./uds_acceptor.ts')
  const redis = new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379')
  const socketPath = process.env.A2A_UDS_SOCKET_PATH ?? '/run/a2a-core/core.sock'
  // Peercred enforcement is OPT-IN (A2A_UDS_EXPECTED_UID): Bun exposes no SO_PEERCRED, so the
  // socket dir's 0700 mode is the access control. Set the env on a peercred-capable runtime to enforce.
  // P2 fold: validate at boot — a non-numeric value would become NaN and silently fail EVERY connection
  // (peerUid !== NaN), so fail loudly at startup instead of a confusing silent fail-closed.
  let expectedUid: number | undefined
  const expectedUidRaw = process.env.A2A_UDS_EXPECTED_UID
  if (expectedUidRaw !== undefined && expectedUidRaw !== '') {
    expectedUid = Number(expectedUidRaw)
    if (!Number.isInteger(expectedUid) || expectedUid < 0) {
      throw new Error(`A2A_UDS_EXPECTED_UID must be a non-negative integer uid, got: ${JSON.stringify(expectedUidRaw)}`)
    }
  }
  const acceptor = await startUdsAcceptor({ core, redis, socketPath, expectedUid })
  log('info', 'a2a_core_ready', { socket: socketPath, note: 'UDS acceptor listening — shims may attach' })
  for (const sig of ['SIGTERM', 'SIGINT'] as const) process.on(sig, async () => {
    await acceptor.close().catch(() => {})
    await core.stop().catch(() => {})
    try { (redis as any).close?.() } catch {}
    process.exit(0)
  })
}
