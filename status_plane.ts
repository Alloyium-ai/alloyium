// status_plane.ts — the CORE side of the liveness/status planes (SLICE 3 + 3.1). Owns the beat/
// status PLANES on the a2a-core's CONTROL traffic-class connection (the 2C pool's ncControl) so they
// can never head-of-line-block the A2A inbox (consume class). Publishes are EPHEMERAL core NATS —
// NOT JetStream — because a replayed durable beat would reset the missed-beat clock and mask a wedge
// (convergence design §5.1 / review #24).
//
// ENVELOPE MODEL (SLICE 3.1, locked with dev-pm thread `beat-schema-boundary`): beats/status ride
// the A2A ENVELOPE layer — the wire payload on alloyium.a2a.topic.agent-{beat,status} is a SIGNED a2a
// Envelope whose `body` is the JSON AgentBeat/AgentStatus; dev-pm's detector runs the standard
// verifyInbound over canonical(envelope) and reads env.body. So:
//   - relayBeatBytes RELAYS the signed envelope bytes UNCHANGED (validate-as-envelope, forward raw →
//     env.sig survives; never re-sign/synthesize — §3/#23 the detector decides on the RAW agent beat).
//   - emitCore EMITS the core's OWN beat+status as SIGNED a2a envelopes from a PER-HOST identity
//     'a2a-core-<host>' (charset-safe; from === beat.agent_id per dev-pm's allowedBeatEmitter), so the
//     detector verifies the core's health via the SAME path as agent beats. No signer → emitCore SKIPS
//     (an unsigned core beat is useless — the detector drops it on verify).
// dev-pm's `wt-heartbeat` owns the agent-side beat EMIT (imports plane_schemas.ts); no duplication.
import { type NatsConnection } from 'nats'
import { hostname } from 'node:os'
import { randomUUID } from 'node:crypto'
import { assertA2ASubject, signEnvelope, isValidInbound, type Envelope, type SigAlg, type SignKey } from './a2a-channel.ts'
import {
  type AgentBeat, type AgentStatus, type AgentState,
  buildBeat, buildStatus, isValidBeat, isValidStatus, beatSubject, statusSubject, BEAT_TOPIC, STATUS_TOPIC,
} from './plane_schemas.ts'
const dec = (u: Uint8Array): string => new TextDecoder().decode(u)
const enc = (s: string): Uint8Array => new TextEncoder().encode(s)

// stderr-only logger (stdout is the MCP stdio pipe; mirror a2a-channel/a2a_core).
type Level = 'debug' | 'info' | 'warn'
const LEVELS: Record<Level, number> = { debug: 10, info: 20, warn: 30 }
const THRESHOLD = LEVELS[(process.env.LOG_LEVEL as Level) ?? 'info'] ?? LEVELS.info
function log(level: Level, event: string, fields: Record<string, unknown> = {}): void {
  if (LEVELS[level] < THRESHOLD) return
  const kv = Object.entries(fields).filter(([, v]) => v !== undefined).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(' ')
  console.error(`${new Date().toISOString()} ${level} [status-plane] ${event}${kv ? ' ' + kv : ''}`)
}
const errFields = (e: any): Record<string, unknown> => ({ err: e instanceof Error ? e.message : String(e) })

// Live core state the periodic core beat/status reports (supplied by A2ACore).
export type CoreState = { state?: AgentState; sessions?: number; phase?: string; attrs?: Record<string, string> }

// The core's PER-HOST signing identity (SLICE 3.1). `agentId` MUST be onboarded (pubkey in canonical
// Redis) so the detector verifies; it is BOTH the beat.agent_id AND the envelope `from` (dev-pm's
// allowedBeatEmitter requires from === beat.agent_id). No CoreSigner → the core does not emit (an
// unsigned core beat would be dropped by the detector's verifyInbound anyway).
export type CoreSigner = { agentId: string; alg: SigAlg; signKey: SignKey }

export type StatusPlaneOpts = {
  prefix?: string        // mirrors A2AChannel.prefix (test isolation); default 'alloyium.a2a.'
  host?: string
  bootId?: string
  coreBeatMs?: number    // core's own beat/status cadence (default 30s); 0 disables the timer
  coreSigner?: CoreSigner // SLICE 3.1: when set, emitCore signs the core beat+status as a2a envelopes; else SKIP
  presenceOwned?: () => boolean // SLICE 3.2: gate self-beat on presence ownership — a non-owner core must NOT emit (one beater per identity)
  now?: () => number
}

export class StatusPlane {
  private readonly beatSub: string
  private readonly statusSub: string
  private readonly host: string
  private readonly bootId: string
  private readonly coreBeatMs: number
  private readonly coreSigner?: CoreSigner
  private readonly presenceOwned?: () => boolean
  private readonly now: () => number
  private timer?: ReturnType<typeof setInterval>
  private started = false
  private coreLoopSeq = 0
  private coreStateFn?: () => CoreState
  private counters = { beats_relayed: 0, status_published: 0, core_beats: 0, dropped: 0 }

  constructor(private readonly nc: NatsConnection, opts: StatusPlaneOpts = {}) {
    const prefix = opts.prefix ?? 'alloyium.a2a.'
    this.beatSub = beatSubject(prefix)
    this.statusSub = statusSubject(prefix)
    // Confinement: assert the plane subjects against the SAME audited allowlist A2AChannel uses, so
    // the planes are provably inside alloyium.a2a.> + deny-prefix-backstopped and a custom/misconfigured
    // prefix can never publish off-namespace (the planes nc.publish directly).
    assertA2ASubject(this.beatSub, prefix)
    assertA2ASubject(this.statusSub, prefix)
    this.host = opts.host ?? hostname()
    this.bootId = opts.bootId ?? randomUUID()
    this.coreBeatMs = opts.coreBeatMs ?? 30_000
    this.coreSigner = opts.coreSigner
    this.presenceOwned = opts.presenceOwned
    this.now = opts.now ?? Date.now
  }

  /** A2ACore sets this so the periodic core beat/status reflects live core state. */
  setCoreStateProvider(fn: () => CoreState): void { this.coreStateFn = fn }
  counts(): typeof this.counters { return { ...this.counters } }

  /** Relay an agent beat's RAW BYTES to the beat plane UNCHANGED — the seam the core uses post-shim
   *  when it receives a UDS beat off the wire. The bytes are a SIGNED a2a ENVELOPE (beat = env.body):
   *  validate-as-envelope (reuse isValidInbound — one verify path, no drift) + body-is-beat, then
   *  publish the ORIGINAL bytes, so the agent's ed25519 env.sig is preserved EXACTLY. Does NOT verify
   *  the sig — that is the detector's job (§3/#23: the core relays, never re-signs/synthesizes). */
  relayBeatBytes(raw: Uint8Array): boolean {
    let env: any
    try { env = JSON.parse(dec(raw)) } catch { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'bad_json' }); return false }
    if (!isValidInbound(env)) { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'bad_envelope' }); return false }
    let beat: any
    try { beat = JSON.parse(env.body) } catch { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'bad_body' }); return false }
    if (!isValidBeat(beat)) { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'bad_beat' }); return false }
    // Relay-side route-binding (folds GPT-5.5 conditional-P1): only forward an envelope ADDRESSED to the
    // beat topic. A structurally-valid signed envelope with a beat body but env.to:'topic:other' would
    // otherwise be republished onto agent-beat. The detector route-binds env.to to the subject and would
    // drop it, but the relay must not LAUNDER a misroute onto the topic in the first place. Fail-closed.
    if (env.to !== `topic:${BEAT_TOPIC}`) { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'wrong_to', to: env.to }); return false }
    try { this.nc.publish(this.beatSub, raw); this.counters.beats_relayed++; return true } // UNMODIFIED bytes → env.sig intact
    catch (e) { this.counters.dropped++; log('warn', 'statusplane_relay_failed', errFields(e)); return false }
  }

  /** Relay a bare beat OBJECT — re-serializes (JSON.stringify) so it is NOT detector-verifiable;
   *  for unsigned/test use only. Production beats ride relayBeatBytes (signed envelope). Fail-closed. */
  relayBeat(beat: AgentBeat): boolean {
    if (!isValidBeat(beat)) { this.counters.dropped++; log('warn', 'statusplane_relay_drop', { reason: 'bad_beat' }); return false }
    try { this.nc.publish(this.beatSub, enc(JSON.stringify(beat))); this.counters.beats_relayed++; return true }
    catch (e) { this.counters.dropped++; log('warn', 'statusplane_relay_failed', errFields(e)); return false }
  }

  /** Publish a BARE agent.status.v1 object (test/forward-compat; not enveloped). Fail-closed. */
  publishStatus(status: AgentStatus): boolean {
    if (!isValidStatus(status)) { this.counters.dropped++; log('warn', 'statusplane_status_drop', { reason: 'bad_status' }); return false }
    try { this.nc.publish(this.statusSub, enc(JSON.stringify(status))); this.counters.status_published++; return true }
    catch (e) { this.counters.dropped++; log('warn', 'statusplane_status_failed', errFields(e)); return false }
  }

  /** Emit the CORE's own beat + status as SIGNED a2a envelopes (one call, two sinks). Requires a
   *  CoreSigner — without one the core can't produce a detector-verifiable beat, so it SKIPS. */
  async emitCore(): Promise<void> {
    const signer = this.coreSigner
    if (!signer) return // no per-host signing identity configured → skip (unsigned beat is dropped by the detector)
    // SLICE 3.2: only the core that OWNS presence:<id> may emit the self-beat — else two live cores would
    // emit two per-process loop_seq streams for one identity and flap the detector's monotonicity check
    // (one beater per identity). When unset (StatusPlane used without a presence claimer), emit normally.
    if (this.presenceOwned && !this.presenceOwned()) return
    const s = this.coreStateFn?.() ?? {}
    const state: AgentState = s.state ?? (s.sessions && s.sessions > 0 ? 'in_task' : 'idle')
    const nowIso = new Date(this.now()).toISOString()
    const beat = buildBeat(signer.agentId, {
      host: this.host, boot_id: this.bootId, session_id: this.bootId,
      loop_seq: ++this.coreLoopSeq, driver_mode: 'service', state,
      inbox_depth: s.sessions ?? 0, last_progress_ts: nowIso, last_turn_ts: nowIso,
    }, this.now)
    // Defense-in-depth (folds review P2): never envelope a self-built beat/status that fails our OWN
    // validator — a future buildBeat/buildStatus regression should DROP here, not emit an unverifiable
    // (or detector-rejected) core beat. All inputs are in-process today, so this can only ever catch a bug.
    if (!isValidBeat(beat)) { this.counters.dropped++; log('warn', 'statusplane_core_emit_drop', { kind: 'beat' }) }
    else if (await this.publishEnvelope(this.beatSub, `topic:${BEAT_TOPIC}`, JSON.stringify(beat))) this.counters.core_beats++
    const status = buildStatus(signer.agentId, {
      host: this.host, boot_id: this.bootId, session_id: this.bootId, driver_mode: 'service', state,
      phase: s.phase ?? 'serving', current_action: `multiplexing ${s.sessions ?? 0} session(s)`,
      attrs: { sessions: String(s.sessions ?? 0), ...(s.attrs ?? {}) },
    }, this.now)
    if (!isValidStatus(status)) { this.counters.dropped++; log('warn', 'statusplane_core_emit_drop', { kind: 'status' }) }
    else if (await this.publishEnvelope(this.statusSub, `topic:${STATUS_TOPIC}`, JSON.stringify(status))) this.counters.status_published++
  }

  /** Wrap a body in a SIGNED a2a envelope (from the core identity) and publish. Built to MATCH
   *  a2a-channel send() exactly so the detector's verifyInbound + verifyEnvelope accept it: env.from
   *  === the core agent id (= the body's agent_id), env.alg/sig over canonical(env). */
  private async publishEnvelope(subject: string, to: string, body: string): Promise<boolean> {
    const signer = this.coreSigner!
    const env: Envelope = { v: 1, id: randomUUID(), from: signer.agentId, to, type: 'msg', ts: new Date(this.now()).toISOString(), body }
    try {
      env.alg = signer.alg
      env.sig = await signEnvelope(env, signer.alg, signer.signKey)
      if (!this.started) return false // stop() raced us DURING the sign — don't publish onto a draining ncControl (folds P2 stop-race; the try/catch still backstops a drain between here and publish)
      this.nc.publish(subject, enc(JSON.stringify(env)))
      return true
    } catch (e) { this.counters.dropped++; log('warn', 'statusplane_core_emit_failed', { subject, ...errFields(e) }); return false }
  }

  start(): void {
    if (this.started) return
    this.started = true
    if (this.coreBeatMs > 0) {
      if (this.coreSigner) {
        void this.emitCore().catch((e) => log('warn', 'statusplane_tick_failed', errFields(e))) // one immediately
        this.timer = setInterval(() => { void this.emitCore().catch((e) => log('warn', 'statusplane_tick_failed', errFields(e))) }, this.coreBeatMs)
      } else {
        log('warn', 'statusplane_core_beat_disabled', { reason: 'no_core_signer', note: 'set A2A_CORE_AGENT_ID + A2A_CORE_SIGNING_KEY (onboard a2a-core-<host>)' })
      }
    }
    log('info', 'statusplane_started', { beat: this.beatSub, status: this.statusSub, core_beat_ms: this.coreBeatMs, signing: this.coreSigner ? this.coreSigner.agentId : 'off' })
  }

  stop(): void {
    if (this.timer) { clearInterval(this.timer); this.timer = undefined }
    if (this.started) log('info', 'statusplane_stopped', this.counts())
    this.started = false
  }
}
