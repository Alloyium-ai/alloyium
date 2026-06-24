#!/usr/bin/env bun
import { randomUUID } from 'node:crypto'
import { RedisClient } from 'bun'
import { A2AChannel } from './a2a-channel.ts'
import { AGENT_BEAT_SCHEMA, AGENT_BEAT_TOPIC, defaultBeatEmitterId, type AgentBeat } from './agent_beat.ts'

/** Advisory wedge alert schema emitted by the detector. */
export const WEDGE_ALERT_SCHEMA = 'agent.wedge.alert.v1' as const

/** Detector classification, including suppressor states that never alert. */
export type WedgeClass =
  | 'healthy'
  | 'idle-ok'
  | 'service-alive'
  | 'offline'
  | 'wedged:loop-dead'
  | 'wedged:long-turn'
  | 'wedged:stale-goal-spin'
  | 'suppressed:boot-grace'
  | 'suppressed:expected-silent'
  | 'suppressed:plane-unhealthy'

/** Redis-backed per-agent detector state. */
export interface DetectorAgentState {
  agent_id: string
  boot_id?: string
  session_id?: string
  presence_started_at?: string
  first_presence_seen_ms?: number
  identity_first_recv_ms?: number
  last_recv_ms?: number
  last_loop_seq?: number
  last_loop_seq_advance_ms?: number
  stale_progress_cadences?: number
  missed_count?: number
  classification?: WedgeClass
  last_alert_class?: WedgeClass
  last_alert_at_ms?: number
  last_beat?: AgentBeat
  updated_ms: number
}

/** A2A presence record written by `A2AChannel`. */
export interface PresenceRecord {
  token: string
  host?: string
  started_at?: string
  last_seen?: string
}

/** Operator-consumable advisory alert. */
export interface WedgeAlert {
  schema: typeof WEDGE_ALERT_SCHEMA
  alert_id: string
  detector_id: string
  agent_id: string
  class: Extract<WedgeClass, `wedged:${string}`>
  reason: string
  observed_at: string
  target_boot_id?: string
  target_session_id?: string
  loop_seq?: number
  last_recv_ts?: string
  last_turn_ts?: string
  last_progress_ts?: string
  presence?: PresenceRecord
  presence_started_at?: string
  advisory_only: true
}

/** Runtime knobs for wedge detection. */
export interface DetectorConfig {
  detectorId: string
  beatTopic: string
  alertTopic: string
  presencePrefix: string
  expectedSilentPrefix: string
  cadenceOverridePrefix: string
  livenessClassPrefix: string
  statePrefix: string
  highwaterPrefix: string
  redisUrl: string
  redisOpTimeoutMs: number
  evalIntervalMs: number
  beatCadenceMs: number
  missedBeats: number
  minCadenceMs: number
  maxCadenceMs: number
  bootGraceMs: number
  presenceFreshMs: number
  longTurnMs: number
  progressStaleMs: number
  staleProgressCadences: number
  alertRepeatMs: number
  stateTtlS: number
  planeFreshMs: number
  globalGateMinAgents: number
  globalMinFreshRatio: number
  beatEmitterSuffix: string
  ignoreAgentRe: RegExp
  transientAgentRe: RegExp
  censusAgeOutMs: number
  maxTrackedAgents: number
}

/** Load detector config from environment with conservative defaults. */
export function loadDetectorConfig(env = process.env): DetectorConfig {
  const cadence = num(env.WEDGE_BEAT_CADENCE_MS, 15 * 60_000)
  return {
    detectorId: env.A2A_AGENT_ID || 'wedge-detector',
    beatTopic: env.WEDGE_BEAT_TOPIC || AGENT_BEAT_TOPIC,
    alertTopic: env.WEDGE_ALERT_TOPIC || 'agent-wedge',
    presencePrefix: env.A2A_PRESENCE_KEY_PREFIX || 'alloyium:a2a:presence:',
    expectedSilentPrefix: env.WEDGE_EXPECTED_SILENT_PREFIX || 'alloyium:a2a:expected-silent:',
    cadenceOverridePrefix: env.WEDGE_CADENCE_OVERRIDE_PREFIX || 'alloyium:a2a:wedge-cadence:',
    livenessClassPrefix: env.WEDGE_LIVENESS_CLASS_PREFIX || 'alloyium:a2a:liveness-class:',
    statePrefix: env.WEDGE_STATE_PREFIX || 'alloyium:a2a:wedge-detector:state:',
    highwaterPrefix: env.WEDGE_HIWATER_PREFIX || 'alloyium:a2a:wedge-detector:hiwater:',
    redisUrl: env.REDIS_URL || 'redis://redis:6379',
    redisOpTimeoutMs: num(env.WEDGE_REDIS_TIMEOUT_MS, 2500),
    evalIntervalMs: num(env.WEDGE_EVAL_INTERVAL_MS, 30_000),
    beatCadenceMs: cadence,
    missedBeats: num(env.WEDGE_MISSED_BEATS, 3),
    // Bounds for a per-agent cadence override (clamp): floor stops a fat-fingered tiny override →
    // instant false loop-dead; ceiling bounds a too-large (or self-set) override from fully
    // suppressing loop-dead. Tests set WEDGE_MIN_CADENCE_MS=0 to use compressed cadences.
    minCadenceMs: num(env.WEDGE_MIN_CADENCE_MS, 120_000),     // 2 min floor
    maxCadenceMs: num(env.WEDGE_MAX_CADENCE_MS, 3_600_000),   // 1 h ceiling
    bootGraceMs: num(env.WEDGE_BOOT_GRACE_MS, 5 * 60_000),
    presenceFreshMs: num(env.WEDGE_PRESENCE_FRESH_MS, 120_000),
    longTurnMs: num(env.WEDGE_LONG_TURN_MS, 60 * 60_000),
    progressStaleMs: num(env.WEDGE_PROGRESS_STALE_MS, 45 * 60_000),
    staleProgressCadences: num(env.WEDGE_STALE_PROGRESS_CADENCES, 3),
    alertRepeatMs: num(env.WEDGE_ALERT_REPEAT_MS, 15 * 60_000),
    stateTtlS: num(env.WEDGE_STATE_TTL_S, 7 * 24 * 3600),
    planeFreshMs: num(env.WEDGE_PLANE_FRESH_MS, Math.max(cadence * 2, 60_000)),
    globalGateMinAgents: num(env.WEDGE_GLOBAL_GATE_MIN_AGENTS, 3),
    globalMinFreshRatio: Number(env.WEDGE_GLOBAL_MIN_FRESH_RATIO ?? 0.5),
    beatEmitterSuffix: env.WEDGE_BEAT_EMITTER_SUFFIX || '-beat',
    ignoreAgentRe: new RegExp(env.WEDGE_IGNORE_AGENT_RE || '^(wedge-detector|layer2-supervisor)$|.*-beat$'),
    // Transient/ephemeral ids (test harnesses, fusion request workers, org-review one-shots) that briefly
    // appear with presence then vanish. Excluded from BOTH the census and the plane fresh-ratio: an
    // ephemeral holding a stale presence record would otherwise sit in the plane denominator un-fresh and
    // drag the gate (the #32 false-trip class, alongside expected-silent). Operator-overridable.
    transientAgentRe: new RegExp(env.WEDGE_TRANSIENT_AGENT_RE || '^(ztest|fusion-req|orgrev)-'),
    // General, prefix-AGNOSTIC census age-out: an OFFLINE (presence-absent) id whose last beat-receive is
    // older than this is a dead/transient remnant, dropped so the census == live presence. New transient
    // namespaces appear faster than transientAgentRe can track; this catches them all. 30 min: safely past
    // any relaunch/presence blip, far under stateTtlS (7d). See isStaleOffline.
    censusAgeOutMs: num(env.WEDGE_CENSUS_AGEOUT_MS, 30 * 60_000),
    maxTrackedAgents: num(env.WEDGE_MAX_TRACKED_AGENTS, 4096),
  }
}

/** Parse and validate a beat body. Invalid beats return null. */
export function parseAgentBeat(body: string): AgentBeat | null {
  let x: any
  try { x = JSON.parse(body) } catch { return null }
  if (x?.schema !== AGENT_BEAT_SCHEMA) return null
  if (!/^[a-z0-9-]{1,64}$/.test(x.agent_id)) return null
  if (!Number.isFinite(x.loop_seq) || x.loop_seq < 0) return null
  // Accept any non-empty driver_mode (loop|goal|service + future modes): classification
  // is mode-agnostic (live-by-loop_seq), so an unknown mode is never dropped or thrown
  // on — and the a2a-core's own 'service' beat is tracked (a2a-core-pm SLICE-3 SoT extension).
  if (typeof x.driver_mode !== 'string' || !x.driver_mode) return null
  if (!['booting', 'idle', 'in_task', 'paused', 'draining'].includes(x.state)) return null
  if (!Array.isArray(x.task_ids)) return null
  return {
    schema: AGENT_BEAT_SCHEMA,
    agent_id: x.agent_id,
    host: String(x.host || ''),
    boot_id: String(x.boot_id || ''),
    session_id: String(x.session_id || ''),
    loop_seq: Math.floor(x.loop_seq),
    turn_start_ts: String(x.turn_start_ts || ''),
    last_turn_ts: String(x.last_turn_ts || ''),
    last_progress_ts: String(x.last_progress_ts || ''),
    driver_mode: x.driver_mode,
    stop_hook_active: Boolean(x.stop_hook_active),
    ...(x.goal_id ? { goal_id: String(x.goal_id) } : {}),
    ...(Number.isFinite(x.goal_epoch) ? { goal_epoch: Number(x.goal_epoch) } : {}),
    state: x.state,
    inbox_depth: Math.max(0, Math.floor(Number(x.inbox_depth || 0))),
    task_ids: x.task_ids.map(String).slice(0, 128),
  }
}

/** Resolve loop-tied vs presence-based liveness (#32 residual-a). Registry (operator/launcher-set
 *  livenessClass) is AUTHORITATIVE; the beat's driver_mode only informs the default; a never-beaten
 *  presence-only agent defaults to 'service' (fail-safe — a no-beat gateway). 'goal'-driver agents are
 *  loop-tied for wedge purposes. Shared by classifyAgent AND the plane-health gate so a service is
 *  treated identically in both — in particular EXCLUDED from the loop-beat fresh-ratio. */
export function resolveLivenessMode(
  livenessClass: 'loop' | 'service' | undefined,
  beatMode: string | undefined,
  hasBeaten: boolean,
): 'loop' | 'service' {
  if (livenessClass === 'service') return 'service'
  if (livenessClass === 'loop') return 'loop'
  if (beatMode === 'service') return 'service'
  if (beatMode === 'loop' || beatMode === 'goal') return 'loop'
  if (!hasBeaten) return 'service'
  return 'loop'
}

/** Per-agent plane-freshness window (ms). Default agents use the single global cfg.planeFreshMs.
 *  An idle /loop agent with an OPERATOR/LAUNCHER-set cadence override (alloyium:a2a:wedge-cadence:)
 *  beats slower than the global window BY DESIGN (e.g. a 30-min /loop), so a single global window would
 *  read it stale and drag the fresh-ratio under the gate (false plane-unhealthy → masks Layer-2). Honor
 *  the SAME operator-set override used for the per-agent missed-calc (classifyAgent), clamped to
 *  [minCadenceMs, maxCadenceMs], scaled x2 to mirror the global default (planeFreshMs = beatCadence*2).
 *  The window only ever WIDENS (max with the global) — a fast agent keeps the tight global window — and a
 *  genuine plane-wide outage is still caught: when every live agent stops, fresh=0 for ANY window.
 *  NOTE: this only helps agents that HAVE an override set; an idle no-override agent still reads stale and
 *  is carried by the rest of the fresh /loop population (the ratio), not by this widening. */
export function planeFreshMsForAgent(cadenceOverrideMs: number | undefined, cfg: DetectorConfig): number {
  if (!(Number.isFinite(cadenceOverrideMs) && (cadenceOverrideMs as number) > 0)) return cfg.planeFreshMs
  const clamped = Math.min(Math.max(cadenceOverrideMs as number, cfg.minCadenceMs), cfg.maxCadenceMs)
  return Math.max(cfg.planeFreshMs, clamped * 2)
}

/** Plane-health over the LOOP-driven, NON-deliberately-silent population. Services/gateways AND
 *  expected-silent agents are excluded: services legitimately don't beat; expected-silent agents are
 *  DELIBERATELY silent during a rolling relaunch / maintenance window. Counting EITHER drags the
 *  fresh-beat ratio under the gate and falsely suppresses loop-dead for real /loop agents
 *  (suppressed:plane-unhealthy → masks Layer-2 — the #32 observe-E2E false-trip). Pure + exported so BOTH
 *  exclusions are REGRESSION-tested (a revert still passes the classifyAgent unit tests, so this gate
 *  needs its own coverage). Freshness is PER-AGENT (planeFreshMsForAgent) so an idle agent at its
 *  operator-set cadence is not counted stale. A genuine plane-WIDE outage cannot be masked — when no loop
 *  agent is fresh, fresh=0 so the ratio is 0 regardless of exclusions or window. (Small-N trade-off, same
 *  as the service exclusion: excluding a FRESH deliberately-silent agent also drops it from the NUMERATOR,
 *  so in a tiny population a lone wedge may fall to suppressed:plane-unhealthy — fail-safe: a MISSED
 *  actuation, never a false one; the operator accepts this for the active-flip, identical to services.) */
export function planeHealthyForAgents(
  agents: Array<{
    livenessClass?: 'loop' | 'service'
    driverMode?: string
    lastRecvMs?: number
    expectedSilent?: boolean
    cadenceOverrideMs?: number
  }>,
  now: number,
  cfg: DetectorConfig,
): boolean {
  const loop = agents.filter(
    (a) =>
      !a.expectedSilent &&
      resolveLivenessMode(a.livenessClass, a.driverMode, Number.isFinite(a.lastRecvMs) && (a.lastRecvMs as number) > 0) === 'loop',
  )
  const fresh = loop.filter((a) => a.lastRecvMs && now - a.lastRecvMs <= planeFreshMsForAgent(a.cadenceOverrideMs, cfg)).length
  return planeHealthy(loop.length, fresh, cfg)
}

/** Classify one agent from persisted beat state plus current presence. */
export function classifyAgent(args: {
  now: number
  state: DetectorAgentState
  presence: PresenceRecord | null
  expectedSilent: boolean
  planeHealthy: boolean
  config: DetectorConfig
  cadenceOverrideMs?: number
  livenessClass?: 'loop' | 'service'
}): { class: WedgeClass; reason: string; alert: boolean; missed_count: number } {
  const { now, state, presence, expectedSilent, planeHealthy, config, cadenceOverrideMs, livenessClass } = args
  if (!presence || (presence.last_seen && now - ts(presence.last_seen) > config.presenceFreshMs)) {
    return { class: 'offline', reason: 'presence_absent_or_stale', alert: false, missed_count: 0 }
  }
  if (expectedSilent) return { class: 'suppressed:expected-silent', reason: 'redis_expected_silent_marker', alert: false, missed_count: 0 }

  // DRIVER-AWARE liveness (#32 residual-a): resolve loop-tied vs presence-based liveness so
  // services/gateways/non-/loop agents are never false-flagged wedged:loop-dead. Authoritative
  // signal = operator/launcher-set livenessClass (readLivenessClass); the beat's driver_mode only
  // informs the DEFAULT when no registry key exists, and the default is fail-safe (presence-based)
  // unless the agent is loop-declared or is actively emitting loop/goal beats. Presence stays the
  // offline baseline above — a service with stale/absent presence already returned 'offline'.
  const beatMode = state.last_beat?.driver_mode
  const hasBeaten = Number.isFinite(state.last_recv_ms) && (state.last_recv_ms as number) > 0
  const mode = resolveLivenessMode(livenessClass, beatMode, hasBeaten)

  // Soft cross-check (non-authoritative): operator class vs self-reported beat mode disagree.
  if (
    livenessClass &&
    beatMode &&
    ((livenessClass === 'service' && (beatMode === 'loop' || beatMode === 'goal')) ||
      (livenessClass === 'loop' && beatMode === 'service'))
  ) {
    log('liveness_class_driver_mode_mismatch', { agent_id: state.agent_id, livenessClass, beatMode })
  }

  if (mode === 'service') {
    const reason =
      livenessClass === 'service'
        ? 'service_class_registry'
        : beatMode === 'service'
          ? 'service_inferred_from_beat_driver_mode'
          : 'service_inferred_presence_only_no_beat'
    return { class: 'service-alive', reason, alert: false, missed_count: 0 }
  }

  const firstSeen = state.first_presence_seen_ms ?? now
  if (!state.last_recv_ms) {
    if (now - firstSeen < config.bootGraceMs) return { class: 'suppressed:boot-grace', reason: 'presence_seen_before_first_beat', alert: false, missed_count: 0 }
    if (!planeHealthy) return { class: 'suppressed:plane-unhealthy', reason: 'global_beat_rate_unhealthy', alert: false, missed_count: 0 }
    return { class: 'wedged:loop-dead', reason: 'presence_fresh_but_no_monotonic_beat', alert: true, missed_count: config.missedBeats }
  }

  const recvAge = now - state.last_recv_ms
  // Per-agent cadence override (OPERATOR/LAUNCHER-set Redis key — NOT self-declared, so an
  // agent can't widen its own watch window to escape detection). Authoritative for THIS
  // agent's missed-calc only; the global plane-health gate keeps using config.beatCadenceMs.
  // Override (operator-set) is CLAMPED to [minCadenceMs, maxCadenceMs]: floor bounds a too-small
  // override (→ instant false loop-dead on a healthy agent); ceiling bounds a too-large/self-set
  // override from fully suppressing loop-dead. The global default is used unclamped (its own knob).
  const effectiveCadence =
    Number.isFinite(cadenceOverrideMs) && cadenceOverrideMs! > 0
      ? Math.min(Math.max(cadenceOverrideMs!, config.minCadenceMs), config.maxCadenceMs)
      : config.beatCadenceMs
  const missed = Math.max(0, Math.floor(recvAge / effectiveCadence))
  if (missed >= config.missedBeats) {
    if (!planeHealthy) return { class: 'suppressed:plane-unhealthy', reason: 'global_beat_rate_unhealthy', alert: false, missed_count: missed }
    return { class: 'wedged:loop-dead', reason: `missed_${missed}_cadences_by_monotonic_receive_time`, alert: true, missed_count: missed }
  }

  const beat = state.last_beat
  if (!beat) return { class: 'healthy', reason: 'presence_fresh', alert: false, missed_count: missed }

  const active = beat.state !== 'idle' && beat.state !== 'paused' && beat.state !== 'draining'
  const spinCandidate = active || beat.stop_hook_active
  if (spinCandidate && (state.stale_progress_cadences ?? 0) >= config.staleProgressCadences) {
    return { class: 'wedged:stale-goal-spin', reason: 'loop_seq_advancing_without_progress', alert: true, missed_count: missed }
  }

  if (beat.state === 'idle' && beat.task_ids.length === 0) return { class: 'idle-ok', reason: 'fresh_idle_beat_no_tasks', alert: false, missed_count: missed }

  if (active) {
    // long-turn = the CURRENT turn's duration → measure from turn_start_ts (set at turn-start,
    // preserved across keepalives), falling back to last_turn_ts. Using last_turn_ts alone
    // conflated the idle gap with the turn ("last COMPLETED turn"), so a healthy agent resuming
    // after a long idle false-tripped wedged:long-turn (panel P1a/P2). Phase A turn-start fix.
    const turnStartedAt = ts(beat.turn_start_ts) || ts(beat.last_turn_ts)
    if (turnStartedAt && now - turnStartedAt > config.longTurnMs) {
      return { class: 'wedged:long-turn', reason: 'turn_start_ts_exceeded_sla', alert: true, missed_count: missed }
    }
  }

  return { class: 'healthy', reason: 'fresh_beat', alert: false, missed_count: missed }
}

/**
 * True when `beat` is a KEEPALIVE relative to `prevBeat`: loop_seq advanced but turn_start_ts,
 * last_turn_ts AND last_progress_ts are all unchanged. The wire schema (agent.beat.v1, FROZEN)
 * carries NO phase field, so the detector infers keepalive from this clock signature — which is
 * exhaustive for the current phases (turn-start changes turn_start_ts; turn-end changes last_turn_ts;
 * progress changes last_progress_ts; only a keepalive advances loop_seq while preserving all three).
 * Any future phase with the same signature is, by definition, proof-of-life-without-progress and
 * SHOULD likewise be excluded from spin accounting. #32 residual-(b) gate.
 */
export function isKeepaliveBeat(prevBeat: AgentBeat | undefined, beat: AgentBeat): boolean {
  return !!prevBeat
    && beat.loop_seq > prevBeat.loop_seq
    && beat.turn_start_ts === prevBeat.turn_start_ts
    && beat.last_turn_ts === prevBeat.last_turn_ts
    && beat.last_progress_ts === prevBeat.last_progress_ts
}

/**
 * Decide the next `stale_progress_cadences` counter (spin detection) for one ingested beat.
 *
 * A KEEPALIVE beat (see isKeepaliveBeat) is intra-turn proof-of-life, NOT a turn completing without
 * progress. It must NEITHER advance NOR reset the counter:
 *   - advancing it would false-trip wedged:stale-goal-spin on a healthy long turn, because the
 *     PostToolUse keepalive advances loop_seq every few minutes while last_progress_ts stays put;
 *   - resetting it would mask a real spin where keepalives interleave with churning turns.
 * loop-dead and long-turn are unaffected (last_recv_ms + turn_start_ts still flow through ingest).
 * A genuine spin advances via TURN beats (turn-end bumps last_turn_ts) → not a keepalive → counted.
 *
 * Counts exactly +1 per real (non-keepalive) active-without-progress beat — NOT seqDelta: keepalives
 * inflate loop_seq, so a detector that dropped a turn-start + early keepalives (NATS is lossy; e.g. a
 * detector restart at turn start) and caught a later beat would see a large seqDelta and FALSELY cross
 * the spin threshold in a single ingest. #32 residual-(b) gate (Opus 4.8 P2).
 */
export function nextStaleProgressCadences(args: {
  prev: number
  activeForSpin: boolean
  progressFresh: boolean
  sameIdentity: boolean
  presenceChanged: boolean
  isKeepalive: boolean
}): number {
  const prev = Math.max(0, args.prev || 0)
  if (args.isKeepalive) return prev
  if (!args.activeForSpin || args.progressFresh || !args.sameIdentity || args.presenceChanged) return 0
  return prev + 1
}

/** True when the signed sender identity may report this watched agent. */
export function allowedBeatEmitter(agentId: string, from: string, cfg: DetectorConfig): boolean {
  return from === agentId || from === defaultBeatEmitterId(agentId) || from === `${agentId}${cfg.beatEmitterSuffix}`
}

/** Compute the global beat-rate gate used to suppress plane-wide outages. */
export function planeHealthy(activeAgents: number, freshBeatAgents: number, cfg: DetectorConfig): boolean {
  if (activeAgents <= 0) return false
  if (activeAgents < cfg.globalGateMinAgents) return freshBeatAgents > 0
  return freshBeatAgents / activeAgents >= cfg.globalMinFreshRatio
}

/** True when an id must be EXCLUDED from the census + plane calc: infra ids (the detector/supervisor and
 *  beat-sidecars, via ignoreAgentRe) and transient/ephemeral ids (test / fusion-req / orgrev one-shots,
 *  via transientAgentRe). A transient that briefly holds a stale presence record would otherwise sit in
 *  the plane denominator un-fresh and drag the gate (stateTtlS=7d is too long to wait it out). Pure +
 *  exported for regression coverage. */
export function isCensusExcluded(id: string, cfg: DetectorConfig): boolean {
  return cfg.ignoreAgentRe.test(id) || cfg.transientAgentRe.test(id)
}

/** General, prefix-AGNOSTIC census age-out (#32 Fix#3): true when an id is an OFFLINE remnant that should
 *  drop from the census — NO current presence AND its last beat-receive (or first-presence sighting, if it
 *  never beat) is older than censusAgeOutMs. New transient namespaces (cb-, cp-, ct- style one-shots) appear
 *  faster than transientAgentRe can track, and stateTtlS=7d keeps their mem state far too long; this catches
 *  them ALL once they go silent, regardless of prefix. CRUCIAL SAFETY INVARIANT: a presence-PRESENT agent is
 *  NEVER aged out, so a genuinely WEDGED agent (presence-fresh + beat-stale — the thing we detect) can never
 *  be hidden by this; only presence-absent (offline, non-actionable) remnants drop, and they reappear the
 *  instant they re-claim presence. Aged-out ids have no presence, so they are ALREADY absent from
 *  activeIds/the plane fresh-ratio — this is pure census==presence hygiene with ZERO plane-health effect.
 *  Pure + exported for regression coverage. */
export function isStaleOffline(presence: PresenceRecord | null, st: DetectorAgentState | undefined, now: number, cfg: DetectorConfig): boolean {
  if (presence) return false
  const lastAlive = st?.last_recv_ms ?? st?.first_presence_seen_ms
  return !Number.isFinite(lastAlive) || now - (lastAlive as number) > cfg.censusAgeOutMs
}

/** Standalone advisory wedge detector service. */
export class WedgeDetector {
  private redis: RedisClient
  private a2a: A2AChannel
  private mem = new Map<string, DetectorAgentState>()
  private timers: ReturnType<typeof setInterval>[] = []

  constructor(private cfg = loadDetectorConfig()) {
    // NB: initialize `redis` inside the ctor body — a class field initializer
    // (`= new RedisClient(this.cfg.redisUrl)`) runs BEFORE the `cfg` parameter
    // property is assigned, so `this.cfg` is undefined there → construction throws.
    this.redis = new RedisClient(this.cfg.redisUrl)
    this.a2a = new A2AChannel((content, attrs) => this.onInbound(content, attrs), {
      enabled: true,
      agentId: cfg.detectorId,
      natsUrl: process.env.NATS_URL,
      redisUrl: cfg.redisUrl,
      transportAuth: (process.env.A2A_TRANSPORT_AUTH as any),
      signingKeyPath: process.env.A2A_SIGNING_KEY,
      sigAlg: (process.env.A2A_SIG_ALG as any) ?? 'ed25519',
      devNoAuth: process.env.A2A_DEV_NO_AUTH === '1' || process.env.A2A_DEV_NO_AUTH === 'true',
    })
  }

  /** Start the bus peer, topic join retry, and periodic evaluator. */
  async start(): Promise<void> {
    await this.a2a.start().catch((e) => log('detector_start_failed', { err: String(e) }))
    await this.joinBeatTopic()
    this.timers.push(setInterval(() => void this.joinBeatTopic(), 30_000))
    this.timers.push(setInterval(() => void this.evaluateOnce(), this.cfg.evalIntervalMs))
    await this.evaluateOnce()
  }

  /** Stop timers and drain resources best-effort. */
  async stop(): Promise<void> {
    for (const t of this.timers) clearInterval(t)
    await this.a2a.stop().catch(() => {})
    try { (this.redis as any).close?.() } catch {}
  }

  private async joinBeatTopic(): Promise<void> {
    if (!this.a2a.isStarted()) return
    await this.a2a.callTool('a2a_join_topic', { topic: this.cfg.beatTopic }).catch(() => {})
  }

  private async onInbound(content: string, attrs: Record<string, string>): Promise<void> {
    const beat = parseAgentBeat(content)
    if (!beat) return
    if (!allowedBeatEmitter(beat.agent_id, attrs.from || '', this.cfg)) {
      log('beat_emitter_rejected', { agent_id: beat.agent_id, from: attrs.from })
      return
    }
    await this.ingestBeat(beat)
  }

  private async ingestBeat(beat: AgentBeat): Promise<void> {
    const now = Date.now()
    const presence = await this.readPresence(beat.agent_id)
    const st = await this.loadState(beat.agent_id) ?? { agent_id: beat.agent_id, updated_ms: now }

    const sameIdentity = st.boot_id === beat.boot_id && st.session_id === beat.session_id
    const presenceChanged = Boolean(presence?.started_at && st.presence_started_at && presence.started_at !== st.presence_started_at)

    if (st.boot_id && st.session_id && !sameIdentity && !presenceChanged) {
      log('superseded_identity_beat_ignored', { agent_id: beat.agent_id, boot_id: beat.boot_id, session_id: beat.session_id, active_boot_id: st.boot_id, active_session_id: st.session_id })
      return
    }

    if (!(await this.claimHighWater(beat, st, sameIdentity))) {
      log('replayed_or_duplicate_beat_ignored', { agent_id: beat.agent_id, boot_id: beat.boot_id, session_id: beat.session_id, loop_seq: beat.loop_seq })
      return
    }

    // Capture the prior beat BEFORE st.last_beat is overwritten below, to detect a keepalive
    // (loop_seq advanced but all turn/progress clocks preserved → must not accumulate spin).
    const prevBeat = sameIdentity ? st.last_beat : undefined
    const isKeepalive = isKeepaliveBeat(prevBeat, beat)
    const activeForSpin = beat.stop_hook_active || (beat.state !== 'idle' && beat.state !== 'paused' && beat.state !== 'draining')
    const progressTs = ts(beat.last_progress_ts)
    const progressFresh = progressTs > 0 && now - progressTs <= this.cfg.progressStaleMs

    st.boot_id = beat.boot_id
    st.session_id = beat.session_id
    st.presence_started_at = presence?.started_at
    st.identity_first_recv_ms = sameIdentity && !presenceChanged ? (st.identity_first_recv_ms ?? now) : now
    st.last_recv_ms = now
    st.last_loop_seq_advance_ms = now
    st.last_beat = beat
    st.last_loop_seq = beat.loop_seq
    st.updated_ms = now
    st.stale_progress_cadences = nextStaleProgressCadences({
      prev: st.stale_progress_cadences ?? 0,
      activeForSpin,
      progressFresh,
      sameIdentity,
      presenceChanged,
      isKeepalive,
    })

    this.setMem(st)
    await this.saveState(st)
  }

  private async evaluateOnce(): Promise<void> {
    const now = Date.now()
    const presences = await this.listPresences()
    await this.loadAllStates()
    const ids = new Set([...presences.keys(), ...this.mem.keys()])
    // Census filter (#32 Fix#3): drop infra + known-transient prefixes (isCensusExcluded) AND general
    // offline remnants — presence-absent ids whose last beat is older than censusAgeOutMs (isStaleOffline),
    // catching new transient namespaces no prefix-regex tracks. Both only ever remove deliberately-ignored
    // or presence-ABSENT ids, so no live (and hence no wedged) agent is dropped.
    for (const id of [...ids]) {
      if (isCensusExcluded(id, this.cfg) || isStaleOffline(presences.get(id) ?? null, this.mem.get(id), now, this.cfg)) ids.delete(id)
    }

    const activeIds = [...ids].filter((id) => presences.has(id))
    // Per-active-agent signals read ONCE here, reused in the per-agent loop below: liveness class,
    // expected-silent marker, cadence override. Services/gateways AND expected-silent agents are excluded
    // from the plane fresh-beat ratio (they don't beat — legitimately or deliberately — so counting them
    // drags the ratio under the gate and falsely suppresses loop-dead: suppressed:plane-unhealthy → masks
    // Layer-2); an idle agent's operator-set cadence widens its plane-freshness window so it is not counted
    // stale. Plane health measures the LOOP-driven, non-deliberately-silent population (#32 residual-a + observe-E2E).
    const livenessClasses = new Map<string, 'loop' | 'service' | undefined>()
    const expectedSilentMap = new Map<string, boolean>()
    const cadenceOverrides = new Map<string, number | undefined>()
    for (const id of activeIds) {
      livenessClasses.set(id, await this.readLivenessClass(id))
      expectedSilentMap.set(id, await this.hasExpectedSilent(id))
      cadenceOverrides.set(id, await this.readCadenceOverrideMs(id))
    }
    const planeOk = planeHealthyForAgents(
      activeIds.map((id) => {
        const st = this.mem.get(id)
        return {
          livenessClass: livenessClasses.get(id),
          driverMode: st?.last_beat?.driver_mode,
          lastRecvMs: st?.last_recv_ms,
          expectedSilent: expectedSilentMap.get(id),
          cadenceOverrideMs: cadenceOverrides.get(id),
        }
      }),
      now,
      this.cfg,
    )

    for (const id of ids) {
      const st = this.mem.get(id) ?? { agent_id: id, updated_ms: now }
      const presence = presences.get(id) ?? null

      if (presence?.started_at && st.presence_started_at && presence.started_at !== st.presence_started_at) {
        st.boot_id = undefined
        st.session_id = undefined
        st.last_recv_ms = undefined
        st.last_loop_seq = undefined
        st.last_loop_seq_advance_ms = undefined
        st.last_beat = undefined
        st.stale_progress_cadences = 0
        st.first_presence_seen_ms = now
        st.presence_started_at = presence.started_at
      } else if (presence && !st.first_presence_seen_ms) {
        st.first_presence_seen_ms = now
        st.presence_started_at = presence.started_at
      }

      // Active ids were read into the maps above; non-active (presence-absent) ids short-circuit to
      // 'offline' in classifyAgent before either value is consulted, so a map miss → safe default.
      const expectedSilent = expectedSilentMap.get(id) ?? false
      const cadenceOverrideMs = cadenceOverrides.get(id)
      const livenessClass = livenessClasses.get(id)
      const cls = classifyAgent({ now, state: st, presence, expectedSilent, planeHealthy: planeOk, config: this.cfg, cadenceOverrideMs, livenessClass })
      st.classification = cls.class
      st.missed_count = cls.missed_count
      st.updated_ms = now
      this.setMem(st)

      if (cls.alert && this.shouldAlert(st, cls.class)) {
        await this.emitAlert(st, presence, cls.class as Extract<WedgeClass, `wedged:${string}`>, cls.reason)
        st.last_alert_class = cls.class
        st.last_alert_at_ms = now
      }
      await this.saveState(st)
    }
  }

  private async claimHighWater(beat: AgentBeat, st: DetectorAgentState, sameIdentity: boolean): Promise<boolean> {
    const memHigh = sameIdentity ? (st.last_loop_seq ?? -1) : -1
    if (beat.loop_seq <= memHigh) return false

    const script =
      "local cur=redis.call('GET',KEYS[1]); " +
      "if cur and tonumber(cur)>=tonumber(ARGV[1]) then return 0 end; " +
      "redis.call('SET',KEYS[1],ARGV[1],'EX',tonumber(ARGV[2])); return 1"
    try {
      const r = await withTimeout(this.redis.send('EVAL', [script, '1', this.highwaterKey(beat), String(beat.loop_seq), String(this.cfg.stateTtlS)]), this.cfg.redisOpTimeoutMs, 'redis.highwater')
      return Number(r) === 1
    } catch {
      return true
    }
  }

  private highwaterKey(beat: AgentBeat): string {
    return this.cfg.highwaterPrefix + [beat.agent_id, beat.boot_id, beat.session_id].map(safeKey).join(':')
  }

  private shouldAlert(st: DetectorAgentState, cls: WedgeClass): boolean {
    return st.last_alert_class !== cls || Date.now() - (st.last_alert_at_ms ?? 0) >= this.cfg.alertRepeatMs
  }

  private async emitAlert(st: DetectorAgentState, presence: PresenceRecord | null, cls: Extract<WedgeClass, `wedged:${string}`>, reason: string): Promise<void> {
    const beat = st.last_beat
    const alert: WedgeAlert = {
      schema: WEDGE_ALERT_SCHEMA,
      alert_id: `wa-${randomUUID()}`,
      detector_id: this.cfg.detectorId,
      agent_id: st.agent_id,
      class: cls,
      reason,
      observed_at: new Date().toISOString(),
      target_boot_id: st.boot_id,
      target_session_id: st.session_id,
      loop_seq: st.last_loop_seq,
      last_recv_ts: st.last_recv_ms ? new Date(st.last_recv_ms).toISOString() : undefined,
      last_turn_ts: beat?.last_turn_ts,
      last_progress_ts: beat?.last_progress_ts,
      presence: presence ?? undefined,
      presence_started_at: presence?.started_at,
      advisory_only: true,
    }
    log('wedge_alert', alert)
    if (!this.a2a.isStarted()) return
    await this.a2a.callTool('a2a_send', {
      to: `topic:${this.cfg.alertTopic}`,
      body: JSON.stringify(alert),
      attrs: { schema: WEDGE_ALERT_SCHEMA, agent_id: st.agent_id, class: cls },
    }).catch(() => {})
  }

  private async readPresence(id: string): Promise<PresenceRecord | null> {
    const raw = await this.redisGet(this.cfg.presencePrefix + id)
    if (!raw) return null
    try { return JSON.parse(raw) as PresenceRecord } catch { return null }
  }

  private async listPresences(): Promise<Map<string, PresenceRecord>> {
    const out = new Map<string, PresenceRecord>()
    for (const key of await this.scanKeys(this.cfg.presencePrefix + '*')) {
      const id = key.slice(this.cfg.presencePrefix.length)
      const raw = await this.redisGet(key)
      if (!raw) continue
      try { out.set(id, JSON.parse(raw)) } catch {}
    }
    return out
  }

  private async hasExpectedSilent(id: string): Promise<boolean> {
    return (await this.redisGet(this.cfg.expectedSilentPrefix + id)) != null
  }

  /** Operator/launcher-set per-agent liveness class ('loop' | 'service'). PROVENANCE: same trust
   *  model as the cadence-override key — operator/launcher-controlled, NOT self-declared in the beat,
   *  so a watched agent can't self-exempt from loop-dead detection. Redis ACL hardening recommended;
   *  worst case is a missed alert (degraded monitoring), never a false fire. The beat's driver_mode is
   *  only a DEFAULT hint when this key is absent (see classifyAgent). Accepts a bare 'loop'/'service'
   *  or JSON {"class":"loop"|"service"}; anything else → undefined (falls back to default inference). */
  private async readLivenessClass(id: string): Promise<'loop' | 'service' | undefined> {
    const raw = await this.redisGet(this.cfg.livenessClassPrefix + id)
    if (raw == null) return undefined
    const trimmed = raw.trim()
    if (trimmed === 'loop' || trimmed === 'service') return trimmed
    try {
      const parsed = JSON.parse(trimmed)
      if (parsed?.class === 'loop' || parsed?.class === 'service') return parsed.class
    } catch {}
    return undefined
  }

  /** Operator/launcher-set per-agent cadence override (ms). JSON {"cadence_ms":n} or a bare int.
   *  PROVENANCE: this key (cadenceOverridePrefix) is OPERATOR/LAUNCHER-controlled, NOT self-declared
   *  in the beat — so a watched agent can't widen its OWN window from the beat payload. In a hardened
   *  deployment the Redis 'alloyium:a2a:wedge-cadence:' (and 'pubkey:') namespaces SHOULD be
   *  write-restricted to operator principals (ACL) — agents have Redis network access, so without an
   *  ACL a malicious/buggy agent could self-write this key. The clamp ([min,max] in classifyAgent)
   *  bounds the blast radius, and ACTUATION still needs the SEPARATE operator-set layer2-policy opt-in,
   *  so the worst case is a missed alert (degraded monitoring), never a false fire. Trusted-peer model. */
  private async readCadenceOverrideMs(id: string): Promise<number | undefined> {
    const raw = await this.redisGet(this.cfg.cadenceOverridePrefix + id)
    if (!raw) return undefined
    const bare = Number(raw.trim())
    if (Number.isFinite(bare) && bare > 0) return Math.floor(bare)
    try {
      const parsed = JSON.parse(raw)
      const n = Number(parsed?.cadence_ms)
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined
    } catch {
      return undefined
    }
  }

  private stateKey(id: string): string { return this.cfg.statePrefix + id }

  private async loadState(id: string): Promise<DetectorAgentState | null> {
    if (this.mem.has(id)) return this.mem.get(id)!
    const raw = await this.redisGet(this.stateKey(id))
    if (!raw) return null
    try {
      const st = JSON.parse(raw) as DetectorAgentState
      if (!st || typeof st !== 'object' || st.agent_id !== id) return null
      this.setMem(st)
      return st
    } catch { return null }
  }

  private async loadAllStates(): Promise<void> {
    for (const key of await this.scanKeys(this.cfg.statePrefix + '*')) {
      const id = key.slice(this.cfg.statePrefix.length)
      await this.loadState(id)
    }
  }

  private setMem(st: DetectorAgentState): void {
    if (!this.mem.has(st.agent_id) && this.mem.size >= this.cfg.maxTrackedAgents) {
      const k = this.mem.keys().next().value
      if (k !== undefined) this.mem.delete(k)
    }
    this.mem.set(st.agent_id, st)
  }

  private async saveState(st: DetectorAgentState): Promise<void> {
    try {
      await withTimeout(this.redis.send('SET', [this.stateKey(st.agent_id), JSON.stringify(st), 'EX', String(this.cfg.stateTtlS)]), this.cfg.redisOpTimeoutMs, 'redis.state.set')
    } catch {}
  }

  private async redisGet(key: string): Promise<string | null> {
    try { return await withTimeout(this.redis.get(key), this.cfg.redisOpTimeoutMs, 'redis.get') } catch { return null }
  }

  private async scanKeys(pattern: string): Promise<string[]> {
    const keys: string[] = []
    let cursor = '0'
    try {
      do {
        const res: any = await withTimeout(this.redis.send('SCAN', [cursor, 'MATCH', pattern, 'COUNT', '200']), this.cfg.redisOpTimeoutMs, 'redis.scan')
        cursor = String(res[0])
        keys.push(...((res[1] ?? []) as string[]))
      } while (cursor !== '0')
    } catch {}
    return keys
  }
}

function ts(s?: string): number {
  const n = s ? Date.parse(s) : NaN
  return Number.isFinite(n) ? n : 0
}

function num(v: string | undefined, d: number): number {
  const n = Number(v)
  return Number.isFinite(n) && n >= 0 ? n : d
}

function safeKey(s: string): string {
  return String(s || 'unknown').replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 128)
}

function withTimeout<T>(p: Promise<T> | T, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((_, rej) => { t = setTimeout(() => rej(new Error(`${label} timed out`)), ms) })
  return Promise.race([Promise.resolve(p).finally(() => clearTimeout(t)), timeout])
}

function log(event: string, fields: Record<string, unknown> = {}): void {
  console.error(`${new Date().toISOString()} info [wedge-detector] ${event} ${JSON.stringify(fields)}`)
}

if (import.meta.main) {
  const svc = new WedgeDetector()
  await svc.start()
  for (const sig of ['SIGINT', 'SIGTERM'] as const) process.on(sig, async () => { await svc.stop(); process.exit(0) })
  await new Promise(() => {})
}
