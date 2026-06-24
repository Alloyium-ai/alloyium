// Driver-aware liveness classification (#32 residual-a).
// Scaffolding (beat()/stateFor()/presence helpers + loop-path cases) authored by GPT-5.5 (codex-gw)
// under Model-B; the registry/override/offline/expected-silent cases and the corrected stale-SERVICE
// assertion (a stale-recv service stays service-alive — NOT wedged:loop-dead) authored by dev-pm.
// The corrected assertion is the whole point of the fix: a service is never loop-dead-by-missing-beats
// (genuine service-hang detection is deferred to Phase-C+; see RCA ops-specs/rca/2026-06-17-driver-aware-liveness).
import { describe, expect, test } from 'bun:test'
import {
  classifyAgent,
  isCensusExcluded,
  isStaleOffline,
  loadDetectorConfig,
  planeFreshMsForAgent,
  planeHealthyForAgents,
  resolveLivenessMode,
  type DetectorAgentState,
  type PresenceRecord,
} from '../wedge_detector.ts'

const now = Date.parse('2026-06-17T12:00:00.000Z')
const iso = (ms: number) => new Date(ms).toISOString()
const config = loadDetectorConfig({ WEDGE_MIN_CADENCE_MS: '0' })

// Beat far enough in the past that missed >= missedBeats (Path-2 loop-dead shape for LOOP agents).
const staleRecvMs = now - config.beatCadenceMs * config.missedBeats - 1_000

type LastBeat = NonNullable<DetectorAgentState['last_beat']>

const presence: PresenceRecord = {
  token: 'presence-token',
  host: 'host-a',
  started_at: iso(now - config.bootGraceMs - 1_000),
  last_seen: iso(now), // fresh presence — passes the offline gate
}

function beat(overrides: Partial<LastBeat> = {}): LastBeat {
  return {
    schema: 'agent.beat.v1',
    agent_id: 'driver-aware-agent',
    host: 'host-a',
    boot_id: 'boot-a',
    session_id: 'session-a',
    loop_seq: 12,
    turn_start_ts: iso(now - 1_000),
    last_turn_ts: iso(now - 500),
    last_progress_ts: iso(now - 500),
    driver_mode: 'loop',
    stop_hook_active: false,
    state: 'idle',
    inbox_depth: 0,
    task_ids: [],
    ...overrides,
  }
}

function stateFor(lastBeat: LastBeat | undefined, overrides: Partial<DetectorAgentState> = {}): DetectorAgentState {
  const recvMs = now - 1_000 // fresh beat receive by default
  return {
    agent_id: lastBeat?.agent_id ?? 'driver-aware-agent',
    boot_id: lastBeat?.boot_id ?? 'boot-a',
    session_id: lastBeat?.session_id ?? 'session-a',
    presence_started_at: presence.started_at,
    first_presence_seen_ms: now - config.bootGraceMs - 1_000, // past boot-grace
    identity_first_recv_ms: now - config.bootGraceMs - 1_000,
    last_recv_ms: lastBeat ? recvMs : undefined,
    last_loop_seq: lastBeat?.loop_seq,
    last_loop_seq_advance_ms: lastBeat ? recvMs : undefined,
    stale_progress_cadences: 0,
    missed_count: 0,
    last_beat: lastBeat,
    updated_ms: recvMs,
    ...overrides,
  }
}

interface ClassifyOpts {
  livenessClass?: 'loop' | 'service'
  presence?: PresenceRecord | null
  expectedSilent?: boolean
  planeHealthy?: boolean
}

function classify(state: DetectorAgentState, opts: ClassifyOpts = {}) {
  return classifyAgent({
    now,
    state,
    presence: 'presence' in opts ? (opts.presence as PresenceRecord | null) : presence,
    expectedSilent: opts.expectedSilent ?? false,
    planeHealthy: opts.planeHealthy ?? true,
    config,
    livenessClass: opts.livenessClass,
  })
}

describe('driver-aware liveness: service/gateway agents are never false loop-dead', () => {
  test('T1 registry=service + never-beaten (Path-1) => service-alive', () => {
    const r = classify(stateFor(undefined, { last_recv_ms: undefined, last_beat: undefined }), { livenessClass: 'service' })
    expect(r.class).toBe('service-alive')
    expect(r.alert).toBe(false)
    expect(r.reason).toBe('service_class_registry')
  })

  test('T2 registry=service + stale receive (Path-2) => service-alive (NOT loop-dead)', () => {
    const r = classify(stateFor(beat({ driver_mode: 'service' }), { last_recv_ms: staleRecvMs, updated_ms: staleRecvMs }), { livenessClass: 'service' })
    expect(r.class).toBe('service-alive')
    expect(r.alert).toBe(false)
  })

  test('T3 default + beat.driver_mode=service + stale receive => service-alive (a2a-core case)', () => {
    // CORRECTED from codex draft (which asserted wedged:loop-dead): a service whose own beat cadence
    // exceeds the loop window must NOT be loop-dead — that was the exact false-trip being fixed.
    const r = classify(stateFor(beat({ driver_mode: 'service' }), { last_recv_ms: staleRecvMs, updated_ms: staleRecvMs }))
    expect(r.class).toBe('service-alive')
    expect(r.reason).toBe('service_inferred_from_beat_driver_mode')
  })

  test('T4 default + never-beaten + no beat (presence-only gateway) => service-alive', () => {
    const r = classify(stateFor(undefined, { last_recv_ms: undefined, last_beat: undefined }))
    expect(r.class).toBe('service-alive')
    expect(r.reason).toBe('service_inferred_presence_only_no_beat')
  })

  test('T5 beat.driver_mode=service in a long-turn shape => service-alive (bypasses long-turn)', () => {
    const r = classify(stateFor(beat({
      driver_mode: 'service', state: 'in_task', task_ids: ['svc'],
      turn_start_ts: iso(now - config.longTurnMs - 1_000),
      last_turn_ts: iso(now - config.longTurnMs - 1_000),
    })))
    expect(r.class).toBe('service-alive')
  })

  test('T6 beat.driver_mode=service in a stale-progress shape => service-alive (bypasses stale-spin)', () => {
    const r = classify(stateFor(beat({
      driver_mode: 'service', stop_hook_active: true, state: 'in_task', task_ids: ['svc'],
      last_progress_ts: iso(now - config.progressStaleMs - 1_000),
    }), { stale_progress_cadences: config.staleProgressCadences }))
    expect(r.class).toBe('service-alive')
  })
})

describe('driver-aware liveness: /loop agents keep full loop-tied detection', () => {
  test('T7 default + beat.driver_mode=loop + stale receive => wedged:loop-dead', () => {
    const r = classify(stateFor(beat({ driver_mode: 'loop' }), { last_recv_ms: staleRecvMs, updated_ms: staleRecvMs }))
    expect(r.class).toBe('wedged:loop-dead')
    expect(r.alert).toBe(true)
  })

  test('T8 registry=loop OVERRIDES beat.driver_mode=service (stale) => wedged:loop-dead', () => {
    const r = classify(stateFor(beat({ driver_mode: 'service' }), { last_recv_ms: staleRecvMs, updated_ms: staleRecvMs }), { livenessClass: 'loop' })
    expect(r.class).toBe('wedged:loop-dead')
    expect(r.alert).toBe(true)
  })

  test('T9 fresh idle loop beat => idle-ok', () => {
    const r = classify(stateFor(beat({ driver_mode: 'loop', state: 'idle', task_ids: [] })))
    expect(r.class).toBe('idle-ok')
  })

  test('T10 fresh active loop beat => healthy', () => {
    const r = classify(stateFor(beat({ driver_mode: 'loop', state: 'in_task', task_ids: ['task-a'] })))
    expect(r.class).toBe('healthy')
  })

  test('T11 loop beat with an old current turn => wedged:long-turn', () => {
    const r = classify(stateFor(beat({
      driver_mode: 'loop', state: 'in_task', task_ids: ['task-a'],
      turn_start_ts: iso(now - config.longTurnMs - 1_000),
      last_turn_ts: iso(now - config.longTurnMs - 1_000),
    })))
    expect(r.class).toBe('wedged:long-turn')
  })

  test('T12 goal beat with stale progress => wedged:stale-goal-spin', () => {
    const r = classify(stateFor(beat({
      driver_mode: 'goal', state: 'in_task', task_ids: ['task-a'],
      last_progress_ts: iso(now - config.progressStaleMs - 1_000),
    }), { stale_progress_cadences: config.staleProgressCadences }))
    expect(r.class).toBe('wedged:stale-goal-spin')
  })
})

describe('driver-aware liveness: gate precedence is preserved', () => {
  test('T13 registry=service but presence absent => offline (offline gate wins)', () => {
    const r = classify(stateFor(beat({ driver_mode: 'service' })), { livenessClass: 'service', presence: null })
    expect(r.class).toBe('offline')
  })

  test('T13b expected-silent + presence absent => offline (offline gate precedes the expected-silent gate)', () => {
    // Proves the hoist is safe for presence-absent ids: classifyAgent returns offline BEFORE expectedSilent
    // is consulted, so a map-miss default (expectedSilent ?? false) can never change a presence-absent class.
    const r = classify(stateFor(beat()), { presence: null, expectedSilent: true })
    expect(r.class).toBe('offline')
  })

  test('T14 expected-silent + registry=service => suppressed:expected-silent (wins over service)', () => {
    const r = classify(stateFor(beat({ driver_mode: 'service' })), { livenessClass: 'service', expectedSilent: true })
    expect(r.class).toBe('suppressed:expected-silent')
  })

  test('T15 never-beaten service-shape + presence absent => offline (offline gate not bypassable)', () => {
    // The offline gate is the FIRST statement in classifyAgent — a never-beaten/service-shaped input
    // cannot reach service-alive when presence is gone. (GPT-5.5 gate P1.3.)
    const r = classify(stateFor(undefined, { last_recv_ms: undefined, last_beat: undefined }), { presence: null })
    expect(r.class).toBe('offline')
  })

  test('T16 registry=service + beat.driver_mode=loop (mismatch) => service-alive, registry wins (+ soft warn)', () => {
    // The opposite mismatch direction to T8: registry overrides the beat hint, and the cross-check
    // warning is side-effect-only (never changes the class). (GPT-5.5 gate P2.1.)
    const r = classify(stateFor(beat({ driver_mode: 'loop' }), { last_recv_ms: staleRecvMs, updated_ms: staleRecvMs }), { livenessClass: 'service' })
    expect(r.class).toBe('service-alive')
    expect(r.reason).toBe('service_class_registry')
  })
})

describe('planeHealthyForAgents: services excluded from the plane gate (#32 residual-a — guards the fold)', () => {
  const cfg = loadDetectorConfig({}) // defaults: globalGateMinAgents=3, globalMinFreshRatio=0.5
  const freshMs = now - 1_000
  const staleMs = now - cfg.planeFreshMs - 1_000
  const loopAgent = (lastRecvMs?: number) => ({ livenessClass: 'loop' as const, lastRecvMs })
  const serviceAgent = (lastRecvMs?: number) => ({ livenessClass: 'service' as const, lastRecvMs })

  test('non-beating services do NOT drag the ratio (REGRESSION: fails if services were counted)', () => {
    // fix: loop-only 3/3 fresh => healthy. bug (count 4 services): 3/7 = 0.43 < 0.5 => false.
    const agents = [loopAgent(freshMs), loopAgent(freshMs), loopAgent(freshMs), serviceAgent(), serviceAgent(), serviceAgent(), serviceAgent()]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(true)
  })

  test('a genuine plane-wide outage is NOT masked (no loop agent fresh => unhealthy)', () => {
    const agents = [loopAgent(staleMs), loopAgent(staleMs), loopAgent(staleMs), serviceAgent(freshMs), serviceAgent(freshMs)]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(false)
  })

  test('accepted P2 trade-off: a lone wedged /loop among beating services => suppressed (safe-side, never false-fire)', () => {
    // old all-active: 3 fresh svc / 4 = 0.75 => healthy (alert fired). new loop-only: 0/1 => unhealthy (suppressed).
    const agents = [loopAgent(staleMs), serviceAgent(freshMs), serviceAgent(freshMs), serviceAgent(freshMs)]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(false)
  })
})

describe('planeHealthyForAgents: expected-silent + idle agents handled (#32 observe-E2E plane-health false-trip)', () => {
  const cfg = loadDetectorConfig({}) // defaults: planeFreshMs=30min, globalGateMinAgents=3, globalMinFreshRatio=0.5
  const freshMs = now - 1_000
  const staleMs = now - cfg.planeFreshMs - 1_000 // stale vs the GLOBAL plane window
  const loopAgent = (lastRecvMs?: number) => ({ livenessClass: 'loop' as const, lastRecvMs })
  // expected-silent /loop agents (rolling-relaunch trailers, e.g. aide-pm + deploy-pm): loop-class but
  // DELIBERATELY not beating during the relaunch window.
  const esLoopAgent = (lastRecvMs?: number) => ({ livenessClass: 'loop' as const, lastRecvMs, expectedSilent: true })

  test('FIX#1 expected-silent agents do NOT drag the ratio (REGRESSION: false-trip if counted)', () => {
    // Rolling-relaunch tail: 3 fresh /loop PMs + 4 deliberately-silent trailers.
    // BUG (count expected-silent): fresh 3 / loop 7 = 0.43 < 0.5 => false plane-unhealthy (masks Layer-2).
    // FIX#1 (exclude expected-silent): 3 / 3 = 1.0 >= 0.5 => HEALTHY.
    const agents = [
      loopAgent(freshMs), loopAgent(freshMs), loopAgent(freshMs),
      esLoopAgent(), esLoopAgent(), esLoopAgent(), esLoopAgent(),
    ]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(true)
  })

  test('FIX#1 realistic incident shape: idle NO-override /loop agent present, plane still HEALTHY', () => {
    // Live fleet during the #32 relaunch: 2 fresh /loop PMs + 2 expected-silent trailers (aide-pm,
    // deploy-pm) + 1 idle /loop agent with NO cadence-override (a2a-core-pm, ~34min idle). The idle
    // no-override agent reads STALE vs the global window (Fix#2 does NOT cover it) — but the plane is
    // healthy because Fix#1 removes the 2 expected-silent from the denominator and enough LIVE agents are
    // fresh. BUG (count expected-silent): fresh 2 / loop 5 = 0.4 < 0.5 => false unhealthy.
    // FIX#1: loop-pop = 3 (2 fresh + 1 idle-stale), fresh 2 => 2/3 = 0.67 >= 0.5 => HEALTHY.
    const agents = [loopAgent(freshMs), loopAgent(freshMs), esLoopAgent(), esLoopAgent(), loopAgent(staleMs)]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(true)
  })

  test('FIX#2 idle /loop agent WITH a cadence-override is fresh-for-its-cadence (not counted stale)', () => {
    // A 30-min /loop agent (operator-set wedge-cadence override) that beat 40 min ago: STALE vs the 30-min
    // global window, but FRESH within its own widened window (override clamp 30min * 2 = 60 min).
    const overrideMs = 1_800_000 // 30 min (within clamp [2min, 1h])
    const idleRecvMs = now - cfg.planeFreshMs - 600_000 // 40 min ago: > global 30 min, < 60 min window
    const overrideLoop = (lastRecvMs: number) => ({ livenessClass: 'loop' as const, lastRecvMs, cadenceOverrideMs: overrideMs })
    // BUG (single global window): fresh 1 / loop 3 = 0.33 < 0.5 => false unhealthy.
    // FIX#2 (per-agent window): all 3 fresh => 3/3 => HEALTHY.
    const agents = [loopAgent(freshMs), overrideLoop(idleRecvMs), overrideLoop(idleRecvMs)]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(true)
  })

  test('a genuine plane-wide outage is STILL caught despite the exclusions (no live loop fresh => unhealthy)', () => {
    // All real /loop agents silent; only expected-silent (excluded) + an idle-override agent that is ALSO
    // beyond its widened window. fresh=0 => ratio 0 => unhealthy — the fix does NOT mask the safety-net.
    const overrideMs = 1_800_000
    const wayStaleMs = now - cfg.maxCadenceMs * 2 - 1_000 // beyond even the clamped*2 (1h ceiling => 2h) window
    const agents = [
      loopAgent(staleMs), loopAgent(staleMs), loopAgent(staleMs),
      esLoopAgent(freshMs), esLoopAgent(freshMs),
      { livenessClass: 'loop' as const, lastRecvMs: wayStaleMs, cadenceOverrideMs: overrideMs },
    ]
    expect(planeHealthyForAgents(agents, now, cfg)).toBe(false)
  })

  test('FIX#2 helper planeFreshMsForAgent: widens with override (clamped, x2), global default otherwise', () => {
    expect(planeFreshMsForAgent(undefined, cfg)).toBe(cfg.planeFreshMs)
    expect(planeFreshMsForAgent(0, cfg)).toBe(cfg.planeFreshMs)
    expect(planeFreshMsForAgent(NaN, cfg)).toBe(cfg.planeFreshMs) // non-finite override → global window
    expect(planeFreshMsForAgent(-5, cfg)).toBe(cfg.planeFreshMs) // negative override → global window
    expect(planeFreshMsForAgent(1_800_000, cfg)).toBe(Math.max(cfg.planeFreshMs, 1_800_000 * 2)) // 60 min
    expect(planeFreshMsForAgent(9_999_999_999, cfg)).toBe(cfg.maxCadenceMs * 2) // clamped to 1h ceiling, then x2
    // Floor clamp GUARDED with a tight plane window (so minCadenceMs*2 > planeFreshMs): a sub-floor override
    // is clamped UP to minCadenceMs before x2 (without the floor this would be 1_000*2=2_000, not 240_000).
    const tightCfg = loadDetectorConfig({ WEDGE_PLANE_FRESH_MS: '60000' })
    expect(planeFreshMsForAgent(1_000, tightCfg)).toBe(tightCfg.minCadenceMs * 2)
  })
})

describe('isCensusExcluded: infra + transient ids dropped from census + plane (#32 Fix#3)', () => {
  const cfg = loadDetectorConfig({})
  test('transient prefixes (ztest-/fusion-req-/orgrev-) are excluded', () => {
    expect(isCensusExcluded('ztest-abc123', cfg)).toBe(true)
    expect(isCensusExcluded('fusion-req-42', cfg)).toBe(true)
    expect(isCensusExcluded('orgrev-run-9', cfg)).toBe(true)
  })
  test('infra ids (detector/supervisor/-beat sidecars) stay excluded (ignoreAgentRe preserved)', () => {
    expect(isCensusExcluded('wedge-detector', cfg)).toBe(true)
    expect(isCensusExcluded('layer2-supervisor', cfg)).toBe(true)
    expect(isCensusExcluded('agent-1-beat', cfg)).toBe(true)
  })
  test('real fleet agents are NOT excluded', () => {
    for (const id of ['agent-1', 'dev-pm', 'aide-pm', 'deploy-pm', 'a2a-core-pm', 'docs-pm']) {
      expect(isCensusExcluded(id, cfg)).toBe(false)
    }
  })
  test('the transient regex is operator-overridable', () => {
    const custom = loadDetectorConfig({ WEDGE_TRANSIENT_AGENT_RE: '^scratch-' })
    expect(isCensusExcluded('scratch-1', custom)).toBe(true)
    expect(isCensusExcluded('ztest-1', custom)).toBe(false) // default prefixes no longer apply
  })
})

describe('isStaleOffline: general census age-out of offline remnants (#32 Fix#3, prefix-agnostic)', () => {
  const cfg = loadDetectorConfig({}) // censusAgeOutMs default = 30 min
  const old = now - cfg.censusAgeOutMs - 1_000 // beyond the age-out window
  const recent = now - 60_000 // within the window

  test('presence-fresh agent is NEVER aged out (a wedged agent cannot be hidden)', () => {
    // presence present ⇒ kept even with an ancient last beat — only offline remnants age out
    expect(isStaleOffline(presence, stateFor(undefined, { last_recv_ms: old }), now, cfg)).toBe(false)
  })
  test('offline + recent last beat (within grace) is kept', () => {
    expect(isStaleOffline(null, stateFor(undefined, { last_recv_ms: recent }), now, cfg)).toBe(false)
  })
  test('offline + last beat older than censusAgeOutMs is aged out (the transient remnant)', () => {
    expect(isStaleOffline(null, stateFor(undefined, { last_recv_ms: old }), now, cfg)).toBe(true)
  })
  test('offline + never beat: ages out by a stale first-presence sighting, kept if recent', () => {
    expect(isStaleOffline(null, stateFor(undefined, { last_recv_ms: undefined, first_presence_seen_ms: old }), now, cfg)).toBe(true)
    expect(isStaleOffline(null, stateFor(undefined, { last_recv_ms: undefined, first_presence_seen_ms: recent }), now, cfg)).toBe(false)
  })
  test('offline + no tracked state at all is aged out', () => {
    expect(isStaleOffline(null, undefined, now, cfg)).toBe(true)
  })
})

describe('resolveLivenessMode: shared loop-vs-service resolver (classify + plane-health)', () => {
  test('registry is authoritative over the beat hint', () => {
    expect(resolveLivenessMode('service', 'loop', true)).toBe('service')
    expect(resolveLivenessMode('loop', 'service', true)).toBe('loop')
  })
  test('beat driver_mode informs the default when no registry key', () => {
    expect(resolveLivenessMode(undefined, 'service', true)).toBe('service')
    expect(resolveLivenessMode(undefined, 'loop', true)).toBe('loop')
    expect(resolveLivenessMode(undefined, 'goal', true)).toBe('loop') // goal is loop-tied for wedges
  })
  test('never-beaten presence-only defaults to service (fail-safe gateway)', () => {
    expect(resolveLivenessMode(undefined, undefined, false)).toBe('service')
  })
  test('beaten with unknown/absent driver_mode defaults to loop (safe for the /loop population)', () => {
    expect(resolveLivenessMode(undefined, undefined, true)).toBe('loop')
    expect(resolveLivenessMode(undefined, 'future-mode', true)).toBe('loop')
  })
})
