// plane_schemas.ts — the ONE shared schema module (single source of truth) for the agent
// liveness/status planes: `agent.beat.v1` (tiny, frequent, ephemeral/lossy liveness) and
// `agent.status.v1` (rich, slower, on-change). Per the convergence design
// `ops-specs/specs/2026-06-16-unified-agent-bus-runtime-convergence-design` §3 (frozen beat
// core fields) + §5.1 (SPLIT planes, one owner/transport).
//
// BOUNDARY (locked with dev-pm, thread `beat-schema-boundary`):
//   - dev-pm's `wt-heartbeat` lane owns the agent-side BEAT EMIT (agent_beat.ts + Stop-hook +
//     wedge detector + Layer-2). It IMPORTS the beat field defs from HERE (no duplication).
//   - this lane (`wt-a2a-core-pm`) owns the CORE side: this schema module + the core StatusPlane
//     relay/fan-out + `agent.status.v1` emit, on the core's CONTROL traffic-class connection.
//   - SUBJECTS reuse the existing topic allowlist (so NO a2a-channel.ts change — 2C-safe both
//     lanes): beat → `alloyium.a2a.topic.agent-beat`, status → `alloyium.a2a.topic.agent-status`.
//   - Transport is EPHEMERAL core NATS (NOT JetStream): a replayed durable beat would reset the
//     missed-beat clock and mask a wedge (§5.1). Consumers keep newest-per-agent client-side.
//   - The beat may be ed25519-SIGNED for attribution (the detector decides on the RAW agent-signed
//     beat — §3/§8 #23: the core relays UNCHANGED, never re-signs/synthesizes). `alg`/`sig` are
//     optional here so signed and unsigned (MVP) beats both validate.
import { hostname } from 'node:os'

export const BEAT_SCHEMA = 'agent.beat.v1' as const
export const STATUS_SCHEMA = 'agent.status.v1' as const
// Topic TOKENS (valid `[a-z0-9-]{1,64}` topic names → ride the existing topic.<token> allowlist).
export const BEAT_TOPIC = 'agent-beat' as const
export const STATUS_TOPIC = 'agent-status' as const

// Subject builders. `prefix` mirrors A2AChannel's prefix (default 'alloyium.a2a.'; a test prefix
// substitutes cleanly) so the planes stay inside the audited alloyium.a2a.> namespace.
export const beatSubject = (prefix = 'alloyium.a2a.'): string => `${prefix}topic.${BEAT_TOPIC}`
export const statusSubject = (prefix = 'alloyium.a2a.'): string => `${prefix}topic.${STATUS_TOPIC}`

// Structured driver state (id1058 §3 — NOT text-matched). `loop` = the standing /loop driver;
// `goal` = a bounded auto-clearing drive; a service/relay (the core itself) uses `service`.
export type DriverMode = 'loop' | 'goal' | 'service'
// Lifecycle state (id1058 §3).
export type AgentState = 'booting' | 'idle' | 'in_task' | 'paused' | 'draining'

// ── agent.beat.v1 — the FROZEN liveness core (id1058 §3) ─────────────────────────────────────
// Tiny + cheap by construction (rides the loop iteration the agent runs anyway). The detector
// keys liveness on `loop_seq` MONOTONICITY (never beat-count, so a duplicate redelivery can't mask
// a miss) and on CORE-RECEIVE time (never the agent-stamped `ts`, so a skewed clock can't forge
// freshness). `last_progress_ts` advances ONLY on real task progress (not idle spins) so a
// "standing-by" stale-goal spin is detectable even while `loop_seq` advances.
export type AgentBeat = {
  v: 1
  schema: typeof BEAT_SCHEMA
  agent_id: string
  host: string
  boot_id: string                 // stable per process boot → ignore older-boot beats on restart
  session_id: string              // stable per CLI session → dedup + ignore older-session
  loop_seq: number                // monotonic; advances each /loop iteration
  ts: string                      // agent-stamped emit time (ISO) — advisory only; detector uses receive-time
  turn_start_ts?: string          // set at iteration START → with the sub-beat catches a MID-turn hang
  last_turn_ts?: string           // updated at iteration END
  last_progress_ts?: string       // advances ONLY on real task progress (not idle spins)
  driver_mode: DriverMode
  stop_hook_active: boolean
  goal_id?: string
  goal_epoch?: number
  state: AgentState
  inbox_depth: number
  task_ids: string[]
  // OPTIONAL attribution signature. CANONICAL-FORM CONTRACT (folds review P1): when a beat is
  // SIGNED, the signed bytes ARE the exact bytes the emitter published — so the core MUST relay
  // those RAW bytes UNCHANGED (StatusPlane.relayBeatBytes), NEVER parse→re-stringify (which can
  // reorder/reformat keys and break the agent's ed25519 sig → the detector would drop a legit beat
  // as a forgery). The object-based relayBeat() re-serializes and is for UNSIGNED beats / tests only.
  alg?: 'ed25519' | 'hmac'
  sig?: string
}

// ── agent.status.v1 — rich, slower, on-change (#18) ──────────────────────────────────────────
// Embeds the beat core (for correlation by agent_id/boot_id/session_id) + the rich status fields.
// Payload is BOUNDED (oversize fields are the emitter's responsibility to cap or claim-check).
export type AgentStatus = {
  v: 1
  schema: typeof STATUS_SCHEMA
  agent_id: string
  host: string
  boot_id: string
  session_id: string
  ts: string
  driver_mode: DriverMode
  state: AgentState
  // rich status (#18) — all optional / on-change:
  phase?: string                  // e.g. 'SLICE 2C', 'analysis'
  progress?: string               // free-form or 'n/m'
  current_action?: string
  blocker?: string
  last_deliverable?: string       // e.g. 'wt-a2a-core-pm @ c49cfc1'
  task_ids?: string[]
  attrs?: Record<string, string>  // extensible, string-valued (bounded)
  alg?: 'ed25519' | 'hmac'
  sig?: string
}

const ISO = (now: () => number): string => new Date(now()).toISOString()

// Build a beat: caller supplies the live fields; this fills schema/v/agent_id/ts + safe defaults.
// `now` is injectable for deterministic tests.
export function buildBeat(agentId: string, f: Partial<AgentBeat> = {}, now: () => number = Date.now): AgentBeat {
  return {
    // defaults first…
    host: hostname(), boot_id: '', session_id: '', loop_seq: 0,
    driver_mode: 'loop', stop_hook_active: false, state: 'idle', inbox_depth: 0, task_ids: [],
    // …caller overrides…
    ...f,
    // …then the authoritative envelope fields (a caller can never spoof these).
    v: 1, schema: BEAT_SCHEMA, agent_id: agentId, ts: ISO(now),
  }
}

// Build a status event: same authoritative-last discipline.
export function buildStatus(agentId: string, f: Partial<AgentStatus> = {}, now: () => number = Date.now): AgentStatus {
  return {
    host: hostname(), boot_id: '', session_id: '', driver_mode: 'loop', state: 'idle',
    ...f,
    v: 1, schema: STATUS_SCHEMA, agent_id: agentId, ts: ISO(now),
  }
}

const TOKEN_RE = /^[a-z0-9-]{1,64}$/
const isStr = (x: unknown): x is string => typeof x === 'string'
const isIsoTs = (x: unknown): boolean => isStr(x) && Number.isFinite(Date.parse(x))
const DRIVERS = new Set<DriverMode>(['loop', 'goal', 'service'])
const STATES = new Set<AgentState>(['booting', 'idle', 'in_task', 'paused', 'draining'])

// Structural validity of an inbound beat (for the core relay + dev-pm's detector). Pure/testable.
// Fail-closed: anything off-shape is rejected (never relayed/acted on).
export function isValidBeat(x: any): x is AgentBeat {
  if (!x || typeof x !== 'object') return false
  if (x.v !== 1 || x.schema !== BEAT_SCHEMA) return false
  if (!isStr(x.agent_id) || !TOKEN_RE.test(x.agent_id)) return false // string AND bounded token (no coercion)
  if (!isStr(x.host) || !isStr(x.boot_id) || !isStr(x.session_id)) return false
  if (x.host.length > 256 || x.boot_id.length > 256 || x.session_id.length > 256) return false // size bounds (defense-in-depth on the ephemeral path)
  if (!Number.isFinite(x.loop_seq)) return false
  if (!isIsoTs(x.ts)) return false
  if (!DRIVERS.has(x.driver_mode) || !STATES.has(x.state)) return false
  if (typeof x.stop_hook_active !== 'boolean') return false
  if (!Number.isFinite(x.inbox_depth)) return false
  if (!Array.isArray(x.task_ids) || x.task_ids.length > 256 || !x.task_ids.every(isStr)) return false
  for (const k of ['turn_start_ts', 'last_turn_ts', 'last_progress_ts'] as const) if (x[k] != null && !isIsoTs(x[k])) return false
  if (x.goal_id != null && !isStr(x.goal_id)) return false
  if (x.goal_epoch != null && !Number.isFinite(x.goal_epoch)) return false
  if (x.alg != null && x.alg !== 'ed25519' && x.alg !== 'hmac') return false
  if (x.sig != null && !isStr(x.sig)) return false
  return true
}

// Structural validity of an inbound status event.
export function isValidStatus(x: any): x is AgentStatus {
  if (!x || typeof x !== 'object') return false
  if (x.v !== 1 || x.schema !== STATUS_SCHEMA) return false
  if (!isStr(x.agent_id) || !TOKEN_RE.test(x.agent_id)) return false // string AND bounded token (no coercion)
  if (!isStr(x.host) || !isStr(x.boot_id) || !isStr(x.session_id)) return false
  if (x.host.length > 256 || x.boot_id.length > 256 || x.session_id.length > 256) return false
  if (!isIsoTs(x.ts)) return false
  if (!DRIVERS.has(x.driver_mode) || !STATES.has(x.state)) return false
  for (const k of ['phase', 'progress', 'current_action', 'blocker', 'last_deliverable'] as const) if (x[k] != null && !isStr(x[k])) return false
  if (x.task_ids != null && (!Array.isArray(x.task_ids) || x.task_ids.length > 256 || !x.task_ids.every(isStr))) return false
  if (x.attrs != null) {
    if (typeof x.attrs !== 'object' || Array.isArray(x.attrs)) return false
    const ks = Object.keys(x.attrs); if (ks.length > 64) return false // bounded attr count
    for (const v of Object.values(x.attrs)) if (!isStr(v)) return false
  }
  if (x.alg != null && x.alg !== 'ed25519' && x.alg !== 'hmac') return false
  if (x.sig != null && !isStr(x.sig)) return false
  return true
}
