#!/usr/bin/env bun
// demo_agent.ts — a keyless, deterministic A2A fabric peer.
//
// ONE role-parameterized agent that brings the Agent-Fabric diagram to life on a
// plain `docker compose up`: a Core agent dispatches direction to Project-Manager
// agents, each PM splits the work across its Worker agents, the workers report
// completion back up, and a worker occasionally "learns" a skill that broadcasts
// on the skills plane. No LLM, no API keys, no login — pure scripted peers built
// on the real A2AChannel (signed ed25519 envelopes, presence, topic planes).
//
// Role is chosen by env DEMO_ROLE (core | pm | worker). The rest of the roster is
// described by a few env knobs so the same image serves every node:
//   A2A_AGENT_ID   this peer's id (also drives the signing-key path + presence)
//   DEMO_ROLE      core | pm | worker
//   DEMO_PLANE     this peer's product plane (design | dev | ops | …)   (pm/worker)
//   DEMO_PEERS     core: the PM ids to drive (comma list)
//   DEMO_WORKERS   pm: the worker ids it owns (comma list)
//   DEMO_PM        worker: the PM id it reports to
//   DEMO_CORE      pm: the core id it reports to (default 'core')
//   DEMO_TOPICS    topic planes to JOIN for membership (comma list)
//   DEMO_TICK_MS   core: dispatch cadence (default 25000)
//   DEMO_WORK_MS   worker: simulated work time before completion (default 3000)
//   DEMO_BEAT_MS   liveness beat + status cadence (default 15000)
import { randomUUID } from 'node:crypto'
import { A2AChannel } from './a2a-channel.ts'
import { buildBeat, buildStatus, BEAT_TOPIC, STATUS_TOPIC, type AgentState } from './plane_schemas.ts'

const env = process.env
const AGENT_ID = env.A2A_AGENT_ID ?? ''
const ROLE = (env.DEMO_ROLE ?? 'worker') as 'core' | 'pm' | 'worker'
const PLANE = env.DEMO_PLANE ?? ''
const PEERS = splitList(env.DEMO_PEERS)
const WORKERS = splitList(env.DEMO_WORKERS)
const PM = env.DEMO_PM ?? ''
const CORE = env.DEMO_CORE ?? 'core'
const TOPICS = splitList(env.DEMO_TOPICS)
const TICK_MS = num(env.DEMO_TICK_MS, 25_000)
const WORK_MS = num(env.DEMO_WORK_MS, 3_000)
const BEAT_MS = num(env.DEMO_BEAT_MS, 15_000)

// Deterministic directive rotation the Core cycles through — no randomness so the
// fabric is reproducible run to run.
const DIRECTIVES = [
  'Ship the onboarding flow',
  'Harden the public API gateway',
  'Cut release 1.4',
  'Reduce p99 latency',
  'Roll out the audit log',
]

function splitList(v: string | undefined): string[] {
  return (v ?? '').split(',').map((s) => s.trim()).filter(Boolean)
}
function num(v: string | undefined, d: number): number {
  const n = Number(v)
  return Number.isFinite(n) && n > 0 ? n : d
}
function log(msg: string): void {
  console.error(`${new Date().toISOString()} info [demo-agent ${AGENT_ID}] ${msg}`)
}

// The plane a PM is responsible for, derived from its id ('pm-design' -> 'design').
function planeOfPm(id: string): string {
  return id.startsWith('pm-') ? id.slice(3) : id
}

let a2a: A2AChannel
let loopSeq = 0
let state: AgentState = 'booting'
let phase = 'booting'
const activeTasks = new Set<string>()
const BOOT_ID = randomUUID()
const SESSION_ID = AGENT_ID || randomUUID()
const DRIVER = ROLE === 'core' ? 'service' : 'loop'

// PM bookkeeping: map each outbound subtask-request id -> the parent task it belongs
// to, plus per-parent reply tracking so the PM reports up only once all workers reply.
type Parent = { corr: string; replyTo: string; plane: string; directive: string; total: number; done: number }
const parents = new Map<string, Parent>()      // parentKey -> Parent
const subToParent = new Map<string, string>()   // subtask-request id -> parentKey
let learnedSkill = false

// ── channel helpers ─────────────────────────────────────────────────────────

// callTool returns an MCP-shaped result; unwrap the JSON text payload.
async function call(name: string, args: Record<string, unknown> = {}): Promise<any> {
  const res = await a2a.callTool(name, args)
  try { return JSON.parse(res?.content?.[0]?.text ?? '{}') } catch { return {} }
}
// Send a direct message or topic broadcast. `body` is JSON-encoded for the wire.
function send(to: string, body: unknown, extra: Record<string, unknown> = {}): Promise<any> {
  return call('a2a_send', { to, body: typeof body === 'string' ? body : JSON.stringify(body), ...extra })
}
const announce = (plane: string, body: unknown) => send(`topic:${plane}`, body)

function setPhase(next: string, s: AgentState): void {
  phase = next
  state = s
}

// ── liveness: presence is claimed by the channel itself; we add a beat + status
// on the agent-beat / agent-status planes so the peer renders rich + live. ───────
function emitBeatStatus(): void {
  loopSeq += 1
  const beat = buildBeat(AGENT_ID, {
    boot_id: BOOT_ID, session_id: SESSION_ID, loop_seq: loopSeq,
    driver_mode: DRIVER, state, inbox_depth: 0, task_ids: [...activeTasks],
  })
  void send(`topic:${BEAT_TOPIC}`, beat)
  const status = buildStatus(AGENT_ID, {
    boot_id: BOOT_ID, session_id: SESSION_ID, driver_mode: DRIVER, state,
    phase, current_action: phase, task_ids: [...activeTasks],
    attrs: { role: ROLE, ...(PLANE ? { plane: PLANE } : {}) },
  })
  void send(`topic:${STATUS_TOPIC}`, status)
}

// ── CORE — periodically announces direction + dispatches a task to each PM ──────
let round = 0
async function coreTick(): Promise<void> {
  const directive = DIRECTIVES[round % DIRECTIVES.length]
  round += 1
  setPhase(`dispatching round ${round}: ${directive}`, 'in_task')
  log(phase)
  for (const pm of PEERS) {
    const plane = planeOfPm(pm)
    // Visible direction on the PM's plane…
    void announce(plane, { schema: 'fabric.directive.v1', round, from: AGENT_ID, plane, directive })
    // …and the actual task as a direct request that drives the workflow.
    const task = { schema: 'fabric.task.v1', round, plane, directive }
    const r = await send(pm, task, { type: 'request', thread: `round-${round}` })
    if (r.ok) { activeTasks.add(r.id); log(`dispatched '${directive}' -> ${pm} (${r.id})`) }
    else log(`dispatch to ${pm} failed: ${r.error ?? 'unknown'}`)
  }
  setPhase(`awaiting reports (round ${round})`, 'idle')
}

// ── PM — splits an inbound task across its workers, tracks replies, reports up ──
async function pmOnTask(content: any, attrs: Record<string, string>): Promise<void> {
  const plane = PLANE || planeOfPm(AGENT_ID)
  const directive = String(content?.directive ?? 'work')
  const parentKey = attrs.id // the core request id — what we'll reply corr= to
  const parent: Parent = { corr: attrs.id, replyTo: attrs.from || CORE, plane, directive, total: WORKERS.length, done: 0 }
  parents.set(parentKey, parent)
  setPhase(`splitting '${directive}' across ${WORKERS.length} workers`, 'in_task')
  log(phase)
  void announce(plane, { schema: 'fabric.plan.v1', from: AGENT_ID, plane, directive, parts: WORKERS.length })
  for (let i = 0; i < WORKERS.length; i++) {
    const worker = WORKERS[i]
    const assignment = { schema: 'fabric.assignment.v1', plane, directive, part: i + 1, of: WORKERS.length }
    const r = await send(worker, assignment, { type: 'request', thread: attrs.thread })
    if (r.ok) { subToParent.set(r.id, parentKey); log(`assigned part ${i + 1} -> ${worker} (${r.id})`) }
    else { parent.total -= 1; log(`assign to ${worker} failed: ${r.error ?? 'unknown'}`) }
  }
  if (parent.total === 0) await pmReportUp(parentKey) // nothing assigned — close it out
}

async function pmOnWorkerReply(attrs: Record<string, string>): Promise<void> {
  const parentKey = subToParent.get(attrs.corr ?? '')
  if (!parentKey) return
  subToParent.delete(attrs.corr!)
  const parent = parents.get(parentKey)
  if (!parent) return
  parent.done += 1
  log(`worker reply ${parent.done}/${parent.total} for '${parent.directive}'`)
  if (parent.done >= parent.total) await pmReportUp(parentKey)
}

async function pmReportUp(parentKey: string): Promise<void> {
  const parent = parents.get(parentKey)
  if (!parent) return
  parents.delete(parentKey)
  setPhase(`reporting '${parent.directive}' complete`, 'idle')
  void announce(parent.plane, { schema: 'fabric.report.v1', from: AGENT_ID, plane: parent.plane, directive: parent.directive, status: 'complete' })
  await send(parent.replyTo, { schema: 'fabric.report.v1', plane: parent.plane, directive: parent.directive, parts: parent.total, status: 'complete' }, { type: 'reply', corr: parent.corr })
  log(`reported '${parent.directive}' up to ${parent.replyTo}`)
}

// ── WORKER — executes an assignment, replies progress + completion, learns once ─
async function workerOnAssignment(content: any, attrs: Record<string, string>): Promise<void> {
  const plane = PLANE || content?.plane || ''
  const part = `part ${content?.part ?? '?'}/${content?.of ?? '?'}`
  const reqId = attrs.id
  const owner = attrs.from || PM
  activeTasks.add(reqId)
  setPhase(`building ${part} of '${content?.directive ?? 'work'}'`, 'in_task')
  log(phase)
  // Progress chatter (visible direct message), then completion as the reply.
  void send(owner, { schema: 'fabric.progress.v1', plane, part: content?.part, progress: '50%' })
  await Bun.sleep(WORK_MS)
  await send(owner, { schema: 'fabric.subresult.v1', plane, part: content?.part, result: 'done' }, { type: 'reply', corr: reqId })
  log(`completed ${part} -> ${owner}`)
  activeTasks.delete(reqId)
  setPhase('idle', 'idle')
  await maybeLearnSkill(plane)
}

// Once in its lifetime a worker "learns" a reusable skill and broadcasts it on the
// skills plane via the real channel API (also writes the shared skill registry).
async function maybeLearnSkill(plane: string): Promise<void> {
  if (learnedSkill) return
  learnedSkill = true
  const name = `demo-${plane || 'general'}-pattern`
  const body = [
    '---', 'scope: global', `tags: [${plane || 'general'}, demo]`, '---',
    `# ${name}`, '',
    `Reusable ${plane || 'general'} delivery pattern, distilled while completing a fabric subtask.`, '',
    '1. Pull the assignment from the owning project-manager.',
    `2. Apply the ${plane || 'general'} checklist and produce the slice.`,
    '3. Report completion upstream so the PM can roll up the task.', '',
  ].join('\n')
  const r = await a2a.broadcastSkillCreated({ name, slug: name, source: AGENT_ID, body, tags: [plane || 'general', 'demo'] })
  log(`skill ${r.ok ? 'broadcast' : 'broadcast-failed'} name=${name} scope=${r.scope ?? '-'}${r.error ? ` error=${r.error}` : ''}`)
}

// ── inbound routing ────────────────────────────────────────────────────────────
async function onInbound(content: string, attrs: Record<string, string>): Promise<void> {
  let msg: any
  try { msg = JSON.parse(content) } catch { msg = { text: content } }
  const type = attrs.type ?? 'msg'
  try {
    if (ROLE === 'pm') {
      if (type === 'request' && msg?.schema === 'fabric.task.v1') return void await pmOnTask(msg, attrs)
      if (type === 'reply') return void await pmOnWorkerReply(attrs)
    } else if (ROLE === 'worker') {
      if (type === 'request' && msg?.schema === 'fabric.assignment.v1') return void await workerOnAssignment(msg, attrs)
    } else if (ROLE === 'core') {
      if (type === 'reply' && msg?.schema === 'fabric.report.v1') {
        activeTasks.delete(attrs.corr ?? '')
        log(`report from ${attrs.from}: '${msg?.directive}' ${msg?.status}`)
      }
    }
  } catch (e) {
    log(`inbound handler error: ${e instanceof Error ? e.message : String(e)}`)
  }
}

// ── boot ────────────────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  if (!AGENT_ID) { console.error('demo_agent: A2A_AGENT_ID is required'); process.exit(2) }
  a2a = new A2AChannel(onInbound, { enabled: true, agentId: AGENT_ID })
  await a2a.start()
  if (!a2a.isStarted()) {
    console.error(`demo_agent: could not join the bus as '${AGENT_ID}' (duplicate id? bus down?). Exiting.`)
    process.exit(1)
  }
  // Join our topic planes for membership (idempotent; also self-heals the seed race).
  const joins = [...TOPICS, 'skills-global'] // every agent is on the skills broadcast plane — a learned skill reaches the whole fleet
  for (const t of joins) await call('a2a_join_topic', { topic: t })
  log(`joined as ${ROLE}${PLANE ? ` plane=${PLANE}` : ''} topics=[${joins.join(',')}]`)

  setPhase('online', 'idle')
  emitBeatStatus()
  const beatTimer = setInterval(emitBeatStatus, BEAT_MS)
  let coreTimer: ReturnType<typeof setInterval> | undefined
  if (ROLE === 'core') {
    await coreTick() // light up the fabric immediately, then on cadence
    coreTimer = setInterval(() => { void coreTick() }, TICK_MS)
  }

  const shutdown = async () => {
    clearInterval(beatTimer)
    if (coreTimer) clearInterval(coreTimer)
    await a2a.stop().catch(() => {})
    process.exit(0)
  }
  for (const sig of ['SIGTERM', 'SIGINT'] as const) process.on(sig, shutdown)
}

if (import.meta.main) await main()
