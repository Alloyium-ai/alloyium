#!/usr/bin/env bun

// A2A Fleet v2 orchestrator core.
//
// Dry-run planning is the default. Live execution is explicit and guarded:
// canaries launch a two-agent smoke slice, and full-fleet execution requires
// --execute --full so a launcher cannot accidentally spawn the whole fleet.
//
// ── launch / stop are pluggable, NOT hard-wired to tmux ──────────────────────
// Orchestration itself (validate → plan → join bus topics → launch → presence →
// dispatch jobs over the A2A bus → collect terminal replies) runs on the GENERIC
// A2AChannel bus primitive. The launch + stop BACKENDS are operator-selected by env,
// with no built-in dependency on tmux or any bundled shell script:
//   A2A_LAUNCHER_URL      — preferred: POST to the a2a-launcher HTTP control-plane
//                           (a2a_launcher.ts), the repo's generic launch primitive.
//   A2A_FLEET_LAUNCH_SCRIPT — optional: path to a custom launch script (e.g. a tmux/
//                           codex launcher); spawned with [agentId, kind, --<mode>,
//                           --worktree <repo[@ref]>]. Opt-in only.
//   A2A_FLEET_STOP_CMD    — optional: a stop command template (space-split); the
//                           session name `a2a-<agentId>` is appended. e.g.
//                           "tmux kill-session -t". Unset ⇒ stop is skipped.
// If no launch backend is configured the launch step fails soft per agent (it never
// blindly shells out). The dry-run PLAN still emits a descriptive launch command for
// backward compatibility, but execution never depends on it.
import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { canonicalizeCwd, registerCwd } from './codex_build_authz.ts'

export const FLEET_SPEC_SCHEMA = 'a2a.fleet.spec.v2'
export const FLEET_PLAN_SCHEMA = 'a2a.fleet.run_plan.v1'
export const CODEX_JOB_SCHEMA = 'codex.job.request.v1'
export const CLAUDE_ASSIGNMENT_SCHEMA = 'fleet.task.assigned.v1'

const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const DEFAULT_BUDGET_MAX_PRIMARY_USED_PERCENT = 99
const DEFAULT_TURN_TIMEOUT_MS = 28_800_000
const DEFAULT_CANARY_TIMEOUT_MS = 300_000
const DEFAULT_CANARY_GATEWAY_TURN_TIMEOUT_MS = 180_000
const DEFAULT_PRESENCE_TIMEOUT_MS = 120_000
const DEFAULT_LAUNCH_TIMEOUT_MS = 120_000
const DEFAULT_EXECUTION_TOOL_TIMEOUT_SEC = 60
const DEFAULT_CANARY_TOOL_TIMEOUT_SEC = 20
const DEFAULT_MAX_TOPIC_EVENTS = 200
const DEFAULT_CWD_ALLOW_TTL_S = 12 * 3600

export type FleetRuntime = 'codex' | 'claude-live'
export type FleetProvider = 'docker-codex' | 'tmux-codex' | 'tmux-claude-live'

export type FleetBase = {
  repo: string
  ref?: string | null
  cwd: string
}

export type FleetDefaults = {
  runtime: FleetRuntime
  provider?: FleetProvider
  model?: string | null
  effort?: string
  sandbox?: 'read-only' | 'workspace-write'
  approval_policy?: 'never'
  a2a_tools_required?: boolean
  brain_required?: boolean
  kai_required?: boolean
  status_topic?: string
  result_topic?: string
  turn_timeout_ms?: number
  budget_policy?: { max_primary_used_percent?: number }
}

export type FleetAgentSpec = {
  id: string
  role?: string
  runtime?: FleetRuntime
  provider?: FleetProvider
  thread_key?: string
  task: string
  cwd?: string
  metadata?: Record<string, unknown>
  expects?: { artifacts?: string[] }
}

export type FleetSpec = {
  schema: typeof FLEET_SPEC_SCHEMA
  fleet_id: string
  created_at?: string
  kind: 'a2a-fleet'
  base: FleetBase
  placement?: {
    strategy?: string
    hosts?: string[]
    needs?: string[]
  }
  defaults: FleetDefaults
  agents: FleetAgentSpec[]
}

export type FleetValidationIssue = {
  path: string
  message: string
}

export type FleetValidationResult =
  | { ok: true; spec: FleetSpec; issues: [] }
  | { ok: false; issues: FleetValidationIssue[] }

export type FleetRunPlanAgent = {
  agent_id: string
  role: string
  runtime: FleetRuntime
  provider: FleetProvider
  cwd: string
  launch: {
    provider: FleetProvider
    mode: 'shim'
    command?: string[]
    request?: {
      method: 'POST'
      path: string
      body: Record<string, unknown>
    }
  }
  dispatch: {
    target_agent_id: string
    schema: typeof CODEX_JOB_SCHEMA | typeof CLAUDE_ASSIGNMENT_SCHEMA
    envelope: Record<string, unknown>
  }
}

export type FleetRunPlan = {
  schema: typeof FLEET_PLAN_SCHEMA
  fleet_id: string
  dry_run: true
  summary: {
    agents: number
    codex: number
    claude_live: number
    providers: Record<string, number>
  }
  agents: FleetRunPlanAgent[]
}

export type FleetJobReply =
  | { kind: 'accepted'; job_id: string; from?: string; corr?: string; msg: Record<string, unknown> }
  | { kind: 'completed'; job_id: string; from?: string; corr?: string; status?: string; output?: string; result_ref?: string; msg: Record<string, unknown> }
  | { kind: 'failed'; job_id: string; from?: string; corr?: string; error: string; msg: Record<string, unknown> }
  | { kind: 'rejected'; job_id: string; from?: string; corr?: string; reason: string; detail?: string; msg: Record<string, unknown> }
  | { kind: 'ignore'; reason: string }

export type FleetExecutionMode = 'canary' | 'full'

export type FleetTopicEvent = {
  id?: string
  topic?: string
  from?: string
  type?: string
  thread?: string
  body_preview: string
}

export type FleetExecutionResult = {
  ok: boolean
  schema: 'a2a.fleet.canary.result.v1' | 'a2a.fleet.run_result.v1'
  mode: FleetExecutionMode
  fleet_id: string
  orchestrator_id: string
  summary: FleetRunPlan['summary']
  joined_topics: Array<{ topic: string; ok: boolean; error?: string }>
  registered_cwds: Array<{ agent_id: string; cwd: string; realpath?: string; ok: boolean; error?: string }>
  launched: Array<{ agent_id: string; provider: FleetProvider; ok: boolean; stdout?: string; stderr?: string; error?: string }>
  jobs: Array<{
    agent_id: string
    runtime: FleetRuntime
    job_id: string
    send_id?: string
    accepted: boolean
    terminal?: Exclude<FleetJobReply, { kind: 'ignore' | 'accepted' }>
    error?: string
  }>
  topic_events: FleetTopicEvent[]
  stopped?: Array<{ agent_id: string; ok: boolean; stderr?: string; error?: string }>
}

export type CanaryRunResult = FleetExecutionResult & { schema: 'a2a.fleet.canary.result.v1'; mode: 'canary' }

export type LegacyParseOptions = {
  runtime?: FleetRuntime
  provider?: FleetProvider
  sandbox?: 'read-only' | 'workspace-write'
  createdAt?: string
}

export function providerForRuntime(runtime: FleetRuntime): FleetProvider {
  return runtime === 'claude-live' ? 'tmux-claude-live' : 'tmux-codex'
}

export function providerMatchesRuntime(provider: FleetProvider, runtime: FleetRuntime): boolean {
  if (runtime === 'claude-live') return provider === 'tmux-claude-live'
  return provider === 'tmux-codex' || provider === 'docker-codex'
}

export function topicToken(...parts: Array<string | null | undefined>): string {
  const token = parts
    .filter(Boolean)
    .join('-')
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 64)
    .replace(/-$/g, '')
  return token || 'fleet'
}

export function buildCanaryFleetSpec(spec: FleetSpec, opts: { suffix?: string; smoke?: boolean } = {}): FleetSpec {
  const suffix = (opts.suffix ?? Date.now().toString(36)).replace(/[^a-z0-9-]/g, '').slice(-10) || 'canary'
  const manager = spec.agents.find((agent) => (agent.role ?? '') === 'manager') ?? spec.agents[0]
  const worker = spec.agents.find((agent) => agent !== manager && (agent.role ?? 'worker') !== 'manager') ?? spec.agents.find((agent) => agent !== manager)
  const selected = [manager, worker].filter(Boolean) as FleetAgentSpec[]
  const ids = selected.map((agent, i) => i === 0 ? `mgr-${suffix}` : `wkr-${suffix}`)
  return {
    ...spec,
    fleet_id: `${spec.fleet_id}-canary-${suffix}`.slice(0, 64),
    defaults: {
      ...spec.defaults,
      status_topic: topicToken('fleet', spec.fleet_id, 'canary', suffix, 'status'),
      result_topic: topicToken('fleet', spec.fleet_id, 'canary', suffix, 'results'),
    },
    agents: selected.map((agent, i) => ({
      ...agent,
      id: ids[i],
      role: i === 0 ? 'manager' : 'worker',
      thread_key: `fleet:${spec.fleet_id}:canary:${ids[i]}`.slice(0, 128),
      task: opts.smoke === false ? agent.task : canarySmokeTask(spec, agent, ids[i], i === 0 ? 'manager' : 'worker'),
      expects: opts.smoke === false ? agent.expects : { artifacts: [] },
    })),
  }
}

export function classifyFleetJobReply(
  content: string,
  attrs: Record<string, unknown>,
  ctx: { jobId: string; target: string; selfId: string; requestId?: string },
): FleetJobReply {
  if (attrs.feed !== 'a2a' || attrs.kind !== 'direct') return { kind: 'ignore', reason: 'not_a2a_direct' }
  if (attrs.from !== ctx.target || attrs.to !== ctx.selfId) return { kind: 'ignore', reason: 'wrong_peer' }
  if (attrs.type !== 'reply' && attrs.type !== 'msg') return { kind: 'ignore', reason: 'wrong_type' }
  if (attrs.type === 'reply' && ctx.requestId && attrs.corr !== ctx.requestId) return { kind: 'ignore', reason: 'wrong_corr' }
  let msg: Record<string, unknown>
  try {
    const parsed = JSON.parse(content)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { kind: 'ignore', reason: 'bad_body' }
    msg = parsed as Record<string, unknown>
  } catch {
    return { kind: 'ignore', reason: 'bad_json' }
  }
  const job_id = stringValue(msg.job_id)
  if (job_id !== ctx.jobId) return { kind: 'ignore', reason: 'wrong_job' }
  const common = { job_id, from: stringValue(attrs.from), corr: stringValue(attrs.corr), msg }
  switch (msg.schema) {
    case 'codex.job.accepted.v1':
    case 'fleet.task.accepted.v1':
      return { kind: 'accepted', ...common }
    case 'codex.job.completed.v1':
    case 'fleet.task.completed.v1':
    case 'fleet.worker.completed.v1':
      return {
        kind: 'completed',
        ...common,
        status: stringValue(msg.status),
        output: stringValue(msg.output) ?? stringValue(msg.summary),
        result_ref: stringValue(msg.result_ref),
      }
    case 'codex.job.failed.v1':
    case 'fleet.task.failed.v1':
      return { kind: 'failed', ...common, error: stringValue(msg.error) ?? 'job failed' }
    case 'codex.job.rejected.v1':
    case 'fleet.task.rejected.v1':
      return {
        kind: 'rejected',
        ...common,
        reason: stringValue(msg.reason) ?? 'job rejected',
        detail: stringValue(msg.detail),
      }
    default:
      return { kind: 'ignore', reason: 'unsupported_schema' }
  }
}

export function parseLegacyFleetSpec(text: string, opts: LegacyParseOptions = {}): FleetSpec {
  let fleetId = 'legacy-fleet'
  let baseRaw = ''
  const agents: FleetAgentSpec[] = []
  const needs: string[] = []
  const runtime = opts.runtime ?? 'codex'
  const provider = opts.provider ?? providerForRuntime(runtime)
  const sandbox = opts.sandbox ?? 'read-only'

  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const match = /^(\S+)(?:\s+(.*))?$/.exec(line)
    if (!match) continue
    const key = match[1]
    const rest = (match[2] ?? '').trim()
    if (key === 'FLEET') {
      fleetId = rest || fleetId
      continue
    }
    if (key === 'BASE') {
      baseRaw = rest
      continue
    }
    if (key === 'NEEDS') {
      if (rest) needs.push(rest)
      continue
    }
    const task = normalizeLegacyTask(rest)
    const metadata = legacyAgentMetadata(task)
    agents.push({
      id: key,
      role: key.endsWith('-manager') ? 'manager' : 'worker',
      runtime,
      provider,
      thread_key: `fleet:${fleetId}:${key}`,
      task,
      ...(Object.keys(metadata).length ? { metadata } : {}),
      ...legacyExpects(task),
    })
  }

  const base = parseBase(baseRaw)
  const statusTopic = topicToken('fleet', fleetId, 'status')
  return {
    schema: FLEET_SPEC_SCHEMA,
    fleet_id: fleetId,
    created_at: opts.createdAt ?? new Date().toISOString(),
    kind: 'a2a-fleet',
    base,
    placement: {
      strategy: 'local',
      hosts: ['localhost'],
      needs,
    },
    defaults: {
      runtime,
      provider,
      effort: 'xhigh',
      sandbox,
      approval_policy: 'never',
      a2a_tools_required: true,
      brain_required: true,
      kai_required: false,
      status_topic: statusTopic,
      result_topic: topicToken('fleet', fleetId, 'results'),
      turn_timeout_ms: DEFAULT_TURN_TIMEOUT_MS,
      budget_policy: { max_primary_used_percent: DEFAULT_BUDGET_MAX_PRIMARY_USED_PERCENT },
    },
    agents,
  }
}

export function validateFleetSpec(input: unknown): FleetValidationResult {
  const issues: FleetValidationIssue[] = []
  const obj = asRecord(input)

  if (!obj) {
    return { ok: false, issues: [{ path: '$', message: 'spec must be a JSON object' }] }
  }

  if (obj.schema !== FLEET_SPEC_SCHEMA) issues.push({ path: 'schema', message: `must equal ${FLEET_SPEC_SCHEMA}` })
  if (obj.kind !== 'a2a-fleet') issues.push({ path: 'kind', message: 'must equal a2a-fleet' })
  if (!nonEmptyString(obj.fleet_id)) issues.push({ path: 'fleet_id', message: 'must be a non-empty string' })

  const base = asRecord(obj.base)
  if (!base) {
    issues.push({ path: 'base', message: 'must be an object' })
  } else {
    if (!nonEmptyString(base.repo)) issues.push({ path: 'base.repo', message: 'must be a non-empty string' })
    if (!absolutePath(base.cwd)) issues.push({ path: 'base.cwd', message: 'must be an absolute path' })
    if (base.cwd === '/tmp') issues.push({ path: 'base.cwd', message: 'must not default to /tmp' })
  }

  const defaults = asRecord(obj.defaults)
  const defaultRuntime = parseRuntime(defaults?.runtime)
  const defaultProvider = parseProvider(defaults?.provider)
  if (!defaults) {
    issues.push({ path: 'defaults', message: 'must be an object' })
  } else {
    if (!defaultRuntime) issues.push({ path: 'defaults.runtime', message: 'must be codex or claude-live' })
    if (defaults.provider != null && !defaultProvider) issues.push({ path: 'defaults.provider', message: 'must be docker-codex, tmux-codex, or tmux-claude-live' })
    if (defaultRuntime && defaultProvider && !providerMatchesRuntime(defaultProvider, defaultRuntime)) {
      issues.push({ path: 'defaults.provider', message: `provider ${defaultProvider} does not match runtime ${defaultRuntime}` })
    }
    if (defaults.approval_policy != null && defaults.approval_policy !== 'never') {
      issues.push({ path: 'defaults.approval_policy', message: 'must be never' })
    }
    if (defaults.sandbox != null && defaults.sandbox !== 'read-only' && defaults.sandbox !== 'workspace-write') {
      issues.push({ path: 'defaults.sandbox', message: 'must be read-only or workspace-write' })
    }
    for (const field of ['status_topic', 'result_topic'] as const) {
      if (defaults[field] != null && !AGENT_ID_RE.test(String(defaults[field]))) {
        issues.push({ path: `defaults.${field}`, message: 'must be a valid A2A topic token' })
      }
    }
  }

  if (!Array.isArray(obj.agents) || obj.agents.length === 0) {
    issues.push({ path: 'agents', message: 'must be a non-empty array' })
  } else {
    const seen = new Set<string>()
    obj.agents.forEach((rawAgent, i) => {
      const agent = asRecord(rawAgent)
      const prefix = `agents[${i}]`
      if (!agent) {
        issues.push({ path: prefix, message: 'must be an object' })
        return
      }
      const id = stringValue(agent.id)
      if (!id || !AGENT_ID_RE.test(id)) issues.push({ path: `${prefix}.id`, message: `must match ${AGENT_ID_RE}` })
      if (id && seen.has(id)) issues.push({ path: `${prefix}.id`, message: `duplicate agent id ${id}` })
      if (id) seen.add(id)
      const runtime = parseRuntime(agent.runtime) ?? defaultRuntime
      const provider = parseProvider(agent.provider) ?? defaultProvider ?? (runtime ? providerForRuntime(runtime) : undefined)
      if (!runtime) issues.push({ path: `${prefix}.runtime`, message: 'must be codex or claude-live, or inherit a valid default' })
      if (!provider) issues.push({ path: `${prefix}.provider`, message: 'must be valid, or inherit a valid default' })
      if (runtime && provider && !providerMatchesRuntime(provider, runtime)) {
        issues.push({ path: `${prefix}.provider`, message: `provider ${provider} does not match runtime ${runtime}` })
      }
      if (!nonEmptyString(agent.task)) issues.push({ path: `${prefix}.task`, message: 'must be a non-empty string' })
      const cwd = stringValue(agent.cwd) ?? stringValue(base?.cwd)
      if (!absolutePath(cwd)) issues.push({ path: `${prefix}.cwd`, message: 'must inherit or set an absolute cwd' })
      if (cwd === '/tmp') issues.push({ path: `${prefix}.cwd`, message: 'must not default to /tmp' })
      const threadKey = stringValue(agent.thread_key)
      if (!threadKey) issues.push({ path: `${prefix}.thread_key`, message: 'must be a non-empty string' })
    })
  }

  if (issues.length) return { ok: false, issues }
  return { ok: true, spec: input as FleetSpec, issues: [] }
}

export function buildFleetRunPlan(spec: FleetSpec): FleetRunPlan {
  const agents = spec.agents.map((agent) => buildPlanAgent(spec, agent))
  const providers: Record<string, number> = {}
  for (const agent of agents) providers[agent.provider] = (providers[agent.provider] ?? 0) + 1
  return {
    schema: FLEET_PLAN_SCHEMA,
    fleet_id: spec.fleet_id,
    dry_run: true,
    summary: {
      agents: agents.length,
      codex: agents.filter((a) => a.runtime === 'codex').length,
      claude_live: agents.filter((a) => a.runtime === 'claude-live').length,
      providers,
    },
    agents,
  }
}

function buildPlanAgent(spec: FleetSpec, agent: FleetAgentSpec): FleetRunPlanAgent {
  const runtime = agent.runtime ?? spec.defaults.runtime
  const provider = agent.provider ?? spec.defaults.provider ?? providerForRuntime(runtime)
  const cwd = resolveAgentCwd(spec, agent, provider)
  const role = agent.role ?? 'worker'
  const worktree = spec.base.ref ? `${spec.base.repo}@${spec.base.ref}` : spec.base.repo
  return {
    agent_id: agent.id,
    role,
    runtime,
    provider,
    cwd,
    launch: buildLaunchPlan(agent.id, provider, worktree),
    dispatch: buildDispatchPlan(spec, agent, runtime, cwd),
  }
}

function resolveAgentCwd(spec: FleetSpec, agent: FleetAgentSpec, provider: FleetProvider): string {
  if (agent.cwd) return agent.cwd
  if ((spec.defaults.sandbox ?? 'read-only') === 'workspace-write' && provider !== 'docker-codex') {
    return join(workerWorktreeRoot(), agent.id, 'wt')
  }
  return spec.base.cwd
}

function workerWorktreeRoot(): string {
  return process.env.WORKER_WORKTREE_ROOT ?? process.env.AGENTS_ROOT ?? join(homedir(), 'a2a-agents')
}

function buildLaunchPlan(agentId: string, provider: FleetProvider, worktree: string): FleetRunPlanAgent['launch'] {
  if (provider === 'docker-codex') {
    return {
      provider,
      mode: 'shim',
      request: {
        method: 'POST',
        path: '/v1/agents/codex',
        body: {
          agent_id: agentId,
          label: fleetLabel(agentId),
          mode: 'shim',
          worktree,
          dry_run: true,
        },
      },
    }
  }
  const kind = provider === 'tmux-claude-live' ? 'claude' : 'codex'
  return {
    provider,
    mode: 'shim',
    command: ['bash', 'a2a-launch.sh', agentId, kind, '--shim', '--worktree', worktree],
  }
}

function buildDispatchPlan(spec: FleetSpec, agent: FleetAgentSpec, runtime: FleetRuntime, cwd: string): FleetRunPlanAgent['dispatch'] {
  const jobId = jobIdFor(spec.fleet_id, agent.id)
  const statusTopic = spec.defaults.status_topic ?? topicToken('fleet', spec.fleet_id, 'status')
  const resultTopic = spec.defaults.result_topic ?? topicToken('fleet', spec.fleet_id, 'results')
  const threadKey = agent.thread_key ?? `fleet:${spec.fleet_id}:${agent.id}`
  if (runtime === 'codex') {
    return {
      target_agent_id: agent.id,
      schema: CODEX_JOB_SCHEMA,
      envelope: {
        schema: CODEX_JOB_SCHEMA,
        job_id: jobId,
        thread_key: threadKey,
        input: [{ type: 'text', text: agent.task }],
        sandbox: spec.defaults.sandbox ?? 'read-only',
        approval_policy: spec.defaults.approval_policy ?? 'never',
        cwd,
        stream_topic: topicToken('fleet', spec.fleet_id, agent.id, 'stream'),
        budget_policy: {
          max_primary_used_percent: spec.defaults.budget_policy?.max_primary_used_percent ?? DEFAULT_BUDGET_MAX_PRIMARY_USED_PERCENT,
        },
        fleet: fleetContext(spec, agent, resultTopic),
      },
    }
  }
  return {
    target_agent_id: agent.id,
    schema: CLAUDE_ASSIGNMENT_SCHEMA,
    envelope: {
      schema: CLAUDE_ASSIGNMENT_SCHEMA,
      fleet_id: spec.fleet_id,
      agent_id: agent.id,
      job_id: jobId,
      thread_key: threadKey,
      task: agent.task,
      cwd,
      sandbox: spec.defaults.sandbox ?? 'read-only',
      approval_policy: spec.defaults.approval_policy ?? 'never',
      status_topic: statusTopic,
      result_topic: resultTopic,
      metadata: agent.metadata ?? {},
      expects: agent.expects ?? {},
    },
  }
}

function fleetContext(spec: FleetSpec, agent: FleetAgentSpec, resultTopic: string): Record<string, unknown> {
  return {
    fleet_id: spec.fleet_id,
    agent_id: agent.id,
    role: agent.role ?? 'worker',
    result_topic: resultTopic,
    metadata: agent.metadata ?? {},
    expects: agent.expects ?? {},
  }
}

function canarySmokeTask(spec: FleetSpec, source: FleetAgentSpec, agentId: string, role: string): string {
  const sourceInfo = source.metadata ? ` Source metadata: ${JSON.stringify(source.metadata)}.` : ''
  return [
    `A2A Fleet v2 canary for ${spec.fleet_id}. You are ${agentId} (${role}).`,
    'This is a smoke test only. Do not modify files, do not run git commit, and do not touch fire/order/executor paths.',
    'Use your A2A tools if available to list peers, then reply with a concise status summary containing your agent id, cwd, runtime/tool availability, and one observed peer count.',
    sourceInfo,
  ].filter(Boolean).join(' ')
}

function normalizeLegacyTask(rest: string): string {
  const stripped = rest.replace(/^\/(?:goal|loop)(?:\s+|$)/, '').trim()
  return stripped
}

function parseBase(baseRaw: string): FleetBase {
  const at = baseRaw.lastIndexOf('@')
  if (at > 0) {
    const repo = baseRaw.slice(0, at)
    const ref = baseRaw.slice(at + 1)
    return { repo, ref, cwd: repo }
  }
  return { repo: baseRaw, cwd: baseRaw }
}

function legacyAgentMetadata(task: string): Record<string, unknown> {
  // Neutral metadata extraction: pull a free-standing ISO date if the task names
  // one ("... on YYYY-MM-DD ..."). Callers can layer richer extraction on top.
  const metadata: Record<string, unknown> = {}
  const date = /\bon\s+([0-9]{4}-[0-9]{2}-[0-9]{2})\b/.exec(task)?.[1]
  if (date) metadata.date = date
  return metadata
}

function legacyExpects(task: string): Pick<FleetAgentSpec, 'expects'> {
  // Extract a declared output artifact from a "Write <path>" instruction in the
  // task text. Generic: any path-like token after "Write" is treated as expected.
  const artifacts = new Set<string>()
  const writeMatch = /\bWrite\s+([^\s,]+)/i.exec(task)?.[1]
  if (writeMatch) artifacts.add(cleanArtifact(writeMatch))
  return artifacts.size ? { expects: { artifacts: [...artifacts] } } : {}
}

function cleanArtifact(value: string): string {
  return value.replace(/[).]+$/g, '')
}

function jobIdFor(fleetId: string, agentId: string): string {
  return `fleet-${fleetId}-${agentId}-001`.replace(/[^a-zA-Z0-9_.-]+/g, '-')
}

function fleetLabel(agentId: string): string {
  return agentId.slice(0, 48) || 'fleet'
}

function parseRuntime(value: unknown): FleetRuntime | null {
  return value === 'codex' || value === 'claude-live' ? value : null
}

function parseProvider(value: unknown): FleetProvider | null {
  return value === 'docker-codex' || value === 'tmux-codex' || value === 'tmux-claude-live' ? value : null
}

function parseSandbox(value: unknown): FleetDefaults['sandbox'] | null {
  return value === 'read-only' || value === 'workspace-write' ? value : null
}

function parseToolJson(res: any): Record<string, any> | null {
  try {
    const text = res?.content?.[0]?.text
    return typeof text === 'string' ? JSON.parse(text) : null
  } catch {
    return null
  }
}

async function loadEnvFileAsync(path: string): Promise<Record<string, string>> {
  const out: Record<string, string> = {}
  const text = await Bun.file(path).text()
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const idx = line.indexOf('=')
    if (idx <= 0) continue
    const key = line.slice(0, idx).trim()
    let value = line.slice(idx + 1).trim()
    value = value.replace(/^['"]|['"]$/g, '')
    out[key] = value
  }
  return out
}

async function spawnCommand(cmd: string[], opts: { cwd: string; env?: Record<string, string | undefined>; timeoutMs: number }): Promise<{ ok: boolean; stdout: string; stderr: string; exitCode: number | null; timedOut?: boolean }> {
  const proc = Bun.spawn(cmd, {
    cwd: opts.cwd,
    env: { ...process.env, ...(opts.env ?? {}) },
    stdout: 'pipe',
    stderr: 'pipe',
  })
  const timeout = setTimeout(() => {
    try { proc.kill() } catch {}
  }, opts.timeoutMs)
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]).finally(() => clearTimeout(timeout))
  return { ok: exitCode === 0, stdout, stderr, exitCode, timedOut: exitCode === null }
}

async function waitForPresence(a2a: A2AChannel, agentId: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const peers = parseToolJson(await a2a.callTool('a2a_peers', {}))
    if (peers?.ok && Array.isArray(peers.peers) && peers.peers.some((peer: any) => peer?.id === agentId)) return true
    await Bun.sleep(2000)
  }
  return false
}

export function fleetExecutionTopics(spec: FleetSpec, plan: FleetRunPlan = buildFleetRunPlan(spec)): string[] {
  const topics = new Set<string>()
  topics.add(spec.defaults.status_topic ?? topicToken('fleet', spec.fleet_id, 'status'))
  topics.add(spec.defaults.result_topic ?? topicToken('fleet', spec.fleet_id, 'results'))
  for (const agent of plan.agents) {
    const streamTopic = stringValue(agent.dispatch.envelope.stream_topic)
    if (streamTopic) topics.add(streamTopic)
  }
  return [...topics].filter((topic) => AGENT_ID_RE.test(topic))
}

async function executeCanary(spec: FleetSpec, opts: {
  envFile: string
  orchestratorId?: string
  timeoutMs: number
  gatewayTurnTimeoutMs: number
  launchTimeoutMs: number
  presenceTimeoutMs: number
  smoke: boolean
  stopAfter: boolean
  maxTopicEvents: number
}): Promise<CanaryRunResult> {
  const canary = buildCanaryFleetSpec(spec, { smoke: opts.smoke })
  if (opts.smoke) canary.defaults.sandbox = 'read-only'
  const result = await executeFleetSpec(canary, {
    ...opts,
    mode: 'canary',
    schema: 'a2a.fleet.canary.result.v1',
    label: 'fleet-canary',
    toolTimeoutSec: DEFAULT_CANARY_TOOL_TIMEOUT_SEC,
  })
  return result as CanaryRunResult
}

async function executeFullFleet(spec: FleetSpec, opts: {
  envFile: string
  orchestratorId?: string
  timeoutMs: number
  gatewayTurnTimeoutMs: number
  launchTimeoutMs: number
  presenceTimeoutMs: number
  stopAfter: boolean
  maxTopicEvents: number
  toolTimeoutSec: number
}): Promise<FleetExecutionResult> {
  return executeFleetSpec(spec, {
    ...opts,
    mode: 'full',
    schema: 'a2a.fleet.run_result.v1',
    label: 'fleet-full',
  })
}

async function executeFleetSpec(spec: FleetSpec, opts: {
  envFile: string
  orchestratorId?: string
  mode: FleetExecutionMode
  schema: FleetExecutionResult['schema']
  label: string
  timeoutMs: number
  gatewayTurnTimeoutMs: number
  launchTimeoutMs: number
  presenceTimeoutMs: number
  toolTimeoutSec: number
  stopAfter: boolean
  maxTopicEvents: number
}): Promise<FleetExecutionResult> {
  const envFromFile = existsSync(opts.envFile) ? await loadEnvFileAsync(opts.envFile) : {}
  Object.assign(process.env, envFromFile)
  const orchestratorId = opts.orchestratorId ?? envFromFile.A2A_AGENT_ID ?? process.env.A2A_AGENT_ID ?? 'fleet-orchestrator'
  const validation = validateFleetSpec(spec)
  if (!validation.ok) throw new Error(`invalid ${opts.mode} spec: ${JSON.stringify(validation.issues)}`)
  const plan = buildFleetRunPlan(validation.spec)
  const topics = fleetExecutionTopics(validation.spec, plan)

  const pending = new Map<string, {
    agent: FleetRunPlanAgent
    requestId?: string
    accepted: boolean
    terminal?: Exclude<FleetJobReply, { kind: 'ignore' | 'accepted' }>
    resolve: () => void
  }>()
  const terminalPromises: Promise<void>[] = []
  const topicEvents: FleetTopicEvent[] = []
  const a2a = new A2AChannel(async (content, attrs) => {
    const a = attrs as Record<string, unknown>
    if (a.kind === 'topic') {
      const topic = topicFromAttrs(a)
      if (topic && topics.includes(topic) && topicEvents.length < opts.maxTopicEvents) {
        topicEvents.push({
          id: stringValue(a.id),
          topic,
          from: stringValue(a.from),
          type: stringValue(a.type),
          thread: stringValue(a.thread),
          body_preview: content.slice(0, 1000),
        })
      }
    }
    for (const item of pending.values()) {
      const jobId = String(item.agent.dispatch.envelope.job_id)
      const reply = classifyFleetJobReply(content, a, {
        jobId,
        target: item.agent.agent_id,
        selfId: orchestratorId,
        requestId: item.requestId,
      })
      if (reply.kind === 'accepted') item.accepted = true
      else if (reply.kind !== 'ignore') {
        item.terminal = reply
        item.resolve()
      }
    }
  }, { enabled: true, agentId: orchestratorId })

  const result: FleetExecutionResult = {
    ok: false,
    schema: opts.schema,
    mode: opts.mode,
    fleet_id: validation.spec.fleet_id,
    orchestrator_id: orchestratorId,
    summary: plan.summary,
    joined_topics: [],
    registered_cwds: [],
    launched: [],
    jobs: [],
    topic_events: topicEvents,
  }

  try {
    await a2a.start()
    if (!a2a.isStarted()) throw new Error(`could not join A2A bus as ${orchestratorId}`)

    for (const topic of topics) {
      const joined = parseToolJson(await a2a.callTool('a2a_join_topic', { topic }))
      result.joined_topics.push({ topic, ok: !!joined?.ok, ...(joined?.ok ? {} : { error: joined?.error ?? 'join_failed' }) })
    }

    for (const agent of plan.agents) {
      const launchResult = await launchFleetAgent(validation.spec, agent, {
        orchestratorId,
        label: opts.label,
        gatewayTurnTimeoutMs: opts.gatewayTurnTimeoutMs,
        toolTimeoutSec: opts.toolTimeoutSec,
        launchTimeoutMs: opts.launchTimeoutMs,
      })
      result.launched.push(launchResult)
      if (!launchResult.ok) continue
      const present = await waitForPresence(a2a, agent.agent_id, opts.presenceTimeoutMs)
      if (!present) {
        result.launched[result.launched.length - 1].ok = false
        result.launched[result.launched.length - 1].error = 'presence_timeout'
      }
    }

    const runnable = plan.agents.filter((agent) => result.launched.some((launch) => launch.agent_id === agent.agent_id && launch.ok))
    const dispatchable: FleetRunPlanAgent[] = []
    for (const agent of runnable) {
      if (agent.dispatch.envelope.sandbox !== 'workspace-write') {
        dispatchable.push(agent)
        continue
      }
      const registered = await registerWritableCwd(agent)
      result.registered_cwds.push(registered)
      if (registered.ok) dispatchable.push(agent)
      else {
        result.jobs.push({
          agent_id: agent.agent_id,
          runtime: agent.runtime,
          job_id: String(agent.dispatch.envelope.job_id),
          accepted: false,
          error: `cwd_registration_failed:${registered.error ?? 'unknown'}`,
        })
      }
    }
    for (const agent of dispatchable) {
      let resolve!: () => void
      terminalPromises.push(new Promise<void>((r) => { resolve = r }))
      const pendingJob = { agent, accepted: false, resolve }
      pending.set(String(agent.dispatch.envelope.job_id), pendingJob)
      const sent = parseToolJson(await a2a.callTool('a2a_send', {
        to: agent.agent_id,
        type: 'request',
        thread: String(agent.dispatch.envelope.thread_key ?? agent.dispatch.envelope.job_id),
        body: JSON.stringify(agent.dispatch.envelope),
        ttl_ms: opts.timeoutMs,
      }))
      pendingJob.requestId = sent?.id
      if (!sent?.ok) {
        pendingJob.terminal = { kind: 'failed', job_id: String(agent.dispatch.envelope.job_id), error: sent?.error ?? 'send_failed', msg: sent ?? {} }
        pendingJob.resolve()
      }
    }

    await Promise.race([
      Promise.allSettled(terminalPromises),
      Bun.sleep(opts.timeoutMs),
    ])

    for (const [jobId, item] of pending.entries()) {
      result.jobs.push({
        agent_id: item.agent.agent_id,
        runtime: item.agent.runtime,
        job_id: jobId,
        send_id: item.requestId,
        accepted: item.accepted,
        ...(item.terminal ? { terminal: item.terminal } : { error: 'terminal_timeout' }),
      })
    }
    result.ok = result.launched.length > 0 && result.launched.every((launch) => launch.ok) && result.jobs.length > 0 && result.jobs.every((job) => job.terminal?.kind === 'completed')
    if (opts.stopAfter) result.stopped = await stopFleetSessions(result.launched.map((launch) => launch.agent_id))
    return result
  } finally {
    for (const joined of result.joined_topics) {
      if (joined.ok) await a2a.callTool('a2a_leave_topic', { topic: joined.topic }).catch(() => {})
    }
    await a2a.stop().catch(() => {})
  }
}

// Launch one fleet agent through the operator-selected backend. No hard dependency
// on tmux or any bundled script: prefer the generic a2a-launcher HTTP control-plane,
// fall back to an explicitly-configured launch script, else fail soft.
async function launchFleetAgent(spec: FleetSpec, agent: FleetRunPlanAgent, opts: {
  orchestratorId: string
  label: string
  gatewayTurnTimeoutMs: number
  toolTimeoutSec: number
  launchTimeoutMs: number
}): Promise<FleetExecutionResult['launched'][number]> {
  const worktree = spec.base.ref ? `${spec.base.repo}@${spec.base.ref}` : spec.base.repo
  const launcherUrl = (process.env.A2A_LAUNCHER_URL ?? '').trim()
  const launchScript = (process.env.A2A_FLEET_LAUNCH_SCRIPT ?? '').trim()
  if (launcherUrl) return launchViaLauncherHttp(launcherUrl, spec, agent, worktree, opts)
  if (launchScript) return launchViaShellScript(launchScript, spec, agent, worktree, opts)
  return {
    agent_id: agent.agent_id,
    provider: agent.provider,
    ok: false,
    error: 'no_launch_backend: set A2A_LAUNCHER_URL (a2a-launcher control-plane) or A2A_FLEET_LAUNCH_SCRIPT (custom launch script)',
  }
}

// Generic backend: POST to the a2a-launcher HTTP control-plane (a2a_launcher.ts),
// the repo's first-class launch primitive. Fail-soft — a slow/dead/absent launcher
// returns ok:false and never throws.
async function launchViaLauncherHttp(launcherUrl: string, spec: FleetSpec, agent: FleetRunPlanAgent, worktree: string, opts: {
  orchestratorId: string
  label: string
  gatewayTurnTimeoutMs: number
  launchTimeoutMs: number
}): Promise<FleetExecutionResult['launched'][number]> {
  const sandbox = spec.defaults.sandbox ?? 'read-only'
  const body = {
    agent_id: agent.agent_id,
    created_by: opts.orchestratorId,
    mode: agent.launch.mode,
    label: opts.label,
    worktree,
    policy: {
      allow_write: sandbox === 'workspace-write',
      sandbox,
      ...(spec.defaults.effort ? { effort: spec.defaults.effort } : {}),
      ...(spec.defaults.model ? { model: spec.defaults.model } : {}),
      turn_timeout_ms: opts.gatewayTurnTimeoutMs,
    },
  }
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), opts.launchTimeoutMs)
  try {
    const headers: Record<string, string> = { 'content-type': 'application/json' }
    const token = process.env.A2A_LAUNCHER_TOKEN ?? process.env.A2A_LAUNCHER_BEARER_TOKEN
    if (token) headers.authorization = `Bearer ${token}`
    const res = await fetch(`${launcherUrl.replace(/\/+$/, '')}/v1/agents/codex`, {
      method: 'POST', headers, body: JSON.stringify(body), signal: ctrl.signal,
    })
    const text = await res.text()
    let parsed: any = null
    try { parsed = text ? JSON.parse(text) : null } catch { /* non-JSON body */ }
    const ok = res.ok && parsed?.ok === true
    return {
      agent_id: agent.agent_id,
      provider: agent.provider,
      ok,
      stdout: text.trim().slice(0, 4000),
      ...(ok ? {} : { error: `launcher_http_${res.status}:${parsed?.error ?? 'launch_failed'}` }),
    }
  } catch (e) {
    const msg = e instanceof Error && e.name === 'AbortError' ? `timeout_after_${opts.launchTimeoutMs}ms` : (e instanceof Error ? e.message : String(e))
    return { agent_id: agent.agent_id, provider: agent.provider, ok: false, error: `launcher_unreachable:${msg}` }
  } finally {
    clearTimeout(timer)
  }
}

// Optional backend: an explicitly-configured launch script (e.g. a tmux/codex
// launcher). Opt-in only via A2A_FLEET_LAUNCH_SCRIPT — there is no bundled default.
async function launchViaShellScript(scriptPath: string, spec: FleetSpec, agent: FleetRunPlanAgent, worktree: string, opts: {
  orchestratorId: string
  label: string
  gatewayTurnTimeoutMs: number
  toolTimeoutSec: number
  launchTimeoutMs: number
}): Promise<FleetExecutionResult['launched'][number]> {
  const kind = agent.provider === 'tmux-claude-live' ? 'claude' : 'codex'
  const launchMode = agent.launch.mode
  const cmd = ['bash', scriptPath, agent.agent_id, kind, `--${launchMode}`, '--worktree', worktree]
  const launch = await spawnCommand(cmd, {
    cwd: import.meta.dir,
    timeoutMs: opts.launchTimeoutMs,
    env: {
      CHANNELS: import.meta.dir,
      A2A_MODE: launchMode,
      A2A_PARENT_AGENT_ID: opts.orchestratorId,
      A2A_LAUNCH_AGENT_LABEL: opts.label,
      A2A_FLEET_ID: spec.fleet_id,
      A2A_FLEET_MODE: opts.label,
      CODEX_GW_A2A_TOOLS_REQUIRED: spec.defaults.a2a_tools_required === false ? '0' : '1',
      CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC: String(opts.toolTimeoutSec),
      CODEX_GW_TURN_TIMEOUT_MS: String(opts.gatewayTurnTimeoutMs),
      ...gatewayWriteEnv(spec, opts.orchestratorId),
      ...(spec.defaults.model ? { CODEX_GW_MODEL: spec.defaults.model } : {}),
      ...(spec.defaults.effort ? { CODEX_GW_EFFORT: spec.defaults.effort } : {}),
    },
  })
  return {
    agent_id: agent.agent_id,
    provider: agent.provider,
    ok: launch.ok,
    stdout: launch.stdout.trim().slice(0, 4000),
    stderr: launch.stderr.trim().slice(0, 4000),
    ...(launch.ok ? {} : { error: `launch_exit_${launch.exitCode}${launch.timedOut ? '_timeout' : ''}` }),
  }
}

function gatewayWriteEnv(spec: FleetSpec, orchestratorId: string): Record<string, string> {
  if ((spec.defaults.sandbox ?? 'read-only') !== 'workspace-write') return {}
  return {
    CODEX_GW_ALLOW_WRITE: '1',
    CODEX_GW_WRITE_ALLOWLIST: process.env.A2A_FLEET_WRITE_ALLOWLIST ?? orchestratorId,
    CODEX_BUILD_CWD_ROOTS: process.env.CODEX_BUILD_CWD_ROOTS ?? workerWorktreeRoot(),
    CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX: process.env.CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX ?? 'workspace-write',
  }
}

// Stop launched sessions via an operator-configured command — NOT hard-wired to
// tmux. A2A_FLEET_STOP_CMD is a command template (space-split) with the session name
// `a2a-<agentId>` appended (e.g. "tmux kill-session -t" reproduces the old behavior).
// Unset ⇒ stop is skipped, reported per agent, never erroring the run.
async function stopFleetSessions(agentIds: string[]): Promise<NonNullable<FleetExecutionResult['stopped']>> {
  const stopped: NonNullable<FleetExecutionResult['stopped']> = []
  const template = (process.env.A2A_FLEET_STOP_CMD ?? '').trim()
  for (const agentId of [...new Set(agentIds)]) {
    const session = `a2a-${agentId}`
    if (!template) {
      stopped.push({ agent_id: agentId, ok: false, error: 'stop_not_configured: set A2A_FLEET_STOP_CMD (e.g. "tmux kill-session -t")' })
      continue
    }
    const cmd = [...template.split(/\s+/).filter(Boolean), session]
    const res = await spawnCommand(cmd, { cwd: import.meta.dir, timeoutMs: 10_000 })
    stopped.push({
      agent_id: agentId,
      ok: res.ok,
      stderr: res.stderr.trim().slice(0, 1000),
      ...(res.ok ? {} : { error: `stop_exit_${res.exitCode}${res.timedOut ? '_timeout' : ''}` }),
    })
  }
  return stopped
}

async function registerWritableCwd(agent: FleetRunPlanAgent): Promise<FleetExecutionResult['registered_cwds'][number]> {
  const cwd = String(agent.dispatch.envelope.cwd ?? '')
  const roots = writeCwdRoots()
  const canonical = canonicalizeCwd(cwd, roots)
  if (!canonical.ok) return { agent_id: agent.agent_id, cwd, ok: false, error: canonical.reason }
  const redis = new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379')
  try {
    const ok = await registerCwd(redis, canonical.realpath, { ttlS: Number(process.env.A2A_FLEET_CWD_ALLOW_TTL_S ?? DEFAULT_CWD_ALLOW_TTL_S) })
    return { agent_id: agent.agent_id, cwd, realpath: canonical.realpath, ok, ...(ok ? {} : { error: 'redis_register_failed' }) }
  } finally {
    try { redis.close() } catch {}
  }
}

function writeCwdRoots(): string[] {
  const raw = process.env.CODEX_BUILD_CWD_ROOTS ?? workerWorktreeRoot()
  return raw.split(',').map((s) => s.trim()).filter(Boolean)
}

function topicFromAttrs(attrs: Record<string, unknown>): string | undefined {
  const to = stringValue(attrs.to)
  if (to?.startsWith('topic:')) return to.slice('topic:'.length)
  const subject = stringValue(attrs.subject)
  return subject?.match(/\.topic\.([a-z0-9-]+)$/)?.[1]
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value != null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function absolutePath(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith('/') && value.length > 1
}

async function readJson(path: string): Promise<unknown> {
  return JSON.parse(await Bun.file(path).text())
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await Bun.write(path, `${JSON.stringify(value, null, 2)}\n`)
}

const FLAGS_WITH_VALUES = new Set([
  '--out',
  '--runtime',
  '--provider',
  '--sandbox',
  '--orchestrator-env',
  '--orchestrator-id',
  '--timeout-ms',
  '--gateway-turn-timeout-ms',
  '--launch-timeout-ms',
  '--presence-timeout-ms',
  '--tool-timeout-sec',
  '--max-topic-events',
])

function positionalArg(args: string[]): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg.startsWith('--')) {
      if (FLAGS_WITH_VALUES.has(arg)) i++
      continue
    }
    return arg
  }
  return undefined
}

function parseFlag(args: string[], name: string): string | undefined {
  const idx = args.indexOf(name)
  if (idx < 0) return undefined
  const value = args[idx + 1]
  return value && !value.startsWith('--') ? value : undefined
}

function hasFlag(args: string[], name: string): boolean {
  return args.includes(name)
}

function usage(): string {
  return [
    'usage:',
    '  bun a2a_fleet_orchestrator.ts validate <spec.v2.json>',
    '  bun a2a_fleet_orchestrator.ts run --dry-run <spec.v2.json>',
    '  bun a2a_fleet_orchestrator.ts run --execute --canary <spec.v2.json> [--orchestrator-env a2a/fleet-orchestrator.a2a.env] [--gateway-turn-timeout-ms 180000] [--stop-after]',
    '  bun a2a_fleet_orchestrator.ts run --execute --full <spec.v2.json> [--orchestrator-env a2a/fleet-orchestrator.a2a.env] [--stop-after]',
    '  bun a2a_fleet_orchestrator.ts convert-v1 <legacy.spec> --out <spec.v2.json> [--runtime codex|claude-live] [--provider docker-codex|tmux-codex|tmux-claude-live] [--sandbox read-only|workspace-write]',
  ].join('\n')
}

async function main(argv: string[]): Promise<number> {
  const [cmd, ...args] = argv
  if (!cmd || cmd === '-h' || cmd === '--help') {
    console.log(usage())
    return cmd ? 0 : 2
  }

  if (cmd === 'convert-v1') {
    const input = positionalArg(args)
    const out = parseFlag(args, '--out')
    if (!input || !out) {
      console.error(usage())
      return 2
    }
    const runtimeFlag = parseFlag(args, '--runtime')
    const providerFlag = parseFlag(args, '--provider')
    const sandboxFlag = parseFlag(args, '--sandbox')
    const runtime = runtimeFlag == null ? undefined : parseRuntime(runtimeFlag)
    const provider = providerFlag == null ? undefined : parseProvider(providerFlag)
    const sandbox = sandboxFlag == null ? undefined : parseSandbox(sandboxFlag)
    if (runtimeFlag && !runtime) {
      console.error(`invalid --runtime: ${runtimeFlag}`)
      return 2
    }
    if (providerFlag && !provider) {
      console.error(`invalid --provider: ${providerFlag}`)
      return 2
    }
    if (sandboxFlag && !sandbox) {
      console.error(`invalid --sandbox: ${sandboxFlag}`)
      return 2
    }
    const spec = parseLegacyFleetSpec(await Bun.file(input).text(), { runtime, provider, sandbox })
    const validation = validateFleetSpec(spec)
    if (!validation.ok) {
      console.error(JSON.stringify(validation, null, 2))
      return 1
    }
    await writeJson(out, spec)
    console.log(JSON.stringify({ ok: true, out, fleet_id: spec.fleet_id, agents: spec.agents.length }, null, 2))
    return 0
  }

  if (cmd === 'validate') {
    const input = positionalArg(args)
    if (!input) {
      console.error(usage())
      return 2
    }
    const validation = validateFleetSpec(await readJson(input))
    console.log(JSON.stringify(validation.ok ? { ok: true, fleet_id: validation.spec.fleet_id, agents: validation.spec.agents.length } : validation, null, 2))
    return validation.ok ? 0 : 1
  }

  if (cmd === 'run') {
    const dryRun = hasFlag(args, '--dry-run')
    const execute = hasFlag(args, '--execute')
    const canary = hasFlag(args, '--canary')
    const full = hasFlag(args, '--full')
    const realTasks = hasFlag(args, '--real-tasks')
    const stopAfter = hasFlag(args, '--stop-after')
    const input = positionalArg(args)
    if (!input) {
      console.error(usage())
      return 2
    }
    const validation = validateFleetSpec(await readJson(input))
    if (!validation.ok) {
      console.error(JSON.stringify(validation, null, 2))
      return 1
    }
    if (!dryRun) {
      if (!execute) {
        console.error(usage())
        return 2
      }
      if (canary && full) {
        console.error(JSON.stringify({ ok: false, error: 'bad_mode', detail: 'choose --canary or --full, not both' }, null, 2))
        return 2
      }
      if (execute && canary) {
        const envFile = parseFlag(args, '--orchestrator-env') ?? join(import.meta.dir, 'a2a/fleet-orchestrator.a2a.env')
        const orchestratorId = parseFlag(args, '--orchestrator-id')
        const timeoutMs = Number(parseFlag(args, '--timeout-ms') ?? DEFAULT_CANARY_TIMEOUT_MS)
        const gatewayTurnTimeoutMs = Number(parseFlag(args, '--gateway-turn-timeout-ms') ?? Math.min(DEFAULT_CANARY_GATEWAY_TURN_TIMEOUT_MS, Math.max(15_000, timeoutMs - 30_000)))
        const launchTimeoutMs = Number(parseFlag(args, '--launch-timeout-ms') ?? DEFAULT_LAUNCH_TIMEOUT_MS)
        const presenceTimeoutMs = Number(parseFlag(args, '--presence-timeout-ms') ?? DEFAULT_PRESENCE_TIMEOUT_MS)
        const maxTopicEvents = Number(parseFlag(args, '--max-topic-events') ?? DEFAULT_MAX_TOPIC_EVENTS)
        const result = await executeCanary(validation.spec, {
          envFile,
          orchestratorId,
          timeoutMs,
          gatewayTurnTimeoutMs,
          launchTimeoutMs,
          presenceTimeoutMs,
          smoke: !realTasks,
          stopAfter,
          maxTopicEvents,
        })
        console.log(JSON.stringify(result, null, 2))
        return result.ok ? 0 : 1
      }
      if (execute && full) {
        const envFile = parseFlag(args, '--orchestrator-env') ?? join(import.meta.dir, 'a2a/fleet-orchestrator.a2a.env')
        const orchestratorId = parseFlag(args, '--orchestrator-id')
        const timeoutMs = Number(parseFlag(args, '--timeout-ms') ?? validation.spec.defaults.turn_timeout_ms ?? DEFAULT_TURN_TIMEOUT_MS)
        const gatewayTurnTimeoutMs = Number(parseFlag(args, '--gateway-turn-timeout-ms') ?? validation.spec.defaults.turn_timeout_ms ?? timeoutMs)
        const launchTimeoutMs = Number(parseFlag(args, '--launch-timeout-ms') ?? DEFAULT_LAUNCH_TIMEOUT_MS)
        const presenceTimeoutMs = Number(parseFlag(args, '--presence-timeout-ms') ?? DEFAULT_PRESENCE_TIMEOUT_MS)
        const maxTopicEvents = Number(parseFlag(args, '--max-topic-events') ?? DEFAULT_MAX_TOPIC_EVENTS)
        const toolTimeoutSec = Number(parseFlag(args, '--tool-timeout-sec') ?? DEFAULT_EXECUTION_TOOL_TIMEOUT_SEC)
        const result = await executeFullFleet(validation.spec, {
          envFile,
          orchestratorId,
          timeoutMs,
          gatewayTurnTimeoutMs,
          launchTimeoutMs,
          presenceTimeoutMs,
          stopAfter,
          maxTopicEvents,
          toolTimeoutSec,
        })
        console.log(JSON.stringify(result, null, 2))
        return result.ok ? 0 : 1
      }
      console.error(JSON.stringify({ ok: false, error: 'missing_mode', detail: 'Use --canary for smoke execution or --full for the guarded full fleet.' }, null, 2))
      return 2
    }
    console.log(JSON.stringify(buildFleetRunPlan(validation.spec), null, 2))
    return 0
  }

  console.error(usage())
  return 2
}

if (import.meta.main) {
  main(Bun.argv.slice(2)).then((code) => process.exit(code))
}
