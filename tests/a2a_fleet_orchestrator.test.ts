import { describe, expect, test } from 'bun:test'
import {
  buildFleetRunPlan,
  buildCanaryFleetSpec,
  classifyFleetJobReply,
  fleetExecutionTopics,
  parseLegacyFleetSpec,
  topicToken,
  validateFleetSpec,
  type FleetSpec,
} from '../a2a_fleet_orchestrator.ts'

const LEGACY_SPEC = `# Research analysis fleet - 2026-06-19
FLEET research-2026-06-19
BASE /srv/git/research-stack@feat/topic-analysis
research-manager /goal You are research-manager for the 2026-06-19 batch (1 task). When all 1 land, CONSOLIDATE every report into reports/2026-06-19/ + write _INDEX.md.
research-alpha /goal You are the research agent for topic alpha on 2026-06-19. Produce the deep-research report. Write reports/2026-06-19/alpha_deep-research.md in your worktree.
`

describe('A2A Fleet v2 legacy conversion', () => {
  test('converts the legacy text fleetspec into a valid v2 codex spec', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC, { createdAt: '2026-06-19T11:20:00Z' })
    expect(spec).toMatchObject({
      schema: 'a2a.fleet.spec.v2',
      kind: 'a2a-fleet',
      fleet_id: 'research-2026-06-19',
      base: {
        repo: '/srv/git/research-stack',
        ref: 'feat/topic-analysis',
        cwd: '/srv/git/research-stack',
      },
      defaults: {
        runtime: 'codex',
        provider: 'tmux-codex',
        sandbox: 'read-only',
        approval_policy: 'never',
      },
    })
    expect(spec.agents).toHaveLength(2)
    expect(spec.agents[0]).toMatchObject({
      id: 'research-manager',
      role: 'manager',
      runtime: 'codex',
      provider: 'tmux-codex',
      thread_key: 'fleet:research-2026-06-19:research-manager',
    })
    expect(spec.agents[1]).toMatchObject({
      id: 'research-alpha',
      role: 'worker',
      metadata: {
        date: '2026-06-19',
      },
      expects: {
        artifacts: ['reports/2026-06-19/alpha_deep-research.md'],
      },
    })
    expect(validateFleetSpec(spec).ok).toBe(true)
  })

  test('can convert workers to claude-live with the tmux Claude provider', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC, { runtime: 'claude-live' })
    expect(spec.defaults.runtime).toBe('claude-live')
    expect(spec.defaults.provider).toBe('tmux-claude-live')
    expect(spec.agents.every((agent) => agent.runtime === 'claude-live')).toBe(true)
    expect(validateFleetSpec(spec).ok).toBe(true)
  })

  test('can convert workers to workspace-write with isolated worktree cwd', () => {
    const prevRoot = process.env.WORKER_WORKTREE_ROOT
    process.env.WORKER_WORKTREE_ROOT = '/tmp/a2a-test-agents'
    try {
      const spec = parseLegacyFleetSpec(LEGACY_SPEC, { sandbox: 'workspace-write' })
      expect(spec.defaults.sandbox).toBe('workspace-write')
      expect(validateFleetSpec(spec).ok).toBe(true)

      const plan = buildFleetRunPlan(spec)
      expect(plan.agents[1].dispatch.envelope).toMatchObject({
        sandbox: 'workspace-write',
        cwd: '/tmp/a2a-test-agents/research-alpha/wt',
      })
    } finally {
      if (prevRoot === undefined) delete process.env.WORKER_WORKTREE_ROOT
      else process.env.WORKER_WORKTREE_ROOT = prevRoot
    }
  })
})

describe('A2A Fleet v2 topic names', () => {
  test('builds valid A2A topic tokens from dotted or mixed input', () => {
    expect(topicToken('fleet.research-2026-06-19.status')).toBe('fleet-research-2026-06-19-status')
    expect(topicToken('Fleet', 'Run_2026_06_19', 'Worker.Stream')).toBe('fleet-run-2026-06-19-worker-stream')
  })
})

describe('A2A Fleet v2 validation', () => {
  test('rejects invalid runtime/provider combinations and /tmp cwd', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC) as FleetSpec
    spec.base.cwd = '/tmp'
    spec.defaults.provider = 'tmux-claude-live'
    const result = validateFleetSpec(spec)
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error('expected validation failure')
    expect(result.issues.map((issue) => issue.path)).toEqual(expect.arrayContaining([
      'base.cwd',
      'defaults.provider',
      'agents[0].cwd',
      'agents[1].cwd',
    ]))
  })

  test('rejects duplicate agent ids', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC) as FleetSpec
    spec.agents[1].id = 'research-manager'
    const result = validateFleetSpec(spec)
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error('expected validation failure')
    expect(result.issues.some((issue) => issue.message.includes('duplicate agent id research-manager'))).toBe(true)
  })

  test('rejects codex realtime-session ids as fleet job targets', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC) as FleetSpec
    spec.agents[1].id = 'codex-rt-gw-2'
    spec.agents[1].thread_key = 'fleet:research-2026-06-19:codex-rt-gw-2'
    const result = validateFleetSpec(spec)
    expect(result.ok).toBe(false)
    if (result.ok) throw new Error('expected validation failure')
    expect(result.issues.some((issue) => issue.path === 'agents[1].id' && issue.message.includes('realtime-session'))).toBe(true)
  })
})

describe('A2A Fleet v2 dry-run planning', () => {
  test('builds codex launch and codex.job.request.v1 dispatch plan', () => {
    const validation = validateFleetSpec(parseLegacyFleetSpec(LEGACY_SPEC))
    if (!validation.ok) throw new Error('expected valid spec')
    const plan = buildFleetRunPlan(validation.spec)
    expect(plan).toMatchObject({
      schema: 'a2a.fleet.run_plan.v1',
      fleet_id: 'research-2026-06-19',
      dry_run: true,
      summary: {
        agents: 2,
        codex: 2,
        claude_live: 0,
        providers: { 'tmux-codex': 2 },
      },
    })
    expect(plan.agents[1].launch).toMatchObject({ provider: 'tmux-codex', mode: 'shim' })
    expect(plan.agents[1].launch.command).toEqual([
      'bash',
      'a2a-launch.sh',
      'research-alpha',
      'codex',
      '--shim',
      '--worktree',
      '/srv/git/research-stack@feat/topic-analysis',
    ])
    expect(plan.agents[1].dispatch.schema).toBe('codex.job.request.v1')
    expect(plan.agents[1].dispatch.envelope).toMatchObject({
      schema: 'codex.job.request.v1',
      job_id: 'fleet-research-2026-06-19-research-alpha-001',
      thread_key: 'fleet:research-2026-06-19:research-alpha',
      cwd: '/srv/git/research-stack',
      sandbox: 'read-only',
      approval_policy: 'never',
      stream_topic: 'fleet-research-2026-06-19-research-alpha-stream',
    })
    expect(fleetExecutionTopics(validation.spec, plan)).toEqual([
      'fleet-research-2026-06-19-status',
      'fleet-research-2026-06-19-results',
      'fleet-research-2026-06-19-research-manager-stream',
      'fleet-research-2026-06-19-research-alpha-stream',
    ])
  })

  test('builds claude-live launch and fleet.task.assigned.v1 dispatch plan', () => {
    const validation = validateFleetSpec(parseLegacyFleetSpec(LEGACY_SPEC, { runtime: 'claude-live' }))
    if (!validation.ok) throw new Error('expected valid spec')
    const plan = buildFleetRunPlan(validation.spec)
    expect(plan.summary).toMatchObject({
      agents: 2,
      codex: 0,
      claude_live: 2,
      providers: { 'tmux-claude-live': 2 },
    })
    expect(plan.agents[1].launch).toMatchObject({ provider: 'tmux-claude-live', mode: 'shim' })
    expect(plan.agents[1].launch.command).toEqual([
      'bash',
      'a2a-launch.sh',
      'research-alpha',
      'claude',
      '--shim',
      '--worktree',
      '/srv/git/research-stack@feat/topic-analysis',
    ])
    expect(plan.agents[1].dispatch.schema).toBe('fleet.task.assigned.v1')
    expect(plan.agents[1].dispatch.envelope).toMatchObject({
      schema: 'fleet.task.assigned.v1',
      fleet_id: 'research-2026-06-19',
      agent_id: 'research-alpha',
      job_id: 'fleet-research-2026-06-19-research-alpha-001',
      thread_key: 'fleet:research-2026-06-19:research-alpha',
      cwd: '/srv/git/research-stack',
      sandbox: 'read-only',
      approval_policy: 'never',
    })
  })

  test('includes launcher policy in docker-codex dry-run plans', () => {
    const spec = parseLegacyFleetSpec(LEGACY_SPEC, { provider: 'docker-codex', sandbox: 'workspace-write' }) as FleetSpec
    spec.defaults.role_scopes = ['forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*']
    const validation = validateFleetSpec(spec)
    if (!validation.ok) throw new Error('expected valid spec')

    const plan = buildFleetRunPlan(validation.spec)
    expect(plan.agents[1].launch.request?.body).toMatchObject({
      agent_id: 'research-alpha',
      worktree: '/srv/git/research-stack@feat/topic-analysis',
      job_id: 'fleet-research-2026-06-19-research-alpha-001',
      base_ref: 'feat/topic-analysis',
      target_branch: 'codex/research-2026-06-19-research-alpha',
      cleanup_policy: 'preserve',
      policy: {
        allow_write: true,
        sandbox: 'workspace-write',
        effort: 'xhigh',
        role_scopes: ['forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*'],
      },
    })
  })
})

describe('A2A Fleet v2 canary helpers', () => {
  test('builds a short smoke-safe two-agent canary spec with unique ids', () => {
    const validation = validateFleetSpec(parseLegacyFleetSpec(LEGACY_SPEC))
    if (!validation.ok) throw new Error('expected valid spec')
    const canary = buildCanaryFleetSpec(validation.spec, { suffix: 'abc123', smoke: true })

    expect(canary.fleet_id).toBe('research-2026-06-19-canary-abc123')
    expect(canary.agents).toHaveLength(2)
    expect(canary.agents[0]).toMatchObject({ id: 'mgr-abc123', role: 'manager' })
    expect(canary.agents[1]).toMatchObject({ id: 'wkr-abc123', role: 'worker' })
    expect(canary.defaults.status_topic).toBe('fleet-research-2026-06-19-canary-abc123-status')
    expect(canary.defaults.result_topic).toBe('fleet-research-2026-06-19-canary-abc123-results')
    expect(canary.agents[0].task).toContain('smoke test only')
    expect(canary.agents[0].task).toContain('Do not modify files')
    expect(canary.agents[1].task).toContain('A2A Fleet v2 canary')
    expect(validateFleetSpec(canary).ok).toBe(true)
  })

  test('classifies codex and fleet task replies for a pending canary job', () => {
    const ctx = { jobId: 'fleet-research-canary-wkr-abc123-001', target: 'wkr-abc123', selfId: 'fleet-orchestrator', requestId: 'msg-1' }
    const attrs = { feed: 'a2a', kind: 'direct', type: 'reply', from: 'wkr-abc123', to: 'fleet-orchestrator', corr: 'msg-1' }

    expect(classifyFleetJobReply(JSON.stringify({ schema: 'codex.job.accepted.v1', job_id: ctx.jobId }), attrs, ctx))
      .toMatchObject({ kind: 'accepted', job_id: ctx.jobId })
    expect(classifyFleetJobReply(JSON.stringify({ schema: 'codex.job.completed.v1', job_id: ctx.jobId, status: 'completed', output: 'ok' }), attrs, ctx))
      .toMatchObject({ kind: 'completed', job_id: ctx.jobId, output: 'ok' })
    expect(classifyFleetJobReply(JSON.stringify({ schema: 'fleet.task.completed.v1', job_id: ctx.jobId, summary: 'done' }), attrs, ctx))
      .toMatchObject({ kind: 'completed', output: 'done' })
    expect(classifyFleetJobReply(JSON.stringify({ schema: 'codex.job.completed.v1', job_id: ctx.jobId }), { ...attrs, corr: 'wrong' }, ctx))
      .toMatchObject({ kind: 'ignore', reason: 'wrong_corr' })
  })
})
