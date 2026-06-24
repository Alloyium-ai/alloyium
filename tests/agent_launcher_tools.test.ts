import { describe, expect, test } from 'bun:test'
import { AgentLauncherTools, allocateChildAgentId, type SpawnImpl } from '../agent_launcher_tools.ts'

const payload = (r: any) => JSON.parse(r.content[0].text)

describe('AgentLauncherTools', () => {
  test('lists the launcher only for authorized identities', () => {
    const allowed = new AgentLauncherTools({ agentId: 'codex-gw', allowedAgentIds: ['codex-gw'], launcherPath: '/x/a2a-launch.sh' })
    const denied = new AgentLauncherTools({ agentId: 'worker-1', allowedAgentIds: ['codex-gw'], launcherPath: '/x/a2a-launch.sh' })

    expect(allowed.listTools().map((t) => t.name)).toEqual(['a2a_launch_codex_agent'])
    expect(denied.listTools()).toEqual([])
  })

  test('dry run returns the exact codex shim launcher command', async () => {
    const tools = new AgentLauncherTools({ agentId: 'codex-gw', allowedAgentIds: ['codex-gw'], launcherPath: '/srv/cc/a2a-launch.sh', launcherUrl: '' })

    const out = payload(await tools.callTool('a2a_launch_codex_agent', { agent_id: 'codex-worker-1', dry_run: true }))

    expect(out).toMatchObject({ ok: true, dry_run: true, agent_id: 'codex-worker-1', kind: 'codex', mode: 'shim' })
    expect(out.cmd).toEqual(['bash', '/srv/cc/a2a-launch.sh', 'codex-worker-1', 'codex', '--shim'])
  })

  test('omitted agent_id allocates a unique child identity', async () => {
    const tools = new AgentLauncherTools({
      agentId: 'codex-gw',
      allowedAgentIds: ['codex-gw'],
      launcherPath: '/srv/cc/a2a-launch.sh',
      launcherUrl: '',
      allocateAgentId: ({ parentAgentId, label }) => `${parentAgentId}-sub-${label}`,
    })

    const out = payload(await tools.callTool('a2a_launch_codex_agent', { label: 'review', dry_run: true }))

    expect(out).toMatchObject({
      ok: true,
      dry_run: true,
      agent_id: 'codex-gw-sub-review',
      parent_agent_id: 'codex-gw',
      kind: 'codex',
      mode: 'shim',
    })
    expect(out.cmd).toEqual(['bash', '/srv/cc/a2a-launch.sh', 'codex-gw-sub-review', 'codex', '--shim'])
  })

  test('spawn path passes mode env and returns session details', async () => {
    const calls: any[] = []
    const spawnImpl: SpawnImpl = async (cmd, opts) => {
      calls.push({ cmd, opts })
      return { exitCode: 0, stdout: 'launched\n', stderr: '' }
    }
    const tools = new AgentLauncherTools({
      agentId: 'codex-gw',
      allowedAgentIds: ['codex-gw'],
      launcherPath: '/srv/cc/a2a-launch.sh',
      launcherUrl: '',
      spawnImpl,
      launchEnv: { NATS_URL: 'nats://nats:4222' },
    })

    const out = payload(await tools.callTool('a2a_launch_codex_agent', {
      agent_id: 'codex-worker-2',
      mode: 'webhook',
      worktree: '/repo@main',
    }))

    expect(out).toMatchObject({ ok: true, agent_id: 'codex-worker-2', parent_agent_id: 'codex-gw', kind: 'codex', mode: 'webhook', session: 'a2a-codex-worker-2' })
    expect(calls[0].cmd).toEqual(['bash', '/srv/cc/a2a-launch.sh', 'codex-worker-2', 'codex', '--webhook', '--worktree', '/repo@main'])
    expect(calls[0].opts.env).toMatchObject({
      NATS_URL: 'nats://nats:4222',
      A2A_MODE: 'webhook',
      A2A_PARENT_AGENT_ID: 'codex-gw',
      A2A_LAUNCH_AGENT_LABEL: undefined,
      CODEX_GW_A2A_TOOLS_MODE: 'webhook',
    })
  })

  test('remote launcher backend posts an authorized launch request', async () => {
    const calls: any[] = []
    const oldToken = process.env.A2A_LAUNCHER_TOKEN
    process.env.A2A_LAUNCHER_TOKEN = 'test-launch-token'
    const fetchImpl = async (url: string, init: RequestInit): Promise<Response> => {
      calls.push({ url, init, body: JSON.parse(String(init.body)) })
      return Response.json({
        ok: true,
        agent_id: 'codex-worker-remote',
        kind: 'codex',
        mode: 'shim',
        provider: 'docker',
        runtime_id: 'cc-agent-codex-worker-remote',
        status: 'starting',
      })
    }
    try {
      const tools = new AgentLauncherTools({
        agentId: 'codex-gw',
        allowedAgentIds: ['codex-gw'],
        launcherPath: '/srv/cc/a2a-launch.sh',
        launcherUrl: 'http://a2a-launcher:8910/',
        fetchImpl,
      })

      const out = payload(await tools.callTool('a2a_launch_codex_agent', {
        agent_id: 'codex-worker-remote',
        label: 'remote smoke',
        dry_run: true,
      }))

      expect(out).toMatchObject({
        ok: true,
        agent_id: 'codex-worker-remote',
        parent_agent_id: 'codex-gw',
        provider: 'docker',
        runtime_id: 'cc-agent-codex-worker-remote',
        status: 'starting',
      })
      expect(calls[0].url).toBe('http://a2a-launcher:8910/v1/agents/codex')
      expect(calls[0].init.headers).toMatchObject({ authorization: 'Bearer test-launch-token' })
      expect(calls[0].body).toMatchObject({
        agent_id: 'codex-worker-remote',
        label: 'remote smoke',
        mode: 'shim',
        created_by: 'codex-gw',
        dry_run: true,
      })
    } finally {
      if (oldToken === undefined) delete process.env.A2A_LAUNCHER_TOKEN
      else process.env.A2A_LAUNCHER_TOKEN = oldToken
    }
  })

  test('rejects unauthorized callers and invalid ids', async () => {
    const denied = new AgentLauncherTools({ agentId: 'worker-1', allowedAgentIds: ['codex-gw'], launcherPath: '/x/a2a-launch.sh' })
    expect(payload(await denied.callTool('a2a_launch_codex_agent', { agent_id: 'codex-worker-1' }))).toMatchObject({ ok: false, error: 'unauthorized' })

    const allowed = new AgentLauncherTools({ agentId: 'codex-gw', allowedAgentIds: ['codex-gw'], launcherPath: '/x/a2a-launch.sh' })
    expect(payload(await allowed.callTool('a2a_launch_codex_agent', { agent_id: 'Bad Id' }))).toEqual({ ok: false, error: 'bad_agent_id' })
    expect(payload(await allowed.callTool('a2a_launch_codex_agent', { agent_id: 'codex-gw' }))).toEqual({ ok: false, error: 'self_launch_refused' })
    expect(payload(await allowed.callTool('a2a_launch_codex_agent', { label: 'bad\nlabel' }))).toEqual({ ok: false, error: 'bad_label' })
  })

  test('allocateChildAgentId returns bounded valid ids', () => {
    const id = allocateChildAgentId('very-long-parent-agent-id-that-needs-to-be-shortened', 'Review Build!', {
      now: 12345,
      randomHex: 'abcdef012345',
    })

    expect(id).toMatch(/^[a-z0-9-]{1,64}$/)
    expect(id.length).toBeLessThanOrEqual(64)
    expect(id).toContain('sub-review-build-9ix-abcdef01')
  })
})
