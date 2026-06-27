import { describe, expect, test } from 'bun:test'
import { buildCodexWriteAllowlist, buildDefaultCodexCwdRoots, buildDockerContainerSpec, launcherRequestAuthorized, resolveCodexHostHome, resolveLaunchWorkspaceHostPath, resolveLaunchWorktreeCwd } from '../a2a_launcher.ts'

async function withEnv<T>(updates: Record<string, string | undefined>, fn: () => Promise<T>): Promise<T> {
  const old: Record<string, string | undefined> = {}
  for (const k of Object.keys(updates)) old[k] = process.env[k]
  try {
    for (const [k, v] of Object.entries(updates)) {
      if (v === undefined) delete process.env[k]
      else process.env[k] = v
    }
    return await fn()
  } finally {
    for (const [k, v] of Object.entries(old)) {
      if (v === undefined) delete process.env[k]
      else process.env[k] = v
    }
  }
}

async function withLauncherModule<T>(updates: Record<string, string | undefined>, fn: (mod: typeof import('../a2a_launcher.ts')) => T | Promise<T>): Promise<T> {
  return withEnv(updates, async () => {
    const mod = await import(`../a2a_launcher.ts?case=${Date.now()}-${Math.random()}`)
    return fn(mod)
  })
}

describe('a2a launcher docker provider', () => {
  test('derives host codex home from mounted workspace root', () => {
    expect(resolveCodexHostHome(undefined, '/home/bun', '/home/dev/git')).toBe('/home/dev/.codex')
    expect(resolveCodexHostHome(undefined, '/home/bun', '/home/ci/git')).toBe('/home/ci/.codex')
    expect(resolveCodexHostHome('/custom/codex', '/home/bun', '/home/dev/git')).toBe('/custom/codex')
  })

  test('resolves launcher shared workspace paths against the host project dir', () => {
    expect(resolveLaunchWorkspaceHostPath('/data/workspace', '/repo')).toBe('/data/workspace')
    expect(resolveLaunchWorkspaceHostPath('./data/workspace', '/repo/alloyium')).toBe('/repo/alloyium/data/workspace')
    expect(resolveLaunchWorkspaceHostPath('data/workspace', 'alloyium', '/home/dev')).toBe('/home/dev/alloyium/data/workspace')
    expect(resolveLaunchWorkspaceHostPath(undefined, '/repo')).toBeNull()
  })

  test('builds default cwd roots with shared workspace first and legacy root retained', () => {
    expect(buildDefaultCodexCwdRoots('/workspace', '/home/dev/git')).toBe('/workspace,/home/dev/git')
    expect(buildDefaultCodexCwdRoots('/workspace', '/workspace')).toBe('/workspace')
    expect(buildDefaultCodexCwdRoots(null, '/home/dev/git')).toBe('/home/dev/git')
  })

  test('builds a codex peer container with unique identity wiring', () => {
    const spec = buildDockerContainerSpec({
      agentId: 'codex-gw-sub-review-abc123',
      mode: 'shim',
      createdBy: 'codex-gw',
      label: 'review',
      allowWrite: true,
      sandbox: 'danger-full-access',
      effort: 'medium',
      model: 'gpt-5.5-codex',
      turnTimeoutMs: 300000,
    })

    expect(spec.name).toBe('cc-agent-codex-gw-sub-review-abc123')
    expect(spec.body.Image).toBe('alloyium-codex-gw:latest')
    expect(spec.body.Cmd).toEqual(['bun', 'codex_gateway.ts'])
    expect(spec.body.Labels).toMatchObject({
      'ai.alloyium.managed': 'true',
      'ai.alloyium.kind': 'codex',
      'ai.alloyium.agent_id': 'codex-gw-sub-review-abc123',
      'ai.alloyium.created_by': 'codex-gw',
      'ai.alloyium.label': 'review',
    })

    const envList = spec.body.Env as string[]
    expect(envList).toContain('A2A_AGENT_ID=codex-gw-sub-review-abc123')
    expect(envList).toContain('A2A_SIGNING_KEY=/run/secrets/a2a/codex-gw-sub-review-abc123.seed')
    expect(envList).toContain('A2A_PARENT_AGENT_ID=codex-gw')
    expect(envList).toContain('A2A_LAUNCH_AGENT_LABEL=review')
    expect(envList).toContain('CODEX_GW_A2A_TOOLS_MODE=shim')
    expect(envList).toContain('CODEX_GW_A2A_SHIM_BIN=/usr/local/bin/a2a-shim')
    expect(envList).toContain('CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC=20')
    expect(envList).toContain('CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC=45')
    expect(envList).toContain('CODEX_GW_EFFORT=medium')
    expect(envList).toContain('CODEX_GW_MODEL=gpt-5.5-codex')
    expect(envList).toContain('CODEX_GW_TURN_TIMEOUT_MS=300000')
    const hostConfig = spec.body.HostConfig as any
    expect(hostConfig.Binds).toEqual(expect.arrayContaining([
      'alloyium_a2a_secrets:/run/secrets/a2a:ro',
      '/run/a2a-core:/run/a2a-core',
      '/srv/git:/srv/git',
      'alloyium_codex_workspaces:/workspaces',
    ]))
  })

  test('mounts configured shared workspace into launched peers', async () => {
    await withLauncherModule({
      A2A_LAUNCH_PROJECT_DIR: '/opt/alloyium',
      A2A_LAUNCH_WORKSPACE_HOST_PATH: './data/workspace',
      A2A_LAUNCH_WORKSPACE_CONTAINER_PATH: '/workspace',
      CODEX_WORKSPACE_ROOT: '/home/dev/git',
      CODEX_BUILD_CWD_ROOTS: undefined,
    }, (mod) => {
      const spec = mod.buildDockerContainerSpec({
        agentId: 'codex-gw-sub-workspace',
        mode: 'shim',
        createdBy: 'codex-gw',
      })

      const hostConfig = spec.body.HostConfig as any
      expect(hostConfig.Binds).toEqual(expect.arrayContaining([
        '/opt/alloyium/data/workspace:/workspace',
        '/home/dev/git:/home/dev/git',
      ]))

      const envList = spec.body.Env as string[]
      expect(envList).toContain('CODEX_BUILD_CWD_ROOTS=/workspace,/home/dev/git')
    })
  })

  test('keeps explicit cwd roots for launched peers', async () => {
    await withLauncherModule({
      A2A_LAUNCH_PROJECT_DIR: '/opt/alloyium',
      A2A_LAUNCH_WORKSPACE_HOST_PATH: './data/workspace',
      A2A_LAUNCH_WORKSPACE_CONTAINER_PATH: '/workspace',
      CODEX_WORKSPACE_ROOT: '/home/dev/git',
      CODEX_BUILD_CWD_ROOTS: '/custom/root',
    }, (mod) => {
      const spec = mod.buildDockerContainerSpec({
        agentId: 'codex-gw-sub-explicit-roots',
        mode: 'shim',
        createdBy: 'codex-gw',
      })

      const envList = spec.body.Env as string[]
      expect(envList).toContain('CODEX_BUILD_CWD_ROOTS=/custom/root')
    })
  })

  test('defaults launched codex peers to container-external sandbox boundary', () => {
    const spec = buildDockerContainerSpec({
      agentId: 'codex-gw-sub-readonly',
      mode: 'shim',
      createdBy: 'codex-gw',
    })

    const envList = spec.body.Env as string[]
    expect(envList).toContain('CODEX_GW_ALLOW_WRITE=0')
    expect(envList).toContain('CODEX_GW_CODEX_SANDBOX=danger-full-access')
  })

  test('defaults write-enabled peers to workspace-write sandbox for authorized write jobs', () => {
    const spec = buildDockerContainerSpec({
      agentId: 'codex-gw-sub-write',
      mode: 'shim',
      createdBy: 'codex-gw',
      allowWrite: true,
    })

    const envList = spec.body.Env as string[]
    expect(envList).toContain('CODEX_GW_ALLOW_WRITE=1')
    expect(envList).toContain('CODEX_GW_CODEX_SANDBOX=danger-full-access')
    expect(envList).toContain('CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX=workspace-write')
  })

  test('maps absolute worktree requests to a launched worker default cwd', () => {
    expect(resolveLaunchWorktreeCwd('/host/work/repo@develop', '/host/work', '/workspace', '/srv/git')).toBe('/workspace/repo')
    expect(resolveLaunchWorktreeCwd('/srv/git/repo@main', '/host/work', '/workspace', '/srv/git')).toBe('/srv/git/repo')
    expect(resolveLaunchWorktreeCwd('Alloyium-ai/alloyium@develop', '/host/work', '/workspace', '/srv/git')).toBeUndefined()
  })

  test('carries worktree, role-scope metadata, and requester-aware write allowlist into codex peers', async () => {
    await withEnv({
      CODEX_GW_WRITE_ALLOWLIST: undefined,
      A2A_LAUNCH_WRITE_ALLOWLIST_EXTRA: 'codex-rt-gw-2',
    }, async () => {
      const spec = buildDockerContainerSpec({
        agentId: 'codex-gw-sub-write-scoped',
        mode: 'shim',
        createdBy: 'codex-gw',
        allowWrite: true,
        worktree: '/srv/git/alloyium@develop',
        roleScopes: ['forgejo:repo:Alloyium-ai/alloyium:pr:create'],
        writeRequesters: ['a2a-portal'],
      })

      const envList = spec.body.Env as string[]
      expect(envList).toContain('A2A_LAUNCH_WORKTREE=/srv/git/alloyium@develop')
      expect(envList).toContain('CODEX_GW_DEFAULT_CWD=/srv/git/alloyium')
      expect(envList).toContain('A2A_REQUESTED_ROLE_SCOPES=["forgejo:repo:Alloyium-ai/alloyium:pr:create"]')
      expect(envList).toContain('CODEX_GW_WRITE_ALLOWLIST=dev-pm,codex-rt-gw-2,codex-gw,agent-1,a2a-portal')
    })
  })

  test('builds a bounded write allowlist from base, extras, creator, and requested identities', () => {
    expect(buildCodexWriteAllowlist('codex-gw', true, ['a2a-portal'], {
      CODEX_GW_WRITE_ALLOWLIST: 'dev-pm,agent-1',
      A2A_LAUNCH_WRITE_ALLOWLIST_EXTRA: 'codex-rt-gw-2',
    })).toBe('dev-pm,agent-1,codex-rt-gw-2,codex-gw,a2a-portal')
  })

  test('requires a bearer token for protected launcher requests', () => {
    const req = (auth?: string) => new Request('http://launcher/v1/agents/codex', {
      method: 'POST',
      headers: auth ? { authorization: auth } : undefined,
    })

    expect(launcherRequestAuthorized(req(), '')).toBe(false)
    expect(launcherRequestAuthorized(req('Bearer secret-token'), '')).toBe(false)
    expect(launcherRequestAuthorized(req('secret-token'), 'secret-token')).toBe(false)
    expect(launcherRequestAuthorized(req('Bearer secret'), 'secret-token')).toBe(false)
    expect(launcherRequestAuthorized(req('Bearer secret-token-extra'), 'secret-token')).toBe(false)
    expect(launcherRequestAuthorized(req('Bearer wrong'), 'secret-token')).toBe(false)
    expect(launcherRequestAuthorized(req('Bearer secret-token'), 'secret-token')).toBe(true)
  })
})
