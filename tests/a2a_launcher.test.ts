import { describe, expect, test } from 'bun:test'
import { buildDockerContainerSpec, launcherRequestAuthorized, resolveCodexHostHome } from '../a2a_launcher.ts'

describe('a2a launcher docker provider', () => {
  test('derives host codex home from mounted workspace root', () => {
    expect(resolveCodexHostHome(undefined, '/home/bun', '/home/dev/git')).toBe('/home/dev/.codex')
    expect(resolveCodexHostHome(undefined, '/home/bun', '/home/ci/git')).toBe('/home/ci/.codex')
    expect(resolveCodexHostHome('/custom/codex', '/home/bun', '/home/dev/git')).toBe('/custom/codex')
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
