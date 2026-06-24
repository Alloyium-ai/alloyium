import { describe, expect, test } from 'bun:test'
import { CODEX_DETACHED_NO_INTERACTIVE_PROMPT_DIRECTIVE, buildCodexA2AToolsConfigArgs, buildCodexAgentPrompt, codexA2AToolsEnabled, codexA2AToolsMode } from '../codex_agent_tools.ts'

function configMap(args: string[]): Map<string, string> {
  const out = new Map<string, string>()
  for (let i = 0; i < args.length; i += 2) {
    expect(args[i]).toBe('-c')
    const raw = args[i + 1]
    const idx = raw.indexOf('=')
    expect(idx).toBeGreaterThan(0)
    out.set(raw.slice(0, idx), raw.slice(idx + 1))
  }
  return out
}

describe('codex A2A tool config', () => {
  test('builds a tool-only MCP server for codex app-server', () => {
    const cfg = configMap(buildCodexA2AToolsConfigArgs({
      channelsDir: '/srv/alloyium/',
      agentId: 'codex-gw',
      signingKeyPath: '/run/secrets/a2a/codex-gw.seed',
      transportAuth: 'none',
      natsUrl: 'nats://nats:4222',
      redisUrl: 'redis://redis:6379',
      brainUrl: 'http://brain:8787',
      kaiHttpUrl: 'http://kai:18789',
      kaiWsUrl: 'ws://kai:18789/ws',
      kaiTokenPath: '/run/secrets/kai-token',
      maxSendBytes: 8192,
      inheritEnvVars: ['KAI_TOKEN'],
    }))

    expect(cfg.get('mcp_servers.a2a_tools.command')).toBe('"bun"')
    expect(cfg.get('mcp_servers.a2a_tools.args')).toBe('["/srv/alloyium/webhook.ts"]')
    expect(cfg.get('mcp_servers.a2a_tools.cwd')).toBe('"/srv/alloyium"')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_TOOL_ONLY')).toBe('"1"')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_AGENT_ID')).toBe('"codex-gw"')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_SIGNING_KEY')).toBe('"/run/secrets/a2a/codex-gw.seed"')
    expect(cfg.get('mcp_servers.a2a_tools.env.BRAIN_URL')).toBe('"http://brain:8787"')
    expect(cfg.get('mcp_servers.a2a_tools.env.KAI_WS_URL')).toBe('"ws://kai:18789/ws"')
    expect(cfg.get('mcp_servers.a2a_tools.env_vars')).toBe('["KAI_TOKEN"]')
    expect(cfg.get('tools.request_user_input')).toBe('false')
  })

  test('does not embed token values in process args', () => {
    const args = buildCodexA2AToolsConfigArgs({
      channelsDir: '/srv/cc',
      agentId: 'agent-1',
      inheritEnvVars: ['KAI_TOKEN'],
    }).join('\n')

    expect(args).toContain('mcp_servers.a2a_tools.env_vars=["KAI_TOKEN"]')
    expect(args).not.toContain('mcp_servers.a2a_tools.env.KAI_TOKEN')
  })

  test('can be disabled explicitly', () => {
    expect(codexA2AToolsEnabled({ CODEX_GW_ENABLE_A2A_TOOLS: '0' })).toBe(false)
    expect(buildCodexA2AToolsConfigArgs({ enabled: false, channelsDir: '/srv/cc', agentId: 'agent-1' })).toEqual([])
  })

  test('builds shim MCP config for shared-core mode', () => {
    const cfg = configMap(buildCodexA2AToolsConfigArgs({
      channelsDir: '/srv/alloyium',
      toolsMode: 'shim',
      shimCommand: '/usr/local/bin/a2a-shim',
      coreSock: '/run/a2a-core/core.sock',
      agentId: 'codex-gw',
      signingKeyPath: '/run/secrets/a2a/codex-gw.seed',
      required: true,
    }))

    expect(cfg.get('mcp_servers.a2a_tools.command')).toBe('"/usr/local/bin/a2a-shim"')
    expect(cfg.get('mcp_servers.a2a_tools.args')).toBe('[]')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_TOOL_ONLY')).toBe('"1"')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_SHIM_TOOL_ONLY')).toBe('"1"')
    expect(cfg.get('mcp_servers.a2a_tools.env.A2A_CORE_SOCK')).toBe('"/run/a2a-core/core.sock"')
    expect(cfg.get('mcp_servers.a2a_tools.required')).toBe('true')
  })

  test('allows app-server MCP startup and tool watchdogs to be tuned', () => {
    const cfg = configMap(buildCodexA2AToolsConfigArgs({
      channelsDir: '/srv/alloyium',
      agentId: 'codex-worker',
      startupTimeoutSec: 7,
      toolTimeoutSec: 31,
    }))

    expect(cfg.get('mcp_servers.a2a_tools.startup_timeout_sec')).toBe('7')
    expect(cfg.get('mcp_servers.a2a_tools.tool_timeout_sec')).toBe('31')
  })

  test('mode defaults to webhook unless explicitly shim', () => {
    expect(codexA2AToolsMode({})).toBe('webhook')
    expect(codexA2AToolsMode({ CODEX_GW_A2A_TOOLS_MODE: 'shim' })).toBe('shim')
    expect(codexA2AToolsMode({ CODEX_AGENT_A2A_TOOLS_MODE: 'webhook' })).toBe('webhook')
    expect(codexA2AToolsMode({ A2A_MCP_MODE: 'SHIM' })).toBe('shim')
  })
})

describe('codex A2A agent prompt', () => {
  test('adds first-class agent and self-learning guidance', () => {
    const out = buildCodexAgentPrompt('Do the work.', { agentId: 'codex-gw', requester: 'agent-1', jobId: 'job-7', streamTopic: 'job-stream-7' })

    expect(out).toContain('agent=codex-gw requester=agent-1 job_id=job-7 stream_topic=job-stream-7')
    expect(out).toContain('a2a_recall')
    expect(out).toContain('a2a_remember or a2a_skill_save')
    expect(out).toContain(CODEX_DETACHED_NO_INTERACTIVE_PROMPT_DIRECTIVE)
    expect(out).toContain('Never use request_user_input')
    expect(out).toContain('agent-1 is the runtime decision and escalation owner')
    expect(out).toContain('multi_agent_v1.spawn_agent')
    expect(out).toContain('not persistent A2A peers')
    expect(out).toContain('a2a_launch_codex_agent')
    expect(out).toContain('omit agent_id unless a specific id is required')
    expect(out).toContain('launch broker codex-gw')
    expect(out).toContain('a2a.launch.request.v1')
    expect(out).toContain('codex.job.request.v1')
    expect(out).toContain('Do the work.')
  })

  test('leaves prompts byte-identical when tools are disabled', () => {
    expect(buildCodexAgentPrompt('plain', { agentId: 'codex-gw', toolsEnabled: false })).toBe('plain')
  })
})
