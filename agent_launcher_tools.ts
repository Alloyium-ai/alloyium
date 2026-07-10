type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export type LaunchMode = 'shim' | 'webhook'

export interface AgentLauncherToolsOpts {
  agentId: string
  allowedAgentIds?: string[]
  launcherPath?: string
  defaultMode?: LaunchMode
  spawnImpl?: SpawnImpl
  launcherUrl?: string
  fetchImpl?: FetchImpl
  launchEnv?: Record<string, string | undefined>
  timeoutMs?: number
  allocateAgentId?: (ctx: { parentAgentId: string; label?: string }) => string
}

export interface SpawnResult {
  exitCode: number | null
  stdout: string
  stderr: string
  timedOut?: boolean
}

export type SpawnImpl = (cmd: string[], opts: { env: Record<string, string | undefined>; timeoutMs: number }) => Promise<SpawnResult>
export type FetchImpl = (url: string, init: RequestInit) => Promise<Response>

export class AgentLauncherTools {
  static readonly TOOL_NAMES = ['a2a_launch_codex_agent'] as const

  static readonly INSTRUCTIONS =
    ' Optional peer-launch tools are not bundled in this public export. ' +
    'Install or implement an AgentLauncherTools adapter for your own runtime.'

  constructor(_opts: AgentLauncherToolsOpts) {}

  handles(name: string): boolean {
    return (AgentLauncherTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'launcher_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
