type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export interface KaiToolsOpts {
  wsUrl?: string
  httpUrl?: string
  token?: string
  tokenPath?: string
  defaultSession?: string
  httpTimeoutMs?: number
  attachTimeoutMs?: number
  fetchImpl?: typeof fetch
  wsImpl?: typeof WebSocket
}

export class KaiTools {
  static readonly TOOL_NAMES = ['kai_sessions', 'kai_history', 'kai_send', 'kai_schedule'] as const

  static readonly INSTRUCTIONS =
    ' Optional Kai bridge tools are not bundled in this public export. ' +
    'Install or implement a KaiTools adapter to enable Kai session access.'

  constructor(_opts: KaiToolsOpts = {}) {}

  handles(name: string): boolean {
    return (KaiTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'kai_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
