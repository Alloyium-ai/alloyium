type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export interface VaultProvenance {
  source: string
  slug: string
}

export interface VaultToolsOpts {
  sources?: VaultProvenance[]
}

export class VaultTools {
  static readonly TOOL_NAMES = ['vault_howto'] as const

  static readonly INSTRUCTIONS =
    ' Optional vault guidance tools are not bundled in this public export. ' +
    'Install or implement a VaultTools adapter for your own secret-management runbooks.'

  constructor(_opts: VaultToolsOpts = {}) {}

  handles(name: string): boolean {
    return (VaultTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'vault_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
