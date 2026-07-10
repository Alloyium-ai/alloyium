import type { AccessTokenIssuerTools } from './access_token_issuer.ts'

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export interface ForgejoToolsOpts {
  access: AccessTokenIssuerTools
  agentId?: string
  forgejoUrl?: string
  apiToken?: string
  tokenPath?: string
  timeoutMs?: number
  fetchImpl?: typeof fetch
}

export class ForgejoTools {
  static readonly TOOL_NAMES = ['a2a_forgejo_create_repo'] as const

  static readonly INSTRUCTIONS =
    ' Optional Forgejo tools are not bundled in this public export. ' +
    'Install or implement a ForgejoTools adapter for repository automation.'

  constructor(_opts: ForgejoToolsOpts) {}

  handles(name: string): boolean {
    return (ForgejoTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'forgejo_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
