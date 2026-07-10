type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export interface BrainToolsOpts {
  brainUrl?: string
  gbrainUrl?: string
  apiToken?: string
  apiTokenFile?: string
  source?: string
  skillSource?: string
  environment?: string
  environments?: string[]
  readSources?: string[]
  writeSources?: string[]
  authority?: 'authoritative' | 'verified' | 'advisory' | 'unverified'
  timeoutMs?: number
  fetchImpl?: typeof fetch
}

export class BrainTools {
  static readonly TOOL_NAMES = [
    'a2a_remember',
    'a2a_recall',
    'a2a_brain_get',
    'a2a_skill_save',
    'a2a_skill_get',
  ] as const

  static readonly INSTRUCTIONS =
    ' Optional agent-brain memory tools are not bundled in this public export. ' +
    'Install or implement a BrainTools adapter to enable memory and skillpack calls.'

  constructor(_opts: BrainToolsOpts = {}) {}

  handles(name: string): boolean {
    return (BrainTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'brain_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
