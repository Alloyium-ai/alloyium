import type { AccessTokenIssuerTools } from './access_token_issuer.ts'

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export interface TaskboardToolsOpts {
  access: AccessTokenIssuerTools
  agentId?: string
  taskboardUrl?: string
  apiToken?: string
  v2ApiToken?: string
  defaultProjectId?: number
  timeoutMs?: number
  nowMs?: () => number
}

export class TaskboardTools {
  static readonly TOOL_NAMES = [
    'a2a_taskboard_projects_list',
    'a2a_taskboard_project_read',
    'a2a_taskboard_task_read',
  ] as const

  static readonly INSTRUCTIONS =
    ' Optional taskboard tools are not bundled in this public export. ' +
    'Install or implement a TaskboardTools adapter for your own task system.'

  constructor(_opts: TaskboardToolsOpts) {}

  handles(name: string): boolean {
    return (TaskboardTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return []
  }

  async callTool(name: string, _args: Record<string, any> = {}): Promise<ToolResult> {
    return {
      content: [{ type: 'text', text: JSON.stringify({ ok: false, error: 'taskboard_tools_not_configured', tool: name }) }],
      isError: true,
    }
  }
}
