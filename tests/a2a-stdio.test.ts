// A2A stdio/boot tests — gating behavior over the real MCP stdio transport.
// Reuses the dead-endpoint boot pattern from stdio.test.ts (no live infra).
import { test, expect, describe } from 'bun:test'

const REPO = new URL('..', import.meta.url).pathname
const init = () => JSON.stringify({
  jsonrpc: '2.0', id: 1, method: 'initialize',
  params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '1' } },
})
const toolsList = () => JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} })

async function boot(extraEnv: Record<string, string | undefined>) {
  const env: Record<string, string> = {
    ...process.env as Record<string, string>,
    NATS_URL: 'nats://127.0.0.1:65010',
    REDIS_URL: 'redis://127.0.0.1:65011',
    HTTP_PORT: '0', // OS-assigned free port — avoids collisions when stdio test files run in parallel
    A2A_INBOX_DB: ':memory:',
    LOG_LEVEL: 'info',
  }
  delete env.A2A_ENABLED
  for (const [k, v] of Object.entries(extraEnv)) { if (v === undefined) delete env[k]; else env[k] = v }
  const proc = Bun.spawn([process.execPath, 'webhook.ts'], { cwd: REPO, env, stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' })
  proc.stdin.write(init() + '\n'); proc.stdin.flush()
  await Bun.sleep(400)
  proc.stdin.write(toolsList() + '\n'); proc.stdin.flush()
  await Bun.sleep(1200)
  proc.kill()
  const stdout = await new Response(proc.stdout).text()
  const lines = stdout.split('\n').map((l) => l.trim()).filter(Boolean)
  for (const l of lines) { const m = JSON.parse(l); expect(m.jsonrpc).toBe('2.0') } // stdout purity
  const msgs = lines.map((l) => JSON.parse(l))
  return { initResp: msgs.find((m) => m.id === 1), toolsResp: msgs.find((m) => m.id === 2) }
}

describe('T-S8 — A2A disabled ⇒ wire-identical to the read-only bridge', () => {
  test('initialize advertises no tools capability and the instructions carry no A2A text', async () => {
    const { initResp } = await boot({ A2A_ENABLED: undefined })
    expect(initResp?.result).toBeDefined()
    expect(initResp.result.capabilities?.tools).toBeUndefined()
    expect(initResp.result.instructions).not.toContain('feed="a2a"')
    expect(initResp.result.instructions).toContain('ADVISORY intel only')
  }, 10_000)

  test('tools/list returns an error (no tools handler registered) when disabled', async () => {
    const { toolsResp } = await boot({ A2A_ENABLED: undefined })
    expect(toolsResp?.error).toBeDefined() // method not found — capability absent
  }, 10_000)
})

describe('A2A enabled (dev bypass) ⇒ a2a + brain + kai + vault tools, stdout stays pure JSON-RPC', () => {
  test('tools/list returns the A2A tools plus the agent-brain memory, kai bridge, and vault guidance tools, and instructions carry the A2A + brain + kai + vault text', async () => {
    const { initResp, toolsResp } = await boot({ A2A_ENABLED: '1', A2A_DEV_NO_AUTH: '1', A2A_AGENT_ID: 'selftest-stdio' })
    expect(initResp.result.capabilities?.tools).toBeDefined()
    expect(initResp.result.instructions).toContain('feed="a2a"')
    // brain_tools.ts appends its own instruction sentence alongside A2AChannel's.
    expect(initResp.result.instructions).toContain('agent-brain memory tools')
    // kai_tools.ts appends its bridge instruction sentence too.
    expect(initResp.result.instructions).toContain('agent-kai bridge tools')
    // vault_tools.ts appends its guidance instruction sentence too.
    expect(initResp.result.instructions).toContain('vault guidance tool')
    const names = (toolsResp?.result?.tools ?? []).map((t: any) => t.name).sort()
    expect(names).toEqual([
      // a2a messaging tools (a2a-channel.ts)
      'a2a-inbox-messages', 'a2a_join_topic', 'a2a_leave_topic', 'a2a_peers', 'a2a_send',
      // agent-brain memory/RAG/skillpack tools (brain_tools.ts)
      'a2a_brain_get', 'a2a_recall', 'a2a_remember', 'a2a_skill_get', 'a2a_skill_save',
      // agent-kai bridge tools (kai_tools.ts)
      'kai_history', 'kai_schedule', 'kai_send', 'kai_sessions',
      // agent-vault guidance tool (vault_tools.ts)
      'vault_howto',
    ].sort())
  }, 10_000)
})
