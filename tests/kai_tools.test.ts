// kai_tools integration test — runs against the LIVE agent-kai daemon (kaid,
// host :18789, WS /ws, REST /api, bearer token). Proves the round-trips epic
// #10047 (agent-kai↔A2A unification) needs:
//   1. kai_sessions       → lists daemon sessions ('ops' present)
//   2. kai_send           → message a fresh session, get a non-empty streamed reply
//   3. kai_history        → read that session's snapshot back
//   4. fail-soft: a dead KAI_WS_URL/KAI_HTTP_URL returns {ok:false,error}, no throw.
//
// Plus pure assertions (listTools shape / name routing) that need no network.
//
// Considerate: only benign no-action test messages, and NEVER to 'ops'.
//
// Run: bun test tests/kai_tools.test.ts
import { test, expect, describe } from 'bun:test'
import { KaiTools } from '../kai_tools.ts'

const TEST_SESSION = 'a2a-kaitools-test'
const BENIGN_MSG =
  'A2A kai_tools connectivity test. Reply with ONE short line confirming receipt. ' +
  'Take NO external/Discord/tool action.'

// Unwrap an MCP ToolResult into its JSON payload.
function payload(r: { content: { text: string }[]; isError?: boolean }): any {
  return JSON.parse(r.content[0].text)
}

describe('KaiTools — pure (no network)', () => {
  const kt = new KaiTools()
  test('listTools registers the four kai tools', () => {
    const names = kt.listTools().map((t: any) => t.name).sort()
    expect(names).toEqual(['kai_history', 'kai_schedule', 'kai_send', 'kai_sessions'])
  })
  test('handles() owns only the kai tool names', () => {
    expect(kt.handles('kai_send')).toBe(true)
    expect(kt.handles('kai_sessions')).toBe(true)
    expect(kt.handles('a2a_remember')).toBe(false)
    expect(kt.handles('a2a_send')).toBe(false)
  })
  test('unknown tool fails soft (does not throw)', async () => {
    const r = await kt.callTool('kai_bogus', {})
    expect(r.isError).toBe(true)
    expect(payload(r).error).toBe('unknown_tool')
  })
  test('missing required args fail soft', async () => {
    expect(payload(await kt.callTool('kai_history', {})).error).toContain('session required')
    expect(payload(await kt.callTool('kai_send', { session: 's' })).error).toContain('text required')
    expect(payload(await kt.callTool('kai_schedule', { cron: '* * * * *' })).error).toContain('prompt required')
  })
})

describe('KaiTools — fail-soft against a dead daemon', () => {
  // Port 1 refuses fast; short timeouts keep the test snappy. A real token is
  // present so we exercise the connect path (not the no-token short-circuit).
  const dead = new KaiTools({
    wsUrl: 'ws://127.0.0.1:1/ws',
    httpUrl: 'http://127.0.0.1:1',
    httpTimeoutMs: 1500,
    attachTimeoutMs: 1500,
  })
  test('kai_sessions on a dead daemon returns {ok:false,error}, no throw', async () => {
    const p = payload(await dead.callTool('kai_sessions', {}))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/kai_unavailable/)
  })
  test('kai_send on a dead daemon returns {ok:false,error}, no throw', async () => {
    const p = payload(await dead.callTool('kai_send', { session: TEST_SESSION, text: 'x', timeout_ms: 2000 }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/kai_unavailable/)
  })
  test('kai_history on a dead daemon returns {ok:false,error}, no throw', async () => {
    const p = payload(await dead.callTool('kai_history', { session: TEST_SESSION }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/kai_unavailable/)
  })
  test('no-token configuration fails soft, no throw', async () => {
    const noTok = new KaiTools({ token: '', tokenPath: '/nonexistent/kai-token', wsUrl: 'ws://127.0.0.1:1/ws', attachTimeoutMs: 1500 })
    const p = payload(await noTok.callTool('kai_history', { session: TEST_SESSION }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/kai_unavailable/)
  })
})

describe('KaiTools — live round-trips against the daemon', () => {
  const kt = new KaiTools()

  test('kai_sessions lists sessions incl. ops', async () => {
    const p = payload(await kt.callTool('kai_sessions', {}))
    expect(p.ok).toBe(true)
    expect(Array.isArray(p.sessions)).toBe(true)
    expect(p.count).toBeGreaterThan(0)
    expect(p.sessions).toContain('ops')
  })

  test('kai_send round-trips a benign message to a fresh session', async () => {
    const p = payload(await kt.callTool('kai_send', {
      session: TEST_SESSION,
      text: BENIGN_MSG,
      create_if_missing: true,
      timeout_ms: 170000,
    }))
    expect(p.ok).toBe(true)
    expect(p.session).toBe(TEST_SESSION)
    expect(typeof p.reply).toBe('string')
    expect(p.reply.length).toBeGreaterThan(0)
  }, 180000)

  test('kai_history returns the session snapshot', async () => {
    const p = payload(await kt.callTool('kai_history', { session: TEST_SESSION, limit: 20 }))
    expect(p.ok).toBe(true)
    expect(p.session).toBe(TEST_SESSION)
    expect(Array.isArray(p.history)).toBe(true)
    expect(p.history.length).toBeGreaterThan(0)
    // The benign message we sent should be in the snapshot.
    const joined = p.history.map((h: any) => h.content).join('\n')
    expect(joined).toContain('connectivity test')
  }, 30000)

  test('kai_history on a missing session fails soft (create_if_missing:false)', async () => {
    const p = payload(await kt.callTool('kai_history', { session: `kaitools-nope-${Date.now().toString(36)}` }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/kai_error|kai_unavailable/)
  }, 20000)
})
