import { describe, expect, test } from 'bun:test'
import { PortalLangChainAgent, PORTAL_LANGCHAIN_CHANNEL, scrubForModel } from '../portal_langchain_agent.ts'

function deps(env: Record<string, string | undefined> = {}) {
  let n = 0
  return {
    portalAgentId: 'a2a-portal',
    env,
    now: () => 1_700_000_000_000 + n,
    randomId: () => `id-${++n}`,
    getSendChannel: () => null,
    getSendStatus: () => ({ enabled: false, agent_id: 'a2a-portal', error: 'disabled' }),
    listPeers: async () => [{ id: 'agent-1', host: 'host-1', ttl: 42 }],
    listChannels: () => [{ name: 'ops', kind: 'topic' as const, count: 1, lastT: 1, lastFrom: 'agent-1' }],
    readChannel: async () => [{ from: 'agent-1', to: 'topic:ops', type: 'msg', ts: '2026-06-19T00:00:00.000Z', body: 'hello' }],
  }
}

describe('PortalLangChainAgent', () => {
  test('reports missing OpenAI key without constructing a model', async () => {
    const agent = new PortalLangChainAgent(deps({ OPENAI_API_KEY: '' }))
    expect(agent.status()).toMatchObject({
      enabled: true,
      ready: false,
      channel: PORTAL_LANGCHAIN_CHANNEL,
      error: 'missing_openai_api_key',
    })

    const result = await agent.chat('hello')
    expect(result.ok).toBe(false)
    expect(result.error).toBe('missing_openai_api_key')
    expect(result.messages.map((m) => m.from)).toEqual(['a2a-portal', 'langchain-agent'])
  })

  test('can be disabled by env flag', async () => {
    const agent = new PortalLangChainAgent(deps({ A2A_PORTAL_LANGCHAIN_ENABLED: '0', OPENAI_API_KEY: 'sk-test-local' }))
    expect(agent.status()).toMatchObject({ enabled: false, ready: false, error: 'disabled' })
    const result = await agent.chat('hello')
    expect(result.ok).toBe(false)
    expect(result.reply).toBe('disabled')
  })

  test('scrubs obvious credential-shaped text before model exposure', () => {
    const out = scrubForModel('OPENAI_API_KEY=sk-123456789abcdef and sk-abcdefghijklmnopqrstuvwxyz012345')
    expect(out).toContain('OPENAI_API_KEY=[redacted]')
    expect(out).toContain('sk-[redacted]')
    expect(out).not.toContain('abcdefghijklmnopqrstuvwxyz012345')
  })
})
