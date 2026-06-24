// brain_tools integration test — runs against the LIVE agent-brain (brain-notepad
// FastAPI, default :8787). Proves the round-trips the substrate Phase-1 gap needs:
//   1. a2a_remember → a2a_recall → a2a_brain_get  (note save / RAG / fetch)
//   2. a2a_skill_save → a2a_skill_get             (procedural memory)
//   3. fail-soft: a dead BRAIN_URL returns {ok:false,error} and NEVER throws.
//
// Plus pure assertions (listTools shape / name routing) that need no network.
//
// Run: bun test tests/brain_tools.test.ts
import { test, expect, describe } from 'bun:test'
import { BrainTools } from '../brain_tools.ts'

const BRAIN_URL = process.env.BRAIN_URL ?? 'http://127.0.0.1:8787'
const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`

// Unwrap an MCP ToolResult into its JSON payload.
function payload(r: { content: { text: string }[]; isError?: boolean }): any {
  return JSON.parse(r.content[0].text)
}

describe('BrainTools — pure (no network)', () => {
  const bt = new BrainTools()
  test('listTools registers the five brain tools', () => {
    const names = bt.listTools().map((t: any) => t.name).sort()
    expect(names).toEqual(['a2a_brain_get', 'a2a_recall', 'a2a_remember', 'a2a_skill_get', 'a2a_skill_save'])
  })
  test('handles() owns only the brain tool names', () => {
    expect(bt.handles('a2a_remember')).toBe(true)
    expect(bt.handles('a2a_send')).toBe(false)
  })
  test('unknown tool fails soft (does not throw)', async () => {
    const r = await bt.callTool('a2a_bogus', {})
    expect(r.isError).toBe(true)
    expect(payload(r).error).toBe('unknown_tool')
  })
  test('missing required args fail soft', async () => {
    expect(payload(await bt.callTool('a2a_remember', { title: 'x' })).error).toContain('body required')
    expect(payload(await bt.callTool('a2a_recall', {})).error).toContain('query required')
  })
})

describe('BrainTools — fail-soft against a dead brain', () => {
  // Port 1 refuses fast; short timeout keeps the test snappy.
  const dead = new BrainTools({ brainUrl: 'http://127.0.0.1:1', timeoutMs: 1500 })
  test('a2a_remember on a dead brain returns {ok:false,error}, no throw', async () => {
    const r = await dead.callTool('a2a_remember', { title: `dead-${nonce}`, body: 'x' })
    const p = payload(r)
    expect(r.isError).toBe(true)
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/brain_unavailable/)
  })
  test('a2a_recall on a dead brain returns {ok:false,error}, no throw', async () => {
    const p = payload(await dead.callTool('a2a_recall', { query: 'anything' }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/brain_unavailable/)
  })
  test('a2a_skill_save on a dead brain returns {ok:false,error}, no throw', async () => {
    const p = payload(await dead.callTool('a2a_skill_save', { name: `dead-${nonce}`, body: 'x' }))
    expect(p.ok).toBe(false)
    expect(p.error).toMatch(/skillpacks_not_available/)
  })
})

describe('BrainTools — live round-trips against the brain', () => {
  const bt = new BrainTools({ brainUrl: BRAIN_URL, timeoutMs: 8000, source: 'a2a' })
  const title = `BrainTools roundtrip ${nonce}`
  // A distinctive sentence so both FTS and embeddings can find it.
  const body = `# Roundtrip test ${nonce}\n\nThe a2a bus trust model is advisory-only; this marker is ${nonce}.`
  let savedSlug = ''
  let savedSource = ''

  test('a2a_remember saves a note', async () => {
    const p = payload(await bt.callTool('a2a_remember', { title, body, tags: ['braintools-test'] }))
    expect(p.ok).toBe(true)
    expect(typeof p.slug).toBe('string')
    expect(p.url).toContain('/md/')
    savedSlug = p.slug
    savedSource = p.source
  })

  test('a2a_recall finds the saved note', async () => {
    // hybrid = FTS exact-match on the nonce + embedding similarity → reliable hit.
    const p = payload(await bt.callTool('a2a_recall', { query: `bus trust model ${nonce}`, mode: 'hybrid', limit: 5, source: savedSource }))
    expect(p.ok).toBe(true)
    expect(p.count).toBeGreaterThan(0)
    const slugs = p.hits.map((h: any) => h.slug)
    expect(slugs).toContain(savedSlug)
    const hit = p.hits.find((h: any) => h.slug === savedSlug)
    expect(hit.source).toBe(savedSource)
    expect(typeof hit.score).toBe('number')
  })

  test('a2a_brain_get fetches the markdown', async () => {
    const p = payload(await bt.callTool('a2a_brain_get', { source: savedSource, slug: savedSlug }))
    expect(p.ok).toBe(true)
    expect(p.markdown).toContain(nonce)
  })

  test('a2a_brain_get on a missing slug → not_found', async () => {
    const p = payload(await bt.callTool('a2a_brain_get', { source: savedSource, slug: `notes/does-not-exist-${nonce}` }))
    expect(p.ok).toBe(false)
    expect(p.error).toBe('not_found')
  })
})

describe('BrainTools — live skillpack round-trip', () => {
  const bt = new BrainTools({ brainUrl: BRAIN_URL, timeoutMs: 8000 })
  const skillName = `braintools-skill-${nonce}`
  const skillBody = `## Procedure\n\n1. Recall before re-deriving.\n2. Marker ${nonce}.`

  test('a2a_skill_save stores a skillpack', async () => {
    const p = payload(await bt.callTool('a2a_skill_save', { name: skillName, body: skillBody, tags: ['retro'] }))
    expect(p.ok).toBe(true)
    expect(p.backend).toBe('brain-notepad')
    expect(p.name).toBe(skillName)
  })

  test('a2a_skill_get returns the stored skillpack', async () => {
    const p = payload(await bt.callTool('a2a_skill_get', { name: skillName }))
    expect(p.ok).toBe(true)
    expect(p.body).toContain(nonce)
    expect(p.tags).toContain('retro')
    expect(p.backend).toBe('brain-notepad')
  })

  test('a2a_skill_get on a missing name → not_found', async () => {
    const p = payload(await bt.callTool('a2a_skill_get', { name: `nope-${nonce}` }))
    expect(p.ok).toBe(false)
    expect(p.error).toBe('not_found')
  })
})
