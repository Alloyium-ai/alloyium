// Skill-broadcast tests (SPEC-A §9) — the two-layer `skill.created.v1` announce.
//
// Pure helpers (classify/summary/build) are tested in isolation. The instance
// method broadcastSkillCreated is driven against INJECTED fake transports: the
// channel is constructed but never start()ed, so we install a fake NATS publish
// + fake Redis on its (soft-private) fields and flip `started`. This keeps the
// publish/registry/size/counter/signing paths deterministic with no live bus.
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { mkdtempSync, mkdirSync, rmSync, existsSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { RedisClient } from 'bun'
import {
  classifySkillScope, skillSummary, buildSkillCreatedEvent,
  A2AChannel, RESERVED_ATTRS, SKILLS_GLOBAL_TOPIC, SKILLS_TOPIC,
  verifyEnvelope, topicSubject, type A2AChannelOpts, type Envelope,
} from '../a2a-channel.ts'

const REPO = new URL('..', import.meta.url).pathname
const decodeEnv = (payload: Uint8Array): Envelope => JSON.parse(new TextDecoder().decode(payload))

// A channel wired to fake transports, forced `started`. devNoAuth ⇒ signing off
// by default (override via opts for the signing path). now/genId are deterministic.
function fakeChannel(opts: Partial<A2AChannelOpts> = {}) {
  const published: { subject: string; payload: Uint8Array }[] = []
  const redisCalls: { cmd: string; args: any[] }[] = []
  const ch = new A2AChannel(async () => {}, {
    enabled: true, agentId: 'tester-1', devNoAuth: true,
    now: () => 1_718_476_800_000, genId: () => 'fixed-id-1', ...opts,
  })
  ;(ch as any).nc = { publish: (subject: string, payload: Uint8Array) => { published.push({ subject, payload }) } }
  ;(ch as any).ncPublish = (ch as any).nc
  ;(ch as any).redis = { send: (cmd: string, args: any[]) => { redisCalls.push({ cmd, args }); return Promise.resolve('OK') } }
  ;(ch as any).started = true
  return { ch, published, redisCalls }
}

const skill = (over: Partial<{ name: string; slug: string; source: string; backend: string; body: string; tags: string[] }> = {}) =>
  ({ name: 'deploy-helper', slug: 'skillpacks/deploy-helper', source: 'skills', body: 'A deploy helper.', tags: [], ...over })

describe('classifySkillScope (§5.2, D3)', () => {
  test('frontmatter scope:global → global', () => {
    expect(classifySkillScope('---\nname: x\nscope: global\n---\nbody', [])).toBe('global')
  })
  test('universal tag → global (case-insensitive)', () => {
    expect(classifySkillScope('no frontmatter here', ['demo', 'universal'])).toBe('global')
    expect(classifySkillScope('plain', ['UNIVERSAL'])).toBe('global')
  })
  test('neither scope:global nor universal tag → tagged', () => {
    expect(classifySkillScope('plain body', ['demo', 'edge'])).toBe('tagged')
    expect(classifySkillScope('---\nscope: tagged\n---\nb', [])).toBe('tagged')
  })
  test('scope value is case-insensitive', () => {
    expect(classifySkillScope('---\nscope: GLOBAL\n---\nb', [])).toBe('global')
    expect(classifySkillScope('---\nscope: Global\n---\nb', [])).toBe('global')
  })
  test('leading-`---`-only frontmatter; a mid-body `---` is NOT frontmatter', () => {
    expect(classifySkillScope('intro paragraph\n---\nscope: global\n---\nx', [])).toBe('tagged')
  })
  test('empty body → tagged; zero-tag non-global is still tagged', () => {
    expect(classifySkillScope('', [])).toBe('tagged')
    expect(classifySkillScope('just text', [])).toBe('tagged')
  })
  test('✓C CRLF-prefixed frontmatter classifies', () => {
    expect(classifySkillScope('---\r\nscope: global\r\n---\r\nbody', [])).toBe('global')
  })
  test('✓C BOM-prefixed frontmatter classifies', () => {
    expect(classifySkillScope('﻿---\nscope: global\n---\nbody', [])).toBe('global')
  })
  test('✓C quoted scope: "global" / \'global\' classifies', () => {
    expect(classifySkillScope('---\nscope: "global"\n---\nb', [])).toBe('global')
    expect(classifySkillScope("---\nscope: 'global'\n---\nb", [])).toBe('global')
  })
})

describe('skillSummary (§5.2)', () => {
  test('skips frontmatter, blanks and headings → first prose line', () => {
    expect(skillSummary('---\nname: x\n---\n\n# Heading\n\nThe real summary line.\nmore')).toBe('The real summary line.')
  })
  test('returns empty string when there is no prose line', () => {
    expect(skillSummary('---\nname: x\n---\n\n# Only A Heading\n')).toBe('')
    expect(skillSummary('')).toBe('')
  })
  test('caps the summary at 280 chars', () => {
    const long = Array.from({ length: 200 }, () => 'lorem').join(' ') // long, no 40+ entropy run
    expect(skillSummary(long).length).toBe(280)
  })
  test('✓R redacts a PEM private-key block', () => {
    const s = skillSummary('-----BEGIN PRIVATE KEY-----abcDEF12345-----END PRIVATE KEY-----')
    expect(s).toContain('[redacted]')
    expect(s).not.toContain('abcDEF12345')
  })
  test('✓R redacts sk-/ghp_/AKIA key shapes', () => {
    expect(skillSummary('token sk-abcdefghijklmnop12345 trailing')).toContain('[redacted]')
    expect(skillSummary('ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 x')).toContain('[redacted]')
    expect(skillSummary('aws key AKIAIOSFODNN7EXAMPLE end')).toContain('[redacted]')
  })
  test('✓R redacts a 40+ char high-entropy run', () => {
    const s = skillSummary('digest ' + 'a1B2'.repeat(12) + ' tail') // 48-char run
    expect(s).toContain('[redacted]')
    expect(s).not.toContain('a1B2a1B2')
  })
})

describe('buildSkillCreatedEvent (§5.2, ✓C tag cap)', () => {
  test('exact shape; pointer (source+slug) present; full body never present', () => {
    const ev = buildSkillCreatedEvent({ name: 'n', slug: 'skillpacks/n', source: 'skills', scope: 'tagged', tags: ['A', 'b', 'A', ''], summary: 'sum', by: 'agent-1', ts: '2026-06-15T00:00:00Z' })
    expect(ev).toEqual({ schema: 'skill.created.v1', name: 'n', slug: 'skillpacks/n', source: 'skills', backend: 'brain-notepad', scope: 'tagged', tags: ['a', 'b'], summary: 'sum', by: 'agent-1', ts: '2026-06-15T00:00:00Z' })
    expect((ev as any).body).toBeUndefined()
  })
  test('✓C normalizes + caps tags: lowercase/trim/dedupe, ≤64 chars, ≤24 tags', () => {
    const tags = [...Array.from({ length: 50 }, (_, i) => `Tag-${i}`), '  Spaced  ', 'DUP', 'dup', 'x'.repeat(80)]
    const ev = buildSkillCreatedEvent({ name: 'n', slug: 's', source: 'skills', scope: 'tagged', tags, summary: '', by: 'a', ts: 't' })
    expect(ev.tags.length).toBeLessThanOrEqual(24)
    expect(ev.tags.every((t) => t === t.toLowerCase().trim())).toBe(true)
    expect(ev.tags.every((t) => t.length <= 64)).toBe(true)
    expect(new Set(ev.tags).size).toBe(ev.tags.length) // deduped
    expect(ev.tags).not.toContain('') // empties dropped
  })
})

describe('broadcastSkillCreated (§5.3) — injected fake publish + redis', () => {
  test('global scope → skills-global topic + HSET registry; counters.sent +1', async () => {
    const { ch, published, redisCalls } = fakeChannel()
    const r = await ch.broadcastSkillCreated(skill({ body: '---\nscope: global\n---\nUniversal deploy helper.' }))
    expect(r).toMatchObject({ ok: true, scope: 'global', topic: SKILLS_GLOBAL_TOPIC, id: 'fixed-id-1' })
    expect(published).toHaveLength(1)
    expect(published[0].subject).toBe(topicSubject('alloyium.a2a.', SKILLS_GLOBAL_TOPIC))
    const env = decodeEnv(published[0].payload)
    expect(env.to).toBe(`topic:${SKILLS_GLOBAL_TOPIC}`)
    const event = JSON.parse(env.body)
    expect(event).toMatchObject({ schema: 'skill.created.v1', name: 'deploy-helper', slug: 'skillpacks/deploy-helper', source: 'skills', scope: 'global', summary: 'Universal deploy helper.' })
    const hset = redisCalls.find((c) => c.cmd === 'HSET')
    expect(hset?.args[0]).toBe('alloyium:a2a:skills:global')
    expect(hset?.args[1]).toBe('deploy-helper')
    expect(JSON.parse(hset!.args[2])).toMatchObject({ source: 'skills', slug: 'skillpacks/deploy-helper', summary: 'Universal deploy helper.' })
    expect(redisCalls.some((c) => c.cmd === 'HDEL')).toBe(false)
    expect(ch.counts().sent).toBe(1)
  })

  test('tagged scope → skills topic, no HSET, ✓R HDEL-on-demote', async () => {
    const { ch, published, redisCalls } = fakeChannel()
    const r = await ch.broadcastSkillCreated(skill({ name: 'demo-edge', body: 'A tagged skill.', tags: ['demo', 'edge'] }))
    expect(r).toMatchObject({ ok: true, scope: 'tagged', topic: SKILLS_TOPIC })
    const env = decodeEnv(published[0].payload)
    expect(env.to).toBe(`topic:${SKILLS_TOPIC}`)
    expect(env.attrs!.skill_tags).toBe('demo,edge') // derived from the capped event.tags
    expect(JSON.parse(env.body).tags).toEqual(['demo', 'edge'])
    expect(redisCalls.some((c) => c.cmd === 'HSET')).toBe(false)
    const hdel = redisCalls.find((c) => c.cmd === 'HDEL')
    expect(hdel?.args).toEqual(['alloyium:a2a:skills:global', 'demo-edge'])
  })

  test('attrs are present, string-valued, and none collide with RESERVED_ATTRS', async () => {
    const { ch, published } = fakeChannel()
    await ch.broadcastSkillCreated(skill({ tags: ['a', 'b'] }))
    const env = decodeEnv(published[0].payload)
    expect(env.attrs).toMatchObject({ event: 'skill.created.v1', skill: 'deploy-helper', scope: 'tagged', skill_tags: 'a,b' })
    for (const k of Object.keys(env.attrs!)) {
      expect(RESERVED_ATTRS.has(k)).toBe(false)
      expect(typeof env.attrs![k]).toBe('string')
    }
  })

  test('✓C many tags: event.tags capped ≤24 AND full signed envelope ≤ maxSendBytes', async () => {
    const { ch, published } = fakeChannel()
    const r = await ch.broadcastSkillCreated(skill({ tags: Array.from({ length: 100 }, (_, i) => `tag${i}`) }))
    expect(r.ok).toBe(true)
    const env = decodeEnv(published[0].payload)
    expect(JSON.parse(env.body).tags.length).toBeLessThanOrEqual(24)
    expect(env.attrs!.skill_tags.split(',').length).toBeLessThanOrEqual(24)
    expect(new TextEncoder().encode(JSON.stringify(env)).byteLength).toBeLessThanOrEqual(8192) // default maxSendBytes
  })

  test('✓C oversize envelope → {ok:false,error:oversize}, no publish, no counter bump', async () => {
    const { ch, published } = fakeChannel({ maxSendBytes: 1 }) // any real envelope exceeds 1 byte
    const r = await ch.broadcastSkillCreated(skill())
    expect(r).toEqual({ ok: false, error: 'oversize' })
    expect(published).toHaveLength(0)
    expect(ch.counts().sent).toBe(0)
  })

  test('disabled/stopped → {ok:false:a2a_disabled} (no throw, no publish)', async () => {
    const never = new A2AChannel(async () => {}, { enabled: true, agentId: 'x', devNoAuth: true })
    expect(await never.broadcastSkillCreated(skill())).toEqual({ ok: false, error: 'a2a_disabled' })
    const { ch, published } = fakeChannel()
    ;(ch as any).stopped = true
    expect((await ch.broadcastSkillCreated(skill())).ok).toBe(false)
    expect(published).toHaveLength(0)
  })

  test('publish throw → {ok:false}, NEVER throws; rejected/denied untouched (✓R counters)', async () => {
    const { ch } = fakeChannel()
    ;(ch as any).nc = { publish: () => { throw new Error('nats down') } }
    ;(ch as any).ncPublish = (ch as any).nc
    const r = await ch.broadcastSkillCreated(skill())
    expect(r.ok).toBe(false)
    expect(ch.counts()).toMatchObject({ sent: 0, rejected: 0, denied: 0 })
  })

  test('a registry (HSET) failure does NOT fail the broadcast (best-effort)', async () => {
    const { ch, published } = fakeChannel()
    ;(ch as any).redis = { send: () => Promise.reject(new Error('redis down')) }
    const r = await ch.broadcastSkillCreated(skill({ body: '---\nscope: global\n---\nx' }))
    expect(r).toMatchObject({ ok: true, scope: 'global' }) // published despite registry failure
    expect(published).toHaveLength(1)
  })

  test('signing path: when signing is on, the envelope is signed and verifies', async () => {
    const { ch, published } = fakeChannel({ devNoAuth: false, transportAuth: 'none', sigAlg: 'hmac', signingKey: 'sekret' })
    const r = await ch.broadcastSkillCreated(skill({ tags: ['x'] }))
    expect(r.ok).toBe(true)
    const env = decodeEnv(published[0].payload)
    expect(env.alg).toBe('hmac')
    expect(typeof env.sig).toBe('string')
    expect(await verifyEnvelope(env, 'sekret', 'hmac')).toBe(true)
  })
})

describe('publish-confinement invariant (§9)', () => {
  test('no new `.publish(` call site — ALL publishes stay INSIDE publishA2A', async () => {
    const code = (await Bun.file(REPO + 'a2a-channel.ts').text()).replace(/\/\/.*$/gm, '')
    const total = [...code.matchAll(/\.publish\s*\(/g)].length
    expect(total).toBeLessThanOrEqual(2)
    // Slice publishA2A's body (signature → the next method) and prove every
    // `.publish(` in the file lives within it (mirrors a2a-safety T-S2).
    const start = code.indexOf('private async publishA2A')
    const end = code.indexOf('async _publishForTest')
    expect(start).toBeGreaterThanOrEqual(0)
    expect(end).toBeGreaterThan(start)
    expect([...code.slice(start, end).matchAll(/\.publish\s*\(/g)].length).toBe(total)
  })
})

// a2a-launch.sh — static contract checks for the §6 wiring shape.
describe('a2a-launch.sh — skill-broadcast wiring shape (§6)', () => {
  let sh = ''
  beforeAll(async () => { sh = await Bun.file(REPO + 'a2a-launch.sh').text() })

  test('§6.1 default-join: A2A_DEFAULT_TOPICS + idempotent EVAL on the topics key, fail-soft', () => {
    expect(sh).toContain('A2A_DEFAULT_TOPICS')
    expect(sh).toContain('alloyium:a2a:topics:$ID')
    expect(sh).toMatch(/redis-cli .*EVAL/)
    expect(sh).toContain('redis.error_reply("bad_json")') // malformed key ⇒ abort-without-write
    expect(sh).toMatch(/continuing \(agent still launches\)/) // redis down ⇒ warn + continue
  })

  test('default-join is kind-agnostic — placed ABOVE the claude/codex exec split', () => {
    const joinIdx = sh.indexOf('A2A_DEFAULT_TOPICS=')
    const execSplitIdx = sh.indexOf('# 6) per-agent launch.sh')
    expect(joinIdx).toBeGreaterThanOrEqual(0)
    expect(execSplitIdx).toBeGreaterThan(joinIdx)
  })

  test('§6.2 autoload: --append-system-prompt-file injected on BOTH claude exec branches', () => {
    expect(sh).toContain('--append-system-prompt-file')
    const execLines = sh.split('\n').filter((l) => l.includes('exec claude'))
    expect(execLines).toHaveLength(2) // pm + non-pm
    expect(execLines.every((l) => l.includes('$APPEND_SKILLS'))).toBe(true)
  })

  test('§6.2 autoload reads the registry via the REG env var, NOT a pipe (the heredoc-stdin bug)', () => {
    // `cmd | python3 - <<PY` would leave sys.stdin at EOF (the heredoc IS stdin),
    // so the registry must arrive via env. Guard against a regression to the pipe.
    expect(sh).toMatch(/REG="\$_reg".*python3 - <<'PY'/s)
    expect(sh).toContain("os.environ.get('REG'")
    expect(sh).not.toMatch(/printf .*"\$_reg" \| .*python3 -/) // no pipe-into-python
  })
})

// a2a-launch.sh — RUNTIME shell smoke for the §6 autoload (DoD evidence for Layer-1
// launch-context injection; codex BLOCK fix). Runs the REAL launcher in
// A2A_DRY_WIRING mode (skips onboard/trust/tmux) against a seeded registry, fully
// isolated via temp CHANNELS/AGENTS_ROOT + a throwaway A2A_SKILLS_GLOBAL_KEY.
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
let redisOk = true
let redis: RedisClient
try { redis = new RedisClient(REDIS_URL); await redis.set('a2a:wiring:probe', '1'); await redis.del('a2a:wiring:probe') } catch { redisOk = false }
const dirsToClean: string[] = []
const keysToClean: string[] = []

afterAll(async () => {
  for (const d of dirsToClean) { try { rmSync(d, { recursive: true, force: true }) } catch {} }
  if (redisOk) { for (const k of keysToClean) { try { await redis.del(k) } catch {} } ; try { (redis as any).close?.() } catch {} }
})

// Run the launcher in dry-wiring mode; returns the materialized artifacts.
function runWiring(id: string, env: Record<string, string>) {
  const root = mkdtempSync(join(tmpdir(), 'a2a-wiring-'))
  dirsToClean.push(root)
  keysToClean.push(`alloyium:a2a:topics:${id}`)
  mkdirSync(join(root, 'channels', 'a2a'), { recursive: true })
  mkdirSync(join(root, 'agents'), { recursive: true })
  const proc = Bun.spawnSync(['bash', REPO + 'a2a-launch.sh', id, 'claude'], {
    env: {
      ...process.env, A2A_DRY_WIRING: '1', A2A_MODE: 'webhook', CHANNELS: join(root, 'channels'), AGENTS_ROOT: join(root, 'agents'),
      REDIS_HOST: '172.17.0.1', REDIS_PORT: '6379', BRAIN_URL: 'http://127.0.0.1:1', ...env,
    },
  })
  const agentDir = join(root, 'agents', id)
  const read = (f: string) => (existsSync(join(agentDir, f)) ? readFileSync(join(agentDir, f), 'utf8') : null)
  return { exitCode: proc.exitCode, gs: read('global-skills.md'), lsh: read('launch.sh') }
}

describe.skipIf(!redisOk)('a2a-launch.sh — §6 autoload RUNTIME smoke', () => {
  test('seeded registry → global-skills.md written AND launch.sh carries --append-system-prompt-file', async () => {
    const id = `wiring-hit-${Math.floor(Math.random() * 1e6)}`
    const key = `alloyium:a2a:skills:global:test-${id}`
    keysToClean.push(key)
    await redis.send('HSET', [key, 'sample-skill', JSON.stringify({ source: 'skills', slug: 'skillpacks/sample-skill', summary: 'A sample global skill.', ts: '2026-06-15T00:00:00Z' })])
    const { exitCode, gs, lsh } = runWiring(id, { A2A_SKILLS_GLOBAL_KEY: key }) // brain unreachable → summary fallback
    expect(exitCode).toBe(0)
    expect(gs).not.toBeNull()
    expect(gs!.length).toBeGreaterThan(0)
    expect(gs).toContain('sample-skill')
    expect(gs).toContain('A sample global skill.') // summary fallback when brain is down
    expect(lsh).toContain('--append-system-prompt-file')
    expect(lsh).toContain('global-skills.md')
  }, 20_000)

  test('fail-soft: empty registry → NO global-skills.md, NO flag, launcher still exits 0', async () => {
    const id = `wiring-empty-${Math.floor(Math.random() * 1e6)}`
    const key = `alloyium:a2a:skills:global:test-${id}` // never seeded
    keysToClean.push(key)
    const { exitCode, gs, lsh } = runWiring(id, { A2A_SKILLS_GLOBAL_KEY: key })
    expect(exitCode).toBe(0)
    expect(gs).toBeNull()
    expect(lsh).not.toBeNull()
    expect(lsh).not.toContain('--append-system-prompt-file')
  }, 20_000)

  test('fail-soft: redis unreachable → launcher still exits 0, no autoload, no flag', async () => {
    const id = `wiring-down-${Math.floor(Math.random() * 1e6)}`
    const { exitCode, gs, lsh } = runWiring(id, { REDIS_PORT: '1' }) // dead redis port
    expect(exitCode).toBe(0)
    expect(gs).toBeNull()
    expect(lsh).not.toContain('--append-system-prompt-file')
  }, 20_000)
})
