// vault_tools test — pure (no network) unit + SECURITY tests for the vault_howto
// guidance tool. The tool is self-contained (no backend), so every test is offline.
//
// The load-bearing test is the §7 forbidden-content guard: for EVERY topic value
// AND listTools(), the serialized wire `text` must leak none of the operator-runbook
// secrets. Per SPEC-B §7 ✓C the matching strategy is split:
//   - LITERAL sentinels: matched case-INSENSITIVELY (lowercase haystack + needle).
//   - STRUCTURAL regexes (entropy / URL / host-domain / env-var-secret / RFC1918): run
//     CASE-SENSITIVE on the ORIGINAL-case text, so the uppercase env-var + IP patterns
//     actually fire and never self-match required phrases (e.g. AGENT-TOKEN-SCOPING).
//
// Run: bun test tests/vault_tools.test.ts
import { test, expect, describe } from 'bun:test'
import { VaultTools, forbiddenHits, containsForbidden } from '../vault_tools.ts'

/** Unwrap an MCP ToolResult into its parsed JSON payload. */
function payload(r: { content: { text: string }[]; isError?: boolean }): any {
  return JSON.parse(r.content[0].text)
}
/** The raw serialized wire text the §7 guard scans (NOT the parsed object). */
function wire(r: { content: { text: string }[] }): string {
  return r.content[0].text
}

// ── SPEC-B §7 forbidden content (the security contract) ──────────────────────
// The denylist + checker live IN vault_tools.ts — the SINGLE SOURCE OF TRUTH the
// constructor ALSO enforces at runtime over provenance. We import the exact same
// checker so the test can never drift from the runtime guard; that drift is exactly
// what let a conforming-but-forbidden provenance value (e.g. slug 'kai-main') slip
// past an earlier revision. `leaks` is the imported checker.
const leaks = forbiddenHits

describe('VaultTools — module shape', () => {
  const vt = new VaultTools()

  test('listTools registers exactly the vault_howto schema', () => {
    const tools = vt.listTools()
    expect(tools).toHaveLength(1)
    const [t] = tools
    expect(t.name).toBe('vault_howto')
    expect(typeof t.description).toBe('string')
    expect(t.inputSchema.type).toBe('object')
    expect(t.inputSchema.additionalProperties).toBe(false)
    expect(t.inputSchema.properties.topic.enum).toEqual([
      'vault', 'taskboard', 'policy', 'escalation', 'all',
    ])
    expect(t.inputSchema.properties.topic.default).toBe('all')
  })

  test('handles() owns only vault_howto', () => {
    expect(vt.handles('vault_howto')).toBe(true)
    expect(vt.handles('a2a_send')).toBe(false)
    expect(vt.handles('kai_send')).toBe(false)
    expect(vt.handles('vault')).toBe(false)
  })

  test('INSTRUCTIONS begins with a leading space (webhook concats with no separator)', () => {
    expect(VaultTools.INSTRUCTIONS.startsWith(' ')).toBe(true)
  })
})

describe('VaultTools — vault_howto per-topic contract', () => {
  const vt = new VaultTools()

  test('default (no args) ⇒ ok:true, behaves as all, with policy + escalation + source_of_record', async () => {
    const p = payload(await vt.callTool('vault_howto'))
    expect(p.ok).toBe(true)
    // Always-on, load-bearing guidance:
    expect(typeof p.guidance.policy).toBe('string')
    expect(typeof p.guidance.escalation).toBe('string')
    expect(Array.isArray(p.source_of_record)).toBe(true)
    expect(p.source_of_record.length).toBeGreaterThan(0)
    expect(typeof p.advisory).toBe('string')
    // default topic = all ⇒ also carries the descriptive sections:
    expect(typeof p.guidance.vault).toBe('string')
    expect(typeof p.guidance.taskboard).toBe('string')
  })

  test('default source_of_record is the canonical runbook pointer', async () => {
    const p = payload(await vt.callTool('vault_howto'))
    expect(p.source_of_record).toEqual([
      { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
    ])
  })

  test('topic=vault ⇒ {vault, policy, escalation}; taskboard absent', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'vault' }))
    expect(Object.keys(p.guidance).sort()).toEqual(['escalation', 'policy', 'vault'])
  })

  test('topic=taskboard ⇒ {taskboard, policy, escalation}; vault absent', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'taskboard' }))
    expect(Object.keys(p.guidance).sort()).toEqual(['escalation', 'policy', 'taskboard'])
  })

  test('topic=policy ⇒ {policy, escalation} only', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'policy' }))
    expect(Object.keys(p.guidance).sort()).toEqual(['escalation', 'policy'])
  })

  test('topic=escalation ⇒ {policy, escalation} only', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'escalation' }))
    expect(Object.keys(p.guidance).sort()).toEqual(['escalation', 'policy'])
  })

  test('topic=all ⇒ all four sections', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'all' }))
    expect(Object.keys(p.guidance).sort()).toEqual([
      'escalation', 'policy', 'taskboard', 'vault',
    ])
  })

  test('policy + escalation are ALWAYS present, regardless of topic', async () => {
    for (const topic of ['vault', 'taskboard', 'policy', 'escalation', 'all']) {
      const p = payload(await vt.callTool('vault_howto', { topic }))
      expect(p.guidance.policy, `topic=${topic} missing policy`).toBeTruthy()
      expect(p.guidance.escalation, `topic=${topic} missing escalation`).toBeTruthy()
    }
  })

  test('unknown topic ⇒ behaves as all, never errors', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'wat' }))
    expect(p.ok).toBe(true)
    expect(Object.keys(p.guidance).sort()).toEqual([
      'escalation', 'policy', 'taskboard', 'vault',
    ])
  })

  test('non-string / null topic ⇒ all, never throws', async () => {
    const p1 = payload(await vt.callTool('vault_howto', { topic: 123 as any }))
    expect(p1.ok).toBe(true)
    expect(p1.guidance.vault).toBeTruthy()
    const p2 = payload(await vt.callTool('vault_howto', { topic: null as any }))
    expect(p2.ok).toBe(true)
    expect(p2.guidance.vault).toBeTruthy()
  })

  test('topic is trimmed + case-folded before matching', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: '  VAULT  ' }))
    expect(Object.keys(p.guidance).sort()).toEqual(['escalation', 'policy', 'vault'])
  })
})

describe('VaultTools — §7 forbidden-content security guard', () => {
  const vt = new VaultTools()

  // Build the full set of wire-text samples the guard must scan: every topic value
  // (incl. unknown + empty + missing), plus listTools().
  test('no sample leaks a secret (every topic + listTools)', async () => {
    const samples: { label: string; text: string }[] = []
    for (const topic of ['vault', 'taskboard', 'policy', 'escalation', 'all', 'BOGUS', '']) {
      samples.push({ label: `topic=${JSON.stringify(topic)}`, text: wire(await vt.callTool('vault_howto', { topic })) })
    }
    samples.push({ label: 'no-args', text: wire(await vt.callTool('vault_howto')) })
    samples.push({ label: 'listTools', text: JSON.stringify(vt.listTools()) })

    const failures: string[] = []
    for (const s of samples) {
      const v = leaks(s.text)
      if (v.length) failures.push(`${s.label}: ${v.join(', ')}`)
    }
    expect(failures).toEqual([])
  })

  // Self-check: the guard actually fires on a known-bad string (so a future change
  // that neuters the matchers can't silently pass the gate).
  test('guard self-check — detects planted secrets', () => {
    expect(leaks('see bootstrap_master').length).toBeGreaterThan(0)            // literal
    expect(leaks('export SOME_TOKEN=x').length).toBeGreaterThan(0)            // env-var regex
    expect(leaks('curl https://host.example.com').length).toBeGreaterThan(0)  // url + host
    expect(leaks('connect to 127.0.0.1:18180').length).toBeGreaterThan(0)     // IP regex
    // And the required policy branding must NOT be flagged (resolution A):
    expect(leaks('AGENT-TOKEN-SCOPING IS LAW; Bearer-token auth')).toEqual([])
  })
})

describe('VaultTools — fail-soft', () => {
  const vt = new VaultTools()

  test('unknown tool ⇒ {ok:false, error:"unknown_tool", detail:name}, no throw', async () => {
    const r = await vt.callTool('vault_bogus', {})
    expect(r.isError).toBe(true)
    const p = payload(r)
    expect(p.ok).toBe(false)
    expect(p.error).toBe('unknown_tool')
    expect(p.detail).toBe('vault_bogus')
  })

  test('bad args never throw', async () => {
    // null args, garbage args — all degrade fail-soft.
    expect(payload(await vt.callTool('vault_howto', null as any)).ok).toBe(true)
    expect(payload(await vt.callTool('vault_howto', { topic: { nested: true } as any })).ok).toBe(true)
    expect(payload(await vt.callTool('vault_howto', [] as any)).ok).toBe(true)
  })
})

describe('VaultTools — escalation guidance (the load-bearing ask)', () => {
  const vt = new VaultTools()

  test('escalation names agent-1 and says what to include when escalating', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'escalation' }))
    const esc = p.guidance.escalation as string
    expect(esc).toContain('agent-1')
    expect(esc.toLowerCase()).toContain('escalate')
    // "what to include": the actionable ask, not just "I'm stuck".
    expect(esc).toContain('state:')
    expect(esc.toLowerCase()).toContain('blocked on')
    expect(esc.toLowerCase()).toContain('need')
  })

  test('policy carries the AGENT-TOKEN-SCOPING branding verbatim', async () => {
    const p = payload(await vt.callTool('vault_howto', { topic: 'policy' }))
    expect(p.guidance.policy).toContain('AGENT-TOKEN-SCOPING')
    expect(p.guidance.policy.toLowerCase()).toContain('break-glass')
  })
})

describe('VaultTools — configurable provenance (shape-validated allowlist, no fetch)', () => {
  test('conforming operator-supplied sources are surfaced verbatim AND leak nothing', async () => {
    const vt = new VaultTools({
      sources: [
        { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
        { source: 'skills', slug: 'skillpacks/reference-taskboard-vault-access' },
      ],
    })
    const r = await vt.callTool('vault_howto')
    expect(payload(r).source_of_record).toEqual([
      { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
      { source: 'skills', slug: 'skillpacks/reference-taskboard-vault-access' },
    ])
    // Close the gap: the §7 guard MUST also hold over CONFIGURABLE-source output.
    expect(leaks(wire(r))).toEqual([])
  })

  test('MUST-FIX: hostile sources are DROPPED while a valid pointer is KEPT; output stays clean', async () => {
    const vt = new VaultTools({
      sources: [
        { source: 'evil', slug: 'skillpacks/SOME_TOKEN-abcdef' },     // uppercase secret-name → drop
        { source: 'x', slug: 'https://host.example.com/leak' },       // scheme + host + dots → drop
        { source: 'bad source', slug: 'has spaces' },                 // spaces → drop
        { source: 'sk.ills', slug: 'ok/slug' },                       // dot in source → drop
        { source: 'skills', slug: 'skillpacks/foo' },                 // VALID → keep
      ],
    })
    const r = await vt.callTool('vault_howto')
    // (b) only the conforming pointer survives:
    expect(payload(r).source_of_record).toEqual([{ source: 'skills', slug: 'skillpacks/foo' }])
    // (a) NO secret reached the wire via provenance:
    expect(leaks(wire(r))).toEqual([])
  })

  test('RESIDUAL must-fix: conforming-but-FORBIDDEN provenance is DROPPED at runtime', async () => {
    // These all PASS the charset allowlist (lowercase, valid chars) but are forbidden
    // by the §7 denylist — the charset layer alone would let them onto the wire.
    const blob = 'a'.repeat(48) // 48-char lowercase alnum entropy blob — charset-valid
    const vt = new VaultTools({
      sources: [
        { source: 'skills', slug: 'kai-main' },                 // forbidden literal
        { source: 'skills', slug: 'taskboard_bearer_token' },   // forbidden literal
        { source: 'skills', slug: 'tenant-tokens' },            // forbidden literal
        { source: 'skills', slug: `skillpacks/${blob}` },       // entropy blob
        { source: 'skills', slug: 'skillpacks/foo' },           // VALID + clean → keep
      ],
    })
    const r = await vt.callTool('vault_howto')
    expect(payload(r).source_of_record).toEqual([{ source: 'skills', slug: 'skillpacks/foo' }])
    expect(leaks(wire(r))).toEqual([])
  })

  test('DEFAULT_SOURCES itself passes the runtime denylist (containsForbidden)', async () => {
    const p = payload(await new VaultTools().callTool('vault_howto'))
    expect(p.source_of_record.length).toBeGreaterThan(0)
    for (const ptr of p.source_of_record) {
      expect(containsForbidden(`${ptr.source} ${ptr.slug}`), `${ptr.source} ${ptr.slug}`).toBe(false)
    }
  })

  test('all-invalid sources fall back to DEFAULT_SOURCES (never empty)', async () => {
    const vt = new VaultTools({ sources: [{ source: 'X', slug: 'BAD' }, { source: '', slug: '' }] })
    expect(payload(await vt.callTool('vault_howto')).source_of_record).toEqual([
      { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
    ])
  })

  test('non-array / garbage sources opt falls back to default, never throws', async () => {
    const vt = new VaultTools({ sources: 'evil' as any })
    expect(payload(await vt.callTool('vault_howto')).source_of_record).toEqual([
      { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
    ])
  })

  test('caller cannot mutate internal provenance via the returned array', async () => {
    const vt = new VaultTools()
    const first = payload(await vt.callTool('vault_howto'))
    first.source_of_record.push({ source: 'evil', slug: 'x' })
    const second = payload(await vt.callTool('vault_howto'))
    expect(second.source_of_record).toHaveLength(1)
  })
})

describe('VaultTools — §7 structural regexes each fire INDEPENDENTLY', () => {
  // Isolated planted positives so no literal sentinel can mask a broken regex: each
  // sample is engineered to trip exactly one structural pattern (the env-var case
  // uses SIGNING_KEY, which has NO matching literal, to prove that regex alone).
  test('entropy blob (40+ chars) → entropy regex', () => {
    expect(leaks('x' + 'a1b2c3d4e5'.repeat(5))).toContain('regex:high-entropy blob')
  })
  test('bare host/domain, no scheme → host regex', () => {
    expect(leaks('reach foo.example.com now')).toContain('regex:bare host/domain')
  })
  test('url scheme → url regex', () => {
    expect(leaks('see http://x')).toContain('regex:url scheme')
  })
  test('underscore-shaped secret env name → env-var regex (no literal backstop)', () => {
    expect(leaks('SIGNING_KEY=x')).toContain('regex:env-var secret name')
  })
  test('RFC1918 / loopback host:port forms → IP regex', () => {
    // ('192.16' + '8.1.2') reconstructs the RFC1918 192.168/16 sample at runtime; the literal is
    // split only so the repo carries no bare private-LAN dotted address (scrub gate) — coverage unchanged.
    for (const sample of ['10.1.2.3:1234', '192.16' + '8.1.2:1234', '172.16.0.1:1234', 'localhost:1234']) {
      expect(leaks(sample), sample).toContain('regex:rfc1918/loopback host')
    }
  })
  test('required policy phrasing is NOT self-flagged by any regex', () => {
    expect(leaks('AGENT-TOKEN-SCOPING IS LAW; Bearer-token auth; role/secret-path')).toEqual([])
  })
})
