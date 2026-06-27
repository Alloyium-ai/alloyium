import { afterEach, describe, expect, test } from 'bun:test'
import {
  ACCESS_TOKEN_REQUEST_DOMAIN,
  AccessTokenIssuer,
  AccessTokenIssuerTools,
  canonicalAccessIssuerRequest,
  scopeMatchesPolicy,
  type AccessIdentityRegistry,
  type AccessIssueAuditRecord,
  type AccessIssuerRequest,
  type AccessIssuerStore,
  type AccessLeaseRecord,
  type AccessPolicy,
} from '../access_token_issuer.ts'

const NOW = Date.parse('2026-06-27T14:00:00.000Z')
const enc = new TextEncoder()

class MemoryRegistry implements AccessIdentityRegistry {
  readonly keys = new Map<string, Uint8Array>()
  async getPublicKey(agentId: string): Promise<Uint8Array | null> {
    return this.keys.get(agentId) ?? null
  }
}

class MemoryStore implements AccessIssuerStore {
  readonly nonces = new Set<string>()
  readonly audits: AccessIssueAuditRecord[] = []
  readonly leases: AccessLeaseRecord[] = []

  async consumeNonce(agentId: string, nonceHash: string): Promise<'ok' | 'replay'> {
    const key = `${agentId}:${nonceHash}`
    if (this.nonces.has(key)) return 'replay'
    this.nonces.add(key)
    return 'ok'
  }

  async writeAudit(record: AccessIssueAuditRecord): Promise<void> {
    this.audits.push(record)
  }

  async storeLease(record: AccessLeaseRecord): Promise<void> {
    this.leases.push(record)
  }
}

type Fixture = {
  issuer: AccessTokenIssuer
  registry: MemoryRegistry
  store: MemoryStore
  privateKey: CryptoKey
}

const policy: AccessPolicy = {
  defaults: { max_ttl_sec: 900 },
  agents: {
    'agent-a': {
      max_ttl_sec: 900,
      scopes: [
        'taskboard:project:13:task:create',
        'forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*',
        'vault:path:team/*:read',
      ],
    },
  },
}

afterEach(() => {
  console.error = originalConsoleError
  console.log = originalConsoleLog
})

const originalConsoleError = console.error
const originalConsoleLog = console.log

async function fixture(): Promise<Fixture> {
  const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
  const pub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
  const registry = new MemoryRegistry()
  registry.keys.set('agent-a', pub)
  const store = new MemoryStore()
  const issuer = new AccessTokenIssuer({
    registry,
    store,
    policy,
    nowMs: () => NOW,
    genLeaseId: () => 'lease-0001',
    runtimeId: 'test-runtime',
  })
  return { issuer, registry, store, privateKey: kp.privateKey }
}

async function signedRequest(privateKey: CryptoKey, over: Partial<AccessIssuerRequest> = {}): Promise<AccessIssuerRequest> {
  const req = {
    agent_id: 'agent-a',
    requested_scope: 'taskboard:project:13:task:create',
    nonce: b64url(crypto.getRandomValues(new Uint8Array(16))),
    issued_at: '2026-06-27T14:00:00.000Z',
    expiry: '2026-06-27T14:10:00.000Z',
    ...over,
  }
  const sig = new Uint8Array(await crypto.subtle.sign({ name: 'Ed25519' }, privateKey, enc.encode(canonicalAccessIssuerRequest(req))))
  return { ...req, signature: b64url(sig) }
}

describe('AccessTokenIssuerTools', () => {
  test('registers the scoped-token issuer schema', () => {
    const issuer = new AccessTokenIssuer({
      registry: new MemoryRegistry(),
      store: new MemoryStore(),
      policy,
      nowMs: () => NOW,
    })
    const tools = new AccessTokenIssuerTools({ issuer }).listTools()
    expect(tools).toHaveLength(1)
    expect(tools[0].name).toBe('a2a_issue_scoped_token')
    expect(tools[0].inputSchema.required).toEqual([
      'agent_id', 'requested_scope', 'nonce', 'issued_at', 'expiry', 'signature',
    ])
    expect(AccessTokenIssuerTools.INSTRUCTIONS.startsWith(' ')).toBe(true)
  })
})

describe('canonical access request encoding', () => {
  test('uses a domain-separated pipe encoding with escaped fields', () => {
    const canonical = canonicalAccessIssuerRequest({
      agent_id: 'agent-a',
      expiry: '2026-06-27T14:10:00.000Z',
      issued_at: '2026-06-27T14:00:00.000Z',
      nonce: 'abc_123',
      requested_scope: String.raw`vault:path:team/a|b\c:read`,
    })

    expect(canonical).toBe(
      String.raw`${ACCESS_TOKEN_REQUEST_DOMAIN}|agent-a|2026-06-27T14:10:00.000Z|2026-06-27T14:00:00.000Z|abc_123|vault:path:team/a\|b\\c:read`,
    )
  })
})

describe('access scope policy matching', () => {
  test('supports bounded wildcards for numeric ids, repos, Vault paths, and branches', () => {
    expect(scopeMatchesPolicy('taskboard:project:*:read', 'taskboard:project:13:read')).toBe(true)
    expect(scopeMatchesPolicy('taskboard:task:*:move', 'taskboard:task:42:move')).toBe(true)
    expect(scopeMatchesPolicy('forgejo:repo:*/*:pr:review', 'forgejo:repo:Alloyium-ai/alloyium:pr:review')).toBe(true)
    expect(scopeMatchesPolicy('vault:path:team/*:read', 'vault:path:team/alpha:read')).toBe(true)
    expect(scopeMatchesPolicy('forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*', 'forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/feature')).toBe(true)
    expect(scopeMatchesPolicy('forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*', 'forgejo:repo:Alloyium-ai/alloyium:branch:push:main')).toBe(false)
  })
})

describe('access role policy evaluation', () => {
  test('allows scopes through reusable roles assigned to an agent', async () => {
    const f = await fixture()
    const issuer = new AccessTokenIssuer({
      registry: f.registry,
      store: f.store,
      policy: {
        defaults: { max_ttl_sec: 900 },
        roles: {
          developer: {
            scopes: ['forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*'],
            max_ttl_sec: 600,
          },
        },
        agent_roles: { 'agent-a': ['developer'] },
      },
      nowMs: () => NOW,
      genLeaseId: () => 'lease-role',
    })
    const req = await signedRequest(f.privateKey, {
      requested_scope: 'forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/role-policy',
    })

    const res = await issuer.issue(req)

    expect(res).toMatchObject({ ok: true, lease_id: 'lease-role' })
  })
})

describe('AccessTokenIssuer', () => {
  test('allowed request succeeds and emits a brokered lease audit', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey)

    const res = await f.issuer.issue(req)

    expect(res).toMatchObject({
      ok: true,
      lease_id: 'lease-0001',
      scope: 'taskboard:project:13:task:create',
      expires_at: '2026-06-27T14:10:00.000Z',
      token_ref: 'lease:lease-0001',
      delivery: 'brokered',
    })
    expect(f.store.leases).toHaveLength(1)
    expect(f.store.audits).toHaveLength(1)
    expect(f.store.audits[0]).toMatchObject({
      agent_id: 'agent-a',
      requested_scope: 'taskboard:project:13:task:create',
      decision: 'allow',
      reason: 'allowed',
      lease_id: 'lease-0001',
      ttl_sec: 600,
      lifecycle: 'brokered',
      revocable: false,
      runtime_id: 'test-runtime',
    })
  })

  test('unauthorized scope is denied after signature verification and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      requested_scope: 'forgejo:repo:Alloyium-ai/alloyium:branch:push:main',
    })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'scope_denied' })
    expect(f.store.leases).toHaveLength(0)
    expect(f.store.audits).toHaveLength(1)
    expect(f.store.audits[0]).toMatchObject({
      decision: 'deny',
      reason: 'scope_denied',
      requested_scope: 'forgejo:repo:Alloyium-ai/alloyium:branch:push:main',
    })
  })

  test('unknown agent is denied and audited without issuing a lease', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, { agent_id: 'agent-missing' })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'unknown_agent' })
    expect(f.store.leases).toHaveLength(0)
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'unknown_agent' })
  })

  test('bad signature is denied and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey)
    req.requested_scope = 'forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/feature'

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'invalid_signature' })
    expect(f.store.leases).toHaveLength(0)
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'invalid_signature' })
  })

  test('replayed nonce is denied and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey)

    expect((await f.issuer.issue(req)).ok).toBe(true)
    const replay = await f.issuer.issue(req)

    expect(replay).toEqual({ ok: false, error: 'nonce_replay' })
    expect(f.store.leases).toHaveLength(1)
    expect(f.store.audits.map((a) => a.reason)).toEqual(['allowed', 'nonce_replay'])
  })

  test('expired request is denied and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      issued_at: '2026-06-27T13:40:00.000Z',
      expiry: '2026-06-27T13:50:00.000Z',
    })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'request_expired' })
    expect(f.store.leases).toHaveLength(0)
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'request_expired' })
  })

  test('future-skewed request is denied and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      issued_at: '2026-06-27T14:02:01.000Z',
      expiry: '2026-06-27T14:12:00.000Z',
    })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'request_future_skew' })
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'request_future_skew' })
  })

  test('ttl above policy max is denied and audited', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      expiry: '2026-06-27T14:16:00.000Z',
    })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'ttl_too_long' })
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'ttl_too_long' })
  })

  test('audit omits raw nonce, signature, token values, and redacts vault path', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      requested_scope: 'vault:path:team/alpha:read',
    })

    const res = await f.issuer.issue(req)
    const auditText = JSON.stringify(f.store.audits)

    expect(res.ok).toBe(true)
    expect(f.store.audits[0].requested_scope).toBe('vault:path:[redacted]:read')
    expect(auditText).not.toContain(req.nonce)
    expect(auditText).not.toContain(req.signature)
    expect(auditText).not.toContain('lease:lease-0001')
    expect(auditText).not.toContain('team/alpha')
  })

  test('no token or credential material is logged', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey)
    const logs: string[] = []
    console.error = (...args: unknown[]) => { logs.push(args.join(' ')) }
    console.log = (...args: unknown[]) => { logs.push(args.join(' ')) }

    const res = await f.issuer.issue(req)

    expect(res.ok).toBe(true)
    expect(logs.join('\n')).not.toContain(String(res.token_ref))
    expect(logs.join('\n')).not.toContain(req.signature)
    expect(logs.join('\n')).not.toContain(req.nonce)
  })

  test('Forgejo branch scope cannot escape an allowed branch pattern', async () => {
    const f = await fixture()
    const req = await signedRequest(f.privateKey, {
      requested_scope: 'forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/../main',
    })

    const res = await f.issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'scope_denied' })
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'scope_denied' })
  })

  test('Forgejo merge scope is refused by the general issuer even if policy grants it', async () => {
    const f = await fixture()
    const issuer = new AccessTokenIssuer({
      registry: f.registry,
      store: f.store,
      policy: {
        defaults: { max_ttl_sec: 900 },
        agents: {
          'agent-a': ['forgejo:repo:Alloyium-ai/alloyium:pr:merge'],
        },
      },
      nowMs: () => NOW,
      genLeaseId: () => 'lease-merge',
    })
    const req = await signedRequest(f.privateKey, {
      requested_scope: 'forgejo:repo:Alloyium-ai/alloyium:pr:merge',
    })

    const res = await issuer.issue(req)

    expect(res).toEqual({ ok: false, error: 'scope_denied' })
    expect(f.store.leases).toHaveLength(0)
    expect(f.store.audits[0]).toMatchObject({ decision: 'deny', reason: 'scope_denied' })
  })
})

function b64url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
