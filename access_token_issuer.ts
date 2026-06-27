import { RedisClient } from 'bun'
import { readFileSync } from 'node:fs'
import { createHash, randomUUID } from 'node:crypto'
import { importEd25519Pub } from './a2a-channel.ts'

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export type AccessDecisionReason =
  | 'allowed'
  | 'bad_request'
  | 'unknown_agent'
  | 'invalid_signature'
  | 'nonce_replay'
  | 'request_expired'
  | 'request_future_skew'
  | 'ttl_too_long'
  | 'scope_denied'
  | 'upstream_scope_unavailable'
  | 'issuer_unavailable'

export type AccessIssuerRequest = {
  agent_id: string
  requested_scope: string
  nonce: string
  issued_at: string
  expiry: string
  signature: string
}

export type AccessIssueAuditRecord = {
  ts: string
  agent_id: string
  requested_scope: string
  decision: 'allow' | 'deny'
  reason: AccessDecisionReason
  lease_id?: string
  ttl_sec?: number
  request_hash: string
  nonce_hash?: string
  runtime_id?: string
}

export type AccessLeaseRecord = {
  lease_id: string
  agent_id: string
  scope: string
  issued_at: string
  expires_at: string
  ttl_sec: number
  delivery: 'brokered'
}

export type AccessPolicyAgentEntry =
  | string[]
  | {
      scopes?: string[]
      max_ttl_sec?: number
    }

export type AccessPolicy = {
  defaults?: {
    max_ttl_sec?: number
  }
  agents?: Record<string, AccessPolicyAgentEntry>
}

export interface AccessIdentityRegistry {
  getPublicKey(agentId: string): Promise<Uint8Array | null>
}

export interface AccessIssuerStore {
  consumeNonce(agentId: string, nonceHash: string, retainUntilMs: number): Promise<'ok' | 'replay'>
  writeAudit(record: AccessIssueAuditRecord): Promise<void>
  storeLease(record: AccessLeaseRecord): Promise<void>
}

export interface AccessTokenIssuerOpts {
  registry: AccessIdentityRegistry
  store: AccessIssuerStore
  policy?: AccessPolicy
  nowMs?: () => number
  genLeaseId?: () => string
  runtimeId?: string
  maxFutureSkewSec?: number
}

export interface AccessTokenIssuerToolsOpts {
  issuer?: AccessTokenIssuer
  redis?: RedisLike
  policy?: AccessPolicy
  policyJson?: string
  policyFile?: string
  runtimeId?: string
}

type RedisLike = {
  get(key: string): Promise<string | null>
  send(cmd: string, args: string[]): Promise<any>
}

const DEFAULT_MAX_TTL_SEC = 900
const DEFAULT_MAX_FUTURE_SKEW_SEC = 60
const DEFAULT_REDIS_TIMEOUT_MS = 2500
const DEFAULT_NONCE_RETENTION_MS = 15 * 60 * 1000
const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const BASE64URL_RE = /^[A-Za-z0-9_-]+$/
const POLICY_SCOPE_MAX = 512
const enc = new TextEncoder()

const REQUEST_CANONICAL_KEYS = ['agent_id', 'expiry', 'issued_at', 'nonce', 'requested_scope'] as const

export function canonicalAccessIssuerRequest(req: Pick<AccessIssuerRequest, typeof REQUEST_CANONICAL_KEYS[number]>): string {
  const canonical: Record<string, string> = {}
  for (const key of REQUEST_CANONICAL_KEYS) canonical[key] = String(req[key])
  return JSON.stringify(canonical)
}

export function accessIssuerRequestHash(canonicalRequest: string): string {
  return sha256Hex(canonicalRequest)
}

export function accessIssuerNonceHash(agentId: string, nonce: string): string {
  return sha256Hex(`${agentId}\0${nonce}`)
}

export function sanitizeScopeForAudit(scope: string): string {
  if (scope.startsWith('vault:path:') && scope.endsWith(':read')) return 'vault:path:[redacted]:read'
  return scope
}

export function isAllowedAccessScope(scope: string): boolean {
  return validateRequestedScope(scope).ok
}

export function scopeMatchesPolicy(pattern: string, requestedScope: string): boolean {
  if (!pattern || pattern.length > POLICY_SCOPE_MAX) return false
  if (!isAllowedPolicyPattern(pattern)) return false
  const re = new RegExp(`^${escapeRegExp(pattern).replace(/\\\*/g, '[^:]*')}$`)
  return re.test(requestedScope)
}

export class RedisA2AIdentityRegistry implements AccessIdentityRegistry {
  constructor(
    private readonly redis: RedisLike,
    private readonly keyPrefix = process.env.A2A_PUBKEY_KEY_PREFIX ?? 'alloyium:a2a:pubkey:',
    private readonly timeoutMs = Number(process.env.REDIS_TIMEOUT_MS ?? DEFAULT_REDIS_TIMEOUT_MS),
  ) {}

  async getPublicKey(agentId: string): Promise<Uint8Array | null> {
    const raw = await redisTimeout(this.redis.get(this.keyPrefix + agentId), this.timeoutMs, 'redis.get(pubkey)')
    if (!raw) return null
    const pub = new Uint8Array(Buffer.from(raw.trim(), 'base64'))
    return pub.length === 32 ? pub : null
  }
}

export class RedisAccessIssuerStore implements AccessIssuerStore {
  constructor(
    private readonly redis: RedisLike,
    private readonly opts: {
      noncePrefix?: string
      auditKey?: string
      leasePrefix?: string
      timeoutMs?: number
    } = {},
  ) {}

  async consumeNonce(agentId: string, nonceHash: string, retainUntilMs: number): Promise<'ok' | 'replay'> {
    const ttlSec = Math.max(1, Math.ceil((retainUntilMs - Date.now()) / 1000))
    const key = `${this.opts.noncePrefix ?? 'alloyium:a2a:access:nonce:'}${agentId}:${nonceHash}`
    const res = await this.send('SET', [key, '1', 'NX', 'EX', String(ttlSec)])
    return res ? 'ok' : 'replay'
  }

  async writeAudit(record: AccessIssueAuditRecord): Promise<void> {
    const key = this.opts.auditKey ?? 'alloyium:a2a:access:audit'
    await this.send('LPUSH', [key, JSON.stringify(record)])
    await this.send('LTRIM', [key, '0', '9999']).catch(() => {})
  }

  async storeLease(record: AccessLeaseRecord): Promise<void> {
    const key = `${this.opts.leasePrefix ?? 'alloyium:a2a:access:lease:'}${record.lease_id}`
    await this.send('SET', [key, JSON.stringify(record), 'EX', String(Math.max(1, record.ttl_sec))])
  }

  private send(cmd: string, args: string[]): Promise<any> {
    return redisTimeout(this.redis.send(cmd, args), this.opts.timeoutMs ?? DEFAULT_REDIS_TIMEOUT_MS, `redis.${cmd}`)
  }
}

export class AccessTokenIssuer {
  private readonly nowMs: () => number
  private readonly genLeaseId: () => string
  private readonly runtimeId?: string
  private readonly maxFutureSkewSec: number

  constructor(private readonly opts: AccessTokenIssuerOpts) {
    this.nowMs = opts.nowMs ?? Date.now
    this.genLeaseId = opts.genLeaseId ?? randomUUID
    this.runtimeId = opts.runtimeId
    this.maxFutureSkewSec = opts.maxFutureSkewSec ?? DEFAULT_MAX_FUTURE_SKEW_SEC
  }

  async issue(raw: Record<string, any>): Promise<Record<string, any>> {
    const parsed = parseRequest(raw)
    if (!parsed.ok) return this.errorWithoutAudit(parsed.reason)
    const req = parsed.req
    const canonical = canonicalAccessIssuerRequest(req)
    const requestHash = accessIssuerRequestHash(canonical)
    const nonceHash = accessIssuerNonceHash(req.agent_id, req.nonce)
    const auditBase = {
      ts: new Date(this.nowMs()).toISOString(),
      agent_id: req.agent_id,
      requested_scope: sanitizeScopeForAudit(req.requested_scope),
      request_hash: requestHash,
      nonce_hash: nonceHash,
      ...(this.runtimeId ? { runtime_id: this.runtimeId } : {}),
    }

    let publicKey: Uint8Array | null
    try {
      publicKey = await this.opts.registry.getPublicKey(req.agent_id)
    } catch {
      return await this.deny(auditBase, 'issuer_unavailable')
    }
    if (!publicKey) return await this.deny(auditBase, 'unknown_agent')

    const signature = decodeBase64Url(req.signature)
    if (!signature || signature.length !== 64) return await this.deny(auditBase, 'invalid_signature')
    let verified = false
    try {
      const key = await importEd25519Pub(publicKey)
      verified = await crypto.subtle.verify({ name: 'Ed25519' }, key, signature, enc.encode(canonical))
    } catch {
      verified = false
    }
    if (!verified) return await this.deny(auditBase, 'invalid_signature')

    const issuedAtMs = Date.parse(req.issued_at)
    const expiryMs = Date.parse(req.expiry)
    const retainUntilMs = Math.max(expiryMs || 0, this.nowMs() + DEFAULT_NONCE_RETENTION_MS)
    try {
      const nonce = await this.opts.store.consumeNonce(req.agent_id, nonceHash, retainUntilMs)
      if (nonce === 'replay') return await this.deny(auditBase, 'nonce_replay')
    } catch {
      return await this.deny(auditBase, 'issuer_unavailable')
    }

    const timeReason = this.validateTime(issuedAtMs, expiryMs)
    if (timeReason) return await this.deny(auditBase, timeReason)

    const scopeValidation = validateRequestedScope(req.requested_scope)
    if (!scopeValidation.ok) return await this.deny(auditBase, 'scope_denied')

    const requestedTtlSec = Math.ceil((expiryMs - issuedAtMs) / 1000)
    const policy = evaluatePolicy(this.opts.policy ?? {}, req.agent_id, req.requested_scope, requestedTtlSec)
    if (!policy.ok) return await this.deny(auditBase, policy.reason)

    const ttlSec = Math.max(1, Math.ceil((expiryMs - this.nowMs()) / 1000))
    const leaseId = this.genLeaseId()
    const expiresAt = new Date(expiryMs).toISOString()
    const lease: AccessLeaseRecord = {
      lease_id: leaseId,
      agent_id: req.agent_id,
      scope: req.requested_scope,
      issued_at: new Date(this.nowMs()).toISOString(),
      expires_at: expiresAt,
      ttl_sec: ttlSec,
      delivery: 'brokered',
    }

    try {
      await this.opts.store.storeLease(lease)
      await this.opts.store.writeAudit({
        ...auditBase,
        decision: 'allow',
        reason: 'allowed',
        lease_id: leaseId,
        ttl_sec: ttlSec,
      })
    } catch {
      return { ok: false, error: 'issuer_unavailable' }
    }

    return {
      ok: true,
      lease_id: leaseId,
      scope: req.requested_scope,
      expires_at: expiresAt,
      token_ref: `lease:${leaseId}`,
      delivery: 'brokered',
    }
  }

  private validateTime(issuedAtMs: number, expiryMs: number): AccessDecisionReason | null {
    const now = this.nowMs()
    if (!Number.isFinite(issuedAtMs) || !Number.isFinite(expiryMs) || expiryMs <= issuedAtMs) return 'request_expired'
    if (expiryMs <= now) return 'request_expired'
    if (issuedAtMs > now + this.maxFutureSkewSec * 1000) return 'request_future_skew'
    return null
  }

  private async deny(
    auditBase: Omit<AccessIssueAuditRecord, 'decision' | 'reason'>,
    reason: AccessDecisionReason,
  ): Promise<Record<string, any>> {
    try {
      await this.opts.store.writeAudit({ ...auditBase, decision: 'deny', reason })
    } catch {
      return { ok: false, error: 'issuer_unavailable' }
    }
    return { ok: false, error: reason }
  }

  private errorWithoutAudit(reason: AccessDecisionReason): Record<string, any> {
    return { ok: false, error: reason }
  }
}

export class AccessTokenIssuerTools {
  private readonly issuer: AccessTokenIssuer

  static readonly TOOL_NAMES = ['a2a_issue_scoped_token'] as const

  static readonly INSTRUCTIONS =
    ' You also have a signed access-token issuer tool: a2a_issue_scoped_token ' +
    'verifies an ed25519-signed request against the A2A public identity registry, ' +
    'enforces deny-by-default scope policy, rejects nonce replay and stale/skewed ' +
    'requests, and returns only a short-lived brokered lease reference. It never ' +
    'logs or returns raw credential material.'

  constructor(opts: AccessTokenIssuerToolsOpts = {}) {
    if (opts.issuer) {
      this.issuer = opts.issuer
      return
    }
    const redis = opts.redis ?? new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379')
    const policy = opts.policy ?? loadPolicy(opts)
    this.issuer = new AccessTokenIssuer({
      registry: new RedisA2AIdentityRegistry(redis),
      store: new RedisAccessIssuerStore(redis),
      policy,
      runtimeId: opts.runtimeId ?? process.env.A2A_AGENT_ID,
    })
  }

  handles(name: string): boolean {
    return (AccessTokenIssuerTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return [
      {
        name: 'a2a_issue_scoped_token',
        description:
          'Issue a short-lived brokered lease for taskboard, Forgejo, or Vault after ' +
          'verifying an ed25519 signature over the canonical request payload against ' +
          'the A2A public identity registry. Deny-by-default; audits decisions without ' +
          'raw token values.',
        inputSchema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            agent_id: { type: 'string', description: 'Registered A2A agent id.' },
            requested_scope: { type: 'string', description: 'Requested taskboard, Forgejo, or Vault scope.' },
            nonce: { type: 'string', description: 'Base64url random nonce, at least 128 bits.' },
            issued_at: { type: 'string', description: 'Request issue time as ISO-8601.' },
            expiry: { type: 'string', description: 'Request/lease expiry as ISO-8601.' },
            signature: {
              type: 'string',
              description:
                'Base64url ed25519 signature over canonical JSON of agent_id, expiry, issued_at, nonce, and requested_scope.',
            },
          },
          required: ['agent_id', 'requested_scope', 'nonce', 'issued_at', 'expiry', 'signature'],
        },
      },
    ]
  }

  async callTool(name: string, args: Record<string, any> = {}): Promise<ToolResult> {
    try {
      switch (name) {
        case 'a2a_issue_scoped_token':
          return this.result(await this.issuer.issue(args), false)
        default:
          return this.result({ ok: false, error: 'unknown_tool', detail: name }, true)
      }
    } catch {
      return this.result({ ok: false, error: 'issuer_unavailable' }, true)
    }
  }

  private result(obj: unknown, forceError = false): ToolResult {
    const isError = forceError || (typeof obj === 'object' && obj !== null && (obj as any).ok === false)
    return { content: [{ type: 'text', text: JSON.stringify(obj) }], ...(isError ? { isError: true } : {}) }
  }
}

function parseRequest(raw: Record<string, any>): { ok: true; req: AccessIssuerRequest } | { ok: false; reason: AccessDecisionReason } {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return { ok: false, reason: 'bad_request' }
  const req: AccessIssuerRequest = {
    agent_id: str(raw.agent_id),
    requested_scope: str(raw.requested_scope),
    nonce: str(raw.nonce),
    issued_at: str(raw.issued_at),
    expiry: str(raw.expiry),
    signature: str(raw.signature),
  }
  if (!AGENT_ID_RE.test(req.agent_id)) return { ok: false, reason: 'bad_request' }
  if (!req.requested_scope || req.requested_scope.length > POLICY_SCOPE_MAX) return { ok: false, reason: 'bad_request' }
  if (!validNonce(req.nonce)) return { ok: false, reason: 'bad_request' }
  if (!Number.isFinite(Date.parse(req.issued_at)) || !Number.isFinite(Date.parse(req.expiry))) return { ok: false, reason: 'bad_request' }
  if (!BASE64URL_RE.test(req.signature)) return { ok: false, reason: 'bad_request' }
  return { ok: true, req }
}

function evaluatePolicy(policy: AccessPolicy, agentId: string, scope: string, requestedTtlSec: number): { ok: true } | { ok: false; reason: AccessDecisionReason } {
  const entry = policy.agents?.[agentId]
  if (!entry) return { ok: false, reason: 'scope_denied' }
  const scopes = Array.isArray(entry) ? entry : Array.isArray(entry.scopes) ? entry.scopes : []
  const maxTtlSec = Math.max(1, Number((Array.isArray(entry) ? undefined : entry.max_ttl_sec) ?? policy.defaults?.max_ttl_sec ?? DEFAULT_MAX_TTL_SEC) || DEFAULT_MAX_TTL_SEC)
  if (!scopes.some((pattern) => scopeMatchesPolicy(String(pattern), scope))) return { ok: false, reason: 'scope_denied' }
  if (requestedTtlSec > maxTtlSec) return { ok: false, reason: 'ttl_too_long' }
  return { ok: true }
}

function validateRequestedScope(scope: string): { ok: true } | { ok: false } {
  if (scope.length > POLICY_SCOPE_MAX || /[\s\0]/.test(scope)) return { ok: false }

  const taskProject = /^taskboard:project:(\d+):(read|task:create)$/.exec(scope)
  if (taskProject) return { ok: true }
  const taskTask = /^taskboard:task:(\d+):(update|move)$/.exec(scope)
  if (taskTask) return { ok: true }

  const forgejo = /^forgejo:repo:([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+):(read|pr:(?:create|review|merge)|branch:push:(.+))$/.exec(scope)
  if (forgejo) {
    const branch = forgejo[3]
    return !branch || validBranchName(branch) ? { ok: true } : { ok: false }
  }

  const vault = /^vault:path:([^:]+):read$/.exec(scope)
  if (vault) return validLogicalPath(vault[1]) ? { ok: true } : { ok: false }

  return { ok: false }
}

function isAllowedPolicyPattern(pattern: string): boolean {
  const probe = pattern.replace(/\*/g, '1')
  return validateRequestedScope(probe).ok
}

function validBranchName(branch: string): boolean {
  if (!branch || branch.length > 160) return false
  if (branch.includes('..') || branch.includes('//') || branch.startsWith('/') || branch.endsWith('/')) return false
  if (branch.startsWith('.') || branch.endsWith('.') || branch.endsWith('.lock')) return false
  if (/[~^:?*[\]\\\s\0]/.test(branch)) return false
  return true
}

function validLogicalPath(path: string): boolean {
  if (!path || path.length > 240) return false
  if (path.includes('..') || path.includes('//') || path.startsWith('/') || path.endsWith('/')) return false
  return /^[A-Za-z0-9_./=-]+$/.test(path)
}

function validNonce(nonce: string): boolean {
  const decoded = decodeBase64Url(nonce)
  return !!decoded && decoded.length >= 16 && decoded.length <= 96
}

function loadPolicy(opts: AccessTokenIssuerToolsOpts): AccessPolicy {
  const raw = opts.policyJson ?? process.env.A2A_ACCESS_POLICY_JSON
  if (raw && raw.trim()) return parsePolicy(raw)
  const file = opts.policyFile ?? process.env.A2A_ACCESS_POLICY_FILE
  if (file && file.trim()) return parsePolicy(readFileSync(file, 'utf8'))
  return { defaults: { max_ttl_sec: DEFAULT_MAX_TTL_SEC }, agents: {} }
}

function parsePolicy(raw: string): AccessPolicy {
  try {
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { agents: {} }
    return parsed as AccessPolicy
  } catch {
    return { agents: {} }
  }
}

function decodeBase64Url(s: string): Uint8Array | null {
  if (!s || !BASE64URL_RE.test(s)) return null
  const base64 = s.replace(/-/g, '+').replace(/_/g, '/')
  const rem = base64.length % 4
  if (rem === 1) return null
  try {
    return new Uint8Array(Buffer.from(base64 + '='.repeat((4 - rem) % 4), 'base64'))
  } catch {
    return null
  }
}

function sha256Hex(s: string): string {
  return createHash('sha256').update(s).digest('hex')
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function str(v: unknown): string {
  return typeof v === 'string' ? v.trim() : ''
}

function redisTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<T>((_, reject) => {
    t = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
  })
  return Promise.race([p, timeout]).finally(() => { if (t) clearTimeout(t) })
}
