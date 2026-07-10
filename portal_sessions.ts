import { createHash, randomUUID } from 'node:crypto'
import { redactPortalPayload } from './portal_events.ts'

export type PortalRole = 'viewer' | 'chatter' | 'sender' | 'admin'

export type PortalSessionScope = {
  tenant: string
  recipients: string[]
  channels: string[]
  inbox_agents: string[]
}

export type PortalSession = {
  session_id: string
  operator_id: string
  role: PortalRole
  scope: PortalSessionScope
}

export type PortalAuthzDecision = {
  ok: boolean
  reason: string
}

export type PortalAuditRecord = {
  ts: string
  session_id: string
  operator_id: string
  role: PortalRole
  action: string
  target: string
  outcome: 'allowed' | 'denied' | 'failed'
  reason: string
  corr: string | null
  thread: string | null
  body_preview: string
  body_sha256: string | null
}

const ROLES = new Set<PortalRole>(['viewer', 'chatter', 'sender', 'admin'])

export function portalSessionFromRequest(req: Request, env: Record<string, string | undefined> = process.env): PortalSession {
  const cookieSession = sessionCookie(req.headers.get('cookie'))
  const headerSession = tokenHeader(req.headers.get('x-portal-session-id'))
  const operator = tokenHeader(req.headers.get('x-portal-operator')) ?? tokenHeader(env.A2A_PORTAL_OPERATOR_ID) ?? 'operator'
  const role = roleFromValue(req.headers.get('x-portal-role')) ?? roleFromValue(env.A2A_PORTAL_DEFAULT_ROLE) ?? 'sender'
  return {
    session_id: headerSession ?? cookieSession ?? `portal-${randomUUID()}`,
    operator_id: operator,
    role,
    scope: {
      tenant: tokenHeader(req.headers.get('x-portal-tenant')) ?? tokenHeader(env.A2A_PORTAL_TENANT) ?? 'default',
      recipients: csv(req.headers.get('x-portal-recipients') ?? env.A2A_PORTAL_ALLOWED_RECIPIENTS ?? '*'),
      channels: csv(req.headers.get('x-portal-channels') ?? env.A2A_PORTAL_ALLOWED_CHANNELS ?? '*'),
      inbox_agents: csv(req.headers.get('x-portal-inbox-agents') ?? env.A2A_PORTAL_ALLOWED_INBOX_AGENTS ?? '*'),
    },
  }
}

export function authorizePortalSend(session: PortalSession, target: string): PortalAuthzDecision {
  if (session.role === 'viewer') return { ok: false, reason: 'role_viewer_cannot_send' }
  if (session.role === 'chatter' && !target.startsWith('topic:')) return { ok: false, reason: 'role_chatter_dm_denied' }
  if (!isAllowed(target, session.scope.recipients)) return { ok: false, reason: 'recipient_not_allowed' }
  return { ok: true, reason: 'allowed' }
}

export function auditPortalAction(args: {
  session: PortalSession
  action: string
  target: string
  outcome: PortalAuditRecord['outcome']
  reason?: string
  corr?: string | null
  thread?: string | null
  body?: unknown
}): PortalAuditRecord {
  const preview = previewBody(args.body)
  return {
    ts: new Date().toISOString(),
    session_id: args.session.session_id,
    operator_id: args.session.operator_id,
    role: args.session.role,
    action: args.action,
    target: args.target,
    outcome: args.outcome,
    reason: args.reason ?? '',
    corr: args.corr ?? null,
    thread: args.thread ?? null,
    body_preview: preview,
    body_sha256: preview ? createHash('sha256').update(preview).digest('hex') : null,
  }
}

export function isAllowed(value: string, allowlist: string[]): boolean {
  if (allowlist.includes('*')) return true
  const normalized = normalize(value)
  if (allowlist.includes(normalized)) return true
  if (normalized.startsWith('topic:')) return allowlist.includes(`#${normalized.slice(6)}`)
  return allowlist.includes(`@${normalized}`)
}

function sessionCookie(cookie: string | null): string | null {
  if (!cookie) return null
  for (const part of cookie.split(';')) {
    const [key, value] = part.split('=').map((s) => s.trim())
    if (key === 'a2a_portal_session') return tokenHeader(decodeURIComponent(value || ''))
  }
  return null
}

function roleFromValue(value: unknown): PortalRole | null {
  const role = normalize(value)
  return ROLES.has(role as PortalRole) ? role as PortalRole : null
}

function tokenHeader(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const token = value.trim()
  return /^[a-zA-Z0-9_.:@/-]{1,128}$/.test(token) ? token : null
}

function csv(value: unknown): string[] {
  if (typeof value !== 'string' || !value.trim()) return []
  const seen = new Set<string>()
  const out: string[] = []
  for (const item of value.split(',')) {
    const token = normalize(item)
    if (token && !seen.has(token)) {
      seen.add(token)
      out.push(token)
    }
  }
  return out
}

function normalize(value: unknown): string {
  return typeof value === 'string' ? value.trim().toLowerCase() : ''
}

function previewBody(body: unknown): string {
  if (body == null) return ''
  const redacted = redactPortalPayload(body)
  const raw = typeof redacted === 'string' ? redacted : JSON.stringify(redacted)
  return raw.length > 512 ? raw.slice(0, 512) + '[truncated]' : raw
}
