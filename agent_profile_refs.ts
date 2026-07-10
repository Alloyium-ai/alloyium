import { isAbsolute, relative, resolve } from 'node:path'

export const AGENT_PROFILE_FEATURE_TOKENS = [
  'a2a.profile.v1',
  'a2a.profile.soul-md.v1',
  'a2a.profile.bootstrap-once.v1',
  'a2a.profile.role-template.v1',
] as const

export type AgentProfileFeatureToken = typeof AGENT_PROFILE_FEATURE_TOKENS[number]

export type ProfileLaunchRefs = {
  role?: string
  profileId?: string
  soulRef?: string
  profileRequired?: boolean
}

export type ProfileLaunchRefError = 'bad_role' | 'bad_profile_id' | 'bad_soul_ref' | 'bad_profile_required'

export type ProfileLaunchRefParseResult =
  | { ok: true; refs: ProfileLaunchRefs }
  | { ok: false; error: ProfileLaunchRefError }

export type AgentProfileSummary = {
  role?: string
  profile_id?: string
  revision?: string
  source?: string
  soul_bytes?: number
}

export type AgentProfilePresence = {
  features?: AgentProfileFeatureToken[]
  profile?: AgentProfileSummary
}

const PROFILE_ID_RE = /^[a-z0-9-]{1,64}$/
const PROFILE_REF_SEGMENT_RE = /^[a-z0-9][a-z0-9-]{0,63}$/

export function isValidProfileIdentifier(value: unknown): value is string {
  return typeof value === 'string' && PROFILE_ID_RE.test(value)
}

export function parseProfileIdentifier(value: unknown): string | undefined | false {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || value.length > 128 || /[\r\n]/.test(value)) return false
  const s = value.trim()
  return PROFILE_ID_RE.test(s) ? s : false
}

export function parseProfileRef(value: unknown): string | undefined | false {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || value.length > 256 || /[\r\n\\]/.test(value)) return false
  const s = value.trim()
  if (!s || s.startsWith('/') || s.includes('://')) return false
  const parts = s.split('/')
  if (parts.length > 8) return false
  for (const part of parts) {
    if (part === '.' || part === '..' || !PROFILE_REF_SEGMENT_RE.test(part)) return false
  }
  return s
}

export function parseProfileLaunchRefs(input: Record<string, unknown>): ProfileLaunchRefParseResult {
  const role = parseProfileIdentifier(input.role)
  if (role === false) return { ok: false, error: 'bad_role' }
  const profileId = parseProfileIdentifier(input.profile_id)
  if (profileId === false) return { ok: false, error: 'bad_profile_id' }
  const soulRef = parseProfileRef(input.soul_ref)
  if (soulRef === false) return { ok: false, error: 'bad_soul_ref' }
  if (input.profile_required != null && typeof input.profile_required !== 'boolean') {
    return { ok: false, error: 'bad_profile_required' }
  }
  const hasRefs = !!(role || profileId || soulRef)
  return {
    ok: true,
    refs: {
      role,
      profileId,
      soulRef,
      profileRequired: input.profile_required === true || (hasRefs && input.profile_required !== false),
    },
  }
}

export function defaultProfileRoot(channelsDir: string): string {
  return `${channelsDir.replace(/\/+$/, '')}/agents`
}

export function buildProfileEnv(refs: ProfileLaunchRefs, profileRoot: string | undefined): Record<string, string | undefined> {
  const hasRefs = !!(refs.role || refs.profileId || refs.soulRef || refs.profileRequired)
  return {
    CODEX_AGENT_ROLE: refs.role,
    CODEX_AGENT_PROFILE_ID: refs.profileId,
    CODEX_AGENT_SOUL_REF: refs.soulRef,
    CODEX_AGENT_PROFILE_SOURCE: refs.soulRef,
    CODEX_AGENT_PROFILE_ROOT: hasRefs ? profileRoot : undefined,
    CODEX_AGENT_PROFILE_REQUIRED: refs.profileRequired ? '1' : undefined,
  }
}

export function profileSummaryFromEnv(env: Record<string, string | undefined> = process.env): AgentProfileSummary | undefined {
  return sanitizeProfileSummary({
    role: parseProfileIdentifier(env.CODEX_AGENT_ROLE) || undefined,
    profile_id: parseProfileIdentifier(env.CODEX_AGENT_PROFILE_ID) || undefined,
    revision: env.CODEX_AGENT_PROFILE_REVISION_SHORT ?? env.CODEX_AGENT_PROFILE_REVISION,
    source: sourceRefFromEnv(env),
    soul_bytes: env.CODEX_AGENT_SOUL_BYTES,
  })
}

export function profilePresenceFromEnv(env: Record<string, string | undefined> = process.env): AgentProfilePresence {
  const profile = profileSummaryFromEnv(env)
  return profile ? { features: [...AGENT_PROFILE_FEATURE_TOKENS], profile } : {}
}

export function sanitizeProfileSummary(value: unknown): AgentProfileSummary | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined
  const v = value as Record<string, unknown>
  const role = parseProfileIdentifier(v.role) || undefined
  const profileId = parseProfileIdentifier(v.profile_id ?? v.profileId) || undefined
  const revision = sanitizeRevision(v.revision ?? v.profile_revision ?? v.profileRevision)
  const source = sanitizeSourceRef(v.source ?? v.ref ?? v.soul_ref ?? v.soulRef)
  const soulBytes = sanitizeSoulBytes(v.soul_bytes ?? v.soulBytes)
  const out: AgentProfileSummary = {}
  if (role) out.role = role
  if (profileId) out.profile_id = profileId
  if (revision) out.revision = revision
  if (source) out.source = source
  if (soulBytes !== undefined) out.soul_bytes = soulBytes
  return Object.keys(out).length ? out : undefined
}

export function sanitizeProfileFeatures(value: unknown): AgentProfileFeatureToken[] | undefined {
  if (!Array.isArray(value)) return undefined
  const allowed = new Set<string>(AGENT_PROFILE_FEATURE_TOKENS)
  const out = value.filter((x): x is AgentProfileFeatureToken => typeof x === 'string' && allowed.has(x))
  return out.length ? [...new Set(out)] : undefined
}

function sourceRefFromEnv(env: Record<string, string | undefined>): string | undefined {
  const direct = sanitizeSourceRef(env.CODEX_AGENT_PROFILE_SOURCE ?? env.CODEX_AGENT_SOUL_REF)
  if (direct) return direct
  const path = env.CODEX_AGENT_PROFILE_PATH
  const root = env.CODEX_AGENT_PROFILE_ROOT
  if (!path || !root || !isAbsolute(path) || !isAbsolute(root)) return undefined
  const rel = relative(resolve(root), resolve(path)).replace(/\\/g, '/').replace(/\/profile\.json$/, '')
  return sanitizeSourceRef(rel)
}

function sanitizeSourceRef(value: unknown): string | undefined {
  const ref = parseProfileRef(value)
  return ref === false ? undefined : ref
}

function sanitizeRevision(value: unknown): string | undefined {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || value.length > 80 || /[\r\n]/.test(value)) return undefined
  const s = value.trim().toLowerCase().replace(/^sha256:/, '')
  return /^[a-f0-9]{8,64}$/.test(s) ? s.slice(0, 12) : undefined
}

function sanitizeSoulBytes(value: unknown): number | undefined {
  if (value == null || value === '') return undefined
  const n = typeof value === 'number' ? value : Number(value)
  if (!Number.isInteger(n) || n < 0 || n > 10_000_000) return undefined
  return n
}
