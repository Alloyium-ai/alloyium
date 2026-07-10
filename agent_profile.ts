import { createHash } from 'node:crypto'
import { existsSync, readFileSync, realpathSync, statSync } from 'node:fs'
import path from 'node:path'

export const AGENT_PROFILE_SCHEMA = 'a2a.agent.profile.v1'
export const DEFAULT_SOUL_MAX_BYTES = 24_576
export const DEFAULT_BOOTSTRAP_MAX_BYTES = 32_768
export const DEFAULT_TURN_HEADER_MAX_BYTES = 2_048
export const HARD_PROFILE_TEXT_MAX_BYTES = 65_536

const PROFILE_ID_RE = /^[a-z0-9-]{1,64}$/

export type AgentProfileErrorCode =
  | 'profile-disabled-required'
  | 'profile-required'
  | 'profile-missing'
  | 'profile-invalid'
  | 'profile-path-unsafe'
  | 'profile-symlink-escape'
  | 'soul-missing'
  | 'soul-oversize'
  | 'soul-path-unsafe'
  | 'soul-symlink-escape'

export class AgentProfileError extends Error {
  code: AgentProfileErrorCode

  constructor(code: AgentProfileErrorCode, message: string) {
    super(message)
    this.name = 'AgentProfileError'
    this.code = code
  }
}

export type AgentProfileSourceKind = 'profile-path' | 'agent-profile' | 'role-profile' | 'plain-soul'

export type ResolvedAgentProfile = {
  schema: typeof AGENT_PROFILE_SCHEMA
  profileId: string
  role: string
  displayName?: string
  agentId?: string
  revision: string
  source: {
    kind: AgentProfileSourceKind
    ref: string
    profilePath?: string
    soulPath?: string
  }
  soul: {
    markdown: string
    bytes: number
    required: boolean
    maxBytes: number
    inject: 'bootstrap' | 'disabled'
    warnings: string[]
  }
  prompt: {
    bootstrapOnce: boolean
    maxBootstrapBytes: number
    maxTurnHeaderBytes: number
  }
  memory?: Record<string, unknown>
  rawProfile: Record<string, unknown>
}

export type AgentProfileResolution =
  | { enabled: false; reason: 'disabled' | 'not-found'; required: boolean; warnings: string[] }
  | { enabled: true; profile: ResolvedAgentProfile; required: boolean; warnings: string[] }

export type ResolveAgentProfileOptions = {
  env?: Record<string, string | undefined>
  channelsDir?: string
  agentId?: string
  cwd?: string
}

function envBool(v: string | undefined, fallback: boolean): boolean {
  if (v == null || v === '') return fallback
  return v === '1' || v.toLowerCase() === 'true' || v.toLowerCase() === 'yes'
}

function hasTraversalSegment(p: string): boolean {
  return p.split(/[\\/]+/).some((part) => part === '..')
}

function isSafePathSegment(s: string): boolean {
  return s.length > 0 && !s.includes('/') && !s.includes('\\') && !hasTraversalSegment(s)
}

function isUnderRoot(candidate: string, root: string): boolean {
  const rel = path.relative(root, candidate)
  return rel === '' || (!!rel && !rel.startsWith('..') && !path.isAbsolute(rel))
}

function uniq(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)))
}

function splitEnvPaths(value: string | undefined): string[] {
  return (value ?? '').split(/[,:]/).map((s) => s.trim()).filter(Boolean)
}

function canonicalizeRoot(root: string, cwd: string): { display: string; real: string } | null {
  try {
    const display = path.resolve(cwd, root)
    return { display: path.normalize(display), real: path.normalize(realpathSync(display)) }
  } catch {
    return null
  }
}

function allowedRootsFor(env: Record<string, string | undefined>, opts: ResolveAgentProfileOptions): { display: string; real: string }[] {
  const cwd = opts.cwd ?? process.cwd()
  const channelsDir = path.resolve(cwd, opts.channelsDir ?? env.CHANNELS ?? process.cwd())
  const profileRoot = path.resolve(cwd, env.CODEX_AGENT_PROFILE_ROOT ?? path.join(channelsDir, 'agents'))
  const configured = splitEnvPaths(env.CODEX_AGENT_PROFILE_ALLOWED_ROOTS).map((p) => path.resolve(cwd, p))
  return uniq([path.join(channelsDir, 'agents'), profileRoot, ...configured])
    .map((root) => canonicalizeRoot(root, cwd))
    .filter((root): root is { display: string; real: string } => root !== null)
}

function safeResolveExistingFile(
  rawPath: string,
  baseDir: string,
  roots: { display: string; real: string }[],
  kind: 'profile' | 'soul',
): { path: string; realpath: string } {
  if (!rawPath || hasTraversalSegment(rawPath)) {
    throw new AgentProfileError(`${kind}-path-unsafe`, `${kind} path is unsafe`)
  }
  const candidate = path.normalize(path.isAbsolute(rawPath) ? rawPath : path.resolve(baseDir, rawPath))
  const candidateUnderRoot = roots.some((root) => isUnderRoot(candidate, root.display) || isUnderRoot(candidate, root.real))
  if (!candidateUnderRoot) {
    throw new AgentProfileError(`${kind}-path-unsafe`, `${kind} path is outside allowed roots`)
  }
  if (!existsSync(candidate)) {
    throw new AgentProfileError(`${kind}-missing`, `${kind} file is missing`)
  }
  let real: string
  try {
    real = path.normalize(realpathSync(candidate))
    const st = statSync(real)
    if (!st.isFile()) throw new Error('not a file')
  } catch {
    throw new AgentProfileError(`${kind}-missing`, `${kind} file is missing`)
  }
  const realUnderRoot = roots.some((root) => isUnderRoot(real, root.real))
  if (!realUnderRoot) {
    throw new AgentProfileError(`${kind}-symlink-escape`, `${kind} file resolves outside allowed roots`)
  }
  return { path: candidate, realpath: real }
}

function safeResolveOptionalFile(
  rawPath: string,
  baseDir: string,
  roots: { display: string; real: string }[],
  kind: 'profile' | 'soul',
): { path: string; realpath: string } | null {
  try {
    return safeResolveExistingFile(rawPath, baseDir, roots, kind)
  } catch (e) {
    if (e instanceof AgentProfileError && e.code === `${kind}-missing`) return null
    throw e
  }
}

function textFileBytes(file: string): number {
  return statSync(file).size
}

function readTextFileBounded(file: string, maxBytes: number, code: AgentProfileErrorCode): { text: string; bytes: number } {
  const size = textFileBytes(file)
  if (size > maxBytes) throw new AgentProfileError(code, `file exceeds byte cap`)
  const text = readFileSync(file, 'utf8')
  const bytes = Buffer.byteLength(text, 'utf8')
  if (bytes > maxBytes) throw new AgentProfileError(code, `file exceeds byte cap`)
  return { text, bytes }
}

function idFromEnv(name: string, value: unknown, fallback?: string): string {
  const raw = typeof value === 'string' && value.trim() ? value.trim() : fallback
  if (!raw || !PROFILE_ID_RE.test(raw)) throw new AgentProfileError('profile-invalid', `${name} must match ${PROFILE_ID_RE}`)
  return raw
}

function optionalId(name: string, value: unknown): string | undefined {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || !PROFILE_ID_RE.test(value)) throw new AgentProfileError('profile-invalid', `${name} must match ${PROFILE_ID_RE}`)
  return value
}

function objectValue(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new AgentProfileError('profile-invalid', `${field} must be an object`)
  }
  return value as Record<string, unknown>
}

function boundedNumber(value: unknown, fallback: number, hardCap: number, field: string): number {
  if (value == null) return fallback
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw new AgentProfileError('profile-invalid', `${field} must be a positive number`)
  }
  return Math.min(Math.floor(value), hardCap)
}

function boolValue(value: unknown, fallback: boolean): boolean {
  return typeof value === 'boolean' ? value : fallback
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  const obj = value as Record<string, unknown>
  return `{${Object.keys(obj).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`).join(',')}}`
}

function sha256Hex(s: string): string {
  return createHash('sha256').update(s, 'utf8').digest('hex')
}

function displayNameFor(role: string): string {
  return role.split('-').map((part) => part ? part[0]!.toUpperCase() + part.slice(1) : part).join(' ')
}

export function detectTokenLookingContent(text: string): string[] {
  const warnings = new Set<string>()
  if (/-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(text)) warnings.add('private-key-looking-content')
  if (/\b(?:api[_-]?key|access[_-]?token|secret|bearer)\b\s*[:=]/i.test(text)) warnings.add('token-looking-content')
  if (/\bsk-[A-Za-z0-9_-]{16,}\b/.test(text)) warnings.add('token-looking-content')
  if (/\bgh[pousr]_[A-Za-z0-9_]{20,}\b/.test(text)) warnings.add('token-looking-content')
  return [...warnings]
}

function profileForRevision(profile: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(canonicalJson(profile)) as Record<string, unknown>
}

function buildResolvedProfile(args: {
  rawProfile: Record<string, unknown>
  profilePath?: string
  soulPath?: string
  soulMarkdown: string
  soulBytes: number
  sourceKind: AgentProfileSourceKind
  sourceRef: string
  agentId?: string
  soulWarnings?: string[]
}): ResolvedAgentProfile {
  const raw = args.rawProfile
  const soul = objectValue(raw.soul, 'soul')
  const prompt = raw.prompt && typeof raw.prompt === 'object' && !Array.isArray(raw.prompt) ? raw.prompt as Record<string, unknown> : {}
  const inject = soul.inject == null ? 'bootstrap' : soul.inject
  if (inject !== 'bootstrap' && inject !== 'disabled') throw new AgentProfileError('profile-invalid', 'soul.inject must be bootstrap or disabled')
  const profileId = idFromEnv('profile_id', raw.profile_id)
  const role = idFromEnv('role', raw.role)
  const declaredAgentId = optionalId('agent_id', raw.agent_id)
  if (declaredAgentId && args.agentId && declaredAgentId !== args.agentId) {
    throw new AgentProfileError('profile-invalid', 'agent_id does not match runtime agent')
  }
  const maxSoulBytes = boundedNumber(soul.max_bytes, DEFAULT_SOUL_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'soul.max_bytes')
  const required = boolValue(soul.required, true)
  const revisionProfile = profileForRevision({
    ...raw,
    soul: {
      ...soul,
      required,
      max_bytes: maxSoulBytes,
      inject,
    },
    prompt: {
      ...prompt,
      bootstrap_once: boolValue(prompt.bootstrap_once, true),
      max_bootstrap_bytes: boundedNumber(prompt.max_bootstrap_bytes, DEFAULT_BOOTSTRAP_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'prompt.max_bootstrap_bytes'),
      max_turn_header_bytes: boundedNumber(prompt.max_turn_header_bytes, DEFAULT_TURN_HEADER_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'prompt.max_turn_header_bytes'),
    },
  })
  const revision = sha256Hex(`${canonicalJson(revisionProfile)}\n${args.soulMarkdown}`)
  return {
    schema: AGENT_PROFILE_SCHEMA,
    profileId,
    role,
    displayName: typeof raw.display_name === 'string' ? raw.display_name : undefined,
    agentId: declaredAgentId,
    revision,
    source: {
      kind: args.sourceKind,
      ref: args.sourceRef,
      ...(args.profilePath ? { profilePath: args.profilePath } : {}),
      ...(args.soulPath ? { soulPath: args.soulPath } : {}),
    },
    soul: {
      markdown: args.soulMarkdown,
      bytes: args.soulBytes,
      required,
      maxBytes: maxSoulBytes,
      inject,
      warnings: args.soulWarnings ?? detectTokenLookingContent(args.soulMarkdown),
    },
    prompt: {
      bootstrapOnce: boolValue(prompt.bootstrap_once, true),
      maxBootstrapBytes: boundedNumber(prompt.max_bootstrap_bytes, DEFAULT_BOOTSTRAP_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'prompt.max_bootstrap_bytes'),
      maxTurnHeaderBytes: boundedNumber(prompt.max_turn_header_bytes, DEFAULT_TURN_HEADER_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'prompt.max_turn_header_bytes'),
    },
    memory: raw.memory && typeof raw.memory === 'object' && !Array.isArray(raw.memory) ? raw.memory as Record<string, unknown> : undefined,
    rawProfile: revisionProfile,
  }
}

function loadProfileFile(args: {
  rawPath: string
  baseDir: string
  roots: { display: string; real: string }[]
  sourceKind: AgentProfileSourceKind
  sourceRef: string
  agentId?: string
}): ResolvedAgentProfile {
  const profileFile = safeResolveExistingFile(args.rawPath, args.baseDir, args.roots, 'profile')
  let parsed: unknown
  try {
    parsed = JSON.parse(readTextFileBounded(profileFile.realpath, HARD_PROFILE_TEXT_MAX_BYTES, 'profile-invalid').text) as unknown
  } catch (e) {
    if (e instanceof AgentProfileError) throw e
    throw new AgentProfileError('profile-invalid', 'profile JSON is invalid')
  }
  const raw = objectValue(parsed, 'profile')
  if (raw.schema !== AGENT_PROFILE_SCHEMA) throw new AgentProfileError('profile-invalid', `schema must be ${AGENT_PROFILE_SCHEMA}`)
  const soul = objectValue(raw.soul, 'soul')
  if (typeof soul.path !== 'string' || !soul.path.trim()) throw new AgentProfileError('profile-invalid', 'soul.path is required')
  const maxSoulBytes = boundedNumber(soul.max_bytes, DEFAULT_SOUL_MAX_BYTES, HARD_PROFILE_TEXT_MAX_BYTES, 'soul.max_bytes')
  const required = boolValue(soul.required, true)
  const soulFile = safeResolveOptionalFile(soul.path, path.dirname(profileFile.realpath), args.roots, 'soul')
  if (!soulFile) {
    if (required) throw new AgentProfileError('soul-missing', 'required soul file is missing')
    return buildResolvedProfile({
      rawProfile: raw,
      profilePath: profileFile.realpath,
      soulMarkdown: '',
      soulBytes: 0,
      sourceKind: args.sourceKind,
      sourceRef: args.sourceRef,
      agentId: args.agentId,
      soulWarnings: [],
    })
  }
  const soulText = readTextFileBounded(soulFile.realpath, maxSoulBytes, 'soul-oversize')
  return buildResolvedProfile({
    rawProfile: raw,
    profilePath: profileFile.realpath,
    soulPath: soulFile.realpath,
    soulMarkdown: soulText.text,
    soulBytes: soulText.bytes,
    sourceKind: args.sourceKind,
    sourceRef: args.sourceRef,
    agentId: args.agentId,
  })
}

function loadPlainSoul(args: {
  rawPath: string
  baseDir: string
  roots: { display: string; real: string }[]
  env: Record<string, string | undefined>
  agentId: string
}): ResolvedAgentProfile {
  const soulFile = safeResolveExistingFile(args.rawPath, args.baseDir, args.roots, 'soul')
  const role = idFromEnv('CODEX_AGENT_ROLE', args.env.CODEX_AGENT_ROLE, 'agent')
  const profileId = idFromEnv('CODEX_AGENT_PROFILE_ID', args.env.CODEX_AGENT_PROFILE_ID, role)
  const soulText = readTextFileBounded(soulFile.realpath, DEFAULT_SOUL_MAX_BYTES, 'soul-oversize')
  const rawProfile = {
    schema: AGENT_PROFILE_SCHEMA,
    profile_id: profileId,
    role,
    display_name: displayNameFor(role),
    soul: {
      path: path.basename(soulFile.realpath),
      required: true,
      max_bytes: DEFAULT_SOUL_MAX_BYTES,
      inject: 'bootstrap',
    },
    prompt: {
      bootstrap_once: true,
      max_bootstrap_bytes: DEFAULT_BOOTSTRAP_MAX_BYTES,
      max_turn_header_bytes: DEFAULT_TURN_HEADER_MAX_BYTES,
    },
  }
  return buildResolvedProfile({
    rawProfile,
    soulPath: soulFile.realpath,
    soulMarkdown: soulText.text,
    soulBytes: soulText.bytes,
    sourceKind: 'plain-soul',
    sourceRef: soulFile.realpath,
    agentId: args.agentId,
  })
}

export function resolveAgentProfile(opts: ResolveAgentProfileOptions = {}): AgentProfileResolution {
  const env = opts.env ?? process.env
  const cwd = opts.cwd ?? process.cwd()
  const agentId = opts.agentId ?? env.A2A_AGENT_ID ?? 'codex-gw'
  const required = envBool(env.CODEX_AGENT_PROFILE_REQUIRED, false)
  const enabled = envBool(env.CODEX_AGENT_PROFILE_ENABLED, true)
  if (!enabled) {
    if (required) throw new AgentProfileError('profile-disabled-required', 'agent profile is disabled but required')
    return { enabled: false, reason: 'disabled', required, warnings: [] }
  }

  const roots = allowedRootsFor(env, { ...opts, cwd })
  if (!roots.length) {
    if (required) throw new AgentProfileError('profile-required', 'no usable agent profile allowed roots')
    return { enabled: false, reason: 'not-found', required, warnings: [] }
  }
  const channelsDir = path.resolve(cwd, opts.channelsDir ?? env.CHANNELS ?? process.cwd())
  const profileRoot = path.resolve(cwd, env.CODEX_AGENT_PROFILE_ROOT ?? path.join(channelsDir, 'agents'))

  const explicitProfilePath = env.CODEX_AGENT_PROFILE_PATH?.trim()
  if (explicitProfilePath) {
    const profile = loadProfileFile({ rawPath: explicitProfilePath, baseDir: profileRoot, roots, sourceKind: 'profile-path', sourceRef: explicitProfilePath, agentId })
    return { enabled: true, profile, required, warnings: profile.soul.warnings }
  }

  const plainSoulPath = env.CODEX_AGENT_SOUL_PATH?.trim()
  if (plainSoulPath) {
    const profile = loadPlainSoul({ rawPath: plainSoulPath, baseDir: profileRoot, roots, env, agentId })
    return { enabled: true, profile, required, warnings: profile.soul.warnings }
  }

  if (isSafePathSegment(agentId)) {
    const agentProfile = path.join(profileRoot, 'agents', agentId, 'profile.json')
    const found = safeResolveOptionalFile(agentProfile, profileRoot, roots, 'profile')
    if (found) {
      const profile = loadProfileFile({ rawPath: found.realpath, baseDir: profileRoot, roots, sourceKind: 'agent-profile', sourceRef: `agents/${agentId}`, agentId })
      return { enabled: true, profile, required, warnings: profile.soul.warnings }
    }
  }

  const role = env.CODEX_AGENT_ROLE?.trim()
  if (role) {
    idFromEnv('CODEX_AGENT_ROLE', role)
    const roleProfile = path.join(profileRoot, 'roles', role, 'profile.json')
    const found = safeResolveOptionalFile(roleProfile, profileRoot, roots, 'profile')
    if (found) {
      const profile = loadProfileFile({ rawPath: found.realpath, baseDir: profileRoot, roots, sourceKind: 'role-profile', sourceRef: `roles/${role}`, agentId })
      return { enabled: true, profile, required, warnings: profile.soul.warnings }
    }
  }

  if (required) throw new AgentProfileError('profile-required', 'agent profile is required but no profile was found')
  return { enabled: false, reason: 'not-found', required, warnings: [] }
}

export function describeAgentProfileResolution(resolution: AgentProfileResolution): string {
  if (!resolution.enabled) return `profile=disabled reason=${resolution.reason}`
  const p = resolution.profile
  const warnings = p.soul.warnings.length ? ` warnings=${p.soul.warnings.join(',')}` : ''
  return `profile=${p.profileId} role=${p.role} revision=${p.revision.slice(0, 12)} soul_bytes=${p.soul.bytes} source=${p.source.ref}${warnings}`
}
