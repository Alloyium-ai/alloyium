#!/usr/bin/env bun
// a2a-launcher — internal launch control-plane for first-class A2A peers.
//
// This is intentionally outside a2a-core. The core serves MCP tools and owns the
// shared A2A bus connections; this service owns runtime provisioning for dev
// Compose now and Kubernetes later.
import './preamble.ts'
import http from 'node:http'
import { createHash, timingSafeEqual } from 'node:crypto'
import { chownSync } from 'node:fs'
import { mkdir, realpath } from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, resolve as resolvePath } from 'node:path'
import { RedisClient } from 'bun'
import { onboard } from './onboard.ts'
import { isCwdRegistered, registerCwd } from './codex_build_authz.ts'

type LaunchMode = 'shim' | 'webhook'

type CodexLaunchRequest = {
  agent_id?: unknown
  label?: unknown
  mode?: unknown
  created_by?: unknown
  worktree?: unknown
  job_id?: unknown
  base_ref?: unknown
  target_branch?: unknown
  cleanup_policy?: unknown
  dry_run?: unknown
  policy?: {
    allow_write?: unknown
    sandbox?: unknown
    effort?: unknown
    model?: unknown
    turn_timeout_ms?: unknown
    role_scopes?: unknown
    write_requesters?: unknown
  }
}

// Claude Code worker launch (kind=claude). The codex path's analog: spawns
// claude_gateway.ts driving the logged-in `claude` CLI (NO API key — the host
// `~/.claude` dir + `~/.claude.json` file login is bound in). Advisory-only pure
// inference, so the policy carries only model/effort/turn_timeout (no sandbox/allow_write).
type ClaudeLaunchRequest = {
  agent_id?: unknown
  label?: unknown
  created_by?: unknown
  dry_run?: unknown
  policy?: { effort?: unknown; model?: unknown; turn_timeout_ms?: unknown }
}

type DockerContainerSpec = {
  name: string
  image: string
  body: Record<string, unknown>
}

export type LaunchWorktreePlan = {
  requested?: string
  isolated: boolean
  sourceRepoHostPath?: string
  sourceRepoLauncherPath?: string
  baseRef?: string
  targetBranch?: string
  jobId?: string
  cleanupPolicy: string
  hostWorktreePath?: string
  launcherWorktreePath?: string
  containerCwd?: string
  cwdRoots?: string
}

export type LaunchWorktreePlanResult =
  | { ok: true; plan?: LaunchWorktreePlan }
  | { ok: false; error: string; detail?: unknown }

const env = process.env
const PORT = Number(env.A2A_LAUNCHER_PORT ?? 8910)
const HOSTNAME = env.A2A_LAUNCHER_HOST ?? '127.0.0.1'
const LAUNCHER_TOKEN = env.A2A_LAUNCHER_TOKEN ?? env.A2A_LAUNCHER_BEARER_TOKEN ?? ''
const PROVIDER = env.A2A_LAUNCH_PROVIDER ?? 'docker'
const REDIS_URL = env.REDIS_URL ?? 'redis://redis:6379'
const NATS_URL = env.NATS_URL ?? 'nats://nats:4222'
const SECRETS_DIR = env.A2A_LAUNCH_SECRETS_DIR ?? '/run/secrets/a2a'
const DOCKER_SOCKET = env.A2A_LAUNCH_DOCKER_SOCKET ?? '/var/run/docker.sock'
const PROJECT = env.COMPOSE_PROJECT_NAME ?? env.A2A_LAUNCH_COMPOSE_PROJECT ?? 'alloyium'
const CODEX_IMAGE = env.A2A_LAUNCH_CODEX_IMAGE ?? `${PROJECT}-codex-gw:latest`
const CLAUDE_IMAGE = env.A2A_LAUNCH_CLAUDE_IMAGE ?? `${PROJECT}-claude-gw:latest`
const NETWORK = env.A2A_LAUNCH_NETWORK ?? `${PROJECT}_a2a-net`
const SECRETS_VOLUME = env.A2A_LAUNCH_SECRETS_VOLUME ?? `${PROJECT}_a2a_secrets`
const CORE_SOURCE = env.A2A_LAUNCH_CORE_SOURCE ?? env.A2A_LAUNCH_CORE_VOLUME ?? '/run/a2a-core'
const WORKSPACES_VOLUME = env.A2A_LAUNCH_WORKSPACES_VOLUME ?? `${PROJECT}_codex_workspaces`
const WORKSPACE_ROOT = env.CODEX_WORKSPACE_ROOT ?? '/srv/git'
const SHARED_WORKSPACE_HOST_PATH = resolveLaunchWorkspaceHostPath(env.A2A_LAUNCH_WORKSPACE_HOST_PATH ?? env.A2A_WORKSPACE_HOST_PATH, env.A2A_LAUNCH_PROJECT_DIR)
const SHARED_WORKSPACE_CONTAINER_PATH = resolveContainerWorkspacePath(env.A2A_LAUNCH_WORKSPACE_CONTAINER_PATH)
const WORKTREE_ROOT_HOST_PATH = resolveLaunchWorkspaceHostPath(env.A2A_LAUNCH_WORKTREE_ROOT, env.A2A_LAUNCH_PROJECT_DIR)
const DEFAULT_CODEX_CWD_ROOTS = buildDefaultCodexCwdRoots(SHARED_WORKSPACE_HOST_PATH ? SHARED_WORKSPACE_CONTAINER_PATH : null, WORKSPACE_ROOT)
const CODEX_HOST_HOME = resolveCodexHostHome(env.CODEX_HOST_HOME, env.HOME, WORKSPACE_ROOT)
// Claude's login is split across a dir (~/.claude) and a file (~/.claude.json); resolve
// each host path the same way codex resolves CODEX_HOST_HOME (explicit env wins).
const CLAUDE_HOST_HOME = resolveClaudeHostHome(env.CLAUDE_HOST_HOME, env.HOME, WORKSPACE_ROOT)
const CLAUDE_HOST_CONFIG = resolveClaudeHostConfig(env.CLAUDE_HOST_CONFIG, env.HOME, WORKSPACE_ROOT)
const CONTAINER_USER = env.A2A_LAUNCH_CONTAINER_USER ?? `${env.CC_UID ?? 1000}:${env.CC_GID ?? 1000}`
const PRESENCE_WAIT_MS = Math.max(0, Number(env.A2A_LAUNCH_PRESENCE_WAIT_MS ?? 15_000) || 0)
const CWD_ALLOW_TTL_S = Math.max(1, Number(env.A2A_LAUNCH_CWD_ALLOW_TTL_S ?? env.CODEX_BUILD_CWD_TTL_S ?? 12 * 3600) || 12 * 3600)
const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const ROLE_SCOPE_RE = /^[A-Za-z0-9:_./*@+=-]{1,256}$/

const allowed = new Set((env.A2A_LAUNCH_ALLOWED_IDS ?? 'codex-gw,claude-gw,agent-1').split(',').map((s) => s.trim()).filter(Boolean))
let redis: RedisClient | null = null
const getRedis = (): RedisClient => (redis ??= new RedisClient(REDIS_URL))

const logAt = (level: 'info' | 'warn', event: string, fields: Record<string, unknown> = {}) => {
  const kv = Object.entries(fields).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(' ')
  console.error(`${new Date().toISOString()} ${level} [a2a-launcher] ${event}${kv ? ' ' + kv : ''}`)
}
const log = (event: string, fields: Record<string, unknown> = {}) => logAt('info', event, fields)
const warn = (event: string, fields: Record<string, unknown> = {}) => logAt('warn', event, fields)

function json(obj: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(obj), { ...init, headers: { 'content-type': 'application/json', ...(init.headers ?? {}) } })
}

function err(error: string, status = 400, detail?: unknown): Response {
  return json({ ok: false, error, detail }, { status })
}

function asAgentId(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const s = value.trim()
  return AGENT_ID_RE.test(s) ? s : null
}

function asCreatedBy(value: unknown): string | null {
  return asAgentId(value)
}

function asMode(value: unknown): LaunchMode {
  return value === 'webhook' ? 'webhook' : 'shim'
}

function asLabel(value: unknown): string | undefined {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || value.length > 128 || /[\r\n]/.test(value)) return undefined
  return value
}

function asSafeEnvValue(value: unknown, maxLen = 128): string | undefined {
  if (value == null || value === '') return undefined
  if (typeof value !== 'string' || value.length > maxLen || /[\r\n]/.test(value)) return undefined
  return value
}

function asSafeStringList(value: unknown, re: RegExp, maxItems: number): string[] | null {
  if (value == null) return []
  if (!Array.isArray(value) || value.length > maxItems) return null
  const out: string[] = []
  for (const item of value) {
    if (typeof item !== 'string') return null
    const s = item.trim()
    if (!re.test(s) || /[\r\n]/.test(s)) return null
    if (!out.includes(s)) out.push(s)
  }
  return out
}

function asPositiveInteger(value: unknown): number | undefined {
  if (!Number.isInteger(value) || value <= 0) return undefined
  return value
}

function envPair(k: string, v: string | number | boolean | undefined): string | null {
  if (v === undefined || v === '') return null
  return `${k}=${String(v)}`
}

export function resolveLaunchWorkspaceHostPath(value: string | undefined, projectDir: string | undefined, cwd = process.cwd()): string | null {
  const raw = value?.trim()
  if (!raw) return null
  if (isAbsolute(raw)) return raw
  const baseRaw = projectDir?.trim() || cwd
  const base = isAbsolute(baseRaw) ? baseRaw : resolvePath(cwd, baseRaw)
  return resolvePath(base, raw)
}

function resolveContainerWorkspacePath(value: string | undefined): string {
  const raw = value?.trim()
  if (!raw || !raw.startsWith('/')) return '/workspace'
  return raw.replace(/\/+$/, '') || '/'
}

export function buildDefaultCodexCwdRoots(sharedWorkspacePath: string | null, legacyWorkspaceRoot: string): string {
  return [sharedWorkspacePath, legacyWorkspaceRoot]
    .map((s) => s?.trim())
    .filter((s): s is string => !!s)
    .filter((s, i, all) => all.indexOf(s) === i)
    .join(',')
}

function stripTrailingSlashes(value: string): string {
  return value.replace(/\/+$/, '') || '/'
}

function isSafeGitRef(value: string): boolean {
  const s = value.trim()
  if (!s || s.length > 256) return false
  if (s.startsWith('-') || s.startsWith('/') || s.endsWith('/') || s.endsWith('.')) return false
  if (s.includes('..') || s.includes('@{') || s.includes('//')) return false
  return !/[\\\s~^:?*\[\]\0]/.test(s)
}

function asSafeGitRef(value: unknown): string | undefined {
  const s = asSafeEnvValue(value, 256)?.trim()
  return s && isSafeGitRef(s) ? s : undefined
}

function asSafeJobId(value: unknown): string | undefined {
  const s = asSafeEnvValue(value, 128)?.trim()
  return s && /^[A-Za-z0-9_.:-]{1,128}$/.test(s) ? s : undefined
}

function asCleanupPolicy(value: unknown): string | undefined {
  const s = asSafeEnvValue(value, 64)?.trim()
  return s && /^[A-Za-z0-9_.:-]{1,64}$/.test(s) ? s : undefined
}

function slugPart(value: string, max = 40): string {
  const s = value.toLowerCase().replace(/[^a-z0-9._-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '')
  return (s || 'x').slice(0, max).replace(/[.-]$/g, '') || 'x'
}

function parseLaunchWorktreeSpec(value: string | undefined): { repo: string; baseRef?: string } | null {
  const raw = value?.trim()
  if (!raw || /[\r\n]/.test(raw)) return null
  const at = raw.lastIndexOf('@')
  if (at > 0 && at < raw.length - 1) {
    const repo = raw.slice(0, at).trim()
    const baseRef = raw.slice(at + 1).trim()
    if (!repo || !isSafeGitRef(baseRef)) return null
    return { repo, baseRef }
  }
  return { repo: raw }
}

function resolveLaunchRepoHostPath(repo: string, workspaceRoot = WORKSPACE_ROOT): string | null {
  const raw = repo.trim().replace(/\/+$/, '')
  if (!raw || /[\r\n]/.test(raw)) return null
  if (isAbsolute(raw)) return raw
  const root = workspaceRoot.trim()
  return root ? resolvePath(root, raw) : null
}

export function mapLaunchHostPathToContainerPath(
  value: string,
  sharedHostPath: string | null = SHARED_WORKSPACE_HOST_PATH,
  sharedContainerPath = SHARED_WORKSPACE_CONTAINER_PATH,
  workspaceRoot = WORKSPACE_ROOT,
): string | undefined {
  const p = stripTrailingSlashes(value)
  const sharedHost = sharedHostPath ? stripTrailingSlashes(sharedHostPath) : null
  if (sharedHost && (p === sharedHost || p.startsWith(`${sharedHost}/`))) {
    const suffix = p.slice(sharedHost.length)
    return `${stripTrailingSlashes(sharedContainerPath)}${suffix}` || sharedContainerPath
  }

  const root = workspaceRoot.trim() ? stripTrailingSlashes(workspaceRoot) : ''
  if (root && (p === root || p.startsWith(`${root}/`))) return p
  return undefined
}

function mapLaunchHostPathToLauncherPath(
  value: string,
  sharedHostPath: string | null = SHARED_WORKSPACE_HOST_PATH,
  sharedContainerPath = SHARED_WORKSPACE_CONTAINER_PATH,
  workspaceRoot = WORKSPACE_ROOT,
): string | undefined {
  return mapLaunchHostPathToContainerPath(value, sharedHostPath, sharedContainerPath, workspaceRoot)
}

function defaultWorktreeRootHostPath(sharedHostPath: string | null, workspaceRoot: string): string {
  return join(stripTrailingSlashes(sharedHostPath ?? workspaceRoot), '.a2a-worktrees')
}

function buildWorktreeSlug(repo: string, baseRef: string, targetBranch: string, jobId: string): string {
  const hash = createHash('sha256').update(`${repo}\0${baseRef}\0${targetBranch}\0${jobId}`).digest('hex').slice(0, 10)
  return [
    slugPart(basename(repo) || 'repo', 32),
    slugPart(baseRef, 28),
    slugPart(targetBranch, 36),
    slugPart(jobId, 32),
    hash,
  ].join('-')
}

function defaultTargetBranch(agentId: string, jobId: string): string {
  return `codex/${slugPart(agentId, 28)}-${slugPart(jobId, 28)}`
}

export function buildLaunchWorktreePlan(opts: {
  agentId: string
  worktree?: string
  allowWrite?: boolean
  jobId?: string
  baseRef?: string
  targetBranch?: string
  cleanupPolicy?: string
  sharedHostPath?: string | null
  sharedContainerPath?: string
  workspaceRoot?: string
  worktreeRootHostPath?: string | null
}): LaunchWorktreePlanResult {
  const allowWrite = opts.allowWrite === true
  const sharedHostPath = opts.sharedHostPath === undefined ? SHARED_WORKSPACE_HOST_PATH : opts.sharedHostPath
  const sharedContainerPath = opts.sharedContainerPath ?? SHARED_WORKSPACE_CONTAINER_PATH
  const workspaceRoot = opts.workspaceRoot ?? WORKSPACE_ROOT
  const cleanupPolicy = opts.cleanupPolicy ?? 'preserve'
  const parsed = parseLaunchWorktreeSpec(opts.worktree)

  if (!parsed) {
    if (allowWrite) return { ok: false, error: 'write_worktree_required' }
    return { ok: true }
  }

  const sourceRepoHostPath = resolveLaunchRepoHostPath(parsed.repo, workspaceRoot)
  if (!sourceRepoHostPath) return { ok: false, error: 'bad_worktree_repo' }
  const sourceRepoLauncherPath = mapLaunchHostPathToLauncherPath(sourceRepoHostPath, sharedHostPath, sharedContainerPath, workspaceRoot)
  const sourceRepoContainerPath = mapLaunchHostPathToContainerPath(sourceRepoHostPath, sharedHostPath, sharedContainerPath, workspaceRoot)
  if (!sourceRepoLauncherPath || !sourceRepoContainerPath) return { ok: false, error: 'worktree_not_mounted', detail: sourceRepoHostPath }

  const baseRef = opts.baseRef ?? parsed.baseRef ?? 'HEAD'
  if (!isSafeGitRef(baseRef)) return { ok: false, error: 'bad_base_ref' }

  if (!allowWrite) {
    return {
      ok: true,
      plan: {
        requested: opts.worktree,
        isolated: false,
        sourceRepoHostPath,
        sourceRepoLauncherPath,
        baseRef,
        cleanupPolicy,
        containerCwd: sourceRepoContainerPath,
      },
    }
  }

  const jobId = opts.jobId ?? opts.agentId
  const targetBranch = opts.targetBranch ?? defaultTargetBranch(opts.agentId, jobId)
  if (!isSafeGitRef(targetBranch)) return { ok: false, error: 'bad_target_branch' }
  const rootHost = opts.worktreeRootHostPath ?? WORKTREE_ROOT_HOST_PATH ?? defaultWorktreeRootHostPath(sharedHostPath, workspaceRoot)
  const rootLauncher = mapLaunchHostPathToLauncherPath(rootHost, sharedHostPath, sharedContainerPath, workspaceRoot)
  const rootContainer = mapLaunchHostPathToContainerPath(rootHost, sharedHostPath, sharedContainerPath, workspaceRoot)
  if (!rootLauncher || !rootContainer) return { ok: false, error: 'worktree_root_not_mounted', detail: rootHost }

  const slug = buildWorktreeSlug(sourceRepoHostPath, baseRef, targetBranch, jobId)
  const hostWorktreePath = join(rootHost, slug)
  const launcherWorktreePath = join(rootLauncher, slug)
  const containerCwd = join(rootContainer, slug)
  return {
    ok: true,
    plan: {
      requested: opts.worktree,
      isolated: true,
      sourceRepoHostPath,
      sourceRepoLauncherPath,
      baseRef,
      targetBranch,
      jobId,
      cleanupPolicy,
      hostWorktreePath,
      launcherWorktreePath,
      containerCwd,
      cwdRoots: containerCwd,
    },
  }
}

export function resolveLaunchWorktreeCwd(
  value: string | undefined,
  sharedHostPath: string | null = SHARED_WORKSPACE_HOST_PATH,
  sharedContainerPath = SHARED_WORKSPACE_CONTAINER_PATH,
  workspaceRoot = WORKSPACE_ROOT,
): string | undefined {
  const parsed = parseLaunchWorktreeSpec(value)
  if (!parsed) return undefined
  if (!isAbsolute(parsed.repo)) return undefined
  const repo = resolveLaunchRepoHostPath(parsed.repo, workspaceRoot)
  return repo ? mapLaunchHostPathToContainerPath(repo, sharedHostPath, sharedContainerPath, workspaceRoot) : undefined
}

export function buildCodexWriteAllowlist(createdBy: string, allowWrite: boolean, requested: string[] = [], envValues: Record<string, string | undefined> = env): string {
  const base = (envValues.CODEX_GW_WRITE_ALLOWLIST ?? 'dev-pm').split(',')
  const extra = (envValues.A2A_LAUNCH_WRITE_ALLOWLIST_EXTRA ?? '').split(',')
  const automatic = allowWrite ? [createdBy, 'agent-1', ...requested] : []
  return [...base, ...extra, ...automatic]
    .map((s) => s.trim())
    .filter((s) => AGENT_ID_RE.test(s))
    .filter((s, i, all) => all.indexOf(s) === i)
    .join(',')
}

export function resolveCodexHostHome(explicit: string | undefined, home: string | undefined, workspaceRoot: string): string {
  if (explicit && explicit.trim()) return explicit.trim()
  const workspaceHome = workspaceRoot.match(/^\/home\/[^/]+(?:\/|$)/)?.[0]?.replace(/\/$/, '')
  if (workspaceHome) return `${workspaceHome}/.codex`
  if (home && home !== '/home/bun') return `${home}/.codex`
  return '/root/.codex'
}

// Resolve a host dotfile path for a spawned worker's login bind exactly like codex does
// (explicit env → derive from the mounted workspace home → $HOME → /root). `leaf` is the
// home-relative name, e.g. `.claude` (login dir) or `.claude.json` (login file).
function resolveHostDotPath(explicit: string | undefined, home: string | undefined, workspaceRoot: string, leaf: string): string {
  if (explicit && explicit.trim()) return explicit.trim()
  const workspaceHome = workspaceRoot.match(/^\/home\/[^/]+(?:\/|$)/)?.[0]?.replace(/\/$/, '')
  if (workspaceHome) return `${workspaceHome}/${leaf}`
  if (home && home !== '/home/bun') return `${home}/${leaf}`
  return `/root/${leaf}`
}

// The logged-in `claude` CLI keeps its session in BOTH ~/.claude (dir) and ~/.claude.json
// (file). These mirror resolveCodexHostHome / CODEX_HOST_HOME — two paths for claude's
// split login, each independently overridable via CLAUDE_HOST_HOME / CLAUDE_HOST_CONFIG.
export function resolveClaudeHostHome(explicit: string | undefined, home: string | undefined, workspaceRoot: string): string {
  return resolveHostDotPath(explicit, home, workspaceRoot, '.claude')
}
export function resolveClaudeHostConfig(explicit: string | undefined, home: string | undefined, workspaceRoot: string): string {
  return resolveHostDotPath(explicit, home, workspaceRoot, '.claude.json')
}

export function buildDockerContainerSpec(req: {
  agentId: string
  mode: LaunchMode
  createdBy: string
  label?: string
  worktree?: string
  worktreePlan?: LaunchWorktreePlan
  allowWrite?: boolean
  sandbox?: string
  effort?: string
  model?: string
  turnTimeoutMs?: number
  roleScopes?: string[]
  writeRequesters?: string[]
}): DockerContainerSpec {
  const name = `cc-agent-${req.agentId}`
  const labels: Record<string, string> = {
    'ai.alloyium.managed': 'true',
    'ai.alloyium.kind': 'codex',
    'ai.alloyium.agent_id': req.agentId,
    'ai.alloyium.created_by': req.createdBy,
  }
  if (req.label) labels['ai.alloyium.label'] = req.label
  if (req.worktree) labels['ai.alloyium.worktree'] = req.worktree
  if (req.worktreePlan?.containerCwd) labels['ai.alloyium.worktree_cwd'] = req.worktreePlan.containerCwd
  if (req.worktreePlan?.baseRef) labels['ai.alloyium.base_ref'] = req.worktreePlan.baseRef
  if (req.worktreePlan?.targetBranch) labels['ai.alloyium.target_branch'] = req.worktreePlan.targetBranch
  if (req.worktreePlan?.jobId) labels['ai.alloyium.job_id'] = req.worktreePlan.jobId
  if (req.worktreePlan?.cleanupPolicy) labels['ai.alloyium.cleanup_policy'] = req.worktreePlan.cleanupPolicy

  const allowWrite = req.allowWrite ?? false
  const sandbox = req.sandbox ?? 'danger-full-access'
  const workspaceWriteSandbox = allowWrite ? req.sandbox ?? env.CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX ?? 'workspace-write' : undefined
  const turnTimeoutMs = req.turnTimeoutMs ?? asPositiveInteger(Number(env.CODEX_GW_TURN_TIMEOUT_MS))
  const defaultCwd = req.worktreePlan?.containerCwd ?? resolveLaunchWorktreeCwd(req.worktree)
  const cwdRoots = req.worktreePlan?.cwdRoots ?? env.CODEX_BUILD_CWD_ROOTS ?? DEFAULT_CODEX_CWD_ROOTS
  const writeAllowlist = buildCodexWriteAllowlist(req.createdBy, allowWrite, req.writeRequesters)
  const binds = [
    `${SECRETS_VOLUME}:/run/secrets/a2a:ro`,
    `${CORE_SOURCE}:/run/a2a-core`,
    `${CODEX_HOST_HOME}:/home/bun/.codex`,
    `${WORKSPACES_VOLUME}:/workspaces`,
  ]
  if (SHARED_WORKSPACE_HOST_PATH) binds.push(`${SHARED_WORKSPACE_HOST_PATH}:${SHARED_WORKSPACE_CONTAINER_PATH}`)
  if (WORKSPACE_ROOT) binds.push(`${WORKSPACE_ROOT}:${WORKSPACE_ROOT}`)
  const codexSshDir = env.CODEX_SSH_DIR?.trim()
  if (codexSshDir) {
    binds.push(`${codexSshDir}:/home/bun/.ssh:ro`)
  } else {
    warn('codex_ssh_dir_unset', { action: 'skip_ssh_mount' })
  }
  const envList = [
    envPair('HOME', '/home/bun'),
    envPair('NATS_URL', NATS_URL),
    envPair('REDIS_URL', REDIS_URL),
    envPair('LOG_LEVEL', env.LOG_LEVEL ?? 'info'),
    envPair('A2A_ENABLED', '1'),
    envPair('A2A_AGENT_ID', req.agentId),
    envPair('A2A_SIG_ALG', 'ed25519'),
    envPair('A2A_SIGNING_KEY', `/run/secrets/a2a/${req.agentId}.seed`),
    envPair('A2A_TRANSPORT_AUTH', 'none'),
    envPair('A2A_MAX_SEND_BYTES', env.A2A_MAX_SEND_BYTES ?? '8192'),
    envPair('A2A_CORE_SOCK', '/run/a2a-core/core.sock'),
    envPair('A2A_PARENT_AGENT_ID', req.createdBy),
    envPair('A2A_LAUNCH_AGENT_LABEL', req.label),
    envPair('A2A_LAUNCH_WORKTREE', req.worktree),
    envPair('A2A_LAUNCH_WORKTREE_CWD', req.worktreePlan?.containerCwd),
    envPair('A2A_LAUNCH_WORKTREE_BASE_REF', req.worktreePlan?.baseRef),
    envPair('A2A_LAUNCH_WORKTREE_TARGET_BRANCH', req.worktreePlan?.targetBranch),
    envPair('A2A_LAUNCH_WORKTREE_CLEANUP_POLICY', req.worktreePlan?.cleanupPolicy),
    envPair('A2A_LAUNCH_JOB_ID', req.worktreePlan?.jobId),
    envPair('A2A_REQUESTED_ROLE_SCOPES', req.roleScopes?.length ? JSON.stringify(req.roleScopes) : undefined),
    envPair('CODEX_GW_EFFORT', req.effort ?? env.CODEX_GW_EFFORT ?? 'xhigh'),
    envPair('CODEX_GW_MODEL', req.model ?? env.CODEX_GW_MODEL),
    envPair('CODEX_GW_TURN_TIMEOUT_MS', turnTimeoutMs),
    envPair('CODEX_GW_BUDGET_MAX_PCT', env.CODEX_GW_BUDGET_MAX_PCT ?? '92'),
    envPair('CODEX_GW_ALLOW_WRITE', allowWrite ? '1' : '0'),
    envPair('CODEX_GW_WRITE_ALLOWLIST', writeAllowlist),
    envPair('CODEX_GW_CODEX_SANDBOX', sandbox),
    envPair('CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX', workspaceWriteSandbox),
    envPair('CODEX_GW_DEFAULT_CWD', defaultCwd),
    envPair('CODEX_BUILD_CWD_ROOTS', cwdRoots),
    envPair('CODEX_GW_ENABLE_A2A_TOOLS', '1'),
    envPair('CODEX_GW_A2A_TOOLS_MODE', req.mode),
    envPair('CODEX_GW_A2A_SHIM_BIN', '/usr/local/bin/a2a-shim'),
    envPair('CODEX_GW_A2A_TOOLS_REQUIRED', '1'),
    envPair('CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC', env.CODEX_GW_A2A_TOOLS_STARTUP_TIMEOUT_SEC ?? '20'),
    envPair('CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC', env.CODEX_GW_A2A_TOOLS_TOOL_TIMEOUT_SEC ?? '45'),
    // Optional external integrations: only injected into spawned agents when explicitly
    // configured. Unset => envPair() returns null and the var is omitted, so the agent's
    // brain/kai tools fall back to their own neutral defaults and fail soft (no LAN assumption).
    envPair('BRAIN_URL', env.BRAIN_URL),
    envPair('KAI_HTTP_URL', env.KAI_HTTP_URL),
    envPair('KAI_WS_URL', env.KAI_WS_URL),
    envPair('KAI_TOKEN_PATH', env.KAI_TOKEN_PATH),
  ].filter((x): x is string => !!x)

  return {
    name,
    image: CODEX_IMAGE,
    body: {
      Image: CODEX_IMAGE,
      Cmd: ['bun', 'codex_gateway.ts'],
      User: CONTAINER_USER,
      WorkingDir: '/app',
      Env: envList,
      Labels: labels,
      HostConfig: {
        Binds: binds,
        RestartPolicy: { Name: 'unless-stopped' },
      },
      NetworkingConfig: {
        EndpointsConfig: { [NETWORK]: {} },
      },
    },
  }
}

// Claude Code worker (kind=claude): the codex spec's analog for the logged-in `claude`
// CLI. Mirrors buildDockerContainerSpec but (a) binds the host claude login — BOTH
// ~/.claude (dir) AND ~/.claude.json (file) read-write, so the CLI can refresh OAuth
// tokens in place — instead of ~/.codex; (b) carries CLAUDE_GW_* (not CODEX_GW_*) knobs;
// and (c) connects straight to NATS/REDIS with no core-sock / workspace mounts — it is
// advisory-only pure inference (--tools "", no MCP), matching the claude-gw service.
export function buildClaudeContainerSpec(req: {
  agentId: string
  createdBy: string
  label?: string
  effort?: string
  model?: string
  turnTimeoutMs?: number
}): DockerContainerSpec {
  const name = `cc-agent-${req.agentId}`
  const labels: Record<string, string> = {
    'ai.alloyium.managed': 'true',
    'ai.alloyium.kind': 'claude',
    'ai.alloyium.agent_id': req.agentId,
    'ai.alloyium.created_by': req.createdBy,
  }
  if (req.label) labels['ai.alloyium.label'] = req.label

  const turnTimeoutMs = req.turnTimeoutMs ?? asPositiveInteger(Number(env.CLAUDE_GW_TURN_TIMEOUT_MS))
  // The host claude login is split across a dir and a file — bind BOTH read-write so the
  // CLI keeps refreshing OAuth tokens in place (mirrors the claude-gw service's two binds).
  const binds = [
    `${SECRETS_VOLUME}:/run/secrets/a2a:ro`,
    `${CLAUDE_HOST_HOME}:/home/bun/.claude`,
    `${CLAUDE_HOST_CONFIG}:/home/bun/.claude.json`,
  ]
  const claudeSshDir = env.CLAUDE_SSH_DIR?.trim()
  if (claudeSshDir) {
    binds.push(`${claudeSshDir}:/home/bun/.ssh:ro`)
  } else {
    warn('claude_ssh_dir_unset', { action: 'skip_ssh_mount' })
  }
  const envList = [
    envPair('HOME', '/home/bun'),
    envPair('NATS_URL', NATS_URL),
    envPair('REDIS_URL', REDIS_URL),
    envPair('LOG_LEVEL', env.LOG_LEVEL ?? 'info'),
    envPair('A2A_ENABLED', '1'),
    envPair('A2A_AGENT_ID', req.agentId),
    envPair('A2A_SIG_ALG', 'ed25519'),
    envPair('A2A_SIGNING_KEY', `/run/secrets/a2a/${req.agentId}.seed`),
    envPair('A2A_TRANSPORT_AUTH', 'none'),
    envPair('A2A_MAX_SEND_BYTES', env.A2A_MAX_SEND_BYTES ?? '8192'),
    envPair('A2A_PARENT_AGENT_ID', req.createdBy),
    envPair('A2A_LAUNCH_AGENT_LABEL', req.label),
    // CLAUDE_GW_* knobs for claude_gateway.ts: model/effort take the request override
    // first, then the launcher's env, then the gateway's own canonical defaults. The
    // pool/turn knobs are pass-through (omitted when unset → the gateway default applies).
    envPair('CLAUDE_GW_MODEL', req.model ?? env.CLAUDE_GW_MODEL ?? 'opus'),
    envPair('CLAUDE_GW_EFFORT', req.effort ?? env.CLAUDE_GW_EFFORT ?? 'high'),
    envPair('CLAUDE_GW_CWD', env.CLAUDE_GW_CWD ?? '/tmp'),
    envPair('CLAUDE_GW_POOL', env.CLAUDE_GW_POOL),
    envPair('CLAUDE_GW_SESSION_MAX_TURNS', env.CLAUDE_GW_SESSION_MAX_TURNS),
    envPair('CLAUDE_GW_THREAD_MAX_TURNS', env.CLAUDE_GW_THREAD_MAX_TURNS),
    envPair('CLAUDE_GW_TURN_TIMEOUT_MS', turnTimeoutMs),
  ].filter((x): x is string => !!x)

  return {
    name,
    image: CLAUDE_IMAGE,
    body: {
      Image: CLAUDE_IMAGE,
      Cmd: ['bun', 'claude_gateway.ts'],
      User: CONTAINER_USER,
      WorkingDir: '/app',
      Env: envList,
      Labels: labels,
      HostConfig: {
        Binds: binds,
        RestartPolicy: { Name: 'unless-stopped' },
      },
      NetworkingConfig: {
        EndpointsConfig: { [NETWORK]: {} },
      },
    },
  }
}

export function launcherRequestAuthorized(req: Request, token = LAUNCHER_TOKEN): boolean {
  if (!token) return false
  const auth = req.headers.get('authorization') ?? ''
  return timingSafeStringEqual(auth, `Bearer ${token}`)
}

function timingSafeStringEqual(actual: string, expected: string): boolean {
  const actualBuffer = Buffer.from(actual)
  const expectedBuffer = Buffer.from(expected)
  if (actualBuffer.length !== expectedBuffer.length) {
    const sameLengthActual = Buffer.alloc(expectedBuffer.length)
    actualBuffer.copy(sameLengthActual, 0, 0, Math.min(actualBuffer.length, expectedBuffer.length))
    timingSafeEqual(sameLengthActual, expectedBuffer)
    return false
  }
  return timingSafeEqual(actualBuffer, expectedBuffer)
}

async function dockerRequest(method: string, path: string, body?: unknown): Promise<{ status: number; text: string }> {
  const payload = body === undefined ? undefined : JSON.stringify(body)
  return new Promise((resolve, reject) => {
    const req = http.request({
      socketPath: DOCKER_SOCKET,
      method,
      path,
      headers: payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : undefined,
    }, (res) => {
      const chunks: Buffer[] = []
      res.on('data', (c) => chunks.push(Buffer.from(c)))
      res.on('end', () => resolve({ status: res.statusCode ?? 0, text: Buffer.concat(chunks).toString('utf8') }))
    })
    req.on('error', reject)
    if (payload) req.write(payload)
    req.end()
  })
}

async function dockerJson(method: string, path: string, body?: unknown): Promise<{ status: number; body: any }> {
  const res = await dockerRequest(method, path, body)
  let parsed: any = null
  try { parsed = res.text ? JSON.parse(res.text) : null } catch { parsed = { raw: res.text } }
  return { status: res.status, body: parsed }
}

async function ensureDockerRuntime(spec: DockerContainerSpec): Promise<{ ok: true; runtimeId: string; status: 'running' | 'starting' } | { ok: false; error: string; detail?: unknown }> {
  const existing = await dockerJson('GET', `/containers/${encodeURIComponent(spec.name)}/json`)
  if (existing.status === 200) {
    const id = String(existing.body?.Id ?? spec.name)
    const running = existing.body?.State?.Running === true
    if (!running) {
      const start = await dockerRequest('POST', `/containers/${encodeURIComponent(id)}/start`)
      if (start.status >= 300 && start.status !== 304) return { ok: false, error: 'docker_start_failed', detail: start.text }
    }
    return { ok: true, runtimeId: id.slice(0, 12), status: running ? 'running' : 'starting' }
  }
  if (existing.status !== 404) return { ok: false, error: 'docker_inspect_failed', detail: existing.body }

  const created = await dockerJson('POST', `/containers/create?name=${encodeURIComponent(spec.name)}`, spec.body)
  if (created.status >= 300) return { ok: false, error: 'docker_create_failed', detail: created.body }
  const id = String(created.body?.Id ?? spec.name)
  const start = await dockerRequest('POST', `/containers/${encodeURIComponent(id)}/start`)
  if (start.status >= 300 && start.status !== 304) return { ok: false, error: 'docker_start_failed', detail: start.text }
  return { ok: true, runtimeId: id.slice(0, 12), status: 'starting' }
}

async function waitPresence(agentId: string, timeoutMs: number): Promise<boolean> {
  if (timeoutMs <= 0) return false
  const key = `alloyium:a2a:presence:${agentId}`
  const until = Date.now() + timeoutMs
  while (Date.now() < until) {
    if (await getRedis().get(key)) return true
    await Bun.sleep(250)
  }
  return !!(await getRedis().get(key))
}

async function recordLaunch(agentId: string, record: Record<string, unknown>): Promise<void> {
  await getRedis().set(`alloyium:a2a:launcher:agent:${agentId}`, JSON.stringify(record))
  await getRedis().send('SADD', ['alloyium:a2a:launcher:index', agentId])
}

function chownIdentityFiles(files: { seedPath: string; pubPath: string; envPath: string; nkeyPath: string | null }): void {
  const [uidRaw, gidRaw] = CONTAINER_USER.split(':')
  const uid = Number(uidRaw)
  const gid = Number(gidRaw ?? uidRaw)
  if (!Number.isInteger(uid) || !Number.isInteger(gid)) return
  for (const p of [files.seedPath, files.pubPath, files.envPath, files.nkeyPath].filter((x): x is string => !!x)) {
    try { chownSync(p, uid, gid) } catch {}
  }
}

async function runGit(args: string[], cwd: string): Promise<{ ok: boolean; stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(['git', '-c', 'safe.directory=*', '-C', cwd, ...args], { stdout: 'pipe', stderr: 'pipe' })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { ok: exitCode === 0, stdout, stderr, exitCode }
}

function mapLaunchRuntimePathToContainerPath(value: string): string | undefined {
  const p = stripTrailingSlashes(value)
  const shared = stripTrailingSlashes(SHARED_WORKSPACE_CONTAINER_PATH)
  if (shared && (p === shared || p.startsWith(`${shared}/`))) return p
  const root = WORKSPACE_ROOT.trim() ? stripTrailingSlashes(WORKSPACE_ROOT) : ''
  if (root && (p === root || p.startsWith(`${root}/`))) return p
  return undefined
}

async function chownWorktreeIfRoot(path: string): Promise<{ ok: true } | { ok: false; error: string; detail?: string }> {
  const getuid = (process as any).getuid
  if (typeof getuid !== 'function' || getuid() !== 0) return { ok: true }
  const [uidRaw, gidRaw] = CONTAINER_USER.split(':')
  const uid = Number(uidRaw)
  const gid = Number(gidRaw ?? uidRaw)
  if (!Number.isInteger(uid) || !Number.isInteger(gid)) return { ok: true }
  const proc = Bun.spawn(['chown', '-R', `${uid}:${gid}`, path], { stdout: 'pipe', stderr: 'pipe' })
  const [stderr, exitCode] = await Promise.all([new Response(proc.stderr).text(), proc.exited])
  return exitCode === 0 ? { ok: true } : { ok: false, error: 'worktree_chown_failed', detail: stderr.trim().slice(0, 1000) }
}

async function finalizedRuntimePlan(plan: LaunchWorktreePlan): Promise<LaunchWorktreePlan> {
  if (!plan.launcherWorktreePath || !plan.containerCwd) return plan
  const launcherRealpath = await realpath(plan.launcherWorktreePath)
  return {
    ...plan,
    launcherWorktreePath: launcherRealpath,
    containerCwd: mapLaunchRuntimePathToContainerPath(launcherRealpath) ?? plan.containerCwd,
    cwdRoots: mapLaunchRuntimePathToContainerPath(launcherRealpath) ?? plan.containerCwd,
  }
}

async function ensureIsolatedGitWorktree(plan: LaunchWorktreePlan): Promise<{ ok: true; plan: LaunchWorktreePlan } | { ok: false; error: string; detail?: unknown }> {
  if (!plan.isolated) return { ok: true, plan }
  if (!plan.sourceRepoLauncherPath || !plan.launcherWorktreePath || !plan.baseRef || !plan.targetBranch) {
    return { ok: false, error: 'bad_worktree_plan' }
  }

  const sourceTop = await runGit(['rev-parse', '--show-toplevel'], plan.sourceRepoLauncherPath)
  if (!sourceTop.ok) return { ok: false, error: 'worktree_source_not_git', detail: sourceTop.stderr.trim() || plan.sourceRepoLauncherPath }
  const sourceRepo = sourceTop.stdout.trim()

  const base = await runGit(['rev-parse', '--verify', '--quiet', `${plan.baseRef}^{commit}`], sourceRepo)
  if (!base.ok) return { ok: false, error: 'base_ref_not_found', detail: plan.baseRef }

  const existing = await runGit(['rev-parse', '--show-toplevel'], plan.launcherWorktreePath)
  if (existing.ok) {
    const branch = await runGit(['branch', '--show-current'], plan.launcherWorktreePath)
    if (!branch.ok || branch.stdout.trim() !== plan.targetBranch) {
      return { ok: false, error: 'existing_worktree_branch_mismatch', detail: { path: plan.launcherWorktreePath, target_branch: plan.targetBranch, current_branch: branch.stdout.trim() } }
    }
    return { ok: true, plan: await finalizedRuntimePlan(plan) }
  }

  await mkdir(dirname(plan.launcherWorktreePath), { recursive: true })
  const branchExists = await runGit(['show-ref', '--verify', '--quiet', `refs/heads/${plan.targetBranch}`], sourceRepo)
  const addArgs = branchExists.ok
    ? ['worktree', 'add', plan.launcherWorktreePath, plan.targetBranch]
    : ['worktree', 'add', '-b', plan.targetBranch, plan.launcherWorktreePath, plan.baseRef]
  const added = await runGit(addArgs, sourceRepo)
  if (!added.ok) return { ok: false, error: 'git_worktree_add_failed', detail: added.stderr.trim().slice(0, 2000) }

  const chowned = await chownWorktreeIfRoot(plan.launcherWorktreePath)
  if (!chowned.ok) return chowned
  return { ok: true, plan: await finalizedRuntimePlan(plan) }
}

async function registerLaunchWorktreeCwd(plan: LaunchWorktreePlan): Promise<{ ok: true } | { ok: false; error: string; detail?: unknown }> {
  if (!plan.isolated || !plan.containerCwd) return { ok: true }
  const ok = await registerCwd(getRedis(), plan.containerCwd, { ttlS: CWD_ALLOW_TTL_S })
  if (!ok) return { ok: false, error: 'cwd_register_failed', detail: plan.containerCwd }
  if (!await isCwdRegistered(getRedis(), plan.containerCwd)) return { ok: false, error: 'cwd_register_verify_failed', detail: plan.containerCwd }
  return { ok: true }
}

function worktreePlanMetadata(plan: LaunchWorktreePlan | undefined): Record<string, unknown> | undefined {
  if (!plan) return undefined
  return {
    requested: plan.requested,
    isolated: plan.isolated,
    source_repo: plan.sourceRepoHostPath,
    source_repo_runtime: plan.sourceRepoLauncherPath,
    worktree_host_path: plan.hostWorktreePath,
    worktree_runtime_path: plan.launcherWorktreePath,
    worktree_path: plan.containerCwd,
    base_ref: plan.baseRef,
    target_branch: plan.targetBranch,
    job_id: plan.jobId,
    cleanup_policy: plan.cleanupPolicy,
    cwd_roots: plan.cwdRoots,
  }
}

async function handleCodexLaunch(input: CodexLaunchRequest): Promise<Response> {
  const agentId = asAgentId(input.agent_id)
  if (!agentId) return err('bad_agent_id')
  const createdBy = asCreatedBy(input.created_by)
  if (!createdBy) return err('bad_created_by')
  if (!allowed.has(createdBy)) return err('unauthorized', 403)
  if (agentId === createdBy) return err('self_launch_refused')
  const mode = asMode(input.mode)
  const label = asLabel(input.label)
  const dryRun = input.dry_run === true
  const worktree = input.worktree == null ? undefined : asSafeEnvValue(input.worktree, 512)
  if (input.worktree != null && !worktree) return err('bad_worktree')
  const jobId = input.job_id == null ? undefined : asSafeJobId(input.job_id)
  if (input.job_id != null && !jobId) return err('bad_job_id')
  const baseRef = input.base_ref == null ? undefined : asSafeGitRef(input.base_ref)
  if (input.base_ref != null && !baseRef) return err('bad_base_ref')
  const targetBranch = input.target_branch == null ? undefined : asSafeGitRef(input.target_branch)
  if (input.target_branch != null && !targetBranch) return err('bad_target_branch')
  const cleanupPolicy = input.cleanup_policy == null ? 'preserve' : asCleanupPolicy(input.cleanup_policy)
  if (input.cleanup_policy != null && !cleanupPolicy) return err('bad_cleanup_policy')
  const allowWrite = typeof input.policy?.allow_write === 'boolean' ? input.policy.allow_write : false
  const sandbox = typeof input.policy?.sandbox === 'string' ? input.policy.sandbox : undefined
  const effort = asSafeEnvValue(input.policy?.effort, 32)
  const model = asSafeEnvValue(input.policy?.model, 128)
  const turnTimeoutMs = asPositiveInteger(input.policy?.turn_timeout_ms)
  const roleScopes = asSafeStringList(input.policy?.role_scopes, ROLE_SCOPE_RE, 32)
  if (!roleScopes) return err('bad_role_scopes')
  const writeRequesters = asSafeStringList(input.policy?.write_requesters, AGENT_ID_RE, 32)
  if (!writeRequesters) return err('bad_write_requesters')

  if (PROVIDER !== 'docker') return err('unsupported_provider', 500, PROVIDER)
  const planned = buildLaunchWorktreePlan({ agentId, worktree, allowWrite, jobId, baseRef, targetBranch, cleanupPolicy })
  if (!planned.ok) return err(planned.error, 400, planned.detail)
  let worktreePlan = planned.plan
  if (allowWrite && !worktreePlan?.isolated) return err('write_worktree_required')

  if (dryRun) {
    const spec = buildDockerContainerSpec({ agentId, mode, createdBy, label, worktree, worktreePlan, allowWrite, sandbox, effort, model, turnTimeoutMs, roleScopes, writeRequesters })
    return json({ ok: true, dry_run: true, agent_id: agentId, parent_agent_id: createdBy, kind: 'codex', mode, provider: 'docker', runtime_id: spec.name, status: 'planned', launch_plan: worktreePlanMetadata(worktreePlan), container: spec })
  }

  if (worktreePlan?.isolated) {
    const ensured = await ensureIsolatedGitWorktree(worktreePlan)
    if (!ensured.ok) return err(ensured.error, 500, ensured.detail)
    worktreePlan = ensured.plan
  }

  const spec = buildDockerContainerSpec({ agentId, mode, createdBy, label, worktree, worktreePlan, allowWrite, sandbox, effort, model, turnTimeoutMs, roleScopes, writeRequesters })
  const identity = await onboard({ id: agentId, dir: SECRETS_DIR, redis: getRedis(), transport: 'none', natsUrl: NATS_URL, redisUrl: REDIS_URL, verify: true })
  if (identity.verified === false) return err('identity_verify_failed', 500)
  chownIdentityFiles(identity.files)

  if (worktreePlan?.isolated) {
    const registered = await registerLaunchWorktreeCwd(worktreePlan)
    if (!registered.ok) return err(registered.error, 500, registered.detail)
  }

  const runtime = await ensureDockerRuntime(spec)
  if (!runtime.ok) return err(runtime.error, 500, runtime.detail)
  const ready = await waitPresence(agentId, PRESENCE_WAIT_MS)
  const state = ready ? 'ready' : runtime.status
  await recordLaunch(agentId, {
    agent_id: agentId,
    kind: 'codex',
    mode,
    provider: 'docker',
    runtime_id: runtime.runtimeId,
    status: state,
    created_by: createdBy,
    label,
    worktree: worktreePlanMetadata(worktreePlan),
    updated_at: new Date().toISOString(),
  })
  log('codex_agent_launched', { agent_id: agentId, created_by: createdBy, runtime_id: runtime.runtimeId, status: state, worktree: worktreePlan?.containerCwd })
  return json({ ok: true, agent_id: agentId, parent_agent_id: createdBy, kind: 'codex', mode, provider: 'docker', runtime_id: runtime.runtimeId, status: state, worktree: worktreePlanMetadata(worktreePlan) })
}

// kind=claude: same guard/onboard/runtime flow as handleCodexLaunch, just with the
// claude policy (model/effort/turn_timeout — no mode/sandbox/allow_write) and spec.
async function handleClaudeLaunch(input: ClaudeLaunchRequest): Promise<Response> {
  const agentId = asAgentId(input.agent_id)
  if (!agentId) return err('bad_agent_id')
  const createdBy = asCreatedBy(input.created_by)
  if (!createdBy) return err('bad_created_by')
  if (!allowed.has(createdBy)) return err('unauthorized', 403)
  if (agentId === createdBy) return err('self_launch_refused')
  const label = asLabel(input.label)
  const dryRun = input.dry_run === true
  const effort = asSafeEnvValue(input.policy?.effort, 32)
  const model = asSafeEnvValue(input.policy?.model, 128)
  const turnTimeoutMs = asPositiveInteger(input.policy?.turn_timeout_ms)

  if (PROVIDER !== 'docker') return err('unsupported_provider', 500, PROVIDER)
  const spec = buildClaudeContainerSpec({ agentId, createdBy, label, effort, model, turnTimeoutMs })
  if (dryRun) {
    return json({ ok: true, dry_run: true, agent_id: agentId, parent_agent_id: createdBy, kind: 'claude', provider: 'docker', runtime_id: spec.name, status: 'planned', container: spec })
  }

  const identity = await onboard({ id: agentId, dir: SECRETS_DIR, redis: getRedis(), transport: 'none', natsUrl: NATS_URL, redisUrl: REDIS_URL, verify: true })
  if (identity.verified === false) return err('identity_verify_failed', 500)
  chownIdentityFiles(identity.files)

  const runtime = await ensureDockerRuntime(spec)
  if (!runtime.ok) return err(runtime.error, 500, runtime.detail)
  const ready = await waitPresence(agentId, PRESENCE_WAIT_MS)
  const state = ready ? 'ready' : runtime.status
  await recordLaunch(agentId, {
    agent_id: agentId,
    kind: 'claude',
    provider: 'docker',
    runtime_id: runtime.runtimeId,
    status: state,
    created_by: createdBy,
    label,
    updated_at: new Date().toISOString(),
  })
  log('claude_agent_launched', { agent_id: agentId, created_by: createdBy, runtime_id: runtime.runtimeId, status: state })
  return json({ ok: true, agent_id: agentId, parent_agent_id: createdBy, kind: 'claude', provider: 'docker', runtime_id: runtime.runtimeId, status: state })
}

export function startLauncher(): void {
  Bun.serve({
    hostname: HOSTNAME,
    port: PORT,
    async fetch(req) {
      const url = new URL(req.url)
      if (url.pathname === '/readyz') return json({ ok: true, provider: PROVIDER })
      if (!launcherRequestAuthorized(req)) return err(LAUNCHER_TOKEN ? 'unauthorized' : 'launcher_auth_required', LAUNCHER_TOKEN ? 401 : 503)
      if (url.pathname === '/v1/agents/codex' && req.method === 'POST') {
        let body: CodexLaunchRequest
        try { body = await req.json() } catch { return err('bad_json') }
        try { return await handleCodexLaunch(body) } catch (e) { return err('launcher_error', 500, e instanceof Error ? e.message : String(e)) }
      }
      if (url.pathname === '/v1/agents/claude' && req.method === 'POST') {
        let body: ClaudeLaunchRequest
        try { body = await req.json() } catch { return err('bad_json') }
        try { return await handleClaudeLaunch(body) } catch (e) { return err('launcher_error', 500, e instanceof Error ? e.message : String(e)) }
      }
      const m = url.pathname.match(/^\/v1\/agents\/([a-z0-9-]{1,64})$/)
      if (m && req.method === 'GET') {
      const raw = await getRedis().get(`alloyium:a2a:launcher:agent:${m[1]}`)
        return raw ? json({ ok: true, agent: JSON.parse(raw) }) : err('not_found', 404)
      }
      return err('not_found', 404)
    },
  })

  log('launcher_started', { host: HOSTNAME, port: PORT, provider: PROVIDER, auth: LAUNCHER_TOKEN ? 'bearer' : 'disabled' })
}

if (import.meta.main) startLauncher()
