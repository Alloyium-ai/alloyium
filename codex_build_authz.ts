import path from 'node:path'
import { realpathSync, statSync } from 'node:fs'

/** Minimal Redis surface used by codex build authorization. */
export interface BuildAuthzRedis {
  send(cmd: string, args: string[]): Promise<any>
}

/** Optional registration controls for a cwd allow-list entry. */
export interface RegisterCwdOpts {
  ttlS?: number
}

/** Inputs supplied by a codex.job requester for a workspace-write job. */
export interface AuthorizeWriteJobArgs {
  requesterId: string
  cwd: string
  threadKey?: string
}

/** Instance-local policy for the workspace-write authorization gate. */
export interface AuthorizeWriteJobConfig {
  allowWrite: boolean
  allowlist: string[]
  cwdRoots: string[]
}

/** Result of cwd canonicalization and root-prefix validation. */
export type CanonicalizeCwdResult =
  | { ok: true; realpath: string }
  | { ok: false; reason: 'cwd-not-absolute' | 'cwd-traversal' | 'cwd-missing' | 'cwd-not-dir' | 'cwd-symlink-escape' }

/** Result of the complete workspace-write authorization gate. */
export type AuthorizeWriteJobResult =
  | { ok: true; realpath: string }
  | { ok: false; reason: string }

/** Redis set of canonical realpaths allowed for codex-build workspace-write jobs. */
export const CWD_ALLOW_SET_KEY = 'alloyium:a2a:codex-build:cwd-allow'

/** Redis sorted-set companion for per-entry cwd allow-list expiry timestamps. */
export const CWD_ALLOW_EXPIRY_ZSET_KEY = `${CWD_ALLOW_SET_KEY}:expiry`

/** Redis key prefix binding a thread_key to its first workspace-write requester. */
export const THREAD_OWNER_KEY_PREFIX = 'alloyium:a2a:codex-build:thread-owner:'

/** TTL for thread_key ownership claims, in seconds. */
export const THREAD_OWNER_TTL_S = Math.max(1, Number(process.env.CODEX_BUILD_THREAD_OWNER_TTL_S ?? 24 * 3600) || 24 * 3600)

/** Redis operation timeout for authz checks; timeout is treated as deny. */
export const BUILD_AUTHZ_REDIS_TIMEOUT_MS = Math.max(1, Number(process.env.CODEX_BUILD_AUTHZ_REDIS_TIMEOUT_MS ?? 2500) || 2500)

function envCwdRoots(): string[] {
  return (process.env.CODEX_BUILD_CWD_ROOTS ?? '').split(',').map((s) => s.trim()).filter(Boolean)
}

function hasTraversalSegment(p: string): boolean {
  return p.split(/[\\/]+/).includes('..')
}

function isUnderRoot(realpath: string, root: string): boolean {
  const rel = path.relative(root, realpath)
  return rel === '' || (!!rel && !rel.startsWith('..') && !path.isAbsolute(rel))
}

function canonicalRoot(root: string): string | null {
  if (!path.isAbsolute(root) || hasTraversalSegment(root)) return null
  const normalized = path.normalize(root)
  if (hasTraversalSegment(normalized)) return null
  try {
    const real = path.normalize(realpathSync(normalized))
    return statSync(real).isDirectory() ? real : null
  } catch {
    return null
  }
}

function normalizeRegisteredRealpath(realpath: string): string | null {
  if (typeof realpath !== 'string' || !path.isAbsolute(realpath) || hasTraversalSegment(realpath)) return null
  const normalized = path.normalize(realpath)
  return hasTraversalSegment(normalized) ? null : normalized
}

/**
 * Resolve and validate a cwd for workspace-write use.
 *
 * The input must be absolute, contain no `..` path segment, exist, be a directory,
 * and resolve via symlinks to a realpath under one configured allowed root.
 */
export function canonicalizeCwd(cwd: string, cwdRoots: string[] = envCwdRoots()): CanonicalizeCwdResult {
  if (typeof cwd !== 'string' || !path.isAbsolute(cwd)) return { ok: false, reason: 'cwd-not-absolute' }
  if (hasTraversalSegment(cwd)) return { ok: false, reason: 'cwd-traversal' }

  const normalized = path.normalize(cwd)
  if (hasTraversalSegment(normalized)) return { ok: false, reason: 'cwd-traversal' }

  let real: string
  try {
    real = path.normalize(realpathSync(normalized))
  } catch {
    return { ok: false, reason: 'cwd-missing' }
  }

  try {
    if (!statSync(real).isDirectory()) return { ok: false, reason: 'cwd-not-dir' }
  } catch {
    return { ok: false, reason: 'cwd-missing' }
  }

  const roots = cwdRoots.map(canonicalRoot).filter((r): r is string => r !== null)
  if (!roots.some((root) => isUnderRoot(real, root))) return { ok: false, reason: 'cwd-symlink-escape' }

  return { ok: true, realpath: real }
}

function withTimeout<T>(p: Promise<T> | T, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<T>((_, reject) => {
    t = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
  })
  return Promise.race([Promise.resolve(p), timeout]).finally(() => { if (t) clearTimeout(t) })
}

async function redisSend(redis: BuildAuthzRedis, cmd: string, args: string[]): Promise<any> {
  return withTimeout(redis.send(cmd, args), BUILD_AUTHZ_REDIS_TIMEOUT_MS, `redis.${cmd}`)
}

function redisBool(v: any): boolean {
  return v === true || v === 1 || v === '1' || v === 'OK'
}

/**
 * Register a canonical realpath as eligible for codex-build workspace-write jobs.
 *
 * Scheme: `CWD_ALLOW_SET_KEY` stores active realpaths with `SADD`; optional per-entry
 * expiry is stored in `CWD_ALLOW_EXPIRY_ZSET_KEY` as score=`expiresAtMs`,
 * member=`realpath`. Expired entries are denied and lazily removed by
 * `isCwdRegistered`.
 */
export async function registerCwd(redis: BuildAuthzRedis, realpath: string, opts: RegisterCwdOpts = {}): Promise<boolean> {
  const rp = normalizeRegisteredRealpath(realpath)
  if (!rp) return false

  try {
    if (opts.ttlS !== undefined) {
      const ttlS = Number(opts.ttlS)
      if (!Number.isFinite(ttlS)) return false
      await redisSend(redis, 'ZADD', [CWD_ALLOW_EXPIRY_ZSET_KEY, String(Date.now() + Math.max(0, ttlS) * 1000), rp])
    } else {
      await redisSend(redis, 'ZREM', [CWD_ALLOW_EXPIRY_ZSET_KEY, rp])
    }
    await redisSend(redis, 'SADD', [CWD_ALLOW_SET_KEY, rp])
    return true
  } catch {
    return false
  }
}

/**
 * Unregister a canonical realpath from codex-build workspace-write eligibility.
 *
 * A temporary expired tombstone is written before removal so a partial Redis failure
 * after the tombstone denies rather than accidentally preserving access.
 */
export async function unregisterCwd(redis: BuildAuthzRedis, realpath: string): Promise<boolean> {
  const rp = normalizeRegisteredRealpath(realpath)
  if (!rp) return false

  try {
    await redisSend(redis, 'ZADD', [CWD_ALLOW_EXPIRY_ZSET_KEY, String(Date.now() - 1), rp])
    await redisSend(redis, 'SREM', [CWD_ALLOW_SET_KEY, rp])
    await redisSend(redis, 'ZREM', [CWD_ALLOW_EXPIRY_ZSET_KEY, rp])
    return true
  } catch {
    return false
  }
}

/**
 * Return whether a canonical realpath is currently registered and unexpired.
 *
 * Redis errors are fail-closed and return false.
 */
export async function isCwdRegistered(redis: BuildAuthzRedis, realpath: string): Promise<boolean> {
  const rp = normalizeRegisteredRealpath(realpath)
  if (!rp) return false

  try {
    const score = await redisSend(redis, 'ZSCORE', [CWD_ALLOW_EXPIRY_ZSET_KEY, rp])
    if (score != null) {
      const expiresAt = Number(score)
      if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
        await redisSend(redis, 'SREM', [CWD_ALLOW_SET_KEY, rp]).catch(() => {})
        await redisSend(redis, 'ZREM', [CWD_ALLOW_EXPIRY_ZSET_KEY, rp]).catch(() => {})
        return false
      }
    }
    return redisBool(await redisSend(redis, 'SISMEMBER', [CWD_ALLOW_SET_KEY, rp]))
  } catch {
    return false
  }
}

async function claimThreadKey(redis: BuildAuthzRedis, threadKey: string, requesterId: string): Promise<{ ok: true } | { ok: false; reason: string }> {
  const key = THREAD_OWNER_KEY_PREFIX + threadKey
  const claimed = await redisSend(redis, 'SET', [key, requesterId, 'NX', 'EX', String(THREAD_OWNER_TTL_S)])
  if (claimed) return { ok: true }

  let owner = await redisSend(redis, 'GET', [key])
  if (owner == null) {
    const reclaimed = await redisSend(redis, 'SET', [key, requesterId, 'NX', 'EX', String(THREAD_OWNER_TTL_S)])
    if (reclaimed) return { ok: true }
    owner = await redisSend(redis, 'GET', [key])
  }

  if (String(owner) !== requesterId) return { ok: false, reason: 'thread-key-conflict' }
  if (!redisBool(await redisSend(redis, 'EXPIRE', [key, String(THREAD_OWNER_TTL_S)]))) return { ok: false, reason: 'redis-error' }
  return { ok: true }
}

/**
 * Pure workspace-write authorization gate for codex-build jobs.
 *
 * Check order is fail-closed: instance write flag, requester allowlist, cwd
 * canonicalization/root check, dev-pm cwd registry, required thread_key, then
 * thread_key ownership. The gateway rejects missing thread_key earlier, but this
 * module also denies it in depth.
 */
export async function authorizeWriteJob(redis: BuildAuthzRedis, args: AuthorizeWriteJobArgs, cfg: AuthorizeWriteJobConfig): Promise<AuthorizeWriteJobResult> {
  try {
    if (!cfg.allowWrite) return { ok: false, reason: 'write-disabled' }
    if (!(cfg.allowlist ?? []).includes(args.requesterId)) return { ok: false, reason: 'requester-not-allowlisted' }

    const cwd = canonicalizeCwd(args.cwd, cfg.cwdRoots ?? [])
    if (!cwd.ok) return cwd

    if (!await isCwdRegistered(redis, cwd.realpath)) return { ok: false, reason: 'cwd-not-registered' }

    const threadKey = typeof args.threadKey === 'string' ? args.threadKey.trim() : ''
    if (!threadKey) return { ok: false, reason: 'thread-key-required' }

    const claimed = await claimThreadKey(redis, threadKey, args.requesterId)
    if (!claimed.ok) return claimed

    return { ok: true, realpath: cwd.realpath }
  } catch {
    return { ok: false, reason: 'authz-error' }
  }
}
