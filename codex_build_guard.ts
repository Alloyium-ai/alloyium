// codex_build_guard.ts - fail-closed pre-commit guard for Model-B build output.
//
// dev-pm runs this after codex-build-gw edits a registered worktree and before any
// commit/push. The guard enforces the manifest, protected-path denylist, secret scan,
// and dirty-state invariants. Any tool or filesystem error returns ok:false.
//
// Modes:
// - committed-range: baseRef..HEAD is guarded. The worktree MUST be clean, and
//   secret scanning reads committed blobs from HEAD, never the filesystem.
// - working-tree: pre-commit/MVP flow. Changed files come from git status
//   --porcelain=v1 -z, content is read from the worktree, and every dirty path must
//   be covered by allowedManifest.
import { isAbsolute, join } from 'node:path'

/** File pattern accepted by the manifest and protected-path denylist. */
export type BuildGuardPattern = string | RegExp

/** Guard mode. Defaults to committed-range when baseRef is non-empty, otherwise working-tree. */
export type BuildGuardMode = 'committed-range' | 'working-tree'

/** Spawn result used by the guard's injectable git runner. */
export interface BuildGuardSpawnResult {
  /** Whether the command exited successfully. */
  ok: boolean
  /** Captured stdout as UTF-8 text. May contain NUL separators for -z git commands. */
  stdout: string
  /** Captured stderr as UTF-8 text. */
  stderr?: string
}

/** Injectable git runner used by tests; production defaults to Bun.spawn(['git', '-C', ...]). */
export type BuildGuardSpawn = (args: string[], worktreeDir: string) => Promise<BuildGuardSpawnResult>

/** Options for runBuildGuard. */
export interface BuildGuardOpts {
  /** Build worktree root. */
  worktreeDir: string
  /** Base ref for committed branch checks. Empty string selects working-tree mode unless mode is explicit. */
  baseRef: string
  /** Exact or glob patterns that define the authorized file manifest. */
  allowedManifest: string[]
  /** Explicit guard mode. Use working-tree only for pre-commit/MVP checks that intentionally scan the filesystem. */
  mode?: BuildGuardMode
  /** Protected path deny patterns. Passing [] intentionally disables the defaults. */
  protectedDenyPatterns?: BuildGuardPattern[]
  /** Secret regexes. Passing [] intentionally disables the defaults except the .seed-specific detector. */
  secretPatterns?: RegExp[]
  /** Test-only git runner injection. */
  spawnGit?: BuildGuardSpawn
}

/** Result of the build guard. dev-pm must refuse commit/push when ok is false. */
export interface BuildGuardResult {
  /** True only when all guard checks passed. */
  ok: boolean
  /** All violations found. Errors are reported as guard-error:* and still fail closed. */
  violations: string[]
  /** Changed files discovered from git, as repo-relative paths from NUL-safe git output. */
  changedFiles: string[]
}

/** Default protected-path denylist for build workers. This is a neutral example
 *  set flagging common sensitive-infrastructure file names; override it per call
 *  via BuildGuardOpts.protectedDenyPatterns to match your own protected paths. */
export const DEFAULT_PROTECTED_DENY_PATTERNS: BuildGuardPattern[] = [
  /(^|\/)(secrets|credentials|admin|keystore|signing)([-_.\/]|$)/i,
  /\.seed$/i,
  /(^|\/)a2a([-_.\/]|$)/i,
  /(^|\/)\.env(\.|$)/i,
]

/** Default content secret patterns for build-worker output. */
export const DEFAULT_SECRET_PATTERNS: RegExp[] = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /\b(?:sk|ghp|gho|ghs|ghu)[-_][A-Za-z0-9_-]{16,}\b/,
  /\bsk-(?:proj|svcacct|admin)-[A-Za-z0-9_-]{16,}\b/,
  /\bgh[posu]_[A-Za-z0-9]{30,}\b/,
  /\bgithub_pat_[A-Za-z0-9_]{30,}\b/,
  /\bxox[baprs]-[A-Za-z0-9-]{10,}\b/,
  /\bAKIA[0-9A-Z]{16}\b/,
  /\b[A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD|APIKEY)[A-Z0-9_]*\s*=\s*['"]?[A-Za-z0-9+/_=-]{32,}['"]?/i,
]

/** Base64 shape of a 32-byte ed25519 seed, applied only to files ending in .seed. */
export const DEFAULT_ED25519_SEED_PATTERN = /^[A-Za-z0-9+/]{43}=$/m

/**
 * Run the Model-B build guard.
 *
 * In committed-range mode, changed paths come from `git diff -z --name-only
 * baseRef..HEAD`, the worktree must be clean, and secret scanning reads
 * `HEAD:<path>` blobs. In working-tree mode, changed paths come from
 * `git status --porcelain=v1 -z` and file content is read from disk.
 */
export async function runBuildGuard(opts: BuildGuardOpts): Promise<BuildGuardResult> {
  const violations: string[] = []
  let changedFiles: string[] = []

  try {
    const git = opts.spawnGit ?? spawnGit
    const mode = opts.mode ?? (opts.baseRef.trim() ? 'committed-range' : 'working-tree')

    if (mode === 'committed-range') {
      const baseRef = opts.baseRef.trim()
      if (!baseRef) throw new Error('committed-range mode requires baseRef')

      const diff = await git(['diff', '-z', '--name-only', `${baseRef}..HEAD`], opts.worktreeDir)
      if (!diff.ok) throw new Error(`git diff failed: ${diff.stderr ?? diff.stdout}`)
      changedFiles = unique(parseNulPaths(diff.stdout))

      const status = await git(['status', '--porcelain=v1', '-z'], opts.worktreeDir)
      if (!status.ok) throw new Error(`git status failed: ${status.stderr ?? status.stdout}`)
      const dirtyFiles = parsePorcelainZ(status.stdout)
      for (const dirty of dirtyFiles) violations.push(`dirty-worktree:${dirty}`)
    } else {
      const status = await git(['status', '--porcelain=v1', '-z'], opts.worktreeDir)
      if (!status.ok) throw new Error(`git status failed: ${status.stderr ?? status.stdout}`)
      changedFiles = unique(parsePorcelainZ(status.stdout))
    }

    for (const file of changedFiles) {
      if (!isSafeRepoPath(file)) {
        violations.push(`unsafe-path:${file}`)
        continue
      }
      if (!matchesAny(opts.allowedManifest, file)) violations.push(`out-of-manifest:${file}`)
      if (matchesAny(opts.protectedDenyPatterns ?? DEFAULT_PROTECTED_DENY_PATTERNS, file)) {
        violations.push(`protected-path:${file}`)
      }
    }

    const secretPatterns = opts.secretPatterns ?? DEFAULT_SECRET_PATTERNS
    for (const file of changedFiles) {
      if (!isSafeRepoPath(file)) continue

      const text = mode === 'committed-range'
        ? await readCommittedText(git, opts.worktreeDir, file)
        : await readWorktreeText(opts.worktreeDir, file)

      if (text == null) continue
      violations.push(...scanTextForSecrets(file, text, secretPatterns))
    }

    return { ok: violations.length === 0, violations, changedFiles }
  } catch (e) {
    return {
      ok: false,
      violations: [`guard-error:${e instanceof Error ? e.message : String(e)}`],
      changedFiles,
    }
  }
}

async function spawnGit(args: string[], worktreeDir: string): Promise<BuildGuardSpawnResult> {
  const proc = Bun.spawn(['git', '-C', worktreeDir, ...args], { stdout: 'pipe', stderr: 'pipe' })
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { ok: code === 0, stdout, stderr }
}

function parseNulPaths(stdout: string): string[] {
  return stdout.split('\0').filter((p) => p.length > 0)
}

function parsePorcelainZ(stdout: string): string[] {
  const parts = stdout.split('\0')
  const files: string[] = []

  for (let i = 0; i < parts.length; i++) {
    const rec = parts[i]
    if (!rec) continue

    const status = rec.slice(0, 2)
    const path = rec.length >= 4 ? rec.slice(3) : ''
    if (path) files.push(path)

    if (status.includes('R') || status.includes('C')) {
      const oldPath = parts[++i]
      if (oldPath) files.push(oldPath)
    }
  }

  return unique(files)
}

function unique<T>(items: T[]): T[] {
  return Array.from(new Set(items))
}

function isSafeRepoPath(path: string): boolean {
  if (!path || path.includes('\0') || isAbsolute(path)) return false
  return !path.split('/').some((part) => part === '..')
}

function matchesAny(patterns: BuildGuardPattern[], file: string): boolean {
  return patterns.some((pattern) => matchesPattern(pattern, file))
}

function matchesPattern(pattern: BuildGuardPattern, file: string): boolean {
  if (pattern instanceof RegExp) {
    pattern.lastIndex = 0
    return pattern.test(file)
  }
  if (/[*?[\]]/.test(pattern)) return globToRegExp(pattern).test(file)
  return pattern === file
}

function globToRegExp(glob: string): RegExp {
  let out = '^'
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i]!
    if (c === '*') {
      if (glob[i + 1] === '*') {
        out += '.*'
        i++
      } else {
        out += '[^/]*'
      }
    } else if (c === '?') {
      out += '[^/]'
    } else {
      out += c.replace(/[|\\{}()[\]^$+?.]/g, '\\$&')
    }
  }
  return new RegExp(out + '$')
}

async function readCommittedText(git: BuildGuardSpawn, worktreeDir: string, file: string): Promise<string | null> {
  const spec = `HEAD:${file}`
  const exists = await git(['cat-file', '-e', spec], worktreeDir)
  if (!exists.ok) return null

  const shown = await git(['show', spec], worktreeDir)
  if (!shown.ok) throw new Error(`git show failed for ${file}: ${shown.stderr ?? shown.stdout}`)
  return shown.stdout
}

async function readWorktreeText(worktreeDir: string, file: string): Promise<string | null> {
  const f = Bun.file(join(worktreeDir, file))
  if (!(await f.exists())) return null
  return await f.text()
}

function scanTextForSecrets(file: string, text: string, patterns: RegExp[]): string[] {
  const violations: string[] = []

  for (const pattern of patterns) {
    pattern.lastIndex = 0
    if (pattern.test(text)) violations.push(`secret:${file}:${pattern.source}`)
  }

  if (/\.seed$/i.test(file)) {
    DEFAULT_ED25519_SEED_PATTERN.lastIndex = 0
    if (DEFAULT_ED25519_SEED_PATTERN.test(text.trim())) {
      violations.push(`secret:${file}:ed25519_seed_base64_32b`)
    }
  }

  return violations
}
