import { describe, expect, test } from 'bun:test'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { buildBuildJob, finishBuild, parseBuildResult, type BuildClientRedis } from '../codex_build_client.ts'
import { runBuildGuard, type BuildGuardSpawn } from '../codex_build_guard.ts'
import { routeBuild, recordOutcome, type GovernorCfg, type GovernorState } from '../codex_routing_governor.ts'
import { isCwdRegistered } from '../codex_build_authz.ts'

function fakeRedis(opts: { throwOnBlobSet?: boolean; refuseSadd?: boolean } = {}) {
  const blobs = new Map<string, string>()
  const sets = new Map<string, Set<string>>()
  const zsets = new Map<string, Map<string, number>>()
  const r = {
    blobs,
    sets,
    async get(k: string) {
      return blobs.has(k) ? blobs.get(k)! : null
    },
    async send(cmd: string, args: string[]) {
      const c = cmd.toUpperCase()
      if (c === 'SET') {
        if (opts.throwOnBlobSet) throw new Error('blob redis down')
        const [k, v, ...rest] = args
        if (rest.includes('NX') && blobs.has(k!)) return null
        blobs.set(k!, v!)
        return 'OK'
      }
      if (c === 'GET') return blobs.has(args[0]!) ? blobs.get(args[0]!)! : null
      if (c === 'DEL') return blobs.delete(args[0]!) ? 1 : 0
      if (c === 'SADD') {
        if (opts.refuseSadd) return 0
        const s = sets.get(args[0]!) ?? new Set<string>()
        sets.set(args[0]!, s)
        const before = s.size
        for (const a of args.slice(1)) s.add(a)
        return s.size - before
      }
      if (c === 'SREM') {
        const s = sets.get(args[0]!) ?? new Set<string>()
        let n = 0
        for (const a of args.slice(1)) if (s.delete(a)) n++
        return n
      }
      if (c === 'SISMEMBER') return sets.get(args[0]!)?.has(args[1]!) ? 1 : 0
      if (c === 'ZADD') {
        const zk = zsets.get(args[0]!) ?? new Map<string, number>()
        zsets.set(args[0]!, zk)
        const existed = zk.has(args[2]!)
        zk.set(args[2]!, Number(args[1]))
        return existed ? 0 : 1
      }
      if (c === 'ZREM') {
        const zk = zsets.get(args[0]!)
        if (!zk) return 0
        let n = 0
        for (const a of args.slice(1)) if (zk.delete(a)) n++
        return n
      }
      if (c === 'ZSCORE') {
        const v = zsets.get(args[0]!)?.get(args[1]!)
        return v === undefined ? null : String(v)
      }
      if (c === 'EXPIRE') return 1
      throw new Error('unsupported redis ' + cmd)
    },
  }
  return r as BuildClientRedis & { blobs: Map<string, string>; sets: Map<string, Set<string>> }
}

async function withWorktree(files: Record<string, string>, fn: (dir: string) => Promise<void>) {
  const dir = await mkdtemp(join(tmpdir(), 'codex-build-guard-'))
  try {
    for (const [path, text] of Object.entries(files)) {
      const abs = join(dir, path)
      await mkdir(dirname(abs), { recursive: true })
      await writeFile(abs, text)
    }
    await fn(dir)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
}

function z(records: string[]): string {
  return records.join('\0') + (records.length ? '\0' : '')
}

function statusZ(files: string[], code = ' M'): string {
  return z(files.map((f) => `${code} ${f}`))
}

function fakeGit(a: {
  changed?: string[]
  status?: string
  committed?: Record<string, string>
  fail?: boolean
} = {}): BuildGuardSpawn {
  return async (args) => {
    if (a.fail) throw new Error('git unavailable')
    if (args[0] === 'diff') return { ok: true, stdout: z(a.changed ?? []), stderr: '' }
    if (args[0] === 'status') return { ok: true, stdout: a.status ?? '', stderr: '' }
    if (args[0] === 'cat-file') {
      const file = String(args[2] ?? '').replace(/^HEAD:/, '')
      return { ok: Object.prototype.hasOwnProperty.call(a.committed ?? {}, file), stdout: '', stderr: '' }
    }
    if (args[0] === 'show') {
      const file = String(args[1] ?? '').replace(/^HEAD:/, '')
      if (Object.prototype.hasOwnProperty.call(a.committed ?? {}, file)) {
        return { ok: true, stdout: a.committed![file]!, stderr: '' }
      }
      return { ok: false, stdout: '', stderr: 'missing object' }
    }
    return { ok: false, stdout: '', stderr: 'unexpected git args' }
  }
}

const governorCfg: Partial<GovernorCfg> = {
  anthropic429Hi: 0.1,
  anthropic429Lo: 0.05,
  codexBudgetHi: 90,
  codexBudgetLo: 80,
  breakerConsecutiveFailures: 2,
  breakerCooldownMs: 60_000,
}

function governorState(): GovernorState {
  return {
    breakers: {
      claude: { consecutiveSheds: 0, consecutive429s: 0, consecutiveFailures: 0, openUntil: 0 },
      codex: { consecutiveSheds: 0, consecutive429s: 0, consecutiveFailures: 0, openUntil: 0 },
    },
  }
}

const neutralSignals = {
  now: 1_800_000_000_000,
  peakWindow: false,
  anthropic429Rate: 0,
  codexBudgetPct: 20,
}

describe('runBuildGuard', () => {
  test('out-of-manifest produces a violation', async () => {
    await withWorktree({ 'src/other.ts': 'export const x = 1\n' }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['src/allowed.ts'],
        spawnGit: fakeGit({ changed: ['src/other.ts'], committed: { 'src/other.ts': 'export const x = 1\n' } }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations).toContain('out-of-manifest:src/other.ts')
    })
  })

  test('protected paths secrets, credentials, .seed, and a2a are denied', async () => {
    await withWorktree({}, async (dir) => {
      const changed = ['secrets/store.ts', 'credentials/job.ts', 'keys/dev.seed', 'a2a/agent.ts']
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['**'],
        spawnGit: fakeGit({ changed, committed: Object.fromEntries(changed.map((f) => [f, ''])) }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations).toEqual(expect.arrayContaining(changed.map((f) => `protected-path:${f}`)))
    })
  })

  test('protected file-form cases are denied without false positives', async () => {
    await withWorktree({}, async (dir) => {
      const denied = ['secrets.ts', 'admin.ts', 'a2a-channel.ts']
      const deniedResult = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['**'],
        spawnGit: fakeGit({ changed: denied, committed: Object.fromEntries(denied.map((f) => [f, ''])) }),
      })
      expect(deniedResult.ok).toBe(false)
      expect(deniedResult.violations).toEqual(expect.arrayContaining(denied.map((f) => `protected-path:${f}`)))

      const allowed = ['secretsmanager.ts', 'administrate.ts', 'keystored.ts', 'designing.ts']
      const allowedResult = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['**'],
        spawnGit: fakeGit({ changed: allowed, committed: Object.fromEntries(allowed.map((f) => [f, ''])) }),
      })
      expect(allowedResult.ok).toBe(true)
      expect(allowedResult.violations).toEqual([])
    })
  })

  test('[#38 MEDIUM] underscore-separated core path forms ARE protected (regression)', async () => {
    await withWorktree({}, async (dir) => {
      // Underscore-separated forms must be flagged: the separator class is
      // ([-_.\/]|$), matching the a2a sibling — not just slash/dot boundaries.
      const denied = ['secrets_store.ts', 'credentials_loader.ts', 'admin_control.ts', 'keystore_service.ts', 'signing_key.ts', 'src/secrets_store.ts']
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['**'],
        spawnGit: fakeGit({ changed: denied, committed: Object.fromEntries(denied.map((f) => [f, ''])) }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations).toEqual(expect.arrayContaining(denied.map((f) => `protected-path:${f}`)))
    })
  })

  test('secret pattern in changed worktree content produces a violation', async () => {
    await withWorktree({ 'src/ok.ts': 'const token = "sk-1234567890abcdef"\n' }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: '',
        allowedManifest: ['src/**'],
        spawnGit: fakeGit({ status: statusZ(['src/ok.ts']) }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations.some((v) => v.startsWith('secret:src/ok.ts:'))).toBe(true)
    })
  })

  test('real token vectors are detected', async () => {
    const tokens = [
      `ghp_${'A'.repeat(30)}`,
      `gho_${'B'.repeat(30)}`,
      `sk-proj-${'C'.repeat(16)}`,
      `github_pat_${'D'.repeat(30)}`,
      `xoxb-1234567890-1234567890-${'e'.repeat(16)}`,
    ].join('\n')

    await withWorktree({ 'src/tokens.ts': tokens }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: '',
        allowedManifest: ['src/**'],
        spawnGit: fakeGit({ status: statusZ(['src/tokens.ts']) }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations.filter((v) => v.startsWith('secret:src/tokens.ts:')).length).toBeGreaterThanOrEqual(5)
    })
  })

  test('baseRef mode scans committed blobs, not the clean worktree file', async () => {
    const ghp = `ghp_${'A'.repeat(30)}`
    await withWorktree({ 'src/secret.ts': 'export const clean = true\n' }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['src/**'],
        spawnGit: fakeGit({
          changed: ['src/secret.ts'],
          status: '',
          committed: { 'src/secret.ts': `const token = "${ghp}"\n` },
        }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations.some((v) => v.startsWith('secret:src/secret.ts:'))).toBe(true)
    })
  })

  test('baseRef mode refuses a dirty worktree', async () => {
    await withWorktree({ 'src/clean.ts': 'export const clean = true\n' }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['src/**'],
        spawnGit: fakeGit({
          changed: ['src/clean.ts'],
          status: statusZ(['src/clean.ts']),
          committed: { 'src/clean.ts': 'export const clean = true\n' },
        }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations).toContain('dirty-worktree:src/clean.ts')
    })
  })

  test('clean change inside manifest passes', async () => {
    await withWorktree({ 'src/ok.ts': 'export const ok = true\n' }, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['src/**'],
        spawnGit: fakeGit({ changed: ['src/ok.ts'], committed: { 'src/ok.ts': 'export const ok = true\n' } }),
      })
      expect(r).toMatchObject({ ok: true, violations: [], changedFiles: ['src/ok.ts'] })
    })
  })

  test('git errors fail closed', async () => {
    await withWorktree({}, async (dir) => {
      const r = await runBuildGuard({
        worktreeDir: dir,
        baseRef: 'origin/main',
        allowedManifest: ['**'],
        spawnGit: fakeGit({ fail: true }),
      })
      expect(r.ok).toBe(false)
      expect(r.violations[0]).toContain('guard-error:git unavailable')
    })
  })
})

describe('routeBuild', () => {
  test('orchestrator routes to Claude', () => {
    const r = routeBuild({ role: 'orchestrator' }, neutralSignals, governorState(), governorCfg)
    expect(r.target).toBe('claude')
  })

  test('role reserve matching trims and lowercases role/config entries', () => {
    const r = routeBuild(
      { role: ' Standing_PM ' },
      { ...neutralSignals, peakWindow: true },
      governorState(),
      { ...governorCfg, claudeReserveRoles: [' STANDING_PM '] },
    )
    expect(r.target).toBe('claude')
    expect(r.reason).toContain('reserved-role')
  })

  test('protected paths route to Claude with human review reason', () => {
    const r = routeBuild({ touchesProtectedPaths: true }, neutralSignals, governorState(), governorCfg)
    expect(r.target).toBe('claude')
    expect(r.reason).toContain('protected-path')
  })

  test('peak-window non-critical work spills to Codex', () => {
    const r = routeBuild({ fit: 'either', peakCritical: false }, { ...neutralSignals, peakWindow: true }, governorState(), governorCfg)
    expect(r.target).toBe('codex')
  })

  test('Anthropic 429 above hi routes to Codex when Codex budget is ok', () => {
    const r = routeBuild({ fit: 'either' }, { ...neutralSignals, anthropic429Rate: 0.2 }, governorState(), governorCfg)
    expect(r.target).toBe('codex')
    expect(r.reason).toContain('anthropic-429')
  })

  test('both pressured routes to queue', () => {
    const r = routeBuild(
      { fit: 'either' },
      { ...neutralSignals, anthropic429Rate: 0.2, codexBudgetPct: 95 },
      governorState(),
      governorCfg,
    )
    expect(r.target).toBe('queue')
    expect(r.reason).toBe('both-pressured')
  })

  test('hysteresis keeps the Anthropic pressure latch in the band', () => {
    const state = governorState()
    expect(routeBuild({ fit: 'either' }, { ...neutralSignals, anthropic429Rate: 0.2 }, state, governorCfg).target).toBe('codex')
    const r = routeBuild({ fit: 'either' }, { ...neutralSignals, anthropic429Rate: 0.07 }, state, governorCfg)
    expect(r.target).toBe('codex')
    expect(r.reason).toContain('anthropic-429')
  })

  test('breaker opens after consecutive sheds and routes away', () => {
    const state = governorState()
    recordOutcome(state, 'codex', 'shed', governorCfg, neutralSignals.now)
    recordOutcome(state, 'codex', 'shed', governorCfg, neutralSignals.now + 1)
    const r = routeBuild({ fit: 'codex_strong' }, { ...neutralSignals, now: neutralSignals.now + 2 }, state, governorCfg)
    expect(r.target).toBe('claude')
    expect(r.reason).toContain('codex-breaker-open')
  })

  test('role reserve keeps configured role on Claude during a peak window', () => {
    const r = routeBuild(
      { role: 'release-captain', peakCritical: false },
      { ...neutralSignals, peakWindow: true },
      governorState(),
      { ...governorCfg, claudeReserveRoles: ['release-captain'] },
    )
    expect(r.target).toBe('claude')
    expect(r.reason).toContain('reserved-role')
  })
})

describe('codex_build_client', () => {
  test('buildBuildJob produces a valid workspace-write codex.job.request.v1 body and registers cwd', async () => {
    const redis = fakeRedis()
    const cwd = '/tmp/codex-build-worktree'
    const built = await buildBuildJob(redis, {
      jobId: 'job-1',
      threadKey: 'thread-a',
      cwdRealpath: cwd,
      promptText: 'edit the requested files',
    })

    expect(await isCwdRegistered(redis, cwd)).toBe(true)
    expect(built.jobId).toBe('job-1')
    expect(built.body).toMatchObject({
      schema: 'codex.job.request.v1',
      job_id: 'job-1',
      thread_key: 'thread-a',
      sandbox: 'workspace-write',
      cwd,
      approval_policy: 'never',
      budget_policy: { max_primary_used_percent: 92 },
    })
    expect((built.body.input as any[])[0]).toMatchObject({ type: 'text', text: 'edit the requested files' })

    await finishBuild(redis, cwd)
    expect(await isCwdRegistered(redis, cwd)).toBe(false)
  })

  test('read-only dispatch does not register the cwd', async () => {
    const redis = fakeRedis()
    const cwd = '/tmp/codex-build-readonly'
    const built = await buildBuildJob(redis, {
      jobId: 'job-ro',
      threadKey: 'thread-ro',
      cwdRealpath: cwd,
      promptText: 'inspect only',
      sandbox: 'read-only',
    })

    expect(built.body).toMatchObject({ sandbox: 'read-only', approval_policy: 'never', cwd })
    expect(await isCwdRegistered(redis, cwd)).toBe(false)
  })

  test('registration failure is detected', async () => {
    const redis = fakeRedis({ refuseSadd: true })
    await expect(buildBuildJob(redis, {
      jobId: 'job-bad-reg',
      threadKey: 'thread-a',
      cwdRealpath: '/tmp/codex-build-refused',
      promptText: 'edit',
    })).rejects.toThrow(/cwd registration failed/)
  })

  test('write registration is unregistered in finally when claim-check construction throws', async () => {
    const redis = fakeRedis({ throwOnBlobSet: true })
    const cwd = '/tmp/codex-build-throw'
    await expect(buildBuildJob(redis, {
      jobId: 'job-throw',
      threadKey: 'thread-a',
      cwdRealpath: cwd,
      promptText: 'x'.repeat(9000),
    })).rejects.toThrow(/blob redis down/)
    expect(await isCwdRegistered(redis, cwd)).toBe(false)
  })

  test('parseBuildResult normalizes accepted, completed envelope, failed, and rejected', () => {
    expect(parseBuildResult({ schema: 'codex.job.accepted.v1', job_id: 'j1' })).toMatchObject({
      ok: true,
      status: 'accepted',
    })

    const completed = parseBuildResult({
      schema: 'codex.job.completed.v1',
      job_id: 'j1',
      status: 'completed',
      output: JSON.stringify({
        files_touched: ['codex_build_client.ts'],
        cwd_realpath: '/tmp/w',
        thread_key: 'thread-a',
        tests_run: ['bun test tests/codex_build_tooling.test.ts'],
        escalations_attempted: [],
        notes: 'done',
      }),
      result_ref: 'blob-key',
      sha256: 'f'.repeat(64),
      len: 123,
    })
    expect(completed.ok).toBe(true)
    expect(completed.envelope?.files_touched).toEqual(['codex_build_client.ts'])
    expect(completed.resultRef).toMatchObject({ result_ref: 'blob-key', len: 123 })

    expect(parseBuildResult({ schema: 'codex.job.failed.v1', error: 'boom' })).toMatchObject({
      ok: false,
      status: 'failed',
      failed: { error: 'boom' },
    })

    expect(parseBuildResult({ schema: 'codex.job.rejected.v1', reason: 'budget-shed', retry_after_ms: 5000 })).toMatchObject({
      ok: false,
      status: 'rejected',
      rejected: { reason: 'budget-shed', retryAfterMs: 5000 },
    })
  })
})
