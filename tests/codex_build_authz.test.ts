import { describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, rm, symlink } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  authorizeWriteJob, canonicalizeCwd, registerCwd, unregisterCwd, isCwdRegistered,
  type BuildAuthzRedis,
} from '../codex_build_authz.ts'

function fakeRedis() {
  const sets = new Map<string, Set<string>>()
  const zsets = new Map<string, Map<string, number>>()
  const strings = new Map<string, string>()
  const expiries = new Map<string, number>()

  const purge = (key: string) => {
    const exp = expiries.get(key)
    if (exp !== undefined && exp <= Date.now()) {
      expiries.delete(key)
      strings.delete(key)
    }
  }

  return {
    async send(cmd: string, args: string[]) {
      cmd = cmd.toUpperCase()
      if (cmd === 'SADD') {
        const set = sets.get(args[0]!) ?? new Set<string>()
        sets.set(args[0]!, set)
        let added = 0
        for (const member of args.slice(1)) if (!set.has(member)) { set.add(member); added++ }
        return added
      }
      if (cmd === 'SREM') {
        const set = sets.get(args[0]!)
        if (!set) return 0
        let removed = 0
        for (const member of args.slice(1)) if (set.delete(member)) removed++
        return removed
      }
      if (cmd === 'SISMEMBER') return sets.get(args[0]!)?.has(args[1]!) ? 1 : 0
      if (cmd === 'ZADD') {
        const zset = zsets.get(args[0]!) ?? new Map<string, number>()
        zsets.set(args[0]!, zset)
        const existed = zset.has(args[2]!)
        zset.set(args[2]!, Number(args[1]))
        return existed ? 0 : 1
      }
      if (cmd === 'ZSCORE') {
        const score = zsets.get(args[0]!)?.get(args[1]!)
        return score === undefined ? null : String(score)
      }
      if (cmd === 'ZREM') return zsets.get(args[0]!)?.delete(args[1]!) ? 1 : 0
      if (cmd === 'SET') {
        const key = args[0]!, value = args[1]!, rest = args.slice(2).map((s) => s.toUpperCase())
        purge(key)
        if (rest.includes('NX') && strings.has(key)) return null
        strings.set(key, value)
        const ex = rest.indexOf('EX')
        if (ex >= 0) expiries.set(key, Date.now() + Number(args[2 + ex + 1]) * 1000)
        return 'OK'
      }
      if (cmd === 'GET') {
        purge(args[0]!)
        return strings.get(args[0]!) ?? null
      }
      if (cmd === 'EXPIRE') {
        purge(args[0]!)
        if (!strings.has(args[0]!)) return 0
        expiries.set(args[0]!, Date.now() + Number(args[1]) * 1000)
        return 1
      }
      throw new Error('unsupported ' + cmd)
    },
  } as BuildAuthzRedis
}

async function withFixture<T>(fn: (f: { base: string; root: string; outside: string }) => Promise<T> | T): Promise<T> {
  const base = await mkdtemp(join(tmpdir(), 'codex-build-authz-'))
  const root = join(base, 'root')
  const outside = join(base, 'outside')
  await mkdir(root)
  await mkdir(outside)
  try {
    return await fn({ base, root, outside })
  } finally {
    await rm(base, { recursive: true, force: true })
  }
}

describe('codex build workspace-write authz', () => {
  test('write-disabled rejects before other checks', async () => {
    const got = await authorizeWriteJob(fakeRedis(), { requesterId: 'dev-pm', cwd: 'relative' }, {
      allowWrite: false, allowlist: ['dev-pm'], cwdRoots: [],
    })
    expect(got).toEqual({ ok: false, reason: 'write-disabled' })
  })

  test('requester-not-allowlisted rejects before cwd checks', async () => {
    const got = await authorizeWriteJob(fakeRedis(), { requesterId: 'reviewer', cwd: 'relative' }, {
      allowWrite: true, allowlist: ['dev-pm'], cwdRoots: [],
    })
    expect(got).toEqual({ ok: false, reason: 'requester-not-allowlisted' })
  })

  test('cwd non-absolute, traversal, and symlink escape are rejected', async () => {
    await withFixture(async ({ root, outside }) => {
      expect(canonicalizeCwd('relative', [root])).toEqual({ ok: false, reason: 'cwd-not-absolute' })
      expect(canonicalizeCwd(`${root}/../root`, [root])).toEqual({ ok: false, reason: 'cwd-traversal' })

      const link = join(root, 'escape')
      await symlink(outside, link)
      expect(canonicalizeCwd(link, [root])).toEqual({ ok: false, reason: 'cwd-symlink-escape' })
    })
  })

  test('cwd-not-registered rejects after canonical cwd succeeds', async () => {
    await withFixture(async ({ root }) => {
      const got = await authorizeWriteJob(fakeRedis(), { requesterId: 'dev-pm', cwd: root }, {
        allowWrite: true, allowlist: ['dev-pm'], cwdRoots: [root],
      })
      expect(got).toEqual({ ok: false, reason: 'cwd-not-registered' })
    })
  })

  test('registered cwd plus allowlisted requester and valid root authorizes', async () => {
    await withFixture(async ({ root }) => {
      const redis = fakeRedis()
      const cwd = canonicalizeCwd(root, [root])
      expect(cwd.ok).toBe(true)
      if (!cwd.ok) return

      expect(await registerCwd(redis, cwd.realpath)).toBe(true)
      const got = await authorizeWriteJob(redis, { requesterId: 'dev-pm', cwd: root, threadKey: 'thread-a' }, {
        allowWrite: true, allowlist: ['dev-pm'], cwdRoots: [root],
      })
      expect(got).toEqual({ ok: true, realpath: cwd.realpath })
    })
  })

  test('thread-key-conflict rejects a second requester on a claimed thread_key', async () => {
    await withFixture(async ({ root }) => {
      const redis = fakeRedis()
      const cwd = canonicalizeCwd(root, [root])
      expect(cwd.ok).toBe(true)
      if (!cwd.ok) return

      await registerCwd(redis, cwd.realpath)
      const cfg = { allowWrite: true, allowlist: ['dev-pm', 'other'], cwdRoots: [root] }
      expect((await authorizeWriteJob(redis, { requesterId: 'dev-pm', cwd: root, threadKey: 'same-thread' }, cfg)).ok).toBe(true)
      expect(await authorizeWriteJob(redis, { requesterId: 'other', cwd: root, threadKey: 'same-thread' }, cfg))
        .toEqual({ ok: false, reason: 'thread-key-conflict' })
    })
  })

  test('register/unregister/isCwdRegistered round-trip', async () => {
    await withFixture(async ({ root }) => {
      const redis = fakeRedis()
      const cwd = canonicalizeCwd(root, [root])
      expect(cwd.ok).toBe(true)
      if (!cwd.ok) return

      expect(await isCwdRegistered(redis, cwd.realpath)).toBe(false)
      expect(await registerCwd(redis, cwd.realpath)).toBe(true)
      expect(await isCwdRegistered(redis, cwd.realpath)).toBe(true)
      expect(await unregisterCwd(redis, cwd.realpath)).toBe(true)
      expect(await isCwdRegistered(redis, cwd.realpath)).toBe(false)
    })
  })

  test('ttl cwd registration expires fail-closed', async () => {
    await withFixture(async ({ root }) => {
      const redis = fakeRedis()
      const cwd = canonicalizeCwd(root, [root])
      expect(cwd.ok).toBe(true)
      if (!cwd.ok) return

      expect(await registerCwd(redis, cwd.realpath, { ttlS: 0 })).toBe(true)
      expect(await isCwdRegistered(redis, cwd.realpath)).toBe(false)
    })
  })
})
