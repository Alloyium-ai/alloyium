import { afterEach, describe, expect, test } from 'bun:test'
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  fetchKaiToken,
  loadAuthToken,
  loadBootstrapMaster,
  materializeKaiTokenMain,
  normalizeAuthRole,
  parseArgs,
  resolveVaultPath,
  rolePathSegment,
  writeSecretFile,
  type CliOpts,
} from '../materialize_kai_token.ts'

const originalFetch = globalThis.fetch
const originalConsoleError = console.error
const originalConsoleLog = console.log

afterEach(() => {
  globalThis.fetch = originalFetch
  console.error = originalConsoleError
  console.log = originalConsoleLog
})

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'materialize-kai-token-'))
}

function currentUidGid(): { uid: number; gid: number } {
  return {
    uid: process.getuid?.() ?? 1000,
    gid: process.getgid?.() ?? 1000,
  }
}

function opts(overrides: Partial<CliOpts> = {}): CliOpts {
  const { uid, gid } = currentUidGid()
  return {
    tokensPath: '/tokens.json',
    outPath: '/kai-token',
    vaultUrl: 'https://vault.example',
    vaultPath: 'kai/daemon-token',
    vaultField: 'password',
    uid,
    gid,
    ...overrides,
  }
}

describe('materialize kai token argparse', () => {
  test('parses token/output ownership flags', () => {
    expect(parseArgs([
      '--tokens', '/tmp/tokens.json',
      '--out', '/tmp/kai-token',
      '--uid', '123',
      '--gid', '456',
    ], {})).toMatchObject({
      tokensPath: '/tmp/tokens.json',
      outPath: '/tmp/kai-token',
      uid: 123,
      gid: 456,
    })
  })

  test('rejects positional arguments and missing flag values', () => {
    expect(() => parseArgs(['positional'], {})).toThrow('unexpected positional argument')
    expect(() => parseArgs(['--tokens'], {})).toThrow('option --tokens needs a value')
    expect(() => parseArgs(['--tokens', '--out'], {})).toThrow('option --tokens needs a value')
  })

  test('normalizes token labels and role aliases', () => {
    expect(parseArgs(['--label', 'forgejo', '--auth-role', 'Developer'], {})).toMatchObject({
      label: 'forgejo',
      authRole: 'agent-developer',
      vaultPath: 'forgejo/agent-developer/api_token',
    })
    expect(parseArgs(['--target', 'github', '--auth-role', 'code_reviewer'], {})).toMatchObject({
      label: 'github',
      authRole: 'agent-code-reviewer',
      vaultPath: 'github/agent-code-reviewer/api_token',
    })
  })

  test('requires roles only for role-scoped labels', () => {
    expect(parseArgs(['--label', 'kai'], {})).toMatchObject({ vaultPath: 'kai/daemon-token' })
    expect(() => parseArgs(['--label', 'taskboard'], {})).toThrow('--auth-role is required')
  })

  test('keeps explicit vault path env overrides compatible with labels', () => {
    expect(parseArgs(['--label', 'taskboard'], { KAI_VAULT_PATH: 'custom/path' })).toMatchObject({
      label: 'taskboard',
      vaultPath: 'custom/path',
    })
  })
})

describe('materialize kai token label mapping', () => {
  test('maps expected foundation roles to canonical path segments', () => {
    expect(normalizeAuthRole('developer')).toBe('agent-developer')
    expect(normalizeAuthRole('code-reviewer')).toBe('agent-code-reviewer')
    expect(normalizeAuthRole('security-auditor')).toBe('agent-security-auditor')
    expect(normalizeAuthRole('qa')).toBe('agent-qa')
    expect(normalizeAuthRole('architect')).toBe('agent-architect')
    expect(normalizeAuthRole('orchestrator')).toBe('agent-orchestrator')
  })

  test('preserves the taskboard QA compatibility label', () => {
    expect(rolePathSegment('forgejo', 'qa')).toBe('agent-qa')
    expect(rolePathSegment('taskboard', 'qa')).toBe('agent-qa-agent')
    expect(resolveVaultPath('taskboard', 'qa')).toBe('taskboard/agent-qa-agent/api_token')
    expect(resolveVaultPath('forgejo', 'qa')).toBe('forgejo/agent-qa/api_token')
  })
})

describe('materialize kai token bootstrap token loading', () => {
  test('throws when bootstrap_master is missing or blank', () => {
    const dir = tempDir()
    try {
      const missing = join(dir, 'missing.json')
      const blank = join(dir, 'blank.json')
      writeFileSync(missing, '{}')
      writeFileSync(blank, JSON.stringify({ bootstrap_master: '  \n' }))

      expect(() => loadBootstrapMaster(missing)).toThrow('bootstrap_master token is missing')
      expect(() => loadBootstrapMaster(blank)).toThrow('bootstrap_master token is missing')
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test('loads role auth tokens by normalized role key', () => {
    const dir = tempDir()
    try {
      const tokens = join(dir, 'tokens.json')
      writeFileSync(tokens, JSON.stringify({
        bootstrap_master: 'bootstrap-token',
        'agent-developer': 'developer-token',
        agent_code_reviewer: 'reviewer-token',
      }))

      expect(loadAuthToken(tokens, 'developer')).toBe('developer-token')
      expect(loadAuthToken(tokens, 'code-reviewer')).toBe('reviewer-token')
      expect(loadAuthToken(tokens)).toBe('bootstrap-token')
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe('materialize kai token vault fetch', () => {
  test('uses the expected vault field endpoint and bearer token', async () => {
    let seenUrl = ''
    let seenAuth = ''
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      seenUrl = String(url)
      seenAuth = String(init?.headers instanceof Headers ? init.headers.get('Authorization') : (init?.headers as Record<string, string>).Authorization)
      return { ok: true, status: 200, text: async () => ' kai-token-value \n' } as Response
    }) as typeof fetch

    await expect(fetchKaiToken(opts(), 'bootstrap-token')).resolves.toBe('kai-token-value')
    expect(seenUrl).toBe('https://vault.example/v1/secrets/kai/daemon-token/field/password')
    expect(seenAuth).toBe('Bearer bootstrap-token')
  })

  test('main can fetch a role-scoped target using a role auth token', async () => {
    const dir = tempDir()
    try {
      const tokens = join(dir, 'tokens.json')
      const out = join(dir, 'forgejo-token')
      writeFileSync(tokens, JSON.stringify({ 'agent-developer': 'developer-auth-token' }))
      let seenUrl = ''
      let seenAuth = ''
      globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
        seenUrl = String(url)
        seenAuth = String(init?.headers instanceof Headers ? init.headers.get('Authorization') : (init?.headers as Record<string, string>).Authorization)
        return { ok: true, status: 200, text: async () => ' target-token \n' } as Response
      }) as typeof fetch

      const code = await materializeKaiTokenMain([
        '--tokens', tokens,
        '--out', out,
        '--label', 'forgejo',
        '--auth-role', 'developer',
        '--uid', String(currentUidGid().uid),
        '--gid', String(currentUidGid().gid),
      ], { KAI_VAULT_URL: 'https://vault.example' })

      expect(code).toBe(0)
      expect(seenUrl).toBe('https://vault.example/v1/secrets/forgejo/agent-developer/api_token/field/password')
      expect(seenAuth).toBe('Bearer developer-auth-token')
      expect(readFileSync(out, 'utf8')).toBe('target-token\n')
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test('main returns exit 1 when vault response is not ok', async () => {
    const dir = tempDir()
    try {
      const tokens = join(dir, 'tokens.json')
      const out = join(dir, 'kai-token')
      writeFileSync(tokens, JSON.stringify({ bootstrap_master: 'bootstrap-token' }))
      globalThis.fetch = (async () => ({ ok: false, status: 503, text: async () => 'unavailable' }) as Response) as typeof fetch
      console.error = () => {}

      const code = await materializeKaiTokenMain(['--tokens', tokens, '--out', out, '--uid', String(currentUidGid().uid), '--gid', String(currentUidGid().gid)], {})

      expect(code).toBe(1)
      expect(existsSync(out)).toBe(false)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  test('main returns exit 1 when vault returns an empty body', async () => {
    const dir = tempDir()
    try {
      const tokens = join(dir, 'tokens.json')
      const out = join(dir, 'kai-token')
      writeFileSync(tokens, JSON.stringify({ bootstrap_master: 'bootstrap-token' }))
      globalThis.fetch = (async () => ({ ok: true, status: 200, text: async () => ' \n\t' }) as Response) as typeof fetch
      console.error = () => {}

      const code = await materializeKaiTokenMain(['--tokens', tokens, '--out', out, '--uid', String(currentUidGid().uid), '--gid', String(currentUidGid().gid)], {})

      expect(code).toBe(1)
      expect(existsSync(out)).toBe(false)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})

describe('materialize kai token output file', () => {
  test('writes atomically with mode 0600', () => {
    const dir = tempDir()
    try {
      const { uid, gid } = currentUidGid()
      const out = join(dir, 'nested', 'kai-token')
      writeSecretFile(out, 'first-token', uid, gid)
      writeSecretFile(out, 'second-token', uid, gid)

      expect(readFileSync(out, 'utf8')).toBe('second-token\n')
      expect(statSync(out).mode & 0o777).toBe(0o600)
      expect(existsSync(`${out}.tmp-${process.pid}`)).toBe(false)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
