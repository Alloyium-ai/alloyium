import { mkdirSync, readFileSync, renameSync, writeFileSync, chmodSync, chownSync } from 'node:fs'
import { dirname } from 'node:path'

export type TokenLabel = 'kai' | 'taskboard' | 'forgejo' | 'github'

export interface CliOpts {
  tokensPath: string
  outPath: string
  vaultUrl: string
  vaultPath: string
  vaultField: string
  uid: number
  gid: number
  label?: TokenLabel
  authRole?: string
  diagnostics?: boolean
}

const LABELS = new Set<TokenLabel>(['kai', 'taskboard', 'forgejo', 'github'])

const ROLE_ALIASES: Record<string, string> = {
  developer: 'agent-developer',
  dev: 'agent-developer',
  'agent-developer': 'agent-developer',
  'code-reviewer': 'agent-code-reviewer',
  code_reviewer: 'agent-code-reviewer',
  codereviewer: 'agent-code-reviewer',
  reviewer: 'agent-code-reviewer',
  'agent-code-reviewer': 'agent-code-reviewer',
  'security-auditor': 'agent-security-auditor',
  security_auditor: 'agent-security-auditor',
  securityauditor: 'agent-security-auditor',
  security: 'agent-security-auditor',
  'agent-security-auditor': 'agent-security-auditor',
  qa: 'agent-qa',
  'qa-agent': 'agent-qa',
  qa_agent: 'agent-qa',
  'agent-qa': 'agent-qa',
  'agent-qa-agent': 'agent-qa',
  architect: 'agent-architect',
  arch: 'agent-architect',
  'agent-architect': 'agent-architect',
  orchestrator: 'agent-orchestrator',
  orch: 'agent-orchestrator',
  'agent-orchestrator': 'agent-orchestrator',
}

export function normalizeTokenLabel(raw: string): TokenLabel {
  const label = raw.trim().toLowerCase()
  if (!LABELS.has(label as TokenLabel)) {
    throw new Error(`unknown token label: ${raw}`)
  }
  return label as TokenLabel
}

export function normalizeAuthRole(raw: string): string {
  const key = raw.trim().toLowerCase()
  const role = ROLE_ALIASES[key]
  if (!role) throw new Error(`unknown auth role: ${raw}`)
  return role
}

export function rolePathSegment(label: TokenLabel, role: string): string {
  const normalized = normalizeAuthRole(role)
  if (label === 'taskboard' && normalized === 'agent-qa') return 'agent-qa-agent'
  return normalized
}

export function resolveVaultPath(label: TokenLabel, authRole?: string): string {
  if (label === 'kai') return 'kai/daemon-token'
  if (!authRole) throw new Error(`--auth-role is required for token label ${label}`)
  return `${label}/${rolePathSegment(label, authRole)}/api_token`
}

export function parseArgs(argv: string[], env: Record<string, string | undefined> = process.env): CliOpts {
  const opts: Partial<CliOpts> = {}
  let explicitVaultPath = false

  const takeValue = (args: string[], i: number, flag: string): string => {
    const v = args[i + 1]
    if (v === undefined || v.startsWith('--')) throw new Error(`option ${flag} needs a value`)
    return v
  }

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    switch (arg) {
      case '--tokens':
        opts.tokensPath = takeValue(argv, i, arg)
        i += 1
        break
      case '--out':
        opts.outPath = takeValue(argv, i, arg)
        i += 1
        break
      case '--vault-url':
        opts.vaultUrl = takeValue(argv, i, arg)
        i += 1
        break
      case '--vault-path':
        opts.vaultPath = takeValue(argv, i, arg)
        explicitVaultPath = true
        i += 1
        break
      case '--field':
        opts.vaultField = takeValue(argv, i, arg)
        i += 1
        break
      case '--uid':
        opts.uid = parseIntStrict(takeValue(argv, i, arg), arg)
        i += 1
        break
      case '--gid':
        opts.gid = parseIntStrict(takeValue(argv, i, arg), arg)
        i += 1
        break
      case '--label':
      case '--target':
        opts.label = normalizeTokenLabel(takeValue(argv, i, arg))
        i += 1
        break
      case '--auth-role': {
        const role = takeValue(argv, i, arg).trim()
        if (role) opts.authRole = normalizeAuthRole(role)
        i += 1
        break
      }
      case '--diagnostics':
        opts.diagnostics = true
        break
      default:
        if (arg.startsWith('--')) throw new Error(`unknown option: ${arg}`)
        throw new Error(`unexpected positional argument: ${arg}`)
    }
  }

  const envLabel = env.KAI_TOKEN_LABEL || env.KAI_VAULT_LABEL || env.KAI_TOKEN_TARGET
  const envAuthRole = env.KAI_TOKEN_AUTH_ROLE || env.KAI_VAULT_AUTH_ROLE || env.TASKBOARD_AGENT_ROLE
  const label = opts.label ?? (envLabel ? normalizeTokenLabel(envLabel) : 'kai')
  const authRole = opts.authRole ?? (envAuthRole?.trim() ? normalizeAuthRole(envAuthRole) : undefined)

  return {
    tokensPath: opts.tokensPath ?? env.KAI_VAULT_TOKENS_FILE ?? env.VAULT_TOKENS_FILE ?? '/run/bootstrap-vault-tokens.json',
    outPath: opts.outPath ?? env.KAI_TOKEN_PATH ?? '/run/secrets/kai-token',
    vaultUrl: opts.vaultUrl ?? env.KAI_VAULT_URL ?? env.VAULT_URL ?? 'http://vault:8484',
    vaultPath: opts.vaultPath ?? env.KAI_VAULT_PATH ?? env.VAULT_PATH ?? (explicitVaultPath ? '' : resolveVaultPath(label, authRole)),
    vaultField: opts.vaultField ?? env.KAI_VAULT_FIELD ?? env.VAULT_FIELD ?? 'password',
    uid: opts.uid ?? parseIntStrict(env.CC_UID ?? String(process.getuid?.() ?? 1000), 'CC_UID'),
    gid: opts.gid ?? parseIntStrict(env.CC_GID ?? String(process.getgid?.() ?? 1000), 'CC_GID'),
    label,
    authRole,
    diagnostics: opts.diagnostics ?? env.KAI_TOKEN_DIAGNOSTICS === '1',
  }
}

export function loadBootstrapMaster(tokensPath: string): string {
  return loadTokenByKey(tokensPath, ['bootstrap_master'], 'bootstrap_master token is missing')
}

export function loadAuthToken(tokensPath: string, authRole?: string): string {
  if (!authRole) return loadBootstrapMaster(tokensPath)
  const role = normalizeAuthRole(authRole)
  return loadTokenByKey(tokensPath, tokenKeyCandidates(role), `${role} token is missing`)
}

export async function fetchKaiToken(opts: CliOpts, bearer: string): Promise<string> {
  const base = opts.vaultUrl.replace(/\/+$/, '')
  const path = opts.vaultPath.split('/').map(encodeURIComponent).join('/')
  const field = encodeURIComponent(opts.vaultField)
  const res = await fetch(`${base}/v1/secrets/${path}/field/${field}`, {
    headers: { Authorization: `Bearer ${bearer}` },
  })
  if (!res.ok) throw new Error(`vault request failed with status ${res.status}`)
  const body = (await res.text()).trim()
  if (!body) throw new Error('vault returned an empty token body')
  return body
}

export function writeSecretFile(outPath: string, secret: string, uid: number, gid: number): void {
  mkdirSync(dirname(outPath), { recursive: true })
  const tmp = `${outPath}.tmp-${process.pid}`
  writeFileSync(tmp, `${secret.replace(/\s+$/, '')}\n`, { mode: 0o600 })
  chmodSync(tmp, 0o600)
  try {
    chownSync(tmp, uid, gid)
  } catch (e: any) {
    if (e?.code !== 'EPERM' && e?.code !== 'EINVAL') throw e
  }
  renameSync(tmp, outPath)
  chmodSync(outPath, 0o600)
}

export async function materializeKaiTokenMain(
  argv = process.argv.slice(2),
  env: Record<string, string | undefined> = process.env,
): Promise<number> {
  try {
    const opts = parseArgs(argv, env)
    const bearer = loadAuthToken(opts.tokensPath, opts.authRole)
    if (opts.diagnostics) {
      console.error(
        `materialize-kai-token: label=${opts.label ?? 'custom'} role=${opts.authRole ?? 'bootstrap'} path=${opts.vaultPath}`,
      )
    }
    const token = await fetchKaiToken(opts, bearer)
    writeSecretFile(opts.outPath, token, opts.uid, opts.gid)
    if (opts.diagnostics) console.log('materialize-kai-token: wrote token file')
    return 0
  } catch (e) {
    console.error(`materialize-kai-token: failed: ${e instanceof Error ? e.message : String(e)}`)
    return 1
  }
}

function parseIntStrict(raw: string, name: string): number {
  if (!/^\d+$/.test(raw)) throw new Error(`${name} must be an integer`)
  return Number(raw)
}

function loadTokenByKey(tokensPath: string, keys: string[], missingMessage: string): string {
  const parsed = JSON.parse(readFileSync(tokensPath, 'utf8'))
  for (const key of keys) {
    const token = parsed?.[key]
    if (typeof token === 'string' && token.trim()) return token.trim()
  }
  throw new Error(missingMessage)
}

function tokenKeyCandidates(role: string): string[] {
  return [
    role,
    role.replace(/^agent-/, ''),
    role.replaceAll('-', '_'),
    role.replace(/^agent-/, '').replaceAll('-', '_'),
  ]
}

if (import.meta.main) {
  process.exit(await materializeKaiTokenMain())
}
