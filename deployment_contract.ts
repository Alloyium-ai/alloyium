import { createHash } from 'node:crypto'
import { hostname } from 'node:os'
import { RedisClient } from 'bun'
import { connect } from 'nats'

export type AlloyiumDeploymentId = 'dev' | 'prod'
export type DeploymentEnv = Record<string, string | undefined>

export const DEPLOYMENT_NAMESPACE_KEYS = [
  'A2A_STREAM',
  'A2A_SUBJECT_PREFIX',
  'A2A_TOPICS_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_SECRET_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_PUBKEY_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_PRESENCE_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_DIRECT_ENC_CAP_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_PEER_PROTOCOL_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_LAUNCHER_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_CORE_EPOCH_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_BLOB_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_CODEX_BUILD_KEY_PREFIX', // gitleaks:allow -- environment-variable name, not a credential
  'A2A_SKILLS_GLOBAL_KEY', // gitleaks:allow -- environment-variable name, not a credential
] as const

type NamespaceKey = typeof DEPLOYMENT_NAMESPACE_KEYS[number]

type DeploymentDefinition = {
  deploymentId: AlloyiumDeploymentId
  busId: string
  busHost: string
  natsUrl: string
  redisUrl: string
  redisPort: string
  namespace: Record<NamespaceKey, string>
  silentSubsKey: string
  sentinelKey: string
  natsSentinelStream: string
  natsSentinelSubject: string
}

const DEV_NAMESPACE: Record<NamespaceKey, string> = {
  A2A_STREAM: 'ALLOYIUM_A2A',
  A2A_SUBJECT_PREFIX: 'alloyium.a2a.',
  A2A_TOPICS_KEY_PREFIX: 'alloyium:a2a:topics:',
  A2A_SECRET_KEY_PREFIX: 'alloyium:a2a:secret:',
  A2A_PUBKEY_KEY_PREFIX: 'alloyium:a2a:pubkey:',
  A2A_PRESENCE_KEY_PREFIX: 'alloyium:a2a:presence:',
  A2A_DIRECT_ENC_CAP_KEY_PREFIX: 'alloyium:a2a:direct-enc:',
  A2A_PEER_PROTOCOL_KEY_PREFIX: 'alloyium:a2a:peer-protocol:',
  A2A_LAUNCHER_KEY_PREFIX: 'alloyium:a2a:launcher:',
  A2A_CORE_EPOCH_KEY_PREFIX: 'alloyium:a2a:org:core-epoch:',
  A2A_BLOB_KEY_PREFIX: 'alloyium:a2a:blob:',
  A2A_CODEX_BUILD_KEY_PREFIX: 'alloyium:a2a:codex-build:',
  A2A_SKILLS_GLOBAL_KEY: 'alloyium:a2a:skills:global',
}

const PROD_NAMESPACE: Record<NamespaceKey, string> = {
  A2A_STREAM: 'ALLOYIUM_A2A',
  A2A_SUBJECT_PREFIX: 'alloyium.a2a.',
  A2A_TOPICS_KEY_PREFIX: 'alloyium:a2a:topics:',
  A2A_SECRET_KEY_PREFIX: 'alloyium:a2a:secret:',
  A2A_PUBKEY_KEY_PREFIX: 'alloyium:a2a:pubkey:',
  A2A_PRESENCE_KEY_PREFIX: 'alloyium:a2a:presence:',
  A2A_DIRECT_ENC_CAP_KEY_PREFIX: 'alloyium:a2a:direct-enc:',
  A2A_PEER_PROTOCOL_KEY_PREFIX: 'alloyium:a2a:peer-protocol:',
  A2A_LAUNCHER_KEY_PREFIX: 'alloyium:a2a:launcher:',
  A2A_CORE_EPOCH_KEY_PREFIX: 'alloyium:a2a:org:core-epoch:',
  A2A_BLOB_KEY_PREFIX: 'alloyium:a2a:blob:',
  A2A_CODEX_BUILD_KEY_PREFIX: 'alloyium:a2a:codex-build:',
  A2A_SKILLS_GLOBAL_KEY: 'alloyium:a2a:skills:global',
}

export const DEPLOYMENT_DEFINITIONS: Record<AlloyiumDeploymentId, DeploymentDefinition> = {
  dev: {
    deploymentId: 'dev',
    busId: 'local-dev-v1',
    busHost: '127.0.0.1',
    natsUrl: 'nats://127.0.0.1:4222',
    redisUrl: 'redis://127.0.0.1:6379',
    redisPort: '6379',
    namespace: DEV_NAMESPACE,
    silentSubsKey: 'alloyium:a2a-silent-subs',
    sentinelKey: 'alloyium:a2a:deployment-contract:v1',
    natsSentinelStream: 'ALLOYIUM_DEPLOYMENT_SENTINEL',
    natsSentinelSubject: '_ALLOYIUM.CONTRACT.DEV',
  },
  prod: {
    deploymentId: 'prod',
    busId: 'local-prod-v1',
    busHost: '127.0.0.1',
    natsUrl: 'nats://127.0.0.1:4223',
    redisUrl: 'redis://127.0.0.1:6380',
    redisPort: '6380',
    namespace: PROD_NAMESPACE,
    silentSubsKey: 'alloyium:a2a-silent-subs',
    sentinelKey: 'alloyium:a2a:deployment-contract:v1',
    natsSentinelStream: 'ALLOYIUM_PROD_DEPLOYMENT_SENTINEL',
    natsSentinelSubject: '_ALLOYIUM.CONTRACT.PROD',
  },
}

export type DeploymentSentinel = {
  version: 1
  deployment_id: AlloyiumDeploymentId
  bus_id: string
  nats_url: string
  redis_url: string
  stream: string
  subject_prefix: string
  key_namespace: string
  fingerprint: string
}

export type DeploymentContract = DeploymentDefinition & {
  hostId: string
  effectiveNatsUrl: string
  effectiveRedisUrl: string
  fingerprint: string
  sentinel: DeploymentSentinel
}

export class DeploymentContractError extends Error {
  constructor(public readonly code: string, detail?: string) {
    super(detail ? `${code}: ${detail}` : code)
    this.name = 'DeploymentContractError'
  }
}

const HOST_ID_RE = /^[a-z0-9](?:[a-z0-9.-]{0,62}[a-z0-9])?$/
const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on'])
const IDENTITY_KEYS = ['ALLOYIUM_DEPLOYMENT_ID', 'ALLOYIUM_BUS_ID', 'ALLOYIUM_HOST_ID'] as const
const HOST_ALIASES: Record<string, string> = {}

function enabled(value: string | undefined): boolean {
  return TRUE_VALUES.has((value ?? '').trim().toLowerCase())
}

export function canonicalAlloyiumHostId(value: string): string {
  const normalized = value.trim().toLowerCase()
  return HOST_ALIASES[normalized] ?? normalized
}

export function deploymentContractRequired(env: DeploymentEnv = process.env): boolean {
  return enabled(env.ALLOYIUM_CONTRACT_REQUIRED) || IDENTITY_KEYS.some((key) => !!env[key]?.trim())
}

function fail(code: string, detail?: string): never {
  throw new DeploymentContractError(code, detail)
}

function canonicalEndpoint(raw: string, protocol: 'nats:' | 'redis:'): string {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    fail('alloyium_contract_bad_endpoint', raw)
  }
  if (url.protocol !== protocol || url.username || url.password || url.search || url.hash) {
    fail('alloyium_contract_bad_endpoint', raw)
  }
  if (url.pathname !== '' && url.pathname !== '/') fail('alloyium_contract_bad_endpoint', raw)
  return `${protocol}//${url.hostname}:${url.port}`
}

function endpointPairAllowed(
  definition: DeploymentDefinition,
  hostId: string,
  natsUrl: string,
  redisUrl: string,
): boolean {
  if (natsUrl === definition.natsUrl && redisUrl === definition.redisUrl) return true
  // The bus sentinel identifies one logical bus, not a network path. Containers
  // on the local authority host may use Docker DNS/internal ports. Pairs are
  // atomic so internal-NATS/external-Redis split brain is refused.
  if (definition.deploymentId === 'dev' && hostId === 'local') {
    return natsUrl === 'nats://nats:4222' && redisUrl === 'redis://redis:6379'
  }
  if (definition.deploymentId === 'prod' && hostId === 'local') {
    return natsUrl === 'nats://nats:4222' && redisUrl === 'redis://redis:6379'
  }
  return false
}

function namespaceRoot(prefix: string): string {
  const marker = prefix.indexOf(':a2a:')
  if (marker <= 0) fail('alloyium_contract_bad_namespace', prefix)
  return prefix.slice(0, marker)
}

function sentinelFor(definition: DeploymentDefinition): { fingerprint: string; sentinel: DeploymentSentinel } {
  const body = {
    version: 1 as const,
    deployment_id: definition.deploymentId,
    bus_id: definition.busId,
    nats_url: definition.natsUrl,
    redis_url: definition.redisUrl,
    stream: definition.namespace.A2A_STREAM,
    subject_prefix: definition.namespace.A2A_SUBJECT_PREFIX,
    key_namespace: namespaceRoot(definition.namespace.A2A_PRESENCE_KEY_PREFIX),
  }
  const fingerprint = createHash('sha256').update(JSON.stringify(body)).digest('hex')
  return { fingerprint, sentinel: { ...body, fingerprint } }
}

function exact(env: DeploymentEnv, key: string, expected: string): void {
  const actual = env[key]?.trim()
  if (!actual) fail('alloyium_contract_missing_setting', key)
  if (actual !== expected) fail('alloyium_contract_mismatch', `${key}=${actual}; expected ${expected}`)
}

export function resolveDeploymentContract(
  env: DeploymentEnv = process.env,
  opts: { required?: boolean; enforceProdHold?: boolean } = {},
): DeploymentContract | null {
  const required = opts.required ?? deploymentContractRequired(env)
  if (!required) return null

  const missing = IDENTITY_KEYS.filter((key) => !env[key]?.trim())
  if (missing.length) fail('alloyium_contract_missing_identity', missing.join(','))
  const deploymentId = env.ALLOYIUM_DEPLOYMENT_ID!.trim()
  if (deploymentId !== 'dev' && deploymentId !== 'prod') fail('alloyium_contract_bad_deployment', deploymentId)
  const definition = DEPLOYMENT_DEFINITIONS[deploymentId]
  exact(env, 'ALLOYIUM_BUS_ID', definition.busId)

  const hostId = canonicalAlloyiumHostId(env.ALLOYIUM_HOST_ID!)
  if (!HOST_ID_RE.test(hostId)) fail('alloyium_contract_bad_host_id', hostId)
  for (const alias of ['A2A_HOST_ID', 'A2A_LOGICAL_HOST']) {
    const value = env[alias] ? canonicalAlloyiumHostId(env[alias]!) : ''
    if (value && value !== hostId) fail('alloyium_contract_host_mismatch', `${alias}=${value}; expected ${hostId}`)
  }
  if (deploymentId === 'prod' && (opts.enforceProdHold ?? true) && !enabled(env.ALLOYIUM_PROD_GO_LIVE)) {
    fail('alloyium_prod_on_hold', 'set ALLOYIUM_PROD_GO_LIVE=1 only under an approved production go-live')
  }

  if (!env.NATS_URL?.trim()) fail('alloyium_contract_missing_setting', 'NATS_URL')
  if (!env.REDIS_URL?.trim()) fail('alloyium_contract_missing_setting', 'REDIS_URL')
  const effectiveNatsUrl = canonicalEndpoint(env.NATS_URL, 'nats:')
  const effectiveRedisUrl = canonicalEndpoint(env.REDIS_URL, 'redis:')
  if (!endpointPairAllowed(definition, hostId, effectiveNatsUrl, effectiveRedisUrl)) {
    fail('alloyium_contract_endpoint_pair_mismatch', `NATS_URL=${effectiveNatsUrl}; REDIS_URL=${effectiveRedisUrl}; host=${hostId}`)
  }
  if (env.A2A_BUS_HOST?.trim() && env.A2A_BUS_HOST.trim() !== definition.busHost) {
    fail('alloyium_contract_mismatch', `A2A_BUS_HOST=${env.A2A_BUS_HOST}; expected ${definition.busHost}`)
  }
  const effectiveRedis = new URL(effectiveRedisUrl)
  if (env.REDIS_HOST?.trim() && env.REDIS_HOST.trim() !== effectiveRedis.hostname) {
    fail('alloyium_contract_redis_host_mismatch', `REDIS_HOST=${env.REDIS_HOST}; REDIS_URL=${effectiveRedisUrl}`)
  }
  if (env.REDIS_PORT?.trim() && env.REDIS_PORT.trim() !== effectiveRedis.port) {
    fail('alloyium_contract_redis_host_mismatch', `REDIS_PORT=${env.REDIS_PORT}; REDIS_URL=${effectiveRedisUrl}`)
  }
  for (const key of DEPLOYMENT_NAMESPACE_KEYS) exact(env, key, definition.namespace[key])
  if (env.A2A_SILENT_SUBS_KEY?.trim()) exact(env, 'A2A_SILENT_SUBS_KEY', definition.silentSubsKey)

  const { fingerprint, sentinel } = sentinelFor(definition)
  if (env.ALLOYIUM_CONTRACT_FINGERPRINT?.trim() && env.ALLOYIUM_CONTRACT_FINGERPRINT.trim() !== fingerprint) {
    fail('alloyium_contract_fingerprint_mismatch', env.ALLOYIUM_CONTRACT_FINGERPRINT)
  }
  if (env.ALLOYIUM_BUS_SENTINEL_KEY?.trim() && env.ALLOYIUM_BUS_SENTINEL_KEY.trim() !== definition.sentinelKey) {
    fail('alloyium_contract_mismatch', `ALLOYIUM_BUS_SENTINEL_KEY=${env.ALLOYIUM_BUS_SENTINEL_KEY}; expected ${definition.sentinelKey}`)
  }
  return { ...definition, hostId, effectiveNatsUrl, effectiveRedisUrl, fingerprint, sentinel }
}

export function materializeDeploymentEnv(
  source: DeploymentEnv = process.env,
  opts: { deploymentId?: AlloyiumDeploymentId; hostId?: string; enforceProdHold?: boolean } = {},
): Record<string, string> {
  const env: DeploymentEnv = { ...source }
  const deploymentId = opts.deploymentId ?? env.ALLOYIUM_DEPLOYMENT_ID?.trim()
  if (deploymentId !== 'dev' && deploymentId !== 'prod') fail('alloyium_contract_bad_deployment', deploymentId || '(missing)')
  const definition = DEPLOYMENT_DEFINITIONS[deploymentId]
  const explicitDeployment = env.ALLOYIUM_DEPLOYMENT_ID?.trim()
  if (explicitDeployment && explicitDeployment !== deploymentId) {
    fail('alloyium_contract_mismatch', `ALLOYIUM_DEPLOYMENT_ID=${explicitDeployment}; selected ${deploymentId}`)
  }

  const hostId = canonicalAlloyiumHostId(opts.hostId ?? env.ALLOYIUM_HOST_ID ?? env.A2A_HOST_ID ?? env.A2A_LOGICAL_HOST ?? hostname())
  const effectiveRedis = new URL(env.REDIS_URL?.trim() || definition.redisUrl)
  const defaults: Record<string, string> = {
    ALLOYIUM_CONTRACT_REQUIRED: '1',
    ALLOYIUM_DEPLOYMENT_ID: deploymentId,
    ALLOYIUM_BUS_ID: definition.busId,
    ALLOYIUM_HOST_ID: hostId,
    A2A_HOST_ID: hostId,
    A2A_LOGICAL_HOST: hostId,
    A2A_BUS_HOST: definition.busHost,
    NATS_URL: definition.natsUrl,
    REDIS_URL: definition.redisUrl,
    REDIS_HOST: effectiveRedis.hostname,
    REDIS_PORT: effectiveRedis.port,
    A2A_SILENT_SUBS_KEY: definition.silentSubsKey,
    ...definition.namespace,
  }
  for (const [key, value] of Object.entries(defaults)) {
    if (!env[key]?.trim() || key === 'ALLOYIUM_HOST_ID' || key === 'A2A_HOST_ID' || key === 'A2A_LOGICAL_HOST') env[key] = value
  }
  const contract = resolveDeploymentContract(env, { required: true, enforceProdHold: opts.enforceProdHold })!
  return {
    ...Object.fromEntries(Object.keys(defaults).map((key) => [key, env[key]!])),
    ALLOYIUM_CONTRACT_FINGERPRINT: contract.fingerprint,
    ALLOYIUM_BUS_SENTINEL_KEY: contract.sentinelKey,
  }
}

export type DeploymentRedis = Pick<import('bun').RedisClient, 'get' | 'send'>

function sameSentinel(raw: string, expected: DeploymentSentinel): boolean {
  try {
    const actual = JSON.parse(raw) as Record<string, unknown>
    return Object.entries(expected).every(([key, value]) => actual[key] === value)
  } catch {
    return false
  }
}

function assertSentinelBootstrapAuthority(contract: DeploymentContract): void {
  const authorityHost = 'local'
  if (contract.hostId !== authorityHost) {
    fail('alloyium_bus_sentinel_bootstrap_refused', `host ${contract.hostId} is not ${authorityHost}`)
  }
}

export async function ensureRedisDeploymentSentinel(
  redis: DeploymentRedis,
  contract: DeploymentContract,
  opts: { bootstrapIfMissing?: boolean } = {},
): Promise<void> {
  if (opts.bootstrapIfMissing) assertSentinelBootstrapAuthority(contract)
  const expected = JSON.stringify(contract.sentinel)
  let raw = await redis.get(contract.sentinelKey)
  if (raw == null && opts.bootstrapIfMissing) {
    await redis.send('SET', [contract.sentinelKey, expected, 'NX'])
    raw = await redis.get(contract.sentinelKey)
  }
  if (raw == null) fail('alloyium_bus_sentinel_missing', contract.sentinelKey)
  if (!sameSentinel(raw, contract.sentinel)) {
    fail('alloyium_bus_sentinel_mismatch', `${contract.sentinelKey}; expected ${contract.fingerprint}`)
  }
}

export type DeploymentNats = Pick<import('nats').NatsConnection, 'jetstreamManager'>

function natsSentinelDescription(contract: DeploymentContract): string {
  return `alloyium-deployment-contract:${JSON.stringify(contract.sentinel)}`
}

function natsNotFound(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false
  const e = error as any
  return e.code === '404' || e.code === 404 || e.api_error?.code === 404 || e.api_error?.err_code === 10059
}

export async function ensureNatsDeploymentSentinel(
  nc: DeploymentNats,
  contract: DeploymentContract,
  opts: { bootstrapIfMissing?: boolean } = {},
): Promise<void> {
  if (opts.bootstrapIfMissing) assertSentinelBootstrapAuthority(contract)
  const jsm = await nc.jetstreamManager()
  let info: any
  try {
    info = await jsm.streams.info(contract.natsSentinelStream)
  } catch (error) {
    if (!natsNotFound(error)) throw error
  }
  if (!info && opts.bootstrapIfMissing) {
    try {
      info = await jsm.streams.add({
        name: contract.natsSentinelStream,
        subjects: [contract.natsSentinelSubject],
        description: natsSentinelDescription(contract),
        retention: 'limits',
        storage: 'file',
        discard: 'old',
        max_msgs: 1,
        max_msgs_per_subject: 1,
      } as any)
    } catch (error) {
      // A racing designated bootstrap may have created it. Re-read and validate rather than
      // treating a stream-name conflict as success.
      try {
        info = await jsm.streams.info(contract.natsSentinelStream)
      } catch {
        throw error
      }
    }
  }
  if (!info) fail('alloyium_nats_sentinel_missing', contract.natsSentinelStream)
  const config = info.config ?? info
  const subjects = Array.isArray(config.subjects) ? config.subjects : []
  if (
    config.name !== contract.natsSentinelStream ||
    config.description !== natsSentinelDescription(contract) ||
    subjects.length !== 1 ||
    subjects[0] !== contract.natsSentinelSubject
  ) {
    fail('alloyium_nats_sentinel_mismatch', `${contract.natsSentinelStream}; expected ${contract.fingerprint}`)
  }
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`
}

if (import.meta.main) {
  const command = process.argv[2] ?? 'json'
  try {
    let source: DeploymentEnv = process.env
    if (command === 'shell-identity') {
      if (!enabled(process.env.A2A_DRY_WIRING)) fail('alloyium_contract_test_mode_refused')
      source = { ...process.env }
      for (const key of [
        'ALLOYIUM_BUS_ID', 'ALLOYIUM_CONTRACT_FINGERPRINT', 'ALLOYIUM_BUS_SENTINEL_KEY',
        'A2A_BUS_HOST', 'NATS_URL', 'REDIS_URL', 'REDIS_HOST', 'REDIS_PORT',
        'A2A_HOST_ID', 'A2A_LOGICAL_HOST', ...DEPLOYMENT_NAMESPACE_KEYS,
      ]) delete source[key]
    }
    let values = materializeDeploymentEnv(source)
    if (command === 'shell-identity') {
      const identityKeys = new Set([
        'ALLOYIUM_CONTRACT_REQUIRED', 'ALLOYIUM_DEPLOYMENT_ID', 'ALLOYIUM_BUS_ID',
        'ALLOYIUM_HOST_ID', 'ALLOYIUM_CONTRACT_FINGERPRINT', 'ALLOYIUM_BUS_SENTINEL_KEY',
        'A2A_HOST_ID', 'A2A_LOGICAL_HOST', 'A2A_SILENT_SUBS_KEY',
      ])
      values = Object.fromEntries(Object.entries(values).filter(([key]) => identityKeys.has(key)))
    }
    if (command === 'shell' || command === 'shell-identity') {
      for (const key of Object.keys(values).sort()) console.log(`export ${key}=${shellQuote(values[key])}`)
    } else if (command === 'json') {
      console.log(JSON.stringify(values, null, 2))
    } else if (command === 'verify' || command === 'bootstrap') {
      const contract = resolveDeploymentContract({ ...process.env, ...values }, { required: true })!
      const bootstrapIfMissing = command === 'bootstrap'
      if (bootstrapIfMissing && !enabled(process.env.ALLOYIUM_BUS_SENTINEL_BOOTSTRAP)) {
        fail('alloyium_bus_sentinel_bootstrap_refused', 'set ALLOYIUM_BUS_SENTINEL_BOOTSTRAP=1 on the designated bus-authority host')
      }
      const authorityHost = 'local'
      if (bootstrapIfMissing && contract.hostId !== authorityHost) {
        fail('alloyium_bus_sentinel_bootstrap_refused', `host ${contract.hostId} is not ${authorityHost}`)
      }
      const redis = new RedisClient(contract.effectiveRedisUrl)
      let nc: import('nats').NatsConnection | undefined
      try {
        nc = await connect({ servers: contract.effectiveNatsUrl, name: `alloyium-contract-${command}@${contract.hostId}` })
        await ensureNatsDeploymentSentinel(nc, contract, { bootstrapIfMissing })
        await ensureRedisDeploymentSentinel(redis, contract, { bootstrapIfMissing })
        console.log(JSON.stringify({
          ok: true,
          action: command,
          deployment_id: contract.deploymentId,
          bus_id: contract.busId,
          host_id: contract.hostId,
          fingerprint: contract.fingerprint,
        }))
      } finally {
        try { await nc?.drain() } catch {}
        try { redis.close() } catch {}
      }
    } else {
      throw new DeploymentContractError('alloyium_contract_bad_command', command)
    }
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    process.exit(2)
  }
}
