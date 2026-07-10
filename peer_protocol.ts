export const PEER_PROTOCOL_SCHEMA = 'a2a.peer.protocol.v1'
export const DEFAULT_A2A_PEER_PROTOCOL_KEY_PREFIX = process.env.A2A_PEER_PROTOCOL_KEY_PREFIX ?? 'alloyium:a2a:peer-protocol:'
// Maintenance rule: any protocol-related behavior change must bump this default
// and update docs/specs/2026-06-20-a2a-peer-protocol-versioning-spec.md.
export const DEFAULT_A2A_PROTOCOL_VERSION = process.env.A2A_PROTOCOL_VERSION ?? '1.0.11'
export const DEFAULT_ALLOYIUM_PRODUCT_VERSION = process.env.A2A_PRODUCT_VERSION ?? '0.1.0'
export const DEFAULT_A2A_APP_NAME = process.env.A2A_APP_NAME ?? process.env.npm_package_name ?? 'alloyium'
export const DEFAULT_A2A_APP_VERSION = process.env.A2A_APP_VERSION ?? process.env.npm_package_version ?? '0.1.0'
export const WORKER_EXECUTION_POLICY_SCHEMA = 'alloyium.worker.execution-policy.v1'

const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const SEMVER_RE = /^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/
const FEATURE_RE = /^[a-z0-9]+(?:[._-][a-z0-9]+)*\.v\d+$/

export type PeerRuntime = {
  kind: string
  mcp_path: string
  host: string
  pid: number
}

export type PeerDeploymentIdentity = {
  deployment_id: string
  bus_id: string
  host_id: string
  contract_fingerprint: string
}

export type WorkerExecutionProvider = 'codex' | 'claude'
export type WorkerSandbox = 'read-only' | 'workspace-write' | 'danger-full-access'

/**
 * Public, non-secret evidence describing the execution floor of a managed worker.
 *
 * Keep credentials, paths, prompts, and arbitrary environment values out of this
 * record. Presence is fleet-visible and the portal returns it to operators.
 */
export type WorkerExecutionPolicyMetadata = {
  schema: typeof WORKER_EXECUTION_POLICY_SCHEMA
  policy_id: string | null
  provider: WorkerExecutionProvider
  requested_model: string | null
  effective_model: string | null
  requested_effort: string | null
  effective_effort: string | null
  cli_version: string | null
  allow_write: boolean
  requested_sandbox: WorkerSandbox | null
  effective_sandbox: WorkerSandbox | null
  workspace_write_sandbox: WorkerSandbox | null
  approval_policy: string | null
  gpu: {
    visible: boolean
    visible_devices: string | null
    driver_capabilities: string[]
  }
  can_accept_jobs: boolean
}

export type PeerAppImageMetadata = {
  name?: string
  tag?: string
  digest?: string
  id?: string
}

export type PeerAppMetadata = {
  name: string
  version: string
  revision?: string
  build_id?: string
  image?: PeerAppImageMetadata
}

export type PeerProtocolDescriptor = {
  schema: typeof PEER_PROTOCOL_SCHEMA
  agent_id: string
  product: 'alloyium'
  product_version: string
  app?: PeerAppMetadata
  protocol_version: string
  deployment?: PeerDeploymentIdentity
  runtime: PeerRuntime
  wire: {
    a2a_envelope: { min: number; max: number }
    shim_hello: { min: number; max: number }
    beat_schemas: string[]
    status_schemas: string[]
    mcp_server: { name: string; version: string }
  }
  features: string[]
  limits: {
    max_send_bytes: number
  }
  started_at: string
  last_seen: string
  expires_at: string
}

export type PeerProtocolSummary = {
  protocol_version: string
  features: string[]
  app?: PeerAppMetadata
  runtime?: PeerRuntime
  metadata_missing?: boolean
  metadata_invalid?: boolean
  descriptor?: PeerProtocolDescriptor
}

export function peerProtocolKey(prefix: string, agentId: string): string {
  return prefix + agentId
}

export function isFeatureToken(value: unknown): value is string {
  return typeof value === 'string' && value.length <= 96 && FEATURE_RE.test(value)
}

export function normalizeFeatureList(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length > 128) return null
  const seen = new Set<string>()
  const out: string[] = []
  for (const item of value) {
    if (!isFeatureToken(item)) return null
    if (!seen.has(item)) {
      seen.add(item)
      out.push(item)
    }
  }
  return out.sort()
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isIsoDate(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value))
}

function boundedString(value: unknown, max = 128): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= max
}

function cleanBoundedString(value: unknown, max = 128): string | undefined {
  if (typeof value !== 'string') return undefined
  const trimmed = value.trim()
  return trimmed.length > 0 && trimmed.length <= max ? trimmed : undefined
}

function boundedInt(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= min && value <= max
}

function cleanPublicWorkerString(value: unknown, max: number, pattern: RegExp): string | null {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed || trimmed.length > max || /[\u0000-\u001f\u007f]/.test(trimmed)) return null
  return pattern.test(trimmed) ? trimmed : null
}

function normalizeWorkerSandbox(value: unknown): WorkerSandbox | null {
  if (typeof value !== 'string') return null
  const normalized = value.trim().toLowerCase()
  if (normalized === 'read-only' || normalized === 'readonly') return 'read-only'
  if (normalized === 'workspace-write' || normalized === 'workspace' || normalized === 'write') return 'workspace-write'
  if (['danger-full-access', 'full-access', 'danger', 'dangerous', 'yolo', 'no-sandbox', 'none'].includes(normalized)) return 'danger-full-access'
  return null
}

function normalizeGpuVisibleDevices(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed || trimmed.length > 256 || /[\u0000-\u001f\u007f]/.test(trimmed)) return null
  const special = trimmed.toLowerCase()
  if (special === 'all' || special === 'none' || special === 'void') return special
  const devices = trimmed.split(',').map((item) => item.trim())
  if (devices.length === 0 || devices.length > 32) return null
  if (!devices.every((item) => /^(?:\d+|GPU-[A-Za-z0-9-]+|MIG-[A-Za-z0-9./-]+)$/.test(item))) return null
  return [...new Set(devices)].join(',')
}

function normalizeGpuDriverCapabilities(value: unknown): string[] {
  const raw = Array.isArray(value)
    ? value
    : typeof value === 'string'
      ? value.split(',')
      : []
  if (raw.length > 16) return []
  const capabilities: string[] = []
  for (const item of raw) {
    if (typeof item !== 'string') return []
    const capability = item.trim().toLowerCase()
    if (!/^[a-z][a-z0-9_-]{0,31}$/.test(capability)) return []
    if (!capabilities.includes(capability)) capabilities.push(capability)
  }
  return capabilities.sort()
}

/**
 * Sanitize untrusted presence metadata and independently clamp its health claim.
 * A peer cannot advertise can_accept_jobs=true while any mandatory dev-yolo-gpu
 * field is missing or downgraded.
 */
export function sanitizeWorkerExecutionPolicy(value: unknown): WorkerExecutionPolicyMetadata | null {
  if (!isObject(value) || value.schema !== WORKER_EXECUTION_POLICY_SCHEMA) return null
  const provider = value.provider === 'codex' || value.provider === 'claude' ? value.provider : null
  if (!provider) return null

  const policyId = cleanPublicWorkerString(value.policy_id, 64, /^[a-z0-9][a-z0-9._-]*$/)
  const requestedModel = cleanPublicWorkerString(value.requested_model, 128, /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/)
  const effectiveModel = cleanPublicWorkerString(value.effective_model, 128, /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/)
  const requestedEffort = cleanPublicWorkerString(value.requested_effort, 32, /^[a-z0-9][a-z0-9._-]*$/)
  const effectiveEffort = cleanPublicWorkerString(value.effective_effort, 32, /^[a-z0-9][a-z0-9._-]*$/)
  const cliVersion = cleanPublicWorkerString(value.cli_version, 64, /^[0-9A-Za-z][0-9A-Za-z.+_-]*$/)
  const requestedSandbox = normalizeWorkerSandbox(value.requested_sandbox)
  const effectiveSandbox = normalizeWorkerSandbox(value.effective_sandbox)
  const workspaceWriteSandbox = normalizeWorkerSandbox(value.workspace_write_sandbox)
  const approvalPolicy = cleanPublicWorkerString(value.approval_policy, 32, /^[a-z][a-z0-9-]*$/)
  const rawGpu = isObject(value.gpu) ? value.gpu : {}
  const visibleDevices = normalizeGpuVisibleDevices(rawGpu.visible_devices)
  const driverCapabilities = normalizeGpuDriverCapabilities(rawGpu.driver_capabilities)
  const gpuVisible = visibleDevices !== null && visibleDevices !== 'none' && visibleDevices !== 'void'
  const allowWrite = value.allow_write === true
  const contractSatisfied =
    policyId === 'dev-yolo-gpu-v1' &&
    requestedModel !== null && effectiveModel !== null &&
    requestedEffort !== null && effectiveEffort !== null &&
    cliVersion !== null && allowWrite &&
    requestedSandbox === 'danger-full-access' &&
    effectiveSandbox === 'danger-full-access' &&
    workspaceWriteSandbox === 'danger-full-access' &&
    approvalPolicy === 'never' &&
    visibleDevices === 'all' && driverCapabilities.includes('all')

  return {
    schema: WORKER_EXECUTION_POLICY_SCHEMA,
    policy_id: policyId,
    provider,
    requested_model: requestedModel,
    effective_model: effectiveModel,
    requested_effort: requestedEffort,
    effective_effort: effectiveEffort,
    cli_version: cliVersion,
    allow_write: allowWrite,
    requested_sandbox: requestedSandbox,
    effective_sandbox: effectiveSandbox,
    workspace_write_sandbox: workspaceWriteSandbox,
    approval_policy: approvalPolicy,
    gpu: {
      visible: gpuVisible,
      visible_devices: visibleDevices,
      driver_capabilities: driverCapabilities,
    },
    can_accept_jobs: value.can_accept_jobs === true && contractSatisfied,
  }
}

export function normalizePeerAppMetadata(value: unknown): PeerAppMetadata | null {
  if (!isObject(value)) return null
  const name = cleanBoundedString(value.name, 64)
  const version = cleanBoundedString(value.version, 128)
  if (!name || !version) return null
  const app: PeerAppMetadata = { name, version }
  const revision = cleanBoundedString(value.revision, 128)
  if (revision) app.revision = revision
  const buildId = cleanBoundedString(value.build_id, 128)
  if (buildId) app.build_id = buildId
  if (value.image !== undefined) {
    if (!isObject(value.image)) return null
    const image: PeerAppImageMetadata = {}
    const imageName = cleanBoundedString(value.image.name, 256)
    if (imageName) image.name = imageName
    const imageTag = cleanBoundedString(value.image.tag, 128)
    if (imageTag) image.tag = imageTag
    const digest = cleanBoundedString(value.image.digest, 256)
    if (digest) image.digest = digest
    const id = cleanBoundedString(value.image.id, 256)
    if (id) image.id = id
    if (Object.keys(image).length > 0) app.image = image
  }
  return app
}

type EnvLike = Record<string, string | undefined>

function firstEnv(env: EnvLike, names: string[]): string | undefined {
  for (const name of names) {
    const value = cleanBoundedString(env[name], 256)
    if (value) return value
  }
  return undefined
}

export function peerAppMetadataFromEnv(env: EnvLike = process.env): PeerAppMetadata {
  const image: PeerAppImageMetadata = {}
  const imageName = firstEnv(env, ['A2A_IMAGE_NAME', 'A2A_CONTAINER_IMAGE', 'IMAGE_NAME'])
  if (imageName) image.name = imageName
  const imageTag = firstEnv(env, ['A2A_IMAGE_TAG', 'CC_IMAGE_TAG', 'IMAGE_TAG'])
  if (imageTag) image.tag = imageTag
  const imageDigest = firstEnv(env, ['A2A_IMAGE_DIGEST', 'IMAGE_DIGEST'])
  if (imageDigest) image.digest = imageDigest
  const imageId = firstEnv(env, ['A2A_IMAGE_ID', 'IMAGE_ID'])
  if (imageId) image.id = imageId

  const app = normalizePeerAppMetadata({
    name: firstEnv(env, ['A2A_APP_NAME', 'npm_package_name']) ?? DEFAULT_A2A_APP_NAME,
    version: firstEnv(env, ['A2A_APP_VERSION', 'npm_package_version']) ?? DEFAULT_A2A_APP_VERSION,
    revision: firstEnv(env, ['A2A_APP_REVISION', 'A2A_GIT_SHA', 'SOURCE_REVISION', 'GIT_SHA']),
    build_id: firstEnv(env, ['A2A_BUILD_ID', 'BUILD_ID']),
    ...(Object.keys(image).length > 0 ? { image } : {}),
  })
  return app ?? { name: 'alloyium', version: '0.1.0' }
}

function parseRuntime(value: unknown): PeerRuntime | null {
  if (!isObject(value)) return null
  if (!boundedString(value.kind, 64)) return null
  if (!boundedString(value.mcp_path, 64)) return null
  if (typeof value.host !== 'string' || value.host.length > 256) return null
  if (!boundedInt(value.pid, 1, 2 ** 31)) return null
  return { kind: value.kind, mcp_path: value.mcp_path, host: value.host, pid: value.pid }
}

function parseDeployment(value: unknown): PeerDeploymentIdentity | null {
  if (!isObject(value)) return null
  if (!boundedString(value.deployment_id, 32)) return null
  if (!boundedString(value.bus_id, 128)) return null
  if (!boundedString(value.host_id, 64)) return null
  if (typeof value.contract_fingerprint !== 'string' || !/^[a-f0-9]{64}$/.test(value.contract_fingerprint)) return null
  return {
    deployment_id: value.deployment_id,
    bus_id: value.bus_id,
    host_id: value.host_id,
    contract_fingerprint: value.contract_fingerprint,
  }
}

function parseWire(value: unknown): PeerProtocolDescriptor['wire'] | null {
  if (!isObject(value) || !isObject(value.a2a_envelope) || !isObject(value.shim_hello) || !isObject(value.mcp_server)) return null
  const envelope = value.a2a_envelope
  const shim = value.shim_hello
  const beat = value.beat_schemas
  const status = value.status_schemas
  const mcp = value.mcp_server
  if (!boundedInt(envelope.min, 1, 99) || !boundedInt(envelope.max, envelope.min, 99)) return null
  if (!boundedInt(shim.min, 1, 99) || !boundedInt(shim.max, shim.min, 99)) return null
  if (!Array.isArray(beat) || !beat.every((x) => boundedString(x, 64))) return null
  if (!Array.isArray(status) || !status.every((x) => boundedString(x, 64))) return null
  if (!boundedString(mcp.name, 64) || !boundedString(mcp.version, 64)) return null
  return {
    a2a_envelope: { min: envelope.min, max: envelope.max },
    shim_hello: { min: shim.min, max: shim.max },
    beat_schemas: [...beat],
    status_schemas: [...status],
    mcp_server: { name: mcp.name, version: mcp.version },
  }
}

export function parsePeerProtocolDescriptor(value: unknown): PeerProtocolDescriptor | null {
  let raw = value
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw) } catch { return null }
  }
  if (!isObject(raw)) return null
  if (raw.schema !== PEER_PROTOCOL_SCHEMA) return null
  if (typeof raw.agent_id !== 'string' || !AGENT_ID_RE.test(raw.agent_id)) return null
  if (raw.product !== 'alloyium') return null
  if (!boundedString(raw.product_version, 64)) return null
  if (typeof raw.protocol_version !== 'string' || !SEMVER_RE.test(raw.protocol_version)) return null
  const app = raw.app === undefined ? undefined : normalizePeerAppMetadata(raw.app)
  if (raw.app !== undefined && !app) return null
  const deployment = raw.deployment === undefined ? undefined : parseDeployment(raw.deployment)
  if (raw.deployment !== undefined && !deployment) return null
  const runtime = parseRuntime(raw.runtime)
  const wire = parseWire(raw.wire)
  const features = normalizeFeatureList(raw.features)
  if (!runtime || !wire || !features) return null
  if (!isObject(raw.limits) || !boundedInt(raw.limits.max_send_bytes, 1, 1024 * 1024 * 64)) return null
  if (!isIsoDate(raw.started_at) || !isIsoDate(raw.last_seen) || !isIsoDate(raw.expires_at)) return null
  return {
    schema: PEER_PROTOCOL_SCHEMA,
    agent_id: raw.agent_id,
    product: 'alloyium',
    product_version: raw.product_version,
    ...(app ? { app } : {}),
    protocol_version: raw.protocol_version,
    ...(deployment ? { deployment } : {}),
    runtime,
    wire,
    features,
    limits: { max_send_bytes: raw.limits.max_send_bytes },
    started_at: raw.started_at,
    last_seen: raw.last_seen,
    expires_at: raw.expires_at,
  }
}

export function buildPeerProtocolDescriptor(input: {
  agentId: string
  features: string[]
  runtime: PeerRuntime
  maxSendBytes: number
  startedAt: string
  lastSeen: string
  ttlS: number
  protocolVersion?: string
  productVersion?: string
  app?: PeerAppMetadata
  deployment?: PeerDeploymentIdentity
}): PeerProtocolDescriptor {
  const features = normalizeFeatureList(input.features)
  if (!features) throw new Error('invalid peer protocol feature list')
  const protocolVersion = input.protocolVersion ?? DEFAULT_A2A_PROTOCOL_VERSION
  if (!SEMVER_RE.test(protocolVersion)) throw new Error(`invalid A2A protocol version '${protocolVersion}'`)
  const productVersion = input.productVersion ?? DEFAULT_ALLOYIUM_PRODUCT_VERSION
  const app = normalizePeerAppMetadata(input.app) ?? peerAppMetadataFromEnv()
  const deployment = input.deployment === undefined ? undefined : parseDeployment(input.deployment)
  if (input.deployment !== undefined && !deployment) throw new Error('invalid peer deployment identity')
  const expiresAt = new Date(Date.parse(input.lastSeen) + input.ttlS * 1000).toISOString()
  return {
    schema: PEER_PROTOCOL_SCHEMA,
    agent_id: input.agentId,
    product: 'alloyium',
    product_version: productVersion,
    app,
    protocol_version: protocolVersion,
    ...(deployment ? { deployment } : {}),
    runtime: input.runtime,
    wire: {
      a2a_envelope: { min: 1, max: 1 },
      shim_hello: { min: 1, max: 1 },
      beat_schemas: ['agent.beat.v1'],
      status_schemas: ['agent.status.v1'],
      mcp_server: { name: 'alloyium', version: '0.1.0' },
    },
    features,
    limits: { max_send_bytes: input.maxSendBytes },
    started_at: input.startedAt,
    last_seen: input.lastSeen,
    expires_at: expiresAt,
  }
}

export function protocolSummary(desc: PeerProtocolDescriptor | null, opts: { invalid?: boolean } = {}): PeerProtocolSummary {
  if (!desc) {
    return {
      protocol_version: 'baseline',
      features: [],
      ...(opts.invalid ? { metadata_invalid: true } : { metadata_missing: true }),
    }
  }
  return {
    protocol_version: desc.protocol_version,
    features: desc.features,
    ...(desc.app ? { app: desc.app } : {}),
    runtime: desc.runtime,
    descriptor: desc,
  }
}

export function missingFeatures(desc: PeerProtocolDescriptor | null, required: string[]): string[] {
  const have = new Set(desc?.features ?? [])
  return required.filter((feature) => !have.has(feature))
}
