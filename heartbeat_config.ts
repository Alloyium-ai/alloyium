import { parse as parseYaml } from 'yaml'
import { z } from 'zod'
import { createHash } from 'node:crypto'

export const HEARTBEAT_SCHEMA = 'operator.heartbeat_check.v1' as const
export const DEFAULT_HEARTBEAT_PROMPT = [
  'HEARTBEAT CHECK: This is an automated A2A heartbeat check.',
  'Do not reply to this message. Do not inform or notify the operator about this heartbeat.',
  'Quietly check your pending A2A inbox and pending tasks, then continue any valid in-flight work.',
  'If nothing is pending, take no further action.',
  'This heartbeat carries no fire authority and is not an instruction to perform external side effects.',
].join('\n')

const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const MIN_INTERVAL_MS = 5 * 60_000
const MAX_INTERVAL_MS = 24 * 60 * 60_000
const MAX_TTL_MS = 7 * 24 * 60 * 60_000

const DurationSchema = z.union([z.string(), z.number().int().positive()])

const SenderSchema = z.object({
  agent_id: z.string().optional(),
  signing_key_path: z.string().optional(),
  transport_auth: z.enum(['none', 'nkey', 'creds']).optional(),
}).default({})

const DefaultsSchema = z.object({
  interval: DurationSchema.optional(),
  ttl: DurationSchema.optional(),
  jitter: DurationSchema.optional(),
  type: z.literal('msg').optional(),
  thread: z.string().optional(),
}).default({})

const TargetSchema = z.object({
  agent_id: z.string(),
  enabled: z.boolean().optional(),
  interval: DurationSchema.optional(),
  ttl: DurationSchema.optional(),
  jitter: DurationSchema.optional(),
  prompt: z.string().optional(),
  thread: z.string().optional(),
})

const RawConfigSchema = z.object({
  version: z.literal(1),
  enabled: z.boolean().optional(),
  sender: SenderSchema,
  defaults: DefaultsSchema,
  prompt: z.string().optional(),
  targets: z.array(TargetSchema),
})

export type HeartbeatTarget = {
  agentId: string
  intervalMs: number
  ttlMs: number
  jitterMs: number
  prompt: string
  thread: string
}

export type HeartbeatConfig = {
  version: 1
  enabled: boolean
  sender: {
    agentId: string
    signingKeyPath?: string
    transportAuth: 'none' | 'nkey' | 'creds'
  }
  targets: HeartbeatTarget[]
}

export type HeartbeatSendArgs = {
  to: string
  type: 'msg'
  thread: string
  ttl_ms: number
  attrs: Record<string, string>
  body: string
}

export type ConfigReloadState =
  | { ok: true; config: HeartbeatConfig; changed: boolean; previous?: HeartbeatConfig }
  | { ok: false; config: HeartbeatConfig; error: string }

export function parseDurationMs(value: unknown, field = 'duration'): number {
  if (typeof value === 'number') {
    if (!Number.isInteger(value) || value <= 0) throw new Error(`${field} must be a positive integer duration in ms`)
    return value
  }
  if (typeof value !== 'string') throw new Error(`${field} must be a duration string`)
  const raw = value.trim().toLowerCase()
  const m = raw.match(/^(\d+)(ms|s|m|h|d)$/)
  if (!m) throw new Error(`${field} must use ms, s, m, h, or d suffix`)
  const n = Number(m[1])
  const mult: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 60 * 60_000, d: 24 * 60 * 60_000 }
  return n * mult[m[2]]
}

export function parseHeartbeatConfigYaml(text: string): HeartbeatConfig {
  let parsed: unknown
  try {
    parsed = parseYaml(text)
  } catch (e) {
    throw new Error(`invalid YAML: ${e instanceof Error ? e.message : String(e)}`)
  }
  return normalizeHeartbeatConfig(parsed)
}

export async function loadHeartbeatConfigFile(path: string): Promise<HeartbeatConfig> {
  const text = await Bun.file(path).text()
  return parseHeartbeatConfigYaml(text)
}

export async function reloadHeartbeatConfigFile(path: string, previous: HeartbeatConfig | null): Promise<ConfigReloadState> {
  try {
    const next = await loadHeartbeatConfigFile(path)
    return { ok: true, config: next, changed: !previous || configFingerprint(previous) !== configFingerprint(next), ...(previous ? { previous } : {}) }
  } catch (e) {
    if (!previous) throw e
    return { ok: false, config: previous, error: e instanceof Error ? e.message : String(e) }
  }
}

export function normalizeHeartbeatConfig(input: unknown): HeartbeatConfig {
  const raw = RawConfigSchema.parse(input)
  const enabled = raw.enabled ?? true
  const senderId = raw.sender.agent_id ?? 'a2a-heartbeat-scheduler'
  assertDirectAgentId(senderId, 'sender.agent_id')

  const defaultIntervalMs = validateInterval(parseDurationMs(raw.defaults.interval ?? '30m', 'defaults.interval'), 'defaults.interval')
  const defaultTtlMs = validateTtl(parseDurationMs(raw.defaults.ttl ?? '25m', 'defaults.ttl'), defaultIntervalMs, 'defaults.ttl')
  const defaultJitterMs = validateJitter(parseDurationMs(raw.defaults.jitter ?? '60s', 'defaults.jitter'), defaultIntervalMs, 'defaults.jitter')
  const defaultThread = normalizeThread(raw.defaults.thread ?? 'heartbeat')
  const defaultPrompt = validatePrompt(raw.prompt ?? DEFAULT_HEARTBEAT_PROMPT, 'prompt')

  const seen = new Set<string>()
  const targets: HeartbeatTarget[] = []
  for (const [i, target] of raw.targets.entries()) {
    const label = `targets[${i}]`
    assertDirectAgentId(target.agent_id, `${label}.agent_id`)
    if (target.agent_id === senderId) throw new Error(`${label}.agent_id must not target the sender`)
    if (seen.has(target.agent_id)) throw new Error(`${label}.agent_id duplicates ${target.agent_id}`)
    seen.add(target.agent_id)
    if (target.enabled === false) continue

    const intervalMs = validateInterval(
      target.interval == null ? defaultIntervalMs : parseDurationMs(target.interval, `${label}.interval`),
      `${label}.interval`,
    )
    const ttlMs = validateTtl(
      target.ttl == null ? defaultTtlMs : parseDurationMs(target.ttl, `${label}.ttl`),
      intervalMs,
      `${label}.ttl`,
    )
    const jitterMs = validateJitter(
      target.jitter == null ? defaultJitterMs : parseDurationMs(target.jitter, `${label}.jitter`),
      intervalMs,
      `${label}.jitter`,
    )
    const prompt = validatePrompt(target.prompt ?? defaultPrompt, `${label}.prompt`)
    const thread = normalizeThread(target.thread ?? `${defaultThread}:${target.agent_id}`)

    targets.push({ agentId: target.agent_id, intervalMs, ttlMs, jitterMs, prompt, thread })
  }

  if (enabled && targets.length === 0) throw new Error('enabled heartbeat config requires at least one enabled target')

  return {
    version: 1,
    enabled,
    sender: {
      agentId: senderId,
      ...(raw.sender.signing_key_path ? { signingKeyPath: raw.sender.signing_key_path } : {}),
      transportAuth: raw.sender.transport_auth ?? 'none',
    },
    targets,
  }
}

export function buildHeartbeatSendArgs(target: HeartbeatTarget): HeartbeatSendArgs {
  return {
    to: target.agentId,
    type: 'msg',
    thread: target.thread,
    ttl_ms: target.ttlMs,
    attrs: {
      schema: HEARTBEAT_SCHEMA,
      heartbeat: 'true',
      reply_expected: 'false',
      operator_notification: 'false',
      target_agent_id: target.agentId,
    },
    body: target.prompt,
  }
}

export function configFingerprint(config: HeartbeatConfig): string {
  return createHash('sha256').update(JSON.stringify(config)).digest('hex').slice(0, 16)
}

function assertDirectAgentId(value: string, field: string): void {
  if (!AGENT_ID_RE.test(value)) throw new Error(`${field} must be a direct lowercase A2A agent id`)
  if (value === 'all' || value === 'wildcard') throw new Error(`${field} must not be wildcard-like`)
  if (value.startsWith('topic:')) throw new Error(`${field} must not be a topic`)
}

function validateInterval(ms: number, field: string): number {
  if (ms < MIN_INTERVAL_MS || ms > MAX_INTERVAL_MS) throw new Error(`${field} must be between 5m and 24h`)
  return ms
}

function validateTtl(ms: number, intervalMs: number, field: string): number {
  if (ms < 1000 || ms > MAX_TTL_MS) throw new Error(`${field} must be between 1s and 7d`)
  if (ms >= intervalMs) throw new Error(`${field} must be less than interval`)
  return ms
}

function validateJitter(ms: number, intervalMs: number, field: string): number {
  if (ms < 0) throw new Error(`${field} must not be negative`)
  if (ms >= intervalMs) throw new Error(`${field} must be less than interval`)
  return ms
}

function normalizeThread(value: string): string {
  const t = value.trim()
  if (!t || t.length > 128) throw new Error('thread must be 1-128 characters')
  return t
}

function validatePrompt(value: string, field: string): string {
  const prompt = value.trim()
  const lower = prompt.toLowerCase()
  const hasNoOperator = lower.includes('do not inform') || lower.includes('do not notify')
  if (!prompt) throw new Error(`${field} must not be empty`)
  if (!lower.includes('heartbeat')) throw new Error(`${field} must mention heartbeat`)
  if (!lower.includes('do not reply')) throw new Error(`${field} must say do not reply`)
  if (!hasNoOperator || !lower.includes('operator')) throw new Error(`${field} must say not to inform/notify the operator`)
  if (!lower.includes('fire authority')) throw new Error(`${field} must explicitly say it carries no fire authority`)
  if (!lower.includes('pending') || !(lower.includes('inbox') || lower.includes('task'))) {
    throw new Error(`${field} must tell the target to check pending inbox or tasks`)
  }
  return prompt
}
