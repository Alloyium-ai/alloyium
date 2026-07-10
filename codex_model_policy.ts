export const CODEX_TESTED_CLI_VERSION = '0.144.0'
export const CODEX_DEFAULT_MODEL = 'gpt-5.6-sol'
export const CODEX_DEFAULT_THINKING_LEVEL = 'xhigh'
export const CODEX_THINKING_LEVELS = ['xhigh', 'max', 'ultra'] as const

export type CodexThinkingLevel = typeof CODEX_THINKING_LEVELS[number]

export type CodexCatalogModel = {
  model: string
  supportedEfforts: string[]
  hidden?: boolean
}

export type CodexModelCatalog = {
  source: string
  cliVersion?: string
  models: CodexCatalogModel[]
}

export type CodexUltraLimits = {
  maxThreads: number
  maxDepth: number
  jobMaxRuntimeSeconds: number
  maxTurnTimeoutMs: number
}

export const CODEX_ULTRA_DEFAULT_LIMITS: CodexUltraLimits = {
  maxThreads: 4,
  maxDepth: 1,
  jobMaxRuntimeSeconds: 900,
  maxTurnTimeoutMs: 3_600_000,
}

export const CODEX_ULTRA_MAX_FLEET_AGENTS = 4

// This contract is generated from and build-checked against
// `codex debug models --bundled` in Codex CLI 0.144.0. The gateway performs a
// second check against app-server `model/list` under the runtime login.
export const CODEX_TESTED_MODEL_CATALOG: CodexModelCatalog = {
  source: `codex-cli-${CODEX_TESTED_CLI_VERSION}:bundled`,
  cliVersion: CODEX_TESTED_CLI_VERSION,
  models: [{
    model: CODEX_DEFAULT_MODEL,
    supportedEfforts: ['low', 'medium', 'high', ...CODEX_THINKING_LEVELS],
  }],
}

export type CodexPolicySelection = {
  requested: { model: string; effort: CodexThinkingLevel }
  effective: { model: string; effort: CodexThinkingLevel }
  cli: { version?: string }
  catalog: {
    source: string
    supported_efforts: string[]
  }
  safeguards?: { ultra: CodexUltraLimits }
}

export type CodexPolicyResult =
  | { ok: true; selection: CodexPolicySelection }
  | { ok: false; error: 'bad_model' | 'bad_effort' | 'model_not_available' | 'effort_not_supported'; detail: string }

export function parseCodexThinkingLevel(value: unknown): CodexThinkingLevel | null {
  return typeof value === 'string' && (CODEX_THINKING_LEVELS as readonly string[]).includes(value)
    ? value as CodexThinkingLevel
    : null
}

export function resolveCodexPolicy(
  input: { model?: unknown; effort?: unknown } = {},
  catalog: CodexModelCatalog = CODEX_TESTED_MODEL_CATALOG,
  ultraLimits: CodexUltraLimits = CODEX_ULTRA_DEFAULT_LIMITS,
): CodexPolicyResult {
  const model = input.model == null || input.model === '' ? CODEX_DEFAULT_MODEL : cleanModel(input.model)
  if (!model) return { ok: false, error: 'bad_model', detail: 'model must be a single-line non-empty string of at most 128 characters' }

  const effort = input.effort == null || input.effort === ''
    ? CODEX_DEFAULT_THINKING_LEVEL
    : parseCodexThinkingLevel(input.effort)
  if (!effort) {
    return { ok: false, error: 'bad_effort', detail: `effort must be one of ${CODEX_THINKING_LEVELS.join('|')}` }
  }

  const entry = catalog.models.find((candidate) => candidate.model === model)
  if (!entry) {
    return { ok: false, error: 'model_not_available', detail: `model ${model} is absent from ${catalog.source}` }
  }
  if (!entry.supportedEfforts.includes(effort)) {
    return {
      ok: false,
      error: 'effort_not_supported',
      detail: `model ${model} does not advertise effort ${effort} in ${catalog.source}`,
    }
  }

  return {
    ok: true,
    selection: {
      requested: { model, effort },
      effective: { model, effort },
      cli: { ...(catalog.cliVersion ? { version: catalog.cliVersion } : {}) },
      catalog: { source: catalog.source, supported_efforts: [...entry.supportedEfforts] },
      ...(effort === 'ultra' ? { safeguards: { ultra: { ...ultraLimits } } } : {}),
    },
  }
}

export function parseCodexModelCatalog(
  value: unknown,
  opts: { source: string; cliVersion?: string } = { source: 'unknown' },
): CodexModelCatalog {
  const root = asRecord(value)
  const rawModels = Array.isArray(root?.data) ? root.data : Array.isArray(root?.models) ? root.models : null
  if (!rawModels) throw new Error('codex_catalog_missing_models')

  const models: CodexCatalogModel[] = []
  for (const raw of rawModels) {
    const model = asRecord(raw)
    if (!model) continue
    const name = firstString(model.model, model.id, model.slug)
    const rawEfforts = Array.isArray(model.supportedReasoningEfforts)
      ? model.supportedReasoningEfforts
      : Array.isArray(model.supported_reasoning_levels)
        ? model.supported_reasoning_levels
        : []
    const supportedEfforts = rawEfforts
      .map((item) => typeof item === 'string' ? item : firstString(asRecord(item)?.reasoningEffort, asRecord(item)?.effort))
      .filter((item): item is string => !!item)
    if (!name || supportedEfforts.length === 0) continue
    models.push({
      model: name,
      supportedEfforts: [...new Set(supportedEfforts)],
      ...(typeof model.hidden === 'boolean' ? { hidden: model.hidden } : {}),
    })
  }
  if (!models.length) throw new Error('codex_catalog_has_no_usable_models')
  return { source: opts.source, ...(opts.cliVersion ? { cliVersion: opts.cliVersion } : {}), models }
}

export function codexCatalogFromEnvironment(
  sourceEnv: Record<string, string | undefined>,
  fallback: CodexModelCatalog = CODEX_TESTED_MODEL_CATALOG,
): CodexModelCatalog {
  const raw = sourceEnv.CODEX_LAUNCH_MODEL_CATALOG_JSON?.trim()
  const cliVersion = sourceEnv.CODEX_CLI_VERSION?.trim() || fallback.cliVersion
  if (raw) {
    let parsed: unknown
    try { parsed = JSON.parse(raw) } catch { throw new Error('codex_catalog_invalid_json') }
    return parseCodexModelCatalog(parsed, {
      source: sourceEnv.CODEX_LAUNCH_MODEL_CATALOG_SOURCE?.trim() || 'launcher-env',
      ...(cliVersion ? { cliVersion } : {}),
    })
  }
  if (cliVersion && fallback.cliVersion && cliVersion !== fallback.cliVersion) {
    throw new Error(`codex_catalog_required_for_cli_version:${cliVersion}`)
  }
  return fallback
}

export function codexUltraLimitsFromEnvironment(sourceEnv: Record<string, string | undefined>): CodexUltraLimits {
  return {
    maxThreads: boundedInteger(sourceEnv.CODEX_GW_ULTRA_MAX_THREADS, 'CODEX_GW_ULTRA_MAX_THREADS', CODEX_ULTRA_DEFAULT_LIMITS.maxThreads, 6),
    maxDepth: boundedInteger(sourceEnv.CODEX_GW_ULTRA_MAX_DEPTH, 'CODEX_GW_ULTRA_MAX_DEPTH', CODEX_ULTRA_DEFAULT_LIMITS.maxDepth, 1),
    jobMaxRuntimeSeconds: boundedInteger(sourceEnv.CODEX_GW_ULTRA_JOB_MAX_RUNTIME_SECONDS, 'CODEX_GW_ULTRA_JOB_MAX_RUNTIME_SECONDS', CODEX_ULTRA_DEFAULT_LIMITS.jobMaxRuntimeSeconds, 3600),
    maxTurnTimeoutMs: boundedInteger(sourceEnv.CODEX_GW_ULTRA_MAX_TURN_TIMEOUT_MS, 'CODEX_GW_ULTRA_MAX_TURN_TIMEOUT_MS', CODEX_ULTRA_DEFAULT_LIMITS.maxTurnTimeoutMs, 3_600_000),
  }
}

export function codexUltraAppServerConfigArgs(effort: CodexThinkingLevel, limits: CodexUltraLimits): string[] {
  if (effort !== 'ultra') return []
  return [
    '-c', `agents.max_threads=${limits.maxThreads}`,
    '-c', `agents.max_depth=${limits.maxDepth}`,
    '-c', `agents.job_max_runtime_seconds=${limits.jobMaxRuntimeSeconds}`,
  ]
}

export function validateUltraTurnTimeout(effort: CodexThinkingLevel, timeoutMs: unknown, limits: CodexUltraLimits): string | null {
  if (effort !== 'ultra') return null
  if (!Number.isInteger(timeoutMs) || Number(timeoutMs) <= 0) return 'ultra requires a positive integer turn_timeout_ms'
  if (Number(timeoutMs) > limits.maxTurnTimeoutMs) return `ultra turn_timeout_ms must be <= ${limits.maxTurnTimeoutMs}`
  return null
}

export function codexPolicyMetadata(selection: CodexPolicySelection): Record<string, unknown> {
  return {
    requested_model: selection.requested.model,
    requested_effort: selection.requested.effort,
    effective_model: selection.effective.model,
    effective_effort: selection.effective.effort,
    cli_version: selection.cli.version ?? null,
    catalog_source: selection.catalog.source,
    catalog_supported_efforts: selection.catalog.supported_efforts,
    ...(selection.safeguards ? { safeguards: selection.safeguards } : {}),
  }
}

function cleanModel(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const model = value.trim()
  return model && model.length <= 128 && !/[\r\n]/.test(model) ? model : null
}

function boundedInteger(value: string | undefined, name: string, fallback: number, max: number): number {
  if (value == null || value.trim() === '') return fallback
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > max) throw new Error(`${name}_must_be_integer_1_to_${max}`)
  return parsed
}

function asRecord(value: unknown): Record<string, any> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, any> : null
}

function firstString(...values: unknown[]): string | undefined {
  return values.find((value): value is string => typeof value === 'string' && value.length > 0)
}
