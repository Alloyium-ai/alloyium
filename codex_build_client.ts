// codex_build_client.ts - dev-pm-side helper for Model-B Codex build dispatches.
//
// This module deliberately does NOT open an A2AChannel. dev-pm already owns its MCP
// session and sends the returned body with its own a2a_send tool:
//   1. buildBuildJob(redis, opts)
//   2. dev-pm a2a_send({ to: 'codex-build-gw', type: 'request', body: JSON.stringify(body) })
//   3. dev-pm waits for codex.job.* replies and calls parseBuildResult(reply)
//   4. dev-pm runs runBuildGuard + gates + commit/push/review
//   5. finishBuild(redis, cwdRealpath) in finally for write-mode jobs
//
// Large prompt input is claim-checked through output_transport.ts so the bus body
// stays below the 8 KiB A2A send cap while codex-build-gw resolves the full prompt.
import { randomUUID } from 'node:crypto'
import { buildClaimCheckedInput, type BlobRedis, type WrapFn } from './output_transport.ts'
import { isCwdRegistered, registerCwd, unregisterCwd, type BuildAuthzRedis } from './codex_build_authz.ts'

/** Default A2A request-body cap used when dev-pm is building a job without an A2AChannel instance. */
export const CODEX_BUILD_INPUT_BODY_CAP_BYTES = Number(process.env.A2A_MAX_SEND_BYTES ?? 8192)

/** Default bounded write-authz TTL for registered build worktrees. */
export const CODEX_BUILD_CWD_TTL_S = Number(process.env.CODEX_BUILD_CWD_TTL_S ?? 3600)

/** Redis surface needed by the build client: cwd authz registry plus claim-check blobs. */
export interface BuildClientRedis extends BuildAuthzRedis, BlobRedis {}

/** Sandbox modes accepted by codex-build-gw for build jobs. */
export type BuildSandbox = 'workspace-write' | 'read-only'

/** Options used to build a codex.job.request.v1 body for the dedicated build gateway. */
export interface BuildJobOpts {
  /** Optional caller-provided job id. A random codex-build-* id is generated when absent. */
  jobId?: string
  /** Stable thread key used by codex-build-gw for warm build context. */
  threadKey: string
  /** Canonical registered worktree realpath. Dispatch A verifies this before allowing writes. */
  cwdRealpath: string
  /** Full build prompt. Large prompts are stored as Redis blobs and referenced by input_ref. */
  promptText: string
  /** Requested Codex sandbox. Defaults to workspace-write for Model-B build jobs. */
  sandbox?: BuildSandbox
  /** Bounded cwd registry TTL in seconds for workspace-write dispatches. Defaults to one hour. */
  cwdTtlS?: number
  /** Admission budget ceiling sent to codex-build-gw. Defaults to 92 percent primary used. */
  budgetMaxPct?: number
  /** Optional lossy streaming topic that dev-pm may join before dispatch. */
  streamTopic?: string
}

/** Built job id plus the JSON body dev-pm should send via its own a2a_send tool. */
export interface BuiltBuildJob {
  /** The job id included in the returned request body. */
  jobId: string
  /** codex.job.request.v1 body, possibly carrying input_ref for the prompt. */
  body: Record<string, unknown>
  /** Input claim-check blob ref (present only for large prompts); caller should delBlob(redis, inputRef) if a2a_send dispatch fails (bounded TTL otherwise). */
  inputRef?: string
}

/** Structured envelope Codex build workers are asked to emit as JSON in completed.output. */
export interface BuildResultEnvelope {
  /** Repo-relative files the worker touched or proposes touching. */
  files_touched?: string[]
  /** Worktree realpath the worker reports it used. */
  cwd_realpath?: string
  /** Thread key used for the Codex build thread. */
  thread_key?: string
  /** Tests, commands, or checks the worker reports running. */
  tests_run?: any
  /** Any attempted approval/escalation operations; should remain empty under approval_policy never. */
  escalations_attempted?: string[]
  /** Free-form worker notes for dev-pm. */
  notes?: string
}

/** Normalized view of codex.job.* replies for dev-pm control flow. */
export interface ParsedBuildResult {
  /** True for accepted/completed, false for failed/rejected/unparseable replies. */
  ok: boolean
  /** Normalized job status such as accepted, completed, failed, or rejected. */
  status?: string
  /** Inline output or structured-envelope output string when present. */
  output?: string
  /** Claim-check reference for a large completed output, if codex-build-gw returned one. */
  resultRef?: object
  /** Rejection details when admission was denied. */
  rejected?: { reason: string; retryAfterMs?: number }
  /** Failure details when the job failed or the reply could not be parsed. */
  failed?: { error: string }
  /** Parsed structured build envelope from completed.output, when present. */
  envelope?: BuildResultEnvelope
}

/**
 * Build a codex.job.request.v1 body for dev-pm to send.
 *
 * workspace-write jobs register cwd authz first, verify the registration, and
 * keep that bounded write window open for the dispatched build. If construction
 * throws after registration, the cwd is unregistered in a finally block. read-only
 * jobs never register the cwd and therefore never open write eligibility.
 */
export async function buildBuildJob(redis: BuildClientRedis, opts: BuildJobOpts): Promise<BuiltBuildJob> {
  if (!opts.threadKey.trim()) throw new Error('buildBuildJob: threadKey is required')
  if (!opts.cwdRealpath.trim()) throw new Error('buildBuildJob: cwdRealpath is required')
  if (!opts.promptText.trim()) throw new Error('buildBuildJob: promptText is required')

  const jobId = opts.jobId ?? `codex-build-${randomUUID()}`
  const sandbox = opts.sandbox ?? 'workspace-write'
  const budgetMax = opts.budgetMaxPct ?? 92
  const cwdTtlS = opts.cwdTtlS ?? CODEX_BUILD_CWD_TTL_S
  const registerForWrite = sandbox === 'workspace-write'
  let unregisterOnFailure = false

  try {
    if (registerForWrite) {
      if (!Number.isFinite(cwdTtlS) || cwdTtlS <= 0) throw new Error('buildBuildJob: cwdTtlS must be positive')
      await registerCwd(redis, opts.cwdRealpath, { ttlS: cwdTtlS })
      unregisterOnFailure = true

      if (!(await isCwdRegistered(redis, opts.cwdRealpath))) {
        throw new Error(`buildBuildJob: cwd registration failed for ${opts.cwdRealpath}`)
      }
    }

    const wrap: WrapFn = (text, extra) => ({
      schema: 'codex.job.request.v1',
      job_id: jobId,
      thread_key: opts.threadKey,
      sandbox,
      cwd: opts.cwdRealpath,
      approval_policy: 'never',
      budget_policy: { max_primary_used_percent: budgetMax },
      input: [{ type: 'text', text }],
      ...(opts.streamTopic ? { stream_topic: opts.streamTopic } : {}),
      ...(extra ?? {}),
    })

    const built = await buildClaimCheckedInput(redis, opts.promptText, CODEX_BUILD_INPUT_BODY_CAP_BYTES, wrap)
    unregisterOnFailure = false
    return { jobId, body: built.body as Record<string, unknown>, ...(built.ref ? { inputRef: built.ref } : {}) }
  } finally {
    if (unregisterOnFailure) await unregisterCwd(redis, opts.cwdRealpath).catch(() => {})
  }
}

/**
 * Normalize codex.job.accepted/completed/failed/rejected.v1 replies.
 *
 * Accepts either the raw body object, a JSON string body, an A2A/MCP content text
 * wrapper, or an object carrying a `body` field. Completed replies with a JSON
 * structured envelope in `output` expose it as `envelope`.
 */
export function parseBuildResult(reply: any): ParsedBuildResult {
  const m = coerceReplyBody(reply)
  if (!isObj(m)) return { ok: false, status: 'unparseable', failed: { error: 'unparseable_reply' } }

  if (m.schema === 'codex.job.accepted.v1') {
    return { ok: true, status: 'accepted' }
  }

  if (m.schema === 'codex.job.completed.v1') {
    const outputInfo = parseEnvelopeOutput(m.output)
    return {
      ok: true,
      status: typeof m.status === 'string' ? m.status : 'completed',
      output: outputInfo.output,
      resultRef: resultRefFrom(m),
      envelope: outputInfo.envelope,
    }
  }

  if (m.schema === 'codex.job.rejected.v1') {
    return {
      ok: false,
      status: 'rejected',
      rejected: {
        reason: String(m.reason ?? m.error ?? 'rejected'),
        retryAfterMs: numberOrUndefined(m.retry_after_ms ?? m.retryAfterMs),
      },
    }
  }

  if (m.schema === 'codex.job.failed.v1') {
    return {
      ok: false,
      status: 'failed',
      failed: { error: String(m.error ?? 'failed') },
    }
  }

  return {
    ok: false,
    status: typeof m.schema === 'string' ? m.schema : 'unknown',
    failed: { error: 'unknown_build_reply_schema' },
  }
}

/** Unregister a build cwd after dev-pm has finished the write-mode build lifecycle. */
export async function finishBuild(redis: BuildAuthzRedis, cwdRealpath: string): Promise<void> {
  await unregisterCwd(redis, cwdRealpath)
}

function isObj(x: unknown): x is Record<string, any> {
  return typeof x === 'object' && x !== null && !Array.isArray(x)
}

function numberOrUndefined(x: unknown): number | undefined {
  return typeof x === 'number' && Number.isFinite(x) ? x : undefined
}

function coerceReplyBody(reply: any): Record<string, any> | null {
  if (isObj(reply) && typeof reply.schema === 'string') return reply
  if (typeof reply === 'string') return parseJsonObject(reply)
  if (isObj(reply?.body)) return reply.body
  if (typeof reply?.body === 'string') return parseJsonObject(reply.body)
  const text = reply?.content?.[0]?.text
  if (typeof text === 'string') return parseJsonObject(text)
  return null
}

function parseJsonObject(text: string): Record<string, any> | null {
  try {
    const parsed = JSON.parse(text)
    return isObj(parsed) ? parsed : null
  } catch {
    return null
  }
}

function resultRefFrom(m: Record<string, any>): object | undefined {
  if (typeof m.result_ref !== 'string' || !m.result_ref) return undefined
  return {
    result_ref: m.result_ref,
    ...(typeof m.sha256 === 'string' ? { sha256: m.sha256 } : {}),
    ...(typeof m.len === 'number' ? { len: m.len } : {}),
    ...(typeof m.encoding === 'string' ? { encoding: m.encoding } : {}),
    ...(typeof m.expires_at === 'string' ? { expires_at: m.expires_at } : {}),
  }
}

function parseEnvelopeOutput(output: unknown): { output?: string; envelope?: BuildResultEnvelope } {
  if (typeof output !== 'string') return {}
  const parsed = parseJsonish(output)
  if (!isObj(parsed)) return { output }

  const candidate = isObj(parsed.envelope) ? parsed.envelope : parsed
  const envelope: BuildResultEnvelope = {}

  if (Array.isArray(candidate.files_touched)) {
    envelope.files_touched = candidate.files_touched.filter((x: unknown): x is string => typeof x === 'string')
  }
  if (typeof candidate.cwd_realpath === 'string') envelope.cwd_realpath = candidate.cwd_realpath
  if (typeof candidate.thread_key === 'string') envelope.thread_key = candidate.thread_key
  if (candidate.tests_run !== undefined) envelope.tests_run = candidate.tests_run
  if (Array.isArray(candidate.escalations_attempted)) {
    envelope.escalations_attempted = candidate.escalations_attempted.filter((x: unknown): x is string => typeof x === 'string')
  }
  if (typeof candidate.notes === 'string') envelope.notes = candidate.notes

  const hasEnvelope = Object.keys(envelope).length > 0
  return {
    output: typeof parsed.output === 'string' ? parsed.output : output,
    envelope: hasEnvelope ? envelope : undefined,
  }
}

function parseJsonish(text: string): unknown {
  const trimmed = text.trim()
  try { return JSON.parse(trimmed) } catch {}

  const fence = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i)
  if (fence?.[1]) {
    try { return JSON.parse(fence[1].trim()) } catch {}
  }

  const start = trimmed.indexOf('{')
  const end = trimmed.lastIndexOf('}')
  if (start >= 0 && end > start) {
    try { return JSON.parse(trimmed.slice(start, end + 1)) } catch {}
  }

  return null
}
