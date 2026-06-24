import { connect, AckPolicy, DeliverPolicy } from 'nats'
import { A2AChannel } from './a2a-channel.ts'
import { NatsTaskboardEventPublisher } from './taskboard_event_bridge.ts'
import {
  TASKBOARD_EVENT_STREAM,
  TASKBOARD_EVENT_SUBJECT,
  TASKBOARD_EVENT_SCHEMA,
  type TaskboardEventEnvelope,
} from './taskboard_event_core.ts'
export type { TaskboardEventEnvelope } from './taskboard_event_core.ts'

export type CodexJobRequest = {
  schema: 'codex.job.request.v1'
  job_id: string
  thread_key: string
  input: Array<{ type: 'text'; text: string }>
  sandbox: 'read-only'
  approval_policy: 'never'
  cwd: string
  budget_policy: { max_primary_used_percent: number }
  task_id?: number | null
  agent_run_id?: number
  event_id?: string
  model?: string
  endpoint_id?: string
  taskboard_url?: string
  task_context?: Record<string, unknown>
  taskboard?: {
    task_id: number | null
    agent_run_id?: number
    event_id: string
    taskboard_url?: string
  }
}

export type AgentRunRecord = { id: number; status?: string; [key: string]: unknown }

export type TaskboardReviewVerdict = 'APPROVE' | 'REQUEST_CHANGES' | 'COMMENT'
export type TaskboardReviewSubmission = {
  verdict: TaskboardReviewVerdict
  // Optional: Chore / no-PR review tasks have no commit-bound artifact, so the verdict is
  // submitted commit-less. The server accepts a commit-less verdict for those tasks (#10556).
  commit_sha?: string
  findings_md?: string
  findings?: Array<Record<string, unknown>>
}
export type TaskboardReviewWritebackResult =
  | { action: 'submitted'; verdict: TaskboardReviewVerdict; review_id?: number | string }
  | { action: 'skipped'; reason: string }
  | { action: 'failed'; reason: string }

export type TaskboardRunStore = {
  createOrReuse(event: TaskboardEventEnvelope, targetAgentId: string): Promise<AgentRunRecord | null>
  update?(runId: number, update: Record<string, unknown>): Promise<void>
  findBySession?(sessionId: string): Promise<AgentRunRecord | null>
  postComment?(taskId: number, agent: string, content: string): Promise<void>
  submitReview?(reviewRequestId: number, roleKey: string, submission: TaskboardReviewSubmission): Promise<TaskboardReviewWritebackResult>
  getTaskCommitSha?(taskId: number): Promise<string | null>
}

export type CodexJobSender = {
  send(targetAgentId: string, job: CodexJobRequest): Promise<{ ok: boolean; id?: string; error?: string }>
}

export type IdempotencyStore = {
  has(key: string): boolean | Promise<boolean>
  mark(key: string): void | Promise<void>
}

export type ProcessTaskboardEventDeps = {
  sender: CodexJobSender
  runs?: TaskboardRunStore
  idempotency?: IdempotencyStore
}

export type ProcessTaskboardEventOpts = {
  defaultAgentId?: string
  defaultCwd?: string
  taskboardUrl?: string
  maxPrimaryUsedPercent?: number
}

export type ProcessTaskboardEventResult =
  | { action: 'ignored'; reason: 'duplicate' | 'unsupported_event' | 'not_in_progress'; event_id: string }
  | { action: 'sent'; event_id: string; target: string; job_id: string; agent_run_id?: number; message_id?: string }
  | { action: 'dispatch_failed'; event_id: string; target: string; job_id: string; agent_run_id?: number; error: string }

export type TaskboardDispatch = {
  event: TaskboardEventEnvelope
  task_id: number | null
  fire_generation: number | null
  event_id: string
  taskboard_url?: string
  target_agent_id: string
  task: Record<string, unknown>
}

export type TaskboardRouteResult =
  | { action: 'dispatch'; dispatch: TaskboardDispatch }
  | { action: 'observe'; reason: 'unsupported_schema' | 'unsupported_event_type' | 'status_not_in_progress' | 'review_target_unconfigured' }
type TaskboardObserveReason = Extract<TaskboardRouteResult, { action: 'observe' }>['reason']

export type TaskboardEventDispatcherResult =
  | { action: 'observed'; ack: true; reason: TaskboardObserveReason; event_id?: string }
  | { action: 'duplicate'; ack: true; reason: 'duplicate'; event_id: string; task_id: number | null; agent_run_id?: number | string }
  | { action: 'dispatched'; ack: true; event_id: string; task_id: number | null; target_agent_id: string; job_id: string; agent_run_id?: number | string; message_id?: string }
  | { action: 'dispatch_failed'; ack: false; event_id: string; task_id: number | null; target_agent_id: string; job_id: string; agent_run_id?: number | string; error: string }

export type CodexJobReply =
  | { kind: 'accepted'; job_id: string; corr?: string; from?: string; msg: Record<string, unknown> }
  | { kind: 'completed'; job_id: string; corr?: string; from?: string; status?: string; output?: string; result_ref?: string; output_preview?: boolean; msg: Record<string, unknown> }
  | { kind: 'failed'; job_id: string; corr?: string; from?: string; error: string; msg: Record<string, unknown> }
  | { kind: 'rejected'; job_id: string; corr?: string; from?: string; reason: string; detail?: string; msg: Record<string, unknown> }
  | { kind: 'ignore'; reason: string }

export type PendingTaskboardJob = {
  corr?: string
  job_id: string
  event_id: string
  task_id: number | null
  agent_run_id?: number
  target: string
  review_request_id?: number
  review_role_key?: string
  review_commit_sha?: string
}

const DEFAULT_AGENT_ID = 'host-ops-gw'
const DEFAULT_CWD = '/srv/git/alloyium'
const TASKBOARD_SOURCE_COMPONENT = 'a2a-taskboard-dispatcher'
const TASK_STATUS_CHANGED_EVENT_TYPE = 'task.status_changed'
const TASK_REVIEW_REQUESTED_EVENT_TYPE = 'task.review_requested'
const TOKEN_RE = /^[a-z0-9-]{1,64}$/

export class MemoryIdempotencyStore implements IdempotencyStore {
  private seen = new Set<string>()
  has(key: string): boolean { return this.seen.has(key) }
  mark(key: string): void { this.seen.add(key) }
}

export class InMemoryTaskboardAgentRunStore {
  private runs = new Map<string, number | string>()
  constructor(private idFactory: (dispatch: TaskboardDispatch) => number | string = (dispatch) => `run-${dispatch.task_id ?? dispatch.event_id}`) {}
  prepareAgentRun(dispatch: TaskboardDispatch): { action: 'dispatch' | 'duplicate'; agent_run_id?: number | string } {
    const key = dispatch.event_id
    const existing = this.runs.get(key)
    if (existing != null) return { action: 'duplicate', agent_run_id: existing }
    const id = this.idFactory(dispatch)
    this.runs.set(key, id)
    return { action: 'dispatch', agent_run_id: id }
  }
}

export function routeTaskboardEvent(input: unknown, opts: { defaultAgentId?: string } = {}): TaskboardRouteResult {
  if (!isTaskboardEvent(input)) return { action: 'observe', reason: 'unsupported_schema' }
  if (![TASK_STATUS_CHANGED_EVENT_TYPE, TASK_REVIEW_REQUESTED_EVENT_TYPE].includes(input.event_type)) {
    return { action: 'observe', reason: 'unsupported_event_type' }
  }
  if (!shouldDispatchTaskboardEvent(input)) return { action: 'observe', reason: 'status_not_in_progress' }
  const task = taskPayload(input)
  const target = targetAgentForEvent(input, opts.defaultAgentId ?? process.env.TASKBOARD_DISPATCH_DEFAULT_AGENT_ID ?? DEFAULT_AGENT_ID)
  if (!target) return { action: 'observe', reason: 'review_target_unconfigured' }
  return {
    action: 'dispatch',
    dispatch: {
      event: input,
      task_id: input.scope.task_id,
      fire_generation: input.idempotency.fire_generation,
      event_id: input.event_id,
      ...(input.source.taskboard_url ? { taskboard_url: input.source.taskboard_url } : {}),
      target_agent_id: target,
      task,
    },
  }
}

export class TaskboardEventDispatcher {
  constructor(private opts: {
    defaultAgentId?: string
    cwd?: string
    taskboardUrl?: string
    hooks?: { prepareAgentRun?: (dispatch: TaskboardDispatch) => { action: 'dispatch' | 'duplicate'; agent_run_id?: number | string } | Promise<{ action: 'dispatch' | 'duplicate'; agent_run_id?: number | string }> } | InMemoryTaskboardAgentRunStore
    sendJob: (input: { target_agent_id: string; job: CodexJobRequest; event: TaskboardEventEnvelope; dispatch: TaskboardDispatch }) => { ok: boolean; id?: string; error?: string } | Promise<{ ok: boolean; id?: string; error?: string }>
  }) {}

  async handle(input: unknown): Promise<TaskboardEventDispatcherResult> {
    const route = routeTaskboardEvent(input, { defaultAgentId: this.opts.defaultAgentId })
    if (route.action === 'observe') {
      return { action: 'observed', ack: true, reason: route.reason, event_id: isTaskboardEvent(input) ? input.event_id : undefined }
    }

    const prepared = await this.opts.hooks?.prepareAgentRun?.(route.dispatch)
    if (prepared?.action === 'duplicate') {
      return {
        action: 'duplicate',
        ack: true,
        reason: 'duplicate',
        event_id: route.dispatch.event_id,
        task_id: route.dispatch.task_id,
        ...(prepared.agent_run_id != null ? { agent_run_id: prepared.agent_run_id } : {}),
      }
    }

    const job = buildCodexJobRequest(route.dispatch, {
      cwd: this.opts.cwd,
      agentRunId: typeof prepared?.agent_run_id === 'number' ? prepared.agent_run_id : undefined,
      taskboardUrl: this.opts.taskboardUrl,
    })
    if (prepared?.agent_run_id != null && typeof prepared.agent_run_id !== 'number') {
      ;(job as any).agent_run_id = prepared.agent_run_id
      ;(job.task_context as any).agent_run_id = prepared.agent_run_id
    }
    const sent = await this.opts.sendJob({ target_agent_id: route.dispatch.target_agent_id, job, event: route.dispatch.event, dispatch: route.dispatch })
    if (!sent.ok) {
      return {
        action: 'dispatch_failed',
        ack: false,
        event_id: route.dispatch.event_id,
        task_id: route.dispatch.task_id,
        target_agent_id: route.dispatch.target_agent_id,
        job_id: job.job_id,
        ...(prepared?.agent_run_id != null ? { agent_run_id: prepared.agent_run_id } : {}),
        error: sent.error ?? 'send_failed',
      }
    }
    return {
      action: 'dispatched',
      ack: true,
      event_id: route.dispatch.event_id,
      task_id: route.dispatch.task_id,
      target_agent_id: route.dispatch.target_agent_id,
      job_id: job.job_id,
      ...(prepared?.agent_run_id != null ? { agent_run_id: prepared.agent_run_id } : {}),
      ...(sent.id ? { message_id: sent.id } : {}),
    }
  }
}

export class TaskboardReplyHandler {
  private byCorr = new Map<string, PendingTaskboardJob>()
  private byJob = new Map<string, PendingTaskboardJob>()

  constructor(private opts: {
    runs?: TaskboardRunStore
    commentAgent?: string
    log?: (...args: any[]) => void
  } = {}) {}

  track(job: PendingTaskboardJob): void {
    this.byJob.set(job.job_id, job)
    if (job.corr) this.byCorr.set(job.corr, job)
  }

  async handleInbound(content: string, attrs: Record<string, unknown>): Promise<{ action: 'ignored' | 'updated'; reply?: CodexJobReply; run_id?: number }> {
    const reply = classifyCodexJobReply(content, attrs)
    if (reply.kind === 'ignore') return { action: 'ignored', reply }
    const pending = await this.resolvePending(reply)
    if (!pending?.agent_run_id || pending.task_id == null || !this.opts.runs?.update) {
      this.opts.log?.('[taskboard-reply] no pending run for', reply.job_id, reply.kind)
      return { action: 'ignored', reply }
    }

    if (reply.kind === 'accepted') {
      await this.opts.runs.update(pending.agent_run_id, { status: 'spawning', session_id: reply.job_id }).catch(() => {})
      return { action: 'updated', reply, run_id: pending.agent_run_id }
    }

    if (reply.kind === 'completed') {
      await this.opts.runs.update(pending.agent_run_id, { status: 'running', session_id: reply.job_id }).catch(() => {})
      const reviewWriteback = await this.submitReviewIfRequested(reply, pending)
      await this.opts.runs.update(pending.agent_run_id, { status: 'succeeded' })
      await this.opts.runs.postComment?.(
        pending.task_id,
        this.opts.commentAgent ?? 'taskboard-dispatcher',
        buildCodexReplyComment(reply, pending, reviewWriteback),
      ).catch((e) => this.opts.log?.('[taskboard-reply] comment failed', errorMessage(e)))
      this.deletePending(pending)
      return { action: 'updated', reply, run_id: pending.agent_run_id }
    }

    if (reply.kind === 'failed') {
      await this.opts.runs.update(pending.agent_run_id, {
        status: 'failed',
        failure_class: codexFailureClass(reply.error),
        failure_detail: truncateForTaskboard(reply.error, 1500),
      })
      await this.opts.runs.postComment?.(
        pending.task_id,
        this.opts.commentAgent ?? 'taskboard-dispatcher',
        buildCodexReplyComment(reply, pending),
      ).catch((e) => this.opts.log?.('[taskboard-reply] comment failed', errorMessage(e)))
      this.deletePending(pending)
      return { action: 'updated', reply, run_id: pending.agent_run_id }
    }

    await this.opts.runs.update(pending.agent_run_id, {
      status: 'endpoint_failed',
      failure_class: 'endpoint_invalid_response',
      failure_detail: truncateForTaskboard(`${reply.reason}${reply.detail ? `: ${reply.detail}` : ''}`, 1500),
    })
    await this.opts.runs.postComment?.(
      pending.task_id,
      this.opts.commentAgent ?? 'taskboard-dispatcher',
      buildCodexReplyComment(reply, pending),
    ).catch((e) => this.opts.log?.('[taskboard-reply] comment failed', errorMessage(e)))
    this.deletePending(pending)
    return { action: 'updated', reply, run_id: pending.agent_run_id }
  }

  private async resolvePending(reply: Exclude<CodexJobReply, { kind: 'ignore' }>): Promise<PendingTaskboardJob | null> {
    const local = (reply.corr ? this.byCorr.get(reply.corr) : undefined) ?? this.byJob.get(reply.job_id)
    if (local) return local
    const run = await this.opts.runs?.findBySession?.(reply.job_id).catch(() => null)
    if (!run) return null
    const pending: PendingTaskboardJob = {
      job_id: reply.job_id,
      corr: reply.corr,
      event_id: typeof run.trigger_event_id === 'string' ? run.trigger_event_id : reply.job_id,
      task_id: numberOrNull(run.task_id),
      agent_run_id: numberOrNull(run.id) ?? undefined,
      target: reply.from ?? String(run.agent_id ?? ''),
      review_request_id: parseReviewJobId(reply.job_id)?.requestId,
      review_role_key: stringValue(run.role) ?? undefined,
    }
    this.track(pending)
    return pending
  }

  private async submitReviewIfRequested(
    reply: Extract<CodexJobReply, { kind: 'completed' }>,
    pending: PendingTaskboardJob,
  ): Promise<TaskboardReviewWritebackResult | undefined> {
    if (pending.review_request_id == null || !pending.review_role_key) return undefined
    if (!this.opts.runs?.submitReview) {
      return { action: 'skipped', reason: 'run store does not support V2 review submission' }
    }

    const extracted = extractTaskboardReviewResult(reply.output ?? '')
    if (!extracted) {
      const suffix = reply.output_preview ? '; output was claim-checked and preview text did not include the block' : ''
      return { action: 'skipped', reason: `missing taskboard_review_result JSON block${suffix}` }
    }

    // #10557: never silently skip a verdict. Resolve the commit deterministically; if neither the
    // reply nor the pending record carries one, fetch the task's current commit from the API. When
    // the task is genuinely commit-less (Chore / no-PR) we submit without a commit — the server
    // accepts a commit-less verdict for those tasks (#10556) and otherwise returns a deterministic
    // error surfaced as action:'failed'. The writeback must never no-op a verdict.
    let commitSha = extracted.commit_sha ?? pending.review_commit_sha
    if (!commitSha && pending.task_id != null && this.opts.runs.getTaskCommitSha) {
      commitSha = (await this.opts.runs.getTaskCommitSha(pending.task_id).catch(() => null)) ?? undefined
    }

    try {
      return await this.opts.runs.submitReview(pending.review_request_id, pending.review_role_key, {
        verdict: extracted.verdict,
        ...(commitSha ? { commit_sha: commitSha } : {}),
        findings_md: extracted.findings_md ?? '',
        findings: extracted.findings ?? [],
      })
    } catch (e) {
      return { action: 'failed', reason: errorMessage(e) }
    }
  }

  private deletePending(job: PendingTaskboardJob): void {
    this.byJob.delete(job.job_id)
    if (job.corr) this.byCorr.delete(job.corr)
  }
}

export function classifyCodexJobReply(content: string, attrs: Record<string, unknown> = {}): CodexJobReply {
  if (attrs.feed !== 'a2a' || attrs.kind !== 'direct' || attrs.type !== 'reply') return { kind: 'ignore', reason: 'not_a2a_reply' }
  let msg: Record<string, unknown>
  try {
    const parsed = JSON.parse(content)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { kind: 'ignore', reason: 'bad_body' }
    msg = parsed as Record<string, unknown>
  } catch {
    return { kind: 'ignore', reason: 'bad_json' }
  }
  const job_id = stringValue(msg.job_id)
  if (!job_id) return { kind: 'ignore', reason: 'missing_job_id' }
  const common = { job_id, corr: stringValue(attrs.corr) ?? undefined, from: stringValue(attrs.from) ?? undefined, msg }
  switch (msg.schema) {
    case 'codex.job.accepted.v1':
      return { kind: 'accepted', ...common }
    case 'codex.job.completed.v1':
      return {
        kind: 'completed',
        ...common,
        status: stringValue(msg.status) ?? undefined,
        output: stringValue(msg.output) ?? undefined,
        result_ref: stringValue(msg.result_ref) ?? undefined,
        output_preview: msg.output_preview === true,
      }
    case 'codex.job.failed.v1':
      return { kind: 'failed', ...common, error: stringValue(msg.error) ?? 'codex job failed' }
    case 'codex.job.rejected.v1':
      return {
        kind: 'rejected',
        ...common,
        reason: stringValue(msg.reason) ?? 'codex job rejected',
        detail: stringValue(msg.detail) ?? undefined,
      }
    default:
      return { kind: 'ignore', reason: 'not_codex_job_reply' }
  }
}

export function buildCodexReplyComment(
  reply: Exclude<CodexJobReply, { kind: 'ignore' | 'accepted' }>,
  job: PendingTaskboardJob,
  reviewWriteback?: TaskboardReviewWritebackResult,
): string {
  const lines = [
    `## Codex job ${reply.kind}`,
    '',
    `Job: ${reply.job_id}`,
    `Agent run: ${job.agent_run_id ?? '(unknown)'}`,
    `Gateway: ${(reply.from ?? job.target) || '(unknown)'}`,
    `Event: ${job.event_id}`,
    '',
  ]
  if (reply.kind === 'completed') {
    lines.push(`Status: ${reply.status ?? 'completed'}`, '')
    if (reply.output) lines.push(truncateForTaskboard(reply.output, 6000))
    if (reply.result_ref) {
      lines.push('', `Result ref: ${reply.result_ref}`)
      if (reply.output_preview) lines.push('Note: output was claim-checked; the text above is a preview.')
    }
    if (reviewWriteback) {
      lines.push('', 'Review verdict writeback:')
      if (reviewWriteback.action === 'submitted') {
        lines.push(`Status: submitted ${reviewWriteback.verdict}`)
        if (reviewWriteback.review_id != null) lines.push(`Review ID: ${reviewWriteback.review_id}`)
      } else {
        lines.push(`Status: ${reviewWriteback.action}`)
        lines.push(`Reason: ${reviewWriteback.reason}`)
      }
    }
  } else if (reply.kind === 'failed') {
    lines.push('Failure:', '', truncateForTaskboard(reply.error, 6000))
  } else {
    lines.push(`Rejected: ${reply.reason}`)
    if (reply.detail) lines.push('', truncateForTaskboard(reply.detail, 6000))
  }
  return lines.join('\n')
}

export function shouldDispatchTaskboardEvent(event: TaskboardEventEnvelope): boolean {
  if (event.schema !== TASKBOARD_EVENT_SCHEMA) return false
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) return reviewRequestId(event) != null
  if (event.event_type !== TASK_STATUS_CHANGED_EVENT_TYPE) return false
  const payload = payloadRecord(event)
  return normalizeStatus(payload.to_status) === 'in progress' && normalizeStatus(payload.from_status) !== 'in progress'
}

export function targetAgentForEvent(event: TaskboardEventEnvelope, defaultAgentId = DEFAULT_AGENT_ID): string | null {
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) {
    const review = reviewRequestPayload(event)
    const payload = payloadRecord(event)
    const role = reviewRoleKey(event)
    const explicit = stringValue(review.reviewer_agent_id) ?? stringValue(review.agent_id) ?? stringValue(payload.reviewer_agent_id) ?? stringValue(payload.target_agent_id)
    if (explicit && explicit !== role && TOKEN_RE.test(explicit)) return explicit
    const roleTarget = role ? process.env[`TASKBOARD_DISPATCH_REVIEW_AGENT_ID_${reviewRoleEnvSuffix(role)}`]?.trim().toLowerCase() : ''
    if (roleTarget && TOKEN_RE.test(roleTarget)) return roleTarget
    const reviewDefault = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID?.trim().toLowerCase()
    if (reviewDefault && TOKEN_RE.test(reviewDefault)) return reviewDefault
    return null
  }
  const task = taskPayload(event)
  const agentId = stringValue(task.agent_id)
  if (agentId && TOKEN_RE.test(agentId)) return agentId
  const fallback = defaultAgentId.trim().toLowerCase()
  return TOKEN_RE.test(fallback) ? fallback : DEFAULT_AGENT_ID
}

export function buildCodexJobRequest(
  input: TaskboardEventEnvelope | TaskboardDispatch,
  opts: {
    agentRunId?: number
    cwd?: string
    taskboardUrl?: string
    maxPrimaryUsedPercent?: number
  } = {},
): CodexJobRequest {
  const event = 'event' in input ? input.event : input
  const route = 'event' in input ? { action: 'dispatch' as const, dispatch: input } : routeTaskboardEvent(input)
  const dispatch = route.action === 'dispatch' ? route.dispatch : null
  const taskId = event.scope.task_id
  const fireGeneration = event.idempotency.fire_generation
  const reviewRequest = reviewRequestId(event)
  const reviewerModel = reviewerModelForEvent(event)
  const reviewerEndpoint = reviewerEndpointForEvent(event)
  const job_id = jobIdForEvent(event)
  const taskboardUrl = opts.taskboardUrl ?? event.source.taskboard_url
  const task = taskPayload(event)
  const taskContext = {
    taskboard_url: taskboardUrl,
    task_id: taskId,
    ...(opts.agentRunId != null ? { agent_run_id: opts.agentRunId } : {}),
    event_id: event.event_id,
    event_type: event.event_type,
    fire_generation: fireGeneration,
    ...(reviewRequest != null ? { review_request_id: reviewRequest } : {}),
    ...(reviewRoleKey(event) ? { review_role_key: reviewRoleKey(event) } : {}),
    ...(reviewerModel ? { reviewer_model: reviewerModel } : {}),
    ...(reviewerEndpoint ? { reviewer_endpoint_id: reviewerEndpoint } : {}),
    title: stringValue(task.title),
    agent_id: stringValue(task.agent_id) ?? dispatch?.target_agent_id,
  }
  return {
    schema: 'codex.job.request.v1',
    job_id,
    thread_key: threadKeyForEvent(event),
    input: [{ type: 'text', text: buildTaskPrompt(event, { agentRunId: opts.agentRunId, taskboardUrl }) }],
    sandbox: 'read-only',
    approval_policy: 'never',
    cwd: opts.cwd ?? DEFAULT_CWD,
    budget_policy: { max_primary_used_percent: opts.maxPrimaryUsedPercent ?? 99 },
    task_id: taskId,
    ...(opts.agentRunId != null ? { agent_run_id: opts.agentRunId } : {}),
    event_id: event.event_id,
    ...(reviewerModel ? { model: reviewerModel } : {}),
    ...(reviewerEndpoint ? { endpoint_id: reviewerEndpoint } : {}),
    ...(taskboardUrl ? { taskboard_url: taskboardUrl } : {}),
    task_context: taskContext,
    taskboard: {
      task_id: taskId,
      ...(opts.agentRunId != null ? { agent_run_id: opts.agentRunId } : {}),
      event_id: event.event_id,
      ...(taskboardUrl ? { taskboard_url: taskboardUrl } : {}),
    },
  }
}

export async function processTaskboardEvent(
  event: TaskboardEventEnvelope,
  deps: ProcessTaskboardEventDeps,
  opts: ProcessTaskboardEventOpts = {},
): Promise<ProcessTaskboardEventResult> {
  const key = event.idempotency?.event_id || event.event_id
  if (deps.idempotency && await deps.idempotency.has(key)) {
    return { action: 'ignored', reason: 'duplicate', event_id: event.event_id }
  }
  const route = routeTaskboardEvent(event)
  if (route.action === 'observe' && route.reason !== 'status_not_in_progress') {
    if (deps.idempotency) await deps.idempotency.mark(key)
    return { action: 'ignored', reason: 'unsupported_event', event_id: event.event_id }
  }
  if (route.action === 'observe') {
    if (deps.idempotency) await deps.idempotency.mark(key)
    return { action: 'ignored', reason: 'not_in_progress', event_id: event.event_id }
  }

  const target = route.dispatch.target_agent_id
  const run = deps.runs ? await deps.runs.createOrReuse(event, target) : null
  if (run?.id && deps.runs?.update) {
    await deps.runs.update(run.id, { status: 'dispatching' }).catch(() => {})
  }
  const job = buildCodexJobRequest(event, {
    agentRunId: run?.id,
    cwd: opts.defaultCwd ?? process.env.TASKBOARD_DISPATCH_DEFAULT_CWD ?? DEFAULT_CWD,
    taskboardUrl: opts.taskboardUrl ?? process.env.TASKBOARD_URL ?? event.source.taskboard_url,
    maxPrimaryUsedPercent: opts.maxPrimaryUsedPercent,
  })

  const sent = await deps.sender.send(target, job)
  if (!sent.ok) {
    if (run?.id && deps.runs?.update) {
      await deps.runs.update(run.id, {
        status: 'preflight_failed',
        failure_class: 'endpoint_transport_drop',
        failure_detail: sent.error ?? 'a2a_send failed',
      }).catch(() => {})
    }
    return {
      action: 'dispatch_failed',
      event_id: event.event_id,
      target,
      job_id: job.job_id,
      ...(run?.id != null ? { agent_run_id: run.id } : {}),
      error: sent.error ?? 'a2a_send failed',
    }
  }

  if (run?.id && deps.runs?.update) {
    await deps.runs.update(run.id, { status: 'spawning', session_id: job.job_id }).catch(() => {})
  }
  if (deps.idempotency) await deps.idempotency.mark(key)
  return {
    action: 'sent',
    event_id: event.event_id,
    target,
    job_id: job.job_id,
    ...(run?.id != null ? { agent_run_id: run.id } : {}),
    ...(sent.id ? { message_id: sent.id } : {}),
  }
}

export class A2ACodexJobSender implements CodexJobSender {
  constructor(private channel: A2AChannel) {}
  async send(targetAgentId: string, job: CodexJobRequest): Promise<{ ok: boolean; id?: string; error?: string }> {
    const res = unwrapToolResult(await this.channel.callTool('a2a_send', {
      to: targetAgentId,
      type: 'request',
      thread: job.thread_key,
      body: JSON.stringify(job),
    }))
    return res?.ok ? { ok: true, id: res.id } : { ok: false, error: res?.error ?? 'send_failed' }
  }
}

export class TaskboardHttpRunStore implements TaskboardRunStore {
  private baseUrl: string
  constructor(
    baseUrl: string,
    private token?: string | null,
    private reviewTokens: Record<string, string | undefined> = reviewTokensFromEnv(),
  ) {
    this.baseUrl = baseUrl.replace(/\/+$/, '')
  }

  async createOrReuse(event: TaskboardEventEnvelope, targetAgentId: string): Promise<AgentRunRecord | null> {
    const taskId = event.scope.task_id
    if (taskId == null) return null
    const existing = await this.listRuns(taskId).catch(() => [])
    const match = existing.find((r) => r.trigger_event_id === event.event_id)
    if (match) return match
    const body = {
      task_id: taskId,
      role: roleForTask(event),
      source_component: TASKBOARD_SOURCE_COMPONENT,
      status: 'queued',
      agent_id: targetAgentId,
      fire_generation: event.idempotency.fire_generation,
      trigger_event_id: event.event_id,
    }
    const res = await fetch(`${this.baseUrl}/api/agent-runs`, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify(body),
    })
    if (!res.ok) throw new Error(`agent_run create failed: ${res.status} ${await safeText(res)}`)
    return await res.json() as AgentRunRecord
  }

  async update(runId: number, update: Record<string, unknown>): Promise<void> {
    const res = await fetch(`${this.baseUrl}/api/agent-runs/${runId}`, {
      method: 'PATCH',
      headers: this.headers(),
      body: JSON.stringify(update),
    })
    if (!res.ok) throw new Error(`agent_run update failed: ${res.status} ${await safeText(res)}`)
  }

  async findBySession(sessionId: string): Promise<AgentRunRecord | null> {
    for (const status of ['spawning', 'running', 'dispatching', 'queued']) {
      const res = await fetch(`${this.baseUrl}/api/agent-runs?status=${encodeURIComponent(status)}&limit=200`, { headers: this.headers(false) })
      if (!res.ok) continue
      const body = await res.json()
      const rows: AgentRunRecord[] = Array.isArray(body) ? body : Array.isArray(body?.items) ? body.items : []
      const match = rows.find((r) => r.session_id === sessionId)
      if (match) return match
    }
    return null
  }

  async postComment(taskId: number, agent: string, content: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}/api/tasks/${taskId}/comments`, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ agent, content }),
    })
    if (!res.ok) throw new Error(`task comment failed: ${res.status} ${await safeText(res)}`)
  }

  async submitReview(reviewRequestId: number, roleKey: string, submission: TaskboardReviewSubmission): Promise<TaskboardReviewWritebackResult> {
    const token = reviewTokenForRole(roleKey, this.reviewTokens)
    if (!token) return { action: 'skipped', reason: `missing review token for role ${roleKey}` }
    const res = await fetch(`${this.baseUrl}/api/v2/review-requests/${reviewRequestId}/reviews`, {
      method: 'POST',
      headers: this.headersWithToken(token),
      body: JSON.stringify({
        verdict: submission.verdict,
        // Omit commit_sha entirely when commit-less (server rejects empty string, accepts absent).
        ...(submission.commit_sha ? { commit_sha: submission.commit_sha } : {}),
        findings_md: submission.findings_md ?? '',
        findings: submission.findings ?? [],
      }),
    })
    if (!res.ok) return { action: 'failed', reason: `review submit failed: ${res.status} ${await safeText(res)}` }
    const body = await res.json().catch(() => null)
    const reviewId = numberOrNull((body as any)?.id) ?? stringValue((body as any)?.id)
    return {
      action: 'submitted',
      verdict: submission.verdict,
      ...(reviewId != null ? { review_id: reviewId } : {}),
    }
  }

  async getTaskCommitSha(taskId: number): Promise<string | null> {
    const res = await fetch(`${this.baseUrl}/api/v2/tasks/${taskId}/gate-state`, { headers: this.headers(false) })
    if (!res.ok) return null
    const body = await res.json().catch(() => null)
    return stringValue((body as any)?.latest_commit_sha) ?? null
  }

  private async listRuns(taskId: number): Promise<AgentRunRecord[]> {
    const res = await fetch(`${this.baseUrl}/api/tasks/${taskId}/agent-runs?limit=200`, { headers: this.headers(false) })
    if (!res.ok) throw new Error(`agent_run list failed: ${res.status} ${await safeText(res)}`)
    const body = await res.json()
    return Array.isArray(body) ? body : Array.isArray(body?.items) ? body.items : []
  }

  private headers(json = true): HeadersInit {
    return {
      ...(json ? { 'Content-Type': 'application/json' } : {}),
      ...(this.token ? { Authorization: `Bearer ${this.token}` } : {}),
    }
  }

  private headersWithToken(token: string, json = true): HeadersInit {
    return {
      ...(json ? { 'Content-Type': 'application/json' } : {}),
      Authorization: `Bearer ${token}`,
    }
  }
}

async function main(): Promise<void> {
  const stream = process.env.TASKBOARD_EVENT_STREAM ?? TASKBOARD_EVENT_STREAM
  const filterSubject = process.env.TASKBOARD_DISPATCH_FILTER_SUBJECT ?? TASKBOARD_EVENT_SUBJECT
  const durable = process.env.TASKBOARD_DISPATCH_DURABLE ?? 'a2a-taskboard-dispatcher'
  const selfId = process.env.A2A_AGENT_ID ?? process.env.TASKBOARD_DISPATCH_AGENT_ID ?? 'taskboard-dispatcher'
  const natsUrl = process.env.NATS_URL ?? 'nats://nats:4222'
  const taskboardUrl = process.env.TASKBOARD_URL
  const taskboardToken = process.env.TASKBOARD_API_TOKEN
  const runStore = taskboardUrl ? new TaskboardHttpRunStore(taskboardUrl, taskboardToken) : undefined
  const replyHandler = new TaskboardReplyHandler({ runs: runStore, log: (...args) => console.error(...args) })

  const a2a = new A2AChannel((content, attrs) => {
    void replyHandler.handleInbound(content, attrs as Record<string, unknown>)
      .then((result) => {
        if (result.action === 'updated') console.error(`[a2a-taskboard-dispatcher] reply_${result.reply?.kind} job=${(result.reply as any)?.job_id} run=${result.run_id}`)
      })
      .catch((e) => console.error(`[a2a-taskboard-dispatcher] reply_failed ${errorMessage(e)}`))
  }, { enabled: true, agentId: selfId })
  await a2a.start()
  if (!a2a.isStarted()) throw new Error(`could not join A2A bus as ${selfId}`)

  const nc = await connect({ servers: natsUrl, name: 'a2a-taskboard-dispatcher', reconnect: true, maxReconnectAttempts: -1 })
  await new NatsTaskboardEventPublisher(nc).ensureStream()
  await ensureConsumer(nc, stream, durable, filterSubject)

  const consumer = await nc.jetstream().consumers.get(stream, durable)
  const messages = await consumer.consume()
  const deps: ProcessTaskboardEventDeps = {
    sender: new A2ACodexJobSender(a2a),
    idempotency: new MemoryIdempotencyStore(),
    ...(runStore ? { runs: runStore } : {}),
  }
  console.error(`[a2a-taskboard-dispatcher] consuming stream=${stream} durable=${durable} filter=${filterSubject} self=${selfId}`)
  for await (const msg of messages) {
    try {
      const event = JSON.parse(new TextDecoder().decode(msg.data)) as TaskboardEventEnvelope
      const result = await processTaskboardEvent(event, deps)
      if (result.action === 'sent') {
        replyHandler.track({
      corr: result.message_id,
      job_id: result.job_id,
      event_id: result.event_id,
      task_id: event.scope.task_id,
      agent_run_id: result.agent_run_id,
      target: result.target,
      review_request_id: reviewRequestId(event) ?? undefined,
      review_role_key: reviewRoleKey(event) ?? undefined,
      review_commit_sha: reviewCommitSha(event) ?? undefined,
    })
  }
      console.error(`[a2a-taskboard-dispatcher] ${result.action} event=${result.event_id}${'target' in result ? ` target=${result.target}` : ''}${'reason' in result ? ` reason=${result.reason}` : ''}`)
      if (result.action === 'dispatch_failed') msg.nak()
      else msg.ack()
    } catch (e) {
      console.error(`[a2a-taskboard-dispatcher] message_failed ${errorMessage(e)}`)
      msg.nak()
    }
  }
}

async function ensureConsumer(nc: Awaited<ReturnType<typeof connect>>, stream: string, durable: string, filterSubject: string): Promise<void> {
  const jsm = await nc.jetstreamManager()
  const cfg: any = {
    durable_name: durable,
    filter_subject: filterSubject,
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: 60_000_000_000,
    max_deliver: 10,
  }
  try {
    const info = await jsm.consumers.info(stream, durable)
    if (info.config.filter_subject !== filterSubject) {
      await jsm.consumers.delete(stream, durable).catch(() => {})
      await jsm.consumers.add(stream, cfg)
    }
  } catch (e) {
    if (!/not found|404/i.test(errorMessage(e))) throw e
    await jsm.consumers.add(stream, cfg)
  }
}

function buildTaskPrompt(event: TaskboardEventEnvelope, opts: { agentRunId?: number; taskboardUrl?: string } = {}): string {
  const task = taskPayload(event)
  const taskId = event.scope.task_id
  const title = stringValue(task.title) || `Task ${taskId ?? event.event_id}`
  const description = stringValue(task.description) || ''
  const taskUrl = opts.taskboardUrl && taskId != null ? `${opts.taskboardUrl.replace(/\/+$/, '')}/tasks/${taskId}` : null
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) {
    const review = reviewRequestPayload(event)
    const role = reviewRoleKey(event) ?? '(unknown)'
    return [
      'A taskboard review request was routed to the A2A/Codex fleet.',
      '',
      `Task #${taskId ?? '(unknown)'}`,
      `Title: ${title}`,
      `Review request ID: ${reviewRequestId(event) ?? '(unknown)'}`,
      `Review role: ${role}`,
      reviewCommitSha(event) ? `Commit SHA: ${reviewCommitSha(event)}` : null,
      opts.agentRunId != null ? `Agent run ID: ${opts.agentRunId}` : null,
      taskUrl ? `Taskboard URL: ${taskUrl}` : null,
      `Event ID: ${event.event_id}`,
      `Fire generation: ${event.idempotency.fire_generation ?? '(unknown)'}`,
      '',
      'Review request payload:',
      JSON.stringify(review, null, 2),
      '',
      'Task payload:',
      JSON.stringify(task, null, 2),
      description ? `\nDescription:\n${description}` : null,
      '',
      'Perform a read-only review for the requested role. Record findings, risks, and the recommended verdict clearly. Do not modify files unless the operator sends a separate write-enabled follow-up through an authorized gateway.',
      '',
      'At the end of the response, include one fenced JSON block for taskboard writeback using exactly this shape:',
      '```json',
      JSON.stringify({
        taskboard_review_result: {
          verdict: 'APPROVE',
          commit_sha: reviewCommitSha(event) ?? '<task latest_commit_sha>',
          findings_md: 'Short markdown summary of findings and rationale.',
          findings: [
            { severity: 'info', message: 'Finding summary', file: 'path/to/file', line: 1 },
          ],
        },
      }, null, 2),
      '```',
      'Use verdict APPROVE only when the task is ready to pass this review role. Use REQUEST_CHANGES for blocking findings. Use COMMENT for non-blocking review notes.',
    ].filter(Boolean).join('\n')
  }
  return [
    'A taskboard task moved to In Progress and was routed to the A2A/Codex fleet.',
    '',
    `Task #${taskId ?? '(unknown)'}`,
    `Title: ${title}`,
    opts.agentRunId != null ? `Agent run ID: ${opts.agentRunId}` : null,
    taskUrl ? `Taskboard URL: ${taskUrl}` : null,
    `Event ID: ${event.event_id}`,
    `Fire generation: ${event.idempotency.fire_generation ?? '(unknown)'}`,
    '',
    'Task payload:',
    JSON.stringify(task, null, 2),
    description ? `\nDescription:\n${description}` : null,
    '',
    'Work read-only unless the operator explicitly sends a write-enabled follow-up through an authorized gateway. Record findings and next steps clearly.',
  ].filter(Boolean).join('\n')
}

function roleForTask(event: TaskboardEventEnvelope): string {
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) return reviewRoleKey(event) ?? 'code-reviewer'
  const task = taskPayload(event)
  const agent = `${stringValue(task.agent) ?? ''} ${stringValue(task.task_type) ?? ''}`.toLowerCase()
  if (agent.includes('security')) return 'security-auditor'
  if (agent.includes('review')) return 'code-reviewer'
  if (agent.includes('qa')) return 'qa-agent'
  if (agent.includes('architect')) return 'architect'
  return 'developer'
}

function isTaskboardEvent(value: unknown): value is TaskboardEventEnvelope {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const event = value as Partial<TaskboardEventEnvelope>
  return (
    event.schema === TASKBOARD_EVENT_SCHEMA &&
    typeof event.event_id === 'string' &&
    typeof event.event_type === 'string' &&
    !!event.scope &&
    typeof event.scope === 'object' &&
    !!event.idempotency &&
    typeof event.idempotency === 'object'
  )
}

function payloadRecord(event: TaskboardEventEnvelope): Record<string, unknown> {
  return event.payload && typeof event.payload === 'object' && !Array.isArray(event.payload) ? event.payload as Record<string, unknown> : {}
}

function taskPayload(event: TaskboardEventEnvelope): Record<string, unknown> {
  const task = payloadRecord(event).task
  return task && typeof task === 'object' && !Array.isArray(task) ? task as Record<string, unknown> : {}
}

function reviewRequestPayload(event: TaskboardEventEnvelope): Record<string, unknown> {
  const payload = payloadRecord(event)
  const review = payload.review_request
  return review && typeof review === 'object' && !Array.isArray(review) ? review as Record<string, unknown> : payload
}

function reviewRequestId(event: TaskboardEventEnvelope): number | null {
  const payload = payloadRecord(event)
  const review = reviewRequestPayload(event)
  return numberOrNull(review.id) ?? numberOrNull(payload.review_request_id)
}

function reviewRoleKey(event: TaskboardEventEnvelope): string | null {
  const payload = payloadRecord(event)
  const review = reviewRequestPayload(event)
  return stringValue(review.role_key) ?? stringValue(payload.role_key) ?? stringValue(payload.reviewer_role_key)
}

function reviewerModelForEvent(event: TaskboardEventEnvelope): string | null {
  const payload = payloadRecord(event)
  const review = reviewRequestPayload(event)
  const explicit = stringValue(review.reviewer_model) ?? stringValue(payload.reviewer_model)
  if (explicit) return explicit
  const role = reviewRoleKey(event)
  return role ? process.env[`TASKBOARD_DISPATCH_REVIEW_MODEL_${reviewRoleEnvSuffix(role)}`]?.trim() || null : null
}

function reviewerEndpointForEvent(event: TaskboardEventEnvelope): string | null {
  const payload = payloadRecord(event)
  const review = reviewRequestPayload(event)
  const explicit = stringValue(review.reviewer_endpoint_id) ?? stringValue(payload.reviewer_endpoint_id)
  if (explicit) return explicit
  const role = reviewRoleKey(event)
  return role ? process.env[`TASKBOARD_DISPATCH_REVIEW_ENDPOINT_${reviewRoleEnvSuffix(role)}`]?.trim() || null : null
}

function reviewCommitSha(event: TaskboardEventEnvelope): string | null {
  const payload = payloadRecord(event)
  const review = reviewRequestPayload(event)
  const task = taskPayload(event)
  return (
    stringValue(task.latest_commit_sha) ??
    stringValue(task.commit_sha) ??
    stringValue(review.commit_sha) ??
    stringValue(review.linked_commit_sha) ??
    stringValue(payload.commit_sha) ??
    stringValue(payload.linked_commit_sha)
  )
}

function jobIdForEvent(event: TaskboardEventEnvelope): string {
  const taskId = event.scope.task_id
  const fireGeneration = event.idempotency.fire_generation
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) {
    const requestId = reviewRequestId(event)
    if (taskId != null && requestId != null) return `tb-review-${taskId}-${requestId}`
  }
  return taskId != null && fireGeneration != null ? `tb-${taskId}-${fireGeneration}` : `tb-${safeId(event.event_id)}`
}

function threadKeyForEvent(event: TaskboardEventEnvelope): string {
  const taskId = event.scope.task_id
  if (event.event_type === TASK_REVIEW_REQUESTED_EVENT_TYPE) {
    const role = reviewRoleKey(event)
    const suffix = role ? `:review:${safeId(role)}` : ':review'
    return `taskboard:task:${taskId ?? safeId(event.event_id)}${suffix}`
  }
  return `taskboard:task:${taskId ?? safeId(event.event_id)}`
}

function parseReviewJobId(jobId: string): { taskId: number; requestId: number } | null {
  const match = /^tb-review-(\d+)-(\d+)$/.exec(jobId)
  if (!match) return null
  return { taskId: Number(match[1]), requestId: Number(match[2]) }
}

export function extractTaskboardReviewResult(output: string): Partial<TaskboardReviewSubmission> & { verdict: TaskboardReviewVerdict } | null {
  const candidates: unknown[] = []
  const trimmed = output.trim()
  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    const parsed = parseJsonCandidate(trimmed)
    if (parsed != null) candidates.push(parsed)
  }
  for (const match of output.matchAll(/```(?:json)?\s*([\s\S]*?)```/gi)) {
    const parsed = parseJsonCandidate(match[1].trim())
    if (parsed != null) candidates.push(parsed)
  }
  for (const candidate of candidates) {
    const normalized = normalizeTaskboardReviewResult(candidate)
    if (normalized) return normalized
  }
  return null
}

function parseJsonCandidate(text: string): unknown | null {
  try { return JSON.parse(text) } catch { return null }
}

function normalizeTaskboardReviewResult(value: unknown): Partial<TaskboardReviewSubmission> & { verdict: TaskboardReviewVerdict } | null {
  const root = recordValue(value)
  if (!root) return null
  const nested = recordValue(root.taskboard_review_result) ?? root
  const verdict = normalizeReviewVerdict(nested.verdict)
  if (!verdict) return null
  const commitSha = stringValue(nested.commit_sha) ?? stringValue(nested.linked_commit_sha)
  const findingsMd = stringValue(nested.findings_md) ?? stringValue(nested.summary)
  const findings = Array.isArray(nested.findings)
    ? nested.findings.filter((finding): finding is Record<string, unknown> => !!recordValue(finding))
    : undefined
  return {
    verdict,
    ...(commitSha ? { commit_sha: commitSha } : {}),
    ...(findingsMd ? { findings_md: findingsMd } : {}),
    ...(findings ? { findings } : {}),
  }
}

function normalizeReviewVerdict(value: unknown): TaskboardReviewVerdict | null {
  const raw = typeof value === 'string' ? value.trim().toUpperCase().replace(/[\s-]+/g, '_') : ''
  if (raw === 'APPROVE' || raw === 'APPROVED') return 'APPROVE'
  if (raw === 'REQUEST_CHANGES' || raw === 'CHANGES_REQUESTED' || raw === 'REQUESTED_CHANGES') return 'REQUEST_CHANGES'
  if (raw === 'COMMENT' || raw === 'COMMENT_ONLY' || raw === 'COMMENTS') return 'COMMENT'
  return null
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null
}

function reviewTokenForRole(roleKey: string, tokens: Record<string, string | undefined>): string | null {
  const suffix = reviewRoleEnvSuffix(roleKey)
  return tokens[`TASKBOARD_REVIEW_TOKEN_${suffix}`]?.trim() || tokens[`TASKBOARD_V2_REVIEW_TOKEN_${suffix}`]?.trim() || null
}

function reviewTokensFromEnv(env: Record<string, string | undefined> = process.env): Record<string, string | undefined> {
  const tokens: Record<string, string | undefined> = {}
  for (const [key, value] of Object.entries(env)) {
    if ((key.startsWith('TASKBOARD_REVIEW_TOKEN_') || key.startsWith('TASKBOARD_V2_REVIEW_TOKEN_')) && value?.trim()) {
      tokens[key] = value.trim()
    }
  }
  return tokens
}

function reviewRoleEnvSuffix(roleKey: string): string {
  return roleKey.toUpperCase().replace(/[^A-Z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'UNKNOWN'
}

function normalizeStatus(value: unknown): string {
  return typeof value === 'string' ? value.trim().toLowerCase() : ''
}

function stringValue(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null
}

function numberOrNull(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.trunc(value)
  if (typeof value === 'string' && /^-?\d+$/.test(value.trim())) return Number(value.trim())
  return null
}

function truncateForTaskboard(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value
  return `${value.slice(0, Math.max(0, maxChars - 32))}\n...[truncated ${value.length - maxChars} chars]`
}

function codexFailureClass(error: string): string {
  const e = error.toLowerCase()
  if (e.includes('timeout')) return 'endpoint_timeout'
  if (e.includes('rate')) return 'endpoint_rate_limited'
  if (e.includes('auth') || e.includes('unauthorized')) return 'endpoint_unauthorized'
  if (e.includes('transport') || e.includes('network')) return 'endpoint_transport_drop'
  return 'tool_runtime_exception'
}

function safeId(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9_-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 96) || crypto.randomUUID()
}

function unwrapToolResult(res: any): { ok: boolean; id?: string; error?: string } | null {
  try {
    const text = res?.content?.[0]?.text
    if (typeof text !== 'string') return null
    const parsed = JSON.parse(text)
    return { ok: parsed.ok === true, id: parsed.id, error: parsed.error }
  } catch {
    return null
  }
}

async function safeText(res: Response): Promise<string> {
  try { return await res.text() } catch { return '' }
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

if (import.meta.main) void main().catch((e) => {
  console.error(`[a2a-taskboard-dispatcher] FATAL ${errorMessage(e)}`)
  process.exit(1)
})
