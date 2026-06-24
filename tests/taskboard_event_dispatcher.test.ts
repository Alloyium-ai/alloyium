import { describe, expect, test } from 'bun:test'
import {
  buildCodexReplyComment,
  buildCodexJobRequest,
  classifyCodexJobReply,
  extractTaskboardReviewResult,
  InMemoryTaskboardAgentRunStore,
  routeTaskboardEvent,
  TaskboardReplyHandler,
  TaskboardEventDispatcher,
  type TaskboardEventEnvelope,
} from '../taskboard_event_dispatcher.ts'

function statusEvent(overrides: Partial<TaskboardEventEnvelope> = {}): TaskboardEventEnvelope {
  return {
    schema: 'taskboard.event.v1',
    event_id: 'tb-evt-10481-12',
    event_type: 'task.status_changed',
    occurred_at: '2026-06-19T13:00:00Z',
    source: {
      system: 'openclawdev-taskboard',
      taskboard_url: 'http://taskboard.local',
      db_event_id: 123,
    },
    scope: {
      org_id: 1,
      project_id: 2,
      epic_id: 10021,
      task_id: 10481,
    },
    idempotency: {
      task_id: 10481,
      fire_generation: 12,
      event_id: 'tb-evt-10481-12',
    },
    actor: { type: 'operator', agent: 'User' },
    payload: {
      from_status: 'Backlog',
      to_status: 'In Progress',
      task: {
        id: 10481,
        title: 'Improve ranking model selection',
        description: 'Long taskboard body should stay on taskboard.',
        status: 'In Progress',
        agent: 'Developer',
        agent_id: 'codex-gw-pm',
        task_type: 'Feature',
        priority: 'High',
        source_ref: 'git:example-org/example-repo#branch',
      },
    },
    ...overrides,
  }
}

function reviewRequestEvent(overrides: Partial<TaskboardEventEnvelope> = {}): TaskboardEventEnvelope {
  return {
    schema: 'taskboard.event.v1',
    event_id: 'tb-review-10481-901',
    event_type: 'task.review_requested',
    occurred_at: '2026-06-19T14:00:00Z',
    source: {
      system: 'openclawdev-taskboard',
      taskboard_url: 'http://taskboard.local',
      db_event_id: 456,
    },
    scope: {
      org_id: 1,
      project_id: 2,
      epic_id: 10021,
      task_id: 10481,
    },
    idempotency: {
      task_id: 10481,
      fire_generation: 13,
      event_id: 'tb-review-10481-901',
    },
    actor: { type: 'principal', agent: 'codex-gw-pm' },
    payload: {
      review_request_id: 901,
      role_key: 'code-reviewer',
      review_type: 'code',
      min_approvals: 1,
      review_request: {
        id: 901,
        task_id: 10481,
        workflow_step_id: 88,
        review_type: 'code',
        role_key: 'code-reviewer',
        status: 'active',
      },
      task: {
        id: 10481,
        title: 'Improve ranking model selection',
        description: 'Review the implementation branch.',
        status: 'Review',
        agent: 'Developer',
        agent_id: 'developer-agent',
        task_type: 'Feature',
        priority: 'High',
        latest_commit_sha: 'bed858bb86385d3dd22e73aa4d5c5c0f2b7d0297',
      },
    },
    ...overrides,
  }
}

describe('taskboard event dispatcher routing', () => {
  test('routes only task.status_changed events that move to In Progress', () => {
    const routed = routeTaskboardEvent(statusEvent())
    expect(routed.action).toBe('dispatch')
    if (routed.action !== 'dispatch') throw new Error('expected dispatch route')
    expect(routed.dispatch.task_id).toBe(10481)
    expect(routed.dispatch.fire_generation).toBe(12)

    expect(routeTaskboardEvent(statusEvent({ event_type: 'review.verdict_submitted' }))).toEqual({
      action: 'observe',
      reason: 'unsupported_event_type',
    })
    expect(routeTaskboardEvent({ schema: 'not-taskboard.event.v1' })).toEqual({
      action: 'observe',
      reason: 'unsupported_schema',
    })
  })

  test('routes task.review_requested events to a role-specific reviewer target', () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = 'code-reviewer'
    try {
      const routed = routeTaskboardEvent(reviewRequestEvent(), { defaultAgentId: 'codex-gw-pm' })
      expect(routed.action).toBe('dispatch')
      if (routed.action !== 'dispatch') throw new Error('expected dispatch route')
      expect(routed.dispatch.task_id).toBe(10481)
      expect(routed.dispatch.fire_generation).toBe(13)
      expect(routed.dispatch.target_agent_id).toBe('code-reviewer')
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previous
    }
  })

  test('fails closed when a review request has no reviewer target', () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
    const previousRole = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
    delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    try {
      expect(routeTaskboardEvent(reviewRequestEvent(), { defaultAgentId: 'codex-gw-pm' })).toEqual({
        action: 'observe',
        reason: 'review_target_unconfigured',
      })
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID = previous
      if (previousRole == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previousRole
    }
  })

  test('role-key placeholders still use review target env routing', () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = 'codex-review-gw'
    try {
      const event = reviewRequestEvent({
        payload: {
          ...reviewRequestEvent().payload,
          target_agent_id: 'code-reviewer',
          reviewer_agent_id: 'code-reviewer',
          review_request: {
            ...(reviewRequestEvent().payload as any).review_request,
            agent_id: 'code-reviewer',
            reviewer_agent_id: 'code-reviewer',
          },
        },
      })
      const routed = routeTaskboardEvent(event, { defaultAgentId: 'codex-gw-pm' })
      expect(routed.action).toBe('dispatch')
      if (routed.action !== 'dispatch') throw new Error('expected dispatch route')
      expect(routed.dispatch.target_agent_id).toBe('codex-review-gw')
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previous
    }
  })

  test('role-key placeholders fail closed when review target env is absent', () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
    const previousRole = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
    delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    try {
      const event = reviewRequestEvent({
        payload: {
          ...reviewRequestEvent().payload,
          target_agent_id: 'code-reviewer',
          reviewer_agent_id: 'code-reviewer',
          review_request: {
            ...(reviewRequestEvent().payload as any).review_request,
            agent_id: 'code-reviewer',
            reviewer_agent_id: 'code-reviewer',
          },
        },
      })
      expect(routeTaskboardEvent(event, { defaultAgentId: 'codex-gw-pm' })).toEqual({
        action: 'observe',
        reason: 'review_target_unconfigured',
      })
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID = previous
      if (previousRole == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previousRole
    }
  })


  test('explicit reviewer_agent_id wins over env routing', () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = 'code-reviewer-env'
    try {
      const event = reviewRequestEvent({
        payload: {
          ...reviewRequestEvent().payload,
          reviewer_agent_id: 'code-reviewer-explicit',
        },
      })
      const routed = routeTaskboardEvent(event)
      expect(routed.action).toBe('dispatch')
      if (routed.action !== 'dispatch') throw new Error('expected dispatch route')
      expect(routed.dispatch.target_agent_id).toBe('code-reviewer-explicit')
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previous
    }
  })
})

describe('taskboard Codex job construction', () => {
  test('builds stable read-only codex.job.request.v1 payload with task context', async () => {
    const event = statusEvent()
    const route = routeTaskboardEvent(event)
    if (route.action !== 'dispatch') throw new Error('expected dispatch route')

    const job = buildCodexJobRequest(route.dispatch, { cwd: '/srv/git', agentRunId: 55 })
    expect(job).toMatchObject({
      schema: 'codex.job.request.v1',
      job_id: 'tb-10481-12',
      thread_key: 'taskboard:task:10481',
      cwd: '/srv/git',
      sandbox: 'read-only',
      approval_policy: 'never',
      task_id: 10481,
      agent_run_id: 55,
      event_id: 'tb-evt-10481-12',
      taskboard_url: 'http://taskboard.local',
      task_context: {
        taskboard_url: 'http://taskboard.local',
        task_id: 10481,
        agent_run_id: 55,
        event_id: 'tb-evt-10481-12',
        fire_generation: 12,
        title: 'Improve ranking model selection',
        agent_id: 'codex-gw-pm',
      },
    })
    expect(job.input[0].text).toContain('Task #10481')
    expect(job.input[0].text).toContain('tb-evt-10481-12')

    const sent: any[] = []
    const dispatcher = new TaskboardEventDispatcher({
      cwd: '/srv/git',
      hooks: {
        prepareAgentRun: () => ({ action: 'dispatch', agent_run_id: 55 }),
      },
      sendJob: (input) => {
        sent.push(input)
        return { ok: true, id: 'a2a-msg-1' }
      },
    })
    const result = await dispatcher.handle(event)
    expect(result.action).toBe('dispatched')
    expect(sent).toHaveLength(1)
    expect(sent[0].target_agent_id).toBe('codex-gw-pm')
    expect(sent[0].job).toMatchObject({ job_id: 'tb-10481-12', agent_run_id: 55 })
  })

  test('uses configured default target when task.agent_id is absent', async () => {
    const event = statusEvent({
      payload: {
        from_status: 'Backlog',
        to_status: 'In Progress',
        task: { id: 10481, title: 'No explicit agent', status: 'In Progress' },
      },
    })
    const sent: any[] = []
    const dispatcher = new TaskboardEventDispatcher({
      defaultAgentId: 'codex-default-gw',
      hooks: { prepareAgentRun: () => ({ action: 'dispatch' }) },
      sendJob: (input) => {
        sent.push(input)
        return { ok: true }
      },
    })
    await dispatcher.handle(event)
    expect(sent[0].target_agent_id).toBe('codex-default-gw')
  })

  test('builds review-request jobs with review context and a role-specific thread', async () => {
    const previous = process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
    process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = 'code-reviewer'
    try {
      const event = reviewRequestEvent({
        payload: {
          ...reviewRequestEvent().payload,
          reviewer_model: 'openai-codex/gpt-5.3-codex',
          reviewer_endpoint_id: 'codex-gw',
        },
      })
      const route = routeTaskboardEvent(event, { defaultAgentId: 'codex-gw-pm' })
      if (route.action !== 'dispatch') throw new Error('expected dispatch route')

      const job = buildCodexJobRequest(route.dispatch, { cwd: '/srv/git', agentRunId: 56 })
      expect(job).toMatchObject({
        schema: 'codex.job.request.v1',
        job_id: 'tb-review-10481-901',
        thread_key: 'taskboard:task:10481:review:code-reviewer',
        cwd: '/srv/git',
        sandbox: 'read-only',
        approval_policy: 'never',
        task_id: 10481,
        agent_run_id: 56,
        event_id: 'tb-review-10481-901',
        task_context: {
          task_id: 10481,
          agent_run_id: 56,
          event_id: 'tb-review-10481-901',
          event_type: 'task.review_requested',
          fire_generation: 13,
          review_request_id: 901,
          review_role_key: 'code-reviewer',
          reviewer_model: 'openai-codex/gpt-5.3-codex',
          reviewer_endpoint_id: 'codex-gw',
        },
        model: 'openai-codex/gpt-5.3-codex',
        endpoint_id: 'codex-gw',
      })
      expect(job.input[0].text).toContain('A taskboard review request was routed')
      expect(job.input[0].text).toContain('Review request ID: 901')
      expect(job.input[0].text).toContain('Review role: code-reviewer')
      expect(job.input[0].text).toContain('taskboard_review_result')
      expect(job.input[0].text).toContain('bed858bb86385d3dd22e73aa4d5c5c0f2b7d0297')
    } finally {
      if (previous == null) delete process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER
      else process.env.TASKBOARD_DISPATCH_REVIEW_AGENT_ID_CODE_REVIEWER = previous
    }
  })
})

describe('taskboard dispatcher idempotency', () => {
  test('duplicate event is an idempotent no-op and does not send twice', async () => {
    const sent: any[] = []
    const dispatcher = new TaskboardEventDispatcher({
      hooks: new InMemoryTaskboardAgentRunStore(() => 'run-1'),
      sendJob: (input) => {
        sent.push(input)
        return { ok: true }
      },
    })

    const first = await dispatcher.handle(statusEvent())
    const second = await dispatcher.handle(statusEvent())

    expect(first.action).toBe('dispatched')
    expect(second).toMatchObject({
      action: 'duplicate',
      ack: true,
      reason: 'duplicate',
      event_id: 'tb-evt-10481-12',
      task_id: 10481,
      agent_run_id: 'run-1',
    })
    expect(sent).toHaveLength(1)
  })

  test('non-In-Progress status changes are observed and never dispatched', async () => {
    const event = statusEvent({
      event_id: 'tb-evt-10481-13',
      idempotency: { task_id: 10481, fire_generation: 13, event_id: 'tb-evt-10481-13' },
      payload: {
        from_status: 'In Progress',
        to_status: 'Done',
        task: { id: 10481, status: 'Done', agent_id: 'codex-gw-pm' },
      },
    })
    let prepareCalls = 0
    let sendCalls = 0
    const dispatcher = new TaskboardEventDispatcher({
      hooks: {
        prepareAgentRun: () => {
          prepareCalls++
          return { action: 'dispatch' }
        },
      },
      sendJob: () => {
        sendCalls++
        return { ok: true }
      },
    })

    const result = await dispatcher.handle(event)
    expect(result).toMatchObject({
      action: 'observed',
      ack: true,
      reason: 'status_not_in_progress',
      event_id: 'tb-evt-10481-13',
    })
    expect(prepareCalls).toBe(0)
    expect(sendCalls).toBe(0)
  })
})

describe('taskboard dispatcher codex reply handling', () => {
  const attrs = { feed: 'a2a', kind: 'direct', type: 'reply', corr: 'a2a-msg-1', from: 'codex-gw-pm', to: 'taskboard-dispatcher' }

  test('classifies codex completed/failed/rejected replies and ignores unrelated traffic', () => {
    expect(classifyCodexJobReply(JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-10481-12', status: 'completed', output: 'done' }), attrs))
      .toMatchObject({ kind: 'completed', job_id: 'tb-10481-12', corr: 'a2a-msg-1', output: 'done' })
    expect(classifyCodexJobReply(JSON.stringify({ schema: 'codex.job.failed.v1', job_id: 'tb-10481-12', error: 'turn_timeout' }), attrs))
      .toMatchObject({ kind: 'failed', error: 'turn_timeout' })
    expect(classifyCodexJobReply(JSON.stringify({ schema: 'codex.job.rejected.v1', job_id: 'tb-10481-12', reason: 'budget-shed' }), attrs))
      .toMatchObject({ kind: 'rejected', reason: 'budget-shed' })
    expect(classifyCodexJobReply('not-json', attrs)).toMatchObject({ kind: 'ignore', reason: 'bad_json' })
    expect(classifyCodexJobReply(JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-10481-12' }), { ...attrs, type: 'msg' }))
      .toMatchObject({ kind: 'ignore', reason: 'not_a2a_reply' })
  })

  test('completed reply transitions run through running to succeeded and posts a task comment', async () => {
    const updates: any[] = []
    const comments: any[] = []
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update(runId, update) { updates.push({ runId, update }) },
        async postComment(taskId, agent, content) { comments.push({ taskId, agent, content }) },
      },
    })
    handler.track({ corr: 'a2a-msg-1', job_id: 'tb-10481-12', event_id: 'tb-evt-10481-12', task_id: 10481, agent_run_id: 55, target: 'codex-gw-pm' })

    const result = await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-10481-12', status: 'completed', output: 'model review complete' }),
      attrs,
    )

    expect(result).toMatchObject({ action: 'updated', run_id: 55, reply: { kind: 'completed' } })
    expect(updates).toEqual([
      { runId: 55, update: { status: 'running', session_id: 'tb-10481-12' } },
      { runId: 55, update: { status: 'succeeded' } },
    ])
    expect(comments).toHaveLength(1)
    expect(comments[0]).toMatchObject({ taskId: 10481, agent: 'taskboard-dispatcher' })
    expect(comments[0].content).toContain('model review complete')
  })

  test('completed review reply submits a structured V2 review verdict', async () => {
    const updates: any[] = []
    const comments: any[] = []
    const submissions: any[] = []
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update(runId, update) { updates.push({ runId, update }) },
        async postComment(taskId, agent, content) { comments.push({ taskId, agent, content }) },
        async submitReview(reviewRequestId, roleKey, submission) {
          submissions.push({ reviewRequestId, roleKey, submission })
          return { action: 'submitted', verdict: submission.verdict, review_id: 777 }
        },
      },
    })
    handler.track({
      corr: 'a2a-msg-1',
      job_id: 'tb-review-10481-901',
      event_id: 'tb-review-10481-901',
      task_id: 10481,
      agent_run_id: 56,
      target: 'host-ops-gw',
      review_request_id: 901,
      review_role_key: 'code-reviewer',
      review_commit_sha: 'bed858bb86385d3dd22e73aa4d5c5c0f2b7d0297',
    })

    const output = [
      'No blocking findings.',
      '',
      '```json',
      JSON.stringify({
        taskboard_review_result: {
          verdict: 'APPROVE',
          findings_md: 'No blocking findings.',
          findings: [{ severity: 'info', message: 'Reviewed focused diff.' }],
        },
      }, null, 2),
      '```',
    ].join('\n')
    await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-review-10481-901', status: 'completed', output }),
      attrs,
    )

    expect(submissions).toEqual([
      {
        reviewRequestId: 901,
        roleKey: 'code-reviewer',
        submission: {
          verdict: 'APPROVE',
          commit_sha: 'bed858bb86385d3dd22e73aa4d5c5c0f2b7d0297',
          findings_md: 'No blocking findings.',
          findings: [{ severity: 'info', message: 'Reviewed focused diff.' }],
        },
      },
    ])
    expect(updates).toEqual([
      { runId: 56, update: { status: 'running', session_id: 'tb-review-10481-901' } },
      { runId: 56, update: { status: 'succeeded' } },
    ])
    expect(comments[0].content).toContain('Review verdict writeback:')
    expect(comments[0].content).toContain('Status: submitted APPROVE')
    expect(comments[0].content).toContain('Review ID: 777')
  })

  test('completed review reply submits a commit-less verdict (Chore / no-PR) instead of skipping', async () => {
    // #10557 / #10556: when no commit is available from the reply, the pending record, or the API,
    // the verdict for a commit-less task must still be submitted (not silently skipped).
    const comments: any[] = []
    const submissions: any[] = []
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update() {},
        async postComment(taskId, agent, content) { comments.push({ taskId, agent, content }) },
        async submitReview(reviewRequestId, roleKey, submission) {
          submissions.push({ reviewRequestId, roleKey, submission })
          return { action: 'submitted', verdict: submission.verdict, review_id: 778 }
        },
        // No getTaskCommitSha and no review_commit_sha => genuinely commit-less.
      },
    })
    handler.track({
      corr: 'a2a-msg-1',
      job_id: 'tb-review-10481-902',
      event_id: 'tb-review-10481-902',
      task_id: 10481,
      agent_run_id: 57,
      target: 'host-ops-gw',
      review_request_id: 902,
      review_role_key: 'code-reviewer',
    })

    const output = [
      '```json',
      JSON.stringify({ taskboard_review_result: { verdict: 'APPROVE', findings_md: 'Chore verified; no code artifact to diff.' } }),
      '```',
    ].join('\n')
    await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-review-10481-902', status: 'completed', output }),
      attrs,
    )

    expect(submissions).toEqual([
      {
        reviewRequestId: 902,
        roleKey: 'code-reviewer',
        submission: { verdict: 'APPROVE', findings_md: 'Chore verified; no code artifact to diff.', findings: [] },
      },
    ])
    expect(submissions[0].submission.commit_sha).toBeUndefined()
    expect(comments[0].content).toContain('Status: submitted APPROVE')
    expect(comments[0].content).not.toContain('missing commit_sha')
  })

  test('completed review reply resolves a missing commit deterministically via getTaskCommitSha', async () => {
    const fetchCalls: number[] = []
    const submissions: any[] = []
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update() {},
        async postComment() {},
        async getTaskCommitSha(taskId) { fetchCalls.push(taskId); return 'fetchedcommitsha0000000000000000000000aa' },
        async submitReview(reviewRequestId, roleKey, submission) {
          submissions.push({ reviewRequestId, roleKey, submission })
          return { action: 'submitted', verdict: submission.verdict, review_id: 779 }
        },
      },
    })
    handler.track({
      corr: 'a2a-msg-1',
      job_id: 'tb-review-10481-903',
      event_id: 'tb-review-10481-903',
      task_id: 10481,
      agent_run_id: 58,
      target: 'host-ops-gw',
      review_request_id: 903,
      review_role_key: 'code-reviewer',
    })

    const output = [
      '```json',
      JSON.stringify({ taskboard_review_result: { verdict: 'APPROVE', findings_md: 'LGTM.' } }),
      '```',
    ].join('\n')
    await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-review-10481-903', status: 'completed', output }),
      attrs,
    )

    expect(fetchCalls).toEqual([10481])
    expect(submissions[0].submission.commit_sha).toBe('fetchedcommitsha0000000000000000000000aa')
  })

  test('completed review reply without structured JSON is safely skipped', async () => {
    const comments: any[] = []
    let submitCalls = 0
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update() {},
        async postComment(taskId, agent, content) { comments.push({ taskId, agent, content }) },
        async submitReview() {
          submitCalls++
          return { action: 'submitted', verdict: 'COMMENT' }
        },
      },
    })
    handler.track({
      corr: 'a2a-msg-1',
      job_id: 'tb-review-10481-901',
      event_id: 'tb-review-10481-901',
      task_id: 10481,
      agent_run_id: 56,
      target: 'host-ops-gw',
      review_request_id: 901,
      review_role_key: 'code-reviewer',
      review_commit_sha: 'bed858bb86385d3dd22e73aa4d5c5c0f2b7d0297',
    })

    await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.completed.v1', job_id: 'tb-review-10481-901', status: 'completed', output: 'Looks fine, but no machine block.' }),
      attrs,
    )

    expect(submitCalls).toBe(0)
    expect(comments[0].content).toContain('Status: skipped')
    expect(comments[0].content).toContain('missing taskboard_review_result JSON block')
  })

  test('extracts review verdicts from fenced JSON blocks', () => {
    const result = extractTaskboardReviewResult([
      'summary',
      '```json',
      '{"taskboard_review_result":{"verdict":"changes requested","commit_sha":"abc123","findings_md":"Fix it","findings":[{"severity":"high"}]}}',
      '```',
    ].join('\n'))

    expect(result).toEqual({
      verdict: 'REQUEST_CHANGES',
      commit_sha: 'abc123',
      findings_md: 'Fix it',
      findings: [{ severity: 'high' }],
    })
  })

  test('rejected reply marks the run endpoint_failed with failure detail', async () => {
    const updates: any[] = []
    const comments: any[] = []
    const handler = new TaskboardReplyHandler({
      runs: {
        async createOrReuse() { return null },
        async update(runId, update) { updates.push({ runId, update }) },
        async postComment(taskId, agent, content) { comments.push({ taskId, agent, content }) },
      },
    })
    handler.track({ corr: 'a2a-msg-1', job_id: 'tb-10481-12', event_id: 'tb-evt-10481-12', task_id: 10481, agent_run_id: 55, target: 'codex-gw-pm' })

    await handler.handleInbound(
      JSON.stringify({ schema: 'codex.job.rejected.v1', job_id: 'tb-10481-12', reason: 'write-unauthorized', detail: 'approval-policy-not-never' }),
      attrs,
    )

    expect(updates).toEqual([
      { runId: 55, update: { status: 'endpoint_failed', failure_class: 'endpoint_invalid_response', failure_detail: 'write-unauthorized: approval-policy-not-never' } },
    ])
    expect(comments[0].content).toContain('Rejected: write-unauthorized')
  })

  test('completion comments include claim-check result references when output was previewed', () => {
    const comment = buildCodexReplyComment(
      { kind: 'completed', job_id: 'tb-10481-12', status: 'completed', output: 'preview', result_ref: 'alloyium:a2a:blob:x', output_preview: true, msg: {} },
      { job_id: 'tb-10481-12', event_id: 'tb-evt-10481-12', task_id: 10481, agent_run_id: 55, target: 'codex-gw-pm' },
    )
    expect(comment).toContain('Result ref: alloyium:a2a:blob:x')
    expect(comment).toContain('claim-checked')
  })
})
