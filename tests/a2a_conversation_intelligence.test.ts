import { describe, expect, test } from 'bun:test'
import { ACIStore } from '../a2a_conversation_intelligence.ts'
import { A2AInboxStore } from '../a2a_inbox_store.ts'

const baseEnv = (id: string, over: Record<string, unknown> = {}) => ({
  v: 1,
  id,
  from: 'agent-a',
  to: 'agent-b',
  type: 'request',
  thread: 'thread-1',
  ts: '2026-06-27T13:00:00.000Z',
  body: JSON.stringify({
    schema: 'codex.job.request.v1',
    job_id: 'job-1',
    thread_key: 'taskboard:task:10702',
    stream_topic: 'stream-job-1',
    task_id: 10702,
    input: [{ text: 'sensitive prompt must not be exposed by ACI APIs' }],
  }),
  alg: 'ed25519',
  sig: 'sig',
  ...over,
})

describe('ACIStore', () => {
  test('creates only additive aci tables and stores metadata-only timelines', () => {
    const store = new ACIStore(':memory:')
    const tables = (store.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all() as any[])
      .map(r => r.name)
    expect(tables).toContain('aci_message_event')
    expect(tables).toContain('aci_raw_envelope')
    expect(tables).not.toContain('message')
    expect(tables).not.toContain('schema_migrations')

    store.ingestA2AEnvelope({
      envelope: baseEnv('env-1'),
      rawEnvelope: JSON.stringify(baseEnv('env-1')),
      subject: 'alloyium.a2a.agent.agent-b.inbox',
      sourceStream: 'test',
      routeKind: 'direct',
      observedAt: '2026-06-27T13:00:01.000Z',
    })

    const timeline = store.timeline({ limit: 10 }) as any
    expect(timeline.events).toHaveLength(1)
    expect(JSON.stringify(timeline)).not.toContain('sensitive prompt')
    expect(timeline.events[0]).toMatchObject({
      from_agent: 'agent-a',
      to_agent: 'agent-b',
      body_schema: 'codex.job.request.v1',
      job_id: 'job-1',
      task_id: 10702,
      thread_key: 'taskboard:task:10702',
    })
    store.close()
  })

  test('ingests duplicate direct messages idempotently and builds graph edges', () => {
    const store = new ACIStore(':memory:')
    const env = baseEnv('env-dupe')
    const first = store.ingestA2AEnvelope({
      envelope: env,
      rawEnvelope: JSON.stringify(env),
      subject: 'alloyium.a2a.agent.agent-b.inbox',
      observedAt: '2026-06-27T13:00:01.000Z',
    })
    const second = store.ingestA2AEnvelope({
      envelope: env,
      rawEnvelope: JSON.stringify(env),
      subject: 'alloyium.a2a.agent.agent-b.inbox',
      observedAt: '2026-06-27T13:00:02.000Z',
    })

    expect(first.inserted).toBe(true)
    expect(second.inserted).toBe(false)
    expect((store.summary() as any).events).toBe(1)
    const graph = store.graph() as any
    expect(graph.edges.find((e: any) => e.edge_kind === 'a2a_request')).toMatchObject({
      from_node_id: 'agent-a',
      to_node_id: 'agent-b',
      count: 1,
    })
    store.close()
  })

  test('records topic publishes and malformed raw envelopes without body exposure', () => {
    const store = new ACIStore(':memory:')
    const topicEnv = baseEnv('topic-env', {
      to: 'topic:ops',
      type: 'msg',
      body: JSON.stringify({ schema: 'ops.status.v1', status: 'green', output: 'do not expose' }),
    })
    store.ingestA2AEnvelope({
      envelope: topicEnv,
      rawEnvelope: JSON.stringify(topicEnv),
      subject: 'alloyium.a2a.topic.ops',
      routeKind: 'topic',
    })
    store.ingestRawEnvelope({
      rawEnvelope: '{"id":',
      subject: 'alloyium.a2a.topic.ops',
      routeKind: 'topic',
    })

    const graph = store.graph() as any
    expect(graph.edges.find((e: any) => e.edge_kind === 'a2a_topic_publish')).toMatchObject({
      to_node_type: 'topic',
      to_node_id: 'ops',
    })
    const timeline = store.timeline({ topic: 'ops', limit: 10 }) as any
    expect(timeline.events.some((e: any) => e.route_status === 'malformed_json')).toBe(true)
    expect(JSON.stringify(timeline)).not.toContain('do not expose')
    store.close()
  })

  test('drops unsafe metadata values while keeping schema-derived status', () => {
    const store = new ACIStore(':memory:')
    const env = baseEnv('unsafe-meta', {
      body: JSON.stringify({
        schema: 'codex.job.completed.v1',
        job_id: 'j'.repeat(300),
        result_ref: 'line\nbreak',
        status: 's'.repeat(300),
        output: 'do not expose this body',
      }),
    })
    store.ingestA2AEnvelope({
      envelope: env,
      rawEnvelope: JSON.stringify(env),
      subject: 'alloyium.a2a.agent.agent-b.inbox',
    })

    const timeline = store.timeline({ limit: 1 }) as any
    expect(timeline.events[0].job_id).toBeNull()
    expect(timeline.events[0].result_ref).toBeNull()
    expect(timeline.events[0].status).toBe('completed')
    expect(JSON.stringify(timeline)).not.toContain('jjjjjj')
    expect(JSON.stringify(timeline)).not.toContain('line\\nbreak')
    expect(JSON.stringify(timeline)).not.toContain('do not expose')
    store.close()
  })

  test('correlates job lifecycle replies by job_id and corr', () => {
    const store = new ACIStore(':memory:')
    const request = baseEnv('req-1', { corr: undefined })
    const accepted = baseEnv('rep-1', {
      from: 'agent-b',
      to: 'agent-a',
      type: 'reply',
      corr: 'req-1',
      body: JSON.stringify({
        schema: 'codex.job.accepted.v1',
        job_id: 'job-1',
        stream_topic: 'stream-job-1',
      }),
      ts: '2026-06-27T13:00:02.000Z',
    })
    const completed = baseEnv('rep-2', {
      from: 'agent-b',
      to: 'agent-a',
      type: 'reply',
      corr: 'req-1',
      body: JSON.stringify({
        schema: 'codex.job.completed.v1',
        job_id: 'job-1',
        status: 'completed',
        output: 'raw output must not be exposed',
        result_ref: 'alloyium:a2a:blob:abc',
      }),
      ts: '2026-06-27T13:00:03.000Z',
    })

    store.ingestA2AEnvelope({ envelope: request, rawEnvelope: JSON.stringify(request), subject: 'alloyium.a2a.agent.agent-b.inbox' })
    store.ingestA2AEnvelope({ envelope: accepted, rawEnvelope: JSON.stringify(accepted), subject: 'alloyium.a2a.agent.agent-a.inbox' })
    store.ingestA2AEnvelope({ envelope: completed, rawEnvelope: JSON.stringify(completed), subject: 'alloyium.a2a.agent.agent-a.inbox' })

    const jobs = store.jobs({ jobId: 'job-1' }) as any
    expect(jobs.jobs[0]).toMatchObject({
      job_id: 'job-1',
      requester: 'agent-a',
      assignee: 'agent-b',
      status: 'completed',
      terminal_event_id: expect.any(String),
    })
    const timeline = store.timeline({ jobId: 'job-1' }) as any
    expect(timeline.events).toHaveLength(3)
    expect(JSON.stringify(timeline)).not.toContain('raw output')
    store.close()
  })

  test('backfills existing a2a_inbox_message rows', () => {
    const inbox = new A2AInboxStore(':memory:')
    inbox.store({
      recipient: 'agent-b',
      subject: 'alloyium.a2a.agent.agent-b.inbox',
      envelope: baseEnv('backfill-1') as any,
      deliveredAt: '2026-06-27T13:05:00.000Z',
    })

    const store = new ACIStore(':memory:')
    const rows = inbox.list({ recipient: 'agent-b' }).messages
    expect(store.backfillFromInboxRows(rows)).toEqual({ scanned: 1, ingested: 1 })
    expect((store.summary() as any).events).toBe(1)
    expect((store.timeline({ limit: 1 }) as any).events[0].env_id).toBe('backfill-1')

    inbox.close()
    store.close()
  })
})
