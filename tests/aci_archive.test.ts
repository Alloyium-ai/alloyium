import { describe, expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { A2AInboxStore } from '../a2a_inbox_store.ts'
import { AciArchiveStore } from '../aci_archive.ts'

const baseEnv = (id: string, over: Record<string, unknown> = {}) => ({
  id,
  from: 'agent-a',
  to: 'agent-b',
  type: 'request',
  ts: '2026-06-27T12:00:00.000Z',
  thread: 'thread-1',
  body: JSON.stringify({
    schema: 'codex.job.request.v1',
    job_id: 'job-1',
    thread_key: 'portal:chat:a2a-portal:agent-b',
    stream_topic: 'codex.job.job-1',
    task_id: '10704',
    agent_run_id: '426',
    secret_like_field: 'do-not-index-this-body-value',
  }),
  ...over,
})

describe('AciArchiveStore', () => {
  test('ingests A2A envelopes into metadata-only timeline, graph, matrix, and ego views', () => {
    const store = new AciArchiveStore(':memory:')
    store.ingestEnvelope({
      env: baseEnv('env-1'),
      observedAt: '2026-06-27T12:00:01.000Z',
      trustStatus: 'verified',
    })
    store.ingestEnvelope({
      env: {
        ...baseEnv('env-2'),
        from: 'agent-b',
        to: 'agent-a',
        type: 'reply',
        corr: 'env-1',
        ts: '2026-06-27T12:00:02.000Z',
        body: JSON.stringify({
          schema: 'codex.job.completed.v1',
          job_id: 'job-1',
          status: 'completed',
          result_ref: 'redis://claim-check/job-1',
          output: 'raw output must not be in query responses',
        }),
      },
      observedAt: '2026-06-27T12:00:03.000Z',
      trustStatus: 'verified',
    })

    const timeline = store.timeline({ order: 'asc' })
    expect(timeline.events).toHaveLength(2)
    expect(timeline.events[0].schema).toBe('codex.job.request.v1')
    expect(timeline.events[0].job_id).toBe('job-1')
    expect(timeline.events[0].body_available).toBe(true)
    expect(timeline.events[0].body_redaction_state).toBe('metadata_only')
    expect(timeline.events[0].can_view_body).toBe(false)
    expect(JSON.stringify(timeline)).not.toContain('do-not-index-this-body-value')
    expect(JSON.stringify(timeline)).not.toContain('raw output must not be in query responses')

    const graph = store.graph()
    expect(graph.nodes.map(n => n.id)).toContain('agent-a')
    expect(graph.nodes.map(n => n.id)).toContain('agent-b')
    expect(graph.nodes.map(n => n.id)).toContain('job:job-1')
    expect(graph.edges.some(e => e.from === 'agent-a' && e.to === 'agent-b' && e.kind === 'direct')).toBe(true)

    const matrix = store.matrix()
    expect(matrix.rows.map(n => n.id)).toContain('agent-a')
    expect(matrix.cols.map(n => n.id)).toContain('agent-b')
    expect(matrix.cells.some(c => c.row === 'agent-a' && c.col === 'agent-b' && c.count === 1)).toBe(true)

    const ego = store.ego('agent-b')
    expect(ego.events.map(e => e.event_id)).toContain('env-1')
    expect(ego.recent_jobs).toContain('job-1')
    store.close()
  })

  test('backfills existing inbox rows idempotently without exposing raw bodies', () => {
    const db = new Database(':memory:')
    const inbox = new A2AInboxStore(db)
    inbox.store({
      recipient: 'agent-b',
      envelope: baseEnv('env-backfill', {
        body: JSON.stringify({
          schema: 'codex.job.failed.v1',
          job_id: 'job-failed',
          error: 'credential token abc123 should be classified, not stored',
        }),
      }),
      deliveredAt: '2026-06-27T12:05:00.000Z',
    })
    inbox.ack('agent-b', 'env-backfill', '2026-06-27T12:06:00.000Z')

    const archive = new AciArchiveStore(db)
    expect(archive.backfillInboxMessages()).toEqual({ scanned: 1, ingested: 1, skipped: 0 })
    expect(archive.backfillInboxMessages()).toEqual({ scanned: 1, ingested: 1, skipped: 0 })
    expect(archive.stats().events).toBe(1)

    const events = archive.timeline().events
    expect(events).toHaveLength(1)
    expect(events[0].trust_status).toBe('legacy_unverified')
    expect(events[0].error_class).toBe('auth')
    expect(JSON.stringify(events)).not.toContain('abc123')
    const delivery = db.prepare(`
      SELECT handled, handled_at FROM aci_message_delivery
      WHERE event_id = ? AND recipient_agent = ?
    `).get('env-backfill', 'agent-b') as { handled: number; handled_at: string }
    expect(delivery.handled).toBe(1)
    expect(delivery.handled_at).toBe('2026-06-27T12:06:00.000Z')
    archive.close()
  })

  test('records topic traffic as first-class graph edges', () => {
    const store = new AciArchiveStore(':memory:')
    store.ingestEnvelope({
      env: {
        ...baseEnv('topic-1'),
        to: 'topic:ops',
        type: 'msg',
        body: JSON.stringify({ schema: 'agent.status.v1', status: 'ok' }),
      },
      subject: 'alloyium.a2a.topic.ops',
      routeKind: 'topic',
      observedAt: '2026-06-27T12:10:00.000Z',
    })
    const matrix = store.matrix()
    expect(matrix.cols.map(n => n.id)).toContain('topic:ops')
    expect(store.timeline({ topics: ['ops'] }).events[0].topic).toBe('ops')
    store.close()
  })
})
