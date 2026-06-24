import { describe, expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  TASKBOARD_EVENT_SCHEMA,
  computeTaskboardWebhookSignature,
  normalizeTaskboardPayload,
  resolveTaskboardEventBridgeSecret,
} from '../taskboard_event_core.ts'
import { TaskboardEventBridge, type TaskboardEventPublisher, type TaskboardPublishAck } from '../taskboard_event_bridge.ts'

const NOW_MS = Date.parse('2026-06-19T15:00:00.000Z')
const SECRET = 'bridge-secret'

class FakePublisher implements TaskboardEventPublisher {
  ensures = 0
  calls: { subject: string; payload: Uint8Array; msgID: string }[] = []
  effects = new Map<string, { subject: string; envelope: any }>()
  failPublish = false

  async ensureStream(): Promise<void> {
    this.ensures++
  }

  async publish(subject: string, payload: Uint8Array, opts: { msgID: string }): Promise<TaskboardPublishAck> {
    this.calls.push({ subject, payload, msgID: opts.msgID })
    if (this.failPublish) throw new Error('jetstream down')
    const duplicate = this.effects.has(opts.msgID)
    if (!duplicate) this.effects.set(opts.msgID, { subject, envelope: JSON.parse(new TextDecoder().decode(payload)) })
    return { stream: 'TASKBOARD_EVENTS', seq: duplicate ? 1 : this.effects.size, duplicate }
  }
}

function statusPayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    event_id: 'tb-evt-1',
    event_type: 'task.status_changed',
    occurred_at: '2026-06-19T14:59:58Z',
    task_id: 10481,
    fire_generation: 12,
    from_status: 'Backlog',
    to_status: 'In Progress',
    actor: { type: 'operator', agent: 'User', principal_id: null },
    task: {
      id: 10481,
      title: 'Improve ranking model selection',
      description: 'Do the work',
      status: 'In Progress',
      agent: 'Developer',
      agent_id: 'codex-gw-pm',
      task_type: 'Feature',
      priority: 'High',
      org_id: 7,
      project_id: 11,
      epic_id: 10021,
      source_ref: 'git:example-org/example-repo#branch',
    },
    ...overrides,
  }
}

function signedRequest(payload: unknown, opts: { secret?: string; timestamp?: string; deliveryId?: string; eventType?: string; signature?: string } = {}): Request {
  const body = JSON.stringify(payload)
  const timestamp = opts.timestamp ?? String(Math.floor(NOW_MS / 1000))
  const deliveryId = opts.deliveryId ?? 'delivery-1'
  const eventType = opts.eventType ?? (payload as any).event_type ?? 'task.status_changed'
  const signature = opts.signature ?? computeTaskboardWebhookSignature(opts.secret ?? SECRET, timestamp, deliveryId, body)
  return new Request('http://bridge.test/api/webhooks/taskboard', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'X-Taskboard-Timestamp': timestamp,
      'X-Taskboard-Delivery': deliveryId,
      'X-Taskboard-Event': eventType,
      'X-Taskboard-Signature': signature,
    },
    body,
  })
}

async function responseJson(res: Response): Promise<any> {
  return JSON.parse(await res.text())
}

describe('taskboard event core', () => {
  test('normalizes existing status webhook payload into taskboard.event.v1 envelope and subject shape', () => {
    const envelope = normalizeTaskboardPayload(statusPayload(), { taskboardUrl: 'http://taskboard.local' })
    expect(envelope).toMatchObject({
      schema: TASKBOARD_EVENT_SCHEMA,
      event_id: 'tb-evt-1',
      event_type: 'task.status_changed',
      occurred_at: '2026-06-19T14:59:58Z',
      source: { system: 'openclawdev-taskboard', taskboard_url: 'http://taskboard.local', db_event_id: null },
      scope: { org_id: 7, project_id: 11, epic_id: 10021, task_id: 10481 },
      idempotency: { task_id: 10481, fire_generation: 12, event_id: 'tb-evt-1' },
      actor: { type: 'operator', agent: 'User', principal_id: null },
      payload: { from_status: 'Backlog', to_status: 'In Progress' },
    })
    expect((envelope.payload as any).task.agent_id).toBe('codex-gw-pm')
  })

  test('loads HMAC secret from file env when direct env is absent', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'tb-bridge-'))
    try {
      const path = join(dir, 'secret')
      await Bun.write(path, 'file-secret\n')
      await expect(resolveTaskboardEventBridgeSecret({ TASKBOARD_EVENT_BRIDGE_HMAC_SECRET_FILE: path })).resolves.toBe('file-secret')
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  })
})

describe('taskboard event bridge', () => {
  test('valid signed webhook publishes one JetStream event after stream ensure', async () => {
    const publisher = new FakePublisher()
    const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS, taskboardUrl: 'http://taskboard.local' })
    const res = await bridge.handleRequest(signedRequest(statusPayload()))
    const body = await responseJson(res)

    expect(res.status).toBe(202)
    expect(body).toMatchObject({
      ok: true,
      event_id: 'tb-evt-1',
      subject: 'taskboard.v1.org.7.project.11.task.status_changed',
      stream: 'TASKBOARD_EVENTS',
      seq: 1,
      duplicate: false,
    })
    expect(publisher.ensures).toBe(1)
    expect(publisher.calls).toHaveLength(1)
    expect(publisher.calls[0].msgID).toBe('tb-evt-1')
    const published = publisher.effects.get('tb-evt-1')?.envelope
    expect(published).toMatchObject({
      schema: TASKBOARD_EVENT_SCHEMA,
      event_id: 'tb-evt-1',
      scope: { org_id: 7, project_id: 11, task_id: 10481 },
      payload: { task: { id: 10481 }, to_status: 'In Progress' },
    })
  })

  test('invalid HMAC is rejected before publish', async () => {
    const publisher = new FakePublisher()
    const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS })
    const res = await bridge.handleRequest(signedRequest(statusPayload(), { signature: `sha256=${'0'.repeat(64)}` }))

    expect(res.status).toBe(401)
    expect(await responseJson(res)).toMatchObject({ ok: false, error: 'signature_mismatch' })
    expect(publisher.calls).toHaveLength(0)
    expect(publisher.ensures).toBe(0)
  })

  test('stale timestamp is rejected before publish', async () => {
    const publisher = new FakePublisher()
    const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS, maxSkewMs: 300_000 })
    const stale = String(Math.floor((NOW_MS - 301_000) / 1000))
    const res = await bridge.handleRequest(signedRequest(statusPayload(), { timestamp: stale, deliveryId: 'delivery-stale' }))

    expect(res.status).toBe(401)
    expect(await responseJson(res)).toMatchObject({ ok: false, error: 'stale_timestamp' })
    expect(publisher.calls).toHaveLength(0)
  })

  test('default max skew honors TASKBOARD_WEBHOOK_MAX_SKEW_MS', async () => {
    const prior = process.env.TASKBOARD_WEBHOOK_MAX_SKEW_MS
    process.env.TASKBOARD_WEBHOOK_MAX_SKEW_MS = '1000'
    try {
      const publisher = new FakePublisher()
      const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS })
      const staleByEnv = String(Math.floor((NOW_MS - 2_000) / 1000))
      const res = await bridge.handleRequest(signedRequest(statusPayload(), { timestamp: staleByEnv, deliveryId: 'delivery-env-skew' }))

      expect(res.status).toBe(401)
      expect(await responseJson(res)).toMatchObject({ ok: false, error: 'stale_timestamp' })
      expect(publisher.calls).toHaveLength(0)
    } finally {
      if (prior === undefined) delete process.env.TASKBOARD_WEBHOOK_MAX_SKEW_MS
      else process.env.TASKBOARD_WEBHOOK_MAX_SKEW_MS = prior
    }
  })

  test('JetStream publish failure returns non-2xx so taskboard retries', async () => {
    const publisher = new FakePublisher()
    publisher.failPublish = true
    const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS })
    const res = await bridge.handleRequest(signedRequest(statusPayload()))

    expect(res.status).toBe(503)
    expect(await responseJson(res)).toMatchObject({ ok: false, error: 'publish_failed' })
    expect(publisher.calls).toHaveLength(1)
  })

  test('replayed webhook uses the same event_id msgID and creates no duplicate side effect', async () => {
    const publisher = new FakePublisher()
    const bridge = new TaskboardEventBridge({ secret: SECRET, publisher, nowMs: () => NOW_MS })
    const payload = statusPayload()
    const first = await bridge.handleRequest(signedRequest(payload, { deliveryId: 'delivery-a' }))
    const second = await bridge.handleRequest(signedRequest(payload, { deliveryId: 'delivery-b' }))

    expect(first.status).toBe(202)
    expect(second.status).toBe(202)
    expect((await responseJson(second)).duplicate).toBe(true)
    expect(publisher.calls.map((call) => call.msgID)).toEqual(['tb-evt-1', 'tb-evt-1'])
    expect(publisher.effects.size).toBe(1)
  })
})
