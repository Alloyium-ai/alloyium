#!/usr/bin/env bun
import { connect, credsAuthenticator, nkeyAuthenticator, type NatsConnection, type JetStreamClient } from 'nats'
import {
  DEFAULT_TASKBOARD_WEBHOOK_MAX_SKEW_MS,
  TASKBOARD_EVENT_STREAM,
  TASKBOARD_EVENT_SUBJECT,
  buildTaskboardSubject,
  normalizeTaskboardPayload,
  resolveTaskboardEventBridgeSecret,
  taskboardWebhookHeaders,
  verifyTaskboardWebhookSignature,
  type TaskboardEventEnvelope,
} from './taskboard_event_core.ts'

const enc = new TextEncoder()

export type TaskboardPublishAck = {
  stream?: string
  seq?: number
  duplicate?: boolean
}

export interface TaskboardEventPublisher {
  ensureStream(): Promise<void>
  publish(subject: string, payload: Uint8Array, opts: { msgID: string }): Promise<TaskboardPublishAck>
  close?(): Promise<void>
}

export type TaskboardEventBridgeOptions = {
  secret: string
  publisher: TaskboardEventPublisher
  maxSkewMs?: number
  nowMs?: () => number
  taskboardUrl?: string | null
  defaultOrgId?: number
  defaultProjectId?: number
}

export class TaskboardEventBridge {
  private ready: Promise<void> | null = null
  private maxSkewMs: number
  private nowMs: () => number
  private taskboardUrl: string | null
  private defaultOrgId: number
  private defaultProjectId: number

  constructor(private opts: TaskboardEventBridgeOptions) {
    this.maxSkewMs = opts.maxSkewMs ?? envNumber('TASKBOARD_WEBHOOK_MAX_SKEW_MS', DEFAULT_TASKBOARD_WEBHOOK_MAX_SKEW_MS)
    this.nowMs = opts.nowMs ?? Date.now
    this.taskboardUrl = opts.taskboardUrl ?? process.env.TASKBOARD_URL ?? null
    this.defaultOrgId = opts.defaultOrgId ?? Number(process.env.TASKBOARD_EVENT_DEFAULT_ORG_ID ?? 1)
    this.defaultProjectId = opts.defaultProjectId ?? Number(process.env.TASKBOARD_EVENT_DEFAULT_PROJECT_ID ?? 1)
  }

  ensureReady(): Promise<void> {
    this.ready ??= this.opts.publisher.ensureStream().catch((err) => {
      this.ready = null
      throw err
    })
    return this.ready
  }

  async handleRequest(req: Request): Promise<Response> {
    const url = new URL(req.url)
    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json({ ok: true, service: 'taskboard-event-bridge' })
    }
    if (req.method === 'GET' && url.pathname === '/readyz') {
      try {
        await this.ensureReady()
        return json({ ok: true, stream: TASKBOARD_EVENT_STREAM, subject: TASKBOARD_EVENT_SUBJECT })
      } catch (err) {
        return json({ ok: false, error: 'stream_unavailable', detail: describeError(err) }, { status: 503 })
      }
    }
    if (url.pathname !== '/api/webhooks/taskboard') return json({ ok: false, error: 'not_found' }, { status: 404 })
    if (req.method !== 'POST') return json({ ok: false, error: 'method_not_allowed' }, { status: 405, headers: { allow: 'POST' } })

    const rawBody = new Uint8Array(await req.arrayBuffer())
    const headers = taskboardWebhookHeaders(req.headers)
    if (!headers.eventType?.trim()) return json({ ok: false, error: 'missing_event_header' }, { status: 401 })

    const verified = verifyTaskboardWebhookSignature({
      secret: this.opts.secret,
      signature: headers.signature,
      timestamp: headers.timestamp,
      deliveryId: headers.deliveryId,
      rawBody,
      nowMs: this.nowMs,
      maxSkewMs: this.maxSkewMs,
    })
    if (!verified.ok) return json({ ok: false, error: verified.reason }, { status: verified.status })

    let decoded: unknown
    try {
      decoded = JSON.parse(new TextDecoder().decode(rawBody))
    } catch {
      return json({ ok: false, error: 'bad_json' }, { status: 400 })
    }

    let envelope: TaskboardEventEnvelope
    try {
      envelope = normalizeTaskboardPayload(decoded, {
        taskboardUrl: this.taskboardUrl,
        defaultOrgId: this.defaultOrgId,
        defaultProjectId: this.defaultProjectId,
        nowMs: this.nowMs,
      })
    } catch (err) {
      return json({ ok: false, error: 'bad_payload', detail: describeError(err) }, { status: 400 })
    }
    if (headers.eventType.trim() !== envelope.event_type) {
      return json({ ok: false, error: 'event_type_mismatch' }, { status: 400 })
    }

    const subject = buildTaskboardSubject(envelope)
    try {
      await this.ensureReady()
      const ack = await this.opts.publisher.publish(subject, enc.encode(JSON.stringify(envelope)), { msgID: envelope.event_id })
      return json({ ok: true, event_id: envelope.event_id, subject, stream: ack.stream, seq: ack.seq, duplicate: ack.duplicate === true }, { status: 202 })
    } catch (err) {
      console.error('[taskboard-event-bridge] publish failed', describeError(err))
      return json({ ok: false, error: 'publish_failed', detail: describeError(err) }, { status: 503 })
    }
  }
}

export type NatsTaskboardEventPublisherOptions = {
  natsUrl?: string
  stream?: string
  subject?: string
  publishTimeoutMs?: number
  streamMaxAgeNs?: number
  duplicateWindowNs?: number
  maxMsgSize?: number
}

export class NatsTaskboardEventPublisher implements TaskboardEventPublisher {
  private js: JetStreamClient
  private stream: string
  private subject: string
  private publishTimeoutMs: number
  private streamMaxAgeNs: number
  private duplicateWindowNs: number
  private maxMsgSize: number

  constructor(private nc: NatsConnection, opts: NatsTaskboardEventPublisherOptions = {}) {
    this.js = nc.jetstream()
    this.stream = opts.stream ?? TASKBOARD_EVENT_STREAM
    this.subject = opts.subject ?? TASKBOARD_EVENT_SUBJECT
    this.publishTimeoutMs = opts.publishTimeoutMs ?? Number(process.env.TASKBOARD_EVENT_PUBLISH_TIMEOUT_MS ?? 10_000)
    this.streamMaxAgeNs = opts.streamMaxAgeNs ?? Number(process.env.TASKBOARD_EVENT_STREAM_MAX_AGE_NS ?? 7 * 24 * 60 * 60 * 1_000_000_000)
    this.duplicateWindowNs = opts.duplicateWindowNs ?? Number(process.env.TASKBOARD_EVENT_DUPLICATE_WINDOW_NS ?? 24 * 60 * 60 * 1_000_000_000)
    this.maxMsgSize = opts.maxMsgSize ?? Number(process.env.TASKBOARD_EVENT_MAX_MSG_SIZE ?? 262_144)
  }

  async ensureStream(): Promise<void> {
    const jsm = await this.nc.jetstreamManager()
    try {
      const info: any = await jsm.streams.info(this.stream)
      const subjects = new Set<string>(info.config?.subjects ?? [])
      if (!subjects.has(this.subject)) {
        subjects.add(this.subject)
        await jsm.streams.update(this.stream, { ...info.config, subjects: [...subjects] })
      }
    } catch (err) {
      if (!isNotFound(err)) throw err
      await jsm.streams.add({
        name: this.stream,
        subjects: [this.subject],
        storage: 'file',
        retention: 'limits',
        discard: 'old',
        max_age: this.streamMaxAgeNs,
        max_msg_size: this.maxMsgSize,
        duplicate_window: this.duplicateWindowNs,
      } as any)
    }
  }

  async publish(subject: string, payload: Uint8Array, opts: { msgID: string }): Promise<TaskboardPublishAck> {
    const ack: any = await this.js.publish(subject, payload, { msgID: opts.msgID, timeout: this.publishTimeoutMs } as any)
    return { stream: ack.stream, seq: ack.seq, duplicate: ack.duplicate === true }
  }

  async close(): Promise<void> {
    await this.nc.drain()
  }
}

export async function createNatsTaskboardEventPublisher(opts: NatsTaskboardEventPublisherOptions = {}): Promise<NatsTaskboardEventPublisher> {
  const natsUrl = opts.natsUrl ?? process.env.NATS_URL ?? 'nats://nats:4222'
  const connOpts: any = { servers: natsUrl, name: 'taskboard-event-bridge', reconnect: true, maxReconnectAttempts: -1 }
  const credsPath = process.env.TASKBOARD_EVENT_BRIDGE_NATS_CREDS ?? process.env.NATS_CREDS
  const nkeyPath = process.env.TASKBOARD_EVENT_BRIDGE_NATS_NKEY ?? process.env.NATS_NKEY
  if (nkeyPath) connOpts.authenticator = nkeyAuthenticator(enc.encode((await Bun.file(nkeyPath).text()).trim()))
  else if (credsPath) connOpts.authenticator = credsAuthenticator(await Bun.file(credsPath).bytes())
  const nc = await connect(connOpts)
  return new NatsTaskboardEventPublisher(nc, opts)
}

export async function startTaskboardEventBridge(): Promise<{
  server: ReturnType<typeof Bun.serve>
  bridge: TaskboardEventBridge
  publisher: TaskboardEventPublisher
}> {
  const secret = await resolveTaskboardEventBridgeSecret()
  if (!secret) throw new Error('missing TASKBOARD_EVENT_BRIDGE_HMAC_SECRET or TASKBOARD_EVENT_BRIDGE_HMAC_SECRET_FILE')
  const publisher = await createNatsTaskboardEventPublisher()
  const bridge = new TaskboardEventBridge({ secret, publisher })
  await bridge.ensureReady()
  const server = Bun.serve({
    hostname: process.env.TASKBOARD_EVENT_BRIDGE_HOST ?? '0.0.0.0',
    port: Number(process.env.TASKBOARD_EVENT_BRIDGE_PORT ?? 18889),
    fetch: (req) => bridge.handleRequest(req),
  })
  console.error(`[taskboard-event-bridge] listening on ${server.hostname}:${server.port}`)
  return { server, bridge, publisher }
}

function json(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
  })
}

function isNotFound(err: unknown): boolean {
  const text = describeError(err)
  return /not\s*found|404|stream.*does not exist/i.test(text)
}

function describeError(err: unknown): string {
  if (err instanceof Error) return err.message
  return String(err)
}

function envNumber(name: string, fallback: number): number {
  const n = Number(process.env[name])
  return Number.isFinite(n) && n >= 0 ? n : fallback
}

if (import.meta.main) {
  const runtime = await startTaskboardEventBridge()
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, async () => {
      runtime.server.stop(true)
      await runtime.publisher.close?.().catch(() => {})
      process.exit(0)
    })
  }
}
