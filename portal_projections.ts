import { type PortalEventEnvelope, type PortalEventFilters } from './portal_events.ts'

export type PortalChannelProjection = {
  name: string
  count: number
  last_seq: number
  last_ts: string
  last_from: string
}

export type PortalMessageProjection = {
  id: string
  channel: string
  from: string
  to: string
  type: string
  thread: string | null
  corr: string | null
  ts: string
  body: string
  seq: number
}

export type PortalProjectionSnapshot = {
  last_seq: number
  filters: PortalEventFilters
  channels: PortalChannelProjection[]
  messages: PortalMessageProjection[]
}

export class PortalProjections {
  readonly channels = new Map<string, PortalChannelProjection>()
  readonly messages = new Map<string, PortalMessageProjection[]>()
  lastSeq = 0

  constructor(private readonly cap = 800) {}

  apply(event: PortalEventEnvelope): void {
    this.lastSeq = Math.max(this.lastSeq, event.seq)
    if (event.event_type === 'a2a.message.v1' || event.event_type === 'a2a.stream.delta.v1') this.applyMessage(event)
  }

  applyMany(events: PortalEventEnvelope[]): void {
    for (const event of events) this.apply(event)
  }

  snapshot(filters: PortalEventFilters = {}): PortalProjectionSnapshot {
    const channels = [...this.channels.values()]
      .filter((channel) => !filters.channels?.length || filters.channels.includes(channel.name))
      .sort((a, b) => b.last_seq - a.last_seq)
    const messages = channels.flatMap((channel) => this.messages.get(channel.name) ?? [])
    return {
      last_seq: this.lastSeq,
      filters,
      channels,
      messages: messages.slice(-Math.max(1, Math.trunc(filters.limit ?? 200))),
    }
  }

  private applyMessage(event: PortalEventEnvelope): void {
    const payload = event.payload as Record<string, unknown>
    const channel = typeof payload.channel === 'string' && payload.channel ? payload.channel : event.resource.id
    const id = typeof payload.id === 'string' && payload.id ? payload.id : `${event.seq}`
    const message: PortalMessageProjection = {
      id,
      channel,
      from: typeof payload.from === 'string' ? payload.from : '',
      to: typeof payload.to === 'string' ? payload.to : '',
      type: typeof payload.type === 'string' ? payload.type : 'msg',
      thread: typeof payload.thread === 'string' ? payload.thread : null,
      corr: typeof payload.corr === 'string' ? payload.corr : null,
      ts: typeof payload.ts === 'string' ? payload.ts : event.ts,
      body: typeof payload.body === 'string' ? payload.body : JSON.stringify(payload.body ?? ''),
      seq: event.seq,
    }
    const bucket = this.messages.get(channel) ?? []
    if (!bucket.some((m) => m.id === message.id)) bucket.push(message)
    this.messages.set(channel, bucket.slice(-this.cap))
    this.channels.set(channel, {
      name: channel,
      count: this.messages.get(channel)?.length ?? 0,
      last_seq: event.seq,
      last_ts: event.ts,
      last_from: message.from,
    })
  }
}

export function buildPortalProjection(events: PortalEventEnvelope[], cap = 800): PortalProjections {
  const projections = new PortalProjections(cap)
  projections.applyMany(events)
  return projections
}
