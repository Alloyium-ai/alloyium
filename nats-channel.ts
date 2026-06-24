// NATS → agent channel bridge.
//
// Subscribes to NATS subjects (core-NATS and JetStream durable consumers) and
// forwards each message into the session via the injected `inject()` callback.
// The desired subscription set lives in Redis (durable, editable by anything);
// a NATS control subject (plus a periodic self-heal) makes this process re-read
// Redis and diff its live subscriptions. This process is READ-ONLY: its only
// outbound NATS traffic is the JetStream `m.ack()` to the server-assigned
// `$JS.ACK.*` inbox — it never publishes to a data/fire subject, so it can never
// become a fire path no matter what it subscribes to.

import {
  connect,
  DeliverPolicy,
  AckPolicy,
  type NatsConnection,
  type JetStreamClient,
} from 'nats'
import { RedisClient } from 'bun'

// ── config (env-overridable; defaults match docs/ops/nats_message_catalog.md) ──
const NATS_URL = process.env.NATS_URL ?? 'nats://nats:4222'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://redis:6379'
const SUBS_KEY = process.env.SUBS_KEY ?? 'alloyium:subscriptions'
const CONTROL_SUBJECT = process.env.CONTROL_SUBJECT ?? 'alloyium.channels.control'
const REDIS_TIMEOUT_MS = Number(process.env.REDIS_TIMEOUT_MS ?? 2500)
const COUNTERS_MS = Number(process.env.COUNTERS_INTERVAL_MS ?? 60_000)
const SELFHEAL_MS = Number(process.env.SELFHEAL_INTERVAL_MS ?? 30_000)

// ── structured, leveled, stderr-only logger (stdout is the MCP stdio pipe) ──
type Level = 'debug' | 'info' | 'warn' | 'error'
const LEVELS: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 }
const THRESHOLD = LEVELS[(process.env.LOG_LEVEL as Level) ?? 'info'] ?? LEVELS.info

const fmt = (v: unknown): string => {
  if (v == null) return String(v)
  if (typeof v === 'string') return /[\s"=]/.test(v) ? JSON.stringify(v) : v
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}
export function log(level: Level, event: string, fields: Record<string, unknown> = {}): void {
  if (LEVELS[level] < THRESHOLD) return
  const kv = Object.entries(fields)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${k}=${fmt(v)}`)
    .join(' ')
  // console.error → stderr. NEVER console.log here (that is the JSON-RPC pipe).
  console.error(`${new Date().toISOString()} ${level} [nats-channel] ${event}${kv ? ' ' + kv : ''}`)
}
const errFields = (e: any): Record<string, unknown> => ({
  err: e instanceof Error ? e.message : String(e),
  code: e?.code,
  ...(THRESHOLD <= LEVELS.debug && e?.stack ? { stack: e.stack } : {}),
})

export const decode = (u8: Uint8Array): string => new TextDecoder().decode(u8)

// Neutralize the channel-tag delimiter so a message body can't break out of
// <channel ...>…</channel> into free-floating instructions, and cap size so one
// large payload can't flood the context window. Applied at the single inject()
// site (covers every source). Exported for unit testing.
const MAX_BODY = Number(process.env.MAX_BODY_BYTES ?? 8192)
export function sanitizeBody(s: string, max = MAX_BODY): string {
  // Case-insensitive so <CHANNEL>/<​/Channel> variants can't slip past a lenient
  // host parser. Insert a zero-width space after '<' to break the tag delimiter.
  let out = s.replace(/<(\/?)channel/gi, '<​$1channel')
  if (out.length > max) out = out.slice(0, max) + `…[truncated ${out.length - max}B]`
  return out
}

export type Inject = (content: string, attrs: Record<string, string>) => Promise<void>

export type SubSpec = {
  subject: string
  mode: 'core' | 'jetstream'
  // jetstream-only:
  stream?: string
  durable?: string
  filter_subject?: string
  deliver?: 'new' | 'all' // default 'new' — suppresses only PRE-creation history (see note on backlog)
  // optional server-side throttling (protects the context window):
  sample?: number // forward the 1st, then 1 in N
  min_interval_ms?: number // forward at most one per this many ms
  // extra attributes added to the <channel ...> tag (cannot override structural keys):
  attrs?: Record<string, string>
}

// Default advisory subscriptions — EMPTY out of the box. This bridge ships with no
// product-specific subjects: seed an initial set via the NATS_DEFAULT_SUBS env var
// (a JSON array of SubSpec). Defaults are written into Redis only when the key is
// genuinely ABSENT (first run); thereafter edit the Redis key (or publish to the
// control subject) to change the live set without a redeploy. Prefer sparse,
// human-meaningful Tier-A subjects (and throttle any high-volume feed) so the
// advisory plane never floods the context window.
export function parseDefaultSubs(raw: string | undefined): SubSpec[] {
  if (!raw || !raw.trim()) return []
  try {
    const data = JSON.parse(raw)
    return Array.isArray(data) ? (data as SubSpec[]) : []
  } catch {
    return []
  }
}

export const DEFAULT_SUBS: SubSpec[] = parseDefaultSubs(process.env.NATS_DEFAULT_SUBS)

// Stable identity for diffing. Intentionally ignores throttle/attrs/filter so a
// spec EDIT keeps the same id; reload() detects edits via specChanged() and
// rebuilds the sub. (Do not fold mutable config in here — that would make every
// edit look like a different subscription.)
export const idOf = (s: SubSpec): string => `${s.mode}:${s.subject}:${s.durable ?? ''}`
const specChanged = (a: SubSpec, b: SubSpec): boolean => JSON.stringify(a) !== JSON.stringify(b)

// A per-subscription throttle. `now` is injectable for deterministic tests.
// Semantics: forward the FIRST message, then 1-in-`sample`; and at most one per
// `min_interval_ms`. Returns true to forward.
export function makeGate(spec: SubSpec, now: () => number = Date.now): () => boolean {
  let count = 0
  let last = 0
  return () => {
    if (spec.sample && spec.sample > 1) {
      const pass = count % spec.sample === 0
      count++
      if (!pass) return false
    }
    if (spec.min_interval_ms) {
      const t = now()
      if (last !== 0 && t - last < spec.min_interval_ms) return false
      last = t
    }
    return true
  }
}

// Build the <channel ...> tag attributes. Claude Code already stamps
// source="alloyium" (the MCP server name) on every event, so we use `feed` to
// mark the sub-source rather than a second `source` attribute. Structural keys
// (feed/subject/mode/stream) are written AFTER spec.attrs so operator-supplied
// attrs can NEVER spoof them. Exported for unit testing.
export function buildAttrs(spec: SubSpec, subject: string): Record<string, string> {
  return {
    ...(spec.attrs ?? {}),
    feed: 'nats',
    subject,
    mode: spec.mode,
    ...(spec.stream ? { stream: spec.stream } : {}),
  }
}

// Parse the Redis value into specs. Throws on malformed JSON or non-array so the
// caller can distinguish "bad config" from "redis down" and keep current subs.
export function parseSpecs(raw: string): SubSpec[] {
  const data = JSON.parse(raw)
  if (!Array.isArray(data)) throw new Error('subscriptions value must be a JSON array')
  return data as SubSpec[]
}

// Validate/clean specs: drop malformed ones, require stream+durable for
// jetstream, and reject duplicate stream:durable (which would silently steal a
// cursor). Returns the usable specs plus human-readable rejection reasons.
export function validateSpecs(specs: SubSpec[]): { valid: SubSpec[]; errors: string[] } {
  const valid: SubSpec[] = []
  const errors: string[] = []
  const durables = new Set<string>()
  for (const s of specs) {
    if (!s || typeof s.subject !== 'string' || (s.mode !== 'core' && s.mode !== 'jetstream')) {
      errors.push(`malformed spec: ${JSON.stringify(s)}`)
      continue
    }
    // S2: the alloyium.a2a.* namespace is owned by the A2A bus (a2a-channel.ts),
    // which has its own attr contract. The read-only advisory plane must never
    // bind there, or the two feeds' <channel> attributes would blur.
    if (s.subject.startsWith('alloyium.a2a.')) {
      errors.push(`alloyium.a2a.* is A2A-bus-owned, not an advisory subject: ${s.subject}`)
      continue
    }
    if (s.mode === 'jetstream') {
      if (!s.stream || !s.durable) {
        errors.push(`jetstream spec needs stream+durable: ${s.subject}`)
        continue
      }
      const key = `${s.stream}:${s.durable}`
      if (durables.has(key)) {
        errors.push(`duplicate durable ${key} (subject ${s.subject})`)
        continue
      }
      durables.add(key)
    }
    valid.push(s)
  }
  return { valid, errors }
}

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((_, rej) => {
    t = setTimeout(() => rej(new Error(`${label} timed out after ${ms}ms`)), ms)
  })
  return Promise.race([p.finally(() => clearTimeout(t)), timeout])
}

const isNotFound = (e: any): boolean => /not found|404/i.test(String(e?.message ?? e))

type Counters = { fwd: number; drop: number; err: number }
type ActiveSub = {
  spec: SubSpec
  counters: Counters
  stop: () => void
  stream?: string
  durable?: string
}

type LoadResult =
  | { ok: true; specs: SubSpec[]; seeded?: boolean; errors?: string[] }
  | { ok: false; reason: 'redis_unavailable' | 'bad_json' | 'key_vanished'; error: unknown }

export type NatsChannelOpts = {
  natsUrl?: string
  redisUrl?: string
  subsKey?: string
  controlSubject?: string
  defaults?: SubSpec[]
  deleteOnDrop?: boolean
}

export class NatsChannel {
  private nc!: NatsConnection
  private js!: JetStreamClient
  private redis!: RedisClient
  private active = new Map<string, ActiveSub>()
  private chain: Promise<void> = Promise.resolve()
  private reloadQueued = false
  private everLoaded = false
  private timers: ReturnType<typeof setInterval>[] = []
  private stopped = false

  private natsUrl: string
  private redisUrl: string
  private subsKey: string
  private controlSubject: string
  private defaults: SubSpec[]
  private deleteOnDrop: boolean

  constructor(private inject: Inject, opts: NatsChannelOpts = {}) {
    this.natsUrl = opts.natsUrl ?? NATS_URL
    this.redisUrl = opts.redisUrl ?? REDIS_URL
    this.subsKey = opts.subsKey ?? SUBS_KEY
    this.controlSubject = opts.controlSubject ?? CONTROL_SUBJECT
    this.defaults = opts.defaults ?? DEFAULT_SUBS
    this.deleteOnDrop = opts.deleteOnDrop ?? process.env.JS_DELETE_ON_DROP !== '0'
  }

  async start(): Promise<void> {
    log('info', 'startup', {
      nats: this.natsUrl, redis: this.redisUrl, subs_key: this.subsKey,
      control: this.controlSubject, level: process.env.LOG_LEVEL ?? 'info',
    })
    // Retry the INITIAL connect so a not-yet-up NATS at boot doesn't kill us;
    // nats.js handles reconnects internally once connected.
    for (;;) {
      try {
        this.nc = await connect({ servers: this.natsUrl, name: 'alloyium', reconnect: true, maxReconnectAttempts: -1 })
        break
      } catch (e) {
        log('warn', 'nats_connect_failed', { ...errFields(e), retry_in_s: 5 })
        await Bun.sleep(5000)
      }
    }
    this.js = this.nc.jetstream()
    this.redis = new RedisClient(this.redisUrl)
    log('info', 'nats_connected', { url: this.natsUrl })

    await this.reload()
    this.watchControl()
    this.watchConnection()
    this.timers.push(setInterval(() => this.flushCounters(), COUNTERS_MS))
    this.timers.push(setInterval(() => this.reload(), SELFHEAL_MS)) // recreates self-removed dead subs
  }

  /** Read desired specs from Redis. Distinguishes: key absent (seed defaults,
   *  first run only) vs malformed JSON vs Redis unreachable — so reload() can
   *  keep current subs instead of silently reverting to built-in defaults. */
  private async loadSpecs(): Promise<LoadResult> {
    let raw: string | null
    try {
      raw = await withTimeout(this.redis.get(this.subsKey), REDIS_TIMEOUT_MS, 'redis.get')
    } catch (e) {
      return { ok: false, reason: 'redis_unavailable', error: e }
    }
    if (raw == null) {
      // Seed defaults ONLY on a genuine first run. After we've ever loaded real
      // config, a null key means Redis eviction/TTL/FLUSHDB — re-seeding would
      // silently rebind production defaults, so keep current subs instead.
      if (this.everLoaded) return { ok: false, reason: 'key_vanished', error: new Error('subscriptions key absent after prior load') }
      try { await this.redis.set(this.subsKey, JSON.stringify(this.defaults, null, 2)) } catch (e) { log('warn', 'seed_write_failed', errFields(e)) }
      this.everLoaded = true
      return { ok: true, specs: this.defaults, seeded: true }
    }
    let parsed: SubSpec[]
    try {
      parsed = parseSpecs(raw)
    } catch (e) {
      return { ok: false, reason: 'bad_json', error: e }
    }
    const { valid, errors } = validateSpecs(parsed)
    this.everLoaded = true
    return { ok: true, specs: valid, errors }
  }

  /** Public entry — serialize every reload trigger (startup, control, self-heal,
   *  manual) through one chain so two passes can't interleave add/drop. */
  reload(): Promise<void> {
    // Coalesce: while one reload is already queued (not yet started), further
    // triggers (self-heal tick, control-nudge flood) fold into it instead of
    // queueing unbounded. The running reload re-reads fresh Redis state anyway.
    if (this.reloadQueued) return this.chain
    this.reloadQueued = true
    this.chain = this.chain
      .then(() => { this.reloadQueued = false; return this._reload() })
      .catch((e) => { this.reloadQueued = false; log('error', 'reload_failed', errFields(e)) })
    return this.chain
  }

  private async _reload(): Promise<void> {
    if (this.stopped) return
    const res = await this.loadSpecs()
    if (!res.ok) {
      log('warn', 'reload_skipped', { reason: res.reason, keeping: this.active.size, ...errFields(res.error) })
      return
    }
    if (res.seeded) log('info', 'seeded_defaults', { key: this.subsKey, n: this.defaults.length })
    for (const e of res.errors ?? []) log('warn', 'spec_rejected', { detail: e })

    // Only delete durables on drop when the load was CLEAN. If some specs were
    // rejected (partial corruption), a "missing" id may be a dropped-by-typo
    // spec — don't destroy its production cursor over a transient bad edit.
    const clean = (res.errors?.length ?? 0) === 0
    const desired = new Map(res.specs.map((s) => [idOf(s), s]))
    let added = 0, dropped = 0, updated = 0

    for (const [id, sub] of [...this.active]) {
      if (!desired.has(id)) { await this.stopSub(id, sub, clean && this.deleteOnDrop); dropped++ }
    }
    for (const [id, spec] of desired) {
      const existing = this.active.get(id)
      if (existing) {
        if (specChanged(existing.spec, spec)) {
          // Rebuild the local sub WITHOUT deleting the durable; subscribe()'s
          // drift check recreates the server consumer only if a consumer-
          // relevant field actually changed, so a throttle/attrs-only edit
          // preserves the JetStream cursor and backlog.
          await this.stopSub(id, existing, false)
          if (await this.addSub(id, spec)) updated++
        }
        continue
      }
      if (await this.addSub(id, spec)) added++
    }
    log('info', 'reload', { added, dropped, updated, active: this.active.size })
  }

  private async addSub(id: string, spec: SubSpec): Promise<boolean> {
    try {
      const sub = await this.subscribe(id, spec)
      this.active.set(id, sub)
      log('info', 'subscribed', {
        id, mode: spec.mode, subject: spec.subject, durable: spec.durable,
        sample: spec.sample, min_interval_ms: spec.min_interval_ms,
      })
      return true
    } catch (e) {
      log('error', 'subscribe_failed', { id, ...errFields(e) })
      return false
    }
  }

  private async stopSub(id: string, sub: ActiveSub, deleteDurable: boolean): Promise<void> {
    try { sub.stop() } catch {}
    this.active.delete(id)
    if (sub.stream && sub.durable && deleteDurable) {
      // Delete the durable so a removed-then-readded sub starts fresh (NEW)
      // instead of replaying the backlog accrued while it was gone. Management
      // call to $JS.API — not a data publish.
      try {
        const jsm = await this.nc.jetstreamManager()
        await jsm.consumers.delete(sub.stream, sub.durable)
        log('debug', 'durable_deleted', { stream: sub.stream, durable: sub.durable })
      } catch (e) {
        log('debug', 'durable_delete_failed', { stream: sub.stream, durable: sub.durable, ...errFields(e) })
      }
    }
    log('info', 'unsubscribed', { id })
  }

  private watchControl(): void {
    const sub = this.nc.subscribe(this.controlSubject)
    ;(async () => {
      for await (const _m of sub) {
        log('info', 'control_nudge')
        await this.reload()
      }
    })().catch((e) => log('warn', 'control_loop_ended', errFields(e)))
  }

  private watchConnection(): void {
    ;(async () => {
      for await (const s of this.nc.status()) {
        const level: Level = s.type === 'error' || s.type === 'ldm' || s.type === 'staleConnection' ? 'warn' : 'info'
        log(level, 'nats_status', { type: s.type, info: (s as any).data })
      }
    })().catch((e) => log('warn', 'status_loop_ended', errFields(e)))
  }

  private flushCounters(): void {
    for (const [id, sub] of this.active) {
      const c = sub.counters
      if (c.fwd || c.drop || c.err) {
        log('info', 'counts', { id, fwd: c.fwd, drop: c.drop, err: c.err })
        c.fwd = 0; c.drop = 0; c.err = 0
      }
    }
  }

  /** Forward one message: gate → inject. Returns false ONLY when an inject was
   *  attempted and failed (caller naks); a gated/throttled drop returns true
   *  (we intentionally skip it and ack so it isn't redelivered). */
  private async deliver(spec: SubSpec, gate: () => boolean, counters: Counters, subject: string, data: Uint8Array, force = false): Promise<boolean> {
    // `force` (a JetStream redelivery) bypasses the gate: the gate already
    // counted this message on its first attempt, so re-gating a retry would
    // drop-and-ack it and silently consume the redelivery budget.
    if (!force && !gate()) { counters.drop++; return true }
    try {
      await this.inject(decode(data), buildAttrs(spec, subject))
      counters.fwd++
      return true
    } catch (e) {
      counters.err++
      log('error', 'inject_failed', { subject, ...errFields(e) })
      return false
    }
  }

  private async subscribe(id: string, spec: SubSpec): Promise<ActiveSub> {
    const gate = makeGate(spec)
    const counters: Counters = { fwd: 0, drop: 0, err: 0 }
    // Identity guard: a dead loop removes itself from `active` (so the next
    // reload recreates it) but must NOT delete a newer sub that replaced it.
    const self: ActiveSub = { spec, counters, stop: () => {} }
    const reapIfStale = () => { if (this.active.get(id) === self) this.active.delete(id) }

    if (spec.mode === 'core') {
      const sub = this.nc.subscribe(spec.subject)
      self.stop = () => sub.unsubscribe()
      ;(async () => {
        try {
          for await (const m of sub) await this.deliver(spec, gate, counters, m.subject, m.data)
        } catch (e) {
          log('warn', 'sub_loop_ended', { id, ...errFields(e) })
        } finally {
          reapIfStale()
        }
      })()
      return self
    }

    // JetStream: reconcile the durable to the desired config, then consume.
    const stream = spec.stream!
    const durable = spec.durable!
    self.stream = stream
    self.durable = durable
    const cfg = {
      durable_name: durable,
      filter_subject: spec.filter_subject ?? spec.subject,
      deliver_policy: spec.deliver === 'all' ? DeliverPolicy.All : DeliverPolicy.New,
      ack_policy: AckPolicy.Explicit,
      // Bound redelivery so a persistently-failing inject (poison message) can't
      // become an infinite nak→redeliver storm; after this it's dropped server-side.
      max_deliver: Number(process.env.JS_MAX_DELIVER ?? 5),
    }
    const jsm = await this.nc.jetstreamManager()
    try {
      const info = await jsm.consumers.info(stream, durable)
      const cur: any = info.config
      if (
        cur.filter_subject !== cfg.filter_subject ||
        cur.deliver_policy !== cfg.deliver_policy ||
        (cur.max_deliver ?? -1) !== cfg.max_deliver ||
        cur.ack_policy !== cfg.ack_policy
      ) {
        // Config drift — recreate fresh so the new filter/deliver actually take
        // effect (a bare add() would be a silent no-op or throw-and-ignore).
        await jsm.consumers.delete(stream, durable).catch(() => {})
        await jsm.consumers.add(stream, cfg)
        log('info', 'consumer_recreated', { stream, durable })
      }
    } catch (e) {
      if (isNotFound(e)) await jsm.consumers.add(stream, cfg)
      else throw e
    }
    const consumer = await this.js.consumers.get(stream, durable)
    const messages = await consumer.consume()
    self.stop = () => messages.stop()
    ;(async () => {
      const nakBackoff = Number(process.env.JS_NAK_BACKOFF_MS ?? 2000)
      try {
        for await (const m of messages) {
          // m.redelivered → bypass the throttle gate so a retry isn't dropped.
          const ok = await this.deliver(spec, gate, counters, m.subject, m.data, m.redelivered)
          if (ok) m.ack() // ack only after the message is safely in the session (or intentionally throttled)
          else m.nak(nakBackoff) // inject failed → redeliver (paced; bounded by max_deliver) rather than silently lose it
        }
      } catch (e) {
        log('warn', 'sub_loop_ended', { id, ...errFields(e) })
      } finally {
        reapIfStale()
      }
    })()
    return self
  }

  async stop(): Promise<void> {
    this.stopped = true
    for (const t of this.timers) clearInterval(t)
    for (const [, sub] of this.active) { try { sub.stop() } catch {} }
    this.active.clear()
    try { await this.nc?.drain() } catch {}
    try { (this.redis as any)?.close?.() } catch {}
    log('info', 'stopped')
  }
}
