// Faithful copy of a2a-channel.ts presence (token-guarded SET NX EX + Lua heartbeat/release). Future DRY: converge A2AChannel + a2a-core onto this module.
//
// DUP-LIFECYCLE (folds the SLICE-3.2 gate, Opus P0/P1 + GPT-5.5 P1): a2a-channel ABORTS start and
// scheduleRetry()s on a non-'ok' claim; a naive port that just "warn+continue, no retry" reintroduces
// the slice's own bug — a core restarted within a dead predecessor's TTL (crash/SIGKILL leaves the key
// up to ttlS) gets 'dup', never re-claims, and once the stale key expires runs PRESENCE-LESS forever →
// the detector false-offlines a healthy beating core. And if a SECOND core genuinely runs, both could
// emit beats for the one identity. So here: start() always arms a maintain timer that RE-claims while
// not owned (converges to ownership once the stale key expires) and heartbeats while owned; and the
// caller (A2ACore) gates its self-beat on isOwned() so a non-owner never emits a 2nd loop_seq stream.
import { RedisClient } from 'bun'
import { hostname } from 'node:os'
import { randomUUID } from 'node:crypto'

export type PresenceClaimerOpts = {
  agentId: string
  host?: string
  keyPrefix?: string
  ttlS?: number
  heartbeatMs?: number
  now?: () => number
}

const DEFAULT_KEY_PREFIX = 'alloyium:a2a:presence:'
const DEFAULT_TTL_S = 90
const DEFAULT_HEARTBEAT_MS = 30_000
const MIN_TTL_S = 5
const MIN_HEARTBEAT_MS = 1000
const DETECTOR_FRESHNESS_MS = 120_000
const REDIS_TIMEOUT_MS = Number(process.env.REDIS_TIMEOUT_MS ?? 2500)

const PRESENCE_HEARTBEAT_SCRIPT =
  "local v=redis.call('GET',KEYS[1]); if v==false then return 0 end; " +
  "local ok,d=pcall(cjson.decode,v); if not ok or type(d)~='table' or d.token~=ARGV[1] then return 0 end; " +
  "redis.call('SET',KEYS[1],ARGV[2],'EX',tonumber(ARGV[3])); return 1"
const PRESENCE_RELEASE_SCRIPT =
  "local v=redis.call('GET',KEYS[1]); if v==false then return 0 end; " +
  "local ok,d=pcall(cjson.decode,v); if ok and type(d)=='table' and d.token==ARGV[1] then redis.call('DEL',KEYS[1]); return 1 end; return 0"

// Bound every presence Redis call so a stall can't hang core boot (the awaited start() leaving
// A2ACore.startPromise unsettled, which also wedges stop()). Mirrors a2a-channel's withTimeout
// (module-private there). Folds Opus P2 (dropped timeout bound).
function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout>
  const timeout = new Promise<never>((_, rej) => { t = setTimeout(() => rej(new Error(`presence_timeout:${label}`)), ms); (t as any).unref?.() })
  return Promise.race([p.finally(() => clearTimeout(t)), timeout])
}

export class PresenceClaimer {
  private redis?: RedisClient
  private agentId: string
  private host: string
  private keyPrefix: string
  private ttlS: number
  private heartbeatMs: number
  private now: () => number
  private key: string
  private instanceToken: string
  private startedAt: string  // set lazily on the FIRST claim attempt; STABLE thereafter (this boot's marker)
  private owned: boolean
  private lastOwnedAt: number // ms of last CONFIRMED ownership (claim win / heartbeat refresh) — drives TTL-aware ownership expiry
  private maintainTimer?: ReturnType<typeof setInterval>
  private started: boolean

  constructor(redis: RedisClient, opts: PresenceClaimerOpts) {
    this.redis = redis
    this.agentId = opts.agentId
    this.host = opts.host ?? hostname()
    this.keyPrefix = opts.keyPrefix ?? DEFAULT_KEY_PREFIX
    this.ttlS = opts.ttlS ?? DEFAULT_TTL_S
    this.heartbeatMs = opts.heartbeatMs ?? DEFAULT_HEARTBEAT_MS
    this.now = opts.now ?? Date.now
    this.key = `${this.keyPrefix}${this.agentId}`
    this.instanceToken = randomUUID()
    this.startedAt = ''
    this.owned = false
    this.lastOwnedAt = 0
    this.started = false

    if (!this.agentId) throw new Error('PresenceClaimer requires agentId')
    if (!Number.isFinite(this.ttlS) || this.ttlS < MIN_TTL_S) throw new Error(`PresenceClaimer ttlS must be >= ${MIN_TTL_S}`)
    if (!Number.isFinite(this.heartbeatMs) || this.heartbeatMs < MIN_HEARTBEAT_MS) throw new Error(`PresenceClaimer heartbeatMs must be >= ${MIN_HEARTBEAT_MS}`)
    // The detector gates non-offline on last_seen freshness (≤120s) and the key must not expire between
    // refreshes — so the heartbeat MUST be < ttlS*1000 AND < 120s. Assert so it can't regress to false-offline.
    if (this.heartbeatMs >= this.ttlS * 1000) throw new Error('PresenceClaimer heartbeatMs must be < ttlS*1000')
    if (this.heartbeatMs >= DETECTOR_FRESHNESS_MS) throw new Error(`PresenceClaimer heartbeatMs must be < ${DETECTOR_FRESHNESS_MS}`)
  }

  /** True iff we currently HOLD the presence key by our token. The core gates its self-beat on this so a
   *  non-owner (lost a 'dup', or lost ownership mid-flight) never emits a 2nd loop_seq stream for the
   *  identity — preserving the "one beater per identity" invariant (folds P1). */
  isOwned(): boolean { return this.started && this.owned }

  /** Claim presence. Returns the FIRST attempt's result ('ok'|'dup'), but ALWAYS arms a maintain timer:
   *  it heartbeat-refreshes while owned and RE-claims while not owned, so a core restarted within a dead
   *  predecessor's TTL converges to owning presence once that stale key expires — instead of running
   *  presence-less forever (folds Opus P0). The immediate continue keeps the core from going dark. */
  async start(): Promise<'ok' | 'dup'> {
    if (this.started) return this.owned ? 'ok' : 'dup'
    this.started = true
    const r = await this.tryClaim()
    this.armMaintain()
    return r
  }

  /** Clear the maintain timer and token-guarded DEL of our key (no-op if we never owned it, so it can
   *  never delete a successor's/foreign key). Safe to call when never started. */
  async stop(): Promise<void> {
    if (this.maintainTimer) { clearInterval(this.maintainTimer); this.maintainTimer = undefined }
    this.started = false
    this.owned = false
    await this.releaseKey()
  }

  // Token-guarded DEL of our key (no-op unless WE hold it by token, so it can never delete a successor's/
  // foreign key). Used by stop() and the stop-race guard in claimWon().
  private async releaseKey(): Promise<void> {
    const redis = this.redis
    if (!redis) return
    try { await withTimeout(redis.send('EVAL', [PRESENCE_RELEASE_SCRIPT, '1', this.key, this.instanceToken]), REDIS_TIMEOUT_MS, 'presence.release') }
    catch (err) { this.warn('presence_release_failed', { err: this.errString(err) }) }
  }

  // One claim attempt. SET NX EX → 'ok'; on NX-fail, if WE already hold it (own token) EXPIRE-refresh →
  // 'ok'; else 'dup'. Maintains this.owned. started_at is set ONCE (first attempt) and reused so a
  // retry-win doesn't mint a new boot id the detector would read as a supersession (folds Opus P2 drift).
  private async tryClaim(): Promise<'ok' | 'dup'> {
    const redis = this.redis
    if (!redis) return 'dup'
    const ts = new Date(this.now()).toISOString()
    if (!this.startedAt) this.startedAt = ts
    const val = this.value(ts)
    try {
      const res = await withTimeout(redis.send('SET', [this.key, val, 'NX', 'EX', String(this.ttlS)]), REDIS_TIMEOUT_MS, 'presence.set')
      if (res) return await this.claimWon()
      const cur = await withTimeout(redis.get(this.key), REDIS_TIMEOUT_MS, 'presence.get')
      if (cur) {
        try {
          if (JSON.parse(cur)?.token === this.instanceToken) {
            await withTimeout(redis.send('EXPIRE', [this.key, String(this.ttlS)]), REDIS_TIMEOUT_MS, 'presence.expire')
            return await this.claimWon()
          }
        } catch { /* malformed record held by someone else → not ours → 'dup' (the maintain timer keeps retrying) */ }
      }
    } catch (err) { this.warn('presence_claim_failed', { err: this.errString(err) }) }
    this.owned = false
    return 'dup'
  }

  // Commit a won claim — but if stop() raced us (started flipped false during the awaited SET/EXPIRE),
  // release the key we just (re)created instead of leaving a GHOST presence record (folds GPT-5.5 P2),
  // and stamp lastOwnedAt so the heartbeat's TTL-aware expiry has a fresh baseline.
  private async claimWon(): Promise<'ok' | 'dup'> {
    if (!this.started) { await this.releaseKey(); this.owned = false; return 'dup' }
    this.owned = true
    this.lastOwnedAt = this.now()
    return 'ok'
  }

  private armMaintain(): void {
    if (this.maintainTimer) return
    this.maintainTimer = setInterval(() => { void this.maintain() }, this.heartbeatMs)
    ;(this.maintainTimer as any).unref?.() // don't keep the process alive on the timer alone (the NATS conns do); harmless in a service, lets tests exit
  }

  // Each tick: refresh if we own it (token-guarded; drop ownership if the key was taken), else RE-claim.
  private async maintain(): Promise<void> {
    if (!this.started) return
    if (this.owned) await this.heartbeat()
    else await this.tryClaim()
  }

  private value(lastSeen: string): string {
    return JSON.stringify({ token: this.instanceToken, host: this.host, started_at: this.startedAt, last_seen: lastSeen })
  }

  // Token-guarded refresh of last_seen + TTL only while WE own the key. On a non-1 reply we LOST the key
  // (a successor took over) → drop ownership so the next maintain tick re-claims.
  private async heartbeat(): Promise<void> {
    const redis = this.redis
    if (!redis) return
    const val = this.value(new Date(this.now()).toISOString())
    try {
      const res = await withTimeout(redis.send('EVAL', [PRESENCE_HEARTBEAT_SCRIPT, '1', this.key, this.instanceToken, val, String(this.ttlS)]), REDIS_TIMEOUT_MS, 'presence.heartbeat')
      if (Number(res) === 1) this.lastOwnedAt = this.now()                       // refreshed → ownership confirmed
      else { this.owned = false; this.warn('presence_heartbeat_lost_ownership') } // a successor took the key → re-claim next tick
    } catch (err) {
      // Couldn't reach Redis. Keep ownership through a TRANSIENT blip (beats continue, presence still
      // fresh), but once we're past ttlS since the last confirmed refresh the key has CERTAINLY expired
      // and another core could claim it — so drop ownership to fail closed BEFORE two cores emit (folds
      // GPT-5.5 P1, without the transient-blip false-wedge a blanket fail-closed would cause).
      if (this.now() - this.lastOwnedAt >= this.ttlS * 1000) { this.owned = false; this.warn('presence_ownership_expired') }
      this.warn('presence_heartbeat_failed', { err: this.errString(err) })
    }
  }

  private warn(event: string, fields: Record<string, unknown> = {}): void {
    console.error(JSON.stringify({ level: 'warn', event, agent_id: this.agentId, key: this.key, ...fields }))
  }

  private errString(err: unknown): string {
    if (err instanceof Error) return err.stack || err.message
    return String(err)
  }
}
