#!/usr/bin/env bun
import { A2AChannel } from './a2a-channel.ts'
import {
  buildHeartbeatSendArgs,
  configFingerprint,
  loadHeartbeatConfigFile,
  reloadHeartbeatConfigFile,
  type HeartbeatConfig,
  type HeartbeatTarget,
} from './heartbeat_config.ts'

export type HeartbeatSender = {
  callTool(name: 'a2a_send', args: Record<string, any>): Promise<any>
}

export type SchedulerLogger = (level: 'info' | 'warn' | 'error', event: string, fields?: Record<string, unknown>) => void

export type HeartbeatSchedulerOptions = {
  configPath: string
  sender: HeartbeatSender
  config: HeartbeatConfig
  reloadMs?: number
  now?: () => number
  random?: () => number
  logger?: SchedulerLogger
}

type TargetTimer = {
  target: HeartbeatTarget
  timer?: ReturnType<typeof setTimeout>
}

export class HeartbeatScheduler {
  private config: HeartbeatConfig
  private timers = new Map<string, TargetTimer>()
  private reloadTimer?: ReturnType<typeof setInterval>
  private running = false
  private readonly now: () => number
  private readonly random: () => number
  private readonly logger: SchedulerLogger

  constructor(private opts: HeartbeatSchedulerOptions) {
    this.config = opts.config
    this.now = opts.now ?? Date.now
    this.random = opts.random ?? Math.random
    this.logger = opts.logger ?? log
  }

  getConfig(): HeartbeatConfig {
    return this.config
  }

  async start(): Promise<void> {
    if (this.running) return
    this.running = true
    this.applyConfig(this.config)
    const reloadMs = Math.max(1000, this.opts.reloadMs ?? 30_000)
    this.reloadTimer = setInterval(() => void this.reload(), reloadMs)
    this.logger('info', 'heartbeat_scheduler_started', { targets: this.config.targets.length, enabled: this.config.enabled, reload_ms: reloadMs })
  }

  stop(): void {
    this.running = false
    if (this.reloadTimer) clearInterval(this.reloadTimer)
    this.reloadTimer = undefined
    for (const { timer } of this.timers.values()) if (timer) clearTimeout(timer)
    this.timers.clear()
    this.logger('info', 'heartbeat_scheduler_stopped')
  }

  async runOnce(): Promise<void> {
    if (!this.config.enabled) {
      this.logger('info', 'heartbeat_config_disabled')
      return
    }
    for (const target of this.config.targets) await this.sendTarget(target)
  }

  async reload(): Promise<void> {
    const state = await reloadHeartbeatConfigFile(this.opts.configPath, this.config)
    if (!state.ok) {
      this.logger('warn', 'heartbeat_config_reload_failed', { error: state.error })
      return
    }
    if (!state.changed) return
    const previous = this.config
    this.config = state.config
    this.applyConfig(this.config, previous)
    this.logger('info', 'heartbeat_config_loaded', { targets: this.config.targets.length, config_hash: configFingerprint(this.config) })
  }

  async sendTarget(target: HeartbeatTarget): Promise<void> {
    const args = buildHeartbeatSendArgs(target)
    const attemptId = `${target.agentId}-${this.now()}`
    try {
      const res = await this.opts.sender.callTool('a2a_send', args)
      const parsed = parseToolResult(res)
      if (!parsed?.ok) {
        this.logger('warn', 'heartbeat_send_failed', { target: target.agentId, attempt_id: attemptId, error: parsed?.error ?? 'send_failed' })
        return
      }
      this.logger('info', 'heartbeat_sent', { target: target.agentId, attempt_id: attemptId, envelope_id: parsed.id, type: args.type, ttl_ms: args.ttl_ms })
    } catch (e) {
      this.logger('warn', 'heartbeat_send_failed', { target: target.agentId, attempt_id: attemptId, error: e instanceof Error ? e.message : String(e) })
    }
  }

  private applyConfig(next: HeartbeatConfig, previous?: HeartbeatConfig): void {
    for (const [agentId, entry] of this.timers.entries()) {
      if (!next.enabled || !next.targets.some((target) => target.agentId === agentId)) {
        if (entry.timer) clearTimeout(entry.timer)
        this.timers.delete(agentId)
        this.logger('info', 'heartbeat_target_disabled', { target: agentId })
      }
    }
    if (!next.enabled) return
    for (const target of next.targets) {
      const current = this.timers.get(target.agentId)
      const changed = !current || !previous || JSON.stringify(current.target) !== JSON.stringify(target)
      if (!changed) continue
      if (current?.timer) clearTimeout(current.timer)
      this.scheduleTarget(target)
    }
  }

  private scheduleTarget(target: HeartbeatTarget): void {
    if (!this.running) return
    const delay = nextDelayMs(target, this.random)
    const timer = setTimeout(() => {
      void this.sendTarget(target).finally(() => {
        if (this.running && this.timers.has(target.agentId)) this.scheduleTarget(target)
      })
    }, delay)
    this.timers.set(target.agentId, { target, timer })
    this.logger('info', 'heartbeat_target_scheduled', { target: target.agentId, interval_ms: target.intervalMs, jitter_ms: target.jitterMs, next_ms: delay })
  }
}

export function nextDelayMs(target: HeartbeatTarget, random: () => number = Math.random): number {
  if (target.jitterMs <= 0) return target.intervalMs
  return target.intervalMs + Math.floor(random() * target.jitterMs)
}

export function parseToolResult(res: any): any {
  try {
    return JSON.parse(res?.content?.[0]?.text ?? '')
  } catch {
    return null
  }
}

export async function createA2AHeartbeatSender(config: HeartbeatConfig): Promise<A2AChannel> {
  const channel = new A2AChannel(async () => {}, {
    enabled: true,
    agentId: config.sender.agentId,
    sigAlg: 'ed25519',
    transportAuth: config.sender.transportAuth,
    signingKeyPath: config.sender.signingKeyPath ?? process.env.A2A_SIGNING_KEY,
    natsUrl: process.env.NATS_URL,
    redisUrl: process.env.REDIS_URL,
    toolOnly: true,
  })
  await channel.start()
  if (!channel.isStarted()) throw new Error('a2a_channel_not_started')
  return channel
}

function log(level: 'info' | 'warn' | 'error', event: string, fields: Record<string, unknown> = {}): void {
  const kv = Object.entries(fields)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => `${key}=${typeof value === 'string' ? JSON.stringify(value) : JSON.stringify(value)}`)
    .join(' ')
  console.error(`${new Date().toISOString()} ${level} [heartbeat-scheduler] ${event}${kv ? ' ' + kv : ''}`)
}

function usage(): never {
  console.error('usage: bun heartbeat_scheduler.ts --config <path> [--once] [--dry-run] [--reload-ms <ms>]')
  process.exit(2)
}

function parseArgs(argv: string[]): { configPath: string; once: boolean; dryRun: boolean; reloadMs?: number } {
  let configPath = process.env.A2A_HEARTBEAT_CONFIG ?? ''
  let once = false
  let dryRun = false
  let reloadMs: number | undefined
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--config') configPath = argv[++i] ?? ''
    else if (arg === '--once') once = true
    else if (arg === '--dry-run') dryRun = true
    else if (arg === '--reload-ms') reloadMs = Number(argv[++i])
    else usage()
  }
  if (!configPath) usage()
  if (reloadMs != null && (!Number.isInteger(reloadMs) || reloadMs < 1000)) usage()
  return { configPath, once, dryRun, ...(reloadMs != null ? { reloadMs } : {}) }
}

async function main(): Promise<void> {
  const args = parseArgs(Bun.argv.slice(2))
  const config = await loadHeartbeatConfigFile(args.configPath)
  log('info', 'heartbeat_config_loaded', { targets: config.targets.length, enabled: config.enabled, config_hash: configFingerprint(config) })

  if (args.dryRun) {
    for (const target of config.targets) log('info', 'heartbeat_dry_run_target', { target: target.agentId, interval_ms: target.intervalMs, ttl_ms: target.ttlMs, thread: target.thread })
    return
  }

  const sender = await createA2AHeartbeatSender(config)
  const scheduler = new HeartbeatScheduler({ configPath: args.configPath, config, sender, reloadMs: args.reloadMs })
  const stop = async () => {
    scheduler.stop()
    await sender.stop().catch(() => {})
    process.exit(0)
  }
  process.on('SIGTERM', () => void stop())
  process.on('SIGINT', () => void stop())
  process.on('SIGHUP', () => void scheduler.reload())

  if (args.once) {
    await scheduler.runOnce()
    await sender.stop().catch(() => {})
    return
  }
  await scheduler.start()
}

if (import.meta.main) {
  main().catch((e) => {
    log('error', 'heartbeat_scheduler_fatal', { error: e instanceof Error ? e.message : String(e) })
    process.exit(1)
  })
}
