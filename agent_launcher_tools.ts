// Gated A2A agent launcher tools.
//
// This module exposes the operator/orchestrator action for launching persistent,
// identity-bearing Codex peers. It deliberately stays outside A2AChannel:
// messaging and presence belong there; provisioning/process launch belongs to the
// host/Kubernetes launcher layer.
import { dirname } from 'node:path'

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

export type LaunchMode = 'shim' | 'webhook'

export interface AgentLauncherToolsOpts {
  /** The authenticated A2A identity for this MCP session. */
  agentId: string
  /** Identities allowed to launch peers. Defaults to A2A_AGENT_LAUNCH_ALLOWED_IDS or codex-gw. */
  allowedAgentIds?: string[]
  /** Absolute path to a2a-launch.sh. Defaults to CHANNELS/a2a-launch.sh. */
  launcherPath?: string
  /** Default MCP transport mode for launched peers. Defaults to A2A_AGENT_LAUNCH_MODE or shim. */
  defaultMode?: LaunchMode
  /** Optional spawn backend for tests or future orchestrators. */
  spawnImpl?: SpawnImpl
  /** Remote launcher API base URL. When set, launch requests go there instead of local shell. */
  launcherUrl?: string
  /** Optional fetch backend for tests. */
  fetchImpl?: FetchImpl
  /** Additional environment passed to the launcher process. */
  launchEnv?: Record<string, string | undefined>
  timeoutMs?: number
  /** Optional id allocator for tests or future orchestrators. */
  allocateAgentId?: (ctx: { parentAgentId: string; label?: string }) => string
}

export interface SpawnResult {
  exitCode: number | null
  stdout: string
  stderr: string
  timedOut?: boolean
}

export type SpawnImpl = (cmd: string[], opts: { env: Record<string, string | undefined>; timeoutMs: number }) => Promise<SpawnResult>
export type FetchImpl = (url: string, init: RequestInit) => Promise<Response>

const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const ROLE_SCOPE_RE = /^[A-Za-z0-9:_./*@+=-]{1,256}$/
const DEFAULT_TIMEOUT_MS = 60_000

type LaunchPolicy = {
  allowWrite?: boolean
  sandbox?: string
  effort?: string
  model?: string
  turnTimeoutMs?: number
  roleScopes?: string[]
  writeRequesters?: string[]
}

export class AgentLauncherTools {
  private readonly agentId: string
  private readonly allowedAgentIds: Set<string>
  private readonly launcherPath: string
  private readonly defaultMode: LaunchMode
  private readonly spawnImpl: SpawnImpl
  private readonly launcherUrl?: string
  private readonly launcherToken?: string
  private readonly fetchImpl: FetchImpl
  private readonly launchEnv: Record<string, string | undefined>
  private readonly timeoutMs: number
  private readonly allocateAgentId: (ctx: { parentAgentId: string; label?: string }) => string

  static readonly TOOL_NAMES = ['a2a_launch_codex_agent'] as const

  static readonly INSTRUCTIONS =
    ' Authorized orchestrators also have an A2A peer launch tool: ' +
    'a2a_launch_codex_agent launches a persistent Codex A2A peer with a unique ' +
    'agent identity and signing key, normally using the shared-core shim. Omit ' +
    'agent_id to allocate a child identity automatically. Use it when parallel ' +
    'work needs a first-class peer instead of an inherited Codex sub-agent identity.'

  constructor(opts: AgentLauncherToolsOpts) {
    this.agentId = opts.agentId
    this.allowedAgentIds = new Set((opts.allowedAgentIds ?? allowedIdsFromEnv()).filter(Boolean))
    this.launcherPath = opts.launcherPath ?? `${stripSlash(process.env.CHANNELS ?? import.meta.dir)}/a2a-launch.sh`
    this.defaultMode = parseMode(opts.defaultMode ?? process.env.A2A_AGENT_LAUNCH_MODE, 'shim')
    this.spawnImpl = opts.spawnImpl ?? defaultSpawn
    this.launcherUrl = stripSlash(opts.launcherUrl ?? process.env.A2A_LAUNCHER_URL ?? '') || undefined
    this.launcherToken = process.env.A2A_LAUNCHER_TOKEN ?? process.env.A2A_LAUNCHER_BEARER_TOKEN
    this.fetchImpl = opts.fetchImpl ?? fetch
    this.launchEnv = opts.launchEnv ?? process.env
    const configuredTimeout = opts.timeoutMs ?? (Number(process.env.A2A_AGENT_LAUNCH_TIMEOUT_MS ?? DEFAULT_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS)
    this.timeoutMs = Math.max(1000, configuredTimeout)
    this.allocateAgentId = opts.allocateAgentId ?? ((ctx) => allocateChildAgentId(ctx.parentAgentId, ctx.label))
  }

  handles(name: string): boolean {
    return (AgentLauncherTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    if (!this.isAuthorized()) return []
    return [
      {
        name: 'a2a_launch_codex_agent',
        description:
          'Launch a persistent Codex A2A peer with a unique identity. This calls the configured host launcher, onboards the id if needed, and starts a real codex gateway peer. Gated to authorized orchestrator identities.',
        inputSchema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            agent_id: { type: 'string', description: 'New peer agent id, matching ^[a-z0-9-]{1,64}$.' },
            label: { type: 'string', description: 'Optional short label used when agent_id is omitted, e.g. "review" or "smoke".' },
            mode: { enum: ['shim', 'webhook'], default: this.defaultMode, description: 'MCP transport mode for the launched peer. Prefer shim.' },
            worktree: { type: 'string', description: 'Optional REPO@BASE worktree argument passed to a2a-launch.sh.' },
            policy: {
              type: 'object',
              additionalProperties: false,
              description: 'Optional runtime policy for the launched Codex peer. Contains no credential material.',
              properties: {
                allow_write: { type: 'boolean', description: 'Start the worker write-enabled. The requester must still pass gateway write authz.' },
                sandbox: { type: 'string', description: 'Codex execution sandbox setting for the worker gateway.' },
                effort: { type: 'string', description: 'Codex effort override.' },
                model: { type: 'string', description: 'Codex model override.' },
                turn_timeout_ms: { type: 'integer', minimum: 1, description: 'Worker turn timeout in milliseconds.' },
                role_scopes: {
                  type: 'array',
                  maxItems: 32,
                  items: { type: 'string' },
                  description: 'Sanitized role/scope names the worker may request from scoped-access tooling.',
                },
                write_requesters: {
                  type: 'array',
                  maxItems: 32,
                  items: { type: 'string' },
                  description: 'Additional A2A requester identities allowed to drive workspace-write jobs on this worker.',
                },
              },
            },
            dry_run: { type: 'boolean', default: false, description: 'Return the launcher command without executing it.' },
          },
        },
      },
    ]
  }

  async callTool(name: string, args: Record<string, any> = {}): Promise<ToolResult> {
    try {
      if (!this.handles(name)) return this.result({ ok: false, error: 'unknown_tool', detail: name }, true)
      if (!this.isAuthorized()) return this.result({ ok: false, error: 'unauthorized', detail: `agent ${this.agentId} may not launch peers` }, true)
      return await this.launchCodex(args)
    } catch (e) {
      return this.result({ ok: false, error: 'launcher_error', detail: errMsg(e) }, true)
    }
  }

  private async launchCodex(args: Record<string, any>): Promise<ToolResult> {
    const label = this.parseLabel(args.label)
    if (label === false) return this.result({ ok: false, error: 'bad_label' }, true)

    const explicitAgentId = String(args.agent_id ?? '').trim()
    const agentId = explicitAgentId || this.allocateAgentId({ parentAgentId: this.agentId, label })
    if (!AGENT_ID_RE.test(agentId)) return this.result({ ok: false, error: 'bad_agent_id' }, true)
    if (agentId === this.agentId) return this.result({ ok: false, error: 'self_launch_refused' }, true)

    const mode = parseMode(args.mode, this.defaultMode)
    const policy = parseLaunchPolicy(args.policy)
    if (policy === false) return this.result({ ok: false, error: 'bad_policy' }, true)
    const cmd = ['bash', this.launcherPath, agentId, 'codex', mode === 'shim' ? '--shim' : '--webhook']

    if (args.worktree != null) {
      if (typeof args.worktree !== 'string' || args.worktree.length > 512 || /[\r\n]/.test(args.worktree)) {
        return this.result({ ok: false, error: 'bad_worktree' }, true)
      }
      cmd.push('--worktree', args.worktree)
    }

    if (this.launcherUrl) {
      return this.remoteLaunchCodex({
        agentId,
        label,
        mode,
        worktree: typeof args.worktree === 'string' ? args.worktree : undefined,
        dryRun: args.dry_run === true,
        policy,
      })
    }

    if (args.dry_run === true) {
      return this.result({ ok: true, dry_run: true, agent_id: agentId, parent_agent_id: this.agentId, kind: 'codex', mode, cmd, ...(policy ? { policy: policyForResponse(policy) } : {}) })
    }

    const r = await this.spawnImpl(cmd, {
      env: {
        ...this.launchEnv,
        CHANNELS: this.launchEnv.CHANNELS ?? dirname(this.launcherPath),
        A2A_MODE: mode,
        A2A_PARENT_AGENT_ID: this.agentId,
        A2A_LAUNCH_AGENT_LABEL: label || undefined,
        CODEX_GW_A2A_TOOLS_MODE: mode,
        ...policyEnv(policy, this.agentId, this.launchEnv),
      },
      timeoutMs: this.timeoutMs,
    })

    if (r.timedOut) return this.result({ ok: false, error: 'launcher_timeout', agent_id: agentId, mode, stderr: trimText(r.stderr), stdout: trimText(r.stdout) }, true)
    if (r.exitCode !== 0) return this.result({ ok: false, error: 'launcher_failed', agent_id: agentId, mode, exit_code: r.exitCode, stderr: trimText(r.stderr), stdout: trimText(r.stdout) }, true)

    return this.result({
      ok: true,
      agent_id: agentId,
      parent_agent_id: this.agentId,
      kind: 'codex',
      mode,
      session: `a2a-${agentId}`,
      stdout: trimText(r.stdout),
      stderr: trimText(r.stderr),
    })
  }

  private async remoteLaunchCodex(args: { agentId: string; label?: string; mode: LaunchMode; worktree?: string; dryRun?: boolean; policy?: LaunchPolicy }): Promise<ToolResult> {
    const headers: Record<string, string> = { 'content-type': 'application/json' }
    if (this.launcherToken) headers.authorization = `Bearer ${this.launcherToken}`
    const policy = args.policy ? policyForLauncher(args.policy) : undefined
    const res = await this.fetchImpl(`${this.launcherUrl}/v1/agents/codex`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        agent_id: args.agentId,
        label: args.label,
        mode: args.mode,
        created_by: this.agentId,
        worktree: args.worktree,
        dry_run: args.dryRun === true,
        ...(policy ? { policy } : {}),
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    })
    let body: any = null
    try { body = await res.json() } catch { body = { ok: false, error: 'bad_launcher_response' } }
    if (!res.ok || !body?.ok) {
      return this.result({
        ok: false,
        error: body?.error ?? 'remote_launcher_failed',
        status: res.status,
        detail: body?.detail,
      }, true)
    }
    return this.result({
      ...body,
      agent_id: body.agent_id ?? args.agentId,
      parent_agent_id: body.parent_agent_id ?? this.agentId,
      kind: body.kind ?? 'codex',
      mode: body.mode ?? args.mode,
    })
  }

  private isAuthorized(): boolean {
    return this.allowedAgentIds.has(this.agentId)
  }

  private result(obj: unknown, isError = false): ToolResult {
    return { content: [{ type: 'text', text: JSON.stringify(obj) }], isError }
  }

  private parseLabel(value: unknown): string | undefined | false {
    if (value == null || value === '') return undefined
    if (typeof value !== 'string' || value.length > 128 || /[\r\n]/.test(value)) return false
    return value
  }
}

export function allocateChildAgentId(parentAgentId: string, label?: string, opts: { now?: number; randomHex?: string } = {}): string {
  const tailParts = [
    'sub',
    slugPart(label, ''),
    (opts.now ?? Date.now()).toString(36),
    (opts.randomHex ?? crypto.randomUUID().replace(/-/g, '')).toLowerCase().replace(/[^a-f0-9]/g, '').slice(0, 8) || '00000000',
  ].filter(Boolean)
  const tail = tailParts.join('-')
  const parent = slugPart(parentAgentId, 'agent')
  const maxParent = Math.max(1, 64 - tail.length - 1)
  const prefix = trimDashes(parent.slice(0, maxParent)) || 'agent'
  const id = trimDashes(`${prefix}-${tail}`.slice(0, 64))
  return AGENT_ID_RE.test(id) ? id : `agent-sub-${tail}`.slice(0, 64).replace(/-+$/, '')
}

function allowedIdsFromEnv(): string[] {
  const raw = process.env.A2A_AGENT_LAUNCH_ALLOWED_IDS ?? 'codex-gw'
  return raw.split(',').map((s) => s.trim()).filter(Boolean)
}

function parseLaunchPolicy(value: unknown): LaunchPolicy | undefined | false {
  if (value == null) return undefined
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const o = value as Record<string, unknown>
  const policy: LaunchPolicy = {}

  if (o.allow_write != null) {
    if (typeof o.allow_write !== 'boolean') return false
    policy.allowWrite = o.allow_write
  }
  if (o.sandbox != null) {
    const sandbox = safeString(o.sandbox, 64)
    if (!sandbox) return false
    policy.sandbox = sandbox
  }
  if (o.effort != null) {
    const effort = safeString(o.effort, 32)
    if (!effort) return false
    policy.effort = effort
  }
  if (o.model != null) {
    const model = safeString(o.model, 128)
    if (!model) return false
    policy.model = model
  }
  if (o.turn_timeout_ms != null) {
    if (!Number.isInteger(o.turn_timeout_ms) || o.turn_timeout_ms <= 0) return false
    policy.turnTimeoutMs = o.turn_timeout_ms
  }
  if (o.role_scopes != null) {
    const roleScopes = safeStringList(o.role_scopes, ROLE_SCOPE_RE)
    if (!roleScopes) return false
    policy.roleScopes = roleScopes
  }
  if (o.write_requesters != null) {
    const writeRequesters = safeStringList(o.write_requesters, AGENT_ID_RE)
    if (!writeRequesters) return false
    policy.writeRequesters = writeRequesters
  }

  return Object.keys(policy).length ? policy : undefined
}

function safeString(value: unknown, maxLen: number): string | null {
  if (typeof value !== 'string') return null
  const s = value.trim()
  if (!s || s.length > maxLen || /[\r\n]/.test(s)) return null
  return s
}

function safeStringList(value: unknown, re: RegExp): string[] | null {
  if (!Array.isArray(value) || value.length > 32) return null
  const out: string[] = []
  for (const item of value) {
    if (typeof item !== 'string') return null
    const s = item.trim()
    if (!re.test(s) || /[\r\n]/.test(s)) return null
    if (!out.includes(s)) out.push(s)
  }
  return out
}

function policyForLauncher(policy: LaunchPolicy): Record<string, unknown> {
  return {
    ...(policy.allowWrite !== undefined ? { allow_write: policy.allowWrite } : {}),
    ...(policy.sandbox ? { sandbox: policy.sandbox } : {}),
    ...(policy.effort ? { effort: policy.effort } : {}),
    ...(policy.model ? { model: policy.model } : {}),
    ...(policy.turnTimeoutMs ? { turn_timeout_ms: policy.turnTimeoutMs } : {}),
    ...(policy.roleScopes?.length ? { role_scopes: policy.roleScopes } : {}),
    ...(policy.writeRequesters?.length ? { write_requesters: policy.writeRequesters } : {}),
  }
}

function policyForResponse(policy: LaunchPolicy): Record<string, unknown> {
  return policyForLauncher(policy)
}

function policyEnv(policy: LaunchPolicy | undefined, parentAgentId: string, baseEnv: Record<string, string | undefined>): Record<string, string | undefined> {
  if (!policy) return {}
  const out: Record<string, string | undefined> = {}
  if (policy.allowWrite !== undefined) out.CODEX_GW_ALLOW_WRITE = policy.allowWrite ? '1' : '0'
  if (policy.sandbox) {
    out.CODEX_GW_CODEX_SANDBOX = policy.sandbox
    if (policy.allowWrite) out.CODEX_GW_WORKSPACE_WRITE_CODEX_SANDBOX = policy.sandbox
  }
  if (policy.effort) out.CODEX_GW_EFFORT = policy.effort
  if (policy.model) out.CODEX_GW_MODEL = policy.model
  if (policy.turnTimeoutMs) out.CODEX_GW_TURN_TIMEOUT_MS = String(policy.turnTimeoutMs)
  if (policy.roleScopes?.length) out.A2A_REQUESTED_ROLE_SCOPES = JSON.stringify(policy.roleScopes)
  if (policy.allowWrite) {
    out.CODEX_GW_WRITE_ALLOWLIST = mergeAllowlist(baseEnv.CODEX_GW_WRITE_ALLOWLIST ?? process.env.CODEX_GW_WRITE_ALLOWLIST ?? 'dev-pm', [
      parentAgentId,
      'agent-1',
      ...(policy.writeRequesters ?? []),
    ])
  }
  return out
}

function mergeAllowlist(raw: string, additions: string[]): string {
  return [...raw.split(','), ...additions]
    .map((s) => s.trim())
    .filter((s) => AGENT_ID_RE.test(s))
    .filter((s, i, all) => all.indexOf(s) === i)
    .join(',')
}

function parseMode(value: unknown, fallback: LaunchMode): LaunchMode {
  return value === 'webhook' || value === 'shim' ? value : fallback
}

function slugPart(value: unknown, fallback: string): string {
  if (typeof value !== 'string') return fallback
  const s = trimDashes(value.toLowerCase().replace(/[^a-z0-9]+/g, '-')).slice(0, 24)
  return trimDashes(s) || fallback
}

function trimDashes(s: string): string {
  return s.replace(/^-+|-+$/g, '')
}

async function defaultSpawn(cmd: string[], opts: { env: Record<string, string | undefined>; timeoutMs: number }): Promise<SpawnResult> {
  const proc = Bun.spawn(cmd, { stdout: 'pipe', stderr: 'pipe', env: opts.env })
  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    try { proc.kill() } catch {}
  }, opts.timeoutMs)
  try {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    return { exitCode, stdout, stderr, timedOut }
  } finally {
    clearTimeout(timer)
  }
}

function stripSlash(s: string): string {
  return s.replace(/\/+$/, '')
}

function trimText(s: string): string {
  const trimmed = s.trim()
  return trimmed.length <= 4000 ? trimmed : `${trimmed.slice(0, 4000)}...[truncated]`
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
