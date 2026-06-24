// codex_routing_governor.ts - advisory D5 routing governor for build work.
//
// This module is pure routing advice. It has no capital/fire authority and does
// not enforce operator overrides; callers own override policy and logging.
export type BuildRouteTarget = 'claude' | 'codex' | 'queue'

/** Provider names tracked by the governor's circuit breakers. */
export type GovernorProvider = 'claude' | 'codex'

/** Build task metadata used for advisory routing. */
export interface BuildRouteTask {
  /** Role requesting the build, such as orchestrator, standing_pm, or specialist. */
  role?: string
  /** Model-family fit for the work. */
  fit?: 'codex_strong' | 'claude_strong' | 'either'
  /** True when the task touches protected paths that Model B must not own. */
  touchesProtectedPaths?: boolean
  /** True when the task is peak-critical and should keep Claude headroom. */
  peakCritical?: boolean
}

/** Live pressure signals supplied by dev-pm. */
export interface BuildRouteSignals {
  /** Deterministic clock in epoch milliseconds. */
  now: number
  /** Whether the operator-defined peak-load window is active. */
  peakWindow: boolean
  /** Recent Anthropic 429/shed rate, 0..1. */
  anthropic429Rate: number
  /** Codex primary budget used percent. */
  codexBudgetPct: number
  /** Optional secondary Codex pressure bit. */
  codexSecondaryHigh?: boolean
}

/** Circuit-breaker state for one provider. */
export interface ProviderBreakerState {
  /** Consecutive shed outcomes. */
  consecutiveSheds: number
  /** Consecutive 429 outcomes. */
  consecutive429s: number
  /** Consecutive shed or 429 outcomes combined. */
  consecutiveFailures: number
  /** Epoch millis until which this breaker is open. Zero means closed. */
  openUntil: number
}

/** Mutable governor state carried by dev-pm between decisions. */
export interface GovernorState {
  /** Last non-initial target selected by routeBuild. */
  lastTarget?: BuildRouteTarget
  /** Hysteresis latch for Anthropic 429 pressure. */
  anthropic429High?: boolean
  /** Hysteresis latch for Codex budget pressure. */
  codexBudgetHigh?: boolean
  /** Per-provider circuit breakers. */
  breakers: Record<GovernorProvider, ProviderBreakerState>
  /** Last routeBuild/recordOutcome clock observed. */
  lastNow?: number
}

/** Governor thresholds and circuit-breaker settings. */
export interface GovernorCfg {
  /** Anthropic 429 rate at which the high-pressure latch opens. */
  anthropic429Hi: number
  /** Anthropic 429 rate at which the high-pressure latch closes. */
  anthropic429Lo: number
  /** Codex budget percentage at which the high-pressure latch opens. */
  codexBudgetHi: number
  /** Codex budget percentage at which the high-pressure latch closes. */
  codexBudgetLo: number
  /** Consecutive shed/429 outcomes needed to open a provider breaker. */
  breakerConsecutiveFailures: number
  /** Breaker cooldown in milliseconds. */
  breakerCooldownMs: number
  /** Advisory backoff emitted on queue decisions. */
  baseBackoffMs: number
  /** Maximum advisory backoff emitted on queue decisions. */
  maxBackoffMs: number
  /** Roles that reserve Claude headroom and route to Claude before peak-window spillover. */
  claudeReserveRoles: string[]
}

/** Default D5 governor configuration. */
export const DEFAULT_GOVERNOR_CFG: GovernorCfg = {
  anthropic429Hi: 0.08,
  anthropic429Lo: 0.03,
  codexBudgetHi: 92,
  codexBudgetLo: 85,
  breakerConsecutiveFailures: 3,
  breakerCooldownMs: 5 * 60_000,
  baseBackoffMs: 30_000,
  maxBackoffMs: 10 * 60_000,
  claudeReserveRoles: ['orchestrator', 'standing_pm', 'standing-pm'],
}

/**
 * Route a build task to claude, codex, or queue.
 *
 * ORDER implemented:
 * reserved/orchestrator roles -> Claude; protected paths -> Claude plus human;
 * peak-window non-critical spillover -> Codex; Anthropic 429 pressure -> Codex if
 * Codex is healthy; Codex pressure -> Claude or queue; both pressured -> queue;
 * codex_strong fit -> Codex; otherwise Claude. Breakers and hysteresis prevent
 * single-threshold flip-flopping.
 */
export function routeBuild(
  task: BuildRouteTask,
  signals: BuildRouteSignals,
  state: GovernorState,
  cfg?: Partial<GovernorCfg>,
): { target: BuildRouteTarget; reason: string; decision: object } {
  const c = withDefaults(cfg)
  ensureState(state)
  state.lastNow = signals.now

  state.anthropic429High = latchHigh(state.anthropic429High, signals.anthropic429Rate, c.anthropic429Hi, c.anthropic429Lo, false)
  state.codexBudgetHigh = latchHigh(state.codexBudgetHigh, signals.codexBudgetPct, c.codexBudgetHi, c.codexBudgetLo, true)

  const claudeBreakerOpen = breakerOpen(state, 'claude', signals.now)
  const codexBreakerOpen = breakerOpen(state, 'codex', signals.now)
  const claudePressured = claudeBreakerOpen || state.anthropic429High === true
  const codexPressured = codexBreakerOpen || state.codexBudgetHigh === true || signals.codexSecondaryHigh === true

  const select = (target: BuildRouteTarget, reason: string, extra: Record<string, unknown> = {}, hardClaude = false) => {
    let finalTarget = target
    let finalReason = reason

    if (target === 'codex' && codexPressured) {
      finalTarget = claudePressured ? 'queue' : 'claude'
      finalReason = codexBreakerOpen ? `${reason}:codex-breaker-open` : `${reason}:codex-pressured`
    }

    if (target === 'claude' && claudeBreakerOpen) {
      finalTarget = hardClaude ? 'queue' : codexPressured ? 'queue' : 'codex'
      finalReason = `${reason}:claude-breaker-open`
    }

    const decision = decisionRecord({
      task,
      signals,
      state,
      target: finalTarget,
      reason: finalReason,
      cfg: c,
      claudeBreakerOpen,
      codexBreakerOpen,
      claudePressured,
      codexPressured,
      extra,
    })
    state.lastTarget = finalTarget
    return { target: finalTarget, reason: finalReason, decision }
  }

  if (isClaudeReserveRole(task.role, c)) {
    return select('claude', 'reserved-role-claude', { rule: 'role-reserve' }, true)
  }

  if (task.touchesProtectedPaths) {
    return select('claude', 'protected-path-human-review', { human_review_required: true }, true)
  }

  if (claudePressured && codexPressured) {
    return select('queue', 'both-pressured', { backoff_ms: backoffMs(state, c) })
  }

  if (signals.peakWindow && !task.peakCritical) {
    return select('codex', 'peak-noncritical-spillover')
  }

  if (state.anthropic429High) {
    return select('codex', 'anthropic-429-high')
  }

  if (state.codexBudgetHigh || signals.codexSecondaryHigh) {
    return select('claude', signals.codexSecondaryHigh ? 'codex-secondary-high' : 'codex-budget-high')
  }

  if (task.fit === 'codex_strong') {
    return select('codex', 'fit-codex-strong')
  }

  if (task.fit === 'claude_strong') {
    return select('claude', 'fit-claude-strong')
  }

  return select('claude', 'default-claude')
}

/**
 * Record a provider outcome and update its circuit breaker.
 *
 * Optional cfg/now parameters keep tests deterministic while preserving the
 * requested simple call shape for production callers.
 */
export function recordOutcome(
  state: GovernorState,
  provider: GovernorProvider,
  outcome: 'ok' | 'shed' | '429',
  cfg?: Partial<GovernorCfg>,
  now = state.lastNow ?? Date.now(),
): GovernorState {
  const c = withDefaults(cfg)
  ensureState(state)
  state.lastNow = now

  const b = state.breakers[provider]
  if (outcome === 'ok') {
    b.consecutiveSheds = 0
    b.consecutive429s = 0
    b.consecutiveFailures = 0
    b.openUntil = 0
    return state
  }

  b.consecutiveFailures++
  if (outcome === 'shed') {
    b.consecutiveSheds++
    b.consecutive429s = 0
  } else {
    b.consecutive429s++
    b.consecutiveSheds = 0
  }

  if (b.consecutiveFailures >= c.breakerConsecutiveFailures) {
    b.openUntil = now + c.breakerCooldownMs
  }

  return state
}

/** Format a decision record as a compact JSON line suitable for dev-pm brain logging. */
export function formatDecisionLog(decision: object): string {
  return JSON.stringify(decision)
}

function withDefaults(cfg?: Partial<GovernorCfg>): GovernorCfg {
  const rawReserveRoles = cfg?.claudeReserveRoles ?? DEFAULT_GOVERNOR_CFG.claudeReserveRoles
  return {
    ...DEFAULT_GOVERNOR_CFG,
    ...(cfg ?? {}),
    claudeReserveRoles: rawReserveRoles.map(normalizeRole).filter(Boolean),
  }
}

function ensureState(state: GovernorState): void {
  state.breakers ??= {
    claude: emptyBreaker(),
    codex: emptyBreaker(),
  }
  state.breakers.claude ??= emptyBreaker()
  state.breakers.codex ??= emptyBreaker()
}

function emptyBreaker(): ProviderBreakerState {
  return { consecutiveSheds: 0, consecutive429s: 0, consecutiveFailures: 0, openUntil: 0 }
}

function latchHigh(current: boolean | undefined, value: number, hi: number, lo: number, hiInclusive: boolean): boolean {
  if (current === true) return value <= lo ? false : true
  return hiInclusive ? value >= hi : value > hi
}

function breakerOpen(state: GovernorState, provider: GovernorProvider, now: number): boolean {
  const b = state.breakers[provider]
  if (b.openUntil > now) return true
  if (b.openUntil && b.openUntil <= now) b.openUntil = 0
  return false
}

function isClaudeReserveRole(role: string | undefined, cfg: GovernorCfg): boolean {
  const normalized = normalizeRole(role)
  if (!normalized) return false
  return cfg.claudeReserveRoles.includes(normalized)
}

function normalizeRole(role: string | undefined): string {
  return (role ?? '').trim().toLowerCase()
}

function backoffMs(state: GovernorState, cfg: GovernorCfg): number {
  const failures = Math.max(state.breakers.claude.consecutiveFailures, state.breakers.codex.consecutiveFailures, 1)
  return Math.min(cfg.maxBackoffMs, cfg.baseBackoffMs * failures)
}

function decisionRecord(args: {
  task: BuildRouteTask
  signals: BuildRouteSignals
  state: GovernorState
  target: BuildRouteTarget
  reason: string
  cfg: GovernorCfg
  claudeBreakerOpen: boolean
  codexBreakerOpen: boolean
  claudePressured: boolean
  codexPressured: boolean
  extra: Record<string, unknown>
}): object {
  return {
    at: new Date(args.signals.now).toISOString(),
    target: args.target,
    reason: args.reason,
    task: args.task,
    signals: args.signals,
    latches: {
      anthropic429High: args.state.anthropic429High === true,
      codexBudgetHigh: args.state.codexBudgetHigh === true,
    },
    breakers: {
      claude: { ...args.state.breakers.claude, open: args.claudeBreakerOpen },
      codex: { ...args.state.breakers.codex, open: args.codexBreakerOpen },
    },
    pressure: {
      claude: args.claudePressured,
      codex: args.codexPressured,
    },
    cfg: {
      anthropic429Hi: args.cfg.anthropic429Hi,
      anthropic429Lo: args.cfg.anthropic429Lo,
      codexBudgetHi: args.cfg.codexBudgetHi,
      codexBudgetLo: args.cfg.codexBudgetLo,
      breakerConsecutiveFailures: args.cfg.breakerConsecutiveFailures,
      breakerCooldownMs: args.cfg.breakerCooldownMs,
    },
    ...args.extra,
  }
}
