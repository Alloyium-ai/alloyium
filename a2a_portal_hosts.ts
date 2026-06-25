// Host and fleet aggregation helpers for the A2A portal.
//
// Presence records are emitted by host processes and containers. Docker containers
// commonly report a 12-hex hostname, which is not useful to operators, so the portal
// normalizes raw hostnames into logical servers before computing dashboard totals.

export type LogicalHost = string
export type HostSource =
  | 'presence-logical-host'
  | 'configured-alias'
  | 'agent-id'
  | 'container-id-fallback'
  | 'raw-host'
  | 'unknown'

export type HostNormalization = { logical_host: LogicalHost; host_source: HostSource }

export const DEFAULT_KNOWN_HOSTS = ['local', 'gpubox', 'srv01', 'homedevbox'] as const

const DEFAULT_HOST_ALIASES: Record<string, LogicalHost> = {
  localhost: 'local',
  dev01: 'gpubox',
  devbox01: 'homedevbox',
}

const CONTAINER_ID_RE = /^[0-9a-f]{12}$/

export function parseKnownHosts(spec: string | undefined | null): LogicalHost[] {
  const seen = new Set<string>()
  const out: string[] = []
  const raw = String(spec || '').trim()
  const values = raw ? raw.split(/[,\s]+/) : [...DEFAULT_KNOWN_HOSTS]
  for (const value of values) {
    const host = cleanHost(value)
    if (host && host !== 'unknown' && !seen.has(host)) {
      seen.add(host)
      out.push(host)
    }
  }
  return out.length ? out : [...DEFAULT_KNOWN_HOSTS]
}

export function parseHostAliases(spec: string | undefined | null, knownHosts = parseKnownHosts(null)): Record<string, LogicalHost> {
  const known = new Set(knownHosts)
  const out: Record<string, LogicalHost> = {}
  if (!spec) return out
  for (const pair of String(spec).split(/[,\s]+/)) {
    if (!pair) continue
    const [rawValue, canonicalValue] = pair.split('=')
    const raw = cleanHost(rawValue)
    const canonical = cleanHost(canonicalValue)
    if (raw && canonical && known.has(canonical)) out[raw] = canonical
  }
  return out
}

export type NormalizeOpts = {
  aliases?: Record<string, LogicalHost>
  knownHosts?: LogicalHost[]
  containerHostFallback?: LogicalHost
  logicalHost?: string
}

export function primaryAgentId(id: unknown): string {
  const s = String(id || '')
  const i = s.indexOf('-sub-')
  return i > 0 ? s.slice(0, i) : s
}

export function normalizeHost(rawHost: unknown, agentId: unknown, opts: NormalizeOpts = {}): HostNormalization {
  const knownHosts = opts.knownHosts?.length ? opts.knownHosts.map(cleanHost).filter(Boolean) : parseKnownHosts(null)
  const known = new Set(knownHosts)
  const raw = cleanHost(rawHost)
  const explicit = cleanHost(opts.logicalHost)
  const aliases = { ...DEFAULT_HOST_ALIASES, ...(opts.aliases || {}) }
  const prim = primaryAgentId(String(agentId || '').trim().toLowerCase())
  const fallback = cleanHost(opts.containerHostFallback) || 'local'

  if (explicit && known.has(explicit)) return { logical_host: explicit, host_source: 'presence-logical-host' }
  if (raw && aliases[raw] && known.has(aliases[raw])) return { logical_host: aliases[raw], host_source: 'configured-alias' }

  for (const host of knownHosts) {
    if (host === 'local' || host === 'unknown') continue
    const token = escapeRegExp(host)
    if (new RegExp(`(?:^|-)${token}(?:-|$)`).test(prim)) return { logical_host: host, host_source: 'agent-id' }
  }

  if (raw && known.has(raw)) return { logical_host: raw, host_source: 'raw-host' }
  if (CONTAINER_ID_RE.test(raw) && known.has(fallback)) return { logical_host: fallback, host_source: 'container-id-fallback' }
  if (CONTAINER_ID_RE.test(raw)) return { logical_host: knownHosts[0] ?? 'local', host_source: 'container-id-fallback' }
  return { logical_host: 'unknown', host_source: 'unknown' }
}

export type RawPresence = { id: string; host?: string; ttl?: number; logical_host?: string }
export type FleetPresence = {
  id: string
  raw_host: string
  logical_host: LogicalHost
  host_source: HostSource
  ttl: number
  online: boolean
  is_pm: boolean
  is_sub_agent: boolean
  primary_id: string
  primary_present: boolean
}

export function toFleetPresence(item: RawPresence, opts: NormalizeOpts = {}): FleetPresence {
  const raw_host = String(item?.host || '')
  const id = String(item?.id || '')
  const { logical_host, host_source } = normalizeHost(raw_host, id, { ...opts, logicalHost: item?.logical_host })
  const ttl = Number(item?.ttl) || 0
  const primary_id = primaryAgentId(id)
  return {
    id,
    raw_host,
    logical_host,
    host_source,
    ttl,
    online: ttl > 0,
    is_pm: /(?:^|-)pm(?:-|$)/.test(id),
    is_sub_agent: primary_id !== id,
    primary_id,
    primary_present: false,
  }
}

export function normalizePresence(items: RawPresence[], opts: NormalizeOpts = {}): FleetPresence[] {
  const normalized = (items || []).map(it => toFleetPresence(it, opts))
  const presentIds = new Set(normalized.filter(p => p.online).map(p => p.id))
  return normalized.map(p => ({ ...p, primary_present: presentIds.has(p.primary_id) }))
}

export type FleetStats = {
  servers: number
  agents: number
  online_agents: number
  offline_agents: number
  pms: number
  teams: number
  unmapped: number
  messages: number
  active_agents: number
  tasks: number | null
  open_tasks: number | null
  done_tasks: number | null
}

export type FleetServer = {
  logical_host: LogicalHost
  label: string
  agents: number
  pms: number
  teams: number
  warned_raw_hosts?: string[]
}

export type FleetTeam = {
  primary_id: string
  logical_host: LogicalHost
  primary_present: boolean
  sub_agents: number
  agents: number
}

export type FleetActivityMessage = {
  from?: string
  to?: string
  t?: number
}

export type FleetActivity = {
  messages: number
  active_agents: number
  agents: Array<{ id: string; messages: number; last_t: number }>
  trend: Array<{ label: string; start_t: number; messages: number }>
}

export type FleetTaskboardTotals = {
  enabled: boolean
  project_id: number
  total: number | null
  open: number | null
  done: number | null
  by_status: Record<string, number>
  error?: string
}

export type Fleet = {
  presence: FleetPresence[]
  stats: FleetStats
  servers: FleetServer[]
  teams: FleetTeam[]
  unmapped: Array<{ id: string; raw_host: string }>
  activity: FleetActivity
  taskboard: FleetTaskboardTotals
}

export type FleetExtras = {
  messages?: FleetActivityMessage[]
  taskboard?: FleetTaskboardTotals
}

export function computeFleet(rawPresence: RawPresence[], opts: NormalizeOpts = {}, extras: FleetExtras = {}): Fleet {
  const knownHosts = opts.knownHosts?.length ? opts.knownHosts : parseKnownHosts(null)
  const presence = normalizePresence(rawPresence, { ...opts, knownHosts })
  const online = presence.filter(p => p.online)
  const offline = presence.filter(p => !p.online)
  const serverHosts = new Set(online.filter(p => p.logical_host !== 'unknown').map(p => p.logical_host))
  const subAgentsByPrimary = new Map<string, FleetPresence[]>()

  for (const p of online) {
    if (!p.is_sub_agent) continue
    const arr = subAgentsByPrimary.get(p.primary_id) ?? []
    arr.push(p)
    subAgentsByPrimary.set(p.primary_id, arr)
  }

  const teams: FleetTeam[] = [...subAgentsByPrimary.entries()].map(([primary_id, subs]) => {
    const primary = online.find(p => p.id === primary_id)
    const hostCounts = new Map<LogicalHost, number>()
    for (const p of subs) hostCounts.set(p.logical_host, (hostCounts.get(p.logical_host) ?? 0) + 1)
    if (primary) hostCounts.set(primary.logical_host, (hostCounts.get(primary.logical_host) ?? 0) + 1)
    const logical_host = [...hostCounts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0] ?? 'unknown'
    return {
      primary_id,
      logical_host,
      primary_present: !!primary,
      sub_agents: subs.length,
      agents: subs.length + (primary ? 1 : 0),
    }
  }).sort((a, b) => a.primary_id.localeCompare(b.primary_id))

  const hostOrder = new Map(knownHosts.map((host, i) => [host, i]))
  const servers: FleetServer[] = [...serverHosts]
    .map((logical_host) => {
      const agents = online.filter(p => p.logical_host === logical_host)
      const hostTeams = teams.filter(t => t.logical_host === logical_host)
      const warned = [...new Set(agents.filter(p => p.host_source === 'container-id-fallback').map(p => p.raw_host).filter(Boolean))].sort()
      return {
        logical_host,
        label: logical_host,
        agents: agents.length,
        pms: agents.filter(p => p.is_pm).length,
        teams: hostTeams.length,
        ...(warned.length ? { warned_raw_hosts: warned } : {}),
      }
    })
    .sort((a, b) => (hostOrder.get(a.logical_host) ?? 999) - (hostOrder.get(b.logical_host) ?? 999) || a.logical_host.localeCompare(b.logical_host))

  const unmapped = online
    .filter(p => p.logical_host === 'unknown')
    .map(p => ({ id: p.id, raw_host: p.raw_host }))
    .sort((a, b) => a.id.localeCompare(b.id))
  const activity = computeFleetActivity(extras.messages ?? [])
  const taskboard = extras.taskboard ?? disabledTaskboardTotals()

  return {
    presence,
    stats: {
      servers: serverHosts.size,
      agents: online.length,
      online_agents: online.length,
      offline_agents: offline.length,
      pms: online.filter(p => p.is_pm).length,
      teams: teams.length,
      unmapped: unmapped.length,
      messages: activity.messages,
      active_agents: activity.active_agents,
      tasks: taskboard.total,
      open_tasks: taskboard.open,
      done_tasks: taskboard.done,
    },
    servers,
    teams,
    unmapped,
    activity,
    taskboard,
  }
}

export function computeFleetActivity(messages: FleetActivityMessage[]): FleetActivity {
  const agents = new Map<string, { id: string; messages: number; last_t: number }>()
  for (const m of messages || []) {
    for (const raw of [m.from, m.to]) {
      const id = String(raw || '').trim()
      if (!id || id === '?' || id.startsWith('topic:')) continue
      const cur = agents.get(id) ?? { id, messages: 0, last_t: 0 }
      cur.messages += 1
      cur.last_t = Math.max(cur.last_t, Number(m.t) || 0)
      agents.set(id, cur)
    }
  }
  const rows = [...agents.values()].sort((a, b) => b.messages - a.messages || b.last_t - a.last_t || a.id.localeCompare(b.id))
  return { messages: messages.length, active_agents: rows.length, agents: rows, trend: computeActivityTrend(messages) }
}

function computeActivityTrend(messages: FleetActivityMessage[]): Array<{ label: string; start_t: number; messages: number }> {
  const now = Date.now()
  const bucketMs = 60 * 60 * 1000
  const buckets = Array.from({ length: 12 }, (_, i) => {
    const start = now - (11 - i) * bucketMs
    const d = new Date(start)
    return {
      label: `${String(d.getHours()).padStart(2, '0')}:00`,
      start_t: start,
      messages: 0,
    }
  })
  const first = buckets[0]?.start_t ?? now
  for (const m of messages || []) {
    const t = Number(m.t) || 0
    if (t < first || t > now + bucketMs) continue
    const idx = Math.min(buckets.length - 1, Math.max(0, Math.floor((t - first) / bucketMs)))
    buckets[idx].messages += 1
  }
  return buckets
}

export function disabledTaskboardTotals(projectId = 0, error = 'disabled'): FleetTaskboardTotals {
  return { enabled: false, project_id: projectId, total: null, open: null, done: null, by_status: {}, error }
}

function cleanHost(value: unknown): string {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9_.-]+/g, '-').replace(/^-+|-+$/g, '')
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
