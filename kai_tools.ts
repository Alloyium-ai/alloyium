// agent-kai bridge tools for the A2A webhook MCP bridge.
//
// OPTIONAL integration. This module fronts an external "kai" session daemon — a
// long-running conversational-session host + cron job scheduler — over its
// WebSocket + REST API, exposing four MCP tools so A2A agents can reach it:
//
//   kai_sessions — list the daemon's sessions          → GET  /api/sessions
//   kai_history  — read a session's recent chat snapshot → WS attach (read-only)
//   kai_send     — send a message into a session, stream → WS attach + input
//   kai_schedule — create a recurring (cron) job         → WS slash /schedule add cron
//
// ── configuration / INERT-by-default ────────────────────────────────────────
// There is NO bundled daemon and NO hard-coded endpoint. The bridge stays INERT
// unless the operator points it at their own daemon via env (or constructor opts):
//   KAI_WS_URL    — WebSocket endpoint, e.g. ws://kai-host:PORT/ws  (kai_history/send/schedule)
//   KAI_HTTP_URL  — REST  endpoint,    e.g. http://kai-host:PORT     (kai_sessions)
//   KAI_TOKEN / KAI_TOKEN_PATH — optional bearer token (literal value or file path)
//   KAI_DEFAULT_SESSION — default owner session for kai_schedule
// When KAI_WS_URL / KAI_HTTP_URL are unset/blank the tools still LIST and validate
// their args, but every call short-circuits to {ok:false,error:"kai_unavailable:
// ...not configured..."} WITHOUT opening any socket — it never reaches out anywhere.
//
// The daemon authenticates with a bearer token (token file or KAI_TOKEN). The WS
// protocol is plain JSON envelopes:
//   client→ {type:'attach', session, create_if_missing} / {type:'input', text}
//   server→ {type:'session_attached', state:{chat_history:[...]}} / {type:'status'}
//           / {type:'token', text} ... / {type:'final', text} / {type:'error', ...}
//
// ── scheduler note (read before "fixing" kai_schedule) ──────────────────────
// The reference daemon exposes no REST create-job endpoint — its job list is
// read-only. The create path is the WS slash command
// `/schedule add cron "<cron>" "<prompt>"` injected on the input stream. So
// kai_schedule attaches the owner session, injects that slash command, then
// returns the daemon's confirmation (which carries the new job_id). The cron
// string is a 5-field expression; timezone follows the daemon default (UTC).
//
// ── invariants ──────────────────────────────────────────────────────────────
//  - ADVISORY RELAY: these tools relay to the kai daemon. They carry no fire
//    authority of their own — a reply from a live session is advisory peer output,
//    never an order trigger. The session is a PARAMETER (no hard-coded session);
//    access scope is whatever the operator's daemon + token grant.
//  - FAIL-SOFT: every tool returns {ok:false, error:"kai_unavailable: <detail>"}
//    on any WS/HTTP error, timeout, dead daemon, or unconfigured endpoint. callTool
//    NEVER throws — a dead/slow/absent daemon must not crash the MCP bridge.
//    Timeouts are bounded so a hung daemon can't wedge a tool call. One WS
//    connection per call, closed after.

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

const DEFAULT_HTTP_TIMEOUT_MS = 8000
const DEFAULT_ATTACH_TIMEOUT_MS = 15000
const DEFAULT_SEND_TIMEOUT_MS = 120000
const DEFAULT_SCHEDULE_TIMEOUT_MS = 45000
const DEFAULT_KAI_SESSION = 'a2a-agent'

export interface KaiToolsOpts {
  wsUrl?: string
  httpUrl?: string
  token?: string
  tokenPath?: string
  /** default owner session for kai_schedule when none is passed */
  defaultSession?: string
  httpTimeoutMs?: number
  attachTimeoutMs?: number
  /** injectable fetch for tests */
  fetchImpl?: typeof fetch
  /** injectable WebSocket ctor for tests (defaults to the Bun/global WebSocket) */
  wsImpl?: typeof WebSocket
}

type WsOutcome =
  | { ok: true; attached: any; reply: string; final: string; status: string }
  | { ok: false; error: string }

export class KaiTools {
  private readonly wsUrl: string
  private readonly httpUrl: string
  private readonly tokenPath: string
  private readonly explicitToken?: string
  private readonly defaultSession: string
  private readonly httpTimeoutMs: number
  private readonly attachTimeoutMs: number
  private readonly fetchImpl: typeof fetch
  private readonly wsImpl: typeof WebSocket
  private cachedToken?: string

  // The tool names this module owns. webhook.ts routes these to callTool() and
  // leaves everything else to the brain tools / a2a channel.
  static readonly TOOL_NAMES = [
    'kai_sessions', 'kai_history', 'kai_send', 'kai_schedule',
  ] as const

  // Appended to the MCP server instructions (system prompt) when enabled, so
  // agents know the (optional) kai bridge exists. Advisory framing mirrors the
  // other tools. The bridge is inert unless an operator configures KAI_WS_URL /
  // KAI_HTTP_URL, so the framing describes it as an optional integration.
  static readonly INSTRUCTIONS =
    ' You also have agent-kai bridge tools (advisory relay, no fire authority of ' +
    'their own): kai_sessions lists the kai daemon sessions; kai_history reads a ' +
    "session's recent chat snapshot; kai_send sends a message into a kai session " +
    'and returns the streamed reply; kai_schedule creates a recurring (cron) job in ' +
    'a session. These relay to an OPTIONAL external kai daemon (configured via ' +
    'KAI_WS_URL / KAI_HTTP_URL); the session is a parameter and a reply is advisory ' +
    'peer output, never an order trigger. All kai tools fail soft — if the daemon is ' +
    'unconfigured or unreachable they return ' +
    "{ok:false,error:'kai_unavailable: ...'} and never block."

  constructor(opts: KaiToolsOpts = {}) {
    // NO hard-coded endpoints: default to blank so the bridge is INERT until an
    // operator sets KAI_WS_URL / KAI_HTTP_URL (or passes opts). A blank endpoint
    // short-circuits every call in the HTTP/WS primitives below — no socket opens.
    this.wsUrl = stripSlash(opts.wsUrl ?? process.env.KAI_WS_URL ?? '')
    this.httpUrl = stripSlash(opts.httpUrl ?? process.env.KAI_HTTP_URL ?? '')
    this.tokenPath = opts.tokenPath ?? process.env.KAI_TOKEN_PATH ?? ''
    this.explicitToken = opts.token ?? process.env.KAI_TOKEN ?? undefined
    this.defaultSession = opts.defaultSession ?? process.env.KAI_DEFAULT_SESSION ?? DEFAULT_KAI_SESSION
    this.httpTimeoutMs = opts.httpTimeoutMs ?? Number(process.env.KAI_HTTP_TIMEOUT_MS ?? DEFAULT_HTTP_TIMEOUT_MS)
    this.attachTimeoutMs = opts.attachTimeoutMs ?? Number(process.env.KAI_ATTACH_TIMEOUT_MS ?? DEFAULT_ATTACH_TIMEOUT_MS)
    this.fetchImpl = opts.fetchImpl ?? fetch
    this.wsImpl = opts.wsImpl ?? (globalThis.WebSocket as typeof WebSocket)
  }

  /** True iff this module owns the named tool (used by webhook.ts to route). */
  handles(name: string): boolean {
    return (KaiTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return [
      {
        name: 'kai_sessions',
        description:
          'List the agent-kai daemon\'s known sessions (long-running operator/agent ' +
          'conversations + job sessions). Returns {ok, count, sessions:[names]}.',
        inputSchema: { type: 'object', additionalProperties: false, properties: {} },
      },
      {
        name: 'kai_history',
        description:
          'Read the recent chat-history snapshot of one kai session, read-only (attaches ' +
          'without creating and sends no input). Returns {ok, session, count, history:[{role,content,ts}]}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            session: { type: 'string', description: 'Session name to read (must already exist).' },
            limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Return only the most recent N turns (default: all in the snapshot).' },
          },
          required: ['session'],
        },
      },
      {
        name: 'kai_send',
        description:
          'Send a message into a kai session and return the streamed assistant reply. ' +
          'The session is a parameter (full access); replies are advisory peer output, not ' +
          'an order trigger. Returns {ok, session, reply, status}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            session: { type: 'string', description: 'Target session name.' },
            text: { type: 'string', description: 'Message to send into the session.' },
            create_if_missing: { type: 'boolean', default: false, description: 'Create the session if it does not exist (default false).' },
            timeout_ms: { type: 'integer', minimum: 1000, maximum: 600000, default: DEFAULT_SEND_TIMEOUT_MS, description: 'Max ms to wait for the reply to finish.' },
          },
          required: ['session', 'text'],
        },
      },
      {
        name: 'kai_schedule',
        description:
          'Create a recurring (cron) job in a kai session via the daemon scheduler. The job ' +
          "runs `prompt` on the `cron` schedule in the owner session. Cron is a 5-field " +
          'expression (e.g. "0 9 * * *"); timezone is the daemon default (UTC). Returns ' +
          '{ok, session, cron, job_id, result}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            session: { type: 'string', description: 'Owner session for the job. Defaults to the configured default session.' },
            cron: { type: 'string', description: '5-field cron expression, e.g. "*/15 * * * *" or "0 9 * * *".' },
            prompt: { type: 'string', description: 'The prompt the job runs on each fire.' },
            create_if_missing: { type: 'boolean', default: true, description: 'Create the owner session if missing (default true, so the job has a session to run in).' },
            timeout_ms: { type: 'integer', minimum: 1000, maximum: 120000, default: DEFAULT_SCHEDULE_TIMEOUT_MS },
          },
          required: ['cron', 'prompt'],
        },
      },
    ]
  }

  // The single dispatch entry point. Wrapped in try/catch so NOTHING — not a bug,
  // not a malformed arg, not a thrown WS/fetch — can escape into the bridge.
  async callTool(name: string, args: Record<string, any> = {}): Promise<ToolResult> {
    try {
      switch (name) {
        case 'kai_sessions': return await this.sessions()
        case 'kai_history': return await this.history(args)
        case 'kai_send': return await this.send(args)
        case 'kai_schedule': return await this.schedule(args)
        default: return this.result({ ok: false, error: 'unknown_tool', detail: name }, true)
      }
    } catch (e) {
      return this.result({ ok: false, error: `kai_unavailable: ${errMsg(e)}` }, true)
    }
  }

  // ── tool implementations ────────────────────────────────────────────────

  private async sessions(): Promise<ToolResult> {
    const r = await this.httpReq('GET', '/api/sessions')
    if (!r.ok) return this.result({ ok: false, error: r.error }, true)
    const parsed = parseJson<{ sessions?: Array<{ name?: string } | string> }>(r.text)
    const rows = Array.isArray(parsed?.sessions) ? parsed!.sessions : []
    const names = rows
      .map((s) => (typeof s === 'string' ? s : str((s as any)?.name)))
      .filter((n) => !!n)
    return this.result({ ok: true, count: names.length, sessions: names })
  }

  private async history(args: Record<string, any>): Promise<ToolResult> {
    const session = str(args.session)
    if (!session) return this.result({ ok: false, error: 'bad_args: session required' }, true)
    const limit = args.limit === undefined ? undefined : clampInt(args.limit, 1, 200, 0)

    const out = await this.runWs({
      session,
      createIfMissing: false,
      input: undefined,
      collectReply: false,
      timeoutMs: this.attachTimeoutMs,
    })
    if (!out.ok) return this.result({ ok: false, error: out.error }, true)

    let history = extractHistory(out.attached)
    if (limit && limit > 0 && history.length > limit) history = history.slice(-limit)
    return this.result({ ok: true, session, count: history.length, history })
  }

  private async send(args: Record<string, any>): Promise<ToolResult> {
    const session = str(args.session)
    const text = typeof args.text === 'string' ? args.text : ''
    if (!session) return this.result({ ok: false, error: 'bad_args: session required' }, true)
    if (!text.trim()) return this.result({ ok: false, error: 'bad_args: text required' }, true)
    const createIfMissing = args.create_if_missing === true
    const timeoutMs = clampInt(args.timeout_ms, 1000, 600000, DEFAULT_SEND_TIMEOUT_MS)

    const out = await this.runWs({
      session,
      createIfMissing,
      input: text,
      collectReply: true,
      timeoutMs,
    })
    if (!out.ok) return this.result({ ok: false, error: out.error }, true)
    const reply = out.final.trim() ? out.final : out.reply
    return this.result({ ok: true, session, reply, status: out.status || 'idle' })
  }

  private async schedule(args: Record<string, any>): Promise<ToolResult> {
    const session = str(args.session) || this.defaultSession
    const cron = str(args.cron)
    const prompt = typeof args.prompt === 'string' ? args.prompt : ''
    if (!cron) return this.result({ ok: false, error: 'bad_args: cron required' }, true)
    if (!prompt.trim()) return this.result({ ok: false, error: 'bad_args: prompt required' }, true)
    // create_if_missing defaults TRUE here: a job needs an existing owner session to run.
    const createIfMissing = args.create_if_missing !== false
    const timeoutMs = clampInt(args.timeout_ms, 1000, 120000, DEFAULT_SCHEDULE_TIMEOUT_MS)

    // The only daemon create-job path is the WS slash command (see scheduler note
    // at the top of file). shQuote so cron (which has spaces) and prompt survive
    // the daemon's shlex.split intact as parts[3] and parts[4].
    const command = `/schedule add cron ${shQuote(cron)} ${shQuote(prompt)}`
    const out = await this.runWs({
      session,
      createIfMissing,
      input: command,
      collectReply: true,
      timeoutMs,
    })
    if (!out.ok) return this.result({ ok: false, error: out.error }, true)
    const resultText = out.final.trim() ? out.final : out.reply
    const jobId = (resultText.match(/job_[0-9_]+_[0-9a-f]+/) ?? [])[0]
    return this.result({ ok: true, session, cron, prompt, job_id: jobId, result: resultText })
  }

  // ── WS primitive ──────────────────────────────────────────────────────────

  // One fail-soft WS round-trip: connect → attach → (optional) input → collect.
  //   collectReply=false (history): resolve on session_attached (snapshot only).
  //   collectReply=true  (send/schedule): resolve on `final` (or `error`), after
  //                       accumulating any streamed `token` text.
  // NEVER throws — every failure path resolves to {ok:false, error:"kai_unavailable…"}.
  private runWs(params: {
    session: string
    createIfMissing: boolean
    input?: string
    collectReply: boolean
    timeoutMs: number
  }): Promise<WsOutcome> {
    const { session, createIfMissing, input, collectReply, timeoutMs } = params
    return new Promise<WsOutcome>((resolve) => {
      // INERT when unconfigured: no KAI_WS_URL ⇒ short-circuit before opening a socket.
      if (!this.wsUrl) return resolve({ ok: false, error: 'kai_unavailable: kai bridge not configured (set KAI_WS_URL)' })
      const token = this.getToken()
      if (!token) return resolve({ ok: false, error: 'kai_unavailable: no daemon token (set KAI_TOKEN or KAI_TOKEN_PATH)' })

      let ws: WebSocket
      try {
        ws = new this.wsImpl(this.wsConnectUrl(token))
      } catch (e) {
        return resolve({ ok: false, error: `kai_unavailable: ${errMsg(e)}` })
      }

      let settled = false
      let attached: any = null
      let inputSent = false
      let reply = ''
      let finalText = ''
      let status = ''

      const finish = (val: WsOutcome) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        try { ws.close() } catch { /* noop */ }
        resolve(val)
      }
      const timer = setTimeout(
        () => finish({ ok: false, error: `kai_unavailable: timeout_after_${timeoutMs}ms` }),
        timeoutMs,
      )

      ws.onopen = () => {
        try {
          ws.send(JSON.stringify({ type: 'attach', session, create_if_missing: createIfMissing }))
        } catch (e) {
          finish({ ok: false, error: `kai_unavailable: ${errMsg(e)}` })
        }
      }

      ws.onmessage = (ev: MessageEvent) => {
        const m = parseJson<any>(toText(ev.data))
        if (!m || typeof m !== 'object') return
        switch (m.type) {
          case 'error':
            finish({ ok: false, error: `kai_error: ${str(m.message) || str(m.code) || 'error'}` })
            return
          case 'session_attached':
            attached = m
            if (input) {
              if (!inputSent) {
                inputSent = true
                try {
                  ws.send(JSON.stringify({ type: 'input', text: input }))
                } catch (e) {
                  finish({ ok: false, error: `kai_unavailable: ${errMsg(e)}` })
                }
              }
            } else {
              // history: the snapshot is all we need.
              finish({ ok: true, attached, reply, final: finalText, status })
            }
            return
          case 'token':
            if (typeof m.text === 'string') reply += m.text
            return
          case 'final':
            finalText = typeof m.text === 'string' ? m.text : ''
            if (collectReply) finish({ ok: true, attached, reply, final: finalText, status })
            return
          case 'status':
            status = str(m.activity) || status
            return
          default:
            return
        }
      }

      ws.onerror = () => finish({ ok: false, error: 'kai_unavailable: ws_error' })
      ws.onclose = () => finish({ ok: false, error: 'kai_unavailable: ws_closed_before_reply' })
    })
  }

  // ── HTTP primitive ──────────────────────────────────────────────────────

  private async httpReq(method: string, path: string): Promise<{ ok: true; text: string } | { ok: false; error: string }> {
    // INERT when unconfigured: no KAI_HTTP_URL ⇒ short-circuit before any network.
    if (!this.httpUrl) return { ok: false, error: 'kai_unavailable: kai bridge not configured (set KAI_HTTP_URL)' }
    const token = this.getToken()
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), this.httpTimeoutMs)
    try {
      const headers: Record<string, string> = { 'content-type': 'application/json' }
      if (token) headers['authorization'] = `Bearer ${token}`
      const res = await this.fetchImpl(`${this.httpUrl}${path}`, { method, headers, signal: ctrl.signal })
      const text = await res.text()
      if (!res.ok) return { ok: false, error: `kai_unavailable: http_${res.status}: ${text.slice(0, 200)}` }
      return { ok: true, text }
    } catch (e) {
      const msg = e instanceof Error && e.name === 'AbortError' ? `timeout_after_${this.httpTimeoutMs}ms` : errMsg(e)
      return { ok: false, error: `kai_unavailable: ${msg}` }
    } finally {
      clearTimeout(timer)
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private getToken(): string | undefined {
    if (this.explicitToken) return this.explicitToken
    if (this.cachedToken) return this.cachedToken
    if (!this.tokenPath) return undefined
    try {
      // Bun: Node fs is available. Read lazily so a missing file fails soft per-call.
      const fs = require('fs') as typeof import('fs')
      const raw = fs.readFileSync(this.tokenPath, 'utf-8').trim()
      if (raw) this.cachedToken = raw
      return this.cachedToken
    } catch {
      return undefined
    }
  }

  private wsConnectUrl(token: string): string {
    const sep = this.wsUrl.includes('?') ? '&' : '?'
    return `${this.wsUrl}${sep}token=${encodeURIComponent(token)}`
  }

  private result(obj: unknown, isError = false): ToolResult {
    return { content: [{ type: 'text', text: JSON.stringify(obj) }], isError }
  }
}

// ── module-local pure helpers ─────────────────────────────────────────────

function stripSlash(u: string): string { return u.replace(/\/+$/, '') }
function str(v: unknown): string { return typeof v === 'string' ? v.trim() : '' }
function errMsg(e: unknown): string { return e instanceof Error ? e.message : String(e) }

function toText(data: unknown): string {
  if (typeof data === 'string') return data
  try {
    if (data instanceof ArrayBuffer) return new TextDecoder().decode(data)
    if (ArrayBuffer.isView(data as any)) return new TextDecoder().decode(data as ArrayBufferView)
    return String(data)
  } catch {
    return ''
  }
}

function clampInt(v: unknown, min: number, max: number, dflt: number): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return dflt
  return Math.min(max, Math.max(min, Math.trunc(n)))
}

function parseJson<T>(text: string): T | undefined {
  try { return JSON.parse(text) as T } catch { return undefined }
}

// Pull the chat-history rows out of a session_attached envelope. Tolerant of a
// missing/short snapshot (daemon may omit older turns — chat_history_omitted).
function extractHistory(attached: any): Array<{ role: string; content: string; ts?: string | null }> {
  const rows = attached?.state?.chat_history
  if (!Array.isArray(rows)) return []
  return rows.map((r: any) => ({
    role: str(r?.role),
    content: typeof r?.content === 'string' ? r.content : '',
    ts: r?.ts ?? null,
  }))
}

// POSIX single-quote shell escaping, compatible with the daemon's shlex.split.
// Bare token when it's all safe chars; otherwise single-quote and escape any '.
function shQuote(s: string): string {
  if (s === '') return "''"
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(s)) return s
  return "'" + s.replace(/'/g, "'\\''") + "'"
}
