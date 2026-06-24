// Agent-brain memory / RAG / skillpack tools for the A2A webhook MCP bridge.
//
// Closes the substrate Phase-1 gap: a2a Claude agents can message each other
// (a2a-channel.ts) but cannot SAVE or RECALL knowledge — so they re-derive
// things (e.g. the bus trust model) every session instead of remembering them.
// This module fronts the LIVE agent-brain HTTP API with five MCP tools:
//
//   a2a_remember   — save a markdown note          → POST /api/pages
//   a2a_recall     — semantic search (RAG)          → POST /api/search
//   a2a_brain_get  — fetch a page by slug           → GET  /md/<source>/<slug>
//   a2a_skill_save — save a skillpack/procedure      → brain page (see gbrain note)
//   a2a_skill_get  — fetch a skillpack by name       → brain page (see gbrain note)
//
// brain-notepad (FastAPI, default :8787) is the canonical system of record:
// markdown pages + FTS5/embedding search. See agent-brain-notepad/brain_notepad/api.py.
//
// ── gbrain skillpack note (read before "fixing" the skill tools) ────────────
// The dedicated gbrain skillpack store exposes skillpacks ONLY via an
// OAuth-2.1-gated MCP-over-HTTP surface (/mcp) plus a git-file CLI — there is
// NO plain REST save/get/list to front from a fail-soft fetch tool — and it is
// not currently running on GBRAIN_URL (:8080 is an unrelated OpenLLM gateway).
// Rather than invent a gbrain endpoint, skillpacks are persisted as brain-notepad
// pages under source=SKILL_SOURCE (default "skills"), type="skillpack". This makes
// the retro→skillpack loop work TODAY against the live brain; every skill result
// carries backend:"brain-notepad" so the provenance is explicit. GBRAIN_URL is
// retained for a future migration to a dedicated gbrain REST/MCP-bridge surface.
//
// ── invariants ──────────────────────────────────────────────────────────────
//  - ADVISORY-ONLY: these are memory tools only. No fire authority, no executor
//    subjects, no order triggers — nothing here can move capital.
//  - FAIL-SOFT: every tool returns {ok:false, error:"brain_unavailable: <detail>"}
//    (or "not_found") on any network error, timeout, or HTTP error. callTool
//    NEVER throws — a dead/slow brain must not crash the MCP bridge. Timeouts
//    are short (~5s) so a hung brain can't wedge a tool call.
//  - Source scoping: notes default to BRAIN_SOURCE (default "a2a"); callers may
//    pass `source`. NOTE (P2 follow-up, do NOT build now): an exposure /
//    alpha-leak denylist guard on a2a_recall — so a recall can't surface a
//    sensitive source to the wrong peer — is a documented follow-up. The
//    `source` param + this comment are the seam for it.

type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

type ReqResult =
  | { ok: true; status: number; text: string }
  | { ok: false; kind: 'network' | 'http'; status?: number; error: string }

const DEFAULT_TIMEOUT_MS = 5000
const SNIPPET_CAP = 400

export interface BrainToolsOpts {
  brainUrl?: string
  gbrainUrl?: string
  apiToken?: string
  /** default note source (override per-call with the `source` arg) */
  source?: string
  /** source skillpacks are stored under (brain-notepad backend) */
  skillSource?: string
  timeoutMs?: number
  /** injectable fetch for tests */
  fetchImpl?: typeof fetch
}

export class BrainTools {
  private readonly brainUrl: string
  private readonly gbrainUrl: string
  private readonly apiToken?: string
  private readonly source: string
  private readonly skillSource: string
  private readonly timeoutMs: number
  private readonly fetchImpl: typeof fetch

  // The tool names this module owns. webhook.ts routes these to callTool() and
  // leaves everything else to the a2a channel.
  static readonly TOOL_NAMES = [
    'a2a_remember', 'a2a_recall', 'a2a_brain_get', 'a2a_skill_save', 'a2a_skill_get',
  ] as const

  // Appended to the MCP server instructions (system prompt) when enabled, so
  // agents know memory exists. Advisory framing mirrors A2AChannel.INSTRUCTIONS.
  static readonly INSTRUCTIONS =
    ' You also have agent-brain memory tools (advisory, no fire authority): ' +
    'a2a_remember saves a markdown note; a2a_recall does a semantic search (RAG) ' +
    'over saved notes; a2a_brain_get fetches a page by source+slug; a2a_skill_save / ' +
    'a2a_skill_get store and fetch reusable skillpacks (procedures). Prefer recalling ' +
    'prior knowledge before re-deriving it. All brain tools fail soft — on a brain ' +
    'outage they return {ok:false,error} and never block.'

  constructor(opts: BrainToolsOpts = {}) {
    this.brainUrl = stripSlash(opts.brainUrl ?? process.env.BRAIN_URL ?? 'http://127.0.0.1:8787')
    this.gbrainUrl = stripSlash(opts.gbrainUrl ?? process.env.GBRAIN_URL ?? 'http://127.0.0.1:8080')
    this.apiToken = opts.apiToken ?? process.env.BRAIN_API_TOKEN ?? undefined
    this.source = opts.source ?? process.env.BRAIN_SOURCE ?? 'a2a'
    this.skillSource = opts.skillSource ?? process.env.SKILL_SOURCE ?? 'skills'
    this.timeoutMs = opts.timeoutMs ?? Number(process.env.BRAIN_TIMEOUT_MS ?? DEFAULT_TIMEOUT_MS)
    this.fetchImpl = opts.fetchImpl ?? fetch
  }

  /** True iff this module owns the named tool (used by webhook.ts to route). */
  handles(name: string): boolean {
    return (BrainTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  listTools(): any[] {
    return [
      {
        name: 'a2a_remember',
        description:
          'Save a markdown note to the agent brain (system of record). Use to persist a ' +
          'decision, fact, or lesson so it can be recalled later instead of re-derived. ' +
          'Returns {ok, slug, source, url}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            title: { type: 'string', description: 'Short human title for the note.' },
            body: { type: 'string', description: 'Markdown body. YAML frontmatter may be embedded.' },
            source: { type: 'string', description: "Brain source namespace. Defaults to 'a2a'." },
            slug: { type: 'string', description: "Optional slug, e.g. 'notes/bus-trust-model'. Derived from title when omitted (idempotent upsert)." },
            tags: { type: 'array', items: { type: 'string' }, description: 'Optional tags stored in frontmatter.' },
          },
          required: ['title', 'body'],
        },
      },
      {
        name: 'a2a_recall',
        description:
          'Semantic search (RAG) over saved brain notes. Returns the top matching hits ' +
          '[{source, slug, title, snippet, score}]. Recall before re-deriving knowledge.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            query: { type: 'string', description: 'Natural-language search query.' },
            limit: { type: 'integer', minimum: 1, maximum: 50, default: 5 },
            source: { type: 'string', description: 'Optional source filter (defaults to all sources).' },
            mode: { enum: ['semantic', 'fts', 'hybrid'], default: 'semantic', description: 'Retrieval mode: embedding similarity, full-text, or merged.' },
          },
          required: ['query'],
        },
      },
      {
        name: 'a2a_brain_get',
        description: 'Fetch the raw markdown of one brain page by source + slug. Returns {ok, source, slug, markdown}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            source: { type: 'string' },
            slug: { type: 'string', description: "Page slug, e.g. 'notes/bus-trust-model'." },
          },
          required: ['source', 'slug'],
        },
      },
      {
        name: 'a2a_skill_save',
        description:
          'Save a reusable skillpack/procedure to the brain (the retro→skillpack loop). ' +
          'Keyed by name; re-saving the same name updates it. Returns {ok, name, slug, backend}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: {
            name: { type: 'string', description: 'Skillpack name (unique key).' },
            body: { type: 'string', description: 'Markdown procedure body.' },
            tags: { type: 'array', items: { type: 'string' } },
          },
          required: ['name', 'body'],
        },
      },
      {
        name: 'a2a_skill_get',
        description: 'Fetch a skillpack by name. Returns {ok, name, body, tags, backend}.',
        inputSchema: {
          type: 'object', additionalProperties: false,
          properties: { name: { type: 'string' } },
          required: ['name'],
        },
      },
    ]
  }

  // The single dispatch entry point. Wrapped in try/catch so NOTHING — not a
  // bug, not a malformed arg, not a thrown fetch — can escape into the bridge.
  async callTool(name: string, args: Record<string, any> = {}): Promise<ToolResult> {
    try {
      switch (name) {
        case 'a2a_remember': return await this.remember(args)
        case 'a2a_recall': return await this.recall(args)
        case 'a2a_brain_get': return await this.brainGet(args)
        case 'a2a_skill_save': return await this.skillSave(args)
        case 'a2a_skill_get': return await this.skillGet(args)
        default: return this.result({ ok: false, error: 'unknown_tool', detail: name }, true)
      }
    } catch (e) {
      // Belt-and-suspenders: the per-tool methods already fail soft, but if one
      // ever throws (e.g. JSON of a huge body), degrade to an error result.
      return this.result({ ok: false, error: `brain_unavailable: ${errMsg(e)}` }, true)
    }
  }

  // ── tool implementations ────────────────────────────────────────────────

  private async remember(args: Record<string, any>): Promise<ToolResult> {
    const title = str(args.title)
    const body = str(args.body)
    if (!title) return this.result({ ok: false, error: 'bad_args: title required' }, true)
    if (!body) return this.result({ ok: false, error: 'bad_args: body required' }, true)
    const source = str(args.source) || this.source
    const slug = str(args.slug) || `notes/${slugify(title)}`
    const tags = strArray(args.tags)

    const ensured = await this.ensureSource(source)
    if (!ensured.ok) return this.result({ ok: false, error: ensured.error }, true)

    const payload: Record<string, any> = { slug, body, source_id: source, title, type: 'note' }
    if (tags.length) payload.frontmatter = { tags }
    const r = await this.req(`${this.brainUrl}/api/pages`, { method: 'POST', body: JSON.stringify(payload) })
    if (!r.ok) return this.result({ ok: false, error: r.error }, true)
    const parsed = parseJson<{ page?: { slug?: string; source_id?: string } }>(r.text)
    const savedSlug = parsed?.page?.slug ?? slug
    const savedSource = parsed?.page?.source_id ?? source
    return this.result({ ok: true, slug: savedSlug, source: savedSource, url: `${this.brainUrl}/md/${savedSource}/${savedSlug}` })
  }

  private async recall(args: Record<string, any>): Promise<ToolResult> {
    const query = str(args.query)
    if (!query) return this.result({ ok: false, error: 'bad_args: query required' }, true)
    const limit = clampInt(args.limit, 1, 50, 5)
    const mode = ['semantic', 'fts', 'hybrid'].includes(args.mode) ? args.mode : 'semantic'
    const body: Record<string, any> = { q: query, limit, mode }
    // Source scoping (P2 follow-up: alpha-leak denylist guard hooks in here).
    if (str(args.source)) body.source_id = str(args.source)

    const r = await this.req(`${this.brainUrl}/api/search`, { method: 'POST', body: JSON.stringify(body) })
    if (!r.ok) return this.result({ ok: false, error: r.error }, true)
    const rows = parseJson<any[]>(r.text) ?? []
    const hits = Array.isArray(rows) ? rows.map((h) => ({
      source: h.source_id, slug: h.slug, title: h.title,
      snippet: typeof h.snippet === 'string' ? h.snippet.slice(0, SNIPPET_CAP) : '',
      score: h.score,
    })) : []
    return this.result({ ok: true, count: hits.length, hits })
  }

  private async brainGet(args: Record<string, any>): Promise<ToolResult> {
    const source = str(args.source)
    const slug = str(args.slug)
    if (!source || !slug) return this.result({ ok: false, error: 'bad_args: source and slug required' }, true)
    const r = await this.req(`${this.brainUrl}/md/${enc(source)}/${encSlug(slug)}`, { method: 'GET' })
    if (!r.ok) {
      if (r.kind === 'http' && r.status === 404) return this.result({ ok: false, error: 'not_found' }, true)
      return this.result({ ok: false, error: r.error }, true)
    }
    return this.result({ ok: true, source, slug, markdown: r.text })
  }

  private async skillSave(args: Record<string, any>): Promise<ToolResult> {
    const name = str(args.name)
    const body = str(args.body)
    if (!name) return this.result({ ok: false, error: 'bad_args: name required' }, true)
    if (!body) return this.result({ ok: false, error: 'bad_args: body required' }, true)
    const tags = strArray(args.tags)
    const slug = `skillpacks/${slugify(name)}`

    // Backed by brain-notepad pages — see the gbrain note at the top of file.
    const ensured = await this.ensureSource(this.skillSource)
    if (!ensured.ok) return this.result({ ok: false, error: skillErr(ensured.error) }, true)

    const payload: Record<string, any> = {
      slug, body, source_id: this.skillSource, title: name, type: 'skillpack',
      frontmatter: { kind: 'skillpack', name, ...(tags.length ? { tags } : {}) },
    }
    const r = await this.req(`${this.brainUrl}/api/pages`, { method: 'POST', body: JSON.stringify(payload) })
    if (!r.ok) return this.result({ ok: false, error: skillErr(r.error) }, true)
    const parsed = parseJson<{ page?: { slug?: string } }>(r.text)
    return this.result({ ok: true, name, slug: parsed?.page?.slug ?? slug, source: this.skillSource, backend: 'brain-notepad' })
  }

  private async skillGet(args: Record<string, any>): Promise<ToolResult> {
    const name = str(args.name)
    if (!name) return this.result({ ok: false, error: 'bad_args: name required' }, true)
    const slug = `skillpacks/${slugify(name)}`
    const r = await this.req(`${this.brainUrl}/api/pages/${enc(this.skillSource)}/${encSlug(slug)}`, { method: 'GET' })
    if (!r.ok) {
      if (r.kind === 'http' && r.status === 404) return this.result({ ok: false, error: 'not_found' }, true)
      return this.result({ ok: false, error: skillErr(r.error) }, true)
    }
    const page = parseJson<{ body?: string; title?: string; frontmatter?: { tags?: string[]; name?: string } }>(r.text)
    return this.result({
      ok: true,
      name: page?.frontmatter?.name ?? page?.title ?? name,
      body: page?.body ?? '',
      tags: page?.frontmatter?.tags ?? [],
      backend: 'brain-notepad',
    })
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  // Idempotently create the brain source namespace. Doubles as a liveness probe:
  // if the brain is down this fails first and the caller returns fail-soft.
  private async ensureSource(source: string): Promise<{ ok: true } | { ok: false; error: string }> {
    const r = await this.req(`${this.brainUrl}/api/sources`, { method: 'POST', body: JSON.stringify({ id: source, name: source }) })
    if (!r.ok) return { ok: false, error: r.error }
    return { ok: true }
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { 'content-type': 'application/json' }
    if (this.apiToken) h['authorization'] = `Bearer ${this.apiToken}`
    return h
  }

  // Single fail-soft HTTP primitive. Aborts on timeout; classifies failures as
  // 'network' (brain down/slow → "brain_unavailable: ...") vs 'http' (brain up,
  // returned >=400). NEVER throws.
  private async req(url: string, init: RequestInit): Promise<ReqResult> {
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs)
    try {
      const res = await this.fetchImpl(url, { ...init, signal: ctrl.signal, headers: { ...this.headers(), ...(init.headers as Record<string, string> | undefined) } })
      const text = await res.text()
      if (!res.ok) return { ok: false, kind: 'http', status: res.status, error: `http_${res.status}: ${text.slice(0, 300)}` }
      return { ok: true, status: res.status, text }
    } catch (e) {
      const msg = e instanceof Error && e.name === 'AbortError' ? `timeout_after_${this.timeoutMs}ms` : errMsg(e)
      return { ok: false, kind: 'network', error: `brain_unavailable: ${msg}` }
    } finally {
      clearTimeout(timer)
    }
  }

  private result(obj: unknown, isError = false): ToolResult {
    return { content: [{ type: 'text', text: JSON.stringify(obj) }], isError }
  }
}

// ── module-local pure helpers ─────────────────────────────────────────────

function stripSlash(u: string): string { return u.replace(/\/+$/, '') }
function str(v: unknown): string { return typeof v === 'string' ? v.trim() : '' }
function strArray(v: unknown): string[] { return Array.isArray(v) ? v.filter((x) => typeof x === 'string') as string[] : [] }
function errMsg(e: unknown): string { return e instanceof Error ? e.message : String(e) }
function enc(s: string): string { return encodeURIComponent(s) }
// Slugs are slash-delimited paths (e.g. notes/foo); keep '/' but encode segments.
function encSlug(s: string): string { return s.split('/').map(encodeURIComponent).join('/') }

function clampInt(v: unknown, min: number, max: number, dflt: number): number {
  const n = Number(v)
  if (!Number.isFinite(n)) return dflt
  return Math.min(max, Math.max(min, Math.trunc(n)))
}

function slugify(s: string): string {
  const out = s.toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 80)
  return out || 'untitled'
}

function parseJson<T>(text: string): T | undefined {
  try { return JSON.parse(text) as T } catch { return undefined }
}

// Skillpacks present a stable error code so a caller can distinguish "no skill
// store" from a transient note failure. brain_unavailable stays surfaced for ops.
function skillErr(detail: string): string {
  return `skillpacks_not_available: ${detail}`
}
