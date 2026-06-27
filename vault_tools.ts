// vault_howto guidance tool for the A2A webhook MCP bridge.
//
// Spawned a2a agents frequently need to know HOW to obtain KAI Vault / taskboard
// access — and what to do when they hold no token. That knowledge lives in an
// operator runbook (the `kai-vault-and-taskboard-access` brain skill) which also
// contains secret-emitting material (token-file paths, secret env names, extraction
// commands, internal hosts). Exposing any of that on a broadly-callable MCP tool is
// dangerous and useless to the audience: the only thing a no-token agent can act on
// is non-secret topology + the escalation path. So this module ships a CURATED SAFE
// PROJECTION of that runbook with ZERO secrets ever on the wire:
//
//   vault_howto — return guidance-only (topology + token-scoping policy + escalation
//                 path + a provenance pointer to the full operator runbook).
//
// ── design invariants ───────────────────────────────────────────────────────
//  - GUIDANCE ONLY, NO SECRETS: the response is a static curated projection authored
//    here. It NEVER contains a token value, secret env name, internal host/endpoint,
//    file path, or secret-emitting command. The forbidden set is enforced by a
//    denylist guard in tests/vault_tools.test.ts; curation is the primary control,
//    the denylist is the safety net.
//  - SELF-CONTAINED: ZERO network, ZERO secret reads, ZERO filesystem reads. Unlike
//    brain_tools.ts / kai_tools.ts (which front live HTTP/WS backends), this tool has
//    no backend at all — every byte it emits is the static projection below.
//  - SINGLE-SOURCE BY REFERENCE, NOT RUNTIME FETCH: the authoritative runbook is
//    surfaced as a STATIC curated projection with provenance pointer(s)
//    (`source_of_record`). Pointers pass a TWO-LAYER guard before reaching the wire:
//    (1) a charset allowlist (structurally bars URLs/hosts/ports/uppercase secret-
//    env-names) AND (2) the §7 denylist `containsForbidden` (catches a conforming
//    lowercase forbidden literal or entropy blob) — then are emitted verbatim, never
//    fetched or existence-checked. The denylist is the SAME checker the test uses, so
//    runtime and test cannot drift. This keeps "NO secrets EVER" true for ANY
//    construction, not just the default. "Auto-pick-up" of a new source = an operator
//    appends a CONFORMING pointer to the config array (no code, no network);
//    "fail-soft skip" = a non-conforming pointer is dropped, never errors the tool
//    (if every pointer drops, we fall back to the default so it is never empty).
//  - FAIL-SOFT: callTool NEVER throws — an unknown tool or a malformed arg degrades
//    to an error ToolResult. A missing/unrecognized `topic` degrades to 'all'.
//
// Mirrors the BrainTools / KaiTools module shape (TOOL_NAMES / INSTRUCTIONS /
// handles / listTools / callTool + a private result() wrapper and a local
// ToolResult type) so webhook.ts routes it identically.

/** MCP tool-call result envelope (defined locally, mirroring brain_tools.ts). */
type ToolResult = { content: { type: 'text'; text: string }[]; isError?: boolean }

/**
 * A provenance pointer to an authoritative runbook page. Surfaced verbatim as
 * `source_of_record` after a shape-allowlist check (see {@link VaultTools}
 * constructor); NEVER fetched or existence-checked.
 */
export interface VaultProvenance {
  source: string
  slug: string
}

/** Construction options for {@link VaultTools}. */
export interface VaultToolsOpts {
  /**
   * Static provenance pointers surfaced as `source_of_record`. Defaults to the
   * canonical KAI vault + taskboard runbook. Each pointer must pass BOTH the brain
   * source/slug charset allowlist AND the §7 denylist ({@link containsForbidden});
   * non-conforming pointers are DROPPED — so no URL, host, port, secret-env-name, or
   * forbidden literal can reach the wire via provenance. If every pointer drops, the
   * default is used. NO fetch, NO existence-check.
   */
  sources?: VaultProvenance[]
}

// Default source of record: the canonical KAI vault + taskboard operator runbook.
// Surfaced as a pointer ONLY (for authorized operators) — never fetched.
const DEFAULT_SOURCES: VaultProvenance[] = [
  { source: 'skills', slug: 'skillpacks/kai-vault-and-taskboard-access' },
]

// Brain source/slug charset — a provenance pointer is surfaced only if BOTH match.
// Allowlist (not denylist): lowercase-only bars uppercase secret env-names
// (e.g. SOME_TOKEN); no dots bars hosts (e.g. host.example.com); no colons/slashes-in-source
// bars schemes/ports (e.g. https://). Real brain sources/slugs already satisfy these, so
// the gate is tight without being restrictive — and it STRUCTURALLY excludes secrets
// from source_of_record regardless of what a caller passes.
const VALID_SOURCE = /^[a-z0-9][a-z0-9_-]*$/
const VALID_SLUG = /^[a-z0-9][a-z0-9_/-]*$/

// ── §7 forbidden-content denylist — the SINGLE SOURCE OF TRUTH ──────────────
// Exported so BOTH the runtime provenance guard (the constructor) AND
// tests/vault_tools.test.ts consume the EXACT same checker — the denylist can
// never drift between runtime and test (that drift is what let a conforming-but-
// forbidden provenance value, e.g. slug 'kai-main' / 'taskboard_bearer_token',
// slip through: the charset allowlist accepts lowercase literals, so the denylist
// must ALSO run at runtime, not only in the test).

/**
 * Literal secret sentinels — matched case-INSENSITIVELY as substrings. These are
 * GENERIC secret/extraction shapes only: token + config-key names, secret-emitting
 * commands, and token filenames. They intentionally name NO specific infrastructure
 * host, filesystem path, or organization; structural hosts/IPs/URLs/env-var-secrets
 * are caught by FORBIDDEN_PATTERNS below. Operators may extend this list for their
 * own deployment's secret vocabulary.
 */
export const FORBIDDEN_LITERALS: readonly string[] = [
  'bootstrap_master', 'kai-main', 'kai_main', '.tokens.json', 'tenant-tokens',
  'docker exec', 'printenv', 'python3 -c', 'json.load', 'x-api-key',
  'taskboard_bearer_token', 'taskboard_url', 'taskboard_agent_role',
  'vault_url', 'vault_tokens_file',
]

/**
 * Structural secret patterns — matched CASE-SENSITIVELY on the original text so the
 * uppercase env-var + IP patterns fire and never self-match required phrases.
 */
export const FORBIDDEN_PATTERNS: readonly { name: string; re: RegExp }[] = [
  { name: 'high-entropy blob', re: /[A-Za-z0-9_\-]{40,}/ },
  { name: 'url scheme', re: /https?:\/\// },
  // Generic bare host/domain (e.g. host.example.com) — was an org-specific host match;
  // generalized so the guard redacts ANY dotted hostname, not one infrastructure's.
  { name: 'bare host/domain', re: /\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\b/ },
  { name: 'env-var secret name', re: /\b[A-Z][A-Z0-9]*_[A-Z0-9_]*(TOKEN|SECRET|BEARER|KEY|PASSWORD)\b/ },
  { name: 'rfc1918/loopback host', re: /\b(?:127\.0\.0\.1|10\.\d|192\.168\.\d|172\.(?:1[6-9]|2\d|3[01])\.\d|localhost:\d)/ },
]

/**
 * Every §7 forbidden-content violation in `s` (empty array ⇒ clean). Literal
 * sentinels match case-insensitively; structural regexes run on the original case.
 */
export function forbiddenHits(s: string): string[] {
  const lower = s.toLowerCase()
  const out: string[] = []
  for (const needle of FORBIDDEN_LITERALS) if (lower.includes(needle)) out.push(`literal:${needle}`)
  for (const { name, re } of FORBIDDEN_PATTERNS) if (re.test(s)) out.push(`regex:${name}`)
  return out
}

/** True iff `s` contains any §7 forbidden secret content. */
export function containsForbidden(s: string): boolean {
  return forbiddenHits(s).length > 0
}

// ── curated SAFE projection — the ONLY guidance content this tool ever emits ──
// Static, authored here, NON-secret. NO host / endpoint / command / file path /
// token value. Every string is verified against the §7 denylist above (the same
// containsForbidden() the constructor enforces) by tests/vault_tools.test.ts.
const GUIDANCE = {
  vault:
    'A canonical KAI Vault provides role-scoped secrets via Bearer-token auth. ' +
    'Spawned agents do NOT hold vault credentials by default.',
  taskboard:
    'A taskboard API supports role-scoped task operations: read/move at agent ' +
    'role; task CREATE requires an elevated role.',
  policy:
    'AGENT-TOKEN-SCOPING IS LAW — never use shared or break-glass tokens; no ' +
    'break-glass without explicit operator override.',
  escalation:
    'If you are a spawned a2a agent with no token, you CANNOT self-resolve role ' +
    'tokens. Escalate to the orchestrator (agent-1). When you escalate, state: ' +
    "the task action you're blocked on, the role/secret-path you believe you " +
    'need, and why. Prefer an explicit signed-token scope lease, or a token label ' +
    'plus normalized role, over guessed paths or raw token material — so agent-1 ' +
    'gets an actionable ask, not "I am stuck".',
} as const

// Advisory framing carried on every response: the no-fire-authority note plus the
// operator-facing pointer framing (a reference, NOT a fetch instruction).
const ADVISORY =
  'Guidance only — no secrets, hosts, endpoints, or commands are exposed here; ' +
  'this is advisory and carries no fire authority. Authorized operators: the full ' +
  'runbook lives at the brain page(s) in source_of_record (a provenance reference, ' +
  'not a fetch instruction).'

/** The selectable guidance topics. `policy` + `escalation` are always returned. */
type Topic = 'vault' | 'taskboard' | 'policy' | 'escalation' | 'all'
const TOPICS: readonly Topic[] = ['vault', 'taskboard', 'policy', 'escalation', 'all']

/**
 * `vault_howto` guidance tool — returns a static, curated, secret-free projection of
 * the KAI vault / taskboard access runbook for spawned a2a agents.
 *
 * Self-contained: no network, no secret reads, no filesystem reads. Fail-soft:
 * {@link VaultTools.callTool} never throws and an unknown/missing topic degrades to
 * `all`. The full forbidden-content contract is enforced by
 * `tests/vault_tools.test.ts`.
 */
export class VaultTools {
  private readonly sources: VaultProvenance[]

  /**
   * The tool names this module owns. webhook.ts routes these to {@link callTool}
   * and leaves everything else to the brain / kai tools / a2a channel.
   */
  static readonly TOOL_NAMES = ['vault_howto'] as const

  /**
   * Appended to the MCP server instructions (system prompt) when enabled, so agents
   * know the tool exists. MUST begin with a leading space — webhook.ts concatenates
   * the INSTRUCTIONS strings with no separator (mirrors brain_tools.ts).
   */
  static readonly INSTRUCTIONS =
    ' You also have a vault guidance tool (advisory, NO secrets): vault_howto ' +
    'returns guidance-only on how an agent obtains KAI Vault / taskboard access ' +
    'and what to do with no token — non-secret topology, the token-scoping policy ' +
    '(no shared or break-glass tokens), and the escalation path to the ' +
    'orchestrator (agent-1). It exposes NO secrets, hosts, endpoints, or commands; ' +
    'all content is a static curated projection with provenance pointers. Advisory ' +
    'only — never an order trigger.'

  /**
   * @param opts.sources Static provenance pointers surfaced as `source_of_record`
   *   (default: the canonical KAI vault + taskboard runbook). Each pointer must pass
   *   BOTH the source/slug charset allowlist AND the §7 denylist
   *   ({@link containsForbidden}); non-conforming pointers are dropped; if all drop,
   *   the default is used. Emitted verbatim, never fetched.
   */
  constructor(opts: VaultToolsOpts = {}) {
    // Guard provenance with TWO layers so a hostile/careless `opts.sources` can never
    // put a secret on the wire, for ANY construction:
    //   1. charset allowlist — bars URLs, hosts (dots), ports/schemes (colons), and
    //      uppercase secret-env-names structurally; and
    //   2. the §7 denylist (containsForbidden) — catches a CONFORMING lowercase value
    //      that is nonetheless a forbidden literal (e.g. 'kai-main',
    //      'taskboard_bearer_token', 'tenant-tokens') or a 40+ char entropy blob.
    // Layer 2 is the SAME checker the test uses, so runtime and test cannot drift.
    // Non-conforming pointers are DROPPED (fail-soft, never throws); if every pointer
    // drops we fall back to DEFAULT_SOURCES so source_of_record is never empty.
    // Surviving pointers are defensively copied + surfaced verbatim (never fetched),
    // so callers can't mutate our state nor smuggle a stray field.
    const src = Array.isArray(opts.sources) ? opts.sources : DEFAULT_SOURCES
    const validated = src
      .map((s) => ({ source: String((s as any)?.source ?? ''), slug: String((s as any)?.slug ?? '') }))
      .filter(
        (s) =>
          VALID_SOURCE.test(s.source) &&
          VALID_SLUG.test(s.slug) &&
          !containsForbidden(`${s.source} ${s.slug}`),
      )
    this.sources = validated.length
      ? validated
      : DEFAULT_SOURCES.map((s) => ({ source: s.source, slug: s.slug }))
  }

  /** True iff this module owns the named tool (used by webhook.ts to route). */
  handles(name: string): boolean {
    return (VaultTools.TOOL_NAMES as readonly string[]).includes(name)
  }

  /** MCP tool descriptors. The schema + description carry NO secret content. */
  listTools(): any[] {
    return [
      {
        name: 'vault_howto',
        description:
          'Guidance-only (NO secrets) on how an agent obtains KAI Vault / taskboard ' +
          'access and what to do with no token: non-secret topology, the ' +
          'token-scoping policy, and the escalation path to the orchestrator ' +
          '(agent-1). Returns a static curated projection plus source_of_record ' +
          'provenance pointers; exposes NO hosts, endpoints, commands, or secret ' +
          'values. topic is one of vault|taskboard|policy|escalation|all (default ' +
          'all); policy and escalation are always included.',
        inputSchema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            topic: {
              enum: ['vault', 'taskboard', 'policy', 'escalation', 'all'],
              default: 'all',
              description:
                'Which guidance section to add. policy + escalation are always ' +
                'returned; an unknown or missing topic behaves as all.',
            },
          },
        },
      },
    ]
  }

  /**
   * Single dispatch entry point. Wrapped in try/catch so NOTHING — not a bug, not a
   * malformed arg — can escape into the bridge. Returns a fail-soft error result for
   * an unknown tool name; never throws.
   */
  async callTool(name: string, args: Record<string, any> = {}): Promise<ToolResult> {
    try {
      switch (name) {
        case 'vault_howto':
          return this.vaultHowto(args)
        default:
          return this.result({ ok: false, error: 'unknown_tool', detail: name }, true)
      }
    } catch (e) {
      // Belt-and-suspenders: vaultHowto is pure + total, but degrade rather than
      // ever let an exception cross the MCP boundary.
      return this.result({ ok: false, error: 'vault_howto_error', detail: errMsg(e) }, true)
    }
  }

  // ── tool implementation ───────────────────────────────────────────────────

  /**
   * Build the curated guidance payload filtered by `topic`. `policy` + `escalation`
   * are the load-bearing guidance and are ALWAYS included; `topic` only adds the
   * requested descriptive section(s). `source_of_record` is always present. A
   * missing/unrecognized `topic` behaves as `all` and never errors.
   */
  private vaultHowto(args: Record<string, any>): ToolResult {
    const topic = resolveTopic(args)
    const guidance: Record<string, string> = {}
    if (topic === 'vault' || topic === 'all') guidance.vault = GUIDANCE.vault
    if (topic === 'taskboard' || topic === 'all') guidance.taskboard = GUIDANCE.taskboard
    // Always-on, regardless of topic (§6.2) — the load-bearing guidance.
    guidance.policy = GUIDANCE.policy
    guidance.escalation = GUIDANCE.escalation
    return this.result({
      ok: true,
      guidance,
      // Fresh copy each call so callers can't mutate our provenance config.
      source_of_record: this.sources.map((s) => ({ source: s.source, slug: s.slug })),
      advisory: ADVISORY,
    })
  }

  /** Wrap an inner payload into the MCP wire shape (mirrors brain_tools.ts). */
  private result(obj: unknown, isError = false): ToolResult {
    return { content: [{ type: 'text', text: JSON.stringify(obj) }], isError }
  }
}

// ── module-local pure helpers ───────────────────────────────────────────────

/**
 * Resolve the requested `topic`, fail-soft. A missing, non-string, or unrecognized
 * value degrades to `'all'`; the input is trimmed + lowercased first. Never throws.
 */
function resolveTopic(args: Record<string, any>): Topic {
  const raw = args && typeof args.topic === 'string' ? args.topic.trim().toLowerCase() : ''
  return (TOPICS as readonly string[]).includes(raw) ? (raw as Topic) : 'all'
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
