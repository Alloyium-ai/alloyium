// output_transport.ts — byte-safe large-output transport for the A2A mesh.
//
// PURE module: ZERO import-time side effects (only `node:crypto`). Safe to import
// from `bun test` or any bus peer without spawning a process or joining the bus.
//
// An unbounded inference output must ride an A2A reply whose body is byte-capped
// (A2A_MAX_SEND_BYTES, enforced on `args.body` in a2a-channel.ts:269 — the bus
// REJECTS oversize sends, it never truncates). Two strategies, both loud, never
// silent (RCA `ops-specs/rca/2026-06-15-fusion-codex-mesh-infra`, Issue 1):
//   1. CLAIM-CHECK (preferred, LOSSLESS): write the full text to a TTL'd Redis
//      blob; the reply carries {result_ref, sha256, len, expires_at} + a bounded,
//      explicitly-marked preview (so consumers that don't resolve refs still see
//      readable, clearly-partial text — never an empty string).
//   2. TRUNCATE (last resort, e.g. Redis down): a byte-exact prefix + explicit
//      {truncated, original_bytes, emitted_bytes}.
// `planReply()` / `fitTruncate()` GUARANTEE the assembled, JSON-serialized reply
// body fits the cap — measured exactly as a2a-channel's sender measures it.
import { createHash, randomUUID } from 'node:crypto'

/** Minimal Redis surface used here — lets tests inject an in-memory fake. */
export interface BlobRedis {
  get(key: string): Promise<string | null>
  send(cmd: string, args: string[]): Promise<any>
}

export const BLOB_KEY_PREFIX = process.env.A2A_BLOB_KEY_PREFIX ?? 'alloyium:a2a:blob:'
export const REDIS_OP_TIMEOUT_MS = Number(process.env.A2A_BLOB_REDIS_TIMEOUT_MS ?? 2500)
export const PREVIEW_BYTES = Number(process.env.A2A_OUTPUT_PREVIEW_BYTES ?? 1024)
// Ceiling on a claim-checked INPUT blob — bounds Redis pressure (incl. the double-blob
// client→fusion-svc→gateway path). A spec/diff is ≪ this; an abusive multi-MB input is
// rejected LOUDLY rather than silently parked in Redis. Generous default, env-tunable.
export const MAX_INPUT_BYTES = Number(process.env.A2A_MAX_INPUT_BYTES ?? 16 * 1024 * 1024)

/**
 * Blob TTL (seconds). Must outlive the durable inbox retention PLUS a redelivery
 * window, so a blob referenced by a still-deliverable completed reply is never
 * already expired. Floor 90000s (25h); otherwise inbox max_age + 6h margin.
 */
export function blobTtlS(): number {
  const env = Number(process.env.A2A_BLOB_TTL_S)
  const streamAgeS = (Number(process.env.A2A_STREAM_MAX_AGE_H) || 24) * 3600
  return Math.max(Number.isFinite(env) ? env : 0, streamAgeS + 21600, 90000)
}

const TRUNCATE_MARKER = (len: number) => `\n…[truncated for transport — full ${len}B via result_ref]`

export function utf8ByteLength(s: string): number { return Buffer.byteLength(s, 'utf8') }
export function sha256Hex(s: string): string { return createHash('sha256').update(s, 'utf8').digest('hex') }

/**
 * Byte-exact truncation that NEVER splits a UTF-8 codepoint. `emittedBytes` ≤
 * `maxBytes`; backing off to the previous codepoint boundary if the cut lands in
 * the middle of a multibyte sequence.
 */
export function truncateToBytes(s: string, maxBytes: number): { text: string; truncated: boolean; originalBytes: number; emittedBytes: number } {
  const originalBytes = utf8ByteLength(s)
  if (originalBytes <= maxBytes) return { text: s, truncated: false, originalBytes, emittedBytes: originalBytes }
  if (maxBytes <= 0) return { text: '', truncated: true, originalBytes, emittedBytes: 0 }
  const buf = Buffer.from(s, 'utf8')
  let end = maxBytes
  // UTF-8 continuation bytes are 10xxxxxx (0x80..0xBF). If buf[end] (the first
  // EXCLUDED byte) is a continuation byte, the codepoint straddles the cut — back
  // off until buf[end] is a leading/ASCII byte, so [0,end) ends on a full codepoint.
  while (end > 0 && (buf[end]! & 0xc0) === 0x80) end--
  const text = buf.toString('utf8', 0, end)
  return { text, truncated: true, originalBytes, emittedBytes: utf8ByteLength(text) }
}

/** A byte-bounded preview head plus an explicit, len-bearing truncation marker. */
export function previewMarked(full: string, previewBytes: number, len: number): string {
  const head = truncateToBytes(full, previewBytes).text
  return head.length < full.length ? head + TRUNCATE_MARKER(len) : full
}

export type BlobRef = { result_ref: string; sha256: string; len: number; encoding: 'utf8'; expires_at: string }

function withTimeout<T>(p: Promise<T> | T, ms: number, label: string): Promise<T> {
  let t: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((_, rej) => { t = setTimeout(() => rej(new Error(`${label} timed out after ${ms}ms`)), ms) })
  return Promise.race([Promise.resolve(p).finally(() => clearTimeout(t)), timeout])
}

/**
 * Write `text` to a fresh blob and return its reference. Key is
 * `BLOB_KEY_PREFIX + randomUUID()` (NO raw job-id in the key → unguessable, no
 * collision with retried/redelivered jobs). `SET key text NX EX ttl`, retried
 * once with a new uuid on the (astronomically unlikely) NX collision. THROWS on
 * Redis timeout/error so the caller can fall back to truncation.
 */
export async function putBlob(redis: BlobRedis, a: { text: string; ttlS?: number; now?: () => number; genId?: () => string }): Promise<BlobRef> {
  const ttl = a.ttlS ?? blobTtlS()
  const now = a.now ?? Date.now
  const gen = a.genId ?? (() => randomUUID())
  const sha256 = sha256Hex(a.text)
  const len = utf8ByteLength(a.text)
  for (let attempt = 0; attempt < 2; attempt++) {
    const ref = BLOB_KEY_PREFIX + gen()
    const res = await withTimeout(redis.send('SET', [ref, a.text, 'NX', 'EX', String(ttl)]), REDIS_OP_TIMEOUT_MS, 'blob.put')
    if (res) return { result_ref: ref, sha256, len, encoding: 'utf8', expires_at: new Date(now() + ttl * 1000).toISOString() }
    // NX returned null → key already exists → retry with a new uuid.
  }
  throw new Error('putBlob: NX collision after retries')
}

/**
 * Fetch + verify a blob. `sha256` is REQUIRED (fail-closed `no_sha` if absent) —
 * the sha rides the SIGNED reply envelope, so it is the tamper-evidence anchor.
 * Verifies BOTH `len` (if given) and `sha256`.
 */
export async function getBlob(redis: BlobRedis, ref: { result_ref: string; sha256: string; len?: number }): Promise<{ ok: true; text: string } | { ok: false; reason: 'missing' | 'checksum' | 'len' | 'error' | 'no_sha' }> {
  if (!ref || typeof ref.sha256 !== 'string' || ref.sha256.length === 0) return { ok: false, reason: 'no_sha' }
  let val: string | null
  try { val = await withTimeout(redis.get(ref.result_ref), REDIS_OP_TIMEOUT_MS, 'blob.get') }
  catch { return { ok: false, reason: 'error' } }
  if (val == null) return { ok: false, reason: 'missing' }
  if (typeof ref.len === 'number' && utf8ByteLength(val) !== ref.len) return { ok: false, reason: 'len' }
  if (sha256Hex(val) !== ref.sha256) return { ok: false, reason: 'checksum' }
  return { ok: true, text: val }
}

/** Best-effort blob delete (orphan cleanup when the final reply send fails after a put). */
export async function delBlob(redis: BlobRedis, result_ref: string): Promise<void> {
  try { await withTimeout(redis.send('DEL', [result_ref]), REDIS_OP_TIMEOUT_MS, 'blob.del') } catch { /* TTL reclaims it anyway */ }
}

/** Unwrap an a2a_send MCP tool result {content:[{text:'{"ok":true,...}'}]} and check ok. Shared by every sender. */
export function sentOk(res: any): boolean {
  try { const t = res?.content?.[0]?.text; return typeof t === 'string' && JSON.parse(t)?.ok === true } catch { return false }
}

/**
 * The TRUE bus rejection reason from an a2a_send MCP result ({ok:false,error}) — e.g.
 * 'body_too_large' | 'rate_limited' | 'publish_failed' | 'a2a_disabled'. '' when the
 * send succeeded; 'unparseable' when the result isn't the expected shape. Surfacing
 * this (instead of a hard-coded "prompt too large?" guess) is the RCA's #1 fix — the
 * masked error is exactly what made the silent-drop impossible to diagnose.
 */
export function sendError(res: any): string {
  try { const j = JSON.parse(res?.content?.[0]?.text); return j?.ok === true ? '' : String(j?.error ?? 'unknown') } catch { return 'unparseable' }
}

/**
 * Resolve a possibly claim-checked output field: if `m.result_ref` is present, fetch +
 * verify the blob (lossless); otherwise return inline `output`. On a blob miss (reason
 * passed to `onMiss`) falls back to the inline marked preview. Shared by every consumer.
 */
export async function resolveRef(redis: BlobRedis, m: { result_ref?: string; sha256?: string; len?: number; output?: string }, onMiss?: (reason: string) => void): Promise<string> {
  if (!m?.result_ref) return m?.output ?? ''
  const r = await getBlob(redis, { result_ref: m.result_ref, sha256: String(m.sha256 ?? ''), len: m.len })
  if (r.ok) return r.text
  onMiss?.(r.reason)
  return m.output ?? `(blob ${r.reason})`
}

/** A claim-checked job INPUT: the gateway resolves `input_ref` to recover the full prompt. */
export type InputRef = { result_ref: string; sha256: string; len: number }

/**
 * Resolve a possibly claim-checked job INPUT (the mirror of resolveRef, for the
 * INPUT direction). If `job.input_ref` is present, fetch + verify the blob and return
 * the FULL prompt; on a blob miss, fall back to the inline `input[].text` (a marked
 * preview — loud, never empty) and call `onMiss`. With no ref, returns the inline
 * input join exactly as before, so jobs WITHOUT a ref are byte-for-byte unchanged.
 * Gateways call this to recover a large prompt that rode a Redis blob, since the bus
 * caps `args.body` and a peer like claude-gw (`--tools ""`) cannot read a file.
 */
export async function resolveJobInput(
  redis: BlobRedis,
  job: { input?: Array<{ text?: string }>; input_ref?: { result_ref?: string; sha256?: string; len?: number } },
  onMiss?: (reason: string) => void,
): Promise<string> {
  const inline = (job?.input || []).map((i) => i?.text ?? '').join('\n').trim()
  const ref = job?.input_ref
  if (!ref?.result_ref) return inline
  const r = await getBlob(redis, { result_ref: ref.result_ref, sha256: String(ref.sha256 ?? ''), len: ref.len })
  if (r.ok) return r.text
  onMiss?.(r.reason)
  return inline
}

// ── reply planning ────────────────────────────────────────────────────────────
// `wrap(text, extra?)` returns the object (job form) or raw string (plain-text
// form) the caller will actually send as the body. We measure exactly what the
// sender measures: a string body is sent verbatim; an object body is JSON.stringify'd
// (a2a-channel.ts sendMsg + validateSendArgs:269).

export type WrapFn = (text: string, extra?: Record<string, unknown>) => unknown
export type ReplyPlan =
  | { kind: 'inline'; output: string }
  | { kind: 'claimcheck'; preview: string; original_bytes: number }
  | { kind: 'truncate'; output: string; original_bytes: number; emitted_bytes: number }

function bodyBytes(wrapped: unknown): number {
  return Buffer.byteLength(typeof wrapped === 'string' ? wrapped : JSON.stringify(wrapped), 'utf8')
}

// Max-WIDTH metadata sentinels: a claim-check preview sized against these is
// guaranteed to still fit once the caller substitutes the REAL ref/sha/len/
// expires_at (all ≤ these widths: uuid=36, sha=64 hex, len≤10 digits, ISO=24).
const SENTINEL_EXTRA: Record<string, unknown> = {
  output_preview: true,
  result_ref: BLOB_KEY_PREFIX + '00000000-0000-0000-0000-000000000000',
  sha256: 'f'.repeat(64),
  // 10-digit upper bound (~10GB) — far above any claim-checked output; the stream's
  // max_msg_size (256KiB) / max_bytes (1GiB) bound it long before this could matter.
  len: 9_999_999_999,
  encoding: 'utf8',
  expires_at: '2026-06-15T00:00:00.000Z',
  truncated: false,
}

/**
 * Choose how to deliver `full` so the ASSEMBLED reply body ≤ `bodyCap`:
 *   inline (fits) → claimcheck (preferred, when `opts.allowClaimcheck !== false`)
 *   → truncate (last resort / plain-text form). PURE.
 */
export function planReply(full: string, bodyCap: number, wrap: WrapFn, opts: { previewBytes?: number; allowClaimcheck?: boolean } = {}): ReplyPlan {
  const original_bytes = utf8ByteLength(full)
  if (bodyBytes(wrap(full)) <= bodyCap) return { kind: 'inline', output: full }
  if (opts.allowClaimcheck !== false) {
    const preview = fitPreview(full, bodyCap, wrap, opts.previewBytes ?? PREVIEW_BYTES, original_bytes)
    if (preview != null) return { kind: 'claimcheck', preview, original_bytes }
  }
  return fitTruncate(full, bodyCap, wrap)
}

// Largest marked preview (head ≤ previewBytes) whose sentinel-bearing claim-check
// reply ≤ bodyCap. Binary-searches the head byte budget. null if even an empty
// marked preview won't fit (pathologically small cap → caller truncates instead).
function fitPreview(full: string, bodyCap: number, wrap: WrapFn, previewBytes: number, len: number): string | null {
  let lo = 0, hi = Math.min(previewBytes, utf8ByteLength(full)), best: string | null = null
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    const marked = truncateToBytes(full, mid).text + TRUNCATE_MARKER(len)
    if (bodyBytes(wrap(marked, SENTINEL_EXTRA)) <= bodyCap) { best = marked; lo = mid + 1 }
    else hi = mid - 1
  }
  return best
}

/** Largest byte-prefix whose explicit-truncation reply ≤ bodyCap. PURE. */
export function fitTruncate(full: string, bodyCap: number, wrap: WrapFn): { kind: 'truncate'; output: string; original_bytes: number; emitted_bytes: number } {
  const original_bytes = utf8ByteLength(full)
  let lo = 0, hi = original_bytes, bestText = ''
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    const head = truncateToBytes(full, mid).text
    const emitted_bytes = utf8ByteLength(head)
    if (bodyBytes(wrap(head, { truncated: true, original_bytes, emitted_bytes })) <= bodyCap) { bestText = head; lo = mid + 1 }
    else hi = mid - 1
  }
  return { kind: 'truncate', output: bestText, original_bytes, emitted_bytes: utf8ByteLength(bestText) }
}

// ── request-INPUT planning (the producer mirror of planReply) ───────────────────
// A dispatch (fusion-svc → claude-gw/codex-gw, or a client → fusion-svc) carries the
// PROMPT in a body that is byte-capped exactly like a reply. The judge fan-in (both
// panel answers) and the opus-leg input routinely exceed 8 KiB → the bus REJECTS the
// send and the job silently drops. Same disease as the reply path (RCA Issue 1), same
// cure: claim-check the prompt to a TTL'd blob and dispatch a tiny `input_ref` + a
// marked preview; the gateway recovers it via resolveJobInput(). LOSSLESS; the cap
// stays 8 KiB (raising it is the rejected band-aid).

/**
 * Build a job-request body that carries `prompt` to a peer WITHOUT exceeding `bodyCap`
 * — the INPUT-side mirror of planReply():
 *   inline (fits)  → wrap(prompt)
 *   else claim-check → putBlob(prompt); wrap(markedPreview, { input_preview, input_ref })
 * `wrap(text, extra)` assembles the body (places `text` as the inline input, spreads
 * `extra`). The preview head is binary-searched against the REAL ref fields so the
 * assembled body is GUARANTEED ≤ bodyCap. putBlob THROWS if Redis is down — the caller
 * decides what to do (an input cannot be safely truncated: the model would silently
 * reason over a partial spec). Returns { body, ref } — `ref` is the blob key (null when
 * inline) for orphan cleanup if the subsequent send is rejected.
 */
export async function buildClaimCheckedInput(
  redis: BlobRedis,
  prompt: string,
  bodyCap: number,
  wrap: WrapFn,
  opts: { previewBytes?: number; maxInputBytes?: number } = {},
): Promise<{ body: unknown; ref: string | null }> {
  if (bodyBytes(wrap(prompt)) <= bodyCap) return { body: wrap(prompt), ref: null }
  const len = utf8ByteLength(prompt)
  // Pressure guard: refuse to claim-check an absurd input rather than park it in Redis.
  const maxIn = opts.maxInputBytes ?? MAX_INPUT_BYTES
  if (len > maxIn) throw new Error(`buildClaimCheckedInput: input ${len}B exceeds max ${maxIn}`)
  const blob = await putBlob(redis, { text: prompt })
  const input_ref: InputRef = { result_ref: blob.result_ref, sha256: blob.sha256, len: blob.len }
  // Largest marked preview head whose body (with the REAL ref) still fits — binary search.
  let lo = 0, hi = Math.min(opts.previewBytes ?? PREVIEW_BYTES, len), best = ''
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    const marked = truncateToBytes(prompt, mid).text + TRUNCATE_MARKER(len)
    if (bodyBytes(wrap(marked, { input_preview: true, input_ref })) <= bodyCap) { best = marked; lo = mid + 1 }
    else hi = mid - 1
  }
  const body = wrap(best, { input_preview: true, input_ref })
  // HARD fit guarantee (never emit a body the bus will reject): even a ref-only body
  // (preview='') must fit. If not, the send cap is misconfigured-tiny → fail loudly here.
  // The input_ref carries the FULL prompt, so a short/empty preview loses nothing — the
  // gateway resolves the blob, and a blob-MISS is failed loudly there (never run on a stub).
  if (bodyBytes(body) > bodyCap) throw new Error(`buildClaimCheckedInput: ref-only body ${bodyBytes(body)}B exceeds cap ${bodyCap} (misconfigured A2A_MAX_SEND_BYTES?)`)
  return { body, ref: blob.result_ref }
}
