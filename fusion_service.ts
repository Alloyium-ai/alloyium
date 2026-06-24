// fusion_service.ts — STANDING Alloyium Panel bus service.
//
// Joins the A2A bus as `fusion-svc` and answers Alloyium Panel jobs: dispatches
// a prompt to a panel of different models in parallel, then an Opus judge
// synthesizes one answer (consensus / contradictions / unique insights).
//   panel  = Opus (the RUNNING claude-gw, over the bus)  +  GPT-5.5 (the RUNNING codex-gw app-server, over the bus)
//   judge  = Opus (claude-gw)
// Both Opus legs go through claude-gw (persistent Claude CLI session) — NO `claude -p`
// one-shot is spawned here. Mirrors the GPT-5.5/codex-gw dispatch exactly.
//
// Contract:
//   inbound  request -> fusion-svc inbox:
//     { schema:'fusion.job.request.v1', job_id?, prompt, input_file?, input_ref?, models?, judge?, panel_only? }
//     input_ref = { result_ref, sha256, len } — a claim-check Redis blob (REMOTE-SAFE: host-1
//       reads it; preferred over input_file, which must be a SAME-HOST path).
//     (a bare text body is wrapped as {prompt:<text>}.)
//   reply    type:reply, corr=<req id>:
//     { schema:'fusion.job.completed.v1', job_id, fused, panel:{...}, models:[...] } | fusion.job.failed.v1
//
// ADVISORY ONLY: read-only, no fire authority. codex runs sandbox=read-only.
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { putBlob, delBlob, previewMarked, truncateToBytes, sentOk, sendError, resolveRef, getBlob, buildClaimCheckedInput, type BlobRef, type WrapFn } from './output_transport.ts'

const AGENT_ID = process.env.A2A_AGENT_ID ?? 'fusion-svc'
const CODEX_GW = process.env.CODEX_GW_ID ?? 'codex-gw'
const CLAUDE_GW = process.env.CLAUDE_GW_ID ?? 'claude-gw'
// 8h service ceiling (never preempts a gateway turn): an ML/deep-analysis panel leg
// may run long. Per-job callers (fusion-ask) bound DOWNWARD via their own client timeout.
const CLAUDE_TIMEOUT_MS = Number(process.env.FUSION_CLAUDE_TIMEOUT_MS ?? 28_800_000)
const CODEX_TIMEOUT_MS = Number(process.env.FUSION_CODEX_TIMEOUT_MS ?? 28_800_000)
const log = (...a: any[]) => console.error(`[fusion-svc]`, ...a)
const uid = (p: string) => `${p}-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`

let a2a: A2AChannel
const sendMsg = (to: string, body: any, extra: any = {}) =>
  a2a.callTool('a2a_send', { to, body: typeof body === 'string' ? body : JSON.stringify(body), ...extra })

let _redis: RedisClient | null = null
const redis = () => (_redis ??= new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379'))

// Byte-safe the fusion reply: if the assembled completed reply exceeds the send cap,
// claim-check the largest string fields (fused, then panel entries) to TTL'd blobs —
// replacing each with a marked preview + ref — until it fits. Lossless; fusion_client
// resolves the refs. (RCA Issue 1: never silently drop a fat reply.)
async function fitFusionReply(res: any, bodyCap: number): Promise<{ reply: any; refs: string[] }> {
  const measure = (o: any) => Buffer.byteLength(JSON.stringify(o), 'utf8')
  const out: any = { ...res }
  const refs: string[] = []
  if (measure(out) <= bodyCap) return { reply: out, refs }
  type Field = { get: () => string; setPreview: (p: string, r: BlobRef) => void; setText: (t: string) => void }
  const fields: Field[] = []
  if (typeof out.fused === 'string') fields.push({ get: () => out.fused, setPreview: (p, r) => { out.fused = p; out.output_preview = true; out.result_ref = r.result_ref; out.sha256 = r.sha256; out.len = r.len; out.encoding = r.encoding; out.expires_at = r.expires_at }, setText: (t) => { out.fused = t } })
  if (out.panel && typeof out.panel === 'object') {
    out.panel = { ...out.panel }; out.panel_refs = { ...(out.panel_refs ?? {}) }
    for (const k of Object.keys(out.panel)) if (typeof out.panel[k] === 'string') fields.push({ get: () => out.panel[k], setPreview: (p, r) => { out.panel[k] = p; out.panel_refs[k] = r }, setText: (t) => { out.panel[k] = t } })
  }
  const bySize = () => [...fields].sort((a, b) => Buffer.byteLength(b.get(), 'utf8') - Buffer.byteLength(a.get(), 'utf8'))
  // 1) claim-check the largest fields until it fits — LOSSLESS (fusion_client resolves refs).
  for (const f of bySize()) {
    if (measure(out) <= bodyCap) break
    const full = f.get()
    try { const ref = await putBlob(redis(), { text: full }); refs.push(ref.result_ref); f.setPreview(previewMarked(full, 512, Buffer.byteLength(full, 'utf8')), ref) }
    catch (e) { log(`claim-check failed: ${e}`) } // Redis down → handled by the truncate fallback below
  }
  // 2) HARD fit-guarantee: if STILL too big (Redis down / metadata-heavy), byte-truncate
  // the largest inline field via binary search so the reply ALWAYS fits — truncated-but-
  // useful, never a dropped reply (mirrors codex_gateway's Redis-down path).
  if (measure(out) > bodyCap) {
    const f = bySize()[0]
    if (f) {
      const full = f.get(); const MARK = '\n…[truncated]'
      let lo = 0, hi = Buffer.byteLength(full, 'utf8'), best = ''
      while (lo <= hi) {
        const mid = (lo + hi) >> 1
        f.setText(truncateToBytes(full, mid).text + MARK)
        if (measure(out) <= bodyCap) { best = truncateToBytes(full, mid).text; lo = mid + 1 } else hi = mid - 1
      }
      f.setText(best + MARK); out.truncated = true
      log(`fusion reply byte-truncated (claim-check unavailable)`)
    }
  }
  return { reply: out, refs }
}

// pending jobs we dispatched over the bus: job_id -> resolver
const pendingCodex = new Map<string, (out: string) => void>()
const pendingClaude = new Map<string, (out: string) => void>()

// ── Opus via the RUNNING claude-gw (persistent Claude CLI session, over the bus) ──
// Mirrors codexGpt55 EXACTLY: dispatch claude.job.request.v1, await claude.job.completed.v1
// (job_id-matched in onInbound; claim-check refs resolved there). NO `claude -p` here —
// claude-gw owns the warm Opus session, eliminating the one-shot anti-pattern.
async function claudeGw(prompt: string): Promise<string> {
  const job_id = uid('fusion-claude')
  const done = new Promise<string>((resolve) => {
    pendingClaude.set(job_id, resolve)
    setTimeout(() => { if (pendingClaude.delete(job_id)) resolve('(opus: timeout)') }, CLAUDE_TIMEOUT_MS)
  })
  // Claim-check the prompt if it exceeds our send cap (the opus-leg input + the judge
  // fan-in routinely do): dispatch a tiny `input_ref` + preview that claude-gw resolves,
  // instead of inlining the full prompt into a body the bus would REJECT. (RCA Issue 1.)
  const wrap: WrapFn = (text, extra) => ({ schema: 'claude.job.request.v1', job_id, input: [{ type: 'text', text }], ...(extra ?? {}) })
  let built: { body: unknown; ref: string | null }
  try { built = await buildClaimCheckedInput(redis(), prompt, a2a.getMaxSendBytes(), wrap) }
  catch (e) { pendingClaude.delete(job_id); return `(opus: input claim-check failed — Redis down? ${e})` }
  const dr = await sendMsg(CLAUDE_GW, built.body, { type: 'request' })
  // Don't delBlob the input on a send-reject: the result can be ambiguous (publish timeout
  // AFTER delivery), and deleting a blob the gateway DID receive would corrupt it to a
  // partial preview. A true orphan is reclaimed by the blob TTL. (Cross-model review, P1.)
  if (!sentOk(dr)) { pendingClaude.delete(job_id); return `(opus: dispatch rejected: ${sendError(dr)})` }
  return done
}

// ── GPT-5.5 via the RUNNING codex-gw app-server (over the bus) ─────────────────
async function codexGpt55(prompt: string): Promise<string> {
  const job_id = uid('fusion-codex')
  const done = new Promise<string>((resolve) => {
    pendingCodex.set(job_id, resolve)
    setTimeout(() => { if (pendingCodex.delete(job_id)) resolve('(gpt-5.5: timeout)') }, CODEX_TIMEOUT_MS)
  })
  // Same claim-check path (codex-gw resolves `input_ref`) so a big input never trips the cap.
  const wrap: WrapFn = (text, extra) => ({ schema: 'codex.job.request.v1', job_id, input: [{ type: 'text', text }], sandbox: 'read-only', approval_policy: 'never', cwd: '/tmp', ...(extra ?? {}) })
  let built: { body: unknown; ref: string | null }
  try { built = await buildClaimCheckedInput(redis(), prompt, a2a.getMaxSendBytes(), wrap) }
  catch (e) { pendingCodex.delete(job_id); return `(gpt-5.5: input claim-check failed — Redis down? ${e})` }
  const dr = await sendMsg(CODEX_GW, built.body, { type: 'request' })
  // See claudeGw: rely on the blob TTL, never delete a possibly-delivered input blob.
  if (!sentOk(dr)) { pendingCodex.delete(job_id); return `(gpt-5.5: dispatch rejected: ${sendError(dr)})` }
  return done
}

function judgePrompt(question: string, panel: Record<string, string>): string {
  const blocks = Object.entries(panel).map(([m, t]) => `### Panelist: ${m}\n${t}`).join('\n\n')
  return `You are the JUDGE in Alloyium Panel. The user's task:\n\n${question}\n\n` +
    `Two independent panelists (different models) answered below. SYNTHESIZE ONE answer per the Alloyium Panel method: ` +
    `note CONSENSUS (agree -> high confidence), resolve CONTRADICTIONS (say which you trust and why), and keep ` +
    `UNIQUE INSIGHTS only one surfaced. Be decisive and quantitative. ADVISORY only.\n\n${blocks}\n\n` +
    `Return the fused answer directly (it goes to the operator).`
}

async function runFusion(req: any): Promise<any> {
  const job_id = req.job_id || uid('fusion')
  const prompt: string = String(req.prompt ?? '').trim()
  if (!prompt) return { schema: 'fusion.job.failed.v1', job_id, error: 'empty prompt' }
  // Resolve the optional shared INPUT. Preferred: `input_ref` — a Redis claim-check blob
  // the dispatcher wrote (REMOTE-SAFE: host-1 reads the blob, never the caller's local FS).
  // Back-compat: `input_file` — a SAME-HOST path read directly here. The resolved text is
  // appended to BOTH legs and rides claim-check DOWNWARD if large (claudeGw/codexGpt55), so
  // the codex leg no longer needs the file on host-1 — the same-host assumption that broke
  // remote callers (Bug 2) is gone. Opus already had no file access (`--tools ""`).
  let inputText = ''
  if (req.input_ref && typeof req.input_ref.result_ref === 'string') {
    const r = await getBlob(redis(), { result_ref: req.input_ref.result_ref, sha256: String(req.input_ref.sha256 ?? ''), len: req.input_ref.len })
    if (!r.ok) return { schema: 'fusion.job.failed.v1', job_id, error: `input_ref blob ${r.reason}` }
    inputText = r.text
  } else if (typeof req.input_file === 'string' && req.input_file) {
    // SECURITY (cross-model review, P1): this reads an ARBITRARY path chosen by the
    // requester. The trust boundary is the bus L1 allowlist + ed25519 signing (only
    // allowlisted, authenticated peers reach this inbox) — but a compromised low-priv
    // peer could still exfiltrate a host-1 file. `input_ref` (claim-check) is the
    // preferred, path-free channel; fusion_client now always uses it. AUDIT-logged so an
    // unexpected read is visible. FOLLOW-UP (ops): gate to an allowlist dir or retire
    // input_file once no caller depends on it.
    log(`JOB ${job_id}: SAME-HOST input_file read '${req.input_file}' (prefer input_ref)`)
    try { inputText = await Bun.file(req.input_file).text() }
    catch (e) { return { schema: 'fusion.job.failed.v1', job_id, error: `input_file unreadable on this host (${req.input_file}) — from a REMOTE host send the input via claim-check (input_ref), not a local path: ${e}` } }
  }
  const merged = inputText ? `${prompt}\n\n--- INPUT ---\n${inputText}` : prompt
  log(`JOB ${job_id}: panel dispatch (opus via ${CLAUDE_GW} + gpt-5.5 via ${CODEX_GW}${inputText ? `, +${Buffer.byteLength(inputText, 'utf8')}B input` : ''})`)
  const [opus, gpt55] = await Promise.all([
    claudeGw(merged).catch((e) => `(opus error: ${e})`),
    codexGpt55(merged).catch((e) => `(gpt-5.5 error: ${e})`),
  ])
  const panel = { opus, 'gpt-5.5': gpt55 }
  if (req.panel_only) return { schema: 'fusion.job.completed.v1', job_id, fused: null, panel, models: ['opus', 'gpt-5.5'] }
  log(`JOB ${job_id}: judging (opus via ${CLAUDE_GW})`)
  const fused = await claudeGw(judgePrompt(prompt, panel)).catch((e) => `(judge error: ${e})`)
  log(`JOB ${job_id}: done (${fused.length}c)`)
  return { schema: 'fusion.job.completed.v1', job_id, fused, panel, models: ['opus', 'gpt-5.5'] }
}

async function onInbound(content: string, attrs: any) {
  if (attrs?.feed !== 'a2a' || attrs?.kind !== 'direct') return
  let m: any = null; try { m = JSON.parse(content) } catch {}
  // a) replies to OUR codex dispatches (only genuine codex-gw replies resolve a pending leg)
  if (m && attrs.from === CODEX_GW && (m.schema === 'codex.job.completed.v1' || m.schema === 'codex.job.failed.v1' || m.schema === 'codex.job.rejected.v1')) {
    const r = pendingCodex.get(m.job_id)
    if (r) {
      pendingCodex.delete(m.job_id)
      if (m.schema === 'codex.job.completed.v1') r(await resolveRef(redis(), m, (reason) => log(`gpt-5.5 blob ${reason}`)))
      else r(m.error ? `(gpt-5.5 failed: ${m.error})` : `(gpt-5.5 rejected: ${m.reason})`)
    }
    return
  }
  if (m && m.schema === 'codex.job.accepted.v1') return
  // a2) replies to OUR claude-gw dispatches (mirror of the codex path; only genuine claude-gw)
  if (m && attrs.from === CLAUDE_GW && (m.schema === 'claude.job.completed.v1' || m.schema === 'claude.job.failed.v1')) {
    const r = pendingClaude.get(m.job_id)
    if (r) {
      pendingClaude.delete(m.job_id)
      if (m.schema === 'claude.job.completed.v1') r(await resolveRef(redis(), m, (reason) => log(`opus blob ${reason}`)))
      else r(`(opus failed: ${m.error})`)
    }
    return
  }
  if (m && m.schema === 'claude.job.accepted.v1') return
  // b) inbound fusion jobs (requests only)
  if (attrs.type !== 'request') return
  const from = attrs.from, corr = attrs.id
  const req = (m && m.schema === 'fusion.job.request.v1') ? m : { prompt: content }
  log(`REQUEST from ${from}`)
  // Background-dispatch: keep the inbox consume loop live so the panel/judge replies
  // (claude-gw + codex-gw completed.v1) that runFusion awaits on THIS agent's own inbox
  // are delivered. Awaiting inline would hold the loop and deadlock on its own replies
  // (both legs now ride the bus). Fire-and-return.
  void (async () => {
  try {
    const res = await runFusion(req)
    const { reply, refs } = await fitFusionReply(res, a2a.getMaxSendBytes())
    const sres = await sendMsg(from, reply, { type: 'reply', corr })
    if (!sentOk(sres)) {
      log(`SEND-FAIL ${res.job_id}: fused reply rejected — sending failed.v1`)
      for (const ref of refs) await delBlob(redis(), ref).catch(() => {}) // orphan cleanup
      await sendMsg(from, { schema: 'fusion.job.failed.v1', job_id: res.job_id, error: 'reply_too_large' }, { type: 'reply', corr }).catch(() => {})
    }
  } catch (e) {
    await sendMsg(from, { schema: 'fusion.job.failed.v1', error: e instanceof Error ? e.message : String(e) }, { type: 'reply', corr }).catch(() => {})
  }
  })()
}

a2a = new A2AChannel(onInbound as any, { enabled: true, agentId: AGENT_ID })
await a2a.start()
if (!a2a.isStarted()) { log('FATAL: could not join bus (duplicate id / stale presence?)'); process.exit(1) }
log(`bus joined as '${AGENT_ID}'. Alloyium Panel fusion.job.* ready (panel: opus via ${CLAUDE_GW} + gpt-5.5 via ${CODEX_GW}, judge: opus via ${CLAUDE_GW}).`)
