// fusion_client.ts — call the standing Alloyium Panel bus service: dispatch a
// fusion.job.request.v1 and await the fused answer. For sessions without the
// a2a_send MCP tool (e.g. the agent-1 orchestrator).
//
//   A2A_AGENT_ID=fusion A2A_SIGNING_KEY=.../fusion.seed NATS_URL=... REDIS_URL=... \
//     bun fusion_client.ts <prompt-file> [input_file] [timeout_ms] [--panel]
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { resolveRef, putBlob, sentOk, sendError } from './output_transport.ts'

let _redis: RedisClient | null = null
const redis = () => (_redis ??= new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379'))

const PROMPT_FILE = process.argv[2]
// argv[3] is input_file ONLY if it's a real path — never a bare integer (that's the
// optional timeout_ms, e.g. `fusion-ask "q"` with no input file passes the timeout here).
const INPUT_FILE = process.argv[3] && !process.argv[3].startsWith('--') && !/^\d+$/.test(process.argv[3]) ? process.argv[3] : undefined
const TIMEOUT_MS = Number(process.argv.find((a) => /^\d+$/.test(a) && Number(a) > 1000) ?? 28_800_000) // 8h library default (≥ service ≥ gateway) for programmatic/ML callers; interactive wrappers pass a short bound DOWNWARD
const PANEL_ONLY = process.argv.includes('--panel')
const TARGET = process.env.FUSION_SVC_ID ?? 'fusion-svc'
if (!PROMPT_FILE) { console.error('usage: bun fusion_client.ts <prompt-file> [input_file] [timeout_ms] [--panel]'); process.exit(2) }

const prompt = (await Bun.file(PROMPT_FILE).text()).trim()
const job_id = `freq-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`
let resolveDone: (s: any) => void
const done = new Promise<any>((r) => { resolveDone = r })

function onInbound(content: string, attrs: any) {
  if (attrs?.feed !== 'a2a') return
  let m: any = null; try { m = JSON.parse(content) } catch { return }
  if (m?.job_id !== job_id) return
  if (m.schema === 'fusion.job.completed.v1' || m.schema === 'fusion.job.failed.v1') resolveDone(m)
}

const a2a = new A2AChannel(onInbound as any, { enabled: true, agentId: process.env.A2A_AGENT_ID || 'fusion' })
await a2a.start()
if (!a2a.isStarted()) { console.error('[client] FATAL: bus join failed'); process.exit(1) }
console.error(`[client] dispatching Alloyium Panel job ${job_id} → ${TARGET}`)
// REMOTE-SAFE input: claim-check the input file into a Redis blob (fusion-svc on host-1
// reads the blob from shared Redis — NEVER the caller's local filesystem) and pass a tiny
// `input_ref`, instead of a local path host-1 cannot open. Works identically local/remote;
// the question (prompt) stays inline (small). REDIS_URL must point at the bus's Redis.
let input_ref: { result_ref: string; sha256: string; len: number } | undefined
if (INPUT_FILE) {
  let data: string
  try { data = await Bun.file(INPUT_FILE).text() }
  catch (e) { console.error(`[client] FATAL: cannot read input file '${INPUT_FILE}': ${e}`); process.exit(2) }
  try { const ref = await putBlob(redis(), { text: data }); input_ref = { result_ref: ref.result_ref, sha256: ref.sha256, len: ref.len } }
  catch (e) { console.error(`[client] FATAL: cannot claim-check input to Redis (${process.env.REDIS_URL ?? 'redis://redis:6379'}) — is it reachable from this host? ${e}`); process.exit(1) }
  console.error(`[client] input claim-checked → ${input_ref.result_ref} (${input_ref.len}B)`)
}
const dispatch = await a2a.callTool('a2a_send', { to: TARGET, type: 'request', body: JSON.stringify({ schema: 'fusion.job.request.v1', job_id, prompt, input_ref, panel_only: PANEL_ONLY }) })
if (!sentOk(dispatch)) { console.error(`[client] FATAL dispatch rejected: ${sendError(dispatch)} — the bare question exceeds the send cap? (the input file rides a claim-check blob, so only the question is inline.)`); process.exit(1) }

const timeout = new Promise<any>((r) => setTimeout(() => r({ schema: '__TIMEOUT__' }), TIMEOUT_MS))
const res = await Promise.race([done, timeout])
if (res.schema === 'fusion.job.completed.v1') {
  const panel = res.panel ?? {}; const refs = res.panel_refs ?? {}
  // resolve a claim-checked panel entry (large outputs ride a blob+ref)
  const resolvePanel = (k: string) => resolveRef(redis(), { result_ref: refs[k]?.result_ref, sha256: refs[k]?.sha256, len: refs[k]?.len, output: panel[k] }, (reason) => console.error(`[client] panel ${k} blob ${reason}`))
  if (res.fused != null || res.result_ref) {
    console.log(await resolveRef(redis(), { result_ref: res.result_ref, sha256: res.sha256, len: res.len, output: res.fused }, (reason) => console.error(`[client] blob ${reason}`)))
  } else {
    // panel-only: resolve EVERY panel entry before printing — not the raw marked previews
    const resolved: Record<string, string> = {}
    for (const k of Object.keys(panel)) resolved[k] = await resolvePanel(k)
    console.log(JSON.stringify(resolved, null, 2))
  }
  if (process.env.FUSION_SHOW_PANEL) for (const k of Object.keys(panel)) console.error(`\n===== ALLOYIUM PANEL: ${k} =====\n` + await resolvePanel(k))
  process.exit(0)
} else { console.error('[client] ' + (res.schema || 'no-reply') + (res.error ? ': ' + res.error : '')); process.exit(1) }
