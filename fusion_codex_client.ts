// fusion_codex_client.ts — dispatch a job to the RUNNING codex-gw app-server over
// the A2A bus and await the completed reply. This is the CORRECT way to reach
// GPT-5.5/codex in the mesh — NOT `codex exec` (which spawns a fresh ephemeral
// process with no bus, no persistent app-server session).
//
//   A2A_AGENT_ID=fusion A2A_SIGNING_KEY=.../fusion.seed NATS_URL=... REDIS_URL=... \
//     bun fusion_codex_client.ts <prompt-file> [timeout_ms]
//
// Contract (see codex_gateway.ts): send a signed request to codex-gw's inbox with
// body = codex.job.request.v1 {job_id, input[], sandbox, approval_policy, cwd};
// receive codex.job.accepted.v1 then codex.job.completed.v1 (corr-matched). Large
// output rides a claim-check blob (result_ref+sha256+len) which we resolve here.
import { A2AChannel } from './a2a-channel.ts'
import { RedisClient } from 'bun'
import { getBlob, buildClaimCheckedInput, sentOk, sendError } from './output_transport.ts'

let _redis: RedisClient | null = null
const redis = () => (_redis ??= new RedisClient(process.env.REDIS_URL ?? 'redis://redis:6379'))

const PROMPT_FILE = process.argv[2]
const TIMEOUT_MS = Number(process.argv[3] ?? 28_800_000) // 8h library default (≥ service ≥ gateway) for programmatic/ML callers; interactive wrappers pass a short bound DOWNWARD
const TARGET = process.env.CODEX_GW_ID ?? 'codex-gw'
if (!PROMPT_FILE) { console.error('usage: bun fusion_codex_client.ts <prompt-file> [timeout_ms]'); process.exit(2) }

const prompt = (await Bun.file(PROMPT_FILE).text()).trim()
const job_id = `fusion-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`

let resolveDone: (s: string) => void
const done = new Promise<string>((r) => { resolveDone = r })

function onInbound(content: string, attrs: any) {
  if (attrs?.feed !== 'a2a') return
  let m: any = null; try { m = JSON.parse(content) } catch { return }
  if (m?.job_id !== job_id) return
  if (m.schema === 'codex.job.accepted.v1') { console.error(`[client] ACCEPTED ${job_id} (codex budget ${m.primary_used_pct ?? '?'}%)`); return }
  if (m.schema === 'codex.job.completed.v1') {
    // large output is claim-checked (blob+ref); resolve it losslessly, else fall to preview.
    if (m.result_ref) {
      void getBlob(redis(), { result_ref: m.result_ref, sha256: m.sha256, len: m.len }).then((r) => {
        if (r.ok) resolveDone(r.text)
        else { console.error(`[client] blob ${r.reason} — falling back to inline preview`); resolveDone(m.output ?? `(blob ${r.reason})`) }
      })
    } else resolveDone(m.output ?? '(empty output)')
  }
  else if (m.schema === 'codex.job.failed.v1') resolveDone(`__FAILED__ ${m.error}`)
  else if (m.schema === 'codex.job.rejected.v1') resolveDone(`__REJECTED__ ${m.reason}`)
}

const a2a = new A2AChannel(onInbound as any, { enabled: true, agentId: process.env.A2A_AGENT_ID || 'fusion' })
await a2a.start()
if (!a2a.isStarted()) { console.error('[client] FATAL: could not join bus'); process.exit(1) }
console.error(`[client] joined bus as '${process.env.A2A_AGENT_ID}', dispatching ${job_id} → ${TARGET}`)

// Claim-check the prompt if it exceeds the send cap: dispatch a tiny `input_ref` + preview
// that codex-gw resolves, instead of inlining a body the bus would REJECT (it never
// truncates). Keeps the cap at 8 KiB — the RCA-sanctioned cure, not raising the number.
const wrap = (text: string, extra?: Record<string, unknown>) => ({ schema: 'codex.job.request.v1', job_id, input: [{ type: 'text', text }], sandbox: 'read-only', approval_policy: 'never', cwd: '/tmp', ...(extra ?? {}) })
let built: { body: unknown; ref: string | null }
try { built = await buildClaimCheckedInput(redis(), prompt, a2a.getMaxSendBytes(), wrap) }
catch (e) { console.error(`[client] FATAL: input claim-check failed (Redis down?): ${e}`); process.exit(1) }
const dispatch = await a2a.callTool('a2a_send', { to: TARGET, type: 'request', body: JSON.stringify(built.body) })
// A rejected request would otherwise look like a silent timeout — fail loudly with the
// real reason. Don't delBlob: a send-reject can be ambiguous; the blob TTL reclaims it.
if (!sentOk(dispatch)) { console.error(`[client] FATAL dispatch rejected: ${sendError(dispatch)}`); process.exit(1) }

const timeout = new Promise<string>((r) => setTimeout(() => r('__TIMEOUT__'), TIMEOUT_MS))
const out = await Promise.race([done, timeout])
console.log(out)
process.exit(out.startsWith('__') ? 1 : 0)
