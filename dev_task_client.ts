// dev_task_client.ts — one-line dispatch of a build/spec/fix task to the STANDING
// dev-pm coordinator over the A2A bus (mirrors fusion_client.ts → fusion-svc).
//
// dev-pm is a long-running Claude agent, not a fast RPC, so the client awaits a
// prompt-ack (dev.task.accepted.v1) — confirming dev-pm received + validated the
// request — then exits; `--wait` additionally blocks for dev.task.completed.v1 /
// failed.v1. The durable inbox (DeliverPolicy.All) guarantees delivery even when
// dev-pm is mid-task, so a missed ack means "delivered, not yet processed".
//
//   A2A_AGENT_ID=dev-task A2A_SIGNING_KEY=.../dev-task.seed NATS_URL=... REDIS_URL=... \
//     bun dev_task_client.ts <prompt-file> [input_file] [--wait]
import { A2AChannel } from './a2a-channel.ts'

const TOKEN_RE = /^[a-z0-9-]{1,64}$/

// ── pure, unit-tested helpers (no bus) ─────────────────────────────────────────

export type ParsedArgs = { promptFile?: string; inputFile?: string; wait: boolean; error?: string }
/** Single source of truth for argv: `<prompt-file> [input_file] [--wait]`. Rejects
 *  unknown `--flags` and a second positional rather than silently overwriting. */
export function parseArgs(argv: string[]): ParsedArgs {
  const pos: string[] = []; let wait = false
  for (const a of argv) {
    if (a === '--wait') { wait = true; continue }
    if (a.startsWith('--')) return { wait, error: `unknown flag: ${a}` }
    pos.push(a)
  }
  if (pos.length === 0) return { wait, error: 'missing <prompt-file>' }
  if (pos.length > 2) return { wait, error: `unexpected extra argument: ${pos[2]}` }
  return { promptFile: pos[0], inputFile: pos[1], wait }
}

export type DevTaskRequest = { schema: 'dev.task.request.v1'; task_id: string; title: string; body?: string; input_file?: string; reply_to?: string }
/** Build the request envelope. id/clock injectable for deterministic tests. */
export function buildRequest(title: string, body: string | undefined, inputFile: string | undefined, opts: { taskId?: string; genId?: () => string; replyTo?: string } = {}): DevTaskRequest {
  const task_id = opts.taskId ?? (opts.genId ?? (() => `dtask-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`))()
  const req: DevTaskRequest = { schema: 'dev.task.request.v1', task_id, title }
  if (body) req.body = body
  if (inputFile) req.input_file = inputFile
  if (opts.replyTo) req.reply_to = opts.replyTo
  return req
}

/** Unwrap the a2a_send MCP result {content:[{text:'{"ok":true,"id":..,"mode":"jetstream","seq":N}'}]}. */
export function unwrapToolResult(res: any): { ok: boolean; id?: string; mode?: string; seq?: number } | null {
  try { const t = res?.content?.[0]?.text; if (typeof t !== 'string') return null; const o = JSON.parse(t); return { ok: o.ok === true, id: o.id, mode: o.mode, seq: o.seq } } catch { return null }
}

export type ReplyKind = 'accepted' | 'rejected' | 'completed' | 'failed' | 'ignore'
/** Strict envelope gating: a reply must be on the a2a/direct feed, FROM the target,
 *  TO us, carry our task_id, and (for the accept) corr-match our request id. */
export function classifyReply(content: string, attrs: any, ctx: { taskId: string; requestId?: string; target: string; selfId: string }): { kind: ReplyKind; msg?: any } {
  if (attrs?.feed !== 'a2a' || attrs?.kind !== 'direct') return { kind: 'ignore' }
  if (attrs.from !== ctx.target || attrs.to !== ctx.selfId) return { kind: 'ignore' }
  let m: any; try { m = JSON.parse(content) } catch { return { kind: 'ignore' } }
  if (m?.task_id !== ctx.taskId) return { kind: 'ignore' }
  if (attrs.type === 'reply') {
    if (ctx.requestId && attrs.corr !== ctx.requestId) return { kind: 'ignore' }
    if (m.schema === 'dev.task.accepted.v1') return { kind: 'accepted', msg: m }
    if (m.schema === 'dev.task.rejected.v1') return { kind: 'rejected', msg: m }
    return { kind: 'ignore' }
  }
  if (attrs.type === 'msg') {
    if (m.schema === 'dev.task.completed.v1') return { kind: 'completed', msg: m }
    if (m.schema === 'dev.task.failed.v1') return { kind: 'failed', msg: m }
  }
  return { kind: 'ignore' }
}

// ── runtime (skipped when imported under `bun test`) ───────────────────────────

async function main(): Promise<void> {
  const ACCEPT_MS = Number(process.env.DEV_TASK_ACCEPT_MS ?? 30_000)
  const WAIT_MS = Number(process.env.DEV_TASK_WAIT_MS ?? 3_600_000)
  const TARGET = process.env.DEV_PM_ID ?? 'dev-pm'
  const SELF = process.env.A2A_AGENT_ID || 'dev-task'

  const parsed = parseArgs(process.argv.slice(2))
  if (parsed.error || !parsed.promptFile) { console.error(`dev_task_client: ${parsed.error ?? 'usage'}\nusage: bun dev_task_client.ts <prompt-file> [input_file] [--wait]`); process.exit(2) }
  if (!TOKEN_RE.test(TARGET)) { console.error(`dev_task_client: invalid DEV_PM_ID '${TARGET}' — must be a bare agent-id (not a topic).`); process.exit(2) }

  const raw = await Bun.file(parsed.promptFile).text()
  const nl = raw.indexOf('\n')
  const title = (nl < 0 ? raw : raw.slice(0, nl)).trim()
  const body = (nl < 0 ? '' : raw.slice(nl + 1)).trim() || undefined
  const req = buildRequest(title, body, parsed.inputFile, { replyTo: SELF })

  let resolveAccept!: (v: { kind: ReplyKind; msg?: any }) => void
  const acceptedP = new Promise<{ kind: ReplyKind; msg?: any }>((r) => { resolveAccept = r })
  let resolveDone!: (v: { kind: ReplyKind; msg?: any }) => void
  const completedP = new Promise<{ kind: ReplyKind; msg?: any }>((r) => { resolveDone = r })
  let requestId: string | undefined

  const onInbound = (content: string, attrs: any) => {
    const c = classifyReply(content, attrs, { taskId: req.task_id, requestId, target: TARGET, selfId: SELF })
    if (c.kind === 'accepted' || c.kind === 'rejected') resolveAccept(c)
    else if (c.kind === 'completed' || c.kind === 'failed') resolveDone(c)
  }

  const a2a = new A2AChannel(onInbound as any, { enabled: true, agentId: SELF })
  let exitCode = 0
  try {
    await a2a.start()
    if (!a2a.isStarted()) { console.error(`[dev-task] FATAL: bus join failed (dup '${SELF}' presence, or run \`bun onboard.ts ${SELF} --transport none\` first).`); exitCode = 1; return }

    const send = unwrapToolResult(await a2a.callTool('a2a_send', { to: TARGET, type: 'request', body: JSON.stringify(req) }))
    if (!send || !send.ok || send.mode !== 'jetstream' || typeof send.seq !== 'number') {
      console.error(`[dev-task] FATAL: dispatch not durably published (${JSON.stringify(send)}) — task NOT delivered. Body too large? use input_file.`); exitCode = 1; return
    }
    requestId = send.id
    console.error(`[dev-task] dispatched ${req.task_id} → ${TARGET} (seq ${send.seq})`)

    const acc = await Promise.race([acceptedP, new Promise<{ kind: ReplyKind }>((r) => setTimeout(() => r({ kind: 'ignore' }), ACCEPT_MS))])
    if (acc.kind === 'accepted') console.error(`[dev-task] accepted by ${TARGET} (task_id=${req.task_id})`)
    else if (acc.kind === 'rejected') { console.error(`[dev-task] REJECTED by ${TARGET}: ${(acc as any).msg?.reason ?? '?'}`); exitCode = 1 }
    else console.error(`[dev-task] no prompt-ack in ${ACCEPT_MS}ms — delivered to ${TARGET}'s durable inbox; it will process when idle (delivery ≠ validation).`)
    console.log(req.task_id) // stdout = the task id (scriptable)

    if (parsed.wait && acc.kind !== 'rejected') {
      const d = await Promise.race([completedP, new Promise<{ kind: ReplyKind }>((r) => setTimeout(() => r({ kind: 'ignore' }), WAIT_MS))])
      if (d.kind === 'completed') {
        const m = (d as any).msg
        console.error(`[dev-task] COMPLETED: ${m.summary ?? ''}`)
        if (Array.isArray(m.commits) && m.commits.length) console.error(`  commits: ${m.commits.join(', ')}`)
        if (m.verification_file) console.error(`  verification: ${m.verification_file}`)
      } else if (d.kind === 'failed') { console.error(`[dev-task] FAILED: ${(d as any).msg?.error ?? '?'}`); exitCode = 1 }
      else { console.error(`[dev-task] no completion in ${WAIT_MS}ms (still in progress; task_id=${req.task_id}).`); exitCode = 1 }
    }
  } finally {
    await a2a.stop().catch(() => {}) // release presence:<self> so the next sequential call doesn't dup-lock
  }
  process.exit(exitCode)
}

if (import.meta.main) void main()
