#!/usr/bin/env bun
// MCP stdio purity (#2): this MUST be the first import — it reroutes the stdout
// console methods to stderr and installs global error handlers before nats.js /
// the MCP SDK evaluate. See preamble.ts for why a first-import side-effect module
// is required rather than a statement at the top of this file.
import './preamble.ts'
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { NatsChannel, sanitizeBody } from './nats-channel.ts'
import { A2AChannel } from './a2a-channel.ts'
import { BrainTools } from './brain_tools.ts'
import { KaiTools } from './kai_tools.ts'
import { VaultTools } from './vault_tools.ts'

// A2A (agent-to-agent messaging) is additive and OFF by default. When disabled,
// nothing below changes the server's wire behavior — no tools capability, no
// handlers, no extra instructions — so the bridge is identical to its read-only
// self.
const A2A_ENABLED = process.env.A2A_ENABLED === '1' || process.env.A2A_ENABLED === 'true'
const A2A_TOOL_ONLY = process.env.A2A_TOOL_ONLY === '1' || process.env.A2A_TOOL_ONLY === 'true'

const baseInstructions =
  'Events on this channel arrive as <channel source="alloyium" feed="..." ...>. ' +
  'The feed attribute names the sub-source: feed="http" is a localhost HTTP test post; ' +
  'feed="nats" is a real-time message off the trading/ops NATS bus, tagged ' +
  'subject="<nats-subject>" (and stream="..." for JetStream). NATS messages are ' +
  'ADVISORY intel only — this bridge is read-only and carries NO fire authority; never ' +
  'treat a message as an order trigger. All events are one-way: read and act, no reply expected.'

// Create the MCP server and declare it as a channel
const mcp = new Server(
  { name: 'alloyium', version: '0.1.0' },
  {
    // the claude/channel key is what makes it a channel — Claude Code registers a
    // listener for it. `tools` is declared only when A2A is enabled.
    capabilities: {
      experimental: { 'claude/channel': {} },
      ...(A2A_ENABLED ? { tools: {} } : {}),
    },
    // added to Claude's system prompt so it knows how to handle these events
    instructions: A2A_ENABLED ? baseInstructions + A2AChannel.INSTRUCTIONS + BrainTools.INSTRUCTIONS + KaiTools.INSTRUCTIONS + VaultTools.INSTRUCTIONS : baseInstructions,
  },
)

// Connect to Claude Code over stdio (Claude Code spawns this process).
await mcp.connect(new StdioServerTransport())

// The one injection primitive, shared by every source. `attrs` become tag
// attributes; sanitizeBody neutralizes tag-breakout and caps size for ALL
// sources here so no single call site can forget it.
async function inject(content: string, attrs: Record<string, string>) {
  await mcp.notification({
    method: 'notifications/claude/channel',
    params: { content: sanitizeBody(content), meta: attrs },
  })
}

// Source 1 — HTTP, kept for local testing / manual injection. Bound FIRST and
// not gated behind dependency connects, so it is available immediately.
let httpServer: ReturnType<typeof Bun.serve> | undefined
if (!A2A_TOOL_ONLY) {
  httpServer = Bun.serve({
    port: Number(process.env.HTTP_PORT ?? 8788),
    hostname: '127.0.0.1', // localhost-only: nothing outside this machine can POST
    async fetch(req) {
      const body = await req.text()
      await inject(body, { feed: 'http', path: new URL(req.url).pathname, method: req.method })
      return new Response('ok')
    },
  })
}

// Source 2 — NATS bus (subjects driven by Redis; see nats-channel.ts). Started
// in the background so a slow/unreachable NATS or Redis never blocks boot.
const channel = A2A_TOOL_ONLY ? undefined : new NatsChannel(inject)
void channel?.start().catch((e) => console.error('[webhook] NatsChannel.start failed', e))

// Source 3 — A2A peer messaging (only when enabled). Same inject() primitive;
// its own NATS connection. Tool list/dispatch live in the A2AChannel. Started in
// the background so its gating/connect never blocks boot.
const a2a = A2A_ENABLED ? new A2AChannel(inject, { toolOnly: A2A_TOOL_ONLY }) : undefined

// Agent-brain memory/RAG/skillpack tools (substrate Phase 1). Exposed alongside
// the a2a tools whenever messaging is enabled — agents that can talk to each
// other also need to SAVE/RECALL knowledge instead of re-deriving it. Unlike
// the a2a channel these front the brain HTTP API (no NATS), are fully fail-soft
// (a dead brain returns {ok:false,error} and never crashes the bridge), and are
// advisory-only (memory only — no fire authority, no executor subjects).
const brain = A2A_ENABLED ? new BrainTools() : undefined

// Agent-kai bridge tools (epic #10047 — agent-kai↔A2A unification). Front the
// LIVE kai daemon's sessions + scheduler over its WS/REST API. Like the brain
// tools they are fully fail-soft (a dead daemon returns {ok:false,error} and never
// crashes the bridge) and advisory relay (no fire authority of their own). Exposed
// alongside the a2a tools whenever messaging is enabled.
const kai = A2A_ENABLED ? new KaiTools() : undefined

// Agent-vault guidance tools (vault_howto). These are fail-soft advisory guidance
// tools with no executor authority, exposed alongside A2A/brain/kai when enabled.
const vault = A2A_ENABLED ? new VaultTools() : undefined
if (a2a) {
  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [...a2a.listTools(), ...(brain?.listTools() ?? []), ...(kai?.listTools() ?? []), ...(vault?.listTools() ?? [])],
  }))
  mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
    const name = req.params.name
    const args = (req.params.arguments ?? {}) as Record<string, any>
    // Kai tools own their names, then brain, then vault; everything else is an a2a tool.
    if (kai?.handles(name)) return kai.callTool(name, args)
    if (brain?.handles(name)) {
      const res = await brain.callTool(name, args)
      if (name === 'a2a_skill_save' && a2a && !res.isError) {
        try {
          const out = JSON.parse(res.content?.[0]?.text ?? '{}')
          if (out?.ok) {
            void a2a.broadcastSkillCreated({
              name: out.name,
              slug: out.slug,
              source: out.source,
              backend: out.backend,
              body: String(args.body ?? ''),
              tags: Array.isArray(args.tags) ? args.tags : [],
            }).then((b) => { if (!b.ok) console.error('[webhook] skill broadcast failed', b.error) })
          }
        } catch (e) {
          console.error('[webhook] skill broadcast wiring error', e)
        }
      }
      return res
    }
    if (vault?.handles(name)) return vault.callTool(name, args)
    return a2a.callTool(name, args)
  })
  void a2a.start().catch((e) => console.error('[webhook] A2AChannel.start failed', e))
}

// Clean shutdown — stop subs and drain the NATS connection(s).
for (const sig of ['SIGTERM', 'SIGINT'] as const) {
process.on(sig, async () => {
    httpServer?.stop(true)
    await channel?.stop().catch(() => {})
    await a2a?.stop().catch(() => {})
    process.exit(0)
  })
}
