import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { A2AChannel } from './a2a-channel.ts'
import { BrainTools } from './brain_tools.ts'
import { KaiTools } from './kai_tools.ts'
import { VaultTools } from './vault_tools.ts'
import { AgentLauncherTools } from './agent_launcher_tools.ts'
import { AccessTokenIssuerTools } from './access_token_issuer.ts'

const baseInstructions =
  'Events on this channel arrive as <channel source="alloyium" feed="..." ...>. ' +
  'The feed attribute names the sub-source: feed="http" is a localhost HTTP test post; ' +
  'feed="nats" is a real-time message off the trading/ops NATS bus, tagged ' +
  'subject="<nats-subject>" (and stream="..." for JetStream). NATS messages are ' +
  'ADVISORY intel only — this bridge is read-only and carries NO fire authority; never ' +
  'treat a message as an order trigger. All events are one-way: read and act, no reply expected.'

function positiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name]
  const parsed = raw === undefined || raw === '' ? fallback : Number(raw)
  const n = Math.trunc(parsed)
  return Number.isFinite(n) && n > 0 ? n : fallback
}

export interface SessionCtx {
  agentId: string
  channel: A2AChannel
  brain: BrainTools
  kai: KaiTools
  vault?: VaultTools
  access?: AccessTokenIssuerTools
  launcher?: AgentLauncherTools
  inject: (notif: unknown) => void | Promise<void>
}

function asJsonRpcNotification(notif: Parameters<Server['notification']>[0]): unknown {
  return { jsonrpc: '2.0', ...notif }
}

export function buildSessionMcpServer(ctx: SessionCtx): Server {
  const launcherTools = ctx.launcher?.listTools() ?? []
  const tools = [
    ...ctx.channel.listTools(),
    ...ctx.brain.listTools(),
    ...ctx.kai.listTools(),
    ...(ctx.vault?.listTools() ?? []),
    ...(ctx.access?.listTools() ?? []),
    ...launcherTools,
  ]
  const pendingInjectCap = positiveIntEnv('A2A_MCP_PENDING_INJECT_CAP', 256)
  const mcp = new Server(
    { name: 'alloyium', version: '0.1.0' },
    {
      capabilities: {
        experimental: { 'claude/channel': {} },
        tools: {},
      },
      instructions:
        baseInstructions +
        A2AChannel.INSTRUCTIONS +
        BrainTools.INSTRUCTIONS +
        KaiTools.INSTRUCTIONS +
        (ctx.vault ? VaultTools.INSTRUCTIONS : '') +
        (ctx.access ? AccessTokenIssuerTools.INSTRUCTIONS : '') +
        (launcherTools.length ? AgentLauncherTools.INSTRUCTIONS : ''),
    },
  )

  // B4 (gate-fold): a bus channel event must NOT reach the client before its MCP
  // initialize/initialized handshake completes. Buffer claude/channel injects until the SDK
  // fires oninitialized, then flush IN ORDER. Durable-ack is preserved: a buffered inject's
  // promise resolves only after its REAL delivery (so the bus message is never acked early; if
  // the session is never initialized the promise stays pending and the message redelivers).
  let initialized = false
  const pendingInjects: Array<{ notif: unknown; resolve: () => void; reject: (e: unknown) => void }> = []
  // P2 fold (3-SoD): serialize ALL claude/channel deliveries through ONE chain so a notif arriving
  // DURING the post-init flush (while a buffered inject is mid-`await ctx.inject`) is delivered AFTER
  // it, never interleaved — strict in-order (was: A,C,B out-of-order under concurrent delivery).
  // Durable-ack preserved: each caller awaits its OWN link (resolves only after its real delivery).
  let deliveryChain: Promise<void> = Promise.resolve()

  mcp.oninitialized = () => {
    initialized = true
    const queued = pendingInjects.splice(0)
    deliveryChain = deliveryChain.then(async () => {
      for (const q of queued) {
        try {
          await ctx.inject(asJsonRpcNotification(q.notif as Parameters<Server['notification']>[0]))
          q.resolve()
        } catch (e) {
          q.reject(e)
        }
      }
    })
  }

  // P2 fold: if the session closes BEFORE initialize, reject any still-buffered injects so the
  // bus inbox naks -> JetStream redelivers promptly (instead of leaking until ack_wait).
  mcp.onclose = () => {
    for (const q of pendingInjects.splice(0)) {
      try { q.reject(new Error('session closed before initialize')) } catch {}
    }
  }

  const notification = mcp.notification.bind(mcp)
  mcp.notification = (async (
    notif: Parameters<typeof mcp.notification>[0],
    options?: Parameters<typeof mcp.notification>[1],
  ) => {
    if (notif.method === 'notifications/claude/channel') {
      if (initialized) {
        // chain after the flush + any prior post-init injects so delivery stays strictly in order
        const done = deliveryChain.then(() => ctx.inject(asJsonRpcNotification(notif)))
        deliveryChain = done.then(() => {}, () => {}) // keep the chain alive regardless of this inject's outcome
        await done
        return
      }
      await new Promise<void>((resolve, reject) => {
        if (pendingInjects.length >= pendingInjectCap) {
          reject(new Error(`pre-initialize notification buffer full (${pendingInjectCap})`))
          return
        }
        pendingInjects.push({ notif, resolve, reject })
      })
      return
    }
    return notification(notif, options)
  }) as typeof mcp.notification

  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [...tools],
  }))

  mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
    const name = req.params.name
    const args = (req.params.arguments ?? {}) as Record<string, any>
    if (ctx.kai.handles(name)) return ctx.kai.callTool(name, args)
    if (ctx.brain.handles(name)) {
      const res = await ctx.brain.callTool(name, args)
      if (name === 'a2a_skill_save' && !res.isError) {
        try {
          const out = JSON.parse(res.content?.[0]?.text ?? '{}')
          const broadcastSkillCreated = (ctx.channel as any).broadcastSkillCreated
          if (out?.ok && typeof broadcastSkillCreated === 'function') {
            void broadcastSkillCreated.call(ctx.channel, {
              name: out.name,
              slug: out.slug,
              source: out.source,
              backend: out.backend,
              body: String(args.body ?? ''),
              tags: Array.isArray(args.tags) ? args.tags : [],
            }).then((b: any) => { if (!b?.ok) console.error('[mcp-session] skill broadcast failed', b?.error) })
          }
        } catch (e) {
          console.error('[mcp-session] skill broadcast wiring error', e)
        }
      }
      return res
    }
    if (ctx.vault?.handles(name)) return ctx.vault.callTool(name, args)
    if (ctx.access?.handles(name)) return ctx.access.callTool(name, args)
    if (ctx.launcher?.handles(name)) return ctx.launcher.callTool(name, args)
    return ctx.channel.callTool(name, args)
  })

  return mcp
}
