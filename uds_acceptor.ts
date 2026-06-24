import { constants as FS } from 'node:fs'
import { chmod, lstat, mkdir, unlink } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { RedisClient } from 'bun'
import type { A2ACore } from './a2a_core.ts'
import { runHello } from './uds_hello.ts'
import {
  FRAME_TYPE_CTRL,
  FRAME_TYPE_MCP,
  FrameDecoder,
  FrameProtocolError,
  FrameWriter,
  PendingMap,
} from './uds_frame.ts'
import type { CtrlRequestId, DecodedFrame } from './uds_frame.ts'
import {
  UdsServerTransport,
  feedCtrlDelivered,
  feedCtrlSig,
  makeUdsInject,
  makeUdsSign,
} from './uds_transport.ts'
import type { DeliveryReply, SignReply } from './uds_transport.ts'

const enc = new TextEncoder()
const dec = new TextDecoder()
const DIR_MODE = FS.S_IRWXU || 0o700
const SOCK_MODE = (FS.S_IRUSR | FS.S_IWUSR | FS.S_IRGRP | FS.S_IWGRP) || 0o660

export interface UdsAcceptorOptions {
  core: A2ACore
  redis: RedisClient
  socketPath: string
  socketDir?: string
  expectedUid?: number
  idleTimeoutMs?: number // close a connection that makes no frame progress for this long (0 disables)
  helloTimeoutMs?: number // bound the pre-auth hello/challenge/auth handshake (runHello default 5s)
}

export interface UdsAcceptorHandle {
  socketPath: string
  close(): Promise<void>
}

type Phase = 'hello' | 'authed' | 'session' | 'closed'

// Pre-session DoS caps: an unauthenticated peer may only buffer this much before the session
// is live (hello CTRL frames; MCP frames buffered until initialize). Exceed -> fail the conn.
const MAX_PRE_HELLO_CTRL = 8
const MAX_PRE_SESSION_MCP_FRAMES = 16
const MAX_PRE_SESSION_BYTES = 1024 * 1024
// Slowloris guard: close an ESTABLISHED conn that makes no frame progress for this long.
const DEFAULT_IDLE_TIMEOUT_MS = 120_000
const DELIVERY_ACK_TIMEOUT_MS = Math.max(1_000, Number(process.env.A2A_DELIVERY_ACK_TIMEOUT_MS ?? 30_000) || 30_000)
const DELIVERY_MAX_INFLIGHT = Math.max(1, Number(process.env.A2A_DELIVERY_MAX_INFLIGHT ?? 64) || 64)
type BunSocket = any
type BunServer = { stop: () => unknown }

function log(level: 'info' | 'warn' | 'error', event: string, fields: Record<string, unknown> = {}): void {
  const kv = Object.entries(fields)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`)
    .join(' ')
  console.error(`${new Date().toISOString()} ${level} [uds-acceptor] ${event}${kv ? ' ' + kv : ''}`)
}

function errFields(err: unknown): Record<string, unknown> {
  return err instanceof Error
    ? { err: err.message, name: err.name, code: (err as any).code }
    : { err: String(err) }
}

function isEnoent(err: unknown): boolean {
  return !!err && typeof err === 'object' && (err as any).code === 'ENOENT'
}

// §A.7 liveness ping: { t:'ping', ts? }. The acceptor pongs { t:'pong', ts } in ANY phase, UNSIGNED —
// pre-hello there is no session key (the `a2a-shim --ping` boots-MCP-dead launch gate is pre-session),
// and in-session the transport is already the authenticated channel. The 0700 dir + peercred gate WHO
// can connect; a pong echoing only ts leaks nothing.
function isCtrlPing(obj: unknown): obj is { t: 'ping'; ts?: unknown } {
  return !!obj && typeof obj === 'object' && (obj as Record<string, unknown>).t === 'ping'
}

function toU8(chunk: Uint8Array | ArrayBuffer): Uint8Array {
  if (chunk instanceof Uint8Array) return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
  return new Uint8Array(chunk)
}

function destroySocket(socket: BunSocket): void {
  try {
    if (typeof socket.destroy === 'function') socket.destroy()
    else if (typeof socket.terminate === 'function') socket.terminate()
    else if (typeof socket.close === 'function') socket.close()
    else if (typeof socket.end === 'function') socket.end()
  } catch {}
}

function peerUidFromSocket(socket: BunSocket): number | undefined {
  // peercred best-effort; 0700 dir is the access control (gate to assess FFI getsockopt)
  try {
    const creds = typeof socket.getPeerCredentials === 'function'
      ? socket.getPeerCredentials()
      : socket.peerCredentials
    const uid = creds?.uid
    return Number.isSafeInteger(uid) ? uid : undefined
  } catch {
    return undefined
  }
}

async function prepareSocketPath(socketDir: string, socketPath: string): Promise<void> {
  await mkdir(socketDir, { recursive: true, mode: DIR_MODE })
  await chmod(socketDir, DIR_MODE)

  try {
    const st = await lstat(socketPath)
    if (!st.isSocket()) throw new Error(`refusing to bind ${socketPath}: existing path is not a socket`)
    await unlink(socketPath)
  } catch (err) {
    if (!isEnoent(err)) throw err
  }
}

async function unlinkSocketIfSocket(socketPath: string): Promise<void> {
  try {
    const st = await lstat(socketPath)
    if (st.isSocket()) await unlink(socketPath)
  } catch (err) {
    if (!isEnoent(err)) log('warn', 'uds_unlink_failed', { socket_path: socketPath, ...errFields(err) })
  }
}

class UdsConnection {
  private phase: Phase = 'hello'
  private readonly decoder = new FrameDecoder()
  private readonly ctrlPending = new PendingMap<CtrlRequestId, SignReply>()
  private readonly deliveryPending = new PendingMap<string, DeliveryReply>()
  private readonly helloQ: any[] = []
  private readonly helloWaiters: ((v: any) => void)[] = []
  private readonly mcpBuffer: Uint8Array[] = []
  private preSessionBytes = 0
  private preHelloPings = 0 // §A.7: bound pre-hello liveness pings (same cap as hello CTRLs)
  private readonly writer: FrameWriter
  private transport?: UdsServerTransport
  private session?: { agentId: string; epoch: number }
  private teardownPromise?: Promise<void>
  private drainTail: Promise<void> = Promise.resolve()
  private idleTimer?: ReturnType<typeof setTimeout>

  constructor(
    private readonly socket: BunSocket,
    private readonly opts: UdsAcceptorOptions,
    private readonly unregister: () => void,
  ) {
    this.writer = new FrameWriter(
      (bytes) => {
        if (this.phase === 'closed') return 0
        try {
          // Bun's socket.write SHORT-WRITES under backpressure (does NOT buffer the remainder):
          // return the accepted count so FrameWriter pauses and resumes from onWritableDrain().
          return this.socket.write(bytes)
        } catch (err) {
          this.fail('socket_write_failed', err)
          return 0
        }
      },
      { highWater: 1024, maxBytes: 32 * 1024 * 1024, onOverflow: () => this.fail('writer_overflow') },
    )
  }

  start(): void {
    void this.run().catch((err) => this.fail('connection_run_failed', err))
  }

  // Read-progress / slowloris guard (gate-fold P1): a peer that announces a frame length then
  // DRIBBLES (never completes the frame) holds buffered bytes hostage. Arm a deadline ONCE while a
  // partial frame is buffered; clear it the moment a complete frame lands (bufferedBytes back to 0).
  // It does NOT fire on a merely-QUIET-but-HEALTHY session (no partial frame buffered — e.g. a
  // passive session receiving bus events with no inbound traffic; that was the false-positive the
  // re-gate caught), and does NOT reset on dribbled partial progress (a slow byte-drip cannot evade
  // it — the frame must COMPLETE within idleTimeoutMs).
  private updateIdle(): void {
    if (this.decoder.bufferedBytes > 0) {
      if (!this.idleTimer && this.phase !== 'closed') {
        const ms = this.opts.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
        if (ms > 0) {
          this.idleTimer = setTimeout(() => this.fail('idle_timeout', new Error('incomplete frame not completed within read-progress timeout')), ms)
        }
      }
    } else if (this.idleTimer) {
      clearTimeout(this.idleTimer)
      this.idleTimer = undefined
    }
  }

  onData(chunk: Uint8Array | ArrayBuffer): void {
    if (this.phase === 'closed') return

    let frames: DecodedFrame[]
    try {
      frames = this.decoder.push(toU8(chunk))
    } catch (err) {
      this.fail(err instanceof FrameProtocolError ? 'frame_protocol_error' : 'frame_decode_failed', err)
      return
    }

    for (const frame of frames) this.routeFrame(frame)
    this.updateIdle() // arm only while a partial frame is buffered (dribble); clear when none remains
  }

  // Bun calls the listener's drain(socket) when a backpressured socket can accept more;
  // resume the writer from where a short write paused it.
  onWritableDrain(): void {
    if (this.phase !== 'closed') this.writer.resume()
  }

  teardown(reason: Error = new Error('connection closed')): Promise<void> {
    if (this.teardownPromise) return this.teardownPromise
    this.phase = 'closed'
    if (this.idleTimer) clearTimeout(this.idleTimer)

    this.teardownPromise = (async () => {
      // Yield once BEFORE any work: destroySocket() below can SYNCHRONOUSLY fire Bun's
      // close handler, which re-enters teardown(). Suspending here guarantees
      // this.teardownPromise is already assigned so the re-entrancy guard catches it
      // (otherwise teardown — and removeSession — would run twice).
      await Promise.resolve()
      this.ctrlPending.rejectAll(new Error('connection closed'))
      this.deliveryPending.rejectAll(new Error('connection closed'))
      this.mcpBuffer.length = 0
      for (const waiter of this.helloWaiters.splice(0)) {
        try { waiter({ t: 'err', code: 'connection_closed' }) } catch {}
      }
      // Settle any pending writer drain() awaits (e.g. an inject paused mid-flush at teardown) so they
      // reject promptly -> the inbox naks -> JetStream redelivers, instead of leaking forever (P2 fold).
      this.writer.close(new Error('connection closed'))

      destroySocket(this.socket)

      if (this.transport) {
        try { await this.transport.close() } catch (err) {
          log('warn', 'uds_transport_close_failed', errFields(err))
        }
      }

      if (this.session) {
        try {
          await this.opts.core.removeSession(this.session.agentId, this.session.epoch)
        } catch (err) {
          log('warn', 'uds_remove_session_failed', { agent_id: this.session.agentId, epoch: this.session.epoch, ...errFields(err) })
        }
      }
    })().finally(this.unregister)

    return this.teardownPromise
  }

  private async run(): Promise<void> {
    const result = await runHello({
      recvCtrl: this.recvCtrl,
      sendCtrl: this.sendCtrl,
      redis: this.opts.redis,
      randomBytes: (n) => crypto.getRandomValues(new Uint8Array(n)),
      now: () => Date.now(),
      peerUid: peerUidFromSocket(this.socket),
      expectedUid: this.opts.expectedUid,
      helloTimeoutMs: this.opts.helloTimeoutMs,
    })

    if (this.phase === 'closed') return
    if (!result.ok) {
      log('warn', 'uds_hello_failed', { code: result.errCode })
      await this.flushTerminal() // FOLD5: flush the {t:err} runHello enqueued before closing
      this.fail('hello_failed')
      return
    }
    if (!result.agentId || result.epoch == null) {
      this.fail('hello_result_incomplete', new Error('missing agentId or epoch'))
      return
    }

    // FOLD4: hello done -> route post-hello CTRL (sign replies) to feedCtrlSig (not the hello queue),
    // so a {t:sig} arriving while addUdsSession runs resolves ctrlPending instead of being mis-queued.
    // MCP frames stay buffered until 'session'.
    this.phase = 'authed'

    const agentId = result.agentId
    const epoch = result.epoch
    const transport = new UdsServerTransport(this.writer)
    this.transport = transport
    const delivery = result.deliveryCapable === true
      ? {
          pending: this.deliveryPending,
          epoch,
          timeoutMs: DELIVERY_ACK_TIMEOUT_MS,
          maxInflight: DELIVERY_MAX_INFLIGHT,
          onEvent: (event: string, fields = {}) => log(event === 'a2a_delivery_inflight_full' ? 'warn' : 'info', event, { agent_id: agentId, ...fields }),
        }
      : undefined

    const add = await this.opts.core.addUdsSession(agentId, {
      epoch,
      transport,
      ctxInject: makeUdsInject(this.writer, delivery),
      externalSign: makeUdsSign({ writer: this.writer, pending: this.ctrlPending, timeoutMs: 10_000 }),
    }, result.toolOnly ? { toolOnly: true } : {})

    if (this.phase === 'closed') {
      if (add.ok) {
        try { await this.opts.core.removeSession(agentId, epoch) } catch (err) {
          log('warn', 'uds_remove_late_session_failed', { agent_id: agentId, epoch, ...errFields(err) })
        }
      }
      return
    }

    if (!add.ok) {
      log('warn', 'uds_add_session_failed', { agent_id: agentId, error: add.error })
      this.sendCtrl({ t: 'err', code: 'session_unavailable' }) // FOLD3: tell the client add failed
      await this.flushTerminal()
      this.fail('add_session_failed')
      return
    }

    // FOLD3: send {t:ok} ONLY now (after addUdsSession succeeded) so the client is never told ok
    // then dropped; FOLD5: flush it before flipping to session traffic.
    this.session = { agentId, epoch }
    this.sendCtrl({ t: 'ok', session: result.session, epoch })
    await this.flushTerminal()
    // teardown may have raced during the flush await (it set phase='closed' + already removed the
    // session since this.session was set) — do NOT flip back to 'session' or feed a closed transport (P2 fold).
    if (this.phase === 'closed') return
    this.phase = 'session'

    const buffered = this.mcpBuffer.splice(0)
    for (const payload of buffered) {
      try {
        transport.feedMcp(payload)
      } catch (err) {
        this.fail('mcp_flush_failed', err)
        break
      }
    }
  }

  // FOLD5: await a real writer flush so a terminal hello CTRL ({t:ok}/{t:err}) reaches the peer
  // before the socket is torn down (otherwise the peer can see EOF instead of the hello result).
  private async flushTerminal(): Promise<void> {
    try {
      await this.writer.drain()
    } catch {}
  }

  private routeFrame(frame: DecodedFrame): void {
    if (this.phase === 'closed') return

    if (frame.type === FRAME_TYPE_CTRL) {
      if (this.phase === 'hello') {
        let obj: any
        try {
          obj = JSON.parse(dec.decode(frame.payload))
        } catch (err) {
          this.fail('hello_ctrl_parse_failed', err)
          return
        }
        // §A.7 liveness: pong an (unsigned) pre-session ping WITHOUT consuming a hello slot —
        // a2a-launch.sh gates EVERY launch on `a2a-shim --ping` exit 0. Bound pings (same cap as hello
        // CTRLs) so a pre-auth peer that only pings (no completed frame -> idle-timer never arms) can't
        // loiter forever.
        if (isCtrlPing(obj)) {
          if (++this.preHelloPings > MAX_PRE_HELLO_CTRL) {
            this.fail('pre_session_ping_overflow', new Error('too many pre-hello pings'))
            return
          }
          this.sendPong(obj)
          return
        }
        // FOLD6: cap pre-auth hello CTRL frames (hello+auth only) — bound a pre-auth peer's memory.
        if (this.helloQ.length >= MAX_PRE_HELLO_CTRL) {
          this.fail('pre_session_ctrl_overflow', new Error('too many pre-hello CTRL frames'))
          return
        }
        this.pushHelloCtrl(obj)
      } else {
        // 'authed' | 'session': a ping is the in-session liveness heartbeat (§A.7, shim pings 5s) ->
        // pong it; everything else is a sign reply -> resolve ctrlPending.
        let obj: any
        try {
          obj = JSON.parse(dec.decode(frame.payload))
        } catch {
          obj = undefined
        }
        if (obj !== undefined && isCtrlPing(obj)) {
          this.sendPong(obj)
          return
        }
        if (this.session && feedCtrlDelivered(frame.payload, this.deliveryPending, this.session.epoch, (err) => {
          log('warn', 'a2a_delivery_ack_unknown', { agent_id: this.session?.agentId, epoch: this.session?.epoch, ...errFields(err) })
        })) {
          return
        }
        // sign replies -> resolve ctrlPending (feedCtrlSig re-parses + reports its own malformed-frame
        // errors via the callback; a parse failure above falls through to it).
        feedCtrlSig(frame.payload, this.ctrlPending, (err) => {
          log('warn', 'ctrl_sig_feed_failed', errFields(err))
        })
      }
      return
    }

    if (frame.type === FRAME_TYPE_MCP) {
      if (this.phase === 'session') {
        try {
          this.transport!.feedMcp(frame.payload)
        } catch (err) {
          this.fail('mcp_feed_failed', err)
        }
      } else {
        // 'hello' | 'authed': buffer until the session is live, but FOLD6: cap the pre-session buffer.
        this.preSessionBytes += frame.payload.byteLength
        if (this.mcpBuffer.length >= MAX_PRE_SESSION_MCP_FRAMES || this.preSessionBytes > MAX_PRE_SESSION_BYTES) {
          this.fail('pre_session_mcp_overflow', new Error('pre-session MCP buffer cap exceeded'))
          return
        }
        this.mcpBuffer.push(frame.payload.slice())
      }
      return
    }

    this.fail('unknown_frame_type', new Error(`unknown frame type ${frame.type}`))
  }

  private recvCtrl = (): Promise<any> => new Promise<any>((res) => {
    const q = this.helloQ.shift()
    if (q !== undefined) res(q)
    else this.helloWaiters.push(res)
  })

  private pushHelloCtrl(obj: any): void {
    const waiter = this.helloWaiters.shift()
    if (waiter) waiter(obj)
    else this.helloQ.push(obj)
  }

  private sendCtrl = (obj: any): void => {
    if (this.phase === 'closed') return
    this.writer.enqueueCtrl(enc.encode(JSON.stringify(obj)))
    this.flushWriter()
  }

  // §A.7 pong: echo the ping's ts so the peer can measure RTT / confirm core liveness. §A defines ts as
  // a timestamp number (string tolerated); echo ONLY a primitive ts so the unsigned pre-auth pong stays a
  // strictly-tiny liveness token — never reflect arbitrary caller JSON (already bounded by the frame cap,
  // but this keeps it minimal + §A-conformant; x-model gate P2). Missing/other-typed ts -> bare {t:'pong'}.
  private sendPong(ping: { ts?: unknown }): void {
    const ts = ping.ts
    if (typeof ts === 'number' || typeof ts === 'string') this.sendCtrl({ t: 'pong', ts })
    else this.sendCtrl({ t: 'pong' })
  }

  private flushWriter(): void {
    this.drainTail = this.drainTail
      .then(() => {
        if (this.phase !== 'closed') return this.writer.drain()
      })
      .catch((err) => this.fail('writer_drain_failed', err))
  }

  private fail(event: string, err?: unknown): void {
    log('warn', event, err ? errFields(err) : {})
    void this.teardown(new Error('connection closed'))
  }
}

export async function startUdsAcceptor(opts: UdsAcceptorOptions): Promise<UdsAcceptorHandle> {
  const socketPath = opts.socketPath
  const socketDir = opts.socketDir ?? dirname(socketPath)
  await prepareSocketPath(socketDir, socketPath)

  const live = new Set<UdsConnection>()
  const bySocket = new WeakMap<object, UdsConnection>()

  let server: BunServer
  try {
    server = Bun.listen({
      unix: socketPath,
      socket: {
        open(socket: BunSocket) {
          const conn = new UdsConnection(socket, opts, () => live.delete(conn))
          bySocket.set(socket, conn)
          live.add(conn)
          conn.start()
        },
        data(socket: BunSocket, chunk: Uint8Array) {
          bySocket.get(socket)?.onData(chunk)
        },
        drain(socket: BunSocket) {
          bySocket.get(socket)?.onWritableDrain()
        },
        close(socket: BunSocket) {
          void bySocket.get(socket)?.teardown()
          bySocket.delete(socket)
        },
        end(socket: BunSocket) {
          void bySocket.get(socket)?.teardown()
          bySocket.delete(socket)
        },
        error(socket: BunSocket, err: unknown) {
          log('warn', 'uds_socket_error', errFields(err))
          void bySocket.get(socket)?.teardown()
          bySocket.delete(socket)
        },
      } as any,
    } as any) as BunServer

    await chmod(socketPath, SOCK_MODE)
  } catch (err) {
    if (server!) {
      try { await Promise.resolve(server.stop()) } catch {}
    }
    await unlinkSocketIfSocket(socketPath)
    throw err
  }

  let closePromise: Promise<void> | undefined
  return {
    socketPath,
    close(): Promise<void> {
      closePromise ??= (async () => {
        let stopErr: unknown
        try { await Promise.resolve(server.stop()) } catch (err) { stopErr = err }
        await Promise.allSettled([...live].map((conn) => conn.teardown()))
        await unlinkSocketIfSocket(socketPath)
        if (stopErr) throw stopErr
      })()
      return closePromise
    },
  }
}
