import { afterEach, expect, test } from 'bun:test'
import { Buffer } from 'node:buffer'
import { lstat, mkdir, rm, writeFile } from 'node:fs/promises'
import { startUdsAcceptor, type UdsAcceptorHandle } from '../uds_acceptor.ts'
import {
  FRAME_TYPE_CTRL,
  FRAME_TYPE_MCP,
  FrameDecoder,
  FrameWriter,
} from '../uds_frame.ts'

const enc = new TextEncoder()
const dec = new TextDecoder()
const PKCS8_ED25519_PREFIX = new Uint8Array([0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20])

const handles: UdsAcceptorHandle[] = []
const roots: string[] = []

afterEach(async () => {
  for (const h of handles.splice(0)) { try { await h.close() } catch {} }
  for (const r of roots.splice(0)) { try { await rm(r, { recursive: true, force: true }) } catch {} }
})

type Calls = {
  add: Array<{ agentId: string; wiring: any }>
  remove: Array<{ agentId: string; epoch: number }>
  mcp: Uint8Array[]
}

class FakeRedis {
  store = new Map<string, string>()

  async get(key: string): Promise<string | null> {
    return this.store.has(key) ? this.store.get(key)! : null
  }

  async set(key: string, value: unknown, ...opts: unknown[]): Promise<'OK' | null> {
    const flags = opts.map((v) => String(v).toUpperCase())
    if (flags.includes('NX') && this.store.has(key)) return null
    if (flags.includes('XX') && !this.store.has(key)) return null
    this.store.set(key, String(value))
    return 'OK'
  }

  async del(...keys: string[]): Promise<number> {
    let n = 0
    for (const key of keys) if (this.store.delete(key)) n++
    return n
  }

  async incr(key: string): Promise<number> {
    const next = Number(this.store.get(key) ?? '0') + 1
    this.store.set(key, String(next))
    return next
  }

  async exists(key: string): Promise<number> {
    return this.store.has(key) ? 1 : 0
  }

  async expire(_key: string, _ttlS: number): Promise<number> {
    return 1
  }

  async eval(_script: string, _keys?: unknown, _args?: unknown): Promise<number> {
    return 1
  }

  async evalsha(_sha: string, _keys?: unknown, _args?: unknown): Promise<number> {
    return 1
  }

  async send(command: string | unknown[], args: unknown[] = []): Promise<unknown> {
    const parts = Array.isArray(command) ? command : [command, ...args]
    const cmd = String(parts[0]).toUpperCase()
    if (cmd === 'GET') return this.get(String(parts[1]))
    if (cmd === 'SET') return this.set(String(parts[1]), parts[2], ...parts.slice(3))
    if (cmd === 'DEL') return this.del(...parts.slice(1).map(String))
    if (cmd === 'INCR') return this.incr(String(parts[1]))
    if (cmd === 'EXISTS') return this.exists(String(parts[1]))
    if (cmd === 'EXPIRE') return this.expire(String(parts[1]), Number(parts[2]))
    if (cmd === 'EVAL') {
      // emulate uds_hello §C.2 PRESENCE_RECLAIM_SCRIPT: EVAL script '1' <presenceKey> <now> <window>
      const key = String(parts[3])
      const now = Number(parts[4])
      const window = Number(parts[5])
      const v = this.store.get(key)
      if (v === undefined) return 'RECLAIMED'
      try {
        const d = JSON.parse(v) as { last_seen?: unknown }
        const last = Number(d?.last_seen)
        if (!Number.isFinite(last)) return 'DUP'
        if (now - last > window) { this.store.delete(key); return 'RECLAIMED' }
      } catch { return 'DUP' }
      return 'DUP'
    }
    if (cmd === 'EVALSHA') return 1
    if (cmd === 'SCRIPT') return 'fake-sha'
    throw new Error(`unsupported fake redis command ${cmd}`)
  }
}

function makeCalls(): Calls {
  return { add: [], remove: [], mcp: [] }
}

function fakeCore(calls: Calls) {
  return {
    addUdsSession: async (agentId: string, wiring: any) => {
      wiring.transport.feedMcp = (payload: Uint8Array) => calls.mcp.push(payload.slice())
      calls.add.push({ agentId, wiring })
      return { ok: true, agentId, epoch: wiring.epoch }
    },
    removeSession: async (agentId: string, epoch: number) => {
      calls.remove.push({ agentId, epoch })
      return true
    },
  } as any
}

function tempPaths(name: string): { root: string; socketDir: string; socketPath: string } {
  const root = `/tmp/uds-acceptor-test-${process.pid}-${Date.now()}-${name}-${Math.random().toString(16).slice(2)}`
  roots.push(root)
  return { root, socketDir: `${root}/sock`, socketPath: `${root}/sock/core.sock` }
}

function b64(u8: Uint8Array): string {
  return Buffer.from(u8).toString('base64')
}

function fromB64(s: string): Uint8Array {
  return new Uint8Array(Buffer.from(s, 'base64'))
}

function hexToU8(hex: string): Uint8Array {
  const h = hex.replace(/^0x/, '')
  const out = new Uint8Array(h.length / 2)
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(h.slice(i * 2, i * 2 + 2), 16)
  return out
}

async function importEd25519Seed(seed: Uint8Array): Promise<CryptoKey> {
  const pkcs8 = new Uint8Array(PKCS8_ED25519_PREFIX.length + seed.length)
  pkcs8.set(PKCS8_ED25519_PREFIX)
  pkcs8.set(seed, PKCS8_ED25519_PREFIX.length)
  return crypto.subtle.importKey('pkcs8', pkcs8, { name: 'Ed25519' }, false, ['sign'])
}

function findString(obj: unknown, keys: string[]): string | undefined {
  if (!obj || typeof obj !== 'object') return undefined
  for (const key of keys) {
    const v = (obj as any)[key]
    if (typeof v === 'string') return v
  }
  for (const v of Array.isArray(obj) ? obj : Object.values(obj)) {
    const found = findString(v, keys)
    if (found) return found
  }
  return undefined
}

async function vectorMaterial(): Promise<{ agentId: string; seedKey: CryptoKey; pubkeyB64: string }> {
  const vectors = await Bun.file(new URL('./shim-conformance/vectors.json', import.meta.url)).json()
  const seedHex = findString(vectors, ['seed_hex', 'seedHex'])
  const pubkeyB64 = findString(vectors, ['pubkey_raw_b64', 'pubkeyRawB64'])
  const agentId = findString(vectors, ['agent_id', 'agentId']) ?? 'uds-acceptor-test-agent'
  if (!seedHex || !pubkeyB64) throw new Error('vectors.json missing seed_hex or pubkey_raw_b64')
  return { agentId, seedKey: await importEd25519Seed(hexToU8(seedHex)), pubkeyB64 }
}

function redisForAgent(agentId: string, pubkeyB64: string): FakeRedis {
  const redis = new FakeRedis()
  redis.store.set(`alloyium:a2a:pubkey:${agentId}`, pubkeyB64)
  redis.store.set(`a2a:pubkey:${agentId}`, pubkeyB64)
  return redis
}

async function waitFor(fn: () => boolean, ms = 3000, step = 20): Promise<boolean> {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    if (fn()) return true
    await Bun.sleep(step)
  }
  return fn()
}

function closeAnySocket(socket: any): void {
  try {
    // terminate() = abrupt close (mirrors a dropped/dead shim) so the server reliably
    // sees the connection close and runs teardown; end() can leave a half-open conn.
    if (typeof socket.terminate === 'function') socket.terminate()
    else if (typeof socket.end === 'function') socket.end()
    else if (typeof socket.close === 'function') socket.close()
  } catch {}
}

async function connectClient(socketPath: string) {
  const decoder = new FrameDecoder()
  const ctrlQ: any[] = []
  const mcpQ: Uint8Array[] = []
  let closed = false
  let lastError: unknown

  const socket = await Bun.connect({
    unix: socketPath,
    socket: {
      data(_socket: any, chunk: Uint8Array) {
        for (const frame of decoder.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength))) {
          if (frame.type === FRAME_TYPE_CTRL) ctrlQ.push(JSON.parse(dec.decode(frame.payload)))
          if (frame.type === FRAME_TYPE_MCP) mcpQ.push(frame.payload.slice())
        }
      },
      close() { closed = true },
      error(_socket: any, err: unknown) { lastError = err; closed = true },
    },
  } as any)

  const writer = new FrameWriter(
    (bytes) => { socket.write(bytes) },
    { highWater: 4096, onOverflow: () => { throw new Error('client writer overflow') } },
  )

  return {
    socket,
    get closed() { return closed },
    get lastError() { return lastError },
    async sendCtrl(obj: unknown) {
      writer.enqueueCtrl(enc.encode(JSON.stringify(obj)))
      await writer.drain()
    },
    async sendMcp(payload: Uint8Array) {
      writer.enqueueMcp(payload)
      await writer.drain()
    },
    async nextCtrl(ms = 3000): Promise<any> {
      if (!ctrlQ.length) await waitFor(() => ctrlQ.length > 0 || closed, ms)
      if (ctrlQ.length) return ctrlQ.shift()
      throw new Error(closed ? `socket closed ${String(lastError ?? '')}` : 'timeout waiting for CTRL')
    },
    async nextMcp(ms = 3000): Promise<Uint8Array> {
      if (!mcpQ.length) await waitFor(() => mcpQ.length > 0 || closed, ms)
      if (mcpQ.length) return mcpQ.shift()!
      throw new Error(closed ? `socket closed ${String(lastError ?? '')}` : 'timeout waiting for MCP')
    },
    close() { closeAnySocket(socket) },
  }
}

async function completeHello(client: Awaited<ReturnType<typeof connectClient>>, agentId: string, seedKey: CryptoKey, caps?: string[]) {
  await client.sendCtrl({ t: 'hello', v: 1, agentId, host: 'test-host', pid: process.pid, subsKey: `test:subs:${agentId}`, ...(caps ? { caps } : {}) })
  const challenge = await client.nextCtrl()
  expect(challenge.t).toBe('challenge')
  const nonce = fromB64(challenge.nonce)
  expect(nonce.length).toBe(32)
  const sig = new Uint8Array(await crypto.subtle.sign('Ed25519', seedKey, nonce))
  await client.sendCtrl({ t: 'auth', alg: 'ed25519', sig: b64(sig) })
  const ok = await client.nextCtrl()
  expect(ok.t).toBe('ok')
  return ok
}

function directNotif(notifId: string): unknown {
  return {
    jsonrpc: '2.0',
    method: 'notifications/claude/channel',
    params: { content: 'hi', meta: { kind: 'direct', id: notifId, notifId } },
  }
}

test('refuses to clobber an existing non-socket path', async () => {
  const { root, socketPath } = tempPaths('regular-file')
  await mkdir(`${root}/sock`, { recursive: true })
  await writeFile(socketPath, 'not a socket')

  await expect(startUdsAcceptor({
    core: fakeCore(makeCalls()),
    redis: new FakeRedis() as any,
    socketPath,
  })).rejects.toThrow(/not a socket|refusing to bind/)
})

test('binds the socket, completes hello, buffers MCP until add, and removes on close', async () => {
  const { socketDir, socketPath } = tempPaths('happy')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const dirMode = (await lstat(socketDir)).mode & 0o777
  const sockStat = await lstat(socketPath)
  expect(dirMode).toBe(0o700)
  expect(sockStat.isSocket()).toBe(true)
  expect(sockStat.mode & 0o777).toBe(0o660)

  const client = await connectClient(socketPath)
  try {
    await client.sendCtrl({ t: 'hello', v: 1, agentId: material.agentId, host: 'test-host', pid: process.pid, subsKey: `test:subs:${material.agentId}` })
    const challenge = await client.nextCtrl()
    expect(challenge.t).toBe('challenge')

    const initPayload = enc.encode(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: {} }))
    await client.sendMcp(initPayload)
    expect(calls.add).toHaveLength(0)

    const nonce = fromB64(challenge.nonce)
    const sig = new Uint8Array(await crypto.subtle.sign('Ed25519', material.seedKey, nonce))
    await client.sendCtrl({ t: 'auth', alg: 'ed25519', sig: b64(sig) })

    const ok = await client.nextCtrl()
    expect(ok.t).toBe('ok')
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    expect(calls.add[0].agentId).toBe(material.agentId)
    expect(calls.add[0].wiring.epoch).toBe(1)

    expect(await waitFor(() => calls.mcp.length === 1)).toBe(true)
    expect(dec.decode(calls.mcp[0])).toContain('"initialize"')

    client.close()
    expect(await waitFor(() => calls.remove.length === 1)).toBe(true)
    expect(calls.remove[0]).toEqual({ agentId: material.agentId, epoch: 1 })
  } finally {
    client.close()
  }
})

test('old shim without delivered cap gets ph2 flush-only direct injection', async () => {
  const { socketPath } = tempPaths('no-delivered-cap')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)

    let resolved = false
    const injected = calls.add[0].wiring.ctxInject(directNotif('n-old')).then(() => { resolved = true })
    const frame = await client.nextMcp()
    expect(dec.decode(frame)).toContain('"notifId":"n-old"')
    await injected
    expect(resolved).toBe(true)
  } finally {
    client.close()
  }
})

test('delivered-capable shim waits for matching delivered ctrl', async () => {
  const { socketPath } = tempPaths('delivered-cap')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey, ['delivered'])
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)

    let resolved = false
    const injected = calls.add[0].wiring.ctxInject(directNotif('n-new')).then(() => { resolved = true })
    await client.nextMcp()
    await Bun.sleep(50)
    expect(resolved).toBe(false)

    await client.sendCtrl({ t: 'delivered', notifId: 'n-new', epoch: 1, status: 'ok' })
    await injected
    expect(resolved).toBe(true)
  } finally {
    client.close()
  }
})

test('bad framing destroys the socket and never adds a session', async () => {
  const { socketPath } = tempPaths('bad-frame')
  const calls = makeCalls()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
  })
  handles.push(handle)

  let closed = false
  const socket = await Bun.connect({
    unix: socketPath,
    socket: {
      data() {},
      close() { closed = true },
      error() { closed = true },
    },
  } as any)

  try {
    socket.write(new Uint8Array([0xff, 0xff, 0xff, 0x7f, FRAME_TYPE_CTRL]))
    expect(await waitFor(() => closed)).toBe(true)
    expect(calls.add).toHaveLength(0)
  } finally {
    closeAnySocket(socket)
  }
})

test('bad auth closes without adding a session', async () => {
  const { socketPath } = tempPaths('bad-auth')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await client.sendCtrl({ t: 'hello', v: 1, agentId: material.agentId, host: 'test-host', pid: process.pid, subsKey: `test:subs:${material.agentId}` })
    const first = await client.nextCtrl()
    if (first.t === 'challenge') {
      const sig = new Uint8Array(64) // garbage signature (correct length, wrong bytes) -> PoP verify must fail
      await client.sendCtrl({ t: 'auth', alg: 'ed25519', sig: b64(sig) })
      const err = await client.nextCtrl()
      expect(err.t).toBe('err')
    } else {
      expect(first.t).toBe('err')
    }

    expect(await waitFor(() => client.closed)).toBe(true)
    expect(calls.add).toHaveLength(0)
  } finally {
    client.close()
  }
})

test('a STALLED PARTIAL frame (dribble/slowloris) is torn down by the read-progress timeout', async () => {
  const { socketPath } = tempPaths('idle-dribble')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
    idleTimeoutMs: 200, // short for the test
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    // Announce a 1000-byte frame but send only the header + 1 payload byte — never completed (dribble).
    // bufferedBytes > 0 with no completion -> read-progress timeout reaps it.
    const partial = new Uint8Array(6)
    new DataView(partial.buffer).setUint32(0, 1000, true) // frameLength=1000, never fully sent
    partial[4] = FRAME_TYPE_MCP
    partial[5] = 0x7b
    client.socket.write(partial)
    expect(await waitFor(() => calls.remove.length === 1, 3000)).toBe(true)
    expect(calls.remove[0]).toEqual({ agentId: material.agentId, epoch: 1 })
  } finally {
    client.close()
  }
})

test('a QUIET healthy session (no partial frame) is NOT reaped by the idle timer', async () => {
  const { socketPath } = tempPaths('idle-quiet')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
    idleTimeoutMs: 150, // short for the test
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    // Send NOTHING for well past idleTimeoutMs. A quiet session has no buffered partial frame and may
    // be passively receiving bus events — it must SURVIVE (the re-gate P1: no false-positive reap).
    await Bun.sleep(500)
    expect(calls.remove.length).toBe(0)
  } finally {
    client.close()
  }
})

// §A.7 ping/pong (S5 pre-flight gap): a2a-launch.sh gates EVERY launch on `a2a-shim --ping` exit 0
// (boots-MCP-dead precondition), and the shim sends an in-session 5s heartbeat ping. The acceptor must
// pong an UNSIGNED ping in ANY phase (pre-hello: the launch gate; in-session: the heartbeat) or both
// the launch gate refuses AND the live session reconnect-storms (2 missed pongs -> drop -> reconnect).
test('§A.7: pongs an UNSIGNED pre-hello liveness ping without consuming the hello', async () => {
  const { socketPath } = tempPaths('preping')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    // pre-session ping (the `a2a-shim --ping` boots-MCP-dead probe) -> pong, NO bad_hello, NO session.
    await client.sendCtrl({ t: 'ping', ts: 12345 })
    const pong = await client.nextCtrl()
    expect(pong.t).toBe('pong')
    expect(pong.ts).toBe(12345) // ts echoed verbatim
    expect(client.closed).toBe(false) // ping must NOT close the connection
    expect(calls.add).toHaveLength(0) // ping must NOT open a session

    // the SAME connection can still complete a normal hello afterwards (the ping didn't consume it).
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    expect(calls.add[0].agentId).toBe(material.agentId)
  } finally {
    client.close()
  }
})

test('§A.7: pongs an in-session liveness ping (5s heartbeat) without dropping the session', async () => {
  const { socketPath } = tempPaths('insession-ping')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)

    await client.sendCtrl({ t: 'ping', ts: 777 })
    const pong = await client.nextCtrl()
    expect(pong.t).toBe('pong')
    expect(pong.ts).toBe(777)
    expect(calls.remove).toHaveLength(0) // session must SURVIVE the heartbeat (no reconnect-storm)
  } finally {
    client.close()
  }
})

test('§A.7: a pong omits ts when the ping carried none (undefined-safe echo)', async () => {
  const { socketPath } = tempPaths('ping-no-ts')
  const calls = makeCalls()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await client.sendCtrl({ t: 'ping' }) // launcher-pm's hand-rolled probe sends no ts
    const pong = await client.nextCtrl()
    expect(pong.t).toBe('pong')
    expect('ts' in pong).toBe(false)
    expect(client.closed).toBe(false)
  } finally {
    client.close()
  }
})

test('§A.7: bounds a pre-hello ping flood (DoS cap)', async () => {
  const { socketPath } = tempPaths('ping-flood')
  const calls = makeCalls()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    // MAX_PRE_HELLO_CTRL (8) pings are answered; the (cap+1)-th trips pre_session_ping_overflow -> close.
    for (let i = 0; i < 8; i++) {
      await client.sendCtrl({ t: 'ping', ts: i })
      const pong = await client.nextCtrl()
      expect(pong.t).toBe('pong')
    }
    await client.sendCtrl({ t: 'ping', ts: 99 })
    expect(await waitFor(() => client.closed)).toBe(true)
    expect(calls.add).toHaveLength(0)
  } finally {
    client.close()
  }
})

// x-model gate P2 test-gap closures (behaviors both gate legs verified hold; locked in here).
test('§A.7: in-session pings are NOT subject to the pre-hello cap (heartbeat runs indefinitely)', async () => {
  const { socketPath } = tempPaths('insession-flood')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    // Far more than MAX_PRE_HELLO_CTRL (8): the pre-hello cap must NOT apply post-hello (the 5s heartbeat
    // runs for the session lifetime). All pong; the session never drops.
    for (let i = 0; i < 20; i++) {
      await client.sendCtrl({ t: 'ping', ts: i })
      const pong = await client.nextCtrl()
      expect(pong.t).toBe('pong')
      expect(pong.ts).toBe(i)
    }
    expect(calls.remove).toHaveLength(0)
  } finally {
    client.close()
  }
})

test('§A.7: an in-session ping interleaved with a sign round-trip leaves the sign pending-map intact', async () => {
  const { socketPath } = tempPaths('ping-sign')
  const calls = makeCalls()
  const material = await vectorMaterial()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: redisForAgent(material.agentId, material.pubkeyB64) as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await completeHello(client, material.agentId, material.seedKey)
    expect(await waitFor(() => calls.add.length === 1)).toBe(true)
    const externalSign = calls.add[0].wiring.externalSign as (canon: string) => Promise<string>

    const signP = externalSign('canon-x') // core -> shim CTRL {t:sign,reqId,canon}
    const signReq = await client.nextCtrl()
    expect(signReq.t).toBe('sign')
    expect(signReq.canon).toBe('canon-x')

    // a heartbeat ping arrives BETWEEN the sign request and the sig reply -> must pong AND must not
    // disturb the pending sign ({t:ping}->sendPong and {t:sig}->feedCtrlSig are disjoint paths).
    await client.sendCtrl({ t: 'ping', ts: 555 })
    await client.sendCtrl({ t: 'sig', reqId: signReq.reqId, sig: 'sig-bytes-x' })

    expect(await signP).toBe('sig-bytes-x') // pending-map resolved to the correct reply (uncorrupted)
    let gotPong = false
    for (let i = 0; i < 4 && !gotPong; i++) {
      const c = await client.nextCtrl().catch(() => null)
      if (c && c.t === 'pong' && c.ts === 555) gotPong = true
    }
    expect(gotPong).toBe(true)
    expect(calls.remove).toHaveLength(0)
  } finally {
    client.close()
  }
})

test('§A.7: a pong echoes only a primitive ts (number/string); other-typed ts -> bare pong', async () => {
  const { socketPath } = tempPaths('ping-edge-ts')
  const calls = makeCalls()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    await client.sendCtrl({ t: 'ping', ts: 42 })
    expect(await client.nextCtrl()).toMatchObject({ t: 'pong', ts: 42 })
    await client.sendCtrl({ t: 'ping', ts: 'abc' })
    expect(await client.nextCtrl()).toMatchObject({ t: 'pong', ts: 'abc' })
    // object / array / bool / null ts -> NOT echoed (bare {t:pong}); never reflect arbitrary caller JSON.
    for (const ts of [{ a: 1 }, [1, 2], true, null]) {
      await client.sendCtrl({ t: 'ping', ts })
      const pong = await client.nextCtrl()
      expect(pong.t).toBe('pong')
      expect('ts' in pong).toBe(false)
    }
    expect(client.closed).toBe(false)
  } finally {
    client.close()
  }
})

test('§A.7: a pre-auth ping-then-quiet connection is reaped by the hello-timeout (time bound, not just count cap)', async () => {
  const { socketPath } = tempPaths('ping-then-quiet')
  const calls = makeCalls()
  const handle = await startUdsAcceptor({
    core: fakeCore(calls),
    redis: new FakeRedis() as any,
    socketPath,
    helloTimeoutMs: 400, // short for the test
  })
  handles.push(handle)

  const client = await connectClient(socketPath)
  try {
    // a couple of pings (under the cap), then go QUIET and never send a hello.
    await client.sendCtrl({ t: 'ping', ts: 1 })
    expect((await client.nextCtrl()).t).toBe('pong')
    await client.sendCtrl({ t: 'ping', ts: 2 })
    expect((await client.nextCtrl()).t).toBe('pong')
    // pings never reach runHello, so they cannot reset its from-start hello deadline -> reaped ~400ms.
    expect(await waitFor(() => client.closed, 3000)).toBe(true)
    expect(calls.add).toHaveLength(0)
  } finally {
    client.close()
  }
}, 8000)
