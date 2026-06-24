import { test, expect } from 'bun:test'
import { FrameWriter, FrameDecoder, FRAME_TYPE_MCP } from '../uds_frame.ts'
import { unlink } from 'node:fs/promises'
test('FrameWriter survives real-socket backpressure with zero byte loss (P0 gate-fold)', async () => {
 const path = `/tmp/bp-load-${process.pid}-${Date.now()}.sock`
try { await unlink(path) } catch {}

const SIZE = 12 * 1024 * 1024   // 12MiB — far exceeds the ~256KB OS socket buffer => real short-writes
const payload = new Uint8Array(SIZE)
for (let i = 0; i < SIZE; i++) payload[i] = i & 0xff   // verifiable pattern

let drainResolved = false
let drainResolvedAt = 0
let shortWrites = 0, totalWriteCalls = 0
let serverWriter: FrameWriter | undefined

const server = Bun.listen({ unix: path, socket: {
  open(sock: any) {
    serverWriter = new FrameWriter(
      (b: Uint8Array) => {
        totalWriteCalls++
        const n = sock.write(b)
        if (typeof n === 'number' && n < b.byteLength) shortWrites++
        return n
      },
      { highWater: 1024, maxBytes: 64 * 1024 * 1024, onOverflow: () => { throw new Error('overflow') } },
    )
    serverWriter.enqueueMcp(payload)
    serverWriter.drain().then(() => { drainResolved = true; drainResolvedAt = Date.now() })
  },
  drain(_sock: any) { serverWriter?.resume() },   // THE FIX: resume the paused writer when the OS buffer frees
}})

// Client: reads everything (the 12MiB > OS buffer guarantees backpressure regardless of read speed)
const decoder = new FrameDecoder()
let received: Uint8Array | undefined
let lastByteAt = 0
const client = await Bun.connect({ unix: path, socket: {
  data(_sock: any, chunk: Uint8Array) {
    for (const f of decoder.push(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength))) {
      if (f.type === FRAME_TYPE_MCP) { received = f.payload.slice(); lastByteAt = Date.now() }
    }
  },
}})

// wait until the frame fully arrives (or timeout)
const t0 = Date.now()
while (!received && Date.now() - t0 < 15000) await Bun.sleep(20)
await Bun.sleep(50)

const ok = received && received.byteLength === SIZE
let intact = false
if (ok) { intact = true; for (let i = 0; i < SIZE; i++) if (received![i] !== (i & 0xff)) { intact = false; break } }

try { client.end?.() } catch {}
 server.stop(); try { await unlink(path) } catch {}
 expect(shortWrites).toBeGreaterThan(0) // real backpressure was exercised
 expect(received?.byteLength).toBe(SIZE)
 expect(intact).toBe(true) // zero drop/corruption
 expect(drainResolved).toBe(true)
}, 30000)
