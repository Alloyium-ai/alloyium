// MCP stdio purity — the single most important protocol invariant (#2):
// STDOUT must carry ONLY JSON-RPC; all logs/library chatter must go to STDERR.
import { test, expect, describe } from 'bun:test'

const REPO = new URL('..', import.meta.url).pathname
const INIT = JSON.stringify({
  jsonrpc: '2.0', id: 1, method: 'initialize',
  params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '1' } },
})

async function bootAndCapture() {
  const proc = Bun.spawn([process.execPath, 'webhook.ts'], {
    cwd: REPO,
    env: {
      ...process.env,
      // dead endpoints so start() loops harmlessly in the background and emits a
      // stderr log line we can assert on, without needing live infra.
      NATS_URL: 'nats://127.0.0.1:65010',
      REDIS_URL: 'redis://127.0.0.1:65011',
      HTTP_PORT: '0', // OS-assigned free port — avoids collisions when stdio test files run in parallel
      LOG_LEVEL: 'info',
    },
    stdin: 'pipe', stdout: 'pipe', stderr: 'pipe',
  })
  proc.stdin.write(INIT + '\n')
  proc.stdin.flush()
  await Bun.sleep(1500)
  proc.kill()
  const stdout = await new Response(proc.stdout).text()
  const stderr = await new Response(proc.stderr).text()
  return { stdout, stderr }
}

describe('stdio purity', () => {
  test('every stdout line is valid JSON-RPC and the initialize response is present', async () => {
    const { stdout } = await bootAndCapture()
    const lines = stdout.split('\n').map((l) => l.trim()).filter(Boolean)
    expect(lines.length).toBeGreaterThan(0)
    for (const line of lines) {
      const msg = JSON.parse(line) // throws → fails the test if anything non-JSON leaked
      expect(msg.jsonrpc).toBe('2.0')
    }
    const resp = lines.map((l) => JSON.parse(l)).find((m) => m.id === 1)
    expect(resp).toBeDefined()
    expect(resp.result ?? resp.error).toBeDefined()
  }, 10_000)

  test('logs go to stderr, never stdout', async () => {
    const { stdout, stderr } = await bootAndCapture()
    expect(stdout).not.toContain('[nats-channel]')
    expect(stderr).toContain('[nats-channel] startup')
  }, 10_000)
})

describe('console redirect (H1: library stdout-leak guard)', () => {
  test('the preamble is the FIRST import (runs before nats.js/SDK evaluate)', async () => {
    const src = await Bun.file(REPO + 'webhook.ts').text()
    const importLines = src.split('\n').filter((l) => /^\s*import\b/.test(l))
    expect(importLines[0]).toContain('./preamble.ts')
    // the preamble must precede the SDK + nats imports in source order
    const preIdx = src.indexOf("import './preamble.ts'")
    expect(preIdx).toBeGreaterThan(-1)
    expect(preIdx).toBeLessThan(src.indexOf("@modelcontextprotocol"))
    expect(preIdx).toBeLessThan(src.indexOf("from './nats-channel.ts'"))
  })
  test('preamble actually reroutes the stdout console methods to stderr', async () => {
    const src = await Bun.file(REPO + 'preamble.ts').text()
    expect(src).toMatch(/console\.log\s*=/)
    expect(src).toMatch(/console\.info\s*=/)
    expect(src).toMatch(/console\.debug\s*=/)
  })
})
