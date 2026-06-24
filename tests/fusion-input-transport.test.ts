// Pure unit tests for the request-INPUT claim-check path (the fusion fix): a large
// prompt — the opus-leg input or the judge fan-in — must ride a Redis blob + tiny
// `input_ref` so the assembled dispatch body stays UNDER the 8 KiB send cap (never
// `body_too_large`-rejected), and the receiving gateway must recover it LOSSLESSLY.
// No live bus. Mirrors the fakeRedis used in output_transport.test.ts.
import { test, expect, describe } from 'bun:test'
import {
  buildClaimCheckedInput, resolveJobInput, sendError,
  utf8ByteLength, sha256Hex, putBlob, BLOB_KEY_PREFIX,
  type BlobRedis, type WrapFn,
} from '../output_transport.ts'

function fakeRedis() {
  const m = new Map<string, string>()
  const r = {
    store: m,
    async get(k: string) { return m.has(k) ? m.get(k)! : null },
    async send(cmd: string, args: string[]) {
      if (cmd === 'SET') { const [k, v, ...rest] = args; if (rest.includes('NX') && m.has(k)) return null; m.set(k, v); return 'OK' }
      if (cmd === 'DEL') { return m.delete(args[0]!) ? 1 : 0 }
      if (cmd === 'GET') { return m.has(args[0]!) ? m.get(args[0]!)! : null }
      throw new Error('unsupported ' + cmd)
    },
  }
  return r as BlobRedis & { store: Map<string, string> }
}

// The EXACT wrap fusion-svc / the gateways use for a claude.job.request.v1 dispatch.
const claudeWrap = (jobId: string): WrapFn => (text, extra) =>
  ({ schema: 'claude.job.request.v1', job_id: jobId, input: [{ type: 'text', text }], ...(extra ?? {}) })
const measure = (o: unknown) => Buffer.byteLength(JSON.stringify(o), 'utf8')
const CAP = 8192 // the default A2A_MAX_SEND_BYTES — the cap we must NOT raise.

describe('buildClaimCheckedInput — small prompt goes inline (no blob)', () => {
  test('inline when the assembled body fits the cap', async () => {
    const redis = fakeRedis()
    const { body, ref } = await buildClaimCheckedInput(redis, 'a short question', CAP, claudeWrap('j1'))
    expect(ref).toBeNull()
    expect(redis.store.size).toBe(0) // nothing claim-checked
    expect((body as any).input[0].text).toBe('a short question')
    expect((body as any).input_ref).toBeUndefined()
    expect(measure(body)).toBeLessThanOrEqual(CAP)
  })
})

describe('buildClaimCheckedInput — large prompt is claim-checked, body stays under the cap', () => {
  test('a 20 KB judge fan-in: body ≤ cap, input_ref present, blob holds the full text', async () => {
    const redis = fakeRedis()
    const judge = 'PANELIST opus said: ' + 'x'.repeat(10_000) + '\nPANELIST gpt-5.5 said: ' + 'y'.repeat(10_000)
    expect(utf8ByteLength(judge)).toBeGreaterThan(CAP)
    const { body, ref } = await buildClaimCheckedInput(redis, judge, CAP, claudeWrap('fusion-claude-1'))
    // The whole point: the dispatch body is UNDER the cap → the bus will NOT reject it.
    expect(measure(body)).toBeLessThanOrEqual(CAP)
    expect(ref).not.toBeNull()
    const b = body as any
    expect(b.input_preview).toBe(true)
    expect(b.input_ref.result_ref).toBe(ref)
    expect(b.input_ref.sha256).toBe(sha256Hex(judge))
    expect(b.input_ref.len).toBe(utf8ByteLength(judge))
    // the inline preview is clearly partial, never the full text
    expect(b.input[0].text.length).toBeLessThan(judge.length)
    expect(b.input[0].text).toContain('truncated for transport')
    // the blob holds the full, verifiable text
    expect(redis.store.get(ref!)).toBe(judge)
  })

  test('escape-heavy + multibyte content round-trips losslessly and still fits', async () => {
    const redis = fakeRedis()
    const nasty = ('"\\\n\t😀é中 — spec line; ' + 'Z'.repeat(60)).repeat(400)
    expect(utf8ByteLength(nasty)).toBeGreaterThan(CAP)
    const { body, ref } = await buildClaimCheckedInput(redis, nasty, CAP, claudeWrap('j2'))
    expect(measure(body)).toBeLessThanOrEqual(CAP)
    // the gateway resolves input_ref back to the EXACT original
    const recovered = await resolveJobInput(redis, body as any)
    expect(recovered).toBe(nasty)
  })

  test('fits across a range of realistic input sizes', async () => {
    for (const n of [9_000, 50_000, 250_000]) {
      const redis = fakeRedis()
      const prompt = 'q\n' + 'A'.repeat(n)
      const { body } = await buildClaimCheckedInput(redis, prompt, CAP, claudeWrap(`job-${n}`))
      expect(measure(body)).toBeLessThanOrEqual(CAP)
      expect(await resolveJobInput(redis, body as any)).toBe(prompt)
    }
  })

  test('Redis down → putBlob throws → builder rejects (caller falls back loudly, never silent truncation of an input)', async () => {
    const down: BlobRedis = { async get() { return null }, async send() { throw new Error('redis down') } }
    const big = 'B'.repeat(20_000)
    await expect(buildClaimCheckedInput(down, big, CAP, claudeWrap('j3'))).rejects.toThrow()
  })

  // Cross-model review (GPT-5.5) P2: bound Redis pressure — refuse an absurd input loudly.
  test('input over maxInputBytes is rejected before any blob is written (pressure guard)', async () => {
    const redis = fakeRedis()
    const over = 'C'.repeat(800)
    await expect(buildClaimCheckedInput(redis, over, 300, claudeWrap('j4'), { maxInputBytes: 400 })).rejects.toThrow(/exceeds max 400/)
    expect(redis.store.size).toBe(0) // guard runs BEFORE putBlob — nothing parked in Redis
  })

  // Cross-model review (GPT-5.5) P1: NEVER emit a body the bus would reject. A pathologically
  // tiny cap that can't fit even a ref-only body throws here, not at the (silent) send.
  test('misconfigured tiny cap → hard fit assertion throws (no body the bus would reject)', async () => {
    const redis = fakeRedis()
    await expect(buildClaimCheckedInput(redis, 'x'.repeat(200), 50, claudeWrap('j5'))).rejects.toThrow(/exceeds cap 50/)
  })
})

describe('resolveJobInput — gateway recovers a claim-checked prompt (the receive side)', () => {
  test('no input_ref → inline join, unchanged (ref-less jobs are byte-for-byte identical)', async () => {
    expect(await resolveJobInput(fakeRedis(), { input: [{ text: 'hello' }, { text: 'world' }] })).toBe('hello\nworld')
    expect(await resolveJobInput(fakeRedis(), { input: [] })).toBe('')
    expect(await resolveJobInput(fakeRedis(), {})).toBe('')
  })

  test('input_ref present + blob exists → the FULL prompt', async () => {
    const redis = fakeRedis()
    const full = 'FULL SPEC\n'.repeat(5_000)
    const blob = await putBlob(redis, { text: full })
    const job = { input: [{ text: 'preview…' }], input_ref: { result_ref: blob.result_ref, sha256: blob.sha256, len: blob.len } }
    expect(await resolveJobInput(redis, job)).toBe(full)
  })

  test('blob missing → falls back to the inline preview + reports onMiss (loud, never empty)', async () => {
    let reason = ''
    const job = { input: [{ text: 'PREVIEW (clearly partial)' }], input_ref: { result_ref: BLOB_KEY_PREFIX + 'gone', sha256: 'a'.repeat(64), len: 9 } }
    const got = await resolveJobInput(fakeRedis(), job, (r) => { reason = r })
    expect(got).toBe('PREVIEW (clearly partial)')
    expect(reason).toBe('missing')
  })

  test('input_ref without sha256 → fail-closed (no_sha) → inline fallback', async () => {
    const redis = fakeRedis()
    const blob = await putBlob(redis, { text: 'secret' })
    let reason = ''
    const job = { input: [{ text: 'pv' }], input_ref: { result_ref: blob.result_ref, sha256: '', len: blob.len } }
    expect(await resolveJobInput(redis, job, (r) => { reason = r })).toBe('pv')
    expect(reason).toBe('no_sha')
  })
})

describe('buildClaimCheckedInput → resolveJobInput is the producer/consumer pair fusion uses', () => {
  test('end-to-end: fusion-svc dispatches a >8KB opus-leg input; claude-gw recovers it intact', async () => {
    const redis = fakeRedis() // ONE shared Redis — exactly the host-1 bus Redis both sides use
    const question = 'Review this spec for P0 bugs.'
    const spec = '## Spec\n' + '- requirement line that is quite verbose and detailed\n'.repeat(2_000)
    const merged = `${question}\n\n--- INPUT ---\n${spec}`
    expect(utf8ByteLength(merged)).toBeGreaterThan(CAP)

    // PRODUCER (fusion-svc claudeGw): build the dispatch body
    const { body, ref } = await buildClaimCheckedInput(redis, merged, CAP, claudeWrap('fusion-claude-e2e'))
    expect(measure(body)).toBeLessThanOrEqual(CAP) // would pass validateSendArgs (NOT body_too_large)
    expect(ref).not.toBeNull()

    // CONSUMER (claude-gw onInbound): resolve the prompt it will feed to the model
    const recovered = await resolveJobInput(redis, body as any, () => { throw new Error('should not miss') })
    expect(recovered).toBe(merged)
  })
})

describe('sendError — surface the TRUE bus rejection reason (RCA #1: no more "prompt too large?" guess)', () => {
  test('ok:true → empty', () => {
    expect(sendError({ content: [{ text: JSON.stringify({ ok: true, mode: 'jetstream', seq: 3 }) }] })).toBe('')
  })
  test('rejected → the actual error string', () => {
    expect(sendError({ content: [{ text: JSON.stringify({ ok: false, error: 'rate_limited' }) }] })).toBe('rate_limited')
    expect(sendError({ content: [{ text: JSON.stringify({ ok: false, error: 'body_too_large' }) }] })).toBe('body_too_large')
  })
  test('ok:false without an error field → unknown (not a fabricated guess)', () => {
    expect(sendError({ content: [{ text: JSON.stringify({ ok: false }) }] })).toBe('unknown')
  })
  test('non-JSON / malformed → unparseable', () => {
    expect(sendError({ content: [{ text: 'not json' }] })).toBe('unparseable')
    expect(sendError(null)).toBe('unparseable')
  })
})
