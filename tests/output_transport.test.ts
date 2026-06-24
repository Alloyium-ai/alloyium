// Pure unit tests for output_transport.ts — byte-safe truncation, claim-check
// blob round-trip, and the planReply/fitTruncate byte-fit guarantee. No live bus.
import { test, expect, describe } from 'bun:test'
import {
  utf8ByteLength, truncateToBytes, sha256Hex, previewMarked, blobTtlS,
  putBlob, getBlob, delBlob, planReply, fitTruncate, BLOB_KEY_PREFIX,
  sentOk, resolveRef, type BlobRedis, type WrapFn,
} from '../output_transport.ts'

// In-memory fake Redis implementing the subset used: GET, SET .. NX EX, DEL.
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

describe('truncateToBytes', () => {
  test('ascii exact boundary', () => {
    const r = truncateToBytes('hello world', 5)
    expect(r.text).toBe('hello'); expect(r.truncated).toBe(true)
    expect(r.originalBytes).toBe(11); expect(r.emittedBytes).toBe(5)
  })
  test('no truncation within budget', () => {
    const r = truncateToBytes('héllo', 100)
    expect(r.truncated).toBe(false); expect(r.text).toBe('héllo')
  })
  test('never splits a 2-byte codepoint at any budget', () => {
    const s = 'aééé' // a(1) + é(2)*3 = 7 bytes
    for (let cap = 0; cap <= utf8ByteLength(s); cap++) {
      const r = truncateToBytes(s, cap)
      expect(r.emittedBytes).toBeLessThanOrEqual(cap)
      expect(r.text).not.toContain('�')
      expect(Buffer.from(r.text, 'utf8').toString('utf8')).toBe(r.text) // clean decode
    }
  })
  test('4-byte emoji never split', () => {
    const r = truncateToBytes('😀😀😀', 6) // cut mid 2nd emoji (each 4 bytes)
    expect(r.text).toBe('😀'); expect(r.emittedBytes).toBe(4)
  })
})

describe('sha256Hex', () => {
  test('known vector + deterministic', () => {
    expect(sha256Hex('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    expect(sha256Hex('x')).toBe(sha256Hex('x'))
  })
})

describe('blobTtlS', () => {
  test('outlives inbox retention + margin and floors at 90000', () => {
    expect(blobTtlS()).toBeGreaterThanOrEqual(90000)
    expect(blobTtlS()).toBeGreaterThanOrEqual(24 * 3600 + 21600)
  })
})

describe('putBlob / getBlob', () => {
  test('round-trips a >200KB mixed payload; sha256 + len verify', async () => {
    const redis = fakeRedis()
    const big = 'A'.repeat(150_000) + '😀'.repeat(20_000) + '\n"q"\\\n'.repeat(4000)
    expect(utf8ByteLength(big)).toBeGreaterThan(200_000)
    const ref = await putBlob(redis, { text: big })
    expect(ref.result_ref.startsWith(BLOB_KEY_PREFIX)).toBe(true)
    expect(ref.len).toBe(utf8ByteLength(big))
    expect(ref.sha256).toBe(sha256Hex(big))
    const got = await getBlob(redis, ref)
    expect(got.ok).toBe(true)
    if (got.ok) expect(got.text).toBe(big)
  })
  test('checksum mismatch → checksum', async () => {
    const redis = fakeRedis()
    const ref = await putBlob(redis, { text: 'hello' })
    redis.store.set(ref.result_ref, 'world') // same len (5), different bytes
    expect(await getBlob(redis, ref)).toEqual({ ok: false, reason: 'checksum' })
  })
  test('len mismatch → len', async () => {
    const redis = fakeRedis()
    const ref = await putBlob(redis, { text: 'hello' })
    redis.store.set(ref.result_ref, 'hello!!') // wrong length
    expect(await getBlob(redis, ref)).toEqual({ ok: false, reason: 'len' })
  })
  test('missing key → missing', async () => {
    const redis = fakeRedis()
    expect(await getBlob(redis, { result_ref: BLOB_KEY_PREFIX + 'nope', sha256: 'a'.repeat(64), len: 1 }))
      .toEqual({ ok: false, reason: 'missing' })
  })
  test('absent sha256 → no_sha (fail-closed)', async () => {
    const redis = fakeRedis()
    const ref = await putBlob(redis, { text: 'hello' })
    expect(await getBlob(redis, { result_ref: ref.result_ref, sha256: '', len: ref.len }))
      .toEqual({ ok: false, reason: 'no_sha' })
  })
  test('NX collision retried with a fresh id', async () => {
    const redis = fakeRedis()
    redis.store.set(BLOB_KEY_PREFIX + 'dup', 'x')
    let i = 0; const ids = ['dup', 'unique']
    const ref = await putBlob(redis, { text: 'hi', genId: () => ids[Math.min(i++, ids.length - 1)]! })
    expect(ref.result_ref).toBe(BLOB_KEY_PREFIX + 'unique')
  })
  test('delBlob removes the key', async () => {
    const redis = fakeRedis()
    const ref = await putBlob(redis, { text: 'hello' })
    await delBlob(redis, ref.result_ref)
    expect(redis.store.has(ref.result_ref)).toBe(false)
  })
  test('redis error → putBlob throws, getBlob returns error (gateway falls back)', async () => {
    const erroringPut: BlobRedis = { async get() { return null }, async send() { throw new Error('redis down') } }
    await expect(putBlob(erroringPut, { text: 'hi' })).rejects.toThrow()
    const erroringGet: BlobRedis = { async get() { throw new Error('redis down') }, async send() { return 'OK' } }
    expect(await getBlob(erroringGet, { result_ref: 'k', sha256: 'a'.repeat(64), len: 1 })).toEqual({ ok: false, reason: 'error' })
  })
})

describe('planReply / fitTruncate — assembled reply ALWAYS fits bodyCap', () => {
  const jobWrap = (jobId: string, triageId: string): WrapFn => (text, extra) => ({
    schema: 'codex.job.completed.v1', job_id: jobId, triage_id: triageId, status: 'completed', output: text, ...(extra ?? {}),
  })
  const measure = (o: unknown) => Buffer.byteLength(JSON.stringify(o), 'utf8')

  test('inline when full fits', () => {
    expect(planReply('short', 100_000, jobWrap('j1', 't1')).kind).toBe('inline')
  })

  test('escape-heavy + multibyte + long ids: every branch fits its cap', () => {
    const wrap = jobWrap('a-very-long-job-id-1234567890-abcdef', 'triage-id-9876543210-zyxwvut')
    const nasty = ('"\\\n\t😀é中' + 'x'.repeat(40)).repeat(500)
    for (const cap of [512, 1024, 2048, 8192]) {
      const plan = planReply(nasty, cap, wrap)
      let reply: unknown
      if (plan.kind === 'inline') reply = wrap(plan.output)
      else if (plan.kind === 'claimcheck') reply = wrap(plan.preview, { output_preview: true, result_ref: BLOB_KEY_PREFIX + '00000000-0000-0000-0000-000000000000', sha256: 'f'.repeat(64), len: utf8ByteLength(nasty), encoding: 'utf8', expires_at: '2026-06-15T00:00:00.000Z', truncated: false })
      else reply = wrap(plan.output, { truncated: true, original_bytes: plan.original_bytes, emitted_bytes: plan.emitted_bytes })
      expect(measure(reply)).toBeLessThanOrEqual(cap)
      // large content must NOT go inline
      expect(plan.kind).not.toBe('inline')
    }
  })

  test('claim-check is preferred over truncate when allowed', () => {
    const wrap = jobWrap('j', 't')
    const plan = planReply('Z'.repeat(20_000), 8192, wrap)
    expect(plan.kind).toBe('claimcheck')
  })

  test('plain-text form (allowClaimcheck:false) truncates to fit, body = raw string', () => {
    const plainWrap: WrapFn = (t) => t
    const nasty = '"\\\n'.repeat(1000) + '😀'.repeat(100)
    for (const cap of [16, 64, 256]) {
      const plan = planReply(nasty, cap, plainWrap, { allowClaimcheck: false })
      expect(['inline', 'truncate']).toContain(plan.kind)
      const body = plan.kind === 'inline' ? plan.output : (plan as { output: string }).output
      expect(Buffer.byteLength(body, 'utf8')).toBeLessThanOrEqual(cap)
      expect(body).not.toContain('�')
    }
  })

  test('fitTruncate never splits multibyte', () => {
    const r = fitTruncate('😀'.repeat(100), 50, jobWrap('j', 't'))
    expect(r.kind).toBe('truncate')
    expect(r.output).not.toContain('�')
    expect(Buffer.from(r.output, 'utf8').toString('utf8')).toBe(r.output)
  })
})

describe('previewMarked', () => {
  test('byte-bounded head + len-bearing marker', () => {
    const full = 'x'.repeat(5000)
    const p = previewMarked(full, 100, utf8ByteLength(full))
    expect(p).toContain('truncated for transport')
    expect(p).toContain('5000B')
    expect(utf8ByteLength(p.split('\n…[')[0]!)).toBeLessThanOrEqual(100)
  })
})

describe('sentOk (shared MCP-result unwrap)', () => {
  test('true only on ok:true', () => {
    expect(sentOk({ content: [{ text: JSON.stringify({ ok: true, seq: 5, mode: 'jetstream' }) }] })).toBe(true)
    expect(sentOk({ content: [{ text: JSON.stringify({ ok: false, error: 'body_too_large' }) }] })).toBe(false)
    expect(sentOk({ content: [{ text: 'not json' }] })).toBe(false)
    expect(sentOk(null)).toBe(false)
    expect(sentOk({})).toBe(false)
  })
})

describe('resolveRef (shared consumer resolution)', () => {
  test('returns inline output when no ref', async () => {
    expect(await resolveRef(fakeRedis(), { output: 'inline' })).toBe('inline')
    expect(await resolveRef(fakeRedis(), {})).toBe('')
  })
  test('fetches + verifies the blob when ref present', async () => {
    const redis = fakeRedis()
    const ref = await putBlob(redis, { text: 'FULL'.repeat(10_000) })
    expect(await resolveRef(redis, { result_ref: ref.result_ref, sha256: ref.sha256, len: ref.len, output: 'preview…' }))
      .toBe('FULL'.repeat(10_000))
  })
  test('falls back to preview + calls onMiss on blob failure', async () => {
    let reason = ''
    const text = await resolveRef(fakeRedis(), { result_ref: BLOB_KEY_PREFIX + 'gone', sha256: 'a'.repeat(64), len: 5, output: 'PREVIEW' }, (r) => { reason = r })
    expect(text).toBe('PREVIEW'); expect(reason).toBe('missing')
  })
})
