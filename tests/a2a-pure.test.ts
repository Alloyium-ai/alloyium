// A2A pure unit tests — no NATS/Redis/process. Envelope canonicalization and
// the ed25519 / HMAC sign+verify primitives (spec §6, §6.3, §15).
import { test, expect, describe } from 'bun:test'
import {
  canonical, signEnvelope, verifyEnvelope, verifyCanonical, signCanonical,
  importEd25519Seed, importEd25519Pub, type Envelope,
  validateSendArgs, TokenBucket, inboxSubject, topicSubject, assertA2ASubject,
  buildA2AAttrs, isValidInbound,
} from '../a2a-channel.ts'

const base = (over: Partial<Envelope> = {}): Envelope => ({
  v: 1, id: 'id1', from: 'scout-1', to: 'analyst-2', type: 'msg',
  ts: '2026-06-12T17:05:00Z', body: 'hello', ...over,
})

describe('canonical serialization (§6.3)', () => {
  test('fixed field order; omitted corr/ttl_ms render empty', () => {
    expect(canonical(base())).toBe('1|id1|scout-1|analyst-2|msg||2026-06-12T17:05:00Z||hello')
  })
  test('corr present (reply) and ttl_ms present', () => {
    expect(canonical(base({ type: 'reply', corr: 'r9', ttl_ms: 600000 })))
      .toBe('1|id1|scout-1|analyst-2|reply|r9|2026-12'.replace('2026-12', '2026-06-12T17:05:00Z') + '|600000|hello')
  })
  test('alg, thread and attrs are excluded from the canonical form', () => {
    const a = canonical(base({ alg: 'ed25519', thread: 'T', attrs: { k: 'v' } }))
    const b = canonical(base({ alg: 'hmac' }))
    expect(a).toBe(b)
    expect(a).toBe(canonical(base()))
  })
  test('body containing | and \\ is escaped so it cannot forge a field boundary', () => {
    // body = a | b \ c
    const c = canonical(base({ body: 'a|b\\c' }))
    expect(c.endsWith('a\\|b\\\\c')).toBe(true)
    // the only UNescaped '|' are the 8 field separators
    expect(c.replace(/\\\|/g, '').replace(/\\\\/g, '').split('|').length - 1).toBe(8)
  })
})

describe('ed25519 sign/verify', () => {
  async function freshKeys() {
    const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
    const rawPub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
    return { priv: kp.privateKey, pub: await importEd25519Pub(rawPub) }
  }

  test('round-trip: a signed envelope verifies under the matching pubkey', async () => {
    const { priv, pub } = await freshKeys()
    const e = base()
    e.alg = 'ed25519'; e.sig = await signEnvelope(e, 'ed25519', priv)
    expect(e.sig.length).toBeGreaterThan(0)
    expect(await verifyEnvelope(e, pub, 'ed25519')).toBe(true)
  })
  test('tamper with any signed field → verify fails', async () => {
    const { priv, pub } = await freshKeys()
    const e = base(); e.alg = 'ed25519'; e.sig = await signEnvelope(e, 'ed25519', priv)
    expect(await verifyEnvelope({ ...e, body: 'hello!' }, pub, 'ed25519')).toBe(false)
    expect(await verifyEnvelope({ ...e, from: 'mallory' }, pub, 'ed25519')).toBe(false)
    expect(await verifyEnvelope({ ...e, to: 'someone-else' }, pub, 'ed25519')).toBe(false)
  })
  test('wrong pubkey → verify fails', async () => {
    const { priv } = await freshKeys()
    const other = await freshKeys()
    const e = base(); e.alg = 'ed25519'; e.sig = await signEnvelope(e, 'ed25519', priv)
    expect(await verifyEnvelope(e, other.pub, 'ed25519')).toBe(false)
  })
  test('missing sig → verify is false (fail closed)', async () => {
    const { pub } = await freshKeys()
    expect(await verifyEnvelope(base(), pub, 'ed25519')).toBe(false)
  })
  test('verifyEnvelope rejects an alg that differs from the expected one (anti-downgrade)', async () => {
    const { priv, pub } = await freshKeys()
    const e = base(); e.alg = 'ed25519'; e.sig = await signEnvelope(e, 'ed25519', priv)
    expect(await verifyEnvelope(e, pub, 'hmac')).toBe(false)      // expected hmac, envelope says ed25519
    expect(await verifyEnvelope({ ...e, alg: undefined }, pub, 'ed25519')).toBe(false) // missing alg
  })
  test('importEd25519Seed round-trips: seed-derived key signs, raw pubkey verifies', async () => {
    const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
    const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', kp.privateKey))
    const seed = pkcs8.slice(pkcs8.length - 32) // last 32 bytes of the PKCS8 wrapper
    const rawPub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
    const signKey = await importEd25519Seed(seed)
    const e = base(); e.alg = 'ed25519'; e.sig = await signEnvelope(e, 'ed25519', signKey)
    expect(await verifyEnvelope(e, await importEd25519Pub(rawPub), 'ed25519')).toBe(true)
  })
  test('importEd25519Seed rejects a non-32-byte seed', async () => {
    await expect(importEd25519Seed(new Uint8Array(16))).rejects.toThrow()
  })
})

describe('HMAC sign/verify', () => {
  test('round-trip with the shared secret', async () => {
    const e = base(); e.alg = 'hmac'; e.sig = await signEnvelope(e, 'hmac', 'sekret')
    expect(await verifyEnvelope(e, 'sekret', 'hmac')).toBe(true)
  })
  test('wrong secret → verify fails', async () => {
    const e = base(); e.alg = 'hmac'; e.sig = await signEnvelope(e, 'hmac', 'sekret')
    expect(await verifyEnvelope(e, 'not-it', 'hmac')).toBe(false)
  })
  test('verifyCanonical is length-safe against a malformed signature', async () => {
    expect(await verifyCanonical('hmac', 'k', 'data', 'not-base64-but-short')).toBe(false)
  })
  test('unknown algorithm: signCanonical throws, verifyCanonical returns false', async () => {
    await expect(signCanonical('bogus' as any, 'k', 'data')).rejects.toThrow()
    expect(await verifyCanonical('bogus' as any, 'k', 'data', 'AAAA')).toBe(false)
  })
})

describe('validateSendArgs — pipeline V2–V6 (§7.5)', () => {
  const ctx = { agentId: 'scout-1', maxSendBytes: 16 }
  const ok = (a: any) => validateSendArgs(a, ctx)
  test('V2 bad recipient shapes', () => {
    for (const to of ['', 'UPPER', 'a.b', 'topic:', 'has space', 'topic:UP']) {
      expect(ok({ to, body: 'x' })).toEqual({ error: 'bad_recipient' })
    }
  })
  test('V3 self-send rejected', () => {
    expect(ok({ to: 'scout-1', body: 'x' })).toEqual({ error: 'self_send' })
  })
  test('V4 body must be a string and within the byte cap (reject, not truncate)', () => {
    expect(ok({ to: 'b', body: 123 })).toEqual({ error: 'bad_body' }) // wrong type, not size
    expect(ok({ to: 'b', body: 'x'.repeat(16) })).not.toHaveProperty('error') // exactly at cap
    expect(ok({ to: 'b', body: 'x'.repeat(17) })).toEqual({ error: 'body_too_large' })
  })
  test('ttl/attrs strictness', () => {
    expect(ok({ to: 'b', body: 'x', ttl_ms: 1e309 })).toEqual({ error: 'bad_ttl' }) // non-finite
    expect(ok({ to: 'b', body: 'x', ttl_ms: 500 })).toEqual({ error: 'bad_ttl' })   // below floor
    expect(ok({ to: 'b', body: 'x', attrs: { k: 5 } as any })).toEqual({ error: 'bad_attrs' }) // non-string value
    expect(ok({ to: 'b', body: 'x', attrs: ['nope'] as any })).toEqual({ error: 'bad_attrs' }) // array
  })
  test('V5 corr/type matrix', () => {
    expect(ok({ to: 'b', body: 'x', type: 'reply' })).toEqual({ error: 'bad_corr' })       // reply needs corr
    expect(ok({ to: 'b', body: 'x', type: 'msg', corr: 'r' })).toEqual({ error: 'bad_corr' }) // non-reply forbids corr
    expect(ok({ to: 'b', body: 'x', type: 'reply', corr: 'r' })).toMatchObject({ type: 'reply' })
    expect(ok({ to: 'b', body: 'x', type: 'bogus' })).toEqual({ error: 'bad_type' })
  })
  test('V6 reserved attrs rejected, plain attrs allowed', () => {
    expect(ok({ to: 'b', body: 'x', attrs: { from: 'mallory' } })).toEqual({ error: 'reserved_attr' })
    expect(ok({ to: 'b', body: 'x', attrs: { note: 'ok' } })).toMatchObject({ to: 'b' })
  })
  test('accepts a direct recipient and a topic recipient', () => {
    expect(ok({ to: 'analyst-2', body: 'x' })).toMatchObject({ isTopic: false, target: 'analyst-2' })
    expect(ok({ to: 'topic:room', body: 'x' })).toMatchObject({ isTopic: true, target: 'room' })
  })
})

describe('TokenBucket — V7 rate limiting', () => {
  test('drains capacity then refuses, refills over time', () => {
    let t = 0
    const b = new TokenBucket(3, 60, () => t) // 60/min = 1/sec
    expect([b.take(), b.take(), b.take()]).toEqual([true, true, true])
    expect(b.take()).toBe(false)
    t = 1000; expect(b.take()).toBe(true)  // +1 token after 1s
    expect(b.take()).toBe(false)
  })
  test('peek does not consume', () => {
    const b = new TokenBucket(1, 60, () => 0)
    expect(b.peek()).toBe(true)
    expect(b.peek()).toBe(true)
    expect(b.take()).toBe(true)
    expect(b.peek()).toBe(false)
  })
})

describe('V8 subject construction always passes the allowlist', () => {
  test('inbox/topic subjects built from validated tokens are accepted', () => {
    expect(() => assertA2ASubject(inboxSubject('alloyium.a2a.', 'analyst-2'))).not.toThrow()
    expect(() => assertA2ASubject(topicSubject('alloyium.a2a.', 'research-room'))).not.toThrow()
  })
  test('with a custom (test) prefix, the same subjects validate under that prefix', () => {
    const p = 'alloyium.a2a.it.'
    expect(() => assertA2ASubject(inboxSubject(p, 'x'), p)).not.toThrow()
    expect(() => assertA2ASubject(topicSubject(p, 'x'), p)).not.toThrow()
  })
})

describe('buildA2AAttrs — structural keys are unspoofable (T-S6)', () => {
  const env = base({ from: 'scout-1', to: 'analyst-2', type: 'request', id: 'mid-1', thread: 'T1', corr: 'c1',
    attrs: { feed: 'fake', from: 'mallory', to: 'victim', subject: 'SPOOF', type: 'evil', id: 'forged', notifId: 'poison', note: 'keepme' } })
  const a = buildA2AAttrs(env, 'alloyium.a2a.agent.analyst-2.inbox', 'direct')
  test('the structural attrs reflect the envelope, not the attrs payload', () => {
    expect(a.feed).toBe('a2a')
    expect(a.kind).toBe('direct')
    expect(a.subject).toBe('alloyium.a2a.agent.analyst-2.inbox')
    expect(a.from).toBe('scout-1')
    expect(a.to).toBe('analyst-2')
    expect(a.type).toBe('request')
    expect(a.id).toBe('mid-1')
  })
  test('non-structural attrs survive; thread/corr promoted', () => {
    expect(a.note).toBe('keepme')
    expect(a.notifId).toBeUndefined()
    expect(a.thread).toBe('T1')
    expect(a.corr).toBe('c1')
  })
})

describe('isValidInbound — structural acceptance (§8.3 step 1)', () => {
  const good = base()
  test('accepts a well-formed envelope', () => { expect(isValidInbound({ ...good })).toBe(true) })
  test('rejects wrong version / missing fields / bad type', () => {
    expect(isValidInbound({ ...good, v: 2 })).toBe(false)
    expect(isValidInbound({ ...good, id: undefined })).toBe(false)
    expect(isValidInbound({ ...good, type: 'bogus' })).toBe(false)
    expect(isValidInbound({ ...good, type: 'reply' })).toBe(false) // reply needs corr
    expect(isValidInbound({ ...good, ttl_ms: 'soon' })).toBe(false)
    expect(isValidInbound('not an object')).toBe(false)
  })
})
