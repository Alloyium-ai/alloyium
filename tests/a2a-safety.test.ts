// A2A safety tests — the publish-confinement invariant that must never regress.
// The analogue of safety.test.ts for the one module (a2a-channel.ts) that is
// allowed to publish. T-S1 (no .publish in nats-channel.ts/webhook.ts) lives in
// safety.test.ts and stays green; here we gate the A2A side.
import { test, expect, describe } from 'bun:test'
import { assertA2ASubject, A2ADenied, DENY_PREFIXES, A2AChannel } from '../a2a-channel.ts'
import { validateSpecs } from '../nats-channel.ts'

const REPO = new URL('..', import.meta.url).pathname

// Slice out one function's source body (from its signature to the first
// column-0 closing brace) so a static check can target it precisely and stay
// valid as the module grows in later phases.
function fnSource(src: string, signature: string): string {
  const i = src.indexOf(signature)
  if (i === -1) throw new Error(`signature not found: ${signature}`)
  const after = src.slice(i)
  const end = after.indexOf('\n}')
  return end === -1 ? after : after.slice(0, end + 2)
}

describe('T-S3 — assertA2ASubject denies everything outside the two allowed shapes', () => {
  // Fire/ops/system subjects + malformed A2A shapes from spec §13/§15.
  const DENIED = [
    'trades.live.fire',
    'orders.submit',
    'fire.execute',
    'exec.run',
    '$JS.API.STREAM.PURGE.ADVISORY',
    'alloyium.channels.control',
    '_INBOX.x',
    'alloyium.a2a.agent.x.inbox.extra', // trailing segment
    'alloyium.a2a.agent.UPPER.inbox',   // uppercase not in token charset
    'alloyium.a2a.agent.a.b.inbox',     // dot inside the id token
    'alloyium.a2a.topic.*',             // wildcard
    'alloyium.a2a.>',                   // wildcard
    'alloyium.a2a.agent..inbox',        // empty id token
    'alloyium.a2a2.agent.x.inbox',      // not the a2a prefix
    'alloyium.a2a.presence.x',          // not one of the two shapes
    '',                               // empty
    'alloyium.a2a.agent.x.inbox ',      // trailing space
    'alloyium.a2a.agent.' + 'a'.repeat(300) + '.inbox', // over length
  ]
  for (const s of DENIED) {
    test(`denies ${JSON.stringify(s).slice(0, 60)}`, () => {
      expect(() => assertA2ASubject(s)).toThrow(A2ADenied)
    })
  }
})

describe('T-S4 — assertA2ASubject accepts exactly the two valid shapes', () => {
  const OK = [
    'alloyium.a2a.agent.scout-1.inbox',
    'alloyium.a2a.agent.analyst-2.inbox',
    'alloyium.a2a.agent.a.inbox',
    'alloyium.a2a.topic.broadcast',
    'alloyium.a2a.topic.research-room',
  ]
  for (const s of OK) {
    test(`accepts ${s}`, () => {
      expect(() => assertA2ASubject(s)).not.toThrow()
    })
  }
  test('the deny list still contains the fire/ops/system prefixes', () => {
    for (const p of ['trades.', 'orders.', 'fire.', 'exec.', '$JS.', 'alloyium.channels.']) {
      expect(DENY_PREFIXES).toContain(p)
    }
  })
})

describe('T-S2 — every publish is confined to publishA2A, after the assert', () => {
  test('nats-channel.ts and webhook.ts remain publish-free (T-S1 still holds)', async () => {
    for (const f of ['nats-channel.ts', 'webhook.ts']) {
      const code = (await Bun.file(REPO + f).text()).replace(/\/\/.*$/gm, '')
      expect(code).not.toMatch(/\.publish\s*\(/)
    }
  })
  test('a2a-channel.ts has <=2 .publish(, all inside publishA2A, after assertA2ASubject', async () => {
    const src = await Bun.file(REPO + 'a2a-channel.ts').text()
    const code = src.replace(/\/\/.*$/gm, '')
    const total = [...code.matchAll(/\.publish\s*\(/g)].length
    expect(total).toBeLessThanOrEqual(2)
    // publishA2A's body is the slice between its signature and the next method.
    const start = code.indexOf('private async publishA2A')
    const end = code.indexOf('async _publishForTest')
    expect(start).toBeGreaterThanOrEqual(0)
    expect(end).toBeGreaterThan(start)
    const body = code.slice(start, end)
    const inFn = [...body.matchAll(/\.publish\s*\(/g)].length
    expect(inFn).toBe(total) // all publishes are inside publishA2A
    const assertIdx = body.indexOf('assertA2ASubject(')
    const firstPub = body.search(/\.publish\s*\(/)
    expect(assertIdx).toBeGreaterThanOrEqual(0)
    expect(assertIdx).toBeLessThan(firstPub)
  })
})

describe('T-S9 (runtime half) — the dev bypass cannot publish outside alloyium.a2a.>', () => {
  test('a devNoAuth bridge still throws A2ADenied at the publish site for fire/ops subjects', async () => {
    const ch = new A2AChannel(async () => {}, { enabled: true, agentId: 'x', devNoAuth: true })
    await expect(ch._publishForTest('trades.live.fire')).rejects.toThrow(A2ADenied)
    await expect(ch._publishForTest('alloyium.channels.control')).rejects.toThrow(A2ADenied)
    await expect(ch._publishForTest('$JS.API.STREAM.PURGE.ADVISORY')).rejects.toThrow(A2ADenied)
  })
})

describe('prod start-gating refuses to come up without L2 creds / a signing key (AC17/AC20)', () => {
  test('ed25519 prod mode (no dev bypass) without a signing key does not start', async () => {
    const ch = new A2AChannel(async () => {}, {
      enabled: true,
      agentId: 'gate-1',
      devNoAuth: false,
      transportAuth: 'creds',
      credsPath: '/nonexistent.creds',
      signingKeyPath: '',
      sigAlg: 'ed25519',
    })
    await ch.start()
    expect(ch.isStarted()).toBe(false) // gated on a2a_signing_key_required, never connects
    await ch.stop()
  })
  test('prod mode without NATS creds does not start', async () => {
    const ch = new A2AChannel(async () => {}, {
      enabled: true,
      agentId: 'gate-2',
      devNoAuth: false,
      transportAuth: 'creds',
      credsPath: '',
      nkeyPath: '',
      sigAlg: 'ed25519',
      signingKey: 'x' as any,
    })
    await ch.start()
    expect(ch.isStarted()).toBe(false) // gated on a2a_creds_required
    await ch.stop()
  })
  test('an invalid agent-id is terminal (A2A stays off, advisory unaffected)', async () => {
    const ch = new A2AChannel(async () => {}, { enabled: true, agentId: 'BAD ID', devNoAuth: true })
    await ch.start()
    expect(ch.isStarted()).toBe(false)
    await ch.stop()
  })
})

describe('hardening regressions (review fixes)', () => {
  test('invalid A2A_SIG_ALG refuses to start (no silent HMAC fallback)', async () => {
    const ch = new A2AChannel(async () => {}, { enabled: true, agentId: 'algx', devNoAuth: true, sigAlg: 'bogus' as any })
    await ch.start()
    expect(ch.isStarted()).toBe(false) // gated a2a_config_invalid
    await ch.stop()
  })
  test('constructor rejects an off-namespace prefix (closes the publish seam)', () => {
    expect(() => new A2AChannel(async () => {}, { prefix: 'evil.' })).toThrow()
    expect(() => new A2AChannel(async () => {}, { prefix: '' })).toThrow()
    expect(() => new A2AChannel(async () => {}, { prefix: 'alloyium.channels.' })).toThrow()
    expect(() => new A2AChannel(async () => {}, { prefix: 'alloyium.a2a.it1.' })).not.toThrow()
    expect(() => new A2AChannel(async () => {}, { prefix: 'alloyium.a2a.' })).not.toThrow()
  })
})

describe('T-S5 — the publish prefix is not env-configurable', () => {
  test('no A2A_PREFIX env is read in any source file', async () => {
    for (const f of ['a2a-channel.ts', 'webhook.ts', 'nats-channel.ts']) {
      const src = (await Bun.file(REPO + f).text()).replace(/\/\/.*$/gm, '')
      expect(src).not.toMatch(/A2A_PREFIX/)
    }
  })
  test('webhook.ts constructs A2AChannel without passing a prefix option', async () => {
    const wh = (await Bun.file(REPO + 'webhook.ts').text()).replace(/\/\/.*$/gm, '')
    const m = wh.match(/new A2AChannel\(inject(?:,\s*\{([^}]*)\})?\)/)
    expect(m).not.toBeNull()
    expect(m?.[1] ?? '').not.toMatch(/prefix\s*:/)
  })
})

describe('T-S7 — the advisory plane refuses alloyium.a2a.* subjects (S2)', () => {
  test('validateSpecs rejects an advisory SubSpec bound under alloyium.a2a.', () => {
    const { valid, errors } = validateSpecs([
      { subject: 'alloyium.a2a.agent.x.inbox', mode: 'core' },
      { subject: 'advisory.alerts.example', mode: 'core' }, // a generic advisory subject still passes
    ])
    expect(valid.map((s) => s.subject)).toEqual(['advisory.alerts.example'])
    expect(errors.join()).toMatch(/A2A-bus-owned/)
  })
})

describe('T-S9 (static half) — the dev bypass never touches the allowlist', () => {
  test('assertA2ASubject body has no reference to A2A_DEV_NO_AUTH / devNoAuth', async () => {
    const src = await Bun.file(REPO + 'a2a-channel.ts').text()
    const body = fnSource(src, 'export function assertA2ASubject')
    expect(body).not.toMatch(/DEV_NO_AUTH|devNoAuth/i)
  })
})
