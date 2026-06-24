import { describe, expect, test } from 'bun:test'
import { Buffer } from 'node:buffer'
import type { RedisClient } from 'bun'
import { runHello } from '../uds_hello.ts'

const PUBKEY_PREFIX = 'alloyium:a2a:pubkey:'
const PRESENCE_PREFIX = 'alloyium:a2a:presence:'
const EPOCH_PREFIX = 'alloyium:a2a:org:core-epoch:'

class FakeRedis {
  store = new Map<string, string>()

  async get(key: string): Promise<string | null> {
    return this.store.get(key) ?? null
  }

  async set(key: string, value: string): Promise<'OK'> {
    this.store.set(key, value)
    return 'OK'
  }

  async send(cmd: string, args: string[]): Promise<any> {
    switch (cmd.toUpperCase()) {
      case 'EVAL': {
        const key = args[2]
        const now = Number(args[3])
        const staleAfter = Number(args[4])
        const cur = this.store.get(key)
        if (cur === undefined) return 'RECLAIMED'

        let lastSeen: number
        try {
          lastSeen = Number(JSON.parse(cur).last_seen)
        } catch {
          return 'DUP'
        }

        if (Number.isFinite(lastSeen) && now - lastSeen > staleAfter) {
          this.store.delete(key)
          return 'RECLAIMED'
        }
        return 'DUP'
      }
      case 'INCR': {
        const key = args[0]
        const n = Number(this.store.get(key) ?? '0') + 1
        this.store.set(key, String(n))
        return n
      }
      default:
        throw new Error(`unsupported redis command: ${cmd}`)
    }
  }
}

function asRedis(redis: FakeRedis): RedisClient {
  return redis as unknown as RedisClient
}

function fixedRandomBytes(n: number): Uint8Array {
  expect(n).toBe(32)
  return Uint8Array.from({ length: n }, (_, i) => i + 1)
}

async function freshAgent(redis: FakeRedis, agentId: string) {
  const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
  const rawPub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
  await redis.set(PUBKEY_PREFIX + agentId, Buffer.from(rawPub).toString('base64'))

  return {
    async signRaw(bytes: Uint8Array): Promise<string> {
      const sig = new Uint8Array(await crypto.subtle.sign({ name: 'Ed25519' }, kp.privateKey, bytes))
      return Buffer.from(sig).toString('base64')
    },
    async signText(text: string): Promise<string> {
      return this.signRaw(new TextEncoder().encode(text))
    },
  }
}

const hello = (agentId: string, toolOnly?: boolean, caps?: string[]) => ({
  t: 'hello',
  v: 1,
  agentId,
  host: 'test-host',
  pid: 1234,
  subsKey: 'alloyium:test:subs',
  ...(toolOnly === undefined ? {} : { toolOnly }),
  ...(caps === undefined ? {} : { caps }),
})

async function runScripted(opts: {
  agentId: string
  redis: FakeRedis
  authSig: (challengeNonceB64: string) => Promise<string>
  now?: number
  helloTimeoutMs?: number
  extraAfterAuth?: any[]
  toolOnly?: boolean
  caps?: string[]
}) {
  const sent: any[] = []
  let step = 0
  const recvCtrl = async (): Promise<any> => {
    if (step++ === 0) return hello(opts.agentId, opts.toolOnly, opts.caps)
    const challenge = sent.find((m) => m.t === 'challenge')
    expect(challenge).toBeTruthy()
    const sig = await opts.authSig(challenge.nonce)
    if (step === 2) return { t: 'auth', alg: 'ed25519', sig }
    return opts.extraAfterAuth?.shift() ?? { t: 'auth', alg: 'ed25519', sig }
  }

  const result = await runHello({
    recvCtrl,
    sendCtrl: (o) => sent.push(o),
    redis: asRedis(opts.redis),
    randomBytes: fixedRandomBytes,
    now: () => opts.now ?? 1_000_000,
    helloTimeoutMs: opts.helloTimeoutMs,
  })
  return { result, sent, calls: step }
}

describe('runHello', () => {
  test('happy path: valid raw-nonce signature returns ok and epoch', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-happy'
    const agent = await freshAgent(redis, agentId)

    const { result, sent } = await runScripted({
      agentId,
      redis,
      authSig: (nonceB64) => agent.signRaw(new Uint8Array(Buffer.from(nonceB64, 'base64'))),
    })

    expect(result.ok).toBe(true)
    expect(result.agentId).toBe(agentId)
    expect(result.epoch).toBe(1)
    expect(result.deliveryCapable).toBe(false)
    expect(result.session).toBeString()
    // FOLD3: runHello returns ok-data but no longer SENDS {t:ok} (the acceptor sends it only
    // after core.addUdsSession succeeds). epoch is verified via result.epoch above.
    expect(sent.map((m) => m.t)).toEqual(['challenge'])
    expect(redis.store.get(EPOCH_PREFIX + agentId)).toBe('1')
  })

  test('happy path: delivered capability is negotiated from hello caps', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-delivered'
    const agent = await freshAgent(redis, agentId)

    const { result } = await runScripted({
      agentId,
      redis,
      caps: ['delivered'],
      authSig: (nonceB64) => agent.signRaw(new Uint8Array(Buffer.from(nonceB64, 'base64'))),
    })

    expect(result.ok).toBe(true)
    expect(result.deliveryCapable).toBe(true)
  })

  test('bad_sig: signature over nonce base64 text is rejected', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-badsig'
    const agent = await freshAgent(redis, agentId)

    const { result, sent } = await runScripted({
      agentId,
      redis,
      authSig: (nonceB64) => agent.signText(nonceB64),
    })

    expect(result).toEqual({ ok: false, errCode: 'bad_sig' })
    expect(sent.at(-1)).toEqual({ t: 'err', code: 'bad_sig' })
  })

  test('unknown_agent: missing pubkey returns pubkey_unavailable', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-unknown'

    const { result, sent } = await runScripted({
      agentId,
      redis,
      authSig: async () => Buffer.from(new Uint8Array(64)).toString('base64'),
    })

    expect(result).toEqual({ ok: false, errCode: 'pubkey_unavailable' })
    expect(sent.at(-1)).toEqual({ t: 'err', code: 'pubkey_unavailable' })
  })

  test('dup_agent: fresh presence key rejects the session', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-dup'
    const now = 1_000_000
    const agent = await freshAgent(redis, agentId)
    redis.store.set(PRESENCE_PREFIX + agentId, JSON.stringify({ last_seen: now }))

    const { result, sent } = await runScripted({
      agentId,
      redis,
      now,
      authSig: (nonceB64) => agent.signRaw(new Uint8Array(Buffer.from(nonceB64, 'base64'))),
    })

    expect(result).toEqual({ ok: false, errCode: 'dup_agent' })
    expect(sent.at(-1)).toEqual({ t: 'err', code: 'dup_agent' })
  })

  test('toolOnly session skips fresh presence collision and returns toolOnly true', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-toolonly'
    const now = 1_000_000
    const agent = await freshAgent(redis, agentId)
    redis.store.set(PRESENCE_PREFIX + agentId, JSON.stringify({ last_seen: now }))

    const { result, sent } = await runScripted({
      agentId,
      redis,
      now,
      toolOnly: true,
      authSig: (nonceB64) => agent.signRaw(new Uint8Array(Buffer.from(nonceB64, 'base64'))),
    })

    expect(result.ok).toBe(true)
    expect(result.agentId).toBe(agentId)
    expect(result.toolOnly).toBe(true)
    expect(result.epoch).toBe(1)
    expect(sent.map((m) => m.t)).toEqual(['challenge'])
    expect(redis.store.has(PRESENCE_PREFIX + agentId)).toBe(true)
  })

  test('C.2 reclaim: stale presence key is deleted and session proceeds', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-reclaim'
    const now = 1_000_000
    const agent = await freshAgent(redis, agentId)
    redis.store.set(PRESENCE_PREFIX + agentId, JSON.stringify({ last_seen: now - 70_000 }))

    const { result, sent } = await runScripted({
      agentId,
      redis,
      now,
      authSig: (nonceB64) => agent.signRaw(new Uint8Array(Buffer.from(nonceB64, 'base64'))),
    })

    expect(result.ok).toBe(true)
    // FOLD3: runHello's last sent CTRL on success is the challenge; {t:ok} is the acceptor's after add.
    expect(sent.at(-1).t).toBe('challenge')
    expect(redis.store.has(PRESENCE_PREFIX + agentId)).toBe(false)
  })

  test('hello_timeout: hanging recvCtrl returns hello_timeout', async () => {
    const redis = new FakeRedis()
    const sent: any[] = []

    const result = await runHello({
      recvCtrl: () => new Promise(() => {}),
      sendCtrl: (o) => sent.push(o),
      redis: asRedis(redis),
      randomBytes: fixedRandomBytes,
      now: () => 0,
      helloTimeoutMs: 5,
    })

    expect(result).toEqual({ ok: false, errCode: 'hello_timeout' })
    expect(sent).toEqual([{ t: 'err', code: 'hello_timeout' }])
  })

  test('nonce single-use: a bad auth is terminal even if a valid auth is queued after it', async () => {
    const redis = new FakeRedis()
    const agentId = 'agent-single-use'
    await freshAgent(redis, agentId)

    const sent: any[] = []
    let calls = 0
    const recvCtrl = async (): Promise<any> => {
      calls += 1
      if (calls === 1) return hello(agentId)
      return { t: 'auth', alg: 'ed25519', sig: Buffer.from(new Uint8Array(64)).toString('base64') }
    }

    const result = await runHello({
      recvCtrl,
      sendCtrl: (o) => sent.push(o),
      redis: asRedis(redis),
      randomBytes: fixedRandomBytes,
      now: () => 1_000_000,
    })

    expect(result).toEqual({ ok: false, errCode: 'bad_sig' })
    expect(calls).toBe(2)
    expect(sent.map((m) => m.t)).toEqual(['challenge', 'err'])
  })

  test('peercred_mismatch rejects before reading hello', async () => {
    const redis = new FakeRedis()
    const sent: any[] = []
    let read = false

    const result = await runHello({
      recvCtrl: async () => {
        read = true
        return hello('agent-peer')
      },
      sendCtrl: (o) => sent.push(o),
      redis: asRedis(redis),
      randomBytes: fixedRandomBytes,
      now: () => 0,
      peerUid: 1001,
      expectedUid: 1002,
    })

    expect(read).toBe(false)
    expect(result).toEqual({ ok: false, errCode: 'peercred_mismatch' })
    expect(sent).toEqual([{ t: 'err', code: 'peercred_mismatch' }])
  })
})
