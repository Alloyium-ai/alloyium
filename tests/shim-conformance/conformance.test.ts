import { describe, expect, test } from 'bun:test'
import {
  FrameDecoder,
  FrameProtocolError,
  FRAME_TYPE_CTRL,
  FRAME_TYPE_MCP,
} from '../../uds_frame.ts'
import {
  canonical,
  importEd25519Pub,
  importEd25519Seed,
  signCanonical,
  type Envelope,
} from '../../a2a-channel.ts'

const fromHex = (s: string) => new Uint8Array(Buffer.from(s, 'hex'))
const toHex = (u: Uint8Array) => Buffer.from(u).toString('hex')
const b64 = (u: Uint8Array) => Buffer.from(u).toString('base64')
const fromB64 = (s: string) => new Uint8Array(Buffer.from(s, 'base64'))

type FrameKind = 'mcp' | 'ctrl'
type ExpectedFrame = { type: FrameKind; payload_hex: string }
type FrameErrorCode =
  | 'FRAME_LENGTH_TOO_LARGE'
  | 'FRAME_LENGTH_TOO_SMALL'
  | 'FRAME_UNKNOWN_TYPE'

type FrameCase = {
  name: string
  desc: string
  input_hex?: string
  chunks_hex?: string[]
  expect?: {
    frames?: ExpectedFrame[]
    buffered?: number
    error?: FrameErrorCode
    frameLength?: number
    typeByte?: number
  }
  expect_per_chunk?: Array<{
    frames: number | ExpectedFrame[]
    buffered: number
  }>
}

type FramingFixture = {
  _note?: string
  frame_layout?: unknown
  error_codes?: string[]
  cases: FrameCase[]
}

type CanonFixture = {
  envelope: Envelope
  canonical_string: string
  canonical_utf8_hex: string
  expected_sig_b64: string
}

type CryptoFixture = {
  seed_hex: string
  pubkey_raw_b64: string
  pubkey_raw_hex: string
  pop: {
    nonce_b64: string
    nonce_hex: string
    expected_sig_b64: string
  }
  canon_4a: CanonFixture
  canon_4b: CanonFixture
}

type DecodedFrame = { type: number; payload: Uint8Array }

const framingFixture = (await Bun.file(
  new URL('./framing-vectors.json', import.meta.url),
).json()) as FramingFixture

const cryptoFixture = (await Bun.file(
  new URL('./vectors.json', import.meta.url),
).json()) as CryptoFixture

const expectedTypeByte = (type: FrameKind) =>
  type === 'mcp' ? FRAME_TYPE_MCP : FRAME_TYPE_CTRL

function expectFrames(got: DecodedFrame[], expected: ExpectedFrame[]) {
  expect(got).toHaveLength(expected.length)

  for (let i = 0; i < expected.length; i++) {
    expect(got[i].type).toBe(expectedTypeByte(expected[i].type))
    expect(toHex(got[i].payload)).toBe(expected[i].payload_hex)
  }
}

describe('§A.8 #1 framing vectors', () => {
  for (const c of framingFixture.cases) {
    test(c.name, () => {
      const dec = new FrameDecoder()
      const inputHex = c.input_hex
      const chunksHex = c.chunks_hex
      const expectPerChunk = c.expect_per_chunk

      if (typeof inputHex === 'string' && c.expect?.error) {
        let thrown: unknown

        try {
          dec.push(fromHex(inputHex))
        } catch (err) {
          thrown = err
        }

        expect(thrown).toBeInstanceOf(FrameProtocolError)
        const err = thrown as FrameProtocolError
        expect(err.code).toBe(c.expect.error)

        if (c.expect.frameLength !== undefined) {
          expect(err.frameLength).toBe(c.expect.frameLength)
        }
        if (c.expect.typeByte !== undefined) {
          expect(err.typeByte).toBe(c.expect.typeByte)
        }
        return
      }

      if (typeof inputHex === 'string' && c.expect) {
        const got = dec.push(fromHex(inputHex))
        expectFrames(got, c.expect.frames ?? [])
        expect(dec.bufferedBytes).toBe(c.expect.buffered ?? 0)
        return
      }

      if (Array.isArray(chunksHex) && expectPerChunk) {
        expect(expectPerChunk).toHaveLength(chunksHex.length)

        for (let i = 0; i < chunksHex.length; i++) {
          const got = dec.push(fromHex(chunksHex[i]))
          const expected = expectPerChunk[i]

          if (typeof expected.frames === 'number') {
            expect(got).toHaveLength(expected.frames)
          } else {
            expectFrames(got, expected.frames)
          }

          expect(dec.bufferedBytes).toBe(expected.buffered)
        }
        return
      }

      throw new Error(`unsupported framing fixture case shape: ${c.name}`)
    })
  }
})

describe('§A.8 #3 proof-of-possession', () => {
  test('signs the decoded nonce bytes and verifies with the public key', async () => {
    const seedKey = await importEd25519Seed(fromHex(cryptoFixture.seed_hex))
    const nonceBytes = fromHex(cryptoFixture.pop.nonce_hex)

    const sig = b64(new Uint8Array(
      await crypto.subtle.sign('Ed25519', seedKey, nonceBytes),
    ))

    expect(sig).toBe(cryptoFixture.pop.expected_sig_b64)
    expect(nonceBytes).toHaveLength(32)
    expect(b64(nonceBytes)).toBe(cryptoFixture.pop.nonce_b64)

    const pubBytes = fromB64(cryptoFixture.pubkey_raw_b64)
    expect(toHex(pubBytes)).toBe(cryptoFixture.pubkey_raw_hex)

    const pubKey = await importEd25519Pub(pubBytes)
    expect(await crypto.subtle.verify(
      'Ed25519',
      pubKey,
      fromB64(cryptoFixture.pop.expected_sig_b64),
      nonceBytes,
    )).toBe(true)

    const sigOverB64Text = b64(new Uint8Array(
      await crypto.subtle.sign(
        'Ed25519',
        seedKey,
        new TextEncoder().encode(cryptoFixture.pop.nonce_b64),
      ),
    ))
    expect(sigOverB64Text).not.toBe(cryptoFixture.pop.expected_sig_b64)
  })
})

describe('§A.8 #4 canonical envelope signing', () => {
  const cases = [
    ['canon_4a', cryptoFixture.canon_4a],
    ['canon_4b', cryptoFixture.canon_4b],
  ] as const

  for (const [name, c] of cases) {
    test(name, async () => {
      const seedKey = await importEd25519Seed(fromHex(cryptoFixture.seed_hex))
      const pubKey = await importEd25519Pub(fromB64(cryptoFixture.pubkey_raw_b64))

      const s1 = b64(new Uint8Array(
        await crypto.subtle.sign('Ed25519', seedKey, fromHex(c.canonical_utf8_hex)),
      ))
      expect(s1).toBe(c.expected_sig_b64)

      const canon = canonical(c.envelope)
      expect(canon).toBe(c.canonical_string)
      expect(Buffer.from(c.canonical_string, 'utf8').toString('hex')).toBe(c.canonical_utf8_hex)
      expect(await signCanonical('ed25519', seedKey, canon)).toBe(c.expected_sig_b64)

      expect(await crypto.subtle.verify(
        'Ed25519',
        pubKey,
        fromB64(c.expected_sig_b64),
        fromHex(c.canonical_utf8_hex),
      )).toBe(true)
    })
  }
})
