import type { RedisClient } from 'bun'
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import { isAbsolute } from 'node:path'
import { importEd25519Pub } from './a2a-channel.ts'
import { normalizeFeatureList, normalizePeerAppMetadata, type PeerAppMetadata } from './peer_protocol.ts'
import { canonicalAlloyiumHostId, type DeploymentContract } from './deployment_contract.ts'

export interface HelloResult {
  ok: boolean
  agentId?: string
  session?: string
  epoch?: number
  toolOnly?: boolean
  deliveryCapable?: boolean
  protocolVersion?: string
  protoFeatures?: string[]
  app?: PeerAppMetadata
  inboxDbPath?: string
  deploymentId?: string
  busId?: string
  hostId?: string
  errCode?: string
}

const AGENT_ID_RE = /^[a-z0-9-]{1,64}$/
const DEFAULT_HEARTBEAT_MS = 30_000
const DEFAULT_HELLO_TIMEOUT_MS = 5_000
const KEY_CACHE_TTL_MS = 60_000
const MAX_CACHE_KEYS = 4096

const PUBKEY_PREFIX = process.env.A2A_PUBKEY_KEY_PREFIX ?? 'claude-channels:a2a:pubkey:'
const PRESENCE_PREFIX = process.env.A2A_PRESENCE_KEY_PREFIX ?? 'claude-channels:a2a:presence:'
const EPOCH_PREFIX = process.env.A2A_CORE_EPOCH_KEY_PREFIX ?? 'claude-channels:a2a:org:core-epoch:'

const PRESENCE_RECLAIM_SCRIPT =
  "local v=redis.call('GET',KEYS[1]); " +
  "if v==false then return 'RECLAIMED' end; " +
  "local ok,d=pcall(cjson.decode,v); " +
  "if not ok or type(d)~='table' then return 'DUP' end; " +
  "local last=tonumber(d['last_seen']); " +
  "if last==nil then return 'DUP' end; " +
  "if tonumber(ARGV[1])-last > tonumber(ARGV[2]) then redis.call('DEL',KEYS[1]); return 'RECLAIMED' end; " +
  "return 'DUP'"

type CachedKey = { key: CryptoKey; exp: number }
const verifyKeyCache = new Map<string, CachedKey>()

class HelloTimeout extends Error {}

const b64 = (u8: Uint8Array): string => Buffer.from(u8).toString('base64')
const fromB64 = (s: string): Uint8Array => new Uint8Array(Buffer.from(s, 'base64'))

function isObj(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

// NB: subsKey is validated for wire-conformance only — it is ADVISORY and INTENTIONALLY NOT
// threaded into routing/subscriptions. The core derives subscription identity from the
// AUTHENTICATED agentId (a client-supplied subsKey must never scope what subjects it receives).
// Per §A: "subsKey ADVISORY (core routes from the authenticated agentId)". (gate-fold: subsKey)
function validCaps(v: unknown): v is string[] | undefined {
  return v === undefined || (Array.isArray(v) && v.length <= 16 && v.every((c) => typeof c === 'string' && c.length > 0 && c.length <= 64))
}

function validProto(v: unknown): v is { protocol_version?: string; features?: string[]; app?: unknown; a2a?: { min?: number; max?: number } } | undefined {
  if (v === undefined) return true
  if (!isObj(v)) return false
  if (v.protocol_version !== undefined && (typeof v.protocol_version !== 'string' || v.protocol_version.length > 64)) return false
  if (v.features !== undefined && normalizeFeatureList(v.features) == null) return false
  if (v.app !== undefined && normalizePeerAppMetadata(v.app) == null) return false
  if (v.a2a !== undefined) {
    if (!isObj(v.a2a)) return false
    const min = v.a2a.min
    const max = v.a2a.max
    if (min !== undefined && (!Number.isInteger(min) || min < 1 || min > 99)) return false
    if (max !== undefined && (!Number.isInteger(max) || max < 1 || max > 99)) return false
    if (typeof min === 'number' && typeof max === 'number' && max < min) return false
  }
  return true
}

function validInboxDbPath(v: unknown, toolOnly: unknown): v is string | undefined {
  if (v === undefined) return true
  return toolOnly === true &&
    typeof v === 'string' &&
    v.length > 0 &&
    v.length <= 1024 &&
    !/[\0\r\n]/.test(v) &&
    isAbsolute(v)
}

function validHello(v: unknown): v is { agentId: string; host: string; pid: number; subsKey: string; deploymentId?: string; busId?: string; hostId?: string; toolOnly?: boolean; inboxDbPath?: string; caps?: string[]; proto?: { protocol_version?: string; features?: string[]; app?: unknown; a2a?: { min?: number; max?: number } } } {
  if (!isObj(v)) return false
  return v.t === 'hello' &&
    v.v === 1 &&
    typeof v.agentId === 'string' &&
    AGENT_ID_RE.test(v.agentId) &&
    typeof v.host === 'string' &&
    v.host.length <= 256 &&
    typeof v.pid === 'number' &&
    Number.isInteger(v.pid) &&
    v.pid >= 1 &&
    v.pid <= 2 ** 31 &&
    typeof v.subsKey === 'string' &&
    v.subsKey.length <= 256 &&
    (v.deploymentId === undefined || (typeof v.deploymentId === 'string' && v.deploymentId.length <= 32)) &&
    (v.busId === undefined || (typeof v.busId === 'string' && v.busId.length <= 128)) &&
    (v.hostId === undefined || (typeof v.hostId === 'string' && v.hostId.length <= 64)) &&
    (v.toolOnly === undefined || typeof v.toolOnly === 'boolean') &&
    validInboxDbPath(v.inboxDbPath, v.toolOnly) &&
    validCaps(v.caps) &&
    validProto(v.proto)
}

function validAuth(v: unknown): v is { alg: 'ed25519'; sig: string } {
  return isObj(v) && v.t === 'auth' && v.alg === 'ed25519' && typeof v.sig === 'string'
}

function evictOldest<V>(m: Map<string, V>): void {
  if (m.size < MAX_CACHE_KEYS) return
  const k = m.keys().next().value
  if (k !== undefined) m.delete(k)
}

async function getVerifyKey(redis: RedisClient, agentId: string, now: number): Promise<CryptoKey | undefined> {
  const cached = verifyKeyCache.get(agentId)
  if (cached && cached.exp > now) return cached.key

  let pubB64: string | null
  try {
    pubB64 = await redis.get(PUBKEY_PREFIX + agentId)
  } catch {
    return undefined
  }
  if (!pubB64) return undefined

  try {
    const raw = fromB64(pubB64.trim())
    if (raw.length !== 32) return undefined
    const key = await importEd25519Pub(raw)
    evictOldest(verifyKeyCache)
    verifyKeyCache.set(agentId, { key, exp: now + KEY_CACHE_TTL_MS })
    return key
  } catch {
    return undefined
  }
}

async function reclaimPresence(
  redis: RedisClient,
  agentId: string,
  now: number,
  heartbeatMs: number,
): Promise<'RECLAIMED' | 'DUP'> {
  const r = await redis.send('EVAL', [
    PRESENCE_RECLAIM_SCRIPT,
    '1',
    PRESENCE_PREFIX + agentId,
    String(now),
    String(2 * heartbeatMs),
  ])
  return r === 'RECLAIMED' ? 'RECLAIMED' : 'DUP'
}

async function nextEpoch(redis: RedisClient, agentId: string): Promise<number | undefined> {
  const raw = await redis.send('INCR', [EPOCH_PREFIX + agentId])
  const n = Number(raw)
  if (!Number.isInteger(n) || n < 1 || n > 0xffffffff) return undefined
  return n
}

export async function runHello(opts: {
  recvCtrl: () => Promise<any>
  sendCtrl: (o: any) => void
  redis: RedisClient
  randomBytes: (n: number) => Uint8Array
  now: () => number
  heartbeatMs?: number
  ttlS?: number
  helloTimeoutMs?: number
  peerUid?: number
  expectedUid?: number
  deploymentContract?: Pick<DeploymentContract, 'deploymentId' | 'busId' | 'hostId'>
}): Promise<HelloResult> {
  const heartbeatMs = opts.heartbeatMs ?? DEFAULT_HEARTBEAT_MS
  const helloTimeoutMs = opts.helloTimeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS

  const fail = (code: string): HelloResult => {
    opts.sendCtrl({ t: 'err', code })
    return { ok: false, errCode: code }
  }

  // Peercred is fail-CLOSED: if enforcement is requested (expectedUid set) but the peer uid is
  // unavailable (e.g. Bun exposes no SO_PEERCRED), REJECT rather than fall open. When expectedUid
  // is unset, the socket dir's 0700 mode is the access control.
  if (opts.expectedUid != null) {
    if (opts.peerUid == null) return fail('peercred_unavailable')
    if (opts.peerUid !== opts.expectedUid) return fail('peercred_mismatch')
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new HelloTimeout('hello timeout')), helloTimeoutMs)
  })
  const clear = (): void => {
    if (timer !== undefined) {
      clearTimeout(timer)
      timer = undefined
    }
  }
  const recvWithDeadline = (): Promise<any> => Promise.race([opts.recvCtrl(), timeout])

  try {
    const hello = await recvWithDeadline()
    if (!validHello(hello)) {
      clear()
      return fail('bad_hello')
    }
    // A ping only proves that some core owns the socket. The authenticated hello carries the
    // deployment/bus/physical-host tuple and is rejected before any Redis presence/epoch write
    // when it does not match the core that accepted the UDS connection.
    if (opts.deploymentContract) {
      if (hello.deploymentId !== opts.deploymentContract.deploymentId) {
        clear()
        return fail('deployment_mismatch')
      }
      if (hello.busId !== opts.deploymentContract.busId) {
        clear()
        return fail('bus_mismatch')
      }
      if (!hello.hostId || canonicalAlloyiumHostId(hello.hostId) !== opts.deploymentContract.hostId) {
        clear()
        return fail('host_mismatch')
      }
    }

    const nonceBytes = new Uint8Array(opts.randomBytes(32))
    if (nonceBytes.length !== 32) {
      clear()
      return fail('pubkey_unavailable')
    }

    let nonce: Uint8Array | undefined = nonceBytes
    opts.sendCtrl({ t: 'challenge', nonce: b64(nonceBytes) })

    const auth = await recvWithDeadline()
    clear()

    if (!validAuth(auth) || nonce === undefined) {
      nonce = undefined
      return fail('bad_sig')
    }

    const verifyBytes = nonce
    nonce = undefined

    const key = await getVerifyKey(opts.redis, hello.agentId, opts.now())
    if (!key) return fail('pubkey_unavailable')

    const sig = fromB64(auth.sig)
    const sigOk = sig.length === 64 &&
      await crypto.subtle.verify({ name: 'Ed25519' }, key, sig, verifyBytes)
    if (!sigOk) return fail('bad_sig')

    if (hello.toolOnly !== true) {
      let presence: 'RECLAIMED' | 'DUP'
      try {
        presence = await reclaimPresence(opts.redis, hello.agentId, opts.now(), heartbeatMs)
      } catch {
        presence = 'DUP'
      }
      if (presence !== 'RECLAIMED') return fail('dup_agent')
    }

    let epoch: number | undefined
    try {
      epoch = await nextEpoch(opts.redis, hello.agentId)
    } catch {
      epoch = undefined
    }
    if (epoch === undefined) return fail('pubkey_unavailable')

    const session = randomUUID()
    // FOLD3: runHello no longer sends {t:ok} — it returns ok-data and the ACCEPTOR sends {t:ok}
    // only AFTER core.addUdsSession succeeds, so a client is never told ok then dropped (session_exists).
    return {
      ok: true,
      agentId: hello.agentId,
      session,
      epoch,
      toolOnly: hello.toolOnly === true,
      deliveryCapable: hello.caps?.includes('delivered') === true,
      protocolVersion: hello.proto?.protocol_version,
      protoFeatures: normalizeFeatureList(hello.proto?.features ?? []) ?? [],
      app: normalizePeerAppMetadata(hello.proto?.app) ?? undefined,
      inboxDbPath: hello.inboxDbPath,
      deploymentId: hello.deploymentId,
      busId: hello.busId,
      hostId: hello.hostId ? canonicalAlloyiumHostId(hello.hostId) : undefined,
    }
  } catch (e) {
    clear()
    if (e instanceof HelloTimeout) return fail('hello_timeout')
    return fail('bad_hello')
  } finally {
    clear()
  }
}
