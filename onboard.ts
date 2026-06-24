#!/usr/bin/env bun
// A2A agent onboarding.
//
// One command stands up everything an agent needs to join the A2A bus:
//   1. an ed25519 signing keypair (the per-message identity) — seed written
//      locally (0600), public key self-registered in Redis with SET NX;
//   2. a NATS nkey (the transport identity) — seed written locally (0600);
//   3. a nats-server.conf authorization block restricting that nkey's PUBLISH
//      to alloyium.a2a.>, $JS.ACK.>, $JS.API.> (the operator applies + reloads);
//   4. an env file the bridge can source;
//   5. a verify step that signs a probe and checks the Redis pubkey verifies it.
//
// Pubkey registration is SELF-SERVICE and NX-guarded (first-write-wins): a fresh
// agent-id claims its own key; a second key for the same id is REFUSED unless
// --force (key rotation). This is the spec's self-registration mode — convenient,
// and safe against silent takeover, but a Redis-writer could squat an UNCLAIMED
// id, so keep the pubkey namespace write-restricted in any hardened deployment.
//
// CLI:  bun onboard.ts <agent-id> [--dir <out>] [--redis <url>] [--nats <url>]
//                       [--force] [--no-verify]
import { RedisClient } from 'bun'
import { writeFileSync, readFileSync, existsSync, mkdirSync, chmodSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { createUser, fromSeed } from 'nkeys.js'
import { importEd25519Seed, importEd25519Pub, signEnvelope, verifyEnvelope, type Envelope } from './a2a-channel.ts'

const PUBKEY_PREFIX = process.env.A2A_PUBKEY_KEY_PREFIX ?? 'alloyium:a2a:pubkey:'
const ID_RE = /^[a-z0-9-]{1,64}$/
const b64 = (u8: Uint8Array): string => Buffer.from(u8).toString('base64')
const unb64 = (s: string): Uint8Array => new Uint8Array(Buffer.from(s, 'base64'))

// ── key generation ──────────────────────────────────────────────────────────

// ed25519 application-signing keypair. Returns the 32-byte seed + raw 32-byte
// public key, both base64. The seed is what the bridge loads as A2A_SIGNING_KEY;
// the public key is what peers fetch from Redis to verify.
export async function generateEd25519Keypair(): Promise<{ seedB64: string; pubB64: string }> {
  const kp = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']) as CryptoKeyPair
  const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', kp.privateKey))
  // RFC 8410 ed25519 PKCS8 is exactly 48 bytes (16-byte header + 32-byte seed).
  // Assert it so a runtime that ever emits a different layout fails loudly here,
  // not by silently extracting a wrong "seed" that importEd25519Seed can't use.
  if (pkcs8.length !== 48) throw new Error(`unexpected ed25519 PKCS8 length ${pkcs8.length} (expected 48)`)
  const seed = pkcs8.slice(pkcs8.length - 32) // last 32 bytes of the PKCS8 wrapper
  const pub = new Uint8Array(await crypto.subtle.exportKey('raw', kp.publicKey))
  return { seedB64: b64(seed), pubB64: b64(pub) }
}

// NATS nkey user identity. `seed` ("SU…") is the secret the bridge connects with
// (A2A_NKEY); `publicKey` ("U…") goes in the server authorization block.
export function generateNkeyUser(): { seed: string; publicKey: string } {
  const kp = createUser()
  return { seed: new TextDecoder().decode(kp.getSeed()), publicKey: kp.getPublicKey() }
}

// Reuse existing key files for an idempotent re-onboard; regenerate on --force or
// first run. Returns the keys plus whether they were reused.
async function loadOrGenEd25519(dir: string, id: string, force: boolean): Promise<{ seedB64: string; pubB64: string; reused: boolean }> {
  const seedPath = join(dir, `${id}.seed`), pubPath = join(dir, `${id}.pub`)
  if (!force && existsSync(seedPath) && existsSync(pubPath)) {
    return { seedB64: readFileSync(seedPath, 'utf8').trim(), pubB64: readFileSync(pubPath, 'utf8').trim(), reused: true }
  }
  return { ...(await generateEd25519Keypair()), reused: false }
}
function loadOrGenNkey(dir: string, id: string, force: boolean): { seed: string } {
  const nkeyPath = join(dir, `${id}.nk`)
  if (!force && existsSync(nkeyPath)) return { seed: readFileSync(nkeyPath, 'utf8').trim() }
  return { seed: generateNkeyUser().seed }
}
export const nkeyPublicOf = (seed: string): string => fromSeed(new TextEncoder().encode(seed)).getPublicKey()

// ── Redis pubkey registration (self-service, NX-guarded) ────────────────────

export class PubkeyConflict extends Error {}

export async function registerPubkey(redis: RedisClient, id: string, pubB64: string, force = false): Promise<'created' | 'exists' | 'forced'> {
  const key = PUBKEY_PREFIX + id
  if (force) { await redis.set(key, pubB64); return 'forced' }
  const res = await redis.send('SET', [key, pubB64, 'NX'])
  if (res) return 'created'
  const cur = await redis.get(key)
  if (cur === pubB64) return 'exists' // idempotent re-onboard with the same key
  throw new PubkeyConflict(`agent-id '${id}' already holds a DIFFERENT pubkey at ${key}; use --force to rotate, or choose another id`)
}

// ── NATS authorization snippet ──────────────────────────────────────────────

// Publish perms scoped to the A2A data subjects + ONLY the JetStream API surface
// this agent's bridge needs for its OWN stream. A broad `$JS.API.>` would let a
// compromised A2A nkey manage/purge other streams (e.g. RAMP_ALERTS); these are
// limited to `<stream>` so it can't touch anything else.
export function natsUserBlock(id: string, nkeyPublic: string, stream = 'ALLOYIUM_A2A'): string {
  return `    # a2a agent: ${id}\n` +
    `    { nkey: ${nkeyPublic}, permissions: {\n` +
    `        publish: { allow: [\n` +
    `          "alloyium.a2a.>",\n` +
    `          "$JS.API.STREAM.INFO.${stream}", "$JS.API.STREAM.CREATE.${stream}",\n` +
    `          "$JS.API.CONSUMER.CREATE.${stream}.>", "$JS.API.CONSUMER.DURABLE.CREATE.${stream}.>",\n` +
    `          "$JS.API.CONSUMER.INFO.${stream}.>", "$JS.API.CONSUMER.DELETE.${stream}.>",\n` +
    `          "$JS.API.CONSUMER.MSG.NEXT.${stream}.>",\n` +
    `          "$JS.ACK.${stream}.>"\n` +
    `        ] }\n` +
    `        subscribe: { allow: ["alloyium.a2a.>", "_INBOX.>"] }\n` +
    `    }}`
}
export function natsAuthSnippet(userBlocks: string[]): string {
  return `authorization {\n  users: [\n${userBlocks.join(',\n')}\n  ]\n}\n`
}

// ── files ───────────────────────────────────────────────────────────────────

export function writeAgentFiles(dir: string, id: string, opts: { ed25519SeedB64: string; ed25519PubB64: string; nkeySeed?: string; transport: 'nkey' | 'none'; natsUrl?: string; redisUrl?: string; stream?: string }): { seedPath: string; pubPath: string; nkeyPath: string | null; envPath: string } {
  mkdirSync(dir, { recursive: true })
  const seedPath = resolve(join(dir, `${id}.seed`))
  const pubPath = resolve(join(dir, `${id}.pub`))
  const envPath = resolve(join(dir, `${id}.a2a.env`))
  writeFileSync(seedPath, opts.ed25519SeedB64 + '\n', { mode: 0o600 })
  writeFileSync(pubPath, opts.ed25519PubB64 + '\n', { mode: 0o644 }) // public, not secret
  let nkeyPath: string | null = null
  if (opts.transport === 'nkey') {
    if (!opts.nkeySeed) throw new Error('nkey transport requires an nkey seed')
    nkeyPath = resolve(join(dir, `${id}.nk`))
    writeFileSync(nkeyPath, opts.nkeySeed + '\n', { mode: 0o600 })
  }
  const lines = ['A2A_ENABLED=1', `A2A_AGENT_ID=${id}`, 'A2A_SIG_ALG=ed25519', `A2A_SIGNING_KEY=${seedPath}`]
  if (opts.transport === 'none') lines.push('A2A_TRANSPORT_AUTH=none') // anonymous NATS connect; signing stays ON
  else lines.push(`A2A_NKEY=${nkeyPath}`)
  if (opts.stream && opts.stream !== 'ALLOYIUM_A2A') lines.push(`A2A_STREAM=${opts.stream}`)
  if (opts.natsUrl) lines.push(`NATS_URL=${opts.natsUrl}`)
  if (opts.redisUrl) lines.push(`REDIS_URL=${opts.redisUrl}`)
  writeFileSync(envPath, lines.join('\n') + '\n', { mode: 0o600 })
  for (const p of [seedPath, envPath, ...(nkeyPath ? [nkeyPath] : [])]) chmodSync(p, 0o600) // vs umask
  return { seedPath, pubPath, nkeyPath, envPath }
}

// ── verify (the same check a receiver performs) ─────────────────────────────

export async function verifyRoundTrip(redis: RedisClient, id: string, seedB64: string): Promise<boolean> {
  const signKey = await importEd25519Seed(unb64(seedB64))
  const env: Envelope = { v: 1, id: 'onboard-verify', from: id, to: 'onboard-check', type: 'msg', ts: new Date().toISOString(), body: 'verify', alg: 'ed25519' }
  env.sig = await signEnvelope(env, 'ed25519', signKey)
  const storedB64 = await redis.get(PUBKEY_PREFIX + id)
  if (!storedB64) return false
  return verifyEnvelope(env, await importEd25519Pub(unb64(storedB64.trim())), 'ed25519')
}

// ── orchestrator ────────────────────────────────────────────────────────────

export type OnboardResult = {
  id: string
  pubkeyB64: string
  nkeyPublic: string | null            // null when transport='none' (no NATS identity)
  pubkeyStatus: 'created' | 'exists' | 'forced'
  reusedKeys: boolean
  transport: 'nkey' | 'none'
  files: { seedPath: string; pubPath: string; nkeyPath: string | null; envPath: string }
  natsUserBlock: string | null          // null when transport='none' (no server step)
  verified: boolean | null
}

export async function onboard(opts: { id: string; dir: string; redis: RedisClient; force?: boolean; verify?: boolean; natsUrl?: string; redisUrl?: string; stream?: string; transport?: 'nkey' | 'none' }): Promise<OnboardResult> {
  if (!ID_RE.test(opts.id)) throw new Error(`invalid agent-id '${opts.id}' (must match ^[a-z0-9-]{1,64}$)`)
  const force = opts.force ?? false
  const stream = opts.stream ?? 'ALLOYIUM_A2A'
  const transport = opts.transport ?? 'nkey'
  // Reuse existing key files for an idempotent re-onboard; --force rotates.
  const ed = await loadOrGenEd25519(opts.dir, opts.id, force)
  // transport='none' (Option A): no NATS identity at all — push-button, no server step.
  const nk = transport === 'nkey' ? loadOrGenNkey(opts.dir, opts.id, force) : undefined
  const pubkeyStatus = await registerPubkey(opts.redis, opts.id, ed.pubB64, force)
  const files = writeAgentFiles(opts.dir, opts.id, { ed25519SeedB64: ed.seedB64, ed25519PubB64: ed.pubB64, nkeySeed: nk?.seed, transport, natsUrl: opts.natsUrl, redisUrl: opts.redisUrl, stream })
  const verified = opts.verify === false ? null : await verifyRoundTrip(opts.redis, opts.id, ed.seedB64)
  const nkeyPublic = nk ? nkeyPublicOf(nk.seed) : null
  return {
    id: opts.id, pubkeyB64: ed.pubB64, nkeyPublic, pubkeyStatus, reusedKeys: ed.reused, transport, files,
    natsUserBlock: nkeyPublic ? natsUserBlock(opts.id, nkeyPublic, stream) : null, verified,
  }
}

// ── CLI ─────────────────────────────────────────────────────────────────────

if (import.meta.main) {
  const usage = 'usage: bun onboard.ts <agent-id> [--transport none|nkey] [--dir <out>] [--redis <url>] [--nats <url>] [--stream <name>] [--force] [--no-verify]'
  const BOOL = new Set(['force', 'no-verify'])
  const VALUE = new Set(['dir', 'redis', 'nats', 'stream', 'transport'])
  // Parse option/value pairs first so a value can't be mistaken for the agent-id
  // (e.g. `--dir a2a scout-1` must NOT pick `a2a` as the id).
  const flags: Record<string, string | boolean> = {}
  const positionals: string[] = []
  const argv = process.argv.slice(2)
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a.startsWith('--')) {
      const name = a.slice(2)
      if (BOOL.has(name)) flags[name] = true
      else if (VALUE.has(name)) {
        const v = argv[i + 1]
        if (v === undefined || v.startsWith('--')) { console.error(`option --${name} needs a value\n${usage}`); process.exit(2) }
        flags[name] = v; i++
      } else { console.error(`unknown option --${name}\n${usage}`); process.exit(2) }
    } else positionals.push(a)
  }
  if (positionals.length !== 1) { console.error(`expected exactly one <agent-id>${positionals.length > 1 ? ` (got ${positionals.length}: ${positionals.join(', ')})` : ''}\n${usage}`); process.exit(2) }
  const id = positionals[0]
  const dir = (flags.dir as string) ?? './a2a'
  const redisUrl = (flags.redis as string) ?? process.env.REDIS_URL ?? 'redis://redis:6379'
  const natsUrl = (flags.nats as string) ?? process.env.NATS_URL ?? 'nats://nats:4222'
  const stream = (flags.stream as string) ?? process.env.A2A_STREAM ?? 'ALLOYIUM_A2A'
  const transport = ((flags.transport as string) ?? 'nkey') as 'nkey' | 'none'
  if (transport !== 'nkey' && transport !== 'none') { console.error(`--transport must be 'none' or 'nkey'\n${usage}`); process.exit(2) }
  const redis = new RedisClient(redisUrl)
  try {
    const r = await onboard({ id, dir, redis, force: flags.force === true, verify: flags['no-verify'] !== true, natsUrl, redisUrl, stream, transport })
    console.log(`\n✓ onboarded a2a agent '${r.id}'${r.reusedKeys ? ' (reused existing keys)' : ''}  [transport: ${r.transport}]`)
    console.log(`  ed25519 pubkey : ${r.pubkeyB64}  (Redis: ${r.pubkeyStatus})`)
    console.log(`  env            : ${r.files.envPath}`)
    console.log(`  ed25519 seed   : ${r.files.seedPath}  (0600, keep private)`)
    if (r.transport === 'nkey') console.log(`  nats nkey      : ${r.nkeyPublic}\n  nats nkey seed : ${r.files.nkeyPath}  (0600, keep private)`)
    console.log(`  verify         : ${r.verified === null ? 'skipped' : r.verified ? 'OK — seed signs, Redis pubkey verifies' : 'FAILED'}`)
    if (r.transport === 'none') {
      console.log(`\n  push-button — no NATS server step. Run the bridge:`)
      console.log(`    set -a; source ${r.files.envPath}; set +a; bun ./webhook.ts\n`)
    } else {
      console.log(`\n  1) add this user to nats-server.conf authorization{}, then \`nats-server --signal reload\`:\n`)
      console.log(natsAuthSnippet([r.natsUserBlock!]))
      console.log(`  2) run the bridge:  set -a; source ${r.files.envPath}; set +a; bun ./webhook.ts\n`)
    }
    if (r.verified === false) process.exit(1)
  } catch (e) {
    console.error(`✗ onboard failed: ${e instanceof Error ? e.message : String(e)}`)
    process.exit(1)
  } finally {
    try { (redis as any).close?.() } catch {}
  }
}
