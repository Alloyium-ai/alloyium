// Portability / remote-safety tests for the `fusion-ask` CLI wrapper. We run the real
// script with a STUB `bun` (and `redis-cli`) on PATH that just echoes the argv + the env
// fusion-ask sets — so we verify the wiring (repo path, seed, NATS/REDIS) WITHOUT a bus.
// This is the "CLI portable/remote-safe" half of the fusion fix (Bug 2).
import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { mkdtempSync, writeFileSync, chmodSync, rmSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const REPO = join(import.meta.dir, '..')
const ASK = join(REPO, 'fusion-ask')

let bindir: string
beforeAll(() => {
  bindir = mkdtempSync(join(tmpdir(), 'fusion-ask-bin-'))
  // stub `bun`: print the resolved argv + the env vars fusion-ask exports, then exit —
  // instead of really launching fusion_client.ts against a live bus.
  writeFileSync(join(bindir, 'bun'), `#!/usr/bin/env bash
echo "BUN_ARGV: $*"
echo "A2A_SIGNING_KEY=$A2A_SIGNING_KEY"
echo "A2A_AGENT_ID=$A2A_AGENT_ID"
echo "NATS_URL=$NATS_URL"
echo "REDIS_URL=$REDIS_URL"
exit 0
`)
  chmodSync(join(bindir, 'bun'), 0o755)
  // stub redis-cli so the presence-clear never needs a real server. fusion-ask silences
  // redis-cli with `>/dev/null 2>&1`, so we log its argv to $RC_LOG (a separate fd the
  // redirection doesn't touch) to assert it targets the CONFIGURED host:port.
  writeFileSync(join(bindir, 'redis-cli'), `#!/usr/bin/env bash\nif [ -n "$RC_LOG" ]; then echo "$*" >> "$RC_LOG"; fi\nexit 0\n`)
  chmodSync(join(bindir, 'redis-cli'), 0o755)
})
afterAll(() => { rmSync(bindir, { recursive: true, force: true }) })

function runAsk(env: Record<string, string>, args: string[] = ['a question']) {
  const r = Bun.spawnSync(['bash', ASK, ...args], {
    // Minimal PATH with our stubs first; keep coreutils available via the real PATH tail.
    env: { PATH: `${bindir}:/usr/bin:/bin`, ...env },
    stdout: 'pipe', stderr: 'pipe',
  })
  return { out: r.stdout.toString(), err: r.stderr.toString(), code: r.exitCode }
}

describe('fusion-ask repo path resolution', () => {
  test('$CHANNELS overrides the repo path (no hard-coded absolute path)', () => {
    const { out, code } = runAsk({ CHANNELS: '/tmp/fake-ch' })
    expect(code).toBe(0)
    expect(out).toContain('BUN_ARGV: /tmp/fake-ch/fusion_client.ts')
  })

  test('defaults to the script\'s own directory when $CHANNELS is unset (portable checkout)', () => {
    const { out, code } = runAsk({})
    expect(code).toBe(0)
    expect(out).toContain(`BUN_ARGV: ${REPO}/fusion_client.ts`)
  })
})

describe('fusion-ask seed configurability', () => {
  test('seed defaults to $CH/a2a/fusion.seed', () => {
    const { out } = runAsk({ CHANNELS: '/tmp/fake-ch' })
    expect(out).toContain('A2A_SIGNING_KEY=/tmp/fake-ch/a2a/fusion.seed')
  })
  test('$FUSION_SEED overrides the seed path', () => {
    const { out } = runAsk({ CHANNELS: '/tmp/fake-ch', FUSION_SEED: '/secrets/custom.seed' })
    expect(out).toContain('A2A_SIGNING_KEY=/secrets/custom.seed')
  })
})

describe('fusion-ask bus address is env-configurable (remote-safe)', () => {
  test('honors NATS_URL / REDIS_URL pointing at host-1 from a remote host', () => {
    const { out } = runAsk({ NATS_URL: 'nats://host-1.internal:4222', REDIS_URL: 'redis://host-1.internal:6379' })
    expect(out).toContain('NATS_URL=nats://host-1.internal:4222')
    expect(out).toContain('REDIS_URL=redis://host-1.internal:6379')
  })
  test('defaults to the neutral compose service-DNS bus address when unset', () => {
    const { out } = runAsk({})
    expect(out).toContain('NATS_URL=nats://nats:4222')
    expect(out).toContain('REDIS_URL=redis://redis:6379')
  })
  test('presence-clear targets the CONFIGURED redis host:port, not a hard-coded one', () => {
    const rclog = join(bindir, `rc-${process.hrtime.bigint()}.log`)
    const { code } = runAsk({ REDIS_URL: 'redis://host-1.internal:6380', RC_LOG: rclog })
    expect(code).toBe(0)
    const logged = readFileSync(rclog, 'utf8')
    expect(logged).toContain('-h host-1.internal')
    expect(logged).toContain('-p 6380')
  })
})

describe('fusion-ask agent-id', () => {
  test('defaults to fusion, overridable via A2A_AGENT_ID', () => {
    expect(runAsk({}).out).toContain('A2A_AGENT_ID=fusion')
    expect(runAsk({ A2A_AGENT_ID: 'fusion-2' }).out).toContain('A2A_AGENT_ID=fusion-2')
  })
})

describe('fusion-ask no longer hard-codes the old absolute path', () => {
  test('source contains no hard-coded home-dir checkout literal', () => {
    // Build the forbidden literal from fragments so this guard itself stays free of
    // any internal host path. fusion-ask must resolve its repo from $CHANNELS / the
    // script dir, never a baked-in absolute home path.
    const forbidden = ['', 'home', 'atc', 'git', 'alloyium'].join('/')
    expect(readFileSync(ASK, 'utf8')).not.toContain(forbidden)
  })
})
