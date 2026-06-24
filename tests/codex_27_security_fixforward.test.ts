// #38 — #27 codex-dev-worker security fix-forward. Covers the FIX-1a signing accessor and the
// FIX-2 protected-path denylist at the unit level. FIX-1c (validateWorkspaceWriteJob signing gate)
// is in codex_gateway_authz.test.ts; the FIX-2 end-to-end runBuildGuard path is in
// codex_build_tooling.test.ts. See brain ops-specs/eval/2026-06-17-pr5-27-opus-reattestation.
import { describe, expect, test } from 'bun:test'
import { A2AChannel } from '../a2a-channel.ts'
import { DEFAULT_PROTECTED_DENY_PATTERNS } from '../codex_build_guard.ts'

describe('[#38 HIGH] A2AChannel.signingEnabled accessor', () => {
  const mk = (opts: any) => new A2AChannel(async () => {}, { enabled: false, agentId: 'sig-test', ...opts })

  test('signing is OFF only under the full dev bypass (devNoAuth)', () => {
    expect(mk({ devNoAuth: true }).signingEnabled).toBe(false)
  })
  test('signing is ON by default (no bypass)', () => {
    expect(mk({}).signingEnabled).toBe(true)
    expect(mk({ devNoAuth: false }).signingEnabled).toBe(true)
  })
  test('transportAuth=none keeps signing ON (only the full bypass disables verification)', () => {
    // The re-attestation notes A2A_TRANSPORT_AUTH=none keeps signing on; the write risk is signing-off.
    expect(mk({ transportAuth: 'none' }).signingEnabled).toBe(true)
  })
})

describe('[#38 MEDIUM] protected-path denylist covers underscore forms (unit)', () => {
  const isProtected = (p: string) => DEFAULT_PROTECTED_DENY_PATTERNS.some((re) => re instanceof RegExp && re.test(p))

  test('underscore-separated core forms are protected', () => {
    for (const p of ['secrets_store.ts', 'credentials_loader.ts', 'admin_control.ts', 'keystore_service.ts', 'signing_key.ts', 'src/secrets_store.ts']) {
      expect(isProtected(p)).toBe(true)
    }
  })
  test('slash/dot forms remain protected', () => {
    for (const p of ['credentials/x.ts', 'secrets.ts', 'admin/control.ts', 'signing.ts', 'keystore.ts']) {
      expect(isProtected(p)).toBe(true)
    }
  })
  test('benign lookalikes are NOT protected (no over-match)', () => {
    for (const p of ['secretsmanager.ts', 'administrate.ts', 'credentialsx.ts', 'keystored.ts', 'src/designing.ts']) {
      expect(isProtected(p)).toBe(false)
    }
  })
})
