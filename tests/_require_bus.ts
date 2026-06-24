// Gate enforcement for the bus-dependent test suites.
//
// The A2A safety-invariant tests (e.g. "stop one session, the others survive over the
// SHARED connection") are guarded `describe.skipIf(!available)` so they don't error in a
// dev checkout with no NATS/Redis. But in the GATE/CI job that is a trap: with no live
// bus those suites self-skip to GREEN while proving NOTHING (gate review GPT5.5-3 / §6
// condition 3). So the gate job sets A2A_TEST_REQUIRE_BUS=1, and this helper turns a
// missing bus into a HARD FAILURE instead of a silent skip.
//
// Usage (in each bus-dependent test file, right after it probes `available`):
//   import { requireBus } from './_require_bus.ts'
//   requireBus(available, 'a2a-core tests', { NATS_URL, REDIS_URL })
import { test, expect } from 'bun:test'

const truthy = (v: string | undefined): boolean => v === '1' || v === 'true' || v === 'yes'

/** When A2A_TEST_REQUIRE_BUS is set and the bus is unreachable, register a FAILING test
 *  (rather than letting the suite skip to green). No-op when the bus is available or the
 *  flag is unset (dev convenience: skips are still allowed). */
export function requireBus(available: boolean, suite: string, urls: { NATS_URL: string; REDIS_URL: string }): void {
  if (available || !truthy(process.env.A2A_TEST_REQUIRE_BUS)) return
  test(`[gate] live NATS+Redis are MANDATORY (A2A_TEST_REQUIRE_BUS=1) but unreachable — ${suite}`, () => {
    // Fail loudly with the endpoints so CI shows exactly what the gate could not reach.
    expect.unreachable(
      `gate requires a live bus for "${suite}" but it was unreachable ` +
      `(NATS=${urls.NATS_URL}, REDIS=${urls.REDIS_URL}). Provision the bus for the gate, ` +
      `or unset A2A_TEST_REQUIRE_BUS to permit dev skips.`,
    )
  })
}
