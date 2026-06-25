import { describe, expect, test } from 'bun:test'
import { computeFleet, normalizeHost, parseHostAliases, parseKnownHosts, primaryAgentId } from '../a2a_portal_hosts.ts'

describe('a2a portal host helpers', () => {
  test('normalizes configured aliases and container fallback', () => {
    const knownHosts = parseKnownHosts('gpubox,srv01')
    const aliases = parseHostAliases('dev01=gpubox srv=srv01 ignored=missing', knownHosts)

    expect(normalizeHost('dev01', 'codex-gw', { knownHosts, aliases }).logical_host).toBe('gpubox')
    expect(normalizeHost('7b78298e1aab', 'codex-gw', { knownHosts, containerHostFallback: 'gpubox' })).toEqual({
      logical_host: 'gpubox',
      host_source: 'container-id-fallback',
    })
  })

  test('infers remote host from agent id before raw container fallback', () => {
    const knownHosts = parseKnownHosts('local,gpubox,srv01')
    expect(normalizeHost('7b78298e1aab', 'host-ops-gw-e2e-srv01', { knownHosts, containerHostFallback: 'gpubox' }).logical_host).toBe('srv01')
  })

  test('groups fleet totals by physical server and primary team', () => {
    const knownHosts = parseKnownHosts('gpubox,srv01')
    const fleet = computeFleet([
      { id: 'codex-gw', host: 'aaaaaaaaaaaa', ttl: 70 },
      { id: 'codex-gw-sub-review-abc', host: 'bbbbbbbbbbbb', ttl: 70 },
      { id: 'host-ops-gw-e2e-srv01', host: 'srv01', ttl: 70 },
      { id: 'offline-peer', host: 'srv01', ttl: -2 },
    ], { knownHosts, containerHostFallback: 'gpubox' }, {
      messages: [
        { from: 'a2a-portal', to: 'codex-gw', t: 1000 },
        { from: 'codex-gw', to: 'a2a-portal', t: 2000 },
      ],
    })

    expect(primaryAgentId('codex-gw-sub-review-abc')).toBe('codex-gw')
    expect(fleet.stats.servers).toBe(2)
    expect(fleet.stats.online_agents).toBe(3)
    expect(fleet.stats.offline_agents).toBe(1)
    expect(fleet.stats.teams).toBe(1)
    expect(fleet.stats.messages).toBe(2)
    expect(fleet.servers.map(s => [s.logical_host, s.agents])).toEqual([['gpubox', 2], ['srv01', 1]])
  })
})
