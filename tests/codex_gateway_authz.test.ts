import { describe, expect, test } from 'bun:test'
import { GatewayThreadCache, validateWorkspaceWriteJob, approvalPolicyAllowed, normalizeCodexSandboxMode, resolveCodexExecutionSandbox, describeGatewayError, defaultPlainRequestApprovalPolicy, buildPeerInboxContext, codexMcpElicitationResult, codexDetachedThreadOverrides, codexHttpGatewayStartAllowed, isCodexHttpLoopbackBind } from '../codex_gateway.ts'

describe('codex gateway workspace-write preflight', () => {
  test('formats structured app-server errors without [object Object]', () => {
    expect(describeGatewayError({ code: -32603, message: 'required MCP servers failed to initialize' }))
      .toBe('required MCP servers failed to initialize')
    expect(describeGatewayError({ code: -1, data: { detail: 'x' } }))
      .toBe('{"code":-1,"data":{"detail":"x"}}')
  })

  test('rejects workspace-write when approval_policy is not never', () => {
    expect(validateWorkspaceWriteJob({ thread_key: 'tk-1' }, 'on-request', true)).toEqual({
      ok: false,
      reason: 'write-unauthorized',
      detail: 'write-approval-policy-not-never',
    })
  })

  test('rejects workspace-write with missing or empty thread_key', () => {
    expect(validateWorkspaceWriteJob({}, 'never', true)).toEqual({
      ok: false,
      reason: 'write-unauthorized',
      detail: 'write-thread-key-required',
    })
    expect(validateWorkspaceWriteJob({ thread_key: '   ' }, 'never', true)).toEqual({
      ok: false,
      reason: 'write-unauthorized',
      detail: 'write-thread-key-required',
    })
  })

  test('accepts workspace-write preflight only with never approval and non-empty thread_key', () => {
    expect(validateWorkspaceWriteJob({ thread_key: '  thread-a  ' }, 'never', true)).toEqual({
      ok: true,
      threadKey: 'thread-a',
    })
  })

  test('[#38 HIGH] REFUSES workspace-write when A2A signing is OFF (sender unverifiable)', () => {
    // A fully-valid write job is still refused under signing-off — env.from would be spoofable.
    expect(validateWorkspaceWriteJob({ thread_key: 'thread-a' }, 'never', false)).toEqual({
      ok: false,
      reason: 'write-unauthorized',
      detail: 'signing-off',
    })
  })

  test('[#38 HIGH] signing-off takes precedence over the other write-invariant failures', () => {
    // Even with a bad approval_policy AND missing thread_key, signing-off is reported first.
    expect(validateWorkspaceWriteJob({}, 'on-request', false)).toEqual({
      ok: false,
      reason: 'write-unauthorized',
      detail: 'signing-off',
    })
  })

  test('[#38 P1] write-enabled gateway forbids non-never approval_policy (self-approval escalation)', () => {
    // On a write-enabled gateway, only 'never' is allowed (approvals route back to the requester).
    expect(approvalPolicyAllowed('never', true)).toBe(true)
    expect(approvalPolicyAllowed('on-request', true)).toBe(false)
    expect(approvalPolicyAllowed('on-failure', true)).toBe(false)
    expect(approvalPolicyAllowed('untrusted', true)).toBe(false)
    // A read-only gateway (write disabled) may honor the requested policy.
    expect(approvalPolicyAllowed('on-request', false)).toBe(true)
    expect(approvalPolicyAllowed('never', false)).toBe(true)
  })

  test('plain peer requests default to never on write-enabled gateways', () => {
    expect(defaultPlainRequestApprovalPolicy(true)).toBe('never')
    expect(defaultPlainRequestApprovalPolicy(false)).toBe('on-request')
    expect(approvalPolicyAllowed(defaultPlainRequestApprovalPolicy(true), true)).toBe(true)
  })

  test('renders async peer replies as bounded next-turn context', () => {
    const out = buildPeerInboxContext([
      { from: 'codex-gw', type: 'reply', id: 'm1', corr: 'req1', ts: '2026-06-19T00:00:00.000Z', body: 'x'.repeat(20) },
    ], 8)

    expect(out).toContain('Recent A2A inbox events')
    expect(out).toContain('from=codex-gw type=reply id=m1 corr=req1')
    expect(out).toContain('body: xxxxxxxx')
    expect(out).toContain('[body truncated 20->8B]')
  })

  test('auto-accepts only gateway-owned A2A MCP tool elicitations', () => {
    expect(codexMcpElicitationResult({
      method: 'mcpServer/elicitation/request',
      params: {
        serverName: 'a2a_tools',
        _meta: { codex_approval_kind: 'mcp_tool_call', tool_description: 'List peer agents' },
      },
    })).toEqual({ action: 'accept', content: {} })

    expect(codexMcpElicitationResult({
      method: 'mcpServer/elicitation/request',
      params: {
        serverName: 'codex_apps',
        _meta: { codex_approval_kind: 'mcp_tool_call' },
      },
    })).toEqual({ action: 'decline' })

    expect(codexMcpElicitationResult({
      method: 'mcpServer/elicitation/request',
      params: {
        serverName: 'a2a_tools',
        _meta: { codex_approval_kind: 'something_else' },
      },
    })).toEqual({ action: 'decline' })
  })

  test('detached codex threads disable interactive user input and route blockers over A2A', () => {
    const overrides = codexDetachedThreadOverrides()

    expect(overrides.config.tools.request_user_input).toBe(false)
    expect(overrides.developerInstructions).toContain('there is no interactive user')
    expect(overrides.developerInstructions).toContain('Never use request_user_input')
    expect(overrides.developerInstructions).toContain('send an A2A request to agent-1')
  })

  test('normalizes Codex no-sandbox aliases for containerized gateways', () => {
    expect(normalizeCodexSandboxMode('yolo')).toBe('danger-full-access')
    expect(normalizeCodexSandboxMode('no-sandbox')).toBe('danger-full-access')
    expect(normalizeCodexSandboxMode('workspace-write')).toBe('workspace-write')
    expect(normalizeCodexSandboxMode('not-a-mode')).toBeNull()
  })

  test('can translate accepted A2A jobs to a Codex execution sandbox', () => {
    expect(resolveCodexExecutionSandbox('read-only')).toBe('read-only')
    expect(resolveCodexExecutionSandbox('workspace-write')).toBe('workspace-write')
    expect(resolveCodexExecutionSandbox('workspace-write', { workspaceWriteSandbox: 'yolo' })).toBe('danger-full-access')
    expect(resolveCodexExecutionSandbox('workspace-write', { defaultSandbox: 'danger-full-access' })).toBe('danger-full-access')
    expect(resolveCodexExecutionSandbox('read-only', { defaultSandbox: 'danger-full-access' })).toBe('danger-full-access')
    expect(resolveCodexExecutionSandbox('read-only', { workspaceWriteSandbox: 'danger-full-access' })).toBe('read-only')
  })

  test('HTTP realtime gateway is loopback-open but requires a token for non-loopback binds', () => {
    expect(isCodexHttpLoopbackBind('127.0.0.1')).toBe(true)
    expect(isCodexHttpLoopbackBind('localhost')).toBe(true)
    expect(isCodexHttpLoopbackBind('0.0.0.0')).toBe(false)
    expect(codexHttpGatewayStartAllowed('127.0.0.1', '')).toBe(true)
    expect(codexHttpGatewayStartAllowed('0.0.0.0', '')).toBe(false)
    expect(codexHttpGatewayStartAllowed('0.0.0.0', 'token')).toBe(true)
  })
})

describe('codex gateway thread cache isolation', () => {
  test('read-only jobs do not reuse a workspace-write thread_key', () => {
    const cache = new GatewayThreadCache({ ttlMs: 60_000, maxEntries: 10, now: () => 1000 })

    cache.set('same-thread', 'write-thread-id', { sandbox: 'workspace-write', cwd: '/work/a' })

    expect(cache.get('same-thread', { sandbox: 'read-only', cwd: '/work/a' })).toBeNull()

    cache.set('same-thread', 'read-thread-id', { sandbox: 'read-only', cwd: '/work/a' })
    expect(cache.get('same-thread', { sandbox: 'read-only', cwd: '/work/a' })).toBe('read-thread-id')
    expect(cache.get('same-thread', { sandbox: 'workspace-write', cwd: '/work/a' })).toBeNull()
  })

  test('workspace-write jobs do not reuse a warm thread when cwd changes', () => {
    const cache = new GatewayThreadCache({ ttlMs: 60_000, maxEntries: 10, now: () => 1000 })

    cache.set('same-thread', 'write-a', { sandbox: 'workspace-write', cwd: '/work/a' })

    expect(cache.get('same-thread', { sandbox: 'workspace-write', cwd: '/work/b' })).toBeNull()
  })

  test('timed-out turns can evict their warm thread_key', () => {
    const cache = new GatewayThreadCache({ ttlMs: 60_000, maxEntries: 10, now: () => 1000 })

    cache.set('timed-out-thread', 'thread-id', { sandbox: 'read-only', cwd: '/tmp' })

    expect(cache.delete('timed-out-thread')).toBe(true)
    expect(cache.get('timed-out-thread', { sandbox: 'read-only', cwd: '/tmp' })).toBeNull()
    expect(cache.delete('timed-out-thread')).toBe(false)
  })

  test('thread cache expires old entries and evicts least recently used entries', () => {
    let now = 0
    const cache = new GatewayThreadCache({ ttlMs: 100, maxEntries: 2, now: () => now })

    cache.set('expired', 'thread-expired', { sandbox: 'read-only', cwd: '/tmp' })
    now = 101
    expect(cache.get('expired', { sandbox: 'read-only', cwd: '/tmp' })).toBeNull()
    expect(cache.size()).toBe(0)

    now = 200
    cache.set('a', 'thread-a', { sandbox: 'read-only', cwd: '/tmp/a' })
    cache.set('b', 'thread-b', { sandbox: 'read-only', cwd: '/tmp/b' })
    expect(cache.get('a', { sandbox: 'read-only', cwd: '/tmp/a' })).toBe('thread-a')
    cache.set('c', 'thread-c', { sandbox: 'read-only', cwd: '/tmp/c' })

    expect(cache.get('b', { sandbox: 'read-only', cwd: '/tmp/b' })).toBeNull()
    expect(cache.get('a', { sandbox: 'read-only', cwd: '/tmp/a' })).toBe('thread-a')
    expect(cache.get('c', { sandbox: 'read-only', cwd: '/tmp/c' })).toBe('thread-c')
  })
})
