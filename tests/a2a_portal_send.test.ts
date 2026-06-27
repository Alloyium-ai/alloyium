import { describe, expect, test } from 'bun:test'
import { buildPortalDefaultCwd, buildPortalRealtimeStreamTopic, buildPortalSendArgs, buildPortalThreadKey, formatPortalRenderedBody, isCodexJobRecipient, isSelfPortalRecipient, normalizePortalRecipient, wrapPlainCodexRealtimeInput, wrapPlainCodexRequest } from '../a2a_portal_send.ts'

describe('a2a portal send helpers', () => {
  test('normalizes dm and topic recipients from UI notation', () => {
    expect(normalizePortalRecipient('@Codex-GW')).toBe('codex-gw')
    expect(normalizePortalRecipient('#Ops-Room')).toBe('topic:ops-room')
    expect(normalizePortalRecipient('topic:Builds')).toBe('topic:builds')
    expect(normalizePortalRecipient('agent-1')).toBe('agent-1')
    expect(normalizePortalRecipient('   ')).toBeNull()
  })

  test('builds safe a2a_send args and rejects malformed inputs', () => {
    expect(buildPortalSendArgs({ to: '@agent-1', body: 'hello' })).toEqual({
      ok: true,
      args: { to: 'agent-1', body: 'hello', type: 'msg' },
      sendMode: 'one-off',
      chatContext: null,
    })
    expect(buildPortalSendArgs({ to: '#room', body: 'hello', type: 'request', thread: 't1', send_mode: 'chat', chat_context: 'C-Reset_01' })).toEqual({
      ok: true,
      args: { to: 'topic:room', body: 'hello', type: 'request', thread: 't1' },
      sendMode: 'chat',
      chatContext: 'c-reset-01',
    })
    expect(buildPortalSendArgs({ to: '@agent-1', body: 'reply', type: 'reply' })).toEqual({ ok: false, error: 'bad_corr' })
    expect(buildPortalSendArgs({ to: '@agent-1', body: '   ' })).toEqual({ ok: false, error: 'empty_body' })
    expect(buildPortalSendArgs({ to: '@agent-1', body: 'x', type: 'bad' })).toEqual({ ok: false, error: 'bad_type' })
    expect(buildPortalSendArgs({ to: '@agent-1', body: 'x', send_mode: 'sticky' })).toEqual({ ok: false, error: 'bad_send_mode' })
    expect(buildPortalSendArgs({ to: '@agent-1', body: 'x', chat_context: '!' })).toEqual({ ok: false, error: 'bad_chat_context' })
  })

  test('detects self recipients after UI normalization', () => {
    expect(isSelfPortalRecipient('@A2A-Portal', 'a2a-portal')).toBe(true)
    expect(isSelfPortalRecipient('a2a-portal', 'a2a-portal')).toBe(true)
    expect(isSelfPortalRecipient('@codex-gw', 'a2a-portal')).toBe(false)
    expect(isSelfPortalRecipient('#a2a-portal', 'a2a-portal')).toBe(false)
  })

  test('wraps plain codex-gw requests in the codex job contract', () => {
    const wrapped = wrapPlainCodexRequest({ to: 'codex-gw', type: 'request', body: 'say hi' }, 'job-1')
    expect(wrapped.to).toBe('codex-gw')
    expect(wrapped.type).toBe('request')
    expect(JSON.parse(wrapped.body)).toMatchObject({
      schema: 'codex.job.request.v1',
      job_id: 'job-1',
      input: [{ type: 'text', text: 'say hi' }],
      sandbox: 'read-only',
      approval_policy: 'never',
      cwd: '/tmp',
    })
    expect(JSON.parse(wrapped.body).thread_key).toBeUndefined()

    const chatWrapped = wrapPlainCodexRequest({ to: 'codex-gw', type: 'request', body: 'say hi' }, 'job-chat', { threadKey: 'portal:chat:a2a-portal:codex-gw' })
    expect(JSON.parse(chatWrapped.body)).toMatchObject({
      schema: 'codex.job.request.v1',
      job_id: 'job-chat',
      thread_key: 'portal:chat:a2a-portal:codex-gw',
    })

    const body = JSON.stringify({ schema: 'codex.job.request.v1', job_id: 'existing' })
    expect(wrapPlainCodexRequest({ to: 'codex-gw', type: 'request', body }, 'job-2').body).toBe(body)
    expect(wrapPlainCodexRequest({ to: 'agent-1', type: 'request', body: 'say hi' }, 'job-3').body).toBe('say hi')
    expect(wrapPlainCodexRequest({ to: 'codex-rt-gw-2', type: 'request', body: 'say hi' }, 'job-rt').body).toBe('say hi')
  })

  test('wraps chat-mode codex requests in the realtime session contract', () => {
    const wrapped = wrapPlainCodexRealtimeInput(
      { to: 'codex-gw', type: 'request', body: 'say hi' },
      {
        sessionId: 'portal:chat:a2a-portal:codex-gw',
        threadKey: 'portal:chat:a2a-portal:codex-gw',
        cwd: '/app',
        streamTopic: 'portal-rt-a2a-portal-codex-gw',
      },
    )
    expect(wrapped.to).toBe('codex-gw')
    expect(wrapped.type).toBe('request')
    expect(JSON.parse(wrapped.body)).toMatchObject({
      schema: 'codex.session.input.v1',
      session_id: 'portal:chat:a2a-portal:codex-gw',
      thread_key: 'portal:chat:a2a-portal:codex-gw',
      input: [{ type: 'text', text: 'say hi' }],
      mode: 'auto',
      sandbox: 'read-only',
      approval_policy: 'never',
      cwd: '/app',
      stream_topic: 'portal-rt-a2a-portal-codex-gw',
    })

    const existing = JSON.stringify({ schema: 'codex.turn.steer.v1', session_id: 's1', text: 'follow up' })
    expect(wrapPlainCodexRealtimeInput({ to: 'codex-gw', type: 'request', body: existing }, { sessionId: 's1' }).body).toBe(existing)
    expect(wrapPlainCodexRealtimeInput({ to: 'agent-1', type: 'request', body: 'say hi' }, { sessionId: 's1' }).body).toBe('say hi')
    expect(wrapPlainCodexRealtimeInput({ to: 'codex-gw', type: 'request', body: 'say hi' }).body).toBe('say hi')
  })

  test('classifies codex job recipients separately from direct claude peers', () => {
    expect(isCodexJobRecipient('codex-gw')).toBe(true)
    expect(isCodexJobRecipient('host-ops-gw-1')).toBe(true)
    expect(isCodexJobRecipient('codex-rt-gw-2')).toBe(false)
    expect(isCodexJobRecipient('codex-realtime-session-abc123')).toBe(false)
    expect(isCodexJobRecipient('agent-1')).toBe(false)
  })

  test('builds chat thread keys only for codex request chats', () => {
    expect(buildPortalThreadKey({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'a2a-portal', 'chat'))
      .toBe('portal:chat:a2a-portal:host-ops-gw')
    expect(buildPortalThreadKey({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'a2a-portal', 'chat', { chatContext: 'c-reset-01' }))
      .toBe('portal:chat:a2a-portal:host-ops-gw:c-reset-01')
    expect(buildPortalThreadKey({ to: 'host-ops-gw-1', type: 'request', body: 'x' }, 'a2a-portal', 'chat'))
      .toBe('portal:chat:a2a-portal:host-ops-gw-1')
    expect(buildPortalThreadKey({ to: 'codex-gw-sub-bus-test-abc123', type: 'request', body: 'x' }, 'a2a-portal', 'chat'))
      .toBe('portal:chat:a2a-portal:codex-gw-sub-bus-test-abc123')
    expect(buildPortalThreadKey({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'a2a-portal', 'one-off')).toBeNull()
    expect(buildPortalThreadKey({ to: 'host-ops-gw', type: 'msg', body: 'x' }, 'a2a-portal', 'chat')).toBeNull()
    expect(buildPortalThreadKey({ to: 'topic:ops', type: 'request', body: 'x' }, 'a2a-portal', 'chat')).toBeNull()
  })

  test('builds deterministic realtime stream topics for portal chat sessions', () => {
    expect(buildPortalRealtimeStreamTopic('portal:chat:a2a-portal:codex-gw'))
      .toBe('portal-rt-portal-chat-a2a-portal-codex-gw')
    expect(buildPortalRealtimeStreamTopic(null)).toBeNull()
  })

  test('builds mode-aware default cwd for plain codex requests', () => {
    expect(buildPortalDefaultCwd({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'chat'))
      .toBe('/srv/git/alloyium')
    expect(buildPortalDefaultCwd({ to: 'host-ops-gw-1', type: 'request', body: 'x' }, 'chat'))
      .toBe('/srv/remote/alloyium')
    expect(buildPortalDefaultCwd({ to: 'host-ops-gw-1', type: 'request', body: 'x' }, 'chat', { remoteHostOpsCwd: '/remote/repo' }))
      .toBe('/remote/repo')
    expect(buildPortalDefaultCwd({ to: 'codex-gw', type: 'request', body: 'x' }, 'chat'))
      .toBe('/app')
    expect(buildPortalDefaultCwd({ to: 'codex-gw-sub-bus-test-abc123', type: 'request', body: 'x' }, 'chat'))
      .toBe('/app')
    expect(buildPortalDefaultCwd({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'one-off'))
      .toBe('/tmp')
    expect(buildPortalDefaultCwd({ to: 'agent-1', type: 'request', body: 'x' }, 'chat'))
      .toBeNull()
    expect(buildPortalDefaultCwd({ to: 'codex-rt-gw-2', type: 'request', body: 'x' }, 'chat'))
      .toBeNull()
    expect(buildPortalDefaultCwd({ to: 'host-ops-gw', type: 'request', body: 'x' }, 'chat', { hostOpsCwd: '/srv/repo' }))
      .toBe('/srv/repo')
  })

  test('wraps host and launched codex peers in the codex job contract', () => {
    for (const to of ['host-ops-gw', 'host-ops-gw-1', 'codex-gw-pm', 'codex-quality-m1-v2', 'codex-gw-sub-bus-test-abc123']) {
      const wrapped = wrapPlainCodexRequest({ to, type: 'request', body: 'status please' }, 'job-host', { cwd: '/work' })
      expect(JSON.parse(wrapped.body)).toMatchObject({
        schema: 'codex.job.request.v1',
        job_id: 'job-host',
        input: [{ type: 'text', text: 'status please' }],
        approval_policy: 'never',
        cwd: '/work',
      })
    }
  })

  test('extracts readable codex job output for portal rendering', () => {
    const body = JSON.stringify({
      schema: 'codex.job.completed.v1',
      job_id: 'job-1',
      status: 'completed',
      output: 'A2A tools worked.\n\n- brain search\n- peer list',
    })
    expect(formatPortalRenderedBody(body)).toBe('Codex job completed: completed\n\nA2A tools worked.\n\n- brain search\n- peer list')
  })

  test('includes codex output preview and truncation notes', () => {
    const body = JSON.stringify({
      schema: 'codex.job.completed.v1',
      output: 'partial result',
      output_preview: true,
      truncated: true,
      result_ref: 'blob:job-1',
    })
    expect(formatPortalRenderedBody(body)).toBe('Codex job completed: completed\n\npartial result\n\n[preview only; truncated; result_ref: blob:job-1]')
    expect(formatPortalRenderedBody(JSON.stringify({
      schema: 'codex.job.completed.v1',
      output: 'full resolved result',
      result_ref: 'blob:job-1',
      output_preview: false,
    }))).toBe('Codex job completed: completed\n\nfull resolved result\n\n[result_ref: blob:job-1]')
    expect(formatPortalRenderedBody(JSON.stringify({
      schema: 'codex.job.completed.v1',
      output: 'partial result',
      output_preview: true,
      blob_error: 'missing',
      result_ref: 'blob:job-1',
    }))).toBe('Codex job completed: completed\n\npartial result\n\n[preview only; blob missing; result_ref: blob:job-1]')
  })

  test('ignores non-codex bodies and renders codex failures compactly', () => {
    expect(formatPortalRenderedBody('plain text')).toBeNull()
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.job.accepted.v1', job_id: 'job-1' }))).toBe('Codex job accepted')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.job.accepted.v1', job_id: 'job-1', primary_used_pct: 4 }))).toBe('Codex job accepted (primary 4%)')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.job.rejected.v1', reason: 'write-unauthorized', detail: 'approval-policy-not-never' }))).toBe('Codex job rejected: write-unauthorized: approval-policy-not-never')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.job.failed.v1', error: 'rate limited' }))).toBe('Codex job failed: rate limited')
  })

  test('renders codex realtime session messages compactly', () => {
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.session.ready.v1', session_id: 's1' }))).toBe('Codex session ready: s1')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.turn.started.v1', turn_id: 'turn-1' }))).toBe('Codex turn started: turn-1')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.turn.completed.v1', status: 'completed' }))).toBe('Codex turn completed: completed')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.turn.completed.v1', status: 'completed', output: 'final text' }))).toBe('Codex turn completed: completed\n\nfinal text')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.turn.failed.v1', error: 'no_active_turn' }))).toBe('Codex turn failed: no_active_turn')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.session.event.v1', event: 'agent_text_delta', text: 'delta' }))).toBe('delta')
    expect(formatPortalRenderedBody(JSON.stringify({ schema: 'codex.session.event.v1', event: 'turn_completed' }))).toBe('Codex realtime turn_completed')
  })
})
