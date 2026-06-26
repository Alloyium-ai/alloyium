import { describe, expect, test } from 'bun:test'
import {
  CODEX_SESSION_CREATE_SCHEMA,
  CODEX_SESSION_EVENT_SCHEMA,
  CODEX_TURN_STEER_SCHEMA,
  CodexRealtimeSessionRegistry,
  buildCodexInjectedTextItem,
  buildCodexRealtimeEvent,
  buildCodexUserTextInput,
  extractRealtimeText,
  isCodexRealtimeSchema,
  normalizeSessionId,
  sessionPublicState,
} from '../codex_realtime.ts'

describe('codex realtime session registry', () => {
  test('creates sessions and tracks active turn ownership', () => {
    const registry = new CodexRealtimeSessionRegistry()
    const session = registry.create({
      sessionId: 'portal:chat:a2a-portal:codex-gw',
      threadId: 'thread-1',
      threadKey: 'portal:chat:a2a-portal:codex-gw',
      owner: 'a2a-portal',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
      streamTopic: 'portal-codex-stream',
      now: '2026-06-26T12:00:00.000Z',
    })

    expect(session.session_id).toBe('portal:chat:a2a-portal:codex-gw')
    expect(registry.getByThreadId('thread-1')?.session_id).toBe(session.session_id)

    registry.setActiveTurn(session.session_id, 'turn-1')
    expect(registry.get(session.session_id)?.active_turn_id).toBe('turn-1')
    expect(registry.getByTurnId('turn-1')?.thread_id).toBe('thread-1')

    registry.clearActiveTurn(session.session_id, 'completed')
    expect(registry.get(session.session_id)?.active_turn_id).toBeUndefined()
    expect(registry.getByTurnId('turn-1')).toBeNull()
    expect(registry.get(session.session_id)?.status).toBe('completed')
  })

  test('refuses context drift for an existing session id', () => {
    const registry = new CodexRealtimeSessionRegistry()
    registry.create({
      sessionId: 's1',
      threadId: 'thread-1',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })

    expect(() => registry.create({
      sessionId: 's1',
      threadId: 'thread-1',
      cwd: '/tmp',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })).toThrow('session_context_mismatch')
  })

  test('refuses rebinding a Codex thread to a different external session id', () => {
    const registry = new CodexRealtimeSessionRegistry()
    registry.create({
      sessionId: 's1',
      threadId: 'thread-1',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })

    expect(() => registry.create({
      sessionId: 's2',
      threadId: 'thread-1',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })).toThrow('thread_already_bound')
  })

  test('builds monotonic normalized realtime events', () => {
    const registry = new CodexRealtimeSessionRegistry()
    const session = registry.create({
      sessionId: 's1',
      threadId: 'thread-1',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })

    expect(buildCodexRealtimeEvent(registry, session, 'turn_started', { method: 'turn/started', turn_id: 'turn-1' }))
      .toMatchObject({ schema: CODEX_SESSION_EVENT_SCHEMA, session_id: 's1', seq: 1, event: 'turn_started', turn_id: 'turn-1' })
    expect(buildCodexRealtimeEvent(registry, session, 'agent_text_delta', { text: 'hello' }))
      .toMatchObject({ seq: 2, text: 'hello' })
  })

  test('normalizes schema and text helpers', () => {
    expect(isCodexRealtimeSchema(CODEX_SESSION_CREATE_SCHEMA)).toBe(true)
    expect(isCodexRealtimeSchema(CODEX_TURN_STEER_SCHEMA)).toBe(true)
    expect(isCodexRealtimeSchema('codex.job.request.v1')).toBe(false)
    expect(normalizeSessionId('portal:chat:a:b')).toBe('portal:chat:a:b')
    expect(() => normalizeSessionId('bad session id with spaces')).toThrow('bad_session_id')

    expect(extractRealtimeText({ text: 'direct' })).toBe('direct')
    expect(extractRealtimeText({ input: [{ type: 'text', text: 'a' }, { type: 'text', text: 'b' }] })).toBe('a\nb')
    expect(buildCodexUserTextInput('hello')).toEqual([{ type: 'text', text: 'hello', text_elements: [] }])
    expect(buildCodexInjectedTextItem('state snapshot')).toEqual({
      type: 'message',
      role: 'assistant',
      content: [{ type: 'output_text', text: 'state snapshot' }],
    })
  })

  test('returns public state without owner metadata', () => {
    const registry = new CodexRealtimeSessionRegistry()
    const session = registry.create({
      sessionId: 's1',
      threadId: 'thread-1',
      owner: 'requester',
      cwd: '/app',
      sandbox: 'read-only',
      approvalPolicy: 'never',
    })
    expect(sessionPublicState(session)).toMatchObject({
      session_id: 's1',
      thread_id: 'thread-1',
      cwd: '/app',
      sandbox: 'read-only',
      approval_policy: 'never',
      status: 'ready',
    })
    expect(sessionPublicState(session)).not.toHaveProperty('owner')
  })
})
