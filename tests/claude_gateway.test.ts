import { test, expect, describe } from 'bun:test'
import { parseStreamEvent } from '../claude_gateway.ts'

describe('claude_gateway parseStreamEvent', () => {
  test('system/init carries session_id', () => {
    expect(parseStreamEvent({ type: 'system', subtype: 'init', session_id: 'abc-123' }))
      .toEqual({ kind: 'init', sessionId: 'abc-123' })
  })

  test('system/init without session_id remains an init event', () => {
    expect(parseStreamEvent({ type: 'system', subtype: 'init' })).toEqual({ kind: 'init', sessionId: undefined })
  })

  test('rate_limit_event reports utilization as a percentage', () => {
    expect(parseStreamEvent({ type: 'rate_limit_event', rate_limit_info: { utilization: 0.42 } }))
      .toEqual({ kind: 'rate_limit', usedPct: 42 })
  })

  test('assistant concatenates text content parts and drops thinking/tool blocks', () => {
    expect(parseStreamEvent({
      type: 'assistant',
      message: {
        content: [
          { type: 'thinking', thinking: 'hmm' },
          { type: 'text', text: 'Hello' },
          { type: 'tool_use', name: 'x' },
          { type: 'text', text: ' world' },
        ],
      },
    })).toEqual({ kind: 'assistant', text: 'Hello world' })
  })

  test('assistant with no text parts produces an empty delta', () => {
    expect(parseStreamEvent({ type: 'assistant', message: { content: [{ type: 'tool_use', name: 'x' }] } }))
      .toEqual({ kind: 'assistant', text: '' })
    expect(parseStreamEvent({ type: 'assistant', message: {} })).toEqual({ kind: 'assistant', text: '' })
  })

  test('result success carries authoritative final text', () => {
    expect(parseStreamEvent({ type: 'result', subtype: 'success', is_error: false, result: 'final answer' }))
      .toEqual({ kind: 'result', text: 'final answer', status: 'success', isError: false })
  })

  test('result error carries raw subtype plus error flag', () => {
    expect(parseStreamEvent({ type: 'result', subtype: 'error_max_turns', is_error: true, result: '' }))
      .toEqual({ kind: 'result', text: '', status: 'error_max_turns', isError: true })
  })

  test('result is_error:true overrides a success subtype for the caller', () => {
    expect(parseStreamEvent({ type: 'result', subtype: 'success', is_error: true, result: 'partial' }))
      .toEqual({ kind: 'result', text: 'partial', status: 'success', isError: true })
  })

  test('top-level error event', () => {
    expect(parseStreamEvent({ type: 'error', message: 'boom' })).toEqual({ kind: 'error', message: 'boom' })
    expect(parseStreamEvent({ type: 'error', error: 'kaboom' })).toEqual({ kind: 'error', message: 'kaboom' })
    expect(parseStreamEvent({ type: 'error' })).toEqual({ kind: 'error', message: 'error' })
  })

  test('user echo and unknown shapes are ignored as other', () => {
    expect(parseStreamEvent({ type: 'user', message: { role: 'user', content: 'hi' } })).toEqual({ kind: 'other' })
    expect(parseStreamEvent({ type: 'whatever' })).toEqual({ kind: 'other' })
    expect(parseStreamEvent({})).toEqual({ kind: 'other' })
  })

  test('non-object input never throws', () => {
    expect(parseStreamEvent(null)).toEqual({ kind: 'other' })
    expect(parseStreamEvent(undefined)).toEqual({ kind: 'other' })
    expect(parseStreamEvent('not json')).toEqual({ kind: 'other' })
    expect(parseStreamEvent(42)).toEqual({ kind: 'other' })
  })
})
