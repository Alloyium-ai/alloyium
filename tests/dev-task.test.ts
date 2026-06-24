// Pure unit tests for dev_task_client.ts helpers — argv parsing, request building,
// MCP-result unwrap, and strict reply gating. No live bus.
import { test, expect, describe } from 'bun:test'
import { parseArgs, buildRequest, classifyReply, unwrapToolResult } from '../dev_task_client.ts'

describe('parseArgs', () => {
  test('prompt only', () => { expect(parseArgs(['p.txt'])).toEqual({ promptFile: 'p.txt', inputFile: undefined, wait: false }) })
  test('prompt + input', () => { expect(parseArgs(['p.txt', 'in.md'])).toEqual({ promptFile: 'p.txt', inputFile: 'in.md', wait: false }) })
  test('--wait anywhere', () => {
    expect(parseArgs(['p.txt', '--wait']).wait).toBe(true)
    expect(parseArgs(['p.txt', 'in.md', '--wait'])).toEqual({ promptFile: 'p.txt', inputFile: 'in.md', wait: true })
    expect(parseArgs(['--wait', 'p.txt']).wait).toBe(true)
  })
  test('missing prompt → error', () => { expect(parseArgs([]).error).toBeTruthy(); expect(parseArgs(['--wait']).error).toBeTruthy() })
  test('unknown flag → error (not treated as input)', () => { expect(parseArgs(['p.txt', '--nope']).error).toContain('unknown flag') })
  test('extra positional → error (no silent overwrite)', () => { expect(parseArgs(['p.txt', 'in.md', 'extra']).error).toContain('extra') })
})

describe('buildRequest', () => {
  test('deterministic with injected taskId', () => {
    expect(buildRequest('build X', undefined, undefined, { taskId: 'dtask-fixed' }))
      .toEqual({ schema: 'dev.task.request.v1', task_id: 'dtask-fixed', title: 'build X' })
  })
  test('carries body + input_file + reply_to', () => {
    expect(buildRequest('t', 'the body', '/abs/in.md', { taskId: 'id1', replyTo: 'dev-task' }))
      .toEqual({ schema: 'dev.task.request.v1', task_id: 'id1', title: 't', body: 'the body', input_file: '/abs/in.md', reply_to: 'dev-task' })
  })
  test('genId shapes dtask-*', () => { expect(buildRequest('t', undefined, undefined, { genId: () => 'dtask-abc-def' }).task_id).toBe('dtask-abc-def') })
  test('omits empty body/input', () => {
    const r = buildRequest('t', undefined, undefined, { taskId: 'x' })
    expect('body' in r).toBe(false); expect('input_file' in r).toBe(false)
  })
})

describe('unwrapToolResult', () => {
  test('durable send → ok+id+mode+seq', () => {
    expect(unwrapToolResult({ content: [{ text: JSON.stringify({ ok: true, id: 'mid', mode: 'jetstream', seq: 7 }) }] }))
      .toEqual({ ok: true, id: 'mid', mode: 'jetstream', seq: 7 })
  })
  test('rejected send → ok:false', () => {
    expect(unwrapToolResult({ content: [{ text: JSON.stringify({ ok: false, error: 'body_too_large' }) }] })?.ok).toBe(false)
  })
  test('garbage → null', () => { expect(unwrapToolResult({ content: [{ text: 'x' }] })).toBeNull(); expect(unwrapToolResult(null)).toBeNull() })
})

describe('classifyReply (strict envelope gating)', () => {
  const ctx = { taskId: 'dtask-1', requestId: 'req-1', target: 'dev-pm', selfId: 'dev-task' }
  const env = (over: any) => ({ feed: 'a2a', kind: 'direct', from: 'dev-pm', to: 'dev-task', type: 'reply', corr: 'req-1', ...over })
  test('accepted (corr + ids match)', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.accepted.v1', task_id: 'dtask-1' }), env({}), ctx).kind).toBe('accepted')
  })
  test('rejected', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.rejected.v1', task_id: 'dtask-1', reason: 'busy' }), env({}), ctx).kind).toBe('rejected')
  })
  test('completed/failed arrive as type:msg', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.completed.v1', task_id: 'dtask-1', summary: 's' }), env({ type: 'msg', corr: undefined }), ctx).kind).toBe('completed')
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.failed.v1', task_id: 'dtask-1', error: 'e' }), env({ type: 'msg', corr: undefined }), ctx).kind).toBe('failed')
  })
  test('foreign task_id → ignore', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.accepted.v1', task_id: 'other' }), env({}), ctx).kind).toBe('ignore')
  })
  test('wrong corr → ignore (reply-theft guard)', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.accepted.v1', task_id: 'dtask-1' }), env({ corr: 'other-req' }), ctx).kind).toBe('ignore')
  })
  test('wrong from/to → ignore', () => {
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.accepted.v1', task_id: 'dtask-1' }), env({ from: 'mallory' }), ctx).kind).toBe('ignore')
    expect(classifyReply(JSON.stringify({ schema: 'dev.task.accepted.v1', task_id: 'dtask-1' }), env({ to: 'someone' }), ctx).kind).toBe('ignore')
  })
  test('non-a2a / non-direct / non-json → ignore', () => {
    expect(classifyReply('{}', env({ feed: 'nats' }), ctx).kind).toBe('ignore')
    expect(classifyReply('{}', env({ kind: 'topic' }), ctx).kind).toBe('ignore')
    expect(classifyReply('not json', env({}), ctx).kind).toBe('ignore')
  })
})
