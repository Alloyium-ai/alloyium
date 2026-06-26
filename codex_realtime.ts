export const CODEX_SESSION_CREATE_SCHEMA = 'codex.session.create.v1'
export const CODEX_SESSION_READY_SCHEMA = 'codex.session.ready.v1'
export const CODEX_SESSION_INPUT_SCHEMA = 'codex.session.input.v1'
export const CODEX_SESSION_STATE_SCHEMA = 'codex.session.state.v1'
export const CODEX_SESSION_EVENT_SCHEMA = 'codex.session.event.v1'
export const CODEX_TURN_START_SCHEMA = 'codex.turn.start.v1'
export const CODEX_TURN_STARTED_SCHEMA = 'codex.turn.started.v1'
export const CODEX_TURN_COMPLETED_SCHEMA = 'codex.turn.completed.v1'
export const CODEX_TURN_FAILED_SCHEMA = 'codex.turn.failed.v1'
export const CODEX_TURN_STEER_SCHEMA = 'codex.turn.steer.v1'
export const CODEX_TURN_STEERED_SCHEMA = 'codex.turn.steered.v1'
export const CODEX_TURN_INTERRUPT_SCHEMA = 'codex.turn.interrupt.v1'
export const CODEX_TURN_INTERRUPTED_SCHEMA = 'codex.turn.interrupted.v1'
export const CODEX_THREAD_INJECT_ITEMS_SCHEMA = 'codex.thread.inject_items.v1'
export const CODEX_THREAD_INJECTED_SCHEMA = 'codex.thread.injected.v1'

const SESSION_ID_RE = /^[a-zA-Z0-9:_@./-]{1,160}$/

export type CodexRealtimeSessionStatus =
  | 'ready'
  | 'running'
  | 'steering'
  | 'injecting_context'
  | 'interrupting'
  | 'completed'
  | 'failed'
  | 'interrupted'

export type CodexRealtimeSession = {
  session_id: string
  thread_id: string
  thread_key?: string
  owner?: string
  cwd: string
  sandbox: string
  approval_policy: string
  stream_topic?: string
  status: CodexRealtimeSessionStatus
  active_turn_id?: string
  seq: number
  created_at: string
  updated_at: string
}

export type CodexRealtimeEvent = {
  schema: typeof CODEX_SESSION_EVENT_SCHEMA
  session_id: string
  thread_id: string
  seq: number
  event: string
  method?: string
  turn_id?: string
  status?: string
  text?: string
  item_id?: string
  payload?: Record<string, unknown>
  ts: string
}

export type CodexRealtimeSessionCreate = {
  sessionId?: string
  threadId: string
  threadKey?: string
  owner?: string
  cwd: string
  sandbox: string
  approvalPolicy: string
  streamTopic?: string
  now?: string
}

export class CodexRealtimeSessionRegistry {
  private readonly sessions = new Map<string, CodexRealtimeSession>()
  private readonly byThreadId = new Map<string, string>()
  private readonly byTurnId = new Map<string, string>()

  create(args: CodexRealtimeSessionCreate): CodexRealtimeSession {
    const sessionId = normalizeSessionId(args.sessionId ?? args.threadKey ?? `session-${crypto.randomUUID()}`)
    const existing = this.sessions.get(sessionId)
    const now = args.now ?? new Date().toISOString()
    if (existing) {
      if (existing.thread_id !== args.threadId || existing.cwd !== args.cwd || existing.sandbox !== args.sandbox || existing.approval_policy !== args.approvalPolicy) {
        throw new Error('session_context_mismatch')
      }
      existing.thread_key = args.threadKey ?? existing.thread_key
      existing.owner = args.owner ?? existing.owner
      existing.stream_topic = args.streamTopic ?? existing.stream_topic
      existing.updated_at = now
      return existing
    }

    const session: CodexRealtimeSession = {
      session_id: sessionId,
      thread_id: args.threadId,
      ...(args.threadKey ? { thread_key: args.threadKey } : {}),
      ...(args.owner ? { owner: args.owner } : {}),
      cwd: args.cwd,
      sandbox: args.sandbox,
      approval_policy: args.approvalPolicy,
      ...(args.streamTopic ? { stream_topic: args.streamTopic } : {}),
      status: 'ready',
      seq: 0,
      created_at: now,
      updated_at: now,
    }
    this.sessions.set(sessionId, session)
    this.byThreadId.set(args.threadId, sessionId)
    return session
  }

  get(sessionId: string): CodexRealtimeSession | null {
    return this.sessions.get(sessionId) ?? null
  }

  getByThreadId(threadId: string): CodexRealtimeSession | null {
    const sessionId = this.byThreadId.get(threadId)
    return sessionId ? this.get(sessionId) : null
  }

  getByTurnId(turnId: string): CodexRealtimeSession | null {
    const sessionId = this.byTurnId.get(turnId)
    return sessionId ? this.get(sessionId) : null
  }

  list(): CodexRealtimeSession[] {
    return [...this.sessions.values()].map((session) => ({ ...session }))
  }

  setActiveTurn(sessionId: string, turnId: string, status: CodexRealtimeSessionStatus = 'running'): CodexRealtimeSession {
    const session = this.require(sessionId)
    if (session.active_turn_id && session.active_turn_id !== turnId) this.byTurnId.delete(session.active_turn_id)
    session.active_turn_id = turnId
    session.status = status
    session.updated_at = new Date().toISOString()
    this.byTurnId.set(turnId, sessionId)
    return session
  }

  clearActiveTurn(sessionId: string, status: CodexRealtimeSessionStatus): CodexRealtimeSession {
    const session = this.require(sessionId)
    if (session.active_turn_id) this.byTurnId.delete(session.active_turn_id)
    delete session.active_turn_id
    session.status = status
    session.updated_at = new Date().toISOString()
    return session
  }

  updateStatus(sessionId: string, status: CodexRealtimeSessionStatus): CodexRealtimeSession {
    const session = this.require(sessionId)
    session.status = status
    session.updated_at = new Date().toISOString()
    return session
  }

  nextSeq(sessionId: string): number {
    const session = this.require(sessionId)
    session.seq += 1
    session.updated_at = new Date().toISOString()
    return session.seq
  }

  private require(sessionId: string): CodexRealtimeSession {
    const session = this.sessions.get(sessionId)
    if (!session) throw new Error('session_not_found')
    return session
  }
}

export function normalizeSessionId(value: string): string {
  const trimmed = String(value ?? '').trim()
  if (!SESSION_ID_RE.test(trimmed)) throw new Error('bad_session_id')
  return trimmed
}

export function isCodexRealtimeSchema(schema: unknown): boolean {
  return typeof schema === 'string' && (
    schema === CODEX_SESSION_CREATE_SCHEMA ||
    schema === CODEX_SESSION_INPUT_SCHEMA ||
    schema === CODEX_SESSION_STATE_SCHEMA ||
    schema === CODEX_TURN_START_SCHEMA ||
    schema === CODEX_TURN_STEER_SCHEMA ||
    schema === CODEX_TURN_INTERRUPT_SCHEMA ||
    schema === CODEX_THREAD_INJECT_ITEMS_SCHEMA
  )
}

export function extractRealtimeText(req: Record<string, unknown>): string {
  if (typeof req.text === 'string') return req.text
  const input = Array.isArray(req.input) ? req.input : []
  return input.map((item) => {
    if (!item || typeof item !== 'object') return ''
    const rec = item as Record<string, unknown>
    return typeof rec.text === 'string' ? rec.text : ''
  }).filter(Boolean).join('\n')
}

export function buildCodexUserTextInput(text: string): Array<Record<string, unknown>> {
  return [{ type: 'text', text, text_elements: [] }]
}

export function buildCodexInjectedTextItem(text: string, role: 'assistant' | 'user' = 'assistant'): Record<string, unknown> {
  return {
    type: 'message',
    role,
    content: [
      role === 'assistant'
        ? { type: 'output_text', text }
        : { type: 'input_text', text },
    ],
  }
}

export function buildCodexRealtimeEvent(
  registry: CodexRealtimeSessionRegistry,
  session: CodexRealtimeSession,
  event: string,
  opts: Omit<Partial<CodexRealtimeEvent>, 'schema' | 'session_id' | 'thread_id' | 'seq' | 'event' | 'ts'> = {},
): CodexRealtimeEvent {
  return {
    schema: CODEX_SESSION_EVENT_SCHEMA,
    session_id: session.session_id,
    thread_id: session.thread_id,
    seq: registry.nextSeq(session.session_id),
    event,
    ...opts,
    ts: new Date().toISOString(),
  }
}

export function sessionPublicState(session: CodexRealtimeSession): Record<string, unknown> {
  return {
    session_id: session.session_id,
    thread_id: session.thread_id,
    ...(session.thread_key ? { thread_key: session.thread_key } : {}),
    cwd: session.cwd,
    sandbox: session.sandbox,
    approval_policy: session.approval_policy,
    ...(session.stream_topic ? { stream_topic: session.stream_topic } : {}),
    status: session.status,
    ...(session.active_turn_id ? { active_turn_id: session.active_turn_id } : {}),
    seq: session.seq,
    created_at: session.created_at,
    updated_at: session.updated_at,
  }
}
