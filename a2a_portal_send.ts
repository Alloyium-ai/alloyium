export type PortalSendType = 'msg' | 'request' | 'reply'

export function normalizePortalRecipient(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const raw = value.trim()
  if (!raw) return null
  if (raw.startsWith('@')) return raw.slice(1).trim().toLowerCase() || null
  if (raw.startsWith('#')) {
    const topic = raw.slice(1).trim().toLowerCase()
    return topic ? `topic:${topic}` : null
  }
  if (raw.toLowerCase().startsWith('topic:')) {
    const topic = raw.slice('topic:'.length).trim().toLowerCase()
    return topic ? `topic:${topic}` : null
  }
  return raw.toLowerCase()
}

export function isSelfPortalRecipient(recipient: unknown, portalAgentId: string): boolean {
  const to = normalizePortalRecipient(recipient)
  const self = normalizePortalRecipient(portalAgentId)
  return !!to && !!self && to === self
}
