# Foundation Access Token Issuer

Alloyium exposes `a2a_issue_scoped_token` as an MCP tool when A2A tools are enabled.
It verifies an ed25519 signature against the existing A2A public-key registry and
returns a short-lived brokered lease reference for taskboard, Forgejo, or Vault.
It does not return raw credential material.

## Request

The signed canonical payload is deterministic JSON with these fields in this order:

```json
{"agent_id":"agent-a","expiry":"2026-06-27T14:10:00.000Z","issued_at":"2026-06-27T14:00:00.000Z","nonce":"base64url-128bit-min","requested_scope":"taskboard:project:13:read"}
```

The tool input adds `signature`, a base64url ed25519 signature over that UTF-8
canonical payload:

```json
{
  "agent_id": "agent-a",
  "requested_scope": "taskboard:project:13:read",
  "nonce": "base64url-128bit-min",
  "issued_at": "2026-06-27T14:00:00.000Z",
  "expiry": "2026-06-27T14:10:00.000Z",
  "signature": "base64url-ed25519-signature"
}
```

## Policy

The issuer is deny-by-default. Configure one of:

- `A2A_ACCESS_POLICY_FILE`
- `A2A_ACCESS_POLICY_JSON`

Example:

```json
{
  "defaults": { "max_ttl_sec": 900 },
  "agents": {
    "agent-access-alloyium-worker": {
      "max_ttl_sec": 900,
      "scopes": [
        "taskboard:project:13:read",
        "taskboard:project:13:task:create",
        "forgejo:repo:Alloyium-ai/alloyium:branch:push:codex/*",
        "forgejo:repo:Alloyium-ai/alloyium:pr:create",
        "vault:path:team/*:read"
      ]
    }
  }
}
```

Supported scope families are taskboard, Forgejo, and Vault only. Branch scopes
reject unsafe branch names before policy matching.

## Storage And Audit

The runtime uses Redis:

- `alloyium:a2a:pubkey:<agent>` for the existing A2A public-key registry.
- `alloyium:a2a:access:nonce:<agent>:<nonce_hash>` for replay rejection.
- `alloyium:a2a:access:lease:<lease_id>` for brokered lease metadata.
- `alloyium:a2a:access:audit` for redacted audit records.

Audit rows include decision metadata, request hash, nonce hash, and lease id. They
do not include raw tokens, raw nonce, raw signature, or Vault logical path values.
