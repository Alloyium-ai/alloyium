# Foundation Access Token Issuer

Alloyium exposes `a2a_issue_scoped_token` as an MCP tool when A2A tools are enabled.
It verifies an ed25519 signature against the existing A2A public-key registry and
returns a short-lived brokered lease reference for taskboard, Forgejo, or Vault.
It does not return raw credential material.

## Request

The signed canonical payload is a domain-separated pipe string. Each field escapes
`\` as `\\` and `|` as `\|`, then joins the fields in this exact order:
domain, agent id, expiry, issued-at, nonce, requested scope.

```text
a2a-token-request:v1|agent-a|2026-06-27T14:10:00.000Z|2026-06-27T14:00:00.000Z|base64url-128bit-min|taskboard:project:13:read
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
  },
  "roles": {
    "code-reviewer": {
      "max_ttl_sec": 900,
      "scopes": ["forgejo:repo:*/*:pr:review"]
    }
  },
  "agent_roles": {
    "agent-reviewer": ["code-reviewer"]
  }
}
```

Supported scope families are taskboard, Forgejo, and Vault only. Branch scopes
reject unsafe branch names before policy matching.

Forgejo merge scopes are intentionally refused by this general issuer even if a
policy grants `forgejo:repo:<owner>/<repo>:pr:merge`. Foundation merges remain a
trusted broker/host action after two distinct non-author approvals of the exact
head SHA and any required security review.

Successful responses are brokered lease references only. They include
`lifecycle:"brokered"`, `revocable:false`, and an `expires_at_meaning` string so
callers do not mistake issuer bookkeeping for downstream-enforced token expiry.

## Storage And Audit

The runtime uses Redis:

- `alloyium:a2a:pubkey:<agent>` for the existing A2A public-key registry.
- `alloyium:a2a:access:nonce:<agent>:<nonce_hash>` for replay rejection.
- `alloyium:a2a:access:lease:<lease_id>` for brokered lease metadata.
- `alloyium:a2a:access:audit` for redacted audit records.

Audit rows include decision metadata, request hash, nonce hash, and lease id. They
do not include raw tokens, raw nonce, raw signature, or Vault logical path values.
