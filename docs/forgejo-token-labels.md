# Forgejo Token Labels

This stack uses non-secret token labels plus normalized agent roles when a worker
needs a role-scoped token. Do not place bearer values, PATs, private keys, or
host credential references in docs, logs, task comments, A2A messages, or PR
text.

## Canonical Labels

| Label | Vault path shape | Notes |
| --- | --- | --- |
| `kai` | `kai/daemon-token` | Roleless daemon token materialized for the Kai integration. |
| `taskboard` | `taskboard/<role>/api_token` | Role-scoped taskboard token. |
| `forgejo` | `forgejo/<role>/api_token` | First-pass source-of-truth provider for repo/PR flows. |
| `github` | `github/<role>/api_token` | Compatibility label only when GitHub mirroring is explicitly enabled. |

## Role Normalization

| Input role | Forgejo/GitHub role segment | Taskboard role segment |
| --- | --- | --- |
| `developer` | `agent-developer` | `agent-developer` |
| `code-reviewer` | `agent-code-reviewer` | `agent-code-reviewer` |
| `security-auditor` | `agent-security-auditor` | `agent-security-auditor` |
| `qa` | `agent-qa` | `agent-qa-agent` |
| `architect` | `agent-architect` | `agent-architect` |
| `orchestrator` | `agent-orchestrator` | `agent-orchestrator` |

## First-Pass Forgejo Scopes

Use Forgejo as the first-pass provider for foundation-access PRs. GitHub can be
used later for preservation or mirroring when a scoped GitHub route exists.

| Operation | Forgejo scope |
| --- | --- |
| Read repository or inspect PRs | `read:repository` |
| Push a branch | `write:repository` plus repository write access |
| Create or update a PR | `write:repository` |
| Submit or update a review | `write:repository` |
| Merge after two distinct reviews | `write:repository` plus repository policy permission |
| Edit issue comments, labels, or issue metadata | add `write:issue` only when needed |

When escalating access, ask for either explicit signed-token scopes or a token
label plus normalized role. Do not ask for raw token values or guess a path.
