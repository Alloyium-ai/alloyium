#!/bin/bash
set -euo pipefail

# ── Conversational Claude agent ──────────────────────────────────────────────
# A real `claude` session on the a2a bus via the Rust a2a-shim (MCP-over-UDS → the
# shared a2a-core). Unlike claude-gw (pure inference: --tools '' --strict-mcp-config),
# THIS claude holds the a2a tools (a2a_send, a2a_peers, …) AND receives channel
# injection in-session — a first-class fabric peer that talks to other agents.
#
# Uses the user's OWN logged-in OAuth session (ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN
# stripped). tmux gives the headless session its pty; the one-time workspace-trust and
# dev-channels confirmations are auto-dismissed.

AGENT_ID="${A2A_AGENT_ID:-claude-agent}"
SECRETS_DIR="${A2A_SECRETS_DIR:-/run/secrets/a2a}"
SIGNING_KEY="${A2A_SIGNING_KEY:-${SECRETS_DIR}/${AGENT_ID}.seed}"
CORE_SOCK="${A2A_CORE_SOCK:-/run/a2a-core/core.sock}"
SHIM_BIN="${A2A_SHIM_BIN:-/usr/local/bin/a2a-shim}"
SUBS_KEY="${SUBS_KEY:-alloyium:a2a:silent-subs:${AGENT_ID}}"
MODEL="${CLAUDE_AGENT_MODEL:-opus}"
EFFORT="${CLAUDE_AGENT_EFFORT:-high}"
RUNDIR="${CLAUDE_AGENT_CWD:-/home/bun/agent}"
SESSION="agent-${AGENT_ID}"

# OAuth subscription only — never a baked or forwarded API key.
unset ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL 2>/dev/null || true

mkdir -p "$RUNDIR"

# Detached-agent directive (system-prompt backstop) — in a file to dodge shell quoting.
cat > "${RUNDIR}/system-prompt.txt" <<'SP'
DETACHED A2A AGENT DIRECTIVE: There is no interactive user watching this session. NEVER use AskUserQuestion or any blocking interactive prompt. For any decision, approval, clarification, or blocker, send an A2A request to a peer (e.g. agent-1) with the a2a_send tool and await the reply. You are a first-class agent on the alloyium bus: use a2a_peers to see who is online and a2a_send to talk to them.
SP

# MCP config: the `alloyium` server is the a2a-shim (UDS → a2a-core). The server name
# is IDENTICAL to what --dangerously-load-development-channels resolves. UDS-only — no
# NATS/Redis here; the shared a2a-core owns the bus and DB.
cat > "${RUNDIR}/.mcp.json" <<JSON
{ "mcpServers": { "alloyium": { "command": "${SHIM_BIN}", "args": [], "env": {
  "A2A_AGENT_ID": "${AGENT_ID}", "A2A_SIG_ALG": "ed25519",
  "A2A_SIGNING_KEY": "${SIGNING_KEY}",
  "A2A_CORE_SOCK": "${CORE_SOCK}", "SUBS_KEY": "${SUBS_KEY}",
  "A2A_SHIM_DEBUG": "${A2A_SHIM_DEBUG:-0}" } } } }
JSON

# Launch script run inside tmux (keeps the tmux command free of nested-quote hazards).
cat > "${RUNDIR}/launch.sh" <<LAUNCH
#!/bin/bash
cd "${RUNDIR}"
exec claude --name "${AGENT_ID}" --model "${MODEL}" --effort "${EFFORT}" \\
  --append-system-prompt "\$(cat "${RUNDIR}/system-prompt.txt")" \\
  --mcp-config "${RUNDIR}/.mcp.json" \\
  --dangerously-load-development-channels server:alloyium \\
  --dangerously-skip-permissions \\
  --disallowedTools AskUserQuestion
LAUNCH
chmod +x "${RUNDIR}/launch.sh"

cleanup() { tmux kill-session -t "$SESSION" 2>/dev/null || true; }
trap cleanup TERM INT

echo "[claude-agent] launching ${AGENT_ID} (model=${MODEL} effort=${EFFORT}) shim→${CORE_SOCK}"
tmux new-session -d -s "$SESSION" -n "$AGENT_ID" "bash '${RUNDIR}/launch.sh'"

# Auto-dismiss the two one-time confirmations (no human to press Enter), which appear
# in sequence: the workspace-trust check ("Yes, I trust this folder") then the
# dev-channels warning ("I am using this for local development"). Each defaults to the
# accept option, so Enter selects it; we only press Enter while a known prompt is shown.
for _i in $(seq 1 40); do
  sleep 2
  pane="$(tmux capture-pane -t "$SESSION" -p 2>/dev/null || true)"
  case "$pane" in
    *"trust this folder"*|*"Quick safety check"*|*"Loading development channels"*|*"local development"*|*"local channel development"*)
      tmux send-keys -t "$SESSION" Enter ;;
  esac
done

echo "[claude-agent] ${AGENT_ID} is up; holding while the session lives"
# Keep the container alive as long as the claude session lives.
while tmux has-session -t "$SESSION" 2>/dev/null; do sleep 5; done
echo "[claude-agent] session ended — exiting"
