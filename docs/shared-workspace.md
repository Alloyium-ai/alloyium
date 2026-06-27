# Shared host ⇆ agent workspace

A single host directory, bind-mounted at **`/workspace`** in the tool-capable agents, so
the operator and the agents share files both ways: drop files in for the agents to use, and
their `git clone`s / edits land back on the host.

## What it is

- **Host path:** `${A2A_WORKSPACE_HOST_PATH:-./data/workspace}` (repo-relative; inherits the
  `data/` gitignore).
- **In-container path:** `/workspace` (same in every agent that gets it).
- **Mounted into:** `codex-gw`, `codex-gw-b`, `claude-agent`, `claude-agent-b`,
  and launcher-spawned `kind=codex` peers when the launcher profile is enabled.
  - **Not** `claude-gw` (pure inference — no tools, can't touch files) or `a2a-core`
    (bus multiplexer) — least privilege.
- **Ownership:** the host dir is owned by the host user (`CC_UID:CC_GID`, here 1003), and the
  containers run as that same uid, so files cross the boundary with correct ownership in both
  directions — no permission friction, nothing loosened.
- `git config --global --add safe.directory '*'` is already baked into the agent images, so a
  repo cloned into `/workspace` is immediately git-safe.

## Behaviour

- `claude-agent` / `claude-agent-b` run their session **with cwd = `/workspace`**
  (`CLAUDE_AGENT_CWD`), so a bare `git clone` lands directly in the shared dir. The agent's
  generated control files (`system-prompt.txt`, `.mcp.json`, `launch.sh`) stay in
  `/home/bun/agent`, keeping `/workspace` clean.
- `codex-gw` and launcher-spawned `kind=codex` peers are authorized to use
  `/workspace`: it is added to
  `CODEX_BUILD_CWD_ROOTS` (`/workspace,${HOME}/git`).

  > Codex keeps its own defence-in-depth gate for **workspace-write jobs**: the requester must
  > be in `CODEX_GW_WRITE_ALLOWLIST` (compose defaults include `dev-pm`, `agent-1`,
  > `codex-gw`, current codex-rt gateway ids, and `a2a-portal`), and the cwd must be
  > registered in the Redis cwd-allow set. The mount + cwd-root make `/workspace`
  > *eligible*; codex's authz still governs *who* may drive a write there. `codex-gw-b`
  > is read-only (`CODEX_GW_ALLOW_WRITE=0`) — it can read `/workspace` but not run
  > write jobs.
- Launcher-spawned peers keep the legacy same-path workspace root bind
  (`CODEX_WORKSPACE_ROOT`, default `${HOME}/git`) and also receive the shared workspace
  bind. Relative launcher workspace paths are resolved against `A2A_LAUNCH_PROJECT_DIR`
  so Docker receives an absolute host path.
- Read-only launch requests may use a shared checkout when no branch mutation is
  needed. When a write-enabled launch request includes `worktree: REPO@BASE`, the
  launcher creates or reuses a deterministic isolated git worktree under
  `.a2a-worktrees` for `repo + base_ref + target_branch + job_id`; it never switches
  the source checkout branch. The spawned peer receives that worktree as
  `CODEX_GW_DEFAULT_CWD`, and `CODEX_BUILD_CWD_ROOTS` is narrowed to the exact
  worker-visible worktree path.

## Setup

```bash
# host dir owned by your user so both you and the agents can read/write:
docker run --rm -v "$PWD/data:/d" alpine \
  sh -c 'mkdir -p /d/workspace && chown ${CC_UID:-1000}:${CC_GID:-1000} /d/workspace && chmod 775 /d/workspace'
docker compose up -d
```

Override the location in `.env`: `A2A_WORKSPACE_HOST_PATH=/abs/or/repo-relative/path`.
When using `--profile launcher`, the launcher inherits that same path through
`A2A_LAUNCH_WORKSPACE_HOST_PATH`; set `A2A_LAUNCH_PROJECT_DIR=/abs/path/to/clone`
if you run Compose from outside the repository directory.
