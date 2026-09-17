# Repository Guidelines

## Project Structure & Module Organization

`mcodex` is a Python 3.10+ CLI/local server with a Vite/React dashboard.

- `src/mcodex/`: Python package. `cli.py` contains the tmux/Codex wrapper, watcher, queues, and CLI commands; `server_local.py` contains the HTTP API and SQLite store.
- `tests/`: Python `unittest` coverage for CLI, watcher, message delivery, and server behavior.
- `frontend/`: React + TypeScript dashboard. Source lives in `frontend/src/`; Vite config and npm scripts live under `frontend/`.
- `docs/`: design notes and project documentation.

## Build, Test, and Development Commands

- `uv sync`: install the Python project environment.
- `uv run mcodex --help`: run the CLI from the managed environment.
- `uv run mcodex serve-local --host 127.0.0.1 --port 8765`: start the local server on loopback.
- `uv run mcodex resume <agent> --yolo`: resume an existing Codex session in tmux and start its watcher.
- `uv run python -m unittest discover -s tests -p 'test_*.py' -v`: run Python tests.
- `cd frontend && npm ci`: install dashboard dependencies.
- `cd frontend && npm run dev -- --host 127.0.0.1 --port 5173`: run the dashboard locally.
- `cd frontend && npm run build`: type-check and build the dashboard.

## Coding Style & Naming Conventions

Match the existing style. Python uses 4-space indentation, `from __future__ import annotations`, type hints, `dataclass` models, `pathlib.Path`, snake_case functions, and uppercase constants. React/TypeScript uses functional components, explicit local types, 2-space indentation, and camelCase helpers. No formatter or linter is configured; run `git diff --check` before submitting.

## Testing Guidelines

Add or update `unittest` tests in `tests/test_*.py` for CLI/server behavior. Prefer focused tests around helpers, parser behavior, watcher state transitions, and HTTP store operations. For frontend changes, run `npm run build` to catch TypeScript and bundling regressions.

## Commit & Pull Request Guidelines

Recent history uses short messages such as `feat: ...`, version markers, and `init`. Prefer concise, imperative commits with a conventional prefix when useful, for example `feat: add watcher restart` or `fix: avoid duplicate pane summaries`.

Pull requests should describe behavior changes, list verification commands, mention local-state or tmux impacts, and include screenshots for dashboard UI changes.

## Agent-Specific Notes

Local runtime state is stored under `~/.mcodex/`, including `local.db`, queues, watcher pids, and logs. Do not assume `mcodex start <agent>` creates a new Codex thread; it runs `codex resume <agent>`, so `<agent>` must already be a valid Codex saved session id or thread name.

The local HTTP API has no authentication. Bind it to a network interface only on a trusted network; messages and pane summaries can contain private conversation content.

API agents are registered over HTTP with `transport=api`; they have an inbox
and identity but no tmux session or watcher. Use
`POST /api/agents/{agent}/inbox/claim` and ACK after the message is incorporated
into current context.

`mcodex send` and the API helper support `--request-id` for retry-safe sends. If
a send times out after printing a request id, retry with that exact
`--request-id`; do not send a fresh duplicate.

Use urgent direct messages only for stop, pause, or ownership-safety
coordination. Put `STOP:`, `URGENT:`, or `MCODEX-URGENT:` at the start of the
first non-empty body line. Watchers prioritize these across sender holds and
idle stability. Busy `URGENT:` and `MCODEX-URGENT:` messages use `Tab`, while
`STOP:` interrupts the current Codex turn with `Escape` and submits with
`Enter`.

Use `uv run mcodex wait <agent> --group <group> --timeout 300 --lines 80` to
wait for a long-running tmux agent to become `idle` and get a named pane tail.
Use this before falling back to `mcodex tail`; never poll raw tmux pane ids from
agent code or instructions.

Use `uv run mcodex issue [options] <type> <body...>` to report mcodex mechanism
problems such as API failures, incomplete watcher data, tmux fallback, suspected
delivery issues, or dashboard mismatches. These issues are stored in
`~/.mcodex/local.db` and are not business task status reports.
Only the agent/person fixing mcodex marks issues handled, for example
`uv run mcodex issue --agent mcodex-dev handle <issue-id>` after the fix is
verified. Reporters should not mark their own tooling issue handled.
