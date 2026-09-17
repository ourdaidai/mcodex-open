# mcodex

`mcodex` is a local coordination layer for multiple interactive Codex sessions.

It combines:

- a `tmux` wrapper around the local `codex` CLI
- a SQLite-backed `server-local` process for groups, agents, messages, sessions, and events
- a watcher process per agent that registers heartbeats and injects pending messages into the tmux pane
- a React dashboard for group messages, agent status, lifecycle controls, and captured pane summaries

## Current Model

The important current limitation is that an `agent` name is also passed to Codex as a resume target:

```bash
codex resume <agent>
```

This means `mcodex start mcodex-doc` does not create a new Codex thread named `mcodex-doc`. It starts a tmux session and runs `codex resume mcodex-doc`. The value must already be a valid Codex saved session id or thread name.

If Codex cannot resume that target, the tmux pane stays open with a diagnostic block such as:

```text
ERROR: No saved session found with ID mcodex-doc.
mcodex: codex exited with status 1
```

## Requirements

- Python 3.10+
- `uv`
- `tmux`
- local `codex` CLI
- Node.js `^20.19.0 || >=22.12.0` and npm for the dashboard

Install the Python project environment:

```bash
uv sync
```

Run CLI commands through `uv` unless you are inside the project virtualenv:

```bash
uv run mcodex --help
```

## Quick Start

Start the local coordination server:

```bash
uv run mcodex serve-local --host 127.0.0.1 --port 8765
```

Start the dashboard in another shell:

```bash
cd frontend
npm ci
npm run dev -- --host 127.0.0.1 --port 5173
```

Open the dashboard:

```text
http://localhost:5173
```

For access through the WSL IP from Windows, bind both processes to an interface
reachable from Windows. The API has no authentication, so do this only on a
trusted network:

```bash
uv run mcodex serve-local --host 0.0.0.0 --port 8765
npm run dev -- --host 0.0.0.0 --port 5173
```

Find the current WSL IP:

```bash
hostname -I
```

Then open:

```text
http://<wsl-ip>:5173
```

Start or resume an agent session:

```bash
uv run mcodex resume <existing-codex-session-id-or-thread-name> --yolo
```

When `--group` is omitted, `resume` and `start` use the current directory's final path segment as the group id. For example, running from `/path/to/mcodex` joins or creates group `mcodex`. Pass `--group <group>` to override this.

For detached startup:

```bash
uv run mcodex start <existing-codex-session-id-or-thread-name> --yolo
```

`start` prints the tmux session name and attach command.

## Commands

### `mcodex resume`

```bash
uv run mcodex resume <agent> [options]
```

Creates `tmux` session `mcodex-<agent>`, starts Codex with `codex resume <agent> --no-alt-screen`, starts the watcher, then attaches or switches your current tmux client to the session.

Useful options:

```bash
--yolo
--group <group>
--server-local http://127.0.0.1:8765
--idle-seconds 15
--poll-interval 2.0
--history-limit 100000
--contact-hold-seconds 60
--dev
```

If `--group` is omitted, the default group id is the current directory name.

`--yolo` maps to Codex:

```bash
--dangerously-bypass-approvals-and-sandbox
```

`--dev` keeps the watcher visible in tmux. Codex runs in the left pane with about two thirds of the width; the watcher runs in the right pane with about one third of the width and prints heartbeat, queue, injection, and shutdown events.

### `mcodex start`

```bash
uv run mcodex start <agent> [options]
```

Uses the same session creation path as `resume`, but does not attach to tmux. It prints:

```text
started mcodex-<agent>; attach with: tmux attach -t mcodex-<agent>
```

If Codex exits immediately, the tmux pane remains open so the dashboard and `tmux attach` can show the failure.

`start --dev` also creates the side-by-side Codex/watcher tmux layout, but still does not attach automatically.

### `mcodex up`

```bash
uv run mcodex up -c .mcodex
```

Starts multiple existing Codex sessions from a project-local config. Put `.mcodex`
in the project directory:

```ini
[mcodex]
group = sample
layout = columns
yolo = true
agents = api, worker, reviewer
```

`agents` order is stable and becomes the tmux pane order from left to right:

```text
api | worker | reviewer
```

`group` defaults to the config file directory name. `cwd` defaults to the config
file directory, so running `mcodex up -c /path/to/project-a/.mcodex` still starts
Codex from `/path/to/project-a`. Only `layout = columns` is currently supported.

The tmux workbench name is derived from the group and config directory. For
example, `/path/to/project-b/.mcodex` with `group = sample` starts
`mcodex-group-sample-project-b`, so it can coexist with another `sample` workbench from
a different directory. Set `session = <label>` in `.mcodex` when the directory
name is not specific enough.

`up` starts one Codex pane per agent, starts a background watcher for each pane,
then attaches or switches your current tmux client to that workbench. Use
`--detach` to start everything in the background and only print the attach
command. It refuses to continue if the target tmux workbench already exists, if
an old per-agent session `mcodex-<agent>` exists, or if server-local reports
that an agent already has a running session.

New `up` workbenches show each pane title as `<group>: <agent>`, for example
`sample: reviewer`. The agent name is the entry from `.mcodex` `agents`, the
same value passed to `codex resume <agent>`.

### `mcodex agents`

```bash
uv run mcodex agents [options]
```

Lists agents in the current group with their current status. If `--group` is omitted, the group id defaults to the current directory name.

Useful options:

```bash
--group <group>
--server-local http://127.0.0.1:8765
--active
--json
```

Default text output is one agent per line:

```text
<agent_id>	<status>	<display_name>
```

For agent-to-agent discovery, prefer machine-readable JSON:

```bash
uv run mcodex agents --json
```

Use `--active` to hide `offline` agents. Without `--active`, the command includes `idle`, `busy`, `online`, and `offline` agents so an agent can distinguish unavailable peers explicitly.

### `mcodex feed`

```bash
uv run mcodex feed
uv run mcodex feed --since-last
uv run mcodex feed --include-direct
uv run mcodex feed --json
```

Reads the group feed without consuming inbox messages. The default is tuned for
agents: it prints recent `pane_summary` entries from the last hour, capped at
20 recent items, and also includes each agent's latest `pane_summary` as
baseline context. The baseline rows are extra, so output can exceed `--limit`.
Use `--since-last` to keep a local per-agent/per-group cursor under
`~/.mcodex/feed-cursors/`; baseline rows may repeat across `--since-last`
reads, but the cursor never moves backward. Use `--include-direct` for
browser-style audit entries that include direct messages.

`feed` is a completed-turn view, not a live stream of a busy pane. Active work
may remain absent until Codex reaches a stable final response; use one bounded
`mcodex wait`, then named `mcodex tail` when current progress is required. That
absence alone is not a watcher defect.

### `mcodex wait`

```bash
uv run mcodex wait <agent> --group <group> --timeout 300 --lines 80
uv run mcodex wait <agent> --until idle --json
```

Waits for a named agent in the group to reach a server-local status, defaulting
to `idle`, then prints a named pane tail. If the timeout expires, it returns
non-zero but still prints the current status and tail when available. Use this
instead of raw `sleep; tmux capture-pane` polling for long-running tasks.

### `mcodex tail`

```bash
uv run mcodex tail <agent> --group <group> --wait 35 --lines 55
uv run mcodex tail <agent> --json
```

Reads the last lines of a running agent's tmux pane through `mcodex`. This is a
read-only fallback when `mcodex feed` or the captured `pane_summary` is not
enough context and `mcodex wait` is not the right shape. It resolves the pane
from `server-local` by agent name and validates the group, so agents should not
use raw pane ids such as
`tmux capture-pane -pt %7`.

### `mcodex inbox` and `mcodex send`

```bash
uv run mcodex inbox
uv run mcodex inbox ack --all
uv run mcodex send <recipient-agent> "message body"
uv run mcodex send --request-id handoff-20260731-01 <recipient-agent> "message body"
```

Claims direct messages for the current agent and ACKs them after they are in
context. In `resume` and `up` panes, identity comes from `MCODEX_AGENT`,
`MCODEX_GROUP`, and `MCODEX_SERVER_LOCAL`. Outside those panes, pass
`--agent`, `--group`, or `--server-local`. Existing panes started before this
feature also need explicit options.

`inbox ack --all` and the external helper's `ack --all` remove local handles
whose server delivery returns HTTP 404 or 409 because the claim is no longer
usable, report them as `Stale`, and
continue with the remaining handles. ACKing one explicit stale handle still
fails so accidental identity or message mistakes remain visible.

A Codex final answer and its `pane_summary` do not create a direct inbox
message. When another agent must consume a result independently of the
dashboard/feed, the sender must call `mcodex send <recipient> ...` explicitly.

`send` uses a client request id so timeout retries are safe. If a send command
fails after printing `request_id=...`, retry with the same `--request-id`
instead of sending a fresh duplicate. The server returns the existing message
when group, sender, recipient, body, and request id match.

### `mcodex issue`

```bash
uv run mcodex issue --title "API call failed" api_failed "GET /api/groups returned 503"
cat /tmp/mcodex-issue.md | uv run mcodex issue --stdin watcher_incomplete
uv run mcodex issue --body-file /tmp/mcodex-issue.md tmux_fallback_used
uv run mcodex issue --agent mcodex-dev handle <issue-id>
```

Reports a problem with the mcodex mechanism itself, not business task status.
Issues are stored in `~/.mcodex/local.db` and shown in the dashboard Status
view for the current group. Use this when an agent had to fall back to tmux,
observed incomplete watcher/API data, suspected message delivery, or found a
dashboard mismatch.

Issue status is maintained by the agent/person fixing mcodex, not by the
reporter. After a tooling fix or operational cleanup is verified, mark it with
`mcodex issue --agent <maintainer-agent> handle <issue-id>`. Use
`mcodex issue reopen <issue-id>` only when a handled issue needs more work.

Supported types:

```text
api_failed
watcher_incomplete
tmux_fallback_used
message_delivery_suspect
dashboard_mismatch
```

The command shape is `mcodex issue [options] <type> <body...>`. Put options
before `<type>` when using positional body, or prefer `--stdin` / `--body-file`
for multiline or shell-sensitive text.

### `mcodex serve-local`

```bash
uv run mcodex serve-local --host 127.0.0.1 --port 8765
```

Runs the local HTTP API and stores state in:

```text
~/.mcodex/local.db
```

Default CLI server URL:

```text
http://127.0.0.1:8765
```

## Session Behavior

Each agent gets one tmux session:

```text
mcodex-<agent>
```

If that session already exists, `mcodex` refuses to start a second copy. Attach manually:

```bash
tmux attach -t mcodex-<agent>
```

The Codex pane command is intentionally not `exec codex`. When Codex exits, `mcodex` prints a horizontal-rule diagnostic block and then opens a shell in the same pane. This keeps short-lived failures visible.

During startup, the watcher automatically answers two known Codex prompts:

- update prompt: chooses `Skip until next version`
- resume working-directory prompt: chooses `Use session directory`

## Watcher Behavior

Every `start` or `resume` session launches a background watcher.

The watcher:

- ensures the target group exists on `server-local`
- registers the agent session
- keeps running and retries registration every 60 seconds if `server-local` is unavailable at startup
- sends heartbeat updates with `busy` or `idle`
- fetches pending messages for that agent
- queues pending messages locally under `~/.mcodex/<agent>/queue.json`
- removes locally queued messages that are no longer pending on the server
- injects queued messages into the tmux pane only after the pane has been stable
- ACKs delivered messages back to `server-local`
- marks the session disconnected when the tmux pane or session ends

Default timing:

```text
idle before message injection: 15 seconds
pane summary capture delay: 5 seconds
poll interval: 2 seconds
current-contact hold: 60 seconds
agent heartbeat timeout: 15 seconds
server-local startup retry: 60 seconds
```

Background watchers write stdout and stderr to rotating log files under:

```text
~/.mcodex/logs/
```

Log file names include the UTC launch timestamp, agent id, and a short random suffix. Each background watcher rotates its active log at 10 MiB and keeps two backup segments. On the next background watcher start or restart, cleanup enforces both an 80-file limit and a 256 MiB aggregate limit while protecting every segment of the watcher being launched. Background watcher commands include `--log-events`, so these logs contain startup, changed heartbeat, queue, injection, connectivity, and shutdown events in addition to tracebacks. An unchanged successful heartbeat is sampled once per hour; errors and operational changes are always logged. Development watchers shown in tmux keep writing directly to their visible pane.

Queue and retry notifications shown through tmux `display-message` are targeted
only to clients attached to that watcher session. If three `mcodex up`
workbenches are open in separate terminals, queued messages for panes in one
workbench flash only in that terminal instead of the last active tmux client.

Queued message injection uses tmux key input. Short prompts are typed literally; long prompts use a tmux paste buffer so multiline bodies are delivered as one block instead of being split by the terminal input UI. If the watcher considers the pane `idle`, it submits with `Enter`. Normal and non-stop urgent messages use `Tab` while the pane is `busy`, so Codex queues them after the active turn. A `STOP:` message instead sends `Escape`, waits briefly for Codex to interrupt the active turn, and submits the claimed STOP batch with `Enter`. The injected text starts with a fixed line listing other active agents in the same group, followed by one or more direct-message lines:

```text
Other active agents in group [<group-id>]: <other-active-agent-id>, <other-active-agent-id>
Message to you [<recipient-agent-id>] from [<sender>] [sent_at=<UTC ISO timestamp>, age=<relative age>]: <body>
```

If there are no other active agents in the same group, the first line ends with `(none)`.

`sent_at` is the server-local message creation time. `age` is calculated at watcher injection time, using compact values such as `42s ago`, `12m ago`, `3h ago`, or `2d ago`, so the receiving agent can judge whether a queued message is stale.

The watcher keeps a sticky current sender for `--contact-hold-seconds`. Messages from other senders wait until the active sender has been silent long enough. For stop/pause/ownership-safety coordination only, a direct message whose first non-empty body line starts with `STOP:`, `URGENT:`, or `MCODEX-URGENT:` is treated as urgent and bypasses contact hold and idle stability. `URGENT:` and `MCODEX-URGENT:` remain queued with `Tab` when busy. `STOP:` is stronger: after claiming the message, the watcher interrupts a busy Codex turn with `Escape`, then injects with `Enter`. Interruption or injection failure releases the claim and does not ACK the delivery. ACK means the input reached Codex, not that Codex acted on it or replied.

Before injecting queued messages, the watcher claims them through `server-local`.
If another API client has already claimed a message, the watcher drops the stale
local copy instead of injecting it. Successful tmux injection is ACKed with the
claim id; failed tmux injection releases the claim so another client can claim
it later.

Pending direct messages can be canceled before injection. The dashboard shows a `Cancel queued` action for messages whose delivery state is still `pending`, or use the API directly:

```bash
curl -s -X POST http://127.0.0.1:8765/api/messages/<message-id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"recipient_agent_id":"<recipient-agent>"}'
```

Canceling marks that delivery as `canceled`; on the next successful pending-message fetch, the watcher drops any matching local queued copy. If the watcher has already injected the message into Codex, canceling cannot undo that injected input.

Restart only the watcher after code changes:

```bash
uv run mcodex restart-watch <agent>
```

This keeps the existing tmux session and Codex pane alive. It terminates the old watcher process from `~/.mcodex/<agent>/watcher.pid`, verifies the PID still looks like `mcodex watch --agent <agent>` before killing it, marks old running server-local sessions disconnected when possible, and starts a new watcher against the existing pane. It first tries to reuse the agent's currently running server-local `tmux_session` and `pane_id`, so it works for both old `mcodex-<agent>` sessions and `mcodex up` group panes. If `--group` is omitted, it reuses the current server-local group and only falls back to the current directory name if the server cannot answer.

In non-dev mode, `restart-watch` prints the new background watcher log path.

Use `--dev` to start the replacement watcher in a visible split pane:

```bash
uv run mcodex restart-watch <agent> --dev
```

Restart active watchers after upgrading watcher delivery behavior:

```bash
uv run mcodex restart-watch <agent> --group <group>
```

### API agents

Trusted LAN clients can join an existing group without tmux. Create the group
first with `POST /api/groups`, or let a watcher create it by starting an
`mcodex` session in that group.

```bash
curl -s -X POST http://127.0.0.1:8765/api/groups/mcodex/agents \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"codex-app-pm","display_name":"Codex App PM","transport":"api"}'
```

API agents default to `idle`, can set `idle`, `busy`, or `offline`, and become
`offline` after one hour without identity-bearing activity. Direct messages must
be consumed through inbox claim/ACK; reading
`GET /api/groups/{group}/messages` is audit-only and does not clear watcher
queues.

Inside an `mcodex resume` or `mcodex up` tmux pane, use the built-in helper:

```bash
uv run mcodex feed
uv run mcodex feed --since-last
uv run mcodex wait reviewer --group sample --timeout 300 --lines 80
uv run mcodex tail reviewer --group sample --wait 35 --lines 55
uv run mcodex inbox
uv run mcodex inbox ack 1
uv run mcodex inbox ack --all
uv run mcodex inbox release 1
uv run mcodex send mcodex-doc "Please review the blocker."
uv run mcodex issue dashboard_mismatch "dashboard showed agent idle after watcher exited"
```

`resume` and `up` set `MCODEX_AGENT`, `MCODEX_GROUP`, and
`MCODEX_SERVER_LOCAL` in the Codex pane, so the helper can infer identity. Use
`--agent`, `--group`, or `--server-local` to override when running outside that
pane. Existing panes started before this feature do not gain those environment
variables automatically; pass explicit options there.

Use the helper for external Codex/Windows sessions:

```bash
uv run python scripts/mcodex_api_agent.py --base-url http://127.0.0.1:8765 --group mcodex --agent codex-app-pm register
uv run python scripts/mcodex_api_agent.py feed --since-last
uv run python scripts/mcodex_api_agent.py wait mcodex-doc --timeout 300 --lines 80
uv run python scripts/mcodex_api_agent.py tail mcodex-doc --lines 55 --wait 35
uv run python scripts/mcodex_api_agent.py inbox
uv run python scripts/mcodex_api_agent.py ack 1
uv run python scripts/mcodex_api_agent.py send mcodex-doc "Please review the blocker."
uv run python scripts/mcodex_api_agent.py send --request-id handoff-20260731-01 mcodex-doc "Please review the blocker."
uv run python scripts/mcodex_api_agent.py issue tmux_fallback_used "feed was incomplete, used named tail"
uv run python scripts/mcodex_api_agent.py issue handle <issue-id>
```

For Windows, multiline text, backslashes, quotes, JSON, or long handoffs, avoid
putting the body in command-line argv. Use stdin or a UTF-8 file so Windows/WSL
quoting cannot truncate the message:

```bash
cat /tmp/mcodex-message.md | uv run python scripts/mcodex_api_agent.py send mcodex-doc --stdin
uv run python scripts/mcodex_api_agent.py send mcodex-doc --body-file /tmp/mcodex-message.md
```

If helper `send` times out or the client disconnects, reuse the printed
`--request-id` on retry. Do not blindly send a second copy.

The helper state file stores API-agent identity plus local claim handles needed
for later ACK/release. When a Windows shim invokes the WSL helper with a
`/mnt/<drive>/...` state directory and that mount returns an I/O error, the
helper automatically falls back to WSL-local state under
`~/.mcodex/api-agents/<agent>`. If the old identity existed only in the failed
Windows state file, pass `--agent`, `--group`, and `--base-url`, or run
`register` again before claiming inbox messages.

For external agents, use helper `tail` only as a read-only diagnostic fallback.
Prefer `feed` first because it is lower-token and stores the watcher-captured
status summaries. When an agent uses that fallback because mcodex data was
missing or suspect, it should also file an `issue` so the maintainer can see the
tooling gap.

## Pane Summary Capture

After the tmux pane stops changing for 5 seconds, the watcher captures the last 2000 lines and extracts the latest Codex-style summary block.

The parser looks for horizontal separators such as:

```text
--------------------------------------------------------------------------------
```

It also recognizes Codex's long box-drawing horizontal separators and `Worked for ...` footers. When the watcher marks the pane idle and a completed prompt-delimited assistant block is available, that complete block takes precedence over separator parsing so full-width rules rendered inside tables cannot truncate the answer. Separator parsing remains the fallback for completed turns without a final input prompt. Candidate summary blocks stop before a Codex input prompt line starting with `›`, so text currently being typed in the Codex input box is not published as an agent summary. A plain `>` line is treated as normal summary content. The stored summary is capped at 20000 characters, with the middle omitted if truncation is required.

The watcher only sends a summary when its whitespace-normalized content has not already been sent by that watcher. The dashboard renders it as a compact terminal-summary row in the group conversation feed. It is persisted as a feed message with `message_type: pane_summary`, deduped by agent and summary body, and is not removed by later heartbeat updates that do not contain a summary. It is not shown inside the agent status sidebar, and it is not a full terminal mirror.

## Local Message Archives

`server-local` keeps direct messages and pane summaries in SQLite for at least
30 days; older rows then become eligible for archival. A direct message is
eligible only when every delivery is `acked` or `canceled`. A message with a
`pending`, `claimed`, or any other nonterminal delivery remains in SQLite. Pane
summaries are eligible based on age alone.

Archive segments are application-managed, write-once/no-replace gzip JSONL
files. With the default database at `~/.mcodex/local.db`, they are written under
`~/.mcodex/archives/<encoded-group>/<yyyy-mm>/`. With a custom
`serve-local --db-path`, the default artifact root is an `archives/` directory
next to that database. Each segment contains at most 5,000 records, and one
pass writes at most 10 segments. Each `server-local` process start schedules
its first pass after about 60 seconds; while that process remains running,
later passes run every 24 hours.

Archive inspection is CLI-only and read-only:

```bash
uv run mcodex archive list
uv run mcodex archive list --group default --kind messages --json
uv run mcodex archive show <archive-id>
```

`mcodex archive list` reads manifest metadata without opening archive files.
For a nondefault database, both `archive list` and `archive show` must receive
`--db-path` pointing to that database. `archive show` uses the database's
sibling `archives/` directory by default; `--archive-root` overrides that
artifact root. It verifies the file's SHA-256 checksum, consumes the complete
gzip stream, and parses every JSONL line as a JSON object containing only
finite numeric values. Invalid JSON, non-object JSON, and non-finite numeric
constants are rejected. It writes nothing to stdout until all of those checks
pass.

Normal dashboard and feed requests never read archives. Archives are retained
indefinitely; there is currently no archive restore or deletion feature.
Idempotency request keys remain in SQLite after their messages are archived.
Within the same group and sender scope, retrying a key with the same recipient
and stripped body returns the archived original message ID; changing
the recipient or body is a conflict.

## Dashboard

The React dashboard lives in `frontend/`.

```bash
cd frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

Views:

- `Workspace`: groups, unified message feed, composer, agent roster
- `Status`: group mcodex issues, agent sessions, events, and start/stop/reconnect controls
- `Performance`: local HTTP, SQLite, heartbeat, delivery, SSE, and maintenance metrics
- `Idle alert`: an opt-in browser-side alert in the navigation sidebar. After enabling it, the dashboard watches all visible groups and fires one system notification plus a short sound when a group transitions into "all agents are idle". Browser notification permission and audio playback both require the enable click.

The workspace feed loads the most recent 80 group messages by default. The group
card still shows the full persisted message count. Conversation detail message
pages default to 100 rows, are capped at 500 rows, and return `next_cursor` for
the next older page.

Message composer behavior:

- messages must start with `@recipient`
- focusing the empty composer or typing `@` shows agent suggestions
- valid recipient ids may contain letters, digits, `.`, `_`, and `-`
- example: `@mail summarize the last failure`
- pending direct messages show delivery state and can be canceled before watcher injection

API base selection:

- if `VITE_SERVER_LOCAL_URL` is set, the dashboard uses only that URL
- otherwise it tries `http://<current-page-host>:8765` first
- then it falls back to `http://127.0.0.1:8765`
- successful API base candidates are reused for later requests
- request timeout per candidate is 1600 ms
- the SSE stream uses `http://<current-page-host>:8765` unless `VITE_SERVER_LOCAL_URL` is set
- while SSE is connected, events trigger debounced, resource-specific refreshes
- after SSE disconnects, a five-second full poll refreshes groups, messages, issues, and agent details until the stream reconnects
- subscriber overflow emits one coalesced `resync_required` event, which triggers one full refresh

The Performance view requests metrics every five seconds only while it is
visible. It keeps at most 720 parsed samples, representing one hour, in browser
memory; refreshing the page clears that history.

For WSL from Windows, both of these can be valid depending on forwarding:

```text
http://127.0.0.1:5173
http://<wsl-ip>:5173
```

If the dashboard is opened from a host where `127.0.0.1:8765` is not the WSL backend, start Vite with an explicit backend URL:

```bash
VITE_SERVER_LOCAL_URL=http://<wsl-ip>:8765 npm run dev -- --host 0.0.0.0 --port 5173
```

## HTTP API

The frontend and watchers use these local routes:

```text
GET  /api/groups
POST /api/groups
GET  /api/groups/:groupId/agents
GET  /api/groups/:groupId/issues
GET  /api/groups/:groupId/messages
GET  /api/groups/:groupId/conversations
GET  /api/groups/:groupId/conversations/:conversationId/messages
GET  /api/issues
GET  /api/issues/:issueId
GET  /api/agents/:agentId
GET  /api/agents/:agentId/sessions
GET  /api/agents/:agentId/events
GET  /api/agents/:agentId/pending-messages
POST /api/groups/:groupId/agents
POST /api/agents/:agentId/status
POST /api/agents/:agentId/inbox/claim
POST /api/agents/register
POST /api/agents/heartbeat
POST /api/agents/disconnect
POST /api/agents/:agentId/start
POST /api/agents/:agentId/stop
POST /api/agents/:agentId/reconnect
POST /api/issues
POST /api/issues/:issueId/handle
POST /api/issues/:issueId/reopen
POST /api/groups/:groupId/messages
POST /api/messages/:messageId/ack
POST /api/messages/:messageId/release
POST /api/messages/:messageId/cancel
GET  /api/events/stream
GET  /metrics
```

`/api/events/stream` is an SSE stream. The dashboard uses it to refresh after server-side changes instead of relying only on polling.

`GET /metrics` is served on the existing `serve-local` HTTP port and returns
Prometheus text. It does not require Jaeger, Grafana, an OpenTelemetry collector,
or a second listener. Metric labels use bounded enums and route templates;
agent, group, message, conversation, session, request, and filesystem identities
are never exposed as labels. If metrics initialization or an individual scrape
fails, normal coordination APIs continue to run. The Performance view reports
the scrape failure and retries on its next visible five-second interval.

Control requests return a `request_id`. Later `session_registered`,
`session_disconnected`, and realtime `agent_updated` events include the same id
when available, so the dashboard can show requested versus confirmed lifecycle
changes.

Routine unchanged heartbeats update `agents.last_heartbeat_at`. The first
unchanged heartbeat and one liveness sample per hour are persisted to
`agent_events`; status, pane-summary, control, and connectivity changes are
always persisted as audit events. Sampled heartbeat events are retained for 7
days.

### Local SQLite performance

`server-local` uses one serialized SQLite write connection in WAL mode and a
short-lived read-only connection for each read operation. This lets dashboard
reads proceed while heartbeat or delivery writes commit without sharing a
connection across request threads.

Maintenance expires message claims and agent presence every 5 seconds. The
heartbeat-sample retention pass first runs after 60 seconds, then hourly, and
deletes old samples in bounded batches. History endpoints use opaque cursor
pagination: group messages default to 80 rows; conversation messages, agent
sessions, and agent events default to 100; every history page is capped at 500.
Responses preserve their existing row array and add `next_cursor` for the next
older page.

Run the repeatable temporary-database benchmark with:

```bash
uv run python scripts/benchmark_server_local.py --events 100000 --messages 20000
```

It seeds no live `~/.mcodex` data and prints one JSON document containing row
counts, SQLite query plans, and timings for recent events, group messages, and
pending deliveries. It exits nonzero if the recent-event workload regresses to
a table scan without `agent_events_agent_created_idx`.

## Troubleshooting

### Dashboard shows `No groups yet`

Check the backend directly:

```bash
curl http://127.0.0.1:8765/api/groups
```

From Windows accessing WSL, check the WSL IP:

```bash
hostname -I
curl http://<wsl-ip>:8765/api/groups
```

If the dashboard host cannot reach the backend through its default candidates, set:

```bash
VITE_SERVER_LOCAL_URL=http://<wsl-ip>:8765
```

### `Address already in use` on port 8765

Find existing server processes:

```bash
pgrep -af 'mcodex serve-local|watchfiles'
```

Stop the stale process or use the existing server. Avoid running multiple hot-reload wrappers for the same port.

### `mcodex start <agent>` appears to do nothing

Attach to the tmux session:

```bash
tmux attach -t mcodex-<agent>
```

If the pane says `No saved session found with ID ...`, then `<agent>` is not a valid Codex resume target. Use an existing Codex session id or thread name.

### Agent is visible but stuck `busy`

`busy` means the watcher has not seen a completed Codex turn or the pane has not been stable for `--idle-seconds`, or the watcher recently injected a message. A completed Codex turn is detected from Codex's final `Worked for ...` footer when it is the last visible non-empty pane line. The default message-injection idle threshold is 15 seconds. The pane summary can update after 5 seconds of stability.

`idle` and `offline` are separate states. `idle` means a watcher is still heartbeating and the pane is stable. `offline` means the watcher disconnected, the session ended, or the server has not seen a heartbeat for 15 seconds.

### Need watcher diagnostics

Inspect the newest background logs:

```bash
ls -lt ~/.mcodex/logs/*<agent>*.log | head
tail -n 160 ~/.mcodex/logs/<log-file>
```

For live watcher output without restarting Codex:

```bash
uv run mcodex restart-watch <agent> --dev
```

## Tests

Backend tests:

```bash
uv run python -m unittest discover -s tests -p 'test_*.py' -v
```

Frontend tests and build:

```bash
cd frontend
npm test
npm run build
```

## License

This project is licensed under [0BSD](LICENSE). The local HTTP API has no
authentication. Keep it on loopback unless every client on the reachable
network is trusted. Messages and pane summaries may contain private content.
