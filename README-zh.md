# mcodex

`mcodex` 是一个面向多个本地交互式 Codex 会话的协作与编排层。它把 `tmux`、本地状态服务、后台 watcher 和 React 工作台组合在一起，让多个 Codex agent 可以按 group 组织、互相收发消息，并在浏览器里查看运行状态。

当前项目包含：

- `tmux` 封装：为每个 agent 维护一个独立的 Codex pane
- `server-local`：基于 SQLite 的本地 HTTP 服务，保存 group、agent、会话、消息和事件
- watcher：每个 agent 一个后台进程，负责心跳、消息拉取、队列管理和 tmux 注入
- React 工作台：展示 group 消息流、agent 状态、生命周期控制和 pane 摘要

## 当前模型与重要限制

当前版本里，`agent` 名称会直接传给 Codex 作为 resume 目标：

```bash
codex resume <agent>
```

因此：

- `mcodex start mcodex-doc` 不会创建一个名为 `mcodex-doc` 的全新 Codex 对话
- 它会启动 `tmux` session，并在 pane 中执行 `codex resume mcodex-doc`
- `<agent>` 必须已经是 Codex 可恢复的 session id 或 thread name

如果 Codex 无法恢复该目标，tmux pane 会保留并显示类似诊断信息：

```text
ERROR: No saved session found with ID mcodex-doc.
mcodex: codex exited with status 1
```

这个限制是当前使用方式里最容易误解的部分。启动 agent 前，请先确认对应的 Codex 会话已经存在。

## 环境要求

- Python 3.10+
- `uv`
- `tmux`
- 本地 `codex` CLI
- Node.js `^20.19.0 || >=22.12.0` 与 npm，用于前端工作台

安装 Python 项目环境：

```bash
uv sync
```

除非已经进入项目虚拟环境，否则建议通过 `uv` 运行 CLI：

```bash
uv run mcodex --help
```

## 快速开始

启动本地协调服务：

```bash
uv run mcodex serve-local --host 127.0.0.1 --port 8765
```

在另一个 shell 中启动前端工作台：

```bash
cd frontend
npm ci
npm run dev -- --host 127.0.0.1 --port 5173
```

打开浏览器：

```text
http://localhost:5173
```

如果需要从 Windows 通过 WSL IP 访问，先把两个服务绑定到 Windows 可达的接口。
本地 API 没有身份认证，只应在可信网络中这样做：

```bash
uv run mcodex serve-local --host 0.0.0.0 --port 8765
npm run dev -- --host 0.0.0.0 --port 5173
```

查看 WSL IP：

```bash
hostname -I
```

然后访问：

```text
http://<wsl-ip>:5173
```

启动并进入一个已有 Codex 会话：

```bash
uv run mcodex resume <existing-codex-session-id-or-thread-name> --yolo
```

后台启动，不自动 attach：

```bash
uv run mcodex start <existing-codex-session-id-or-thread-name> --yolo
```

`start` 会输出 tmux session 名称和 attach 命令。

## 常用命令

### `mcodex resume`

```bash
uv run mcodex resume <agent> [options]
```

创建 `tmux` session `mcodex-<agent>`，运行 `codex resume <agent> --no-alt-screen`，启动 watcher，然后 attach 到该 tmux session。如果当前 shell 已经在 tmux 中，则切换到新 session。

常用选项：

```bash
--yolo
--group default
--server-local http://127.0.0.1:8765
--idle-seconds 15
--poll-interval 2.0
--history-limit 100000
--contact-hold-seconds 60
```

`--yolo` 会映射到 Codex 参数：

```bash
--dangerously-bypass-approvals-and-sandbox
```

### `mcodex start`

```bash
uv run mcodex start <agent> [options]
```

使用与 `resume` 相同的 session 创建路径，但不会 attach 到 tmux。成功后输出：

```text
started mcodex-<agent>; attach with: tmux attach -t mcodex-<agent>
```

如果 Codex 立即退出，tmux pane 仍会保留，便于通过前端工作台或 `tmux attach` 查看失败原因。

### `mcodex up`

```bash
uv run mcodex up -c .mcodex
```

从项目目录中的配置一次启动多个已有 Codex 会话。在项目目录放置 `.mcodex`：

```ini
[mcodex]
group = sample
layout = columns
yolo = true
agents = api, worker, reviewer
```

`agents` 的顺序是稳定顺序，也就是 tmux pane 从左到右的顺序：

```text
api | worker | reviewer
```

`group` 省略时默认使用配置文件所在目录名。`cwd` 省略时默认使用配置文件所在目录，因此即使从其他目录执行 `mcodex up -c /path/to/project-a/.mcodex`，Codex 仍会在 `/path/to/project-a` 中启动。当前只支持 `layout = columns`。

tmux workbench 名称由 group 和配置文件所在目录共同决定。例如 `/path/to/project-b/.mcodex` 配置 `group = sample` 时，会创建 `mcodex-group-sample-project-b`，因此可以和另一个目录里的 `sample` workbench 同时存在。如果目录名不够明确，可以在 `.mcodex` 里设置 `session = <label>`。

`up` 会为每个 agent 创建一个 Codex pane，并为每个 pane 启动独立后台 watcher，然后 attach 或切换当前 tmux client 到该 workbench。需要只后台启动时使用 `--detach`，此时命令只输出 attach 命令。如果目标 tmux workbench 已存在、旧的单 agent session `mcodex-<agent>` 已存在，或 server-local 显示该 agent 已有 running session，命令会拒绝继续，避免同一个 Codex 会话被重复 resume。

新建的 `up` workbench 会把每个 pane 顶部标题显示为 `<group>: <agent>`，例如 `sample: reviewer`。这里的 agent 是 `.mcodex` 中 `agents` 配置项里的名字，也就是传给 `codex resume <agent>` 的值。

### `mcodex feed`

```bash
uv run mcodex feed
uv run mcodex feed --since-last
uv run mcodex feed --include-direct
uv run mcodex feed --json
```

只读查看 group feed，不消费 inbox。默认行为面向 agent：输出最近 1 小时的
`pane_summary`，最近项最多 20 条，并额外并入每个 agent 最新一条
`pane_summary` 作为基线上下文。因此实际输出可能超过 `--limit`。
`--since-last` 会在 `~/.mcodex/feed-cursors/` 维护当前 agent/group 的本地游标；
保底 summary 可能在多次 `--since-last` 中重复出现，但游标不会倒退。
`--include-direct` 用于查看接近浏览器 feed 的审计视图，包含 direct messages。

`feed` 是已完成回合视图，不是 busy pane 的实时流。agent 仍在执行时可能暂时没有
新 summary；需要当前进度时先做一次有界 `mcodex wait`，再按名称使用
`mcodex tail`。仅仅缺少进行中内容不代表 watcher 故障。

### `mcodex wait`

```bash
uv run mcodex wait <agent> --group <group> --timeout 300 --lines 80
uv run mcodex wait <agent> --until idle --json
```

等待 group 内某个命名 agent 到达 server-local 状态，默认等到 `idle`，然后输出该
agent 的命名 pane tail。如果超时，命令返回非 0，但仍会尽量输出当前状态和 tail。
长任务等待完成或查看阶段性进度时，用它替代裸 `sleep; tmux capture-pane` 轮询。

### `mcodex tail`

```bash
uv run mcodex tail <agent> --group <group> --wait 35 --lines 55
uv run mcodex tail <agent> --json
```

通过 `mcodex` 只读查看某个运行中 agent 的 tmux pane 最后几行。只有当
`mcodex feed` 或 watcher 捕获的 `pane_summary` 不够完整，且 `mcodex wait` 不适合
当前场景时才使用它。它会通过 server-local 按 agent 名称解析 pane，并校验 group，
因此 agent 不应该直接使用 `tmux capture-pane -pt %7` 这类裸 pane id 命令。

### `mcodex inbox` 和 `mcodex send`

```bash
uv run mcodex inbox
uv run mcodex inbox ack --all
uv run mcodex send <recipient-agent> "message body"
uv run mcodex send --request-id handoff-20260731-01 <recipient-agent> "message body"
```

为当前 agent claim direct messages，并在消息进入当前上下文后 ACK。`resume`
和 `up` pane 会通过 `MCODEX_AGENT`、`MCODEX_GROUP` 和
`MCODEX_SERVER_LOCAL` 提供身份信息。在这些 pane 外运行时，用
`--agent`、`--group` 或 `--server-local` 覆盖。本功能上线前已经存在的
pane 不会自动获得这些环境变量，也需要显式传参。

`inbox ack --all` 和外部 helper 的 `ack --all` 遇到服务端返回 HTTP 404 或
409、已经无法使用的本地残留 claim handle 时，会将其删除、输出为 `Stale`，并继续处理其余 handle。
显式 ACK 单个 stale handle 仍会失败，避免掩盖身份或消息参数错误。

Codex final answer 及其 `pane_summary` 不会自动创建 direct inbox 消息。如果结果
必须由另一个 agent 独立消费，发送方必须显式执行 `mcodex send <recipient> ...`，
不能只在 pane 文本中写“给某 agent”。

`send` 会携带 client request id，因此超时重试是安全的。如果发送命令失败并
打印了 `request_id=...`，重试时复用同一个 `--request-id`，不要直接再发一条新
消息。group、sender、recipient、body 和 request id 全部一致时，server 会返回
已有消息。

### `mcodex issue`

```bash
uv run mcodex issue --title "API call failed" api_failed "GET /api/groups returned 503"
cat /tmp/mcodex-issue.md | uv run mcodex issue --stdin watcher_incomplete
uv run mcodex issue --body-file /tmp/mcodex-issue.md tmux_fallback_used
uv run mcodex issue --agent mcodex-dev handle <issue-id>
```

用于报告 mcodex 机制本身的问题，不用于汇报业务任务状态。Issue 会持久化到
`~/.mcodex/local.db`，并显示在前端当前 group 的 Status 页面。适用场景包括：
agent 被迫使用 tmux fallback、watcher/API 数据不完整、消息投递可疑、或前端显示
和实际状态不一致。

Issue 的处理状态由修复 mcodex 的 agent/维护者填写，不由普通使用者或报告者确认。
完成工具修复或运维清理并验证后，使用
`mcodex issue --agent <maintainer-agent> handle <issue-id>` 标记为
`handled`。如果误标或问题复发，再用 `mcodex issue reopen <issue-id>`。

支持的类型：

```text
api_failed
watcher_incomplete
tmux_fallback_used
message_delivery_suspect
dashboard_mismatch
```

命令形态是 `mcodex issue [options] <type> <body...>`。使用位置参数正文时，
把 `--title`、`--stdin`、`--body-file` 等 option 放在 `<type>` 前；多行或 shell
敏感文本优先用 `--stdin` 或 `--body-file`。

### `mcodex serve-local`

```bash
uv run mcodex serve-local --host 127.0.0.1 --port 8765
```

运行本地 HTTP API。默认状态数据库为：

```text
~/.mcodex/local.db
```

CLI 默认连接的本地服务地址为：

```text
http://127.0.0.1:8765
```

## 项目结构

```text
.
├── README.md                         # 英文入口文档
├── README-zh.md                      # 中文入口文档
├── pyproject.toml                    # Python 包与 CLI 配置
├── src/mcodex/
│   ├── cli.py                        # CLI、tmux session、watcher 逻辑
│   ├── server_local.py               # 本地 HTTP API 与 SQLite 状态存储
│   └── __main__.py                   # python -m mcodex 入口
├── frontend/
│   ├── package.json                  # React/Vite 前端命令
│   └── src/                          # 工作台 UI 与 API client
├── tests/                            # 后端单元测试
└── docs/                             # 架构与设计记录
```

## 架构概览

当前本地闭环由三层组成：

- `frontend`：浏览器工作台，负责 group、消息流、agent roster、状态详情和控制按钮
- `server-local`：本地权威状态源，负责消息派发、聊天记录、agent 状态、session 和事件
- `mcodex`：本地客户端，负责启动 Codex、维护 tmux pane、上报状态、接收待投递消息并注入 Codex

未来架构中预留了云端 `server` 的边界，但当前版本只实现本地闭环，不实现跨设备云端通信。

## Session 行为

每个 agent 对应一个 tmux session：

```text
mcodex-<agent>
```

如果同名 session 已存在，`mcodex` 会拒绝启动第二份实例，并提示手动 attach：

```bash
tmux attach -t mcodex-<agent>
```

Codex pane 命令没有使用 `exec codex`。当 Codex 退出时，`mcodex` 会打印分隔线诊断块，并在同一 pane 中打开 shell。这能保留短生命周期失败现场，方便前端和 tmux 查看。

## Watcher 行为

每次 `start` 或 `resume` 都会为该 agent 启动一个后台 watcher。

watcher 负责：

- 确保目标 group 存在于 `server-local`
- 注册 agent session
- 如果启动时 `server-local` 不可用，保持运行并每 60 秒重试注册
- 上报 `busy` 或 `idle` 心跳
- 拉取发给该 agent 的 pending messages
- 将消息缓存到 `~/.mcodex/<agent>/queue.json`
- 等待 pane 稳定后，把队列消息注入 tmux pane
- 注入成功后向 `server-local` ACK
- 当 pane 或 tmux session 结束时标记 session disconnected

默认时间参数：

```text
消息注入前 idle 等待：15 秒
pane summary 捕获等待：5 秒
轮询间隔：2 秒
当前联系人保持时间：60 秒
agent 心跳超时：15 秒
server-local 启动重试：60 秒
```

后台 watcher 会把 stdout 和 stderr 写入轮转日志：

```text
~/.mcodex/logs/
```

日志文件名包含 UTC 启动时间、agent id 和短随机后缀。每个后台 watcher 在 active log 达到 10 MiB 时轮转，并保留 2 个备份分段。下一次后台 watcher start 或 restart 时，清理会同时执行 80 个文件和总计 256 MiB 的上限，并保护本次启动 watcher 的全部日志分段。后台 watcher 默认带 `--log-events`，所以日志里会包含启动、变化的心跳、队列、注入、连接状态、退出事件以及 traceback。未变化且成功的 heartbeat 每小时采样记录一次；错误和运行状态变化始终记录。显示在 tmux 中的开发 watcher 仍直接输出到可见 pane。

通过 tmux `display-message` 展示的队列和重试提示只会发送给 attach 到该 watcher
所属 tmux session 的 client。如果三个 `mcodex up` workbench 分别开在三个终端里，
某个 workbench 内 pane 的 queued message 只会在对应终端闪烁，不会打到最后活跃的
tmux client。

消息注入使用 tmux key input。短消息使用 literal typing；长消息使用 tmux paste buffer，把多行正文作为一个整体交给 Codex，避免终端输入区拆段。注入到 Codex 的文本先列出同组其他活跃 agent，然后是一条或多条 direct message：

```text
Other active agents in group [<group-id>]: <同组其他活跃 agent>
Message to you [<recipient-agent-id>] from [<sender>] [sent_at=<UTC ISO 时间>, age=<距今时间>]: <body>
```

`sent_at` 是 `server-local` 记录的消息创建时间。`age` 是 watcher 注入时计算出的相对时间，例如 `42s ago`、`12m ago`、`3h ago`、`2d ago`，接收方 agent 可以据此判断 queued message 是否已经过期。

watcher 会在 `--contact-hold-seconds` 时间内保持当前发送者优先。其他发送者的消息会等到当前发送者静默足够久后再投递，避免多个对话交错打断。只有停止、暂停、所有权安全这类协调消息才应使用紧急格式：direct message 正文第一条非空行以 `STOP:`、`URGENT:` 或 `MCODEX-URGENT:` 开头时，watcher 会绕过 contact hold 和 idle 稳定等待。`URGENT:` 和 `MCODEX-URGENT:` 在 busy 时仍使用 `Tab` 排队；`STOP:` 更强：watcher claim 成功后先发送 `Escape` 中断当前 Codex turn，再用 `Enter` 提交 STOP。中断或注入失败会 release claim，不会 ACK。ACK 只表示输入已进入 Codex，不表示 Codex 已执行或回复。

在注入队列消息之前，watcher 会先通过 `server-local` claim 这些消息。如果另一个 API client 已经 claim 了某条消息，watcher 会丢弃本地 stale copy，不会再注入到 tmux。tmux 注入成功后会带 claim id ACK；如果 tmux 注入失败，会 release claim，方便其他 client 后续重新 claim。

只重启 watcher、不重启 Codex：

```bash
uv run mcodex restart-watch <agent>
```

这会保留现有 tmux session 和 Codex pane，停止旧 watcher，并在同一个 pane 上启动新 watcher。它会优先复用 server-local 里该 agent 当前 running session 的 `tmux_session` 和 `pane_id`，所以既支持旧的 `mcodex-<agent>` 单 session，也支持 `mcodex up` 创建的 group 多 pane session。非 dev 模式会输出新的后台 watcher 日志路径。需要实时看日志时使用：

```bash
uv run mcodex restart-watch <agent> --dev
```

升级 watcher 投递逻辑后，建议重启正在运行的 watcher：

```bash
uv run mcodex restart-watch <agent> --group <group>
```

### API agents

受信任的局域网 client 可以不经过 tmux 直接加入已有 group。需要先用
`POST /api/groups` 创建 group，或先启动一个该 group 内的 `mcodex` session
让 watcher 创建 group。

```bash
curl -s -X POST http://127.0.0.1:8765/api/groups/mcodex/agents \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"codex-app-pm","display_name":"Codex App PM","transport":"api"}'
```

API agent 默认是 `idle`，可以主动设置 `idle`、`busy` 或 `offline`。如果一小时内没有带身份的活动，会自动变成 `offline`。Direct message 必须通过 inbox claim/ACK 消费；读取 `GET /api/groups/{group}/messages` 只是审计视图，不会清掉 watcher 队列。

在 `mcodex resume` 或 `mcodex up` 的 tmux pane 内，优先用内置 helper：

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

`resume` 和 `up` 会在 Codex pane 里设置 `MCODEX_AGENT`、`MCODEX_GROUP` 和
`MCODEX_SERVER_LOCAL`，所以 helper 可以自动识别身份。在 pane 外运行时用
`--agent`、`--group` 或 `--server-local` 覆盖。本功能上线前已经存在的
pane 不会自动获得这些环境变量，也需要显式传参。

外部 Codex/Windows 会话可以使用 helper：

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

在 Windows、多行文本、反斜杠、引号、JSON 或长 handoff 场景，不要把正文放在
命令行 argv 里。改用 stdin 或 UTF-8 文件，避免 Windows/WSL 引号转义截断消息：

```bash
cat /tmp/mcodex-message.md | uv run python scripts/mcodex_api_agent.py send mcodex-doc --stdin
uv run python scripts/mcodex_api_agent.py send mcodex-doc --body-file /tmp/mcodex-message.md
```

如果 helper `send` 超时或客户端断开，重试时复用输出里的 `--request-id`。不要
盲目发送第二份内容。

helper 状态文件保存 API-agent 身份和后续 ACK/release 所需的本地 claim handle。
如果 Windows shim 调用 WSL helper 时传入 `/mnt/<drive>/...` 状态目录，并且该挂载返回
I/O error，helper 会自动切到 WSL 本地状态目录 `~/.mcodex/api-agents/<agent>`。
如果旧身份只存在已经不可读的 Windows 状态文件里，需要显式传 `--agent`、`--group`
和 `--base-url`，或重新执行 `register`，再 claim inbox。

外部 agent 只能把 helper `tail` 当作只读诊断兜底。优先使用 `feed`，因为它输出更短，并保留 watcher 捕获的状态 summary。如果因为 mcodex 数据缺失或可疑而使用 fallback，agent 也应该提交一条 `issue`，让维护者能看到工具机制缺口。

## Pane 摘要

当 tmux pane 5 秒内不再变化时，watcher 会捕获最近 2000 行，并提取最近的 Codex 风格总结块。

解析器会识别普通整行横线、Codex 常见的长 box-drawing 横线和 `Worked for ...` footer。当 watcher 已把 pane 判为 idle 且存在完整的 prompt-delimited assistant block 时，优先保留该完整 block，避免表格内部渲染出的整行横线截断答案；没有最终输入提示的已完成回合才回退到分隔符解析。候选摘要会在 Codex 输入提示行 `›` 前截断，避免把人类正在输入的文本当成 agent summary。保存的摘要最多 20000 字符，需要截断时省略中间部分并保留首尾。

watcher 只会发送当前 watcher 运行期间尚未发送过的摘要，判断时会做空白归一化。前端把它作为 group 消息流里的 `pane_summary` 消息展示，并按 agent 和正文去重。它不是 agent 状态字段，也不是完整终端镜像。

## 本地消息归档

`server-local` 会把直接消息和 pane 摘要在 SQLite 中至少保留 30 天；更旧的
row 之后才可能进入归档。只有所有 delivery 都处于 `acked` 或 `canceled`
状态的直接消息才可归档。任何 delivery 仍为 `pending`、`claimed` 或其他非终态的
消息都会继续留在 SQLite 中。Pane 摘要则只按时间判断是否可归档。

归档 segment 是由应用管理、只写一次且不覆盖已有文件的 gzip JSONL。使用默认
数据库 `~/.mcodex/local.db` 时，归档写入
`~/.mcodex/archives/<encoded-group>/<yyyy-mm>/`。使用自定义
`serve-local --db-path` 时，默认 artifact root 是该数据库同目录的 `archives/`。
每个 segment 最多包含 5,000 条 record，每次 pass 最多写入 10 个 segment。
每次启动 `server-local` 进程后约 60 秒执行首次 pass；该进程持续运行时，
之后每 24 小时执行一次。

归档检查只通过 CLI 提供，且是只读操作：

```bash
uv run mcodex archive list
uv run mcodex archive list --group default --kind messages --json
uv run mcodex archive show <archive-id>
```

`mcodex archive list` 只读取 manifest metadata，不打开归档文件。
对于非默认数据库，`archive list` 和 `archive show` 都必须用 `--db-path` 指向
相应数据库。`archive show` 默认使用该数据库同目录的 `archives/`；
`--archive-root` 可以覆盖该 artifact root。它会校验文件的 SHA-256，完整消费
gzip stream，并把每一行 JSONL 解析为只包含有限数值的 JSON object。无效
JSON、非 object JSON 或非有限数值常量都会被拒绝。全部检查通过前不会向
stdout 写出任何内容。

常规 dashboard 和 feed 请求不会读取归档。归档无限期保留；当前没有归档恢复或
删除功能。消息归档后，idempotency request key 仍保留在 SQLite 中。在同一
group 和 sender scope 内，使用相同 recipient 和去除首尾空白后的 body
重试该 key，会返回已归档的原 message ID；更改 recipient 或 body 则会产生冲突。

## 前端工作台

前端位于 `frontend/`：

```bash
cd frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

当前视图：

- `Workspace`：group 列表、统一消息流、消息输入框、agent roster
- `Status`：group mcodex issues、agent session、事件、pane 摘要、start/stop/reconnect 控制
- `Performance`：本地 HTTP、SQLite、heartbeat、消息投递、SSE 和 maintenance 指标
- `Idle alert`：导航侧栏里的浏览器端 opt-in 提示。启用后，前端会监控所有可见 group；某个 group 从“非全 idle”变成“全部 agent idle”时，只触发一次系统通知和短提示音。浏览器 Notification 权限和音频播放都需要点击启用按钮。

Workspace 消息流默认只加载最近 80 条 group message。group 卡片仍显示完整持久化消息数。
conversation detail 消息页默认返回 100 条、最多返回 500 条，并通过 `next_cursor`
请求下一页更旧数据。

消息输入规则：

- 消息必须以 `@recipient` 开头
- recipient id 可包含字母、数字、`.`、`_` 和 `-`
- 示例：

```text
@mail summarize the last failure
```

API base 选择规则：

- 如果设置了 `VITE_SERVER_LOCAL_URL`，前端只使用该地址
- 否则先尝试 `http://<current-page-host>:8765`
- 再回退到 `http://127.0.0.1:8765`
- 成功的 API base 会被后续请求复用
- 每个候选地址的请求超时为 1600 ms
- SSE 已连接时，事件会触发经过防抖的、按资源类型区分的局部刷新
- SSE 断开后，前端才会每 5 秒完整刷新 group、消息、issue 和 agent 详情，直到重新连接
- subscriber 队列溢出时只合并产生一个 `resync_required` 事件，并触发一次完整刷新

Performance 视图只在可见时每 5 秒请求一次指标。它在浏览器内存中最多保留 720 个
解析后的采样点，对应一小时历史；刷新页面会清空这些历史数据。

WSL 场景下，下面两个前端地址可能都有效，取决于端口转发方式：

```text
http://127.0.0.1:5173
http://<wsl-ip>:5173
```

如果浏览器所在主机无法通过默认候选地址访问 WSL 后端，可以显式指定后端：

```bash
VITE_SERVER_LOCAL_URL=http://<wsl-ip>:8765 npm run dev -- --host 0.0.0.0 --port 5173
```

## 本地 HTTP API

前端和 watcher 使用这些本地路由：

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

`/api/events/stream` 是 SSE stream。前端通过它在服务端状态变化后刷新界面，而不是只依赖轮询。

`GET /metrics` 由现有的 `serve-local` HTTP 端口提供，返回 Prometheus 文本；无需运行
Jaeger、Grafana、OpenTelemetry collector，也不会额外开启监听端口。指标 label 只使用
有界枚举值和路由模板，不会把 agent、group、message、conversation、session、request
或文件系统路径等标识作为 label 暴露。即使指标初始化或某次抓取失败，常规协调 API
仍会继续运行；Performance 视图会显示抓取失败，并在下一次可见的 5 秒间隔重试。

start、stop、reconnect 这类控制请求会返回 `request_id`。后续
`session_registered`、`session_disconnected` 和实时 `agent_updated` 事件在可用时
会带上同一个 id，便于前端区分“已请求”和“已确认”的生命周期变化。

普通未变化 heartbeat 会更新 `agents.last_heartbeat_at`。第一条未变化
heartbeat 以及之后每小时一条存活采样会写入 `agent_events`；状态、pane summary、
control 和连接变化始终作为审计事件保存。heartbeat 采样事件保留 7 天。

### 本地 SQLite 性能

`server-local` 使用 WAL 模式下的单个串行 SQLite 写连接，每个读操作使用短生命周期的
只读连接。这样 dashboard 读取可在 heartbeat 或 delivery 写事务提交期间继续进行，
不同请求线程也不会共享同一个读连接。

maintenance 每 5 秒清理过期 message claim 和 agent presence。heartbeat 采样保留任务
首次延迟 60 秒运行，之后每小时运行一次，并以有界批次删除旧采样。历史接口使用不透明
cursor 分页：group messages 默认 80 条；conversation messages、agent sessions 和
agent events 默认 100 条；所有历史分页上限均为 500。响应保留原有数组字段，并增加
`next_cursor` 用于请求下一页更旧数据。

可用以下命令运行可重复的临时数据库 benchmark：

```bash
uv run python scripts/benchmark_server_local.py --events 100000 --messages 20000
```

它不会写入真实的 `~/.mcodex` 数据，只输出一份 JSON，包含行数、SQLite query plan，
以及 recent events、group messages 和 pending deliveries 的耗时。如果 recent-event
查询退化成未使用 `agent_events_agent_created_idx` 的全表扫描，命令会以非零状态退出。

## 测试

后端单元测试：

```bash
uv run python -m unittest discover -s tests -p 'test_*.py' -v
```

前端测试与构建：

```bash
cd frontend
npm test
npm run build
```

## 排障

### 前端显示 `No groups yet`

先直接检查后端：

```bash
curl http://127.0.0.1:8765/api/groups
```

如果从 Windows 访问 WSL，检查 WSL IP：

```bash
hostname -I
curl http://<wsl-ip>:8765/api/groups
```

如果浏览器主机无法通过默认候选地址访问后端，启动前端时设置：

```bash
VITE_SERVER_LOCAL_URL=http://<wsl-ip>:8765
```

### 端口 8765 显示 `Address already in use`

查找已有服务进程：

```bash
pgrep -af 'mcodex serve-local|watchfiles'
```

停止旧进程，或复用已有 server。避免为同一个端口同时运行多个热重载包装器。

### `mcodex start <agent>` 看起来没有反应

先 attach 到对应 tmux session：

```bash
tmux attach -t mcodex-<agent>
```

如果 pane 中显示 `No saved session found with ID ...`，说明 `<agent>` 不是有效的 Codex resume 目标。请换成已有 Codex session id 或 thread name。

### Agent 一直显示 `busy`

`busy` 表示 pane 在 `--idle-seconds` 阈值内仍有变化，或 watcher 刚刚注入过消息。默认 idle 阈值是 15 秒。pane summary 仍可能在 5 秒稳定后更新。

## 设计记录

更多中文设计与架构上下文见：

- [本地化架构重构方案](docs/2026-03-20-mcodex-local-architecture.zh-CN.md)
- [首页工作台改版记录](docs/2026-03-23-frontend-home-workspace.zh-CN.md)

## 许可证

本项目采用 [0BSD](LICENSE)。本地 HTTP API 没有身份认证；除非可访问该服务的客户端
全部可信，否则只在 loopback 上运行。消息与 pane 摘要可能包含私密对话内容。
