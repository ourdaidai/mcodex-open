# mcodex 本地化架构重构方案

日期：2026-03-20

## 1. 背景

当前 `mcodex` 已经验证了 A2A 直接沟通可行，但现状仍是实验形态：

- 消息来源依赖本地文件邮箱
- watcher、队列、联系人优先级和 tmux 注入都堆在 `mcodex` 进程内
- 没有统一状态源
- 没有可供前端直接消费的实时事件流
- 聊天记录、运行状态、在线状态没有正式的数据模型

这套实现适合验证交互路径，不适合继续扩展到多 group、前端监控、本地统一编排，以及未来的跨设备云端通信。

## 2. 目标

把现有实现重构为四层架构：

- `frontend`
  - 本地 Web 面板
  - 负责按 group 展示 agent、聊天记录、运行状态
  - 支持基础控制和发消息
- `server-local`
  - 本地权威服务
  - 负责消息派发、聊天记录保存、group 组织、agent 状态汇总
- `mcodex`
  - 客户端
  - 负责启动 Codex、维护 tmux pane、接收待投递消息、回报状态
- `server`
  - 未来云端服务
  - 用于跨设备 agent 通信
  - 本期不实现，只预留接口边界

## 3. V1 范围

本期只做本地闭环：

- `frontend + server-local + mcodex`
- 不实现云端 `server`
- 不兼容旧 `~/.codex-mail` 作为运行时后端
- V1 只支持 group 内点对点 direct message
- V1 必须实现 group

group 的约束如下：

- 一个 agent 在 V1 只属于一个 group
- 一个 group 下有多个 agent
- group 主要用于按项目分隔 agent 视图和消息上下文
- 虽然 agent 在同一个 group 中，但 V1 仍然只允许点对点发消息
- 不实现 group broadcast

## 4. 组件职责

### 4.1 server-local

`server-local` 是本地唯一真源，负责：

- 保存 group、agent、conversation、message、delivery、event
- 给 frontend 提供 HTTP 查询接口
- 给 frontend 和 `mcodex` 提供实时事件流
- 决定哪些消息是 pending
- 保存聊天记录和系统事件
- 汇总在线、离线、忙、空闲状态

建议持久化统一使用 SQLite。

### 4.2 mcodex

`mcodex` 保留 tmux 宿主与 Codex 注入能力，但降级为客户端：

- 启动 `codex resume <agent> --no-alt-screen`
- 向 `server-local` 注册 agent session
- 上报 heartbeat 和 pane activity
- 接收发给本 agent 的 pending message
- 本地判断何时可以安全注入
- 注入成功后回 ack

`mcodex` 不再负责：

- 扫描文件 inbox
- 维护 cursor
- 写 `queue.json`
- 直接把文件邮箱作为真源

### 4.3 frontend

frontend 第一版按 group 展示：

- Group 列表页
- Group 详情页
- Agent 详情页或抽屉
- 聊天页

发消息必须在 group 上下文内完成，并且通过 `@agent-id` 指定收件人。

例如：

```text
@task-loop 帮我检查 watcher 的状态切换逻辑
```

规则固定为：

- 只接受消息开头的第一个 `@agent-id`
- 只允许单一收件人
- 若不在当前 group 中，拒绝发送

## 5. 数据模型

V1 最小模型如下：

### groups

- `group_id`
- `name`
- `created_at`
- `archived_at`

### agents

- `agent_id`
- `display_name`
- `group_id`
- `status`
- `last_heartbeat_at`
- `last_seen_at`
- `created_at`
- `updated_at`

### agent_sessions

- `session_id`
- `agent_id`
- `tmux_session`
- `pane_id`
- `cwd`
- `status`
- `started_at`
- `ended_at`

### conversations

- `conversation_id`
- `group_id`
- `participant_a`
- `participant_b`
- `created_at`
- `updated_at`

约束：

- 同一个 group 内，`participant_a + participant_b` 唯一
- participant 排序后存储，避免 A/B 与 B/A 生成两条会话

### messages

- `message_id`
- `conversation_id`
- `group_id`
- `sender_agent_id`
- `recipient_agent_id`
- `body`
- `created_at`

### message_deliveries

- `message_id`
- `recipient_agent_id`
- `state`
- `delivered_at`
- `acked_at`
- `error`

### agent_events

- `event_id`
- `group_id`
- `agent_id`
- `session_id`
- `type`
- `payload_json`
- `created_at`

## 6. 状态语义

V1 暴露四种运行状态：

- `offline`
- `online`
- `busy`
- `idle`

含义如下：

- `offline`
  - 未连接，或 session 已结束
- `online`
  - 已连接，但尚未进入明确的忙闲判断
- `busy`
  - pane 活动持续变化，或客户端主动上报正在生成
- `idle`
  - 在线且超过阈值没有明显 pane 活动变化

## 7. 消息流

消息链路固定为：

1. frontend 在某个 group 页面中输入 `@target-agent ...`
2. `server-local` 解析收件人
3. 校验 sender 与 recipient 同属当前 group
4. 创建或定位 direct conversation
5. 保存 message 和 pending delivery
6. 若目标 agent 在线，则实时下发给对应 `mcodex`
7. `mcodex` 本地排队，等待 pane 可安全注入
8. 采用 tmux literal input + `Tab` 提交
9. 注入成功后回 ACK
10. `server-local` 更新 delivery 状态

## 8. V1 API

### HTTP

- `GET /api/groups`
- `POST /api/groups`
- `GET /api/groups/:groupId/agents`
- `GET /api/groups/:groupId/conversations`
- `GET /api/groups/:groupId/conversations/:conversationId/messages`
- `POST /api/groups/:groupId/messages`
- `GET /api/agents/:agentId`

### 实时通道

V1 目标是采用 Socket.IO，但第一批实现可以先完成：

- SQLite 真源
- HTTP 查询与写入
- `mcodex` 本地服务启动入口

随后接入：

- dashboard 实时订阅
- `mcodex` 客户端实时连接
- delivery ACK 和 presence 广播

## 9. 第一批实现落点

为了避免一次性重构过大，第一批先完成：

- 新增 `server-local` Python 模块
- SQLite schema 初始化
- group 与 agent 的最小管理能力
- group 内 direct message 的创建规则
- 基础 HTTP API
- `mcodex serve-local` 子命令

这一批代码的目标不是一次完成全部架构，而是把“本地真源”从概念变成可运行实体，为后续前端与 client 常连提供稳定基础。

## 10. 后续实现顺序

推荐按以下顺序推进：

1. 完成 `server-local` 的 presence、session、delivery 管理
2. 让 `mcodex` 改为向 `server-local` 注册与收消息
3. 去掉文件 inbox / cursor / queue 运行时依赖
4. 落 React frontend
5. 接上 Socket.IO 实时更新
6. 预留云端 `server` 同步边界

## 11. 当前实现进度

截至 2026-03-20，仓库内已经完成以下落地：

- Python 项目已切到 `uv` 管理，并整理为 `src/mcodex` 包结构
- `server-local` 已实现 SQLite schema、group/agent/session/conversation/message/delivery 基础模型
- `mcodex` 已硬切到 `server-local` 链路，不再兼容旧文件邮箱运行时
- `mcodex resume/watch` 已支持：
  - agent session 注册
  - heartbeat 上报
  - pending message 拉取
  - tmux 注入后 ACK
- frontend 已落最小 React + Vite 面板，支持：
  - 按 group 浏览 agent
  - 查看 session、conversation、message
  - 在当前 group 下用 `@agent-id` 发送 direct message
- `agent_events` 已接入：
  - session register / heartbeat / disconnect
  - direct message sent / pending / acked
  - start / stop / reconnect 请求事件
- frontend 已显示选中 agent 的事件时间线
- `start / stop / reconnect` 已补 request id 闭环：
  - 控制接口返回 `request_id`
  - 前端显示 requested / confirmed 状态
  - `session_registered / heartbeat / session_disconnected` 会携带对应 `request_id`
- `server-local` 已提供基础控制接口：
  - `POST /api/agents/:agentId/start`
  - `POST /api/agents/:agentId/stop`
  - `POST /api/agents/:agentId/reconnect`
- CLI 已新增无 attach 的 `mcodex start`，供 `server-local` 在本机后台拉起 agent
- frontend 已从纯轮询切到 SSE 事件流驱动刷新
- frontend 当前按区域增量刷新：
  - group 列表
  - group 内 agent / conversation
  - 当前 conversation messages
  - 当前 agent sessions / events
- `server-local` 已提供：
  - `GET /api/events/stream`
  - 变更后广播 `group_updated / agent_updated / message_created / message_delivery_updated / agent_control`

当前仍未完成的部分：

- 当前实时层使用 SSE，尚未切到 Socket.IO
- `server-local` 的 `start` 当前依赖本机拉起 `mcodex start`，还没有完整的作业编排层
- 云端 `server` 仍然只停留在架构边界定义

## 12. 下一步执行顺序

在当前代码基础上，建议按下面顺序继续推进：

1. 评估是否从 SSE 升级到 Socket.IO，并统一 agent / dashboard 实时协议
2. 收敛 `start / stop / reconnect` 的本地作业编排与状态确认
3. 增加更完整的 agent 事件流与 group 级事件聚合
4. 为未来云端 `server` 预留同步协议和身份边界
