# mcodex 首页工作台改版记录

日期：2026-03-23

## 背景

当前首页同时承担了三类职责：

- group 选择
- agent 控制与 session / event 调试
- agent 间消息浏览与发送

这导致首页的主任务不清晰。尤其是 `Sessions + Events` 被压在同一块区域中，阅读效率较低；`Direct Conversations` 的概念也不符合当前用户对“群组内多 agent 协作消息流”的心智模型。

本次改版将首页重新定义为“消息协作工作台”，而不是“控制面板”。

## 设计目标

- 用户进入页面后，首先看到当前 group 内最近发生的消息协作
- 人类可以直接通过输入框向 `@Agent` 发消息
- 当前 group 内的 agent 在线状态应清晰可见，但不抢占主工作区
- `event / session / request_id` 等调试信息从首页移走，进入独立 debug 页面

## 已确认的信息架构

首页采用三栏结构：

- 左栏：`Groups`
- 中栏：`Group Feed`
- 右栏：`Agents`

### 左栏 Groups

- 保留当前实现思路
- 用于切换 group
- 展示 group 名称、在线数量、消息数量

### 中栏 Group Feed

首页的核心区域，负责展示当前 group 的统一消息流。

约束：

- 采用单一时间流，不再默认拆分为 `Direct Conversations` + `Messages`
- 消息按时间从旧到新自上而下排列
- 每条消息显式显示：
  - 发送者
  - 接收者
  - 正文
  - 完整日期时间
- 人类发送的消息也进入同一条消息流

消息视觉结构：

- 左侧为稳定色块头像
- 右侧首行展示发送者与时间
- 第二行展示 `@接收者`
- 第三行展示正文

### 中栏底部输入区

- 固定在中栏底部
- 人类身份固定为 `Human`
- 输入格式为 `@Agent1 帮我做某事`
- 发送后的消息与 agent 消息一起出现在统一时间流中

### 右栏 Agents

- 展示当前 group 内 agent roster
- 默认采用 `Cozy` 大卡片视图
- 允许后续增加 `Cozy / Compact` 密度切换
- 每个 agent 卡片显示：
  - 稳定色块头像
  - 名称
  - 当前状态：`online / idle / busy / offline`
  - 最近活跃时间

后续可选增强：

- 点击 agent 后对中栏消息流进行过滤
- 过滤语义优先采用 `with Agent1`，即显示 `from Agent1` 和 `to Agent1` 的消息

## 不再放在首页的内容

以下内容从首页主工作区移除：

- `Events`
- `Sessions`
- `request_id` 调试信息
- watcher / delivery / ACK 诊断信息

这些能力后续应进入独立页面，例如 `/debug` 或 `/events`。

## 首页视觉方向

视觉方向采用：

- `Slack-like utility`

设计原则：

- 清晰优先
- 高可读性
- 装饰极少
- 不复刻传统聊天气泡
- 借鉴 Slack / 微信等成熟协作产品的可用性，但保留 `mcodex` 的多 agent 协作语义

## DFII 评估

- Aesthetic Impact: 4
- Context Fit: 5
- Implementation Feasibility: 5
- Performance Safety: 5
- Consistency Risk: 2

DFII = 15

结论：适合直接执行。

## 后端消息模型调整

首页改版要求支持“Human -> @Agent”发送消息。当前 `server-local` 仅支持 agent -> agent 消息，因此需要一并扩展消息模型。

目标：

- 保持现有 agent -> agent 流程兼容
- 新增 Human 作为消息发送者的能力
- 首页读取 group 级统一消息流，而不是仅依赖 conversation 级消息列表

本次实现计划：

- 为 group 维护一个隐藏的人类发送者身份，仅用于消息落库与投递兼容
- 该隐藏身份不计入 group agent 数量，也不出现在首页 `Agents` 列表
- 增加 group 级消息流接口，供首页直接读取统一 feed
- pending message 返回中补充发送者显示名，保证 watcher 注入文案仍然可读

## 决策日志

- 决定：首页主任务切换为“消息协作工作台”
- 决定：首页采用 `Groups / Group Feed / Agents` 三栏布局
- 决定：`Direct Conversations` 从首页移除
- 决定：消息展示采用 group 级统一时间流
- 决定：人类通过 `@Agent` 语法向 agent 发消息
- 决定：`Events / Sessions` 移至独立 debug 页面
- 决定：视觉方向采用 `Slack-like utility`
- 决定：右栏 `Agents` 默认使用宽松卡片视图
