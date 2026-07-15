# Phase F：SIP 呼入 Dispatch-driven Worker 正式设计

最后更新：2026-07-14

## 1. 文档定位

本文档定义 AI Call 正式 SIP 呼入能力的目标架构、运行边界、数据契约、失败补偿和验收标准。

Phase F 只解决一个问题：

> 当客户拨打公司公开号码时，self-hosted LiveKit SIP 能把每通来电送入独立 Room，并通过 dispatch-driven Agent worker 启动现有 Qwen Realtime 通话能力，最终复用 P1 打断、录音、对话、转人工、事件和通话后分析闭环。

Phase F 采用正式生产目标设计，不先建设一套由 webhook 在 API 进程内临时启动 Agent 的过渡运行链路。

本文档是设计基线，不代表当前代码已经实现 SIP 呼入，也不代表 trunk、dispatch rule、Agent worker 或真实号码已经验收。

实现前必须先读：

1. [phase-a-e2e-core-engine.md](phase-a-e2e-core-engine.md)
2. [phase-e-sip-minimal-entry-design.md](phase-e-sip-minimal-entry-design.md)
3. [phase-e-sip-barge-in-p1-design.md](phase-e-sip-barge-in-p1-design.md)
4. [phase-b3-minimal-handoff-design.md](phase-b3-minimal-handoff-design.md)
5. [phase-b5-handoff-semantic-snapshot-contract.md](phase-b5-handoff-semantic-snapshot-contract.md)
6. 当前代码中的 `app/api/v1/ai_call/`、`app/services/ai_call/` 和 `deploy/livekit-egress/`

## 2. 目标、约束和成功标准

### 2.1 目标

1. 支持真实号码呼入，并为每通电话创建独立 LiveKit Room。
2. 通过 LiveKit explicit agent dispatch 将每通呼入分配给可用 Agent worker。
3. 每通通话由独立 Job 持有 Room、Qwen 和实时媒体生命周期。
4. 复用现有 `RealtimeCallAgentRunner`、Qwen Realtime、SIP P1、录音、对话、转人工和通话结束能力。
5. 支持多个 worker、并发通话、优雅发布和单通故障隔离。
6. 保证来电认领、webhook、worker 重启和控制命令具有幂等性。
7. 呼入初始定位为“公司综合接待”，同一通话内可以切换业务主题，不因主题切换重建 Room 或 Qwen Session。

### 2.2 已知约束

1. 当前项目使用 self-hosted LiveKit，不以 LiveKit Cloud 为前提。
2. 当前依赖只有 `livekit` 和 `livekit-api`，尚未引入 `livekit-agents`。
3. 当前实时运行链由 API 进程内的 `AiCallOrchestrator` 主动创建和持有。
4. 当前 `InMemorySessionRegistry`、`InMemoryEventStore` 和 Runner 内部任务均为进程内状态，不能直接作为多 worker 的跨进程事实源。
5. 当前 `LiveKitRoomAudioTransport` 会自行签 Token、连接 Room 和断开 Room，不能直接用于已经由 `JobContext` 持有的 Room。
6. 当前转人工控制会直接调用 API 进程中的 Orchestrator；呼入 worker 化后必须补跨进程控制边界。
7. Qwen Realtime Session 无法在 worker 崩溃后原样恢复，正式设计不能承诺模型会话无感续接。

### 2.3 成功标准

1. 一次真实呼入只生成一条 `entry_type=sip_inbound` 通话记录。
2. 客户接通后能听到开场白，并与 Qwen 完成双向语音对话。
3. AI 播放期间客户插话时，现有 SIP P1 打断策略不退化。
4. 录音、分轨、对话文本、关键事件和通话后语义分析可按 `call_id` 查询。
5. 转人工后 AI 停止播放，坐席加入同一 Room 后能与客户双向通话。
6. 客户挂机、系统挂断、worker 失败和 Room 结束都能进入明确终态。
7. 重复 webhook、Job 重派和控制命令重试不产生重复记录或重复副作用。
8. 双 worker 部署和滚动发布不主动中断已有通话。

## 3. 当前代码事实

本节只记录当前 checkout 已核对事实，不把设计目标写成已实现能力。

### 3.1 当前运行链

当前实时主链是：

```text
AiCallService
  -> AiCallOrchestrator
  -> RealtimeCallAgentRunner
  -> LiveKitRoomAudioTransport
  -> Qwen Realtime
```

其中：

1. `AiCallOrchestrator.create_web_session(...)` 创建 Web Room 和 Agent。
2. `AiCallOrchestrator.create_sip_session(...)` 创建 SIP 外呼 Room 和 Agent。
3. `LiveKitSipClient.create_participant(...)` 只负责外呼 `CreateSIPParticipant`。
4. `LiveKitRoomAudioTransport.start(...)` 在连接 Room 前记录目标 participant identity，并只接收该 identity 的音轨。
5. `RealtimeCallAgentRunner` 已包含 Qwen 连接、音频桥、播放、P1 打断、结束判断和转人工挂起能力。
6. `CallSession` 当前未携带 `entry_type`，SIP 特性仍存在 identity 约定依赖。

### 3.2 当前持久化能力

当前已具备：

1. `ai_call_record`：保存 `call_id`、`entry_type`、Room、用户 participant identity、状态和终态。
2. `ai_call_event`：保存低频关键事件，`event_id` 唯一。
3. `ai_call_recording` 和 `ai_call_recording_track`：保存混音与分参与方录音。
4. 对话文本、handoff 和语义分析相关表及服务。
5. 录音和语义分析 reconciliation worker。

当前缺口：

1. `ai_call_record` 只有 SIP 外呼被叫号码字段，没有呼入主叫号码和被叫号码语义字段。
2. 没有 SIP 呼入幂等键、trunk ID、dispatch rule ID 的稳定字段。
3. 没有跨进程实时控制命令表。
4. worker 进程内产生的事件和对话不能依赖 API 进程中的内存 listener 自动持久化。

### 3.3 当前 webhook

当前 `/ai-call/livekit-webhook` 已完成 LiveKit webhook 签名验证和异步调度，但业务处理主要覆盖 `participant_left`。

Phase F 需要扩展到：

1. `participant_joined`：记录 SIP caller 已进入 Room，并参与幂等认领。
2. `participant_left`：识别远端挂机并补充终态。
3. `room_finished`：在 worker 未完成清理时执行最终 reconciliation。

webhook 不是音频通道，也不是 Phase F 正常启动 Agent 的入口。

## 4. LiveKit 官方模型和采用方式

LiveKit SIP 呼入由三个边界组成：

1. inbound trunk：定义哪些号码和 SIP 来源可以进入 LiveKit SIP。
2. dispatch rule：决定 caller 进入哪个 Room。
3. agent dispatch：决定哪个 Agent worker 接管该 Room。

Phase F 采用：

1. `SIPDispatchRuleIndividual`：每通 caller 创建独立 Room。
2. `roomConfig.agents`：显式派发名为 `ai-call-inbound` 的 Agent。
3. `JobContext`：向每个 Job 提供 Room、job metadata 和生命周期。
4. raw track processing：只使用 LiveKit Agents 的 dispatch/job/Room 能力，不强制迁移到 STT -> LLM -> TTS 三段式 `AgentSession`。

官方参考：

1. [Inbound trunk](https://docs.livekit.io/telephony/accepting-calls/inbound-trunk/)
2. [Dispatch rule](https://docs.livekit.io/telephony/accepting-calls/dispatch-rule/)
3. [Agent dispatch](https://docs.livekit.io/agents/server/agent-dispatch/)
4. [Agent server lifecycle](https://docs.livekit.io/agents/server/lifecycle/)
5. [Job lifecycle](https://docs.livekit.io/agents/server/job/)
6. [Self-hosting](https://docs.livekit.io/transport/self-hosting/)

`JobContext` 只负责 Job 和 Room 边界，不替代项目现有的 P1、录音、转人工、事件、语义分析和业务状态机。

## 5. 第一性原理与设计结论

SIP 呼入的本质不是“增加一个创建会话 API”，而是处理一个已经由外部电话网络发起、已经进入 LiveKit Room 的实时会话。

因此调用时序与外呼相反：

```text
SIP 外呼：应用先创建 call_id / Room / Agent，再创建 SIP participant

SIP 呼入：SIP caller 先触发 Room / Agent Job，worker 再认领 call_id 和业务记录
```

Phase F 的核心结论：

1. 呼入不复用 `POST /ai-call/sip-sessions`，该接口继续只表示 SIP 外呼。
2. 呼入正常启动由 dispatch rule + AgentServer 完成，不由 webhook 启动。
3. worker 使用 `JobContext` 持有 Room，不再由 API 进程持有呼入实时任务。
4. Qwen、P1、播放、打断和通话结束继续由现有 Runner 负责。
5. API 继续负责查询、坐席操作、Token 签发和业务持久化入口，但不直接访问呼入 Runner 内存。
6. webhook 负责外部事实、孤儿通话发现和终态补偿。
7. trunk 和 dispatch rule 由部署或运维显式创建；应用只读校验，不自动创建、更新或删除电话基础设施。

## 6. 范围

### 6.1 Phase F 必须做

1. self-hosted inbound trunk 和 individual dispatch rule 配置基线。
2. `roomConfig.agents` 显式派发 Agent worker。
3. 独立 AgentServer 进程和 JobContext entrypoint。
4. 入站 SIP participant 识别、allowlist 校验和幂等认领。
5. `entry_type=sip_inbound` 通话记录和号码脱敏字段。
6. 复用现有 Qwen Realtime Runner 和 SIP P1。
7. 复用现有录音、对话、handoff、事件和通话后分析。
8. API 与 worker 之间的持久化控制命令。
9. `participant_joined`、`participant_left`、`room_finished` webhook reconciliation。
10. 多 worker、重启、重复事件和故障场景验收。

### 6.2 Phase F 不做

1. 不把 Web 会话迁到 LiveKit Agents worker。
2. 不把 SIP 外呼立即迁到 LiveKit Agents worker。
3. 不重写 Qwen Realtime provider。
4. 不迁移到 STT -> LLM -> TTS 三段式模型。
5. 不做完整 IVR、按键导航和多级菜单。
6. 不做排队、技能组、坐席分配和完整坐席中心。
7. 不做多租户 trunk/rule 动态管理。
8. 不做 SIP REFER 或运营商级呼叫转移。
9. 不让应用自动创建、修改或删除 trunk / dispatch rule。
10. 不承诺 worker 崩溃后 Qwen Session 无感恢复。

## 7. 总体架构

```mermaid
flowchart TB
  Phone["客户电话"] --> Provider["运营商 / FreeSWITCH / SIP trunk"]
  Provider --> SipSvc["self-hosted livekit-sip"]
  SipSvc --> Trunk["Inbound trunk<br/>号码 / 来源 IP / 认证"]
  Trunk --> Rule["Individual dispatch rule<br/>roomPrefix=ai-call-in-"]
  Rule --> Room["LiveKit Room<br/>每通电话唯一"]
  Rule --> Dispatch["roomConfig.agents<br/>agentName=ai-call-inbound"]
  Dispatch --> AgentServer["AI Call AgentServer pool"]
  AgentServer --> Job["Per-call JobContext"]

  Job --> Claim["SipInboundClaimService<br/>校验 / 幂等认领"]
  Claim --> Record["ai_call_record<br/>entry_type=sip_inbound"]
  Job --> Transport["JobContextRoomAudioTransport"]
  Transport --> Room
  Job --> Runner["RealtimeCallAgentRunner"]
  Runner <--> Qwen["Qwen Realtime"]
  Runner <--> Transport

  Room --> Egress["LiveKit Egress"]
  Egress --> Recording["录音 / 分轨 / OSS"]

  Api["AI Call API"] --> Record
  Api --> Command["ai_call_command"]
  Command --> Job
  Api --> Token["坐席 Room Token"]
  Token --> Room

  Webhook["LiveKit webhook"] --> Reconcile["Webhook reconciliation"]
  Reconcile --> Record
```

## 8. 组件职责

| 组件 | 负责 | 不负责 |
|---|---|---|
| inbound trunk | 号码、SIP 来源、认证、安全边界 | 业务场景、Qwen、数据库 |
| dispatch rule | 为 caller 选 Room、触发指定 Agent | 启动业务记录、处理 P1 |
| AgentServer | worker 注册、Job 分配、进程隔离、优雅退出 | 业务状态、录音、handoff |
| JobContext entrypoint | 连接 Room、识别 caller、组织单通生命周期 | 维护全局坐席队列 |
| SipInboundClaimService | 校验 attributes、生成幂等键、创建或返回 call record | 创建 trunk/rule |
| JobContextRoomAudioTransport | 绑定 `ctx.room`、订阅 caller、发布 AI 音轨 | 签发独立 Room Token |
| RealtimeCallAgentRunner | Qwen、播放、P1、话轮、结束、挂起 | 管理 trunk/rule |
| AI Call API | 查询、坐席操作、Token、控制命令 | 直接持有呼入 Runner |
| webhook reconciler | 外部事实、异常补偿、终态对账 | 正常启动 Agent |

## 9. Trunk 和 Dispatch Rule 设计

### 9.1 Inbound trunk

示例配置仅用于表达字段，真实号码、IP 和认证由运营商资料决定：

```json
{
  "trunk": {
    "name": "ai-call-inbound-prod",
    "numbers": ["+<company-number>"],
    "allowedAddresses": ["<provider-egress-cidr>"]
  }
}
```

规则：

1. `numbers` 只配置运营商实际路由到该 trunk 的公司号码。
2. 供应商有固定 SIP 出口 IP 时，正式环境必须配置 `allowedAddresses`。
3. `allowedAddresses` 是 SIP 来源服务器 IP/CIDR 白名单，不是客户手机号白名单。
4. 供应商没有固定出口 IP 时，必须补 SIP 认证、防火墙、精确号码和应用层 attributes 校验，不能直接把公网 SIP 入口完全放开。

### 9.2 Individual dispatch rule

```json
{
  "dispatch_rule": {
    "name": "ai-call-inbound-prod-rule",
    "trunk_ids": ["ST_<inbound-trunk-id>"],
    "rule": {
      "dispatchRuleIndividual": {
        "roomPrefix": "ai-call-in-"
      }
    },
    "roomConfig": {
      "agents": [{
        "agentName": "ai-call-inbound",
        "metadata": "{\"schemaVersion\":1,\"entryType\":\"sip_inbound\",\"routeKey\":\"company_reception\"}"
      }]
    }
  }
}
```

规则：

1. rule 必须显式绑定允许的 inbound trunk，不能省略 `trunk_ids` 形成全 trunk 通配。
2. 每通电话使用独立 Room，不把不同客户放入共享 Room。
3. `agentName` 必须和 worker 注册名完全一致。
4. metadata 只传 schema、入口和稳定路由键，不传手机号、客户资料、Token 或密钥。
5. `roomPrefix` 只用于排障和识别，不能从 Room 名反推出业务 `call_id`。
6. JSON 字段以实际部署版本的 LiveKit CLI/API schema 为准，部署前必须执行只读 list 校验。

### 9.3 运维显式配置、应用只读校验

部署流程负责：

1. 创建和更新 inbound trunk。
2. 创建和更新 dispatch rule。
3. 保存实际 `trunk_id` 和 `rule_id` 到环境配置或 Secret 管理系统。
4. 变更运营商号码、来源 IP、认证和路由。

应用只负责：

1. 启动时读取允许的 `trunk_id`、`rule_id` 和被叫号码。
2. 通过 SDK/API list 检查资源是否存在。
3. 检查 rule 是否为 individual、是否绑定正确 trunk、是否派发正确 `agentName`。
4. 运行时校验 SIP participant attributes 是否和允许配置一致。
5. 校验失败时拒绝处理并告警，不自动修正远端资源。

## 10. 呼入时序

### 10.1 正常流程

```mermaid
sequenceDiagram
  participant C as 客户电话
  participant S as livekit-sip
  participant L as LiveKit Server
  participant W as Agent worker
  participant D as AI Call DB
  participant Q as Qwen Realtime
  participant E as Egress

  C->>S: SIP INVITE / RTP
  S->>L: 创建 SIP participant 和独立 Room
  L->>W: dispatch ai-call-inbound Job
  W->>L: JobContext 连接 Room
  W->>W: 查找 Kind=SIP participant
  W->>W: 校验 trunk/rule/dialed number
  W->>D: 按 SIP call key 幂等认领
  D-->>W: call_id / existing or created
  W->>E: 启动混音和分轨录音
  W->>Q: 建立 Qwen Realtime Session
  W->>L: 发布 ai_audio 音轨
  W->>C: 播放开场白
  C->>W: 客户音频
  W->>Q: PCM 音频
  Q->>W: 文本 / 音频 / VAD 事件
  W->>C: AI 音频
  C-->>L: caller 离开 Room
  W->>Q: cancel / close
  W->>E: 停止录音
  W->>D: 写终态、事件、对话
```

### 10.2 详细步骤

1. SIP caller 通过 trunk 和 dispatch rule 进入独立 Room。
2. LiveKit 将 `ai-call-inbound` Job 派发给一个可用 worker。
3. worker 解析 job metadata，并校验 `schemaVersion`、`entryType` 和 `routeKey`。
4. worker 注册 Room 事件监听，再连接 Room，避免漏掉已经存在或紧接着发布的音轨。
5. worker 在当前 remote participants 中查找 `Participant.Kind == SIP`；如果尚未出现，则等待到超时。
6. worker 读取 `sip.callIDFull`、`sip.callID`、`sip.trunkID`、`sip.ruleID`、`sip.phoneNumber`、`sip.trunkPhoneNumber` 等 attributes。
7. worker 执行 trunk、rule 和被叫号码 allowlist 校验。
8. worker 调用统一认领服务，创建或取得原有 `call_id`。
9. worker 根据 `routeKey=company_reception` 解析综合接待提示词和开场白。
10. worker 创建单 Job 内的 session registry、event store、dialogue runtime store 和持久化 listener。
11. worker 启动 Egress 录音，并记录实际 SIP participant identity 和实际 Agent identity。
12. worker 启动 Qwen Runner 和 JobContext Room transport。
13. caller 音轨和 AI 音轨准备完成后，worker 播放开场白。
14. 通话中沿用现有 Qwen、P1、handoff 和结束状态机。
15. caller 离开后，worker 先收敛实时任务，再停止录音并写通话终态。
16. webhook 对 `participant_left` 和 `room_finished` 做最终补偿。

## 11. SIP Participant 识别与安全校验

### 11.1 边界识别

在 LiveKit 系统边界使用：

1. `Participant.Kind == SIP`。
2. `sip.*` attributes。
3. dispatch rule 的 job metadata。

进入内部 `CallSession` 后使用：

```text
entry_type in {sip_inbound, sip_outbound}
```

不要把 `participant_identity.startswith("sip-")` 作为呼入长期判定依据。呼入 participant identity 由 LiveKit SIP 实际创建，不能假设沿用外呼命名规则。

### 11.2 Allowlist 校验

必须校验：

1. `sip.trunkID` 属于允许的 inbound trunk。
2. `sip.ruleID` 属于允许的 dispatch rule。
3. `sip.trunkPhoneNumber` 或等价被叫号码属于允许的公司号码。
4. Room 名符合预期前缀，但 Room 前缀只作为辅助证据。
5. Room 中只能认领一个初始 SIP caller；额外 SIP participant 作为异常事件记录。

校验失败时：

1. 不连接 Qwen。
2. 不播放业务提示词。
3. 写 `sip_inbound_rejected` 和明确 `failure_stage`。
4. 结束当前 Job / Room，并触发安全告警。

### 11.3 号码和敏感字段

1. 主叫号码只保存 hash 和 masked 值。
2. 被叫公司号码只保存 hash 和 masked 值。
3. `sip.callIDFull` 不直接写普通日志；幂等使用规范化后的 hash。
4. webhook payload 进入事件前必须做 attributes allowlist 和脱敏。
5. dispatch metadata 中禁止放手机号和客户资料。

## 12. 幂等认领设计

### 12.1 为什么必须认领

以下情况都可能同时观察到同一通电话：

1. Agent Job 正常启动。
2. `participant_joined` webhook 到达。
3. worker 崩溃后 Job 被重新派发。
4. webhook 重试或乱序。

因此不能由“谁先收到事件”简单创建记录，必须通过稳定键认领。

### 12.2 Canonical SIP call key

按以下顺序构造原始 canonical key：

1. `sip.callIDFull` 存在：`full:<sip.callIDFull>`。
2. 否则使用：`trunk:<sip.trunkID>|call:<sip.callID>`。
3. attributes 不完整时，仅作为诊断兜底：`room:<room_name>|participant:<identity>`。

数据库只保存 canonical key 的稳定 hash，不保存完整原文。

兜底键必须写 `sip_idempotency_weak` 事件；正式验收中如果出现兜底键，应先修复 SIP attributes，再扩大流量。

### 12.3 认领事务

`SipInboundClaimService.claim(...)` 的语义：

1. 根据 `sip_call_key_hash` 查询已有记录。
2. 不存在时生成新的业务 `call_id` 并插入 `sip_inbound` 记录。
3. 唯一约束冲突时重新读取并返回已存在记录。
4. 已终态记录收到新 Job 时，不直接复活；写异常事件并结束该 Job。
5. 活跃记录收到重派 Job 时，写入包含新 `job_id/worker_id` 的重派事件，不创建第二条记录。

worker 和 webhook 都可以调用认领服务，但只有 worker 可以启动 Runner。

## 13. 通话状态设计

Phase F 复用现有 `CallSessionStatus`，不为 SIP 呼入另建一套通话状态机。

| 状态 | 呼入含义 |
|---|---|
| `created` | 已按 SIP call key 创建或认领记录 |
| `preparing` | 正在校验、解析提示词、启动录音和 Qwen |
| `ready` | worker 和实时能力已准备，等待 caller 音轨或开场白 |
| `connected` | caller 音轨可用，已经进入可对话状态 |
| `user_speaking` | 客户说话 |
| `ai_thinking` | Qwen 正在处理 |
| `ai_speaking` | AI 正在播放 |
| `interrupted` | AI 输出被确认打断 |
| `waiting` | AI 因转人工等原因挂起 |
| `ending` | 正在关闭 Qwen、音轨、录音和任务 |
| `completed` | 正常终态 |
| `failed` | 技术或策略失败终态 |

时间字段口径：

1. `started_at`：系统首次观察并认领来电的时间。
2. `answered_at`：SIP caller 已进入 Room 且电话侧已处于可通话状态的时间。
3. `ended_at`：终态写入时间。
4. `duration_ms`：优先按 `answered_at -> ended_at` 计算。

## 14. 数据模型

### 14.1 扩展 `ai_call_record`

保留现有字段，新增：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `sip_call_key_hash` | `varchar(80)` | 否 | 呼入幂等键 hash，唯一 |
| `sip_trunk_id` | `varchar(64)` | 否 | 实际 inbound trunk ID |
| `sip_rule_id` | `varchar(64)` | 否 | 实际 dispatch rule ID |
| `caller_phone_number_hash` | `varchar(80)` | 否 | 呼入主叫号码指纹 |
| `caller_phone_number_masked` | `varchar(32)` | 否 | 呼入主叫号码脱敏展示 |
| `dialed_phone_number_hash` | `varchar(80)` | 否 | 客户拨打的公司号码指纹 |
| `dialed_phone_number_masked` | `varchar(32)` | 否 | 客户拨打的公司号码脱敏展示 |

索引：

1. `sip_call_key_hash` 唯一索引。
2. `entry_type + started_at` 沿用现有索引。
3. `sip_trunk_id + started_at` 普通索引，用于线路排障。
4. 第一版不新增 SIP 呼入专表。

### 14.2 新增 `ai_call_command`

呼入 worker 和 API 不共享内存，需要一张持久化命令表。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---:|---|
| `id` | `bigint` | 是 | 雪花主键 |
| `command_id` | `varchar(64)` | 是 | 命令业务 ID，唯一 |
| `call_id` | `varchar(64)` | 是 | 目标通话 |
| `command_type` | `varchar(32)` | 是 | 命令类型 |
| `payload_json` | `text` | 否 | 脱敏后的命令参数 |
| `status` | `varchar(20)` | 是 | `pending/claimed/succeeded/failed/expired` |
| `created_at` | `timestamp with time zone` | 是 | 创建时间 |
| `claimed_at` | `timestamp with time zone` | 否 | worker 认领时间 |
| `completed_at` | `timestamp with time zone` | 否 | 执行完成时间 |
| `error_message` | `varchar(500)` | 否 | 脱敏后的失败摘要 |

第一版命令仅包括：

1. `suspend_for_handoff`
2. `resume_after_handoff`
3. `end_call`

命令表不替代 `ai_call_event`：命令表表达“要求执行什么及结果”，事件表表达“实际上发生了什么”。

### 14.3 不新增运行态主表

LiveKit 已经负责 Job 分配和 worker ownership，Phase F 第一版不再复制一套完整 worker ownership 表。

`job_id`、`worker_id` 和 Agent identity 先进入事件及指标。LiveKit 负责 worker 注册和 Job ownership；只有后续证明业务查询或控制确实需要稳定运行态表时，再单独设计。

## 15. Agent Worker 设计

### 15.1 进程模型

新增独立启动入口，例如：

```text
app/workers/ai_call_inbound_agent.py
```

该进程：

1. 以 `agent_name=ai-call-inbound` 注册到 self-hosted LiveKit Server。
2. 等待 explicit dispatch。
3. 每个 Job 只处理一通 Room。
4. 不暴露公网业务 API。
5. 使用独立健康检查和结构化日志。

API 服务和 Agent worker 可以使用同一代码仓库与镜像，但必须是两个独立进程/Deployment，不能在 FastAPI startup 中顺带启动 AgentServer。

### 15.2 JobContext entrypoint

entrypoint 只做单通编排：

1. 解析 metadata。
2. 连接 Room。
3. 找到并校验 SIP caller。
4. 幂等认领通话。
5. 创建单 Job runtime 依赖。
6. 启动录音、Qwen、transport 和 opening。
7. 监听实时控制命令。
8. 等待 caller 离开或本地结束。
9. 执行幂等 cleanup。

不要把全局 trunk 管理、坐席队列或批处理塞入 Job entrypoint。

### 15.3 Runtime factory

当前 Runner 构造逻辑集中在 `AiCallOrchestrator._build_default_agent_runner()`。

Phase F 应抽取一个最小 runtime factory，用于统一构造：

1. `AliyunQwenRealtimeProvider`
2. `RealtimeCallAgentRunner`
3. SIP P1 参数
4. call-end policy
5. event/dialogue persistence listener

现有 Web/外呼 Orchestrator 和呼入 worker 共用该 factory。factory 不负责 Room 创建、trunk、dispatch 或数据库认领。

### 15.4 单 Job 内存态

Runner 现有字典以 `call_id` 为键。每个 Job 独立进程后，可以继续使用：

1. 单 Job `InMemorySessionRegistry`
2. 单 Job `InMemoryEventStore`
3. 单 Job metrics 和音频任务

但内存态只能作为当前 Job 的执行状态，数据库事件和记录才是跨进程可查询事实。

## 16. JobContext Room Audio Transport

### 16.1 新 transport 的必要性

当前 `LiveKitRoomAudioTransport` 会：

1. 创建自己的 `rtc.Room()`。
2. 自行签发 `agent-{call_id}` Token。
3. 自行 `room.connect(...)`。
4. `close()` 时自行断开 Room。

JobContext 已经拥有 Room 生命周期，不能再次连接同一个 Agent participant。因此新增 `JobContextRoomAudioTransport`，实现现有 `RoomAudioTransportProtocol`，但使用 `ctx.room`。

### 16.2 生命周期规则

1. transport 不签 Token。
2. transport 不创建第二个 Room。
3. transport 不删除 Room。
4. Room connect/shutdown 由 JobContext entrypoint 控制。
5. transport 只创建并发布 `ai_audio` track、接收 caller 音轨和清理自身 stream/source。

### 16.3 精确订阅

worker 在启动 Runner 前必须得到真实 inbound SIP identity，并写入 `CallSession.participant_identity`。

transport 只接收该 identity 的音轨：

```text
target identity = 实际 inbound SIP participant identity
```

必须同时覆盖：

1. transport 注册前音轨已经存在。
2. transport 注册后 caller 才发布音轨。
3. Room 后续加入 human agent。
4. Room 出现额外 SIP participant。

human agent 或其他 participant 音轨不能被错误发送给 Qwen。

### 16.4 Agent identity

录音和事件不能继续无条件假设 Agent identity 等于 `agent-{call_id}`。

优先策略：

1. SDK 支持时显式设置稳定 Agent identity。
2. 无法显式设置时，从 `ctx.room.local_participant.identity` 读取实际 identity。
3. 录音、分轨和事件统一使用实际 identity。

## 17. Qwen 与业务路由

### 17.1 Qwen 接入

Phase F 不改 Qwen Realtime provider 协议：

1. caller PCM 继续送入 Qwen Realtime WebSocket。
2. Qwen 音频继续由现有 audio bridge 发布到 Room。
3. `server_vad`、P1 和通话结束策略继续由现有 Runner 处理。
4. LiveKit Agents 只承担 dispatch/job/Room 生命周期，不接管模型推理。

### 17.2 公司综合接待

dispatch metadata 使用：

```text
routeKey=company_reception
```

它表示呼入初始角色是公司综合接待，不表示每个号码固定绑定某一个 `intro_*` 业务主题。

设计口径：

1. 同一个 `call_id`。
2. 同一个 LiveKit Room。
3. 同一个 Qwen Realtime Session。
4. 根据客户兴趣在合同、单证、海外增长、GEO、催收等业务主题间切换。
5. 主题切换写 `business_topic_switched` 事件，不重建 Job。
6. `ai_call_record.scene_code` 记录入口提示词配置，不承担逐轮主题历史。

正式实现前需要新增或确认 `company_reception` prompt profile；在该 profile 可查询且 preview 通过之前，不把任意 `intro_*` 场景硬编码为呼入默认值。

## 18. SIP P1 打断

Phase F 不重写 SIP P1。

改造边界：

1. `CallSession` 增加 `entry_type`。
2. 系统边界通过 `Participant.Kind == SIP` 和 attributes 确认 SIP caller。
3. 进入 Runner 后通过 `entry_type in {sip_inbound, sip_outbound}` 启用 SIP P1。
4. 保留现有 candidate、pre-stop、confirm、reject、recovery 和相关事件。
5. inbound 必须重新执行真实电话 P1 样本验收，不能以 outbound 结果直接推断 inbound 一定等价。

## 19. 事件和对话持久化

### 19.1 跨进程边界

当前 event/dialogue listener 主要在 API 进程配置。呼入 Runner 位于独立 Job 进程后，API 进程不会自动收到 worker 的内存事件。

每个 Job 必须显式创建并绑定：

1. event persistence listener
2. dialogue persistence listener
3. handoff realtime coordinator

Job 结束前应在有限超时内 drain 持久化队列；超时写 cleanup failure，但不能无限阻塞 Job 退出。

### 19.2 新增事件建议

| 事件 | 来源 | 说明 |
|---|---|---|
| `sip_inbound_observed` | worker/webhook | 首次观察到 caller |
| `sip_inbound_claimed` | claim service | 已取得唯一 `call_id` |
| `sip_inbound_rejected` | worker | attributes 或 allowlist 不通过 |
| `agent_job_started` | worker | Job entrypoint 启动 |
| `agent_job_restarted` | worker | 活跃通话发生 Job 重派 |
| `sip_media_connected` | transport | caller 音轨已可消费 |
| `opening_started` | runner | 开场白开始发布 |
| `runtime_command_claimed` | worker | 控制命令已认领 |
| `runtime_command_succeeded` | worker | 控制命令成功 |
| `runtime_command_failed` | worker | 控制命令失败 |
| `agent_job_ended` | worker | Job cleanup 完成 |
| `webhook_reconciled` | webhook | webhook 执行了状态补偿 |

事件 payload 不保存原始手机号、完整 SIP headers、Token、API Key 或未脱敏异常对象。

## 20. 跨进程控制与转人工

### 20.1 为什么不能直接调用 Orchestrator

呼入 Runner 在 Agent Job 进程，API 中的 `AiCallOrchestrator.registry` 无法访问该 Runner。

因此以下调用不能继续直接依赖 API 内存：

1. `suspend_for_handoff`
2. `resume_after_handoff`
3. `end_session`

### 20.2 持久化命令 + Redis 通知

控制流程：

```text
API
  -> 写 ai_call_command(status=pending)
  -> 提交数据库事务
  -> Redis publish ai-call:command:{call_id} command_id
  -> worker 读取命令并原子 claimed
  -> 执行 Runner 动作
  -> 更新 succeeded / failed
  -> 写 runtime command 事件
```

可靠性规则：

1. 数据库命令表是事实源，Redis 只负责低延迟唤醒。
2. Redis publish 失败不回滚已提交命令；worker 定期补查 pending 命令。
3. worker 启动或重派后必须先扫描该 `call_id` 的 pending 命令。
4. 命令按 `command_id` 幂等，成功命令不能重复执行副作用。
5. `suspend/resume` 必须携带 `handoff_id`，执行前核对数据库当前 handoff，避免旧命令作用于新状态。
6. `end_call` 对已终态通话返回幂等成功。

### 20.3 AI 主动转人工

当同一 Job 内的 Runner 识别到高可信转人工意图时，优先在 worker 内完成实时动作：

1. 创建或取得 active handoff 记录。
2. 立即挂起当前 Runner。
3. 清空播放队列并停止新的模型回复。
4. 进入 `waiting`。
5. 持久化 handoff 和事件。

不需要先绕到 API 再返回 worker，避免增加实时控制延迟。

### 20.4 坐席接入

坐席 API：

1. 从数据库读取 `call_id`、`room_name`、通话状态和 handoff 状态。
2. 使用实际 Room 签发 `human-agent-{handoff_id}` Token。
3. 不通过 `InMemorySessionRegistry` 查找呼入 session。
4. 坐席加入后，AI 保持挂起。
5. 坐席离开或 handoff 失败时，根据现有 B3/B3.1 规则决定终态或恢复命令。

## 21. 录音与通话后处理

### 21.1 录音启动

worker 认领通话并得到实际 participant identities 后启动：

1. Room composite egress。
2. SIP caller participant egress。
3. Agent participant egress。
4. handoff 后的 human agent participant egress。

录音服务必须使用实际 identity，不能依赖呼入 identity 前缀或固定 Agent identity。

### 21.2 录音失败策略

技术设计支持两种业务策略：

1. fail-open：记录高优先级事件并继续接待。
2. fail-closed：播放故障提示并结束，不允许无录音通话继续。

Phase F 上线前必须由业务和合规明确采用哪一种；在决定前，不能把录音失败静默忽略。

### 21.3 通话后处理

1. worker 完成实时终态和录音停止请求。
2. API 侧 recording reconciler 继续核对 Egress 产物。
3. 录音关闭后进入现有 offline ASR。
4. 对话和录音证据准备完成后进入现有 semantic analysis。
5. handoff 后 snapshot 契约继续沿用 Phase B5。

## 22. Webhook 设计

### 22.1 角色

webhook 负责：

1. 记录 LiveKit 观察到的 participant/Room 外部事实。
2. 在 worker 尚未认领时调用同一 claim service 创建最小记录。
3. 发现没有 Agent Job 的孤儿来电。
4. worker 未写终态时补充 `remote_hangup`、`room_finished` 或 `runtime_failed`。
5. 触发录音和 handoff 的最终 reconciliation。

webhook 不负责：

1. 正常启动 Runner。
2. 持有 Qwen Session。
3. 订阅音频。
4. 直接修改 trunk 或 dispatch rule。

### 22.2 幂等和乱序

1. 优先使用 LiveKit webhook event id 作为 `ai_call_event.event_id`。
2. 找不到 record 时，使用 SIP call key 认领，而不是按 Room 名随意创建第二条记录。
3. `participant_left` 早于 worker cleanup 时只记录观察事实，不强行覆盖正在执行的 `ending`。
4. 已终态记录只允许补充事件和缺失字段，不允许回退到运行态。
5. `room_finished` 到达后仍无 worker 终态时，reconciler 才执行终态补偿。

## 23. 失败处理与恢复

| 故障 | 检测位置 | 处理 |
|---|---|---|
| trunk/rule 不匹配 | worker preflight | 拒绝、记录、结束 Room |
| SIP participant 超时 | Job entrypoint | `participant_timeout`，结束 Job |
| SIP attributes 缺失 | claim/preflight | 使用弱幂等键并告警；关键校验缺失则拒绝 |
| 没有可用 worker | webhook/reconciler | 超过 dispatch grace period 后标记 `agent_dispatch_timeout`，告警，不由 webhook 临时启动 Agent |
| 数据库不可用 | worker claim | 不启动 Qwen，播放本地故障提示后结束 |
| prompt 解析失败 | worker preparing | 记录 `prompt_resolve_failed`，不使用任意默认业务提示词 |
| Qwen 连接失败 | Runner | 本地故障提示，按策略转人工或结束 |
| caller 无音轨 | transport timeout | `media_timeout`，结束或提示后结束 |
| 录音失败 | recording service | 按已确认的 fail-open/fail-closed 策略 |
| worker 中途崩溃 | LiveKit AgentServer | Job 重派，幂等认领原记录 |
| Redis 通知失败 | API/worker | 命令保留 pending，worker 补查数据库 |
| webhook 重复/乱序 | reconciler | event id + 状态条件更新 |
| cleanup 超时 | worker | 写 cleanup failure，释放 Job，后续 reconciler 补偿 |

### 23.1 Worker 重派

新 Job 观察到已有活跃记录时：

1. 不创建新 `call_id`。
2. 写包含新 `job_id/worker_id` 的 `agent_job_restarted` 事件。
3. 重新读取当前通话、handoff 和 pending command 状态。
4. 重新建立 Qwen Session。
5. 第一版不承诺恢复原 Qwen 隐状态。
6. 有可信对话文本时，可以在后续实现中构造有限恢复摘要；本阶段不提前设计完整会话重放。
7. 重建失败时优先转人工；无可用人工时播放本地提示并结束。

## 24. 配置设计

第一版只增加必要配置：

```text
AI_CALL_SIP_INBOUND_ENABLED
AI_CALL_SIP_INBOUND_AGENT_NAME
AI_CALL_SIP_INBOUND_ALLOWED_TRUNK_IDS
AI_CALL_SIP_INBOUND_ALLOWED_RULE_IDS
AI_CALL_SIP_INBOUND_ALLOWED_DIALED_NUMBERS
AI_CALL_SIP_INBOUND_ROUTE_KEY
AI_CALL_SIP_INBOUND_AGENT_DISPATCH_TIMEOUT_SECONDS
AI_CALL_SIP_INBOUND_PARTICIPANT_TIMEOUT_SECONDS
AI_CALL_SIP_INBOUND_MEDIA_TIMEOUT_SECONDS
AI_CALL_RUNTIME_COMMAND_REDIS_PREFIX
```

默认原则：

1. `AI_CALL_SIP_INBOUND_ENABLED=false`，没有显式开启时 worker 不接正式呼入。
2. 正式环境 allowlist 不能为空。
3. Agent name 默认建议 `ai-call-inbound`，但必须和 dispatch rule 一致。
4. route key 默认建议 `company_reception`，前提是对应 prompt profile 已存在并验收。
5. `AGENT_DISPATCH_TIMEOUT_SECONDS` 是 webhook/reconciler 判断孤儿来电的宽限时间，不能用单次 webhook 到达就立即判失败。
6. 不新增“自动创建 trunk/rule”开关。
7. 不在普通配置文件保存 SIP 密码、LiveKit secret 或 DashScope key。

## 25. 部署设计

### 25.1 服务组成

正式环境至少包括：

1. LiveKit Server。
2. LiveKit 共用 Redis。
3. `livekit-sip`。
4. `livekit-egress`。
5. AI Call API。
6. `ai-call-inbound` AgentServer worker pool。
7. 业务数据库。
8. AI Call runtime command Redis 或独立逻辑命名空间。

LiveKit Redis 和应用命令 Redis 可以物理复用，但必须使用独立 key prefix、权限和容量监控；正式环境更推荐逻辑隔离，避免应用命令流量影响 LiveKit 控制面。

### 25.2 网络

1. SIP signaling 和 RTP 端口按 `livekit-sip` 要求对运营商开放。
2. LiveKit API/Twirp endpoint、Redis 和数据库优先走国内内网/VPC。
3. Agent worker 只需要主动连接 LiveKit、数据库、Redis 和 DashScope，不暴露公网业务端口。
4. Twirp 是 HTTP 控制面，不承载 SIP/RTP 音频；国内风险主要来自跨境网络和公网媒体端口，而不是 Twirp 协议本身。
5. 项目继续优先使用官方 SDK；只有 SDK 不可用或版本不兼容时才使用显式、可观测的 Twirp fallback。

### 25.3 Worker 容量和发布

1. 正式环境至少部署两个 worker 实例。
2. worker readiness 必须包含 LiveKit 注册状态和必要配置校验。
3. 发布时使用 AgentServer graceful drain，不接受新 Job，等待存量通话结束。
4. 单 Job 崩溃不能导致同 worker 上其他通话一起失败。
5. 上线前必须根据实际模型内存、CPU 和目标并发完成容量测试，不使用拍脑袋并发值。

## 26. 可观测性

### 26.1 关联字段

所有结构化日志和指标至少携带：

1. `call_id`
2. `room_name`
3. `job_id`
4. `worker_id`
5. `entry_type=sip_inbound`
6. `sip_call_key_hash`
7. `sip_trunk_id`
8. `sip_rule_id`

不携带原始手机号和完整 SIP Call-ID。

### 26.2 核心指标

1. `sip_inbound_received_total`
2. `sip_inbound_rejected_total`
3. `agent_dispatch_timeout_total`
4. `agent_job_restart_total`
5. `dispatch_to_job_started_ms`
6. `job_started_to_media_connected_ms`
7. `media_connected_to_opening_audio_ms`
8. `qwen_connect_ms`
9. `runtime_command_latency_ms`
10. `recording_start_failed_total`
11. `webhook_reconcile_total`
12. `sip_inbound_terminal_reason_total`

P95/P99 阈值和目标并发必须在真实 PoC 基线后写入上线清单；本设计不虚构尚未测量的延迟目标。

## 27. 安全设计

1. trunk 优先配置 `allowedAddresses` 或 SIP 认证。
2. dispatch rule 显式绑定 trunk，不使用全 trunk 通配。
3. worker 运行时再次校验 trunk/rule/dialed number。
4. LiveKit API Key 按 API、SIP、Egress、Agent worker 的权限需求分离。
5. 控制面使用内网或 TLS，禁止将 Redis 和数据库暴露到公网。
6. caller/dialed number 只保存 hash 和 masked 值。
7. webhook 必须继续验证 LiveKit 签名。
8. 控制命令执行前校验 call/handoff 当前状态，防止重放旧命令。
9. 日志和事件不保存 SIP auth、JWT、DashScope key、完整 headers 和原始客户隐私数据。

## 28. 实施分期

### 28.1 F0：兼容性 Spike

目标：证明当前 self-hosted LiveKit 可以运行 Agent dispatch 和 raw track Job。

必须验证：

1. 当前 LiveKit Server 版本支持 AgentServer 注册和 explicit dispatch。
2. `livekit-agents` 与现有 `livekit` / `livekit-api` 依赖可以锁定兼容版本。
3. JobContext 可以获得 inbound SIP participant 和 attributes。
4. Job 可以订阅 SIP 音轨并发布测试音频。
5. worker graceful drain 可观察。

F0 不接现有 Qwen 和业务记录。

### 28.2 F1：Worker Runtime 适配

目标：让现有 Runner 在 JobContext Room 中工作。

必须完成：

1. runtime factory。
2. `JobContextRoomAudioTransport`。
3. `CallSession.entry_type`。
4. worker-local event/dialogue persistence。
5. Qwen 双向音频和开场白。

### 28.3 F2：SIP 呼入记录闭环

目标：真实电话呼入可追踪、可录音、可结束。

必须完成：

1. trunk/rule 只读 preflight。
2. SIP participant 校验。
3. canonical SIP call key 和幂等认领。
4. `sip_inbound` 数据字段。
5. 录音和终态。
6. webhook joined/left/room finished reconciliation。

### 28.4 F3：转人工与跨进程控制

目标：API 和坐席可以可靠控制 worker 中的实时通话。

必须完成：

1. `ai_call_command`。
2. Redis 低延迟通知和数据库补查。
3. suspend/resume/end 幂等执行。
4. 坐席 Token 从数据库 Room 信息签发。
5. AI 主动转人工的 worker-local 闭环。

### 28.5 F4：生产加固

目标：满足公开号码上线条件。

必须完成：

1. 双 worker 与滚动发布。
2. Job 崩溃和重派测试。
3. webhook 重复、乱序和丢失测试。
4. Redis、数据库、Qwen、Egress 故障演练。
5. 目标并发压测和容量结论。
6. 告警、runbook、回滚路径和运营商联调记录。

## 29. 验收矩阵

### 29.1 功能验收

1. 测试号码拨入后生成唯一 `sip_inbound` 记录。
2. caller/dialed number 只以 hash/masked 形式出现。
3. 开场白、客户讲话、AI 回复均可听见。
4. AI 播放期间客户插话，SIP P1 正常。
5. 同一通话内可以从综合接待切换到不同业务主题。
6. 转人工后 AI 停止，坐席与客户双向通话。
7. 客户挂机后记录、录音、对话和语义分析闭环。

### 29.2 幂等验收

1. 同一 `participant_joined` webhook 重放多次仍只有一条记录。
2. worker 和 webhook 同时认领仍只有一个 `call_id`。
3. worker 重派不创建第二条记录。
4. `end_call` 命令重试不会重复执行破坏性动作。
5. 已终态记录不会被晚到 webhook 改回运行态。

### 29.3 故障验收

1. 无可用 worker。
2. trunk/rule 校验失败。
3. SIP participant attributes 缺失。
4. caller 音轨未出现。
5. Qwen 连接失败或中途断开。
6. Egress 启动或停止失败。
7. Redis publish 失败。
8. 数据库短暂不可用。
9. worker 通话中崩溃。
10. API 和 worker 分别滚动发布。

### 29.4 证据要求

每个验收样本至少保存：

1. `call_id`
2. Room 和实际 participant identities
3. SIP call key hash、trunk ID、rule ID
4. Job/worker 标识
5. 关键事件时间线
6. 录音与分轨
7. 对话文本
8. P1 summary
9. handoff 记录（如适用）
10. 最终状态和结束原因

## 30. 上线和回滚

### 30.1 上线顺序

1. 部署并验证 Agent worker，不绑定公开号码。
2. 使用隔离测试 trunk/rule 和测试号码完成 F0-F3。
3. 双 worker 完成 F4 故障与发布演练。
4. 将一个 canary 号码绑定正式 rule。
5. 观察呼入成功率、开场延迟、P1、录音和终态。
6. 达到上线门槛后再切换公开号码。

### 30.2 回滚

回滚不能只停止 Agent worker，否则 caller 可能进入 Room 后无人响应。

正确回滚顺序：

1. 先在运营商或 dispatch 配置层停止新呼入进入该 Agent rule，或切回已验证的备用路由。
2. Agent worker 进入 drain，不再接新 Job。
3. 等待存量通话结束。
4. 再回滚 worker/API 版本。

trunk/rule 变更必须由运维显式执行并记录，不由应用在启动或回滚时自动修改。

## 31. 上线前待确认项

以下问题不阻塞设计，但必须在生产实现完成前给出明确答案：

1. 真实运营商/SBC 是否提供固定 SIP 出口 IP 或 SIP 认证。
2. 公司公开号码和测试号码分别是什么。
3. `company_reception` prompt profile 的正式内容、开场白和允许业务主题。
4. 录音失败采用 fail-open 还是 fail-closed。
5. 无人工可接时的业务处理：恢复 AI、留言还是结束通话。
6. 目标峰值并发、可接受的开场延迟和可用性目标。
7. worker 重派后是否需要基于已持久化对话做有限上下文恢复。
8. 公开号码回滚时的运营商备用路由。

## 32. 最终结论

Phase F 的正式目标形态是：

```text
self-hosted livekit-sip
  -> inbound trunk
  -> individual dispatch rule
  -> roomConfig.agents explicit dispatch
  -> AgentServer / JobContext per-call worker
  -> existing Qwen Realtime Runner
  -> existing P1 / recording / dialogue / handoff / semantic closure
```

这不是对现有 AI 通话业务能力的重写，而是把 SIP 呼入的实时运行 ownership 从 API 进程迁到专用 Job worker。

Phase F 最大的工程风险不是 Qwen 兼容性，而是以下三个跨进程边界：

1. worker 内事件和对话必须独立持久化。
2. API 对实时通话的控制必须通过持久化命令完成。
3. worker/webhook/Job 重派必须通过 SIP call key 幂等认领同一 `call_id`。

只有这三个边界通过故障和并发验收，才能把公开号码呼入称为正式生产能力。
