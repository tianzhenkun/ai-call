# AI Call 单通话所有权与跨进程命令设计

## 1. 文档状态

- 设计日期：2026-07-31
- 适用仓库：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- 目标阶段：正式多进程部署前的运行时可靠性改造
- 评审状态：第八轮合同化集中闭环候选稿；是否通过以闭环审查报告记录的冻结哈希和两轮只读冷审结果为准，P0=0、P1=0 前不得编写实现计划
- 真实拨打：设计、实现和自动化验证阶段禁止发起真实电话；真实验收必须再次获得用户明确确认

## 2. 问题本质

当前 AI Call 已经同时存在 API 进程和外呼执行器进程，但通话控制仍保留单进程假设：

- `InMemorySessionRegistry` 只保存当前进程创建的会话；
- `RealtimeCallAgentRunner` 的音频、响应、打断和恢复任务只存在当前进程；
- 转人工等待音、超时和异常收尾任务只存在当前进程；
- LiveKit webhook、坐席认领、`media-ready` 和结束通话请求可能到达另一个进程；
- 进程内事件、对话预览和坐席 SSE 推送不能天然跨进程传播。

因此，收到请求的进程不一定拥有对应通话。继续用“当前进程查找并操作本地任务”的方式，会产生等待音无法停止、挂机无法收尾、重复清理、状态回退和资源泄漏。

本设计不把所有服务永久合并成单进程，而是建立以下规则：

> 同一通电话在任意时刻只能由一个 Runtime Worker 控制；API 进程通过持久命令把操作路由给该 Worker。

## 3. 目标与非目标

### 3.1 目标

1. 明确 API、Runtime Worker、Dispatcher 和离线 Job Worker 的职责。
2. 为每个 `call_id` 建立可过期、可接管、带 fencing token 的所有权租约。
3. 使用数据库命令表保证命令可追溯、可重试，使用 Redis Streams 保证低延迟路由。
4. 让转人工接通、停止等待音、客户挂机、坐席挂机和录音收尾都在通话所属 Worker 串行执行。
5. 允许 webhook 和坐席请求重复、延迟或乱序，不允许终态被重新打开。
6. Worker 异常退出后能够安全清理通话、录音、LiveKit Room 和坐席状态。
7. 支持多通电话并行执行，同时保证同一通电话的命令顺序。
8. 让 Web、SIP 和正式外呼的新通话通过同一 `START_CALL` 分配流程首次获得所有者。
9. 让已发布、处理中和 Redis 故障期间的命令都有可验证的恢复路径。
10. 让 webhook、录音对账、离线 ASR、语义分析和话后决策不依赖可丢失的进程内队列。

### 3.2 非目标

1. V1 不恢复 Worker 崩溃前的完整 AI 实时上下文。
2. V1 不承诺 Worker 崩溃后客户无感继续与 AI 对话；接管 Worker 执行安全收尾。
3. 不把 Redis 作为通话、转人工、录音或任务状态的最终事实来源。
4. 不改变现有正式外呼任务、线路快照、Attempt、录音、语义分析和话后决策的业务含义。
5. 不在本阶段实现多地域部署、跨机房容灾或动态负载预测。
6. V1 不包含 `sip_inbound`。本文 `direct_sip` 仅表示应用 API 发起请求、Runtime Owner 创建 Room 并发起 SIP Participant 的受控外呼；由 trunk/dispatch rule 先创建 Room、再由 AgentServer `JobContext` 接管的呼入继续遵循 Phase F 独立设计，不进入本规格的创建型 `START_CALL`。

### 3.3 运行时不变量

后续表结构、状态机、接口和恢复器必须同时满足以下不变量；任何实现如果违反其中一条，即使正常流程能够运行也不得验收：

1. **数据库事实唯一**：Record、Command、Effect、Evidence、Inbox 和离线 Job 的数据库状态是唯一业务权威；Redis、SSE 和进程内任务只负责加速。
2. **单通话单 Owner**：任何时刻只有持有有效租约和当前 fencing token 的 Runtime Worker 可以提交通话业务状态或发起新的 Provider 副作用；旧 Owner 已在途的 Provider 调用可能迟到生效，但只能由 Effect、generation 和新 Owner 收敛，不能获得新的数据库写入资格。
3. **意图与执行分离**：非 Owner 可以通过 Command Repository 持久化经过授权的业务意图，但不能直接操作 Session、媒体或 Provider。
4. **终态单调**：任何已认证入口都可以幂等追加终止 Evidence 并建立单调终态屏障；Owner 失联不能阻止挂机，终态一旦建立不得重新打开。
5. **状态只能前进**：命令、Effect、Inbox 和 Job 的每次状态更新都必须使用旧状态、处理令牌和必要 fencing 条件；网络确认迟到不能把状态回退。
6. **副作用先登记后执行**：除 `OWN-04` 明确限定的本地 fail-closed 紧急媒体隔离外，所有 Provider 创建和销毁动作先持久化 Effect；调用结果不确定时先对账，不盲目重放。
7. **崩溃不恢复旧上下文**：V1 的新 Owner 不恢复旧 AI Session；依赖旧 Session 的普通命令全部得到 `SUPERSEDED` 决议，只允许安全终止和资源对账。
8. **租户授权不靠全局 ID**：即使 `call_id`、Room 或 Provider event ID 保持全局唯一，所有业务数据写入和查询仍必须使用权威 `tenant_id`。

### 3.4 规范性合同索引

本节为全文唯一的规范性合同索引。后续表结构、状态矩阵、故障恢复和测试必须引用这些 ID；正文不得用未编号的例外覆盖合同。若描述冲突，必须先修订合同和全部引用处，不能由实现者自行选择其中一种解释。

| 合同组 | ID | 规范性合同 |
| --- | --- | --- |
| 不变量 | `INV-01` | PostgreSQL 是 `owner_command_v1` 的唯一业务事实源；Redis、本地内存和 Provider 查询都不能覆盖数据库终态。 |
| 不变量 | `INV-02` | 一个 `call_id` 在任意数据库时刻最多只有一个有效 Runtime Owner；Runtime 提交必须匹配 Owner、fencing 和未过期租约。 |
| 不变量 | `INV-03` | 非 Runtime 角色只能持久化意图、证据、资源预占或确定性投影，不能直接操作 Session 或 Provider。 |
| 不变量 | `INV-04` | `terminal_requested_at` 是吸收性屏障；建立后不得清除，普通命令不得重新打开通话或登记新的非终止创建 Effect。 |
| 不变量 | `INV-05` | Provider 创建和销毁动作必须先登记 Effect；唯一例外是 `OWN-04` 定义的本地 fail-closed 紧急媒体隔离。 |
| 不变量 | `INV-06` | V1 不恢复旧进程的 AI Session 或对话上下文；Owner 丢失后，新 Owner 只允许安全收尾。 |
| 不变量 | `INV-07` | 租户业务记录和命令必须具有明确租户；未解析租户的 Provider 事件只能进入 Provider 级隔离区。 |
| Owner | `OWN-01` | 首次 Owner 只能由 Dispatcher 在完成 Worker 容量、SIP 线路和 Attempt 原子分配后写入。 |
| Owner | `OWN-02` | 过期 Owner 的接管和无 Owner attention 的到期重新分配只能由 Recovery Repository 按恢复矩阵和全局锁合同写入。 |
| Owner | `OWN-03` | Runtime 只能验证、续租和执行已分配给自己的 Owner；不得自行取得无主或过期 Record。 |
| Owner | `OWN-04` | 数据库不可达时，旧 Owner 必须在 monotonic 硬截止前停止本地 AI 媒体，且不得发起新的创建动作。 |
| Owner | `OWN-05` | 普通容量、短时 cleanup 执行容量和长期资源隔离必须分别表达；长期异常不得永久占住有限 cleanup 执行槽。 |
| Command | `CMD-01` | 同租户、同入口、同幂等键、同请求指纹返回原命令；同键异指纹拒绝。 |
| Command | `CMD-02` | 普通命令严格按 `command_seq` 前进，重试和重复投递不得跳号或回退决议游标。 |
| Command | `CMD-03` | `END_CALL` 可越过普通序号缺口并撤销旧 Command token；迟到普通命令只能 `SUPERSEDED`。 |
| Command | `CMD-04` | 只有数据库 CAS 能把命令变为可执行的 `PROCESSING`；Redis 消息本身不授予执行权。 |
| Command | `CMD-05` | `DISPATCHING` 发布权由 `dispatch_token` 隔离，迟到确认不得回退数据库状态。 |
| Command | `CMD-06` | 命令恢复必须按命令类型、Owner、Effect 和终态屏障决议，不能统一改投新 Worker。 |
| Effect | `EFF-01` | 来源 Command token 只授权首次登记 Effect；Effect 登记后拥有独立生命周期。 |
| Effect | `EFF-02` | Effect 认领和完成使用自身 Owner、fencing、token 和租约，并再次匹配 Record 当前 Owner。 |
| Effect | `EFF-03` | 旧 Owner、旧 Command token 或旧 Effect token 的迟到写入必须影响 0 行。 |
| Effect | `EFF-04` | 销毁 Effect 必须关联可能迟到的创建 Effect；在创建进入静默态前，任何销毁成功或资源不存在观察都不能成为最终销毁终态，静默后必须重新确认。 |
| Effect | `EFF-05` | `resource_cleanup_status=clean` 必须证明全部创建已进入静默态、全部销毁在静默门禁后确认终态、资源不存在且线路已释放。 |
| 写权限 | `WRITE-01` | API、Webhook、Outbound 和 Jobs 可创建命令、终止 Evidence 或自身 Job 状态，但不能提交 Runtime 结果。 |
| 写权限 | `WRITE-02` | Handoff Trigger 只创建 `requested`；Agent Console Claim 只执行 `requested -> accepted` 和 `available -> claiming` 预占。 |
| 写权限 | `WRITE-03` | 当前 Runtime Owner 是 Record 运行态、Handoff 运行态、坐席通话态和实时媒体状态的唯一写入者。 |
| 写权限 | `WRITE-04` | Attempt Reconciler 使用独立投影租约单调写 Attempt、Target、Task，不操作 Provider 或借用 Runtime Owner。 |
| 写权限 | `WRITE-05` | Inbox、ASR、语义分析等 Job Worker 只使用各自处理租约；产生 Runtime 动作时必须创建命令。 |
| 写权限 | `WRITE-06` | SIP Reservation 运行期转换同时匹配当前 Owner、Effect token 和 reservation token，且只能单调前进。 |
| START | `START-01` | 分配截止只适用于从未取得 Owner、Reservation 或 Effect 的 START；确认无资源后才允许无 END 失败。 |
| START | `START-02` | START 成功由同一组持久化 Room、Participant、Agent 和必要 Egress 事实共同证明，不能由单个返回值推断。 |
| START | `START-03` | `START_UNCERTAIN` 到聚合截止后必须离开普通 `preparing/RETRY_WAIT`，进入确认无资源失败或建立 END 后清理。 |
| START | `START-04` | 任一创建 Effect 登记后原 Owner 失联，新 Owner 不得恢复为可服务状态；必须建立终态屏障并安全收尾。 |
| END | `END-01` | 多来源终止只能形成一条 `END_CALL`，每个来源 Evidence 独立去重保存。 |
| END | `END-02` | END 基于全部已登记及可能迟到的创建动作预登记完整销毁图。 |
| END | `END-03` | 全部 SIP、Agent Participant 和 Egress 销毁 Effect `APPLIED` 前，`DELETE_ROOM` 依赖不得满足；任何销毁结论都必须通过对应创建静默门禁。 |
| END | `END-04` | END 逻辑完成与 Provider 资源清理完成分离，逻辑终态不能隐藏后续 Effect 对账。 |
| 数据库 | `DB-01` | 参与 V1 原子事务的业务表和控制表位于同一 PostgreSQL 数据源，隔离级别为 `READ COMMITTED`。 |
| 数据库 | `DB-02` | 所有同时持有两类以上业务行锁的 Repository 遵循一份覆盖 Runtime、业务、Evidence、Inbox/Quarantine 和离线 Job 的全局顺序；任务认领与业务提交分事务。 |
| 数据库 | `DB-03` | 租约、截止、屏障和条件认领使用数据库时间、行锁、CAS 与 `SKIP LOCKED`，不依赖应用本机时间。 |
| 路由 | `ROUTE-01` | Redis 只降低发现延迟；Redis 全丢失时数据库扫描仍能完成全部正确性路径。 |
| 路由 | `ROUTE-02` | Stream 消息的 ACK、重领和删除必须先读取数据库权威状态。 |
| 路由 | `ROUTE-03` | Worker 永久离线后，其旧 Stream 和 Pending 消息必须由跨 Worker janitor 收敛。 |
| Webhook | `WEBHOOK-01` | Webhook 在 Inbox 或 Quarantine 持久化成功后才返回 2xx；持久化失败返回可重试 5xx。 |
| Webhook | `WEBHOOK-02` | Quarantine 不能猜测租户；关联成功后才在同一事务写主 Inbox。 |
| Webhook | `WEBHOOK-03` | 多实例 Quarantine Worker 使用独立 processing owner、generation、token、租约和 CAS。 |

## 4. 现有进程内状态清单

| 状态 | 当前载体 | 新归属 | 处理方式 |
| --- | --- | --- | --- |
| 通话 Session 和状态转换 | `InMemorySessionRegistry` | Runtime Worker | 保持内存执行，但必须受通话租约约束 |
| AI Agent、音频传输、播放队列 | `RealtimeCallAgentRunner` | Runtime Worker | 只允许所有者创建、停止和操作 |
| 回合响应、打断、恢复、清音任务 | Agent Runner 内部 `asyncio.Task` | Runtime Worker | 租约丢失时立即取消 |
| 转人工提示、等待音 | `AiCallHandoffExceptionManager` | Runtime Worker | 由 `AGENT_MEDIA_READY` 或终态统一停止 |
| 转人工超时和异常收尾 | `AiCallHandoffExceptionManager` | Runtime Worker | 运行任务归所有者，持久状态支持恢复扫描 |
| 实时事件 | `InMemoryEventStore` | Runtime Worker | 所有者产生，关键事件继续持久化 |
| 实时对话预览 | `AiCallDialogueRuntimeStore` | Runtime Worker | 所有者聚合并持续落库 |
| 通话、转人工、录音、Attempt | 数据库 | 共享事实 | 保持最终权威 |
| 坐席状态与当前会话 | 数据库 | 共享事实 | 保持最终权威 |
| 坐席 SSE 连接 | API 进程 | API 本地连接 | Redis 广播事件，各 API 实例只推送本地连接 |
| webhook 后台处理 | API 进程内 `asyncio.Task` | 持久 Inbox Worker | 请求返回前先落 Inbox，不能确认后丢失 |
| 离线 ASR、语义分析、录音对账 | 业务表 + Job Worker 进程内队列 | 数据库认领的独立后台任务 | 业务表作为任务事实，队列只负责唤醒，不持有实时通话所有权 |

## 5. 方案选择

### 5.1 方案 A：Redis Streams + 数据库命令表（采用）

- 数据库保存命令、状态、重试和结果；
- Redis Streams 将命令快速投递到所属 Worker；
- Redis 故障时由数据库 Dispatcher 补偿；
- 重复投递由幂等键和命令状态消除。

该方案同时满足实时性、可追溯性和故障恢复要求。

### 5.2 方案 B：只使用 Redis Streams（不采用）

实现较少、投递快，但 Redis 数据清理、故障恢复和运维误操作可能造成关键命令不可追溯，不适合挂机和录音收尾。

### 5.3 方案 C：只使用数据库命令表轮询（不采用）

数据可靠，但低延迟需要高频轮询，并发增加后会放大数据库查询和锁竞争。

## 6. 目标进程架构

```text
API 实例
  ├── Web / SIP 通话创建
  ├── 管理接口
  ├── 坐席 claim / media-ready / complete
  ├── LiveKit webhook
  └── 写 Runtime Command / Webhook Inbox

Outbound Worker
  ├── 认领 Task / Target / Attempt
  ├── 写 START_CALL
  └── 异步收口 Attempt / Target / Task

ai_call_runtime_command
                 │
                 ▼
Command Dispatcher
  ├── 扫描待分配、待发布和过期命令
  ├── 为 START_CALL 选择可用 Runtime Worker
  ├── 查询或原子写入 call_id 当前所有者
  └── 投递 Redis Stream
                 │
                 ▼
ai-call:runtime:{owner_instance_id}
                 │
                 ▼
Runtime Worker
  ├── 注册实例、容量和心跳
  ├── 验证并续租 Dispatcher/Recovery 已分配的通话所有权
  ├── 正式拨号
  ├── AI 实时会话与音频
  ├── 转人工和等待音
  ├── 录音控制
  └── 通话统一收尾
                 │
                 ▼
数据库事实 + Redis 坐席事件
                 │
                 ▼
Job Worker
  ├── Webhook Inbox
  ├── 录音对账 / ASR / 语义分析
  └── 话后决策 / 跟进执行
```

API 不再直接操作：

- `orchestrator.registry`；
- Agent Runner 内部任务；
- 等待音任务；
- 实时音频传输；
- 通话所属 LiveKit Room 的破坏性操作。

API 可以继续直接执行不依赖运行时内存的查询、数据库认领事务和 Token 签发。Token 签发不是 Runtime 状态写入，但必须服从第 15 节的签发门禁；Room 尚未由 `START_CALL` 创建完成、Owner 已失效或终态屏障已经建立时，API 不得签发加入该 Room 的新 Token。

## 7. 通话所有权租约

在 `ai_call_record` 增加：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `tenant_id` | varchar(20) | 通话所属租户；与平台用户租户字段一致，命令租户必须从该字段读取 |
| `runtime_control_mode` | varchar(32) | `legacy_local` 或 `owner_command_v1`，创建后不可变 |
| `runtime_owner_id` | varchar(128) nullable | 当前 Runtime Worker 实例 ID |
| `runtime_fencing_token` | bigint | 所有权版本，初始为 0，每次接管递增 |
| `runtime_lease_expires_at` | datetime nullable | 当前租约过期时间 |
| `runtime_heartbeat_at` | datetime nullable | 最近成功续租时间 |
| `runtime_capacity_class` | varchar(16) | 当前容量归属：`none`、`active`、`cleanup` 或 `attention`，初始为 `none`；`attention` 不占 Worker 执行槽，但保留资源隔离事实 |
| `startup_reconcile_deadline_at` | datetime nullable | `START_CALL` 已调用 Provider 但结果不确定时的聚合保护截止时间；区别于分配前的 `allocation_deadline_at` |
| `startup_reconcile_policy_version` | varchar(64) nullable | 计算聚合保护截止时间所用策略版本 |
| `startup_reconcile_budget_json` | text nullable | 资源依赖 DAG、各调用超时、迟到窗口和计算结果的 JSON 字符串快照，不使用 `jsonb` |
| `agent_participant_identity` | varchar(255) nullable | 当前 generation 的 AI Agent Participant identity |
| `agent_participant_sid` | varchar(255) nullable | LiveKit 当前 AI Participant SID |
| `agent_audio_track_sid` | varchar(255) nullable | 当前 AI 输出音轨 SID |
| `agent_resource_generation` | bigint nullable | AI Participant 对应的 fencing generation |
| `agent_media_ready_at` | datetime nullable | 当前 generation 完成连接并发布音轨的数据库验证时间 |
| `next_command_seq` | bigint | 下一条命令序号，初始为 1 |
| `last_applied_command_seq` | bigint | 最后已经完成决议的命令序号，初始为 0 |
| `terminal_requested_at` | datetime nullable | 已受理终止请求的数据库时间；非空后禁止非终止动作产生新副作用 |
| `resource_cleanup_status` | varchar(32) | `not_started`、`reconciling`、`clean`、`attention_required` |
| `resource_cleanup_error` | varchar(1000) nullable | 最近资源清理异常 |
| `resource_cleanup_next_retry_at` | datetime nullable | `attention_required` 停放后下一次允许 Recovery 分配 cleanup Owner 的数据库时间 |
| `resource_cleanup_completed_at` | datetime nullable | Provider 资源全部确认终态的时间 |

约束和索引：

- 不创建物理外键；
- `(runtime_owner_id, runtime_lease_expires_at)` 建普通索引；
- 所有 API 中的 bigint 继续按字符串返回；
- 本文表格中的 `datetime` 在 V1 PostgreSQL 迁移中统一实现为 `timestamptz` 并保存 UTC；API 输出再转换展示时区；
- 租约判断、续租、命令租约和终态屏障一律使用数据库时间，不能依赖 Worker 本机时间；
- 现有数据的 `tenant_id` 必须从现有权威业务关联回填并校验，无法确定租户的数据不得进入新控制模式。

容量与 Owner 字段必须满足以下数据库 CHECK 或等价 Repository 不变量：`active/cleanup` 必须有非空 Owner 和租约；`attention` 必须同时满足 Owner/租约为空、`resource_cleanup_status=attention_required`、`resource_cleanup_next_retry_at` 非空；`clean` 必须同时满足容量为 `none`、Owner/租约为空和 `resource_cleanup_completed_at` 非空。若迁移阶段因历史数据暂不能直接建立 CHECK，启动前数据审计和所有写 Repository 仍必须执行同一条件，完成回填后补上约束。

Record 的 `completed/failed` 是业务通话逻辑终态，不代表 Provider 资源已经全部清理。存在 `PENDING/APPLYING/RECONCILE_REQUIRED` 的终止 Effect 且仍处于自动清理窗口时，`resource_cleanup_status` 必须为 `reconciling`；超过自动清理时限或 Provider 长期不可确认并完成停放事务后，允许在 Effect 仍为 `RECONCILE_REQUIRED` 时写 `attention_required`。全部创建 Effect 已进入 `EFF-04` 定义的静默态、全部销毁 Effect 已在静默门禁后确认并进入 `APPLIED`、Room、Agent Participant、SIP Participant 和 Egress 均确认不存在且 SIP 线路 Reservation 已释放后，才允许按 `EFF-05` 写 `clean`。`attention_required` 必须继续展示清理告警并保留资源和线路隔离，但按 `OWN-05` 释放 Record 原先占用的 Runtime active 或 cleanup 执行槽，不能因 Record 已完成而隐藏，也不能永久占住 Worker。

新增 `ai_call_runtime_worker` 表记录可参与分配的 Worker：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `worker_id` | varchar(128) | Runtime Worker 实例 ID |
| `status` | varchar(32) | `STARTING`、`READY`、`DRAINING`、`OFFLINE` |
| `capacity` | integer | 最大并发通话数 |
| `active_call_count` | integer | 当前由该 Worker 持有且 `runtime_capacity_class=active` 的通话数 |
| `cleanup_capacity` | integer | 最大并发终止/恢复清理数，独立于正常通话容量 |
| `active_cleanup_count` | integer | 当前由该 Worker 持有且 `runtime_capacity_class=cleanup` 的通话数 |
| `heartbeat_at` | datetime | 最近心跳数据库时间 |
| `lease_expires_at` | datetime | Worker 注册租约过期时间 |
| `stream_cleanup_owner_id` | varchar(128) nullable | 当前认领该离线 Worker 旧 Stream 清理任务的 Dispatcher/Janitor 实例 |
| `stream_cleanup_token` | varchar(128) nullable | 旧 Stream 清理尝试令牌 |
| `stream_cleanup_expires_at` | datetime nullable | 旧 Stream 清理租约到期时间 |
| `stream_cleanup_after` | datetime nullable | 清理失败后的下一次扫描时间 |
| `created_at` / `updated_at` | datetime | 审计时间 |

Worker 注册表是首次分配和容量判断的数据库事实。Redis 只负责唤醒，不单独决定 Worker 是否可用。

`worker_id` 必须包含部署实例身份和本次启动 UUID；两个同时存活的进程不得复用同一 ID。进程重启生成新 ID，旧 ID 只由恢复器清理。

容量计数以 Record 的 `runtime_capacity_class` 为逐通话事实，Worker 计数是可重算汇总：

- 首次分配在同一事务中将 Record 从 `none -> active`，并将目标 Worker `active_call_count + 1`；
- 当前 Owner 受理 `END_CALL` 后必须立即开始终止，不能因为清理槽已满而延迟挂机；有空闲清理槽时可在同一事务中将 `active -> cleanup`，同时 `active_call_count - 1`、`active_cleanup_count + 1`；
- 清理槽已满时，当前 Owner 保持 `active` 容量完成安全收尾，不能先释放正常容量再无界创建清理任务；
- Worker 失联时，cleanup Owner 分配事务按固定锁顺序将旧 Worker 对应容量减一，并将 Record 转为新 Worker 的 `cleanup` 容量；只有成功占用清理槽的新 Worker 才能接管；
- 只有满足 `EFF-05`（全部创建静默、全部销毁在门禁后确认、资源不存在且线路已释放）后，才能根据 Record 当前容量类别原子递减对应 Worker 计数，并在同一事务写 `runtime_capacity_class=none`、清空 Owner/租约和下次重试时间以及 `resource_cleanup_status=clean/resource_cleanup_completed_at`；
- 一次有界 cleanup 尝试结束后仍无法确认资源存在性时，在同一事务中按原容量类别执行 `cleanup -> attention` 并递减当前 Worker `active_cleanup_count`，或执行 `active -> attention` 并递减当前 Worker `active_call_count`；两条路径都清空 `runtime_owner_id/runtime_lease_expires_at`、写 `resource_cleanup_next_retry_at`，并保持 SIP Reservation、Effect、资源键和告警不变；
- 停放事务只能在本次本地 Provider 调用已返回、超时或取消后执行；相关 Effect 必须先以当前 token 提交为 `RECONCILE_REQUIRED` 并清除 processing owner/token/租约，`END_CALL` 满足逻辑完成条件后提交终态，随后才清空 Record Owner；旧调用迟到结果只能由下一次 cleanup Owner 查询回填；
- `attention` 到期后只能由 Recovery Repository 重新选择有清理槽的 Worker，将 `attention -> cleanup`、`resource_cleanup_status: attention_required -> reconciling`、递增 fencing、写入临时 cleanup Owner 并增加目标 Worker `active_cleanup_count`；Runtime 不得自行从 `attention` 取回 Owner；本次仍失败时再原子停放回 `attention_required`；
- `attention` 不计入任何 Worker 的 `active_call_count/active_cleanup_count`，但未释放 SIP Reservation 继续计入线路 `max_concurrency`，防止未知真实通话突破线路上限；
- Record 已是 `none` 的重复完成、重试或迟到恢复不得再次递减计数；
- Worker 失联或计数异常时，由恢复器按 Record 的 Owner 和 `runtime_capacity_class` 重新计算两个计数，不把 Worker 计数当作不可修正的绝对事实。

### 7.1 新通话首次分配与正式外呼主链

所有进入 `owner_command_v1` 的新通话都通过 `START_CALL` 获得 Owner；迁移期未启用入口继续按 `legacy_local` 创建。新模式创建命令的上游职责按入口明确区分：

- Web、音色试听和直接 SIP：API 在同一事务中创建 `ai_call_record` 与序号为 1 的 `START_CALL`；
- 正式外呼和 Linphone 验证：`OutboundTaskExecutor` 只认领 Task、Target、Attempt，并在同一事务中创建预分配标识的 `ai_call_record` 与 `START_CALL`；它不得等待 Provider 拨号或通话终态；
- Mock 只替换 `START_CALL` 使用的底层拨号适配器，不得绕开命令和 Owner 分配。

统一流程：

1. 创建方先根据入口业务请求计算不包含服务端生成 ID 的 `START_CALL` 创建指纹，并按 `(tenant_id, idempotency_key)` 查询已有命令；
2. 命中相同指纹时直接返回原 `call_id/command_id`；命中不同指纹时返回 `409 IDEMPOTENCY_CONFLICT`，不得先生成新的 `call_id`；
3. 未命中时才生成 `call_id`，并在同一事务中创建 `runtime_control_mode=owner_command_v1`、无 Owner 的 Record 和序号为 1 的 `START_CALL`；
4. 创建事务提交后返回 `202 ACCEPTED`、`call_id` 和 `command_id`，不把 Runtime 尚未完成描述为通话已建立；
5. Dispatcher 从 `ai_call_runtime_worker` 中选择租约有效、状态为 `READY` 且未满载的 Worker；
6. Dispatcher 通过数据库条件更新原子写入 Owner、递增 fencing token 和占用容量；竞争失败时重新选择；
7. `START_CALL` 投递给新 Owner，由 Runtime Worker 的 `StartCallHandler` 创建 Session、LiveKit Room、Agent Runner，并调用非阻塞启动型拨号适配器；
8. `StartCallHandler` 只负责将外呼推进到“已向 Provider 发起并可由持久事实追踪”，不得等待振铃、接通或整通电话结束；
9. 独立 `OutboundAttemptReconciler` 根据 Record、媒体证据、Provider 事件和终态命令异步收口 Attempt、Target 与 Task；
10. `START_CALL` 成功后持久化 Room 和运行时启动结果；Web 客户端此时才可通过 bootstrap 获取 Room 信息并由 API 签发 Token。

并发创建时，两个事务可能同时查询为不存在并各自生成 `call_id`。数据库以 `(tenant_id, idempotency_key)` 唯一约束决定唯一赢家；失败事务必须整体回滚其 Record 和 Command，然后读取赢家并比较创建指纹。禁止只回滚 Command 而遗留第二条孤儿 Record。

现有同步 `OutboundTaskExecutor.execute_claimed() -> SipOutboundDialer.dial()` 会等待整个通话终态，不能直接搬进 `StartCallHandler`。实施时必须拆成：

```text
OutboundTaskExecutor
  -> 认领 Task / Target / Attempt
  -> 创建 Record + START_CALL

Runtime StartCallHandler
  -> OutboundDialer.start()
  -> SipOutboundDialer.start()
  -> 返回 STARTED / START_UNCERTAIN

OutboundAttemptReconciler
  -> 根据持久 Record / Provider / Effect 证据
  -> 收口 Attempt / Target / Task
```

因此保留的是正式业务适配器关系，不保留“Executor 同步等待 `dial()` 到通话结束”的调用方式。`OutboundTaskExecutor`、`OutboundDialer` 和 `SipOutboundDialer` 仍是正式外呼唯一业务入口与拨号适配器，Linphone/Mock 不另建产品流程。

Token 不写入命令表或 Redis。API 根据已持久化的 Room、参与者身份和当前授权即时签发。

没有可用 Worker 时，`START_CALL` 保持 `PENDING` 并返回明确的“等待运行资源”状态；超过业务等待上限后进入失败终态，不允许 API 进程回退为本地创建 Session。

`START_CALL` 的等待上限必须在创建事务中写入持久化 `allocation_deadline_at`，不能由某个 Dispatcher 的本机计时器决定。Dispatcher 使用数据库时间发现到期命令：尚未产生任何 Provider Effect、Owner 或 Reservation 时，将命令置为 `DEAD`、Record 置为 `failed`，正式 Attempt 明确失败；已经占用任一资源或存在结果不确定 Effect 时不得按普通排队超时释放，必须进入对应 Effect 和资源对账。

创建方和 Dispatcher 还必须实施有界背压与公平分配：

- Outbound Executor 按租户、Task 和线路限制未分配 `START_CALL`/`QUEUED` Attempt 数量，达到上限时停止继续认领 Target，不把整批名单一次性变成待分配命令；
- Web、试听和直接 SIP 的单次请求仍只创建一条命令，但必须受租户级待分配上限保护，超过时返回明确的资源繁忙结果，不创建孤儿 Record；
- Dispatcher 在候选批次中按租户和线路轮转，不能长期只按全局最早 `created_at` 让单个大任务垄断 Runtime 或线路；
- 背压上限、当前排队数、最长等待时间和超时数必须形成指标；调整阈值不改变 `allocation_deadline_at` 已持久化的现有命令。

正式 SIP、直接 SIP 和 Linphone 验证还必须受线路 `max_concurrency` 约束。Runtime Worker 容量与 SIP 线路槽是两个独立资源，不能用 Worker `capacity` 代替线路并发。

新增 `ai_call_sip_line_reservation` 持久化线路占用：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `tenant_id` | varchar(20) | 租户隔离 |
| `line_id` | bigint | 线路 ID，无物理外键 |
| `call_id` | varchar(64) | 全局唯一通话 ID |
| `attempt_id` | bigint nullable | 正式外呼 Attempt；Web/直接 SIP 可以为空 |
| `status` | varchar(32) | `RESERVED`、`ACTIVE`、`RECONCILE_REQUIRED`、`RELEASED` |
| `reservation_token` | varchar(128) | 本次占槽令牌 |
| `fencing_token` | bigint | 占槽时的 Runtime fencing generation |
| `acquired_at` | datetime | 数据库占槽时间 |
| `reconcile_after` | datetime nullable | 不确定状态下的下次对账时间 |
| `released_at` | datetime nullable | 明确释放时间 |
| `error_message` | varchar(1000) nullable | 最近异常 |
| `created_at` / `updated_at` | datetime | 审计时间 |

约束与索引：

- `call_id` 全局唯一，保证同一通话最多持有一个线路槽；
- `(tenant_id, line_id, status, acquired_at)` 用于线路占用统计和恢复；
- 不创建物理外键；
- `RESERVED`、`ACTIVE` 和 `RECONCILE_REQUIRED` 均计入 `max_concurrency`，只有 `RELEASED` 不计入。

所有同时持有两类以上业务行锁的事务都使用 `DB-02` 的唯一全局锁顺序。未参与当前事务的行类可以跳过，但禁止先锁定后序行类、再回头锁定前序行类：

```text
Outbound Task
  -> Outbound Target
  -> Outbound Attempt
  -> Record
  -> SIP Line（如有）
  -> Runtime Worker（涉及多个 Worker 时按 worker_id 升序）
  -> Handoff（涉及多个时按 handoff_id 升序）
  -> Agent Presence（涉及多个坐席时按 agent_id 升序）
  -> Command
  -> Reservation
  -> Effect / Effect Dependency
  -> End Evidence / Handoff Media Evidence
  -> Runtime Event / Dialogue Segment
  -> Recording / Recording Track
  -> ASR Job
  -> Semantic Analysis
  -> Post-call Decision / Follow-up Task
  -> Webhook Inbox
  -> Webhook Quarantine
```

同类多行按主键或稳定业务键升序锁定。任何 Outbound 创建/投影、首次分配、Handoff 认领、Runtime 状态提交、正常终止、cleanup Owner 接管、容量转换、Webhook/Quarantine 关联、录音与离线 Job 推进和恢复器都不得另行定义相反顺序。Outbound Reconciler 如需同时写 Task、Target、Attempt，必须按 `Task -> Target -> Attempt` 认领后再读取或锁定 Record；Dispatcher 如需推进 Attempt，必须先锁定其 `Task -> Target -> Attempt`，再锁定 Record。

Inbox、Quarantine、Attempt 投影和离线 Job 的批量候选认领允许在单独的短事务中只锁定自身任务行、写入 processing/reconcile token 后立即提交；该认领事务不得同时锁定业务父行。Worker 完成外部计算后的结果提交必须开启新事务，先按上述顺序锁定 Record 等业务父行和需要创建的下游事实，最后锁定自身 Inbox/Quarantine/Job 行并重新校验 token、generation 和未过期租约；校验失败时整个提交回滚。禁止持有通过 `SKIP LOCKED` 取得的任务行，再回头锁定 Record、Handoff、Command 或上游 Job。读取候选 Worker、Record 或业务父行可以在事务外进行，但最终事务必须按上述顺序重新读取并校验，竞争失败时回滚重选。

SIP `START_CALL` 首次分配必须在一个短数据库事务中同时获得 Runtime 容量和线路槽：

1. 正式 Outbound 先按 `Task -> Target -> Attempt` 锁定业务行；所有入口随后锁定 Record，再按全局顺序锁定 SIP Line 和候选 Runtime Worker；
2. 在锁定 Line 后统计未释放 Reservation；达到 `max_concurrency` 时不分配 Owner、不占 Worker 容量，命令保持 `PENDING` 并展示“等待线路资源”；
3. Line 有槽且 Worker 有容量时，原子创建 `RESERVED` Reservation、写入 Owner/fencing、递增 Worker `active_call_count`，并将正式 Attempt 从 `QUEUED` 推进为 `STARTING`；
4. 任一步竞争失败时整个事务回滚，不能只持有其中一个资源；
5. Worker 调用 Provider 被明确受理后，只持久化 Effect/Provider 事实并将 Reservation 置为 `ACTIVE`；`OutboundAttemptReconciler` 读取这些事实后，使用自己的处理租约将 Attempt 由 `STARTING` 单调投影为 `DIALING`；
6. Provider 结果不确定时 Reservation 进入 `RECONCILE_REQUIRED` 并继续计入线路并发，直到确认资源不存在或通话终止；
7. 明确未产生 SIP 资源的启动失败、明确挂机终态或对账确认资源不存在时，通过 `reservation_token` 条件更新为 `RELEASED`；
8. Worker 崩溃不能仅凭租约超时释放线路槽；Reconciler 必须结合 SIP Participant、Effect 和 Provider 事实决议，防止实际通话仍存在却突破线路并发。

Attempt 状态语义统一为：

```text
QUEUED
  # 已创建 START_CALL，等待 Runtime 和线路双资源，不占线路槽

STARTING
  # 已原子获得 Runtime Owner 与线路槽，准备或正在调用 Provider

DIALING
  # Provider 已受理并开始拨号，继续占线路槽

IN_CALL
  # 已有接通/媒体证据，继续占线路槽

COMPLETED / FAILED / CANCELED
  # 已有明确业务终态；线路槽仍以 Reservation/Provider 对账结果为准释放
```

Target 在 Attempt 为 `QUEUED/STARTING/DIALING/IN_CALL` 时不得被其他 Outbound Worker 重复认领。`QUEUED` 由 Outbound 创建，`STARTING` 由 Dispatcher 在首次双资源分配事务中写入，`DIALING/IN_CALL/COMPLETED/FAILED/CANCELED` 由 `OutboundAttemptReconciler` 根据持久事实单调投影。页面只有在 Attempt 进入 `DIALING` 后才展示“拨号中”；`QUEUED` 显示“等待运行资源”或“等待线路资源”，`STARTING` 显示“正在发起拨号”。

`OutboundAttemptReconciler` 的多实例认领不能借用 Runtime Owner 或 Command 处理租约。`ai_call_outbound_attempt` 增加以下投影租约字段：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `reconcile_owner_id` | varchar(128) nullable | 当前 Attempt 投影 Worker |
| `reconcile_token` | varchar(128) nullable | 每次认领生成的新令牌 |
| `reconcile_expires_at` | datetime nullable | 投影处理租约 |
| `reconcile_after` | datetime nullable | 下次需要投影或对账的数据库时间 |
| `reconcile_attempt_count` | integer | 投影尝试次数 |

增加 `(status, reconcile_after)` 和 `(reconcile_expires_at)` 索引。Reconciler 只能通过单条 CAS 认领待投影或租约过期的 Attempt，提交 Attempt、Target、Task 时必须匹配 `reconcile_owner_id + reconcile_token + reconcile_expires_at`；影响行数为 0 时立即放弃。它只投影持久事实，不取得 Record Owner 权限，也不得调用 Provider。

### 7.2 租户来源和创建入口权限

`owner_command_v1` 通话禁止使用代码默认值猜测租户：

- 后台 Web 和音色试听从已认证用户读取 `tenant_id`；
- 正式外呼和 Linphone 验证从 Task/Attempt 的权威租户字段读取；
- 内部 SIP 创建接口必须使用已认证用户或签名服务身份；
- LiveKit webhook 根据已持久化的 Room、Participant 或 call_id 反查 Record 租户；
- 请求参数中的 `tenant_id` 只可用于与权威值比对，不能作为唯一事实。

当前没有认证上下文的 `/sessions` 和 `/sip-sessions` 在迁入新模式前必须补充认证；如果保留为本地调试入口，只能在明确的开发开关下写入 `legacy_local`，正式环境启动时必须拒绝该配置。

租户字段以平台 `sys_user.tenant_id` 的 `varchar(20)` 为逻辑规范。现有 Outbound Task/Target/Attempt 的物理字段为 `varchar(64)`，迁移前不得直接缩短；先验证全部有效值长度不超过 20，再决定是否单独收窄。新表与补字段统一使用 `varchar(20)`。

| 表或对象 | 当前租户字段 | V1 处理 | 回填或权威来源 | 隔离与唯一约束 |
| --- | --- | --- | --- | --- |
| `ai_call_outbound_task/target/attempt` | `varchar(64)` | 暂保留物理宽度，逻辑校验不超过 20 | 已有 Task 租户 | 所有认领和状态查询带租户 |
| `ai_call_record` | 缺失 | 新增 `varchar(20)` | Task/Attempt、认证用户或已认证 SIP 服务身份 | 保留 `call_id` 全局唯一；业务查询同时带 `tenant_id` |
| `ai_call_event` | 缺失 | 新增 `varchar(20)` | 关联 Record | 事件写入、批量持久化和查询同时带 `tenant_id`；保留 `event_id` 现有全局唯一语义 |
| `ai_call_dialogue_segment` | 缺失 | 新增 `varchar(20)` | 关联 Record | 唯一约束和查询增加租户边界；语义分析快照不得跨租户读取客户原话 |
| `ai_call_recording` 与分轨 | 缺失 | 新增 `varchar(20)` | 关联 Record | 录音、Egress 查询必须带租户 |
| `ai_call_asr_job` | 缺失 | 新增 `varchar(20)` | 录音资产所属 Record | Job 幂等键和认领带租户 |
| `ai_call_semantic_analysis` | 缺失 | 新增 `varchar(20)` | Record | `(tenant_id, call_id, analysis_scene_code)` 唯一 |
| Handoff、Agent、ACW、Follow-up | 已有 `varchar(20)` | 保持 | 现有认证或 Record | 继续使用现有租户约束 |
| Handoff Media Evidence | 新表 | 使用 `varchar(20)` | Handoff 与 Record | Evidence 去重、版本和查询均带租户 |
| Runtime Command、Effect、Effect Dependency、SIP Line Reservation | 新表 | 使用 `varchar(20)` | Record、Attempt 或 Line | 所有幂等键和业务查询均带租户 |
| Webhook Inbox | 新表 | 使用 `varchar(20)` | Record | Inbox 业务查询和命令创建带租户 |
| Webhook Quarantine | Provider 级隔离表 | 关联成功前无租户 | 由 Room/Participant/Record 对账后写 resolved tenant | 不进入租户 API，不得猜默认租户 |

ORM、迁移、Repository 和 Worker 认领必须同时落地租户隔离；不能只增加字段后继续按 `call_id` 跨租户查询。历史数据无法从权威关联确定租户时保持 `legacy_local`，不得猜成默认租户进入新模式。

租户隔离不能放松 Provider 命名空间约束：

- `ai_call_record.call_id` 继续保留全局唯一约束，不能改成仅 `(tenant_id, call_id)` 唯一；
- `room_name=ai-call-{call_id}` 在 LiveKit 集群内全局唯一，数据库增加 `room_name` 唯一约束；
- Participant identity 在 Room 内唯一；AI identity 额外携带 fencing generation；
- Webhook 允许先按全局唯一 Room、`Room + Participant identity` 或 `call_id` 定位 Record，再校验和绑定其 `tenant_id`；
- 所有面向业务用户的 Record、Recording、Handoff、ASR 和语义查询仍必须同时带 `tenant_id`，全局唯一 ID 不是绕过租户授权的理由。

### 7.3 Owner 分配、验证与续租边界

Runtime Worker 永远不得自行扫描并抢占无 Owner 或租约过期的 Record。Owner 只有两条数据库权威写入路径：

1. **Dispatcher 首次分配**：对从未分配的 `START_CALL`，按第 7.1 节同时锁定 Record、线路和 Worker 容量，写入 Owner、递增 fencing token、占用容量并推进必要的 Reservation/Attempt 分配事实；
2. **Recovery Repository 恢复分配**：Owner 租约过期，或无 Owner 的 `attention` 已到 `resource_cleanup_next_retry_at` 后，先按第 8 节恢复矩阵和 Effect 事实判断是可重新执行的“尚无 Effect START_CALL”，还是只允许安全终止的 cleanup 接管，再按全局锁顺序分配新 Owner 和容量。

Runtime Worker 只允许：

- 校验 Record 已经由 Dispatcher/Recovery 分配给自己的 `runtime_owner_id` 和 fencing token；
- 在 Owner、fencing token、租约旧值和 Worker 注册租约均匹配时续租；
- 获取对应 Command/Effect 的独立处理租约后执行动作。

Runtime 收到目标为自己但 Record 尚未分配、Owner 已变化或租约已过期的 Redis/数据库消息时，只能拒绝领取并通知 Dispatcher/Recovery 扫描，不能通过本地 CAS “顺手获得”所有权。首次分配和接管都必须先完成容量、线路、Attempt 和全局锁顺序合同，不能由 Runtime 绕过。

### 7.4 续租

- 默认每 5 秒续租；
- 默认租约有效期 15 秒；
- 每次发起数据库续租前记录 `renew_started_monotonic`；只有续租成功才将它保存为 `last_successful_renew_started_monotonic`，不能用响应返回时刻作为租约起点；
- Runtime 首次成功验证 Dispatcher/Recovery 已分配给自己的 Owner 时，按同样方式初始化 watchdog 基准；
- 每通电话启动独立 fail-closed watchdog，本地硬截止时间为 `last_successful_renew_started_monotonic + lease_ttl - safety_margin`，默认 `15 - 3 = 12` 秒；
- 任意一次续租返回 fencing token 已变化时立即失权；数据库超时或不可达时不得延长本地截止时间，到达硬截止时间必须失权，不依赖“连续失败次数”或下一次数据库请求；
- 失权 Worker 立即取消本地 AI 输出、音频发布、等待音、响应和超时任务，并主动静音或断开本进程当前持有、identity 与失权前 generation 完全匹配的 AI Participant；这是 `INV-05` 唯一允许未先新登记 Effect 的 fail-closed 紧急例外，不等待数据库恢复；
- 失权后不再执行新的创建型外部副作用；无法确认结果的在飞行调用由 Effect 进入 `RECONCILE_REQUIRED`；
- 紧急静音/断开不得提交 `completed/failed/clean`、Effect `APPLIED` 或任何业务成功状态；数据库恢复后，Recovery Repository 必须依据原有创建 Effect 和资源键分配 cleanup Owner，通过标准终止 Effect 图确认并持久化最终结果，不能把本地调用返回值当成恢复证据；
- Worker 注册租约和各通话租约分别续租，Worker 进入 `DRAINING` 后不再接受新通话，但继续续租并收尾已有通话。

AI Participant 使用 `agent-{call_id}-g{runtime_fencing_token}` identity，并在 metadata 中写入 `call_id`、`runtime_owner_id` 和 `resource_generation`。客户与坐席 Participant 可以保持既有稳定 identity，但所有 AI 输出必须来自当前 generation。

### 7.5 接管

V1 接管流程：

1. 确认租约已过期；
2. Recovery Repository 按全局锁顺序原子递增 fencing token、转换容量并写入新所有者；
3. 查询 LiveKit Room，先静音或断开所有 generation 小于当前 fencing token 的 AI Participant；
4. 确认 Room 中不再存在旧 generation AI 音频发布；
5. 查询数据库、Agent Participant、SIP Participant、线路 Reservation、录音和 handoff 状态；
6. 对仍在运行的通话执行安全收尾；
7. 写入明确的恢复原因和终态。

运行时写入权限按下表分层，不能把“创建控制意图”和“执行运行时业务状态”混为同一种权限：

| 写入类型 | 允许角色 | 是否要求有效 Owner/fencing/租约 | 必须满足的数据库条件 | 明确禁止 |
| --- | --- | --- | --- | --- |
| 创建普通命令、分配 `command_seq` | 已认证 API、Outbound、Job 或 Runtime | 否 | 锁定 Record；校验租户、`runtime_control_mode`、幂等键和终态屏障 | 直接执行运行时副作用 |
| 决议未分配 `START_CALL` 排队超时 | Dispatcher/Recovery Repository | 否 | 锁定 Record 和 Command；数据库时间超过 `allocation_deadline_at`；无 Owner、Reservation 和任何 Effect；Record 容量为 `none` | 对已产生或无法排除 Provider 资源的命令按排队超时释放 |
| 追加终止 Evidence、单调建立终态屏障、创建或读取唯一 `END_CALL` | Command Repository，可由 API、Webhook、Outbound、Job 或 Runtime 调用 | 否 | 锁定 Record；只允许 `terminal_requested_at: null -> 数据库时间`、状态单调进入 `ending`、前序命令决议和 Evidence 追加 | 清除终态屏障、重新打开终态、直接写 `PROCESSING` |
| 追加坐席媒体 Evidence、单调递增 `media_state_version`、创建媒体失效命令 | Webhook Inbox/Command Repository | 否 | 锁定 Record 和 Handoff；只允许追加脱敏 Evidence、版本递增和写 `media_invalidated_at` | 直接修改 Handoff 业务状态、坐席状态、录音或 Session |
| 创建 Handoff 请求 | Handoff Trigger Repository，可由已持久化触发 Job 调用 | 否 | 根据租户、Record、稳定触发键幂等创建 `requested` Handoff；只保存请求和业务证据 | 直接写 `accepted/connected/ended`、占用坐席或操作媒体 |
| 坐席 Presence 生命周期 | 已认证 Agent Presence/Wrap-up Repository | 否 | 只处理登录/离线、无活跃认领时的 `offline <-> available`，以及 ACW 已提交且无未决 Handoff 后的 `acw -> available` | `available -> claiming`、`claiming -> in_call`，或绕过 ACW 完成条件 |
| 坐席/Handoff 资源预占 | 已认证 API 的 Agent Console Claim Repository | 否 | 锁定 Record、Handoff 和坐席 Presence；只允许 `requested -> accepted`、坐席 `available -> claiming`、写认领租约并在同一事务创建 `HANDOFF_ACCEPTED` | 写 `connected/reconnecting/in_call/ACW`、停止等待音或操作媒体 |
| Outbound 创建和首次资源分配 | Outbound Task Executor、Dispatcher | 否 | Outbound 创建 `QUEUED` Attempt；Dispatcher 按全局锁顺序完成 `QUEUED -> STARTING`、Owner、容量和 Reservation 原子分配 | 根据本机观察伪报 `DIALING/IN_CALL` 或终态 |
| Outbound 确定性投影 | Outbound Attempt Reconciler | 否 | 使用 Attempt 自身 `reconcile_owner_id + reconcile_token + reconcile_expires_at`；只根据 Record、Effect、Reservation、媒体 Evidence 和终态单调推进 Attempt、Target、Task | 操作 Session/Provider、改写 Runtime Record/Handoff，或让终态回退 |
| 首次 Owner 分配或 cleanup Owner 接管 | Dispatcher/Recovery Repository | 否 | 锁定 Record 和 Worker 容量行；使用数据库时间；CAS 校验旧 Owner/租约/容量 | 创建 Session 或业务媒体 |
| Runtime 权威业务状态 | 当前 Runtime Owner | 是 | Owner ID、fencing token、租约、Command `processing_token` 和终态屏障全部匹配；只写 Record、Handoff `connected/reconnecting/ended`、符合 `CANCEL_HANDOFF` 门禁的 `canceled`，坐席 `in_call/ACW`、符合取消门禁的 `claiming -> available` 和实时媒体状态 | 直接收口 Target/Task、从 `in_call` 绕过 ACW 回到 available，或覆盖 API 认领和 Reconciler 投影事实 |
| Effect 首次登记 | 当前 Runtime Owner 或明确的 cleanup Owner | 是 | Owner/fencing/租约与来源 Command `processing_token` 匹配；在同一事务中插入稳定幂等 Effect | Command 结束后补登记新的非终止创建 Effect |
| Effect 认领、对账和结果写入 | 当前 Runtime Owner 或明确的 cleanup Owner | 是 | 使用 Effect 自身处理 Owner、fencing token、processing token 和租约；依赖已满足；不要求来源 Command 仍为 `PROCESSING` | 使用旧 Effect token、绕过 Record Owner/fencing，或盲目重放创建 |
| SIP Reservation 运行期转换 | 当前 Runtime Owner 或明确的 cleanup Owner，通过 Effect Repository 写入 | 是 | 同时匹配 Effect 自身处理令牌、当前 Record Owner/fencing/租约和 `reservation_token`；只允许 `RESERVED -> ACTIVE/RECONCILE_REQUIRED/RELEASED` 或 `ACTIVE/RECONCILE_REQUIRED -> RELEASED` | Attempt Reconciler 写 Reservation、无资源证据释放，或从 `RELEASED` 回退 |
| Inbox/离线 Job 自身任务状态 | 对应 Job Worker | 不使用通话 Owner | 使用各自 `processing_token` 和处理租约；产生 Runtime 动作时只能创建命令 | 通过 Job Worker 操作 Session |

Command Repository 的非 Owner 例外仅用于持久化业务意图和单调终止决议，不授予任何 Runtime 控制权。Owner 已失联或租约已经过期时，客户挂机仍必须能够建立终态屏障；这类事务不能因为没有有效 Owner 而拒绝。

写权限不能再统一表述为“所有业务表只允许 Runtime Owner 写”。除上表逐项明确的持久意图、Handoff 请求、Presence、资源预占、分配、投影和恢复例外外，Runtime 权威状态写入必须包含 `runtime_owner_id`、`runtime_fencing_token`、租约未过期、Command `processing_token` 和终态屏障条件。各非 Owner 写入者只能执行表中列出的单调转换，并使用对应触发键、认领条件或独立处理租约；条件更新影响行数为 0 时必须停止，不得降级为无条件更新。

涉及 Handoff `connected/reconnecting/ended/canceled`、坐席 `claiming/in_call/ACW/available` 等 Runtime 权威子表状态的事务，必须按 `Record -> Handoff -> Presence -> Command` 锁定参与行，再使用数据库时间验证 Owner、fencing token、租约、Command 处理令牌、终态屏障和命令特定门禁，在同一短事务中更新子表。Command 在此事务中虽然后锁，Runtime 在发起事务前已经通过独立命令认领 CAS 获得处理资格；最终提交仍必须再次匹配 token。所有权接管、API 资源预占和 Command Repository 建立终态屏障同样遵守全局顺序，使旧 Owner 的最后一次合法提交、新 Owner 接管、坐席认领和非 Owner 终止请求按数据库顺序串行化。

仅在外部动作前检查租约不能消除检查与执行之间的竞态。为此新增 `ai_call_runtime_effect` 持久化外部操作日志：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `tenant_id` | varchar(20) | 租户隔离 |
| `call_id` | varchar(64) | 目标通话 |
| `command_id` | bigint | 来源命令 |
| `effect_type` | varchar(64) | 创建型或销毁型外部动作 |
| `idempotency_key` | varchar(160) | 租户内唯一的外部动作幂等键 |
| `fencing_token` | bigint | 发起动作时的所有权版本 |
| `status` | varchar(32) | `PENDING`、`APPLYING`、`APPLIED`、`RECONCILE_REQUIRED`、`FAILED` |
| `processing_token` | varchar(128) nullable | 当前 effect 执行尝试令牌 |
| `processing_expires_at` | datetime nullable | effect 执行租约 |
| `provider_namespace` | varchar(128) | Provider account、project 或 cluster 的稳定命名空间；单命名空间部署也写固定配置值 |
| `provider_idempotency_key` | varchar(160) | 传给 Provider 的稳定幂等键；Provider 不支持时仍用于内部对账 |
| `resource_key` | varchar(255) | 内部确定性对账键，必须包含 `call_id`、effect 类型和资源 generation |
| `resource_generation` | bigint | 通常取创建 effect 的 fencing token，用于区分旧 Owner 迟到资源 |
| `source_create_effect_id` | bigint nullable | 销毁 Effect 对应的创建 Effect ID；创建 Effect 自身为空，不创建物理外键 |
| `create_protection_deadline_at` | datetime nullable | 对应创建结果持续不确定时，销毁 Effect 进入创建静默态前必须跨过的保护截止时间 |
| `absence_observation_count` | integer | 当前保护周期内符合 Provider 查询合同的连续不存在次数 |
| `absence_confirmed_at` | datetime nullable | 创建进入静默态后达到连续不存在确认规则的数据库时间 |
| `terminal_confirmed_at` | datetime nullable | 创建已进入静默态后，销毁明确终态或不存在事实的最终确认时间；早于静默门禁的成功不得写入 |
| `provider_reference` | varchar(255) nullable | Provider、Room 或 Egress 标识 |
| `execution_phase` | smallint | 同一命令内的执行阶段；依赖未满足时不得认领 |
| `processing_owner_id` | varchar(128) nullable | 当前 Effect 执行或对账 Worker |
| `processing_fencing_token` | bigint nullable | 本次 Effect 尝试使用的当前 Record fencing token；区别于资源原始 generation |
| `reconcile_after` | datetime nullable | 下一次向 Provider 对账时间 |
| `reconcile_deadline_at` | datetime nullable | 迟到调用仍可能完成的最晚保护时间 |
| `attempt_count` | integer | Effect 执行和对账尝试次数 |
| `error_message` | varchar(1000) nullable | 最近错误 |
| `created_at` / `updated_at` | datetime | 审计时间 |

Effect 约束和索引：

- `(tenant_id, idempotency_key)` 唯一，保证同一业务动作只登记一次；
- `(tenant_id, provider_namespace, effect_type, resource_key)` 唯一，保证同一 Provider 命名空间、同一 generation 的同类资源动作不重复创建；
- `(tenant_id, provider_namespace, provider_idempotency_key)` 唯一；
- 销毁 Effect 的 `source_create_effect_id` 必须指向同租户、同通话、同 `provider_namespace/resource_generation/resource_key` 所代表资源的创建 Effect；Repository 在代码中校验，不创建物理外键；
- `(status, reconcile_after)` 对账扫描索引；
- `(status, processing_expires_at)` 崩溃恢复索引；
- `(tenant_id, call_id, status, created_at)` 通话资源审计索引。

需要顺序门禁的 Effect 使用 `ai_call_runtime_effect_dependency`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `tenant_id` | varchar(20) | 租户隔离 |
| `effect_id` | bigint | 当前 Effect，无物理外键 |
| `prerequisite_effect_id` | bigint | 前置 Effect，无物理外键 |
| `required_status` | varchar(32) | V1 固定为 `APPLIED` |
| `created_at` | datetime | 审计时间 |

`(tenant_id, effect_id, prerequisite_effect_id)` 唯一。Effect Worker 认领前必须查询全部依赖并确认达到要求；未满足时保持 `PENDING`，不能仅依赖进程内执行顺序。

V1 必须纳入 effect 的动作至少包括：

- 创建型：`CREATE_ROOM`、`CREATE_SIP_PARTICIPANT`、`ATTACH_AGENT_PARTICIPANT`、`START_EGRESS`；
- 销毁型：`HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT`、`STOP_EGRESS`、`DELETE_ROOM`。

`Agent Runner` 的 Python 对象、响应协程和音频队列属于 Owner 本地资源，不单独登记 Effect；但 Runner 调用 LiveKit `room.connect()`、创建 Agent Participant 和发布 AI 音轨属于 Provider 副作用，必须使用 `ATTACH_AGENT_PARTICIPANT`：

1. identity 固定为 `agent-{call_id}-g{runtime_fencing_token}`，Effect 的 `resource_generation` 使用相同 fencing token；
2. 调用 `room.connect()` 前先登记并认领 Effect，`resource_key` 包含 Room、Agent identity 和 generation；
3. 连接成功并发布 AI 音轨后，保存 Participant SID、Track SID、generation 和验证时间，Effect 才能进入 `APPLIED`；
4. 连接超时、Participant 已出现但音轨状态未知、或 Worker 在结果写库前失权时进入 `RECONCILE_REQUIRED`，不能重新创建另一个相同 generation；
5. 接管和终止流程对每个旧 generation 登记 `DISCONNECT_AGENT_PARTICIPANT`；只取消本地 Runner 不等于 Provider Participant 已消失；
6. `START_CALL` 成功必须同时满足 Room、必要的 SIP/Egress Effect 和 `ATTACH_AGENT_PARTICIPANT` 已有明确结果，不能仅凭本地 Runner 已启动。

每个创建型 effect 在调用 Provider 前必须同时确定：

1. Provider 支持幂等键时使用稳定 `provider_idempotency_key`；
2. Provider 不支持幂等键时使用可查询、确定性的 `resource_key`；
3. Provider 支持自定义命名或元数据时携带 `call_id` 和 `resource_generation`，让恢复器能直接识别旧 Owner 迟到创建的资源；
4. Provider 只能使用预分配稳定标识时，允许 Room 名或 Participant identity 不包含 generation，但必须能按该稳定标识和 `provider_namespace` 查询，并保留覆盖其最大调用超时与异步创建窗口的 `reconcile_deadline_at`；孤儿资源扫描器在保护窗口内重复查询，不能一次“查无资源”后就宣布清理完成；
5. END 为该资源登记销毁 Effect 时，必须写入 `source_create_effect_id`，并把对应创建 Effect 的 `reconcile_deadline_at` 复制为 `create_protection_deadline_at`；同一销毁 Effect 可以在窗口内幂等发出终止请求，但在对应创建进入静默态前，销毁成功、仅受理或资源不存在都不能进入最终 `APPLIED`。

销毁 Effect 必须执行统一的“创建静默门禁”。对应创建只有满足以下任一条件才算进入静默态：

1. 创建 Effect 已 `FAILED(no_resource)`，明确不会再产生资源；
2. 创建 Effect 已 `APPLIED`，原创建调用已经完成且资源 generation 已确定；
3. 创建 Effect 仍不确定，但数据库时间已经跨过 `create_protection_deadline_at`，覆盖 Provider 最大调用超时和异步创建窗口。

除第 1 类可直接完成 noop 销毁外，销毁 Effect 必须在创建进入静默态后再次查询或再次幂等终止，并把确认时间写入 `terminal_confirmed_at`；静默前返回的“销毁成功”也只能作为审计 Evidence，不能沿用为最终确认。查询不存在还必须满足 Provider 配置的连续观察规则并写 `absence_confirmed_at`。这样可以覆盖“销毁先成功、旧创建随后迟到落地”的顺序，而不只是覆盖一次查询不存在。

Command 与 Effect 的租约边界必须分离：

1. 当前 Runtime Owner 只在来源 Command 仍持有有效 `processing_token` 时首次登记 Effect；登记事务保存 `command_id`、稳定幂等键、资源 generation 和依赖，但 `command_id` 只作为来源审计与首次登记授权；
2. Effect 登记提交后，即使来源 Command 进入 `SUCCEEDED/RETRY_WAIT/SUPERSEDED`，或其处理令牌被 `END_CALL` 撤销，Effect 仍是独立持久任务，不能因 Command 生命周期结束而失去恢复路径；
3. Effect Worker 从 `PENDING`、到期 `RECONCILE_REQUIRED` 或执行租约已过期的 `APPLYING` 中，通过单条 CAS 写入新的 `processing_owner_id`、`processing_fencing_token`、`processing_token`、`processing_expires_at` 和 `attempt_count`；
4. Effect 认领必须验证当前 Worker 是 Record 有效 Runtime Owner 或明确 cleanup Owner、当前 fencing token/租约有效、终态规则允许该 Effect，且全部依赖满足；不再要求来源 Command 仍是 `PROCESSING`；
5. Effect 完成写入必须同时匹配 Effect 自身 `processing_owner_id + processing_fencing_token + processing_token` 以及 Record 当前 Owner/fencing/租约；旧 Owner 的迟到写入影响行数必须为 0；
6. Worker 失权、Effect 租约过期或调用结果不确定时，其他有效 Owner 可以通过 Effect 自身 CAS 接管；创建型动作先查 Provider，销毁型动作按幂等资源键继续收敛，均不得盲目重放；
7. Record 已无有效 Owner 但仍有未完成 Effect 时，Recovery Repository 必须先分配 cleanup Owner；普通 Jobs Worker 不能绕过 Owner 直接认领 Provider Effect；
8. Command 逻辑完成只表示业务命令已经得到决议，不表示其来源 Effect 已全部终态；`resource_cleanup_status`、Effect 状态和 Provider 对账继续独立推进。

外部副作用使用以下权威结果矩阵；实现不得为单个 Provider 另行发明不同的终态含义：

| Provider 结果 | Effect 状态 | Runtime/命令动作 | 容量与后续处理 |
| --- | --- | --- | --- |
| 创建资源已存在且可查询 | `APPLIED` | 持久化 Provider reference、generation 和最终状态后推进命令 | 按业务状态继续持有或释放容量 |
| 销毁请求返回成功或查询不存在，但对应创建尚未进入静默态 | `RECONCILE_REQUIRED` | 只追加审计 Evidence；静默后必须再次查询或幂等终止，禁止沿用本次结果提交终态 | 保留资源/线路隔离和后续销毁资格 |
| 创建已进入静默态，销毁在门禁后得到明确终态 | `APPLIED` | 写 `terminal_confirmed_at` 和 Provider 最终事实；迟到旧 token 仍不能提交 | 只有全部资源满足同一门禁后才允许 `clean` |
| 销毁查询显示资源不存在，创建 Effect 已明确 `FAILED(no_resource)`；或创建已进入其他静默态且达到 Provider 配置的连续不存在确认规则 | `APPLIED` | noop 场景或门禁后写 `absence_confirmed_at + terminal_confirmed_at` | 只有全部资源满足同一门禁后才允许 `clean` |
| 明确失败且确认未产生资源 | 可重试时 `PENDING`，不可重试时 `FAILED` | 记录稳定错误；达到上限后按命令结果矩阵决议 | 不创建重复资源；失败启动按 `START_CALL` 矩阵释放容量 |
| 调用超时、连接中断或返回结果不确定 | `RECONCILE_REQUIRED` | 在 `reconcile_deadline_at` 前只允许查询 Provider，不允许盲目重放创建 | 保留必要容量或转入 cleanup capacity，直到资源存在性得到决议 |
| 数据库提交前 Provider 已成功 | `RECONCILE_REQUIRED` | 使用确定性资源键、generation 和 Provider 查询回填，不接受旧执行尝试直接补写 | 对账成功后继续；终态通话则创建销毁 Effect |
| 旧 Owner 失权后调用迟到成功 | `RECONCILE_REQUIRED` | 新 Owner/孤儿扫描识别旧 generation 或稳定资源键；终态已建立时认领 `END_CALL` 按对应创建 Effect 预登记的销毁 Effect | 只允许安全收尾，不恢复旧 AI 上下文 |
| 销毁请求明确成功但资源仍可见 | `RECONCILE_REQUIRED` | 有界重试查询和幂等销毁；超过时限转人工告警 | `resource_cleanup_status` 不得写 `clean` |
| Provider 无法查询且不支持幂等 | `RECONCILE_REQUIRED` | 等待保护窗口并持续对账；规格和实现不得声称 exactly-once | 超时进入 `attention_required` 并停放执行槽，不能伪报清理完成 |

Effect 使用以下唯一状态机；Provider 适配器只能返回矩阵中的事实类别，不能自行新增状态语义：

| 当前状态 | 允许目标状态 | 写入者与条件 | 是否终态 |
| --- | --- | --- | --- |
| `PENDING` | `APPLYING` | 当前有效 Runtime/cleanup Owner 使用 Effect CAS 写入新的 processing owner、fencing、token、租约和尝试次数 | 否 |
| `APPLYING` | `APPLYING` | 原执行租约已过期时，新有效 Owner 使用旧状态、旧 token/过期时间 CAS 接管；旧 token 失效 | 否 |
| `APPLYING` | `PENDING` | Provider 明确可重试失败且确认本次未产生资源；清除处理租约并写 `reconcile_after` | 否 |
| `APPLYING` | `RECONCILE_REQUIRED` | 调用超时、返回不确定、资源仍可见、销毁结果发生在 `EFF-04` 创建静默门禁前，或 Provider 仅受理尚未终态 | 否 |
| `APPLYING` | `APPLIED` | 创建资源明确存在；或创建已进入静默态且销毁在门禁后重新确认明确终态/不存在。提交必须匹配当前 Effect/Record token | 是 |
| `APPLYING` | `FAILED` | 仅限调用前永久校验失败，或创建动作明确不可重试且确认从未产生资源 | 是 |
| `RECONCILE_REQUIRED` | `APPLYING` | `reconcile_after` 到期且当前有效 Owner 以新 Effect token 认领 | 否 |
| `RECONCILE_REQUIRED` | `APPLIED` | 创建查询得到明确资源事实；或创建已进入静默态且 Provider 在门禁后重新确认销毁终态/不存在 | 是 |
| `RECONCILE_REQUIRED` | `FAILED` | 仅创建 Effect 在聚合决议中确认资源不存在且不再允许创建；可能存在资源的销毁 Effect禁止进入 `FAILED` | 是 |
| `APPLIED` / `FAILED` | 无 | 终态不得回退；后续相反 Provider 事实只能追加 Evidence、恢复已有销毁 Effect或建立运维告警 | 是 |

`attempt_count` 达到普通自动重试上限不自动产生 `FAILED`。创建结果不确定时服从 `START-03/START-04`，销毁结果不确定时保持 `RECONCILE_REQUIRED` 并按 `OWN-05` 停放为 `attention_required`；任何重试耗尽策略都不得丢失资源清理资格。

接管 Worker 对终态通话执行双向对账：

- 数据库存在创建 Effect，但 Provider 资源不存在：只有原 Runtime Owner 仍有效、没有终态屏障且该 Effect 状态机明确允许时，才可按稳定幂等键继续创建；Owner 已失联或当前是 cleanup Owner 时，按 `START-04` 建立/保持终态屏障，禁止重试创建，并对账至确认无资源或执行已有销毁图；
- Provider 存在资源，但数据库 effect 未确认：回填 `provider_reference` 并继续正常状态机；
- Provider 存在旧 generation 资源，或终态通话的稳定资源键重新出现：若尚无终态则先建立唯一 `END_CALL`；终态已建立时认领由完整终止图预登记的对应销毁 Effect；
- 数据库已终态且 Provider 在保护窗口内迟到创建资源：孤儿扫描器必须发现并清理；
- 销毁 effect 已执行但资源仍存在：继续幂等重试并告警，不能把数据库终态当成 Provider 已清理的证据。

数据库租约提供单一业务权威，Provider 资源通过 generation 和 Effect 最终收敛。除非 Provider 原生支持 fencing，V1 不声称媒体侧瞬时 exactly-once；它保证旧 Owner 在本地硬截止前 fail closed，并由新 Owner 优先隔离旧 generation。

对无法接收 fencing token 的 LiveKit、SIP 或录音 Provider，V1 不声称严格 exactly-once，只保证可对账、可重试、generation 或稳定资源键可识别，以及终止动作单调幂等。外部资源最终收敛由 effect 对账器负责，而不是依赖旧 Owner 在返回后还能写数据库。

## 8. 持久命令模型

新增 `ai_call_runtime_command` 表，集成现有租户能力，不使用物理外键和数据库专有 JSON 类型。

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID，接口按字符串返回 |
| `tenant_id` | varchar(20) | 租户隔离 |
| `call_id` | varchar(64) | 目标通话 |
| `command_seq` | bigint | 同一通电话内严格递增 |
| `command_type` | varchar(64) | 业务命令类型 |
| `idempotency_key` | varchar(128) | 租户内唯一 |
| `request_fingerprint` | varchar(64) | 按命令类型计算的稳定业务意图摘要；创建型命令不得包含服务端生成 ID |
| `dispatch_priority` | smallint | 数值越小优先级越高；普通命令默认 100，`END_CALL` 固定为 0 |
| `allocation_deadline_at` | datetime nullable | `START_CALL` 等待 Runtime/线路资源的数据库截止时间；其他命令为空 |
| `payload_json` | text nullable | 非敏感命令参数 |
| `sensitive_payload_ciphertext` | text nullable | 仅在无法通过业务引用解析时保存的加密敏感启动参数 |
| `payload_key_version` | varchar(64) nullable | 加密密钥版本 |
| `expected_fencing_token` | bigint nullable | 投递时观察到的所有权版本 |
| `target_owner_id` | varchar(128) nullable | 投递目标 Worker |
| `status` | varchar(32) | 命令状态 |
| `dispatch_token` | varchar(128) nullable | 每次进入 `DISPATCHING` 生成的发布尝试令牌 |
| `dispatch_expires_at` | datetime nullable | 发布尝试租约到期时间 |
| `attempt_count` | integer | 执行次数 |
| `next_retry_at` | datetime nullable | 下次重试时间 |
| `published_at` | datetime nullable | 最近成功发布时间 |
| `stream_message_id` | varchar(128) nullable | 最近 Redis Stream 消息 ID |
| `processing_owner_id` | varchar(128) nullable | 当前处理 Worker |
| `processing_fencing_token` | bigint nullable | 当前处理使用的所有权版本 |
| `processing_token` | varchar(128) nullable | 每次进入 `PROCESSING` 生成的新令牌，用于隔离同一 Worker 的旧执行尝试 |
| `processing_expires_at` | datetime nullable | 命令处理租约到期时间 |
| `claimed_at` | datetime nullable | 最近开始处理时间 |
| `cancel_requested_at` | datetime nullable | 终态屏障要求当前处理停止的时间 |
| `preempted_by_command_id` | bigint nullable | 抢占当前命令的 `END_CALL` 命令 ID |
| `finished_at` | datetime nullable | 最终完成时间 |
| `result_json` | text nullable | 幂等结果 |
| `error_message` | varchar(1000) nullable | 最近失败原因 |
| `created_at` / `updated_at` | datetime | 审计时间 |

唯一约束和索引：

- `(tenant_id, idempotency_key)` 唯一；
- `(tenant_id, call_id, command_seq)` 唯一；
- `(status, next_retry_at)` 索引；
- `(command_type, status, allocation_deadline_at)` 首次分配超时扫描索引；
- `(status, dispatch_expires_at)` 索引；
- `(status, published_at)` 索引；
- `(status, processing_expires_at)` 索引；
- `(target_owner_id, status, dispatch_priority, created_at)` 索引；
- `(tenant_id, call_id, created_at)` 索引。

命令状态：

```text
PENDING
  -> DISPATCHING

到期 RETRY_WAIT
  -> DISPATCHING

DISPATCHING
  -> PUBLISHED

DISPATCHING
  -> PENDING

DISPATCHING
  -> PROCESSING        # 仅高优先级 END_CALL 数据库直领，原子撤销 dispatch_token

DISPATCHING
  -> SUPERSEDED

PUBLISHED
  -> PROCESSING

PENDING
  -> PROCESSING        # 高优先级终止扫描或 Redis 故障时数据库直领

PROCESSING
  -> RETRY_WAIT
  -> DEAD
  -> SUCCEEDED

RETRY_WAIT
  -> PROCESSING        # 高优先级终止扫描或 Redis 故障时数据库直领

PENDING / DISPATCHING / PUBLISHED / RETRY_WAIT
  -> SUPERSEDED

PENDING / DISPATCHING / PUBLISHED / RETRY_WAIT
  -> CANCELED

PENDING START_CALL
  -> DEAD              # 仅 allocation_deadline_at 到期且确认从未分配或产生资源

PROCESSING
  -> SUPERSEDED
```

`SUCCEEDED`、`DEAD`、`SUPERSEDED` 和 `CANCELED` 为终态。`DISPATCHING` 表示 Dispatcher 已通过数据库 CAS 保留一次发布权，但 Redis 发布结果尚未确认；它不是可执行状态。`SUPERSEDED` 表示被更高优先级的终态屏障吸收；`CANCELED` 表示业务在执行前明确撤销。

命令恢复使用以下权威矩阵：

| 当前命令 | Owner 状态 | 恢复决议 | 是否允许新 Owner 执行原命令 |
| --- | --- | --- | --- |
| `START_CALL` 尚未产生 Effect | 无 Owner 或 Owner 失联 | 由 Dispatcher/Recovery Repository 按第 7.3 节重新分配；若已存在终态屏障则 `SUPERSEDED` | 仅无终态屏障时允许 |
| `START_CALL` 已有任一创建 Effect，结果明确 | Owner 失联 | 追加 `runtime_recovery` Evidence，建立终态屏障和唯一 `END_CALL`；已有资源只作为销毁输入 | 否；不得把新 Owner 没有 Session 的通话恢复为成功 |
| `START_CALL` 有结果不确定 Effect | Owner 失联 | 立即建立终态屏障和唯一 `END_CALL`，再由 cleanup Owner 查询 Provider；存在、迟到存在或无法排除存在时持续销毁，无资源时完成失败收口 | 否；禁止盲目重放创建 |
| `HANDOFF_ACCEPTED`、`AGENT_MEDIA_READY`、`AGENT_MEDIA_INVALIDATED`、`CANCEL_HANDOFF` 等普通命令 | Owner 失联 | 原命令 `SUPERSEDED`；追加 `runtime_recovery` Evidence；创建或读取唯一 `END_CALL` | 否，新 Owner 没有旧 Session |
| `END_CALL` | Owner 失联 | 保持同一命令，分配 cleanup Owner 并继续终止对账 | 是，只允许安全收尾 |
| 任意普通命令 | Record 已有终态屏障 | `SUPERSEDED`，推进明确决议游标 | 否 |

普通命令的“重新路由”仅适用于同一有效 Owner 的 Redis 投递恢复，不适用于 Runtime Owner 已失联后的 Session 恢复。V1 不恢复旧 AI 上下文，因此 Owner 失联后不得把依赖旧 Session 的普通命令改投新 Worker；一旦登记过任一创建 Effect，`START_CALL` 也属于不可恢复原 Session 的命令，必须按 `START-04` 进入终止收敛。

重复请求命中相同 `(tenant_id, idempotency_key)` 时必须比较 `request_fingerprint`：

- 指纹一致：返回原命令、真实状态和原结果；
- 指纹不一致：返回 `409 IDEMPOTENCY_CONFLICT`，禁止静默复用原命令；
- 相同业务意图的 payload 必须先做字段排序、默认值消歧和稳定序列化再计算指纹。

指纹按命令类型使用以下权威规则：

- `START_CALL`：`tenant_id + START_CALL + 入口类型 + 规范化业务请求`，明确排除服务端生成的 `call_id`、Record/Command ID、创建时间和随机资源标识；
- 正式外呼 `START_CALL` 的幂等键由稳定 Attempt 身份生成，指纹使用 Attempt、线路快照、提示词和音色等权威业务引用，不包含新生成的 `call_id`；
- Web、试听和直接 SIP 创建入口必须要求 `Idempotency-Key`，在分配 `call_id` 前先查询原命令；
- `END_CALL` 按下述终止特例计算；
- 其他已有通话命令使用 `tenant_id + call_id + command_type + 规范化 payload`。

`END_CALL` 是指纹规则的明确特例：其 `request_fingerprint` 只由 `tenant_id + call_id + END_CALL` 计算，不包含 `source`、`end_reason`、`requested_at` 或 Provider event ID。不同终止来源不是不同命令，而是同一终止意图的多条审计证据，按 10.4 节追加保存。

`START_CALL` 参数规则：

- 正式外呼优先保存 Task、Target、Attempt、线路和提示词配置的业务引用，Worker 从同一租户的权威表读取；
- 直接 SIP 请求中的原始号码不能写入 `payload_json`、Redis、日志或命令结果；必须使用应用 KMS/密钥服务加密后写入 `sensitive_payload_ciphertext`；
- 没有可用加密能力时，正式环境禁止启用接受原始号码的直接 SIP 创建入口；
- Worker 完成 Provider 建链并持久化脱敏审计字段后清除短期敏感密文；失败终态按安全审计保留策略清理；
- Room 名、号码 hash、脱敏号码、业务引用和配置快照可以持久化，但不能由 hash 反推原始号码。

### 8.1 命令创建和完成的事务边界

- `ai_call_record.next_command_seq` 通过行锁或数据库条件更新分配，命令插入与序号推进必须在同一事务；
- Handoff 认领、坐席占用和 `HANDOFF_ACCEPTED` 命令必须在同一事务提交；
- `media-ready` 只验证并持久化媒体证据、创建 `AGENT_MEDIA_READY`，不能提前将 Handoff 写成 `connected`；
- webhook 去重记录和由该 webhook 产生的命令必须在同一事务提交；
- 未分配 `START_CALL` 的排队超时决议必须在同一事务中锁定 Record 和 Command，写入 `DEAD/failed/ALLOCATION_TIMEOUT` 并将 `last_applied_command_seq` 推进到 1；任一 Owner、Reservation、Effect 或非 `none` 容量存在时条件更新必须失败并转资源对账；
- Worker 完成业务状态写入、推进 `last_applied_command_seq` 和更新命令结果必须在同一事务提交；
- 事务提交后再发布 Redis 事件；发布失败由数据库扫描补偿。

### 8.2 Webhook 持久 Inbox

当前请求返回后再用 `asyncio.create_task` 处理 webhook，API 进程崩溃时会丢失已经向 LiveKit 确认成功的终止事件。新模式新增 `ai_call_webhook_inbox`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `provider` | varchar(32) | `livekit` 等事件来源 |
| `provider_namespace` | varchar(128) | Provider account、project 或 cluster 的稳定命名空间 |
| `dedupe_key` | varchar(160) | Provider 内唯一的 event ID 或稳定指纹 |
| `tenant_id` | varchar(20) | 从 Record 反查得到的租户 |
| `call_id` | varchar(64) nullable | 关联通话 |
| `event_type` | varchar(64) | Provider 事件类型 |
| `payload_json` | text nullable | 已清理 Token、音频和敏感字段的必要证据 |
| `status` | varchar(32) | `RECEIVED`、`PROCESSING`、`RETRY_WAIT`、`SUCCEEDED`、`DEAD` |
| `attempt_count` | integer | 已执行次数 |
| `next_retry_at` | datetime nullable | 下次允许重试时间 |
| `processing_owner_id` | varchar(128) nullable | 当前 Inbox Worker |
| `processing_token` | varchar(128) nullable | 本次处理尝试令牌 |
| `processing_expires_at` | datetime nullable | 处理租约到期时间 |
| `error_message` | varchar(1000) nullable | 处理错误 |
| `received_at` / `claimed_at` / `processed_at` | datetime nullable | 审计时间 |

约束和索引：

- `(provider, provider_namespace, dedupe_key)` 全局唯一；
- `(status, next_retry_at, received_at)` 重试扫描索引；
- `(status, processing_expires_at)` 崩溃恢复索引；
- `(tenant_id, call_id, received_at)` 业务审计索引。

签名有效但暂时无法关联 Record 的 Provider 事件不能伪造租户进入主 Inbox。新增 Provider 级 `ai_call_webhook_quarantine`，它不是租户业务查询表：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `provider` | varchar(32) | 事件来源 |
| `provider_namespace` | varchar(128) | Provider account、project 或 cluster 的稳定命名空间 |
| `dedupe_key` | varchar(160) | Provider 内稳定去重键 |
| `room_name` | varchar(255) nullable | 用于后续关联的 Room |
| `participant_identity` | varchar(255) nullable | 用于后续关联的参与者 |
| `event_type` | varchar(64) | Provider 事件类型 |
| `payload_json` | text nullable | 已清理敏感字段的最小必要证据 |
| `status` | varchar(32) | `UNMATCHED`、`PROCESSING`、`RETRY_WAIT`、`RESOLVED`、`IGNORED`、`DEAD` |
| `attempt_count` | integer | 关联尝试次数 |
| `next_retry_at` | datetime nullable | 下次关联时间 |
| `processing_owner_id` | varchar(128) nullable | 当前 Quarantine Worker 实例 |
| `processing_generation` | bigint | 每次成功认领递增，用于隔离同一行的旧执行尝试 |
| `processing_token` | varchar(128) nullable | 本次关联尝试随机令牌 |
| `processing_expires_at` | datetime nullable | 处理租约到期时间 |
| `claimed_at` | datetime nullable | 最近认领数据库时间 |
| `resolved_tenant_id` | varchar(20) nullable | 成功关联后的租户 |
| `resolved_call_id` | varchar(64) nullable | 成功关联后的通话 |
| `error_message` | varchar(1000) nullable | 最近错误 |
| `received_at` / `resolved_at` | datetime nullable | 审计时间 |

`(provider, provider_namespace, dedupe_key)` 全局唯一，并为 `(status, next_retry_at, received_at)`、`(status, processing_expires_at)` 建扫描和崩溃恢复索引。Quarantine 仅允许内部 Provider 接收和对账进程访问，不进入普通租户 API；解析成功后才把事件写入带非空 `tenant_id` 的主 Inbox。已 `RESOLVED/IGNORED/DEAD` 的脱敏 payload 按短期运维保留策略清理。

处理规则：

1. API 首先同步完成签名校验；签名无效返回 `401/403`，不持久化为可信事件；
2. 签名有效且能反查 Record 时，在同一事务中写入带非空 `tenant_id` 的 Inbox；能直接映射为 `END_CALL` 的事件同时追加终止 Evidence 并创建或读取唯一命令，其他命令同样与 Inbox 决议保持原子边界；
3. 签名有效、能够确认属于共享 Provider 中无关命名空间的事件，返回 HTTP 2xx 并记录计数指标，不写入业务 Inbox，避免 Provider 重投风暴；
4. 签名有效、标识符合受管 Room/Participant 命名或 metadata，但 Record 暂时不可见时，幂等写入 Quarantine；事务成功后返回 HTTP 2xx，由隔离对账 Worker 在短窗口内重试关联；
5. Quarantine Worker 从 `UNMATCHED`、到期 `RETRY_WAIT` 或租约过期的 `PROCESSING` 中使用只锁 Quarantine 行的短事务 CAS 认领，递增 `processing_generation` 并写入新的 owner、token、租约和尝试次数后立即提交；关联结果事务不得继续持有该认领行锁后回锁 Record；
6. Quarantine 关联成功时，按 `DB-02` 在新事务中先锁定解析出的 Record 和需要写入的主 Inbox，最后锁定 Quarantine 并重新校验 generation、token 和未过期租约，再原子写入 Inbox、保存 `resolved_tenant_id/call_id` 并置为 `RESOLVED`；暂时不可关联时进入有限退避 `RETRY_WAIT`，超过关联时限仍无法确认归属时置为 `DEAD` 并告警，明确无关时置为 `IGNORED`；旧 Worker 迟到提交影响行数为 0；
7. 数据库不可用或 Quarantine/Inbox 持久化失败时返回可重试 5xx，让 Provider 重投；不能在没有持久化事实时返回成功；
8. 已在接收事务中完成全部持久决议的事件直接置为 `SUCCEEDED`；需要异步处理的事件保持 `RECEIVED`；
9. Inbox Worker 通过只锁 Inbox 行的短事务，从 `RECEIVED`、到期 `RETRY_WAIT` 或处理租约已过期的 `PROCESSING` 原子进入新的 `PROCESSING`，同时写入新的 `processing_token`、Owner、租约和 `attempt_count` 后立即提交；
10. Worker 结果提交在新事务中按 `DB-02` 先锁 Record/Handoff/Command/Evidence 等业务行，最后锁 Inbox 并校验 `processing_token` 和未过期租约；失败进入有限退避的 `RETRY_WAIT`，达到上限后进入 `DEAD` 并告警；
11. `PROCESSING` Worker 崩溃后，其他 Worker 只能在 `processing_expires_at` 到期后重领；旧 Worker 的迟到提交因 token 不匹配失败；
12. 非命令事件由持久 Inbox Worker 分批处理，不能依赖请求进程内任务；
13. 重复 webhook 命中主 Inbox 或 Quarantine 的 `provider + provider_namespace + dedupe_key` 后返回原处理状态，不重复创建命令或 Evidence。

## 9. Redis 路由

每个 Worker 消费普通与终止优先级两个独立 Stream：

```text
ai-call:runtime:{owner_instance_id}:priority
ai-call:runtime:{owner_instance_id}
```

每个 Stream 使用 Consumer Group `runtime`。`END_CALL` 只投递到 priority Stream，Worker 每轮先消费 priority，再消费普通 Stream。Worker 通过 `XREADGROUP` 获取消息；当前投递已经在数据库中持久决议为终态、`RETRY_WAIT` 或已改投其他 Owner 后才 `XACK`，不能在业务状态提交前确认消息。

消息只包含：

- `command_id`
- `call_id`
- `command_seq`
- `expected_fencing_token`
- `dispatch_token`

业务 payload 由 Worker 从数据库读取，避免 Redis 成为事实来源。

Dispatcher 规则：

1. 先按 9.1 和 9.2 节恢复发布或处理租约已经过期的命令，再扫描到期的 `PENDING` 和 `RETRY_WAIT`；
2. `START_CALL` 没有 Owner 时按 7.1 节执行首次分配；普通命令只有在原 Owner 租约仍有效时才能继续投递给该 Owner；
3. Owner 已失联时按第 8 节命令恢复矩阵处理：依赖旧 Session 的普通命令置为 `SUPERSEDED` 并建立 `END_CALL`，不得改投执行；已有 `END_CALL` 分配 cleanup Owner；`START_CALL` 无 Effect 时才允许重新分配，有任一创建 Effect 时先建立终态屏障和 `END_CALL`，再在终止上下文中对账；
4. Dispatcher 使用单条数据库 CAS，将 `PENDING` 或到期 `RETRY_WAIT` 更新为 `DISPATCHING`，同时写入 `target_owner_id`、`expected_fencing_token`、新的 `dispatch_token` 和 `dispatch_expires_at`；
5. CAS 成功后，`END_CALL` `XADD` 到目标 priority Stream，其他命令写入普通 Stream；消息携带本次 `dispatch_token`；
6. Redis 返回 Stream message ID 后，Dispatcher 仅允许通过 `status=DISPATCHING + 相同 dispatch_token + 相同 target_owner_id + 相同 expected_fencing_token` 的 CAS 写入 `PUBLISHED`、`published_at` 和 `stream_message_id`；
7. Redis 明确失败时，Dispatcher 以相同 token CAS 将命令恢复为 `PENDING`；到期的 `RETRY_WAIT` 已经满足重试时间，因此不需要恢复旧状态。结果不确定时保持 `DISPATCHING`，等待发布租约到期后恢复，禁止立即重复 `XADD`。

Redis 写成功但数据库 `PUBLISHED` 确认失败时允许后续重复发布，但旧 Dispatcher 不得无条件覆盖数据库状态。Worker 可能在数据库确认前先收到 Stream 消息；普通命令在一个短发布确认窗口内轮询数据库，确认相同 token 的 `PUBLISHED` 后再领取，超时则不 ACK 并交给 `XAUTOCLAIM` 或数据库恢复，不能从 `DISPATCHING` 直接执行。普通数据库降级扫描不得领取发布租约仍有效的 `DISPATCHING`，因此不存在 `PROCESSING -> PUBLISHED` 状态回退。

`END_CALL` 是唯一例外：始终运行的高优先级数据库终止扫描允许通过 `status=DISPATCHING + dispatch_token + target_owner_id + fencing` 的 CAS 直接撤销 `dispatch_token` 并进入 `PROCESSING`。Dispatcher 随后的 `PUBLISHED` 确认必然因 token 或状态不匹配失败；已经写入 Redis 的迟到消息只检查数据库终态并 ACK。这样既保持终止发现时限，也不允许状态回退。

### 9.1 `PUBLISHED` 和 Redis Pending 恢复

- `DISPATCHING` 超过 `dispatch_expires_at` 时，恢复器先查询命令终态、当前 Owner 和 Redis 可见证据；仍未决时通过旧状态与旧 `dispatch_token` CAS 恢复为 `PENDING`，再生成新 token 发布；
- 旧 Dispatcher 的迟到确认必须因 `dispatch_token` 或状态不匹配失败，绝不能把 `PROCESSING`、`SUPERSEDED` 或其他终态写回 `PUBLISHED`；
- Worker 收到消息后，以数据库条件更新将 `PUBLISHED` 变为 `PROCESSING`，同时生成新的 `processing_token`，写入处理 Owner、fencing token 和处理租约；
- 同一 Owner 内发生短暂消费者重启时，使用 `XAUTOCLAIM` 重领超过可见性窗口的 Pending 消息；
- `PUBLISHED` 超过发布可见性窗口仍未进入 `PROCESSING` 时，Dispatcher 重新读取当前 Owner：Owner 未变化则允许重复发布；Owner 已变化时按命令恢复矩阵决议，不能统一改投原命令；
- 数据库命令进入终态、目标 Owner 已变化或已经由恢复矩阵吸收后，旧 Stream 中的迟到消息只做数据库状态检查并 `XACK`，不得执行副作用；
- 活动 Stream 每小时按 `MINID` 近似裁剪，默认保留 24 小时；裁剪前 Dispatcher 必须先重新发布超过可见性窗口但数据库仍未决的命令；
- 已 ACK 且数据库已经终态的单条消息允许异步 `XDEL`，但不把逐条删除作为正确性前提；
- 旧 Worker 的两个 Stream 只有在数据库没有未决命令、Consumer Group pending 为 0 且超过 24 小时保留期后才能删除；
- Stream 被误裁剪或丢失时，由数据库未决命令扫描重新发布，Redis 保留策略不能成为业务恢复的唯一依据。

永久离线 Worker 的旧 Stream 由 `ROUTE-03` 的跨 Worker Stream Janitor 收敛：

1. Dispatcher/Janitor 只扫描数据库租约已过期且状态已转为 `OFFLINE` 的 Worker，通过 Worker 行上的 `stream_cleanup_owner_id + stream_cleanup_token + stream_cleanup_expires_at` 单条 CAS 获得一次清理权；
2. Janitor 使用独立 consumer 名对该 Worker 的普通和 priority Consumer Group Pending 执行 `XAUTOCLAIM`，但每条消息都先读取 Command、当前 Owner、fencing 和处理租约；
3. 数据库已终态、已改投或已由恢复矩阵吸收的消息直接 `XACK`；数据库仍未决且目标仍是离线 Owner 时，先由 Recovery Repository 完成命令决议或 cleanup Owner 分配，再向新目标 Stream 发布，数据库确认新发布权后才 `XACK` 旧消息；
4. Pending 以外的旧 Stream 消息不授予执行权；数据库扫描负责恢复未决命令，Janitor 可以在确认无未决数据库命令后按保留期裁剪；
5. 两个 Stream 均满足数据库无未决命令、Pending 为 0 和保留期已过时才删除；清理失败写 `stream_cleanup_after` 有界重试，旧 token 的迟到 ACK/删除请求必须因租约或 token 不匹配放弃；
6. 多 Dispatcher 并发时同一旧 Worker 只能有一个有效 Janitor 租约，任何 Redis 管理操作都不能修改 Command 权威状态。

### 9.2 `PROCESSING` 恢复

- Worker 执行长动作期间使用 `processing_token` 条件续租 `processing_expires_at`；
- 处理租约到期后，恢复器通过条件更新将命令转为 `RETRY_WAIT` 并清除旧处理令牌；旧执行尝试后续写入因 `processing_token` 不匹配而失败；
- 处理 Owner 的 Worker 注册租约已失效或通话 Owner 已变化时，按第 8 节命令恢复矩阵决议：普通命令 `SUPERSEDED` 并建立 `END_CALL`，终止命令分配 cleanup Owner；`START_CALL` 无 Effect 时才可重新分配，有任一 Effect 时先建立 `END_CALL` 再对账；不得统一描述为“重新路由原命令”；
- 处理租约到期但通话已经进入终态时，恢复器依据数据库事实将命令决议为 `SUCCEEDED` 或 `SUPERSEDED`；
- 外部动作状态不确定时，先读取 `ai_call_runtime_effect` 和 Provider 实际状态，不允许直接重复创建不可逆资源；
- `END_CALL` 即使达到普通重试上限也不允许静默停止，必须持续进入高优先级终止对账，直到所有资源得到终态或明确人工告警。

### 9.3 Redis 故障时的数据库降级

高优先级数据库终止扫描在 Redis 正常时也始终运行，不等待“判定 Redis 故障”后才启用：

- Runtime Worker 每 500 毫秒只扫描目标为自己且 Owner/fencing/租约仍有效、状态为 `PENDING`/可重领 `PUBLISHED`/到期 `RETRY_WAIT` 的 `END_CALL`；
- Dispatcher/Recovery Repository 每 500 毫秒扫描尚未分配、Owner 已失联或目标 Owner 不再有效的 `END_CALL`，先按第 13.4 节分配 cleanup Owner，再允许目标 Runtime 领取；Runtime 不得在扫描中自行写 Owner；
- `END_CALL` 已处于 `DISPATCHING` 时，高优先级扫描可以使用第 9 节定义的 token CAS 撤销发布尝试并直领；普通命令必须等待发布租约到期；
- 普通命令在 Redis 不可用时，每 1 秒分批扫描一次目标为自己、状态为 `PENDING`、可重领 `PUBLISHED` 或到期 `RETRY_WAIT` 的数据库命令；
- `END_CALL`、客户离开和租约失效收尾使用高优先级查询，不等待 Redis 恢复；
- Dispatcher 仍可在数据库中完成 `START_CALL` 首次分配，目标 Worker 通过降级扫描发现命令；
- Redis 恢复后允许重复发布，由数据库命令状态和幂等键消除重复；
- 降级扫描必须按批次读取，不一次加载全量命令。

数据库直领不是绕过状态机。Worker 只能通过单条条件更新执行以下转换：

```text
PENDING
  -> PROCESSING

到期 RETRY_WAIT
  -> PROCESSING

可重领 PUBLISHED
  -> PROCESSING

DISPATCHING END_CALL
  -> PROCESSING        # 原子清除 dispatch_token，使 Dispatcher 迟到确认失败
```

条件更新必须同时满足：

- 普通命令的 `target_owner_id` 等于当前 Worker；
- `END_CALL.target_owner_id` 必须非空并等于当前 Worker；目标为空、Record 无有效 Owner、处于 `attention` 或 Owner 已变化时，Runtime 必须拒绝领取并通知 Dispatcher/Recovery 扫描，只有 Recovery Repository 可以先占用清理槽、写入 cleanup Owner 和新的 `expected_fencing_token`；
- Record 当前 `runtime_owner_id`、`runtime_fencing_token` 和租约均有效；
- 命令 `expected_fencing_token` 与 Record 一致；
- `PROCESSING` 处理租约未被其他执行尝试持有；
- 普通命令满足 `command_seq = last_applied_command_seq + 1`；
- 普通命令未命中 `terminal_requested_at`，`END_CALL` 则允许执行终态抢占；
- 当前命令状态和重试时间仍符合扫描条件。

更新成功时一次性写入 `PROCESSING`、`processing_owner_id`、`processing_fencing_token`、新的 `processing_token`、`claimed_at` 和 `processing_expires_at`。影响行数为 0 表示已经被其他路径领取，Worker 不得执行任何副作用。Redis 恢复后的迟到消息只读取数据库状态并 ACK。

## 10. 业务命令

### 10.1 `START_CALL`

表示新通话已经持久化并等待 Runtime Worker 创建实时运行资源。

Worker 动作：

1. 校验 Owner、fencing token、租约和 `runtime_control_mode`；
2. 从同租户权威业务记录解析号码引用、音色、提示词和线路配置，并在外部副作用前持久化本次有效配置快照；
3. 根据持久化的通话类型和配置快照创建 Session；
4. 在首次 Provider 创建调用前，根据 Room、SIP Participant、Agent Participant、Egress 的依赖 DAG 计算最坏关键路径：串行节点累加调用超时，并行分支取最大值，最后加各资源最大迟到创建保护窗口和固定安全裕量；使用数据库当前时间得到 `startup_reconcile_deadline_at`，同时持久化 `startup_reconcile_policy_version/startup_reconcile_budget_json`，再通过 `CREATE_ROOM`、`CREATE_SIP_PARTICIPANT`、`ATTACH_AGENT_PARTICIPANT`、`START_EGRESS` Effect 创建或连接资源；禁止用“单个资源最大超时”替代整个关键路径；
5. 正式外呼由 `StartCallHandler -> OutboundDialer.start() -> SipOutboundDialer.start()` 发起，不在该命令内等待通话终态；
6. 持久化 Room、Participant、Provider 和运行时启动状态；
7. 成功后允许 Web 客户端获取 Token；启动明确失败时写入错误，启动结果不确定时进入 effect 对账；正式 Attempt 由 `OutboundAttemptReconciler` 异步收口。

`START_CALL` 使用以下权威结果矩阵。`StartCallHandler`、Effect Reconciler 和 `OutboundAttemptReconciler` 必须消费同一组持久结果，禁止分别推断启动成功：

| 启动结果 | Command | Record | Effect | 容量 | Attempt/后续动作 |
| --- | --- | --- | --- | --- | --- |
| Provider 明确受理，所有必需创建资源可查询 | `SUCCEEDED` | Web/试听在 Room 和 Agent 就绪后进入 `ready`；SIP 保持 `preparing`，等待媒体事实推进 | 包括 `ATTACH_AGENT_PARTICIPANT` 在内的必需创建 Effect 均为 `APPLIED` | 保留普通通话容量；SIP Reservation 进入 `ACTIVE` | Reconciler 将 Attempt 投影为 `DIALING`，并继续等待振铃、接通或终态 |
| Provider 返回结果不确定，无法确认资源是否创建，且未到 `startup_reconcile_deadline_at` | `RETRY_WAIT`，错误标记为 `START_UNCERTAIN` | 保持 `preparing`，不得签发就绪 Token 或伪报已拨号 | 对应创建 Effect 为 `RECONCILE_REQUIRED` | 保留普通容量；SIP Reservation 进入 `RECONCILE_REQUIRED` 并继续计数 | Reconciler 标记启动对账中；重试只能先查 Provider，禁止创建第二个资源 |
| 等待 Runtime/线路超过 `allocation_deadline_at`，且确认从未分配或产生资源 | `DEAD` | `failed` | 不存在 Effect | 容量保持 `none`，不存在 Reservation | Attempt 从 `QUEUED` 明确失败；记录 `ALLOCATION_TIMEOUT`，不创建 `END_CALL` |
| 调用前或调用结果明确失败，且确认没有任何 Provider 资源 | `DEAD` | `failed` | 未登记的 Effect 不再创建；已经登记但确认未执行的 Effect 置为 `FAILED` | 释放普通容量和 SIP Reservation | Attempt 明确失败；不需要为了不存在的资源创建 `END_CALL` |
| Room、SIP Participant、Egress、Agent Participant 或本地 Runner 部分创建后失败 | 由终态屏障置为 `SUPERSEDED` | 进入 `ending`，最终由终止结果写 `failed` | 已创建资源保留事实；对应销毁 Effect 由 `END_CALL` 创建 | 原子释放普通容量并占用 cleanup capacity；线路槽保留到 SIP 终态确认 | 追加启动失败 Evidence，创建或读取唯一 `END_CALL`；Attempt 等清理决议后收口 |
| Owner 已分配但 `START_CALL` 执行前收到 `END_CALL` | `SUPERSEDED` | `ending` | 禁止认领任何创建 Effect | 释放普通容量和尚未调用 Provider 的 `RESERVED` 线路槽；仅在存在资源时占用清理容量 | `END_CALL` 对账确认无资源后完成 |
| 执行中 Owner 失联，且已登记任一创建 Effect | 原 `START_CALL` 固定为 `SUPERSEDED` | 建立终态屏障并进入 `ending` | 明确资源和不确定 Effect 都进入 END 销毁图；后续确认无资源也不回写原 START 为 `DEAD` | 转入 cleanup capacity；线路槽在确认终态前保持 `RECONCILE_REQUIRED` | 追加 `runtime_recovery` Evidence，创建或读取 `END_CALL`；最终 Record 可按资源事实写 `failed`，但不得恢复 AI Session |

结果矩阵中的“确认没有资源”必须同时覆盖 Room、SIP Participant、Egress 和 Agent Participant；只检查其中一种不能决议为无资源。`START_CALL` 处于 `START_UNCERTAIN` 时，后续普通命令不得越过执行，只有 `END_CALL` 可以通过终态屏障抢占。

`startup_reconcile_deadline_at` 是整个启动流程的聚合终局门禁，不等同于单个 Effect 的 `reconcile_deadline_at`。到期前 Effect Reconciler 可以按各自保护窗口查询和回填；到期后必须在锁定 Record 的事务中按以下矩阵建立吸收性决议，禁止继续把普通 `START_CALL` 留在 `RETRY_WAIT`：

| 截止时 Provider 事实 | `START_CALL`/Record 决议 | Effect/资源处理 | 容量、线路与 Attempt |
| --- | --- | --- | --- |
| 明确确认 Room、SIP Participant、Agent Participant 和 Egress 全部不存在 | Command `DEAD`，Record `failed`，错误 `START_NOT_CREATED` | 未执行 Effect 置为 `FAILED`；不存在销毁动作 | 释放 active 容量和 Reservation；Reconciler 将 Attempt 明确失败 |
| 确认存在任一资源，包括实际 SIP 已接通或部分资源已创建 | 追加 `startup_reconcile_timeout` Evidence，建立终态屏障，原 Command `SUPERSEDED`，创建或读取唯一 `END_CALL` | 保留创建 Effect 事实，由 Effect 独立租约登记或执行对应销毁图 | 转入安全收尾；Reservation 在 SIP 明确终态前保持占用；Attempt 等终止事实收口 |
| Provider 查询持续失败，仍无法排除任一资源存在 | 与“存在资源”相同，Record 进入 `ending`；`resource_cleanup_status=attention_required` 并写明确告警 | 本次有界 cleanup 尝试结束后按 `OWN-05` 停放，Effect 保持 `RECONCILE_REQUIRED`；到 `resource_cleanup_next_retry_at` 后由 Recovery 重新分配 cleanup Owner | 不释放可能仍有效的 SIP Reservation 或资源隔离；释放 Record 原先占用的 Worker active/cleanup 执行槽，业务页面不再停留 `preparing`，运维持续可见 |

一旦截止矩阵建立 `END_CALL`，所有后续普通命令都按终态屏障 `SUPERSEDED`。即使 Provider 后续迟到返回“资源已创建”，也只能通过旧 generation/稳定资源键进入销毁收敛，不能重新打开通话或恢复原 `START_CALL`。

Linphone 或 Mock 只能替换底层测试端点，不能绕过 `START_CALL` 或另建一套运行时分配流程。

Record 创建合同：

- `room_name` 和 `participant_identity` 继续保持非空，在创建 Record 时预分配确定性标识；
- 预分配标识只表示“计划使用的资源名”，不表示 Provider 资源已经创建；
- Provider 资源是否存在以对应创建 effect 和 Provider 对账结果为准，不能仅凭字段非空判断；
- Record 状态沿用当前小写数据库规范值：`created`、`preparing`、`ready`、`connected`、`user_speaking`、`ai_thinking`、`ai_speaking`、`interrupted`、`waiting`、`ending`、`completed`、`failed`；
- 本文中的大写状态只用于强调状态名称，ORM、迁移、查询和事件 payload 必须写入上述小写值，禁止引入第二套数据库状态值。

### 10.2 `HANDOFF_ACCEPTED`

表示坐席认领事务已经提交。

Worker 动作：

- 校验 handoff 与 call_id；
- 调整接入媒体超时；
- 保持等待音；
- 发布坐席认领状态。

### 10.3 `AGENT_MEDIA_READY`

表示服务端已经验证坐席 Participant 加入 LiveKit，并发布一条未静音的麦克风轨道。该状态不等于坐席已经讲话，也不等于双向 RTP 验收完成。

必须持久化的媒体证据：

- `participant_identity`
- `participant_sid`
- `track_sid`
- `verified_at`
- `evidence_source`
- `media_state_version`
- `media_invalidated_at`
- `last_media_event_key`

这些稳定字段写入 `ai_call_handoff`；`media_state_version` 初始为 0，每次坐席 Participant join/leave、麦克风 track published/unpublished 或 muted/unmuted 的持久事件都必须原子递增。disconnect、unpublished 和 muted 会同时写入 `media_invalidated_at`，使此前的 Participant/Track 证据失效。每次变化都必须写入下述独立 Evidence 表，不能只把证据放在进程内对象或前端上报 payload 中。

新增 `ai_call_handoff_media_evidence` 保存每次媒体变化，避免让 Webhook Inbox 通过改写 Handoff 业务状态表达 Provider 事实：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `tenant_id` | varchar(20) | 租户隔离 |
| `call_id` | varchar(64) | 关联通话 |
| `handoff_id` | varchar(64) | 关联 Handoff |
| `provider_namespace` | varchar(128) | Provider account、project 或 cluster 的稳定命名空间 |
| `participant_identity` | varchar(255) | 坐席 Participant identity |
| `participant_sid` | varchar(255) nullable | 本次事件中的 Participant SID |
| `track_sid` | varchar(255) nullable | 本次事件中的麦克风 Track SID |
| `event_type` | varchar(64) | `joined`、`left`、`published`、`unpublished`、`muted`、`unmuted` 等 |
| `media_state_version` | bigint | 本条 Evidence 提交后对应的 Handoff 媒体版本 |
| `provider_event_id` | varchar(160) nullable | Provider 事件 ID |
| `dedupe_key` | varchar(160) | 租户内稳定去重键 |
| `event_at` | datetime nullable | Provider 明确给出的发生时间 |
| `received_at` | datetime | 数据库接收时间 |
| `evidence_json` | text nullable | 已脱敏的必要媒体证据 |

约束使用 `(tenant_id, provider_namespace, dedupe_key)` 唯一，并为 `(tenant_id, call_id, handoff_id, media_state_version)` 建唯一约束和查询索引；不创建物理外键。Provider 事件时间不确定或乱序时仍先追加 Evidence 和递增版本，最终业务状态由 Runtime Owner 查询当前 LiveKit 事实后决议。

五秒证据缓存不作为 `connected` 判定依据。API 接收 `media-ready` 时可以先查询并持久化候选证据，但 Runtime Worker 每次执行 `AGENT_MEDIA_READY` 都必须重新查询 LiveKit 当前 Participant 和未静音麦克风轨道；不能因为候选证据“未超过 5 秒”跳过查询。

Worker 串行动作：

1. 校验所有权和 fencing token；
2. 校验通话尚未终止；
3. 读取当前 `media_state_version` 作为期望版本并记录查询开始时间，随后强制查询 LiveKit 当前 Participant、Participant SID、未静音麦克风 Track SID 和发布状态；
4. 当前媒体不满足条件时保持 handoff 为 `accepted/reconnecting`，写入失效事实并使命令进入有限重试，不得伪报接通；
5. 查询成功后开启短数据库事务，锁定 Record 和 Handoff，重新校验 Owner、fencing、命令 `processing_token`、终态屏障和 `media_state_version`；
6. 如果查询开始后已持久化 disconnect/unpublished/muted 事件，或 `media_state_version` 与本次期望值不一致，则放弃提交并重新查询；
7. 条件仍成立时保存当前 Participant/Track SID、`verified_at`，原子递增 `media_state_version` 并将 handoff 置为 `connected`；
8. 停止转人工提示和等待音；
9. 停止 AI 输出并保持 AI 不再抢占人工通话；
10. 启动人工坐席录音轨道；
11. 将坐席状态置为 `in_call`；
12. 写入 `handoff_connected` 和命令结果。

如果 disconnect/unpublished/muted 事件在 `connected` 提交后到达，Webhook Inbox 事务只能追加媒体失效 Evidence、单调递增 `media_state_version`、写入 `media_invalidated_at` 并创建 `AGENT_MEDIA_INVALIDATED`；它不能直接把 Handoff 改为 `reconnecting/ended`，也不能直接修改坐席、录音或 Session。Provider 事件时间或顺序不明确时由 Runtime Owner 重新查询 LiveKit，以当前资源事实决议，不能仅比较本机接收时间。

不单独创建 `STOP_WAITING_TONE` 命令，避免媒体接通和停止等待音发生乱序。

#### 10.3.1 `AGENT_MEDIA_INVALIDATED`

表示已经持久化坐席 Participant disconnect、麦克风 track unpublished、muted 或等价的媒体失效 Evidence。命令 payload 只保存 `handoff_id`、Evidence ID 和触发时观察到的 `media_state_version`，不把 webhook 原始 payload 复制到命令表。

Webhook Inbox 与命令创建必须在同一事务中：

1. 锁定 Record 和 Handoff，校验租户、通话模式及 Provider 事件与当前 Participant/Track 的关联；
2. 先按 `(tenant_id, dedupe_key)` 查询 Evidence；重复事件直接返回已有命令状态，不再次递增版本，否则追加脱敏媒体 Evidence；
3. 单调递增 `media_state_version` 并写入 `media_invalidated_at`；
4. 使用包含 Evidence 去重键的稳定幂等键创建或读取 `AGENT_MEDIA_INVALIDATED`；
5. 只提交证据、版本和命令，不修改 Handoff 业务状态、坐席状态、录音或运行时 Session。

Runtime Owner 串行动作：

1. 校验 Owner、fencing token、命令处理令牌、终态屏障和当前 `media_state_version`；
2. 强制查询 LiveKit 当前 Participant、麦克风 Track、静音和发布状态；
3. 媒体已经以更高版本恢复时，将本命令幂等决议为 `SUCCEEDED`，不回退已恢复状态；
4. 媒体仍无效时，将 Handoff 从 `connected` 推进到 `reconnecting` 或按业务终止条件进入结束状态，使坐席离开 `in_call`，停止或切换人工录音，并决定等待重连还是创建 `END_CALL`；
5. 写入明确的媒体失效原因和命令结果。

Owner 已失联时，本命令属于依赖旧 Session 的普通命令，必须按命令恢复矩阵置为 `SUPERSEDED` 并建立唯一 `END_CALL`，不得改投新 Worker 恢复旧人工会话。

坐席首次接入和 `reconnecting` 后重新就绪使用同一个持久入口，不能依赖前端一定再次点击：

1. Provider 的 join、track published 或 unmuted 事件由 Inbox 在同一事务中追加 Handoff Media Evidence 并递增 `media_state_version`；
2. 当事件 identity 与当前认领坐席一致、Handoff 为 `accepted/reconnecting`、通话无终态屏障时，Inbox 使用 `handoff_id + evidence.dedupe_key + media_state_version` 作为幂等键创建或读取 `AGENT_MEDIA_READY`；
3. API 主动提交 `media-ready` 也进入同一 Command Repository，并与相同 Evidence/版本命中同一命令语义，不建立第二条接通路径；
4. Inbox 和 API 都不能直接写 `connected/in_call`；Runtime 必须按 10.3 节重新查询当前 LiveKit 媒体并以版本 CAS 提交；
5. 更高版本失效 Evidence 在命令提交前到达时，旧 `AGENT_MEDIA_READY` 只能重查或 `SUPERSEDED`，不能用旧 join/unmute 证据恢复 `connected`。

### 10.4 `END_CALL`

来源包括：

- 客户 SIP Participant 离开；
- 坐席点击结束；
- Web 客户端结束；
- 系统超时或异常收尾。

`END_CALL` 命令 payload 只保存终止执行所需的稳定引用，不保存会随来源变化的终止事实。所有来源统一使用租户内稳定幂等键 `end-call:{call_id}` 和来源无关的指纹。API、webhook 或 Runtime Worker 检测到终止事件时，都必须通过命令仓储创建或读取同一条 `END_CALL`，禁止绕过命令直接收尾。

新增 `ai_call_end_evidence` 保存每次终止证据：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID |
| `tenant_id` | varchar(20) | 租户隔离 |
| `call_id` | varchar(64) | 全局唯一通话 ID |
| `command_id` | bigint nullable | 关联的唯一 `END_CALL` 命令 |
| `source` | varchar(32) | `customer_sip`、`agent`、`web_client`、`timeout`、`runtime_recovery` 等 |
| `end_reason` | varchar(64) | 本条证据声明的结束原因 |
| `provider` | varchar(32) nullable | `livekit`、`freeswitch` 等外部来源 |
| `provider_namespace` | varchar(128) nullable | Provider account、project 或 cluster 的稳定命名空间；非 Provider 来源为空 |
| `provider_event_id` | varchar(160) nullable | Provider 原始事件 ID |
| `event_at` | datetime nullable | Provider 明确给出的发生时间 |
| `received_at` | datetime | 数据库接收时间 |
| `dedupe_key` | varchar(160) | 本条证据的稳定去重键 |
| `evidence_json` | text nullable | 已脱敏的必要 Provider 证据 |

约束使用 `(tenant_id, dedupe_key)` 唯一，并为 `(tenant_id, call_id, received_at)` 建索引。Provider 来源的 `dedupe_key` 由 `provider + provider_namespace + provider_event_id` 生成，非 Provider 来源使用 `call_id + source + 稳定事件指纹`。每个终止入口在同一事务中幂等追加 Evidence、创建或读取唯一 `END_CALL` 并回填 `command_id`。即使命令已经存在或已经完成，新的合法证据仍可追加，但不得重新打开终态。

Record 的主 `end_reason` 使用第一个在 Record 行锁事务中成功建立终态屏障的有效 Evidence，之后保持不变；后续来源只用于审计和诊断。这样并发客户挂机与坐席挂机只产生一个终止命令，也不会因为 payload 不同触发 `409`。

创建 `END_CALL` 的数据库事务必须同时：

- 写入 `terminal_requested_at`；
- 将通话状态推进到小写 `ending`；
- 将 `resource_cleanup_status` 从 `not_started` 推进到 `reconciling`；
- 将第一条有效 Evidence 的 `end_reason` 写入 Record；Record 已有主原因时不覆盖；
- 将未开始或等待重试的前序非终止命令标记为 `SUPERSEDED`；
- 对正在处理的前序非终止命令写入 `cancel_requested_at` 和 `preempted_by_command_id`，将其状态置为 `SUPERSEDED` 并清除旧 `processing_token`，使旧执行尝试立刻失去数据库写入资格；
- 确认所有更早序号都已有明确决议后，将 `last_applied_command_seq` 推进到 `END_CALL.command_seq - 1`；
- 创建或读取 `dispatch_priority=0`、状态为 `PENDING` 的 `END_CALL`；
- 写入当前 `target_owner_id` 和 `expected_fencing_token` 快照；没有有效 Owner 时保持目标为空，由恢复器分配清理 Owner。

API、Webhook、Outbound 和 Job 进程都不得把 `END_CALL` 直接写成 `PROCESSING`。即使终止事件恰好在 Owner 进程内产生，也必须先提交同一条高优先级 `PENDING` 命令，再由 Runtime 命令消费者通过统一条件更新领取，不保留第二条快速直领路径。

Runtime 领取 `END_CALL` 时同时按第 7 节容量合同处理：当前 Owner 的 Record 为 `active` 时，有空闲清理槽可原子转换为 `cleanup`，否则继续占用原 `active` 槽完成收尾；无有效 Owner 时先由恢复器占用 cleanup 槽并完成旧/新 Worker 计数转换。容量转换事务失败不能撤销终态屏障；当前有效 Owner 立即执行不依赖数据库提交的本地 fail-closed 停播，重试成功领取并获得处理令牌后再登记和执行 Provider 终止 Effect。

`END_CALL` 是“一次只执行一条普通命令”的唯一抢占例外。权威状态机为：

```text
普通命令 PROCESSING
  -> END_CALL 事务写入终态屏障
  -> 旧 processing_token 被撤销
  -> 普通命令 SUPERSEDED
  -> 高优先级 END_CALL PENDING
  -> 有效 Owner 原子领取为 PROCESSING
  -> 本地取消旧协程并有限等待
  -> 执行终止 effect
```

Runtime Worker 收到抢占后必须取消本地旧命令任务，并最多等待一个很短的可配置宽限期；默认 2 秒，且不得超过命令处理租约。宽限期只用于让可取消的本地任务退出，不是执行 `END_CALL` 的前置成功条件。旧 Provider 调用仍在飞行时，`END_CALL` 继续执行，迟到副作用由对应创建 effect、generation 和孤儿资源对账收口。

Worker 终止动作：

1. 校验终态屏障、Owner、fencing token 和命令处理令牌；
2. 取消等待音、AI 音频、响应、打断、恢复和超时任务；
3. 在同一数据库事务中，根据该通话全部已登记创建 Effect（包括 `PENDING/APPLYING/RECONCILE_REQUIRED/APPLIED`）及确定性资源键登记完整终止 Effect 图：阶段 10 的 `HANGUP_SIP`、每个可能 generation 的 `DISCONNECT_AGENT_PARTICIPANT` 和全部 `STOP_EGRESS`，以及阶段 20 的 `DELETE_ROOM`；每个销毁 Effect 必须写 `source_create_effect_id` 和对应 `create_protection_deadline_at`，这样创建调用在 `END_CALL` 逻辑成功后迟到生效时，只需认领既有销毁 Effect，不需要已结束 Command 再授权新登记；
4. 为每个 `DELETE_ROOM` 建立到该通话全部阶段 10 销毁 Effect 的依赖，包括每个可能 generation 的 `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT` 和全部 `STOP_EGRESS`；只有从未登记过对应创建动作，或创建 Effect 已明确 `FAILED(no_resource)` 时，才允许登记并直接 `APPLIED` 的 noop 销毁 Effect；其他场景必须先跨过创建静默门禁，再重新查询或幂等终止并确认，静默前的销毁成功或不存在观察仍必须保持 `RECONCILE_REQUIRED`；
5. 阶段 10 Effect 可以并行认领；立即请求 SIP 挂机、断开 Agent Participant 和停止总录音及客户、AI、人工坐席分轨录音；
6. Provider 仅明确受理停止请求但 Egress 尚未终态时，`STOP_EGRESS` 进入 `RECONCILE_REQUIRED` 并保存 Provider 状态、引用和下次查询时间；只有对应 `START_EGRESS` 已进入静默态，且 Egress 在门禁后再次确认进入 Provider 明确终态或不存在时，才能置为 `APPLIED`；
7. 调用超时、终止请求仅受理但资源终态未知、创建尚未进入静默态、静默后尚未重新确认或资源仍可见时，均不满足 `DELETE_ROOM` 依赖；只有全部阶段 10 销毁 Effect 均为 `APPLIED` 后才能认领 `DELETE_ROOM`，既防止丢失录音资产，也防止 SIP/Agent 的迟到创建在 Room 删除后重新形成孤儿 Room；
8. 结束或取消 active handoff；
9. 坐席进入快速话后处理；
10. 通话记录进入小写 `completed` 或 `failed`；
11. 创建录音对账持久任务；
12. 写入 `sip_hangup`、`session_completed` 和命令结果；Attempt、Target 和 Task 由 `OutboundAttemptReconciler` 根据该终态异步收口。

录音停止请求在处理 `END_CALL` 时立即发出，不等待坐席提交话后结果。`END_CALL` 的逻辑成功最小条件为：

- 本地实时任务已经取消，旧命令处理令牌已经撤销；
- Handoff 和坐席状态已经进入话后或明确异常状态；
- 完整终止 Effect 图和依赖已经持久化；
- `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT` 和全部 `STOP_EGRESS` 至少获得过一次执行尝试，并已进入 `APPLIED` 或 `RECONCILE_REQUIRED`；
- `DELETE_ROOM` 已经 `APPLIED`，或者因任一阶段 10 销毁 Effect 尚未 `APPLIED` 而保持可恢复的依赖阻塞状态；
- SIP 线路 Reservation 已经释放，或者以 `RECONCILE_REQUIRED` 继续占用线路并由 SIP 终止 Effect/cleanup Owner 对账；Attempt Reconciler 只读取该事实投影业务状态。

`END_CALL` 不同步等待录音文件生成、离线 ASR 或语义分析。任一创建 Effect 尚未进入静默态、任一销毁 Effect 未在静默门禁后确认并进入 `APPLIED` 时，清理状态只能是自动处理中的 `reconciling` 或已经停放的 `attention_required`；只有 `EFF-05` 全部条件成立才允许写 `clean`。依赖或 Provider 对账超过自动保护窗口时进入 `attention_required`，按 `OWN-05` 停放并释放对应的 Worker active/cleanup 执行槽，但不得释放仍可能有效的 SIP Reservation、绕过依赖强制删除 Room 或丢弃未完成 Effect。

### 10.5 `CANCEL_HANDOFF`

用于客户在人工媒体首次接通前取消转人工、坐席接入超时或接入前异常。已经 `connected` 或从 `connected` 进入 `reconnecting` 的 Handoff 不允许通过本命令回到 AI；这两类情况由 `AGENT_MEDIA_INVALIDATED` 决定继续等待重连或建立 `END_CALL`。

当前 Runtime Owner 的原子动作：

1. 按 `Record -> Handoff -> Presence -> Command` 锁定参与行，校验 Owner、fencing、租约、命令 token、无终态屏障；
2. Handoff 必须为 `requested`，或为尚无 `connected_at`/人工媒体成功 Evidence 的 `accepted`；命中 `connected/reconnecting/ended` 时本命令 `SUPERSEDED` 并创建或读取唯一 `END_CALL`；
3. 将符合门禁的 Handoff 单调写为 `canceled` 并记录原因；若坐席仍为该 Handoff 独占的 `claiming`、没有其他活跃认领且从未进入 `in_call`，同一事务写 `claiming -> available`；
4. 提交后停止提示和等待音；只有原 AI Session 仍由同一有效 Owner 持有、没有终态屏障且媒体安全检查通过时才恢复 AI，否则建立 `END_CALL`；
5. 任一 CAS 失败都重新读取权威状态，不允许 Presence/Wrap-up Repository 或 API 无条件释放坐席。

### 10.6 录音与离线任务依赖链

离线处理不能由 `END_CALL` 中的一次普通函数调用串联。统一使用以下持久状态推进：

```text
END_CALL
  -> Egress STOP_REQUESTED
  -> Provider Egress COMPLETE / FAILED
  -> 录音资产 AVAILABLE / FAILED
  -> 离线 ASR SUCCEEDED / FAILED
  -> 语义分析 SUCCEEDED / FAILED
  -> 话后决策 SUCCEEDED / FAILED
  -> 符合规则时创建跟进任务
```

- 录音总轨和分轨表的 `status`、`next_verify_at`、`verify_deadline_at` 继续作为录音对账的持久任务事实；
- 录音资产得到终态时，必须在同一事务中创建所需 `ai_call_asr_job`，然后才能发送低延迟唤醒；
- `ai_call_asr_job` 增加 `next_retry_at`、`processing_owner_id`、`processing_token` 和 `processing_expires_at`，ASR Worker 从数据库分批认领，不能把 `asyncio.Queue` 当作任务事实；
- 所有 ASR Job 得到终态时，必须在同一事务中创建或确认 `ai_call_semantic_analysis` 的待分析记录；
- `ai_call_semantic_analysis` 增加 `next_retry_at`、`processing_owner_id`、`processing_token` 和 `processing_expires_at`，Semantic Worker 从数据库分批认领；
- 语义分析成功、确定性话后决策和符合规则时创建 `ai_call_follow_up_task` 必须在同一事务提交；
- Recording、ASR、Semantic 和 Follow-up Worker 的批量 `SKIP LOCKED` 认领只在短事务中写自身 processing token 后提交；结果事务按 `DB-02` 从 Record/上游资产向下游 Job 顺序重新加锁，最后校验当前任务 token，禁止持有下游 Job 行锁再回锁 Record、Recording 或上游 Job；
- Redis 或进程内队列只可用作唤醒优化；队列满、进程重启或通知丢失后，数据库扫描仍能推进整个链路；
- 每一步都有独立持久任务、状态、`error_message` 和稳定幂等键；
- 下游只消费上游明确终态，不通过固定延迟猜测录音是否已经可用；
- 单个分轨失败不能伪装成整条录音成功，必须保留分轨级结果并按业务规则决定是否继续；
- 转人工通话以坐席提交的话后结果为最终业务权威，AI 分析只提供事实和建议；
- Job Worker 分批领取任务，支持重试和人工对账。

## 11. 命令顺序与幂等

1. API 在数据库事务中为同一 `call_id` 分配严格递增的 `command_seq`。
2. Runtime Worker 一次只执行该通电话的一条普通命令；`END_CALL` 是唯一允许撤销旧处理令牌后抢占执行的命令。
3. `last_applied_command_seq` 表示该序号之前的命令都已经得到明确决议，决议包括 `SUCCEEDED`、`DEAD`、`SUPERSEDED` 或 `CANCELED`。
4. 收到小于等于 `last_applied_command_seq` 的重复命令时，返回已有结果。
5. 普通命令存在序号缺口时不越过执行；缺失命令进入 `DEAD` 后必须先由恢复器写入明确决议并推进游标，不能永久阻塞后续命令。
6. `END_CALL` 是吸收性终态屏障，不受普通序号缺口阻塞。创建 `END_CALL` 的事务同时写入 `terminal_requested_at`、决议未开始的前序命令并撤销正在处理命令的 `processing_token`，使所有前序非终止命令失去继续写入业务状态的资格。
7. 普通命令不能在数据库事务内等待 Provider 网络调用；先持久化 Effect 并提交，再执行外部动作。来源命令在首次登记新的 Effect 和提交 Runtime 业务状态前必须检查 `terminal_requested_at`、`cancel_requested_at` 与 Command `processing_token`；任一条件不满足时不得登记新的非终止 Effect，并按已有 `SUPERSEDED` 结果退出。已经登记的 Effect 后续认领和对账只使用 Effect 自身处理租约以及当前 Record Owner/fencing，不因来源 Command token 被撤销而失去恢复路径。
8. `END_CALL` 成功后，后续重复 `END_CALL` 按幂等成功。
9. 通话终态后收到 `AGENT_MEDIA_READY`、`HANDOFF_ACCEPTED` 或 `CANCEL_HANDOFF` 时标记为 `SUPERSEDED`，不能重新打开终态。
10. fencing token 不匹配时不得执行副作用，也不得统一重新路由原命令；必须按第 8 节恢复矩阵决议：依赖旧 Session 的普通命令置为 `SUPERSEDED` 并建立 `END_CALL`；`START_CALL` 无任何 Effect 时才允许 Recovery 重新分配，有任一创建 Effect 时立即建立终态屏障和 `END_CALL` 后对账；`END_CALL` 由 Recovery 分配 cleanup Owner；只有当前 Owner 未变化、仅 Redis 投递丢失或重复时才允许重新投递同一命令。

`DEAD` 不等于“忽略失败”。普通命令进入 `DEAD` 后推进序列游标，但必须记录业务影响并触发对账；`END_CALL` 不允许因普通重试耗尽而永久停在 `DEAD`，必须继续进入终止对账。

## 12. 坐席事件和界面一致性

坐席 SSE 连接仍属于接收连接的 API 实例，但逻辑状态来自数据库。

Runtime Worker 完成命令后：

1. 提交数据库事务；
2. 发布 Redis 坐席领域事件；
3. 所有 API 实例订阅该事件；
4. 每个 API 实例只向本地 SSE 连接推送；
5. 前端断线重连后通过 bootstrap 读取数据库事实。

Redis 坐席事件只用于降低刷新延迟。SSE 丢失、API 重启或 Redis 短暂不可用时，bootstrap 和现有轮询必须恢复正确状态。

## 13. 异常、重试和对账

### 13.1 Redis 不可用

- API 写入数据库命令成功后返回“已受理”；
- Dispatcher 和 Runtime Worker 启用 9.3 节的数据库降级扫描；
- `START_CALL` 可以完成首次 Owner 分配，目标 Worker 从数据库领取；
- 已有通话的 `END_CALL` 不等待 Redis 恢复；
- Redis 恢复后重新投递未决命令；
- 前端展示“处理中”，不能显示已经完成。

### 13.2 Worker 执行失败

- 可重试异常进入 `RETRY_WAIT`，使用有限退避；
- 幂等外部动作在重试前重新读取实际资源状态；
- 达到最大次数后进入 `DEAD`；
- 普通命令进入 `DEAD` 时推进序列游标并触发业务对账；
- `START_CALL` 已有 `START_UNCERTAIN` 或任一创建 Effect 时，不适用通用重试耗尽即 `DEAD`；必须按 `startup_reconcile_deadline_at` 聚合矩阵决议；
- `END_CALL` 普通执行失败时触发高优先级资源对账，不允许静默遗留或永久阻塞终态。

### 13.3 重复或乱序 webhook

- webhook 生成稳定幂等键；
- 相同 LiveKit event ID 不重复创建命令；
- 没有 event ID 时使用 `call_id + participant_identity + event_type + disconnect_reason` 生成指纹；
- 命令序号和终态门禁阻止迟到事件回退状态。

### 13.4 Worker 失联

- Owner 租约过期，或无 Owner 的 `attention` 到达 `resource_cleanup_next_retry_at` 后，由 Recovery Repository 选择并分配新的 cleanup Owner；目标 Runtime 只验证并执行该分配；
- 终止接管不占用普通 `capacity`，而是原子占用独立 `cleanup_capacity`；正常通话满载不能阻止挂机和孤儿资源清理；
- `READY` 和仍存活的 `DRAINING` Worker 都可以承担清理，但不得超过 `cleanup_capacity`；允许的 overcommit 仅限清理槽，不能无限制创建恢复任务；
- 没有普通通话空闲槽时，恢复器仍可选择有清理槽的 Worker 作为 cleanup Owner；竞争失败时重新选择，不能把 `END_CALL` 留给已经失联的 Owner；
- cleanup Owner 分配事务必须按全局顺序锁定 Record，并按 `worker_id` 升序锁定旧 Worker 与目标 Worker；确认目标有空闲清理槽后递增 `runtime_fencing_token`、写入新的 `runtime_owner_id` 和清理租约；根据 Record 原 `runtime_capacity_class` 幂等递减旧 Worker 对应计数，将 Record 写为 `cleanup` 并递增目标 Worker `active_cleanup_count`；它获得的是只允许安全收尾的 Owner 权限，不得恢复 AI 对话或创建新业务媒体；
- 恢复器按第 8 节矩阵重新决议该 Worker 遗留的 `DISPATCHING`、`PUBLISHED` 和过期 `PROCESSING` 命令：依赖旧 Session 的普通命令置为 `SUPERSEDED`，追加 `runtime_recovery` Evidence 并建立唯一 `END_CALL`；已有 `END_CALL` 交给 cleanup Owner；`START_CALL` 无 Effect 时才可重新分配，有任一 Effect 时同样建立 `END_CALL` 后对账；
- 接管只执行安全终止，不恢复旧 AI 对话；
- cleanup Owner 或仍以 `active` 容量执行终止的原 Owner 每次只持有一个有界执行租约；到达自动对账上限仍无法确认资源时，事务将 Record 停放为 `attention`、按原容量类别释放 `active_cleanup_count` 或 `active_call_count` 以及 Owner 租约，并保留 `resource_cleanup_next_retry_at`、Effect、线路 Reservation 和告警；到期后只能由 Recovery 再次分配，不允许常驻协程永久占槽；
- 对账检查 LiveKit Room、Agent Participant、SIP Participant、线路 Reservation、录音 Egress、handoff、坐席和 Attempt；
- 所有异常写入 `error_message`、事件和命令结果。

### 13.5 故障恢复权威矩阵

以下矩阵是实现和故障注入测试的共同依据。任何恢复逻辑都必须落到一个持久状态，禁止用进程内“稍后再试”作为唯一恢复事实。

| 故障点 | 已有持久事实 | 立即行为 | 恢复方式 | 禁止结果 |
| --- | --- | --- | --- | --- |
| Command 事务提交前 API 崩溃 | 无完整事务 | 客户端可使用同一幂等键重试 | 数据库回滚保证没有半条命令 | 创建半个 Record 或跳号后直接执行 |
| `START_CALL` 创建事务已提交但响应丢失 | 原 Record、Command 和创建指纹 | 客户端使用同一幂等键重试 | 生成任何新 ID 前返回原 `call_id/command_id` | 新 `call_id` 导致 409 或第二条 Record |
| Command 已提交、Redis 未发布 | `PENDING/RETRY_WAIT` | API 返回已受理 | Dispatcher 或数据库降级扫描继续 | 把未投递描述成业务完成 |
| Outbound 等待 Runtime 或线路资源 | Attempt `QUEUED`，无 Reservation | 保持 `START_CALL PENDING` | 双资源同时可用时才原子分配 | 提前显示 `DIALING` 或只占线路/Worker 单侧资源 |
| `START_CALL` 排队超过持久截止时间 | Command `PENDING`、无 Owner/Reservation/Effect、容量 `none` | Dispatcher 在锁定事务中写 `DEAD/failed` | Attempt Reconciler 收口为 `ALLOCATION_TIMEOUT` | 使用本机计时器、创建 `END_CALL`，或释放无法排除存在的资源 |
| 已占线路槽但 Provider 调用前 Worker 崩溃 | Reservation `RESERVED`、Attempt `STARTING` | 不自动按租约释放线路 | 对账确认无 SIP 资源后释放；否则转 `RECONCILE_REQUIRED` | 实际已拨号却释放槽导致超卖 |
| Dispatcher 写 `DISPATCHING` 后崩溃 | 带 token 的发布租约 | Worker 不从 `DISPATCHING` 执行 | 到期后 CAS 恢复并用新 token 发布 | 旧 Dispatcher 迟到覆盖新状态 |
| Redis `XADD` 成功、数据库确认失败 | Redis 可能有消息，数据库仍 `DISPATCHING` | Worker 保留 Pending 消息但不执行 | 发布租约到期后按 token 恢复和重复发布 | `PROCESSING -> PUBLISHED` 回退 |
| Runtime 收到无 Owner、Owner 已过期或分配给其他 Worker 的消息 | Record 不证明当前 Runtime 有效持有 | 拒绝领取并触发 Dispatcher/Recovery 扫描 | Dispatcher 首次分配或 Recovery 分配 cleanup Owner 后重新投递 | Runtime 自行 CAS 抢 Owner 或只补 Worker 容量 |
| Worker 领取命令后、调用 Provider 前崩溃 | `PROCESSING`，无 Effect 或 Effect `PENDING` | 不产生外部动作 | 处理租约到期；按命令恢复矩阵决议 | 无 Effect 证据却假定 Provider 已执行 |
| 来源 Command 已结束或 token 被撤销，但 Effect 未收敛 | Effect 有独立状态和处理租约 | 不恢复旧 Command token，也不丢弃 Effect | 当前有效 Owner/cleanup Owner 以新的 Effect token CAS 认领并对账 | 因 Command 非 `PROCESSING` 永久卡住，或用旧命令 token 补写 |
| Provider 调用中 Worker 失联 | Effect `APPLYING` 或结果不确定 | 旧 Worker 在本地租约硬截止前 fail closed | Effect 转 `RECONCILE_REQUIRED`；新 Owner 只对账和收尾 | 重建旧 Session 或盲目重复创建 |
| Provider 成功后、结果写库前崩溃 | Provider 可能有资源，Effect 未确认 | 不允许直接重放创建 | 用稳定资源键/generation 查询并回填或清理 | 一次查询不到就宣布无资源 |
| 创建调用仍未静默，销毁已返回成功或查询暂时不存在 | 创建 Effect 未决，销毁 Effect 已关联 `source_create_effect_id` | 销毁保持 `RECONCILE_REQUIRED`，只记录审计 Evidence | 创建进入静默态后必须再次查询或幂等终止；窗口内迟到创建仍用同一销毁 Effect 终止 | 沿用门禁前的销毁成功/不存在结果提前 `APPLIED`，`clean` 后资源迟到泄漏 |
| `START_UNCERTAIN` 到达聚合截止时间 | Record `preparing`、创建 Effect 未决、Reservation 可能占槽 | 在锁定 Record 的事务中建立吸收性决议，停止普通启动重试 | 全部资源确认不存在则 `DEAD/failed` 并释放；存在任一资源则建立终态屏障和 `END_CALL`；仍无法排除存在则进入 `ending + attention_required`，停放执行槽并按到期时间重新分配清理 | 无限保持 `preparing/RETRY_WAIT`、盲目释放线路、永久占 cleanup 槽，或在不确定资源存在性时直接 `DEAD` |
| Owner 失联时 `START_CALL` 已有结果明确的创建 Effect | Provider 资源事实存在，但新 Worker 没有旧 Session | 立即建立终态屏障，原 START `SUPERSEDED` | Recovery 分配 cleanup Owner，已有资源只进入完整销毁图 | 新 Owner 把 START 完成成 ready/connected，或重建旧 AI 上下文 |
| Agent Participant 连接或发布音轨结果不确定 | `ATTACH_AGENT_PARTICIPANT` 未决、identity 含 generation | 不创建第二个 Agent identity | 按 Room/identity 查询并回填 SID/Track，终态则断开 | 只取消本地 Runner 后宣称无 Agent 资源 |
| 非 Owner 收到客户/坐席挂机 | Record 可能无有效 Owner | Command Repository 锁 Record，建立单调终态屏障和唯一 `END_CALL` | Dispatcher 分配有效 Owner 或 cleanup Owner | 因 Owner 失联拒绝挂机 |
| Runtime Owner 数据库持续不可达 | 本地只有最后一次成功续租基准 | 到 monotonic 硬截止前停止 AI 媒体和新副作用 | 数据库恢复后由新 Owner 对账 | 旧 Owner 无限继续播放 |
| Webhook 签名有效但 Record 暂不可见 | Provider event 可验证，无租户 | 持久化 Quarantine 后返回 2xx | 短期关联成功后进入主 Inbox；超时告警 | 猜默认租户或直接丢弃受管事件 |
| Webhook/Quarantine 持久化失败 | 无可靠接收事实 | 返回可重试 5xx | Provider 重投后按 dedupe key 幂等处理 | 未持久化却返回 2xx |
| Quarantine Worker 认领后崩溃 | Quarantine `PROCESSING`、generation、token 和租约 | 旧 Worker 不得继续提交 | 租约过期后其他 Worker 递增 generation、生成新 token 重领 | 两个 Worker 同时写主 Inbox 或错误租户 |
| 坐席媒体查询后、提交 `connected` 前掉线 | Handoff 有期望 `media_state_version` | Inbox 追加 Evidence、递增版本并创建 `AGENT_MEDIA_INVALIDATED`，版本 CAS 使旧提交失败 | Runtime Owner 重新查询；已提交后由失效命令推进 `reconnecting/ended` | Webhook 直接修改 Handoff/坐席，或使用五秒旧证据伪报接通 |
| Handoff 为 `reconnecting` 后坐席重新发布媒体 | 新 join/published/unmuted Evidence 和更高 `media_state_version` | Inbox 幂等创建 `AGENT_MEDIA_READY`，不直接写 connected | Runtime 查询当前媒体并以新版本 CAS 恢复 connected | 依赖前端必然重提、或 Webhook 直接恢复业务状态 |
| Token 门禁读取后并发建立终态屏障 | 已签短期 Token，Record 已 `ending` | Token 不作为业务成功证据，迟到加入只追加 Evidence | Runtime/Inbox 按终态断开参与者，不创建普通恢复命令 | Token 让终态重开或把迟到加入写成 connected |
| Inbox/离线 Job Worker 处理中崩溃 | Job `PROCESSING` 和处理租约 | 旧 token 失效后不得提交 | 租约到期由其他 Worker 重领 | 永久卡在 `PROCESSING` |
| Attempt Reconciler 处理中崩溃 | Attempt 有独立 `reconcile_token` 和租约 | 旧 token 失效后不得提交 Attempt/Target/Task | 租约到期后由其他 Outbound Reconciler CAS 重领并从持久事实重新投影 | 借用 Runtime Owner、重复推进终态，或直接操作 Provider |
| `STOP_EGRESS` 超时或结果不确定 | 停止 Effect 为 `RECONCILE_REQUIRED`，`DELETE_ROOM` 依赖未满足 | 保留 Room，不执行删除 | 对账停止结果；明确 `APPLIED` 后再删除 Room | 绕过依赖导致录音资产丢失 |
| 录音停止已受理但资产未生成 | STOP Effect 已持久化 | `END_CALL` 可完成逻辑终止，清理状态保持 `reconciling` | 录音对账推进 ASR、语义分析和话后决策 | 用固定延时猜测资产可用 |
| 销毁 Effect 超过保护窗口仍无法确认 | `RECONCILE_REQUIRED` | 保持资源/线路隔离和运维可见；结束本次有界执行 | 转 `attention_required`，原子释放对应 active/cleanup 执行槽并写下次对账时间；Recovery 到期重新分配，也允许人工对账 | Record 已完成就隐藏残留，或永久占住有限 cleanup 槽 |
| Worker 永久离线且旧 Stream 有 Pending | Worker DB 状态 `OFFLINE`、旧 Stream/PEL、Command 权威状态 | Janitor 先以 Worker 行 token 认领，不从 Redis 直接执行 | 逐条读取 DB 后 ACK、恢复或改投；Pending 清零且保留期满足后删除 Stream | 永久保留旧 Stream，或未查 DB 就重放副作用 |

## 14. 部署角色

同一代码库支持不同进程角色：

- `api`：HTTP、webhook、坐席 SSE、命令创建；
- `runtime`：Runtime Worker、实时通话控制、实时事件与对话落库；
- `dispatcher`：数据库命令投递和恢复；
- `outbound`：正式任务认领、`START_CALL` 生产、Attempt 异步收口和 Linphone 验证恢复；
- `jobs`：Webhook Inbox、录音对账、离线 ASR、语义分析、转人工触发、跟进和音色后台任务；
- `legacy_runtime`：仅迁移期使用，为 `legacy_local` 通话保留当前 API 本地 Orchestrator、Registry 和旧终态任务；禁止处理 `owner_command_v1`。

本地可以在一个服务进程中同时启用多个角色，但角色间仍必须通过相同命令接口协作，禁止因为同进程部署而恢复直接调用内存任务。

现有 Worker 到目标角色的权威映射：

| 现有能力或 Worker | 目标角色 | 持久任务事实 | 是否允许多实例 | V1 要求 |
| --- | --- | --- | --- | --- |
| HTTP、LiveKit webhook、坐席 SSE | `api` | Command、Webhook Inbox、业务表 | 是 | 只受理与查询，不持有实时 Session |
| Runtime 命令消费者、Orchestrator、Agent Runner | `runtime` | Record Owner + Runtime Command | 是 | 每通电话仅一个有效 Owner |
| Effect Executor/Reconciler | `runtime` | Runtime Effect + Effect Dependency + Record Owner | 是，按有效 Owner/cleanup Owner | 使用 Effect 独立处理租约；来源 Command 结束后仍可恢复 |
| Event persistence | `runtime` | 事件表；本地队列只缓冲 | 是，按 call Owner | 失权后停止写入，批次写入带 fencing |
| Dialogue persistence | `runtime` | 对话表；本地队列只缓冲 | 是，按 call Owner | 失权后停止写入，终态前排空或持久补偿 |
| Command Dispatcher 与命令恢复 | `dispatcher` | Runtime Command + Worker 注册表 | 是 | 使用数据库原子认领避免重复分配 |
| Outbound Task Executor | `outbound` | Task/Target/Attempt | 是 | 只认领并创建 `START_CALL`，不等待通话结束 |
| Outbound Attempt Reconciler | `outbound` | Attempt + Record + Effect + Reservation | 是 | 使用独立处理租约，作为 Attempt/Target/Task 确定性投影唯一写入者 |
| Linphone Test Recovery | `outbound` | 验证任务与 Attempt | 是 | 仍走正式 `START_CALL`，仅底层端点不同 |
| Outbound validation recovery | `outbound` | 名单校验任务 | 是 | 分批认领，不在 API 启动时做一次性全局恢复 |
| Handoff trigger | `jobs` | 已持久化对话/触发任务 | 是 | 生成持久 Handoff/Command，不直接调用 Session |
| Webhook Inbox/Quarantine Worker | `jobs` | Webhook Inbox + Provider Quarantine | 是 | Inbox 与 Quarantine 分别使用独立处理租约；未知租户事件仅在隔离区解析 |
| Recording reconcile | `jobs` | Recording/Track 状态 | 是 | 录音终态后创建 ASR Job |
| Offline ASR | `jobs` | ASR Job | 是 | 数据库分批认领，队列只唤醒 |
| Semantic analysis | `jobs` | Semantic Analysis | 是 | 数据库分批认领，结果进入话后决策 |
| Follow-up execution | `jobs` | Follow-up Task/Attempt | 是 | AI 建议不等于已执行 |
| Voice enrollment/cleanup | `jobs` | 音色任务表 | 是 | 与实时通话 Owner 分离 |
| Voice preview | `api` + `runtime` | Preview Record + `START_CALL` | 是 | API 受理，Runtime 创建试听 Session，不保留 API 本地 `_sessions` 事实 |

使用显式 `AI_CALL_PROCESS_ROLES` 配置启动角色，并在启动阶段校验：

- 单独的 `api` 不启动 Outbound Executor、Orchestrator Agent Runner、Runtime 命令消费者、离线 Job 或进程内终态收尾任务；
- 迁移期允许单实例、单进程 worker 的 `api,legacy_runtime` 组合仅服务尚未迁移的 `legacy_local` 入口和既有活跃通话；所有本地 Registry 查询必须先验证 Record 模式，命中 `owner_command_v1` 时立即拒绝旧路径；
- `runtime` 启动 Worker 注册、Owner 续租、命令消费者、Effect Executor/Reconciler、实时事件/对话持久化和 Orchestrator；不启动 Owner 首次分配或接管写入器；
- `dispatcher` 启动首次分配、Recovery Repository、cleanup Owner 接管、命令发布和过期命令恢复；
- `outbound` 启动 Task Executor、Attempt Reconciler、名单校验恢复和 Linphone 验证恢复；
- `jobs` 启动 Webhook Inbox、录音对账、离线 ASR、语义分析、转人工触发、跟进和音色后台 Worker；
- `owner_command_v1` 下，API 进程调用 `get_default_ai_call_service` 不得惰性创建本地 Orchestrator；
- API 需要签发 Token 或核验 Participant 时注入无 Session Registry 的轻量 LiveKit Client，不能为了调用 Room API 构造完整 Orchestrator；
- `legacy_runtime` 不得与 `runtime` 在同一进程启用，不得水平扩容，不得创建新 `owner_command_v1` 通话；
- 配置了 `owner_command_v1` 能力的 `api`、`runtime`、`dispatcher`、`outbound` 或处理 V1 数据的 `jobs` 必须确认全部参与表绑定同一个 PostgreSQL datasource identity、database/schema 和事务管理器；任一参与表仍指向 MySQL 或另一个 PostgreSQL 数据源时启动失败；
- 所有入口迁移完成且没有活跃 `legacy_local` 通话后，删除 `legacy_runtime` 角色及其启动开关；
- 正式环境如果角色冲突、Runtime Worker 没有唯一实例 ID，或非迁移配置仍启用旧本地运行时开关，启动必须失败。

迁移期拓扑使用以下唯一合同，不能只依赖 `runtime_control_mode` 后再让请求随机落到没有本地 Session 的 API：

1. 只要数据库仍存在活跃 `legacy_local` 通话，对外 HTTP/API 入口保持单个 `api,legacy_runtime` 实例；Runtime、Dispatcher、Outbound 和 Jobs 可以拆成独立进程，但不再增加第二个对外 API 实例；
2. 该 `api,legacy_runtime` 实例收到 `owner_command_v1` 请求时只创建或查询持久命令，不得因为本进程有旧 Orchestrator 而执行新模式 Session；
3. 迁移窗口不采用“尽量粘滞”的负载均衡作为正确性前提；如果未来要在旧通话未清空前扩容 API，必须另行设计可验证的按 `call_id` 强路由或 legacy runtime 内部控制协议并重新评审；
4. 所有活跃 `legacy_local` 通话收尾、旧进程内任务排空且数据库扫描确认数量为 0 后，删除 `legacy_runtime`，再允许 API 水平扩容；
5. 回滚新入口只影响后续新建 Record；已创建的 `owner_command_v1` 仍由 Runtime 命令链收尾，不能回到 legacy 进程。

正式环境：

- API 在 `legacy_local` 活跃通话清零后可水平扩容；迁移期遵循上述单 API 拓扑；
- Runtime Worker 可水平扩容；
- Dispatcher 至少一个活动实例，多个实例时使用数据库原子认领；
- Outbound Worker 可水平扩容，Task/Target/Attempt 通过数据库原子认领；
- Job Worker 按既有持久任务规则扩容；
- `DB-01` 要求 V1 的 Record、Handoff、Presence、Task、Target、Attempt、Worker、Command、Effect、Reservation、Inbox、Quarantine 及相关 Evidence/Job 表位于同一 PostgreSQL 数据源和本地事务边界，隔离级别固定为 `READ COMMITTED`；“单独 PostgreSQL 控制面 + MySQL 业务表”的跨库部署不属于 V1；
- Owner、容量、命令序列和终态屏障使用 `SELECT ... FOR UPDATE` 与带旧值条件的 `UPDATE ... RETURNING`/影响行数 CAS；批量任务认领使用 PostgreSQL `FOR UPDATE SKIP LOCKED`；
- 所有合同字段中的 `datetime` 在正式迁移中使用 PostgreSQL `timestamptz`；租约、分配截止时间和终态屏障使用 PostgreSQL 数据库时间，应用不得用本机时间替代；
- 死锁、锁等待超时和连接中断使用有限重试并重新读取数据库事实，禁止在未知提交结果后直接重复 Provider 副作用；
- 项目现有 MySQL 只可继续服务完全不进入 `owner_command_v1`、且不会参加 V1 原子事务的旧业务；若某个 V1 入口依赖的 Record/Handoff/Attempt 等权威表仍在 MySQL，必须先迁移到同一 PostgreSQL 数据源，否则该入口启动失败；未来支持 MySQL 时必须另写锁定/CAS SQL、隔离级别和完整故障测试合同并重新评审；
- SQLite 只用于不涉及并发锁、租约竞争和隔离级别的单元测试；
- 首次分配、命令序号、多 Dispatcher 竞争、Worker 崩溃恢复和 fencing 集成测试必须在与正式环境同主版本的 PostgreSQL 执行。

所有时间值集中由版本化 `AiCallRuntimeTimingPolicy` 提供，禁止在 Handler、Repository 和 Worker 中分别硬编码。策略必须区分：

- **协议上限**：Owner fail-closed 安全裕量、Command/Effect/Job 租约必须满足的大小关系，以及 Provider 创建保护窗口不得小于适配器声明的最大调用超时与异步受理窗口；
- **默认配置**：续租 5 秒、Owner 租约 15 秒、默认安全裕量 3 秒、END 本地取消宽限 2 秒、终止扫描 500 毫秒、普通 DB 扫描 1 秒等当前推荐值；
- **验收 SLO**：第 19 节的 P95、RTO 和最终清理时间，只用于验收，不直接作为状态机常量。

每个创建 Effect 持久化适用的 Provider 超时/保护截止，每个 START 持久化策略版本和关键路径预算；部署修改协议上限或默认值时使用新策略版本，不能静默改变已创建通话的截止时间。

## 15. 前端交互

动作接口返回命令已受理，不代表业务完成：

- 创建 Web、SIP 或正式外呼通话返回 HTTP `202`，响应中包含 `acceptance_status=ACCEPTED`、`call_id`、`command_id` 和真实 `command_status`；
- Web 通话在 `START_CALL` 成功后通过 bootstrap 获取 Room 就绪信息，再调用 Token 接口；
- 音色试听使用相同异步就绪和 Token 流程，不再由 API 进程维护独立的 `_sessions` 作为跨请求事实；
- claim 事务成功只表示 Handoff 和坐席预占已提交；API 必须在事务提交后重新执行 Token 门禁，门禁仍成立时才可一并返回短期 Token，否则返回已认领状态和 bootstrap 查询地址，不能用旧事务快照签发；
- `media-ready` 返回 `acceptance_status=ACCEPTED`、命令 ID 和真实 `command_status`；刚创建时通常为 `PENDING`，不能固定伪报 `PROCESSING`；
- 结束通话返回 `acceptance_status=ACCEPTED`、命令 ID 和真实 `command_status`；
- 前端通过 SSE、bootstrap 或命令查询获得最终状态。

每次实际签名之前，Token Service 必须从 V1 PostgreSQL 重新读取并校验：

1. Record 为 `owner_command_v1`、`terminal_requested_at` 为空、业务状态允许加入；
2. `runtime_owner_id` 非空、Owner 租约和 Worker 注册租约按数据库时间均有效；
3. Room 创建 Effect 已 `APPLIED`，`agent_media_ready_at` 与当前 `runtime_fencing_token/agent_resource_generation` 一致，不能只因 `room_name` 非空签发；
4. Web/试听 Token 的参与者身份和权限来自当前 Record；坐席 Token 还必须校验 Handoff 为当前坐席独占的 `accepted/reconnecting`，认领租约有效且 Presence 为 `claiming` 或该 Handoff 对应的 `in_call`；
5. Token metadata 携带 `call_id`、当前 generation 和受控参与者身份，TTL 使用第 14 节集中配置的短期值；签名后终态屏障仍可能并发建立，因此 Token 不是业务成功证据，迟到加入事件必须由 Inbox 持久化并由 Runtime 立即断开，不得重开终态；
6. 任一门禁失败都返回明确的 `CALL_NOT_READY`、`CALL_ENDING`、`OWNER_UNAVAILABLE` 或 `HANDOFF_CLAIM_INVALID`，前端只能重新 bootstrap，API 不得为了签 Token 构造完整 Orchestrator 或读取本地 Registry。

按钮规则：

- Record 为小写 `ending` 或结束命令处理中时禁用重复结束按钮；
- 重复点击使用相同幂等键，不创建第二个业务动作；
- `START_CALL` 等待 Owner 时展示“等待运行资源”，超过业务上限展示明确失败原因；
- `AGENT_MEDIA_READY` 未完成前展示“正在确认坐席媒体”；
- `END_CALL` 完成后进入快速话后处理；
- Record 已为 `completed/failed` 但 `resource_cleanup_status=reconciling` 时，业务页面可显示“通话已结束”，运维详情必须同时显示“资源清理中”；
- `resource_cleanup_status=attention_required` 时在通话详情和运维列表展示明确清理告警及 `resource_cleanup_error`，不能伪装为完全完成；
- 命令失败展示明确错误，不直接把坐席恢复成空闲。

## 16. 实施切片

本规格是端到端架构合同，继续保留 Webhook、录音、离线 ASR、语义分析和跟进链路，避免后续切片重新发明不兼容接口；但不得为整份规格编写一个巨大实现计划。每个切片单独编写、评审和执行实现计划；完成 16.1 Schema 后，第一份行为实现计划只覆盖 16.2A 数据库控制面，不同时引入 Redis、真实 Provider 或业务入口。

实施拆成七个可独立验证的切片：

入口级灰度使用集合配置 `AI_CALL_OWNER_COMMAND_V1_ENTRIES`，合法值只有：

- `web`
- `preview`
- `direct_sip`
- `outbound`，同时覆盖正式任务、Linphone 验证和使用相同正式主链的 Mock

不使用一个全局布尔值，也不为 Linphone/Mock 创建独立产品开关。每个入口在创建 Record 前读取集合配置：命中时写 `owner_command_v1`，未命中时写 `legacy_local`；Record 创建后模式不可改变。

`sip_inbound` 不是合法集合值，也不能映射成 `direct_sip`。`direct_sip` 必须满足“API 先创建 Record/Command，Runtime Owner 再创建 Room/Agent 并主动创建 SIP Participant”；呼入则是 trunk/dispatch rule 已经创建或选定 Room、AgentServer `JobContext` 已持有实时任务，未来如要纳入统一控制面必须使用 `ADOPT_EXISTING_ROOM` 或独立 inbound start mode，并单独定义既有 Room 认领、JobContext/Runtime Owner 关系、容量和终止 Effect。本 V1 不实现、不灰度也不测试该未来命令。

16.1 至 16.5 期间，正式环境的集合必须保持为空。开发和隔离集成环境可以为当前切片临时启用入口，但必须使用独立数据库、Redis 和 Provider 命名空间，不承载业务流量；直接 SIP、Linphone 或正式号码仍受本文件第 1 节的真实拨打确认门禁。隔离验证不等于正式灰度，不能据此提前修改正式环境集合。

切片、入口和进程角色使用以下权威矩阵：

| 阶段 | 正式环境允许启用的新入口 | 隔离环境验证范围 | 新模式必须运行的角色 | 旧模式承载 |
| --- | --- | --- | --- | --- |
| 16.1 Schema/角色门禁 | 无，集合必须为空 | 只验证 Schema、角色冲突和旧模式互斥 | 可部署但不接收新模式流量 | `api,legacy_runtime` 和现有同步 Outbound |
| 16.2A 数据库控制面 | 无，集合必须为空 | PostgreSQL + Provider stub 下验证 Command、Owner、Effect 独立租约和 `END_CALL` 终态屏障 | `runtime + dispatcher`，只使用数据库唤醒 | 同上 |
| 16.2B DB-only 恢复 | 无，集合必须为空 | 双 Dispatcher/Runtime、接管、线路双资源、`START_UNCERTAIN` 截止决议和故障注入 | `runtime + dispatcher`，仍不依赖 Redis | 同上 |
| 16.2C Redis 加速 | 无，集合必须为空 | 增加 Streams/Consumer Group/Pending 恢复，并验证 Redis 丢失不改变 DB-only 正确性 | `runtime + dispatcher` | 同上 |
| 16.3 Web/Preview/Direct SIP | 无，集合必须为空 | 隔离验证 `web`、`preview`、`direct_sip` 的创建、启动失败与 Stub 收尾 | `api + runtime + dispatcher` | 单个 `api,legacy_runtime` 承载正式流量；Outbound 继续同步 `dial()` |
| 16.4 Outbound | 无，集合必须为空 | 隔离验证 `outbound`、Linphone/Mock 适配器、背压和 Attempt Reconciler | `api + runtime + dispatcher + outbound` | 同上 |
| 16.5 生命周期闭环 | 无，集合必须为空 | 四类入口完成真实终止 Effect、Webhook、媒体失效和孤儿清理闭环 | 五个目标角色按职责运行 | 同上 |
| 16.6 租户与离线链路 | 逐个加入已通过本入口全链路租户测试的入口 | 按 `preview -> web -> direct_sip -> outbound` 分别验证；真实电话仍需确认 | 五个目标角色按职责运行 | 单个 `api,legacy_runtime` 只收尾历史通话 |
| 16.7 多实例收口 | 扩大已通过故障和压测的入口流量 | 多实例、故障注入、回滚和量化指标 | 五个目标角色按职责运行 | 清空历史通话后删除 `legacy_runtime` |

正式环境集合只能在 16.6 中按入口增加：该入口必须已经完成真实终止 Effect、Webhook/媒体失效、资源对账、所写全部业务表的租户隔离以及新旧路径互斥测试。每次只增加一个入口，观察期内不得同时扩大其他入口；16.7 故障和压测通过前只允许有界灰度。回滚只影响后续新建通话；已有 Record 继续按不可变的 `runtime_control_mode` 收尾，禁止切换路径。

### 16.1 Schema、租户与角色门禁

- 只实现核心控制面 Schema：Record Owner/终态/`attention` 停放字段、Worker（含 Stream Janitor 租约）、Command、End Evidence、Effect/Dependency、SIP Line Reservation；
- 为本切片涉及的 Record、Attempt 和新表增加租户字段、回填脚本和查询隔离；Event、Dialogue、Recording、ASR、Semantic 的迁移留到 16.6；
- 明确 Record 预分配标识与小写状态合同；
- 增加控制模式、所有权、序列、终态屏障、线路槽和资源清理状态字段；
- 实现命令仓储、`request_fingerprint` 和幂等冲突；
- 按角色矩阵增加启动门禁，API 不再自动启动全部 Worker；
- 建立两个独立 Service/Registry 和目标 PostgreSQL 的集成测试基座；
- `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 保持为空，不迁移现有业务入口；`legacy_runtime` 继续承载旧路径。

### 16.2 数据库控制面、恢复与 Redis 加速

#### 16.2A 数据库命令、Owner 与终态

- 仅以单一 PostgreSQL datasource 的 `READ COMMITTED` 实现 Worker 注册、正常/清理/attention 容量状态、心跳、Owner 租约、fencing token 和 monotonic fail-closed watchdog；
- 实现命令仓储、普通命令严格序列、高优先级 `END_CALL PENDING`、命令处理租约和数据库批量认领；本切片不接入 Redis；
- Dispatcher/Recovery Repository 是首次 Owner 分配和 cleanup Owner 接管的唯一写入者；Runtime 只验证、续租已分配给自己的 Owner；
- 使用 Provider stub 实现非 Owner 终态屏障、`END_CALL` 抢占和终止 Stub，不连接真实 LiveKit/SIP/Egress；
- 实现 Effect 唯一状态机、首次登记授权与独立处理租约：来源 Command 结束后，当前 Owner 或 cleanup Owner 仍可通过 Effect 自身 CAS 继续对账；销毁 Effect 必须关联创建 Effect 和保护截止；
- 覆盖命令令牌撤销、Effect 令牌接管、终态不重开和旧 fencing 写入被拒绝；仍不创建真实 Provider 资源。

#### 16.2B DB-only 恢复与故障注入

- 保持 Redis 完全关闭，仅通过数据库 500 毫秒终止扫描和 1 秒普通扫描验证控制面闭环；
- 实现 Runtime Worker 与 SIP 线路槽的双资源原子分配和恢复，覆盖 Worker 正常容量、cleanup 容量、attention 停放/到期重新分配和 Reservation；
- 实现双 Dispatcher 竞争、Worker 失联、Recovery Repository 接管、Effect 租约过期重领和旧 generation 清理；
- 使用 Provider stub 覆盖 `START_UNCERTAIN` 在 `startup_reconcile_deadline_at` 前后的三种最终决议，不允许无限停留在 `preparing/RETRY_WAIT`；
- 覆盖数据库连接结果不确定、锁冲突、处理租约过期和重复恢复；验证覆盖 Task、Target、Attempt、Record、Line、Worker、Handoff、Presence、Command、Reservation、Effect 的 PostgreSQL 全局锁顺序、CAS 与数据库时间合同；
- 只有 DB-only 路径的故障注入全部通过后，才允许进入 16.2C。

#### 16.2C Redis Streams 加速

- 在已经正确的 DB-only 路径上增加 `DISPATCHING + dispatch_token` CAS 发布协议、Redis Consumer Group、优先级 Stream 和 `XAUTOCLAIM`；
- 覆盖发布确认竞态、重复发布、Redis Pending、Stream 裁剪、永久离线 Worker 的跨 Worker Stream Janitor，以及 Redis 整体暂停或丢失；
- Redis 消息只负责唤醒；Worker 仍以数据库状态、Owner、fencing 和处理令牌决定能否执行；
- Redis 不可用时自动回到 16.2B 的扫描延迟，恢复后允许幂等重复发布；
- DB-only 与 Redis 加速模式必须运行同一组状态机合同测试，除发现延迟外结果完全一致。

### 16.3 Web、音色试听与直接 SIP `START_CALL`

- 接入 `START_CALL` 和首次 Owner 原子分配；
- 将 Web、音色试听和直接 SIP 创建迁移到异步启动流程；
- 补齐入口认证、租户权威来源和直接 SIP 敏感参数加密；
- 实现 `CREATE_ROOM`、`ATTACH_AGENT_PARTICIPANT` 等创建 Effect、generation、Agent readiness evidence 和 Provider 对账；
- 实现 START 依赖 DAG 关键路径截止预算、bootstrap 就绪查询和签名前数据库门禁；覆盖签发读取后并发 END 的迟到加入隔离；
- 隔离环境按 `preview`、`web`、`direct_sip` 依次验证新旧路径互斥、部分创建失败和 Stub 收尾；正式环境集合保持为空，此阶段禁止任何入口进入正式新模式。

### 16.4 正式外呼异步改造

- `OutboundTaskExecutor` 只认领 Task/Target，创建 `QUEUED` Attempt、Record 和 `START_CALL`，不把等待 Runtime/线路资源伪报为 `DIALING`；
- 将同步 `dial()` 拆成 `OutboundDialer.start()` / `SipOutboundDialer.start()`；
- 接入 SIP Line Reservation，只有 Runtime Owner 和线路槽同时获得后 Attempt 才进入 `STARTING`；
- 新增 `OutboundAttemptReconciler`，根据持久状态异步收口 Attempt/Target/Task；
- Linphone/Mock 仅替换底层适配器；
- 隔离环境验证 `outbound`、Linphone 和 Mock 随正式主链一起切换；正式环境集合仍保持为空，不能因为 Mock 通过就提前切换正式任务；
- 实现按租户、Task 和线路的有界 `QUEUED` 背压、`allocation_deadline_at` 超时和 Dispatcher 公平分配；
- 覆盖进程重启、启动结果不确定和重复认领测试。

### 16.5 转人工、挂机与外部 effect

- 接入 `HANDOFF_ACCEPTED`、`AGENT_MEDIA_READY`、`AGENT_MEDIA_INVALIDATED`、`END_CALL` 和 `CANCEL_HANDOFF`；
- 增加 Handoff 的 Participant/Track SID、`media_state_version` 和媒体失效字段，提交 `connected` 前强制查询 LiveKit；join/published/unmuted Evidence 为首次接入和 `reconnecting` 统一幂等创建 `AGENT_MEDIA_READY`；
- 将等待音、人工录音和通话收尾统一移入所属 Worker；
- API 动作接口改为持久命令；
- 实现 `END_CALL` Evidence、来源无关指纹、处理令牌撤销、有限等待和抢占执行；
- 将 16.2A/16.2B 的终止 Stub 替换为真实 `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT`、`STOP_EGRESS`、`DELETE_ROOM` 和依赖门禁；
- 实现 generation、旧 AI Participant 隔离、创建—销毁保护窗口、孤儿扫描、可崩溃恢复 Webhook Inbox 和带独立 processing generation/token/租约的未匹配事件 Quarantine；
- 新控制模式下禁止调用旧本地兜底路径；
- 四类入口在隔离环境完成创建、正常挂机、部分启动失败、Owner 失联和录音终止闭环；正式环境集合仍保持为空。

### 16.6 录音、ASR、语义分析与话后决策

- 为 Event、Dialogue、Recording、Track、ASR 和 Semantic 增加 `tenant_id`、回填和跨租户查询隔离；
- 将录音对账、离线 ASR 和语义分析从进程内队列迁移为数据库认领；
- 按持久依赖链推进录音资产、ASR、语义分析、确定性话后决策和跟进任务；
- 保持坐席 ACW 为转人工通话的最终业务权威；
- 覆盖通知丢失、队列满、Worker 重启和部分分轨失败；
- 对每个候选入口列出其会写入的全部业务表，完成字段非空、Repository 条件、认领、唯一约束和跨租户测试后，才允许把该入口加入正式环境集合；
- 正式灰度顺序默认为 `preview -> web -> direct_sip -> outbound`，每次只增加一个入口；涉及真实 SIP/Linphone/号码时仍需用户明确确认。

### 16.7 多实例故障、压测与迁移收口

- 实现租约失效后的安全接管；
- 实现 `PUBLISHED`、Redis Pending 和 `PROCESSING` 崩溃恢复；
- 实现 Redis 坐席事件广播和 API 本地 SSE 扇出；
- 实现命令 `DEAD` 和资源对账；
- 按第 13.5 节故障矩阵在目标 PostgreSQL 完成并发、乱序、Redis 故障、Worker 重启和迟到 Provider 调用测试；
- 验证第 19 节全部量化指标；
- 验证背压、公平调度和排队超时不会被单个租户、Task 或线路垄断；
- 通过后扩大已灰度入口流量；只有活跃 `legacy_local` 为 0 时才删除 API 直接操作运行时内存的旧路径并水平扩容 API。

每个切片必须拥有独立实现计划，并分别通过测试、静态检查和代码审查；不得把 16.1 至 16.7 合并成一次实现，16.2A、16.2B、16.2C 也必须分别评审和验收。16.1 和 16.2A/16.2B 的 DB-only 核心控制面未稳定前不得接入 Redis 或真实 Provider；16.2C 只允许增加加速能力，不能引入新的业务状态；16.5 未完成真实终止 Effect、Webhook 和录音依赖前不得进入正式灰度；16.6 未完成入口涉及数据的租户隔离前不得向正式环境集合增加该入口；任何真实电话复验仍需再次获得用户明确确认。

## 17. 数据迁移与兼容

1. 同时更新 ORM 模型和目标 PostgreSQL 迁移脚本；正式环境禁止依赖 `metadata.create_all` 修改既有表。
2. 16.1 只增加核心控制面字段与表：Record 可空 `tenant_id`、所有权、终态、支持 `attention` 的 `runtime_capacity_class`、`resource_cleanup_next_retry_at`、START 截止/策略/预算和 Agent readiness 字段；`runtime_control_mode` 使用非空 `legacy_local` 服务端默认值；创建 Worker（含 Stream cleanup 租约）、Command（含 `allocation_deadline_at`）、End Evidence、Effect、Effect Dependency 和 SIP Line Reservation。
3. Effect 迁移必须包含 `provider_namespace`、`source_create_effect_id`、`create_protection_deadline_at`、`absence_observation_count`、`absence_confirmed_at`、`terminal_confirmed_at`、独立的 `processing_owner_id`、`processing_fencing_token`、`processing_token`、`processing_expires_at`、`attempt_count` 和恢复索引；Attempt 增加独立的 `reconcile_*` 投影租约字段，不能在实现时退回借用 Command token。
4. 16.5 再创建带 `provider_namespace` 的 Webhook Inbox、带独立 processing generation/token/租约的 Webhook Quarantine、Handoff 媒体版本字段和 `ai_call_handoff_media_evidence`；在切片启用前不启动对应 Worker，正式环境入口集合仍保持为空。
5. 16.6 按 Record、Task、Attempt、Handoff 等权威关联为 Event、Dialogue、Recording、Track、ASR、Semantic 等对象分批回填 `tenant_id`，输出无法确定租户的记录清单；每个入口涉及的表验证并增加所需非空约束后，才允许灰度该入口。
6. 先审计现有 Outbound 租户值长度，不在本改造中直接把 `varchar(64)` 缩为 `varchar(20)`。
7. V1 参与事务的控制面和业务表迁移只生成和验收同一 PostgreSQL datasource 的 SQL；任一 V1 参与表检测为 MySQL、另一个 PostgreSQL datasource 或不同事务管理器时，`api/runtime/dispatcher/outbound/jobs` 对应角色必须启动失败，不得跨库拼接业务事务或静默换成近似锁语义。
8. 上线前为现有活跃通话确认 `runtime_control_mode=legacy_local`；这些通话继续由旧路径收尾，不尝试运行时接管。
9. `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 为空时所有新通话写入 `legacy_local`；16.1 至 16.5 的正式环境集合必须为空，16.6 起每个入口只能在生命周期闭环和本入口全链路租户隔离通过后独立加入，未加入入口继续写 `legacy_local`，禁止一个全局开关同时切换全部入口。
10. 配置解析必须拒绝 `sip_inbound`，不能将其归一化为 `direct_sip`；现有 Phase F 呼入和 `JobContext` 路径不写 `owner_command_v1 START_CALL`。
11. `runtime_control_mode` 创建后不可改变。每个入口首先读取该字段，只能执行一条路径。
12. `owner_command_v1` 禁止“先创建命令，失败后再直接操作本地 Session”或“先本地执行，再补写命令”。
13. 旧跨进程数据库轮询和持久化收尾只服务 `legacy_local` 通话，查询必须带控制模式条件。
14. 当前 `schedule_livekit_webhook_event`、终态事件后台收尾、离线 ASR 和语义分析的进程内队列只服务迁移前逻辑；新模式分别改由持久 Inbox、`END_CALL` 和数据库任务扫描驱动。
15. 新主链稳定并完成并发验收后，停止创建 `legacy_local` 通话；迁移期保持单个对外 `api,legacy_runtime` 实例，确认没有活跃旧通话后删除 API 直接操作运行时内存的旧路径，再水平扩容 API。
16. 历史终态通话不回填 Owner 或命令，但租户归属必须可查询和审计。
17. 同步 `SipOutboundDialer.dial()` 在迁移期只服务 `legacy_local`；`owner_command_v1` 只能调用新 `start()` 合同并由 `OutboundAttemptReconciler` 收口，禁止同一 Attempt 同时进入两种调用方式。
18. 保留现有 `call_id` 全局唯一约束，并在迁移前检查 `room_name` 重复数据；清理重复后增加 `room_name` 全局唯一约束。AI Participant generation identity 只用于新控制模式，不改写活跃 `legacy_local` Participant。

## 18. 测试与验收

### 18.1 自动化

必须使用两个完全独立的 Service、Orchestrator、Registry 和 Agent Runner，共享同一测试 PostgreSQL；16.2A/16.2B 必须在 Redis 完全关闭时通过，16.2C 及后续 Redis 场景才共享测试 Redis。纯状态单元测试可以使用 SQLite；以下并发与恢复场景必须使用与正式环境同主版本的 PostgreSQL：

1. Web、SIP 和正式外呼分别创建 `START_CALL`，Dispatcher 只向一个可用 Runtime Worker 原子分配 Owner；
2. 两个 Dispatcher 同时分配同一通话，只产生一个 Owner 和一个容量占用；
3. 没有可用 Worker 时命令保持待分配，Worker 上线后能够继续启动；超过持久化 `allocation_deadline_at` 且未产生资源时按排队超时失败；
4. API 实例收到 `media-ready`，所属 Worker 停止等待音并启动人工录音；
5. 媒体证据不足时不创建成功的 `AGENT_MEDIA_READY`；
6. API 实例收到客户挂机，所属 Worker 完成统一收尾；
7. 重复 webhook 只执行一次；
8. `AGENT_MEDIA_READY` 和 `END_CALL` 乱序时终态不回退；
9. 前序命令为 `DEAD` 或存在序号缺口时，`END_CALL` 仍能建立终态屏障；
10. 客户和坐席同时挂机只产生一个终态；
11. Redis 发布成功、数据库状态更新失败时重复投递仍幂等；
12. `PUBLISHED` 消息未消费、Redis Pending 未 ACK 和 `PROCESSING` Worker 崩溃后都能恢复；
13. Redis 暂停期间，`START_CALL` 可通过数据库完成分配，已有通话的 `END_CALL` 可通过降级扫描执行；
14. Worker 续租失败后停止本地控制；
15. 新 Worker 接管后，旧 fencing token 不能写入 Record、Handoff、坐席、Runtime Effect 结果或命令结果；Attempt 投影另外验证旧 `reconcile_token` 不能提交；
16. `legacy_local` 和 `owner_command_v1` 对同一通话严格互斥，不发生双执行；
17. Egress 停止后只有录音资产终态才能依次触发离线 ASR、语义分析和话后决策；
18. 20 至 50 通并发，其中多通同时转人工；
19. 命令 `DEAD` 和 `RECONCILE_REQUIRED` effect 能被对账 Worker 发现；
20. 现有正式任务、录音、语义分析和话后处理测试继续通过。
21. 未认证或无法确定租户的创建请求不能进入 `owner_command_v1`，直接 SIP 原始号码不出现在普通 payload、Redis 或日志；
22. Webhook 在数据库提交前崩溃时不返回成功，提交后 API 崩溃仍能由 Inbox Worker 继续处理；
23. API、Runtime、Dispatcher、Outbound、Jobs 角色启动互斥正确；纯 `api` 进程不会惰性创建 Orchestrator，迁移期 `api,legacy_runtime` 只为 `legacy_local` 创建本地运行时；
24. 离线 ASR 或语义分析唤醒通知丢失、队列满或 Worker 重启后，数据库扫描仍能完成摘要和话后决策。
25. `OutboundTaskExecutor` 创建 `START_CALL` 后即释放执行槽，不等待 SIP 振铃、接通或通话终态；
26. `SipOutboundDialer.start()` 调用超时后由 effect 和 Attempt Reconciler 对账，不重复创建第二个 SIP Participant；
27. 普通命令阻塞在 Provider 调用时，`END_CALL` 能撤销旧处理令牌并在宽限期后继续终止；
28. 旧 Owner 的创建调用在新 Owner 清理后迟到成功，孤儿扫描器能按旧 generation 或稳定资源键发现并删除资源；
29. Redis 降级扫描对 `PENDING`、到期 `RETRY_WAIT` 和可重领 `PUBLISHED` 只允许一个 Worker 原子进入 `PROCESSING`；
30. 相同幂等键与相同指纹返回原命令，相同幂等键与不同指纹返回 `409 IDEMPOTENCY_CONFLICT`；
31. Record 预分配 Room/Participant 标识时 Provider 资源尚不存在，查询和前端不会把非空标识误报为已就绪；
32. Record、Event、Dialogue、Recording、Track、ASR、Semantic、Command、Effect 和 Inbox 的创建、查询、认领与唯一约束均通过跨租户隔离测试；
33. `api`、`runtime`、`dispatcher`、`outbound`、`jobs` 五个目标角色分别启动时，只启动角色矩阵允许的 Worker；迁移期 `legacy_runtime` 只能与单实例 `api` 组合且拒绝 `owner_command_v1`；
34. 客户、坐席和 Provider 同时提交不同 `source/end_reason/requested_at` 时，只产生一条 `END_CALL`，不返回幂等冲突，并保存三条去重后的 Evidence；
35. API、Webhook、Outbound 和 Job 进程只能创建高优先级 `PENDING END_CALL`，不能直接写 `PROCESSING`；
36. 终止事件在 Owner 进程内产生时也经过同一 `PENDING -> PROCESSING` 原子领取路径；
37. 数据库续租持续超时后，旧 Owner 在 monotonic 硬截止前停止 AI 音频，不因本机时钟或数据库重连继续播放；
38. 新 Owner 接管时先断开旧 generation AI Participant，旧 Owner 的迟到媒体任务不能重新发布有效音频；
39. Webhook Inbox Worker 在写入 `PROCESSING` 后被杀死，处理租约到期后其他 Worker 能以新 token 重领并幂等完成；
40. 两个租户尝试复用同一 `call_id` 或 `room_name` 时数据库拒绝；合法业务查询仍必须带租户；
41. 两个 Worker 并发登记同一 Effect 时，唯一约束只保留一条资源动作；
42. Redis 活动 Stream 裁剪、单条消息删除或整个旧 Stream 丢失后，数据库扫描仍能恢复未决命令；
43. Record 已进入 `completed/failed` 但 Effect 未收敛时，`resource_cleanup_status` 保持 `reconciling`；超时后进入 `attention_required`，释放对应的 Worker active/cleanup 执行槽但保留线路/资源隔离、下次对账时间和可见告警；
44. Runtime Worker 普通通话容量满载时，Recovery Repository 仍能向其独立清理容量分配 `END_CALL` cleanup Owner；超过 `cleanup_capacity` 时有界排队而非无限 overcommit，长期 attention 记录不占住执行槽；
45. Owner 已失联且租约过期时，API/Webhook 仍能通过 Command Repository 原子建立终态屏障，但不能写 Runtime 执行结果或调用 Provider；
46. `HANDOFF_ACCEPTED`、`AGENT_MEDIA_READY`、`AGENT_MEDIA_INVALIDATED` 和 `CANCEL_HANDOFF` 在 Owner 失联后均置为 `SUPERSEDED`，不改投新 Worker，并只生成一条 `runtime_recovery END_CALL`；
47. `START_CALL` 的明确成功、排队超时、截止前结果不确定、截止时确认无资源、截止时存在任一资源、截止时仍无法排除资源、部分资源失败、执行前终止和执行中失联均按权威矩阵收口容量、Attempt、Effect 和 Record；Owner 失联时只要已有任一创建 Effect，无论结果明确或不确定都只能 `SUPERSEDED + END_CALL`；
48. Dispatcher 在 `XADD` 与数据库确认之间暂停，同时数据库降级扫描运行时，命令不得从 `PROCESSING` 回退为 `PUBLISHED`；旧 `dispatch_token` 的迟到确认影响行数为 0；
49. Event 和 Dialogue 的写入、批量持久化、语义分析快照和查询均验证 `tenant_id`，跨租户请求无法读取客户原话或事件；
50. 签名无效、明确无关、受管但暂未匹配、数据库不可用四类 Webhook 分别得到 4xx、2xx 忽略、Quarantine 后 2xx、可重试 5xx；
51. 第 13.5 节每个故障点至少有一个目标数据库或 Provider stub 故障注入测试，且不存在双 Owner、双创建、终态重开和无限期 `PROCESSING`。
52. `START_CALL` 创建事务提交后模拟响应丢失，使用同一幂等键重试返回原 `call_id/command_id`，数据库只有一条 Record 和 Command；不同业务请求复用该键才返回 409；
53. 16.1 至 16.5 的正式环境入口集合非空时启动检查失败；隔离环境可以按切片启用测试入口，但不得使用正式数据库、Redis 或 Provider 命名空间；
54. 两个 Outbound/Dispatcher 并发争抢最后一个线路槽时只产生一个未释放 Reservation；等待 Runtime 或线路时 Attempt 保持 `QUEUED`，Provider 受理前不显示 `DIALING`；
55. Agent Participant 在 `room.connect()` 成功后 Worker 崩溃或旧 generation 迟到加入时，Effect Reconciler 能发现、回填或断开，测试结束后不存在旧 generation 音轨；
56. 坐席媒体查询后立即触发 track unpublished/disconnect，Inbox 只能追加 Evidence、递增 `media_state_version` 和创建 `AGENT_MEDIA_INVALIDATED`，不能直接改 Handoff/坐席；版本 CAS 阻止旧证据提交，Runtime Owner 查询后才推进 `reconnecting/ended`；随后 join/published/unmuted 以更高版本幂等创建 `AGENT_MEDIA_READY` 并由 Runtime 重查恢复；
57. `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT` 或 `STOP_EGRESS` 调用超时、仅受理终止、创建尚未静默或静默后尚未重新确认时保持 `RECONCILE_REQUIRED`，`DELETE_ROOM` 始终依赖阻塞；只有全部阶段 10 Effect `APPLIED` 后才允许删除 Room；
58. 正常 Owner 结束、清理槽已满、Worker 失联接管、`active/cleanup -> attention -> cleanup` 和重复完成场景均按 `runtime_capacity_class` 原子转换，Worker 的正常/清理计数不重复增加或递减；
59. Outbound 创建/投影、首次分配、Handoff 认领、Runtime 提交、`END_CALL`、cleanup 接管、Webhook/Quarantine 关联、录音/ASR/语义/跟进推进和恢复器并发执行时，都遵循第 7 节从 `Task` 到 `Quarantine` 的完整全局顺序；任务认领事务不持有任务行回锁业务父行，目标 PostgreSQL 死锁重试之外不存在稳定锁反转；
60. 大型 Outbound Task、多个租户和多条线路并发排队时，未分配 `START_CALL` 数量受有界背压控制，Dispatcher 不长期饿死其他租户或线路，排队超时使用数据库时间；
61. 存在活跃 `legacy_local` 时部署检查和流量验证确保只有一个对外 `api,legacy_runtime` 实例；旧通话清零前不能通过增加纯 `api` 实例绕过本地 Registry 所有权；
62. 普通命令 fencing 不匹配时按恢复矩阵置为 `SUPERSEDED + END_CALL`；`START_CALL` 无 Effect 才允许重新分配，有任一 Effect 时同样 `SUPERSEDED + END_CALL` 后对账；`END_CALL` 由 Recovery 分配 cleanup Owner，不存在统一改投旧 Session 命令的路径。
63. 来源 Command 已 `SUCCEEDED/RETRY_WAIT/SUPERSEDED` 或处理 token 已被撤销时，未完成 Effect 仍可由当前 Owner 或新 cleanup Owner 使用新的 Effect token 接管；旧 Effect token 和旧 Command token 均不能提交。
64. Handoff Trigger 只能幂等创建 `requested`，API 坐席认领只能推进 Handoff `requested -> accepted` 和坐席 `available -> claiming`；Runtime Owner 才能写 `connected/reconnecting/in_call/ACW`，并只在人工媒体从未接通且无其他认领时执行 `canceled + claiming -> available`；Presence/Wrap-up Repository 只能按门禁处理 `offline/available` 和 `acw -> available`；Attempt Reconciler 只能使用独立投影租约写 Attempt/Target/Task。
65. Runtime 收到无 Owner、过期 Owner 或属于其他 Worker 的 Command/Effect 消息时拒绝执行且不能修改 Owner；只有 Dispatcher/Recovery Repository 能完成首次分配或 cleanup 接管。
66. `START_UNCERTAIN` 到达 `startup_reconcile_deadline_at` 后，确认无资源、存在任一资源、仍无法排除资源三种场景分别进入规定决议，并在截止后一个普通扫描周期内离开普通 `preparing/RETRY_WAIT`。
67. 单一 PostgreSQL datasource 的 `READ COMMITTED` 下覆盖 `FOR UPDATE SKIP LOCKED`、`timestamptz` 数据库时间、完整锁顺序、锁冲突和 CAS 故障注入；任一 V1 参与表配置为 MySQL、其他 PostgreSQL datasource 或其他事务管理器时对应角色启动失败。
68. `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 包含 `sip_inbound` 时配置校验失败；Phase F 呼入 `JobContext` 不创建 `START_CALL`、不执行 `CREATE_ROOM`，`direct_sip` 仍只覆盖应用主动外呼。
69. 同一组 Command/Owner/Effect/终态合同先在 16.2B DB-only 模式通过，再在 16.2C Redis 加速模式复跑；除发现延迟外，所有持久状态和 Provider stub 调用次数一致。
70. `END_CALL.target_owner_id` 为空、Owner 过期或 Record 为 `attention` 时，任意 Runtime 扫描都只能拒绝；两个 Recovery 并发时只有一个成功占用清理槽、递增 fencing 并写入目标 Owner。
71. 创建调用未返回、END 已执行，且销毁先返回成功或查询暂时不存在时，销毁 Effect 在创建进入静默态前始终不能 `APPLIED`；创建静默后必须以新 Effect token 再次查询或幂等终止并写 `terminal_confirmed_at`。旧创建在窗口内迟到成功后，同一销毁 Effect 能发现并终止资源，`clean` 不会提前出现。
72. 超过 `cleanup_capacity` 数量的 Provider 长期不可确认记录从 `active` 或 `cleanup` 进入 `attention_required` 后均释放对应 Worker 执行槽并清除旧 Effect token/Record Owner；新的 `END_CALL` 仍在 RTO 内获得槽，attention 到期后按有界批次重新分配，旧 token 不能提交且线路 Reservation 不提前释放。
73. Effect 的所有合法状态边、终态不可回退、租约过期同状态换 token、创建 `FAILED(no_resource)` 和销毁不允许因重试耗尽进入 `FAILED` 使用参数化状态机测试覆盖。
74. V1 控制表在 PostgreSQL、而 Record/Handoff/Attempt 任一表在 MySQL 或另一 datasource 时，五类相关进程角色启动检查失败，不能执行跨库首次分配、认领或投影。
75. 两个 Quarantine Worker 并发认领同一事件只产生一个 processing generation；旧 Worker 超时后的迟到 token 不能写主 Inbox，租约到期后新 Worker 能幂等完成关联。
76. Worker 永久离线且普通/priority Stream 均有 Pending 时，两个 Janitor 竞争只有一个获得数据库清理租约；未决命令先按恢复矩阵处理，旧消息 ACK，Pending 清零和保留期满足后旧 Stream 被删除。
77. Token Service 读取 ready 后并发建立 `terminal_requested_at`，已签短期 Token 的迟到参与者不会重开 Record/Handoff；门禁在 Owner 过期、generation 不一致或 Handoff claim 无效时分别拒绝签发。
78. Handoff 从 `connected -> reconnecting` 后，Provider join/published/unmuted Evidence 自动幂等创建唯一 `AGENT_MEDIA_READY`；Inbox 不直接写 connected，Runtime 以新版本重查提交，旧版本命令无法覆盖再次失效。
79. START 资源 DAG 包含串行与并行分支时，`startup_reconcile_deadline_at` 等于数据库时间加最坏关键路径、迟到保护窗口和安全裕量；策略版本和预算快照可复算，不能退化为单项最大超时。
80. `CANCEL_HANDOFF` 只允许未接通的 `requested/accepted` 进入 `canceled`；坐席仍独占 `claiming` 时同事务释放为 `available`，已 `connected/reconnecting` 时转唯一 `END_CALL`，API、Presence 和旧 Runtime token 均不能无条件释放。

### 18.2 真实验收

自动化和双实例验证通过后，重新获得用户明确拨打确认，只使用单个授权号码验证：

1. 真实 SIP 接听和双向媒体；
2. 坐席接听后等待音停止；
3. 客户挂机后坐席停止听音并进入话后处理；
4. 坐席挂机后客户侧结束；
5. 总录音和客户、AI、人工分轨进入终态；
6. FreeSWITCH/Provider 无活动残留；
7. Task、Target、Attempt、Record、Handoff、Agent 状态一致；
8. 离线 ASR、摘要和话后决策正常生成。

## 19. 成功标准

- 任意 API 实例收到事件，都不会直接操作非本地通话运行态；
- Web、SIP 和正式外呼通过同一首次分配流程获得 Runtime Owner；
- 同一通电话只有一个有效所有者；
- Runtime 不得自行取得无主或过期 Record；首次分配和接管只由 Dispatcher/Recovery Repository 完成；
- 关键命令可追溯、可重试、可幂等；
- Command token 只授权首次登记 Effect；来源 Command 结束后，Effect 仍能通过自身租约被当前 Owner 或 cleanup Owner 恢复；
- Owner 失联时只要 START 已登记任一创建 Effect，就必然建立终态屏障和 `END_CALL`，不会把没有旧 Session 的新 Owner 判成启动成功；
- 销毁 Effect 与创建 Effect/保护窗口一一关联，窗口内一次查询不存在不会提前产生 `APPLIED` 或 `clean`；
- API 资源预占、Runtime 权威状态和 Outbound 确定性投影各有唯一写入边界，不借用彼此租约；
- `END_CALL` 的命令幂等与多来源 Evidence 分离，不因同时挂机产生冲突；
- `PUBLISHED`、Redis Pending、永久离线 Worker 的旧 Stream 和过期 `PROCESSING` 都有自动恢复路径；
- `END_CALL` 不被普通失败或序号缺口永久阻塞；
- 数据库不可达时旧 Owner 在本地硬截止前停止媒体，新 Owner 优先隔离旧 generation；
- `call_id` 和 LiveKit Room 保持全局唯一，租户条件继续作为业务授权边界；
- `START_CALL` 响应丢失重试返回原通话，不生成第二条 Record；
- Runtime 容量和 SIP 线路并发同时受数据库原子门禁，不超卖也不让排队命令提前占槽；
- `runtime_capacity_class` 与 Worker 正常/清理计数可由 Record 重算，结束、接管、attention 停放和到期重领不重复占用或释放容量；长期异常保留资源隔离但不永久占 cleanup 执行槽；
- Webhook Inbox 只保存媒体 Evidence 和创建命令，不直接写 Runtime 状态；Handoff 请求、API 认领、Presence/Wrap-up 与 Runtime 的 `connected/reconnecting/canceled/in_call/ACW` 分别按权限矩阵写入；
- Webhook Quarantine 在多实例下具有独立 generation、token 和租约，未知租户事件不发生并发重复关联；
- 正式环境在真实终止闭环和入口全链路租户隔离完成前不创建 `owner_command_v1` 业务通话；
- 待分配 `START_CALL` 有持久截止时间、有界背压和跨租户/线路公平调度；
- `START_UNCERTAIN` 到达聚合截止时间后必然离开普通 `preparing/RETRY_WAIT`，无法排除资源存在时进入终态屏障和持续清理；
- V1 参与原子事务的控制面和业务表只支持同一 PostgreSQL datasource 的 `READ COMMITTED`；MySQL、跨 datasource 和 `sip_inbound` 均由启动/配置门禁明确拒绝；
- DB-only 控制面先独立通过恢复测试，Redis 只降低发现延迟；
- 新旧控制模式按通话严格互斥；
- 等待音、录音和挂机不再依赖请求恰好到达所属进程；
- Worker 崩溃不会留下不可见、不可重领的通话、录音或坐席占用；Provider 长期不可确认时只能形成可见、停放且持续调度的 `attention_required`；
- 多通并发不破坏单通话命令顺序；
- 真实验收同时满足媒体、资源、数据库和话后终态证据。

量化验收门槛：

| 指标 | 验收门槛 |
| --- | --- |
| 正常 Redis 路由延迟 | 命令数据库提交到进入 `PROCESSING` 的 P95 不超过 1 秒 |
| 高优先级终止发现延迟 | 无论 Redis 是否正常，`END_CALL` 在数据库提交后 1 秒内被有效 Owner 或恢复扫描发现；清理槽已满时进入可观测有界队列，不伪报已领取 |
| Effect 独立恢复 | 来源 Command 终态或 token 撤销后，有可用 Owner/cleanup Owner 时，未完成 Effect 在 1 秒数据库扫描周期内被新 Effect token 发现并开始对账；因 Command 状态导致永久卡住的数量为 0 |
| 启动不确定性收口 | `startup_reconcile_deadline_at` 到期后 1 秒内，Command 和 Record 离开普通 `RETRY_WAIT/preparing`；确认无资源时释放容量，其他情况建立终态屏障并持续终止对账 |
| Worker 失联恢复 RTO | 有可用清理槽时，从最后一次成功心跳起 30 秒内完成新 Owner 接管并开始安全收尾；无清理槽时 30 秒内进入可观测有界队列并告警 |
| Attention 不阻塞清理 | 一次有界自动对账结束后 1 个数据库事务内释放原 active 或 cleanup 执行槽；任意数量 attention 记录不改变新 `END_CALL` 在有空闲槽时的发现/领取 RTO，未释放线路仍继续计入线路并发 |
| 旧 Owner 媒体 fail-closed | 默认 15 秒租约下，从最后一次数据库续租成功起不超过 12 秒停止 AI 音频发布 |
| `END_CALL` 终止请求延迟 | 命令受理到 `HANGUP_SIP`、`STOP_EGRESS` effect 获得执行尝试的 P95 不超过 3 秒 |
| SIP 线路并发正确性 | 任意并发和崩溃组合下，每条线路未释放 Reservation 数不超过 `max_concurrency`，无实际 SIP 资源的异常 Reservation 在对账时限内释放 |
| 创建—销毁静默门禁 | 所有销毁 Effect 在对应创建进入静默态前，沿用销毁成功或查询不存在结果进入 `APPLIED` 的次数为 0；静默后未经重新确认就进入 `APPLIED` 的次数为 0；`resource_cleanup_status=clean` 同样为 0 |
| Room 删除门禁 | 所有测试中 `DELETE_ROOM` 在全部 SIP、Agent Participant、Egress 销毁 Effect `APPLIED` 前执行次数为 0；任何创建尚未静默、静默后尚未重新确认、仅终止请求受理或一次查询不存在都不满足门禁 |
| 容量计数正确性 | 故障注入和重复恢复后，Worker 计数与按 Record `runtime_capacity_class` 重算结果差异为 0，不出现负数 |
| 排队背压与公平性 | 压测中待分配数量不超过策略上限；持续有可用资源时，任一未超限租户或线路不会连续两个 Dispatcher 调度批次完全得不到候选机会 |
| 正式灰度门禁 | 16.1 至 16.5 正式环境集合非空启动次数为 0；16.6 每个入口在生命周期和租户测试通过前进入集合次数为 0 |
| 数据库与呼入门禁 | 任一 V1 参与表位于 MySQL/其他 datasource、包含 `sip_inbound` 或事务管理器不一致时，对应角色成功启动次数为 0；PostgreSQL 主版本与正式环境不一致的并发验收不得计为通过 |
| DB-only/Redis 等价性 | 16.2B 与 16.2C 合同测试的最终持久状态、Owner/Effect fencing 结果和 Provider stub 调用次数差异为 0；仅允许发现延迟不同 |
| 迟到资源清理 | 对应创建保护截止时间结束后 120 秒内，旧 generation 或终态下重现的稳定键资源被发现并进入销毁流程 |
| 离线链路推进 | 录音资产进入终态后 5 秒内创建或确认对应 ASR Job；后续阶段同样由上游终态在 5 秒内推进 |
| 多实例正确性 | 50 通并发、重复 webhook、双 Dispatcher、Worker 重启和 Redis 暂停组合测试中，双 Owner、双拨号、终态重开均为 0 |
| 孤儿资源与未知资源 | Provider 可查询的故障测试在保护期后活动 Agent Participant、SIP Participant、Room、Egress、异常线路 Reservation 和坐席占用残留均为 0；注入 Provider 永久不可查询时，未决资源数必须与可见 `attention_required`/Reservation 逐项一致，隐藏残留为 0 |

若目标环境无法达到上述延迟，必须先用压测数据修改本节门槛并重新评审，不能在实现中静默放宽。
