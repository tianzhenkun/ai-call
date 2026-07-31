# AI Call 单通话所有权与跨进程命令设计

## 1. 文档状态

- 设计日期：2026-07-31
- 适用仓库：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- 目标阶段：正式多进程部署前的运行时可靠性改造
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

### 3.2 非目标

1. V1 不恢复 Worker 崩溃前的完整 AI 实时上下文。
2. V1 不承诺 Worker 崩溃后客户无感继续与 AI 对话；接管 Worker 执行安全收尾。
3. 不把 Redis 作为通话、转人工、录音或任务状态的最终事实来源。
4. 不改变现有正式外呼任务、线路快照、Attempt、录音、语义分析和话后决策的业务含义。
5. 不在本阶段实现多地域部署、跨机房容灾或动态负载预测。

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
| 离线 ASR、语义分析、录音对账 | Job Worker | 独立后台任务 | 继续使用持久任务和幂等处理，不持有实时通话所有权 |

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
  ├── 管理接口
  ├── 坐席 claim / media-ready / complete
  ├── LiveKit webhook
  └── 写 ai_call_runtime_command
                 │
                 ▼
Command Dispatcher
  ├── 扫描 PENDING / RETRY_WAIT
  ├── 查询 call_id 当前所有者
  └── 投递 Redis Stream
                 │
                 ▼
ai-call:runtime:{owner_instance_id}
                 │
                 ▼
Runtime Worker
  ├── 获取并续租通话所有权
  ├── 正式拨号
  ├── AI 实时会话与音频
  ├── 转人工和等待音
  ├── 录音控制
  └── 通话统一收尾
                 │
                 ▼
数据库事实 + Redis 坐席事件
```

API 不再直接操作：

- `orchestrator.registry`；
- Agent Runner 内部任务；
- 等待音任务；
- 实时音频传输；
- 通话所属 LiveKit Room 的破坏性操作。

API 可以继续直接执行不依赖运行时内存的查询、数据库认领事务和 Token 签发。

## 7. 通话所有权租约

在 `ai_call_record` 增加：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `runtime_owner_id` | varchar(128) nullable | 当前 Runtime Worker 实例 ID |
| `runtime_fencing_token` | bigint | 所有权版本，初始为 0，每次接管递增 |
| `runtime_lease_expires_at` | datetime nullable | 当前租约过期时间 |
| `runtime_heartbeat_at` | datetime nullable | 最近成功续租时间 |

约束和索引：

- 不创建物理外键；
- `(runtime_owner_id, runtime_lease_expires_at)` 建普通索引；
- 所有 API 中的 bigint 继续按字符串返回；
- 时间统一保存 UTC。

### 7.1 获取所有权

Worker 只能通过带旧值条件的原子更新获取所有权：

1. 通话没有所有者；或
2. 现有租约已经过期；或
3. 当前所有者就是自己。

接管其他 Worker 的过期租约时，必须递增 `runtime_fencing_token`。获取成功后，Worker 才能创建 Session、Agent Runner 和音频任务。

### 7.2 续租

- 默认每 5 秒续租；
- 默认租约有效期 15 秒；
- 连续续租失败或发现 fencing token 已变化时，Worker 立即进入失权状态；
- 失权 Worker取消本地音频、等待音、响应和超时任务，不再执行新的外部副作用。

### 7.3 接管

V1 接管流程：

1. 确认租约已过期；
2. 原子递增 fencing token 并写入新所有者；
3. 等待一个租约保护窗口，确认旧所有者没有恢复续租；
4. 查询数据库、LiveKit Room、录音和 handoff 状态；
5. 对仍在运行的通话执行安全收尾；
6. 写入明确的恢复原因和终态。

由于 LiveKit API 本身不接收 fencing token，任何删除 Room、停止录音等破坏性动作前都必须重新校验数据库租约。接管 Worker 不得尝试恢复旧 AI 对话。

## 8. 持久命令模型

新增 `ai_call_runtime_command` 表，集成现有租户能力，不使用物理外键和数据库专有 JSON 类型。

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `id` | bigint | 雪花 ID，接口按字符串返回 |
| `tenant_id` | varchar(64) | 租户隔离 |
| `call_id` | varchar(64) | 目标通话 |
| `command_seq` | bigint | 同一通电话内严格递增 |
| `command_type` | varchar(64) | 业务命令类型 |
| `idempotency_key` | varchar(128) | 租户内唯一 |
| `payload_json` | text nullable | 非敏感命令参数 |
| `expected_fencing_token` | bigint nullable | 投递时观察到的所有权版本 |
| `target_owner_id` | varchar(128) nullable | 投递目标 Worker |
| `status` | varchar(32) | 命令状态 |
| `attempt_count` | integer | 执行次数 |
| `next_retry_at` | datetime nullable | 下次重试时间 |
| `claimed_at` | datetime nullable | Worker 开始处理时间 |
| `finished_at` | datetime nullable | 最终完成时间 |
| `result_json` | text nullable | 幂等结果 |
| `error_message` | varchar(1000) nullable | 最近失败原因 |
| `created_at` / `updated_at` | datetime | 审计时间 |

唯一约束和索引：

- `(tenant_id, idempotency_key)` 唯一；
- `(tenant_id, call_id, command_seq)` 唯一；
- `(status, next_retry_at)` 索引；
- `(target_owner_id, status, created_at)` 索引；
- `(tenant_id, call_id, created_at)` 索引。

命令状态：

```text
PENDING
  -> PUBLISHED
  -> PROCESSING
  -> SUCCEEDED

PROCESSING
  -> RETRY_WAIT
  -> PUBLISHED

PROCESSING
  -> DEAD
```

`SUCCEEDED` 和 `DEAD` 为终态。重复请求命中相同幂等键时返回原命令和原结果。

## 9. Redis 路由

每个 Worker 消费独立 Stream：

```text
ai-call:runtime:{owner_instance_id}
```

消息只包含：

- `command_id`
- `call_id`
- `command_seq`
- `expected_fencing_token`

业务 payload 由 Worker 从数据库读取，避免 Redis 成为事实来源。

Dispatcher 规则：

1. 扫描到期的 `PENDING` 和 `RETRY_WAIT`；
2. 查询当前未过期的 `runtime_owner_id` 和 fencing token；
3. 没有所有者时保持 `PENDING`；
4. 写入 `target_owner_id` 和 `expected_fencing_token`；
5. `XADD` 到目标 Stream；
6. 将命令更新为 `PUBLISHED`。

Redis 写成功但数据库更新失败时允许重复发布。Worker 必须先锁定数据库命令行并检查状态，只有一个执行路径能够进入 `PROCESSING`。

## 10. 业务命令

### 10.1 `HANDOFF_ACCEPTED`

表示坐席认领事务已经提交。

Worker 动作：

- 校验 handoff 与 call_id；
- 调整接入媒体超时；
- 保持等待音；
- 发布坐席认领状态。

### 10.2 `AGENT_MEDIA_READY`

表示坐席已经加入 LiveKit 且媒体可用。

Worker 串行动作：

1. 校验所有权和 fencing token；
2. 校验通话尚未终止；
3. 将 handoff 置为 `connected`；
4. 停止转人工提示和等待音；
5. 停止 AI 输出并保持 AI 不再抢占人工通话；
6. 启动人工坐席录音轨道；
7. 将坐席状态置为 `in_call`；
8. 写入 `handoff_connected` 和命令结果。

不单独创建 `STOP_WAITING_TONE` 命令，避免媒体接通和停止等待音发生乱序。

### 10.3 `END_CALL`

来源包括：

- 客户 SIP Participant 离开；
- 坐席点击结束；
- Web 客户端结束；
- 系统超时或异常收尾。

payload 至少包含：

- `source`
- `end_reason`
- `requested_at`

Worker 串行动作：

1. 状态进入 `ENDING`；
2. 取消等待音、AI 音频、响应、打断、恢复和超时任务；
3. 停止总录音和客户、AI、人工坐席分轨录音；
4. 删除或确认不存在 LiveKit Room；
5. 结束或取消 active handoff；
6. 坐席进入快速话后处理；
7. 通话记录进入 `COMPLETED` 或 `FAILED`；
8. Attempt、Target 和 Task 根据既有执行规则收口；
9. 触发离线 ASR、语义分析和话后决策；
10. 写入 `sip_hangup`、`session_completed` 和命令结果。

录音在处理 `END_CALL` 时结束，不等待坐席提交话后结果。

### 10.4 `CANCEL_HANDOFF`

用于客户取消转人工、坐席接入超时或转人工异常。

Worker 动作：

- 停止提示和等待音；
- 取消 handoff；
- 根据通话是否仍可继续决定恢复 AI 或生成 `END_CALL`；
- 释放尚未进入 `in_call` 的坐席占用；
- 写入明确的取消原因。

## 11. 命令顺序与幂等

1. API 在数据库事务中为同一 `call_id` 分配严格递增的 `command_seq`。
2. Runtime Worker 一次只执行该通电话的一条命令。
3. 收到小于等于最后成功序号的重复命令时，返回已有结果。
4. 收到存在序号缺口的命令时，不越过执行，等待 Dispatcher 补发。
5. `END_CALL` 成功后，后续重复 `END_CALL` 按幂等成功。
6. 通话终态后收到 `AGENT_MEDIA_READY`、`HANDOFF_ACCEPTED` 或 `CANCEL_HANDOFF` 时拒绝执行，不能重新打开终态。
7. fencing token 不匹配时不执行副作用，将命令退回 Dispatcher 重新路由。

## 12. 坐席事件和界面一致性

坐席 SSE 连接仍属于接收连接的 API 实例，但逻辑状态来自数据库。

Runtime Worker完成命令后：

1. 提交数据库事务；
2. 发布 Redis 坐席领域事件；
3. 所有 API 实例订阅该事件；
4. 每个 API 实例只向本地 SSE 连接推送；
5. 前端断线重连后通过 bootstrap 读取数据库事实。

Redis 坐席事件只用于降低刷新延迟。SSE 丢失、API 重启或 Redis 短暂不可用时，bootstrap 和现有轮询必须恢复正确状态。

## 13. 异常、重试和对账

### 13.1 Redis 不可用

- API 写入数据库命令成功后返回“已受理”；
- 命令保持 `PENDING`；
- Dispatcher 恢复后重新投递；
- 前端展示“处理中”，不能显示已经完成。

### 13.2 Worker 执行失败

- 可重试异常进入 `RETRY_WAIT`，使用有限退避；
- 幂等外部动作在重试前重新读取实际资源状态；
- 达到最大次数后进入 `DEAD`；
- `END_CALL` 进入 `DEAD` 时触发高优先级资源对账，不允许静默遗留。

### 13.3 重复或乱序 webhook

- webhook 生成稳定幂等键；
- 相同 LiveKit event ID 不重复创建命令；
- 没有 event ID 时使用 `call_id + participant_identity + event_type + disconnect_reason` 生成指纹；
- 命令序号和终态门禁阻止迟到事件回退状态。

### 13.4 Worker 失联

- 租约过期后由恢复 Worker接管；
- 接管只执行安全终止，不恢复旧 AI 对话；
- 对账检查 LiveKit Room、SIP Participant、录音 Egress、handoff、坐席和 Attempt；
- 所有异常写入 `error_message`、事件和命令结果。

## 14. 部署角色

同一代码库支持不同进程角色：

- `api`：HTTP、webhook、坐席 SSE、命令创建；
- `runtime`：Outbound Executor、Runtime Worker、实时通话控制；
- `dispatcher`：数据库命令投递和恢复；
- `jobs`：录音对账、离线 ASR、语义分析、跟进执行。

本地可以在一个服务进程中同时启用多个角色，但角色间仍必须通过相同命令接口协作，禁止因为同进程部署而恢复直接调用内存任务。

正式环境：

- API 可水平扩容；
- Runtime Worker 可水平扩容；
- Dispatcher 至少一个活动实例，多个实例时使用数据库原子认领；
- Job Worker 按既有持久任务规则扩容；
- 正式数据库使用 PostgreSQL 或 MySQL，SQLite 仅用于本地开发和测试。

## 15. 前端交互

动作接口返回命令已受理，不代表业务完成：

- claim 成功仍返回加入 LiveKit 所需 Token；
- `media-ready` 返回命令 ID 和 `PROCESSING`；
- 结束通话返回命令 ID 和 `PROCESSING`；
- 前端通过 SSE、bootstrap 或命令查询获得最终状态。

按钮规则：

- `ENDING` 或结束命令处理中时禁用重复结束按钮；
- 重复点击使用相同幂等键，不创建第二个业务动作；
- `AGENT_MEDIA_READY` 未完成前展示“正在确认坐席媒体”；
- `END_CALL` 完成后进入快速话后处理；
- 命令失败展示明确错误，不直接把坐席恢复成空闲。

## 16. 实施切片

本规格涉及运行时基础设施、业务命令迁移和多实例恢复，不能在一个大提交中同时完成。实施拆成三个独立计划：

### 16.1 基础设施

- 增加所有权字段和命令表；
- 实现租约、fencing token、命令仓储和 Dispatcher；
- 实现测试 Redis Stream 适配器；
- 建立两个独立 Service/Registry 的集成测试基座；
- 不迁移现有业务入口。

### 16.2 转人工与挂机主链

- 接入 `HANDOFF_ACCEPTED`、`AGENT_MEDIA_READY`、`END_CALL` 和 `CANCEL_HANDOFF`；
- 将等待音、人工录音和通话收尾统一移入所属 Worker；
- API 动作接口改为持久命令；
- 保留并验证旧兜底路径。

### 16.3 多实例恢复与坐席事件

- 实现租约失效后的安全接管；
- 实现 Redis 坐席事件广播和 API 本地 SSE 扇出；
- 实现命令 `DEAD` 和资源对账；
- 完成并发、乱序、Redis 故障和 Worker 重启测试；
- 通过后再删除 API 直接操作运行时内存的旧路径。

每个切片必须独立通过测试、静态检查和代码审查，前一切片没有稳定前不得开始真实电话复验。

## 17. 数据迁移与兼容

1. 先增加可空所有权字段和命令表，不改变现有通话读取接口。
2. 新建通话必须使用所有权和命令路由。
3. 迁移期间已有活跃通话继续由旧路径收尾，不尝试接管。
4. 当前跨进程数据库轮询和持久化收尾兜底暂时保留。
5. 新主链稳定并完成并发验收后，删除 API 直接操作运行时内存的旧路径。
6. 不为历史终态通话回填所有者或命令。

## 18. 测试与验收

### 18.1 自动化

必须使用两个完全独立的 Service、Orchestrator、Registry 和 Agent Runner，共享数据库和测试 Redis：

1. API 实例收到 `media-ready`，所属 Worker停止等待音并启动人工录音；
2. API 实例收到客户挂机，所属 Worker完成统一收尾；
3. 重复 webhook 只执行一次；
4. `AGENT_MEDIA_READY` 和 `END_CALL` 乱序时终态不回退；
5. 客户和坐席同时挂机只产生一个终态；
6. Redis 发布成功、数据库状态更新失败时重复投递仍幂等；
7. Redis 暂停后命令保留，恢复后成功执行；
8. Worker 续租失败后停止本地控制；
9. 新 Worker 接管后，旧 fencing token 不能写入；
10. 20 至 50 通并发，其中多通同时转人工；
11. 命令 `DEAD` 能被对账 worker 发现；
12. 现有正式任务、录音、语义分析和话后处理测试继续通过。

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
- 同一通电话只有一个有效所有者；
- 关键命令可追溯、可重试、可幂等；
- 等待音、录音和挂机不再依赖请求恰好到达所属进程；
- Worker 崩溃不会留下无限期通话、录音或坐席占用；
- 多通并发不破坏单通话命令顺序；
- 真实验收同时满足媒体、资源、数据库和话后终态证据。
