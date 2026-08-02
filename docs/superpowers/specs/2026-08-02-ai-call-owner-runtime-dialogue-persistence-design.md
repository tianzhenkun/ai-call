# Owner Runtime 完成句对话持久化设计

## 1. 状态与目标

- 日期：2026-08-02
- 状态：待书面复核
- 上位合同：`2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 实施切片：16.6 的第一个最小闭环

本切片解决 `owner_command_v1` 真实通话可以正常收发语音、但
`ai_call_dialogue_segment` 没有客户和 AI 对话文本的问题。

成功标准：

1. Qwen 已完成的客户句和 AI 句在正常运行时 1 秒内写入 PostgreSQL；
2. 不保存逐字 partial，不让数据库 I/O 进入实时音频等待路径；
3. 旧 Owner 失权后的批量写入影响 0 行；
4. 同一 Provider 完成事件重放不产生重复对话段；
5. 正常终态标记 `complete`，无法证明已排空时标记 `uncertain`，禁止假报完整；
6. `runtime` 与 `legacy_runtime` 继续互斥，不启用 Redis 或真实外部服务作为实现依赖。

## 2. 已确认根因

Owner Runtime 在 `build_livekit_runtime_provider()` 中创建独立
`AiCallOrchestrator`，`OwnerRuntimeAgentManager` 只启动
`RealtimeCallAgentRunner`。这套 Orchestrator 的 `InMemoryEventStore` 没有接入
`AiCallDialogueRuntimeStore` 和 `AiCallDialoguePersistenceWorker`。

旧对话 worker 只随 `legacy_runtime` 启动；角色校验又明确禁止
`runtime + legacy_runtime` 同进程。因此不能通过增加旧角色解决，也不能把
Owner Runtime 重新耦合到 API 进程的全局 Orchestrator。

## 3. 方案选择

### 3.1 采用：Owner-aware Dialogue Bridge

在 `runtime` 角色内创建专用桥，复用现有聚合、过滤和异步 upsert：

```text
Qwen completed event
  -> Owner Runtime InMemoryEventStore
  -> OwnerRuntimeDialogueBridge
  -> AiCallDialogueRuntimeStore
  -> bounded non-blocking queue
  -> fenced PostgreSQL batch upsert
```

桥只消费形成完成句所需的既有事件，继续沿用打断、未播放 AI、短噪声和重复片段过滤。

### 3.2 不采用：开启 legacy_runtime

该方案违反角色互斥，且旧 worker 绑定的是 API 全局 Orchestrator，不是 Owner Runtime
的 EventStore。

### 3.3 不采用：Agent 热路径直接写数据库

该方案会把连接池等待和事务延迟带入实时语音路径，并复制现有对话聚合规则。

## 4. 数据合同

### 4.1 Record 完整性状态

为 `ai_call_record` 增加：

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `dialogue_persistence_status` | `varchar(16)` | `not_started / pending / complete / uncertain` |
| `dialogue_persistence_error` | `varchar(500)` | 仅保存安全错误摘要，不含原始对话、Token 或堆栈 |
| `dialogue_persistence_completed_at` | `timestamptz` | `complete` 或 `uncertain` 收口时间 |

`owner_command_v1` 创建 Record 时写 `pending`；未迁移的 `legacy_local` 保持
`not_started`。`complete` 只表示所有已观察到的 final/interrupted 片段已成功落库，
不承诺 Qwen 或网络从未漏发事件。

迁移时，现有非终态 `owner_command_v1` Record 写 `pending`；现有终态
`owner_command_v1` Record 因无法重新证明进程内队列已经排空，保守写 `uncertain`；
`legacy_local` 写 `not_started`。数据库增加状态枚举 Check，并禁止新的
`owner_command_v1` Record 使用 `not_started`。

### 4.2 Dialogue 租户字段和唯一性

`ai_call_dialogue_segment` 增加 `tenant_id varchar(20)`。迁移先通过全局唯一
`call_id` 从 Record 回填；存在无法关联的历史行时迁移门禁失败并输出清单，不猜租户。

Repository 的写入、读取和 upsert 必须统一接收 `tenant_id`。唯一约束调整为：

- `(tenant_id, call_id, segment_no)`；
- `(tenant_id, call_id, speaker_type, source, source_segment_id)`。

不创建物理外键。

### 4.3 Owner 上下文

每个入队完成句携带不可变上下文：

```text
tenant_id, call_id, owner_id, fencing_token, source_segment_id, snapshot
```

持久化事务先锁定 Record，再使用数据库当前时间验证：

- `runtime_control_mode = owner_command_v1`；
- `tenant_id / call_id` 匹配；
- `runtime_owner_id / runtime_fencing_token` 匹配；
- Owner 租约仍有效；
- `dialogue_persistence_status = pending`。

任一条件不满足时旧批次不得写 Dialogue。Provider 的来源段 ID 在持久化键中加入
fencing generation，保证接管后的新 Qwen 会话不会与旧会话误碰撞；同一 generation
内重放仍幂等。

## 5. 生命周期与终态

1. LiveKit Runtime Provider 启动前，创建并启动 bridge、runtime store 和 persistence
   worker；Stub Provider 不启动该组件。
2. Owner 获得 `START_CALL` 后，在 Agent 启动前用数据库现有最大 `segment_no + 1`
   初始化该 call 的段号，避免接管后从 1 重新编号。
3. 事件监听只做内存聚合和 `put_nowait`，不等待数据库。
4. `DISCONNECT_AGENT_PARTICIPANT` 先停止 Agent，再完成当前 call 的 finalization 和
   有界 drain；drain 成功后以当前 Owner/fencing、有效租约及
   `terminal_requested_at is not null` CAS 写 `complete`。
5. 队列满、数据库重试耗尽、Runtime 异常退出或旧 Owner 已失权时不得写
   `complete`。Recovery 在持有新 cleanup Owner/fencing 后将无法恢复的 `pending`
   收口为 `uncertain`，再允许 cleanup clean gate 释放 Owner 和容量。
6. `mark_cleanup_clean` 必须拒绝仍为 `pending` 的 Owner Runtime Record；允许
   `complete` 或 `uncertain` 继续资源清理，避免文本问题永久占用 Room 和线路。
7. 关闭顺序固定为：停止 Runtime 取新任务 -> fail-closed/停止本地 Agent ->
   按 call drain -> detach listener -> flush worker -> stop worker。

Dialogue 是通话证据，不授予执行权；通知、内存队列和页面轮询均不能替代 PostgreSQL
Record Owner/fencing。

## 6. API 与前端

- 继续复用现有记录对话查询接口，不新增 Room 或试听会话接口；
- API 从认证上下文取得租户，并通过统一 Repository 查询 PostgreSQL；
- 通话中页面只展示已完成句，建议 500 ms 至 1 s 轮询；
- `pending` 显示“对话整理中”，`uncertain` 显示“对话可能不完整”，不得展示为
  “暂无对话”；
- 所有普通列表、详情和日志不得返回未授权租户的文本。

## 7. 故障处理

| 场景 | 行为 |
| --- | --- |
| 重复 completed 事件 | 来源唯一键 upsert，不新增重复段 |
| PostgreSQL 短暂失败 | 后台 worker 有界重试，实时音频不等待 |
| 队列已满 | 不阻塞音频；记录结构化告警，终态收口为 `uncertain` |
| 旧 Owner 晚提交 | Record/fencing/租约 CAS 失败，Dialogue 影响 0 行 |
| Owner 崩溃且内存队列丢失 | 新 Recovery 明确写 `uncertain`，不伪造文本 |
| Agent 从未成功启动且没有观察到对话事件 | 当前 Owner 可将空对话收口为 `complete` |
| 正常挂机最后一句仍在聚合 | 停止 Agent 后 finalize，再 drain，成功才写 `complete` |
| drain 超时 | 写 `uncertain` 后允许资源清理；写状态也失败则保持 cleanup 可恢复 |

## 8. 测试与验收

### 8.1 单元测试

1. Owner Runtime EventStore 的 customer/AI completed 事件进入现有聚合器；
2. partial 不入库，final/interrupted 入库，未播放 AI 不作为正常句；
3. 热路径使用非阻塞入队；
4. 启停顺序和异常启动回滚不会遗留 worker/listener；
5. Stub Provider 不创建 bridge，也不连接 LiveKit、SIP 或 Provider。

### 8.2 隔离 PostgreSQL

1. 正常客户句和 AI 句分别落一行，完成句 P95 小于 1000 ms；
2. 同一事件重放只保留一行；
3. Owner A 失权、Owner B 接管后，A 的晚批次影响 0 行；
4. B 从现有最大段号继续编号；
5. 正常终态为 `complete`；进程崩溃/队列失败为 `uncertain`；
6. `pending` 阻止 cleanup clean，`complete/uncertain` 允许清理；
7. 两个 Runtime 并发不产生重复段或跨租户写入；
8. Repository 的跨租户读、写、upsert 全部失败或返回空。

### 8.3 回归与真实验证

- 重跑 Runtime lifecycle、Owner fencing、双 Runtime、END_CALL/cleanup 测试；
- 运行 Ruff、相关类型/单元检查和 `git diff --check`；
- 代码和隔离 PostgreSQL 全绿后，另行获得明确确认再拨打本地 Linphone；
- 真实验收必须同时核对页面、Dialogue 行数、speaker/source、完整性状态和终态清理，
  不能仅凭“听到了”判断通过。

## 9. 非目标

- 不做逐字字幕和跨进程 partial 预览；
- 不在本切片启动 Egress、录音、离线 ASR、语义分析或跟进任务；
- 不引入 Redis Streams；
- 不修改 Qwen prompt、VAD、打断或转人工规则；
- 不扩大正式环境 `AI_CALL_OWNER_COMMAND_V1_ENTRIES`；
- 不通过本切片拨打任何真实电话。

## 10. 实施边界

本设计是一个可独立提交和回滚的切片。实施前必须先处理与
`livekit_provider.py` 重叠的既有脏改动：核验后单独提交，或保留为明确的前置提交；
不得把既有真实拨号修复混入对话持久化提交。
