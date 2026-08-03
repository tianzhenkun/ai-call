# Owner Runtime 混合主录音设计

## 1. 状态与目标

- 日期：2026-08-03
- 状态：已批准，待实施计划
- 上位合同：`2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 前置实现：Owner Command、Effect/Recovery、真实 LiveKit Runtime Provider、对话持久化

本切片只把 `owner_command_v1` 的整通混合音频接入现有 Egress、Recording 和话后对账链路，
不同时实现参与方分轨。PostgreSQL 继续是唯一事实源；Provider 返回值、进程内状态和通知均
不能授予执行权。

成功标准：

1. 录音开启且使用真实 LiveKit Provider 时，`START_CALL` 登记唯一、可恢复的
   `START_EGRESS` Effect；Stub 不伪造录音；
2. Egress 启动成功时，Effect `APPLIED` 与租户化 Recording 业务投影在同一 PostgreSQL
   事务提交；
3. Egress 启动失败或结果不确定时通话受控 fail-open，但页面可见 `failed` 或
   `starting`，不得假报正在录音；
4. `END_CALL` 为该创建 Effect 自动登记 `STOP_EGRESS`，只有 Provider 终态确认后才满足
   `DELETE_ROOM` 依赖；
5. 旧 Owner、过期 fencing 或失效 Effect processing token 的晚提交影响 0 行；
6. Runtime 崩溃、超时和接管不会重复启动第二份混合主录音；
7. Egress 资源收口与 OSS 文件验证解耦：资源清理不等待长时间对象验证，Recording 由独立
   对账 worker 收口为 `completed` 或 `failed`；
8. 单元、隔离 PostgreSQL 和 Fake Provider 测试全程不连接真实 LiveKit、SIP、OSS 或电话。

## 2. 业务边界

### 2.1 本切片包含

- Room mixed audio 的一份主录音；
- `START_EGRESS / STOP_EGRESS` 的 Provider 执行、观察、恢复和终态门禁；
- Owner/fencing/Effect token 保护下的 Recording 投影；
- 现有录音查询接口所需的状态、对象名和最终播放资产；
- 现有独立 Recording reconcile worker 的租户化与 OSS 文件验证接续。

### 2.2 本切片不包含

- 客户、AI、人工坐席参与方分轨；
- 离线 ASR、语义分析、跟进任务的新逻辑；
- 前端重做；
- Redis Streams 或其他执行权来源；
- 真实 LiveKit、SIP、OSS、Provider 或电话验收。

以上非目标按“分轨录音 → 话后处理 → 前端闭环 → 真实验收”继续独立实施，不混入本次
提交。

## 3. 合同

| ID | 合同 |
| --- | --- |
| `REC-01` | `START_EGRESS` 只在录音开关开启且 Runtime 使用真实 LiveKit Provider 时登记；DB-only Stub 不登记、不创建 Recording。 |
| `REC-02` | 混合主录音是可选业务能力，不是通话媒体 ready 的必要条件；其失败不得把已具备 Room、Agent 和必要 SIP 的 `START_CALL` 判失败。 |
| `REC-03` | 同一租户、同一 call 只允许一个逻辑主录音和一个稳定的主录音创建 Effect；Owner 接管不得按新 fencing 登记第二个 `START_EGRESS`。 |
| `REC-04` | Effect 提交与 Recording 投影共用一个事务，并同时校验当前 Record Owner、fencing、未过期租约及 Effect processing token。 |
| `REC-05` | `START_EGRESS` 不确定时保持 `RECONCILE_REQUIRED`；恢复只能查询稳定资源键，不得盲目再次调用 Start。 |
| `REC-06` | 每个已登记 `START_EGRESS` 都必须有对应 `STOP_EGRESS`；创建静默前、停止仅受理、停止结果不确定或 Egress 仍活动时，`DELETE_ROOM` 依赖均不满足。 |
| `REC-07` | `STOP_EGRESS APPLIED` 只证明 Provider Egress 已终态或不存在，不证明 OSS 对象已可读；Recording `completed` 必须由独立对象验证证明。 |
| `REC-08` | cleanup clean 等待 `STOP_EGRESS` 终态，但不等待 Recording 的长时间 OSS 验证；验证失败继续形成可见业务失败，不重新占用 Runtime Owner。 |
| `REC-09` | 所有 Recording 创建、更新、查询、claim 和唯一约束均包含 `tenant_id`；禁止依赖全局 call_id 猜租户。 |
| `REC-10` | 错误字段只保存安全摘要，不保存 OSS 密钥、Provider Token、请求体、原始音频或堆栈。 |

## 4. 方案选择

### 4.1 采用：Effect 事务内的 Owner-aware Recording 投影

```text
START_CALL
  -> 登记稳定 START_EGRESS Effect
  -> Provider Start/Query
  -> 同一 PostgreSQL 事务：提交 Effect + 投影 Recording

END_CALL
  -> 从 START_EGRESS 生成 STOP_EGRESS
  -> Provider Stop/Query
  -> 同一 PostgreSQL 事务：提交 Effect + 投影 Recording
  -> 独立 Recording Reconciler 验证 OSS 对象
```

Effect 是外部动作及恢复事实，Recording 是面向业务/API 的投影。两者使用同一 session 和
事务提交，但不把无 fencing 的 legacy `AiCallRecordingService.start_for_session()` 当作
Runtime 写入口。

所有 Effect 提交入口统一经过一个 submission coordinator，顺序固定为：

1. 锁定并校验 Record Owner/fencing/数据库租约；
2. 锁定并校验 Effect processing token；
3. 应用 Effect 状态机；
4. 对 `START_EGRESS / STOP_EGRESS` 应用 Recording 投影；
5. 一次提交或整体回滚。

### 4.2 不采用：Agent 启停钩子直接调用 RecordingService

该方案会绕过 Command/Effect 的恢复事实，也无法阻止旧 Owner 在接管后更新 Recording。

### 4.3 不采用：启动失败即中止通话

主录音是重要的审计能力，但当前产品已确认采用受控 fail-open。失败必须可见、可告警，
不能牺牲已建立的实时通话；生产是否把录音升级为强制合规门禁属于后续策略变更。

## 5. 数据合同

### 5.1 `ai_call_recording`

在现有表上增加并约束：

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| `tenant_id` | `varchar(20)` | 非空；由关联 Record 回填，无法唯一关联时 migration fail closed |
| `egress_generation` | `bigint` | nullable；登记主录音 Effect 后等于该 Effect 的稳定资源 generation |

唯一约束改为 `(tenant_id, call_id)`；索引至少覆盖：

- `(tenant_id, status, next_verify_at)`；
- `(tenant_id, egress_id)`；
- `(tenant_id, oss_id)`。

不创建物理外键，不增加 JSONB，不保存 OSS access key/secret。

### 5.2 稳定幂等标识

混合主录音是 call 级单例，跨 Owner 接管保持：

```text
idempotency_key          = start:{call_id}:start-main-egress
provider_idempotency_key = egress:main:{call_id}
resource_key             = egress:main:{call_id}
resource_generation      = 1
```

Owner fencing 仍用于授权 Effect claim/submit，但不能进入上述主录音单例键。重复登记必须与
首次规格完全一致；不允许因新 Owner fencing 创建第二代主录音。

Provider Start 不具备强幂等保证，因此首次调用超时后 Effect 进入 reconcile-only：通过
Room、稳定对象名和已知 Egress 引用查询，不再盲目重发 Start。超过创建保护窗口仍明确无
资源时，Effect 才以 `FAILED(no_resource)` 静默收口。

### 5.3 Provider observation

扩展强类型 observation，允许携带以下录音事实：

- `egress_id`；
- `object_name`；
- `provider_status`；
- `started_at / ended_at`；
- `duration_ms / file_size`。

这些字段只描述观察结果，不授予执行权。缺失关键引用时不得把 Start 判为
`RESOURCE_PRESENT`；不得用任意 JSON 字符串代替结构化字段。

## 6. 启动流程与 fail-open

1. `_default_start_specs` 继续生成 Room、Agent 和必要 SIP Effect；仅当 Provider 明确声明
   main recording capability 时追加稳定的 `START_EGRESS`。
2. `START_EGRESS` 排在 Room 创建之后执行；实现不得假设 Provider 返回即已持久化。
3. 找不到有效 OSS 配置时不调用 Egress，返回明确的
   `PERMANENT_NO_RESOURCE(oss_config_missing)`，投影一行 `failed` Recording。
4. Start 成功且查询确认资源存在时，同事务写：
   - Effect `APPLIED` 与 `provider_reference=egress_id`；
   - Recording `recording`、`egress_id`、`object_name`、`started_at`、generation；
   - 清空旧的安全错误摘要。
5. Provider 超时或状态不可证明时：
   - Effect `RECONCILE_REQUIRED`；
   - Recording `starting`，`failure_stage=egress_start_uncertain`；
   - 通话 readiness 只检查 Room、Agent 和入口必要 SIP，不等待该可选 Effect。
6. 明确永久失败或保护期后确认未创建时：
   - Effect `FAILED(no_resource)`；
   - Recording `failed`，保存稳定错误码和安全摘要；
   - `START_CALL` 仍可成功，前端不得显示“录音中”。

START readiness 必须显式区分 required specs 与 auxiliary specs，不能通过忽略所有未应用
Effect 实现 fail-open。Room、Agent 和 direct/outbound 的 SIP 仍是强制门禁。

## 7. 停止、恢复与对象验证

1. `END_CALL` 根据全部已登记创建 Effect 自动生成终止图；主录音因此获得唯一
   `STOP_EGRESS`，并成为 `DELETE_ROOM` 的阶段 10 prerequisite。
2. Stop 请求被受理但 Provider 尚未终态时：Effect 保持 `RECONCILE_REQUIRED`，
   Recording 为 `stopping`。
3. Provider 在创建静默门禁后确认 Egress 已终态或不存在时：
   - `STOP_EGRESS` 写 `APPLIED`；
   - 有主录音事实的 Recording 写 `verifying`，保存可得的 ended/duration/file_size；
   - 从未创建成功的 Recording 保持 `failed`，不伪造待验证对象。
4. `DELETE_ROOM` 只在全部非 Room 销毁 Effect `APPLIED` 后执行，不能因录音 fail-open
   绕过 Egress 停止门禁。
5. Runtime 或 Provider 调用后崩溃时，新 Owner 认领同一 Effect；Start 只查询稳定资源，
   Stop 使用已持久化 provider reference 或稳定资源查询恢复。
6. 独立 Recording Reconciler 只 claim Recording 行，不锁 Record、不修改 Effect、也不要求
   Runtime Owner；它按租户查询对象：
   - 对象存在且可读：登记/复用 `sys_oss`，写 `completed`；
   - 截止前不可见：指数退避并保持 `verifying`；
   - 截止后仍不可证明：写 `failed` 和安全错误摘要。
7. `mark_cleanup_clean` 仍以 Effect 静默与销毁终态为资源门禁，不等待第 6 步完成。

## 8. 并发、权限与事务边界

- Runtime 投影事务沿用当前 Effect 提交锁序，并把 Recording 放在 Effect 之后；不得从
  Recording 反向锁 Record 或 Effect。
- 独立 Reconciler 只锁 Recording，使用 `FOR UPDATE SKIP LOCKED` 或等价 claim，避免与
  Runtime 形成反向锁序。
- 重放同一 Start/Stop observation 只能更新同一 `(tenant_id, call_id)` 行；不同 payload
  不能创建第二行。
- Owner A 失权、Owner B 接管后，A 的晚 Start/Stop submit 因 Record/fencing/租约或
  Effect token 不匹配整体回滚，Recording 影响 0 行。
- API 从认证上下文获取租户；普通用户不得传入或覆盖 `tenant_id`，跨租户录音返回空或
  拒绝。
- Stub、Dispatcher、Recovery 和 API 角色不得因为查询 Recording 而初始化 LiveKit、OSS
  或 SIP 客户端；只有显式真实 Provider 执行 Effect 时允许外部 I/O。

## 9. API 与前端状态

继续复用：

```text
GET /ai-call/records/{callId}/recording
```

前端规则：

| Recording 状态 | 展示 |
| --- | --- |
| 无记录 | “未启用录音”，不显示播放器 |
| `starting` | “录音启动确认中”，不显示播放器 |
| `recording` | “录音中”，不提前构造 OSS URL |
| `stopping` | “录音停止确认中” |
| `verifying` | “录音文件处理中” |
| `completed` | 使用接口返回的 `playUrl` 显示播放器 |
| `failed` | “录音失败”，显示安全错误摘要；不影响通话成功状态 |

异步接口和 Provider 受理结果均不代表录音文件已完成；只有 `completed` 可以播放。

## 10. 测试与验收

### 10.1 单元测试

1. 真实 Provider + 录音开启时登记稳定 `START_EGRESS`；关闭录音或 Stub 时不登记；
2. readiness 忽略辅助 Egress 失败，但继续强制 Room、Agent 和必要 SIP；
3. Start/Stop observation 正确映射 Recording 状态和安全错误；
4. Provider Start 超时后只 query 不重复 start；Provider 自身 `TimeoutError` 不误伤 Owner；
5. END graph 包含 `STOP_EGRESS`，且 `DELETE_ROOM` 依赖未满足前执行次数为 0；
6. Runtime、Dispatcher、Recovery 的 Stub 测试不初始化或调用真实外部客户端。

### 10.2 隔离 PostgreSQL

1. Effect `APPLIED` 与 Recording `recording` 原子提交，投影失败时二者同时回滚；
2. 旧 Owner、旧 fencing、过期数据库租约、错误 processing token 的提交均影响 0 行；
3. 两个 Runtime 并发只保留一个主录音 Effect 和一行 Recording；
4. Start 提交后崩溃、Start 超时后迟到创建、Stop 仅受理、Stop 后对象迟到等序列可恢复；
5. 跨租户登记、查询、投影和 Reconciler claim 均不可见；
6. `STOP_EGRESS APPLIED` 后 cleanup 可 clean，Recording 仍可独立从 `verifying` 收口；
7. 重复 Start/Stop observation 不产生重复行、重复 `sys_oss` 或状态倒退。

### 10.3 回归与外部边界

- 重跑 Runtime lifecycle、Owner/fencing、Effect、Recovery、END cleanup、双
  Dispatcher/Runtime 和 legacy Recording 回归；
- 运行 Ruff、CodeGraph sync/status 和 `git diff --check`；
- 使用 Fake Egress/OSS 做超时、迟到和恢复测试，不连接真实依赖；
- 真实 LiveKit/SIP/OSS/Linphone 验收必须在代码与隔离 PostgreSQL 全绿后另行明确确认。

## 11. 提交边界

本设计按数据迁移、Owner-aware projection、Provider Start/Stop、Runtime/Recovery 接线和
验证分别提交。不得暂存 `.playwright-cli/`、环境备份、密钥、本地数据库、录音文件或任何
与本切片无关的脏改动。
