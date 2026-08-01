# AI Call 16.2B DB-only 恢复与故障注入设计

## 1. 目标与冻结依据

本切片在 16.2A 的 PostgreSQL 单事实源控制面之上，验证两个独立 Dispatcher/Runtime 实例在 Redis 完全关闭、Provider 仅使用 Stub 的条件下，仍能通过数据库恢复 Owner、容量、SIP 线路 Reservation、Command 和 Effect，并在提交结果不确定、处理租约过期或进程失联时保持单执行与可恢复。

冻结依据为：

- 总设计：`docs/superpowers/specs/2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 总设计 SHA-256：`c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8`
- 16.2A 控制面已提交的实现与审计证据：`a0afe57`、`27a665a`

本文件只定义 16.2B 的 DB-only 恢复和故障注入合同，不修改 16.2A 已冻结的权限矩阵、状态名称或终态屏障语义。

## 2. 范围边界

### 2.1 纳入本切片

1. **双实例竞争**：两个 Dispatcher 竞争首次 Owner 和最后一个 Worker/SIP 线路槽；两个 Recovery 竞争过期 Owner、cleanup Owner 和 attention 到期接管。
2. **双资源一致性**：Runtime Worker active/cleanup 容量与 SIP Line Reservation 必须在同一 PostgreSQL 事务中原子占用、转换和释放；不能只成功占用一侧。
3. **Owner 与 Effect 接管**：旧租约、旧 fencing、旧 Effect processing token 失效后，迟到写入影响 0 行；新 Owner 可以从持久事实继续恢复。
4. **启动不确定性收口**：`START_UNCERTAIN` 在 `startup_reconcile_deadline_at` 前保持 `RETRY_WAIT`；到期时按 `NO_RESOURCE`、`RESOURCE_PRESENT`、`UNKNOWN` 三分支决议。
5. **分配截止收口**：未产生 Owner、Reservation 或任意 Effect，且容量仍为 `none` 的 `START_CALL` 超过持久化 `allocation_deadline_at` 后，在锁定 Record 和 Command 的同一事务中进入 `DEAD/failed/ALLOCATION_TIMEOUT`。
6. **数据库故障注入**：覆盖已提交但响应丢失、事务中途异常回滚、锁竞争、租约过期和重复恢复；验证重试不会创建第二个 Provider Effect 或第二个 Reservation。
7. **数据库时间与锁合同**：租约、截止、CAS 和恢复决议使用 PostgreSQL 时间；并发事务在 `READ COMMITTED` 下遵循统一锁顺序。

### 2.2 明确排除

- Redis Streams、Consumer Group、`DISPATCHING` 发布协议和 Pending 恢复；这些属于 16.2C。
- 真实 LiveKit、SIP、Egress、Linphone 或 Provider；Provider 只使用现有 Stub。
- Web、Preview、Direct SIP、Outbound Task/Target/Attempt 正式入口迁移和 `OutboundAttemptReconciler`；这些属于后续业务入口切片。
- 前端页面、SSE、浏览器状态和真实电话验收。
- 跨 PostgreSQL/MySQL 的 V1 原子事务、Schema 扩展以及与本切片无关的 API、坐席、录音、ASR、语义和跟进改动。

## 3. 角色与写入边界

| 角色 | 本切片允许的动作 | 禁止事项 |
| --- | --- | --- |
| Dispatcher | 首次分配 Owner；原子占用 Worker 容量和可选 SIP Reservation；决议未分配 `START_CALL` 的排队超时 | 不执行 Provider；不接管已有 Owner；不释放无法排除存在的资源 |
| Recovery | 接管过期 Owner；分配 cleanup 容量；停放 `attention_required`；在重试时间到达后再次接管 | 不把不确定 Effect 直接标记为无资源；不借用 Runtime Owner token |
| Runtime | 只续租当前 Owner；认领 Command/Effect；登记并提交 Provider Stub 结果；在硬截止时 fail-closed | 不自行取得无主/过期 Record；不绕过 Effect 登记；不用本机时间延长租约 |
| Command Repository | 创建命令、建立 `END_CALL` 屏障、严格推进命令结果、处理 allocation timeout | 不直接执行 Provider；不把普通命令写成 `PROCESSING` |
| Effect Repository | 登记、独立认领、提交和恢复 Effect；依据 Reservation token 转换线路状态 | 不接受旧 Owner、旧 fencing 或旧 Effect token 的迟到写入 |
| Provider Stub | 返回明确成功、明确无资源、失败、超时和不确定结果 | 不连接真实外部服务、不产生真实电话或网络副作用 |

## 4. 核心不变量

本切片沿用总设计的 `OWN-01` 至 `OWN-05`、`CMD-01` 至 `CMD-04`/`CMD-06`、`EFF-01` 至 `EFF-05`、`END-01` 至 `END-04` 和 `DB-01` 至 `DB-03`，并增加以下 16.2B 约束：

- **B-01 双资源原子性**：首次分配只有在 Worker 有 active 容量且指定 SIP Line 有可用槽时，才同时写入 Owner、fencing、Worker 计数和 `RESERVED` Reservation；任一写入失败全部回滚。
- **B-02 资源不提前释放**：`RESERVED` 在 Provider 建链结果明确前不能释放；`ACTIVE`/`RECONCILE_REQUIRED` 只能依据持久 Effect 和终止对账转换。
- **B-03 恢复唯一性**：同一 Record 的两个 Dispatcher/Recovery 事务最多一个提交 Owner 或 cleanup Owner；竞争失败只能重新扫描，不能产生第二个容量增量。
- **B-04 旧写入隔离**：旧 Record fencing、旧 Owner lease、旧 Effect processing token 和旧 Reservation token 的提交影响 0 行，并且不得修改任何业务终态或容量计数。
- **B-05 不确定性优先**：只要存在任意 Effect、未释放 Reservation、非 `none` 容量或无法排除 Provider 资源，就不得按普通排队超时或“无资源”收口。
- **B-06 提交结果可重试**：数据库事务已提交但调用方收到连接错误时，重复请求只能返回原命令/原 Effect/原 Reservation 事实，不得生成第二个对象。
- **B-07 单调终态**：`terminal_requested_at` 一旦建立不得清除；普通命令不能越过终态屏障；逻辑终态和资源清理终态保持分离。

## 5. 状态决议矩阵

### 5.1 未分配 START_CALL

| 条件 | Command | Record | Owner/容量 | Effect/Reservation | 后续 |
| --- | --- | --- | --- | --- | --- |
| 截止前，无 Worker/线路资源 | `PENDING` | `preparing` | 无 Owner，容量 `none` | 无 | Dispatcher 继续按数据库时间公平扫描 |
| `allocation_deadline_at` 到期，且无 Owner/Reservation/任意 Effect，容量 `none` | `DEAD`，`error_message=ALLOCATION_TIMEOUT`，写 `result_json`、`finished_at`，清理 dispatch/processing lease | `failed`，`failure_stage=allocation`，`failure_message=ALLOCATION_TIMEOUT`，`ended_at` 和 cleanup 完成时间写入 | 保持 `none` | 不存在 | 同一事务将 `last_applied_command_seq` 推进到 `START_CALL.command_seq`（初始值必须为 1），不创建 `END_CALL`；Attempt 由后续业务投影明确失败 |
| 到期但存在 Owner、Reservation、任意 Effect 或非 `none` 容量 | 不得改为 allocation timeout | 进入对应终态/清理流程 | 保留资源隔离 | 进入 Effect/Reservation 对账 | 由 Runtime/Recovery 处理，不能释放未知资源 |

### 5.2 START_UNCERTAIN 截止

| Provider 事实 | Command | Record | Effect/Reservation | 后续 |
| --- | --- | --- | --- |
| 确认无 Room、Participant、Egress 等资源 | `DEAD` | `failed`，`failure_stage=startup_reconcile` | 已确认失败/不存在；线路释放 | Owner、容量和本地 handle 收口 |
| 任一资源存在 | 由终态屏障抢占为 `SUPERSEDED`，创建/读取唯一 `END_CALL` | `ending`，等待清理 | 创建完整销毁图；线路保持占用直到终态确认 | 只能走 Effect 对账和 cleanup Owner |
| 截止前 Provider 查询仍不确定 | `RETRY_WAIT`，错误标记 `START_UNCERTAIN` | `preparing` | Effect 保持 `RECONCILE_REQUIRED`；Reservation 保持占用 | 只允许有限重试，不能创建第二个资源 |
| 截止时 Provider 仍不确定 | 由终态屏障抢占为 `SUPERSEDED`，创建/读取唯一 `END_CALL` | `ending` | Effect 保持 `RECONCILE_REQUIRED`；Reservation 保持占用 | 停止普通启动重试；有限 cleanup 后进入 `attention_required`，不得继续停留在 `RETRY_WAIT` |

### 5.3 Effect 与 Reservation

- 只有 CREATE Effect 已为 `APPLIED`，且已持久化可验证的 Provider reference、资源事实和必要依赖，才允许 Reservation 从 `RESERVED` 进入 `ACTIVE`。
- `ACCEPTED`、超时、取消、查询失败、Provider 错误或任何无法排除资源的结果都不得直接进入 `ACTIVE`；统一进入 `RECONCILE_REQUIRED` 并继续计入线路并发。
- 只有调用前失败或已确认 `PERMANENT_NO_RESOURCE` 的结果才能释放 Reservation；Provider 已受理、资源查询失败或结果不确定时不得以“永久失败”名义释放。
- HANGUP/销毁 Effect 在提交 `APPLIED` 后释放 Reservation；迟到或旧 token 不能回退到未释放前状态。
- `resource_cleanup_status=clean` 只有在全部创建静默、全部销毁通过保护截止确认、资源不存在且 Reservation 已释放时成立。

### 5.4 失败阶段与结果字段归属

- `ai_call_record.failure_stage` 是业务失败阶段的权威字段，仅使用 `allocation`、`startup_reconcile`、`runtime`、`cleanup` 等已定义枚举；本切片新增的 allocation timeout 固定为 `allocation`，启动不确定性截止固定为 `startup_reconcile`。
- `ai_call_record.failure_message` 是页面和业务投影读取的权威错误码；allocation timeout 固定为 `ALLOCATION_TIMEOUT`，启动三分支使用 `START_UNCERTAIN:NO_RESOURCE`、`START_UNCERTAIN:RESOURCE_PRESENT`、`START_UNCERTAIN:UNKNOWN`。
- `ai_call_runtime_command.error_message` 保存命令级错误码，`result_json` 保存可审计的结构化决议，`finished_at` 表示命令终态提交时间；Command 与 Record 必须在同一事务写入，不能由测试或实现自行选择其他字段解释失败阶段。

## 6. 事务与锁顺序

### 6.1 首次分配

Dispatcher 先在事务外读取候选 Worker/Record，进入短事务后必须按以下顺序重新锁定和复核：

1. `Record`；
2. 指定 SIP `Line`（有线路参数时）；
3. 所有涉及的 `Worker`（旧 Worker、目标 Worker 和容量转换涉及的其他 Worker，按 `worker_id` 升序）；
4. `START_CALL Command`；
5. `Reservation`；
6. 创建/更新 Reservation、Owner/fencing、Worker 计数和命令目标。

无 Worker、无线路槽、终态屏障、已有资源或命令状态不再满足时，事务返回无变更。

### 6.2 Recovery 接管与 Effect 提交

Recovery 接管和容量转换必须使用同一完整顺序：`Record -> SIP Line -> Worker（所有旧/目标 Worker 按 worker_id 升序） -> Command -> Reservation -> Effect/Effect Dependency`。涉及旧 Worker 与目标 Worker 时，不能按业务角色分别加锁，必须合并后按稳定键升序；任何锁冲突或死锁都只能整事务回滚并重试，禁止留下单侧计数变化。

Effect 提交事务先以 `FOR UPDATE` 锁定 Record，锁定完成后重新读取数据库时间；随后按完整顺序锁定 SIP Line、Worker、Command、Reservation 和 Effect。提交前必须再次同时匹配 Record 当前 Owner、fencing、未过期租约，以及 Effect 当前 processing owner/fencing/token/未过期租约；任一 CAS 影响 0 行，整个事务回滚，不能继续修改 Reservation、Worker 容量或 Command 结果。来源 Command 是否仍为 `PROCESSING` 不属于 Effect 独立恢复的前置条件：来源 Command 已为 `SUCCEEDED`、`RETRY_WAIT` 或被 `END_CALL` 抢占时，当前有效 Owner/cleanup Owner 仍可使用新的 Effect token 接管。

`attention_required` 停放必须在同一事务完成：先锁定 Record、相关 Line、旧 Worker 和未完成 Effect/Reservation，确认本轮 Provider 调用已返回/超时且 Effect 已提交 `RECONCILE_REQUIRED`，再将 `runtime_capacity_class` 从 `active` 或 `cleanup` 转为 `attention`，清空 Record Owner、fencing 租约和本地执行租约，递减与原容量类别对应的 Worker 计数，保留 Effect、Reservation、资源隔离和 `resource_cleanup_next_retry_at`。停放完成后必须能按 Record 重算 Worker 计数；不能释放 Reservation 或清除 Effect。

### 6.3 数据库时间与提交结果

- 所有租约、deadline、重试时间和静默保护时间读取 PostgreSQL 时间；禁止使用事务开始时间的 `now()`/`CURRENT_TIMESTAMP` 作为锁等待后的最终判断。
- 在完成 Record 及本事务所需下游行锁后，重新读取 `clock_timestamp()` 作为最终 `db_now`；所有租约、deadline 和 CAS 过期判断必须使用同一锁后时间。若在最终更新前再次等待锁，必须重新读取 `clock_timestamp()`，不能复用旧值。
- 事务提交后响应丢失时，调用方只允许按租户/幂等键重读并返回已提交事实。
- 不使用本机 watchdog 判断数据库事务是否成功；本地硬截止只负责停止 Provider/媒体调用，最终状态由数据库恢复扫描决定。

## 7. 故障注入矩阵

| 场景 | 注入点 | 必须观察的事实 |
| --- | --- | --- |
| 两个 Dispatcher 争抢最后一个 Worker/SIP 槽 | `SELECT ... FOR UPDATE`/Reservation 插入 | 一个 Owner、一个 Reservation、Worker 计数只增加一次 |
| 两个 Recovery 争抢过期 Owner | 两个独立 Recovery 实例同时执行 Record/Worker 锁竞争 | 一个新 fencing token；旧 token 提交影响 0 行；Worker 计数无重复变化 |
| Owner 在 CREATE 前失联 | Command lease/Record lease 过期 | 无 Effect 时可重领；不产生重复 Provider 调用 |
| Owner 在 CREATE 后失联 | Effect 已登记且状态不确定 | 新 Owner/cleanup Owner 继续 Effect 对账；不按 allocation timeout 释放 |
| Effect processing lease 过期 | Effect CAS 提交前注入延迟 | 只有新 token 能提交，旧 token 影响 0 行 |
| Reservation 写入异常 | Reservation INSERT/flush 失败 | Owner、Worker 计数和 Record 事务全部回滚 |
| Command 提交后响应丢失 | `after_commit` 注入连接错误 | 重试命中原命令，命令序号和副作用数量不变 |
| 首次双资源分配提交后响应丢失 | Owner/Worker/Reservation 事务提交后注入连接错误 | 重试只重读原 Owner、fencing、Worker 计数和 Reservation；不产生第二个 Reservation 或容量增量 |
| Effect 登记提交后响应丢失 | Effect INSERT 提交后注入连接错误 | 重试命中同一 Effect 幂等键；Effect 数量、resource key 和 provider idempotency key 不变 |
| Effect 结果提交后响应丢失 | Effect/Reservation 更新提交后注入连接错误 | 重试只重读原 `APPLIED`/`RECONCILE_REQUIRED` 事实；不重复调用 Provider、不重复释放线路 |
| Recovery 接管提交后响应丢失 | 新 Owner/fencing/容量转换提交后注入连接错误 | 重试只重读新 fencing 和 Worker 计数；旧 Owner 不能再次接管 |
| allocation deadline 到期 | 数据库时间推进 | 无任何资源时 `DEAD/failed/ALLOCATION_TIMEOUT`；不创建 END |
| 锁等待跨越租约截止 | 事务先开始，锁等待后再释放；最终判断使用 `clock_timestamp()` | 过期租约提交影响 0 行；事务完整回滚，不留下单侧容量变化 |
| START_UNCERTAIN deadline 三分支 | 截止前 Stub 返回 no-resource/present/unknown；截止后连续返回 unknown | 截止前按有限重试运行；截止后分别进入失败、END 清理、`SUPERSEDED + END_CALL + attention`，不能无限停留在 `RETRY_WAIT` |
| 来源 Command 已结束的 Effect 接管 | START 为 `SUCCEEDED/RETRY_WAIT` 或被 END 抢占，Effect 为 `RECONCILE_REQUIRED` | 新有效 Owner/cleanup Owner 用新的 Effect token 接管；不要求来源 Command 仍为 `PROCESSING` |
| 迟到创建与销毁保护窗口 | Stub 按固定 resource key/generation 先返回不确定，销毁观察后在保护窗口内迟到创建，静默后再次查询 | 创建 Effect 不早于静默门禁收口；销毁图按 generation 二次确认，`DELETE_ROOM` 依赖不提前满足，Provider Stub 调用次数符合脚本 |

## 8. 验收与证据

必须在隔离 PostgreSQL 16 容器中验证并保留输出：

- `tests/postgres/test_ai_call_runtime_control_postgres.py`：并发分配、Reservation 生命周期、Recovery 接管、START_UNCERTAIN、allocation deadline、事务回滚和提交响应丢失。
- `tests/test_ai_call_runtime_command_repository.py`：幂等、命令序号、终态屏障和旧 token 拒绝。
- `tests/test_ai_call_runtime_owner_repository.py`：Owner/容量/租约转换。
- `tests/test_ai_call_runtime_effect_repository.py`：Effect 独立租约、fencing 和资源状态转换。
- `tests/test_ai_call_runtime_lifecycle.py`、`tests/test_ai_call_runtime_startup_recovery.py`、`tests/test_ai_call_runtime_stub_handlers.py`：生命周期、三分支决议和 Provider Stub。

### 8.1 必须保留的数据库不变量快照

每个并发或故障注入场景至少保留以下 SQL 快照，而不只报告测试通过：

1. `ai_call_record`：`runtime_owner_id`、`runtime_fencing_token`、`runtime_capacity_class`、租约字段、`last_applied_command_seq`、`failure_stage`、`failure_message`、`resource_cleanup_status`。
2. `ai_call_runtime_worker`：`active_call_count`、`active_cleanup_count`，并与按 Record 当前容量类别重算的计数逐 Worker 比较，差异必须为 0。
3. `ai_call_sip_line_reservation`：每个 `call_id` 最多一条记录；`RELEASED` 之外的行数与 Line `max_concurrency` 比较，旧 token 不能产生第二行。
4. `ai_call_runtime_effect`：同一资源键/幂等键的唯一性、`status`、`processing_*`、`fencing_token`、`resource_generation` 和调用次数；旧 token 影响行数必须为 0。
5. `ai_call_runtime_command`：命令序号连续性、`last_applied_command_seq`、`finished_at`、`error_message`、`result_json` 和 dispatch/processing lease 是否清空。

### 8.2 Provider Stub 迟到序列

Provider Stub 必须支持可编排的固定脚本：同一 `provider_namespace + resource_key + resource_generation` 先返回 `UNCERTAIN`，随后在创建保护窗口内观察到销毁请求，再返回迟到的资源存在事实；保护窗口结束后再次查询确认资源终态，并记录每次查询、创建和销毁调用次数。测试必须证明迟到创建不会使销毁 Effect 提前 `APPLIED`，也不会让 `DELETE_ROOM` 越过 SIP/Agent/Egress 销毁依赖；二次销毁只能复用同一稳定幂等键。

完成门槛：

1. PostgreSQL 测试、Runtime 单测、ruff 和 `git diff --check` 全部通过。
2. 不连接 Redis、LiveKit、SIP、Egress 或真实 Provider，不拨号，不启动/重启业务服务。
3. 测试实际实例化两个独立 Dispatcher、两个独立 Recovery 和两个独立 Runtime service；它们只共享同一 PostgreSQL，最终 Owner、fencing、Effect、Reservation 和 Record 事实符合本文件矩阵。
4. 任何失败注入都能由数据库扫描恢复，且没有双执行、容量泄漏、未知资源提前释放或终态屏障重开。
5. 所有故障注入都保留第 8.1 节数据库快照；锁等待跨截止测试证明最终使用锁后 `clock_timestamp()`，而不是事务开始时间。

## 9. 进入下一切片的门槛

只有本文件的 DB-only 故障注入矩阵全部通过，才允许创建 16.2C 计划。16.2C 只能增加 Redis 加速和发布恢复，不得改变本文件定义的持久状态、CAS、锁顺序、Provider Stub 调用次数或终态结果。
