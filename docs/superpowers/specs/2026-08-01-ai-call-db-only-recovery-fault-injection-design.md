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
| `allocation_deadline_at` 到期，且无 Owner/Reservation/任意 Effect，容量 `none` | `DEAD`，`ALLOCATION_TIMEOUT` | `failed`，`failure_stage=allocation` | 保持 `none` | 不存在 | 不创建 `END_CALL`；Attempt 由后续业务投影明确失败 |
| 到期但存在 Owner、Reservation、任意 Effect 或非 `none` 容量 | 不得改为 allocation timeout | 进入对应终态/清理流程 | 保留资源隔离 | 进入 Effect/Reservation 对账 | 由 Runtime/Recovery 处理，不能释放未知资源 |

### 5.2 START_UNCERTAIN 截止

| Provider 事实 | Command | Record | Effect/Reservation | 后续 |
| --- | --- | --- | --- |
| 确认无 Room、Participant、Egress 等资源 | `DEAD` | `failed`，`failure_stage=startup_reconcile` | 已确认失败/不存在；线路释放 | Owner、容量和本地 handle 收口 |
| 任一资源存在 | 由终态屏障抢占为 `SUPERSEDED`，创建/读取唯一 `END_CALL` | `ending`，等待清理 | 创建完整销毁图；线路保持占用直到终态确认 | 只能走 Effect 对账和 cleanup Owner |
| Provider 查询仍不确定 | `RETRY_WAIT` 或终止后 `attention_required` | `ending` | Effect 保持 `RECONCILE_REQUIRED`；不释放线路 | 停放并按 `resource_cleanup_next_retry_at` 恢复 |

### 5.3 Effect 与 Reservation

- CREATE Effect 的 `ACCEPTED`/明确资源存在结果使 Reservation 从 `RESERVED` 进入 `ACTIVE`。
- 明确无资源/永久失败释放 Reservation；结果不确定进入 `RECONCILE_REQUIRED` 并继续计数。
- HANGUP/销毁 Effect 在提交 `APPLIED` 后释放 Reservation；迟到或旧 token 不能回退到未释放前状态。
- `resource_cleanup_status=clean` 只有在全部创建静默、全部销毁通过保护截止确认、资源不存在且 Reservation 已释放时成立。

## 6. 事务与锁顺序

### 6.1 首次分配

Dispatcher 先在事务外读取候选 Worker/Record，进入短事务后必须按以下顺序重新锁定和复核：

1. `Record`；
2. 指定 SIP `Line`（有线路参数时）；
3. 候选 `Worker`；
4. `START_CALL Command`；
5. 创建 `Reservation` 并更新 Owner/fencing、Worker 计数和命令目标。

无 Worker、无线路槽、终态屏障、已有资源或命令状态不再满足时，事务返回无变更。

### 6.2 Recovery 接管与 Effect 提交

Recovery 接管先锁 Record，再锁 Worker、Effect/Reservation 和需要更新的 Command；Effect 提交先校验 Record 当前 Owner/fencing，再锁 Effect 自身并匹配 processing token。旧 token 影响 0 行。任何事务异常都回滚 Owner、容量、Reservation、Effect 和命令结果，不能留下单侧占用。

### 6.3 数据库时间与提交结果

- 所有租约、deadline、重试时间和静默保护时间读取 PostgreSQL 时间。
- 事务提交后响应丢失时，调用方只允许按租户/幂等键重读并返回已提交事实。
- 不使用本机 watchdog 判断数据库事务是否成功；本地硬截止只负责停止 Provider/媒体调用，最终状态由数据库恢复扫描决定。

## 7. 故障注入矩阵

| 场景 | 注入点 | 必须观察的事实 |
| --- | --- | --- |
| 两个 Dispatcher 争抢最后一个 Worker/SIP 槽 | `SELECT ... FOR UPDATE`/Reservation 插入 | 一个 Owner、一个 Reservation、Worker 计数只增加一次 |
| 两个 Recovery 争抢过期 Owner | Record/Worker 锁竞争 | 一个新 fencing token；旧 token 提交影响 0 行 |
| Owner 在 CREATE 前失联 | Command lease/Record lease 过期 | 无 Effect 时可重领；不产生重复 Provider 调用 |
| Owner 在 CREATE 后失联 | Effect 已登记且状态不确定 | 新 Owner/cleanup Owner 继续 Effect 对账；不按 allocation timeout 释放 |
| Effect processing lease 过期 | Effect CAS 提交前注入延迟 | 只有新 token 能提交，旧 token 影响 0 行 |
| Reservation 写入异常 | Reservation INSERT/flush 失败 | Owner、Worker 计数和 Record 事务全部回滚 |
| Command 提交后响应丢失 | `after_commit` 注入连接错误 | 重试命中原命令，命令序号和副作用数量不变 |
| allocation deadline 到期 | 数据库时间推进 | 无任何资源时 `DEAD/failed/ALLOCATION_TIMEOUT`；不创建 END |
| START_UNCERTAIN deadline 三分支 | Stub 查询返回 no-resource/present/unknown | 分别进入失败、END 清理、重试/attention，不能无限停留 |

## 8. 验收与证据

必须在隔离 PostgreSQL 16 容器中验证并保留输出：

- `tests/postgres/test_ai_call_runtime_control_postgres.py`：并发分配、Reservation 生命周期、Recovery 接管、START_UNCERTAIN、allocation deadline、事务回滚和提交响应丢失。
- `tests/test_ai_call_runtime_command_repository.py`：幂等、命令序号、终态屏障和旧 token 拒绝。
- `tests/test_ai_call_runtime_owner_repository.py`：Owner/容量/租约转换。
- `tests/test_ai_call_runtime_effect_repository.py`：Effect 独立租约、fencing 和资源状态转换。
- `tests/test_ai_call_runtime_lifecycle.py`、`tests/test_ai_call_runtime_startup_recovery.py`、`tests/test_ai_call_runtime_stub_handlers.py`：生命周期、三分支决议和 Provider Stub。

完成门槛：

1. PostgreSQL 测试、Runtime 单测、ruff 和 `git diff --check` 全部通过。
2. 不连接 Redis、LiveKit、SIP、Egress 或真实 Provider，不拨号，不启动/重启业务服务。
3. 两个独立 Runtime/Dispatcher 实例只共享同一 PostgreSQL；最终 Owner、fencing、Effect、Reservation 和 Record 事实符合本文件矩阵。
4. 任何失败注入都能由数据库扫描恢复，且没有双执行、容量泄漏、未知资源提前释放或终态屏障重开。

## 9. 进入下一切片的门槛

只有本文件的 DB-only 故障注入矩阵全部通过，才允许创建 16.2C 计划。16.2C 只能增加 Redis 加速和发布恢复，不得改变本文件定义的持久状态、CAS、锁顺序、Provider Stub 调用次数或终态结果。
