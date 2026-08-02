# AI Call 16.4B 外呼背压、公平调度与恢复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:executing-plans 逐任务实现此计划；每个行为先写失败测试并确认红灯，再写最少实现并确认绿灯。步骤使用复选框（`- [ ]`）跟踪。

**目标：** 在 16.4A 已完成的 Outbound DB-only 单任务闭环上，补齐按租户、Task、线路的有界 `QUEUED` 背压、Dispatcher 公平候选、Attempt 终态/重试投影和双实例故障恢复，使 16.4 具备进入 16.5 的条件。

**架构：** Outbound Executor 在创建 Attempt 前，于同一事务获取 PostgreSQL 租户/线路事务锁并重算排队快照；达到任一上限就停止认领 Target。Dispatcher 通过数据库窗口排序让每个 `(tenant_id, line_id)` lane 在同一批次先获得一个候选机会，最终执行权仍由 Owner/fencing/Reservation 事务授予。Outbound Attempt Reconciler 继续使用独立 token/租约，只根据 Record、Command、Effect、Reservation 和媒体证据投影 Attempt/Target/Task，不调用 Provider。

**技术栈：** Python 3.12、SQLAlchemy Async、PostgreSQL 16、pytest、ruff、CodeGraph。

---

## 边界与合同

纳入：

1. `outbound` Owner Runtime 模式下，未分配 Attempt 的租户、Task、线路三层硬上限；
2. PostgreSQL 事务锁保证两个 Outbound Executor 并发时不突破上限；
3. 排队快照包含策略上限、三层当前数量、最长等待秒数和累计 `ALLOCATION_TIMEOUT` 数；
4. Dispatcher 候选按租户/线路 lane 轮转，同一 lane 的第二条不能排在其他 lane 第一条之前；
5. `ALLOCATION_TIMEOUT`、明确启动失败和已结束 Record 向 Attempt/Target/Task 的单调投影；
6. Reconciler 旧 token、过期租约、进程重启和重复认领 fail closed；
7. 双 Executor、双 Dispatcher、双 Reconciler 共用单一 PostgreSQL 的并发与故障测试。

明确排除：

- Redis Streams、Consumer Group 和 Pending 恢复；
- Handoff、Agent Presence、浏览器坐席、真实媒体和 16.5 生命周期；
- 真实 LiveKit、SIP、Linphone、Provider 或号码拨打；
- 录音、ASR、语义分析和话后跟进；
- 正式环境入口放量。

## 文件职责

- 创建 `app/api/v1/ai_call/outbound/queue_control.py`：三层排队上限、PostgreSQL 事务锁、容量快照和排队指标。
- 修改 `app/api/v1/ai_call/outbound/task_executor.py`：Owner Runtime 创建 Attempt 前调用背压门禁；旧 `legacy_local` 路径不变。
- 修改 `app/services/ai_call/runtime_control/dispatcher_service.py`：查询候选时按租户/线路 lane 公平排序；通知仍只负责唤醒。
- 创建 `app/api/v1/ai_call/outbound/attempt_projection.py`：共用重试策略、终态结果和 Task 计数投影。
- 修改 `app/api/v1/ai_call/outbound/attempt_reconciler.py`：覆盖 `DIALING/IN_CALL` 和终态 Record，保持独立租约与全局锁序。
- 修改 `app/config/setting.py`、`app/plugin/init_app.py`：显式注入默认上限与 Outbound 分配截止；正式入口默认仍关闭。
- 修改 `tests/test_ai_call_outbound_owner_runtime.py`：背压、lane 排序、终态/重试与旧 token 单测。
- 修改 `tests/test_ai_call_outbound_task_executor.py`、`tests/test_ai_call_process_roles.py`：设置装配和旧链路回归。
- 修改 `tests/postgres/test_ai_call_runtime_control_postgres.py`：双实例上限、公平、超时和接管故障注入。

### 任务 1：外呼创建端三层背压

**文件：**

- 创建：`app/api/v1/ai_call/outbound/queue_control.py`
- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 测试：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

覆盖以下行为：

```python
async def test_owner_runtime_stops_claiming_when_task_queue_limit_is_reached(): ...
async def test_owner_runtime_stops_claiming_when_tenant_queue_limit_is_reached(): ...
async def test_owner_runtime_stops_claiming_when_line_queue_limit_is_reached(): ...
async def test_queue_snapshot_reports_counts_oldest_wait_and_timeouts(): ...
```

断言达到任一上限时不更新下一个 Target、不创建第二条 Attempt/Record/Command，现有 Target 保持 `PENDING`；未达到上限时正常创建。

- [ ] **步骤 2：运行测试并确认红灯**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py -k 'queue_limit or queue_snapshot'
```

预期：因 `OutboundQueueLimits`、`OutboundQueueRepository` 或构造参数尚不存在而失败。

- [ ] **步骤 3：实现最少背压代码**

`queue_control.py` 提供不可变配置和快照：

```python
@dataclass(frozen=True, slots=True)
class OutboundQueueLimits:
    per_tenant: int
    per_task: int
    per_line: int

@dataclass(frozen=True, slots=True)
class OutboundQueueSnapshot:
    tenant_queued: int
    task_queued: int
    line_queued: int
    oldest_wait_seconds: float
    allocation_timeout_count: int
    limits: OutboundQueueLimits

    @property
    def has_capacity(self) -> bool: ...
```

Repository 在 PostgreSQL 中按固定顺序取得 `tenant -> line` transaction advisory locks，再在同一事务统计 `dialer_type='owner_runtime' AND status='QUEUED'`。SQLite 单元环境跳过 advisory lock，但使用同一计数合同。

`OutboundTaskExecutor._claim_target()` 只在 `owner_runtime_start` 非空时调用；快照无容量则返回，不改变 Target。创建 Attempt 与计数检查保持同一事务。

- [ ] **步骤 4：运行目标测试并确认绿灯**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py -k 'queue_limit or queue_snapshot'
```

预期：全部通过。

- [ ] **步骤 5：提交**

```bash
git add app/api/v1/ai_call/outbound/queue_control.py app/api/v1/ai_call/outbound/task_executor.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 限制外呼待分配队列"
```

### 任务 2：Dispatcher 租户/线路公平候选

**文件：**

- 修改：`app/services/ai_call/runtime_control/dispatcher_service.py`
- 测试：`tests/test_ai_call_outbound_owner_runtime.py`
- 测试：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写失败测试**

```python
async def test_dispatcher_orders_first_candidate_from_each_tenant_line_lane_first(): ...
async def test_two_dispatchers_do_not_starve_small_tenant_behind_large_task(): ...
```

构造租户 A/线路 A 的多条旧命令以及租户 B/线路 B 的一条较新命令，`batch_size` 足以覆盖 lane 数；断言一批候选先出现 A1、B1，再出现 A2，而不是完全按全局 `started_at` 返回 A1、A2、A3。

- [ ] **步骤 2：运行测试并确认红灯**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py -k 'dispatcher and lane'
```

预期：当前全局 `started_at` 排序导致小租户 lane 未进入首轮。

- [ ] **步骤 3：实现窗口排序**

在 `DispatcherControlService` 的只读候选事务中关联 `START_CALL` 和 Outbound Attempt，使用：

```text
row_number() over (
  partition by tenant_id, coalesce(line_id, entry_type)
  order by record.started_at, record.id
)
```

外层按 `lane_rank, started_at, record_id` 排序并限制 `batch_size`。这只决定尝试顺序；`assign_initial_owner()` 仍执行 Task -> Target -> Attempt -> Record -> Line -> Worker -> Command -> Reservation 的锁和 CAS。

- [ ] **步骤 4：运行单测和 PostgreSQL 公平测试**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py -k 'dispatcher and lane'
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'outbound and fair'
```

预期：全部通过，两个 Dispatcher 不产生双 Owner/双 Reservation。

- [ ] **步骤 5：提交**

```bash
git add app/services/ai_call/runtime_control/dispatcher_service.py tests/test_ai_call_outbound_owner_runtime.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "feat(ai-call): 公平轮转外呼分配候选"
```

### 任务 3：Attempt 终态与重试投影

**文件：**

- 创建：`app/api/v1/ai_call/outbound/attempt_projection.py`
- 修改：`app/api/v1/ai_call/outbound/attempt_reconciler.py`
- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 测试：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

```python
async def test_reconciler_projects_allocation_timeout_to_failed_attempt_and_retry_wait(): ...
async def test_reconciler_projects_terminal_record_once_and_refreshes_task_counters(): ...
async def test_expired_reconcile_token_cannot_overwrite_new_terminal_projection(): ...
```

断言：

- `Record failed + START_CALL DEAD + ALLOCATION_TIMEOUT` 使 Attempt 进入 `FAILED`；
- 命中冻结规则的 `call_failed` 重试时 Target 进入 `RETRY_WAIT`，否则进入 `COMPLETED`；
- 成功接通必须同时有 `answered_at` 和持久 `media_connected` 证据；
- Task 计数和终态由同一投影事务刷新；
- 旧 token/过期租约提交影响 0 行，不重开终态。

- [ ] **步骤 2：运行测试并确认红灯**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py -k 'terminal_projection or allocation_timeout or expired_reconcile'
```

预期：当前 Reconciler 只认领 `QUEUED/STARTING` 且只投影到 `DIALING`，测试失败。

- [ ] **步骤 3：实现最少终态投影**

提取现有 retry interval 和 Task counter 计算为共用函数；Reconciler 认领 `QUEUED/STARTING/DIALING/IN_CALL`，保持短认领事务，提交仍按 `Task -> Target -> Attempt -> Record -> Command -> Reservation -> Effect` 锁序并重新校验 token/租约。

终态映射：

```text
Record failed -> Attempt FAILED, call_result=call_failed
Record completed + answered_at + media_connected -> Attempt COMPLETED, call_result=connected
Record completed + busy/no-answer end_reason -> Attempt FAILED, call_result=busy/no_answer
其他已结束 Record -> Attempt FAILED, call_result=call_failed
```

活动 Attempt 每次投影后写 `reconcile_after`，避免 `DIALING` 紧循环；终态清空 reconcile lease 且不再可认领。

- [ ] **步骤 4：运行测试并确认绿灯**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py tests/test_ai_call_outbound_task_executor.py
```

预期：新旧投影和 legacy 回归全部通过。

- [ ] **步骤 5：提交**

```bash
git add app/api/v1/ai_call/outbound/attempt_projection.py app/api/v1/ai_call/outbound/attempt_reconciler.py app/api/v1/ai_call/outbound/task_executor.py tests/test_ai_call_outbound_owner_runtime.py tests/test_ai_call_outbound_task_executor.py
git commit -m "feat(ai-call): 收口外呼终态与重试投影"
```

### 任务 4：配置装配与 PostgreSQL 双实例故障闭环

**文件：**

- 修改：`app/config/setting.py`
- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_process_roles.py`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写失败测试**

覆盖默认配置和装配：

```python
assert Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TENANT"].default == 100
assert Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TASK"].default == 20
assert Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_LINE"].default == 50
assert Settings.model_fields["AI_CALL_OUTBOUND_ALLOCATION_TIMEOUT_SECONDS"].default == 30.0
```

PostgreSQL 故障矩阵至少覆盖：

1. 两个 Executor 同时竞争最后一个租户/线路排队名额，最终只新增一条 Attempt/Record/Command；
2. 大租户旧队列与小租户新队列同时存在，单个 Dispatcher 批次给两个 lane 候选机会；
3. 分配截止后 Dispatcher 原子写 `DEAD/failed`，两个 Reconciler 只有一个投影 Attempt/Target/Task；
4. Reconciler 认领后进程“崩溃”，租约到期由第二实例接管，旧 token 的迟到提交失败；
5. 最终无双 Owner、双 Reservation、双 Effect，Worker 计数与 Record 重算一致。

- [ ] **步骤 2：运行测试并确认红灯**

```bash
uv run pytest -q tests/test_ai_call_process_roles.py -k outbound
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py -k 'outbound and (backpressure or fair or timeout or reconcile_takeover)'
```

- [ ] **步骤 3：装配配置**

仅在 Outbound Owner Runtime 模式下注入 `OutboundQueueLimits` 和 `allocation_timeout_seconds`。所有默认开关仍为关闭，legacy mock/SIP 不应用新队列门禁。

- [ ] **步骤 4：运行完整验证**

```bash
uv run pytest -q --disable-warnings \
  tests/test_ai_call_outbound_owner_runtime.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_runtime_start_readiness.py
tools/run_ai_call_runtime_postgres_tests.sh -q tests/postgres/test_ai_call_runtime_control_postgres.py
uv run ruff check .
codegraph sync
codegraph status
git diff --check
git diff --cached --check
```

预期：所有测试和静态检查通过；测试容器、网络和卷均被脚本清理；不产生任何真实外部调用。

- [ ] **步骤 5：提交**

```bash
git add app/config/setting.py app/plugin/init_app.py tests/test_ai_call_process_roles.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "test(ai-call): 验证外呼背压与恢复闭环"
```

## 完成门禁

只有同时满足以下条件，才可声明 16.4B 完成并允许进入 16.5：

1. 两个 Executor 并发后，三层 `QUEUED` 数量均不超过配置上限；
2. 背压只停止新 Target 认领，不修改已有 `allocation_deadline_at`，不制造孤儿 Record；
3. Dispatcher 每批先覆盖不同租户/线路 lane，执行权仍只由数据库 Owner/fencing/CAS 授予；
4. 排队超时使用 PostgreSQL 时间，Attempt/Target/Task 由独立 Reconciler 最终收口；
5. 终态投影需要持久 Record 和必要媒体事实，旧 token、过期租约和重复认领不能重开终态；
6. 进程重启和响应不确定场景可从 PostgreSQL 恢复，不产生双 Owner、双 Reservation 或双 Effect；
7. legacy mock/SIP 行为不变，正式入口集合仍为空；
8. 单元测试、隔离 PostgreSQL、ruff、CodeGraph 和 diff 检查全部通过；
9. 不连接 Redis、LiveKit、SIP、Linphone 或真实 Provider，不拨号、不启动业务服务。
