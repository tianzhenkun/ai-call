# AI Call 16.4A Outbound DB-only 入口实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:executing-plans 逐任务实现此计划；每个任务坚持红灯测试、最小实现、绿灯回归。步骤使用复选框（`- [ ]`）跟踪。

**目标：** 将正式 Outbound Task/Target/Attempt 以原子事务接入 PostgreSQL `START_CALL`，用两个 Dispatcher/Runtime 和确定性 Provider Stub 验证 `QUEUED -> STARTING -> DIALING` 的单执行闭环，不连接真实 SIP、LiveKit、Provider 或 Redis。

**架构：** Outbound 角色只认领 Task/Target，并在同一事务创建 Attempt、Record 和 `START_CALL`；Dispatcher 按 `Task -> Target -> Attempt -> Record -> SIP Line -> Worker -> Command -> Reservation` 锁序分配 Owner 和线路槽；Runtime 只执行已获得 Owner/fencing 的 Stub Effect；独立 Attempt Reconciler 使用自己的 token/租约，把持久 Runtime 事实投影回 Attempt/Target/Task。旧 `legacy_local -> SipOutboundDialer.dial()` 保持不变。

**技术栈：** Python 3.12、FastAPI、SQLAlchemy Async、PostgreSQL 16、pytest、ruff、CodeGraph。

---

## 范围与停止规则

本计划只覆盖冻结总设计第 7.1 节的最小 DB-only Outbound 垂直闭环：

- 同租户 Task、Target、Attempt、Record、Command 原子创建；
- Attempt 创建态为 `QUEUED`，不得伪报 `DIALING`；
- Dispatcher 双资源分配后才进入 `STARTING`；
- Provider Stub 创建 Room、Agent Participant、SIP Participant 后，Reconciler 才投影为 `DIALING`；
- 双 Outbound、双 Dispatcher、双 Runtime、双 Reconciler 竞争只产生一套事实；
- `outbound` 未启用时，旧 mock/SIP 执行器行为和回归不变。

明确不在本计划内：

- 真实 LiveKit、SIP、Linphone、Provider、号码拨打和业务服务启动；
- Handoff、Agent Presence、浏览器坐席和实时媒体；
- 录音、ASR、语义分析、跟进；
- Redis Streams；
- 多租户/多线路公平轮转与批量队列背压压测；这些作为 16.4B 的独立吞吐切片，不阻止本计划证明单任务正确性。

## 文件职责

- `app/api/v1/ai_call/outbound/rule_task_model.py`：补齐 Attempt 独立投影租约 ORM 字段和索引，与既有 PostgreSQL migration 对齐。
- `app/api/v1/ai_call/outbound/owner_runtime_start.py`：封装 Outbound Attempt、Record、`START_CALL` 的同事务创建，不执行 Provider。
- `app/api/v1/ai_call/outbound/task_executor.py`：在 `outbound` owner 模式下调用持久化 starter，提交后不调用旧同步 `dial()`。
- `app/services/ai_call/runtime_control/owner_repository.py`：按完整锁序分配 Outbound Owner、线路 Reservation，并将 Attempt `QUEUED -> STARTING`。
- `app/services/ai_call/runtime_control/runtime_service.py`：让 `outbound` 使用与 Direct SIP 相同的 DB-only 创建 Effect 图。
- `app/services/ai_call/runtime_control/start_readiness_repository.py`：允许 `outbound` 持久化 Stub readiness。
- `app/api/v1/ai_call/outbound/attempt_reconciler.py`：用独立 token/租约把 Runtime 持久事实投影为 `DIALING`，不调用 Provider。
- `app/plugin/init_app.py`：仅在 `outbound` owner entry 开启时装配新 starter/reconciler；旧模式继续装配原 Dialer。
- `tests/test_ai_call_outbound_owner_runtime.py`：SQLite 单元合同、旧路径隔离和错误分支。
- `tests/test_ai_call_runtime_owner_repository.py`：Outbound 完整锁序、Attempt/Reservation 原子分配和旧 token 保护。
- `tests/postgres/test_ai_call_runtime_control_postgres.py`：两个实例的 PostgreSQL 全闭环竞争验收。

### 任务 1：补齐 Attempt 投影租约 ORM 合同

**文件：**

- 修改：`app/api/v1/ai_call/outbound/rule_task_model.py`
- 修改：`tests/test_ai_call_runtime_models.py`
- 验证：`docs/livekit-ai-outbound/sql/phase-i1-owner-command-db-control-plane.sql`

- [ ] **步骤 1：运行现有失败测试**

运行：

```bash
uv run pytest -q tests/test_ai_call_runtime_models.py::test_outbound_attempt_has_independent_projection_lease_fields
```

预期：FAIL，指出 `AiCallOutboundAttemptModel` 缺少 `reconcile_owner_id` 等字段；这证明 migration 与 ORM 当前不一致。

- [ ] **步骤 2：补 migration 文本断言**

在测试中读取 `phase-i1-owner-command-db-control-plane.sql`，明确断言五个字段和两个索引都存在：

```python
for fragment in (
    "reconcile_owner_id varchar(128)",
    "reconcile_token varchar(128)",
    "reconcile_expires_at timestamptz",
    "reconcile_after timestamptz",
    "reconcile_attempt_count integer not null default 0",
    "idx_outbound_attempt_reconcile",
    "idx_outbound_attempt_reconcile_lease",
):
    assert fragment in migration
```

- [ ] **步骤 3：实现最小 ORM 对齐**

在 Attempt 模型增加：

```python
reconcile_owner_id: Mapped[str | None] = mapped_column(String(128))
reconcile_token: Mapped[str | None] = mapped_column(String(128))
reconcile_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
reconcile_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
reconcile_attempt_count: Mapped[int] = mapped_column(
    Integer, nullable=False, default=0, server_default="0"
)
```

并增加 `(status, reconcile_after)`、`(reconcile_expires_at)` 索引。不新建重复 migration。

- [ ] **步骤 4：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_models.py
uv run ruff check app/api/v1/ai_call/outbound/rule_task_model.py tests/test_ai_call_runtime_models.py
git add app/api/v1/ai_call/outbound/rule_task_model.py tests/test_ai_call_runtime_models.py
git commit -m "fix(ai-call): 对齐外呼 Attempt 投影租约模型"
```

### 任务 2：原子创建 Outbound Attempt、Record 和 START_CALL

**文件：**

- 创建：`app/api/v1/ai_call/outbound/owner_runtime_start.py`
- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 创建：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

构造一条 `RUNNING` Task、一个 `PENDING` Target 和线路快照，启用 `AI_CALL_OWNER_COMMAND_V1_ENTRIES=outbound`，断言一次认领在同一提交中产生：

```python
assert attempt.status == "QUEUED"
assert attempt.dialer_type == "owner_runtime"
assert attempt.call_id == record.call_id == command.call_id
assert record.entry_type == "outbound"
assert record.runtime_control_mode == "owner_command_v1"
assert command.command_type == "START_CALL"
assert json.loads(command.payload_json) == {
    "attempt_id": str(attempt.id),
    "attempt_no": attempt.attempt_no,
    "line_code": task.sip_line_code_snapshot,
    "line_id": str(task.sip_line_id_snapshot),
    "prompt_profile_id": task.prompt_profile_id,
    "scene_code": task.scene_code,
    "target_id": str(target.id),
    "task_id": str(task.id),
    "voice": task.voice,
}
assert target.phone_number not in command.payload_json
```

并断言在 Record/Command flush 失败时 Task、Target、Attempt 全部回滚；两个执行器竞争只创建一条 Attempt/Record/Command。

- [ ] **步骤 2：实现持久 starter**

新增 `OwnerRuntimeOutboundStart`，只接受现有事务中的 `AsyncSession`、权威 Task/Target 和 `attempt_no`。它预生成 `attempt_id`，以稳定键：

```python
idempotency_key = (
    f"outbound:{task.tenant_id}:{task.id}:{target.id}:{attempt_no}"
)
```

调用同 session 的 `RuntimeCommandRepository.create_start_call()`，再用返回的 `call_id` 插入 `AiCallOutboundAttemptModel(status="QUEUED")`。完整号码只留在 Target 权威列，不进入 Command、日志或 Provider Stub。

- [ ] **步骤 3：接入 TaskExecutor，但不调用 dial**

给 `OutboundTaskExecutor` 增加可选 `owner_runtime_start`。启用时：

```python
claimed = await self._claim_target(...)
if self.owner_runtime_start is None:
    await self.execute_claimed(claimed)
```

owner 模式保留旧 Task 调度与 Target 条件认领，但由 starter 创建 Attempt/Record/Command；提交后立即返回，不调用 `dial()`、`terminate()` 或任何外部系统。未启用时旧逻辑逐字节行为不变。

- [ ] **步骤 4：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py tests/test_ai_call_outbound_task_executor.py
uv run ruff check app/api/v1/ai_call/outbound/owner_runtime_start.py app/api/v1/ai_call/outbound/task_executor.py tests/test_ai_call_outbound_owner_runtime.py
git add app/api/v1/ai_call/outbound/owner_runtime_start.py app/api/v1/ai_call/outbound/task_executor.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 原子创建外呼 START_CALL"
```

### 任务 3：Dispatcher 按完整锁序分配 Attempt、Owner 和线路槽

**文件：**

- 修改：`app/services/ai_call/runtime_control/owner_repository.py`
- 修改：`tests/test_ai_call_runtime_owner_repository.py`
- 修改：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

测试 Outbound 命令 payload 中的 Task/Target/Attempt 引用。两个 Dispatcher 竞争同一 call 时断言：

```python
assert attempt.status == "STARTING"
assert reservation.attempt_id == attempt.id
assert reservation.status == "RESERVED"
assert record.runtime_owner_id == worker.worker_id
assert worker.active_call_count == 1
```

增加 fail-closed 场景：Attempt 缺失、租户不一致、call_id 不匹配、状态不是 `QUEUED`、命令锁后 payload 引用变化，均不得分配 Owner、不得占 Worker/线路、不得推进 Attempt。

- [ ] **步骤 2：实现 Outbound 引用解析器**

只接受规范 payload：

```python
@dataclass(frozen=True, slots=True)
class OutboundStartRefs:
    task_id: int
    target_id: int
    attempt_id: int
    line_id: int
```

缺字段、布尔值冒充整数、非正数或层级错误返回无效引用并 fail closed。

- [ ] **步骤 3：实现唯一锁序和原子推进**

`assign_initial_owner()` 对 Outbound 先无锁读取 Command 引用作为候选，再在最终事务中依次锁：

```text
Task -> Target -> Attempt -> Record -> SIP Line -> Worker -> Command
```

锁住 Command 后重新解析并与候选引用比较；最后一次验证全部通过时，才在同一 flush 中写 Owner、Worker 容量、`Reservation(attempt_id=...)` 和 `Attempt QUEUED -> STARTING`。非 Outbound 沿用现有 Record 起始锁序。

- [ ] **步骤 4：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_outbound_owner_runtime.py
uv run ruff check app/services/ai_call/runtime_control/owner_repository.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_outbound_owner_runtime.py
git add app/services/ai_call/runtime_control/owner_repository.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 原子分配外呼 Owner 与线路槽"
```

### 任务 4：让 Outbound Runtime Stub 完成 START_CALL

**文件：**

- 修改：`app/services/ai_call/runtime_control/runtime_service.py`
- 修改：`app/services/ai_call/runtime_control/start_readiness_repository.py`
- 修改：`tests/test_ai_call_runtime_stub_handlers.py`
- 修改：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

断言 `outbound` 生成三项创建 Effect：

```python
assert [spec.effect_type for spec in specs] == [
    "CREATE_ROOM",
    "ATTACH_AGENT_PARTICIPANT",
    "CREATE_SIP_PARTICIPANT",
]
```

缺少任一 Effect、Owner 失效或 fencing 变化都不能提交 readiness；Provider Stub 的 `calls` 只能包含 namespace、effect type、resource key，不能包含 Task 电话号码。

- [ ] **步骤 2：实现最小 entry 扩展**

让 `_default_start_specs()` 接受 `outbound`，并与 `direct_sip` 一样增加 `CREATE_SIP_PARTICIPANT`；让 `RuntimeStartReadinessRepository` 接受 `outbound` Record。不得新建 Provider 类型，不得增加网络参数。

- [ ] **步骤 3：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_outbound_owner_runtime.py
uv run ruff check app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/start_readiness_repository.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_outbound_owner_runtime.py
git add app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/start_readiness_repository.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 执行外呼 DB-only START_CALL"
```

### 任务 5：独立 Reconciler 投影 QUEUED、STARTING 和 DIALING

**文件：**

- 创建：`app/api/v1/ai_call/outbound/attempt_reconciler.py`
- 修改：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

覆盖：

- 无 Owner/Reservation 时保持 `QUEUED`；
- Reservation `RESERVED` 且 Owner 有效时投影 `STARTING`；
- `CREATE_SIP_PARTICIPANT APPLIED`、Reservation `ACTIVE`、`START_CALL SUCCEEDED`、Record `ready` 同时存在时投影 `DIALING`；
- 任一事实缺失时不得提前进入 `DIALING`；
- 两个 Reconciler 只允许一个 token 获胜；旧 token、过期租约提交影响 0 行；
- Reconciler 不持有 Runtime Owner、不调用 Provider。

- [ ] **步骤 2：实现短事务认领**

`claim_next()` 只锁 Attempt，通过数据库时间写：

```python
reconcile_owner_id = worker_id
reconcile_token = token
reconcile_expires_at = now + lease_ttl
reconcile_attempt_count = reconcile_attempt_count + 1
```

立即提交，不在持有 `SKIP LOCKED` Attempt 时回头锁上游行。

- [ ] **步骤 3：实现结果提交锁序**

新事务按 `Task -> Target -> Attempt -> Record -> Command -> Reservation -> Effect` 锁定需要验证的行，重新校验 token 和未过期租约，再单调投影 Attempt；Target 保持现有活动态，不把 `QUEUED` 展示成通话已接通。投影完成清空 token/owner/lease，未满足条件写有限 `reconcile_after`。

- [ ] **步骤 4：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_outbound_owner_runtime.py
uv run ruff check app/api/v1/ai_call/outbound/attempt_reconciler.py tests/test_ai_call_outbound_owner_runtime.py
git add app/api/v1/ai_call/outbound/attempt_reconciler.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 投影外呼 Runtime 启动状态"
```

### 任务 6：进程角色装配与旧路径隔离

**文件：**

- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_process_roles.py`
- 修改：`tests/test_ai_call_outbound_owner_runtime.py`

- [ ] **步骤 1：编写失败测试**

断言：

- `outbound` owner entry 要求 `outbound` 角色；
- owner 模式装配 starter 和 Reconciler，不实例化 `SipOutboundDialer`；
- 未启用 owner entry 时继续使用现有 mock/sip Dialer；
- API、Dispatcher、Runtime 角色不能直接推进 Attempt 投影，只有 Outbound Reconciler 可以。

- [ ] **步骤 2：实现生命周期装配**

在 `_start_ai_call_outbound_task_worker()` 根据 `runtime_control_mode_for_entry(settings, OwnerCommandEntry.OUTBOUND)` 选择 owner starter；在同一 Outbound worker 生命周期中启动/停止 Attempt Reconciler。默认开关仍为关闭，不启动业务服务做验证。

- [ ] **步骤 3：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_process_roles.py tests/test_ai_call_outbound_owner_runtime.py tests/test_ai_call_outbound_task_executor.py
uv run ruff check app/plugin/init_app.py tests/test_ai_call_process_roles.py tests/test_ai_call_outbound_owner_runtime.py
git add app/plugin/init_app.py tests/test_ai_call_process_roles.py tests/test_ai_call_outbound_owner_runtime.py
git commit -m "feat(ai-call): 隔离外呼 Owner Runtime 进程角色"
```

### 任务 7：隔离 PostgreSQL 双实例闭环与最终验证

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 可能修改：上述实现与测试文件，仅限修复本切片暴露的问题

- [ ] **步骤 1：增加 PostgreSQL 全闭环测试**

同一条 Task/Target 同时运行两个 Outbound Executor、两个 Dispatcher、两个 Runtime、两个 Reconciler，断言：

```python
assert counts == {
    "attempt": 1,
    "record": 1,
    "start_command": 1,
    "reservation": 1,
}
assert attempt.status == "DIALING"
assert reservation.attempt_id == attempt.id
assert reservation.status == "ACTIVE"
assert record.status == "ready"
assert len(provider_a.calls + provider_b.calls) == 3
```

同时断言无电话号码进入 Command、Effect、Provider calls 或错误字段，旧路径未创建第二条 Record。

- [ ] **步骤 2：运行目标单元与隔离 PostgreSQL 测试**

```bash
uv run pytest -q \
  tests/test_ai_call_runtime_models.py \
  tests/test_ai_call_outbound_owner_runtime.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_process_roles.py

tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py
```

预期：全部 PASS；仅连接脚本启动的隔离 PostgreSQL 16。

- [ ] **步骤 3：运行静态和改动范围验证**

```bash
codegraph sync
uv run ruff check .
git diff --check
git status --short
git diff --stat 5a13f8d..HEAD
git diff --cached --check
```

确认未连接 Redis、LiveKit、SIP、Egress、Linphone 或真实 Provider；未拨号、未启动/重启业务服务；原 ed81/a3cd 脏 worktree 未变化。

- [ ] **步骤 4：提交 PostgreSQL 验收证据**

```bash
git add tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "test(ai-call): 验证外呼 DB-only 双实例闭环"
```

## 完成门禁

只有同时满足以下条件，才可声明 16.4A 完成：

1. Task/Target/Attempt/Record/`START_CALL` 同事务创建，失败整体回滚；
2. Attempt 在未分配资源前为 `QUEUED`，Dispatcher 原子分配后才为 `STARTING`；
3. `DIALING` 只由独立 Reconciler 根据完整持久事实投影；
4. 两套 Outbound/Dispatcher/Runtime/Reconciler 只产生一套事实和三次创建 Stub Effect；
5. 旧 token、旧租约、缺失/跨租户引用都 fail closed；
6. 旧 mock/SIP `legacy_local` 回归不变；
7. 没有真实外部调用，也没有 Handoff/Presence/Redis 扩张；
8. 单元、隔离 PostgreSQL、ruff、CodeGraph、`git diff --check` 全通过；
9. 每个提交只包含本计划文件，原 worktree 脏改动完全保留。

完成本计划只证明 16.4A 单任务 DB-only 正确性；16.4B 再处理批量背压、公平调度、终态/重试投影和故障注入，之后才能进入 16.5 转人工与媒体生命周期。
