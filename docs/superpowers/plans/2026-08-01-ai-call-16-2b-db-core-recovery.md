# AI Call 16.2B-DB-Core 恢复与一致性实施计划

> **面向 AI 代理的工作者：** 必需子技能：使用 `superpowers:executing-plans` 逐任务执行；每个任务坚持红灯测试、最小实现、绿灯回归。当前工作树已有用户改动，只能修改本计划列出的文件，不自动暂存或提交无关文件。

**目标：** 在已完成的 16.2A 控制面之上，收口当前可交付的 DB-Core 恢复闭环：Record、Runtime Worker、Command、Effect、SIP Line 和 Reservation 在双实例、数据库时间、租约过期和提交响应丢失场景下保持单执行、资源不泄漏和终态可恢复。

**架构：** API/业务入口只持久化 Command；Dispatcher 负责首次 Owner 与双资源分配；Recovery 负责过期 Owner、cleanup 和 attention 接管；Runtime 只执行已授权 Owner；Effect 使用独立 processing token；所有事实位于同一 PostgreSQL `READ COMMITTED` 数据库。Handoff、Presence、Attempt、Outbound 和前端不在本计划内，后续集成必须另立切片并遵循总设计的完整锁序。

**技术栈：** Python 3.12、SQLAlchemy 2 AsyncIO、PostgreSQL 16、asyncpg、pytest/anyio、Docker Compose、ruff。

---

## 范围与现状基线

本计划只修改或新增以下 DB-Core 文件：

- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 修改：`app/services/ai_call/runtime_control/owner_repository.py`
- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改：`app/services/ai_call/runtime_control/startup_recovery.py`
- 修改：`app/services/ai_call/runtime_control/recovery_service.py`
- 修改：`app/services/ai_call/runtime_control/timing.py`（仅在测试证明现有接口不足时）
- 修改测试：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 修改测试：`tests/test_ai_call_runtime_effect_repository.py`
- 修改测试：`tests/test_ai_call_runtime_owner_repository.py`
- 修改测试：`tests/test_ai_call_runtime_lifecycle.py`
- 修改测试：`tests/test_ai_call_runtime_startup_recovery.py`

已经完成、只作为基线不重复实现的内容：

- `ded5203`：16.2A DB-only Command/Owner/Effect/END_CALL 控制面；
- `006f32a`、`49df27b`、`5c49e83`：Recovery 接管、attention 停放、START_UNCERTAIN；
- `f9c0845`、`1522ad4`、`80991e9`：Worker/SIP 双资源 Reservation、fencing 和事务回滚；
- `99a4a7a`、`a0afe57`：提交响应丢失与 allocation deadline；
- 已有隔离 PostgreSQL 33 passed、Runtime 单测 32 passed、ruff 和 `git diff --check` 通过。

明确不在本计划内：

- Handoff、Agent Presence、Task/Target/Attempt、Outbound、前端和真实业务入口；
- Redis Streams、Consumer Group、`DISPATCHING` 和 Pending 恢复；
- LiveKit、SIP、Egress、Linphone、真实 Provider 和真实电话；
- Schema 扩展、MySQL/跨库事务、录音、ASR、语义和跟进。

## 成功标准

1. CREATE SIP Effect 只有在 `APPLIED` 且 Provider reference、资源事实和必要依赖均可验证时，才将 Reservation 从 `RESERVED` 转为 `ACTIVE`。
2. `ACCEPTED`、超时、查询失败、重试失败和未知结果不得提前释放或激活线路；只有调用前失败或确认 `PERMANENT_NO_RESOURCE` 才能 `RELEASED`。
3. 所有最终租约/CAS 判断使用锁定相关行之后重新读取的 PostgreSQL `clock_timestamp()`。
4. 旧 Owner、旧 fencing、旧 Command/Effect token 的迟到提交影响 0 行；attention 停放不得清除新 Owner 或递减新 Worker 的计数。
5. 首次分配、Effect 登记、Effect 提交、Recovery 接管和 allocation timeout 在提交响应丢失后可按稳定幂等键重读，不产生第二个对象或第二次容量增量。
6. DB-Core 所有隔离 PostgreSQL 测试通过；不启动业务服务、不连接真实外部依赖。

---

### 任务 1：为 Reservation 结果矩阵补充红灯测试

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 修改：`tests/test_ai_call_runtime_effect_repository.py`

- [ ] **步骤 1：新增 ACCEPTED 不得 ACTIVE 的 PostgreSQL 测试**

在现有 `test_sip_reservation_follows_effect_lifecycle_and_rejects_stale_token` 附近增加场景：登记 `CREATE_SIP_PARTICIPANT` Effect，提交 `ProviderObservationKind.ACCEPTED`，断言 Effect 可记录受理事实但 Reservation 仍为 `RECONCILE_REQUIRED`，不能为 `ACTIVE`。

```python
assert await effects.submit(
    create_claim,
    ProviderObservation(
        kind=ProviderObservationKind.ACCEPTED,
        provider_reference=None,
    ),
)
assert await session.scalar(text("select status from ai_call_sip_line_reservation where call_id=:call_id").bindparams(call_id=start.call_id)) == "RECONCILE_REQUIRED"
```

- [ ] **步骤 2：运行红灯测试**

运行：

```bash
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k reservation_follows_effect_lifecycle -q
```

预期：当前实现因把 `ACCEPTED` 转为 `ACTIVE` 而失败。

- [ ] **步骤 3：增加明确无资源与永久失败边界测试**

断言 `PERMANENT_NO_RESOURCE` 且 Effect 进入 `FAILED` 时才释放 Reservation；`RETRYABLE_FAILURE`、`UNCERTAIN` 和没有 Provider reference 的受理结果均保持线路占用。

- [ ] **步骤 4：提交测试切片**

```bash
git add tests/postgres/test_ai_call_runtime_control_postgres.py tests/test_ai_call_runtime_effect_repository.py
git commit -m "test(ai-call): freeze db-core reservation result matrix"
```

### 任务 2：修正 Effect/Reservation 结果提交

**文件：**

- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 修改测试：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：实现最小结果映射**

将 CREATE SIP 的 Reservation 转换收敛为以下分支：

```python
if observation.kind is RESOURCE_PRESENT and effect.status is APPLIED:
    if not effect.provider_reference or not effect.resource_key or effect.resource_generation <= 0:
        return False
    reservation.status = "ACTIVE"
elif observation.kind is PERMANENT_NO_RESOURCE and effect.status is FAILED:
    reservation.status = "RELEASED"
elif observation.kind in {ACCEPTED, UNCERTAIN, RETRYABLE_FAILURE}:
    reservation.status = "RECONCILE_REQUIRED"
else:
    return False
```

`RELEASED` 不允许回退；Reservation 更新必须继续匹配 Effect processing token、Record 当前 Owner/fencing/租约和 `reservation_token`。

- [ ] **步骤 2：运行任务 1 的测试确认转绿**

```bash
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k reservation_follows_effect_lifecycle -q
```

预期：所有 Reservation 结果矩阵断言通过。

- [ ] **步骤 3：运行 Effect 单测和 diff 校验**

```bash
uv run pytest tests/test_ai_call_runtime_effect_repository.py -q
uv run ruff check app/services/ai_call/runtime_control/effect_repository.py tests/test_ai_call_runtime_effect_repository.py && git diff --check
```

- [ ] **步骤 4：提交实现切片**

```bash
git add app/services/ai_call/runtime_control/effect_repository.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "fix(ai-call): fence db-core sip reservation outcomes"
```

### 任务 3：锁后数据库时间与过期租约测试

**文件：**

- 修改：`app/services/ai_call/runtime_control/owner_repository.py`
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改：`app/services/ai_call/runtime_control/startup_recovery.py`
- 修改：`app/services/ai_call/runtime_control/recovery_service.py`
- 修改测试：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：编写跨租约锁等待红灯测试**

使用两个 PostgreSQL session：Session A 锁定 Record；Session B 在租约尚未到期时开始停放或 Effect 提交并等待；Session A 将租约推进到过去时间后提交；释放锁；断言 Session B 使用锁后的 `clock_timestamp()` 返回 `False`，不修改 Worker、Record、Effect 或 Reservation。

```sql
select * from ai_call_record where tenant_id = :tenant_id and call_id = :call_id for update;
update ai_call_record set runtime_lease_expires_at = clock_timestamp() - interval '1 second' where call_id = :call_id;
```

- [ ] **步骤 2：运行红灯测试**

```bash
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k lock_wait_crosses_lease_deadline -q
```

预期：至少一个当前使用锁前旧时间的路径失败，输出包含旧租约仍被接受或容量发生变化。

- [ ] **步骤 3：在每个锁定事务的最终 CAS 前重新读取数据库时间**

保留 `read_database_time()` 使用 `clock_timestamp()`；在锁定 Record、Worker、Command、Reservation、Effect 后重新调用它，最终条件更新统一使用该值。候选扫描时间只能做筛选，不能作为最终授权时间。

- [ ] **步骤 4：运行 Owner/Effect/Command 回归**

```bash
uv run pytest tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_command_repository.py -q
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k 'owner or effect or command or lock_wait' -q
```

- [ ] **步骤 5：提交时间语义切片**

```bash
git add app/services/ai_call/runtime_control/owner_repository.py app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/command_repository.py app/services/ai_call/runtime_control/startup_recovery.py app/services/ai_call/runtime_control/recovery_service.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "fix(ai-call): validate leases with post-lock db time"
```

### 任务 4：补齐 DB-Core 提交响应丢失矩阵

**文件：**

- 修改：`app/services/ai_call/runtime_control/owner_repository.py`
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改测试：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：为四类事务分别注入 after-commit 响应丢失**

覆盖首次双资源分配、Effect 登记、Effect 结果提交、Recovery 接管和 allocation timeout；每个测试在数据库提交后抛出 `ConnectionError`，再以稳定键重试。

```python
def fail_after_commit(_session) -> None:
    raise ConnectionError("injected committed response loss")

event.listen(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
try:
    with pytest.raises(ConnectionError, match="committed response loss"):
        await RuntimeCommandRepository(session).expire_unallocated_start(tenant_id, call_id)
finally:
    event.remove(AsyncSession.sync_session_class, "after_commit", fail_after_commit)
```

- [ ] **步骤 2：断言重复请求只重读原事实**

每个场景必须断言：命令、Effect、Reservation 数量不增加，Owner fencing 不再次递增，Worker active/cleanup 计数不重复增加，Resource key 和 Provider idempotency key 保持唯一。

- [ ] **步骤 3：运行 PostgreSQL 故障矩阵**

```bash
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k 'response_loss or rollback or idempotency or reservation' -q
```

- [ ] **步骤 4：提交故障注入切片**

```bash
git add app/services/ai_call/runtime_control/owner_repository.py app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/command_repository.py tests/postgres/test_ai_call_runtime_control_postgres.py
git commit -m "test(ai-call): cover db-core committed response loss"
```

### 任务 5：最终 DB-Core 验收与交接

**文件：**

- 不新增业务实现文件；只更新本计划勾选状态和验收记录。

- [ ] **步骤 1：运行完整 DB-Core 单元测试**

```bash
uv run pytest tests/test_ai_call_runtime_*.py -q
```

预期：所有 Runtime 单元测试通过，且没有真实外部依赖调用。

- [ ] **步骤 2：运行隔离 PostgreSQL 全量测试**

```bash
bash tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -q
```

预期：所有 DB-Core PostgreSQL 场景通过，关键 SQL 快照证明 Worker 计数、Reservation 数量、Effect 唯一性、命令游标和旧 token 影响行数符合合同。

- [ ] **步骤 3：运行静态校验**

```bash
uv run ruff check . && git diff --check
```

- [ ] **步骤 4：确认外部依赖门禁**

确认未连接 Redis、LiveKit、SIP、Egress 或真实 Provider，未拨号，未启动/重启业务服务；确认既有无关 dirty changes 未被暂存。

- [ ] **步骤 5：提交验收证据**

```bash
git add docs/superpowers/plans/2026-08-01-ai-call-16-2b-db-core-recovery.md
git commit -m "docs(ai-call): plan 16.2B db-core delivery"
```

完成本任务只表示 `16.2B-DB-Core` 可进入下一阶段审查，不表示完整 16.2B、16.2C 或真实业务入口已经完成。
