# Owner Runtime 混合主录音实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在 PostgreSQL Owner/Effect/fencing 控制下，为 `owner_command_v1` 接入唯一、可恢复、受控 fail-open 的 LiveKit 混合主录音，并复用独立 OSS 对账链路完成业务收口。

**架构：** `START_EGRESS / STOP_EGRESS` 是唯一外部动作事实，`RuntimeEffectRepository.submit()` 在同一事务内委托 Owner-aware Recording projector 更新业务投影。主录音 Effect 使用跨 Owner 稳定键；readiness 只等待 Room、Agent 和必要 SIP；STOP 终态后释放资源，OSS 对象由独立租户化 Reconciler 验证。

**技术栈：** Python 3.12、FastAPI、SQLAlchemy Async、PostgreSQL 16、LiveKit Egress Twirp、pytest、Ruff、CodeGraph。

---

## 文件结构

- 创建 `docs/livekit-ai-outbound/sql/phase-i5-owner-runtime-main-recording.sql`：租户、generation、唯一约束和索引 migration。
- 修改 `app/api/v1/ai_call/model.py`、`app/api/v1/ai_call/crud.py`：同步主录音数据合同和租户化读写。
- 修改 `app/services/ai_call/recording_service.py`、`app/api/v1/ai_call/service.py`、`app/api/v1/ai_call/controller.py`、`app/services/ai_call/event_persistence.py`：API、legacy 和独立对账传递租户。
- 创建 `app/services/ai_call/runtime_control/recording_repository.py`：单调投影 Egress observation。
- 修改 `app/services/ai_call/runtime_control/effect_repository.py`：强类型 observation 与原子投影。
- 修改 `app/services/ai_call/runtime_control/start_readiness_repository.py`、`startup_recovery.py`、`runtime_service.py`、`provider_stub.py`：稳定辅助 Effect 与 fail-open readiness。
- 修改 `app/services/ai_call/runtime_control/livekit_provider.py`、`app/services/ai_call/livekit_egress.py`：Start/Query/Stop 适配。
- 新增聚焦的 unit/PostgreSQL 测试；不继续把全部场景堆进一个测试文件。

### 任务 1：租户化主录音数据合同

**文件：**
- 创建：`docs/livekit-ai-outbound/sql/phase-i5-owner-runtime-main-recording.sql`
- 修改：`app/api/v1/ai_call/model.py`
- 修改：`app/api/v1/ai_call/crud.py`
- 修改：`app/services/ai_call/recording_service.py`
- 修改：`app/api/v1/ai_call/service.py`
- 修改：`app/api/v1/ai_call/controller.py`
- 修改：`app/services/ai_call/event_persistence.py`
- 创建：`tests/test_ai_call_owner_recording_contract.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写失败的数据合同测试**

```python
def test_main_recording_contract_is_tenant_scoped() -> None:
    table = AiCallRecordingModel.__table__
    assert table.c.tenant_id.nullable is False
    assert table.c.egress_generation.nullable is True
    assert ("tenant_id", "call_id") in _unique_column_sets(table)
    assert ("call_id",) not in _unique_column_sets(table)


def test_migration_fails_closed_before_tenant_backfill() -> None:
    migration = MIGRATION_PATH.read_text()
    assert "ai_call_recording_tenant_backfill_failed" in migration
    assert "alter column tenant_id set not null" in migration
```

另加跨租户同 call 查询为空、update 不影响另一租户的异步测试。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_owner_recording_contract.py tests/test_ai_call_phase_b1_records.py -k 'recording and tenant'
```

预期：字段、migration 或 tenant 参数缺失导致 FAIL；不得是收集错误。

- [ ] **步骤 3：实现 migration 与 ORM**

```sql
alter table ai_call_recording add column if not exists tenant_id varchar(20);
alter table ai_call_recording add column if not exists egress_generation bigint;
-- 先验证每行都能关联到唯一且 tenant 非空的 Record，否则 raise
update ai_call_recording recording
set tenant_id = record.tenant_id
from ai_call_record record
where record.call_id = recording.call_id and recording.tenant_id is null;
alter table ai_call_recording alter column tenant_id set not null;
```

删除旧 `call_id` 唯一约束，增加 `(tenant_id, call_id)` 唯一约束及租户化 due、egress、oss 索引；不创建物理外键或 JSONB。

- [ ] **步骤 4：把所有主录音读写改为显式租户**

Repository 的固定接口为：`create_recording` 必须接收 keyword-only 的 `tenant_id`、
`call_id`、`room_name`、`status`、`started_at` 和可选 `object_name`；`get_recording` 与
`update_recording` 必须同时接收 keyword-only 的 `tenant_id + call_id`；系统对账更新入口
`update_due_recording` 必须同时接收 keyword-only 的 `tenant_id + recording_id`，并返回
是否实际更新一行。

API controller 用 `_identity(auth)` 取租户；系统对账扫描可跨租户，但更新必须使用结果携带的 `tenant_id + id`。Legacy 调用先从已有 Record 取得明确 tenant，缺失时 fail closed，不写占位租户。

- [ ] **步骤 5：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_owner_recording_contract.py tests/test_ai_call_phase_b1_records.py
uv run ruff check app/api/v1/ai_call/model.py app/api/v1/ai_call/crud.py app/services/ai_call/recording_service.py app/api/v1/ai_call/service.py app/api/v1/ai_call/controller.py app/services/ai_call/event_persistence.py tests/test_ai_call_owner_recording_contract.py
git diff --check
git add docs/livekit-ai-outbound/sql/phase-i5-owner-runtime-main-recording.sql app/api/v1/ai_call/model.py app/api/v1/ai_call/crud.py app/services/ai_call/recording_service.py app/api/v1/ai_call/service.py app/api/v1/ai_call/controller.py app/services/ai_call/event_persistence.py tests/test_ai_call_owner_recording_contract.py tests/test_ai_call_phase_b1_records.py
git commit -m "feat(ai-call): tenant-scope main recordings"
```

### 任务 2：Effect 事务内 Owner-aware Recording 投影

**文件：**
- 创建：`app/services/ai_call/runtime_control/recording_repository.py`
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 创建：`tests/test_ai_call_runtime_recording_repository.py`
- 创建：`tests/postgres/test_ai_call_owner_recording_postgres.py`

- [ ] **步骤 1：编写失败的状态映射测试**

参数化测试固定验证五组映射：`START_EGRESS + RESOURCE_PRESENT -> recording`、
`START_EGRESS + UNCERTAIN -> starting`、
`START_EGRESS + PERMANENT_NO_RESOURCE -> failed`、
`STOP_EGRESS + ACCEPTED -> stopping`、
`STOP_EGRESS + TERMINAL_CONFIRMED -> verifying`。

同时验证 `completed/failed` 不被迟到观察倒退，错误摘要不含 secret 或堆栈。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_runtime_recording_repository.py
```

预期：repository 或 observation 字段不存在导致 FAIL。

- [ ] **步骤 3：实现 observation 与 projector**

```python
@dataclass(frozen=True, slots=True)
class ProviderObservation:
    kind: ProviderObservationKind
    provider_reference: str | None = None
    provider_status: str | None = None
    object_name: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    file_size: int | None = None
    failure_code: str | None = None
```

`OwnerRecordingRepository.project()` 只接收已锁 Record、Effect/source、observation 和数据库时间；按 `(tenant_id, call_id)` 锁定或创建，不做外部 I/O。`RuntimeEffectRepository.submit()` 应用 Effect 后调用 projector 并统一 flush；异常使整个事务回滚。

- [ ] **步骤 4：添加并运行隔离 PostgreSQL 测试**

新增六个明确场景：Effect 与投影原子提交；投影异常回滚 Effect；旧 Owner 晚提交
Recording 影响 0 行；错误 processing token 影响 0 行；两个 Runtime 竞争只保留一行；
另一租户查询与投影均不可见。每个测试分别查询 Effect 状态和 Recording 行数，不能只检查
repository 返回值。

```bash
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_recording_postgres.py
uv run pytest -q tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_recording_repository.py
```

PostgreSQL 必须显示实际 PASS；环境变量缺失只能报告 SKIP。

- [ ] **步骤 5：静态检查并提交**

```bash
uv run ruff check app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/recording_repository.py tests/test_ai_call_runtime_recording_repository.py tests/postgres/test_ai_call_owner_recording_postgres.py
git diff --check
git add app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/recording_repository.py tests/test_ai_call_runtime_recording_repository.py tests/postgres/test_ai_call_owner_recording_postgres.py
git commit -m "feat(ai-call): fence runtime recording projection"
```

### 任务 3：稳定主录音 Effect 与可选 readiness

**文件：**
- 修改：`app/services/ai_call/runtime_control/runtime_service.py`
- 修改：`app/services/ai_call/runtime_control/start_readiness_repository.py`
- 修改：`app/services/ai_call/runtime_control/startup_recovery.py`
- 修改：`app/services/ai_call/runtime_control/provider_stub.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`
- 修改：`tests/test_ai_call_runtime_start_readiness.py`
- 修改：`tests/test_ai_call_runtime_startup_recovery.py`
- 修改：`tests/test_ai_call_runtime_stub_handlers.py`

- [ ] **步骤 1：编写失败的稳定键/fail-open 测试**

新增四个明确场景：fencing 7 和 8 生成相同 main Egress key 且 generation 均为 1；辅助
Egress 失败仍可 ready，但缺 SIP 必须失败；startup recovery 的 `NO_RESOURCE` 决策忽略
辅助 Egress；Stub 的 `main_recording_enabled` 恒为 false 且默认 specs 不含
`START_EGRESS`。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/test_ai_call_runtime_stub_handlers.py
```

- [ ] **步骤 3：实现固定辅助策略**

```python
AUXILIARY_START_EFFECT_TYPES = frozenset({"START_EGRESS"})
```

真实 Provider 暴露 `main_recording_enabled`，Stub 固定 `False`。开启时追加稳定 spec：

```python
EffectSpec(
    effect_type="START_EGRESS",
    idempotency_key=f"start:{call_id}:start-main-egress",
    provider_namespace=namespace,
    provider_idempotency_key=f"egress:main:{call_id}",
    resource_key=f"egress:main:{call_id}",
    resource_generation=1,
)
```

readiness/startup recovery 只从 `CREATE_EFFECT_TYPES - AUXILIARY_START_EFFECT_TYPES` 计算强制结果，Room、Agent、必要 SIP 仍 fail closed。

- [ ] **步骤 4：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_runtime_owner_repository.py
uv run ruff check app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/start_readiness_repository.py app/services/ai_call/runtime_control/startup_recovery.py app/services/ai_call/runtime_control/provider_stub.py
git diff --check
git add app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/start_readiness_repository.py app/services/ai_call/runtime_control/startup_recovery.py app/services/ai_call/runtime_control/provider_stub.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/test_ai_call_runtime_stub_handlers.py
git commit -m "feat(ai-call): register optional main egress effect"
```

### 任务 4：LiveKit Egress Start/Query/Stop 适配

**文件：**
- 修改：`app/services/ai_call/livekit_egress.py`
- 修改：`app/services/ai_call/runtime_control/livekit_provider.py`
- 修改：`tests/test_ai_call_runtime_livekit_provider.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写失败的 Fake Provider 测试**

Fake manager 记录每次 `start/query/stop` 调用。六个场景分别断言：Start 使用 active OSS
配置并返回结构化元数据；缺 OSS 配置时外部调用次数为 0；Start 超时后的所有恢复轮次
`start_calls == 1`；响应丢失可通过 Room + 对象名找到同一 Egress；Stop 仅受理时保持
非终态；Stop 终态返回 object、duration 和 size。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py -k egress
```

- [ ] **步骤 3：扩展 manager 查询事实**

```python
@dataclass(frozen=True, slots=True)
class LiveKitEgressObservation:
    egress_id: str
    status: str
    object_name: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    file_size: int | None = None
```

提供 `get_egress(egress_id)` 和 `find_room_audio_recording(room_name, object_name)`；后者按稳定对象名精确匹配，不按“最新记录”猜测。

- [ ] **步骤 4：实现 Runtime Provider**

注入只读 `oss_config_provider`。缺配置不调用 Egress；首次非 reconcile claim 才 Start；reconcile-only 只按已持久化 reference 或 Room + object 查询。任何非空 Egress 引用都随 observation 持久化。Stop 仅在终态返回 `TERMINAL_CONFIRMED`；活动为 `ACCEPTED`，超时为 `UNCERTAIN`。

- [ ] **步骤 5：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_phase_b1_records.py -k 'egress or recording'
uv run ruff check app/services/ai_call/livekit_egress.py app/services/ai_call/runtime_control/livekit_provider.py tests/test_ai_call_runtime_livekit_provider.py
git diff --check
git add app/services/ai_call/livekit_egress.py app/services/ai_call/runtime_control/livekit_provider.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_phase_b1_records.py
git commit -m "feat(ai-call): execute recoverable main egress"
```

### 任务 5：END/cleanup 与独立 OSS 对账闭环

**文件：**
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 修改：`app/services/ai_call/recording_service.py`
- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`
- 修改：`tests/test_ai_call_runtime_effect_repository.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`
- 修改：`tests/postgres/test_ai_call_owner_recording_postgres.py`

- [ ] **步骤 1：编写失败的恢复闭环测试**

新增五个明确场景：DELETE_ROOM 在 STOP_EGRESS 前执行次数为 0；STOP 已 APPLIED 且
Recording verifying 时 cleanup 可以 clean；Owner 释放后 Reconciler 仍能 completed；
对象迟到先重试后只完成一次；验证截止后写 failed 且不重新认领 Runtime Owner。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_phase_b1_records.py -k 'egress or recording or cleanup'
```

- [ ] **步骤 3：实现停止投影与对账接续**

保持 `STOP_EGRESS` phase 10、`DELETE_ROOM` phase 20。STOP `APPLIED` 时写 `verifying`、`next_verify_at=database_now` 和固定 verify deadline；Reconciler 按 `tenant_id + recording_id` 更新并幂等登记 `sys_oss`，不锁 Record/Effect。`mark_cleanup_clean()` 不等待 Recording `completed`。

- [ ] **步骤 4：完整验证并提交**

```bash
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_recording_postgres.py tests/postgres/test_ai_call_runtime_control_postgres.py
uv run pytest -q tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/test_ai_call_phase_b1_records.py
uv run ruff check .
codegraph sync
codegraph status
git diff --check
git add app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/recording_service.py app/plugin/init_app.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_phase_b1_records.py tests/postgres/test_ai_call_owner_recording_postgres.py
git commit -m "feat(ai-call): close main recording recovery loop"
```

### 任务 6：冻结前证据与下一切片边界

**文件：** 不修改业务文件，只验证。

- [ ] **步骤 1：核对提交和受保护文件**

```bash
git status --short --branch
git log --oneline --decorate -8
git show --stat --oneline HEAD
```

预期：只保留 `.playwright-cli/` 与 `env/.env.dev.bak-before-local-outbound-20260727`。

- [ ] **步骤 2：最终安全验证**

重跑任务 5 的 unit、隔离 PostgreSQL、Ruff、CodeGraph 和 `git diff --check`。不得连接真实 LiveKit、SIP、OSS、Provider，不启动或重启服务，不拨打电话。

- [ ] **步骤 3：交付边界**

只声明 mixed main recording 代码和隔离验证；分轨、离线 ASR/语义/跟进、前端闭环、真实外部验收继续独立推进，Redis 不作为正确性前置。
