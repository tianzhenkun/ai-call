# 本机 Linphone 外呼任务后端实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在不启用普通任务自动扫描的前提下，让默认租户的单号码通用外呼任务可以人工触发本机 Linphone SIP 通话，并用真实通话终态完成 Attempt、对象和任务。

**架构：** 保留 `OutboundTaskExecutor` 对任务状态、Attempt 和重试的唯一写入职责，通过扩展拨号器协议传入稳定 `callId` 和“已接通”回调。新增 `LinphoneTestDialer` 复用 `AiCallService` 创建真实 SIP 会话、轮询真实通话记录并映射 `DialResult`；新增 `LinphoneTestService` 负责能力查询、人工命令、安全门禁、状态派生和重启恢复。普通 Mock worker 与本地 Linphone 测试分别由独立开关控制。

**技术栈：** Python 3.13、FastAPI、SQLAlchemy Async、Pydantic、pytest、LiveKit SIP、FreeSWITCH、Linphone。

---

## 文件结构

### 新建

- `app/api/v1/ai_call/outbound/linphone_test_schema.py`：测试资格、启动命令、状态查询的 Pydantic 契约。
- `app/api/v1/ai_call/outbound/linphone_test_dialer.py`：真实 SIP 启动、终态轮询和线路结果映射。
- `app/api/v1/ai_call/outbound/linphone_test_service.py`：人工启动门禁、幂等、单通槽位、状态派生、主动结束和恢复对账。
- `docs/livekit-ai-outbound/sql/phase-h4-outbound-linphone-test-postgres.sql`：Attempt 新字段和唯一索引的幂等迁移。
- `tests/test_ai_call_outbound_linphone_test.py`：本地测试领域、API、拨号、状态和恢复测试。

### 修改

- `app/api/v1/ai_call/outbound/rule_task_model.py`：Attempt 增加拨号器、场景、命令幂等键和活动槽位。
- `app/api/v1/ai_call/outbound/task_executor.py`：拨号器接收稳定 `callId`、支持接通中间态和真实记录模式。
- `app/api/v1/ai_call/outbound/rule_task_controller.py`：注册四个本地测试接口。
- `app/api/v1/ai_call/service.py`：内部 SIP 创建允许复用预生成 `callId` 和业务类型。
- `app/services/ai_call/record_service.py`：现有 `create_sip_record()` 的 `business_type` 参数被真实任务调用，不新增第二套记录入口。
- `app/config/setting.py`：新增四个默认关闭或安全值配置。
- `app/plugin/init_app.py`：只在 Linphone 测试开关开启时启动恢复 worker。
- `tests/test_ai_call_outbound_task_executor.py`：锁定 Mock 回归、新拨号器协议和中间态。
- `tests/test_ai_call_phase_e_sip.py`：锁定外部浏览器 SIP 接口不受内部 `callId` 参数影响。

### 查阅但不修改

- `app/services/ai_call/handoff_service.py`：Handoff 状态来源。
- `app/services/ai_call/agent_console_service.py`：坐席可用条件和 30 秒心跳边界。
- `app/services/ai_call/livekit_room.py`：`room_exists()` 恢复判断。
- `docs/superpowers/specs/2026-07-28-local-linphone-outbound-task-adapter-design.md`：已确认规格。

---

### 任务 1：持久化 Linphone 测试 Attempt 元数据

**文件：**

- 修改：`app/api/v1/ai_call/outbound/rule_task_model.py`
- 创建：`docs/livekit-ai-outbound/sql/phase-h4-outbound-linphone-test-postgres.sql`
- 测试：`tests/test_ai_call_outbound_linphone_test.py`

- [ ] **步骤 1：编写失败测试，锁定字段和约束**

```python
def test_linphone_attempt_fields_and_unique_guards_are_declared() -> None:
    table = AiCallOutboundAttemptModel.__table__

    assert table.c.dialer_type.nullable is True
    assert table.c.test_scenario.nullable is True
    assert table.c.command_idempotency_key.nullable is True
    assert table.c.active_slot.nullable is True
    constraint_names = {item.name for item in table.constraints}
    assert "uk_outbound_attempt_tenant_command" in constraint_names
    assert "uk_outbound_attempt_tenant_active_slot" in constraint_names

    sql = Path(
        "docs/livekit-ai-outbound/sql/"
        "phase-h4-outbound-linphone-test-postgres.sql"
    ).read_text()
    assert "ADD COLUMN IF NOT EXISTS dialer_type" in sql
    assert "uk_outbound_attempt_tenant_command" in sql
    assert "uk_outbound_attempt_tenant_active_slot" in sql
```

- [ ] **步骤 2：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py::test_linphone_attempt_fields_and_unique_guards_are_declared -v
```

预期：FAIL，指出 Attempt 缺少四个字段或迁移文件不存在。

- [ ] **步骤 3：增加模型字段和数据库约束**

在 `AiCallOutboundAttemptModel` 增加：

```python
dialer_type: Mapped[str | None] = mapped_column(String(32))
test_scenario: Mapped[str | None] = mapped_column(String(32))
command_idempotency_key: Mapped[str | None] = mapped_column(String(128))
active_slot: Mapped[str | None] = mapped_column(String(32))
```

在 `__table_args__` 增加：

```python
UniqueConstraint(
    "tenant_id",
    "command_idempotency_key",
    name="uk_outbound_attempt_tenant_command",
),
UniqueConstraint(
    "tenant_id",
    "active_slot",
    name="uk_outbound_attempt_tenant_active_slot",
),
```

- [ ] **步骤 4：创建幂等 PostgreSQL 迁移**

```sql
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS dialer_type varchar(32);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS test_scenario varchar(32);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS command_idempotency_key varchar(128);
ALTER TABLE ai_call_outbound_attempt
    ADD COLUMN IF NOT EXISTS active_slot varchar(32);

CREATE UNIQUE INDEX IF NOT EXISTS uk_outbound_attempt_tenant_command
    ON ai_call_outbound_attempt (tenant_id, command_idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS uk_outbound_attempt_tenant_active_slot
    ON ai_call_outbound_attempt (tenant_id, active_slot);
```

- [ ] **步骤 5：运行测试确认通过**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py::test_linphone_attempt_fields_and_unique_guards_are_declared -v
```

预期：PASS。

- [ ] **步骤 6：提交本任务**

```bash
git add app/api/v1/ai_call/outbound/rule_task_model.py \
  docs/livekit-ai-outbound/sql/phase-h4-outbound-linphone-test-postgres.sql \
  tests/test_ai_call_outbound_linphone_test.py
git commit -m "feat(ai-call): add Linphone test attempt guards"
```

---

### 任务 2：让真实 SIP 记录复用任务预生成的 callId

**文件：**

- 修改：`app/api/v1/ai_call/service.py`
- 测试：`tests/test_ai_call_phase_e_sip.py`

- [ ] **步骤 1：编写失败测试，锁定内部 callId 与业务关联**

```python
@pytest.mark.asyncio
async def test_create_sip_session_reuses_internal_call_id_and_business_type() -> None:
    (
        service,
        _room_manager,
        _agent_runner,
        _sip_client,
        record_service,
        _prompt_resolver,
    ) = build_service_with_sip_fakes()

    result = await service.create_sip_session(
        callee_phone_number="19900001001",
        voice="Cherry",
        call_id="call-task-1",
        business_type="outbound_task",
        business_id="1001",
        scene_code="intro_contract",
    )

    assert result.call_id == "call-task-1"
    assert record_service.created_sip_records[0]["call_id"] == "call-task-1"
    assert record_service.created_sip_records[0]["business_type"] == "outbound_task"
    assert record_service.created_sip_records[0]["business_id"] == "1001"
```

同时给现有 `FakeRecordService.create_sip_record()` 增加
`business_type: str | None = None` 参数，并把该值写入
`created_sip_records`；现有默认调用断言应增加 `"business_type": None`。

- [ ] **步骤 2：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_phase_e_sip.py::test_create_sip_session_reuses_internal_call_id_and_business_type -v
```

预期：FAIL，`create_sip_session()` 不接受 `call_id` 或 `business_type`。

- [ ] **步骤 3：扩展内部方法签名并保持浏览器接口不变**

将服务签名改为：

```python
async def create_sip_session(
    self,
    *,
    callee_phone_number: str,
    voice: str | None,
    business_id: str | None = None,
    business_type: str | None = None,
    scene_code: str | None = None,
    business_params: dict | None = None,
    ringing_timeout_seconds: int | None = None,
    call_id: str | None = None,
) -> CreateSipSessionResult:
    resolved_call_id = call_id or f"call_{generate_snowflake_id()}"
```

创建记录时传入：

```python
await self.record_service.create_sip_record(
    call_id=resolved_call_id,
    business_type=business_type,
    business_id=business_id,
    room_name=f"ai-call-{resolved_call_id}",
    participant_identity=f"sip-{resolved_call_id}",
    callee_phone_number_hash=callee_phone_number_hash,
    callee_phone_number_masked=callee_phone_number_masked,
)
```

`CreateSipSessionRequest` 和 `/ai-call/sip-sessions` 控制器不增加 `callId` 字段，避免外部调用者指定主键。

- [ ] **步骤 4：运行目标 SIP 测试**

运行：

```bash
uv run pytest \
  tests/test_ai_call_phase_e_sip.py::test_create_sip_session_reuses_internal_call_id_and_business_type \
  tests/test_ai_call_phase_e_sip.py::test_create_sip_session_reuses_room_agent_prompt_and_records_sip_events \
  tests/test_ai_call_phase_e_sip.py::test_create_sip_session_controller_accepts_dynamic_callee_without_browser_token \
  -v
```

预期：3 PASS。

- [ ] **步骤 5：提交本任务**

```bash
git add app/api/v1/ai_call/service.py tests/test_ai_call_phase_e_sip.py
git commit -m "feat(ai-call): allow internal SIP call id reuse"
```

---

### 任务 3：扩展执行器拨号协议并保持 Mock 行为

**文件：**

- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 修改：`tests/test_ai_call_outbound_task_executor.py`

- [ ] **步骤 1：编写失败测试，锁定稳定 callId 和接通中间态**

```python
class CapturingLifecycleDialer:
    dialer_type = "linphone_test"
    manages_call_record = True

    def __init__(self) -> None:
        self.call_ids: list[str] = []

    async def dial(self, request, *, call_id, on_connected):
        self.call_ids.append(call_id)
        await on_connected()
        return DialResult(call_result="connected", duration_ms=3_000)


@pytest.mark.asyncio
async def test_executor_passes_call_id_and_marks_real_call_in_call(database) -> None:
    dialer = CapturingLifecycleDialer()
    executor = OutboundTaskExecutor(database.session_factory, dialer)
    claimed = await executor.claim_manual_test(
        TaskKey("000000", TASK_ID),
        command_idempotency_key="cmd-1",
        test_scenario="ai_only",
        active_slot="linphone_test",
    )

    await executor.execute_claimed(claimed)

    assert dialer.call_ids == [claimed.call_id]
    assert await load_attempt_status(database, claimed.call_id) == "COMPLETED"
    assert await count_records(database, entry_type="outbound_mock") == 0
```

- [ ] **步骤 2：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_task_executor.py -k "call_id or in_call or mock" -v
```

预期：FAIL，现有拨号器不接收 `call_id`，并且执行器总会创建 Mock 记录。

- [ ] **步骤 3：定义一致的拨号器协议**

```python
ConnectedCallback = Callable[[], Awaitable[None]]


class OutboundDialer(Protocol):
    dialer_type: str
    manages_call_record: bool

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult:
        raise NotImplementedError
```

`MockOutboundDialer` 固定：

```python
dialer_type = "mock"
manages_call_record = False
```

它忽略 `call_id`，不调用 `on_connected`，保持现有立即返回模拟结果的行为。

- [ ] **步骤 4：拆出可复用的人工认领与执行方法**

新增公开方法：

```python
async def claim_manual_test(
    self,
    task_key: TaskKey,
    *,
    command_idempotency_key: str,
    test_scenario: str,
    active_slot: str,
) -> ClaimedAttempt:
    return await self._claim_target(
        task_key,
        self.now_provider(),
        command_idempotency_key=command_idempotency_key,
        test_scenario=test_scenario,
        active_slot=active_slot,
        ignore_call_window=True,
    )

async def execute_claimed(self, claimed: ClaimedAttempt) -> None:
    result = await self.dialer.dial(
        claimed.request,
        call_id=claimed.call_id,
        on_connected=lambda: self._mark_in_call(claimed),
    )
    await self._finish_attempt(
        claimed.request,
        claimed.call_id,
        result,
        self.now_provider(),
    )
```

认领时：

- 普通 worker 仅在 `manages_call_record=False` 时创建 `outbound_mock` 记录；
- Attempt 写入 `dialer_type`；
- 人工命令同时写入 `test_scenario`、`command_idempotency_key`、`active_slot`；
- `_mark_in_call()` 将 Attempt 与 Target 从 `DIALING` 原子更新为 `IN_CALL`；
- `_finish_attempt()` 接受 `DIALING` 和 `IN_CALL`，终态时清空 `active_slot`；
- 真实拨号器不覆盖 `AiCallRecord` 的状态、结束原因和失败字段；
- 真实 SIP 在创建记录前失败时，仍允许 Attempt、Target 和 Task 正常收尾。

- [ ] **步骤 5：让普通 run_once 使用同一执行入口**

```python
claimed = await self._claim_target(task_key, self.now_provider())
if claimed is not None:
    await self.execute_claimed(claimed)
    processed += 1
```

更新 `SequenceDialer`、`FailingDialer`、`BlockingDialer` 的签名，但不改变原测试语义。

- [ ] **步骤 6：运行执行器回归测试**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_task_executor.py -v
```

预期：全部 PASS；Mock 仍创建 `outbound_mock`，暂停/停止/重试行为不回退。

- [ ] **步骤 7：提交本任务**

```bash
git add app/api/v1/ai_call/outbound/task_executor.py \
  tests/test_ai_call_outbound_task_executor.py
git commit -m "refactor(ai-call): support real outbound dial lifecycle"
```

---

### 任务 4：实现 LinphoneTestDialer 的真实记录终态映射

**文件：**

- 创建：`app/api/v1/ai_call/outbound/linphone_test_dialer.py`
- 测试：`tests/test_ai_call_outbound_linphone_test.py`

- [ ] **步骤 1：编写失败测试，锁定接听后不提前完成**

```python
@pytest.mark.asyncio
async def test_linphone_dialer_waits_for_terminal_record_after_answer() -> None:
    records = FakeRecordReader([
        record(status="connected", answered_at=NOW),
        record(status="ai_speaking", answered_at=NOW),
        record(status="completed", answered_at=NOW, ended_at=NOW_PLUS_5),
    ])
    connected = AsyncMock()
    dialer = LinphoneTestDialer(
        session_factory=fake_session_factory,
        ai_call_service_factory=FakeAiCallServiceFactory(call_id="call-1"),
        record_reader=records,
        poll_seconds=0,
    )

    result = await dialer.dial(
        REQUEST,
        call_id="call-1",
        on_connected=connected,
    )

    connected.assert_awaited_once()
    assert records.read_count == 3
    assert result == DialResult(call_result="connected", duration_ms=5_000)
```

- [ ] **步骤 2：编写线路失败映射参数化测试**

```python
@pytest.mark.parametrize(
    ("end_reason", "expected"),
    [
        ("busy", "busy"),
        ("ringing_timeout", "no_answer"),
        ("user_unavailable", "no_answer"),
        ("sip_connect_timeout", "no_answer"),
        ("sip_preflight_failed", "call_failed"),
        ("room_create_failed", "call_failed"),
    ],
)
def test_linphone_result_mapping(end_reason: str, expected: str) -> None:
    value = LinphoneTestDialer.map_record_result(
        record(status="failed", end_reason=end_reason),
    )
    assert value.call_result == expected
```

- [ ] **步骤 3：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "dialer or result_mapping" -v
```

预期：FAIL，`LinphoneTestDialer` 尚不存在。

- [ ] **步骤 4：实现真实 SIP 启动和终态轮询**

核心结构：

```python
class LinphoneTestDialer:
    dialer_type = "linphone_test"
    manages_call_record = True
    terminal_statuses = {"completed", "failed"}

    async def dial(self, request, *, call_id, on_connected) -> DialResult:
        async with self.session_factory() as db:
            service = self.ai_call_service_factory(db)
            await service.create_sip_session(
                callee_phone_number=request.phone_number,
                voice=request.voice,
                call_id=call_id,
                business_type="outbound_task",
                business_id=str(request.task_id),
                scene_code=request.scene_code,
                business_params={
                    "customer_name": request.customer_name or "",
                    "target_id": str(request.target_id),
                },
            )
            await db.commit()
        await on_connected()
        record = await self._wait_terminal_record(call_id)
        return self.map_record_result(record)
```

轮询每次打开新 `AsyncSession`，确保不会读取事务缓存。若 `create_sip_session()` 抛错，先读取已经落库的失败记录；有记录时按记录映射，没有记录时返回 `call_failed` 和明确异常消息。

- [ ] **步骤 5：实现结果映射**

```python
if record.answered_at is not None:
    return DialResult(
        call_result="connected",
        error_message=None,
        duration_ms=max(0, duration_ms(record)),
    )
if record.end_reason == "busy":
    return DialResult(call_result="busy", error_message=record.failure_message)
if record.end_reason in {
    "ringing_timeout",
    "user_unavailable",
    "sip_connect_timeout",
    "no_answer",
}:
    return DialResult(call_result="no_answer", error_message=record.failure_message)
return DialResult(
    call_result="call_failed",
    error_message=record.failure_message or record.end_reason or "真实外呼失败",
)
```

- [ ] **步骤 6：运行目标测试**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "dialer or result_mapping" -v
```

预期：全部 PASS。

- [ ] **步骤 7：提交本任务**

```bash
git add app/api/v1/ai_call/outbound/linphone_test_dialer.py \
  tests/test_ai_call_outbound_linphone_test.py
git commit -m "feat(ai-call): add Linphone task test dialer"
```

---

### 任务 5：实现测试资格、启动幂等和单通门禁

**文件：**

- 创建：`app/api/v1/ai_call/outbound/linphone_test_schema.py`
- 创建：`app/api/v1/ai_call/outbound/linphone_test_service.py`
- 修改：`app/api/v1/ai_call/outbound/rule_task_controller.py`
- 测试：`tests/test_ai_call_outbound_linphone_test.py`

- [ ] **步骤 1：定义响应和请求类型**

```python
class LinphoneTestScenario(str, Enum):
    AI_ONLY = "ai_only"
    HANDOFF = "handoff"


class LinphoneTestRunIn(OutboundSchema):
    scenario: LinphoneTestScenario


class LinphoneTestCapabilityOut(OutboundSchema):
    enabled: bool
    eligible: bool
    reasons: list[str]
    available_agent_count: int
    active_call_id: str | None = None
    can_end_active_call: bool


class LinphoneTestAcceptedOut(AcceptedCommandOut):
    task_id: str
    attempt_id: str
    call_id: str
```

- [ ] **步骤 2：编写失败测试，覆盖全部门禁**

```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enabled", "tenant_id", "task_mode", "task_status", "phone", "reason"),
    [
        (False, "000000", "single", "SCHEDULED", "19900001001", "本地测试能力未开启"),
        (True, "100001", "single", "SCHEDULED", "19900001001", "仅允许默认测试租户"),
        (True, "000000", "batch", "SCHEDULED", "19900001001", "仅支持单号码任务"),
        (True, "000000", "single", "RUNNING", "19900001001", "任务不是待执行状态"),
        (True, "000000", "single", "SCHEDULED", "13900000000", "号码不在测试白名单"),
    ],
)
async def test_capability_rejects_unsafe_task(
    database, enabled, tenant_id, task_mode, task_status, phone, reason
) -> None:
    service = build_test_service(database, enabled=enabled)
    task_id = await seed_task(database, tenant_id, task_mode, task_status, [phone])

    capability = await service.get_capability(database.session, tenant_id, task_id)

    assert capability.eligible is False
    assert reason in capability.reasons
```

再增加：

- 两个对象时拒绝；
- SIP preflight 失败时拒绝；
- `handoff` 且场景授权范围内可用坐席数为 0 时拒绝；
- 存在 `active_slot=linphone_test` 时拒绝；
- 所有拒绝路径均断言 SIP factory 未调用。

- [ ] **步骤 3：运行门禁测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "capability or unsafe or preflight" -v
```

预期：FAIL，服务和接口尚不存在。

- [ ] **步骤 4：实现资格查询**

资格查询按固定顺序收集原因：

1. `AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED`；
2. `tenant_id == "000000"`；
3. 任务存在且状态为 `SCHEDULED`，或已有该任务活动 Attempt；
4. `task_mode == "single"` 且对象数等于 1；
5. 对象号码严格等于 `AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE`；
6. 不存在其他 `active_slot="linphone_test"`；
7. 使用 `SipOutboundConfig.from_settings()` 做 SIP preflight；
8. 可用坐席为 `enabled profile + scene scope + presence available + 无 active_handoff_id + last_seen_at 30 秒内`。

`eligible` 表示 AI-only 场景可启动；Handoff 额外用 `available_agent_count > 0` 校验。

- [ ] **步骤 5：编写失败测试，锁定幂等和并发单通**

```python
@pytest.mark.asyncio
async def test_duplicate_command_creates_one_attempt(database) -> None:
    service = build_test_service(database, enabled=True)

    first = await service.start_test(
        tenant_id="000000",
        task_id=TASK_ID,
        idempotency_key="cmd-1",
        scenario="ai_only",
    )
    second = await service.start_test(
        tenant_id="000000",
        task_id=TASK_ID,
        idempotency_key="cmd-1",
        scenario="ai_only",
    )

    assert first == second
    assert await count_attempts(database, command_key="cmd-1") == 1


@pytest.mark.asyncio
async def test_active_slot_rejects_second_linphone_call(database) -> None:
    first_service, second_service = build_two_services(database)
    results = await asyncio.gather(
        first_service.start_test("000000", TASK_ID_1, "cmd-1", "ai_only"),
        second_service.start_test("000000", TASK_ID_2, "cmd-2", "ai_only"),
        return_exceptions=True,
    )

    assert sum(isinstance(item, LinphoneTestAcceptedOut) for item in results) == 1
    assert sum(isinstance(item, CustomException) for item in results) == 1
```

- [ ] **步骤 6：实现原子启动**

`start_test()` 使用服务自己的 `session_factory`：

```python
async with self.session_factory() as db:
    existing = await self._attempt_by_command(db, tenant_id, idempotency_key)
    if existing is not None:
        return self.accepted_out(existing)
    await self.require_start_capability(db, tenant_id, task_id, scenario)
    claimed = await self.executor.claim_manual_test(
        TaskKey(tenant_id, task_id),
        command_idempotency_key=idempotency_key,
        test_scenario=scenario,
        active_slot="linphone_test",
    )
self._dispatch(claimed)
return self.accepted_out(claimed)
```

捕获唯一约束竞争后回滚，再按 `command_idempotency_key` 返回原 Attempt；如果冲突来自 `active_slot`，返回 409“已有 Linphone 测试通话进行中”。

- [ ] **步骤 7：注册 capability 和 test-run 接口**

```python
@OutboundRuleTaskRouter.get(
    "/outbound-tasks/{task_id}/test-capability",
    response_model=ResponseSchema[LinphoneTestCapabilityOut],
)
async def get_test_capability_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_capability(auth.db, tenant_id, task_id)
    return SuccessResponse(data=result, msg="查询成功")


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/test-run",
    response_model=ResponseSchema[LinphoneTestAcceptedOut],
)
async def run_linphone_test_controller(
    task_id: Annotated[int, Path(gt=0)],
    request: LinphoneTestRunIn,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[LinphoneTestService, Depends(get_linphone_test_service)],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.start_test(
        tenant_id=tenant_id,
        task_id=task_id,
        idempotency_key=idempotency_key,
        scenario=request.scenario.value,
    )
    return SuccessResponse(data=result, msg="测试拨打已受理")
```

- [ ] **步骤 8：运行领域和 API 测试**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "capability or command or active_slot or routes" -v
```

预期：全部 PASS，接口继续返回 RuoYi 统一 envelope。

- [ ] **步骤 9：提交本任务**

```bash
git add app/api/v1/ai_call/outbound/linphone_test_schema.py \
  app/api/v1/ai_call/outbound/linphone_test_service.py \
  app/api/v1/ai_call/outbound/rule_task_controller.py \
  tests/test_ai_call_outbound_linphone_test.py
git commit -m "feat(ai-call): add guarded Linphone task commands"
```

---

### 任务 6：实现页面状态派生和主动结束当前通话

**文件：**

- 修改：`app/api/v1/ai_call/outbound/linphone_test_schema.py`
- 修改：`app/api/v1/ai_call/outbound/linphone_test_service.py`
- 修改：`app/api/v1/ai_call/outbound/rule_task_controller.py`
- 测试：`tests/test_ai_call_outbound_linphone_test.py`

- [ ] **步骤 1：定义状态响应**

```python
class LinphoneTestStatusOut(OutboundSchema):
    task_id: str
    target_id: str
    attempt_id: str
    call_id: str
    target_status: str
    attempt_status: str
    call_status: str | None = None
    handoff_status: str | None = None
    phase: Literal[
        "dialing",
        "ai_call",
        "waiting_handoff",
        "human_call",
        "completed",
        "failed",
    ]
    elapsed_seconds: int
    end_reason: str | None = None
    error_message: str | None = None
    can_end_active_call: bool
```

- [ ] **步骤 2：编写失败测试，锁定派生阶段**

```python
@pytest.mark.parametrize(
    ("attempt_status", "handoff_status", "expected"),
    [
        ("DIALING", None, "dialing"),
        ("IN_CALL", None, "ai_call"),
        ("IN_CALL", "requested", "waiting_handoff"),
        ("IN_CALL", "accepted", "waiting_handoff"),
        ("IN_CALL", "connected", "human_call"),
        ("COMPLETED", "completed", "completed"),
        ("FAILED", None, "failed"),
    ],
)
def test_phase_is_derived_not_persisted(
    attempt_status: str,
    handoff_status: str | None,
    expected: str,
) -> None:
    assert derive_test_phase(attempt_status, handoff_status) == expected
    assert "phase" not in AiCallOutboundAttemptModel.__table__.c
```

同时断言 `elapsed_seconds`：

- 未接听为 0；
- 进行中使用 `now - answered_at`；
- 结束后使用 `ended_at - answered_at`；
- 负值被压到 0。

- [ ] **步骤 3：运行状态测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "phase or elapsed or test_status" -v
```

预期：FAIL，状态派生尚未实现。

- [ ] **步骤 4：实现状态聚合查询**

按 `tenant_id + task_id` 查询最近 Attempt，再用 `call_id` 查询 `AiCallRecord` 和最近 Handoff。不要新增 `phase` 数据库字段。

阶段优先级固定为：

```python
if attempt.status == "FAILED":
    phase = "failed"
elif attempt.status == "COMPLETED":
    phase = "completed"
elif handoff and handoff.status == "connected":
    phase = "human_call"
elif handoff and handoff.status in {"requested", "accepted"}:
    phase = "waiting_handoff"
elif attempt.status == "IN_CALL":
    phase = "ai_call"
else:
    phase = "dialing"
```

- [ ] **步骤 5：编写主动结束测试**

```python
@pytest.mark.asyncio
async def test_end_active_call_uses_attempt_call_id(database) -> None:
    ai_call_service = FakeAiCallService()
    service = build_test_service(database, ai_call_service=ai_call_service)

    result = await service.end_active_call(
        tenant_id="000000",
        task_id=TASK_ID,
        idempotency_key="end-1",
    )

    assert result.accepted is True
    assert ai_call_service.end_calls == [
        ("call-1", "outbound_task_manual_end"),
    ]
```

还要覆盖：无活动 Attempt 返回 409；重复结束返回已受理；结束命令不把 Task 直接改成 `STOPPING` 或 `STOPPED`。

- [ ] **步骤 6：实现并注册状态与结束接口**

```python
@OutboundRuleTaskRouter.get(
    "/outbound-tasks/{task_id}/test-status",
    response_model=ResponseSchema[LinphoneTestStatusOut],
)
async def get_test_status_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_status(auth.db, tenant_id, task_id)
    return SuccessResponse(data=result, msg="查询成功")


@OutboundRuleTaskRouter.post(
    "/outbound-tasks/{task_id}/active-call/end",
    response_model=ResponseSchema[AcceptedCommandOut],
)
async def end_active_test_call_controller(
    task_id: Annotated[int, Path(gt=0)],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    service: Annotated[
        LinphoneTestService,
        Depends(get_linphone_test_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    result = await service.end_active_call(
        tenant_id=tenant_id,
        task_id=task_id,
        idempotency_key=idempotency_key,
    )
    return SuccessResponse(data=result, msg="结束通话命令已受理")
```

`end_active_call()` 只根据活动 Attempt 的 `call_id` 调用：

```python
await ai_call_service.end_session(
    attempt.call_id,
    end_reason="outbound_task_manual_end",
)
```

Attempt 的最终完成仍由真实通话记录终态驱动。

- [ ] **步骤 7：运行状态和结束测试**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "phase or elapsed or test_status or end_active" -v
```

预期：全部 PASS。

- [ ] **步骤 8：提交本任务**

```bash
git add app/api/v1/ai_call/outbound/linphone_test_schema.py \
  app/api/v1/ai_call/outbound/linphone_test_service.py \
  app/api/v1/ai_call/outbound/rule_task_controller.py \
  tests/test_ai_call_outbound_linphone_test.py
git commit -m "feat(ai-call): expose Linphone task test status"
```

---

### 任务 7：实现重启恢复与安全配置

**文件：**

- 修改：`app/config/setting.py`
- 修改：`app/plugin/init_app.py`
- 修改：`app/api/v1/ai_call/outbound/linphone_test_service.py`
- 测试：`tests/test_ai_call_outbound_linphone_test.py`

- [ ] **步骤 1：编写默认关闭配置测试**

```python
def test_linphone_task_test_is_disabled_by_default() -> None:
    fields = Settings.model_fields
    assert fields["AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED"].default is False
    assert (
        fields["AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE"].default
        == "19900001001"
    )
    assert fields["AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS"].default == 1.0
    assert (
        fields["AI_CALL_OUTBOUND_LINPHONE_RECOVERY_GRACE_SECONDS"].default
        == 30
    )
```

- [ ] **步骤 2：编写五类恢复测试**

```python
@pytest.mark.asyncio
async def test_recovery_finishes_terminal_record(database) -> None:
    service = await seed_recovery_case(database, record_status="completed")
    assert await service.reconcile_once() == 1
    assert await load_attempt_status(database, "call-1") == "COMPLETED"

@pytest.mark.asyncio
async def test_recovery_keeps_active_record_when_room_exists(database) -> None:
    service = await seed_recovery_case(
        database,
        record_status="connected",
        room_exists=True,
    )
    assert await service.reconcile_once() == 0
    assert await load_attempt_status(database, "call-1") == "IN_CALL"

@pytest.mark.asyncio
async def test_recovery_waits_inside_missing_room_grace(database) -> None:
    service = await seed_recovery_case(
        database,
        record_status="connected",
        room_exists=False,
        age_seconds=10,
    )
    assert await service.reconcile_once() == 0
    assert await load_active_slot(database, "call-1") == "linphone_test"

@pytest.mark.asyncio
async def test_recovery_fails_unanswered_call_after_room_grace(database) -> None:
    service = await seed_recovery_case(
        database,
        record_status="ready",
        room_exists=False,
        age_seconds=31,
        answered=False,
    )
    assert await service.reconcile_once() == 1
    assert await load_attempt_result(database, "call-1") == "call_failed"

@pytest.mark.asyncio
async def test_recovery_fails_attempt_without_record_after_start_grace(database) -> None:
    service = await seed_recovery_case(
        database,
        record_status=None,
        room_exists=False,
        age_seconds=31,
    )
    assert await service.reconcile_once() == 1
    assert await load_attempt_result(database, "call-1") == "call_failed"
```

每个测试都要断言 Attempt、Target、Task、`active_slot` 和 `call_result`。已存在 `answered_at` 时，即使 Room 丢失，线路结果仍为 `connected`，详细恢复错误保留在通话记录失败字段。

- [ ] **步骤 3：运行恢复测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "recovery or disabled_by_default" -v
```

预期：FAIL，配置与恢复 worker 尚不存在。

- [ ] **步骤 4：增加安全配置**

```python
AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED: bool = False
AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE: str = "19900001001"
AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS: float = 1.0
AI_CALL_OUTBOUND_LINPHONE_RECOVERY_GRACE_SECONDS: int = 30
```

不修改 `AI_CALL_OUTBOUND_EXECUTOR_ENABLED=False` 的默认值。

- [ ] **步骤 5：实现恢复单轮与 worker**

`reconcile_once()` 只查询：

```python
AiCallOutboundAttemptModel.dialer_type == "linphone_test"
AiCallOutboundAttemptModel.status.in_(["DIALING", "IN_CALL"])
AiCallOutboundAttemptModel.active_slot == "linphone_test"
```

固定恢复顺序：

1. 记录终态：立即映射并完成 Attempt；
2. 记录非终态且 Room 存在：保持活动；
3. Room 不存在但未过 30 秒：保持活动；
4. Room 不存在且过宽限：将记录标记失败，再按 `answered_at` 映射；
5. 无记录且过宽限：`call_failed`。

`LinphoneTestRecoveryWorker` 捕获单轮异常并记录日志，不因一次查询失败退出。

- [ ] **步骤 6：接入应用生命周期**

在 `app/plugin/init_app.py` 增加：

```python
if settings.AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED:
    linphone_test_worker = await _start_ai_call_linphone_test_worker()
```

关闭时调用 `await worker.stop()`。启动日志必须明确：

```text
AI Call 本机 Linphone 测试恢复 worker 已启动；普通任务自动执行器保持独立开关
```

- [ ] **步骤 7：运行恢复和生命周期测试**

运行：

```bash
uv run pytest tests/test_ai_call_outbound_linphone_test.py -k "recovery or worker or disabled_by_default" -v
```

预期：全部 PASS。

- [ ] **步骤 8：提交本任务**

```bash
git add app/config/setting.py \
  app/plugin/init_app.py \
  app/api/v1/ai_call/outbound/linphone_test_service.py \
  tests/test_ai_call_outbound_linphone_test.py
git commit -m "feat(ai-call): recover active Linphone task tests"
```

---

### 任务 8：后端回归、静态检查和无拨号门禁

**文件：**

- 修改：`docs/superpowers/specs/2026-07-28-local-linphone-outbound-task-adapter-design.md`
- 验证：本计划列出的全部后端文件

- [ ] **步骤 1：同步 CodeGraph**

运行：

```bash
codegraph sync
```

预期：索引同步成功。若本机仍未安装 `codegraph`，记录“命令不可用”，继续执行下面的测试与 `rg` 定向核对，不把索引失败误报为代码失败。

- [ ] **步骤 2：运行三个目标测试集**

运行：

```bash
uv run pytest \
  tests/test_ai_call_outbound_linphone_test.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_phase_e_sip.py \
  -v
```

预期：全部 PASS。

- [ ] **步骤 3：运行规则任务和转人工回归**

运行：

```bash
uv run pytest \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_agent_console_api.py \
  tests/test_ai_call_agent_console_claim.py \
  -v
```

预期：全部 PASS。

- [ ] **步骤 4：运行 Ruff**

运行：

```bash
uv run ruff check \
  app/api/v1/ai_call/outbound \
  app/api/v1/ai_call/service.py \
  app/config/setting.py \
  app/plugin/init_app.py \
  tests/test_ai_call_outbound_linphone_test.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_phase_e_sip.py
```

预期：`All checks passed!`

- [ ] **步骤 5：确认默认配置不会真实拨号**

运行：

```bash
uv run python - <<'PY'
from app.config.setting import Settings

settings = Settings()
assert settings.AI_CALL_OUTBOUND_EXECUTOR_ENABLED is False
assert settings.AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED is False
print("SAFE_DEFAULTS_OK")
PY
```

预期：输出 `SAFE_DEFAULTS_OK`。此步骤不启动 19011、不连接 SIP、不写 `/tmp/ai_call_ed81_local.db`。

- [ ] **步骤 6：检查接口和状态字段覆盖**

运行：

```bash
rg -n \
  "test-capability|test-run|test-status|active-call/end|elapsed_seconds|active_slot" \
  app/api/v1/ai_call/outbound tests/test_ai_call_outbound_linphone_test.py
```

预期：四个接口、时长字段和单通槽位均同时出现在实现与测试中。

- [ ] **步骤 7：提交规格的时长契约补充**

```bash
git add docs/superpowers/specs/2026-07-28-local-linphone-outbound-task-adapter-design.md
git commit -m "docs(ai-call): specify Linphone test elapsed time"
```

---

### 任务 9：隔离环境联调与真人验收门禁

**文件：**

- 查阅：`env/.env.dev`
- 查阅：`docs/livekit-ai-outbound/p1-local-test-baseline.md`
- 产出：命令行验证记录和真人验收结果，不修改正式环境配置。

- [ ] **步骤 1：记录当前工作树、分支和运行实例**

运行：

```bash
pwd
git branch --show-current
lsof -nP -iTCP:19011 -sTCP:LISTEN
```

预期：代码目录为 `/Users/liuhongli/.codex/worktrees/ed81/ai-call`，分支为 `codex/ai-call-workflow-split`；若 19011 listener 的 cwd 不属于该工作树，先停止验收，不复用错误实例。

- [ ] **步骤 2：只读检查依赖服务和 Linphone 注册**

运行：

```bash
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' | \
  grep -E '19011|freeswitch'
lsof -nP -iTCP:19011 -sTCP:LISTEN
docker exec sip_realtime_freeswitch \
  fs_cli -x 'sofia status profile internal reg'
```

预期：

- LiveKit Server、LiveKit SIP、Redis、Egress 正常；
- `sip_realtime_freeswitch` healthy；
- `1000@192.168.0.111` 为 Registered 且 Reachable。

- [ ] **步骤 3：用独立数据库启动测试实例**

先创建独立数据库副本：

```bash
linphone_test_dir=$(mktemp -d /tmp/ai-call-linphone-test.XXXXXX)
cp /tmp/ai_call_ed81_local.db \
  "${linphone_test_dir}/ai_call_linphone_test.db"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "ALTER TABLE ai_call_outbound_attempt ADD COLUMN dialer_type varchar(32);"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "ALTER TABLE ai_call_outbound_attempt ADD COLUMN test_scenario varchar(32);"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "ALTER TABLE ai_call_outbound_attempt ADD COLUMN command_idempotency_key varchar(128);"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "ALTER TABLE ai_call_outbound_attempt ADD COLUMN active_slot varchar(32);"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "CREATE UNIQUE INDEX uk_outbound_attempt_tenant_command
   ON ai_call_outbound_attempt (tenant_id, command_idempotency_key);"
sqlite3 "${linphone_test_dir}/ai_call_linphone_test.db" \
  "CREATE UNIQUE INDEX uk_outbound_attempt_tenant_active_slot
   ON ai_call_outbound_attempt (tenant_id, active_slot);"
```

确认 19011 没有旧 listener 后，在本计划的后端工作树启动：

```bash
ENVIRONMENT=dev \
SERVER_PORT=19011 \
DATABASE_TYPE=sqlite \
DATABASE_NAME="${linphone_test_dir}/ai_call_linphone_test" \
ROOT_PATH= \
AI_CALL_OUTBOUND_EXECUTOR_ENABLED=false \
AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=true \
AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE=19900001001 \
AI_CALL_SIP_OUTBOUND_ENABLED=true \
uv run python main.py run --env dev
```

预期：健康检查成功，启动日志表明 Linphone 恢复 worker 开启、普通任务自动执行器关闭。

- [ ] **步骤 4：先做无拨号 API 门禁验收**

用非白名单号码、批量任务、非 `SCHEDULED` 任务和无在线坐席的 Handoff 场景请求 `test-run`。

预期：全部返回明确 4xx 原因；Linphone 不响铃，Attempt 不增加。

- [ ] **步骤 5：等待用户确认后做 AI-only 真人验收**

用户准备好 Linphone 后才点击“测试拨打”。用户接听并与 AI 对话，再从 Linphone 挂断。

预期：

- Linphone 真实响铃；
- Attempt/Target 进入 `IN_CALL` 时 Task 不提前完成；
- 挂断后 `callResult=connected`；
- Task、Target、Attempt、AiCallRecord 使用同一个 `callId`；
- `active_slot` 被清空。

- [ ] **步骤 6：等待用户和坐席都在线后做 Handoff 真人验收**

用户在 Linphone 明确说“转人工”，浏览器坐席从待接池 claim，完成人工双向通话并点击结束。

预期：

- `phase` 依次出现 `ai_call -> waiting_handoff -> human_call -> completed`；
- AI 在人工接管后保持挂起；
- 坐席和 Linphone 可以双向说话；
- Handoff 与 AiCallRecord 终态正确。

- [ ] **步骤 7：做主动结束、未接听和重启恢复验收**

分别执行：

1. 通话中点击“结束当前通话”；
2. Linphone 不接听直到超时；
3. 通话进行中重启测试 API，等待恢复 worker 对账。

预期：主动结束不改变“停止任务”语义；未接听映射 `no_answer`；重启后没有永久占用 `active_slot`。

- [ ] **步骤 8：提交最终修复并请求代码审查**

只提交真人验收暴露且属于本规格的修复文件，然后使用 `requesting-code-review` 检查：

- 真实 SIP 是否可能越过白名单；
- 是否存在两个并发 Linphone Attempt；
- 是否会在接听时提前完成任务；
- 是否误改 Mock worker 或浏览器单次 SIP 行为；
- 是否把自动测试结果误当成双向音频验收。
