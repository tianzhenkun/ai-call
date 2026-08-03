# Owner Runtime 客户分轨录音实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在 PostgreSQL Owner/Effect/fencing 控制下，为 `owner_command_v1` 增加一份可恢复的客户 Participant 分轨录音；它与混合主录音共用 `AI_CALL_RECORDING_ENABLED`，不新增 AI 分轨。

**架构：** `START_CALL` 预登记稳定 `START_TRACK_EGRESS(customer)`，但数据库 `answered_at` 成立前不可 claim。Web/SIP 只写就绪事实，当前 Owner 才能执行 Participant Egress；Effect submit 与租户化 Track 投影同事务，END 生成独立 Stop 并进入完整 DELETE_ROOM 依赖图，OSS 文件由独立 Track Reconciler 收口。

**技术栈：** Python 3.12、FastAPI、SQLAlchemy Async、PostgreSQL 16、LiveKit Track Egress Twirp、pytest、Ruff、CodeGraph。

---

## 文件结构

- 创建 `docs/livekit-ai-outbound/sql/phase-i6-owner-runtime-customer-track-recording.sql`：Track 租户回填、generation、唯一约束和索引 migration。
- 修改 `app/api/v1/ai_call/model.py`、`app/api/v1/ai_call/crud.py`：租户化 Track 数据合同、读写和带 claim token 的对账接口。
- 修改 `app/services/ai_call/recording_service.py`、`app/services/ai_call/offline_asr_service.py`：legacy/对账/ASR 显式传递 Track tenant，不改变离线 ASR 业务规则。
- 修改 `tools/ai_call_p1_realtime_shadow_compare.py`：诊断查询通过 Record tenant 限定 Track。
- 创建 `app/services/ai_call/runtime_control/customer_track.py`：稳定 identity digest、Effect key 与资源键的纯函数。
- 修改 `app/services/ai_call/runtime_control/command_repository.py`、`runtime_service.py`、`effect_repository.py`：传递 customer identity、登记辅助 Effect、原子就绪 claim gate 和 END 映射；Stub 通过测试证明保持录音 capability=false，无需修改。
- 创建 `app/services/ai_call/runtime_control/track_recording_repository.py`：只负责 Owner-aware 客户 Track 单调投影。
- 修改 `app/services/ai_call/runtime_control/livekit_provider.py`：解析客户 Track 资源，执行/观察 Participant Egress，恢复时只查询。
- 创建 `app/services/ai_call/runtime_control/customer_media_repository.py`：Web ready 的 Record 行锁、终态屏障、数据库时钟和 PostgreSQL 唤醒。
- 修改 `app/api/v1/ai_call/service.py`：Owner 模式只写 ready 事实，禁止 legacy 分轨直接调用。
- 新增聚焦的 unit/PostgreSQL 测试；保留 legacy customer+AI 测试，只验证 Owner 模式不走该链路。

### 任务 1：租户化 Track 数据与独立对账 claim

**文件：**
- 创建：`docs/livekit-ai-outbound/sql/phase-i6-owner-runtime-customer-track-recording.sql`
- 修改：`app/api/v1/ai_call/model.py`
- 修改：`app/api/v1/ai_call/crud.py`
- 修改：`app/services/ai_call/recording_service.py`
- 修改：`app/services/ai_call/offline_asr_service.py`
- 修改：`tools/ai_call_p1_realtime_shadow_compare.py`
- 创建：`tests/test_ai_call_owner_track_recording_contract.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`
- 修改：`tests/test_ai_call_outbound_task_executor.py`
- 修改：`tests/test_ai_call_sip_outbound_dialer.py`
- 修改：`tests/test_ai_call_semantic_analysis.py`
- 修改：`tests/test_ai_call_p1_realtime_shadow_compare.py`

- [ ] **步骤 1：编写失败的数据合同和跨租户测试**

```python
def test_track_contract_is_tenant_scoped() -> None:
    table = AiCallRecordingTrackModel.__table__
    unique_sets = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert table.c.tenant_id.nullable is False
    assert table.c.egress_generation.nullable is True
    assert (
        "tenant_id",
        "call_id",
        "track_role",
        "participant_identity",
    ) in unique_sets
    assert ("call_id", "track_role", "participant_identity") not in unique_sets


def test_track_migration_fails_closed_before_tenant_backfill() -> None:
    migration = MIGRATION_PATH.read_text()
    assert "ai_call_recording_track_tenant_backfill_failed" in migration
    assert "alter column tenant_id set not null" in migration
    assert "unique (tenant_id, call_id, track_role, participant_identity)" in migration
```

另加异步测试：两个租户可使用相同 `call_id + role + identity`；tenant A 的 get/update/list
不能看见或修改 tenant B；错误 claim token 不能更新 `verifying` Track。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_owner_track_recording_contract.py tests/test_ai_call_phase_b1_records.py -k 'track and tenant'
```

预期：`tenant_id`、`egress_generation`、migration 或 repository tenant 参数缺失导致 FAIL；
不得是测试收集错误。

- [ ] **步骤 3：实现 PostgreSQL migration 与 ORM**

```sql
begin;

alter table ai_call_recording_track
    add column if not exists tenant_id varchar(20),
    add column if not exists egress_generation bigint;

do $$
begin
    if exists (
        select 1
        from ai_call_recording_track track
        left join ai_call_record record on record.call_id = track.call_id
        where track.tenant_id is null
          and (record.call_id is null or record.tenant_id is null)
    ) then
        raise exception 'ai_call_recording_track_tenant_backfill_failed';
    end if;
end
$$;

update ai_call_recording_track track
set tenant_id = record.tenant_id
from ai_call_record record
where record.call_id = track.call_id and track.tenant_id is null;

alter table ai_call_recording_track alter column tenant_id set not null;
```

删除旧唯一约束和非租户索引，创建
`(tenant_id, call_id, track_role, participant_identity)` 唯一约束，以及 tenant call/egress/oss/due
索引。不得写默认租户、物理外键或 JSONB。

- [ ] **步骤 4：固定 Track repository 接口**

新增不可变 claim：

```python
@dataclass(frozen=True, slots=True)
class RecordingTrackVerificationClaim:
    track_id: int
    tenant_id: str
    call_id: str
    track_role: str
    participant_identity: str
    object_name: str | None
    started_at: datetime
    ended_at: datetime | None
    duration_ms: int | None
    verify_attempts: int
    verify_deadline_at: datetime | None
    claim_token: datetime
```

`create/get/list/update_recording_track` 必须接收 keyword-only `tenant_id`；
`claim_due_recording_track_verifications(now, limit, claim_ttl)` 使用
`FOR UPDATE SKIP LOCKED`，返回 `RecordingTrackVerificationClaim`；
`update_due_recording_track` 与
`lock_due_recording_track` 必须同时匹配 `tenant_id + track_id + status=verifying + claim_token`。

- [ ] **步骤 5：迁移现有调用方但不改变 legacy 行为**

`AiCallRecordingService` 的 participant start/stop/list/verify 全链路携带明确 tenant；
`AiCallOfflineAsrService.process_call(call_id)` 先读取 Record，tenant 缺失时 fail closed，再按
`tenant_id + call_id` 读取 Track。同步给现有测试夹具中的 Track 显式补入其 Record tenant。
诊断脚本的 Track SQL 必须 join Record 并匹配 `track.tenant_id = record.tenant_id`。现有 legacy
customer+AI/human_agent 录音与 ASR 规则保持不变。

- [ ] **步骤 6：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_owner_track_recording_contract.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_outbound_task_executor.py tests/test_ai_call_sip_outbound_dialer.py tests/test_ai_call_semantic_analysis.py tests/test_ai_call_p1_realtime_shadow_compare.py
uv run ruff check app/api/v1/ai_call/model.py app/api/v1/ai_call/crud.py app/services/ai_call/recording_service.py app/services/ai_call/offline_asr_service.py tools/ai_call_p1_realtime_shadow_compare.py tests/test_ai_call_owner_track_recording_contract.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_outbound_task_executor.py tests/test_ai_call_sip_outbound_dialer.py tests/test_ai_call_semantic_analysis.py tests/test_ai_call_p1_realtime_shadow_compare.py
git diff --check
git add docs/livekit-ai-outbound/sql/phase-i6-owner-runtime-customer-track-recording.sql app/api/v1/ai_call/model.py app/api/v1/ai_call/crud.py app/services/ai_call/recording_service.py app/services/ai_call/offline_asr_service.py tools/ai_call_p1_realtime_shadow_compare.py tests/test_ai_call_owner_track_recording_contract.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_outbound_task_executor.py tests/test_ai_call_sip_outbound_dialer.py tests/test_ai_call_semantic_analysis.py tests/test_ai_call_p1_realtime_shadow_compare.py
git commit -m "feat(ai-call): tenant-scope recording tracks"
```

### 任务 2：稳定客户 Track Effect 与数据库 claim gate

**文件：**
- 创建：`app/services/ai_call/runtime_control/customer_track.py`
- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改：`app/services/ai_call/runtime_control/runtime_service.py`
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 创建：`tests/test_ai_call_runtime_customer_track_effect.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`
- 修改：`tests/test_ai_call_runtime_effect_repository.py`
- 修改：`tests/test_ai_call_runtime_stub_handlers.py`
- 修改：`tests/test_ai_call_runtime_start_readiness.py`
- 修改：`tests/test_ai_call_runtime_startup_recovery.py`
- 创建：`tests/postgres/test_ai_call_owner_track_recording_postgres.py`

- [ ] **步骤 1：编写失败的稳定键、单开关和 Stub 测试**

```python
def test_customer_track_spec_is_stable_across_owner_fencing() -> None:
    first = _default_start_specs(
        "call-a",
        _lease(fencing_token=7),
        "runtime-a",
        entry_type="web",
        participant_identity="browser-call-a",
        provider_namespace="livekit:test",
        main_recording_enabled=True,
    )
    takeover = _default_start_specs(
        "call-a",
        _lease(fencing_token=8),
        "runtime-b",
        entry_type="web",
        participant_identity="browser-call-a",
        provider_namespace="livekit:test",
        main_recording_enabled=True,
    )
    first_track = next(s for s in first if s.effect_type == "START_TRACK_EGRESS")
    takeover_track = next(s for s in takeover if s.effect_type == "START_TRACK_EGRESS")
    assert first_track == takeover_track
    assert first_track.resource_generation == 1
    assert len(first_track.idempotency_key) <= 160
    assert len(first_track.provider_idempotency_key) <= 160
```

同时断言：现有单一 `main_recording_enabled` capability 开启时只登记 main + customer，
不登记 AI；全部 Stub 的该 capability 固定为 false；默认 Stub specs 不含 Track Effect。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest -q tests/test_ai_call_runtime_customer_track_effect.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_stub_handlers.py
```

预期：新 Effect 类型、identity helper 或 `_default_start_specs` 参数不存在导致 FAIL。

- [ ] **步骤 3：实现稳定键纯函数与 Command identity 传递**

```python
def customer_identity_digest(participant_identity: str) -> str:
    if not participant_identity or not participant_identity.strip():
        raise ValueError("customer participant identity is required")
    return hashlib.sha256(participant_identity.encode("utf-8")).hexdigest()


def customer_track_keys(call_id: str, participant_identity: str) -> tuple[str, str, str]:
    digest = customer_identity_digest(participant_identity)
    return (
        f"start:{call_id}:ctr:{digest}",
        f"egress:ctr:{call_id}:{digest}",
        f"egress:track:{call_id}:customer:{digest}",
    )
```

`CommandClaim` 增加 `participant_identity: str = ""`；`_claim_for_owner()` 在 claim 成功后从同一
Record 读取 `entry_type + participant_identity` 并返回。生产 Runtime 仅在 capability=true 时
要求非空 identity，并把它传给 `_default_start_specs()`。该函数新增 keyword-only
`participant_identity: str | None = None`，录音 capability=false 时允许省略，避免改变
DB-only Stub 调用合同。

- [ ] **步骤 4：扩展 Effect 类型和辅助 readiness**

```python
CREATE_EFFECT_TYPES = frozenset(
    {
        "CREATE_ROOM",
        "CREATE_SIP_PARTICIPANT",
        "ATTACH_AGENT_PARTICIPANT",
        "START_EGRESS",
        "START_TRACK_EGRESS",
    }
)
AUXILIARY_START_EFFECT_TYPES = frozenset({"START_EGRESS", "START_TRACK_EGRESS"})
DESTROY_EFFECT_TYPES = frozenset(
    {
        "HANGUP_SIP",
        "DISCONNECT_AGENT_PARTICIPANT",
        "STOP_EGRESS",
        "STOP_TRACK_EGRESS",
        "DELETE_ROOM",
    }
)
_DESTROY_FOR_CREATE = {
    "CREATE_ROOM": "DELETE_ROOM",
    "CREATE_SIP_PARTICIPANT": "HANGUP_SIP",
    "ATTACH_AGENT_PARTICIPANT": "DISCONNECT_AGENT_PARTICIPANT",
    "START_EGRESS": "STOP_EGRESS",
    "START_TRACK_EGRESS": "STOP_TRACK_EGRESS",
}
```

Track Start spec 使用 generation `1`，与 main 一样不进入强制 readiness。重复登记必须比较
完整 spec；`_destroy_spec()` 自动生成 phase 10 的 Track Stop。startup recovery 必须忽略
该辅助 Start，但 Room、Agent、必要 SIP 仍 fail closed。

`register_end_graph()` 必须先按全局顺序锁定并校验 Record、END Command、Owner/fencing、数据库
租约和 Command processing token，再锁定全部创建 Effect。随后只对“从未领取、无
processing token 的 PENDING Track Start”写
`FAILED + error_message=no_resource + reconcile_after=None`，再生成 Stop；不得先锁 Effect 再
反向锁 Record/Command，也不得对 APPLYING、RECONCILE_REQUIRED 或已有外部调用可能性的
Start 使用该捷径。后续 Stop 登记失败时，整个 END 图事务连同该状态转换一起回滚。

- [ ] **步骤 5：先写 PostgreSQL 就绪门禁和终态竞争测试**

新增场景：`answered_at=NULL` 时 Track Start 不可 claim；写入 answered 后可 claim；identity
或资源 digest 不匹配不可 claim；PENDING Track 与 END 并发时终态屏障获胜且 Start 原子写为
`FAILED(no_resource)`；已 APPLYING 的 Track 在 END 后仍可 reconcile，且随后必须有 Stop。

```bash
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_track_recording_postgres.py -k 'claim or terminal'
```

预期：门禁尚未实现时至少一个测试 FAIL；DSN 缺失只能报告 SKIP。

- [ ] **步骤 6：在 Effect claim CAS 内实现 ready + identity 门禁**

候选查询携带 `resource_key`；对 `START_TRACK_EGRESS` 读取 Record 的
`answered_at + participant_identity`，用纯函数验证 digest，然后在同一 UPDATE CAS 中再次
约束：Record identity 未变化、`answered_at is not null`、`terminal_requested_at is null`、
Effect `resource_key` 未变化。禁止“先查 ready，后无条件 update”。普通 Effect 和 Track
Stop 不受此门禁影响。

- [ ] **步骤 7：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_customer_track_effect.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_track_recording_postgres.py -k 'claim or terminal'
uv run ruff check app/services/ai_call/runtime_control/customer_track.py app/services/ai_call/runtime_control/command_repository.py app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/effect_repository.py tests/test_ai_call_runtime_customer_track_effect.py
git diff --check
git add app/services/ai_call/runtime_control/customer_track.py app/services/ai_call/runtime_control/command_repository.py app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/effect_repository.py tests/test_ai_call_runtime_customer_track_effect.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git commit -m "feat(ai-call): gate customer track effects"
```

### 任务 3：Effect 事务内客户 Track 投影

**文件：**
- 创建：`app/services/ai_call/runtime_control/track_recording_repository.py`
- 修改：`app/services/ai_call/runtime_control/effect_repository.py`
- 创建：`tests/test_ai_call_runtime_track_recording_repository.py`
- 修改：`tests/postgres/test_ai_call_owner_track_recording_postgres.py`

- [ ] **步骤 1：编写失败的状态映射与隔离测试**

参数化验证：Start present → `recording`；Start uncertain → `starting`；Start permanent no
resource → `failed`；Stop accepted → `stopping`；Stop terminal → `verifying`；从未创建的
Stop terminal 不伪造 `verifying`。另测 `completed/failed` 不倒退、安全错误摘要不泄密、
main Recording 行完全不变。

```bash
uv run pytest -q tests/test_ai_call_runtime_track_recording_repository.py
```

预期：projector 不存在导致 FAIL。

- [ ] **步骤 2：实现单一职责 projector**

```python
class OwnerTrackRecordingRepository:
    async def project(
        self,
        *,
        record: AiCallRecordModel,
        effect: AiCallRuntimeEffectModel,
        source_effect: AiCallRuntimeEffectModel | None,
        observation: ProviderObservation,
        now: datetime,
    ) -> AiCallRecordingTrackModel | None:
        if effect.effect_type not in {"START_TRACK_EGRESS", "STOP_TRACK_EGRESS"}:
            return None
        # 验证 customer resource key 与 Record identity digest；按
        # tenant_id + call_id + customer + participant_identity FOR UPDATE。
        # 只应用单调状态映射，不执行 Provider/OSS I/O。
```

创建 Track 时写 `tenant_id`、完整 identity、`egress_generation=1`、稳定 object name；Stop
必须使用 `source_create_effect_id`，缺失 source fail closed。该 repository 不处理主录音类型。

- [ ] **步骤 3：接入原子 submit**

`RuntimeEffectRepository.submit()` 在锁定 Record 与 Effect、校验 Owner/fencing/数据库租约和
processing token、应用 Effect 状态机后，依次调用 main projector 与 Track projector，最后
统一 flush。任何 projector 异常使 Effect 和两个业务投影全部回滚；不得捕获后继续提交。

- [ ] **步骤 4：运行隔离 PostgreSQL 原子性测试**

新增：Effect APPLIED 与 Track recording 原子提交；projector 异常回滚 Effect；旧 Owner、
旧 fencing、过期租约、错误 token、跨租户 submit 均影响 0 行；两个 Runtime 只能保留一个
Track；重复 observation 不产生第二行。

```bash
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_track_recording_postgres.py -k 'projection or stale or concurrent'
uv run pytest -q tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_track_recording_repository.py
```

- [ ] **步骤 5：静态检查并提交**

```bash
uv run ruff check app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/track_recording_repository.py tests/test_ai_call_runtime_track_recording_repository.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git diff --check
git add app/services/ai_call/runtime_control/effect_repository.py app/services/ai_call/runtime_control/track_recording_repository.py tests/test_ai_call_runtime_track_recording_repository.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git commit -m "feat(ai-call): project fenced customer tracks"
```

### 任务 4：LiveKit Participant Egress 执行与 reconcile-only 恢复

**文件：**
- 修改：`app/services/ai_call/runtime_control/livekit_provider.py`
- 修改：`tests/test_ai_call_runtime_livekit_provider.py`
- 修改：`tests/test_ai_call_runtime_customer_track_effect.py`

- [ ] **步骤 1：编写失败的 Fake Egress 测试**

Fake manager 必须分别记录 `participant_start_calls`、`find_calls`、`stop_calls`。测试固定验证：
首次 Start 使用 Record customer identity；缺 OSS 配置外部调用为 0；Start 超时后恢复轮次只
find、`participant_start_calls == 1`；响应丢失可按 Room + 稳定对象名找到同一 Egress；Stop
无 reference 时先 find；Stop accepted 非终态；Stop terminal 才确认。

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py -k 'customer_track'
```

预期：Provider 不支持新类型或 protocol 方法缺失导致 FAIL。

- [ ] **步骤 2：扩展资源快照与 Egress protocol**

`RuntimeProviderResource` 增加 `egress_scope: str | None`。resolver 对 Track Start/Stop：验证
resource digest、读取完整 customer identity；Stop 从 source Effect 读取持久化
`provider_reference`。Participant object name 由持有 Egress manager 的 Provider 层构造，
resolver 不读取录音格式配置。`RuntimeEgressManager` protocol 增加已存在于实现中的：

```python
def build_participant_object_name(
    self, *, call_id: str, track_role: str, participant_identity: str
) -> str: ...

async def start_participant_audio_recording(
    self,
    *,
    room_name: str,
    call_id: str,
    track_role: str,
    participant_identity: str,
    oss_config: dict,
) -> object: ...
```

- [ ] **步骤 3：实现 Start/Query/Stop 分支**

`apply()` 将 `START_TRACK_EGRESS` 路由到 Participant Start，将 `STOP_TRACK_EGRESS` 路由到
通用 Stop。首次非 reconcile claim 才调用 `start_participant_audio_recording()`；
reconcile-only 只用 `get_egress(reference)` 或
`find_room_audio_recording(room, participant_object_name)`。只有确认存在且有 egress ID 才
`RESOURCE_PRESENT`；Stop 只有终态或稳定查询不存在才 `TERMINAL_CONFIRMED`。Provider 的
not-found/timeout 分支必须把 `STOP_TRACK_EGRESS` 与现有 Stop 一样安全映射，不能落入未知
Effect 异常。

- [ ] **步骤 4：统一总开关能力声明**

继续使用现有 `LiveKitRuntimeProvider.main_recording_enabled =
bool(settings.AI_CALL_RECORDING_ENABLED)` 作为 Owner Runtime 唯一录音 capability；true 时同时
登记 main + customer，false 时两者都不登记。不得增加
`customer_track_recording_enabled`。Owner Runtime 不读取
`AI_CALL_PARTICIPANT_RECORDING_ENABLED`，legacy 接线暂不删除。

- [ ] **步骤 5：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_runtime_customer_track_effect.py
uv run ruff check app/services/ai_call/runtime_control/livekit_provider.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_runtime_customer_track_effect.py
git diff --check
git add app/services/ai_call/runtime_control/livekit_provider.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_runtime_customer_track_effect.py
git commit -m "feat(ai-call): execute recoverable customer track egress"
```

### 任务 5：Web/SIP 就绪、END 图与进程角色隔离

**文件：**
- 创建：`app/services/ai_call/runtime_control/customer_media_repository.py`
- 修改：`app/api/v1/ai_call/service.py`
- 修改：`app/services/ai_call/record_service.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`
- 修改：`tests/test_ai_call_runtime_effect_repository.py`
- 修改：`tests/postgres/test_ai_call_owner_track_recording_postgres.py`

- [ ] **步骤 1：编写失败的 Web ready 角色隔离测试**

构造 `owner_command_v1` Web Record 和 spy RecordingService。上报 `browser_ready` 后断言：
Record `answered_at` 只写一次；状态进入 connected；spy 的
`start_session_participant_recordings` 调用次数为 0；Track Start 仍由 Runtime 后续 claim。
终态 Record 的迟到 ready 影响 0 行。现有 legacy 测试继续断言 customer+AI 直接录音行为
不变。

```bash
uv run pytest -q tests/test_ai_call_phase_b1_records.py -k 'browser_ready and recording'
```

预期：当前 Owner 模式仍会调用 legacy helper 或缺少数据库 ready repository，测试 FAIL。

- [ ] **步骤 2：实现 fenced-by-terminal Web ready repository**

```python
class OwnerCustomerMediaRepository:
    async def mark_browser_ready(self, *, tenant_id: str, call_id: str) -> bool:
        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == tenant_id,
                AiCallRecordModel.call_id == call_id,
            )
            .with_for_update()
        )
        now = await read_database_time(self._session)
        if (
            record is None
            or record.runtime_control_mode != "owner_command_v1"
            or record.terminal_requested_at is not None
            or str(record.status).lower() in {"ending", "completed", "failed"}
        ):
            return False
        record.answered_at = record.answered_at or now
        record.status = "connected"
        await self._session.flush()
        await publish_control_wakeup(self._session)
        return True
```

API 先调用现有 orchestrator 完成 browser event 校验，再从 Record 取得非空 tenant。Owner
模式只调用上述 repository；Legacy 模式才在校验成功后调用
`_start_browser_ready_recording_tracks()` 和原 `mark_answered()`。不得接受请求覆盖 tenant，
也不得在 API 进程执行 Owner Track Provider I/O。

`AiCallRecordService.mark_owner_customer_ready(tenant_id, call_id)` 只委托
`OwnerCustomerMediaRepository(self.repository.db)`，让 API service 不直接依赖 SQLAlchemy
session；该方法返回 bool，不把“ready 写入成功”解释成 Track 已启动。

- [ ] **步骤 3：验证 SIP ready 与旧 fencing**

不新增第二套 SIP 写入口。扩展现有 `record_sip_connected()` PostgreSQL 测试：当前
Owner/fencing 写 answered 后 Track 可 claim；旧 fencing、过期 Record/Worker 租约、终态
Record 均返回 false，Track 仍不可 claim；该方法不调用 Egress。

- [ ] **步骤 4：验证完整 END graph 与竞争序列**

测试 graph 同时包含 main `STOP_EGRESS` 和 customer `STOP_TRACK_EGRESS`；DELETE_ROOM 的
dependency source 集合覆盖 Room 之外全部 create Effect。未领取的 PENDING customer Start
必须在同一事务先变为 `FAILED(no_resource)`。缺失 Track Stop、缺失 dependency 行、Stop 仅
accepted 时 DELETE_ROOM claim 均为 none。

PostgreSQL 竞争测试固定只接受两种结果：ready 先提交且 Track 已 claim，则 END 必须 Stop；
END 先建立终态，则 ready/Track Start 影响 0 行。禁止 Room 删除后晚创建。

- [ ] **步骤 5：验证 cleanup 与本地 handle**

Track Stop `APPLIED` 后，`mark_cleanup_clean()` 可在 Track 仍 `verifying` 时释放 Owner、容量
并触发现有 `_clear_owner_tracking()` 停止本地 handle。Stop 未终态时不得 clean。

- [ ] **步骤 6：验证并提交**

```bash
uv run pytest -q tests/test_ai_call_phase_b1_records.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py -k 'browser_ready or customer_track or cleanup or end_graph'
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_track_recording_postgres.py
uv run ruff check app/services/ai_call/runtime_control/customer_media_repository.py app/api/v1/ai_call/service.py app/services/ai_call/record_service.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git diff --check
git add app/services/ai_call/runtime_control/customer_media_repository.py app/api/v1/ai_call/service.py app/services/ai_call/record_service.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_effect_repository.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git commit -m "feat(ai-call): close customer track lifecycle"
```

### 任务 6：Track OSS 独立恢复与完整回归

**文件：**
- 修改：`app/services/ai_call/recording_service.py`
- 创建：`tests/test_ai_call_owner_track_recording_reconcile.py`
- 修改：`tests/test_ai_call_owner_recording_reconcile.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`
- 修改：`tests/postgres/test_ai_call_owner_track_recording_postgres.py`

- [ ] **步骤 1：编写失败的独立恢复测试**

测试固定覆盖：Track Stop terminal 后为 `verifying`；Runtime Owner 已释放时 Reconciler 仍可
完成；对象第一次不可见、第二次可见只登记一个 `sys_oss`；错误 claim token 更新 0 行；
验证截止写 `failed`；Track Reconciler 不锁 Record/Effect、不重新调用 Start/Stop Egress。

```bash
uv run pytest -q tests/test_ai_call_owner_track_recording_reconcile.py
```

- [ ] **步骤 2：实现 claim snapshot 驱动的 Track verify**

`reconcile_due_recordings()` 先 claim main，再按剩余 limit claim Track；每次外部 OSS 查询前
提交 claim 事务。`_verify_participant_recording()` 只接收
`RecordingTrackVerificationClaim`，所有写入使用
`tenant_id + track_id + claim_token` CAS；完成对象注册前再次
`lock_due_recording_track()`，锁失效则不登记 OSS、不更新 Track。

- [ ] **步骤 3：验证离线 ASR 门禁**

`is_ready_for_offline_asr(tenant_id, call_id)` 只查看同租户 Track；`verifying/stopping/recording`
阻止入队，`completed/failed` 允许继续。客户 Track failed 不改变通话终态，但必须保留可见
错误摘要。

- [ ] **步骤 4：运行受影响单元与隔离 PostgreSQL**

```bash
uv run pytest -q tests/test_ai_call_owner_track_recording_contract.py tests/test_ai_call_runtime_customer_track_effect.py tests/test_ai_call_runtime_track_recording_repository.py tests/test_ai_call_runtime_livekit_provider.py tests/test_ai_call_owner_track_recording_reconcile.py tests/test_ai_call_owner_recording_reconcile.py tests/test_ai_call_runtime_effect_repository.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_startup_recovery.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_phase_b1_records.py tests/test_ai_call_outbound_task_executor.py tests/test_ai_call_sip_outbound_dialer.py tests/test_ai_call_semantic_analysis.py tests/test_ai_call_p1_realtime_shadow_compare.py
AI_CALL_TEST_POSTGRES_DSN="$AI_CALL_TEST_POSTGRES_DSN" uv run pytest -q tests/postgres/test_ai_call_owner_track_recording_postgres.py tests/postgres/test_ai_call_owner_recording_postgres.py tests/postgres/test_ai_call_runtime_control_postgres.py
```

必须读取实际通过/失败/跳过数量；DSN 缺失不得声称 PostgreSQL 通过。

- [ ] **步骤 5：静态验证并提交**

```bash
uv run ruff check .
codegraph sync
codegraph status
git diff --check
git add app/services/ai_call/recording_service.py tests/test_ai_call_owner_track_recording_reconcile.py tests/test_ai_call_owner_recording_reconcile.py tests/test_ai_call_phase_b1_records.py tests/postgres/test_ai_call_owner_track_recording_postgres.py
git commit -m "feat(ai-call): recover customer track verification"
```

## 规格覆盖矩阵

| 合同 | 实现任务 | 主要验证 |
| --- | --- | --- |
| `CTR-01` | 任务 2、4 | 单一 capability 开关、Stub 不登记 |
| `CTR-02` | 任务 2 | specs 只有 main + customer，无 AI/human |
| `CTR-03` | 任务 2 | answered 前不可 claim 的 PostgreSQL 测试 |
| `CTR-04` | 任务 5 | Web/SIP 只写 ready，Provider 调用次数为 0 |
| `CTR-05` | 任务 1、2、3 | tenant 唯一约束、稳定键、重复投影单行 |
| `CTR-06` | 任务 4 | Start 超时后只 find，不重复 Start |
| `CTR-07` | 任务 3 | Effect + Track 原子提交及旧 Owner 0 行 |
| `CTR-08` | 任务 2、5 | 未领取 PENDING 先静默，全部 Start 都有 Stop |
| `CTR-09` | 任务 5 | 完整 prerequisite 与 DELETE_ROOM fail closed |
| `CTR-10` | 任务 2 | auxiliary readiness fail-open，必要资源仍 fail closed |
| `CTR-11` | 任务 6 | Stop terminal 与 OSS completed 分离 |
| `CTR-12` | 任务 1、3、6 | Track CRUD、投影、claim、更新全租户隔离 |
| `CTR-13` | 任务 5 | Owner Web ready 不调用 legacy RecordingService |
| `CTR-14` | 任务 3、6 | 安全错误摘要与泄密回归测试 |

### 任务 7：冻结前证据与外部边界

**文件：** 不修改业务文件，只验证。

- [ ] **步骤 1：核对分支、提交和受保护文件**

```bash
pwd
git status --short --branch
git log --oneline --decorate -10
git diff --check
```

预期：目标 worktree 与分支正确，只保留既有 `.playwright-cli/` 和
`env/.env.dev.bak-before-local-outbound-20260727`；不得暂存或清理它们。

- [ ] **步骤 2：重复任务 6 的完整安全验证**

重新运行任务 6 的 unit、隔离 PostgreSQL、Ruff、CodeGraph 和 diff 检查。不得依赖上一轮
输出，不得连接真实 LiveKit、SIP、OSS、Provider，不启动或重启业务服务，不拨打电话。

- [ ] **步骤 3：逐项核对合同**

按 `CTR-01` 至 `CTR-14` 输出“测试/SQL/代码证据 → 合同”的映射。若任何合同只有设计文档
而没有实现或测试证据，不得声明切片冻结。

- [ ] **步骤 4：交付范围**

只声明“混合主录音 + 客户分轨”的 Owner Runtime 代码和隔离验证。AI 分轨、人工坐席分轨、
新的离线 ASR/语义/跟进功能、前端重做、Redis 和真实外部验收继续作为后续独立边界，不能
作为本切片通过或失败的替代证据。
