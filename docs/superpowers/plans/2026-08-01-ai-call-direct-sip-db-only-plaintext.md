# AI Call Direct SIP DB-only 明文号码实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将现有 `/ai-call/sip-sessions` 接入 PostgreSQL Owner Command 控制面，以专用 Record 列永久保存明文号码、向所有前端接口仅返回脱敏号码，并用确定性 DB-only Provider Stub 验证 Direct SIP 的 START/END 恢复闭环。

**架构：** API 在认证租户和 `Idempotency-Key` 门禁后规范化号码，Record 与 `START_CALL` 在同一事务持久化；完整号码只进入 `ai_call_record.callee_phone_number`，Command、Effect、通知、日志和响应均不携带完整号码。Runtime 根据 Record 的 `entry_type` 选择 Web 或 Direct SIP Effect 集合，PostgreSQL Owner、租约和 fencing 仍是唯一执行授权，Provider Stub 永不连接网络。

**技术栈：** Python 3.12、FastAPI、Pydantic v2、SQLAlchemy Async、PostgreSQL 16、pytest/anyio、ruff、CodeGraph

---

## 0. 规格、范围和工作树保护

权威规格：

- `docs/superpowers/specs/2026-08-01-ai-call-direct-sip-db-only-plaintext-design.md`
- `docs/superpowers/specs/2026-07-31-ai-call-single-owner-runtime-command-design.md`
- `docs/superpowers/specs/2026-08-01-ai-call-16-2-scope-simplification-design.md`

本计划只实现 `direct_sip` 的 DB-only Stub。禁止连接真实 LiveKit、SIP、Linphone、
Egress、Provider 或 Redis，禁止拨号，禁止启动或重启业务服务。

工作树已有用户改动。以下文件存在既有脏内容：

- `app/api/v1/ai_call/model.py`
- `app/api/v1/ai_call/controller.py`
- `app/api/v1/ai_call/crud.py`
- `app/api/v1/ai_call/service.py`
- `app/services/ai_call/record_service.py`
- 以及 `git status --short` 当前列出的其他文件

本计划只允许对 `model.py` 的号码字段区域和 `controller.py` 的 `/sip-sessions` 路由区域
做外科手术式修改，并只暂存本切片 hunk。不得修改 `crud.py`、`service.py`、
`record_service.py`，不得清理、恢复、覆盖或顺手提交任何既有改动。

### 计划文件结构

**创建：**

- `app/services/ai_call/runtime_control/direct_sip_phone.py`：号码规范化、脱敏、hash 和
  payload 明文防扩散。
- `tests/test_ai_call_direct_sip_phone.py`：号码值对象与 payload 防扩散单元测试。
- `tests/test_ai_call_direct_sip_runtime_entry.py`：`/sip-sessions` 新旧路径互斥、认证、
  幂等和脱敏响应测试。
- `docs/livekit-ai-outbound/sql/phase-i2-direct-sip-db-only-plaintext.sql`：只新增 Record
  明文号码列的幂等 PostgreSQL migration。

**修改：**

- `app/api/v1/ai_call/model.py`：在 Record 号码字段区增加唯一明文列。
- `app/api/v1/ai_call/schema.py`：增加 Direct SIP 异步响应模型，并把通用 Runtime 请求
  从密文字段改为显式号码字段。
- `app/api/v1/ai_call/controller.py`：给 `/sip-sessions` 增加 Owner Command 分支。
- `app/api/v1/ai_call/runtime_control_controller.py`：通用 Runtime 入口传递显式号码，
  不再接受 KMS 参数。
- `app/config/setting.py`：增加 Direct SIP 排队截止配置。
- `app/services/ai_call/runtime_control/command_repository.py`：指纹、原子 Record/Command
  写入、entry-aware claim。
- `app/services/ai_call/runtime_control/entry_start_service.py`：Direct SIP 明文号码合同和
  非敏感 payload 门禁。
- `app/services/ai_call/runtime_control/runtime_service.py`：按 entry 生成 Web/Direct SIP
  Effect 集合。
- `app/services/ai_call/runtime_control/provider_stub.py`：增加支持 SIP create/destroy 的
  DB-only 复合 Stub。
- `app/services/ai_call/runtime_control/start_readiness_repository.py`：允许 Direct SIP 在
  三个创建 Effect 全部 `APPLIED` 后提交 readiness。
- `app/services/ai_call/runtime_control/lifecycle.py`：Runtime 生命周期使用复合 DB-only
  Stub。
- `tests/test_ai_call_runtime_command_repository.py`
- `tests/test_ai_call_runtime_models.py`
- `tests/test_ai_call_runtime_entry_start_service.py`
- `tests/test_ai_call_runtime_entry_controller.py`
- `tests/test_ai_call_runtime_stub_handlers.py`
- `tests/test_ai_call_runtime_start_readiness.py`
- `tests/test_ai_call_runtime_lifecycle.py`
- `tests/postgres/test_ai_call_runtime_control_postgres.py`

## 任务 1：建立唯一的 Direct SIP 号码值对象

**文件：**

- 创建：`app/services/ai_call/runtime_control/direct_sip_phone.py`
- 创建：`tests/test_ai_call_direct_sip_phone.py`

- [ ] **步骤 1：编写号码规范化和脱敏失败测试**

```python
import pytest

from app.services.ai_call.runtime_control.direct_sip_phone import (
    DirectSipPhoneError,
    prepare_direct_sip_phone,
)


def test_prepare_direct_sip_phone_keeps_plaintext_and_builds_mask_and_hash() -> None:
    phone = prepare_direct_sip_phone(" 13812345678 ")

    assert phone.plaintext == "13812345678"
    assert phone.masked == "138****5678"
    assert phone.fingerprint.startswith("sha256:")
    assert "13812345678" not in phone.fingerprint


@pytest.mark.parametrize("value", ["", "138-1234-5678", "abc", "+12"])
def test_prepare_direct_sip_phone_rejects_non_canonical_number(value: str) -> None:
    with pytest.raises(DirectSipPhoneError):
        prepare_direct_sip_phone(value)
```

- [ ] **步骤 2：编写 payload 防扩散失败测试**

```python
from app.services.ai_call.runtime_control.direct_sip_phone import (
    payload_contains_phone,
)


def test_payload_contains_phone_checks_nested_business_params() -> None:
    assert payload_contains_phone(
        {"business_params": {"contact": "13812345678"}},
        "13812345678",
    )
    assert not payload_contains_phone(
        {"business_params": {"customerName": "张三"}},
        "13812345678",
    )
```

- [ ] **步骤 3：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_direct_sip_phone.py -q
```

预期：FAIL，`ModuleNotFoundError` 指向 `direct_sip_phone`。

- [ ] **步骤 4：实现最小号码值对象**

```python
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_PHONE_PATTERN = re.compile(r"^\+?\d{5,20}$")


class DirectSipPhoneError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DirectSipPhone:
    plaintext: str
    masked: str
    fingerprint: str


def prepare_direct_sip_phone(value: str) -> DirectSipPhone:
    plaintext = str(value or "").strip()
    if not _PHONE_PATTERN.fullmatch(plaintext):
        raise DirectSipPhoneError("Direct SIP 被叫号码格式不合法")
    digits = "".join(character for character in plaintext if character.isdigit())
    masked = "***" if len(digits) <= 7 else f"{digits[:3]}****{digits[-4:]}"
    digest = hashlib.sha256(digits.encode("utf-8")).hexdigest()
    return DirectSipPhone(
        plaintext=plaintext,
        masked=masked,
        fingerprint=f"sha256:{digest}",
    )


def payload_contains_phone(value: object, phone_number: str) -> bool:
    if isinstance(value, str):
        return value.strip() == phone_number
    if isinstance(value, Mapping):
        return any(payload_contains_phone(item, phone_number) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(payload_contains_phone(item, phone_number) for item in value)
    return False
```

- [ ] **步骤 5：运行测试确认通过**

运行：`uv run pytest tests/test_ai_call_direct_sip_phone.py -q`

预期：PASS。

- [ ] **步骤 6：提交号码值对象**

```bash
git add app/services/ai_call/runtime_control/direct_sip_phone.py \
  tests/test_ai_call_direct_sip_phone.py
git diff --cached --check
git commit -m "feat(ai-call): 增加 Direct SIP 号码值对象"
```

## 任务 2：原子持久化明文、脱敏值和 hash

**文件：**

- 创建：`docs/livekit-ai-outbound/sql/phase-i2-direct-sip-db-only-plaintext.sql`
- 修改：`app/api/v1/ai_call/model.py:124-133`（已有脏文件，只暂存新增字段 hunk）
- 修改：`app/services/ai_call/runtime_control/command_repository.py:60-145,250-315`
- 修改：`tests/test_ai_call_runtime_command_repository.py:18-80,100-130`
- 修改：`tests/test_ai_call_runtime_models.py:20-65`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py:72-260`

- [ ] **步骤 1：编写 migration 失败测试**

在 PostgreSQL 测试中增加独立路径和测试：

```python
DIRECT_SIP_MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-i2-direct-sip-db-only-plaintext.sql"
)


async def test_direct_sip_plaintext_migration_is_idempotent() -> None:
    _reset_legacy_schema()
    _execute_script(MIGRATION_PATH.read_text(encoding="utf-8"))
    migration_sql = DIRECT_SIP_MIGRATION_PATH.read_text(encoding="utf-8")
    _execute_script(migration_sql)
    _execute_script(migration_sql)

    engine = create_async_engine(_async_dsn(), isolation_level="READ COMMITTED")
    try:
        async with engine.connect() as connection:
            column = (
                await connection.execute(
                    text(
                        "select data_type, character_maximum_length, is_nullable "
                        "from information_schema.columns "
                        "where table_schema=current_schema() "
                        "and table_name='ai_call_record' "
                        "and column_name='callee_phone_number'"
                    )
                )
            ).one()
        assert tuple(column) == ("character varying", 32, "YES")
    finally:
        await engine.dispose()
```

- [ ] **步骤 2：编写 repository 持久化和指纹失败测试**

```python
def test_direct_sip_start_fingerprint_changes_with_phone_number() -> None:
    first = StartCallIntent(
        tenant_id="tenant-a",
        entry_type="direct_sip",
        idempotency_key="sip:1",
        payload={"voice": "v1"},
        callee_phone_number="13812345678",
        callee_phone_number_masked="138****5678",
        callee_phone_number_hash="sha256:first",
    )
    second = replace(first, callee_phone_number="13912345678")

    assert start_call_request_fingerprint(first) != start_call_request_fingerprint(second)


@pytest.mark.anyio
async def test_direct_sip_start_stores_phone_only_on_record() -> None:
    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    ids = iter((101, 102, 103))
    session = _FakeSession()
    repository = RuntimeCommandRepository(
        session,
        id_generator=lambda: next(ids),
        database_clock=lambda _session: _constant_time(now),
    )

    await repository.create_start_call(
        StartCallIntent(
            tenant_id="tenant-a",
            entry_type="direct_sip",
            idempotency_key="start:direct-sip:persistence",
            payload={"voice": "v1"},
            callee_phone_number="13812345678",
            callee_phone_number_masked="138****5678",
            callee_phone_number_hash="sha256:stable",
        )
    )

    record, command = session.rows
    assert record.callee_phone_number == "13812345678"
    assert record.callee_phone_number_masked == "138****5678"
    assert record.callee_phone_number_hash == "sha256:stable"
    assert "13812345678" not in (command.payload_json or "")
    assert command.sensitive_payload_ciphertext is None
    assert command.payload_key_version is None
```

在模型合同测试中增加：

```python
def test_record_has_direct_sip_plaintext_column_without_plaintext_index() -> None:
    columns = inspect(AiCallRecordModel).columns

    assert columns.callee_phone_number.nullable is True
    assert columns.callee_phone_number.type.length == 32
    assert all(
        "callee_phone_number" not in tuple(index.columns.keys())
        for index in AiCallRecordModel.__table__.indexes
    )
```

- [ ] **步骤 3：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_models.py -q
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_direct_sip_plaintext_migration_is_idempotent -q
```

预期：单元测试因 `StartCallIntent` 没有号码字段失败；PostgreSQL 测试因 migration 文件不存在失败。

- [ ] **步骤 4：新增幂等 migration**

```sql
begin;

alter table ai_call_record
    add column if not exists callee_phone_number varchar(32);

commit;
```

- [ ] **步骤 5：给 ORM Record 增加唯一明文列**

在现有 hash/masked 字段旁增加：

```python
callee_phone_number: Mapped[str | None] = mapped_column(
    String(32),
    nullable=True,
    comment="Direct SIP 被叫号码明文",
)
```

不要调整或格式化 `model.py` 的其他内容。

- [ ] **步骤 6：扩展 `StartCallIntent` 和原子创建**

增加三个可空字段：

```python
callee_phone_number: str | None = None
callee_phone_number_masked: str | None = None
callee_phone_number_hash: str | None = None
```

`start_call_request_fingerprint()` 增加独立的
`"callee_phone_number": request.callee_phone_number`。`create_start_call()` 在
`direct_sip` 下要求三个字段均非空、KMS 两字段均为空、payload 不含完整号码；Web 下
要求三个号码字段均为空。创建 Record 时写入三字段，并使用
`participant_identity=f"sip-{call_id}"`；Command 仍只保存非敏感 payload。

- [ ] **步骤 7：运行定向测试确认通过**

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_models.py -q
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_direct_sip_plaintext_migration_is_idempotent -q
```

预期：全部 PASS。

- [ ] **步骤 8：只暂存 `model.py` 新字段 hunk 并提交**

```bash
git add docs/livekit-ai-outbound/sql/phase-i2-direct-sip-db-only-plaintext.sql \
  app/services/ai_call/runtime_control/command_repository.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_models.py \
  tests/postgres/test_ai_call_runtime_control_postgres.py
git add -p app/api/v1/ai_call/model.py
git diff --cached --check
git diff --cached --name-status
git commit -m "feat(ai-call): 持久化 Direct SIP 明文号码"
```

在 `git add -p` 中只接受 `callee_phone_number` 新字段，不接受现有 runtime-control 或其他
用户 hunk。提交后再次运行 `git diff -- app/api/v1/ai_call/model.py`，确认既有脏改动仍在。

## 任务 3：把 Direct SIP 入口合同改为明文 Record、非敏感 Command

**文件：**

- 修改：`app/services/ai_call/runtime_control/entry_start_service.py:20-110`
- 修改：`app/api/v1/ai_call/schema.py:120-165`
- 修改：`app/api/v1/ai_call/runtime_control_controller.py:85-130`
- 修改：`tests/test_ai_call_runtime_entry_start_service.py:115-180`
- 修改：`tests/test_ai_call_runtime_entry_controller.py:220-330`

- [ ] **步骤 1：替换 KMS 合同失败测试**

将旧的“必须提供密文”测试替换为：

```python
@pytest.mark.anyio
async def test_direct_sip_builds_plain_record_fields_without_sensitive_command_payload() -> None:
    repository = _FakeRepository([])
    service = RuntimeEntryStartService(
        settings=_settings("direct_sip"),
        repository=repository,
    )

    result = await service.submit(
        StartEntryRequest(
            tenant_id="tenant-a",
            entry_type="direct_sip",
            idempotency_key="start:sip:1",
            payload={"voice": "v1", "business_params": {"customerName": "张三"}},
            callee_phone_number="13812345678",
        )
    )

    assert result == "command-snapshot"
    intent = repository.requests[-1]
    assert intent.callee_phone_number == "13812345678"
    assert intent.callee_phone_number_masked == "138****5678"
    assert intent.callee_phone_number_hash.startswith("sha256:")
    assert "13812345678" not in json.dumps(intent.payload, ensure_ascii=False)
    assert intent.sensitive_payload_ciphertext is None
    assert intent.payload_key_version is None
```

再增加嵌套 `business_params` 重复完整号码必须抛 `RuntimeEntryStartError` 的测试。

- [ ] **步骤 2：运行测试确认旧 KMS 行为失败**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py -q
```

预期：FAIL，旧服务仍要求密文且请求模型没有 `callee_phone_number`。

- [ ] **步骤 3：修改入口 DTO 和服务**

`StartEntryRequest` 删除 `sensitive_payload_ciphertext`、`payload_key_version`，增加：

```python
callee_phone_number: str | None = None
```

Direct SIP 分支调用 `prepare_direct_sip_phone()`，拒绝 payload 递归包含完整号码，并把
`plaintext/masked/fingerprint` 写入 `StartCallIntent` 的专用字段。Web 若携带号码则抛
`RuntimeEntryStartError`，避免入口类型混淆。

- [ ] **步骤 4：修改通用 Runtime 请求**

`RuntimeStartCallRequest` 删除 KMS 两个请求字段并增加：

```python
callee_phone_number: str | None = Field(default=None, min_length=5, max_length=32)
```

`create_runtime_start_call_controller()` 只把该显式字段传给 `StartEntryRequest`。响应模型
不增加号码字段，通用 Runtime API 不回显完整号码。

- [ ] **步骤 5：运行定向测试确认通过**

```bash
uv run pytest tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py -q
```

预期：全部 PASS，旧 KMS 测试已被新明文合同替代。

- [ ] **步骤 6：提交入口合同**

```bash
git add app/services/ai_call/runtime_control/entry_start_service.py \
  app/api/v1/ai_call/schema.py \
  app/api/v1/ai_call/runtime_control_controller.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py
git diff --cached --check
git commit -m "feat(ai-call): 改造 Direct SIP START_CALL 入口合同"
```

## 任务 4：迁移 `/sip-sessions` 并保证所有前端只见星号号码

**文件：**

- 修改：`app/config/setting.py:179-195`
- 修改：`app/api/v1/ai_call/schema.py:105-145`
- 修改：`app/api/v1/ai_call/controller.py:225-260`（已有脏文件，只暂存路由 hunk）
- 创建：`tests/test_ai_call_direct_sip_runtime_entry.py`

- [ ] **步骤 1：编写 Owner/legacy 互斥失败测试**

新测试至少包含：

```python
@pytest.mark.anyio
async def test_direct_sip_owner_mode_returns_masked_202_without_legacy_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy_service = SimpleNamespace(create_sip_session=AsyncMock())
    response = await controller.create_sip_session_controller(
        service=legacy_service,
        request=_request(callee_phone_number="13812345678"),
        auth=_auth("tenant-a"),
        idempotency_key="sip:req:1",
    )

    body = json.loads(response.body)
    assert response.status_code == 202
    assert body["data"]["calleePhoneNumberMasked"] == "138****5678"
    assert "13812345678" not in response.body.decode("utf-8")
    legacy_service.create_sip_session.assert_not_awaited()
```

同时覆盖：缺租户为 `401`、缺幂等键为 `400/IDEMPOTENCY_KEY_REQUIRED`、相同请求复用、
更换号码为 `409/IDEMPOTENCY_CONFLICT`、legacy 模式仍只调用原服务一次。

- [ ] **步骤 2：运行测试确认失败**

运行：`uv run pytest tests/test_ai_call_direct_sip_runtime_entry.py -q`

预期：FAIL，当前 `/sip-sessions` 没有 `auth`、`Idempotency-Key` 或 Owner 分支。

- [ ] **步骤 3：增加 Direct SIP 异步响应模型和配置**

```python
class RuntimeDirectSipStartCallOut(RuntimeStartCallOut):
    callee_phone_number_masked: str
```

在 Settings 增加：

```python
AI_CALL_DIRECT_SIP_ALLOCATION_TIMEOUT_SECONDS: float = 30.0
```

- [ ] **步骤 4：实现 `/sip-sessions` Owner 分支**

路由响应 union 为 `CreateSipSessionOut | RuntimeDirectSipStartCallOut`。函数增加认证依赖和
`Idempotency-Key` Header。`direct_sip` 启用时：

1. 从 `auth.user.tenant_id` 读取租户；
2. 校验幂等键；
3. 调用 `RuntimeEntryStartService.submit()`，完整号码只传专用字段；
4. 非敏感 payload 只含 voice、business_id、scene_code、business_params、
   ringing_timeout_seconds；
5. 返回 `202`、持久标识和 `calleePhoneNumberMasked`；
6. 不调用 `AiCallService.create_sip_session()`。

未启用时保持原 legacy 调用和同步响应，不双写 Record/Command。

- [ ] **步骤 5：验证所有前端序列化只见脱敏号码**

测试同时断言：

```python
serialized = response.body.decode("utf-8")
assert "138****5678" in serialized
assert "13812345678" not in serialized
assert "callee_phone_number" not in serialized
assert '"calleePhoneNumber":' not in serialized
```

普通 Record/Command/bootstrap schema 均没有明文字段，不修改已有列表或详情序列化代码。

- [ ] **步骤 6：运行测试确认通过**

```bash
uv run pytest tests/test_ai_call_direct_sip_runtime_entry.py \
  tests/test_ai_call_web_runtime_entry.py \
  tests/test_ai_call_runtime_entry_controller.py -q
```

预期：全部 PASS，Web 和 legacy Direct SIP 行为不回归。

- [ ] **步骤 7：只暂存 `/sip-sessions` hunk 并提交**

```bash
git add app/config/setting.py app/api/v1/ai_call/schema.py \
  tests/test_ai_call_direct_sip_runtime_entry.py
git add -p app/api/v1/ai_call/controller.py
git diff --cached --check
git diff --cached --name-status
git commit -m "feat(ai-call): 迁移 Direct SIP 异步创建入口"
```

在 `git add -p` 中拒绝 `formalOutboundOnly` 等既有 hunk。提交后确认这些 hunk 仍出现在
`git diff -- app/api/v1/ai_call/controller.py`。

## 任务 5：让 Runtime 执行 Direct SIP DB-only Effect 图

**文件：**

- 修改：`app/services/ai_call/runtime_control/command_repository.py:100-125,690-760`
- 修改：`app/services/ai_call/runtime_control/runtime_service.py:315-335,440-485`
- 修改：`app/services/ai_call/runtime_control/provider_stub.py:25-85`
- 修改：`app/services/ai_call/runtime_control/start_readiness_repository.py:110-155`
- 修改：`app/services/ai_call/runtime_control/lifecycle.py:15-70`
- 修改：`tests/test_ai_call_runtime_command_repository.py`
- 修改：`tests/test_ai_call_runtime_stub_handlers.py`
- 修改：`tests/test_ai_call_runtime_start_readiness.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`

- [ ] **步骤 1：编写 entry-aware claim/spec 失败测试**

增加 `_default_start_specs()` 测试：

```python
def test_direct_sip_default_specs_include_sip_participant() -> None:
    specs = _default_start_specs(
        "call-a",
        _lease(fencing_token=7),
        "runtime-a",
        entry_type="direct_sip",
    )

    assert [spec.effect_type for spec in specs] == [
        "CREATE_ROOM",
        "ATTACH_AGENT_PARTICIPANT",
        "CREATE_SIP_PARTICIPANT",
    ]
    assert specs[-1].resource_key == "sip:call-a:g7"
```

Repository claim 测试断言 `CommandClaim.entry_type == "direct_sip"`，默认 Web 测试继续为
`web`。

- [ ] **步骤 2：编写复合 Stub 和 readiness 失败测试**

```python
@pytest.mark.anyio
@pytest.mark.parametrize(
    "effect_type",
    ["CREATE_SIP_PARTICIPANT", "HANGUP_SIP"],
)
async def test_db_only_provider_stub_supports_direct_sip_effects(effect_type: str) -> None:
    observation = await DeterministicDbOnlyProviderStub().apply(
        _effect_claim(effect_type=effect_type, resource_key="sip:call-a:g1")
    )
    assert observation.kind in {
        ProviderObservationKind.RESOURCE_PRESENT,
        ProviderObservationKind.TERMINAL_CONFIRMED,
    }
```

readiness 测试使用 Room、Agent、SIP 三个 specs，缺少 SIP 或 SIP 未 `APPLIED` 时必须返回
`None`；三个均完成时 `applied_effect_count == 3`。持久化测试把 entry 改为
`direct_sip` 并断言成功，`preview/outbound` 仍返回 false。

- [ ] **步骤 3：运行测试确认失败**

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_runtime_lifecycle.py -q
```

预期：FAIL，claim 没有 entry、默认 specs 只有 Web 两项、Web Stub 拒绝 SIP Effect。

- [ ] **步骤 4：给 `CommandClaim` 带上 Record entry**

在 `CommandClaim` 末尾增加 `entry_type: str = "web"`，保持现有手工构造测试兼容。
`_claim_for_owner()` 的 CAS 成功后，在同一事务按 tenant/call 查询 Record `entry_type`，
并写入返回的 claim；找不到 Record 必须 fail closed 返回 `None`。

- [ ] **步骤 5：生成 Direct SIP specs**

`_default_start_specs()` 接受 keyword-only `entry_type`：

- `web` 返回既有 Room + Agent 两项；
- `direct_sip` 在相同 generation 下再返回
  `CREATE_SIP_PARTICIPANT`，稳定键为 `sip:{call_id}:g{fencing}`；
- 其他 entry 抛 `ValueError`，禁止误套 Web 图。

`_process_owned_call()` 使用 `command_claim.entry_type` 调用该函数。当前 Stub 按列表顺序执行，
不声称已经实现真实 Provider 的网络级 START DAG；真实 Provider 仍在范围外。

- [ ] **步骤 6：增加复合 DB-only Stub**

```python
class DeterministicDbOnlyProviderStub(DeterministicWebProviderStub):
    _CREATE_EFFECT_TYPES = (
        DeterministicWebProviderStub._CREATE_EFFECT_TYPES
        | {"CREATE_SIP_PARTICIPANT"}
    )
    _DESTROY_EFFECT_TYPES = (
        DeterministicWebProviderStub._DESTROY_EFFECT_TYPES
        | {"HANGUP_SIP"}
    )
```

保留 `DeterministicWebProviderStub` 的 Web-only 测试合同；生命周期改用复合 Stub，以便同一
Runtime 处理 Web 和 Direct SIP。Stub 的 `calls` 仍只能记录 namespace、effect type、
resource key，不允许记录明文号码。

- [ ] **步骤 7：允许 Direct SIP readiness**

`build_stub_start_readiness()` 已逐项要求传入 specs 全部 `APPLIED`，保持该算法不变；
`persist_stub_ready()` 的合法 entry 集合改为 `{"web", "direct_sip"}`。Owner、fencing、
租约和终态屏障条件不得修改。

- [ ] **步骤 8：运行定向测试确认通过**

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_runtime_lifecycle.py -q
```

预期：全部 PASS。

- [ ] **步骤 9：提交 Runtime Stub**

```bash
git add app/services/ai_call/runtime_control/command_repository.py \
  app/services/ai_call/runtime_control/runtime_service.py \
  app/services/ai_call/runtime_control/provider_stub.py \
  app/services/ai_call/runtime_control/start_readiness_repository.py \
  app/services/ai_call/runtime_control/lifecycle.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_runtime_lifecycle.py
git diff --cached --check
git commit -m "feat(ai-call): 增加 Direct SIP DB-only Runtime Stub"
```

## 任务 6：验证双 Dispatcher/Runtime、END_CALL 和永久保留

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py:4060-4550`

- [ ] **步骤 1：编写 Direct SIP PostgreSQL 全闭环测试**

测试使用 `AI_CALL_OWNER_COMMAND_V1_ENTRIES="direct_sip"`，通过
`RuntimeEntryStartService` 创建号码 `13812345678`，注册两个 Worker，并发运行两个
Dispatcher 和两个 Runtime。核心断言：

```python
assert sum(dispatcher_counts) == 1
assert sorted(start_counts) == [0, 1]
assert start_query.status == "SUCCEEDED"
assert record.callee_phone_number == "13812345678"
assert record.callee_phone_number_masked == "138****5678"
assert "13812345678" not in (command_payload_json or "")
assert sensitive_payload_ciphertext is None
assert payload_key_version is None
assert effect_types == {
    "CREATE_ROOM",
    "ATTACH_AGENT_PARTICIPANT",
    "CREATE_SIP_PARTICIPANT",
}
```

创建唯一 `END_CALL` 后再次并发运行两个 Runtime，断言：

```python
assert sorted(end_counts) == [0, 1]
assert end_query.status == "SUCCEEDED"
assert cleanup_status == "clean"
assert runtime_owner_id is None
assert runtime_capacity_class == "none"
assert retained_plaintext == "13812345678"
assert destroy_effect_types == {
    "HANGUP_SIP",
    "DISCONNECT_AGENT_PARTICIPANT",
    "DELETE_ROOM",
}
assert sip_reservation_count == 0
```

Provider calls 总数必须为 6，且每项只含
`provider_namespace/effect_type/resource_key`，任何序列化结果均不得含完整号码。

- [ ] **步骤 2：运行 PostgreSQL 全闭环测试**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_direct_sip_db_only_two_dispatchers_and_runtimes_complete_start_end_loop -q
```

预期：PASS。该测试是在任务 2-5 的单元级 TDD 完成后增加的跨组件验收测试。

- [ ] **步骤 3：运行 Direct SIP 与既有 Web PostgreSQL 闭环**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_direct_sip_plaintext_migration_is_idempotent \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_direct_sip_db_only_two_dispatchers_and_runtimes_complete_start_end_loop \
  tests/postgres/test_ai_call_runtime_control_postgres.py::test_web_db_only_two_dispatchers_and_runtimes_complete_start_end_loop -q
```

预期：3 passed。

- [ ] **步骤 4：提交 PostgreSQL 全闭环测试**

```bash
git add tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --cached --check
git commit -m "test(ai-call): 覆盖 Direct SIP DB-only 完整闭环"
```

## 任务 7：全量验证、索引同步和脏文件审计

**文件：**

- 不新增业务文件。

- [ ] **步骤 1：同步并核对 CodeGraph**

```bash
codegraph sync
codegraph status
```

预期：索引成功，状态为 up to date。

- [ ] **步骤 2：运行控制面和入口单元测试**

```bash
uv run pytest \
  tests/test_ai_call_direct_sip_phone.py \
  tests/test_ai_call_direct_sip_runtime_entry.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_models.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_runtime_effect_repository.py \
  tests/test_ai_call_web_runtime_entry.py -q
```

预期：全部 PASS。

- [ ] **步骤 3：运行隔离 PostgreSQL 16 全套测试**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py -q
```

预期：全部 PASS；脚本创建独立临时容器并在结束时删除，不连接业务数据库。

- [ ] **步骤 4：运行受影响旧 Direct SIP 路径回归**

```bash
uv run pytest \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_phase_e_sip.py \
  tests/test_ai_call_sip_outbound_dialer.py -q
```

预期：全部 PASS，且测试使用 Stub/Fake，不拨打真实电话。

- [ ] **步骤 5：运行 lint 和 diff 检查**

```bash
uv run ruff check .
git diff --check
git diff --cached --check
```

预期：三条命令退出码均为 0。

- [ ] **步骤 6：审计敏感号码没有扩散**

```bash
rg -n "callee_phone_number|calleePhoneNumber|13812345678" \
  app/services/ai_call/runtime_control \
  app/api/v1/ai_call/schema.py \
  app/api/v1/ai_call/controller.py \
  tests/test_ai_call_direct_sip_runtime_entry.py \
  tests/postgres/test_ai_call_runtime_control_postgres.py
```

人工确认业务实现中的完整号码只进入请求对象、号码值对象和
`AiCallRecordModel.callee_phone_number`；Command/Effect 序列化、日志和响应均没有明文。
测试夹具出现 `13812345678` 属于预期。

- [ ] **步骤 7：核对提交和既有脏改动**

```bash
git log --oneline -8
git diff --cached --name-only
git status --short --branch
git diff -- app/api/v1/ai_call/model.py app/api/v1/ai_call/controller.py
```

预期：暂存区为空；计划生成前已有的无关脏改动仍存在；没有 `.env`、录音、数据库、
`.playwright-cli` 或备份文件进入任何提交。

## 完成门禁

只有同时满足以下条件，才可声明本切片完成：

1. `DSIP-P01` 至 `DSIP-P09` 均有对应自动化测试。
2. Record 永久保留明文和脱敏值，Command/Effect/响应不含明文。
3. 所有面向前端的号码字段只返回 `138****5678` 形式。
4. 两个 Dispatcher/Runtime 竞争只产生一套创建和销毁 Effect。
5. `HANGUP_SIP`、`DISCONNECT_AGENT_PARTICIPANT` 完成后才允许 `DELETE_ROOM` 和
   cleanup clean。
6. 没有真实外部调用，没有 Redis 事实，没有 SIP Reservation。
7. 单元测试、隔离 PostgreSQL 16、受影响旧链路回归、ruff、`git diff --check` 全通过。
8. 既有脏文件和未跟踪文件全部保留，未被修改、暂存、提交或清理。
