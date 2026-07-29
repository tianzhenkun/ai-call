# AI Call SIP 线路配置与正式拨号适配器实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在不重做现有 LiveKit SIP 主链的前提下，增加租户级默认外呼线路，并让正式外呼任务通过 `SipOutboundDialer` 自动执行。

**架构：** 新增独立的 SIP 线路模型、服务和接口；任务校验时固化默认线路，执行器从任务快照构造请求级 `SipOutboundConfig`，再复用 `AiCallService.create_sip_session()`。Mock 继续作为默认安全模式，正式 SIP 模式需要执行器开关、拨号器模式和 SIP 总开关同时启用。

**技术栈：** Python 3.12、FastAPI、Pydantic v2、SQLAlchemy Async、SQLite/PostgreSQL、LiveKit SIP、Pytest、Ruff。

---

## 0. 实施边界和工作树保护

- 实施目录固定为 `/Users/liuhongli/.codex/worktrees/ed81/ai-call`。
- 当前工作树已有未提交的坐席、录音、鉴权和 Linphone 诊断改动，禁止批量暂存、回滚或格式化这些文件。
- `app/api/v1/ai_call/outbound/linphone_test_dialer.py` 和 `tests/test_ai_call_outbound_linphone_test.py` 当前有未提交修改，本计划不编辑这两个文件。
- 自动化测试只能使用 Fake LiveKit、Fake AiCallService 和临时 SQLite。
- 不重启 `19011`，不修改 `/tmp/ai_call_ed81_local.db`，不拨打 Linphone 或真实号码。
- 每次提交只暂存当个任务列出的文件。
- 现有 `LiveKitSipClient`、`AiCallService.create_sip_session()`、录音、Handoff 和事件模型是复用对象，不另建 SIP 网关。

## 1. 文件结构

### 1.1 新建文件

- `app/api/v1/ai_call/outbound/sip_line_model.py`：租户 SIP 线路模型。
- `app/api/v1/ai_call/outbound/sip_line_schema.py`：线路请求、响应、快照和健康状态。
- `app/api/v1/ai_call/outbound/sip_line_service.py`：线路 CRUD、默认线路解析和非拨号预检。
- `app/api/v1/ai_call/outbound/sip_line_controller.py`：线路管理接口。
- `app/api/v1/ai_call/outbound/sip_outbound_dialer.py`：正式 SIP 拨号器。
- `docs/livekit-ai-outbound/sql/phase-h5-outbound-sip-line-postgres.sql`：PostgreSQL 增量迁移。
- `tests/test_ai_call_outbound_sip_line.py`：线路模型、服务、接口和任务快照测试。
- `tests/test_ai_call_sip_outbound_dialer.py`：正式拨号器接听证据和结果映射测试。

### 1.2 修改文件

- `app/api/v1/ai_call/outbound/__init__.py`：注册线路 Router。
- `app/api/v1/ai_call/outbound/model.py`：校验记录增加固化线路字段。
- `app/api/v1/ai_call/outbound/rule_task_model.py`：任务和 Attempt 增加线路及 Provider 诊断字段。
- `app/api/v1/ai_call/outbound/rule_task_schema.py`：任务响应增加线路摘要。
- `app/api/v1/ai_call/outbound/rule_task_service.py`：单号校验、创建任务和快照绑定默认线路。
- `app/api/v1/ai_call/outbound/service.py`：批量校验受理时绑定默认线路。
- `app/api/v1/ai_call/outbound/task_executor.py`：请求携带线路快照、Attempt 保存诊断信息。
- `app/api/v1/ai_call/service.py`：允许默认服务工厂接收请求级 SIP 配置。
- `app/config/setting.py`：增加显式拨号器模式。
- `app/plugin/init_app.py`：按 `mock` 或 `sip` 构造正式任务执行器。
- `tests/test_ai_call_outbound_rule_task.py`：锁定单号任务线路绑定。
- `tests/test_ai_call_outbound_validation.py`：锁定批量校验线路绑定。
- `tests/test_ai_call_outbound_task_executor.py`：锁定线路请求和 Attempt 诊断落库。
- `tests/test_ai_call_phase_e_sip.py`：锁定请求级 SIP 配置不会改变默认行为。

## 2. 核心类型

后续任务统一使用以下名称，不在不同文件中重新命名：

```python
LineHealthStatus = Literal[
    "UNKNOWN",
    "AVAILABLE",
    "MISCONFIGURED",
    "UNAVAILABLE",
]
LineRouteMode = Literal["managed_trunk_id", "inline_hostname"]
LineAuthMode = Literal["managed_trunk", "ip_allowlist"]


class SipLineSnapshot(OutboundSchema):
    line_id: str
    line_code: str
    line_name: str
    adapter_type: Literal["livekit_sip"]
    route_mode: LineRouteMode
    trunk_id: str | None = None
    proxy_host: str | None = None
    proxy_port: int | None = None
    auth_mode: LineAuthMode
    caller_number: str
    destination_country: str
    max_concurrency: int
    originate_timeout_seconds: int
```

`OutboundDialRequest` 增加：

```python
line: SipLineSnapshot
```

`DialResult` 增加：

```python
provider_status_code: str | None = None
provider_reason: str | None = None
hangup_cause: str | None = None
```

## 3. 任务拆分

### 任务 1：增加线路、校验、任务和 Attempt 持久化字段

**文件：**

- 创建：`app/api/v1/ai_call/outbound/sip_line_model.py`
- 创建：`docs/livekit-ai-outbound/sql/phase-h5-outbound-sip-line-postgres.sql`
- 修改：`app/api/v1/ai_call/outbound/model.py`
- 修改：`app/api/v1/ai_call/outbound/rule_task_model.py`
- 创建：`tests/test_ai_call_outbound_sip_line.py`

- [ ] **步骤 1：编写失败的模型和迁移测试**

```python
def test_sip_line_models_are_tenant_scoped_without_secrets_or_foreign_keys():
    columns = {
        column.name for column in AiCallSipLineModel.__table__.columns
    }
    assert {
        "tenant_id",
        "line_code",
        "line_name",
        "default_marker",
        "adapter_type",
        "route_mode",
        "trunk_id",
        "proxy_host",
        "proxy_port",
        "auth_mode",
        "caller_number",
        "health_status",
        "deleted",
    } <= columns
    assert "password" not in columns
    assert "secret" not in columns
    assert not AiCallSipLineModel.__table__.foreign_keys


def test_sip_line_migration_adds_line_and_attempt_diagnostics():
    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h5-outbound-sip-line-postgres.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    assert "create table if not exists ai_call_sip_line" in migration
    assert "add column if not exists line_id" in migration
    assert "add column if not exists provider_status_code" in migration
    assert "uk_ai_call_sip_line_tenant_default" in migration
```

- [ ] **步骤 2：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：FAIL，`sip_line_model` 尚不存在。

- [ ] **步骤 3：实现最小数据库模型**

模型必须使用：

```python
class AiCallSipLineModel(MappedBase):
    __tablename__ = "ai_call_sip_line"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "line_code",
            name="uk_ai_call_sip_line_tenant_code",
        ),
        UniqueConstraint(
            "tenant_id",
            "default_marker",
            name="uk_ai_call_sip_line_tenant_default",
        ),
        Index(
            "idx_ai_call_sip_line_tenant_enabled",
            "tenant_id",
            "deleted",
            "enabled",
            "updated_at",
        ),
        {"comment": "AI Call 租户级 SIP 外呼线路"},
    )
```

`AiCallOutboundValidationModel` 增加可空的 `line_id`、`line_snapshot_json`；`AiCallOutboundTaskModel` 增加可空的 `line_id`、`line_name`；`AiCallOutboundAttemptModel` 增加可空的：

```python
line_id
line_code
provider_status_code
provider_reason
hangup_cause
```

存量列保持可空，新创建任务由业务服务保证不为空。不增加物理外键。

- [ ] **步骤 4：编写 PostgreSQL 幂等迁移**

迁移必须：

- 创建 `ai_call_sip_line`；
- 给校验、任务和 Attempt 增加字段；
- 创建租户、默认线路和线路诊断索引；
- 使用普通 `varchar`、`text` 和 `timestamptz`；
- 不使用 `jsonb`；
- 不保存密码字段。

- [ ] **步骤 5：运行测试验证通过**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：PASS。

- [ ] **步骤 6：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/sip_line_model.py \
  app/api/v1/ai_call/outbound/model.py \
  app/api/v1/ai_call/outbound/rule_task_model.py \
  docs/livekit-ai-outbound/sql/phase-h5-outbound-sip-line-postgres.sql \
  tests/test_ai_call_outbound_sip_line.py
git commit -m "feat(ai-call): 增加租户 SIP 线路模型"
```

### 任务 2：实现线路 Schema、默认线路和非拨号预检

**文件：**

- 创建：`app/api/v1/ai_call/outbound/sip_line_schema.py`
- 创建：`app/api/v1/ai_call/outbound/sip_line_service.py`
- 修改：`tests/test_ai_call_outbound_sip_line.py`

- [ ] **步骤 1：编写失败的 Schema 与服务测试**

在 `tests/test_ai_call_outbound_sip_line.py` 中先加入测试数据助手，后续线路测试复用，避免依赖线上配置：

```python
async def create_line(
    database,
    tenant_id: str,
    line_code: str,
    *,
    health_status: str = "AVAILABLE",
) -> AiCallSipLineModel:
    async with database() as db:
        line = AiCallSipLineModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            line_code=line_code,
            line_name=f"线路 {line_code}",
            enabled=True,
            default_marker=None,
            adapter_type="livekit_sip",
            route_mode="inline_hostname",
            trunk_id=None,
            proxy_host="127.0.0.1",
            proxy_port=5089,
            auth_mode="ip_allowlist",
            caller_number="1000",
            destination_country="CN",
            max_concurrency=1,
            originate_timeout_seconds=45,
            health_status=health_status,
            health_message=None,
            deleted=False,
        )
        db.add(line)
        await db.commit()
        await db.refresh(line)
        return line


async def list_defaults(database, tenant_id: str) -> list[AiCallSipLineModel]:
    async with database() as db:
        return list(
            (
                await db.scalars(
                    select(AiCallSipLineModel).where(
                        AiCallSipLineModel.tenant_id == tenant_id,
                        AiCallSipLineModel.default_marker == "OUTBOUND",
                        AiCallSipLineModel.deleted.is_(False),
                    )
                )
            ).all()
        )
```

```python
def test_inline_ip_allowlist_line_requires_host_port_and_caller():
    line = SipLineIn.model_validate({
        "lineCode": "provider-a",
        "lineName": "运营商 A",
        "enabled": True,
        "adapterType": "livekit_sip",
        "routeMode": "inline_hostname",
        "proxyHost": "sip-provider.example.com",
        "proxyPort": 5089,
        "authMode": "ip_allowlist",
        "callerNumber": "037100000000",
        "destinationCountry": "CN",
        "maxConcurrency": 1,
        "originateTimeoutSeconds": 45,
    })
    assert line.trunk_id is None


@pytest.mark.anyio
async def test_set_default_keeps_one_default_per_tenant(database):
    first = await create_line(database, "tenant-a", "line-a")
    second = await create_line(database, "tenant-a", "line-b")
    await service.set_default(database, "tenant-a", first.id, user_id=1)
    await service.set_default(database, "tenant-a", second.id, user_id=1)
    defaults = await list_defaults(database, "tenant-a")
    assert [row.id for row in defaults] == [second.id]


def test_line_response_has_no_password_or_secret_fields():
    assert "password" not in SipLineOut.model_fields
    assert "secret" not in SipLineOut.model_fields
```

- [ ] **步骤 2：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：FAIL，线路 Schema 和 Service 尚不存在。

- [ ] **步骤 3：实现严格字段组合校验**

`SipLineIn` 使用 `model_validator`：

```python
@model_validator(mode="after")
def validate_route(self):
    if self.route_mode == "managed_trunk_id":
        if not self.trunk_id or self.proxy_host or self.proxy_port:
            raise ValueError("托管线路必须且只能提供 trunkId")
        if self.auth_mode != "managed_trunk":
            raise ValueError("托管线路必须使用 managed_trunk 鉴权")
    else:
        if not self.proxy_host or self.proxy_port is None or self.trunk_id:
            raise ValueError("内联线路必须且只能提供 proxyHost 和 proxyPort")
        if self.auth_mode != "ip_allowlist":
            raise ValueError("V1 内联线路只支持 IP 白名单鉴权")
    return self
```

- [ ] **步骤 4：实现线路服务**

`SipLineService` 必须提供：

```python
list_lines()
get_line()
create_line()
update_line()
set_default()
enable()
disable()
delete()
preflight()
resolve_default()
snapshot()
```

`set_default()` 在一个事务内：

1. 锁定目标租户的未删除线路；
2. 校验目标线路已启用；
3. 清空其他线路 `default_marker`；
4. 设置目标线路 `default_marker="OUTBOUND"`。

`disable()` 和 `delete()` 发现目标是默认线路时返回 409，提示先指定其他默认线路；V1 不通过一次动作隐式停用整个租户外呼。

`resolve_default()` 只返回当前租户、未删除、启用、健康状态为 `AVAILABLE` 的默认线路，否则抛出明确 `CustomException`。

- [ ] **步骤 5：实现可注入的非拨号预检**

生产实现把线路字段和全局网络字段组合成 `SipOutboundConfig`：

```python
SipOutboundConfig(
    enabled=True,
    allowed_callee_prefixes=settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES,
    default_ringing_timeout_seconds=line.originate_timeout_seconds,
    max_ringing_timeout_seconds=settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS,
    max_call_duration_seconds=settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS,
    trunk_id=line.trunk_id or "",
    trunk_hostname=inline_hostname(line),
    destination_country=line.destination_country,
    auth_username="",
    auth_password="",
    caller_number=line.caller_number,
    signaling_port=settings.SIP_SIGNALING_PORT,
    rtp_range=settings.SIP_RTP_RANGE,
    public_ip=settings.SIP_PUBLIC_IP,
    use_external_ip=settings.SIP_USE_EXTERNAL_IP,
)
```

预检不得发起 SIP INVITE。测试注入以下协议：

```python
class SipLinePreflightChecker(Protocol):
    async def check(self, config: SipOutboundConfig) -> None: ...
```

生产实现先调用现有 `validate_sip_outbound_preflight(config)`，再用现有 `LIVEKIT_URL`、`LIVEKIT_API_KEY` 和 `LIVEKIT_API_SECRET` 创建短生命周期客户端并执行只读探测：

```python
async with api.LiveKitAPI(
    url=settings.LIVEKIT_URL,
    api_key=settings.LIVEKIT_API_KEY,
    api_secret=settings.LIVEKIT_API_SECRET,
) as livekit_api:
    await livekit_api.room.list_rooms(api.ListRoomsRequest(names=[]))
```

字段校验异常写入 `MISCONFIGURED`；SDK 配置缺失或只读探测异常写入 `UNAVAILABLE`；两步均成功写入 `AVAILABLE`。每次都更新 `health_message` 和 `last_checked_at`。该接口不调用 `livekit_api.sip.create_sip_participant()`。

- [ ] **步骤 6：运行测试验证通过**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：PASS。

- [ ] **步骤 7：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/sip_line_schema.py \
  app/api/v1/ai_call/outbound/sip_line_service.py \
  tests/test_ai_call_outbound_sip_line.py
git commit -m "feat(ai-call): 实现 SIP 线路服务"
```

### 任务 3：暴露租户线路管理接口

**文件：**

- 创建：`app/api/v1/ai_call/outbound/sip_line_controller.py`
- 修改：`app/api/v1/ai_call/outbound/__init__.py`
- 修改：`tests/test_ai_call_outbound_sip_line.py`

- [ ] **步骤 1：编写失败的路由和租户隔离测试**

```python
def test_sip_line_routes_are_registered():
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/outbound-lines",
        "/ai-call/outbound-lines/{line_id}",
        "/ai-call/outbound-lines/{line_id}/set-default",
        "/ai-call/outbound-lines/{line_id}/enable",
        "/ai-call/outbound-lines/{line_id}/disable",
        "/ai-call/outbound-lines/{line_id}/preflight",
    } <= paths


@pytest.mark.anyio
async def test_tenant_cannot_read_another_tenant_line(database):
    line = await create_line(database, "tenant-a", "line-a")
    with pytest.raises(CustomException) as exc_info:
        await service.get_line(database, "tenant-b", line.id)
    assert exc_info.value.status_code == 404
```

- [ ] **步骤 2：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：FAIL，线路 Router 尚未注册。

- [ ] **步骤 3：实现接口**

控制器固定使用以下路径：

```text
GET    /ai-call/outbound-lines
GET    /ai-call/outbound-lines/{line_id}
POST   /ai-call/outbound-lines
PUT    /ai-call/outbound-lines/{line_id}
POST   /ai-call/outbound-lines/{line_id}/set-default
POST   /ai-call/outbound-lines/{line_id}/enable
POST   /ai-call/outbound-lines/{line_id}/disable
POST   /ai-call/outbound-lines/{line_id}/preflight
DELETE /ai-call/outbound-lines/{line_id}
```

接口通过 `_identity(auth)` 获取 `tenant_id` 和 `user_id`；详情、修改和动作都把 `tenant_id` 传入服务，不能只按 `line_id` 查询。

- [ ] **步骤 4：运行测试验证通过**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_outbound_sip_line.py -q
```

预期：PASS。

- [ ] **步骤 5：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/sip_line_controller.py \
  app/api/v1/ai_call/outbound/__init__.py \
  tests/test_ai_call_outbound_sip_line.py
git commit -m "feat(ai-call): 增加 SIP 线路接口"
```

### 任务 4：让单号和批量校验固化默认线路

**文件：**

- 修改：`app/api/v1/ai_call/outbound/rule_task_service.py`
- 修改：`app/api/v1/ai_call/outbound/service.py`
- 修改：`app/api/v1/ai_call/outbound/rule_task_schema.py`
- 修改：`tests/test_ai_call_outbound_rule_task.py`
- 修改：`tests/test_ai_call_outbound_validation.py`
- 修改：`tests/test_ai_call_outbound_sip_line.py`

- [ ] **步骤 1：编写失败的单号校验和任务快照测试**

扩展现有 `test_single_validation_checks_phone_and_all_references()`：

1. 在调用 `validate_single()` 前插入一条 `AVAILABLE` 线路并设为租户默认；
2. 用 `OutboundRuleTaskService(database, line_service=SipLineService())` 替代无线路服务的构造；
3. 在已有断言后增加：

```python
assert validation.line_id == line.id
assert json.loads(validation.line_snapshot_json)["lineId"] == str(line.id)
```

扩展现有 `test_batch_task_creation_copies_targets_in_batches_and_is_idempotent()`：让 `_seed_passed_validation()` 同时保存上一步线路的 `line_id` 和 `line_snapshot_json`，随后增加：

```python
assert task.line_id == line.id
assert task.line_name == line.line_name
snapshot = json.loads(task.config_snapshot_json)
assert snapshot["sipLine"]["lineId"] == str(line.id)
assert "password" not in json.dumps(snapshot).lower()
assert "secret" not in json.dumps(snapshot).lower()
```

- [ ] **步骤 2：编写失败的批量校验测试**

```python
@pytest.mark.anyio
async def test_batch_validation_rejects_missing_default_line(database):
    service = OutboundValidationService(
        database,
        line_service=SipLineService(),
    )
    with pytest.raises(CustomException) as exc_info:
        await _accept(
            service,
            database,
            _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
        )
    assert exc_info.value.msg == "当前租户没有可用的默认外呼线路"
```

- [ ] **步骤 3：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_outbound_validation.py \
  tests/test_ai_call_outbound_sip_line.py \
  -q
```

预期：FAIL，校验和任务尚未保存线路。

- [ ] **步骤 4：为两个校验服务注入同一线路解析器**

构造函数统一接受：

```python
line_service: SipLineService | None = None
```

单号和批量校验受理时调用：

```python
line = await self.line_service.resolve_default(db, tenant_id)
validation.line_id = line.id
validation.line_snapshot_json = self.line_service.snapshot_json(line)
```

线路缺失或不可用时，不创建 `PASSED` 校验结果。

- [ ] **步骤 5：创建任务时验证固化线路仍有效**

`create_task()` 必须：

1. 读取 `validation.line_id` 和 `line_snapshot_json`；
2. 按租户重新查询同一线路；
3. 线路已删除或停用时返回 409；
4. 保存 `task.line_id`、`task.line_name`；
5. 把非敏感快照放入 `config_snapshot_json["sipLine"]`。

请求 Schema 不增加 `lineId`，防止前端绕过默认线路路由。

`OutboundTaskOut` 增加只读 `line_id` 和 `line_name`。

- [ ] **步骤 6：运行测试验证通过**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_outbound_validation.py \
  tests/test_ai_call_outbound_sip_line.py \
  -q
```

预期：PASS。

- [ ] **步骤 7：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/rule_task_service.py \
  app/api/v1/ai_call/outbound/service.py \
  app/api/v1/ai_call/outbound/rule_task_schema.py \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_outbound_validation.py \
  tests/test_ai_call_outbound_sip_line.py
git commit -m "feat(ai-call): 外呼任务绑定默认 SIP 线路"
```

### 任务 5：实现只认真实接听证据的 `SipOutboundDialer`

**文件：**

- 创建：`app/api/v1/ai_call/outbound/sip_outbound_dialer.py`
- 修改：`app/api/v1/ai_call/service.py`
- 创建：`tests/test_ai_call_sip_outbound_dialer.py`
- 修改：`tests/test_ai_call_phase_e_sip.py`

- [ ] **步骤 1：编写失败的接听证据测试**

在 `tests/test_ai_call_sip_outbound_dialer.py` 中实现本文件自用的 `FakeAiCallService`、`dial_request()`、`finish_record()`、`mark_answered()`、`add_event()` 和 `record()`；所有写入只操作 `database` 临时 SQLite fixture。轮询助手固定为：

```python
async def wait_until(
    predicate: Callable[[], bool],
    *,
    attempts: int = 20,
) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition was not reached")
```

```python
@pytest.mark.anyio
async def test_dial_does_not_mark_connected_after_session_creation(database):
    service = FakeAiCallService()
    dialer = SipOutboundDialer(
        database,
        ai_call_service_factory=lambda db, config: service,
        sleep=zero_sleep,
    )
    connected = AsyncMock()

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-1",
            on_connected=connected,
        )
    )
    await service.session_created.wait()
    connected.assert_not_awaited()
    await finish_record(database, "call-1", answered=False, reason="no_answer")
    result = await task
    assert result.call_result == "no_answer"


@pytest.mark.anyio
async def test_dial_marks_connected_once_after_answer_and_media(database):
    connected = AsyncMock()
    task = asyncio.create_task(dialer.dial(
        dial_request(),
        call_id="call-2",
        on_connected=connected,
    ))
    await mark_answered(database, "call-2")
    connected.assert_not_awaited()
    await add_event(database, "call-2", "media_connected")
    await wait_until(lambda: connected.await_count == 1)
    await finish_record(database, "call-2", answered=True)
    result = await task
    assert result.call_result == "connected"
    connected.assert_awaited_once()
```

- [ ] **步骤 2：编写失败结果映射测试**

```python
@pytest.mark.parametrize(
    ("end_reason", "expected"),
    [
        ("sip_busy", "busy"),
        ("sip_connect_timeout", "no_answer"),
        ("sip_403", "call_failed"),
        ("sip_503", "call_failed"),
        ("sip_508", "call_failed"),
    ],
)
def test_maps_terminal_sip_reason(end_reason, expected):
    result = SipOutboundDialer.map_terminal_record(
        record(answered_at=None, end_reason=end_reason)
    )
    assert result.call_result == expected
```

- [ ] **步骤 3：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_sip_outbound_dialer.py -q
```

预期：FAIL，`SipOutboundDialer` 尚不存在。

- [ ] **步骤 4：允许服务工厂接收请求级 SIP 配置**

保持所有既有调用兼容：

```python
def get_default_ai_call_service(
    db: AsyncSession | None = None,
    *,
    sip_config: SipOutboundConfig | None = None,
) -> AiCallService:
    ...
    sip_client=_build_sip_client(config=sip_config),
```

```python
def _build_sip_client(
    config: SipOutboundConfig | None = None,
) -> LiveKitSipClient:
    return LiveKitSipClient(
        config=config or SipOutboundConfig.from_settings(settings),
        livekit_url=settings.LIVEKIT_URL,
        api_key=settings.LIVEKIT_API_KEY,
        api_secret=settings.LIVEKIT_API_SECRET,
    )
```

不传 `sip_config` 时，现有浏览器 SIP、人工回拨和诊断接口行为保持不变。

- [ ] **步骤 5：实现正式拨号器**

`SipOutboundDialer`：

- `dialer_type = "sip"`；
- `manages_call_record = True`；
- 从 `request.line` 和全局网络配置构造 `SipOutboundConfig`；
- 使用稳定 `call_id` 调用 `create_sip_session()`；
- 查询 `AiCallRecordModel` 和 `AiCallEventModel`；
- `answered_at` 与 `media_connected` 同时存在时调用一次 `on_connected()`；
- 等待记录进入 `completed` 或 `failed`；
- 把终态映射为 `DialResult`；
- 创建失败但记录已进入终态时，以记录为准；
- 创建失败且无记录时返回 `call_failed`。

- [ ] **步骤 6：运行测试验证通过**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_sip_outbound_dialer.py \
  tests/test_ai_call_phase_e_sip.py \
  -q
```

预期：PASS。

- [ ] **步骤 7：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/sip_outbound_dialer.py \
  app/api/v1/ai_call/service.py \
  tests/test_ai_call_sip_outbound_dialer.py \
  tests/test_ai_call_phase_e_sip.py
git commit -m "feat(ai-call): 增加正式 SIP 外呼拨号器"
```

### 任务 6：把正式拨号器接入任务执行器和 Worker

**文件：**

- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 修改：`app/config/setting.py`
- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_outbound_task_executor.py`
- 修改：`tests/test_ai_call_outbound_sip_line.py`

- [ ] **步骤 1：编写失败的执行器线路快照测试**

扩展现有 `_seed_task()`，增加仅供测试使用的 `line_snapshot: dict | None = None` 参数；传入时同步写入任务的 `line_id`、`line_name` 和 `config_snapshot_json["sipLine"]`。新增 `available_line_snapshot()`，返回与第 2 节 `SipLineSnapshot` 字段完全一致的字典。

```python
@pytest.mark.anyio
async def test_executor_passes_snapshotted_line_to_dialer(database):
    dialer = SequenceDialer([DialResult(call_result="connected")])
    task_id = await _seed_task(
        database,
        line_snapshot=available_line_snapshot(),
    )
    executor = OutboundTaskExecutor(database, dialer)
    assert await executor.run_once() == 1
    assert dialer.requests[0].line.line_code == "provider-a"

    async with database() as db:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel).where(
                AiCallOutboundAttemptModel.task_id == task_id
            )
        )
        assert attempt.line_code == "provider-a"
```

- [ ] **步骤 2：编写失败的 Provider 诊断落库测试**

```python
@pytest.mark.anyio
async def test_executor_persists_provider_diagnostics(database):
    result = DialResult(
        call_result="call_failed",
        error_message="上游线路错误",
        provider_status_code="508",
        provider_reason="Q.850 cause=31",
        hangup_cause="NORMAL_UNSPECIFIED",
    )
    dialer = SequenceDialer([result])
    await _seed_task(database, line_snapshot=available_line_snapshot())
    executor = OutboundTaskExecutor(database, dialer)
    assert await executor.run_once() == 1
    async with database() as db:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel)
            .order_by(AiCallOutboundAttemptModel.created_at.desc())
            .limit(1)
        )
    assert attempt is not None
    assert attempt.provider_status_code == "508"
    assert attempt.provider_reason == "Q.850 cause=31"
    assert attempt.hangup_cause == "NORMAL_UNSPECIFIED"
```

- [ ] **步骤 3：编写失败的 Worker 模式测试**

```python
def test_outbound_dialer_mode_defaults_to_mock():
    assert Settings().AI_CALL_OUTBOUND_DIALER_MODE == "mock"


@pytest.mark.anyio
async def test_worker_uses_sip_only_when_explicitly_selected(monkeypatch):
    monkeypatch.setattr(settings, "AI_CALL_OUTBOUND_EXECUTOR_ENABLED", True)
    monkeypatch.setattr(settings, "AI_CALL_OUTBOUND_DIALER_MODE", "sip")
    monkeypatch.setattr(settings, "AI_CALL_SIP_OUTBOUND_ENABLED", True)
    worker = await _start_ai_call_outbound_task_worker()
    assert worker.executor.dialer.dialer_type == "sip"
    await worker.stop()
```

- [ ] **步骤 4：运行测试验证正确失败**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_outbound_sip_line.py \
  -q
```

预期：FAIL，执行请求、Attempt 和 Worker 尚未使用正式线路。

- [ ] **步骤 5：扩展执行器**

`_claim_target()` 从 `task.config_snapshot_json["sipLine"]` 构造 `SipLineSnapshot`。快照不存在或不合法时：

1. 不调用拨号器；
2. 不增加新的网络请求；
3. 当前任务进入 `FAILED`；
4. `error_message` 写“任务缺少有效的 SIP 线路快照”。

新 Attempt 创建时保存 `line_id` 和 `line_code`；`_finish_attempt()` 保存 `DialResult` 的三个 Provider 诊断字段。

- [ ] **步骤 6：增加显式拨号器模式**

配置：

```python
AI_CALL_OUTBOUND_DIALER_MODE: Literal["mock", "sip"] = "mock"
```

启动规则：

```text
executor=false                         -> 不启动
executor=true, dialer_mode=mock        -> MockOutboundDialer
executor=true, dialer_mode=sip,
  sip_outbound_enabled=false           -> 拒绝启动并记录明确错误
executor=true, dialer_mode=sip,
  sip_outbound_enabled=true            -> SipOutboundDialer
```

日志必须明确打印实际 `dialer_type`，不能继续统一写“模拟执行器”。

- [ ] **步骤 7：运行测试验证通过**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_outbound_sip_line.py \
  tests/test_ai_call_sip_outbound_dialer.py \
  -q
```

预期：PASS。

- [ ] **步骤 8：提交**

```bash
git add \
  app/api/v1/ai_call/outbound/task_executor.py \
  app/config/setting.py \
  app/plugin/init_app.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_outbound_sip_line.py
git commit -m "feat(ai-call): 正式任务接入 SIP 拨号器"
```

### 任务 7：回归、迁移检查和安全审查

**文件：**

- 可能修改：本计划已经列出的实现和测试文件

- [ ] **步骤 1：运行线路与任务定向测试**

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_sip_line.py \
  tests/test_ai_call_sip_outbound_dialer.py \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_outbound_validation.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_phase_e_sip.py \
  -q
```

预期：全部通过。

- [ ] **步骤 2：运行相关 SIP、记录和转人工回归**

```bash
.venv/bin/pytest \
  tests/test_ai_call_outbound_record_filter.py \
  tests/test_ai_call_phase_b1_records.py \
  tests/test_ai_call_agent_console_claim.py \
  -q
```

预期：全部通过；如工作树原有未提交改动导致失败，先证明失败与本计划文件无关，再向用户报告，不能回滚原有改动。

- [ ] **步骤 3：运行 Ruff**

```bash
.venv/bin/ruff check \
  app/api/v1/ai_call/outbound \
  app/api/v1/ai_call/service.py \
  app/config/setting.py \
  app/plugin/init_app.py \
  tests/test_ai_call_outbound_sip_line.py \
  tests/test_ai_call_sip_outbound_dialer.py \
  tests/test_ai_call_outbound_rule_task.py \
  tests/test_ai_call_outbound_validation.py \
  tests/test_ai_call_outbound_task_executor.py \
  tests/test_ai_call_phase_e_sip.py
```

预期：无错误。

- [ ] **步骤 4：运行全量测试**

```bash
.venv/bin/pytest -q
```

预期：全部通过；若存在基线失败，保存精确测试名和错误，不能笼统宣称通过。

- [ ] **步骤 5：验证迁移与安全边界**

使用独立临时 SQLite：

```bash
DATABASE_TYPE=sqlite \
DATABASE_NAME=/tmp/ai_call_sip_line_plan_verify \
.venv/bin/python -c \
'from app.core.base_model import MappedBase; print(sorted(MappedBase.metadata.tables))'
```

并运行：

```bash
rg -n -i 'password|secret|api_key|api_secret' \
  app/api/v1/ai_call/outbound/sip_line_*.py \
  docs/livekit-ai-outbound/sql/phase-h5-outbound-sip-line-postgres.sql
```

预期：

- 表元数据包含 `ai_call_sip_line`；
- 线路模型、接口响应和迁移中不存在密码或 Secret 存储字段；
- 命中的 LiveKit SDK 全局配置只允许出现在预检或客户端构造逻辑，不进入线路响应。

- [ ] **步骤 6：确认没有真实拨打**

验证期间不得调用：

```text
POST /ai-call/sip-sessions
POST /ai-call/outbound-tasks/{taskId}/test-run
POST /ai-call/outbound-tasks
```

检查当前 `19011` 仅用于只读健康确认，不以其存活证明新代码已加载。

- [ ] **步骤 7：代码审查**

使用 `requesting-code-review` 检查：

- 是否重复实现了 `LiveKitSipClient`；
- 是否存在跨租户线路读取；
- 是否存在明文凭据；
- 是否把 Session 创建误判为接通；
- 是否出现未开启开关就能拨号的路径；
- 是否覆盖了工作树原有未提交改动。

- [ ] **步骤 8：提交审查修复**

只暂存本计划文件：

```bash
git add <本计划中实际修正的文件>
git commit -m "fix(ai-call): 收口 SIP 线路与拨号安全边界"
```

如果审查没有产生代码修改，则不创建空提交。

## 4. 完成口径

只有同时满足以下条件才能宣称后端基础完成：

1. 线路模型、接口、默认线路和租户隔离测试通过；
2. 单号、批量校验和任务快照均绑定默认线路；
3. 正式拨号器复用现有 `create_sip_session()`；
4. 未出现 `answered_at + media_connected` 前不进入 `IN_CALL`；
5. Attempt 保存实际线路和 Provider 诊断信息；
6. 执行器默认仍是 `mock`，SIP 模式必须显式开启；
7. 自动化与迁移检查没有发起真实呼叫；
8. 未覆盖当前工作树中与本计划无关的未提交改动。

完成这些条件只代表“正式 SIP 线路和任务接入代码已具备”，不代表：

- 当前 `19011` 已加载新代码；
- Linphone 已完成本轮人工验收；
- `recov_ten` 的运营商线路当前仍有效；
- 真实手机号已经接通。
