# AI Call Web DB-only 垂直入口实施计划

> **面向 AI 代理的工作者：** 必需子技能：使用 `superpowers:executing-plans` 逐任务实现此计划。步骤使用复选框；每个任务执行红灯测试、最小实现、绿灯验证和独立提交。当前工作树已有用户脏改动，禁止清理、覆盖或夹带提交。

**目标：** 在不连接 LiveKit、SIP、Redis 或真实 Provider 的前提下，把现有 Web 创建入口接到已冻结的 PostgreSQL Owner/Command 控制面，提供持久命令查询、bootstrap 与 END_CALL 的完整 Stub 闭环，并彻底移除 `preview` 作为 Runtime 入口的残留。

**架构：** `/ai-call/sessions` 保留 legacy 默认行为；仅当 `web` 被显式加入 `AI_CALL_OWNER_COMMAND_V1_ENTRIES` 时，API 使用认证租户和 `Idempotency-Key`，在同一事务创建 Record 与 `START_CALL` 并返回 `202`。Dispatcher/Runtime 继续只以 PostgreSQL CAS 授权执行；Runtime 使用确定性、无网络 Provider Stub 生成资源事实。前端通过 bootstrap 和命令查询读取持久状态，通过现有 END_CALL API 建立终态屏障。Preview、SIP、Outbound、Handoff、Redis 与 Token/LiveKit 不进入本切片。

**技术栈：** Python 3.12、FastAPI、Pydantic、SQLAlchemy 2 AsyncIO、PostgreSQL 16 `READ COMMITTED`、pytest/anyio、ruff、Docker Compose 隔离测试。

**冻结依据：**

- 总设计：`docs/superpowers/specs/2026-07-31-ai-call-single-owner-runtime-command-design.md`
- 总设计 SHA-256：`c3a4300d3426359ff9cecf3d051be5d700c820571838ecb74e88053d13e3ceb8`
- 范围修正规格：`docs/superpowers/specs/2026-08-01-ai-call-16-2-scope-simplification-design.md`
- 当前基线提交：`6ff08ff8473e3e1cafcbc04d74b8cc5c5924bb91`

---

## 范围与停止规则

本计划只实现：

- 删除/拒绝 Runtime 控制面中的 `preview`；
- 现有 Web `/ai-call/sessions` 在显式开关下提交持久 `START_CALL`；
- 认证租户、HTTP `Idempotency-Key`、稳定请求指纹、数据库截止时间和 `202` 语义；
- 租户隔离的 Command Query；
- bootstrap 的 waiting/ready/ending/terminal 投影；
- 确定性、无网络 Provider Stub；
- 两个独立 Dispatcher/Runtime 的 PostgreSQL 闭环与 DB-only 延迟测量。

明确不实现：

- 新 Preview Runtime 行为或音色轻量试听；
- LiveKit Room/Token、浏览器麦克风、Qwen、真实 Provider；
- Direct SIP、Outbound、Linphone、Reservation 业务接入；
- Handoff/Presence、Task/Target/Attempt、录音/ASR/语义/跟进；
- Redis Streams、Pub/Sub、LISTEN/NOTIFY、SSE 或前端实时改造；
- 新 PostgreSQL migration 或现有 runtime migration 变更。

出现以下任一情况立即停止，不自行扩大范围：

1. 实现需要修改 Owner、fencing、Effect、Recovery 或终态屏障算法；
2. 实现需要连接网络 Provider、LiveKit、SIP 或 Redis；
3. DB-only 实测 P95 超过 1 秒时，只记录证据并把“简单唤醒”列为下一独立切片，不在本计划加入 Redis；
4. 需要修改现有脏文件时，只允许任务列出的局部 hunk，提交前必须逐 hunk 检查暂存区。

## 成功标准

1. `preview` 不再是 `OwnerCommandEntry`、Runtime API 请求或配置的合法值；旧音色试听控制器不再提示转到 `START_CALL`。
2. 未开启 `web` 时，`POST /ai-call/sessions` 继续走 legacy；开启后必须要求认证租户与 `Idempotency-Key`，只创建一条 Record 与一条 `START_CALL`，返回 `202`，且不调用 legacy Orchestrator。
3. 同租户、同幂等键、同规范化请求返回原 `callId/commandId`；同键不同业务请求返回 `409 IDEMPOTENCY_CONFLICT`，不产生孤儿 Record。
4. Web 排队截止时间由创建事务读取的数据库时间计算并持久化；无 Worker 到期时形成 `DEAD/failed/ALLOCATION_TIMEOUT`。
5. Command Query 只能读取当前租户数据，能区分 waiting、processing、succeeded、failed/superseded/canceled；不返回密文、processing token 或 Provider 内部字段。
6. 确定性 Stub 不调用网络：创建 Effect 返回 `RESOURCE_PRESENT`，销毁 Effect 返回 `TERMINAL_CONFIRMED`，未知 Effect fail closed。
7. 两个独立 Dispatcher/Runtime 竞争时，每条命令与 Effect 只执行一次；END_CALL 后 Record 为逻辑终态、cleanup 为 `clean`、Owner 与容量释放。
8. 测试证明未创建 Preview、SIP、Outbound、Handoff 或 Redis 事实，现有休眠 `dispatch_*`/`published_at`/`stream_*` 字段保持为空。

---

## Task 1：移除 Preview Runtime 入口残留

**合同：** 范围修正规格第 4 节；Preview 不属于 16.2 Runtime。

**文件：**

- 修改：`app/services/ai_call/runtime_control/types.py`
- 修改：`app/services/ai_call/runtime_control/runtime_token_service.py`
- 修改：`app/services/ai_call/runtime_control/start_readiness_repository.py`
- 修改：`app/api/v1/ai_call/schema.py`
- 修改：`app/api/v1/ai_call/voice/controller.py`
- 修改：`tests/test_ai_call_process_roles.py`
- 修改：`tests/test_ai_call_runtime_entry_start_service.py`
- 修改：`tests/test_ai_call_runtime_entry_controller.py`
- 修改：`tests/test_ai_call_runtime_entry_legacy_guards.py`
- 修改：`tests/test_ai_call_runtime_token_service.py`
- 修改：`tests/test_ai_call_runtime_start_readiness.py`

- [ ] **步骤 1：先写 Preview 非法的红灯测试**

增加或改写断言：

```python
def test_parse_owner_entries_rejects_preview() -> None:
    with pytest.raises(RuntimeRoleConfigurationError, match="preview"):
        parse_owner_command_entries("preview")


@pytest.mark.anyio
async def test_preview_is_not_an_owner_command_entry() -> None:
    with pytest.raises(RuntimeEntryStartError, match="不是合法"):
        await service.submit(StartEntryRequest(
            tenant_id="tenant-a",
            entry_type="preview",
            idempotency_key="start:preview:1",
            payload={},
        ))
```

同时固定以下边界：

- `RuntimeStartCallRequest(entryType="preview", idempotencyKey="preview:1")` Pydantic 校验失败；
- disabled-entry 测试改用仍合法但未启用的 `direct_sip`，不能再用 `preview` 表示 legacy；
- Token gate 与 stub readiness 只接受 `entry_type == "web"`；
- 旧 `/voice-preview-sessions` 控制器不再读取 Runtime 入口开关，也不再返回“使用异步 START_CALL”的 409；本任务不实现新的轻量试听。

- [ ] **步骤 2：运行红灯测试**

```bash
uv run pytest \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_runtime_token_service.py \
  tests/test_ai_call_runtime_start_readiness.py -q
```

预期：现有 `PREVIEW` 枚举、API Literal 和 `{"web", "preview"}` 特判导致新增断言失败。

- [ ] **步骤 3：做最小删除**

实现要求：

```python
class OwnerCommandEntry(StrEnum):
    WEB = "web"
    DIRECT_SIP = "direct_sip"
    OUTBOUND = "outbound"
```

- `RuntimeStartCallRequest.entry_type` 删除 `preview`，不提前把 Outbound 业务创建接入通用 Web API；
- Token/readiness 使用严格 `entry_type == "web"`；
- `voice/controller.py` 仅删除 Runtime 配置导入与切换分支，保留其余旧接口现状，轻量试听另按独立规格实施；
- 不修改冻结设计、migration、Runtime/Owner/Effect 核心代码。

- [ ] **步骤 4：运行绿灯与 Preview 调用方扫描**

```bash
uv run pytest \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_runtime_token_service.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_voice_preview.py -q

rg -n 'OwnerCommandEntry\.PREVIEW|entry_type.*preview|runtime_control_mode_for_entry\([^\n]*preview|"web", "preview"' \
  app/services/ai_call/runtime_control app/api/v1/ai_call
```

预期：测试通过；扫描不得再命中 Runtime Preview 行为。拒绝 `preview` 的测试字符串和音色模块自身的旧 preview 命名不作为本任务失败。

- [ ] **步骤 5：仅提交 Task 1 文件**

```bash
git add \
  app/services/ai_call/runtime_control/types.py \
  app/services/ai_call/runtime_control/runtime_token_service.py \
  app/services/ai_call/runtime_control/start_readiness_repository.py \
  app/api/v1/ai_call/schema.py \
  app/api/v1/ai_call/voice/controller.py \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_runtime_token_service.py \
  tests/test_ai_call_runtime_start_readiness.py
git diff --cached --check
git diff --cached --stat
git commit -m "refactor(ai-call): 移除 Preview Runtime 入口"
```

---

## Task 2：把现有 Web 创建入口适配为持久 START_CALL

**合同：** `CMD-01`、总设计 7.1；范围修正规格第 6 节。

**文件：**

- 修改：`app/config/setting.py`
- 修改：`app/api/v1/ai_call/controller.py`（当前已有无关脏 hunk，必须 `git add -p`）
- 修改：`app/api/v1/ai_call/schema.py`
- 修改：`app/api/v1/ai_call/runtime_control_controller.py`
- 修改：`app/services/ai_call/runtime_control/entry_start_service.py`
- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 新增：`tests/test_ai_call_web_runtime_entry.py`
- 修改：`tests/test_ai_call_runtime_command_repository.py`
- 修改：`tests/test_ai_call_runtime_entry_controller.py`

- [ ] **步骤 1：写 Web 原入口的红灯测试**

新测试必须直接调用 `create_session_controller`，覆盖：

```python
@pytest.mark.anyio
async def test_web_owner_mode_returns_202_without_calling_legacy_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # settings entries = "web"
    # auth.user.tenant_id = "tenant-a"
    # Idempotency-Key = "web:biz-1:req-1"
    # 断言 response.status_code == 202
    # 断言 acceptanceStatus/callId/commandId/commandSeq/status
    # 断言 legacy AiCallService.create_web_session 未调用
```

还需覆盖：

- `web` 未开启时继续调用 legacy service 并返回原 `CreateSessionOut`；
- owner 模式缺少租户返回 401；
- owner 模式缺少/空 `Idempotency-Key` 返回 422，且仓储未写入；
- 同键同请求返回原 ID；同键改变 `voice`、`sceneCode`、`businessId` 或 `businessParams` 任一字段返回 `409`；
- generic `/runtime/start-call` 的幂等冲突也映射为稳定 `409 IDEMPOTENCY_CONFLICT`，不能泄漏 500。

- [ ] **步骤 2：写数据库截止时间红灯测试**

`tests/test_ai_call_runtime_command_repository.py` 固定 `StartCallIntent` 的服务器排队 TTL 不进入业务指纹；PostgreSQL 测试在 Task 5 验证实际 deadline。最小接口：

```python
StartEntryRequest(
    tenant_id="tenant-a",
    entry_type="web",
    idempotency_key="web:biz-1:req-1",
    payload={"voice": "v1", "scene_code": "collection"},
    allocation_timeout_seconds=30.0,
)
```

仓储使用同一创建事务已读取的数据库 `now` 计算：

```python
deadline = request.allocation_deadline_at
if deadline is None and request.allocation_timeout_seconds is not None:
    deadline = now + timedelta(seconds=request.allocation_timeout_seconds)
```

`StartEntryRequest` 与 `StartCallIntent` 都增加 `allocation_timeout_seconds: float | None`，入口服务透传，仓储负责用数据库 `now` 转为绝对时间。TTL 是服务器调度策略，不属于客户端业务请求指纹；显式业务 payload 必须规范化包含 `voice/business_id/scene_code/business_params`。

- [ ] **步骤 3：运行红灯测试**

```bash
uv run pytest \
  tests/test_ai_call_web_runtime_entry.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_command_repository.py -q
```

预期：现有 `/sessions` 仍调用 legacy service、没有 Idempotency-Key/认证租户分支，也没有数据库相对 TTL。

- [ ] **步骤 4：实现 Web 模式分流**

实现边界：

- 新增 `AI_CALL_WEB_ALLOCATION_TIMEOUT_SECONDS: float = 30.0`；入口服务拒绝非正数；
- `/ai-call/sessions` 增加 `AuthSchema` 与可选 `Header(alias="Idempotency-Key")`；只有 owner 模式要求该 Header；
- owner 模式构造稳定 payload：

```python
payload = {
    "voice": request.voice,
    "business_id": request.business_id,
    "scene_code": request.scene_code,
    "business_params": request.business_params,
}
```

- 调用 `RuntimeEntryStartService`，依赖 `RuntimeCommandRepository(auth.db)`；Record 与命令仍由仓储同事务创建；
- owner 模式返回现有 `RuntimeStartCallOut` 形状和 HTTP 202；legacy 模式响应保持不变；
- `IdempotencyConflictError` 映射为 `409`，`data.errorCode == "IDEMPOTENCY_CONFLICT"`；
- 不调用 Orchestrator、Prompt Resolver、Room Manager 或 Token 服务；
- 不删除 `AiCallService.create_web_session` 的 legacy guard，它继续防止旁路双创建。

- [ ] **步骤 5：运行绿灯与旧 Web schema 回归**

```bash
uv run pytest \
  tests/test_ai_call_web_runtime_entry.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_phase_b4_prompt_config.py -q
```

- [ ] **步骤 6：对脏 controller 做逐 hunk 提交**

```bash
git add \
  app/config/setting.py \
  app/api/v1/ai_call/schema.py \
  app/api/v1/ai_call/runtime_control_controller.py \
  app/services/ai_call/runtime_control/entry_start_service.py \
  app/services/ai_call/runtime_control/command_repository.py \
  tests/test_ai_call_web_runtime_entry.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_entry_controller.py
git add -p app/api/v1/ai_call/controller.py
git diff --cached --check
git diff --cached -- app/api/v1/ai_call/controller.py
git commit -m "feat(ai-call): 接入 Web DB-only 创建入口"
```

暂存区中的 `controller.py` 不得包含既有 `formalOutboundOnly` hunk。

---

## Task 3：增加租户隔离的持久 Command Query

**合同：** 范围修正规格第 6 节；查询只能反映 PostgreSQL 事实。

**文件：**

- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改：`app/api/v1/ai_call/schema.py`
- 修改：`app/api/v1/ai_call/runtime_control_controller.py`
- 修改：`tests/test_ai_call_runtime_entry_controller.py`
- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：写 Query 红灯测试**

定义只读快照与路由：

```text
GET /ai-call/runtime/commands/{command_id}
```

响应只包含：

```text
commandId, callId, commandSeq, commandType, status,
result, errorMessage, createdAt, claimedAt, finishedAt
```

测试覆盖：

- PENDING/RETRY_WAIT 显示等待；PROCESSING 显示处理中；
- SUCCEEDED 返回规范化 `result_json`；DEAD/SUPERSEDED/CANCELED 返回错误与终态；
- 当前租户不存在返回 404；另一个租户拥有相同命令 ID 时仍返回 404；
- 非数字或越界 ID 返回 422；
- 不暴露 `sensitive_payload_ciphertext`、`processing_token`、`dispatch_token`。

- [ ] **步骤 2：运行红灯测试**

```bash
uv run pytest tests/test_ai_call_runtime_entry_controller.py -k 'command_query' -q
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'command_query' -q
```

- [ ] **步骤 3：实现最小只读查询**

- `RuntimeCommandRepository.get_command(tenant_id, command_id)` 使用二者同时过滤；
- `result_json` 只接受 JSON object 或 `null`；损坏数据 fail closed 为明确服务错误，不把原始字符串透传；
- Controller 从认证上下文取 tenant，不接受客户端 tenant 参数；
- 查询不加行锁、不续租、不改变命令状态、不访问 Redis 或 Provider。

- [ ] **步骤 4：运行绿灯并提交**

```bash
uv run pytest tests/test_ai_call_runtime_entry_controller.py -q
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'command_query' -q
git add \
  app/services/ai_call/runtime_control/command_repository.py \
  app/api/v1/ai_call/schema.py \
  app/api/v1/ai_call/runtime_control_controller.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --cached --check
git commit -m "feat(ai-call): 增加运行时命令状态查询"
```

---

## Task 4：提供可运行但绝不联网的确定性 Provider Stub

**合同：** 16.2A Provider Stub；真实 Provider 与 16.3 均范围外。

**文件：**

- 修改：`app/services/ai_call/runtime_control/provider_stub.py`
- 修改：`app/services/ai_call/runtime_control/lifecycle.py`
- 修改：`tests/test_ai_call_runtime_stub_handlers.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`

- [ ] **步骤 1：写默认 Stub 红灯测试**

新增 `DeterministicWebProviderStub` 或等价工厂，测试：

```python
create = await stub.apply(create_effect_claim)
assert create.kind is ProviderObservationKind.RESOURCE_PRESENT
assert create.provider_reference.startswith("stub:")

destroy = await stub.apply(destroy_effect_claim)
assert destroy.kind is ProviderObservationKind.TERMINAL_CONFIRMED

with pytest.raises(LookupError):
    await stub.apply(unknown_effect_claim)
```

同时断言 lifecycle 注入的是该 Stub，且构造/start/stop 期间没有网络客户端、LiveKit、SIP 或 Redis 调用。

- [ ] **步骤 2：运行红灯测试**

```bash
uv run pytest \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_lifecycle.py -q
```

- [ ] **步骤 3：实现严格映射**

- 只允许 `CREATE_ROOM`、`ATTACH_AGENT_PARTICIPANT` 返回 `RESOURCE_PRESENT` 与稳定 `stub:{resource_key}` 引用；
- 只允许 `DISCONNECT_AGENT_PARTICIPANT`、`DELETE_ROOM` 返回 `TERMINAL_CONFIRMED`；
- SIP、Egress 和其他未识别 Effect 一律抛出 `LookupError`，禁止默认成功；
- 保留 `ScriptedProviderStub` 供故障/恢复测试注入迟到、未知和重试序列；
- `start_runtime_control_lifecycle` 只替换空脚本 Stub，不引入任何 SDK 或网络配置。

- [ ] **步骤 4：运行绿灯并提交**

```bash
uv run pytest \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_runtime_owner_repository.py -q
git add \
  app/services/ai_call/runtime_control/provider_stub.py \
  app/services/ai_call/runtime_control/lifecycle.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_lifecycle.py
git diff --cached --check
git commit -m "feat(ai-call): 增加确定性 DB-only Provider Stub"
```

---

## Task 5：用隔离 PostgreSQL 证明 Web 双实例恢复闭环

**合同：** `CMD-01`、`OWN-02`、`EFF-02`、终态屏障；范围修正规格第 6、7 节。

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [ ] **步骤 1：新增单条 Web 垂直闭环测试**

测试必须通过真实仓储和服务对象执行以下顺序：

```text
RuntimeEntryStartService.submit(web)
  -> 两个 DispatcherControlService 并发 run_once
  -> 两个独立 RuntimeControlService 并发 run_once
  -> Command Query = SUCCEEDED
  -> Bootstrap = ready 且 tokenAvailable = false
  -> RuntimeCommandRepository.request_end
  -> 两个 Runtime 并发 run_once
  -> Command Query = SUCCEEDED
  -> Bootstrap = terminal
```

断言：

- Dispatcher 分配成功总数为 1；非 Owner Runtime 处理数为 0；
- 每个 create/destroy resource key 的 Stub 调用恰好一次；
- Record 最终 `completed/clean`，`runtime_owner_id is null`，容量类别为 `none`；
- Worker `active_call_count/active_cleanup_count` 均归零；
- 不存在 Reservation；Record 的 `entry_type` 只有 `web`；
- 命令从未进入 `DISPATCHING/PUBLISHED`，`dispatch_token/dispatch_expires_at/published_at/stream_message_id` 全为空；
- 全程不创建 Preview、SIP、Outbound 或 Handoff 业务行。

- [ ] **步骤 2：新增等待、超时和幂等分支**

同一 PostgreSQL 测试文件再覆盖：

- 无 Worker 时 command query 为 `PENDING`、bootstrap 为 `starting`；
- 数据库截止到期后 Dispatcher 写入 `DEAD/failed/ALLOCATION_TIMEOUT`，没有 Owner/Effect/Reservation；
- 同键同请求仅一条 Record/Command；同键异请求 409 所对应的仓储异常不留下第二条 Record；
- END 建屏障后任何普通命令或创建 Effect 都被拒绝。

- [ ] **步骤 3：运行隔离 PostgreSQL 16 测试**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'web_db_only or command_query' -q
```

预期：新增测试与既有 migration、Owner、Effect、Recovery 测试共享同一隔离 PostgreSQL 16，未连接任何外部依赖。

- [ ] **步骤 4：提交集成证据**

```bash
git add tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --cached --check
git commit -m "test(ai-call): 验证 Web DB-only 双实例闭环"
```

---

## Task 6：测量 DB-only 延迟并执行最终门禁

**合同：** 范围修正规格第 5.2 节；测量决定是否另开加速切片，不改变本计划实现。

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 新增：`docs/superpowers/reports/2026-08-01-ai-call-web-db-only-latency.md`

- [ ] **步骤 1：增加可重复的 DB-only 延迟测量**

在隔离 PostgreSQL 中提交至少 20 条 Web `START_CALL`，由实际 Dispatcher/Runtime 循环推进。使用 PostgreSQL `created_at` 与首次 `claimed_at` 计算“提交到 PROCESSING”毫秒数，报告：

```text
sample_count, p50_ms, p95_ms, max_ms,
scan_backlog_remaining, worker_active_count,
dispatch_or_stream_fields_written
```

测量不得 monkeypatch 数据库时间，不得调用 Redis 或 Provider 网络；Provider 使用 Task 4 的确定性 Stub。报告记录命令、PostgreSQL 版本、隔离级别和原始样本摘要。

- [ ] **步骤 2：运行测量并执行停止规则**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'web_db_only_latency' -q -s
```

判定：

- `p95_ms <= 1000`、无持续 backlog、无池饱和证据：记录“继续 DB-only”；
- 任一条件不满足：记录具体数值和复现命令，停止本计划的加速讨论；下一切片只允许先评审简单唤醒，不能在此提交 Redis Streams。

性能测量不使用脆弱的硬编码 CI 断言；一致性测试仍必须硬断言所有命令最终被处理且无双执行。

- [ ] **步骤 3：运行完整安全验证**

```bash
uv run pytest \
  tests/test_ai_call_process_roles.py \
  tests/test_ai_call_runtime_command_repository.py \
  tests/test_ai_call_runtime_entry_start_service.py \
  tests/test_ai_call_runtime_entry_controller.py \
  tests/test_ai_call_runtime_entry_legacy_guards.py \
  tests/test_ai_call_runtime_bootstrap_service.py \
  tests/test_ai_call_runtime_token_service.py \
  tests/test_ai_call_runtime_start_readiness.py \
  tests/test_ai_call_runtime_stub_handlers.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_web_runtime_entry.py \
  tests/test_ai_call_voice_preview.py \
  tests/test_ai_call_phase_b4_prompt_config.py -q

./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py -q

uv run ruff check .
git diff --check
```

禁止启动业务服务、拨号或连接 Redis、LiveKit、SIP、Egress、Linphone、真实 Provider。

- [ ] **步骤 4：检查范围与脏树隔离**

```bash
rg -n 'redis|xadd|xread|xautoclaim|livekit|sip|linphone|egress' \
  app/services/ai_call/runtime_control/provider_stub.py \
  tests/test_ai_call_web_runtime_entry.py
git status --short --branch
git diff --cached --check
git diff --cached --stat
```

预期：新增 Web/Stub 文件不存在外部连接代码；用户原有脏改动仍在工作树但不在暂存区。

- [ ] **步骤 5：提交延迟证据**

```bash
git add \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  docs/superpowers/reports/2026-08-01-ai-call-web-db-only-latency.md
git diff --cached --check
git commit -m "test(ai-call): 记录 Web DB-only 延迟证据"
```

---

## 最终交付说明

实施完成后只允许宣称：

> Web 创建入口已经接入可恢复的 PostgreSQL DB-only 控制面，并在确定性 Provider Stub 下完成双实例 START/END、查询、幂等和清理闭环。

不得宣称：

- 浏览器实时语音已可用；
- LiveKit/Qwen/真实 Provider 已接入；
- SIP/正式外呼已完成；
- Redis 加速已实现；
- 音色轻量试听已完成。

最终汇报必须列出：提交列表、单元测试数量、PostgreSQL 测试数量、P50/P95/max、lint、`git diff --check`、剩余既有脏文件，以及下一切片是否需要简单唤醒评审。
