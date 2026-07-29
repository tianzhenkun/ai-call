# 转人工意图确认与坐席可用性分流实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 客户转人工意图不完整时先进行一次确认；意图明确后按无人在线、坐席繁忙、可立即接听、等待超时和技术故障分别处理，并保持现有 15 秒媒体接入和 60 秒总等待约束。

**架构：** 在实时 Agent 层只负责识别“明确意图”和“需要确认”，由转人工触发服务持有确认状态。新增只读坐席可用性服务，根据租户、场景、心跳和 presence 状态返回在线数与空闲数；转人工触发服务依据快照选择排队或立即失败。所有客户口播由异常协调器根据后端状态选择固定音频，模型不得自行推断坐席状态。

**技术栈：** Python 3.10、FastAPI、SQLAlchemy AsyncSession、pytest/anyio、LiveKit、现有 `AiCallHandoffExceptionManager` 固定音频播放链路。

---

## 文件结构

- 创建 `app/services/ai_call/handoff_availability_service.py`：只负责读取某通电话对应租户与场景的在线坐席数和空闲坐席数。
- 修改 `app/services/ai_call/agent_runner.py`：区分无关误触发与“转”“人工”等需要确认的残缺转人工表达。
- 修改 `app/services/ai_call/handoff_trigger_service.py`：保存待确认状态、消费客户确认，并按坐席可用性快照选择转人工路径。
- 修改 `app/api/v1/ai_call/service.py`：允许调用方传入等待提示类型，并让失败原因驱动确定性的异常提示。
- 修改 `app/services/ai_call/handoff_exception_manager.py`：根据等待/失败原因选择固定提示音和事件中的固定文案。
- 修改 `app/plugin/init_app.py`：生产 worker 显式注入坐席可用性服务，测试可按需注入 fake。
- 修改 `app/config/setting.py`：登记忙碌等待、无人在线、等待超时和技术故障提示音路径及固定文案。
- 创建 `static/ai-call/audio/handoff-busy-waiting.wav`：有在线坐席但当前全部忙碌时播放。
- 创建 `static/ai-call/audio/handoff-no-online-agent.wav`：没有符合场景的在线坐席时播放。
- 创建 `static/ai-call/audio/handoff-busy-timeout.wav`：60 秒仍无人真实接通时播放。
- 创建 `static/ai-call/audio/handoff-service-unavailable.wav`：转人工技术故障时播放。
- 修改 `tests/test_ai_call_phase_a_core.py`：覆盖残缺表达确认和无关误触发。
- 修改 `tests/test_ai_call_phase_b1_records.py`：覆盖确认后创建 handoff、坐席状态分流和原因化提示音。
- 修改 `tests/test_ai_call_agent_console_claim.py`：只补充 15 秒认领超时仍受 60 秒总等待约束的回归断言。

### 任务 1：残缺转人工表达只触发一次确认

**文件：**
- 修改：`app/services/ai_call/agent_runner.py`
- 修改：`app/services/ai_call/handoff_trigger_service.py`
- 测试：`tests/test_ai_call_phase_a_core.py`
- 测试：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写 Agent 层失败测试**

在 `tests/test_ai_call_phase_a_core.py` 增加：

```python
@pytest.mark.anyio
async def test_realtime_agent_runner_requests_confirmation_for_partial_handoff_intent() -> None:
    ...
    runner._pending_turn(call_id).transcript_parts = ["转。"]
    await runner._handle_handoff_tool_done(
        call_id,
        provider,
        ProviderEvent(
            type="tool_call_done",
            payload={
                "call_id": "handoff_tool_partial",
                "name": "request_handoff",
                "arguments": json.dumps({"reason": "customer_request"}),
            },
        ),
    )
    requested = next(event for event in store.list(call_id) if event.type == "handoff_tool_requested")
    assert requested.payload["confirmationRequired"] is True
    assert provider.submitted_tool_results == [
        (
            "handoff_tool_partial",
            "用户的转人工表达不完整。请只询问：您是希望转接人工客服吗？"
            "不得声称坐席繁忙、暂无人工接入或正在转接。",
        )
    ]
```

保留现有“干什么？”测试，并断言它仍是 `handoff_tool_ignored`，不会进入确认流程。

- [ ] **步骤 2：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_phase_a_core.py \
  -k 'partial_handoff_intent or rejects_customer_handoff_tool_without_explicit_intent' -q
```

预期：新增测试因没有 `confirmationRequired` 失败；现有无关误触发测试通过。

- [ ] **步骤 3：实现最小 Agent 判定**

在 `agent_runner.py` 增加窄范围残缺表达判断：

```python
PARTIAL_HANDOFF_INTENT_VALUES = frozenset({"转", "人工", "客服", "真人"})
CUSTOMER_HANDOFF_CONFIRMATION_TOOL_RESULT = (
    "用户的转人工表达不完整。请只询问：您是希望转接人工客服吗？"
    "不得声称坐席繁忙、暂无人工接入或正在转接。"
)
```

当规则分类未命中但标准化文本属于上述集合时，写入：

```python
{
    "toolCallId": tool_call_id,
    "reason": "customer_request",
    "confirmationRequired": True,
    "transcriptPreview": self._text_preview(transcript),
}
```

并提交固定确认工具结果。其他未命中文本继续沿用原有拒绝路径。

- [ ] **步骤 4：编写触发服务确认测试**

在 `tests/test_ai_call_phase_b1_records.py` 增加：

```python
@pytest.mark.anyio
async def test_customer_partial_handoff_is_created_only_after_affirmative_confirmation(
    b1_service,
) -> None:
    ...
    event_store.append(
        call_id,
        "handoff_tool_requested",
        "agent",
        {
            "toolCallId": "handoff_tool_partial",
            "reason": "customer_request",
            "confirmationRequired": True,
        },
    )
    await worker.flush_pending()
    assert (await service.list_handoffs(call_id))["total"] == 0

    event_store.append(
        call_id,
        "user_transcript_done",
        "provider",
        {"item_id": "confirm_1", "transcript": "是的，转人工。"},
    )
    await worker.flush_pending()
    assert (await service.list_handoffs(call_id))["rows"][0]["status"] == "requested"
```

- [ ] **步骤 5：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'customer_partial_handoff_is_created_only_after_affirmative_confirmation' -q
```

预期：FAIL，触发服务当前会立即创建 handoff。

- [ ] **步骤 6：实现确认状态**

扩展 `_handle_tool_request()`：

```python
if event.payload.get("confirmationRequired") is True:
    self._pending_confirmations[event.call_id] = PendingHandoffConfirmation(
        reason="customer_request",
        request_message="客户确认转人工",
        tool_call_id=tool_call_id,
    )
    event_store.append(
        event.call_id,
        "handoff_confirmation_requested",
        "handoff",
        {"reason": "customer_request", "toolCallId": tool_call_id},
    )
    return
```

复用现有肯定、拒绝和催促表达判断；肯定后调用 `_confirm_handoff()`，拒绝后清理 pending，不创建 handoff。

- [ ] **步骤 7：运行定向测试**

运行：

```bash
uv run pytest tests/test_ai_call_phase_a_core.py \
  -k 'handoff_tool' -q
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'handoff_trigger_worker' -q
```

预期：全部通过。

- [ ] **步骤 8：提交**

```bash
git add app/services/ai_call/agent_runner.py \
  app/services/ai_call/handoff_trigger_service.py \
  tests/test_ai_call_phase_a_core.py \
  tests/test_ai_call_phase_b1_records.py
git commit -m "fix(ai-call): confirm partial handoff intent"
```

### 任务 2：按租户和场景计算在线与空闲坐席

**文件：**
- 创建：`app/services/ai_call/handoff_availability_service.py`
- 修改：`app/plugin/init_app.py`
- 测试：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写失败测试**

增加三组数据库用例：

```python
@pytest.mark.anyio
async def test_handoff_availability_counts_online_and_available_agents(...):
    snapshot = await service.get_for_call(call_id)
    assert snapshot.online_agent_count == 2
    assert snapshot.available_agent_count == 1

@pytest.mark.anyio
async def test_handoff_availability_excludes_stale_paused_and_wrong_scene_agents(...):
    snapshot = await service.get_for_call(call_id)
    assert snapshot.online_agent_count == 0
    assert snapshot.available_agent_count == 0

@pytest.mark.anyio
async def test_handoff_availability_requires_existing_call(...):
    with pytest.raises(CustomException):
        await service.get_for_call("missing-call")
```

- [ ] **步骤 2：运行测试确认失败**

运行：

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py -k 'handoff_availability' -q
```

预期：FAIL，模块尚不存在。

- [ ] **步骤 3：实现只读快照服务**

创建：

```python
@dataclass(frozen=True, slots=True)
class HandoffAgentAvailability:
    online_agent_count: int
    available_agent_count: int


class AiCallHandoffAvailabilityService:
    ONLINE_STATUSES = frozenset(
        {"available", "claiming", "in_call", "reconnecting", "wrap_up_quick"}
    )

    def __init__(self, db: AsyncSession, *, heartbeat_seconds: int = 30) -> None:
        self.db = db
        self.heartbeat_seconds = max(1, heartbeat_seconds)

    async def get_for_call(self, call_id: str) -> HandoffAgentAvailability:
        ...
```

查询必须同时连接 `ai_call_agent_profile`、`ai_call_agent_scene_scope` 和 `ai_call_handoff_agent`，校验同租户、场景匹配、档案启用、心跳不早于 `now - 30s`。`available_agent_count` 额外要求 `status='available'` 且 `active_handoff_id IS NULL`。

- [ ] **步骤 4：生产 worker 显式注入**

在 `app/plugin/init_app.py` 构造触发服务时传入：

```python
availability_service_factory=AiCallHandoffAvailabilityService,
```

测试中未注入时保持原有行为；需要验证分流的测试显式注入 fake 或真实服务，避免破坏 B1 旧测试构造。

- [ ] **步骤 5：运行测试**

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py -k 'handoff_availability' -q
uv run pytest tests/test_ai_call_phase_a_core.py \
  -k 'app_handoff_trigger_worker_enables_transcript_trigger' -q
```

预期：全部通过。

- [ ] **步骤 6：提交**

```bash
git add app/services/ai_call/handoff_availability_service.py \
  app/plugin/init_app.py \
  tests/test_ai_call_phase_b1_records.py \
  tests/test_ai_call_phase_a_core.py
git commit -m "feat(ai-call): inspect handoff agent availability"
```

### 任务 3：坐席可用性驱动排队或立即失败

**文件：**
- 修改：`app/services/ai_call/handoff_trigger_service.py`
- 修改：`app/api/v1/ai_call/service.py`
- 测试：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写三个失败测试**

```python
@pytest.mark.anyio
async def test_handoff_with_available_agent_enters_requested_pool(...):
    fake_availability.result = HandoffAgentAvailability(1, 1)
    ...
    assert row["status"] == "requested"
    assert waiting_prompt_kind == "available"

@pytest.mark.anyio
async def test_handoff_with_only_busy_agents_enters_requested_pool(...):
    fake_availability.result = HandoffAgentAvailability(1, 0)
    ...
    assert row["status"] == "requested"
    assert waiting_prompt_kind == "busy"

@pytest.mark.anyio
async def test_handoff_without_online_agent_fails_without_waiting(...):
    fake_availability.result = HandoffAgentAvailability(0, 0)
    ...
    assert row["status"] == "failed"
    assert row["endReason"] == "no_online_agent"
    assert row["failureStage"] == "availability_check"
```

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'with_available_agent or only_busy_agents or without_online_agent' -q
```

预期：FAIL，当前触发服务不读取坐席快照。

- [ ] **步骤 3：实现触发分流**

在创建 handoff 前读取快照：

```python
availability = await self._get_availability(call_id)
waiting_prompt_kind = "available" if availability.available_agent_count > 0 else "busy"
handoff = await self._create_handoff(
    call_id=call_id,
    reason=reason,
    request_message=request_message,
    waiting_prompt_kind=waiting_prompt_kind,
)
if availability.online_agent_count == 0:
    handoff = await self._fail_handoff(
        handoff_id=handoff["handoffId"],
        failure_stage="availability_check",
        failure_message="当前场景没有在线可接范围坐席",
        end_reason="no_online_agent",
    )
```

快照查询失败时不能伪装成坐席繁忙，创建 handoff 后以 `handoff_service_unavailable` 结束，并记录异常类型。

- [ ] **步骤 4：扩展 AiCallService 的原因透传**

扩展方法签名：

```python
async def create_handoff(
    ...,
    waiting_prompt_kind: str = "default",
) -> dict:
    ...

async def fail_handoff(
    ...,
    end_reason: str | None = None,
) -> dict:
    ...
```

`end_reason` 存在时通过现有 repository 更新 handoff 的 `end_reason`，异常协调器使用该原因选择提示；不修改 `AiCallHandoffService`，避免与当前话后状态收敛改动交叉。

- [ ] **步骤 5：运行定向测试**

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'handoff_trigger_worker or handoff_timeout or handoff_fail' -q
```

预期：全部通过。

- [ ] **步骤 6：提交**

```bash
git add app/services/ai_call/handoff_trigger_service.py \
  app/api/v1/ai_call/service.py \
  tests/test_ai_call_phase_b1_records.py
git commit -m "feat(ai-call): route handoff by agent availability"
```

### 任务 4：按后端原因播放确定性提示

**文件：**
- 修改：`app/services/ai_call/handoff_exception_manager.py`
- 修改：`app/config/setting.py`
- 修改：`app/api/v1/ai_call/service.py`
- 创建：`static/ai-call/audio/handoff-busy-waiting.wav`
- 创建：`static/ai-call/audio/handoff-no-online-agent.wav`
- 创建：`static/ai-call/audio/handoff-busy-timeout.wav`
- 创建：`static/ai-call/audio/handoff-service-unavailable.wav`
- 测试：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：编写提示选择失败测试**

```python
@pytest.mark.anyio
@pytest.mark.parametrize(
    ("reason", "expected_file", "expected_text"),
    [
        (
            "no_online_agent",
            "handoff-no-online-agent.wav",
            "当前暂无人工坐席在线，我先为您记录需求，稍后安排工作人员联系您。",
        ),
        (
            "handoff_timeout",
            "handoff-busy-timeout.wav",
            "当前人工坐席繁忙，暂未接通，我先为您记录需求。",
        ),
        (
            "handoff_service_unavailable",
            "handoff-service-unavailable.wav",
            "人工转接服务暂时不可用，我先为您记录需求。",
        ),
    ],
)
async def test_handoff_exception_prompt_is_selected_by_reason(...):
    ...
```

另增加 `waiting_prompt_kind="busy"` 选择 `handoff-busy-waiting.wav` 的测试。

- [ ] **步骤 2：运行测试确认失败**

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'prompt_is_selected_by_reason or busy_waiting_prompt' -q
```

预期：FAIL，当前只有统一 `handoff-unavailable.wav`。

- [ ] **步骤 3：实现提示配置与选择**

异常协调器内部使用不可变配置：

```python
@dataclass(frozen=True, slots=True)
class HandoffPrompt:
    audio_path: Path | None
    text: str
```

`start_waiting_tone()` 按 `available`、`busy`、`default` 选择首次提示；`trigger_exception_close()` 将 `call_end_reason` 传到 `_play_unavailable_prompt()`，分别选择无人在线、等待超时、技术故障和默认兜底。事件中的 `promptText` 必须与实际音频语义一致。

- [ ] **步骤 4：生成固定音频资产**

使用 macOS 系统中文音色生成临时 AIFF，再统一转换为 24kHz、16-bit、mono PCM：

```bash
say -v Tingting "当前人工坐席繁忙，正在为您排队转接，请稍候。" \
  -o /tmp/handoff-busy-waiting.aiff
ffmpeg -y -i /tmp/handoff-busy-waiting.aiff -ar 24000 -ac 1 -c:a pcm_s16le \
  static/ai-call/audio/handoff-busy-waiting.wav
```

对另外三条固定文案执行同样转换。运行：

```bash
for audio_file in static/ai-call/audio/handoff-*.wav; do
  ffprobe -v error -show_entries stream=sample_rate,channels,sample_fmt \
    -of default=nw=1 "$audio_file"
done
```

预期：新增文件均为 `sample_rate=24000`、`channels=1`、`sample_fmt=s16`。

- [ ] **步骤 5：运行定向测试**

```bash
uv run pytest tests/test_ai_call_phase_b1_records.py \
  -k 'handoff_timeout or handoff_fail or handoff_prompt or waiting_tone' -q
```

预期：全部通过。

- [ ] **步骤 6：提交**

```bash
git add app/services/ai_call/handoff_exception_manager.py \
  app/config/setting.py \
  app/api/v1/ai_call/service.py \
  static/ai-call/audio/handoff-busy-waiting.wav \
  static/ai-call/audio/handoff-no-online-agent.wav \
  static/ai-call/audio/handoff-busy-timeout.wav \
  static/ai-call/audio/handoff-service-unavailable.wav \
  tests/test_ai_call_phase_b1_records.py
git commit -m "feat(ai-call): play reason-specific handoff prompts"
```

### 任务 5：验证 15 秒媒体接入与 60 秒总等待

**文件：**
- 修改：`tests/test_ai_call_agent_console_claim.py`

- [ ] **步骤 1：补充精确回归断言**

在现有认领超时测试中断言：

```python
assert handoff.claim_expires_at <= handoff.expires_at
assert (
    handoff.claim_expires_at - handoff.accepted_at
).total_seconds() <= settings.AI_CALL_AGENT_CLAIM_CONNECT_TIMEOUT_SECONDS
```

并保留以下状态链：

```python
accepted --15秒未媒体接入且总等待未结束--> requested
requested --60秒总等待结束--> expired + 唯一 handoff_unanswered 回访任务
```

- [ ] **步骤 2：运行坐席生命周期测试**

```bash
uv run pytest tests/test_ai_call_agent_console_claim.py -q
uv run pytest tests/test_ai_call_agent_console_reconcile.py -q
uv run pytest tests/test_ai_call_agent_console_models.py -q
```

预期：全部通过，且现有 `15/15/60` 默认配置断言不变。

- [ ] **步骤 3：提交**

```bash
git add tests/test_ai_call_agent_console_claim.py
git commit -m "test(ai-call): lock handoff claim and wait deadlines"
```

### 任务 6：回归与真实 Linphone 验收

**文件：**
- 不新增业务文件。

- [ ] **步骤 1：运行静态检查**

```bash
uv run ruff check \
  app/services/ai_call/agent_runner.py \
  app/services/ai_call/handoff_trigger_service.py \
  app/services/ai_call/handoff_availability_service.py \
  app/services/ai_call/handoff_exception_manager.py \
  app/api/v1/ai_call/service.py \
  app/plugin/init_app.py \
  app/config/setting.py \
  tests/test_ai_call_phase_a_core.py \
  tests/test_ai_call_phase_b1_records.py \
  tests/test_ai_call_agent_console_claim.py
```

预期：无错误。

- [ ] **步骤 2：运行转人工回归测试**

```bash
uv run pytest tests/test_ai_call_phase_a_core.py -k 'handoff' -q
uv run pytest tests/test_ai_call_phase_b1_records.py -k 'handoff' -q
uv run pytest tests/test_ai_call_agent_console_claim.py -q
uv run pytest tests/test_ai_call_agent_console_reconcile.py -q
```

预期：全部通过。

- [ ] **步骤 3：重启并核对 19011**

按 `docs/livekit-ai-outbound/p1-local-test-baseline.md` 重新检查 LiveKit、FreeSWITCH、Linphone 注册、Redis 和 19011 配置。启动参数必须包含：

```bash
ENVIRONMENT=dev
SERVER_PORT=19011
DATABASE_TYPE=sqlite
DATABASE_NAME=/tmp/ai_call_ed81_local
REDIS_HOST=127.0.0.1
REDIS_PORT=6391
AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=true
```

预期：健康检查成功，任务详情可见“测试拨打”。

- [ ] **步骤 4：依次验收三条真实路径**

1. 坐席在线且空闲：客户说“转人工”，handoff 进入 `requested`，坐席认领后 15 秒内媒体接通。
2. 坐席在线但忙碌：客户听到繁忙排队提示，handoff 保持 `requested`，释放坐席后可接听。
3. 坐席离线：客户听到无人在线提示，不等待 60 秒，handoff 立即 `failed` 且 `endReason=no_online_agent`。

每条路径核对：

```text
call_id
handoff_id
事件顺序
客户实际听到的话术
坐席 presence 状态
handoff 终态
是否只创建一条未接回访任务
```

- [ ] **步骤 5：整理验收结果**

输出每条路径的 call ID、handoff ID、关键事件、数据库状态、客户听感和未通过项。真实通话未验证前，不得宣称转人工完成。
