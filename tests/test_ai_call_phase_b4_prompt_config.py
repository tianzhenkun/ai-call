from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.schema import (
    CreateWebSessionRequest,
    PromptProfileCreateRequest,
    PromptProfilePreviewRequest,
    VoiceProfileCreateRequest,
)
from app.api.v1.ai_call.service import AiCallService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.prompt_config import (
    PROMPT_PROVIDER_BUSINESS_QUERY,
    BusinessPromptResolver,
    BusinessPromptResult,
    DebugPromptProvider,
    DefaultPromptProvider,
    PromptComposer,
)
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.services.ai_call.voice_profile import BUILTIN_QWEN_OMNI_REALTIME_VOICES


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    async def delete_room(self, room_name: str) -> None:
        _ = room_name


class CapturingAgentRunner:
    def __init__(self) -> None:
        self.started_sessions: list[CallSession] = []

    async def start(self, session: CallSession) -> None:
        self.started_sessions.append(session)

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def suspend_for_handoff(self, call_id: str) -> None:
        _ = call_id

    async def stop(self, call_id: str) -> None:
        _ = call_id


def build_runtime_config() -> AiCallRuntimeConfig:
    return AiCallRuntimeConfig(
        livekit_url="wss://livekit.test",
        livekit_api_key="livekit-key",
        livekit_api_secret="livekit-secret",
        browser_token_ttl_seconds=600,
        dashscope_api_key="dashscope-secret",
        dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
        qwen_realtime_model="qwen3.5-omni-plus-realtime",
        qwen_realtime_voice="Tina",
        default_prompt="你是一个电话外呼助手，回答要简短自然。",
        opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
        web_audio_echo_cancellation=True,
        web_audio_noise_suppression=True,
        web_audio_auto_gain_control=True,
        vad_type="server_vad",
        vad_threshold=0.5,
        vad_silence_duration_ms=800,
    )


def test_legacy_effective_config_keeps_agent_runner_prompt_composition() -> None:
    orchestrator = AiCallOrchestrator(
        config=build_runtime_config(),
        livekit_room_manager=FakeLiveKitRoomManager(),
        agent_runner=CapturingAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    legacy_config = orchestrator._build_effective_config(voice=None, prompt=None)
    composed_prompt = PromptComposer(handoff_component_enabled=True).compose(
        BusinessPromptResult(
            prompt="业务话术",
            opening_message="您好，这是开场白。",
            source_key="test.scene",
        )
    )
    b4_config = orchestrator._build_effective_config(
        voice=None,
        prompt=None,
        prompt_effective_config=composed_prompt,
    )

    assert legacy_config.instructions is None
    assert legacy_config.prompt == "你是一个电话外呼助手，回答要简短自然。"
    assert b4_config.instructions == composed_prompt.instructions
    assert b4_config.prompt == composed_prompt.instructions


@pytest.fixture
async def b4_service():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        runtime_config = build_runtime_config()
        room_manager = FakeLiveKitRoomManager()
        agent_runner = CapturingAgentRunner()
        orchestrator = AiCallOrchestrator(
            config=runtime_config,
            livekit_room_manager=room_manager,
            agent_runner=agent_runner,
            registry=InMemorySessionRegistry(),
            event_store=InMemoryEventStore(),
        )
        service = AiCallService(
            orchestrator,
            AiCallRecordService(repository),
            prompt_repository=repository,
            prompt_resolver=BusinessPromptResolver(
                repository=repository,
                default_provider=DefaultPromptProvider(
                    default_prompt=runtime_config.default_prompt,
                    opening_message=runtime_config.opening_message,
                ),
                debug_provider=DebugPromptProvider(
                    opening_message=runtime_config.opening_message,
                ),
                timeout_seconds=2.0,
            ),
            prompt_composer=PromptComposer(handoff_component_enabled=True),
        )
        yield service, repository, room_manager, agent_runner

    await engine.dispose()


async def create_static_profile(service: AiCallService) -> dict:
    return await service.create_prompt_profile({
        "scene_code": "debt_promise_repay_reminder",
        "name": "承诺还款提醒",
        "provider_key": "static_profile",
        "prompt_text": "你正在联系{{customerName}}，提醒客户按承诺时间还款，语气克制。",
        "opening_message": "您好{{customerName}}，我是灵宸智能助手，想和您确认一下还款安排。",
    })


@pytest.mark.anyio
async def test_prompt_components_include_runtime_common_constraints(b4_service) -> None:
    service, _repository, _room_manager, _agent_runner = b4_service

    components = await service.list_prompt_components()

    assert [row["componentKey"] for row in components["rows"]] == [
        "platform_constraints",
        "handoff_capability",
        "call_end_tool",
    ]
    assert components["rows"][0]["name"] == "平台关键约束"
    assert "当前日期：" in components["rows"][0]["content"]
    assert "Asia/Shanghai" in components["rows"][0]["content"]
    assert "schedule_call_end" in components["rows"][2]["content"]
    assert "仅当上下文明确表明通话已适合结束" in components["rows"][2]["content"]
    assert "必须调用 schedule_call_end" in components["rows"][2]["content"]


@pytest.mark.anyio
async def test_static_prompt_profile_composes_effective_instructions(b4_service) -> None:
    service, _repository, _room_manager, agent_runner = b4_service
    profile = await create_static_profile(service)

    preview = await service.preview_prompt_profile(
        business_id="324800000000000001",
        scene_code="debt_promise_repay_reminder",
        business_params={"customerName": "张总"},
        prompt=None,
    )
    result = await service.create_web_session(
        voice="Cindy",
        prompt=None,
        business_id="324800000000000001",
        scene_code="debt_promise_repay_reminder",
        business_params={"customerName": "张总"},
    )

    assert profile["sceneCode"] == "debt_promise_repay_reminder"
    assert preview["promptSourceKey"] == "debt_promise_repay_reminder"
    assert "平台关键约束" in preview["instructions"]
    assert "当前日期：" in preview["instructions"]
    assert "转人工能力约束" in preview["instructions"]
    assert "业务话术" in preview["instructions"]
    assert "你正在联系张总" in preview["instructions"]
    assert "{{customerName}}" not in preview["instructions"]
    assert preview["openingMessage"] == "您好张总，我是灵宸智能助手，想和您确认一下还款安排。"
    assert result.effective_config.prompt_source_key == "debt_promise_repay_reminder"
    assert result.effective_config.prompt_hash == preview["promptHash"]
    assert agent_runner.started_sessions[0].effective_config.instructions == preview["instructions"]
    events = await service.orchestrator.list_events(result.call_id)
    assert events.rows[0].payload == {
        "promptHash": preview["promptHash"],
        "openingMessageHash": preview["openingMessageHash"],
        "promptSourceKey": "debt_promise_repay_reminder",
    }
    assert "提醒客户按承诺时间还款" not in str(events.rows[0].payload)


@pytest.mark.anyio
async def test_static_prompt_profile_missing_placeholder_fails_before_room_create(
    b4_service,
) -> None:
    service, repository, room_manager, _agent_runner = b4_service
    await create_static_profile(service)

    with pytest.raises(CustomException) as preview_exc_info:
        await service.preview_prompt_profile(
            business_id="324800000000000001",
            scene_code="debt_promise_repay_reminder",
            business_params={},
            prompt=None,
        )

    assert preview_exc_info.value.msg == "businessParams 缺少提示词占位符：customerName"
    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt=None,
            business_id="324800000000000001",
            scene_code="debt_promise_repay_reminder",
            business_params={},
        )

    assert exc_info.value.msg == "businessParams 缺少提示词占位符：customerName"
    assert room_manager.created_rooms == []
    rows, total = await repository.list_records()
    assert total == 1
    assert rows[0].status == CallSessionStatus.FAILED.value
    assert rows[0].end_reason == "prompt_placeholder_missing"


@pytest.mark.anyio
async def test_voice_profiles_seed_and_validate_session_voice(b4_service) -> None:
    service, repository, _room_manager, agent_runner = b4_service

    listed = await service.list_voice_profiles(page_size=200)
    female = await service.list_voice_profiles(gender="女声", page_size=200)
    await create_static_profile(service)
    result = await service.create_web_session(
        voice="Cindy",
        prompt=None,
        business_id="324800000000000001",
        scene_code="debt_promise_repay_reminder",
        business_params={"customerName": "张总"},
    )

    assert listed["total"] == len(BUILTIN_QWEN_OMNI_REALTIME_VOICES)
    assert listed["rows"][0]["voice"] == "Tina"
    assert listed["rows"][0]["gender"] == "女声"
    assert any(row["voice"] == "Cindy" for row in female["rows"])
    assert result.effective_config.voice == "Cindy"
    assert agent_runner.started_sessions[-1].effective_config.voice == "Cindy"
    profile = await repository.get_voice_profile_by_voice(
        voice="Cindy",
        target_model="qwen3.5-omni-plus-realtime",
    )
    assert profile is not None


@pytest.mark.anyio
async def test_create_custom_voice_profile_and_use_for_session(b4_service) -> None:
    service, repository, _room_manager, agent_runner = b4_service

    created = await service.create_voice_profile({
        "voice": "custom_voice_001",
        "display_name": "张总自定义音色",
        "gender": "男声",
        "target_model": "qwen3.5-omni-plus-realtime",
        "remark": "百炼复刻音色",
    })
    await create_static_profile(service)
    result = await service.create_web_session(
        voice="custom_voice_001",
        prompt=None,
        business_id="324800000000000001",
        scene_code="debt_promise_repay_reminder",
        business_params={"customerName": "张总"},
    )
    listed = await service.list_voice_profiles(voice_type="自定义复刻", page_size=200)
    profile = await repository.get_voice_profile_by_voice(
        voice="custom_voice_001",
        target_model="qwen3.5-omni-plus-realtime",
    )

    assert created["voice"] == "custom_voice_001"
    assert created["displayName"] == "张总自定义音色"
    assert created["voiceType"] == "自定义复刻"
    assert created["gender"] == "男声"
    assert profile is not None
    assert listed["total"] == 1
    assert result.effective_config.voice == "custom_voice_001"
    assert agent_runner.started_sessions[-1].effective_config.voice == "custom_voice_001"


@pytest.mark.anyio
async def test_duplicate_custom_voice_profile_is_rejected(b4_service) -> None:
    service, _repository, _room_manager, _agent_runner = b4_service
    values = {
        "voice": "custom_voice_001",
        "display_name": "张总自定义音色",
        "target_model": "qwen3.5-omni-plus-realtime",
    }

    await service.create_voice_profile(values)

    with pytest.raises(CustomException) as exc_info:
        await service.create_voice_profile(values)

    assert exc_info.value.msg == "该模型下音色已存在"


@pytest.mark.anyio
async def test_unknown_voice_is_rejected_before_room_create(b4_service) -> None:
    service, _repository, room_manager, _agent_runner = b4_service

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice="missing_custom_voice",
            prompt=None,
            business_id="324800000000000001",
            scene_code="debt_promise_repay_reminder",
            business_params={},
        )

    assert exc_info.value.msg == "音色不存在或不适用于当前模型"
    assert room_manager.created_rooms == []


@pytest.mark.anyio
async def test_missing_prompt_profile_fails_before_livekit_room_created(b4_service) -> None:
    service, repository, room_manager, _agent_runner = b4_service

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt=None,
            business_id="324800000000000001",
            scene_code="missing_scene",
            business_params={},
        )

    assert exc_info.value.msg == "业务场景提示词配置不存在"
    assert room_manager.created_rooms == []
    rows, total = await repository.list_records()
    assert total == 1
    assert rows[0].status == CallSessionStatus.FAILED.value
    assert rows[0].end_reason == "prompt_profile_not_found"


@pytest.mark.anyio
async def test_empty_resolved_prompt_fails_before_livekit_room_created(b4_service) -> None:
    service, repository, room_manager, _agent_runner = b4_service
    await repository.create_prompt_profile(
        scene_code="empty_prompt_scene",
        name="空提示词异常场景",
        provider_key="static_profile",
        prompt_text=None,
        opening_message="您好，我是灵宸智能助手。",
    )

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt=None,
            business_id="324800000000000001",
            scene_code="empty_prompt_scene",
            business_params={},
        )

    assert exc_info.value.msg == "业务提示词不能为空"
    assert room_manager.created_rooms == []
    rows, total = await repository.list_records()
    assert total == 1
    assert rows[0].status == CallSessionStatus.FAILED.value
    assert rows[0].end_reason == "prompt_empty"


@pytest.mark.anyio
async def test_empty_opening_fails_before_livekit_room_created(b4_service) -> None:
    service, repository, room_manager, _agent_runner = b4_service
    await repository.create_prompt_profile(
        scene_code="empty_opening_scene",
        name="空开场白异常场景",
        provider_key="static_profile",
        prompt_text="提醒客户按承诺时间还款，语气克制。",
        opening_message=None,
    )

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt=None,
            business_id="324800000000000001",
            scene_code="empty_opening_scene",
            business_params={},
        )

    assert exc_info.value.msg == "开场白不能为空"
    assert room_manager.created_rooms == []
    rows, total = await repository.list_records()
    assert total == 1
    assert rows[0].status == CallSessionStatus.FAILED.value
    assert rows[0].end_reason == "opening_message_empty"


@pytest.mark.anyio
async def test_business_query_scene_builds_prompt_from_business_params(
    b4_service,
) -> None:
    service, _repository, _room_manager, _agent_runner = b4_service
    await service.create_prompt_profile({
        "scene_code": "intro_collection",
        "name": "催收产品介绍",
        "provider_key": PROMPT_PROVIDER_BUSINESS_QUERY,
        "prompt_text": None,
        "opening_message": None,
    })

    preview = await service.preview_prompt_profile(
        business_id="collection_product",
        scene_code="intro_collection",
        business_params={
            "productName": "灵宸智能催收中台",
            "targetCustomer": "有批量应收账款管理需求的企业",
            "openingMessage": "您好，我是灵宸智能助手，想向您介绍一下智能催收中台。",
        },
        prompt=None,
    )

    assert preview["promptSourceKey"] == "intro_collection"
    assert "灵宸智能催收中台" in preview["instructions"]
    assert "有批量应收账款管理需求的企业" in preview["instructions"]
    assert "预计什么时候归还" in preview["instructions"]
    assert "只追问一次大致时间范围" in preview["instructions"]
    assert preview["openingMessage"] == "您好，我是灵宸智能助手，想向您介绍一下智能催收中台。"


@pytest.mark.anyio
async def test_missing_scene_code_is_rejected_before_record_create(b4_service) -> None:
    service, repository, room_manager, _agent_runner = b4_service

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt=None,
            business_id="324800000000000001",
            scene_code=None,
            business_params={},
        )

    assert exc_info.value.msg == "sceneCode 不能为空"
    assert room_manager.created_rooms == []
    rows, total = await repository.list_records()
    assert total == 0
    assert rows == []


@pytest.mark.anyio
async def test_debug_prompt_is_removed_from_business_session(b4_service) -> None:
    service, _repository, room_manager, _agent_runner = b4_service

    with pytest.raises(CustomException) as exc_info:
        await service.create_web_session(
            voice=None,
            prompt="临时覆盖提示词",
            business_id="324800000000000001",
            scene_code="debt_promise_repay_reminder",
            business_params={},
        )

    assert exc_info.value.msg == "调试提示词已下线，请使用业务场景提示词配置"
    assert room_manager.created_rooms == []


def test_prompt_config_page_and_customer_page_use_business_fields() -> None:
    root = Path(__file__).parents[1] / "static/ai-call"
    index_html = (root / "index.html").read_text(encoding="utf-8")
    customer_html = (root / "customer.html").read_text(encoding="utf-8")
    customer_js = (root / "customer.js").read_text(encoding="utf-8")
    agent_html = (root / "agent.html").read_text(encoding="utf-8")
    agent_css = (root / "agent.css").read_text(encoding="utf-8")
    agent_js = (root / "agent.js").read_text(encoding="utf-8")
    ai_call_css = (root / "ai-call.css").read_text(encoding="utf-8")
    prompt_config_html = (root / "prompt-config.html").read_text(encoding="utf-8")
    prompt_config_js = (root / "prompt-config.js").read_text(encoding="utf-8")
    voice_config_html = (root / "voice-config.html").read_text(encoding="utf-8")
    voice_config_js = (root / "voice-config.js").read_text(encoding="utf-8")

    assert 'id="business-type-input"' not in customer_html
    assert '<select id="scene-code-input">' in customer_html
    assert "不指定，使用服务端默认话术" not in customer_html
    assert "加载业务场景中..." in customer_html
    assert 'id="prompt-input"' not in customer_html
    assert "调试提示词" not in customer_html
    assert 'placeholder="可选"' in customer_html
    assert "可选，JSON object" in customer_html
    assert '"customerName": "张总"' in customer_html
    assert 'id="session-status"' in customer_html
    assert 'id="connect-handoff"' not in customer_html
    assert 'id="cancel-handoff"' not in customer_html
    assert 'id="handoff-agent-identity"' not in customer_html
    assert "通话文本" in customer_html
    assert "payload.businessType" not in customer_js
    assert "payload.sceneCode" in customer_js
    assert "请先配置并选择业务场景" in customer_js
    assert "暂无可用业务场景" in customer_js
    assert "loadPromptProfiles" in customer_js
    assert "/ai-call/prompt-profiles" in customer_js
    assert "payload.prompt" not in customer_js
    assert "joinSeatAndCompleteHandoff" not in customer_js
    assert "/accept" not in customer_js
    assert "/connected" not in customer_js
    assert "/cancel" not in customer_js
    assert "/ai-call/voice-profiles" in customer_js
    assert "function apiPath" in customer_js
    assert "人工接管" in index_html
    assert "POC" in index_html
    assert "仅测试" not in index_html
    assert "portal-status-poc" in index_html
    assert "portal-status-test" not in index_html
    assert "打开测试台" not in index_html
    assert "agent.html" in index_html
    assert "voice-config.html" in index_html
    assert "语义分析" in index_html
    assert "entry-semantic-analysis.svg" in index_html
    assert "portal-card-disabled" in index_html
    assert "未实现" in index_html
    assert "semantic-analysis.html" not in index_html
    assert "business_query" in prompt_config_html
    assert "业务查询" in prompt_config_js
    assert 'src="./prompt-config.js' in prompt_config_html
    assert "/ai-call/prompt-profiles" in prompt_config_js
    assert "/ai-call/prompt-components" in prompt_config_js
    assert "function apiPath" in prompt_config_js
    assert "component-meta" not in prompt_config_js
    assert "business_prompt" not in prompt_config_js
    assert "opening_constraint" not in prompt_config_js
    assert 'id="status-pill"' not in voice_config_html
    assert 'id="open-create-voice"' in voice_config_html
    assert 'data-voice-type="自定义复刻"' in voice_config_html
    assert 'id="voice-form"' in voice_config_html
    assert 'id="voice-input"' in voice_config_html
    assert 'id="display-name-input"' in voice_config_html
    assert 'id="voice-gender-select"' not in voice_config_html
    assert 'id="voice-type-select"' not in voice_config_html
    assert "/ai-call/voice-profiles" in voice_config_js
    assert 'method: "POST"' in voice_config_js
    assert "function apiPath" in voice_config_js
    assert ".toast-stack" in ai_call_css
    assert "坐席标识" in agent_html
    assert "客户不可见" in agent_html
    assert "坐席状态" in agent_html
    assert 'id="agent-presence"' in agent_html
    assert 'id="save-agent-status"' in agent_html
    assert "仅测试" not in agent_html
    assert "坐席创建与管理" not in agent_html
    assert "排队与分配策略" not in agent_html
    assert "技能组路由" not in agent_html
    assert "接入弹屏" not in agent_html
    assert "会话工单" not in agent_html
    assert ".agent-status-card" in agent_css
    assert ".agent-pending-list" not in agent_css
    assert "agent-handoff-connect-state-pin-20260624" in agent_html
    assert "/ai-call/handoff-agents/" in agent_js
    assert "setAgentPresence(\"online\")" not in agent_js
    assert "fetchAgentStatus()" in agent_js
    assert "MIN_ACCEPT_REMAINING_MS" in agent_js
    assert "function ensureAgentMediaPreflight" in agent_js
    assert "当前页面无法使用麦克风" in agent_js
    assert agent_js.index("await ensureAgentMediaPreflight();") < agent_js.index(
        "await acceptHandoff("
    )
    assert "function selectFirstJoinableHandoffWhenIdle" in agent_js
    assert "selectFirstJoinableHandoffWhenIdle();" in agent_js
    assert "state.selectedHandoff = state.handoffs[0]" in agent_js
    assert 'state.selectedHandoff.status === "requested"' not in agent_js
    assert "failAcceptedHandoff" in agent_js
    assert "align-items: stretch" in agent_css
    assert ".agent-main-panel" in agent_css
    assert "function notify" in customer_js
    assert "function notify" in agent_js
    assert "function notify" in prompt_config_js
    assert "function notify" in voice_config_js
    assert "window.alert" not in prompt_config_js


def test_prompt_requests_reject_removed_fields() -> None:
    with pytest.raises(ValidationError):
        CreateWebSessionRequest.model_validate({
            "businessType": "debt_collection",
            "sceneCode": "debt_promise_repay_reminder",
        })
    with pytest.raises(ValidationError):
        CreateWebSessionRequest.model_validate({
            "sceneCode": "debt_promise_repay_reminder",
            "prompt": "临时调试提示词",
        })
    with pytest.raises(ValidationError):
        CreateWebSessionRequest.model_validate({
            "voice": "Tina",
        })
    with pytest.raises(ValidationError):
        PromptProfileCreateRequest.model_validate({
            "profileCode": "legacy_code",
            "sceneCode": "debt_promise_repay_reminder",
            "name": "承诺还款提醒",
            "providerKey": "static_profile",
            "promptText": "业务话术",
            "openingMessage": "您好",
        })
    with pytest.raises(ValidationError):
        PromptProfileCreateRequest.model_validate({
            "sceneCode": "debt_promise_repay_reminder",
            "name": "承诺还款提醒",
            "providerKey": "static_profile",
            "promptText": "业务话术",
            "openingMessage": "您好",
            "remark": "旧备注字段",
        })
    with pytest.raises(ValidationError):
        PromptProfilePreviewRequest.model_validate({
            "businessType": "debt_collection",
            "sceneCode": "debt_promise_repay_reminder",
            "businessParams": {},
        })
    with pytest.raises(ValidationError):
        VoiceProfileCreateRequest.model_validate({
            "voice": "custom_voice_001",
            "displayName": "张总自定义音色",
            "voiceType": "内置",
        })


def test_prompt_config_api_routes_return_expected_response_shapes() -> None:
    class FakePromptConfigService:
        async def list_prompt_profiles(self, **kwargs):
            _ = kwargs
            return {
                "rows": [
                    {
                        "id": "1",
                        "sceneCode": "debt_promise_repay_reminder",
                        "name": "承诺还款提醒",
                        "providerKey": "static_profile",
                        "promptText": "业务话术",
                        "openingMessage": "您好",
                        "createdAt": "2026-06-17T00:00:00Z",
                        "updatedAt": "2026-06-17T00:00:00Z",
                    }
                ],
                "total": 1,
            }

        async def list_prompt_components(self):
            return {
                "rows": [
                    {
                        "componentKey": "platform_constraints",
                        "name": "平台关键约束",
                        "content": "平台关键约束",
                    }
                ],
                "total": 1,
            }

        async def list_voice_profiles(self, **kwargs):
            _ = kwargs
            return {
                "rows": [
                    {
                        "id": "1",
                        "voice": "Tina",
                        "displayName": "甜甜 Tina",
                        "voiceType": "内置",
                        "gender": "女声",
                        "targetModel": "qwen3.5-omni-plus-realtime",
                        "description": "默认音色",
                        "sortOrder": 1,
                        "remark": "",
                        "createdAt": "2026-06-18T00:00:00Z",
                        "updatedAt": "2026-06-18T00:00:00Z",
                    }
                ],
                "total": 1,
            }

        async def create_voice_profile(self, values):
            _ = values
            return {
                "id": "2",
                "voice": "custom_voice_001",
                "displayName": "张总自定义音色",
                "voiceType": "自定义复刻",
                "gender": "未知",
                "targetModel": "qwen3.5-omni-plus-realtime",
                "description": None,
                "sortOrder": 1000,
                "remark": "百炼复刻音色",
                "createdAt": "2026-06-18T00:00:00Z",
                "updatedAt": "2026-06-18T00:00:00Z",
            }

        async def preview_prompt_profile(self, **kwargs):
            _ = kwargs
            return {
                "instructions": "最终提示词",
                "openingMessage": "您好",
                "promptHash": "sha256:prompt",
                "openingMessageHash": "sha256:opening",
                "promptSourceKey": "debt_promise_repay_reminder",
            }

    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: FakePromptConfigService()

    with TestClient(app) as client:
        profiles = client.get("/ai-call/prompt-profiles").json()
        components = client.get("/ai-call/prompt-components").json()
        voices = client.get("/ai-call/voice-profiles").json()
        custom_voice = client.post(
            "/ai-call/voice-profiles",
            json={
                "voice": "custom_voice_001",
                "displayName": "张总自定义音色",
                "remark": "百炼复刻音色",
            },
        ).json()
        preview = client.post(
            "/ai-call/prompt-profiles/preview",
            json={
                "sceneCode": "debt_promise_repay_reminder",
                "businessParams": {},
            },
        ).json()

    assert profiles["code"] == 200
    assert profiles["rows"][0]["sceneCode"] == "debt_promise_repay_reminder"
    assert components["rows"][0]["componentKey"] == "platform_constraints"
    assert voices["rows"][0]["voice"] == "Tina"
    assert voices["rows"][0]["gender"] == "女声"
    assert custom_voice["data"]["voice"] == "custom_voice_001"
    assert custom_voice["data"]["voiceType"] == "自定义复刻"
    assert preview["data"]["promptSourceKey"] == "debt_promise_repay_reminder"
