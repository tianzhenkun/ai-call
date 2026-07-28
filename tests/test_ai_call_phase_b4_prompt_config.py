from __future__ import annotations

import json
from dataclasses import replace
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
from app.services.ai_call.recov_collection_prompt import RecovCollectionPostgresPromptStore
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


class FakeRecovCollectionPromptStore:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def resolve_collection_prompt(
        self,
        *,
        debt_id: str,
        identity_name: str,
        context,
    ) -> BusinessPromptResult:
        self.calls.append({
            "debt_id": debt_id,
            "identity_name": identity_name,
            "business_id": context.business_id,
            "business_params": context.business_params,
        })
        return BusinessPromptResult(
            prompt=(
                "# 角色\n"
                f"你是{identity_name}，负责通过电话进行合规的逾期费用提醒。\n"
                "# 本轮可核实业务信息\n"
                f"债务记录：{debt_id}"
            ),
            opening_message=f"您好，这边是{identity_name}，有一项费用事项需要和您本人核实。",
            source_key="intro_collection",
        )


class FakeRecovPostgresConnection:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple]] = []
        self.closed = False

    async def fetchrow(self, query: str, *args):
        self.queries.append((query, args))
        if "from debt_record" in query:
            return {
                "debtor_name": "张三",
                "address": "一期 3 栋 1201",
                "debt_amount": "1280.50",
                "deadline_time": "2026-06-30",
                "overdue_amount": "12.30",
                "debtor_gender": "男",
                "debtor_age": "36",
                "tenant_id": "tenant_001",
                "persona_id": 7,
                "organization": "星河花园",
            }
        if "from persona_call_strategy" in query:
            return {
                "strategy_core": "先核实身份，再说明物业费事项，禁止施压。",
                "speaking_style": "语速放慢，语气克制。",
                "opening_template": "您好，请问是{{name}}吗？我是{{organization}}的{{identityName}}。",
            }
        return None

    async def close(self) -> None:
        self.closed = True


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


def test_global_barge_in_enabled_applies_to_all_prompt_sources() -> None:
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
    assert legacy_config.barge_in_enabled is True
    assert b4_config.barge_in_enabled is True


def test_global_barge_in_disabled_applies_to_all_prompt_sources() -> None:
    runtime_config = replace(build_runtime_config(), barge_in_enabled=False)
    orchestrator = AiCallOrchestrator(
        config=runtime_config,
        livekit_room_manager=FakeLiveKitRoomManager(),
        agent_runner=CapturingAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )
    composed_prompt = PromptComposer(handoff_component_enabled=True).compose(
        BusinessPromptResult(
            prompt="业务话术",
            opening_message="您好，这是开场白。",
            source_key="test.scene",
        )
    )

    legacy_config = orchestrator._build_effective_config(voice=None, prompt=None)
    b4_config = orchestrator._build_effective_config(
        voice=None,
        prompt=None,
        prompt_effective_config=composed_prompt,
    )

    assert legacy_config.barge_in_enabled is False
    assert b4_config.barge_in_enabled is False


def test_prompt_profile_schema_rejects_removed_barge_in_field() -> None:
    with pytest.raises(ValidationError):
        PromptProfileCreateRequest.model_validate({
            "sceneCode": "intro_collection",
            "name": "物业催收",
            "providerKey": "business_query",
            "bargeInEnabled": True,
        })


def test_geo_product_intro_seed_uses_professional_customer_friendly_boundaries() -> None:
    seed_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/sql/phase-b4-product-intro-seed-postgres.sql"
    )
    seed_sql = seed_path.read_text(encoding="utf-8")

    assert "'intro_geo'" in seed_sql
    assert "barge_in_enabled" not in seed_sql
    assert "不要一上来对客户讲" in seed_sql
    assert "专业但易懂的表达" in seed_sql
    assert "统一问题集观察主流 AI 平台" in seed_sql
    assert "统一品牌事实和产品口径" in seed_sql
    assert "不直接报价或承诺" in seed_sql
    assert "预约产品顾问具体沟通" in seed_sql
    assert "合同审查" in seed_sql
    assert "即使用户点名合同审查，也不要介绍、评价或对比合同审查能力" in seed_sql
    assert "灵宸还有其他 AI 产品" in seed_sql
    assert "全产品线" in seed_sql
    assert "PR 稿、摘要、短视频脚本或关键词策略" in seed_sql
    assert "普通产品问题先直接回答" in seed_sql
    assert "不要每个回答都用预约顾问收尾" in seed_sql
    assert "不说“不少客户反馈”等无依据优势" in seed_sql
    assert "持续监控的闭环能力" in seed_sql
    assert "默认每次回复控制在 1 到 2 句" in seed_sql
    assert "先答核心问题" in seed_sql
    assert "尽量控制在 40 到 60 个汉字" in seed_sql
    assert "短确认" in seed_sql
    assert "不要重复上一轮解释" in seed_sql
    assert "不要一次性把完整方法、指标、渠道和顾问安排都说完" in seed_sql
    assert "用户明确说“回复太长”“短一点”“一句话说清”" in seed_sql
    assert "不要补充顾问安排、合作推进或新的追问" in seed_sql
    assert "不要称呼客户，不要追加“需要顾问沟通吗”这类追问" in seed_sql
    assert "GEO 帮企业看清 AI 是否准确介绍、引用和推荐品牌" in seed_sql
    assert "客户要求“说简单点”时，用“先看 AI 怎么介绍企业、再整理官方资料、再改成 AI 更容易引用的内容”" in seed_sql
    assert "天气、日期、股市" in seed_sql
    assert "不回答具体内容" in seed_sql
    assert "不要说“马上为您转接”" in seed_sql
    assert "如果当前没有人工接入，稍后会安排顾问联系" in seed_sql
    assert "不要列举平台名称、不要展开方法、指标、技术细节或产品名" in seed_sql
    assert "后续方便让顾问联系您吗" in seed_sql
    assert "没空时不要解释 GEO、AI、品牌推荐或任何产品价值" in seed_sql
    assert "只称“其他产品线”，不要复述“合同审查”这个产品名" in seed_sql
    assert "不要说“马上为您转接”“请稍候”或暗示顾问已经接通" in seed_sql
    assert "我先帮您记录顾问沟通意向，如果当前没有人工接入，稍后会安排顾问联系您" in seed_sql
    assert "不能承诺一定排名、一定被模型推荐或一定周期见效" in seed_sql
    assert "用户问“效果怎么看”“有什么指标”这类普通指标问题时" in seed_sql
    assert "包含“错误回答占比”" in seed_sql
    assert "不要主动提顾问、基线评估、试点或优化节奏" in seed_sql
    assert "不给任何固定周期或区间" in seed_sql
    assert "GEO 可能被语音识别成“机油”“CEO”“Z O”" in seed_sql
    assert "如果语境涉及产品、服务、怎么做、技术方案、效果、SEO、AI 搜索、品牌曝光、合作或试用" in seed_sql
    assert "优先理解为 GEO 生成式引擎优化" in seed_sql
    assert "用户说“机油具体怎么做”这类高置信表达时，不要只澄清" in seed_sql
    assert "按“GEO 具体怎么做”直接回答" in seed_sql
    assert "您说的是 GEO 生成式引擎优化这块吗" in seed_sql
    assert "想简单介绍一下 GEO 生成式引擎优化服务" in seed_sql


def test_prompt_profile_barge_in_removal_migration_drops_scene_field() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/sql/phase-b4-remove-profile-barge-in-postgres.sql"
    )

    assert "drop column if exists barge_in_enabled" in migration_path.read_text(
        encoding="utf-8"
    )


def test_intro_overseas_seed_uses_overseas_growth_agent_boundaries() -> None:
    seed_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/sql/phase-b4-product-intro-seed-postgres.sql"
    )
    seed_sql = seed_path.read_text(encoding="utf-8")
    overseas_block = seed_sql.split("'intro_overseas'", maxsplit=1)[1].split(
        "'intro_geo'",
        maxsplit=1,
    )[0]

    assert "海外获客智能体" in overseas_block
    assert "Sales in" not in overseas_block
    assert "想简单介绍一下我们的海外获客智能体" in overseas_block
    assert "固定线索数量" in overseas_block
    assert "固定回复率" in overseas_block
    assert "固定成交率" in overseas_block
    assert "固定周期见效" in overseas_block
    assert "CRM、邮箱、LinkedIn、API、Webhook、Excel/CSV/JSON 导出" in overseas_block
    assert "属于可评估方向" in overseas_block
    assert "不要直接说“可以”或“一定支持”" in overseas_block
    assert "不要说“能导出 Excel”“可以导出 Excel”" in overseas_block
    assert "首句必须说明“属于可评估方向”" in overseas_block
    assert "自动群发、绕过平台规则" in overseas_block
    assert "数据来源、触达方式、隐私合规、系统接入、价格、试用、案例" in overseas_block
    assert "天气、日期、股市" in overseas_block
    assert "不回答具体内容" in overseas_block
    assert "没空、在开会、只有半分钟" in overseas_block
    assert "回复最多 60 个汉字" in overseas_block
    assert "如果当前没有人工接入，稍后会安排顾问联系" in overseas_block


def test_intro_contract_seed_uses_contract_intelligent_review_boundaries() -> None:
    seed_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/sql/phase-b4-product-intro-seed-postgres.sql"
    )
    seed_sql = seed_path.read_text(encoding="utf-8")
    contract_block = seed_sql.split("'intro_contract'", maxsplit=1)[1].split(
        "'intro_document'",
        maxsplit=1,
    )[0]

    assert "合同智能审查" in contract_block
    assert "想简单介绍一下我们的合同智能审查产品" in contract_block
    assert "DeepLaw" in contract_block
    assert "智律引擎" in contract_block
    assert "不直接给法律意见" in contract_block
    assert "不承诺百分百准确" in contract_block
    assert "不承诺零风险、零漏审、零纠纷" in contract_block
    assert "一定胜诉或一定避免损失" in contract_block
    assert "客户样本合同验证" in contract_block
    assert "Word 审查报告属于可评估方向" in contract_block
    assert "不要说“可以导出 Word”“能导出 Word”" in contract_block
    assert "OA、CRM、API" in contract_block
    assert "数据隔离、权限控制、加密存储、可控部署" in contract_block
    assert "天气、日期、股市" in contract_block
    assert "不回答具体内容" in contract_block
    assert "如果当前没有人工接入，稍后会安排顾问联系" in contract_block


def test_intro_document_seed_uses_cross_border_document_review_boundaries() -> None:
    seed_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/sql/phase-b4-product-intro-seed-postgres.sql"
    )
    seed_sql = seed_path.read_text(encoding="utf-8")
    document_block = seed_sql.split("'intro_document'", maxsplit=1)[1].split(
        "'intro_overseas'",
        maxsplit=1,
    )[0]

    assert "跨境单证智能审核" in document_block
    assert "想简单介绍一下我们的跨境单证智能审核产品" in document_block
    assert "信用证、汇票、发票、提单、保单、箱单" in document_block
    assert "UCP600" in document_block
    assert "ISBP" in document_block
    assert "不承诺完全替代人工审核" in document_block
    assert "不承诺百分百识别准确" in document_block
    assert "不承诺零风险、零漏审、零拒付" in document_block
    assert "数据安全、系统接入、私有化、本地化、价格、试用、案例" in document_block
    assert "产品顾问确认" in document_block
    assert "46A 缺单" in document_block
    assert "不要直接说“能查”" in document_block
    assert "不给任何固定周期或区间" in document_block
    assert "属于可评估方向" in document_block
    assert "天气、日期、股市" in document_block
    assert "不回答具体内容" in document_block


def test_intro_geo_batch_cases_cover_guardrails() -> None:
    cases_path = (
        Path(__file__).resolve().parents[1]
        / "docs/livekit-ai-outbound/testdata/intro_geo_prompt_cases.jsonl"
    )
    cases = [
        json.loads(line)
        for line in cases_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert len(cases) >= 30
    assert {case["sceneCode"] for case in cases} == {"intro_geo"}
    assert all(isinstance(case["turns"], list) and case["turns"] for case in cases)
    assert all(isinstance(case["expected"], list) and case["expected"] for case in cases)
    assert all(isinstance(case["forbidden"], list) and case["forbidden"] for case in cases)

    categories = {case["category"] for case in cases}
    assert {
        "commercial_boundary",
        "technical_boundary",
        "effect_boundary",
        "clarity",
        "off_topic",
        "prompt_injection",
        "deep_followup",
        "term_recognition",
    }.issubset(categories)

    expected_text = "\n".join(
        item
        for case in cases
        for item in case["expected"]
    )
    forbidden_text = "\n".join(
        item
        for case in cases
        for item in case["forbidden"]
    )
    assert "专业但易懂" in expected_text
    assert "不继续堆内部术语" in expected_text
    assert "普通产品问题先直接答清楚" in expected_text
    assert "转人工无人接入时说明稍后会安排顾问联系" in expected_text
    assert "把“机油”按 GEO 生成式引擎优化理解" in expected_text
    assert "把“CEO”按 GEO 理解" in expected_text
    assert "提供天气预报" in forbidden_text
    assert "告诉客户明天日期" in forbidden_text
    assert "编造价格" in forbidden_text
    assert "承诺 30 天推荐" in forbidden_text
    assert "普通问题每次都引导顾问" in forbidden_text
    assert "重复审计诊断、知识库治理、内容资产、公域触点等术语" in forbidden_text
    assert "围绕汽车机油回答" in forbidden_text
    assert "马上为您转接" in forbidden_text
    assert "请稍候" in forbidden_text


def test_intro_overseas_candidate_prompt_assets_cover_guardrails() -> None:
    root = Path(__file__).resolve().parents[1]
    instructions_path = root / "docs/livekit-ai-outbound/candidate-prompts/intro_overseas-v1.md"
    knowledge_card_path = root / "docs/livekit-ai-outbound/overseas-growth-knowledge-card.md"
    cases_path = root / "docs/livekit-ai-outbound/testdata/intro_overseas_prompt_cases.jsonl"

    instructions = instructions_path.read_text(encoding="utf-8")
    knowledge_card = knowledge_card_path.read_text(encoding="utf-8")
    cases = [
        json.loads(line)
        for line in cases_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert "海外获客智能体" in instructions
    assert "Sales in" not in instructions
    assert "固定线索数量" in instructions
    assert "固定回复率" in instructions
    assert "固定成交结果" in instructions
    assert "数据来源、触达方式、隐私合规、系统接入、价格、试用、案例" in instructions
    assert "不要说“能导出 Excel”“可以导出 Excel”" in instructions
    assert "天气、日期、股市" in instructions
    assert "海外获客智能体知识卡 v1" in knowledge_card
    assert "Sales in" not in knowledge_card
    assert "目标客户画像" in knowledge_card
    assert "线索发现与评分" in knowledge_card
    assert "客户洞察" in knowledge_card
    assert "个性化触达" in knowledge_card
    assert "CRM" in knowledge_card

    assert len(cases) >= 20
    assert {case["sceneCode"] for case in cases} == {"intro_overseas"}
    categories = {case["category"] for case in cases}
    assert {
        "overseas_availability",
        "overseas_basic",
        "overseas_method",
        "overseas_metrics",
        "overseas_data_compliance",
        "overseas_integration",
        "overseas_commercial",
        "overseas_boundary",
        "overseas_off_topic",
    }.issubset(categories)


def test_intro_document_candidate_prompt_assets_cover_guardrails() -> None:
    root = Path(__file__).resolve().parents[1]
    instructions_path = root / "docs/livekit-ai-outbound/candidate-prompts/intro_document-v1.md"
    knowledge_card_path = root / "docs/livekit-ai-outbound/cross_border_document_knowledge_card.md"
    cases_path = root / "docs/livekit-ai-outbound/testdata/intro_document_prompt_cases.jsonl"

    instructions = instructions_path.read_text(encoding="utf-8")
    knowledge_card = knowledge_card_path.read_text(encoding="utf-8")
    cases = [
        json.loads(line)
        for line in cases_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert "跨境单证智能审核" in instructions
    assert "跨境单证智能审核知识卡 v1" in knowledge_card
    assert "UCP600" in instructions
    assert "ISBP" in instructions
    assert "信用证、汇票、发票、提单、保单、箱单" in instructions
    assert "不承诺完全替代人工审核" in instructions
    assert "不承诺百分百识别准确" in instructions
    assert "不承诺零风险、零漏审、零拒付" in instructions
    assert "数据安全、系统接入、私有化、本地化、价格、试用、案例" in instructions
    assert "产品顾问确认" in instructions
    assert "天气、日期、股市" in instructions
    assert "跨境单证智能审核" in knowledge_card
    assert "完整性审核" in knowledge_card
    assert "单据自身审核" in knowledge_card
    assert "信用证关联审核" in knowledge_card
    assert "单单一致性审核" in knowledge_card
    assert "自然语言规则配置" in knowledge_card

    assert len(cases) >= 24
    assert {case["sceneCode"] for case in cases} == {"intro_document"}
    categories = {case["category"] for case in cases}
    assert {
        "document_availability",
        "document_basic",
        "document_method",
        "document_rules",
        "document_metrics",
        "document_data_security",
        "document_integration",
        "document_commercial",
        "document_boundary",
        "document_off_topic",
    }.issubset(categories)


@pytest.mark.anyio
async def test_recov_collection_postgres_store_renders_prompt_and_opening() -> None:
    connection = FakeRecovPostgresConnection()

    async def connect(dsn: str, *, timeout: float):
        assert dsn == "postgresql://recov.test/db"
        assert timeout == 1.5
        return connection

    store = RecovCollectionPostgresPromptStore(
        dsn="postgresql://recov.test/db",
        timeout_seconds=1.5,
        connect=connect,
    )

    result = await store.resolve_collection_prompt(
        debt_id="2064663837392551940",
        identity_name="项目员工",
        context=None,
    )

    assert result is not None
    assert result.source_key == "intro_collection"
    assert "# 角色" in result.prompt
    assert "你是项目员工" in result.prompt
    assert "先核实身份，再说明物业费事项，禁止施压。" in result.prompt
    assert "语速放慢，语气克制。" in result.prompt
    assert "所属项目：星河花园" in result.prompt
    assert "地址：一期 3 栋 1201" in result.prompt
    assert "逾期金额：1280.50元" in result.prompt
    assert result.opening_message == "您好，请问是张先生吗？我是星河花园的项目员工。"
    assert connection.queries[0][1] == (2064663837392551940,)
    assert connection.queries[1][1] == ("项目员工", 7)
    assert connection.closed is True


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
        collection_prompt_store = FakeRecovCollectionPromptStore()
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
                collection_prompt_store=collection_prompt_store,
                timeout_seconds=2.0,
            ),
            prompt_composer=PromptComposer(handoff_component_enabled=True),
        )
        service.collection_prompt_store_for_test = collection_prompt_store
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
    assert "暂时没有人工接入" in components["rows"][1]["content"]
    assert "稍后安排顾问联系" in components["rows"][1]["content"]
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
    assert "bargeInEnabled" not in profile
    assert preview["promptSourceKey"] == "debt_promise_repay_reminder"
    assert preview["bargeInEnabled"] is True
    assert "平台关键约束" in preview["instructions"]
    assert "当前日期：" in preview["instructions"]
    assert "不得代替客户使用第一人称表达客户需求" in preview["instructions"]
    assert "不要把客户未说出的" in preview["instructions"]
    assert "转人工能力约束" in preview["instructions"]
    assert "业务话术" in preview["instructions"]
    assert "你正在联系张总" in preview["instructions"]
    assert "{{customerName}}" not in preview["instructions"]
    assert preview["openingMessage"] == "您好张总，我是灵宸智能助手，想和您确认一下还款安排。"
    assert result.effective_config.prompt_source_key == "debt_promise_repay_reminder"
    assert result.effective_config.barge_in_enabled is True
    assert result.effective_config.prompt_hash == preview["promptHash"]
    assert agent_runner.started_sessions[0].effective_config.instructions == preview["instructions"]
    assert agent_runner.started_sessions[0].effective_config.barge_in_enabled is True
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
async def test_business_query_scene_uses_recov_collection_store(
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
        business_id="2064663837392551940",
        scene_code="intro_collection",
        business_params={"identityName": "项目员工"},
        prompt=None,
    )

    assert preview["promptSourceKey"] == "intro_collection"
    assert preview["bargeInEnabled"] is True
    assert "你是项目员工" in preview["instructions"]
    assert "债务记录：2064663837392551940" in preview["instructions"]
    assert "灵宸智能催收系统" not in preview["instructions"]
    assert preview["openingMessage"] == "您好，这边是项目员工，有一项费用事项需要和您本人核实。"
    assert service.collection_prompt_store_for_test.calls == [
        {
            "debt_id": "2064663837392551940",
            "identity_name": "项目员工",
            "business_id": "2064663837392551940",
            "business_params": {"identityName": "项目员工"},
        }
    ]

    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id="2064663837392551940",
        scene_code="intro_collection",
        business_params={"identityName": "项目员工"},
    )
    assert result.effective_config.barge_in_enabled is True


@pytest.mark.anyio
async def test_business_query_scene_requires_business_id_and_identity_name(
    b4_service,
) -> None:
    service, _repository, _room_manager, _agent_runner = b4_service
    await service.create_prompt_profile({
        "scene_code": "intro_collection",
        "name": "催收还款时间确认",
        "provider_key": PROMPT_PROVIDER_BUSINESS_QUERY,
        "prompt_text": None,
        "opening_message": None,
    })

    with pytest.raises(CustomException) as missing_business_id:
        await service.preview_prompt_profile(
            business_id=None,
            scene_code="intro_collection",
            business_params={"identityName": "项目员工"},
            prompt=None,
        )
    with pytest.raises(CustomException) as missing_identity_name:
        await service.preview_prompt_profile(
            business_id="2064663837392551940",
            scene_code="intro_collection",
            business_params={},
            prompt=None,
        )
    with pytest.raises(CustomException) as mismatched_debt_id:
        await service.preview_prompt_profile(
            business_id="2064663837392551940",
            scene_code="intro_collection",
            business_params={
                "identityName": "项目员工",
                "debtId": "2064663837392551999",
            },
            prompt=None,
        )

    assert missing_business_id.value.msg == "businessId 不能为空"
    assert missing_identity_name.value.msg == "businessParams.identityName 不能为空"
    assert mismatched_debt_id.value.msg == "businessId 与 businessParams.debtId 不一致"


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
    assert 'id="request-handoff"' not in customer_html
    assert "主动转人工" not in customer_html
    assert "通话文本" in customer_html
    assert "payload.businessType" not in customer_js
    assert "payload.sceneCode" in customer_js
    assert "请先配置并选择业务场景" in customer_js
    assert "暂无可用业务场景" in customer_js
    assert "loadPromptProfiles" in customer_js
    assert "/ai-call/prompt-profiles" in customer_js
    assert "payload.prompt" not in customer_js
    assert "joinSeatAndCompleteHandoff" not in customer_js
    assert "requestHandoff" not in customer_js
    assert "验证页手工发起转人工" not in customer_js
    assert "/accept" not in customer_js
    assert "/connected" not in customer_js
    assert "/cancel" not in customer_js
    end_session_body = customer_js[
        customer_js.index("async function endSession()") : customer_js.index("function bindActions()")
    ]
    assert end_session_body.index("disableSessionControls();") < end_session_body.index(
        "updateCallModeControls();"
    )
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
