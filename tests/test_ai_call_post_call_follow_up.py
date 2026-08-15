from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.agent_console_schema import FollowUpAttemptIn
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.services.ai_call import post_call_follow_up_service, semantic_analysis
from app.services.ai_call.follow_up_data_service import AiCallFollowUpDataService
from app.services.ai_call.follow_up_service import AiCallFollowUpService


def test_post_call_models_keep_portable_scalar_contract() -> None:
    semantic_columns = {
        column.key: column for column in sa_inspect(AiCallSemanticAnalysisModel).columns
    }
    assert semantic_columns["customer_intent"].type.length == 16
    assert semantic_columns["follow_up_suggested"].nullable is False
    assert semantic_columns["follow_up_consent"].type.length == 16
    assert semantic_columns["follow_up_reason"].type.length == 500
    assert semantic_columns["follow_up_confidence"].type.length == 16
    assert "follow_up_preferred_at" in semantic_columns
    assert semantic_columns["follow_up_review_status"].type.length == 16
    assert semantic_columns["follow_up_reviewed_by"].type.length == 64
    assert semantic_columns["follow_up_reviewed_by_name"].type.length == 64
    assert "follow_up_reviewed_at" in semantic_columns

    task_columns = {
        column.key: column for column in sa_inspect(AiCallFollowUpTaskModel).columns
    }
    assert task_columns["source_key"].nullable is False
    assert task_columns["source_handoff_id"].nullable is True
    assert not sa_inspect(AiCallFollowUpTaskModel).relationships

    source_constraints = {
        tuple(column.name for column in constraint.columns)
        for constraint in AiCallFollowUpTaskModel.__table__.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert ("tenant_id", "source_type", "source_key") in source_constraints


def test_normalize_post_call_result_maps_intent_and_follow_up() -> None:
    result = semantic_analysis.normalize_analysis_result({
        "summary": "客户要求顾问回访",
        "feedback_type": "正向",
        "key_points": ["客户要求回访"],
        "time_hint": {
            "time_text": "明天下午",
            "time_value": "2026-07-31T14:00:00+08:00",
            "original_texts": ["明天下午联系"],
        },
        "tags": ["明确回访"],
        "follow_up": {
            "required": True,
            "consent": "explicit",
            "reason": "客户明确要求回访",
            "preferred_time": "2026-07-31T14:00:00+08:00",
            "confidence": "high",
        },
    })

    assert result["feedback_type"] == "正向"
    assert result["follow_up"] == {
        "required": True,
        "consent": "explicit",
        "reason": "客户明确要求回访",
        "preferred_time": "2026-07-31T14:00:00+08:00",
        "confidence": "high",
    }
    materialized = semantic_analysis.post_call_materialized_fields(result)
    assert materialized == {
        "customer_intent": "positive",
        "follow_up_suggested": True,
        "follow_up_consent": "explicit",
        "follow_up_reason": "客户明确要求回访",
        "follow_up_preferred_at": datetime(2026, 7, 31, 6, 0, tzinfo=timezone.utc),
        "follow_up_confidence": "high",
    }
    assert materialized["follow_up_preferred_at"].isoformat() == (
        "2026-07-31T06:00:00+00:00"
    )


def test_normalize_post_call_result_accepts_only_ai_classifications() -> None:
    result = semantic_analysis.normalize_analysis_result({
        "summary": "客户明确要求下周安排产品演示。",
        "classification": "interested",
        "confidence": "high",
        "valid_dialogue": True,
        "reason": "客户提出了明确的演示安排。",
        "evidence": ["客户：下周可以安排演示"],
        "evidence_conflict": False,
    })

    assert result["classification"] == "interested"
    assert result["confidence"] == "high"
    assert result["valid_dialogue"] is True
    assert result["reason"] == "客户提出了明确的演示安排。"
    assert result["evidence"] == ["客户：下周可以安排演示"]
    assert result["low_value_reason"] is None

    converted = semantic_analysis.normalize_analysis_result({
        "classification": "converted",
        "confidence": "high",
        "valid_dialogue": True,
        "reason": "模型声称已经转化",
    })
    assert converted["classification"] is None
    assert converted["valid_dialogue"] is False


def test_follow_up_explicit_consent_is_downgraded_without_customer_evidence() -> None:
    result = semantic_analysis.enforce_semantic_evidence_on_result(
        {
            "summary": "AI 说可以安排顾问联系",
            "feedback_type": "正向",
            "key_points": [],
            "time_hint": {},
            "tags": [],
            "follow_up": {
                "required": True,
                "consent": "explicit",
                "reason": "安排顾问联系",
                "preferred_time": None,
                "confidence": "high",
            },
        },
        {
            "call_id": "call-assistant-only",
            "turns": [
                {
                    "role": "assistant",
                    "speaker_type": "ai",
                    "text": "我可以安排顾问联系您。",
                }
            ],
        },
    )

    assert result["follow_up"]["consent"] == "missing"
    assert result["follow_up"]["confidence"] == "low"


def test_follow_up_explicit_consent_ignores_unrelated_customer_commitment() -> None:
    result = semantic_analysis.enforce_semantic_evidence_on_result(
        {
            "summary": "客户关注价格，AI 表示会安排顾问回访",
            "feedback_type": "正向",
            "key_points": ["客户关注价格"],
            "time_hint": {},
            "tags": [],
            "follow_up": {
                "required": True,
                "consent": "explicit",
                "reason": "安排顾问回访",
                "preferred_time": None,
                "confidence": "high",
            },
        },
        {
            "call_id": "call-unrelated-commitment",
            "turns": [
                {
                    "role": "user",
                    "speaker_type": "customer",
                    "text": "我主要关注价格，可以接受按年付费。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "supports_strong_fact": True,
                        "supported_strong_fact_types": [
                            "commitment",
                            "requirement_conclusion",
                        ],
                    },
                },
                {
                    "role": "assistant",
                    "speaker_type": "ai",
                    "text": "好的，我安排顾问稍后联系您。",
                },
            ],
        },
    )

    assert result["follow_up"]["consent"] == "missing"
    assert result["follow_up"]["confidence"] == "low"


def test_follow_up_explicit_consent_keeps_direct_customer_contact_evidence() -> None:
    result = semantic_analysis.enforce_semantic_evidence_on_result(
        {
            "summary": "客户同意顾问明天下午联系",
            "feedback_type": "正向",
            "key_points": ["客户同意回访"],
            "time_hint": {},
            "tags": [],
            "follow_up": {
                "required": True,
                "consent": "explicit",
                "reason": "客户同意回访",
                "preferred_time": None,
                "confidence": "high",
            },
        },
        {
            "call_id": "call-direct-follow-up-consent",
            "turns": [
                {
                    "role": "user",
                    "speaker_type": "customer",
                    "text": "可以，让顾问明天下午联系我。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "supports_strong_fact": True,
                        "supported_strong_fact_types": ["follow_up_consent"],
                    },
                }
            ],
        },
    )

    assert result["follow_up"]["consent"] == "explicit"
    assert result["follow_up"]["confidence"] == "high"


def test_direct_customer_call_instruction_corrects_conservative_model_result() -> None:
    result = semantic_analysis.enforce_semantic_evidence_on_result(
        {
            "summary": "客户要求明天上午电话联系",
            "feedback_type": "正向",
            "key_points": ["客户要求电话联系"],
            "time_hint": {},
            "tags": [],
            "follow_up": {
                "required": True,
                "consent": "missing",
                "reason": "客户要求明天上午电话联系",
                "preferred_time": "2026-07-31T09:00:00+08:00",
                "confidence": "medium",
            },
        },
        {
            "call_id": "call-direct-future-call-instruction",
            "turns": [
                {
                    "role": "user",
                    "speaker_type": "customer",
                    "text": "你到时候打一个电话就行。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "supports_strong_fact": True,
                        "supported_strong_fact_types": ["follow_up_consent"],
                    },
                }
            ],
        },
    )

    assert result["follow_up"]["consent"] == "explicit"
    assert result["follow_up"]["confidence"] == "high"
    assert result["follow_up"]["reason"] == "客户明确同意后续跟进"


def test_trial_link_consent_corrects_model_no_follow_up_result() -> None:
    result = semantic_analysis.enforce_semantic_evidence_on_result(
        {
            "summary": "客户希望获得免费试用链接",
            "feedback_type": "正向",
            "key_points": ["客户询问免费试用"],
            "time_hint": {},
            "tags": [],
            "follow_up": {
                "required": False,
                "consent": "missing",
                "reason": None,
                "preferred_time": None,
                "confidence": "low",
            },
        },
        {
            "call_id": "call-trial-link",
            "turns": [
                {
                    "role": "user",
                    "speaker_type": "customer",
                    "text": "方便。",
                    "semantic_evidence": {
                        "analysis_usage": "use_as_customer_signal",
                        "supports_strong_fact": True,
                        "supported_strong_fact_types": ["follow_up_consent"],
                    },
                }
            ],
        },
    )

    assert result["follow_up"] == {
        "required": True,
        "consent": "explicit",
        "reason": "客户明确同意后续跟进",
        "preferred_time": None,
        "confidence": "high",
    }


@pytest.mark.parametrize(
    ("required", "consent", "confidence", "expected_action"),
    [
        (True, "explicit", "high", "create"),
        (True, "missing", "high", "suggest"),
        (True, "refused", "high", "none"),
        (True, "explicit", "medium", "suggest"),
        (False, "missing", "low", "none"),
    ],
)
def test_post_call_follow_up_decision_matrix(
    required: bool,
    consent: str,
    confidence: str,
    expected_action: str,
) -> None:
    analysis = SimpleNamespace(
        follow_up_suggested=required and consent != "refused",
        follow_up_consent=consent,
        follow_up_confidence=confidence,
        follow_up_reason="客户要求回访" if required else None,
        follow_up_preferred_at=None,
    )

    decision = post_call_follow_up_service.decide_post_call_follow_up(analysis)

    assert decision.action == expected_action


def test_ai_suggested_no_follow_up_requires_manual_review_unless_refused() -> None:
    suggested = SimpleNamespace(
        analysis_status="2",
        follow_up_review_status=None,
        follow_up_suggested=True,
        follow_up_consent="missing",
        follow_up_confidence="low",
        follow_up_reason="客户询问试用",
        follow_up_preferred_at=None,
    )
    automatic = SimpleNamespace(
        **{
            **suggested.__dict__,
            "follow_up_consent": "explicit",
            "follow_up_confidence": "high",
        }
    )
    no_follow_up = SimpleNamespace(
        **{
            **suggested.__dict__,
            "follow_up_suggested": False,
            "follow_up_consent": "missing",
            "follow_up_reason": None,
        }
    )
    refused = SimpleNamespace(
        **{
            **no_follow_up.__dict__,
            "follow_up_consent": "refused",
        }
    )

    assert post_call_follow_up_service.requires_manual_follow_up_review(suggested)
    assert post_call_follow_up_service.requires_manual_follow_up_review(no_follow_up)
    assert not post_call_follow_up_service.requires_manual_follow_up_review(
        automatic
    )
    assert not post_call_follow_up_service.requires_manual_follow_up_review(refused)


@pytest.fixture
async def session_factory(tmp_path):
    database_path = tmp_path / "post-call-follow-up.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_post_call_analysis(
    session_factory,
    *,
    call_id: str,
    entry_type: str,
    with_formal_attempt: bool,
    status: str = "2",
    dialer_type: str = "sip",
    with_phone: bool = True,
) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=100,
                call_id=call_id,
                business_type="outbound_task",
                business_id="200",
                scene_code="intro_geo",
                entry_type=entry_type,
                room_name=f"room-{call_id}",
                participant_identity=f"participant-{call_id}",
                callee_phone_number_hash="hash-1" if with_phone else None,
                callee_phone_number_masked="199****1001" if with_phone else None,
                status="completed",
                started_at=now,
                ended_at=now,
            )
        )
        if with_formal_attempt:
            db.add(
                AiCallOutboundTaskModel(
                    id=300,
                    tenant_id="tenant-a",
                    validation_id=1,
                    idempotency_key=f"task-{call_id}",
                    request_fingerprint=f"fingerprint-{call_id}",
                    task_name="正式外呼任务",
                    task_mode="batch",
                    status="COMPLETED",
                    total_targets=1,
                    completed_targets=1,
                    connected_targets=1,
                    failed_targets=0,
                    execution_mode="immediate",
                    scheduled_at=None,
                    started_at=now,
                    ended_at=now,
                    prompt_profile_id=None,
                    prompt_name="GEO 产品介绍",
                    scene_code="intro_geo",
                    voice="Cherry",
                    voice_name="芊悦",
                    rule_id=1,
                    rule_name="工作日规则",
                    rule_summary="09:00-18:00",
                    config_snapshot_json="{}",
                    error_message=None,
                    created_by=1,
                    created_by_name="管理员",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                AiCallOutboundTargetModel(
                    id=400,
                    tenant_id="tenant-a",
                    task_id=300,
                    validation_id=1,
                    source_validation_row_id=1,
                    source_row_number=2,
                    phone_number="19900001001" if with_phone else None,
                    customer_name="本地白名单终端",
                    status="COMPLETED",
                    attempt_count=1,
                    latest_result="connected",
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                AiCallOutboundAttemptModel(
                    id=200,
                    tenant_id="tenant-a",
                    task_id=300,
                    target_id=400,
                    attempt_no=1,
                    call_id=call_id,
                    dialer_type=dialer_type,
                    test_scenario=None,
                    command_idempotency_key=f"command-{call_id}",
                    active_slot=None,
                    status="COMPLETED",
                    call_result="connected",
                    error_message=None,
                    line_id=500,
                    line_code="line-local",
                    provider_status_code=None,
                    provider_reason=None,
                    hangup_cause="NORMAL_CLEARING",
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
        db.add(
            AiCallSemanticAnalysisModel(
                id=300,
                call_id=call_id,
                scene_code="intro_geo",
                analysis_scene_code="ai_call_semantic_analysis",
                analysis_status=status,
                analysis_result="{}",
                customer_intent="positive",
                follow_up_suggested=True,
                follow_up_consent="explicit",
                follow_up_reason="客户明确要求顾问回访",
                follow_up_preferred_at=None,
                follow_up_confidence="high",
                analysis_retry_count=0,
                created_at=now,
                updated_at=now,
            )
        )


async def _seed_scoped_agent(session_factory) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallAgentProfileModel(
                id=20,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                user_id=20,
                enabled=True,
                created_by=1,
                created_at=now,
                updated_by=1,
                updated_at=now,
            )
        )
        db.add(
            AiCallAgentSceneScopeModel(
                id=2000,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                scene_code="intro_geo",
                created_by=1,
                created_at=now,
            )
        )
        db.add(
            AiCallHandoffAgentModel(
                id=20,
                tenant_id="tenant-a",
                agent_identity="agent-20",
                skill_group="default",
                status="available",
                active_handoff_id=None,
                active_call_id=None,
                console_session_id="console-session-20",
                last_seen_at=now,
                status_updated_at=now,
            )
        )


def _agent_auth(db) -> AuthSchema:
    return AuthSchema(
        db=db,
        check_data_scope=False,
        user=UserModel(
            user_id=20,
            tenant_id="tenant-a",
            user_name="agent-20",
            nick_name="坐席20",
            user_type="sys_user",
        ),
    )


@pytest.mark.anyio
async def test_post_call_follow_up_is_idempotent_for_same_formal_call(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-formal-1",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id="call-formal-1")
        assert analysis is not None
        service = post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        )

        first = await service.apply(analysis)
        second = await service.apply(analysis)

        assert first is not None
        assert second is not None
        assert first.id == second.id
        assert first.source_type == "ai_post_call"
        assert first.source_key == "call:call-formal-1"
        count = await db.scalar(
            select(func.count())
            .select_from(AiCallFollowUpTaskModel)
            .where(AiCallFollowUpTaskModel.source_call_id == "call-formal-1")
        )
        assert count == 1


@pytest.mark.anyio
async def test_manual_review_creates_follow_up_for_suggested_formal_call(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-manual-review-create",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-manual-review-create"
        )
        assert analysis is not None
        analysis.follow_up_consent = "missing"
        analysis.follow_up_confidence = "medium"

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).review(
            analysis,
            action="create",
            reviewed_by="20",
            reviewed_by_name="坐席20",
        )

        assert created is not None
        assert created.status == "pending"
        assert analysis.follow_up_review_status == "created"
        assert analysis.follow_up_reviewed_by == "20"
        assert analysis.follow_up_reviewed_by_name == "坐席20"
        assert analysis.follow_up_reviewed_at is not None


@pytest.mark.anyio
async def test_manual_review_creates_follow_up_when_ai_suggests_none(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-manual-review-ai-none",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-manual-review-ai-none"
        )
        assert analysis is not None
        analysis.follow_up_suggested = False
        analysis.follow_up_consent = "missing"
        analysis.follow_up_confidence = "low"
        analysis.follow_up_reason = None

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).review(
            analysis,
            action="create",
            reviewed_by="20",
            reviewed_by_name="坐席20",
        )

        assert created is not None
        assert created.follow_up_reason == "人工确认需要跟进"
        assert analysis.follow_up_review_status == "created"


@pytest.mark.anyio
async def test_manual_review_dismissal_prevents_later_automatic_creation(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-manual-review-dismiss",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-manual-review-dismiss"
        )
        assert analysis is not None
        analysis.follow_up_consent = "missing"
        analysis.follow_up_confidence = "low"
        service = post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        )

        dismissed = await service.review(
            analysis,
            action="dismiss",
            reviewed_by="20",
            reviewed_by_name="坐席20",
        )

        assert dismissed is None
        assert analysis.follow_up_review_status == "dismissed"
        assert analysis.follow_up_reviewed_at is not None

        with pytest.raises(ValueError, match="已确认无需跟进"):
            await service.review(
                analysis,
                action="create",
                reviewed_by="20",
                reviewed_by_name="坐席20",
            )

        analysis.follow_up_consent = "explicit"
        analysis.follow_up_confidence = "high"
        assert await service.apply(analysis) is None
        count = await db.scalar(
            select(func.count())
            .select_from(AiCallFollowUpTaskModel)
            .where(
                AiCallFollowUpTaskModel.source_call_id
                == "call-manual-review-dismiss"
            )
        )
        assert count == 0


@pytest.mark.anyio
async def test_manual_review_rechecks_persisted_status_before_accepting_action(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-manual-review-stale",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as stale_db:
        stored_analysis = await AiCallRecordRepository(
            stale_db
        ).get_semantic_analysis(call_id="call-manual-review-stale")
        assert stored_analysis is not None
        stale_analysis = SimpleNamespace(
            call_id=stored_analysis.call_id,
            analysis_status="2",
            follow_up_review_status=None,
            follow_up_suggested=True,
            follow_up_consent="missing",
            follow_up_confidence="low",
            follow_up_reason="客户询问试用",
            follow_up_preferred_at=None,
            analysis_result_dict={},
            scene_code=stored_analysis.scene_code,
        )
        await stale_db.rollback()

        async with session_factory() as current_db, current_db.begin():
            current_repository = AiCallRecordRepository(current_db)
            current_analysis = await current_repository.get_semantic_analysis(
                call_id="call-manual-review-stale"
            )
            assert current_analysis is not None
            current_analysis.follow_up_consent = "missing"
            current_analysis.follow_up_confidence = "low"
            await post_call_follow_up_service.AiCallPostCallFollowUpService(
                current_repository
            ).review(
                current_analysis,
                action="dismiss",
                reviewed_by="20",
                reviewed_by_name="坐席20",
            )

        async with stale_db.begin():
            with pytest.raises(ValueError, match="已确认无需跟进"):
                await post_call_follow_up_service.AiCallPostCallFollowUpService(
                    AiCallRecordRepository(stale_db)
                ).review(
                    stale_analysis,
                    action="create",
                    reviewed_by="21",
                    reviewed_by_name="坐席21",
                )


@pytest.mark.anyio
async def test_formal_web_call_creates_post_call_follow_up_without_phone(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-formal-web",
        entry_type="web",
        with_formal_attempt=True,
        dialer_type="owner_runtime",
        with_phone=False,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id="call-formal-web")
        assert analysis is not None

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).apply(analysis)

        assert created is not None
        assert created.masked_contact == "Web 浏览器"


@pytest.mark.anyio
@pytest.mark.parametrize("handoff_status", ["requested", "expired", "completed"])
async def test_post_call_follow_up_skips_calls_with_any_handoff(
    session_factory,
    handoff_status: str,
) -> None:
    call_id = f"call-with-handoff-{handoff_status}"
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=500,
                tenant_id="tenant-a",
                handoff_id=f"handoff-{handoff_status}",
                call_id=call_id,
                room_name=f"room-{call_id}",
                scene_code="intro_geo",
                status=handoff_status,
                request_source="ai",
                requested_at=now,
            )
        )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        assert analysis is not None

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).apply(analysis)

        assert created is None
        count = await db.scalar(
            select(func.count())
            .select_from(AiCallFollowUpTaskModel)
            .where(AiCallFollowUpTaskModel.source_call_id == call_id)
        )
        assert count == 0


@pytest.mark.anyio
async def test_reanalysis_refreshes_existing_pending_follow_up(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-formal-pending-refresh",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-formal-pending-refresh"
        )
        assert analysis is not None
        service = post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        )
        created = await service.apply(analysis)
        assert created is not None

        callback_at = datetime(2026, 7, 31, 1, 0, tzinfo=timezone.utc)
        analysis.follow_up_reason = "客户明确同意明日电话联系"
        analysis.follow_up_preferred_at = callback_at
        analysis.analysis_result = json.dumps(
            {"summary": "重分析后的通话摘要"},
            ensure_ascii=False,
        )

        refreshed = await service.apply(analysis)

        assert refreshed.id == created.id
        assert refreshed.status == "pending"
        assert refreshed.follow_up_reason == "客户明确同意明日电话联系"
        assert refreshed.customer_callback_at == callback_at
        assert refreshed.summary == "重分析后的通话摘要"


@pytest.mark.anyio
async def test_formal_post_call_follow_up_runs_through_list_claim_and_complete(
    session_factory,
) -> None:
    await _seed_scoped_agent(session_factory)
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-formal-lifecycle",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db:
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-formal-lifecycle"
        )
        assert analysis is not None
        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).apply(analysis)
        assert created is not None
        await db.commit()

        follow_up_service = AiCallFollowUpService(db)
        auth = _agent_auth(db)
        listed = await follow_up_service.list_follow_ups(auth)
        assert [task.id for task in listed] == [created.id]

        claimed = await follow_up_service.claim_follow_up(
            auth,
            follow_up_id=created.id,
        )
        assert claimed.owner_agent_identity == "agent-20"
        assert claimed.status == "processing"

        await follow_up_service.append_attempt(
            auth,
            follow_up_id=created.id,
            payload=FollowUpAttemptIn(
                contact_channel="wechat",
                attempt_result="connected",
                remark="客户已确认后续安排",
            ),
        )
        completed = await follow_up_service.complete_follow_up(
            auth,
            follow_up_id=created.id,
        )
        await db.commit()
        assert completed.status == "completed"
        assert completed.completed_at is not None

        records, total = await repository.list_records(
            tenant_id="tenant-a",
            call_id="call-formal-lifecycle",
        )
        assert total == 1
        assert records[0]._follow_up_context == {
            "followUpId": str(created.id),
            "followUpStatus": "completed",
        }


@pytest.mark.anyio
async def test_reanalysis_never_reopens_or_replaces_terminal_task(
    session_factory,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id="call-formal-terminal",
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(
            call_id="call-formal-terminal"
        )
        assert analysis is not None
        service = post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        )
        created = await service.apply(analysis)
        assert created is not None
        created.status = "completed"
        created.completed_at = datetime.now(timezone.utc)
        original_id = created.id
        original_reason = created.follow_up_reason
        await db.flush()

        analysis.follow_up_reason = "重分析后的新建议"
        repeated = await service.apply(analysis)

        assert repeated is not None
        assert repeated.id == original_id
        assert repeated.status == "completed"
        assert repeated.follow_up_reason == original_reason


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("call_id", "entry_type", "with_formal_attempt"),
    [
        ("call-mock-1", "outbound_mock", True),
        ("call-web-1", "web", False),
    ],
)
async def test_post_call_follow_up_ignores_non_formal_records(
    session_factory,
    call_id: str,
    entry_type: str,
    with_formal_attempt: bool,
) -> None:
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type=entry_type,
        with_formal_attempt=with_formal_attempt,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        assert analysis is not None

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).apply(analysis)

        assert created is None
        count = await db.scalar(
            select(func.count()).select_from(AiCallFollowUpTaskModel)
        )
        assert count == 0


@pytest.mark.anyio
async def test_ai_classification_updates_follow_up_data_without_creating_task(
    session_factory,
) -> None:
    call_id = "call-follow-up-data"
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        assert analysis is not None
        analysis.analysis_version = 1
        analysis.analysis_result = json.dumps(
            semantic_analysis.normalize_analysis_result({
                "summary": "客户明确要求下周安排产品演示。",
                "classification": "interested",
                "confidence": "high",
                "valid_dialogue": True,
                "reason": "客户提出了明确的演示安排。",
                "evidence": ["客户：下周可以安排演示"],
                "evidence_conflict": False,
            }),
            ensure_ascii=False,
        )
        service = AiCallFollowUpDataService(repository)

        created = await service.apply_ai_analysis(analysis)
        duplicate = await service.apply_ai_analysis(analysis)

        assert created is not None
        assert duplicate is not None
        assert duplicate.id == created.id
        assert created.classification == "interested"
        assert created.classification_source == "ai"
        assert created.classification_confidence == "high"
        assert created.suggest_review is False
        record = await repository.get_record(call_id)
        assert record is not None
        assert record.follow_up_data_id == created.id
        assert await db.scalar(
            select(func.count()).select_from(AiCallFollowUpTaskModel)
        ) == 0
        assert await db.scalar(
            select(func.count()).select_from(
                AiCallFollowUpClassificationHistoryModel
            )
        ) == 1

        created.classification_source = "human"
        created.classification_reason = "坐席已核实客户有明确演示需求。"
        analysis.analysis_version = 2
        analysis.analysis_result = json.dumps(
            semantic_analysis.normalize_analysis_result({
                "summary": "客户本次表示暂时不考虑。",
                "classification": "low_value",
                "confidence": "low",
                "valid_dialogue": True,
                "reason": "客户表示当前暂无需求。",
                "evidence": ["客户：现在暂时不考虑"],
                "evidence_conflict": False,
                "low_value_reason": "no_current_need",
            }),
            ensure_ascii=False,
        )

        protected = await service.apply_ai_analysis(analysis)

        assert protected is not None
        assert protected.classification == "interested"
        assert protected.classification_source == "human"
        assert protected.suggest_review is False
        assert protected.latest_conclusion == "客户本次表示暂时不考虑。"
        histories = list(
            (
                await db.scalars(
                    select(AiCallFollowUpClassificationHistoryModel).order_by(
                        AiCallFollowUpClassificationHistoryModel.semantic_analysis_version
                    )
                )
            ).all()
        )
        assert len(histories) == 2
        assert histories[-1].ai_suggested_classification == "low_value"
        assert histories[-1].ai_adopted is False


@pytest.mark.anyio
async def test_invalid_dialogue_does_not_create_follow_up_data(session_factory) -> None:
    call_id = "call-invalid-follow-up-data"
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        assert analysis is not None
        analysis.analysis_version = 1
        analysis.analysis_result = json.dumps(
            semantic_analysis.normalize_analysis_result({
                "summary": "仅有无业务含义的简短回应。",
                "classification": "nurturing",
                "confidence": "low",
                "valid_dialogue": False,
                "reason": "没有形成有效业务对话。",
                "evidence": [],
                "evidence_conflict": False,
            }),
            ensure_ascii=False,
        )

        assert await AiCallFollowUpDataService(repository).apply_ai_analysis(
            analysis
        ) is None
        assert await db.scalar(
            select(func.count()).select_from(AiCallFollowUpDataModel)
        ) == 0


@pytest.mark.anyio
async def test_semantic_analysis_creates_classification_not_callback_task(
    session_factory,
) -> None:
    call_id = "call-semantic-follow-up-data"
    now = datetime.now(timezone.utc)
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type="sip_outbound",
        with_formal_attempt=True,
        status="0",
    )

    class FakeAnalyzer:
        async def analyze(self, **_kwargs):
            return {
                "summary": "客户希望了解产品演示。",
                "classification": "interested",
                "confidence": "high",
                "valid_dialogue": True,
                "reason": "客户主动询问产品演示。",
                "evidence": ["客户：可以演示一下吗"],
                "evidence_conflict": False,
            }

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        await repository.upsert_dialogue_segment(
            call_id=call_id,
            segment_no=1,
            speaker_type="customer",
            speaker_identity="customer-1",
            source="qwen_realtime",
            source_segment_id="customer-1",
            segment_text="可以演示一下吗？",
            segment_status="final",
            started_at=now,
            ended_at=now,
            duration_ms=1000,
        )

        analysis = await semantic_analysis.AiCallSemanticAnalysisService(
            repository,
            analyzer=FakeAnalyzer(),
        ).analyze_call_once(call_id=call_id, scene_code="intro_geo", now=now)

        assert analysis.analysis_version == 1
        data = await db.scalar(select(AiCallFollowUpDataModel))
        assert data is not None
        assert data.classification == "interested"
        assert await db.scalar(
            select(func.count()).select_from(AiCallFollowUpTaskModel)
        ) == 0
