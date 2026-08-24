from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import UniqueConstraint, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.agent_console_controller import list_follow_ups_controller
from app.api.v1.ai_call.agent_console_schema import (
    AfterCallWorkIn,
    AgentPresenceSessionIn,
    FollowUpAttemptIn,
    FollowUpCallIn,
    FollowUpCloseIn,
    FollowUpDataCallIn,
    FollowUpHandlingResultIn,
)
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpCallRequestModel,
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpScheduleRequestModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.follow_up_service import AiCallFollowUpService
from app.services.ai_call.livekit_sip import (
    HumanCallbackSessionResult,
    HumanOnlySipSessionFactory,
)


def _auth(db, *, user_id: int, tenant_id: str = "tenant-a") -> AuthSchema:
    return AuthSchema(
        db=db,
        check_data_scope=False,
        user=UserModel(
            user_id=user_id,
            tenant_id=tenant_id,
            user_name=f"agent-{user_id}",
            nick_name=f"坐席{user_id}",
            user_type="sys_user",
        ),
    )


@pytest.fixture
async def session_factory(tmp_path):
    database_path = tmp_path / "agent-console-follow-up.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}",
        connect_args={"timeout": 5},
    )
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_agent(
    session_factory,
    *,
    user_id: int,
    agent_identity: str,
    scene_code: str = "intro_contract",
    status: str = "available",
    active_handoff_id: str | None = None,
    active_call_id: str | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    console_session_id = str(uuid4())
    async with session_factory() as db, db.begin():
        db.add(
            AiCallAgentProfileModel(
                id=user_id,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                user_id=user_id,
                enabled=True,
                created_by=1,
                created_at=now,
                updated_by=1,
                updated_at=now,
            )
        )
        db.add(
            AiCallAgentSceneScopeModel(
                id=user_id * 100,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                scene_code=scene_code,
                created_by=1,
                created_at=now,
            )
        )
        db.add(
            AiCallHandoffAgentModel(
                id=user_id,
                tenant_id="tenant-a",
                agent_identity=agent_identity,
                skill_group="default",
                status=status,
                active_handoff_id=active_handoff_id,
                active_call_id=active_call_id,
                console_session_id=console_session_id,
                last_seen_at=now,
                status_updated_at=now,
            )
        )
    return console_session_id


async def _seed_completed_handoff(session_factory) -> str:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="wrap_up_quick",
        active_handoff_id="handoff-1",
        active_call_id="call-1",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=1,
                call_id="call-1",
                business_type="lead",
                business_id="lead-1",
                scene_code="intro_contract",
                entry_type="sip",
                room_name="room-1",
                participant_identity="customer-1",
                callee_phone_number_hash="hash-1",
                callee_phone_number_masked="138****0000",
                status="completed",
                started_at=now,
                ended_at=now,
            )
        )
        db.add(
            AiCallHandoffModel(
                id=1,
                tenant_id="tenant-a",
                handoff_id="handoff-1",
                call_id="call-1",
                room_name="room-1",
                scene_code="intro_contract",
                status="completed",
                request_source="customer",
                request_reason="customer_requested_human",
                request_message="请转人工",
                human_agent_identity="agent-20",
                requested_at=now,
                accepted_at=now,
                connected_at=now,
                ended_at=now,
            )
        )
    return console_session_id


async def _seed_unanswered_follow_up(
    session_factory,
    *,
    task_id: int = 100,
    scene_code: str = "intro_contract",
    source_type: str = "handoff_unanswered",
    owner_agent_identity: str | None = None,
    created_at: datetime | None = None,
) -> None:
    now = created_at or datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallRecordModel(
                id=task_id + 10_000,
                tenant_id="tenant-a",
                call_id=f"call-unanswered-{task_id}",
                business_type="lead",
                business_id="lead-2",
                scene_code="intro_contract",
                entry_type="sip",
                room_name=f"room-unanswered-{task_id}",
                participant_identity=f"customer-unanswered-{task_id}",
                callee_phone_number="13800000000",
                callee_phone_number_hash="source-phone-hash",
                callee_phone_number_masked="138****0000",
                status="completed",
                started_at=now,
                ended_at=now,
            )
        )
        db.add(
            AiCallFollowUpTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                source_type=source_type,
                source_key=f"{source_type}:follow-up-{task_id}",
                source_call_id=f"call-unanswered-{task_id}",
                source_handoff_id=f"handoff-unanswered-{task_id}",
                scene_code=scene_code,
                business_type="lead",
                business_id="lead-2",
                contact_ref=f"call:call-unanswered-{task_id}",
                masked_contact="139****0000",
                owner_agent_identity=owner_agent_identity,
                status="pending",
                follow_up_reason="首次人工接通等待超时",
                customer_callback_at=None,
                summary=None,
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
        )


async def _seed_outbound_context(session_factory, *, call_id: str = "call-1") -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        record = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        assert record is not None
        record.tenant_id = "tenant-a"
        db.add_all(
            [
                AiCallOutboundTaskModel(
                    id=300,
                    tenant_id="tenant-a",
                    validation_id=1,
                    idempotency_key="acw-outbound-task",
                    request_fingerprint="acw-outbound-fingerprint",
                    task_name="AI 外呼任务",
                    task_mode="single",
                    status="COMPLETED",
                    total_targets=1,
                    completed_targets=1,
                    connected_targets=1,
                    failed_targets=0,
                    execution_mode="immediate",
                    prompt_name="产品介绍",
                    scene_code="intro_contract",
                    voice="Tina",
                    rule_id=1,
                    rule_name="工作日规则",
                    rule_summary="全天",
                    config_snapshot_json="{}",
                    created_by=1,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundTargetModel(
                    id=301,
                    tenant_id="tenant-a",
                    task_id=300,
                    validation_id=1,
                    source_validation_row_id=1,
                    source_row_number=1,
                    phone_number="13800000000",
                    customer_name="测试客户",
                    status="COMPLETED",
                    attempt_count=1,
                    latest_result="connected",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=302,
                    tenant_id="tenant-a",
                    task_id=300,
                    target_id=301,
                    attempt_no=1,
                    call_id=call_id,
                    dialer_type="sip",
                    status="COMPLETED",
                    call_result="connected",
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
            ]
        )


async def _link_follow_up_data(
    session_factory,
    *,
    follow_up_id: int,
    classification: str = "interested",
) -> int:
    now = datetime.now(timezone.utc)
    data_id = follow_up_id + 20_000
    async with session_factory() as db, db.begin():
        task = await db.get(AiCallFollowUpTaskModel, follow_up_id)
        assert task is not None
        record = await db.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.call_id == task.source_call_id
            )
        )
        assert record is not None
        task.follow_up_data_id = data_id
        record.follow_up_data_id = data_id
        db.add(
            AiCallFollowUpDataModel(
                id=data_id,
                tenant_id="tenant-a",
                task_id=follow_up_id + 30_000,
                target_id=follow_up_id + 40_000,
                source_call_id=task.source_call_id,
                classification=classification,
                classification_reason="AI 初始分类",
                classification_source="ai",
                classification_confidence="high",
                suggest_review=False,
                low_value_reason=None,
                latest_conclusion="客户希望继续了解",
                last_contact_at=now,
                blocking_human_call_id=None,
                version=1,
                classification_updated_at=now,
                classification_updated_by="ai",
                created_at=now,
                updated_at=now,
            )
        )
    return data_id


async def _seed_follow_up_data_without_task(session_factory) -> tuple[str, int]:
    console_session_id = await _seed_completed_handoff(session_factory)
    await _seed_outbound_context(session_factory)
    async with session_factory() as db:
        source = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == "call-1")
        )
        assert source is not None
        source.callee_phone_number = "13800000000"
        service = AiCallFollowUpService(db)
        work, task = await service.submit_after_call_work(
            _auth(db, user_id=20),
            call_id="call-1",
            payload=AfterCallWorkIn(
                classification="interested",
                conclusion="客户希望继续了解产品。",
                schedule_follow_up=False,
                expected_version=0,
            ),
            idempotency_key="seed-follow-up-data-no-task",
        )
        await db.commit()
        assert task is None
        assert work.follow_up_data_id is not None
        return console_session_id, work.follow_up_data_id


async def _seed_ai_post_call_follow_up(session_factory, *, task_id: int = 102) -> None:
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallFollowUpTaskModel(
                id=task_id,
                tenant_id="tenant-a",
                source_type="ai_post_call",
                source_key=f"call:call-ai-post-{task_id}",
                source_call_id=f"call-ai-post-{task_id}",
                source_handoff_id=None,
                scene_code="intro_contract",
                business_type="lead",
                business_id="lead-ai",
                contact_ref=f"call:call-ai-post-{task_id}",
                masked_contact="137****0000",
                owner_agent_identity=None,
                status="pending",
                follow_up_reason="客户明确要求顾问回访",
                customer_callback_at=None,
                summary="客户希望顾问后续联系。",
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
        )


def _error_code(exc: CustomException) -> str | None:
    return exc.data.get("errorCode") if isinstance(exc.data, dict) else None


def test_classification_contract_rejects_mixed_or_invalid_after_call_fields() -> None:
    with pytest.raises(ValidationError, match="新旧话后结果字段不能混用"):
        AfterCallWorkIn(
            disposition_code="resolved",
            classification="interested",
            conclusion="客户有意向",
            schedule_follow_up=False,
            expected_version=0,
        )
    with pytest.raises(ValidationError):
        AfterCallWorkIn(
            classification="converted",
            conclusion="客户已签约",
            schedule_follow_up=False,
            expected_version=0,
        )


def test_follow_up_data_models_keep_classification_separate_from_tasks() -> None:
    data_table = AiCallFollowUpDataModel.__table__
    history_table = AiCallFollowUpClassificationHistoryModel.__table__
    schedule_table = AiCallFollowUpScheduleRequestModel.__table__
    call_request_table = AiCallFollowUpCallRequestModel.__table__
    record_table = AiCallRecordModel.__table__
    task_table = AiCallFollowUpTaskModel.__table__
    handling_table = AiCallFollowUpHandlingResultModel.__table__

    assert {
        "tenant_id",
        "task_id",
        "target_id",
        "source_call_id",
        "classification",
        "classification_source",
        "suggest_review",
        "blocking_human_call_id",
        "version",
    } <= set(data_table.columns.keys())
    assert {
        frozenset(constraint.columns.keys())
        for constraint in data_table.constraints
        if isinstance(constraint, UniqueConstraint)
    } >= {frozenset({"tenant_id", "task_id", "target_id"})}
    assert not data_table.foreign_keys

    assert {
        "follow_up_data_id",
        "follow_up_id",
        "call_id",
        "idempotency_key",
        "assignment_action",
        "previous_owner_agent_identity",
        "new_owner_agent_identity",
    } <= set(call_request_table.columns.keys())
    assert not call_request_table.foreign_keys

    assert {
        "follow_up_data_id",
        "from_classification",
        "to_classification",
        "semantic_analysis_id",
        "semantic_analysis_version",
        "ai_suggested_classification",
        "ai_adopted",
        "idempotency_key",
        "request_fingerprint",
        "result_version",
    } <= set(history_table.columns.keys())
    assert {
        frozenset(constraint.columns.keys())
        for constraint in history_table.constraints
        if isinstance(constraint, UniqueConstraint)
    } >= {
        frozenset(
            {"tenant_id", "semantic_analysis_id", "semantic_analysis_version"}
        ),
        frozenset({"tenant_id", "idempotency_key"}),
    }
    assert not history_table.foreign_keys

    assert {
        "follow_up_data_id",
        "follow_up_id",
        "idempotency_key",
        "request_fingerprint",
        "result_version",
    } <= set(schedule_table.columns.keys())
    assert {
        frozenset(constraint.columns.keys())
        for constraint in schedule_table.constraints
        if isinstance(constraint, UniqueConstraint)
    } >= {frozenset({"tenant_id", "idempotency_key"})}
    assert not schedule_table.foreign_keys

    assert {"follow_up_data_id", "operator_agent_identity"} <= set(
        record_table.columns.keys()
    )
    assert "follow_up_data_id" in task_table.columns
    active_task_index = next(
        index
        for index in task_table.indexes
        if index.name == "uk_ai_call_follow_up_data_active_task"
    )
    assert active_task_index.unique is True
    assert {"follow_up_data_id", "request_fingerprint"} <= set(
        handling_table.columns.keys()
    )
    assert handling_table.c.follow_up_id.nullable is True


def test_task5_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/agent-console/handoffs/{handoff_id}/complete",
        "/ai-call/agent-console/calls/{call_id}/after-call-work",
        "/ai-call/agent-console/follow-ups",
        "/ai-call/agent-console/follow-ups/{follow_up_id}",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/attempts",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/handling-results",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/claim",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/call",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/call/{call_id}/end",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/call",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/call/{call_id}/end",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/handling-results",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/complete",
        "/ai-call/agent-console/follow-ups/{follow_up_id}/close",
    } <= paths


def test_quick_wrap_up_requires_only_disposition_and_follow_up_flag() -> None:
    payload = AfterCallWorkIn(disposition_code="resolved", needs_follow_up=False)
    assert payload.summary is None

    with pytest.raises(ValidationError):
        AfterCallWorkIn(needs_follow_up=False)
    with pytest.raises(ValidationError):
        AfterCallWorkIn(disposition_code="resolved")


def test_handling_result_requires_one_valid_next_action() -> None:
    with pytest.raises(ValidationError, match="下次跟进时间"):
        FollowUpHandlingResultIn(
            contact_channel="wechat",
            contact_result="no_answer",
            remark="客户暂未回复",
            next_action="continue",
        )
    with pytest.raises(ValidationError, match="只有已接通"):
        FollowUpHandlingResultIn(
            contact_channel="wechat",
            contact_result="no_answer",
            remark="客户暂未回复",
            next_action="complete",
        )
    with pytest.raises(ValidationError, match="终止原因"):
        FollowUpHandlingResultIn(
            contact_channel="wechat",
            contact_result="rejected",
            remark="客户拒绝继续沟通",
            next_action="close",
        )
    with pytest.raises(ValidationError, match="必须关联 callId"):
        FollowUpHandlingResultIn(
            contact_channel="manual_phone",
            contact_result="connected",
            remark="电话已接通",
            next_action="complete",
        )


@pytest.mark.anyio
async def test_submit_handling_result_is_atomic_and_idempotent(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    await _seed_unanswered_follow_up(session_factory)
    next_follow_up_at = datetime.now(timezone.utc) + timedelta(days=1)
    payload = FollowUpHandlingResultIn(
        contact_channel="wechat",
        contact_result="no_answer",
        remark="客户暂未回复，明天再次联系",
        next_action="continue",
        next_follow_up_at=next_follow_up_at,
    )

    async with session_factory() as db, db.begin():
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        first = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=payload,
            idempotency_key="handling-result-1",
        )
        first_id = first[1].id
        assert first[0].status == "processing"
        assert first[0].customer_callback_at == next_follow_up_at

    async with session_factory() as db, db.begin():
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        second = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=payload,
            idempotency_key="handling-result-1",
        )

        assert first_id == second[1].id
        assert await db.scalar(select(func.count(AiCallFollowUpAttemptModel.id))) == 1
        assert (
            await db.scalar(select(func.count(AiCallFollowUpHandlingResultModel.id)))
            == 1
        )
        with pytest.raises(CustomException, match="当前坐席不是"):
            await service.submit_handling_result(
                _auth(db, user_id=21),
                follow_up_id=100,
                payload=payload,
                idempotency_key="handling-result-1",
            )


@pytest.mark.anyio
async def test_handling_result_produces_completed_and_closed_terminal_states(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(
        session_factory, task_id=100, owner_agent_identity="agent-20"
    )
    await _seed_unanswered_follow_up(
        session_factory, task_id=101, owner_agent_identity="agent-20"
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        completed, _ = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=FollowUpHandlingResultIn(
                contact_channel="wechat",
                contact_result="connected",
                remark="客户问题已解决",
                next_action="complete",
            ),
            idempotency_key="handling-complete",
        )
        closed, _ = await service.submit_handling_result(
            auth,
            follow_up_id=101,
            payload=FollowUpHandlingResultIn(
                contact_channel="wechat",
                contact_result="rejected",
                remark="客户明确拒绝继续联系",
                next_action="close",
                closed_reason="customer_refused",
            ),
            idempotency_key="handling-close",
        )

        assert completed.status == "completed"
        assert completed.completed_at is not None
        assert closed.status == "closed"
        assert closed.closed_reason == "customer_refused"
        assert closed.closed_remark == "客户明确拒绝继续联系"


@pytest.mark.anyio
async def test_classification_handling_result_updates_data_and_completes_task(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(
        session_factory, task_id=100, owner_agent_identity="agent-20"
    )
    data_id = await _link_follow_up_data(session_factory, follow_up_id=100)
    payload = FollowUpHandlingResultIn(
        contact_channel="wechat",
        contact_result="connected",
        classification="converted",
        conclusion="客户已确认采购并完成签约。",
        schedule_follow_up=False,
        expected_version=1,
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        task, first = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=payload,
            idempotency_key="handling-classification-1",
        )
        replay_task, replay = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=payload,
            idempotency_key="handling-classification-1",
        )
        with pytest.raises(CustomException, match="幂等键已用于其他请求"):
            await service.submit_handling_result(
                auth,
                follow_up_id=100,
                payload=payload.model_copy(
                    update={"conclusion": "同一幂等键对应了不同结论。"}
                ),
                idempotency_key="handling-classification-1",
            )
        await db.commit()

        data = await db.get(AiCallFollowUpDataModel, data_id)
        history_count = await db.scalar(
            select(func.count()).select_from(
                AiCallFollowUpClassificationHistoryModel
            )
        )
        assert task.status == "completed"
        assert replay_task.status == "completed"
        assert first.id == replay.id
        assert first.follow_up_data_id == data_id
        assert first.classification == "converted"
        assert first.result_version == 2
        assert data.classification == "converted"
        assert data.latest_conclusion == payload.conclusion
        assert data.version == 2
        assert history_count == 1


@pytest.mark.anyio
async def test_unconnected_classification_result_keeps_classification_and_can_finish_task(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(
        session_factory, task_id=100, owner_agent_identity="agent-20"
    )
    data_id = await _link_follow_up_data(session_factory, follow_up_id=100)

    async with session_factory() as db:
        task, result = await AiCallFollowUpService(db).submit_handling_result(
            _auth(db, user_id=20),
            follow_up_id=100,
            payload=FollowUpHandlingResultIn(
                contact_channel="wechat",
                contact_result="no_answer",
                remark="本次未取得联系，暂不安排再次回访。",
                schedule_follow_up=False,
                expected_version=1,
            ),
            idempotency_key="handling-unconnected-1",
        )
        await db.commit()

        data = await db.get(AiCallFollowUpDataModel, data_id)
        history_count = await db.scalar(
            select(func.count()).select_from(
                AiCallFollowUpClassificationHistoryModel
            )
        )
        assert task.status == "completed"
        assert result.next_action == "complete"
        assert result.classification is None
        assert data.classification == "interested"
        assert data.latest_conclusion == result.remark
        assert data.version == 2
        assert history_count == 0


@pytest.mark.anyio
async def test_follow_up_list_filters_customer_and_returns_task_labels(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add_all(
            [
                AiCallOutboundTaskModel(
                    id=201,
                    tenant_id="tenant-a",
                    validation_id=1,
                    idempotency_key="follow-up-label-task",
                    request_fingerprint="follow-up-label-fingerprint",
                    task_name="GEO 产品回访",
                    task_mode="single",
                    status="COMPLETED",
                    total_targets=1,
                    completed_targets=1,
                    connected_targets=0,
                    failed_targets=1,
                    execution_mode="immediate",
                    prompt_name="GEO 产品介绍",
                    scene_code="intro_contract",
                    voice="Tina",
                    rule_id=1,
                    rule_name="工作日规则",
                    rule_summary="全天",
                    config_snapshot_json="{}",
                    created_by=1,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundTargetModel(
                    id=202,
                    tenant_id="tenant-a",
                    task_id=201,
                    validation_id=1,
                    source_validation_row_id=1,
                    source_row_number=1,
                    phone_number="13800000000",
                    customer_name="张三",
                    status="COMPLETED",
                    attempt_count=1,
                    latest_result="no_answer",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=203,
                    tenant_id="tenant-a",
                    task_id=201,
                    target_id=202,
                    attempt_no=1,
                    call_id="call-unanswered-100",
                    status="COMPLETED",
                    call_result="no_answer",
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
            ]
        )

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        rows, total = await service.list_follow_up_page(
            _auth(db, user_id=20),
            page_num=1,
            page_size=10,
            customer_name="张",
        )
        payload = service.follow_up_payload(rows[0])

        assert total == 1
        assert payload["customer_name"] == "张三"
        assert payload["task_name"] == "GEO 产品回访"


@pytest.mark.anyio
async def test_owned_handoff_completion_enters_quick_wrap_up(session_factory) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="in_call",
        active_handoff_id="handoff-connected",
        active_call_id="call-connected",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=2,
                tenant_id="tenant-a",
                handoff_id="handoff-connected",
                call_id="call-connected",
                room_name="room-connected",
                scene_code="intro_contract",
                status="connected",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
            )
        )

    async with session_factory() as db:
        service = AiCallAgentConsoleService(db)
        handoff = await service.complete_handoff(
            _auth(db, user_id=20),
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )
        assert handoff.status == "completed"
        assert handoff.end_reason == "agent_completed"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-connected"


@pytest.mark.anyio
async def test_repeated_owned_handoff_completion_is_idempotent(session_factory) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="in_call",
        active_handoff_id="handoff-connected",
        active_call_id="call-connected",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=21,
                tenant_id="tenant-a",
                handoff_id="handoff-connected",
                call_id="call-connected",
                room_name="room-connected",
                scene_code="intro_contract",
                status="connected",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
            )
        )

    async with session_factory() as db, db.begin():
        service = AiCallAgentConsoleService(db)
        auth = _auth(db, user_id=20)
        first = await service.complete_handoff(
            auth,
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )
        second = await service.complete_handoff(
            auth,
            handoff_id="handoff-connected",
            console_session_id=console_session_id,
        )

        assert first.status == "completed"
        assert second.status == "completed"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-connected"
        assert presence.active_call_id == "call-connected"


@pytest.mark.anyio
async def test_connected_terminal_handoff_completion_recovers_wrap_up_state(
    session_factory,
) -> None:
    now = datetime.now(timezone.utc)
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
        status="online",
        active_handoff_id=None,
        active_call_id="call-ended",
    )
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=22,
                tenant_id="tenant-a",
                handoff_id="handoff-ended",
                call_id="call-ended",
                room_name="room-ended",
                scene_code="intro_contract",
                status="canceled",
                request_source="customer",
                human_agent_identity="agent-20",
                accepted_console_session_id=console_session_id,
                requested_at=now,
                accepted_at=now,
                connected_at=now,
                ended_at=now,
                end_reason="remote_hangup",
            )
        )

    async with session_factory() as db, db.begin():
        service = AiCallAgentConsoleService(db)
        handoff = await service.complete_handoff(
            _auth(db, user_id=20),
            handoff_id="handoff-ended",
            console_session_id=console_session_id,
        )

        assert handoff.status == "canceled"
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == "handoff-ended"
        assert presence.active_call_id == "call-ended"


@pytest.mark.anyio
async def test_submit_acw_creates_one_owned_follow_up_and_releases_agent(session_factory) -> None:
    console_session_id = await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        presence.last_seen_at = datetime.now(timezone.utc) - timedelta(minutes=2)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        acw, follow_up = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="follow_up_required",
                needs_follow_up=True,
            ),
        )
        await db.commit()

        assert acw.summary is None
        assert follow_up is not None
        assert follow_up.owner_agent_identity == "agent-20"
        assert follow_up.follow_up_reason == "人工通话后续跟进"
        assert follow_up.customer_callback_at is None

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_handoff_id is None
        assert presence.active_call_id is None
        assert presence.last_seen_at == presence.status_updated_at

        _, available_presence = await service.agent_service.require_available_presence(
            auth,
            console_session_id=console_session_id,
        )
        assert available_presence.status == "available"


@pytest.mark.anyio
async def test_submit_acw_accepts_connected_handoff_ended_by_remote_hangup(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(
                select(AiCallHandoffModel).where(
                    AiCallHandoffModel.handoff_id == "handoff-1"
                )
            )
        ).scalar_one()
        handoff.status = "canceled"
        handoff.end_reason = "remote_hangup"

    async with session_factory() as db:
        acw, follow_up = await AiCallFollowUpService(db).submit_after_call_work(
            _auth(db, user_id=20),
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="resolved",
                needs_follow_up=False,
            ),
        )
        await db.commit()

        assert acw.handoff_id == "handoff-1"
        assert follow_up is None
        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_handoff_id is None
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_submit_acw_rejects_canceled_handoff_that_never_connected(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    async with session_factory() as db, db.begin():
        handoff = (
            await db.execute(
                select(AiCallHandoffModel).where(
                    AiCallHandoffModel.handoff_id == "handoff-1"
                )
            )
        ).scalar_one()
        handoff.status = "canceled"
        handoff.connected_at = None
        handoff.end_reason = "customer_canceled"

    async with session_factory() as db:
        with pytest.raises(CustomException) as conflict:
            await AiCallFollowUpService(db).submit_after_call_work(
                _auth(db, user_id=20),
                call_id="call-1",
                payload=AfterCallWorkIn(
                    disposition_code="other",
                    needs_follow_up=False,
                ),
            )

    assert _error_code(conflict.value) == "HANDOFF_STATE_CONFLICT"


@pytest.mark.anyio
async def test_repeated_acw_submission_does_not_duplicate_work_or_follow_up(session_factory) -> None:
    await _seed_completed_handoff(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        payload = AfterCallWorkIn(
            disposition_code="follow_up_required",
            needs_follow_up=True,
        )
        first = await service.submit_after_call_work(auth, call_id="call-1", payload=payload)
        second = await service.submit_after_call_work(auth, call_id="call-1", payload=payload)
        await db.commit()

        work_count = await db.scalar(select(func.count()).select_from(AiCallAfterCallWorkModel))
        task_count = await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel))
        assert work_count == 1
        assert task_count == 1
        assert first[0].id == second[0].id
        assert first[1].id == second[1].id


@pytest.mark.anyio
async def test_classification_acw_updates_follow_up_data_without_creating_task(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    await _seed_outbound_context(session_factory)
    payload = AfterCallWorkIn(
        classification="interested",
        conclusion="客户希望先查看方案，本次暂不安排回访。",
        schedule_follow_up=False,
        expected_version=0,
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        first = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=payload,
            idempotency_key="acw-classification-1",
        )
        second = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=payload,
            idempotency_key="acw-classification-1",
        )
        with pytest.raises(CustomException, match="幂等键已用于其他请求"):
            await service.submit_after_call_work(
                auth,
                call_id="call-1",
                payload=payload.model_copy(
                    update={"conclusion": "同一幂等键对应了不同结论。"}
                ),
                idempotency_key="acw-classification-1",
            )
        await db.commit()

        data = await db.scalar(select(AiCallFollowUpDataModel))
        history_count = await db.scalar(
            select(func.count()).select_from(
                AiCallFollowUpClassificationHistoryModel
            )
        )
        task_count = await db.scalar(
            select(func.count()).select_from(AiCallFollowUpTaskModel)
        )
        record = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == "call-1")
        )

        assert first[0].id == second[0].id
        assert first[1] is None
        assert data is not None
        assert data.classification == "interested"
        assert data.latest_conclusion == payload.conclusion
        assert data.version == 1
        assert history_count == 1
        assert task_count == 0
        assert record.follow_up_data_id == data.id
        assert record.operator_agent_identity == "agent-20"


@pytest.mark.anyio
async def test_classification_acw_creates_task_only_when_callback_is_scheduled(
    session_factory,
) -> None:
    await _seed_completed_handoff(session_factory)
    await _seed_outbound_context(session_factory)
    callback_at = datetime.now(timezone.utc) + timedelta(days=1)

    async with session_factory() as db:
        work, follow_up = await AiCallFollowUpService(db).submit_after_call_work(
            _auth(db, user_id=20),
            call_id="call-1",
            payload=AfterCallWorkIn(
                classification="nurturing",
                conclusion="客户希望明天下午再沟通。",
                schedule_follow_up=True,
                next_follow_up_at=callback_at,
                expected_version=0,
            ),
            idempotency_key="acw-schedule-1",
        )
        await db.commit()

        assert follow_up is not None
        assert follow_up.follow_up_data_id == work.follow_up_data_id
        assert follow_up.owner_agent_identity == "agent-20"
        assert follow_up.customer_callback_at == callback_at


@pytest.mark.anyio
async def test_unanswered_follow_up_claim_is_atomic_and_owner_is_fixed(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    await _seed_unanswered_follow_up(session_factory)

    ready = asyncio.Event()
    started = 0
    lock = asyncio.Lock()

    async def claim(user_id: int):
        nonlocal started
        async with session_factory() as db:
            async with lock:
                started += 1
                if started == 2:
                    ready.set()
            await ready.wait()
            try:
                task = await AiCallFollowUpService(db).claim_follow_up(
                    _auth(db, user_id=user_id),
                    follow_up_id=100,
                )
                await db.commit()
                return task.owner_agent_identity
            except CustomException as exc:
                return _error_code(exc)

    results = await asyncio.gather(claim(20), claim(21))
    assert sorted(results) in [
        ["FOLLOW_UP_ALREADY_CLAIMED", "agent-20"],
        ["FOLLOW_UP_ALREADY_CLAIMED", "agent-21"],
    ]

    winner = next(value for value in results if value.startswith("agent-"))
    loser_id = 21 if winner == "agent-20" else 20
    async with session_factory() as db:
        with pytest.raises(CustomException) as conflict:
            await AiCallFollowUpService(db).claim_follow_up(
                _auth(db, user_id=loser_id),
                follow_up_id=100,
            )
        assert _error_code(conflict.value) == "FOLLOW_UP_ALREADY_CLAIMED"


@pytest.mark.anyio
async def test_claim_keeps_request_owned_transaction_valid(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        async with db.begin():
            task = await AiCallFollowUpService(db).claim_follow_up(
                _auth(db, user_id=20),
                follow_up_id=100,
            )
            assert task.owner_agent_identity == "agent-20"

    async with session_factory() as db:
        persisted = await db.get(AiCallFollowUpTaskModel, 100)
        assert persisted is not None
        assert persisted.owner_agent_identity == "agent-20"
        assert persisted.status == "processing"


@pytest.mark.anyio
async def test_ai_post_call_follow_up_can_be_claimed_by_scoped_agent(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_ai_post_call_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)

        rows = await service.list_follow_ups(auth)
        task = await service.claim_follow_up(auth, follow_up_id=102)

        assert [row.id for row in rows] == [102]
        assert task.owner_agent_identity == "agent-20"
        assert task.status == "processing"


@pytest.mark.anyio
async def test_owner_can_append_attempt_complete_or_close_with_rules(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        task = await service.claim_follow_up(auth, follow_up_id=100)
        assert task.owner_agent_identity == "agent-20"

        attempt = await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="manual_phone",
                attempt_result="no_answer",
            ),
        )
        assert attempt.attempt_result == "no_answer"
        assert attempt.customer_callback_at is None
        assert task.status == "pending"

        with pytest.raises(CustomException, match="请先登记已联系结果"):
            await service.complete_follow_up(auth, follow_up_id=100)

        connected_attempt = await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="wechat",
                attempt_result="connected",
                remark="客户已确认问题解决",
            ),
        )
        assert connected_attempt.attempt_result == "connected"

        completed = await service.complete_follow_up(auth, follow_up_id=100)
        assert completed.status == "completed"
        assert completed.owner_agent_identity == "agent-20"

        attempts = list(
            (
                await db.execute(
                    select(AiCallFollowUpAttemptModel).where(
                        AiCallFollowUpAttemptModel.follow_up_id == 100
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(attempts) == 2

    with pytest.raises(ValidationError):
        FollowUpCloseIn(closed_reason="other")
    assert FollowUpCloseIn(closed_reason="customer_refused").closed_remark is None
    with pytest.raises(ValidationError):
        FollowUpCloseIn(closed_reason="created_by_error")
    assert (
        FollowUpCloseIn(
            closed_reason="created_by_error",
            closed_remark="本地验收夹具重复创建",
        ).closed_reason
        == "created_by_error"
    )


@pytest.mark.anyio
async def test_agent_follow_up_payload_exposes_latest_contact_attempt(
    session_factory,
) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="manual_phone",
                attempt_result="no_answer",
            ),
        )
        await service.append_attempt(
            auth,
            follow_up_id=100,
            payload=FollowUpAttemptIn(
                contact_channel="wechat",
                attempt_result="connected",
            ),
        )

        rows = await service.list_follow_ups(auth)
        payload = service.follow_up_payload(rows[0])

        assert payload["latest_attempt"]["contact_channel"] == "wechat"
        assert payload["latest_attempt"]["attempt_result"] == "connected"


@pytest.mark.anyio
async def test_follow_up_list_respects_owner_and_scene_and_close_is_terminal(session_factory) -> None:
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory, task_id=100)
    await _seed_unanswered_follow_up(session_factory, task_id=101)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        rows = await service.list_follow_ups(auth)
        assert {task.id for task in rows} == {100, 101}

        await service.claim_follow_up(auth, follow_up_id=101)
        closed = await service.close_follow_up(
            auth,
            follow_up_id=101,
            payload=FollowUpCloseIn(closed_reason="customer_refused"),
        )
        assert closed.status == "closed"
        assert closed.closed_reason == "customer_refused"
        assert closed.closed_at is not None


@pytest.mark.anyio
async def test_follow_up_list_applies_scene_status_source_and_created_period_filters(
    session_factory,
) -> None:
    now = datetime.now(timezone.utc)
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=100,
        created_at=now,
    )
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=101,
        source_type="ai_post_call",
        created_at=now,
    )
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=102,
        scene_code="intro_geo",
        owner_agent_identity="agent-20",
        created_at=now,
    )
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=103,
        created_at=now - timedelta(days=2),
    )

    async with session_factory() as db:
        rows = await AiCallFollowUpService(db).list_follow_ups(
            _auth(db, user_id=20),
            status=["pending"],
            scene_code="intro_contract",
            source_type="handoff_unanswered",
            created_at_begin=now - timedelta(hours=1),
            created_at_end=now + timedelta(hours=1),
        )

    assert [task.id for task in rows] == [100]


@pytest.mark.anyio
async def test_follow_up_list_controller_forwards_filters() -> None:
    now = datetime(2026, 8, 5, tzinfo=timezone.utc)
    auth = SimpleNamespace()
    service = SimpleNamespace(
        list_follow_up_page=AsyncMock(return_value=([], 0)),
        follow_up_payload=lambda task: task,
    )

    await list_follow_ups_controller(
        auth,
        service,
        page_num=2,
        page_size=10,
        ownership="mine",
        status="pending",
        scene_code="intro_contract",
        source_type="handoff_unanswered",
        customer_name=None,
        created_at_begin=now - timedelta(days=1),
        created_at_end=now,
    )

    service.list_follow_up_page.assert_awaited_once_with(
        auth,
        page_num=2,
        page_size=10,
        ownership="mine",
        status=["pending"],
        scene_code="intro_contract",
        source_type="handoff_unanswered",
        customer_name=None,
        created_at_begin=now - timedelta(days=1),
        created_at_end=now,
    )


@pytest.mark.anyio
async def test_follow_up_page_filters_ownership_before_pagination(session_factory) -> None:
    now = datetime.now(timezone.utc)
    await _seed_agent(session_factory, user_id=20, agent_identity="agent-20")
    await _seed_unanswered_follow_up(session_factory, task_id=100)
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=101,
        owner_agent_identity="agent-20",
        created_at=now - timedelta(hours=1),
    )
    await _seed_unanswered_follow_up(
        session_factory,
        task_id=102,
        owner_agent_identity="agent-20",
        created_at=now,
    )

    async with session_factory() as db:
        rows, total = await AiCallFollowUpService(db).list_follow_up_page(
            _auth(db, user_id=20),
            page_num=1,
            page_size=1,
            ownership="mine",
            status=["pending"],
        )

    assert total == 2
    assert [task.id for task in rows] == [102]


@pytest.mark.anyio
async def test_ai_summary_draft_never_overwrites_human_summary(session_factory) -> None:
    await _seed_completed_handoff(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        auth = _auth(db, user_id=20)
        acw, follow_up = await service.submit_after_call_work(
            auth,
            call_id="call-1",
            payload=AfterCallWorkIn(
                disposition_code="follow_up_required",
                needs_follow_up=True,
                summary="人工确认摘要",
            ),
        )
        await service.apply_ai_summary_draft(
            tenant_id="tenant-a",
            handoff_id="handoff-1",
            summary="后到的 AI 摘要",
        )
        await db.commit()

        await db.refresh(acw)
        await db.refresh(follow_up)
        assert acw.summary == "人工确认摘要"
        assert follow_up.summary == "人工确认摘要"


class _FakeRoomManager:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.deleted: list[str] = []

    async def create_room(self, room_name: str) -> None:
        self.created.append(room_name)

    async def delete_room(self, room_name: str) -> None:
        self.deleted.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str):
        return type(
            "Token",
            (),
            {
                "livekit_url": "wss://livekit.example.com",
                "participant_token": "agent-token",
                "participant_identity": participant_identity,
                "expires_in_seconds": 60,
            },
        )()


class _FakeSipClient:
    def __init__(self) -> None:
        self.created: list[dict] = []

    async def create_participant(self, **kwargs):
        self.created.append(kwargs)
        return type(
            "SipResult",
            (),
            {
                "participant_identity": kwargs["participant_identity"],
                "sip_call_id": "sip-call-1",
                "sip_call_status": "dialing",
            },
        )()


class _FakeCallbackFactory:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.ended_calls: list[str] = []
        self.sip_call_status = "active"

    async def create(self, **kwargs) -> HumanCallbackSessionResult:
        self.calls.append(kwargs)
        call_id = kwargs["call_id"]
        return HumanCallbackSessionResult(
            call_id=call_id,
            room_name=f"ai-call-{call_id}",
            customer_participant_identity=f"sip-{call_id}",
            agent_participant_identity=f"human-callback-{call_id}",
            livekit_url="wss://livekit.example.com",
            participant_token="agent-token",
            expires_in_seconds=60,
        )

    async def end(self, *, call_id: str) -> None:
        self.ended_calls.append(call_id)

    async def get_call_status(self, *, call_id: str) -> str:
        return self.sip_call_status


class _ConcurrentStatusCallbackFactory(_FakeCallbackFactory):
    def __init__(self) -> None:
        super().__init__()
        self.waiting = 0
        self.ready = asyncio.Event()

    async def get_call_status(self, *, call_id: str) -> str:
        self.waiting += 1
        if self.waiting == 2:
            self.ready.set()
        await self.ready.wait()
        return await super().get_call_status(call_id=call_id)


class _FailingCallbackFactory:
    async def create(self, **_kwargs):
        raise AiCallError(
            error_id="sip_create_participant_failed",
            msg="SIP Participant 创建失败",
            status_code=502,
        )


@pytest.mark.parametrize(
    ("disconnect_reason", "expected"),
    [
        ("USER_UNAVAILABLE", "no_answer"),
        ("CONNECTION_TIMEOUT", "no_answer"),
        ("USER_REJECTED", "rejected"),
        ("SIP_TRUNK_FAILURE", "technical_failure"),
        ("MEDIA_FAILURE", "technical_failure"),
    ],
)
def test_callback_result_maps_livekit_disconnect_reason(
    disconnect_reason: str,
    expected: str,
) -> None:
    assert (
        AiCallFollowUpService._callback_attempt_result(
            {"disconnectReason": disconnect_reason}
        )
        == expected
    )


@pytest.mark.anyio
async def test_follow_up_data_direct_call_does_not_create_task_and_submits_result(
    session_factory,
) -> None:
    console_session_id, data_id = await _seed_follow_up_data_without_task(
        session_factory
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        callback, task = await service.start_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpDataCallIn(console_session_id=console_session_id),
            idempotency_key="direct-call-1",
        )

        assert task is None
        record = await db.scalar(
            select(AiCallRecordModel).where(
                AiCallRecordModel.call_id == callback.call_id
            )
        )
        data = await db.get(AiCallFollowUpDataModel, data_id)
        request = await db.scalar(
            select(AiCallFollowUpCallRequestModel).where(
                AiCallFollowUpCallRequestModel.call_id == callback.call_id
            )
        )
        assert record is not None
        assert record.follow_up_id is None
        assert record.follow_up_data_id == data_id
        assert record.operator_agent_identity == "agent-20"
        assert data is not None
        assert data.blocking_human_call_id == callback.call_id
        assert request is not None
        assert request.assignment_action == "direct"

        with pytest.raises(CustomException) as replayed:
            await service.start_follow_up_data_callback(
                auth,
                follow_up_data_id=data_id,
                payload=FollowUpDataCallIn(console_session_id=console_session_id),
                idempotency_key="direct-call-1",
            )
        assert _error_code(replayed.value) == "FOLLOW_UP_CALL_ALREADY_STARTED"

        joined = await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={"participant": {"attributes": {"sip.callStatus": "active"}}},
        )
        assert joined["attemptResult"] == "connected"
        await service.end_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )
        data, next_task, result = (
            await service.submit_follow_up_data_handling_result(
                auth,
                follow_up_data_id=data_id,
                payload=FollowUpHandlingResultIn(
                    call_id=callback.call_id,
                    contact_result="connected",
                    classification="nurturing",
                    conclusion="客户暂未确定时间，先不安排回访。",
                    schedule_follow_up=False,
                    expected_version=1,
                ),
                idempotency_key="direct-result-1",
            )
        )
        await db.commit()

        assert next_task is None
        assert result.follow_up_id is None
        assert result.follow_up_data_id == data_id
        assert data.classification == "nurturing"
        assert data.blocking_human_call_id is None
        assert data.version == 2
        assert await db.scalar(
            select(func.count()).select_from(AiCallFollowUpTaskModel)
        ) == 0


@pytest.mark.anyio
async def test_follow_up_data_callback_connection_requires_active_sip_status(
    session_factory,
) -> None:
    console_session_id, data_id = await _seed_follow_up_data_without_task(
        session_factory
    )

    async with session_factory() as db:
        factory = _FakeCallbackFactory()
        factory.sip_call_status = "ringing"
        recording_service = SimpleNamespace(
            start_for_session=AsyncMock(),
            start_session_participant_recordings=AsyncMock(),
            start_human_agent_recording=AsyncMock(),
        )
        service = AiCallFollowUpService(
            db,
            callback_factory=factory,
            recording_service=recording_service,
        )
        auth = _auth(db, user_id=20)
        callback, _ = await service.start_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpDataCallIn(console_session_id=console_session_id),
            idempotency_key="direct-call-connected-status",
        )

        with pytest.raises(CustomException) as ringing:
            await service.confirm_follow_up_data_callback_connected(
                auth,
                follow_up_data_id=data_id,
                call_id=callback.call_id,
                payload=AgentPresenceSessionIn(
                    console_session_id=console_session_id,
                ),
            )
        assert _error_code(ringing.value) == "CALLBACK_NOT_CONNECTED"

        factory.sip_call_status = "active"
        record = await service.confirm_follow_up_data_callback_connected(
            auth,
            follow_up_data_id=data_id,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )

        assert record.status == "running"
        assert record.answered_at is not None
        recording_service.start_for_session.assert_awaited_once()


@pytest.mark.anyio
async def test_follow_up_data_direct_call_keeps_classification_when_not_connected(
    session_factory,
) -> None:
    console_session_id, data_id = await _seed_follow_up_data_without_task(
        session_factory
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        callback, _ = await service.start_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpDataCallIn(console_session_id=console_session_id),
            idempotency_key="direct-call-no-answer",
        )
        await service.handle_livekit_webhook_event(
            event_type="participant_left",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={"participant": {"attributes": {"sip.callStatus": "no_answer"}}},
        )

        data, task, _ = await service.submit_follow_up_data_handling_result(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpHandlingResultIn(
                call_id=callback.call_id,
                contact_result="no_answer",
                remark="本次人工外呼无人接听。",
                schedule_follow_up=False,
                expected_version=1,
            ),
            idempotency_key="direct-result-no-answer",
        )

        assert task is None
        assert data.classification == "interested"
        assert data.latest_conclusion == "本次人工外呼无人接听。"
        assert data.blocking_human_call_id is None


@pytest.mark.anyio
async def test_follow_up_data_direct_call_can_schedule_owned_task(
    session_factory,
) -> None:
    console_session_id, data_id = await _seed_follow_up_data_without_task(
        session_factory
    )
    callback_at = datetime.now(timezone.utc) + timedelta(days=1)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        callback, _ = await service.start_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpDataCallIn(console_session_id=console_session_id),
            idempotency_key="direct-call-schedule",
        )
        await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={"participant": {"attributes": {"sip.callStatus": "active"}}},
        )
        await service.end_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )
        _, task, _ = await service.submit_follow_up_data_handling_result(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpHandlingResultIn(
                call_id=callback.call_id,
                contact_result="connected",
                classification="interested",
                conclusion="客户约定明天再沟通。",
                schedule_follow_up=True,
                next_follow_up_at=callback_at,
                expected_version=1,
            ),
            idempotency_key="direct-result-schedule",
        )
        await db.commit()

        assert task is not None
        assert task.owner_agent_identity == "agent-20"
        assert task.customer_callback_at == callback_at
        assert task.source_type == "manual_schedule"


@pytest.mark.anyio
async def test_follow_up_data_call_requires_confirmation_before_takeover(
    session_factory,
) -> None:
    console_session_id, data_id = await _seed_follow_up_data_without_task(
        session_factory
    )
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallFollowUpTaskModel(
                id=700,
                tenant_id="tenant-a",
                follow_up_data_id=data_id,
                source_type="manual_schedule",
                source_key="takeover-test",
                source_call_id="call-1",
                source_handoff_id=None,
                scene_code="intro_contract",
                business_type="outbound_attempt",
                business_id="302",
                contact_ref="call:call-1",
                masked_contact="138****0000",
                owner_agent_identity="agent-21",
                status="processing",
                follow_up_reason="原坐席跟进",
                customer_callback_at=None,
                summary=None,
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            )
        )

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        with pytest.raises(CustomException) as confirmation:
            await service.start_follow_up_data_callback(
                auth,
                follow_up_data_id=data_id,
                payload=FollowUpDataCallIn(console_session_id=console_session_id),
                idempotency_key="takeover-call",
            )
        assert _error_code(confirmation.value) == "FOLLOW_UP_TAKEOVER_REQUIRED"

        callback, task = await service.start_follow_up_data_callback(
            auth,
            follow_up_data_id=data_id,
            payload=FollowUpDataCallIn(
                console_session_id=console_session_id,
                takeover=True,
                takeover_reason="管理员确认由当前坐席接管并外呼",
            ),
            idempotency_key="takeover-call",
        )

        assert task is not None
        assert task.owner_agent_identity == "agent-20"
        request = await db.scalar(
            select(AiCallFollowUpCallRequestModel).where(
                AiCallFollowUpCallRequestModel.call_id == callback.call_id
            )
        )
        assert request is not None
        assert request.assignment_action == "takeover"
        assert request.previous_owner_agent_identity == "agent-21"
        assert request.new_owner_agent_identity == "agent-20"


@pytest.mark.anyio
async def test_human_only_factory_creates_room_and_sip_without_agent_runner() -> None:
    room_manager = _FakeRoomManager()
    sip_client = _FakeSipClient()
    factory = HumanOnlySipSessionFactory(
        room_manager=room_manager,
        sip_client=sip_client,
    )

    result = await factory.create(
        call_id="call-callback",
        callee_phone_number="13800000000",
    )

    assert room_manager.created == ["ai-call-call-callback"]
    assert sip_client.created == [
        {
            "room_name": "ai-call-call-callback",
            "participant_identity": "sip-call-callback",
            "callee_phone_number": "13800000000",
            "wait_until_answered": False,
        }
    ]
    assert result.agent_participant_identity == "human-callback-call-callback"
    assert result.participant_token == "agent-token"


@pytest.mark.anyio
async def test_callback_requires_owner_and_available_presence_without_persisting_phone(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_agent(session_factory, user_id=21, agent_identity="agent-21")
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        owner_service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        owner_auth = _auth(db, user_id=20)
        await owner_service.claim_follow_up(owner_auth, follow_up_id=100)

        with pytest.raises(CustomException):
            await owner_service.start_callback(
                _auth(db, user_id=21),
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )

        result = await owner_service.start_callback(
            owner_auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )
        assert result.status == "accepted"
        assert result.call_id.startswith("call_")
        assert "13800000000" not in repr(result)

        record = (
            await db.execute(
                select(AiCallRecordModel).where(AiCallRecordModel.call_id == result.call_id)
            )
        ).scalar_one()
        assert record.follow_up_id == 100
        assert record.callee_phone_number_hash != "13800000000"
        assert record.callee_phone_number_masked == "138****0000"
        assert "13800000000" not in repr(record.__dict__)
        assert owner_service.callback_factory.calls == [
            {"call_id": result.call_id, "callee_phone_number": "13800000000"}
        ]

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "claiming"
        assert presence.active_call_id == result.call_id

        with pytest.raises(CustomException) as busy:
            await owner_service.start_callback(
                owner_auth,
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )
        assert _error_code(busy.value) == "AGENT_ALREADY_IN_CALL"


@pytest.mark.anyio
async def test_owned_follow_up_detail_lists_source_and_callback_records(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )

        task = await service.get_follow_up(auth, follow_up_id=100)
        detail = service.follow_up_payload(task)

        assert detail["source_record"]["call_id"] == "call-unanswered-100"
        assert [record["call_id"] for record in detail["callback_records"]] == [
            callback.call_id
        ]
        assert detail["attempts"] == []


@pytest.mark.anyio
async def test_callback_rejects_follow_up_without_a_saved_source_phone(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        source = (
            await db.execute(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.call_id == "call-unanswered-100"
                )
            )
        ).scalar_one()
        source.callee_phone_number = None
        await db.commit()

        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        with pytest.raises(CustomException) as failed:
            await service.start_callback(
                auth,
                follow_up_id=100,
                payload=FollowUpCallIn(console_session_id=console_session_id),
            )

        assert _error_code(failed.value) == "CALLBACK_NUMBER_UNAVAILABLE"
        assert service.callback_factory.calls == []


@pytest.mark.anyio
async def test_no_answer_callback_waits_for_atomic_handling_result(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        first = await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="no_answer",
            ring_duration_seconds=12,
        )
        second = await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="no_answer",
            ring_duration_seconds=12,
        )
        assert first.id == second.id
        assert first.related_call_id == callback.call_id

        task = (
            await db.execute(
                select(AiCallFollowUpTaskModel).where(AiCallFollowUpTaskModel.id == 100)
            )
        ).scalar_one()
        assert task.status == "processing"
        attempts = list((await db.execute(select(AiCallFollowUpAttemptModel))).scalars().all())
        assert len(attempts) == 1
        assert attempts[0].attempt_result == "no_answer"
        assert attempts[0].ring_duration_seconds == 12
        record = (
            await db.execute(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.call_id == callback.call_id
                )
            )
        ).scalar_one()
        assert record.duration_ms is not None
        assert record.duration_ms >= 0

        detail = service.follow_up_payload(
            await service.get_follow_up(auth, follow_up_id=100)
        )
        assert detail["awaiting_handling_result"] is True
        assert detail["pending_handling_call_id"] == callback.call_id

        next_follow_up_at = datetime.now(timezone.utc) + timedelta(days=1)
        with pytest.raises(CustomException, match="联系结果与回拨事实不一致"):
            await service.submit_handling_result(
                auth,
                follow_up_id=100,
                payload=FollowUpHandlingResultIn(
                    call_id=callback.call_id,
                    contact_result="connected",
                    remark="错误地改成已接通",
                    next_action="continue",
                    next_follow_up_at=next_follow_up_at,
                ),
                idempotency_key="callback-mismatch",
            )
        handled_task, handling = await service.submit_handling_result(
            auth,
            follow_up_id=100,
            payload=FollowUpHandlingResultIn(
                call_id=callback.call_id,
                contact_result="no_answer",
                remark="本次回拨未接通",
                next_action="continue",
                next_follow_up_at=next_follow_up_at,
            ),
            idempotency_key="callback-no-answer",
        )
        assert handled_task.status == "processing"
        assert handling.contact_channel == "manual_phone"
        assert service.follow_up_payload(handled_task)["awaiting_handling_result"] is False
        with pytest.raises(CustomException, match="已提交处理结果"):
            await service.submit_handling_result(
                auth,
                follow_up_id=100,
                payload=FollowUpHandlingResultIn(
                    call_id=callback.call_id,
                    contact_result="no_answer",
                    remark="重复提交同一次回拨",
                    next_action="continue",
                    next_follow_up_at=next_follow_up_at,
                ),
                idempotency_key="callback-no-answer-second-key",
            )

        presence = (
            await db.execute(
                select(AiCallHandoffAgentModel).where(
                    AiCallHandoffAgentModel.agent_identity == "agent-20"
                )
            )
        ).scalar_one()
        assert presence.status == "available"
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_callback_connection_requires_active_livekit_sip_status(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        factory = _FakeCallbackFactory()
        factory.sip_call_status = "ringing"
        recording_service = SimpleNamespace(
            start_for_session=AsyncMock(),
            start_session_participant_recordings=AsyncMock(),
            start_human_agent_recording=AsyncMock(),
        )
        service = AiCallFollowUpService(
            db,
            callback_factory=factory,
            recording_service=recording_service,
        )
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )

        with pytest.raises(CustomException) as ringing:
            await service.confirm_callback_connected(
                auth,
                follow_up_id=100,
                call_id=callback.call_id,
                payload=AgentPresenceSessionIn(
                    console_session_id=console_session_id,
                ),
            )
        assert _error_code(ringing.value) == "CALLBACK_NOT_CONNECTED"

        factory.sip_call_status = "active"
        attempt = await service.confirm_callback_connected(
            auth,
            follow_up_id=100,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )

        assert attempt.attempt_result == "connected"
        detail = service.follow_up_payload(
            await service.get_follow_up(auth, follow_up_id=100)
        )
        assert [item["attempt_result"] for item in detail["attempts"]] == [
            "connected"
        ]
        recording_service.start_for_session.assert_awaited_once()


@pytest.mark.anyio
async def test_callback_joined_while_sip_is_ringing_is_not_connected(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )

        result = await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={
                "participant": {
                    "attributes": {"sip.callStatus": "ringing"},
                }
            },
        )

        assert result == {"handled": False, "reason": "sip_not_connected"}
        detail = service.follow_up_payload(
            await service.get_follow_up(auth, follow_up_id=100)
        )
        assert detail["attempts"] == []


@pytest.mark.anyio
async def test_callback_livekit_webhook_maps_no_answer_to_follow_up_outcome(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FakeCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        result = await service.handle_livekit_webhook_event(
            event_type="participant_left",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={
                "participant": {
                    "attributes": {"sip.callStatus": "no_answer"},
                }
            },
        )

        assert result == {
            "handled": True,
            "action": "record_callback_outcome",
            "callId": callback.call_id,
            "attemptResult": "no_answer",
        }
        attempt = (
            await db.execute(
                select(AiCallFollowUpAttemptModel).where(
                    AiCallFollowUpAttemptModel.related_call_id == callback.call_id
                )
            )
        ).scalar_one()
        assert attempt.attempt_result == "no_answer"


@pytest.mark.anyio
async def test_immediate_sip_failure_records_technical_failure_and_releases_agent(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=_FailingCallbackFactory())
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)

        with pytest.raises(CustomException) as failed:
            await service.start_callback(
                auth,
                follow_up_id=100,
                payload=FollowUpCallIn(
                    console_session_id=console_session_id,
                ),
            )
        assert failed.value.data == {"errorCode": "sip_create_participant_failed"}

        record = (
            await db.execute(
                select(AiCallRecordModel).where(AiCallRecordModel.follow_up_id == 100)
            )
        ).scalar_one()
        assert record.status == "failed"
        assert record.failure_stage == "sip_callback"
        attempt = (
            await db.execute(
                select(AiCallFollowUpAttemptModel).where(
                    AiCallFollowUpAttemptModel.related_call_id == record.call_id
                )
            )
        ).scalar_one()
        assert attempt.attempt_result == "technical_failure"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "processing"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None


@pytest.mark.anyio
async def test_concurrent_callbacks_allow_only_one_active_call(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)
    async with session_factory() as db:
        service = AiCallFollowUpService(db)
        await service.claim_follow_up(_auth(db, user_id=20), follow_up_id=100)
        await db.commit()

    callback_factory = _FakeCallbackFactory()

    async def start_callback():
        async with session_factory() as db:
            service = AiCallFollowUpService(db, callback_factory=callback_factory)
            try:
                return await service.start_callback(
                    _auth(db, user_id=20),
                    follow_up_id=100,
                    payload=FollowUpCallIn(
                        console_session_id=console_session_id,
                    ),
                )
            except CustomException as exc:
                return exc

    results = await asyncio.gather(start_callback(), start_callback())

    accepted = [item for item in results if not isinstance(item, CustomException)]
    conflicts = [item for item in results if isinstance(item, CustomException)]
    assert len(accepted) == 1
    assert len(conflicts) == 1
    assert _error_code(conflicts[0]) == "AGENT_ALREADY_IN_CALL"
    assert len(callback_factory.calls) == 1
    async with session_factory() as db:
        record_count = await db.scalar(
            select(func.count(AiCallRecordModel.id)).where(
                AiCallRecordModel.follow_up_id == 100
            )
        )
        assert record_count == 1


@pytest.mark.anyio
async def test_connected_callback_hangup_finishes_call_and_releases_agent(
    session_factory,
) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        factory = _FakeCallbackFactory()
        recording_service = SimpleNamespace(
            start_for_session=AsyncMock(),
            start_session_participant_recordings=AsyncMock(),
            start_human_agent_recording=AsyncMock(),
            stop_for_session=AsyncMock(),
        )
        service = AiCallFollowUpService(
            db,
            callback_factory=factory,
            recording_service=recording_service,
        )
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(
                console_session_id=console_session_id,
            ),
        )

        await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={"participant": {"attributes": {"sip.callStatus": "active"}}},
        )
        await service.handle_livekit_webhook_event(
            event_type="participant_joined",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
            payload={"participant": {"attributes": {"sip.callStatus": "active"}}},
        )
        recording_service.start_for_session.assert_awaited_once_with(
            tenant_id="tenant-a",
            call_id=callback.call_id,
            room_name=f"ai-call-{callback.call_id}",
            customer_participant_identity=f"sip-{callback.call_id}",
            ai_participant_identity=None,
        )
        recording_service.start_session_participant_recordings.assert_awaited_once_with(
            tenant_id="tenant-a",
            call_id=callback.call_id,
            room_name=f"ai-call-{callback.call_id}",
            customer_participant_identity=f"sip-{callback.call_id}",
            ai_participant_identity=None,
        )
        recording_service.start_human_agent_recording.assert_awaited_once_with(
            tenant_id="tenant-a",
            call_id=callback.call_id,
            room_name=f"ai-call-{callback.call_id}",
            handoff_id=None,
            participant_identity=f"human-callback-{callback.call_id}",
        )
        await db.commit()
        result = await service.handle_livekit_webhook_event(
            event_type="participant_left",
            room_name=f"ai-call-{callback.call_id}",
            participant_identity=f"sip-{callback.call_id}",
        )

        assert result == {
            "handled": True,
            "action": "complete_connected_callback",
            "callId": callback.call_id,
            "attemptResult": "connected",
        }
        recording_service.stop_for_session.assert_awaited_once_with(
            tenant_id="tenant-a",
            call_id=callback.call_id,
        )
        record = (
            await db.execute(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.call_id == callback.call_id
                )
            )
        ).scalar_one()
        assert record.status == "completed"
        assert record.ended_at is not None
        assert record.duration_ms is not None
        assert record.duration_ms >= 0
        assert record.end_reason == "callback_completed"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "processing"
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None

        ended = await service.end_callback(
            auth,
            follow_up_id=100,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )
        assert ended.status == "completed"
        assert factory.ended_calls == []


@pytest.mark.anyio
async def test_concurrent_connected_confirmations_record_once(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)
    factory = _ConcurrentStatusCallbackFactory()
    recording_service = SimpleNamespace(
        start_for_session=AsyncMock(),
        start_session_participant_recordings=AsyncMock(),
        start_human_agent_recording=AsyncMock(),
    )

    async with session_factory() as db:
        service = AiCallFollowUpService(db, callback_factory=factory)
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )
        await db.commit()

    async def confirm_connected():
        async with session_factory() as db:
            service = AiCallFollowUpService(
                db,
                callback_factory=factory,
                recording_service=recording_service,
            )
            attempt = await service.confirm_callback_connected(
                _auth(db, user_id=20),
                follow_up_id=100,
                call_id=callback.call_id,
                payload=AgentPresenceSessionIn(
                    console_session_id=console_session_id,
                ),
            )
            await db.commit()
            return attempt

    attempts = await asyncio.gather(confirm_connected(), confirm_connected())

    assert attempts[0].id == attempts[1].id
    recording_service.start_for_session.assert_awaited_once()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(AiCallFollowUpAttemptModel.id)).where(
                AiCallFollowUpAttemptModel.related_call_id == callback.call_id
            )
        ) == 1


@pytest.mark.anyio
async def test_agent_ends_connected_callback_and_releases_presence(session_factory) -> None:
    console_session_id = await _seed_agent(
        session_factory,
        user_id=20,
        agent_identity="agent-20",
    )
    await _seed_unanswered_follow_up(session_factory)

    async with session_factory() as db:
        factory = _FakeCallbackFactory()
        recording_service = SimpleNamespace(stop_for_session=AsyncMock())
        service = AiCallFollowUpService(
            db,
            callback_factory=factory,
            recording_service=recording_service,
        )
        auth = _auth(db, user_id=20)
        await service.claim_follow_up(auth, follow_up_id=100)
        callback = await service.start_callback(
            auth,
            follow_up_id=100,
            payload=FollowUpCallIn(console_session_id=console_session_id),
        )
        await service.record_callback_outcome(
            call_id=callback.call_id,
            attempt_result="connected",
        )
        active_detail = service.follow_up_payload(
            await service.get_follow_up(auth, follow_up_id=100)
        )
        assert active_detail["awaiting_handling_result"] is False

        record = await service.end_callback(
            auth,
            follow_up_id=100,
            call_id=callback.call_id,
            payload=AgentPresenceSessionIn(console_session_id=console_session_id),
        )

        assert factory.ended_calls == [callback.call_id]
        assert record.status == "completed"
        assert record.duration_ms is not None
        assert record.duration_ms >= 0
        assert record.end_reason == "callback_ended_by_agent"
        task = await db.get(AiCallFollowUpTaskModel, 100)
        assert task.status == "processing"
        ended_detail = service.follow_up_payload(
            await service.get_follow_up(auth, follow_up_id=100)
        )
        assert ended_detail["awaiting_handling_result"] is True
        assert ended_detail["pending_handling_call_id"] == callback.call_id
        presence = await db.get(AiCallHandoffAgentModel, 20)
        assert presence.status == "available"
        assert presence.active_call_id is None
        recording_service.stop_for_session.assert_awaited_once_with(
            tenant_id="tenant-a",
            call_id=callback.call_id,
        )
