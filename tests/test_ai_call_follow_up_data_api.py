from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter, follow_up_data_controller
from app.api.v1.ai_call.follow_up_data_schema import (
    FollowUpDataClassificationIn,
    FollowUpDataScheduleIn,
)
from app.api.v1.ai_call.model import (
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpScheduleRequestModel,
    AiCallFollowUpTaskModel,
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
from app.core.dependencies import get_ai_call_manager
from app.core.exceptions import CustomException
from app.services.ai_call.follow_up_data_service import AiCallFollowUpDataService
from app.services.ai_call.semantic_analysis import AiCallSemanticAnalysisService


@pytest.fixture
async def session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'follow-up-data.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_follow_up_data(
    session_factory,
    *,
    tenant_id: str = "tenant-a",
    data_id: int = 100,
    task_id: int = 200,
    target_id: int = 300,
    call_id: str = "call-source-1",
    classification: str = "interested",
    with_active_task: bool = True,
    with_data: bool = True,
) -> None:
    now = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallOutboundTaskModel(
                id=task_id,
                tenant_id=tenant_id,
                validation_id=1,
                idempotency_key=f"task-{tenant_id}-{task_id}",
                request_fingerprint=f"fingerprint-{task_id}",
                task_name="SaaS 产品回访",
                task_mode="batch",
                status="COMPLETED",
                total_targets=1,
                completed_targets=1,
                connected_targets=1,
                failed_targets=0,
                execution_mode="immediate",
                prompt_name="产品介绍",
                scene_code="intro_product",
                voice="Cherry",
                rule_id=1,
                rule_name="工作日",
                rule_summary="09:00-18:00",
                config_snapshot_json="{}",
                created_by=1,
                created_at=now,
                updated_at=now,
            )
        )
        db.add(
            AiCallOutboundTargetModel(
                id=target_id,
                tenant_id=tenant_id,
                task_id=task_id,
                validation_id=1,
                source_validation_row_id=target_id,
                source_row_number=1,
                phone_number="13800001001",
                customer_name="科技公司",
                status="COMPLETED",
                attempt_count=1,
                latest_result="connected",
                created_at=now,
                updated_at=now,
            )
        )
        if with_data:
            db.add(
                AiCallFollowUpDataModel(
                    id=data_id,
                    tenant_id=tenant_id,
                    task_id=task_id,
                    target_id=target_id,
                    source_call_id=call_id,
                    classification=classification,
                    classification_reason="客户明确希望了解产品演示。",
                    classification_source="ai",
                    classification_confidence="high",
                    suggest_review=False,
                    low_value_reason=("no_current_need" if classification == "low_value" else None),
                    latest_conclusion="客户希望下周查看产品演示。",
                    last_contact_at=now,
                    blocking_human_call_id=None,
                    version=1,
                    classification_updated_at=now,
                    classification_updated_by="ai",
                    created_at=now,
                    updated_at=now,
                )
            )
        db.add(
            AiCallRecordModel(
                id=data_id + 1,
                tenant_id=tenant_id,
                call_id=call_id,
                follow_up_data_id=data_id if with_data else None,
                business_type="outbound_task",
                business_id=str(task_id),
                scene_code="intro_product",
                entry_type="sip_outbound",
                room_name=f"room-{call_id}",
                participant_identity=f"customer-{target_id}",
                callee_phone_number_masked="138****1001",
                status="completed",
                started_at=now - timedelta(minutes=3),
                ended_at=now,
                duration_ms=180000,
            )
        )
        db.add(
            AiCallOutboundAttemptModel(
                id=data_id + 3,
                tenant_id=tenant_id,
                task_id=task_id,
                target_id=target_id,
                attempt_no=1,
                call_id=call_id,
                dialer_type="sip",
                test_scenario=None,
                command_idempotency_key=f"command-{call_id}",
                active_slot=None,
                status="COMPLETED",
                call_result="connected",
                error_message=None,
                started_at=now,
                ended_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        if with_data and with_active_task:
            db.add(
                AiCallFollowUpTaskModel(
                    id=data_id + 2,
                    tenant_id=tenant_id,
                    follow_up_data_id=data_id,
                    source_type="manual_schedule",
                    source_key=f"follow-up-data:{data_id}",
                    source_call_id=call_id,
                    source_handoff_id=None,
                    scene_code="intro_product",
                    business_type="outbound_task",
                    business_id=str(task_id),
                    contact_ref=f"call:{call_id}",
                    masked_contact="138****1001",
                    owner_agent_identity=None,
                    status="pending",
                    follow_up_reason="客户约定下周继续沟通",
                    customer_callback_at=now + timedelta(days=7),
                    summary=None,
                    closed_reason=None,
                    closed_remark=None,
                    completed_at=None,
                    closed_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )


@pytest.mark.anyio
async def test_transfer_failure_creates_nurturing_data_without_task(
    session_factory,
) -> None:
    await _seed_follow_up_data(
        session_factory,
        with_data=False,
        with_active_task=False,
    )
    now = datetime(2026, 8, 15, 10, 5, tzinfo=timezone.utc)

    async with session_factory() as db, db.begin():
        handoff = AiCallHandoffModel(
            id=104,
            tenant_id="tenant-a",
            handoff_id="handoff-failed-1",
            call_id="call-source-1",
            room_name="room-call-source-1",
            scene_code="intro_product",
            status="failed",
            request_source="customer",
            request_reason="customer_requested_human",
            request_message="请转人工",
            requested_at=now,
            ended_at=now,
            end_reason="no_online_agent",
            failure_stage="availability_check",
        )
        db.add(handoff)
        await db.flush()

        service = AiCallFollowUpDataService.from_session(db)
        created = await service.apply_transfer_failure(
            handoff,
            reason="转人工时当前场景没有在线坐席",
        )
        repeated = await service.apply_transfer_failure(
            handoff,
            reason="转人工时当前场景没有在线坐席",
        )

        assert created is not None
        assert repeated is not None
        assert repeated.id == created.id
        assert created.classification == "nurturing"
        assert created.classification_source == "system"
        assert created.latest_conclusion == "转人工时当前场景没有在线坐席"
        record = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == "call-source-1")
        )
        assert record is not None
        assert record.follow_up_data_id == created.id
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0
        history = list(
            (await db.execute(select(AiCallFollowUpClassificationHistoryModel))).scalars().all()
        )
        assert len(history) == 1
        assert history[0].source == "transfer_failed"


@pytest.mark.anyio
async def test_transfer_failure_ai_mismatch_requires_review(
    session_factory,
) -> None:
    await _seed_follow_up_data(
        session_factory,
        with_data=False,
        with_active_task=False,
    )
    now = datetime(2026, 8, 15, 10, 5, tzinfo=timezone.utc)

    async with session_factory() as db, db.begin():
        handoff = AiCallHandoffModel(
            id=105,
            tenant_id="tenant-a",
            handoff_id="handoff-failed-ai-mismatch",
            call_id="call-source-1",
            room_name="room-call-source-1",
            scene_code="intro_product",
            status="failed",
            request_source="customer",
            request_reason="customer_requested_human",
            request_message="请转人工",
            requested_at=now,
            ended_at=now,
            end_reason="no_online_agent",
            failure_stage="availability_check",
        )
        db.add(handoff)
        await db.flush()

        service = AiCallFollowUpDataService.from_session(db)
        data = await service.apply_transfer_failure(
            handoff,
            reason="转人工时当前场景没有在线坐席",
        )
        assert data is not None

        analysis = AiCallSemanticAnalysisModel(
            id=503,
            call_id="call-source-1",
            scene_code="intro_product",
            analysis_scene_code="ai_call_semantic_analysis",
            analysis_status="2",
            analysis_version=1,
            analysis_result=json.dumps(
                {
                    "classification": "low_value",
                    "low_value_reason": "non_target_customer",
                    "confidence": "high",
                    "valid_dialogue": False,
                    "reason": "客户并非目标客户。",
                    "evidence": [],
                    "evidence_conflict": False,
                },
                ensure_ascii=False,
            ),
            customer_intent="negative",
            follow_up_suggested=False,
            follow_up_consent=None,
            follow_up_reason=None,
            follow_up_preferred_at=None,
            follow_up_confidence=None,
            follow_up_review_status=None,
            follow_up_reviewed_by=None,
            follow_up_reviewed_by_name=None,
            follow_up_reviewed_at=None,
            analysis_error=None,
            analysis_retry_count=0,
            analysis_started_at=now,
            analysis_finished_at=now,
            transcript_hash=None,
            transcript_snapshot_json=None,
            created_at=now,
            updated_at=now,
        )
        db.add(analysis)
        await db.flush()

        response = AiCallSemanticAnalysisService.analysis_to_dict(
            analysis,
            current_classification=data.classification,
        )
        assert response["classificationRequiresReview"] is True

        await service.review_classification(
            tenant_id="tenant-a",
            follow_up_data_id=data.id,
            analysis=analysis,
            payload=FollowUpDataClassificationIn(
                classification="nurturing",
                reason="人工复核后保留当前业务分类。",
                expected_version=1,
            ),
            idempotency_key="transfer-failure-review-keep-current",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert data.classification == "nurturing"
        assert data.classification_source == "human"
        assert analysis.follow_up_review_status == "adjusted"
        history = list(
            (await db.execute(select(AiCallFollowUpClassificationHistoryModel))).scalars().all()
        )
        assert [item.source for item in history] == [
            "transfer_failed",
            "manual_adjustment",
        ]
        assert history[-1].ai_adopted is False
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0


def test_follow_up_data_routes_require_manager_permission() -> None:
    expected_paths = {
        "/ai-call/follow-up-data",
        "/ai-call/follow-up-data/{follow_up_data_id}",
        "/ai-call/follow-up-data/{follow_up_data_id}/classification",
        "/ai-call/follow-up-data/{follow_up_data_id}/schedule",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/call",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/call/{call_id}/end",
        "/ai-call/agent-console/follow-up-data/{follow_up_data_id}/handling-results",
    }
    routes = {route.path: route for route in AiCallRouter.routes}
    assert expected_paths <= routes.keys()
    for path in expected_paths:
        dependencies = {dependency.call for dependency in routes[path].dependant.dependencies}
        assert get_ai_call_manager in dependencies


def test_classification_payload_requires_low_value_reason_only_for_low_value() -> None:
    with pytest.raises(ValidationError, match="低价值原因"):
        FollowUpDataClassificationIn(
            classification="low_value",
            reason="客户暂无需求",
            expected_version=1,
        )
    with pytest.raises(ValidationError, match="只有低价值"):
        FollowUpDataClassificationIn(
            classification="interested",
            reason="客户需要演示",
            low_value_reason="no_current_need",
            expected_version=1,
        )


def test_schedule_payload_requires_zoned_time() -> None:
    with pytest.raises(ValidationError, match="必须包含时区"):
        FollowUpDataScheduleIn(
            follow_up_reason="客户要求下周回访",
            next_follow_up_at=datetime.now() + timedelta(days=1),
            expected_version=1,
        )


@pytest.mark.anyio
async def test_schedule_follow_up_rejects_past_time(session_factory) -> None:
    await _seed_follow_up_data(session_factory, with_active_task=False)
    payload = FollowUpDataScheduleIn(
        follow_up_reason="客户要求回访",
        next_follow_up_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        with pytest.raises(CustomException, match="必须晚于当前时间") as rejected:
            await AiCallFollowUpDataService.from_session(db).schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload,
                idempotency_key="schedule-in-past",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert rejected.value.status_code == 422


@pytest.mark.anyio
async def test_follow_up_data_list_and_detail_are_tenant_scoped(session_factory) -> None:
    await _seed_follow_up_data(session_factory)
    await _seed_follow_up_data(
        session_factory,
        tenant_id="tenant-b",
        data_id=110,
        task_id=210,
        target_id=310,
        call_id="call-tenant-b",
    )

    async with session_factory() as db:
        service = AiCallFollowUpDataService.from_session(db)
        rows, total = await service.list_page(
            tenant_id="tenant-a",
            classification="interested",
            page_num=1,
            page_size=10,
        )

        assert total == 1
        assert rows[0]["follow_up_data_id"] == "100"
        assert rows[0]["tenant_id"] == "tenant-a"
        assert rows[0]["customer_name"] == "科技公司"
        assert rows[0]["masked_contact"] == "138****1001"
        assert rows[0]["task_name"] == "SaaS 产品回访"
        assert rows[0]["active_follow_up_id"] == "102"
        assert rows[0]["next_follow_up_at"] is not None
        assert rows[0]["after_call_result_status"] == "not_applicable"

        detail = await service.get_detail(tenant_id="tenant-a", follow_up_data_id=100)
        assert detail["follow_up_data_id"] == "100"
        assert [item["call_id"] for item in detail["timeline"]] == ["call-source-1"]

        with pytest.raises(CustomException) as exc_info:
            await service.get_detail(
                tenant_id="tenant-b",
                follow_up_data_id=100,
            )
        assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_follow_up_data_searches_customer_name_only(session_factory) -> None:
    await _seed_follow_up_data(session_factory)

    async with session_factory() as db:
        service = AiCallFollowUpDataService.from_session(db)
        rows, total = await service.list_page(
            tenant_id="tenant-a",
            classification="interested",
            customer_name="科技",
            page_num=1,
            page_size=10,
        )
        assert total == 1
        assert rows[0]["customer_name"] == "科技公司"

        rows, total = await service.list_page(
            tenant_id="tenant-a",
            classification="interested",
            customer_name="13800001001",
            page_num=1,
            page_size=10,
        )
        assert total == 0
        assert rows == []


@pytest.mark.anyio
async def test_follow_up_data_projects_pending_and_submitted_after_call_status(
    session_factory,
) -> None:
    await _seed_follow_up_data(session_factory)
    now = datetime(2026, 8, 15, 11, 0, tzinfo=timezone.utc)

    async with session_factory() as db, db.begin():
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        data.blocking_human_call_id = "manual-call-1"
        service = AiCallFollowUpDataService.from_session(db)
        rows, _ = await service.list_page(
            tenant_id="tenant-a",
            classification="interested",
            page_num=1,
            page_size=10,
        )
        assert rows[0]["after_call_result_status"] == "pending"

        data.blocking_human_call_id = None
        db.add(
            AiCallFollowUpHandlingResultModel(
                id=999,
                tenant_id="tenant-a",
                follow_up_id=102,
                follow_up_data_id=100,
                idempotency_key="handling-1",
                request_fingerprint=None,
                related_call_id="manual-call-1",
                contact_channel="phone",
                contact_result="connected",
                remark="客户同意继续沟通",
                next_action="continue",
                next_follow_up_at=now + timedelta(days=2),
                closed_reason=None,
                agent_identity="agent-1",
                handled_at=now,
                created_at=now,
            )
        )
        rows, _ = await service.list_page(
            tenant_id="tenant-a",
            classification="interested",
            page_num=1,
            page_size=10,
        )
        assert rows[0]["after_call_result_status"] == "submitted"


@pytest.mark.anyio
@pytest.mark.parametrize(
    (
        "classification",
        "low_value_reason",
        "expected_task_status",
        "keeps_callback_at",
    ),
    [
        ("interested", None, "pending", True),
        ("nurturing", None, "pending", True),
        ("low_value", "no_current_need", "closed", False),
        ("converted", None, "completed", False),
    ],
)
async def test_manual_classification_is_atomic_idempotent_and_versioned(
    session_factory,
    classification: str,
    low_value_reason: str | None,
    expected_task_status: str,
    keeps_callback_at: bool,
) -> None:
    await _seed_follow_up_data(session_factory)
    payload = FollowUpDataClassificationIn(
        classification=classification,
        reason="坐席确认本次分类结果。",
        conclusion="客户关注效果指标，但尚未接受产品演示。",
        low_value_reason=low_value_reason,
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        service = AiCallFollowUpDataService.from_session(db)
        first = await service.adjust_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            payload=payload,
            idempotency_key="classification-request-1",
            changed_by="20",
            changed_by_name="管理员坐席",
        )
        repeated = await service.adjust_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            payload=payload,
            idempotency_key="classification-request-1",
            changed_by="20",
            changed_by_name="管理员坐席",
        )

        assert repeated == first
        assert first["classification"] == classification
        assert first["version"] == 2
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        assert data.latest_conclusion == "客户关注效果指标，但尚未接受产品演示。"
        task = await db.scalar(select(AiCallFollowUpTaskModel))
        assert task is not None
        assert task.status == expected_task_status
        assert (task.customer_callback_at is not None) is keeps_callback_at
        assert (
            await db.scalar(
                select(func.count()).select_from(AiCallFollowUpClassificationHistoryModel)
            )
            == 1
        )

        conflicting_payload = payload.model_copy(update={"reason": "相同键但不同请求内容"})
        with pytest.raises(CustomException) as conflict:
            await service.adjust_classification(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=conflicting_payload,
                idempotency_key="classification-request-1",
                changed_by="20",
                changed_by_name="管理员坐席",
            )
        assert conflict.value.status_code == 409

        with pytest.raises(CustomException) as stale:
            await service.adjust_classification(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload,
                idempotency_key="classification-request-2",
                changed_by="20",
                changed_by_name="管理员坐席",
            )
        assert stale.value.status_code == 409
        assert stale.value.data["currentVersion"] == 2


@pytest.mark.anyio
async def test_follow_up_data_http_flow_keeps_classification_and_task_separate(
    session_factory,
    monkeypatch,
) -> None:
    await _seed_follow_up_data(session_factory, with_active_task=False)
    await _seed_follow_up_data(
        session_factory,
        tenant_id="tenant-b",
        data_id=110,
        task_id=210,
        target_id=310,
        call_id="call-tenant-b",
        with_active_task=False,
    )

    async def ignore_console_event(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(
        follow_up_data_controller,
        "publish_agent_console_event",
        ignore_console_event,
    )
    app = FastAPI()
    app.include_router(follow_up_data_controller.FollowUpDataRouter, prefix="/ai-call")

    async with session_factory() as db:
        auth = AuthSchema(
            db=db,
            user=UserModel(
                user_id=20,
                tenant_id="tenant-a",
                user_name="manager",
                nick_name="管理员",
            ),
            permissions=frozenset({"ai_call:agent:manage"}),
        )
        app.dependency_overrides[get_ai_call_manager] = lambda: auth
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            listed = await client.get(
                "/ai-call/follow-up-data",
                params={"classification": "interested"},
            )
            assert listed.status_code == 200
            assert listed.json()["total"] == 1
            assert listed.json()["rows"][0]["follow_up_data_id"] == "100"

            adjusted = await client.put(
                "/ai-call/follow-up-data/100/classification",
                headers={"Idempotency-Key": "http-adjust-1"},
                json={
                    "classification": "nurturing",
                    "reason": "客户希望稍后再联系。",
                    "expected_version": 1,
                },
            )
            assert adjusted.status_code == 200
            assert adjusted.json()["data"]["classification"] == "nurturing"
            assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0

            callback_at = datetime.now(timezone.utc) + timedelta(days=2)
            scheduled = await client.post(
                "/ai-call/follow-up-data/100/schedule",
                headers={"Idempotency-Key": "http-schedule-1"},
                json={
                    "follow_up_reason": "客户要求两天后回访。",
                    "next_follow_up_at": callback_at.isoformat(),
                    "expected_version": 2,
                },
            )
            assert scheduled.status_code == 200
            assert scheduled.json()["data"]["follow_up_data_id"] == "100"
            task = await db.scalar(select(AiCallFollowUpTaskModel))
            assert task is not None
            assert task.follow_up_data_id == 100
            assert task.status == "pending"


@pytest.mark.anyio
async def test_classification_review_after_follow_up_dismissal_creates_no_task(
    session_factory,
) -> None:
    await _seed_follow_up_data(session_factory, with_active_task=False)
    now = datetime.now(timezone.utc)
    analysis_result = {
        "classification": "interested",
        "confidence": "low",
        "valid_dialogue": True,
        "reason": "客户询问产品演示。",
        "evidence": ["客户：可以先看看演示"],
        "evidence_conflict": False,
    }
    payload = FollowUpDataClassificationIn(
        classification="interested",
        reason="客户询问产品演示。",
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        data.classification_confidence = "low"
        data.suggest_review = True
        analysis = AiCallSemanticAnalysisModel(
            id=500,
            call_id="call-source-1",
            scene_code="intro_product",
            analysis_scene_code="ai_call_semantic_analysis",
            analysis_status="2",
            analysis_version=1,
            analysis_result=json.dumps(analysis_result, ensure_ascii=False),
            customer_intent="positive",
            follow_up_suggested=False,
            follow_up_consent=None,
            follow_up_reason=None,
            follow_up_preferred_at=None,
            follow_up_confidence=None,
            follow_up_review_status="dismissed",
            follow_up_reviewed_by=None,
            follow_up_reviewed_by_name=None,
            follow_up_reviewed_at=None,
            analysis_error=None,
            analysis_retry_count=0,
            analysis_started_at=now,
            analysis_finished_at=now,
            transcript_hash=None,
            transcript_snapshot_json=None,
            created_at=now,
            updated_at=now,
        )
        db.add(analysis)
        db.add(
            AiCallFollowUpClassificationHistoryModel(
                id=501,
                tenant_id="tenant-a",
                follow_up_data_id=100,
                from_classification=None,
                to_classification="interested",
                change_reason=analysis_result["reason"],
                source="ai_auto",
                call_id="call-source-1",
                semantic_analysis_id=500,
                semantic_analysis_version=1,
                ai_suggested_classification="interested",
                ai_confidence="low",
                ai_reason=analysis_result["reason"],
                ai_evidence_json=json.dumps(analysis_result["evidence"]),
                ai_conflict=False,
                ai_adopted=True,
                idempotency_key=None,
                request_fingerprint=None,
                result_version=1,
                changed_by="ai",
                changed_by_name="AI",
                created_at=now,
            )
        )
        await db.flush()

        service = AiCallFollowUpDataService.from_session(db)
        first = await service.review_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            analysis=analysis,
            payload=payload,
            idempotency_key="classification-review-1",
            changed_by="20",
            changed_by_name="管理员",
        )
        repeated = await service.review_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            analysis=analysis,
            payload=payload,
            idempotency_key="classification-review-1",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert repeated == first
        assert data.classification == "interested"
        assert data.classification_source == "human"
        assert data.suggest_review is False
        assert analysis.follow_up_review_status == "confirmed"
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0
        assert (
            await db.scalar(
                select(func.count()).select_from(AiCallFollowUpClassificationHistoryModel)
            )
            == 2
        )


@pytest.mark.anyio
async def test_high_confidence_ai_mismatch_requires_review_and_can_keep_current(
    session_factory,
) -> None:
    await _seed_follow_up_data(
        session_factory,
        classification="nurturing",
        with_active_task=False,
    )
    now = datetime.now(timezone.utc)
    analysis_result = {
        "classification": "low_value",
        "low_value_reason": "non_target_customer",
        "confidence": "high",
        "valid_dialogue": True,
        "reason": "客户并非目标客户。",
        "evidence": ["客户：我不是负责人"],
        "evidence_conflict": False,
    }

    async with session_factory() as db, db.begin():
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        data.classification_source = "system"
        data.classification_reason = "转人工未接通，保持持续跟进。"
        analysis = AiCallSemanticAnalysisModel(
            id=502,
            call_id="call-source-1",
            scene_code="intro_product",
            analysis_scene_code="ai_call_semantic_analysis",
            analysis_status="2",
            analysis_version=1,
            analysis_result=json.dumps(analysis_result, ensure_ascii=False),
            customer_intent="negative",
            follow_up_suggested=False,
            follow_up_consent=None,
            follow_up_reason=None,
            follow_up_preferred_at=None,
            follow_up_confidence=None,
            follow_up_review_status=None,
            follow_up_reviewed_by=None,
            follow_up_reviewed_by_name=None,
            follow_up_reviewed_at=None,
            analysis_error=None,
            analysis_retry_count=0,
            analysis_started_at=now,
            analysis_finished_at=now,
            transcript_hash=None,
            transcript_snapshot_json=None,
            created_at=now,
            updated_at=now,
        )
        db.add(analysis)
        await db.flush()

        service = AiCallFollowUpDataService.from_session(db)
        applied = await service.apply_ai_analysis(analysis)

        assert applied == data
        assert data.classification == "nurturing"
        assert data.classification_source == "system"
        assert data.suggest_review is True
        assert data.version == 2
        response = AiCallSemanticAnalysisService.analysis_to_dict(
            analysis,
            current_classification=data.classification,
        )
        assert response["classificationRequiresReview"] is True
        assert response["classificationReviewStatus"] == "suggested"
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0

        with pytest.raises(CustomException) as review_required:
            await service.schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=FollowUpDataScheduleIn(
                    follow_up_reason="客户要求下周回访",
                    next_follow_up_at=now + timedelta(days=7),
                    expected_version=2,
                ),
                idempotency_key="schedule-before-classification-review",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert review_required.value.status_code == 409
        assert (
            review_required.value.data["errorCode"]
            == "CLASSIFICATION_REVIEW_REQUIRED"
        )

        await service.review_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            analysis=analysis,
            payload=FollowUpDataClassificationIn(
                classification="nurturing",
                reason="人工复核后保留当前业务分类。",
                expected_version=2,
            ),
            idempotency_key="classification-review-keep-current",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert data.classification == "nurturing"
        assert data.classification_source == "human"
        assert data.suggest_review is False
        assert analysis.follow_up_review_status == "adjusted"
        assert analysis.follow_up_reviewed_by == "20"
        assert analysis.follow_up_reviewed_by_name == "管理员"
        assert analysis.follow_up_reviewed_at is not None
        history = await db.scalar(
            select(AiCallFollowUpClassificationHistoryModel)
            .where(AiCallFollowUpClassificationHistoryModel.source == "manual_adjustment")
            .order_by(AiCallFollowUpClassificationHistoryModel.id.desc())
        )
        assert history is not None
        assert history.changed_by_name == "管理员"
        assert history.ai_adopted is False
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0


@pytest.mark.anyio
async def test_same_call_after_call_classification_can_be_reviewed_once(
    session_factory,
) -> None:
    await _seed_follow_up_data(
        session_factory,
        classification="interested",
        with_active_task=False,
    )
    now = datetime.now(timezone.utc)
    analysis_result = {
        "classification": "low_value",
        "low_value_reason": "non_target_customer",
        "confidence": "high",
        "valid_dialogue": False,
        "reason": "客户并非目标客户。",
        "evidence": ["客户：请转人工"],
        "evidence_conflict": True,
    }

    async with session_factory() as db, db.begin():
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        data.classification_source = "human"
        data.classification_updated_by = "agent-admin"
        analysis = AiCallSemanticAnalysisModel(
            id=504,
            call_id="call-source-1",
            scene_code="intro_product",
            analysis_scene_code="ai_call_semantic_analysis",
            analysis_status="2",
            analysis_version=1,
            analysis_result=json.dumps(analysis_result, ensure_ascii=False),
            customer_intent="negative",
            follow_up_suggested=False,
            follow_up_consent=None,
            follow_up_reason=None,
            follow_up_preferred_at=None,
            follow_up_confidence=None,
            follow_up_review_status=None,
            follow_up_reviewed_by=None,
            follow_up_reviewed_by_name=None,
            follow_up_reviewed_at=None,
            analysis_error=None,
            analysis_retry_count=0,
            analysis_started_at=now,
            analysis_finished_at=now,
            transcript_hash=None,
            transcript_snapshot_json=None,
            created_at=now,
            updated_at=now,
        )
        db.add(analysis)
        db.add(
            AiCallFollowUpClassificationHistoryModel(
                id=505,
                tenant_id="tenant-a",
                follow_up_data_id=100,
                from_classification=None,
                to_classification="interested",
                change_reason="坐席确认客户有意向。",
                source="handoff_after_call",
                call_id="call-source-1",
                semantic_analysis_id=None,
                semantic_analysis_version=None,
                ai_suggested_classification=None,
                ai_confidence=None,
                ai_reason=None,
                ai_evidence_json=None,
                ai_conflict=None,
                ai_adopted=None,
                idempotency_key="after-call-work-1",
                request_fingerprint="after-call-work-fingerprint-1",
                result_version=1,
                changed_by="agent-admin",
                changed_by_name="管理员",
                created_at=now,
            )
        )
        await db.flush()

        service = AiCallFollowUpDataService.from_session(db)
        await service.review_classification(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            analysis=analysis,
            payload=FollowUpDataClassificationIn(
                classification="interested",
                reason="人工复核后保留当前业务分类。",
                expected_version=1,
            ),
            idempotency_key="classification-review-after-call",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert data.classification == "interested"
        assert data.version == 2
        assert analysis.follow_up_review_status == "adjusted"

        analysis.follow_up_review_status = None

        with pytest.raises(CustomException) as stale:
            await service.review_classification(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                analysis=analysis,
                payload=FollowUpDataClassificationIn(
                    classification="interested",
                    reason="不得重复覆盖人工复核。",
                    expected_version=2,
                ),
                idempotency_key="classification-review-after-manual-review",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert stale.value.data["errorCode"] == "CLASSIFICATION_REVIEW_STALE"


@pytest.mark.anyio
async def test_schedule_follow_up_rejects_pending_same_classification_review(
    session_factory,
) -> None:
    await _seed_follow_up_data(session_factory, with_active_task=False)
    payload = FollowUpDataScheduleIn(
        follow_up_reason="客户要求下周回访",
        next_follow_up_at=datetime.now(timezone.utc) + timedelta(days=7),
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        data = await db.get(AiCallFollowUpDataModel, 100)
        assert data is not None
        data.suggest_review = True

        with pytest.raises(CustomException) as review_required:
            await AiCallFollowUpDataService.from_session(db).schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload,
                idempotency_key="schedule-before-same-classification-review",
                changed_by="20",
                changed_by_name="管理员",
            )

        assert review_required.value.status_code == 409
        assert (
            review_required.value.data["errorCode"]
            == "CLASSIFICATION_REVIEW_REQUIRED"
        )
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 0


@pytest.mark.anyio
async def test_schedule_follow_up_creates_one_unassigned_task_idempotently(
    session_factory,
) -> None:
    await _seed_follow_up_data(session_factory, with_active_task=False)
    payload = FollowUpDataScheduleIn(
        follow_up_reason="客户要求下周回访",
        next_follow_up_at=datetime.now(timezone.utc) + timedelta(days=7),
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        service = AiCallFollowUpDataService.from_session(db)
        first = await service.schedule_follow_up(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            payload=payload,
            idempotency_key="schedule-request-1",
            changed_by="20",
            changed_by_name="管理员",
        )
        repeated = await service.schedule_follow_up(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            payload=payload,
            idempotency_key="schedule-request-1",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert repeated == first
        assert first["follow_up_data_id"] == "100"
        assert first["version"] == 2
        task = await db.scalar(select(AiCallFollowUpTaskModel))
        assert task is not None
        assert first["follow_up_id"] == str(task.id)
        assert task.follow_up_data_id == 100
        assert task.source_type == "manual_schedule"
        assert task.owner_agent_identity is None
        assert task.status == "pending"
        assert task.follow_up_reason == "客户要求下周回访"
        assert task.customer_callback_at.replace(tzinfo=timezone.utc) == payload.next_follow_up_at
        assert (
            await db.scalar(select(func.count()).select_from(AiCallFollowUpScheduleRequestModel))
            == 1
        )

        with pytest.raises(CustomException) as conflict:
            await service.schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload.model_copy(update={"follow_up_reason": "另一个请求"}),
                idempotency_key="schedule-request-1",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert conflict.value.status_code == 409

        with pytest.raises(CustomException) as stale:
            await service.schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload,
                idempotency_key="schedule-request-2",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert stale.value.status_code == 409
        assert stale.value.data["currentVersion"] == 2


@pytest.mark.anyio
async def test_schedule_follow_up_updates_active_task_and_preserves_owner(
    session_factory,
) -> None:
    await _seed_follow_up_data(session_factory)
    payload = FollowUpDataScheduleIn(
        follow_up_reason="改为周五联系",
        next_follow_up_at=datetime.now(timezone.utc) + timedelta(days=3),
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        task = await db.get(AiCallFollowUpTaskModel, 102)
        assert task is not None
        task.owner_agent_identity = "agent-1"
        service = AiCallFollowUpDataService.from_session(db)
        result = await service.schedule_follow_up(
            tenant_id="tenant-a",
            follow_up_data_id=100,
            payload=payload,
            idempotency_key="schedule-update-1",
            changed_by="20",
            changed_by_name="管理员",
        )

        assert result["follow_up_id"] == "102"
        assert task.owner_agent_identity == "agent-1"
        assert task.follow_up_reason == "改为周五联系"
        assert task.customer_callback_at.replace(tzinfo=timezone.utc) == payload.next_follow_up_at
        assert await db.scalar(select(func.count()).select_from(AiCallFollowUpTaskModel)) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("classification", ["low_value", "converted"])
async def test_schedule_follow_up_rejects_terminal_classification(
    session_factory,
    classification: str,
) -> None:
    await _seed_follow_up_data(
        session_factory,
        classification=classification,
        with_active_task=False,
    )
    payload = FollowUpDataScheduleIn(
        follow_up_reason="再次联系",
        next_follow_up_at=datetime.now(timezone.utc) + timedelta(days=3),
        expected_version=1,
    )

    async with session_factory() as db, db.begin():
        service = AiCallFollowUpDataService.from_session(db)
        with pytest.raises(CustomException) as rejected:
            await service.schedule_follow_up(
                tenant_id="tenant-a",
                follow_up_data_id=100,
                payload=payload,
                idempotency_key=f"schedule-{classification}",
                changed_by="20",
                changed_by_name="管理员",
            )
        assert rejected.value.status_code == 409
