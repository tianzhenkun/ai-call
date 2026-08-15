from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
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
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.core.base_model import MappedBase
from app.core.dependencies import get_ai_call_manager
from app.core.exceptions import CustomException
from app.services.ai_call.follow_up_data_service import AiCallFollowUpDataService


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
                follow_up_data_id=data_id,
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
        if with_active_task:
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
    ("classification", "low_value_reason", "expected_task_status"),
    [
        ("low_value", "no_current_need", "closed"),
        ("converted", None, "completed"),
    ],
)
async def test_manual_classification_is_atomic_idempotent_and_versioned(
    session_factory,
    classification: str,
    low_value_reason: str | None,
    expected_task_status: str,
) -> None:
    await _seed_follow_up_data(session_factory)
    payload = FollowUpDataClassificationIn(
        classification=classification,
        reason="坐席确认本次分类结果。",
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
        task = await db.scalar(select(AiCallFollowUpTaskModel))
        assert task is not None
        assert task.status == expected_task_status
        assert task.customer_callback_at is None
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
