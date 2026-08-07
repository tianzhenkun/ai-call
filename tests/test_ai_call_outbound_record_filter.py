from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallFollowUpTaskModel,
    AiCallQualityReviewModel,
    AiCallQualityScoreModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.config.setting import settings
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user, redis_getter
from app.services.ai_call.record_service import AiCallRecordService


class _ListRecordsService:
    def __init__(self) -> None:
        self.query: dict | None = None

    async def list_records(self, **query) -> dict:
        self.query = query
        return {"rows": [], "total": 0}


@pytest.mark.anyio
async def test_standalone_auth_dependency_does_not_require_redis(monkeypatch) -> None:
    monkeypatch.setattr(settings, "AI_CALL_STANDALONE_ENABLE", True)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    assert await redis_getter(request) is None


def test_record_list_controller_forwards_outbound_filters() -> None:
    service = _ListRecordsService()
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: service
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )

    response = TestClient(app).get(
        "/ai-call/records",
        params={
            "taskId": "101",
            "targetId": "201",
            "phoneNumber": "13800138011",
            "customerName": "客户甲",
            "callResult": "no_answer",
            "customerIntent": "positive",
            "followUpStatus": "suggested",
            "formalOutboundOnly": "true",
        },
    )

    assert response.status_code == 200
    assert service.query == {
        "tenant_id": "tenant-a",
        "call_id": None,
        "task_id": 101,
        "target_id": 201,
        "phone_number": "13800138011",
        "customer_name": "客户甲",
        "call_result": "no_answer",
        "customer_intent": "positive",
        "follow_up_status": "suggested",
        "business_type": None,
        "business_id": None,
        "status": None,
        "entry_type": None,
        "formal_outbound_only": True,
        "started_at_begin": None,
        "started_at_end": None,
        "page_num": 1,
        "page_size": 10,
    }


def test_record_list_controller_rejects_unknown_post_call_filters() -> None:
    service = _ListRecordsService()
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: service
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )

    response = TestClient(app).get(
        "/ai-call/records",
        params={"customerIntent": "very_positive"},
    )

    assert response.status_code == 422
    assert service.query is None


def test_outbound_attempt_model_has_no_physical_foreign_keys() -> None:
    attempt_table = MappedBase.metadata.tables.get("ai_call_outbound_attempt")
    assert attempt_table is not None
    assert {
        "tenant_id",
        "task_id",
        "target_id",
        "attempt_no",
        "call_id",
        "status",
        "call_result",
        "error_message",
    } <= {column.name for column in attempt_table.columns}
    assert not attempt_table.foreign_keys
    unique_column_sets = {
        frozenset(column.name for column in constraint.columns)
        for constraint in attempt_table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert frozenset({"call_id"}) in unique_column_sets


@pytest.mark.anyio
async def test_record_list_filters_and_enriches_outbound_attempt_context() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)

    async with session_maker() as session:
        task = AiCallOutboundTaskModel(
            id=101,
            tenant_id="000000",
            validation_id=1,
            idempotency_key="test-task-101",
            request_fingerprint="test-fingerprint",
            task_name="真实批量外呼浏览器验收",
            task_mode="batch",
            status="RUNNING",
            total_targets=1,
            completed_targets=0,
            connected_targets=0,
            failed_targets=0,
            execution_mode="immediate",
            scheduled_at=None,
            started_at=now,
            ended_at=None,
            prompt_profile_id=None,
            prompt_name="GEO 产品介绍",
            scene_code="intro_geo",
            voice="Tina",
            voice_name="甜甜 Tina",
            rule_id=1,
            rule_name="工作日规则",
            rule_summary="09:00–12:00",
            config_snapshot_json="{}",
            error_message=None,
            created_by=1,
            created_by_name="管理员",
            created_at=now,
            updated_at=now,
        )
        target = AiCallOutboundTargetModel(
            id=201,
            tenant_id="000000",
            task_id=101,
            validation_id=1,
            source_validation_row_id=1,
            source_row_number=2,
            phone_number="13800138011",
            customer_name="客户甲",
            status="RETRY_WAIT",
            attempt_count=1,
            latest_result="no_answer",
            created_at=now,
            updated_at=now,
        )
        web_target = AiCallOutboundTargetModel(
            id=202,
            tenant_id="000000",
            task_id=101,
            validation_id=2,
            source_validation_row_id=2,
            source_row_number=3,
            phone_number="13800138012",
            customer_name="客户乙",
            status="COMPLETED",
            attempt_count=1,
            latest_result="connected",
            created_at=now,
            updated_at=now,
        )
        record = AiCallRecordModel(
            id=301,
            call_id="call-outbound-1",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="outbound",
            room_name="room-outbound-1",
            participant_identity="sip-13800138011",
            status="completed",
            started_at=now,
        )
        unrelated = AiCallRecordModel(
            id=302,
            call_id="call-web-1",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="web",
            room_name="room-web-1",
            participant_identity="browser-user",
            status="completed",
            started_at=now,
        )
        generic_web = AiCallRecordModel(
            id=304,
            call_id="call-generic-web-1",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="web",
            room_name="room-generic-web-1",
            participant_identity="browser-lab",
            status="completed",
            started_at=now,
        )
        manual_sip_record = AiCallRecordModel(
            id=303,
            call_id="call-manual-sip-1",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="sip_outbound",
            room_name="room-manual-sip-1",
            participant_identity="sip-manual",
            status="completed",
            started_at=now,
        )
        attempt_table = MappedBase.metadata.tables["ai_call_outbound_attempt"]
        session.add_all(
            [
                task,
                target,
                web_target,
                record,
                unrelated,
                generic_web,
                manual_sip_record,
            ]
        )
        await session.flush()
        await session.execute(
            attempt_table.insert().values(
                id=401,
                tenant_id="000000",
                task_id=101,
                target_id=201,
                attempt_no=1,
                call_id="call-outbound-1",
                status="COMPLETED",
                call_result="no_answer",
                error_message=None,
                started_at=now,
                ended_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        await session.execute(
            attempt_table.insert().values(
                id=402,
                tenant_id="000000",
                task_id=101,
                target_id=202,
                attempt_no=1,
                call_id="call-web-1",
                status="COMPLETED",
                call_result="connected",
                error_message=None,
                started_at=now,
                ended_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()

        service = AiCallRecordService(AiCallRecordRepository(session))
        rows, total = await service.list_records(
            tenant_id="000000",
            task_id=101,
            target_id=201,
            phone_number="13800138011",
            customer_name="客户甲",
            call_result="no_answer",
        )

        assert total == 1
        assert [row.call_id for row in rows] == ["call-outbound-1"]
        payload = service.record_to_dict(rows[0])
        assert payload["taskId"] == "101"
        assert payload["targetId"] == "201"
        assert payload["taskName"] == "真实批量外呼浏览器验收"
        assert payload["customerName"] == "客户甲"
        assert payload["phoneNumber"] == "13800138011"
        assert payload["attemptNo"] == 1
        assert payload["callResult"] == "no_answer"

        formal_rows, formal_total = await service.list_records(
            tenant_id="000000",
            formal_outbound_only=True,
        )
        assert formal_total == 2
        assert {row.call_id for row in formal_rows} == {
            "call-outbound-1",
            "call-web-1",
        }

        empty_rows, empty_total = await service.list_records(
            tenant_id="000000",
            task_id=999,
        )
        assert empty_rows == []
        assert empty_total == 0

    await engine.dispose()


@pytest.mark.anyio
async def test_record_list_is_tenant_scoped_and_rejects_inconsistent_target_task() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)

    def task(task_id: int, tenant_id: str) -> AiCallOutboundTaskModel:
        return AiCallOutboundTaskModel(
            id=task_id,
            tenant_id=tenant_id,
            validation_id=task_id,
            idempotency_key=f"task-{task_id}",
            request_fingerprint=f"fingerprint-{task_id}",
            task_name=f"任务{task_id}",
            task_mode="batch",
            status="RUNNING",
            total_targets=1,
            completed_targets=0,
            connected_targets=0,
            failed_targets=0,
            execution_mode="immediate",
            scheduled_at=None,
            started_at=now,
            ended_at=None,
            prompt_profile_id=None,
            prompt_name="提示词",
            scene_code="intro_geo",
            voice="Tina",
            voice_name="甜甜 Tina",
            rule_id=1,
            rule_name="工作日规则",
            rule_summary="09:00–12:00",
            config_snapshot_json="{}",
            error_message=None,
            created_by=1,
            created_by_name="管理员",
            created_at=now,
            updated_at=now,
        )

    def target(
        target_id: int,
        tenant_id: str,
        task_id: int,
        phone_number: str,
    ) -> AiCallOutboundTargetModel:
        return AiCallOutboundTargetModel(
            id=target_id,
            tenant_id=tenant_id,
            task_id=task_id,
            validation_id=task_id,
            source_validation_row_id=target_id,
            source_row_number=2,
            phone_number=phone_number,
            customer_name=f"客户{target_id}",
            status="PENDING",
            attempt_count=0,
            latest_result=None,
            created_at=now,
            updated_at=now,
        )

    def record(record_id: int, call_id: str) -> AiCallRecordModel:
        return AiCallRecordModel(
            id=record_id,
            call_id=call_id,
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="sip_outbound",
            room_name=f"room-{record_id}",
            participant_identity=f"sip-{record_id}",
            status="completed",
            started_at=now,
        )

    async with session_maker() as session:
        session.add_all(
            [
                task(101, "tenant-a"),
                task(102, "tenant-b"),
                task(103, "tenant-a"),
                target(201, "tenant-a", 101, "13800138011"),
                target(202, "tenant-b", 102, "13900139012"),
                target(203, "tenant-a", 103, "13700137013"),
                record(301, "call-tenant-a"),
                record(302, "call-tenant-b"),
                record(303, "call-inconsistent"),
                record(304, "call-legacy-default-tenant"),
            ]
        )
        session.add_all(
            [
                AiCallOutboundAttemptModel(
                    id=401,
                    tenant_id="tenant-a",
                    task_id=101,
                    target_id=201,
                    attempt_no=1,
                    call_id="call-tenant-a",
                    status="COMPLETED",
                    call_result="connected",
                    error_message=None,
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=402,
                    tenant_id="tenant-b",
                    task_id=102,
                    target_id=202,
                    attempt_no=1,
                    call_id="call-tenant-b",
                    status="COMPLETED",
                    call_result="connected",
                    error_message=None,
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=403,
                    tenant_id="tenant-a",
                    task_id=101,
                    target_id=203,
                    attempt_no=1,
                    call_id="call-inconsistent",
                    status="COMPLETED",
                    call_result="connected",
                    error_message=None,
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
            ]
        )
        await session.commit()

        service = AiCallRecordService(AiCallRecordRepository(session))
        rows, total = await service.list_records(
            tenant_id="tenant-a",
            task_id=101,
        )
        assert total == 1
        assert [row.call_id for row in rows] == ["call-tenant-a"]

        blank_rows, blank_total = await service.list_records(
            tenant_id="tenant-a",
            phone_number="   ",
        )
        assert blank_total == 1
        assert [row.call_id for row in blank_rows] == ["call-tenant-a"]

        legacy_rows, legacy_total = await service.list_records(
            tenant_id="000000",
        )
        assert legacy_total == 1
        assert [row.call_id for row in legacy_rows] == ["call-legacy-default-tenant"]

    await engine.dispose()


@pytest.mark.anyio
async def test_record_list_aggregates_and_filters_post_call_statuses() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    call_cases = [
        ("call-positive-none", "positive", False, "2"),
        ("call-positive-suggested", "positive", True, "2"),
        ("call-pending-task", "neutral", True, "2"),
        ("call-completed-task", "negative", True, "2"),
        ("call-analysis-running", None, False, "1"),
        ("call-analysis-pending", None, False, "0"),
        ("call-analysis-failed", None, False, "3"),
        ("call-no-user-input", None, False, "4"),
    ]

    async with session_maker() as session:
        session.add(
            AiCallOutboundTaskModel(
                id=501,
                tenant_id="tenant-a",
                validation_id=501,
                idempotency_key="post-call-list",
                request_fingerprint="post-call-list-fingerprint",
                task_name="话后结果筛选",
                task_mode="batch",
                status="COMPLETED",
                total_targets=len(call_cases),
                completed_targets=len(call_cases),
                connected_targets=len(call_cases),
                failed_targets=0,
                execution_mode="immediate",
                prompt_name="GEO 产品介绍",
                scene_code="intro_geo",
                voice="Tina",
                rule_id=1,
                rule_name="工作日规则",
                rule_summary="09:00–12:00",
                config_snapshot_json="{}",
                created_by=1,
                created_at=now,
                updated_at=now,
            )
        )
        for offset, (
            call_id,
            customer_intent,
            follow_up_suggested,
            analysis_status,
        ) in enumerate(call_cases, start=1):
            target_id = 600 + offset
            session.add_all(
                [
                    AiCallOutboundTargetModel(
                        id=target_id,
                        tenant_id="tenant-a",
                        task_id=501,
                        validation_id=501,
                        source_validation_row_id=target_id,
                        source_row_number=offset + 1,
                        phone_number=f"19900001{offset:03d}",
                        customer_name=f"客户{offset}",
                        status="COMPLETED",
                        attempt_count=1,
                        latest_result="connected",
                        created_at=now,
                        updated_at=now,
                    ),
                    AiCallRecordModel(
                        id=700 + offset,
                        call_id=call_id,
                        business_type="outbound_task",
                        business_id="501",
                        scene_code="intro_geo",
                        entry_type="sip_outbound",
                        room_name=f"room-{call_id}",
                        participant_identity=f"sip-{offset}",
                        status="completed",
                        started_at=now,
                        ended_at=now,
                        follow_up_id=(
                            1002 if call_id == "call-completed-task" else None
                        ),
                    ),
                    AiCallOutboundAttemptModel(
                        id=800 + offset,
                        tenant_id="tenant-a",
                        task_id=501,
                        target_id=target_id,
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
                    AiCallSemanticAnalysisModel(
                        id=900 + offset,
                        call_id=call_id,
                        scene_code="intro_geo",
                        analysis_scene_code="ai_call_semantic_analysis",
                        analysis_status=analysis_status,
                        analysis_result='{"summary":"结构化摘要"}',
                        customer_intent=customer_intent,
                        follow_up_suggested=follow_up_suggested,
                        follow_up_consent=(
                            "explicit" if follow_up_suggested else "missing"
                        ),
                        follow_up_reason=(
                            "客户要求回访" if follow_up_suggested else None
                        ),
                        follow_up_confidence=(
                            "high" if follow_up_suggested else "low"
                        ),
                        analysis_retry_count=0,
                        created_at=now,
                        updated_at=now,
                    ),
                ]
            )
        session.add_all(
            [
                AiCallFollowUpTaskModel(
                    id=1001,
                    tenant_id="tenant-a",
                    source_type="ai_post_call",
                    source_key="call:call-pending-task",
                    source_call_id="call-pending-task",
                    source_handoff_id=None,
                    scene_code="intro_geo",
                    contact_ref="call:call-pending-task",
                    masked_contact="199****1003",
                    status="pending",
                    follow_up_reason="客户要求回访",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallFollowUpTaskModel(
                    id=1002,
                    tenant_id="tenant-a",
                    source_type="ai_post_call",
                    source_key="call:call-completed-task",
                    source_call_id="call-completed-task",
                    source_handoff_id=None,
                    scene_code="intro_geo",
                    contact_ref="call:call-completed-task",
                    masked_contact="199****1004",
                    status="completed",
                    follow_up_reason="客户要求回访",
                    completed_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallFollowUpTaskModel(
                    id=1003,
                    tenant_id="tenant-a",
                    source_type="ai_post_call",
                    source_key="call:call-completed-task:second",
                    source_call_id="call-completed-task",
                    source_handoff_id=None,
                    scene_code="intro_geo",
                    contact_ref="call:call-completed-task",
                    masked_contact="199****1004",
                    status="pending",
                    follow_up_reason="新一轮跟进建议",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallQualityScoreModel(
                    id=1101,
                    tenant_id="tenant-a",
                    call_id="call-pending-task",
                    status="completed",
                    score=86,
                    reason="客户问题回应完整，转人工时机合理。",
                    model_version="quality-v1",
                    retry_count=0,
                    started_at=now,
                    finished_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallQualityReviewModel(
                    id=1201,
                    tenant_id="tenant-a",
                    call_id="call-pending-task",
                    quality_result="fail",
                    quality_reason="关键问题未确认",
                    reviewed_by="1",
                    reviewed_by_name="管理员",
                    reviewed_at=now,
                    created_at=now,
                    updated_at=now,
                ),
            ]
        )
        await session.commit()

        service = AiCallRecordService(AiCallRecordRepository(session))
        rows, total = await service.list_records(
            tenant_id="tenant-a",
            page_size=10,
        )
        payloads = {
            row.call_id: service.record_to_dict(row)
            for row in rows
        }

        assert total == 8
        assert payloads["call-pending-task"] | {
            "analysisStatus": "2",
            "customerIntent": "neutral",
            "followUpSuggested": True,
            "followUpId": "1001",
            "followUpStatus": "pending",
            "qualityScoreStatus": "completed",
            "qualityScore": 86,
            "qualityReviewResult": "fail",
        } == payloads["call-pending-task"]
        assert payloads["call-positive-none"]["qualityScoreStatus"] == "pending"
        assert payloads["call-positive-none"]["qualityScore"] is None
        assert payloads["call-positive-none"]["qualityReviewResult"] is None
        assert payloads["call-completed-task"]["followUpStatus"] == "completed"
        assert payloads["call-analysis-running"]["analysisStatus"] == "1"

        positive_rows, positive_total = await service.list_records(
            tenant_id="tenant-a",
            customer_intent="positive",
        )
        suggested_rows, suggested_total = await service.list_records(
            tenant_id="tenant-a",
            follow_up_status="suggested",
        )
        pending_rows, pending_total = await service.list_records(
            tenant_id="tenant-a",
            follow_up_status="pending",
        )
        none_rows, none_total = await service.list_records(
            tenant_id="tenant-a",
            follow_up_status="none",
        )

        assert positive_total == 2
        assert {row.call_id for row in positive_rows} == {
            "call-positive-none",
            "call-positive-suggested",
        }
        assert suggested_total == 1
        assert [row.call_id for row in suggested_rows] == [
            "call-positive-suggested"
        ]
        assert pending_total == 1
        assert [row.call_id for row in pending_rows] == ["call-pending-task"]
        assert none_total == 1
        assert [row.call_id for row in none_rows] == ["call-positive-none"]

    await engine.dispose()


@pytest.mark.anyio
async def test_record_list_uses_exclusive_started_at_end() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    begin = datetime(2026, 7, 31, tzinfo=timezone.utc)
    end = begin + timedelta(days=1)

    async with session_maker() as session:
        session.add_all(
            [
                AiCallRecordModel(
                    id=9101,
                    call_id="call-at-begin",
                    entry_type="web",
                    room_name="room-at-begin",
                    participant_identity="browser-at-begin",
                    status="completed",
                    started_at=begin,
                ),
                AiCallRecordModel(
                    id=9102,
                    call_id="call-before-end",
                    entry_type="web",
                    room_name="room-before-end",
                    participant_identity="browser-before-end",
                    status="completed",
                    started_at=end - timedelta(microseconds=1),
                ),
                AiCallRecordModel(
                    id=9103,
                    call_id="call-at-end",
                    entry_type="web",
                    room_name="room-at-end",
                    participant_identity="browser-at-end",
                    status="completed",
                    started_at=end,
                ),
            ]
        )
        await session.commit()

        rows, total = await AiCallRecordRepository(session).list_records(
            started_at_begin=begin,
            started_at_end=end,
            page_size=10,
        )

    await engine.dispose()

    assert total == 2
    assert {row.call_id for row in rows} == {
        "call-at-begin",
        "call-before-end",
    }
