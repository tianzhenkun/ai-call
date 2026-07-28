from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call.record_service import AiCallRecordService


class _ListRecordsService:
    def __init__(self) -> None:
        self.query: dict | None = None

    async def list_records(self, **query) -> dict:
        self.query = query
        return {"rows": [], "total": 0}


def test_record_list_controller_forwards_outbound_filters() -> None:
    service = _ListRecordsService()
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: service

    response = TestClient(app).get(
        "/ai-call/records",
        params={
            "taskId": "101",
            "targetId": "201",
            "phoneNumber": "13800138011",
            "customerName": "客户甲",
            "callResult": "no_answer",
        },
    )

    assert response.status_code == 200
    assert service.query == {
        "call_id": None,
        "task_id": 101,
        "target_id": 201,
        "phone_number": "13800138011",
        "customer_name": "客户甲",
        "call_result": "no_answer",
        "business_type": None,
        "business_id": None,
        "status": None,
        "entry_type": None,
        "started_at_begin": None,
        "started_at_end": None,
        "page_num": 1,
        "page_size": 10,
    }


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
            tenant_id="tenant-a",
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
            tenant_id="tenant-a",
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
        record = AiCallRecordModel(
            id=301,
            call_id="call-outbound-1",
            business_type=None,
            business_id=None,
            scene_code="intro_geo",
            prompt_source_key=None,
            entry_type="sip_outbound",
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
        attempt_table = MappedBase.metadata.tables["ai_call_outbound_attempt"]
        session.add_all([task, target, record, unrelated])
        await session.flush()
        await session.execute(
            attempt_table.insert().values(
                id=401,
                tenant_id="tenant-a",
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
        await session.commit()

        service = AiCallRecordService(AiCallRecordRepository(session))
        rows, total = await service.list_records(
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

        empty_rows, empty_total = await service.list_records(task_id=999)
        assert empty_rows == []
        assert empty_total == 0

    await engine.dispose()
