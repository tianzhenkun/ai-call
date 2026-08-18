from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.outbound.exception_service import (
    EXCEPTION_DEFAULTS,
    OutboundExceptionService,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
    AiCallOutboundExceptionPolicyModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.utils.id_util import generate_snowflake_id

pytestmark = pytest.mark.anyio


def _dsn() -> str:
    value = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return value


async def test_concurrent_same_category_starts_only_one_batch() -> None:
    engine = create_async_engine(_dsn(), isolation_level="READ COMMITTED")
    tables = [
        AiCallOutboundTaskModel.__table__,
        AiCallOutboundTargetModel.__table__,
        AiCallOutboundAttemptModel.__table__,
        AiCallOutboundExceptionPolicyModel.__table__,
        AiCallOutboundExceptionBatchModel.__table__,
    ]
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all, tables=tables)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    task_id = generate_snowflake_id()
    target_id = generate_snowflake_id()

    try:
        async with factory.begin() as session:
            session.add(
                AiCallOutboundTaskModel(
                    id=task_id,
                    tenant_id="tenant-a",
                    validation_id=generate_snowflake_id(),
                    idempotency_key="postgres-exception-task",
                    request_fingerprint="a" * 64,
                    task_name="异常补呼并发测试",
                    task_mode="single",
                    status="COMPLETED",
                    total_targets=1,
                    completed_targets=1,
                    connected_targets=0,
                    failed_targets=1,
                    execution_mode="immediate",
                    prompt_profile_id="prompt-1",
                    prompt_name="测试提示词",
                    scene_code="intro_contract",
                    voice="Tina",
                    voice_name="Tina",
                    rule_id=generate_snowflake_id(),
                    rule_name="测试规则",
                    rule_summary="测试规则摘要",
                    config_snapshot_json="{}",
                    created_by=1,
                    created_by_name="测试用户",
                    created_at=now,
                    updated_at=now,
                    ended_at=now,
                )
            )
            session.add(
                AiCallOutboundTargetModel(
                    id=target_id,
                    tenant_id="tenant-a",
                    task_id=task_id,
                    validation_id=generate_snowflake_id(),
                    source_validation_row_id=generate_snowflake_id(),
                    source_row_number=1,
                    phone_number="13800138001",
                    customer_name="并发测试客户",
                    status="COMPLETED",
                    attempt_count=1,
                    latest_result="busy",
                    exception_category="no_answer",
                    exception_source_result="busy",
                    exception_original_attempt_count=1,
                    exception_entered_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            for category, (interval_days, max_retry_count) in EXCEPTION_DEFAULTS.items():
                session.add(
                    AiCallOutboundExceptionPolicyModel(
                        id=generate_snowflake_id(),
                        tenant_id="tenant-a",
                        category=category,
                        interval_days=interval_days,
                        max_retry_count=max_retry_count,
                        created_by=1,
                        updated_by=1,
                        created_at=now,
                        updated_at=now,
                    )
                )

        async def start_once(key: str):
            async with factory() as session:
                try:
                    result = await OutboundExceptionService().start_batch(
                        session,
                        "tenant-a",
                        1,
                        "no_answer",
                        key,
                    )
                    await session.commit()
                    return result
                except CustomException as error:
                    await session.rollback()
                    return error

        first, second = await asyncio.gather(
            start_once("concurrent-batch-a"),
            start_once("concurrent-batch-b"),
        )
        results = (first, second)
        assert len([item for item in results if not isinstance(item, Exception)]) == 1
        conflicts = [item for item in results if isinstance(item, CustomException)]
        assert len(conflicts) == 1
        assert conflicts[0].status_code == 409

        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count(AiCallOutboundExceptionBatchModel.id))
                )
                == 1
            )
            target = await session.get(AiCallOutboundTargetModel, target_id)
            assert target is not None
            assert target.exception_batch_id is not None
            assert target.status == "RETRY_WAIT"
    finally:
        await engine.dispose()
