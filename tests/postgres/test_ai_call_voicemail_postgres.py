from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallEventModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
)
from app.api.v1.ai_call.outbound.attempt_projection import (
    AttemptTerminalDecision,
    apply_terminal_projection,
    reconcile_analyzed_call,
    refresh_task_counters,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.core.base_model import MappedBase
from app.utils.id_util import generate_snowflake_id


@pytest.mark.anyio
async def test_late_analysis_waits_for_terminal_projection(monkeypatch):
    dsn = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    engine = create_async_engine(dsn, isolation_level="READ COMMITTED")
    tables = [
        model.__table__
        for model in (
            AiCallOutboundTaskModel,
            AiCallOutboundTargetModel,
            AiCallOutboundAttemptModel,
            AiCallRecordModel,
            AiCallSemanticAnalysisModel,
            AiCallEventModel,
        )
    ]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 9, 21, 8, 0, tzinfo=timezone.utc)
    task_id, target_id, attempt_id = (generate_snowflake_id() for _ in range(3))
    call_id = f"call-voicemail-race-{attempt_id}"
    context_read = asyncio.Event()
    terminal_committed = asyncio.Event()

    try:
        async with engine.begin() as connection:
            await connection.run_sync(MappedBase.metadata.create_all, tables=tables)
        async with factory.begin() as db:
            db.add(
                AiCallOutboundTaskModel(
                    id=task_id,
                    tenant_id="voicemail-race",
                    validation_id=task_id,
                    idempotency_key=call_id,
                    request_fingerprint="a" * 64,
                    task_name="信箱并发结算",
                    task_mode="single",
                    status="RUNNING",
                    total_targets=1,
                    completed_targets=0,
                    connected_targets=0,
                    failed_targets=0,
                    execution_mode="immediate",
                    prompt_profile_id="prompt-1",
                    prompt_name="测试",
                    scene_code="intro_geo",
                    voice="Tina",
                    voice_name="Tina",
                    rule_id=task_id,
                    rule_name="测试",
                    rule_summary="测试",
                    config_snapshot_json=json.dumps({
                        "rule": {
                            "retryCount": 1,
                            "retryIntervalsMinutes": [30],
                            "retryableResults": ["no_answer"],
                        }
                    }),
                    created_by=1,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                AiCallOutboundTargetModel(
                    id=target_id,
                    tenant_id="voicemail-race",
                    task_id=task_id,
                    validation_id=task_id,
                    source_validation_row_id=target_id,
                    source_row_number=1,
                    phone_number="13800138001",
                    status="IN_CALL",
                    attempt_count=1,
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                AiCallOutboundAttemptModel(
                    id=attempt_id,
                    tenant_id="voicemail-race",
                    task_id=task_id,
                    target_id=target_id,
                    call_id=call_id,
                    attempt_no=1,
                    status="IN_CALL",
                    dialer_type="sip",
                    active_slot="sip:voicemail-race",
                    started_at=now - timedelta(seconds=10),
                    created_at=now,
                    updated_at=now,
                )
            )
            db.add(
                AiCallRecordModel(
                    id=generate_snowflake_id(),
                    tenant_id="voicemail-race",
                    call_id=call_id,
                    room_name=f"room-{attempt_id}",
                    participant_identity="sip-customer",
                    entry_type="sip_outbound",
                    status="completed",
                    started_at=now - timedelta(seconds=10),
                    answered_at=now - timedelta(seconds=10),
                    ended_at=now,
                    duration_ms=10_000,
                )
            )
            await AiCallRecordRepository(db).ensure_semantic_analysis_record(
                call_id=call_id, scene_code="intro_geo"
            )

        async def analyze():
            async with factory.begin() as db:
                await AiCallRecordRepository(db).update_semantic_analysis_success(
                    call_id=call_id,
                    analysis_result={"valid_dialogue": False, "tags": ["语音留言"]},
                    transcript_snapshot_json="{}",
                    transcript_hash="race",
                    now=now,
                )
                original_scalar = db.scalar

                async def observe_context(statement, *args, **kwargs):
                    result = await original_scalar(statement, *args, **kwargs)
                    if any(
                        item.get("entity") is AiCallOutboundAttemptModel
                        for item in statement.column_descriptions
                    ):
                        context_read.set()
                    return result

                monkeypatch.setattr(db, "scalar", observe_context)
                await reconcile_analyzed_call(db, call_id, now=now)
                # 保持分析未提交，复现结算事务看不到分析、分析先读到未结算 Attempt 的交错。
                await terminal_committed.wait()

        async with factory() as db:
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(AiCallOutboundTaskModel.id == task_id)
                .with_for_update()
            )
            analysis_task = asyncio.create_task(analyze())
            try:
                await asyncio.wait_for(context_read.wait(), timeout=5)
                target = await db.scalar(
                    select(AiCallOutboundTargetModel)
                    .where(AiCallOutboundTargetModel.id == target_id)
                    .with_for_update()
                )
                attempt = await db.scalar(
                    select(AiCallOutboundAttemptModel)
                    .where(AiCallOutboundAttemptModel.id == attempt_id)
                    .with_for_update()
                )
                record = await db.scalar(
                    select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
                )
                await apply_terminal_projection(
                    db,
                    task=task,
                    target=target,
                    attempt=attempt,
                    record=record,
                    decision=AttemptTerminalDecision("COMPLETED", "connected", None),
                    now=now,
                )
                await refresh_task_counters(db, task, now)
                await db.commit()
                terminal_committed.set()
                await asyncio.wait_for(analysis_task, timeout=5)
            finally:
                analysis_task.cancel()
                await asyncio.gather(analysis_task, return_exceptions=True)

        async with factory() as db:
            target = await db.get(AiCallOutboundTargetModel, target_id)
            attempt = await db.get(AiCallOutboundAttemptModel, attempt_id)
            task = await db.get(AiCallOutboundTaskModel, task_id)
            assert target.latest_result == "no_answer"
            assert target.status == "RETRY_WAIT"
            assert target.next_attempt_at == now + timedelta(minutes=30)
            assert attempt.call_result == "connected"
            assert task.connected_targets == 0
    finally:
        await engine.dispose()
