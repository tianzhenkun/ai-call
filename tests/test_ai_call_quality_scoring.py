from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Text, UniqueConstraint
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.crud import (
    QUALITY_SCORE_STATUS_COMPLETED,
    QUALITY_SCORE_STATUS_PENDING,
    AiCallRecordRepository,
)
from app.api.v1.ai_call.model import (
    AiCallQualityReviewModel,
    AiCallQualityScoreModel,
)
from app.api.v1.ai_call.service import AiCallService
from app.core.base_model import MappedBase
from app.services.ai_call.quality_scoring import (
    AiCallQualityScoringService,
    AiCallQualityScoringWorker,
)
from app.services.ai_call.record_service import AiCallRecordService


@pytest.fixture
async def session_maker(tmp_path) -> AsyncIterator[async_sessionmaker]:
    db_path = tmp_path / "quality_scoring.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(MappedBase.metadata.drop_all)
        await engine.dispose()


class FakeQualityScorer:
    async def score(self, *, transcript_snapshot: dict):
        turns = transcript_snapshot.get("turns") or []
        assert any(turn.get("speaker_type") == "customer" for turn in turns)
        return {"score": 86, "reason": "客户问题回应完整，转人工时机合理。"}


def test_quality_models_use_portable_scalar_columns() -> None:
    score_columns = {
        column.key: column for column in sa_inspect(AiCallQualityScoreModel).columns
    }
    assert score_columns["tenant_id"].type.length == 20
    assert score_columns["call_id"].type.length == 64
    assert score_columns["status"].type.length == 16
    assert score_columns["score"].nullable is True
    assert score_columns["reason"].nullable is True
    assert isinstance(score_columns["reason"].type, Text)
    assert score_columns["model_version"].server_default is not None
    assert score_columns["error_message"].type.length == 500
    assert not sa_inspect(AiCallQualityScoreModel).relationships

    review_columns = {
        column.key: column for column in sa_inspect(AiCallQualityReviewModel).columns
    }
    assert review_columns["quality_result"].type.length == 16
    assert review_columns["quality_reason"].type.length == 500
    assert review_columns["reviewed_by"].type.length == 64
    assert not sa_inspect(AiCallQualityReviewModel).relationships

    score_uniques = {
        frozenset(column.name for column in constraint.columns)
        for constraint in AiCallQualityScoreModel.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert frozenset({"tenant_id", "call_id", "model_version"}) in score_uniques


@pytest.mark.anyio
async def test_quality_scoring_waits_for_recording_and_dialogue(session_maker) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await _create_completed_record(repository, call_id="call-quality-wait")
        score = await repository.ensure_quality_score(
            tenant_id="000000",
            call_id="call-quality-wait",
            model_version="quality-v1",
        )
        service = AiCallQualityScoringService(repository, scorer=FakeQualityScorer())

        result = await service.score_call_once(
            tenant_id="000000",
            call_id="call-quality-wait",
            model_version="quality-v1",
        )

        assert result.id == score.id
        assert result.status == QUALITY_SCORE_STATUS_PENDING


@pytest.mark.anyio
async def test_quality_scoring_scores_after_recording_and_dialogue_ready(
    session_maker,
) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await _create_completed_record(repository, call_id="call-quality-ready")
        now = datetime.now(timezone.utc)
        await repository.create_recording(
            tenant_id="000000",
            call_id="call-quality-ready",
            room_name="room-call-quality-ready",
            status="completed",
            started_at=now,
            object_name="recov/ai-call/recordings/call-quality-ready.ogg",
        )
        await repository.upsert_dialogue_segment(
            call_id="call-quality-ready",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="sip-customer",
            source="offline_asr",
            source_segment_id="offline-1",
            segment_text="我想了解一下你们这个服务怎么收费。",
            segment_status="final",
            started_at=now,
            ended_at=now,
            duration_ms=1000,
        )
        service = AiCallQualityScoringService(repository, scorer=FakeQualityScorer())

        result = await service.score_call_once(
            tenant_id="000000",
            call_id="call-quality-ready",
            model_version="quality-v1",
        )

        assert result.status == QUALITY_SCORE_STATUS_COMPLETED
        assert result.score == 86
    assert result.reason == "客户问题回应完整，转人工时机合理。"


@pytest.mark.anyio
async def test_quality_scoring_recovers_stale_processing_after_restart(session_maker) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await _create_completed_record(repository, call_id="call-quality-recovery")
        now = datetime.now(timezone.utc)
        await repository.create_recording(
            tenant_id="000000",
            call_id="call-quality-recovery",
            room_name="room-call-quality-recovery",
            status="completed",
            started_at=now,
            object_name="recov/ai-call/recordings/call-quality-recovery.ogg",
        )
        await repository.upsert_dialogue_segment(
            call_id="call-quality-recovery",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="sip-customer",
            source="offline_asr",
            source_segment_id="offline-recovery-1",
            segment_text="我想了解一下你们这个服务怎么收费。",
            segment_status="final",
            started_at=now,
            ended_at=now,
            duration_ms=1000,
        )
        score = await repository.ensure_quality_score(
            tenant_id="000000",
            call_id="call-quality-recovery",
        )
        score.status = "processing"
        score.started_at = now - timedelta(days=366)
        score.updated_at = score.started_at
        await db.commit()

    worker = AiCallQualityScoringWorker(session_maker, scorer=FakeQualityScorer())
    await worker.recover_pending()
    assert await worker.process_one() is True

    async with session_maker() as db:
        result = await AiCallRecordRepository(db).get_quality_score(
            tenant_id="000000",
            call_id="call-quality-recovery",
        )
        assert result is not None
        assert result.status == QUALITY_SCORE_STATUS_COMPLETED


@pytest.mark.anyio
async def test_service_scores_record_quality_once(session_maker) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await _create_completed_record(repository, call_id="call-quality-manual")
        now = datetime.now(timezone.utc)
        await repository.create_recording(
            tenant_id="000000",
            call_id="call-quality-manual",
            room_name="room-call-quality-manual",
            status="completed",
            started_at=now,
            object_name="recov/ai-call/recordings/call-quality-manual.ogg",
        )
        await repository.upsert_dialogue_segment(
            call_id="call-quality-manual",
            segment_no=1,
            speaker_type="customer",
            speaker_identity="sip-customer",
            source="offline_asr",
            source_segment_id="offline-manual-1",
            segment_text="我想了解一下你们这个服务怎么收费。",
            segment_status="final",
            started_at=now,
            ended_at=now,
            duration_ms=1000,
        )
        service = AiCallService(
            object(),
            record_service=AiCallRecordService(repository),
            quality_scorer=FakeQualityScorer(),
        )

        detail = await service.score_record_quality(
            tenant_id="000000",
            call_id="call-quality-manual",
        )

        assert detail["score"]["status"] == QUALITY_SCORE_STATUS_COMPLETED
        assert detail["score"]["score"] == 86
        assert detail["score"]["reason"] == "客户问题回应完整，转人工时机合理。"
        assert detail["review"] is None


@pytest.mark.anyio
async def test_quality_review_requires_reason_when_failed(session_maker) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)

        with pytest.raises(ValueError, match="不合格原因不能为空"):
            await repository.upsert_quality_review(
                tenant_id="000000",
                call_id="call-review-1",
                quality_result="fail",
                quality_reason=" ",
                reviewed_by="1",
                reviewed_by_name="管理员",
            )


@pytest.mark.anyio
async def test_service_reads_and_saves_quality_review(session_maker) -> None:
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        await _create_completed_record(repository, call_id="call-quality-review")
        now = datetime.now(timezone.utc)
        db.add(
            AiCallQualityScoreModel(
                id=1301,
                tenant_id="000000",
                call_id="call-quality-review",
                status=QUALITY_SCORE_STATUS_COMPLETED,
                score=92,
                reason="命中关键问题，解释清晰。",
                model_version="quality-v1",
                retry_count=0,
                started_at=now,
                finished_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        await db.commit()
        service = AiCallService(
            object(),
            record_service=AiCallRecordService(repository),
        )

        review = await service.save_record_quality_review(
            tenant_id="000000",
            call_id="call-quality-review",
            quality_result="excellent",
            quality_reason=None,
            reviewed_by="1",
            reviewed_by_name="管理员",
        )
        detail = await service.get_record_quality(
            tenant_id="000000",
            call_id="call-quality-review",
        )

        assert review["qualityResult"] == "excellent"
        assert detail["score"]["score"] == 92
        assert detail["score"]["reason"] == "命中关键问题，解释清晰。"
        assert detail["review"]["qualityResult"] == "excellent"


async def _create_completed_record(
    repository: AiCallRecordRepository,
    *,
    call_id: str,
) -> None:
    await repository.create_record(
        tenant_id="000000",
        call_id=call_id,
        business_type=None,
        business_id=None,
        scene_code="intro_geo",
        entry_type="outbound",
        room_name=f"room-{call_id}",
        participant_identity="sip-13800138000",
        status="completed",
        started_at=datetime.now(timezone.utc),
    )
