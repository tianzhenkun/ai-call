from __future__ import annotations

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallEventModel,
    AiCallRecordingTrackModel,
)

REQUIRED_RECORDING_TRACK_ROLES = {"ai", "customer"}


async def has_persisted_media_evidence(
    db: AsyncSession,
    call_id: str,
) -> bool:
    """优先使用实时媒体事件，缺失时以完整双向分轨作为持久化兜底。"""
    if await db.scalar(
        select(
            exists().where(
                AiCallEventModel.call_id == call_id,
                AiCallEventModel.event_type == "media_connected",
            )
        )
    ):
        return True

    completed_roles = set(
        (
            await db.scalars(
                select(AiCallRecordingTrackModel.track_role).where(
                    AiCallRecordingTrackModel.call_id == call_id,
                    AiCallRecordingTrackModel.track_role.in_(
                        REQUIRED_RECORDING_TRACK_ROLES
                    ),
                    AiCallRecordingTrackModel.status == "completed",
                    AiCallRecordingTrackModel.oss_id.is_not(None),
                    AiCallRecordingTrackModel.object_name.is_not(None),
                    AiCallRecordingTrackModel.duration_ms > 0,
                )
            )
        ).all()
    )
    return completed_roles == REQUIRED_RECORDING_TRACK_ROLES
