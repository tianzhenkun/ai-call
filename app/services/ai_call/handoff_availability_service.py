from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import status
from sqlalchemy import and_, distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
    AiCallRecordModel,
)
from app.core.exceptions import CustomException


@dataclass(frozen=True, slots=True)
class HandoffAgentAvailability:
    online_agent_count: int
    available_agent_count: int


class AiCallHandoffAvailabilityService:
    """读取当前通话场景下的在线与空闲坐席快照。"""

    ONLINE_STATUSES = frozenset(
        {"available", "claiming", "in_call", "reconnecting", "wrap_up_quick"}
    )

    def __init__(
        self,
        db: AsyncSession,
        *,
        heartbeat_seconds: int = 30,
        tenant_id: str = "000000",
    ) -> None:
        self.db = db
        self.heartbeat_seconds = max(1, heartbeat_seconds)
        self.tenant_id = tenant_id

    async def get_for_call(self, call_id: str) -> HandoffAgentAvailability:
        record = await self.db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        if record is None:
            raise CustomException(
                msg="通话记录不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )

        scene_code = (record.scene_code or "default").strip() or "default"
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.heartbeat_seconds)
        common_conditions = (
            AiCallAgentProfileModel.tenant_id == self.tenant_id,
            AiCallAgentProfileModel.enabled.is_(True),
            AiCallAgentSceneScopeModel.scene_code == scene_code,
            AiCallHandoffAgentModel.last_seen_at.is_not(None),
            AiCallHandoffAgentModel.last_seen_at >= cutoff,
        )
        base_joins = (
            (
                AiCallAgentSceneScopeModel,
                and_(
                    AiCallAgentSceneScopeModel.tenant_id
                    == AiCallAgentProfileModel.tenant_id,
                    AiCallAgentSceneScopeModel.agent_identity
                    == AiCallAgentProfileModel.agent_identity,
                ),
            ),
            (
                AiCallHandoffAgentModel,
                and_(
                    AiCallHandoffAgentModel.tenant_id
                    == AiCallAgentProfileModel.tenant_id,
                    AiCallHandoffAgentModel.agent_identity
                    == AiCallAgentProfileModel.agent_identity,
                ),
            ),
        )

        online_query = select(
            func.count(distinct(AiCallAgentProfileModel.agent_identity))
        ).select_from(AiCallAgentProfileModel)
        available_query = select(
            func.count(distinct(AiCallAgentProfileModel.agent_identity))
        ).select_from(AiCallAgentProfileModel)
        for target, condition in base_joins:
            online_query = online_query.join(target, condition)
            available_query = available_query.join(target, condition)

        online_count = await self.db.scalar(
            online_query.where(
                *common_conditions,
                AiCallHandoffAgentModel.status.in_(self.ONLINE_STATUSES),
            )
        )
        available_count = await self.db.scalar(
            available_query.where(
                *common_conditions,
                AiCallHandoffAgentModel.status == "available",
                AiCallHandoffAgentModel.active_handoff_id.is_(None),
            )
        )
        return HandoffAgentAvailability(
            online_agent_count=int(online_count or 0),
            available_agent_count=int(available_count or 0),
        )
