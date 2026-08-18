from __future__ import annotations

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel
from app.services.ai_call.follow_up_data_service import AiCallFollowUpDataService


class AiCallHandoffUnansweredService:
    """幂等记录转人工未接通的客户分类。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def ensure_for_handoff(
        self,
        handoff: AiCallHandoffModel,
        *,
        reason: str,
    ) -> None:
        await AiCallFollowUpDataService(self.repository).apply_transfer_failure(
            handoff,
            reason=reason,
        )
