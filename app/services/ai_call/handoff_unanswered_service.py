from __future__ import annotations

from datetime import datetime, timezone

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallHandoffModel
from app.utils.id_util import generate_snowflake_id


class AiCallHandoffUnansweredService:
    """幂等创建人工未接回访任务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def ensure_for_handoff(
        self,
        handoff: AiCallHandoffModel,
        *,
        reason: str,
    ) -> None:
        record = await self.repository.get_record(handoff.call_id)
        now = datetime.now(timezone.utc)
        await self.repository.create_unanswered_follow_up_if_missing(
            {
                "id": generate_snowflake_id(),
                "tenant_id": handoff.tenant_id,
                "source_type": "handoff_unanswered",
                "source_key": f"handoff:{handoff.handoff_id}",
                "source_call_id": handoff.call_id,
                "source_handoff_id": handoff.handoff_id,
                "scene_code": handoff.scene_code,
                "business_type": record.business_type if record is not None else None,
                "business_id": record.business_id if record is not None else None,
                "contact_ref": f"call:{handoff.call_id}",
                "masked_contact": (
                    record.callee_phone_number_masked
                    if record is not None and record.callee_phone_number_masked
                    else "未提供"
                ),
                "owner_agent_identity": None,
                "status": "pending",
                "follow_up_reason": reason,
                "customer_callback_at": None,
                "summary": None,
                "closed_reason": None,
                "closed_remark": None,
                "completed_at": None,
                "closed_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )
