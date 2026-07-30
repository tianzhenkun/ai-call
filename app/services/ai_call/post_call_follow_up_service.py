"""AI Call 结构化话后跟进决策。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallFollowUpTaskModel,
    AiCallSemanticAnalysisModel,
)
from app.utils.id_util import generate_snowflake_id


class FollowUpAnalysis(Protocol):
    follow_up_suggested: bool
    follow_up_consent: str | None
    follow_up_confidence: str | None
    follow_up_reason: str | None
    follow_up_preferred_at: datetime | None


@dataclass(frozen=True, slots=True)
class PostCallFollowUpDecision:
    action: str
    reason: str | None
    customer_callback_at: datetime | None


def decide_post_call_follow_up(
    analysis: FollowUpAnalysis,
) -> PostCallFollowUpDecision:
    if analysis.follow_up_consent == "refused":
        return PostCallFollowUpDecision("none", None, None)
    if (
        analysis.follow_up_suggested
        and analysis.follow_up_consent == "explicit"
        and analysis.follow_up_confidence == "high"
    ):
        return PostCallFollowUpDecision(
            "create",
            analysis.follow_up_reason or "客户明确要求后续联系",
            analysis.follow_up_preferred_at,
        )
    if analysis.follow_up_suggested:
        return PostCallFollowUpDecision(
            "suggest",
            analysis.follow_up_reason,
            analysis.follow_up_preferred_at,
        )
    return PostCallFollowUpDecision("none", None, None)


class AiCallPostCallFollowUpService:
    """只为正式 SIP 外呼创建结构化 AI 话后跟进任务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def apply(
        self,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpTaskModel | None:
        if analysis.analysis_status != "2":
            return None
        decision = decide_post_call_follow_up(analysis)
        if decision.action != "create":
            return None

        record = await self.repository.get_record(analysis.call_id)
        if record is None or record.entry_type != "sip_outbound":
            return None
        attempt = await self.repository.get_outbound_attempt_by_call_id(
            analysis.call_id
        )
        if attempt is None or attempt.dialer_type != "sip":
            return None

        now = datetime.now(timezone.utc)
        result = analysis.analysis_result_dict or {}
        follow_up = await self.repository.create_follow_up_if_missing({
            "id": generate_snowflake_id(),
            "tenant_id": attempt.tenant_id,
            "source_type": "ai_post_call",
            "source_key": f"call:{analysis.call_id}",
            "source_call_id": analysis.call_id,
            "source_handoff_id": None,
            "scene_code": record.scene_code or analysis.scene_code or "default",
            "business_type": record.business_type,
            "business_id": record.business_id,
            "contact_ref": f"call:{analysis.call_id}",
            "masked_contact": record.callee_phone_number_masked or "未提供",
            "owner_agent_identity": None,
            "status": "pending",
            "follow_up_reason": decision.reason or "客户明确要求后续联系",
            "customer_callback_at": decision.customer_callback_at,
            "summary": result.get("summary"),
            "closed_reason": None,
            "closed_remark": None,
            "completed_at": None,
            "closed_at": None,
            "created_at": now,
            "updated_at": now,
        })
        if follow_up.status == "pending":
            follow_up.follow_up_reason = (
                decision.reason or "客户明确要求后续联系"
            )
            follow_up.customer_callback_at = decision.customer_callback_at
            follow_up.summary = result.get("summary")
            follow_up.updated_at = now
            await self.repository.db.flush()
        return follow_up
