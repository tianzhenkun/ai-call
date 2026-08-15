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


def requires_manual_follow_up_review(
    analysis: AiCallSemanticAnalysisModel,
) -> bool:
    return (
        analysis.analysis_status == "2"
        and analysis.follow_up_review_status is None
        and analysis.follow_up_consent != "refused"
        and not (
            analysis.follow_up_suggested
            and analysis.follow_up_consent == "explicit"
            and analysis.follow_up_confidence == "high"
        )
    )


class AiCallPostCallFollowUpService:
    """为正式外呼创建结构化 AI 话后跟进任务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository

    async def apply(
        self,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpTaskModel | None:
        if analysis.analysis_status != "2":
            return None
        if analysis.follow_up_review_status is not None:
            return None
        decision = decide_post_call_follow_up(analysis)
        if decision.action != "create":
            return None

        context = await self._formal_call_context(analysis)
        if context is None:
            return None
        record, attempt = context
        return await self._create_follow_up(
            analysis=analysis,
            record=record,
            attempt=attempt,
            decision=decision,
            fallback_reason="客户明确要求后续联系",
        )

    async def review(
        self,
        analysis: AiCallSemanticAnalysisModel,
        *,
        action: str,
        reviewed_by: str,
        reviewed_by_name: str | None,
    ) -> AiCallFollowUpTaskModel | None:
        if action not in {"create", "dismiss"}:
            raise ValueError("不支持的人工跟进处理方式")

        analysis = await self.repository.get_semantic_analysis_for_update(
            call_id=analysis.call_id
        )
        if analysis is None:
            raise ValueError("当前通话不存在待人工确认的跟进建议")

        context = await self._formal_call_context(analysis)
        if context is None:
            raise ValueError("当前通话不支持人工确认跟进")
        record, attempt = context

        if analysis.follow_up_review_status == "dismissed":
            if action == "dismiss":
                return None
            raise ValueError("该通话已确认无需跟进")
        if analysis.follow_up_review_status == "created":
            if action == "create":
                return await self.repository.get_follow_up_by_source(
                    tenant_id=attempt.tenant_id,
                    source_type="ai_post_call",
                    source_key=f"call:{analysis.call_id}",
                )
            raise ValueError("该通话已创建跟进任务")

        if analysis.analysis_status != "2":
            raise ValueError("通话分析尚未完成，不能确认跟进")
        if not requires_manual_follow_up_review(analysis):
            raise ValueError("当前通话不存在待人工确认的跟进建议")
        decision = decide_post_call_follow_up(analysis)

        now = datetime.now(timezone.utc)
        if action == "dismiss":
            analysis.follow_up_review_status = "dismissed"
            analysis.follow_up_reviewed_by = reviewed_by
            analysis.follow_up_reviewed_by_name = reviewed_by_name
            analysis.follow_up_reviewed_at = now
            analysis.updated_at = now
            await self.repository.db.flush()
            return None

        follow_up = await self._create_follow_up(
            analysis=analysis,
            record=record,
            attempt=attempt,
            decision=decision,
            fallback_reason="人工确认需要跟进",
        )
        analysis.follow_up_review_status = "created"
        analysis.follow_up_reviewed_by = reviewed_by
        analysis.follow_up_reviewed_by_name = reviewed_by_name
        analysis.follow_up_reviewed_at = now
        analysis.updated_at = now
        await self.repository.db.flush()
        return follow_up

    async def _formal_call_context(
        self,
        analysis: AiCallSemanticAnalysisModel,
    ) -> tuple[object, object] | None:
        record = await self.repository.get_record(analysis.call_id)
        if record is None or record.entry_type not in {
            "sip_outbound",
            "outbound",
            "web",
        }:
            return None
        attempt = await self.repository.get_outbound_attempt_by_call_id(
            analysis.call_id
        )
        if attempt is None or attempt.dialer_type not in {"sip", "owner_runtime"}:
            return None
        if await self.repository.list_handoffs(analysis.call_id):
            return None
        return record, attempt

    async def _create_follow_up(
        self,
        *,
        analysis: AiCallSemanticAnalysisModel,
        record: object,
        attempt: object,
        decision: PostCallFollowUpDecision,
        fallback_reason: str,
    ) -> AiCallFollowUpTaskModel:
        now = datetime.now(timezone.utc)
        result = analysis.analysis_result_dict or {}
        follow_up_reason = decision.reason or fallback_reason
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
            "masked_contact": (
                record.callee_phone_number_masked
                or ("Web 浏览器" if record.entry_type == "web" else "未提供")
            ),
            "owner_agent_identity": None,
            "status": "pending",
            "follow_up_reason": follow_up_reason,
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
            follow_up.follow_up_reason = follow_up_reason
            follow_up.customer_callback_at = decision.customer_callback_at
            follow_up.summary = result.get("summary")
            follow_up.updated_at = now
            await self.repository.db.flush()
        return follow_up
