from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallFollowUpClassificationHistoryModel,
    AiCallFollowUpDataModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.utils.id_util import generate_snowflake_id

AI_CLASSIFICATIONS = {"interested", "nurturing", "low_value"}
PROTECTED_CLASSIFICATION_SOURCES = {"human", "system"}


class AiCallFollowUpDataService:
    """把正式外呼的 AI 分类写入独立跟进数据，不创建回访任务。"""

    def __init__(self, repository: AiCallRecordRepository) -> None:
        self.repository = repository
        self.db = repository.db

    async def apply_ai_analysis(
        self,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpDataModel | None:
        if analysis.analysis_status != "2" or analysis.analysis_version < 1:
            return None
        context = await self._formal_call_context(analysis.call_id)
        if context is None:
            return None
        record, attempt = context
        data = await self._get_for_update(
            tenant_id=attempt.tenant_id,
            task_id=attempt.task_id,
            target_id=attempt.target_id,
        )
        result = analysis.analysis_result_dict or {}
        classification = result.get("classification")
        valid_dialogue = (
            result.get("valid_dialogue") is True
            and classification in AI_CLASSIFICATIONS
            and bool(str(result.get("reason") or "").strip())
        )
        if not valid_dialogue:
            return await self._link_invalid_dialogue(record, data)

        if await self._get_analysis_history(attempt.tenant_id, analysis) is not None:
            if data is not None and record.follow_up_data_id != data.id:
                record.follow_up_data_id = data.id
                await self.db.flush()
            return data

        now = datetime.now(timezone.utc)
        created_here = False
        if data is None:
            candidate_id = generate_snowflake_id()
            await self._insert_data_if_missing({
                "id": candidate_id,
                "tenant_id": attempt.tenant_id,
                "task_id": attempt.task_id,
                "target_id": attempt.target_id,
                "source_call_id": analysis.call_id,
                "classification": classification,
                "classification_reason": result["reason"],
                "classification_source": "ai",
                "classification_confidence": result.get("confidence") or "low",
                "suggest_review": self._suggest_review(result),
                "low_value_reason": result.get("low_value_reason"),
                "latest_conclusion": result.get("summary") or None,
                "last_contact_at": record.ended_at or record.started_at,
                "blocking_human_call_id": None,
                "version": 1,
                "classification_updated_at": now,
                "classification_updated_by": "ai",
                "created_at": now,
                "updated_at": now,
            })
            data = await self._get_for_update(
                tenant_id=attempt.tenant_id,
                task_id=attempt.task_id,
                target_id=attempt.target_id,
            )
            if data is None:
                return None
            created_here = data.id == candidate_id

        history = await self._get_analysis_history(attempt.tenant_id, analysis)
        if history is not None:
            record.follow_up_data_id = data.id
            await self.db.flush()
            return data

        from_classification = None if created_here else data.classification
        protected = data.classification_source in PROTECTED_CLASSIFICATION_SOURCES
        if not protected:
            data.classification = classification
            data.classification_reason = result["reason"]
            data.classification_source = "ai"
            data.classification_confidence = result.get("confidence") or "low"
            data.suggest_review = self._suggest_review(result)
            data.low_value_reason = result.get("low_value_reason")
            data.classification_updated_at = now
            data.classification_updated_by = "ai"
        data.latest_conclusion = result.get("summary") or None
        data.last_contact_at = record.ended_at or record.started_at
        data.updated_at = now
        if not created_here:
            data.version += 1
        record.follow_up_data_id = data.id

        await self._insert_history_if_missing({
            "id": generate_snowflake_id(),
            "tenant_id": attempt.tenant_id,
            "follow_up_data_id": data.id,
            "from_classification": from_classification,
            "to_classification": data.classification,
            "change_reason": result["reason"],
            "source": "ai_auto",
            "call_id": analysis.call_id,
            "semantic_analysis_id": analysis.id,
            "semantic_analysis_version": analysis.analysis_version,
            "ai_suggested_classification": classification,
            "ai_confidence": result.get("confidence") or "low",
            "ai_reason": result["reason"],
            "ai_evidence_json": json.dumps(
                result.get("evidence") or [], ensure_ascii=False
            ),
            "ai_conflict": result.get("evidence_conflict") is True,
            "ai_adopted": not protected,
            "changed_by": "ai",
            "changed_by_name": "AI",
            "created_at": now,
        })
        await self.db.flush()
        return data

    async def _formal_call_context(
        self,
        call_id: str,
    ) -> tuple[AiCallRecordModel, AiCallOutboundAttemptModel] | None:
        record = await self.repository.get_record(call_id)
        if record is None or record.entry_type not in {
            "sip_outbound",
            "outbound",
            "web",
        }:
            return None
        attempt = await self.db.scalar(
            select(AiCallOutboundAttemptModel)
            .join(
                AiCallOutboundTargetModel,
                and_(
                    AiCallOutboundTargetModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTargetModel.task_id
                    == AiCallOutboundAttemptModel.task_id,
                    AiCallOutboundTargetModel.id
                    == AiCallOutboundAttemptModel.target_id,
                ),
            )
            .join(
                AiCallOutboundTaskModel,
                and_(
                    AiCallOutboundTaskModel.tenant_id
                    == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTaskModel.id == AiCallOutboundAttemptModel.task_id,
                ),
            )
            .where(AiCallOutboundAttemptModel.call_id == call_id)
        )
        if attempt is None or attempt.dialer_type not in {"sip", "owner_runtime"}:
            return None
        if record.tenant_id and record.tenant_id != attempt.tenant_id:
            return None
        if await self.repository.list_handoffs(call_id):
            return None
        return record, attempt

    async def _get_for_update(
        self,
        *,
        tenant_id: str,
        task_id: int,
        target_id: int,
    ) -> AiCallFollowUpDataModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpDataModel)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.task_id == task_id,
                AiCallFollowUpDataModel.target_id == target_id,
            )
            .with_for_update()
        )

    async def _get_analysis_history(
        self,
        tenant_id: str,
        analysis: AiCallSemanticAnalysisModel,
    ) -> AiCallFollowUpClassificationHistoryModel | None:
        return await self.db.scalar(
            select(AiCallFollowUpClassificationHistoryModel).where(
                AiCallFollowUpClassificationHistoryModel.tenant_id == tenant_id,
                AiCallFollowUpClassificationHistoryModel.semantic_analysis_id
                == analysis.id,
                AiCallFollowUpClassificationHistoryModel.semantic_analysis_version
                == analysis.analysis_version,
            )
        )

    async def _link_invalid_dialogue(
        self,
        record: AiCallRecordModel,
        data: AiCallFollowUpDataModel | None,
    ) -> AiCallFollowUpDataModel | None:
        if data is None or record.follow_up_data_id == data.id:
            return data
        record.follow_up_data_id = data.id
        data.last_contact_at = record.ended_at or record.started_at
        data.updated_at = datetime.now(timezone.utc)
        data.version += 1
        await self.db.flush()
        return data

    async def _insert_data_if_missing(self, values: dict[str, Any]) -> None:
        table = AiCallFollowUpDataModel.__table__
        dialect = self._dialect_name()
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.task_id, table.c.target_id]
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=[table.c.tenant_id, table.c.task_id, table.c.target_id]
            )
        else:
            self.db.add(AiCallFollowUpDataModel(**values))
            await self.db.flush()
            return
        await self.db.execute(statement)

    async def _insert_history_if_missing(self, values: dict[str, Any]) -> None:
        table = AiCallFollowUpClassificationHistoryModel.__table__
        dialect = self._dialect_name()
        conflict_columns = [
            table.c.tenant_id,
            table.c.semantic_analysis_id,
            table.c.semantic_analysis_version,
        ]
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=conflict_columns
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(table).values(**values).on_conflict_do_nothing(
                index_elements=conflict_columns
            )
        else:
            self.db.add(AiCallFollowUpClassificationHistoryModel(**values))
            await self.db.flush()
            return
        await self.db.execute(statement)

    def _dialect_name(self) -> str:
        bind = self.db.get_bind()
        return bind.dialect.name if bind is not None else ""

    @staticmethod
    def _suggest_review(result: dict[str, Any]) -> bool:
        return result.get("confidence") != "high" or result.get(
            "evidence_conflict"
        ) is True
