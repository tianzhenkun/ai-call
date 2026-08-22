from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, and_, case, func, literal, or_, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Selectable

from .model import (
    AiCallFollowUpDataModel,
    AiCallFollowUpTaskModel,
    AiCallRecordModel,
)
from .outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)

FORMAL_TASK_ENTRY_TYPES = ("outbound", "sip_outbound", "web")


@dataclass(frozen=True)
class StatisticsOverviewAggregate:
    dial_attempts: int
    connected_calls: int
    total_duration_ms: int
    intent_leads: int
    pending_follow_ups: int


@dataclass(frozen=True)
class StatisticsTrendAggregate:
    bucket_start: datetime
    dial_attempts: int
    connected_calls: int


class OutboundStatisticsRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    @staticmethod
    def _formal_outbound_from() -> Selectable:
        return (
            AiCallRecordModel.__table__
            .join(
                AiCallOutboundAttemptModel.__table__,
                AiCallOutboundAttemptModel.call_id == AiCallRecordModel.call_id,
            )
            .join(
                AiCallOutboundTargetModel.__table__,
                and_(
                    AiCallOutboundTargetModel.tenant_id == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTargetModel.id == AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundTargetModel.task_id == AiCallOutboundAttemptModel.task_id,
                ),
            )
            .join(
                AiCallOutboundTaskModel.__table__,
                and_(
                    AiCallOutboundTaskModel.tenant_id == AiCallOutboundAttemptModel.tenant_id,
                    AiCallOutboundTaskModel.id == AiCallOutboundAttemptModel.task_id,
                ),
            )
        )

    @staticmethod
    def _apply_period(
        statement: Select,
        *,
        tenant_id: str,
        started_at: datetime,
        ended_at: datetime,
        scene_code: str | None,
        task_id: int | None,
    ) -> Select:
        statement = statement.where(
            AiCallOutboundAttemptModel.tenant_id == tenant_id,
            AiCallRecordModel.entry_type.in_(FORMAL_TASK_ENTRY_TYPES),
            AiCallRecordModel.started_at >= started_at,
            AiCallRecordModel.started_at < ended_at,
        )
        if scene_code:
            statement = statement.where(AiCallOutboundTaskModel.scene_code == scene_code)
        if task_id is not None:
            statement = statement.where(AiCallOutboundTaskModel.id == task_id)
        return statement

    async def aggregate_overview(
        self,
        *,
        tenant_id: str,
        started_at: datetime,
        ended_at: datetime,
        include_pending_follow_ups: bool,
        scene_code: str | None = None,
        task_id: int | None = None,
    ) -> StatisticsOverviewAggregate:
        pending_exists = (
            select(AiCallFollowUpTaskModel.id)
            .where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.status == "pending",
                or_(
                    AiCallFollowUpTaskModel.id == AiCallRecordModel.follow_up_id,
                    AiCallFollowUpTaskModel.source_call_id == AiCallRecordModel.call_id,
                ),
            )
            .exists()
        )
        pending_expression = (
            func.sum(case((pending_exists, 1), else_=0))
            if include_pending_follow_ups
            else literal(0)
        )
        interested_lead_id = (
            select(AiCallFollowUpDataModel.id)
            .where(
                AiCallFollowUpDataModel.tenant_id == tenant_id,
                AiCallFollowUpDataModel.task_id == AiCallOutboundAttemptModel.task_id,
                AiCallFollowUpDataModel.target_id == AiCallOutboundAttemptModel.target_id,
                AiCallFollowUpDataModel.source_call_id == AiCallRecordModel.call_id,
                AiCallFollowUpDataModel.classification == "interested",
            )
            .scalar_subquery()
        )
        connected = AiCallOutboundAttemptModel.call_result == "connected"
        statement = select(
            func.count(AiCallRecordModel.id),
            func.coalesce(
                func.sum(case((connected, 1), else_=0)),
                0,
            ),
            func.coalesce(
                func.sum(
                    case(
                        (connected, func.coalesce(AiCallRecordModel.duration_ms, 0)),
                        else_=0,
                    )
                ),
                0,
            ),
            func.coalesce(func.count(func.distinct(interested_lead_id)), 0),
            func.coalesce(pending_expression, 0),
        ).select_from(self._formal_outbound_from())
        statement = self._apply_period(
            statement,
            tenant_id=tenant_id,
            started_at=started_at,
            ended_at=ended_at,
            scene_code=scene_code,
            task_id=task_id,
        )
        row = (await self.db.execute(statement)).one()
        return StatisticsOverviewAggregate(
            dial_attempts=int(row[0] or 0),
            connected_calls=int(row[1] or 0),
            total_duration_ms=int(row[2] or 0),
            intent_leads=int(row[3] or 0),
            pending_follow_ups=int(row[4] or 0),
        )

    async def aggregate_results(
        self,
        *,
        tenant_id: str,
        started_at: datetime,
        ended_at: datetime,
        scene_code: str | None = None,
        task_id: int | None = None,
    ) -> dict[str, int]:
        connected = AiCallOutboundAttemptModel.call_result == "connected"
        early_hangup = and_(
            connected,
            AiCallRecordModel.answered_at.is_not(None),
            func.coalesce(AiCallRecordModel.duration_ms, 0) <= 5_000,
        )
        rejected = or_(
            AiCallOutboundAttemptModel.call_result == "rejected",
            AiCallOutboundAttemptModel.provider_status_code == "603",
            func.upper(AiCallOutboundAttemptModel.hangup_cause).in_({"CALL_REJECTED", "21"}),
        )
        invalid_number = or_(
            AiCallOutboundAttemptModel.call_result == "invalid_number",
            AiCallOutboundAttemptModel.provider_status_code.in_({"404", "410", "484", "604"}),
            func.upper(AiCallOutboundAttemptModel.hangup_cause).in_({
                "ADDRESS_INCOMPLETE",
                "SUBSCRIBER_ABSENT",
                "UNALLOCATED_NUMBER",
                "USER_NOT_REGISTERED",
            }),
        )
        result_group = case(
            (early_hangup, "early_hangup"),
            (connected, "connected"),
            (AiCallOutboundAttemptModel.call_result == "no_answer", "no_answer"),
            (rejected, "rejected"),
            (invalid_number, "invalid_number"),
            else_="other",
        )
        statement = (
            select(result_group.label("result"), func.count(AiCallRecordModel.id))
            .select_from(self._formal_outbound_from())
            .group_by(result_group)
        )
        statement = self._apply_period(
            statement,
            tenant_id=tenant_id,
            started_at=started_at,
            ended_at=ended_at,
            scene_code=scene_code,
            task_id=task_id,
        )
        rows = (await self.db.execute(statement)).all()
        return {str(row[0]): int(row[1]) for row in rows}

    async def aggregate_trend(
        self,
        *,
        tenant_id: str,
        buckets: list[tuple[datetime, datetime]],
        scene_code: str | None = None,
        task_id: int | None = None,
    ) -> list[StatisticsTrendAggregate]:
        if not buckets:
            return []

        statements = []
        for bucket_index, (bucket_start, bucket_end) in enumerate(buckets):
            statement = select(
                literal(bucket_index).label("bucket_index"),
                func.count(AiCallRecordModel.id).label("dial_attempts"),
                func.coalesce(
                    func.sum(
                        case(
                            (
                                AiCallOutboundAttemptModel.call_result == "connected",
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    0,
                ).label("connected_calls"),
            ).select_from(self._formal_outbound_from())
            statements.append(
                self._apply_period(
                    statement,
                    tenant_id=tenant_id,
                    started_at=bucket_start,
                    ended_at=bucket_end,
                    scene_code=scene_code,
                    task_id=task_id,
                )
            )

        rows = (await self.db.execute(union_all(*statements))).all()
        aggregates_by_index = {
            int(row.bucket_index): (int(row.dial_attempts), int(row.connected_calls))
            for row in rows
        }
        return [
            StatisticsTrendAggregate(
                bucket_start=bucket_start,
                dial_attempts=aggregates_by_index[bucket_index][0],
                connected_calls=aggregates_by_index[bucket_index][1],
            )
            for bucket_index, (bucket_start, _) in enumerate(buckets)
        ]
