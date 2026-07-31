from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Select, and_, case, func, literal, or_, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Selectable

from .model import AiCallFollowUpTaskModel, AiCallRecordModel
from .outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)


@dataclass(frozen=True)
class StatisticsOverviewAggregate:
    dial_attempts: int
    connected_calls: int
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
    ) -> Select:
        return statement.where(
            AiCallOutboundAttemptModel.tenant_id == tenant_id,
            AiCallRecordModel.entry_type == "sip_outbound",
            AiCallRecordModel.started_at >= started_at,
            AiCallRecordModel.started_at < ended_at,
        )

    async def aggregate_overview(
        self,
        *,
        tenant_id: str,
        started_at: datetime,
        ended_at: datetime,
        include_pending_follow_ups: bool,
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
        statement = select(
            func.count(AiCallRecordModel.id),
            func.coalesce(
                func.sum(
                    case(
                        (AiCallOutboundAttemptModel.call_result == "connected", 1),
                        else_=0,
                    )
                ),
                0,
            ),
            func.coalesce(pending_expression, 0),
        ).select_from(self._formal_outbound_from())
        statement = self._apply_period(
            statement,
            tenant_id=tenant_id,
            started_at=started_at,
            ended_at=ended_at,
        )
        row = (await self.db.execute(statement)).one()
        return StatisticsOverviewAggregate(
            dial_attempts=int(row[0] or 0),
            connected_calls=int(row[1] or 0),
            pending_follow_ups=int(row[2] or 0),
        )

    async def aggregate_results(
        self,
        *,
        tenant_id: str,
        started_at: datetime,
        ended_at: datetime,
    ) -> dict[str, int]:
        result_group = case(
            (AiCallOutboundAttemptModel.call_result == "connected", "connected"),
            (AiCallOutboundAttemptModel.call_result == "no_answer", "no_answer"),
            (AiCallOutboundAttemptModel.call_result == "busy", "busy"),
            (
                AiCallOutboundAttemptModel.call_result == "invalid_number",
                "invalid_number",
            ),
            (
                AiCallOutboundAttemptModel.call_result == "call_failed",
                "call_failed",
            ),
            (
                AiCallRecordModel.status.in_({
                    "created",
                    "starting",
                    "running",
                    "active",
                    "ending",
                }),
                "processing",
            ),
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
        )
        rows = (await self.db.execute(statement)).all()
        return {str(row[0]): int(row[1]) for row in rows}

    async def aggregate_trend(
        self,
        *,
        tenant_id: str,
        buckets: list[tuple[datetime, datetime]],
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
