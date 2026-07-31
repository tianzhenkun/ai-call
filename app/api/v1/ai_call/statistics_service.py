from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .statistics_repository import (
    OutboundStatisticsRepository,
    StatisticsOverviewAggregate,
)
from .statistics_schema import (
    CallResultGroup,
    OutboundStatisticsOut,
    StatisticsComparisonOut,
    StatisticsGranularity,
    StatisticsOverviewOut,
    StatisticsPeriodOut,
    StatisticsResultOut,
    StatisticsTrendPointOut,
)


@dataclass(frozen=True)
class StatisticsPeriod:
    time_zone: str
    current_started_at: datetime
    current_ended_at: datetime
    previous_started_at: datetime
    previous_ended_at: datetime
    granularity: StatisticsGranularity


class StatisticsPeriodFactory:
    @staticmethod
    def build(
        *,
        started_at_begin: datetime,
        started_at_end: datetime,
        time_zone: str,
        granularity: StatisticsGranularity | str,
        now: datetime | None = None,
    ) -> StatisticsPeriod:
        try:
            zone = ZoneInfo(time_zone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timeZone 不合法") from exc

        if started_at_begin.tzinfo is None or started_at_end.tzinfo is None:
            raise ValueError("统计时间必须包含时区")

        begin = started_at_begin.astimezone(zone)
        requested_end = started_at_end.astimezone(zone)
        if begin >= requested_end:
            raise ValueError("统计开始时间必须早于结束时间")

        current = (now or datetime.now(zone)).astimezone(zone)
        if begin > current:
            raise ValueError("统计开始时间不能晚于当前时间")

        natural_days = (requested_end.date() - begin.date()).days
        if natural_days > 90:
            raise ValueError("统计范围不能超过 90 个自然日")

        effective_end = min(requested_end, current)
        if effective_end <= begin:
            raise ValueError("统计区间内暂无已生成数据")

        normalized_granularity = StatisticsGranularity(granularity)
        last_included_at = effective_end - timedelta(microseconds=1)
        same_natural_day = begin.date() == last_included_at.date()
        if (same_natural_day and normalized_granularity != StatisticsGranularity.HOUR) or (
            not same_natural_day and normalized_granularity != StatisticsGranularity.DAY
        ):
            raise ValueError("统计粒度与时间范围不匹配")

        absolute_duration = effective_end.astimezone(UTC) - begin.astimezone(UTC)
        previous_started_at = (begin.astimezone(UTC) - absolute_duration).astimezone(zone)
        return StatisticsPeriod(
            time_zone=time_zone,
            current_started_at=begin,
            current_ended_at=effective_end,
            previous_started_at=previous_started_at,
            previous_ended_at=begin,
            granularity=normalized_granularity,
        )


class OutboundStatisticsService:
    _RESULT_ORDER = (
        CallResultGroup.CONNECTED,
        CallResultGroup.NO_ANSWER,
        CallResultGroup.BUSY,
        CallResultGroup.INVALID_NUMBER,
        CallResultGroup.CALL_FAILED,
        CallResultGroup.PROCESSING,
        CallResultGroup.OTHER,
    )

    def __init__(self, repository: OutboundStatisticsRepository) -> None:
        self.repository = repository

    async def get_statistics(
        self,
        *,
        tenant_id: str,
        started_at_begin: datetime,
        started_at_end: datetime,
        time_zone: str,
        granularity: StatisticsGranularity | str,
        now: datetime | None = None,
    ) -> OutboundStatisticsOut:
        period = StatisticsPeriodFactory.build(
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
            time_zone=time_zone,
            granularity=granularity,
            now=now,
        )
        current = await self.repository.aggregate_overview(
            tenant_id=tenant_id,
            started_at=period.current_started_at,
            ended_at=period.current_ended_at,
            include_pending_follow_ups=True,
        )
        previous = await self.repository.aggregate_overview(
            tenant_id=tenant_id,
            started_at=period.previous_started_at,
            ended_at=period.previous_ended_at,
            include_pending_follow_ups=False,
        )
        buckets = self._build_buckets(period)
        trend_aggregates = await self.repository.aggregate_trend(
            tenant_id=tenant_id,
            buckets=buckets,
        )
        result_counts = await self.repository.aggregate_results(
            tenant_id=tenant_id,
            started_at=period.current_started_at,
            ended_at=period.current_ended_at,
        )
        if sum(result_counts.values()) != current.dial_attempts:
            raise RuntimeError("外呼统计结果分布与拨打次数不一致")

        current_rate = self._safe_rate(
            current.connected_calls,
            current.dial_attempts,
        )
        previous_rate = self._safe_rate(
            previous.connected_calls,
            previous.dial_attempts,
        )
        generated_at = (now or datetime.now(ZoneInfo(time_zone))).astimezone(ZoneInfo(time_zone))
        return OutboundStatisticsOut(
            generated_at=generated_at,
            period=StatisticsPeriodOut(
                time_zone=period.time_zone,
                current_started_at=period.current_started_at,
                current_ended_at=period.current_ended_at,
                previous_started_at=period.previous_started_at,
                previous_ended_at=period.previous_ended_at,
            ),
            overview=StatisticsOverviewOut(
                dial_attempts=current.dial_attempts,
                connected_calls=current.connected_calls,
                connect_rate=current_rate,
                pending_follow_ups=current.pending_follow_ups,
            ),
            comparison=self._comparison(
                current=current,
                previous=previous,
                current_rate=current_rate,
                previous_rate=previous_rate,
            ),
            trend=[
                StatisticsTrendPointOut(
                    bucket_start=item.bucket_start,
                    dial_attempts=item.dial_attempts,
                    connected_calls=item.connected_calls,
                    connect_rate=self._safe_rate(
                        item.connected_calls,
                        item.dial_attempts,
                    ),
                )
                for item in trend_aggregates
            ],
            results=self._result_items(
                counts=result_counts,
                total=current.dial_attempts,
            ),
        )

    @staticmethod
    def _safe_rate(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 4) if denominator else 0.0

    @staticmethod
    def _change_rate(current: int, previous: int) -> float | None:
        if previous == 0:
            return None
        return round((current - previous) / previous, 4)

    def _comparison(
        self,
        *,
        current: StatisticsOverviewAggregate,
        previous: StatisticsOverviewAggregate,
        current_rate: float,
        previous_rate: float,
    ) -> StatisticsComparisonOut:
        return StatisticsComparisonOut(
            dial_attempts_change_rate=self._change_rate(
                current.dial_attempts,
                previous.dial_attempts,
            ),
            connected_calls_change_rate=self._change_rate(
                current.connected_calls,
                previous.connected_calls,
            ),
            connect_rate_change_points=(
                round((current_rate - previous_rate) * 100, 2) if previous.dial_attempts else None
            ),
        )

    def _result_items(
        self,
        *,
        counts: dict[str, int],
        total: int,
    ) -> list[StatisticsResultOut]:
        items = []
        for result in self._RESULT_ORDER:
            count = counts.get(result.value, 0)
            if (
                result
                in {
                    CallResultGroup.PROCESSING,
                    CallResultGroup.OTHER,
                }
                and not count
            ):
                continue
            items.append(
                StatisticsResultOut(
                    result=result,
                    count=count,
                    rate=self._safe_rate(count, total),
                )
            )
        return items

    @staticmethod
    def _build_buckets(
        period: StatisticsPeriod,
    ) -> list[tuple[datetime, datetime]]:
        buckets: list[tuple[datetime, datetime]] = []
        cursor = period.current_started_at
        if period.granularity == StatisticsGranularity.HOUR:
            end_utc = period.current_ended_at.astimezone(UTC)
            cursor_utc = cursor.astimezone(UTC)
            while cursor_utc < end_utc:
                next_utc = min(cursor_utc + timedelta(hours=1), end_utc)
                buckets.append((
                    cursor_utc.astimezone(cursor.tzinfo),
                    next_utc.astimezone(cursor.tzinfo),
                ))
                cursor_utc = next_utc
            return buckets

        zone = ZoneInfo(period.time_zone)
        while cursor < period.current_ended_at:
            next_date = cursor.date() + timedelta(days=1)
            next_midnight = datetime.combine(next_date, time.min, tzinfo=zone)
            bucket_end = min(next_midnight, period.current_ended_at)
            buckets.append((cursor, bucket_end))
            cursor = bucket_end
        return buckets
