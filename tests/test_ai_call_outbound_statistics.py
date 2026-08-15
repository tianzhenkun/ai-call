import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.model import AiCallFollowUpTaskModel, AiCallRecordModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.statistics_controller import (
    get_outbound_statistics_service,
)
from app.api.v1.ai_call.statistics_repository import (
    OutboundStatisticsRepository,
    StatisticsOverviewAggregate,
    StatisticsTrendAggregate,
)
from app.api.v1.ai_call.statistics_service import (
    OutboundStatisticsService,
    StatisticsPeriodFactory,
)
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user

SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
NOW = datetime(2026, 7, 31, 16, 20, tzinfo=SHANGHAI)


def test_period_caps_future_end_and_builds_equal_previous_period() -> None:
    period = StatisticsPeriodFactory.build(
        started_at_begin=datetime(2026, 7, 25, tzinfo=SHANGHAI),
        started_at_end=datetime(2026, 8, 1, tzinfo=SHANGHAI),
        time_zone="Asia/Shanghai",
        granularity="day",
        now=NOW,
    )

    assert period.current_ended_at == NOW
    assert period.previous_ended_at == period.current_started_at
    assert (
        period.previous_ended_at - period.previous_started_at
        == period.current_ended_at - period.current_started_at
    )


def test_period_uses_hour_granularity_for_one_natural_day() -> None:
    period = StatisticsPeriodFactory.build(
        started_at_begin=datetime(2026, 7, 31, tzinfo=SHANGHAI),
        started_at_end=datetime(2026, 8, 1, tzinfo=SHANGHAI),
        time_zone="Asia/Shanghai",
        granularity="hour",
        now=NOW,
    )

    assert period.granularity == "hour"
    assert period.current_started_at == datetime(2026, 7, 31, tzinfo=SHANGHAI)
    assert period.current_ended_at == NOW


def test_period_keeps_equal_absolute_duration_across_dst_boundary() -> None:
    now = datetime(2026, 3, 9, 0, 0, tzinfo=NEW_YORK)
    period = StatisticsPeriodFactory.build(
        started_at_begin=datetime(2026, 3, 7, 0, 0, tzinfo=NEW_YORK),
        started_at_end=datetime(2026, 3, 9, 0, 0, tzinfo=NEW_YORK),
        time_zone="America/New_York",
        granularity="day",
        now=now,
    )

    assert period.previous_ended_at.astimezone(
        ZoneInfo("UTC")
    ) - period.previous_started_at.astimezone(
        ZoneInfo("UTC")
    ) == period.current_ended_at.astimezone(ZoneInfo("UTC")) - period.current_started_at.astimezone(
        ZoneInfo("UTC")
    )


@pytest.mark.parametrize(
    ("begin", "end", "time_zone", "granularity", "message"),
    [
        (
            "2026-08-01T00:00:00+08:00",
            "2026-08-02T00:00:00+08:00",
            "Asia/Shanghai",
            "day",
            "统计开始时间不能晚于当前时间",
        ),
        (
            "2026-07-01T00:00:00+08:00",
            "2026-10-01T00:00:00+08:00",
            "Asia/Shanghai",
            "day",
            "统计范围不能超过 90 个自然日",
        ),
        (
            "2026-07-31T00:00:00+08:00",
            "2026-08-01T00:00:00+08:00",
            "Bad/Zone",
            "hour",
            "timeZone 不合法",
        ),
        (
            "2026-07-30T00:00:00+08:00",
            "2026-08-01T00:00:00+08:00",
            "Asia/Shanghai",
            "hour",
            "统计粒度与时间范围不匹配",
        ),
        (
            "2026-07-31T12:00:00+08:00",
            "2026-07-31T12:00:00+08:00",
            "Asia/Shanghai",
            "hour",
            "统计开始时间必须早于结束时间",
        ),
    ],
)
def test_period_rejects_invalid_range(
    begin: str,
    end: str,
    time_zone: str,
    granularity: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        StatisticsPeriodFactory.build(
            started_at_begin=datetime.fromisoformat(begin),
            started_at_end=datetime.fromisoformat(end),
            time_zone=time_zone,
            granularity=granularity,
            now=NOW,
        )


def test_period_rejects_more_than_ninety_natural_days_even_across_dst() -> None:
    with pytest.raises(ValueError, match="统计范围不能超过 90 个自然日"):
        StatisticsPeriodFactory.build(
            started_at_begin=datetime(2026, 1, 1, tzinfo=NEW_YORK),
            started_at_end=datetime(2026, 4, 2, tzinfo=NEW_YORK),
            time_zone="America/New_York",
            granularity="day",
            now=datetime(2026, 4, 2, tzinfo=NEW_YORK) + timedelta(hours=1),
        )


def _task(*, task_id: int, tenant_id: str, now: datetime) -> AiCallOutboundTaskModel:
    return AiCallOutboundTaskModel(
        id=task_id,
        tenant_id=tenant_id,
        validation_id=task_id,
        idempotency_key=f"task-{tenant_id}",
        request_fingerprint=f"fingerprint-{tenant_id}",
        task_name=f"{tenant_id} 正式外呼",
        task_mode="batch",
        status="RUNNING",
        total_targets=20,
        completed_targets=0,
        connected_targets=0,
        failed_targets=0,
        execution_mode="immediate",
        prompt_name="产品介绍",
        scene_code="product_intro",
        voice="Cherry",
        rule_id=1,
        rule_name="工作日规则",
        rule_summary="09:00-18:00",
        config_snapshot_json="{}",
        created_by=1,
        created_at=now,
        updated_at=now,
    )


def _target(
    *,
    target_id: int,
    tenant_id: str,
    task_id: int,
    now: datetime,
) -> AiCallOutboundTargetModel:
    return AiCallOutboundTargetModel(
        id=target_id,
        tenant_id=tenant_id,
        task_id=task_id,
        validation_id=task_id,
        source_validation_row_id=target_id,
        source_row_number=target_id,
        phone_number=f"138{target_id:08d}",
        customer_name=f"客户 {target_id}",
        status="COMPLETED",
        attempt_count=1,
        created_at=now,
        updated_at=now,
    )


def _record(
    *,
    row_id: int,
    started_at: datetime,
    entry_type: str = "sip_outbound",
    status: str = "completed",
    follow_up_id: int | None = None,
    answered_at: datetime | None = None,
    duration_ms: int | None = None,
) -> AiCallRecordModel:
    return AiCallRecordModel(
        id=row_id,
        call_id=f"call-{row_id}",
        follow_up_id=follow_up_id,
        entry_type=entry_type,
        room_name=f"room-{row_id}",
        participant_identity=f"sip-{row_id}",
        status=status,
        started_at=started_at,
        answered_at=answered_at,
        duration_ms=duration_ms,
    )


def _attempt(
    *,
    row_id: int,
    tenant_id: str,
    task_id: int,
    call_result: str | None,
    started_at: datetime,
) -> AiCallOutboundAttemptModel:
    return AiCallOutboundAttemptModel(
        id=10_000 + row_id,
        tenant_id=tenant_id,
        task_id=task_id,
        target_id=row_id,
        attempt_no=1,
        call_id=f"call-{row_id}",
        status="COMPLETED",
        call_result=call_result,
        started_at=started_at,
        created_at=started_at,
        updated_at=started_at,
    )


def _follow_up(
    *,
    follow_up_id: int,
    tenant_id: str,
    source_call_id: str,
    status: str,
    now: datetime,
) -> AiCallFollowUpTaskModel:
    return AiCallFollowUpTaskModel(
        id=follow_up_id,
        tenant_id=tenant_id,
        source_type="ai_suggested",
        source_key=f"call:{source_call_id}",
        source_call_id=source_call_id,
        scene_code="product_intro",
        contact_ref=f"contact-{follow_up_id}",
        masked_contact="138****0000",
        status=status,
        follow_up_reason="客户要求后续联系",
        created_at=now,
        updated_at=now,
    )


@pytest.mark.anyio
async def test_repository_counts_only_current_tenant_formal_outbound_calls() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    begin = datetime(2026, 7, 25, tzinfo=ZoneInfo("UTC"))
    end = datetime(2026, 7, 27, tzinfo=ZoneInfo("UTC"))

    async with session_maker() as session:
        session.add_all([
            _task(task_id=100, tenant_id="tenant-a", now=begin),
            _task(task_id=200, tenant_id="tenant-b", now=begin),
        ])
        formal_results = [
            "connected",
            "connected",
            "no_answer",
            "busy",
            "invalid_number",
            "call_failed",
            "rejected",
            None,
        ]
        for row_id, call_result in enumerate(formal_results, start=1):
            started_at = begin + timedelta(hours=row_id * 5)
            session.add_all([
                _target(
                    target_id=row_id,
                    tenant_id="tenant-a",
                    task_id=100,
                    now=started_at,
                ),
                _record(
                    row_id=row_id,
                    started_at=started_at,
                    entry_type="outbound" if row_id == 1 else "sip_outbound",
                    status="running" if row_id == 8 else "completed",
                    follow_up_id=9001 if row_id == 1 else None,
                    answered_at=started_at if row_id in {1, 2} else None,
                    duration_ms={1: 4_000, 2: 10_000}.get(row_id),
                ),
                _attempt(
                    row_id=row_id,
                    tenant_id="tenant-a",
                    task_id=100,
                    call_result=call_result,
                    started_at=started_at,
                ),
            ])

        for row_id, entry_type, call_result in (
            (9, "web", "connected"),
            (10, "outbound_mock", "connected"),
            (11, "sip_inbound", "connected"),
            (14, "web", "no_answer"),
            (15, "web", "busy"),
            (16, "web", "invalid_number"),
        ):
            started_at = begin + timedelta(hours=2)
            session.add_all([
                _target(
                    target_id=row_id,
                    tenant_id="tenant-a",
                    task_id=100,
                    now=started_at,
                ),
                _record(
                    row_id=row_id,
                    started_at=started_at,
                    entry_type=entry_type,
                ),
                _attempt(
                    row_id=row_id,
                    tenant_id="tenant-a",
                    task_id=100,
                    call_result=call_result,
                    started_at=started_at,
                ),
            ])

        session.add(_record(row_id=12, started_at=begin + timedelta(hours=2)))
        session.add_all([
            _target(
                target_id=13,
                tenant_id="tenant-b",
                task_id=200,
                now=begin,
            ),
            _record(row_id=13, started_at=begin + timedelta(hours=2)),
            _attempt(
                row_id=13,
                tenant_id="tenant-b",
                task_id=200,
                call_result="connected",
                started_at=begin + timedelta(hours=2),
            ),
            _follow_up(
                follow_up_id=9001,
                tenant_id="tenant-a",
                source_call_id="unrelated-source",
                status="pending",
                now=begin,
            ),
            _follow_up(
                follow_up_id=9002,
                tenant_id="tenant-a",
                source_call_id="call-2",
                status="pending",
                now=begin,
            ),
            _follow_up(
                follow_up_id=9003,
                tenant_id="tenant-a",
                source_call_id="call-3",
                status="processing",
                now=begin,
            ),
            _follow_up(
                follow_up_id=9004,
                tenant_id="tenant-b",
                source_call_id="call-13",
                status="pending",
                now=begin,
            ),
        ])
        await session.commit()

        repository = OutboundStatisticsRepository(session)
        overview = await repository.aggregate_overview(
            tenant_id="tenant-a",
            started_at=begin,
            ended_at=end,
            include_pending_follow_ups=True,
        )
        result_counts = await repository.aggregate_results(
            tenant_id="tenant-a",
            started_at=begin,
            ended_at=end,
        )
        trend = await repository.aggregate_trend(
            tenant_id="tenant-a",
            buckets=[
                (begin, begin + timedelta(days=1)),
                (begin + timedelta(days=1), end),
            ],
        )

    await engine.dispose()

    assert overview.dial_attempts == 12
    assert overview.connected_calls == 3
    assert overview.total_duration_ms == 14_000
    assert overview.intent_leads == 0
    assert overview.pending_follow_ups == 2
    assert result_counts == {
        "connected": 2,
        "no_answer": 2,
        "rejected": 1,
        "early_hangup": 1,
        "invalid_number": 2,
        "other": 4,
    }
    assert [(item.dial_attempts, item.connected_calls) for item in trend] == [
        (8, 3),
        (4, 0),
    ]


class _StatisticsRepositoryStub:
    def __init__(self) -> None:
        self.buckets: list[tuple[datetime, datetime]] = []

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
        del tenant_id, started_at, ended_at, scene_code, task_id
        if include_pending_follow_ups:
            return StatisticsOverviewAggregate(
                dial_attempts=8,
                connected_calls=2,
                total_duration_ms=60_000,
                intent_leads=1,
                pending_follow_ups=2,
            )
        return StatisticsOverviewAggregate(
            dial_attempts=4,
            connected_calls=2,
            total_duration_ms=30_000,
            intent_leads=0,
            pending_follow_ups=0,
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
        del tenant_id, started_at, ended_at, scene_code, task_id
        return {
            "connected": 2,
            "no_answer": 2,
            "rejected": 1,
            "early_hangup": 1,
            "invalid_number": 1,
            "other": 1,
        }

    async def aggregate_trend(
        self,
        *,
        tenant_id: str,
        buckets: list[tuple[datetime, datetime]],
        scene_code: str | None = None,
        task_id: int | None = None,
    ) -> list[StatisticsTrendAggregate]:
        del tenant_id, scene_code, task_id
        self.buckets = buckets
        return [
            StatisticsTrendAggregate(
                bucket_start=bucket_start,
                dial_attempts=1,
                connected_calls=1 if index == 0 else 0,
            )
            for index, (bucket_start, _) in enumerate(buckets)
        ]


@pytest.mark.anyio
async def test_service_builds_comparison_buckets_and_ordered_results() -> None:
    repository = _StatisticsRepositoryStub()
    service = OutboundStatisticsService(repository)

    result = await service.get_statistics(
        tenant_id="tenant-a",
        started_at_begin=datetime(2026, 7, 25, tzinfo=SHANGHAI),
        started_at_end=datetime(2026, 8, 1, tzinfo=SHANGHAI),
        time_zone="Asia/Shanghai",
        granularity="day",
        now=NOW,
    )

    assert result.generated_at == NOW
    assert result.overview.dial_attempts == 8
    assert result.overview.connect_rate == 0.25
    assert result.overview.total_duration_ms == 60_000
    assert result.overview.intent_leads == 1
    assert result.comparison.dial_attempts_change_rate == 1.0
    assert result.comparison.connected_calls_change_rate == 0.0
    assert result.comparison.connect_rate_change_points == -25.0
    assert result.comparison.total_duration_change_rate == 1.0
    assert result.comparison.intent_leads_change_rate is None
    assert len(repository.buckets) == 7
    assert repository.buckets[-1][1] == NOW
    assert [item.result for item in result.results] == [
        "connected",
        "no_answer",
        "rejected",
        "early_hangup",
        "invalid_number",
        "other",
    ]
    assert sum(item.count for item in result.results) == 8


@pytest.mark.anyio
async def test_service_returns_null_comparison_when_previous_period_is_empty() -> None:
    repository = _StatisticsRepositoryStub()

    async def empty_previous(**kwargs) -> StatisticsOverviewAggregate:
        if kwargs["include_pending_follow_ups"]:
            return StatisticsOverviewAggregate(1, 1, 1_000, 1, 0)
        return StatisticsOverviewAggregate(0, 0, 0, 0, 0)

    repository.aggregate_overview = empty_previous
    repository.aggregate_results = AsyncMock(return_value={"connected": 1})
    service = OutboundStatisticsService(repository)

    result = await service.get_statistics(
        tenant_id="tenant-a",
        started_at_begin=datetime(2026, 7, 31, tzinfo=SHANGHAI),
        started_at_end=datetime(2026, 8, 1, tzinfo=SHANGHAI),
        time_zone="Asia/Shanghai",
        granularity="hour",
        now=NOW,
    )

    assert result.comparison.dial_attempts_change_rate is None
    assert result.comparison.connected_calls_change_rate is None
    assert result.comparison.connect_rate_change_points is None


def test_statistics_controller_uses_camel_case_contract() -> None:
    service = SimpleNamespace(get_statistics=AsyncMock())

    async def build_result():
        return await OutboundStatisticsService(_StatisticsRepositoryStub()).get_statistics(
            tenant_id="tenant-a",
            started_at_begin=datetime(2026, 7, 31, tzinfo=SHANGHAI),
            started_at_end=datetime(2026, 8, 1, tzinfo=SHANGHAI),
            time_zone="Asia/Shanghai",
            granularity="hour",
            now=NOW,
        )

    service.get_statistics.return_value = asyncio.run(build_result())
    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_outbound_statistics_service] = lambda: service
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )

    response = TestClient(app).get(
        "/ai-call/outbound-statistics",
        params={
            "startedAtBegin": "2026-07-31T00:00:00+08:00",
            "startedAtEnd": "2026-08-01T00:00:00+08:00",
            "timeZone": "Asia/Shanghai",
            "granularity": "hour",
            "sceneCode": "product_intro",
            "taskId": "100",
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["period"]["timeZone"] == "Asia/Shanghai"
    assert payload["overview"]["dialAttempts"] == 8
    assert "dial_attempts" not in payload["overview"]
    service.get_statistics.assert_awaited_once_with(
        tenant_id="tenant-a",
        started_at_begin=datetime.fromisoformat("2026-07-31T00:00:00+08:00"),
        started_at_end=datetime.fromisoformat("2026-08-01T00:00:00+08:00"),
        time_zone="Asia/Shanghai",
        granularity="hour",
        scene_code="product_intro",
        task_id=100,
    )
