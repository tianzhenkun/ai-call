from datetime import datetime
from enum import StrEnum

from .schema import AiCallBaseSchema


class StatisticsGranularity(StrEnum):
    HOUR = "hour"
    DAY = "day"


class CallResultGroup(StrEnum):
    CONNECTED = "connected"
    VOICEMAIL = "voicemail"
    TRANSPORT_CONNECTED = "transport_connected"
    NO_ANSWER = "no_answer"
    REJECTED = "rejected"
    EARLY_HANGUP = "early_hangup"
    INVALID_NUMBER = "invalid_number"
    OTHER = "other"


class StatisticsPeriodOut(AiCallBaseSchema):
    time_zone: str
    current_started_at: datetime
    current_ended_at: datetime
    previous_started_at: datetime
    previous_ended_at: datetime


class StatisticsOverviewOut(AiCallBaseSchema):
    dial_attempts: int
    connected_calls: int
    connect_rate: float
    total_duration_ms: int
    intent_leads: int
    pending_follow_ups: int


class StatisticsComparisonOut(AiCallBaseSchema):
    dial_attempts_change_rate: float | None
    connected_calls_change_rate: float | None
    connect_rate_change_points: float | None
    total_duration_change_rate: float | None
    intent_leads_change_rate: float | None


class StatisticsTrendPointOut(AiCallBaseSchema):
    bucket_start: datetime
    dial_attempts: int
    connected_calls: int
    connect_rate: float


class StatisticsResultOut(AiCallBaseSchema):
    result: CallResultGroup
    count: int
    rate: float


class OutboundStatisticsOut(AiCallBaseSchema):
    generated_at: datetime
    period: StatisticsPeriodOut
    overview: StatisticsOverviewOut
    comparison: StatisticsComparisonOut
    trend: list[StatisticsTrendPointOut]
    results: list[StatisticsResultOut]
