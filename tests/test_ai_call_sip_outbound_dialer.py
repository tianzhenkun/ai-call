from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallEventModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.sip_line_schema import SipLineSnapshot
from app.api.v1.ai_call.outbound.sip_outbound_dialer import SipOutboundDialer
from app.config.setting import Settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_sip import SipOutboundConfig
from app.utils.id_util import generate_snowflake_id


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'sip-dialer.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


class FakeAiCallService:
    def __init__(
        self,
        *,
        error: Exception | None = None,
        terminate_error: Exception | None = None,
    ) -> None:
        self.error = error
        self.terminate_error = terminate_error
        self.session_created = asyncio.Event()
        self.requests: list[dict[str, object]] = []
        self.terminated: list[dict[str, str]] = []
        self.before_sip_invite_calls = 0

    async def create_sip_session(self, **kwargs):
        self.requests.append(kwargs)
        before_sip_invite = kwargs.get("before_sip_invite")
        if before_sip_invite is not None:
            await before_sip_invite()
            self.before_sip_invite_calls += 1
        self.session_created.set()
        if self.error is not None:
            raise self.error
        return SimpleNamespace(call_id=kwargs["call_id"])

    async def terminate_sip_session(
        self,
        call_id: str,
        *,
        end_reason: str,
    ) -> None:
        self.terminated.append({
            "call_id": call_id,
            "end_reason": end_reason,
        })
        if self.terminate_error is not None:
            raise self.terminate_error


def line_snapshot() -> SipLineSnapshot:
    return SipLineSnapshot(
        lineId="340700000000000001",
        lineCode="local-freeswitch",
        lineName="本地 FreeSWITCH",
        adapterType="livekit_sip",
        routeMode="inline_hostname",
        proxyHost="127.0.0.1",
        proxyPort=5089,
        authMode="ip_allowlist",
        callerNumber="1000",
        destinationCountry="CN",
        maxConcurrency=1,
        originateTimeoutSeconds=45,
    )


def dial_request():
    return SimpleNamespace(
        tenant_id="tenant-a",
        task_id=340700000000000010,
        target_id=340700000000000011,
        attempt_no=1,
        phone_number="19900001001",
        customer_name="刘先生",
        scene_code="intro_geo",
        voice="Tina",
        prompt_profile_id=None,
        line=line_snapshot(),
    )


def record(
    *,
    call_id: str = "call-map",
    status: str = "failed",
    answered_at: datetime | None = None,
    end_reason: str | None = None,
    failure_message: str | None = None,
    duration_ms: int | None = None,
) -> AiCallRecordModel:
    now = datetime.now(timezone.utc)
    return AiCallRecordModel(
        id=generate_snowflake_id(),
        call_id=call_id,
        business_type="outbound_task",
        business_id="340700000000000010",
        scene_code="intro_geo",
        prompt_source_key=None,
        entry_type="sip_outbound",
        room_name=f"ai-call-{call_id}",
        participant_identity=f"sip-{call_id}",
        callee_phone_number_hash=None,
        callee_phone_number_masked="199****1001",
        status=status,
        end_reason=end_reason,
        failure_stage=None,
        failure_message=failure_message,
        started_at=now,
        answered_at=answered_at,
        ended_at=now if status in {"completed", "failed"} else None,
        duration_ms=duration_ms,
    )


async def _get_record(database, call_id: str) -> AiCallRecordModel | None:
    async with database() as db:
        return await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )


async def mark_answered(database, call_id: str) -> None:
    async with database() as db:
        row = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        if row is None:
            row = record(call_id=call_id, status="connected")
            db.add(row)
        row.status = "connected"
        row.answered_at = datetime.now(timezone.utc)
        row.ended_at = None
        await db.commit()


async def add_event(database, call_id: str, event_type: str) -> None:
    async with database() as db:
        db.add(
            AiCallEventModel(
                id=generate_snowflake_id(),
                call_id=call_id,
                event_id=f"event-{generate_snowflake_id()}",
                event_type=event_type,
                source="test",
                event_time=datetime.now(timezone.utc),
                payload_json=None,
            )
        )
        await db.commit()


async def add_completed_media_tracks(database, call_id: str) -> None:
    now = datetime.now(timezone.utc)
    async with database() as db:
        for role in ("ai", "customer"):
            db.add(
                AiCallRecordingTrackModel(
                    id=generate_snowflake_id(),
                    call_id=call_id,
                    room_name=f"ai-call-{call_id}",
                    track_role=role,
                    participant_identity=f"{role}-{call_id}",
                    handoff_id=None,
                    status="completed",
                    egress_id=f"egress-{role}-{call_id}",
                    oss_id=generate_snowflake_id(),
                    object_name=f"ai-call/recordings/{call_id}-{role}.ogg",
                    started_at=now,
                    ended_at=now,
                    duration_ms=1200,
                )
            )
        await db.commit()


async def finish_record(
    database,
    call_id: str,
    *,
    answered: bool,
    reason: str = "customer_end",
) -> None:
    async with database() as db:
        row = await db.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
        if row is None:
            row = record(call_id=call_id)
            db.add(row)
        row.status = "completed" if answered else "failed"
        row.end_reason = reason
        row.answered_at = (
            row.answered_at or datetime.now(timezone.utc) if answered else None
        )
        row.ended_at = datetime.now(timezone.utc)
        row.duration_ms = 1200 if answered else 0
        await db.commit()


async def wait_until(
    predicate: Callable[[], bool],
    *,
    attempts: int = 20,
) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


async def zero_sleep(_: float) -> None:
    await asyncio.sleep(0)


def build_dialer(database, service: FakeAiCallService) -> SipOutboundDialer:
    return SipOutboundDialer(
        database,
        ai_call_service_factory=lambda db, config: service,
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES="199",
            SIP_PUBLIC_IP="127.0.0.1",
        ),
        sleep=zero_sleep,
    )


@pytest.mark.anyio
async def test_dial_does_not_mark_connected_after_session_creation(database):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)
    connected = AsyncMock()

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-1",
            on_connected=connected,
        )
    )
    await service.session_created.wait()
    connected.assert_not_awaited()
    await finish_record(
        database,
        "call-1",
        answered=False,
        reason="sip_connect_timeout",
    )
    result = await asyncio.wait_for(task, timeout=1)

    assert result.call_result == "no_answer"
    connected.assert_not_awaited()


@pytest.mark.anyio
async def test_dial_commits_persisted_record_before_waiting_for_sip_answer(database):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-pre-invite-commit",
            on_connected=AsyncMock(),
        )
    )
    await service.session_created.wait()
    await finish_record(
        database,
        "call-pre-invite-commit",
        answered=False,
        reason="sip_connect_timeout",
    )
    await asyncio.wait_for(task, timeout=1)

    assert service.before_sip_invite_calls == 1


@pytest.mark.anyio
async def test_dial_marks_connected_once_after_answer_and_media(database):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)
    connected = AsyncMock()

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-2",
            on_connected=connected,
        )
    )
    await service.session_created.wait()
    await mark_answered(database, "call-2")
    await asyncio.sleep(0)
    connected.assert_not_awaited()

    await add_event(database, "call-2", "media_connected")
    await wait_until(lambda: connected.await_count == 1)
    await finish_record(database, "call-2", answered=True)
    result = await asyncio.wait_for(task, timeout=1)

    assert result.call_result == "connected"
    assert result.duration_ms == 1200
    connected.assert_awaited_once()


@pytest.mark.anyio
async def test_dial_accepts_completed_ai_and_customer_tracks_as_media_evidence(
    database,
):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)
    connected = AsyncMock()

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-track-media",
            on_connected=connected,
        )
    )
    await service.session_created.wait()
    await mark_answered(database, "call-track-media")
    await add_completed_media_tracks(database, "call-track-media")
    await finish_record(database, "call-track-media", answered=True)
    result = await asyncio.wait_for(task, timeout=1)

    assert result.call_result == "connected"
    connected.assert_awaited_once()


@pytest.mark.anyio
async def test_dial_does_not_report_connected_without_media_evidence(database):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)
    connected = AsyncMock()

    task = asyncio.create_task(
        dialer.dial(
            dial_request(),
            call_id="call-no-media",
            on_connected=connected,
        )
    )
    await service.session_created.wait()
    await finish_record(database, "call-no-media", answered=True)
    result = await asyncio.wait_for(task, timeout=1)

    assert result.call_result == "call_failed"
    assert result.error_message == "未检测到媒体接通证据"
    connected.assert_not_awaited()


@pytest.mark.anyio
async def test_dial_stops_polling_at_reconciliation_deadline(database):
    service = FakeAiCallService()
    monotonic_values = iter([0.0, 700.0])
    dialer = SipOutboundDialer(
        database,
        ai_call_service_factory=lambda db, config: service,
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES="199",
            AI_CALL_SIP_MAX_CALL_DURATION_SECONDS=600,
            SIP_PUBLIC_IP="127.0.0.1",
        ),
        sleep=zero_sleep,
        monotonic=lambda: next(monotonic_values),
        reconciliation_grace_seconds=30,
    )

    result = await dialer.dial(
        dial_request(),
        call_id="call-timeout",
        on_connected=AsyncMock(),
    )

    assert result.call_result == "call_failed"
    assert result.retry_allowed is False
    assert result.error_message == "SIP 通话状态对账超时，禁止自动重拨"
    assert service.terminated == [{
        "call_id": "call-timeout",
        "end_reason": "outbound_reconcile_timeout",
    }]


@pytest.mark.anyio
async def test_dial_keeps_attempt_unsettled_when_timeout_cleanup_fails(database):
    service = FakeAiCallService(
        terminate_error=RuntimeError("room delete failed"),
    )
    monotonic_values = iter([0.0, 700.0])
    dialer = SipOutboundDialer(
        database,
        ai_call_service_factory=lambda db, config: service,
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES="199",
            AI_CALL_SIP_MAX_CALL_DURATION_SECONDS=600,
            SIP_PUBLIC_IP="127.0.0.1",
        ),
        sleep=zero_sleep,
        monotonic=lambda: next(monotonic_values),
        reconciliation_grace_seconds=30,
    )

    result = await dialer.dial(
        dial_request(),
        call_id="call-cleanup-failed",
        on_connected=AsyncMock(),
    )

    assert result.call_result == "call_failed"
    assert result.settle_attempt is False
    assert result.retry_allowed is False
    assert result.error_message == "SIP 通话状态对账超时，资源清理失败，保持待对账"


@pytest.mark.anyio
async def test_dial_cleans_up_before_settling_unexpected_reconciliation_error(
    database,
    monkeypatch,
):
    service = FakeAiCallService()
    dialer = build_dialer(database, service)
    monkeypatch.setattr(
        dialer,
        "_read_evidence",
        AsyncMock(side_effect=RuntimeError("database read failed")),
    )

    result = await dialer.dial(
        dial_request(),
        call_id="call-read-error",
        on_connected=AsyncMock(),
    )

    assert result.call_result == "call_failed"
    assert result.retry_allowed is False
    assert result.settle_attempt is True
    assert service.terminated == [{
        "call_id": "call-read-error",
        "end_reason": "outbound_reconcile_error",
    }]


@pytest.mark.anyio
async def test_dial_returns_failure_when_session_creation_fails_without_record(database):
    service = FakeAiCallService(error=RuntimeError("provider unavailable"))
    result = await build_dialer(database, service).dial(
        dial_request(),
        call_id="call-no-record",
        on_connected=AsyncMock(),
    )

    assert result.call_result == "call_failed"
    assert result.error_message == "provider unavailable"
    assert await _get_record(database, "call-no-record") is None


@pytest.mark.anyio
async def test_dial_preserves_provider_diagnostics_from_real_error_chain(database):
    provider_error = AiCallError(
        error_id="sip_create_participant_failed",
        msg="LiveKit SIP Participant 创建失败",
        details={
            "providerStatusCode": "486",
            "providerReason": "SIP 486 Busy Here",
            "hangupCause": "USER_BUSY",
        },
    )
    try:
        try:
            raise provider_error from RuntimeError("raw SDK failure")
        except AiCallError as cause:
            raise CustomException(msg=cause.msg) from cause
    except CustomException as error:
        service = FakeAiCallService(error=error)

    result = await build_dialer(database, service).dial(
        dial_request(),
        call_id="call-provider-error",
        on_connected=AsyncMock(),
    )

    assert result.provider_status_code == "486"
    assert result.provider_reason == "SIP 486 Busy Here"
    assert result.hangup_cause == "USER_BUSY"
    assert result.call_result == "busy"


@pytest.mark.parametrize(
    ("end_reason", "expected"),
    [
        ("sip_busy", "busy"),
        ("sip_connect_timeout", "no_answer"),
        ("sip_403", "call_failed"),
        ("sip_503", "call_failed"),
        ("sip_508", "call_failed"),
    ],
)
def test_maps_terminal_sip_reason(end_reason, expected):
    result = SipOutboundDialer.map_terminal_record(
        record(answered_at=None, end_reason=end_reason)
    )
    assert result.call_result == expected


@pytest.mark.anyio
async def test_builds_request_level_sip_config_without_credentials(database):
    service = FakeAiCallService()
    captured: list[SipOutboundConfig] = []
    dialer = SipOutboundDialer(
        database,
        ai_call_service_factory=lambda db, config: (
            captured.append(config) or service
        ),
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES="199",
            SIP_PUBLIC_IP="127.0.0.1",
        ),
        sleep=zero_sleep,
    )

    config = dialer.build_sip_config(line_snapshot())

    assert config.trunk_hostname == "127.0.0.1:5089"
    assert config.trunk_id == ""
    assert config.auth_username == ""
    assert config.auth_password == ""
    assert config.caller_number == "1000"
