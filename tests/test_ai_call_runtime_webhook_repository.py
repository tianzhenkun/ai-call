from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallHandoffModel, AiCallRecordModel
from app.services.ai_call.runtime_control.models import (
    AiCallEndEvidenceModel,
    AiCallHandoffMediaEvidenceModel,
    AiCallRuntimeCommandModel,
    AiCallWebhookInboxModel,
    AiCallWebhookQuarantineModel,
)


@pytest.fixture
async def webhook_session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runtime-webhook.db'}")
    async with engine.begin() as connection:
        for table in (
            AiCallRecordModel.__table__,
            AiCallHandoffModel.__table__,
            AiCallEndEvidenceModel.__table__,
            AiCallHandoffMediaEvidenceModel.__table__,
            AiCallRuntimeCommandModel.__table__,
            AiCallWebhookInboxModel.__table__,
            AiCallWebhookQuarantineModel.__table__,
        ):
            await connection.run_sync(table.create)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def _seed_owner_record(
    session_factory,
    *,
    entry_type: str = "web",
    with_handoff: bool = True,
    handoff_status: str = "accepted",
    record_id: int = 101,
    call_id: str = "call-1",
    room_name: str = "ai-call-call-1",
) -> None:
    now = datetime(2026, 8, 2, 3, tzinfo=timezone.utc)
    async with session_factory() as session, session.begin():
        session.add(
            AiCallRecordModel(
                id=record_id,
                tenant_id="tenant-a",
                call_id=call_id,
                entry_type=entry_type,
                room_name=room_name,
                participant_identity=f"caller-{call_id}",
                status="ready",
                started_at=now,
                runtime_control_mode="owner_command_v1",
                runtime_owner_id="runtime-1",
                runtime_fencing_token=7,
                runtime_lease_expires_at=now + timedelta(minutes=1),
                runtime_capacity_class="active",
                next_command_seq=2,
                last_applied_command_seq=1,
            )
        )
        if with_handoff:
            session.add(
                AiCallHandoffModel(
                    id=201,
                    tenant_id="tenant-a",
                    handoff_id="handoff-1",
                    call_id=call_id,
                    room_name=room_name,
                    scene_code="default",
                    status=handoff_status,
                    request_source="customer",
                    requested_at=now,
                    human_agent_identity="agent-1",
                    accepted_console_session_id="11111111-1111-1111-1111-111111111111",
                    accepted_at=now,
                    claim_expires_at=now + timedelta(seconds=15),
                )
            )


def _receive_intent(**overrides):
    from app.services.ai_call.runtime_control.webhook_repository import (
        WebhookReceiveIntent,
    )

    values = {
        "provider": "livekit",
        "provider_namespace": "livekit:test",
        "dedupe_key": "EV_join_1",
        "event_type": "participant_joined",
        "room_name": "ai-call-call-1",
        "participant_identity": "human-agent-handoff-1",
        "payload": {
            "event": "participant_joined",
            "id": "EV_join_1",
            "room": {"name": "ai-call-call-1", "metadata": "secret"},
            "participant": {
                "identity": "human-agent-handoff-1",
                "sid": "PA_1",
                "metadata": "secret",
            },
        },
    }
    values.update(overrides)
    return WebhookReceiveIntent(**values)


@pytest.mark.anyio
async def test_authenticated_webhook_commits_inbox_before_returning_success(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory)
    now = datetime(2026, 8, 2, 3, 0, 1, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        decision = await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9001,
            database_clock=lambda _session: _constant_time(now),
        ).receive(_receive_intent())
        inbox = await session.scalar(select(AiCallWebhookInboxModel))
        assert inbox is not None

    assert decision.disposition == "INBOX"
    assert decision.row_id == 9001
    assert inbox.tenant_id == "tenant-a"
    assert inbox.call_id == "call-1"
    assert "secret" not in (inbox.payload_json or "")


@pytest.mark.anyio
async def test_duplicate_provider_event_reuses_one_inbox_row(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory)
    now = datetime(2026, 8, 2, 3, 0, 1, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        repository = RuntimeWebhookRepository(
            session,
            id_generator=iter((9001, 9002)).__next__,
            database_clock=lambda _session: _constant_time(now),
        )
        first = await repository.receive(_receive_intent())
        second = await repository.receive(_receive_intent())

    async with webhook_session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(AiCallWebhookInboxModel))
    assert second == first
    assert count == 1


@pytest.mark.anyio
async def test_unmatched_valid_event_is_quarantined_without_state_write(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    now = datetime(2026, 8, 2, 3, 0, 1, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        decision = await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9001,
            database_clock=lambda _session: _constant_time(now),
        ).receive(_receive_intent(room_name="ai-call-call-not-visible"))

    async with webhook_session_factory() as session:
        quarantine = await session.scalar(select(AiCallWebhookQuarantineModel))
        evidence_count = await session.scalar(
            select(func.count()).select_from(AiCallHandoffMediaEvidenceModel)
        )
        command_count = await session.scalar(
            select(func.count()).select_from(AiCallRuntimeCommandModel)
        )
    assert decision.disposition == "QUARANTINE"
    assert quarantine.status == "UNMATCHED"
    assert quarantine.resolved_tenant_id is None
    assert evidence_count == 0
    assert command_count == 0


@pytest.mark.anyio
async def test_expired_inbox_and_quarantine_leases_are_taken_over(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory)
    received_at = datetime(2026, 8, 2, 3, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        session.add(
            AiCallWebhookInboxModel(
                id=9001,
                provider="livekit",
                provider_namespace="livekit:test",
                dedupe_key="EV_inbox_expired",
                tenant_id="tenant-a",
                call_id="call-1",
                event_type="participant_joined",
                status="PROCESSING",
                processing_owner_id="old-worker",
                processing_token="old-inbox-token",
                processing_expires_at=received_at - timedelta(seconds=1),
                attempt_count=1,
                received_at=received_at,
                claimed_at=received_at - timedelta(seconds=30),
            )
        )
        session.add(
            AiCallWebhookQuarantineModel(
                id=9002,
                provider="livekit",
                provider_namespace="livekit:test",
                dedupe_key="EV_quarantine_expired",
                room_name="ai-call-call-late",
                event_type="participant_joined",
                status="PROCESSING",
                processing_owner_id="old-worker",
                processing_generation=1,
                processing_token="old-quarantine-token",
                processing_expires_at=received_at - timedelta(seconds=1),
                attempt_count=1,
                received_at=received_at,
                claimed_at=received_at - timedelta(seconds=30),
            )
        )

    now = received_at + timedelta(seconds=1)
    tokens = iter(("new-inbox-token", "new-quarantine-token"))
    async with webhook_session_factory() as session, session.begin():
        repository = RuntimeWebhookRepository(
            session,
            processing_token_generator=tokens.__next__,
            database_clock=lambda _session: _constant_time(now),
        )
        inbox_claim = await repository.claim_inbox("jobs-1")
        quarantine_claim = await repository.claim_quarantine("jobs-1")

    assert inbox_claim.processing_token == "new-inbox-token"
    assert inbox_claim.attempt_count == 2
    assert quarantine_claim.processing_token == "new-quarantine-token"
    assert quarantine_claim.processing_generation == 2
    assert quarantine_claim.attempt_count == 2


@pytest.mark.anyio
async def test_join_publish_unmuted_create_one_ready_command_per_version(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory)
    base_time = datetime(2026, 8, 2, 3, 1, tzinfo=timezone.utc)
    events = (
        _receive_intent(),
        _receive_intent(
            dedupe_key="EV_publish_1",
            event_type="track_published",
            payload={
                "event": "track_published",
                "id": "EV_publish_1",
                "room": {"name": "ai-call-call-1"},
                "participant": {
                    "identity": "human-agent-handoff-1",
                    "sid": "PA_1",
                },
                "track": {"sid": "TR_1", "type": "AUDIO", "muted": False},
            },
        ),
        _receive_intent(
            dedupe_key="EV_unmuted_1",
            event_type="track_unmuted",
            payload={
                "event": "track_unmuted",
                "id": "EV_unmuted_1",
                "room": {"name": "ai-call-call-1"},
                "participant": {
                    "identity": "human-agent-handoff-1",
                    "sid": "PA_1",
                },
                "track": {"sid": "TR_1", "type": "AUDIO", "muted": False},
            },
        ),
    )
    ids = iter(range(9100, 9200))
    for index, event in enumerate(events):
        now = base_time + timedelta(seconds=index)
        async with webhook_session_factory() as session, session.begin():
            await RuntimeWebhookRepository(
                session,
                id_generator=ids.__next__,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).receive(event)
        async with webhook_session_factory() as session, session.begin():
            claim = await RuntimeWebhookRepository(
                session,
                processing_token_generator=lambda value=f"token-{index}": value,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).claim_inbox("jobs-1")
        async with webhook_session_factory() as session, session.begin():
            await RuntimeWebhookRepository(
                session,
                id_generator=ids.__next__,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).apply_inbox_media(claim)

    async with webhook_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        evidences = list(
            (
                await session.scalars(
                    select(AiCallHandoffMediaEvidenceModel).order_by(
                        AiCallHandoffMediaEvidenceModel.media_state_version
                    )
                )
            ).all()
        )
        commands = list(
            (
                await session.scalars(
                    select(AiCallRuntimeCommandModel).order_by(
                        AiCallRuntimeCommandModel.command_seq
                    )
                )
            ).all()
        )
    assert handoff.status == "accepted"
    assert handoff.media_state_version == 3
    assert handoff.participant_identity == "human-agent-handoff-1"
    assert handoff.participant_sid == "PA_1"
    assert handoff.track_sid == "TR_1"
    assert [evidence.media_state_version for evidence in evidences] == [1, 2, 3]
    assert [command.command_type for command in commands] == [
        "AGENT_MEDIA_READY",
        "AGENT_MEDIA_READY",
        "AGENT_MEDIA_READY",
    ]


@pytest.mark.anyio
async def test_leave_unpublish_muted_increment_version_and_invalidate(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory, handoff_status="connected")
    base_time = datetime(2026, 8, 2, 3, 2, tzinfo=timezone.utc)
    events = (
        ("participant_left", "EV_left_1"),
        ("track_unpublished", "EV_unpublished_1"),
        ("track_muted", "EV_muted_1"),
    )
    ids = iter(range(9200, 9300))
    for index, (event_type, event_id) in enumerate(events):
        now = base_time + timedelta(seconds=index)
        intent = _receive_intent(
            dedupe_key=event_id,
            event_type=event_type,
            payload={
                "event": event_type,
                "id": event_id,
                "room": {"name": "ai-call-call-1"},
                "participant": {
                    "identity": "human-agent-handoff-1",
                    "sid": "PA_1",
                },
                "track": {"sid": "TR_1", "type": "AUDIO", "muted": True},
            },
        )
        async with webhook_session_factory() as session, session.begin():
            await RuntimeWebhookRepository(
                session,
                id_generator=ids.__next__,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).receive(intent)
        async with webhook_session_factory() as session, session.begin():
            claim = await RuntimeWebhookRepository(
                session,
                processing_token_generator=lambda value=f"invalid-{index}": value,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).claim_inbox("jobs-1")
        async with webhook_session_factory() as session, session.begin():
            await RuntimeWebhookRepository(
                session,
                id_generator=ids.__next__,
                database_clock=lambda _session, value=now: _constant_time(value),
            ).apply_inbox_media(claim)

    async with webhook_session_factory() as session:
        handoff = await session.scalar(select(AiCallHandoffModel))
        commands = list((await session.scalars(select(AiCallRuntimeCommandModel))).all())
    assert handoff.status == "connected"
    assert handoff.media_state_version == 3
    assert handoff.media_invalidated_at is not None
    assert all(command.command_type == "AGENT_MEDIA_INVALIDATED" for command in commands)


@pytest.mark.anyio
async def test_old_processing_token_cannot_write_evidence_or_command(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
        StaleWebhookClaimError,
    )

    await _seed_owner_record(webhook_session_factory)
    received_at = datetime(2026, 8, 2, 3, 3, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9300,
            database_clock=lambda _session: _constant_time(received_at),
        ).receive(_receive_intent())
    async with webhook_session_factory() as session, session.begin():
        old_claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "old-token",
            processing_lease_ttl=timedelta(seconds=1),
            database_clock=lambda _session: _constant_time(received_at),
        ).claim_inbox("old-worker")

    takeover_at = received_at + timedelta(seconds=2)
    async with webhook_session_factory() as session, session.begin():
        new_claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "new-token",
            database_clock=lambda _session: _constant_time(takeover_at),
        ).claim_inbox("new-worker")
    async with webhook_session_factory() as session:
        with pytest.raises(StaleWebhookClaimError):
            async with session.begin():
                await RuntimeWebhookRepository(
                    session,
                    id_generator=lambda: 9301,
                    database_clock=lambda _session: _constant_time(takeover_at),
                ).apply_inbox_media(old_claim)

    async with webhook_session_factory() as session:
        assert await session.scalar(
            select(func.count()).select_from(AiCallHandoffMediaEvidenceModel)
        ) == 0
        assert await session.scalar(
            select(func.count()).select_from(AiCallRuntimeCommandModel)
        ) == 0

    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=iter((9302, 9303)).__next__,
            database_clock=lambda _session: _constant_time(takeover_at),
        ).apply_inbox_media(new_claim)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("entry_type", "disconnect_reason", "expected_end_reason"),
    (
        ("web", None, "browser_disconnect"),
        ("direct_sip", None, "sip_participant_left"),
        ("direct_sip", "USER_UNAVAILABLE", "user_unavailable"),
    ),
)
async def test_customer_participant_left_uses_entry_specific_end_reason(
    webhook_session_factory,
    entry_type: str,
    disconnect_reason: str | None,
    expected_end_reason: str,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory, entry_type=entry_type)
    now = datetime(2026, 8, 2, 3, 4, tzinfo=timezone.utc)
    intent = _receive_intent(
        dedupe_key="EV_sip_left_1",
        event_type="participant_left",
        participant_identity="caller-call-1",
        payload={
            "event": "participant_left",
            "id": "EV_sip_left_1",
            "room": {"name": "ai-call-call-1"},
            "participant": {
                "identity": "caller-call-1",
                "sid": "PA_SIP_1",
                **(
                    {"disconnectReason": disconnect_reason}
                    if disconnect_reason
                    else {}
                ),
            },
        },
    )
    ids = iter((9400, 9401, 9402))
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=ids.__next__,
            database_clock=lambda _session: _constant_time(now),
        ).receive(intent)
    async with webhook_session_factory() as session, session.begin():
        claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "sip-token",
            database_clock=lambda _session: _constant_time(now),
        ).claim_inbox("jobs-1")
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=ids.__next__,
            database_clock=lambda _session: _constant_time(now),
        ).apply_inbox_media(claim)

    async with webhook_session_factory() as session:
        record = await session.scalar(select(AiCallRecordModel))
        commands = list((await session.scalars(select(AiCallRuntimeCommandModel))).all())
        evidence = await session.scalar(select(AiCallEndEvidenceModel))
        inbox = await session.scalar(select(AiCallWebhookInboxModel))
    assert record.status == "ending"
    assert record.terminal_requested_at.replace(tzinfo=timezone.utc) == now
    assert [command.command_type for command in commands] == ["END_CALL"]
    assert evidence.source == "livekit_webhook"
    assert evidence.end_reason == expected_end_reason
    assert evidence.provider_event_id == "EV_sip_left_1"
    if disconnect_reason:
        assert disconnect_reason in evidence.evidence_json
        assert record.failure_stage == "sip"
        assert record.failure_message == (
            "SIP 480 Temporarily Unavailable; hangup_cause=USER_UNAVAILABLE"
        )
    assert inbox.status == "SUCCEEDED"


@pytest.mark.anyio
async def test_old_quarantine_token_cannot_associate_after_takeover(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
        StaleWebhookClaimError,
    )

    received_at = datetime(2026, 8, 2, 3, 5, tzinfo=timezone.utc)
    intent = _receive_intent(
        dedupe_key="EV_late_1",
        room_name="ai-call-call-late",
        payload={
            "event": "participant_joined",
            "id": "EV_late_1",
            "room": {"name": "ai-call-call-late"},
            "participant": {"identity": "human-agent-handoff-late"},
        },
    )
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9500,
            database_clock=lambda _session: _constant_time(received_at),
        ).receive(intent)
    async with webhook_session_factory() as session, session.begin():
        old_claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "old-quarantine-token",
            processing_lease_ttl=timedelta(seconds=1),
            database_clock=lambda _session: _constant_time(received_at),
        ).claim_quarantine("old-worker")

    await _seed_owner_record(
        webhook_session_factory,
        with_handoff=False,
        record_id=102,
        call_id="call-late",
        room_name="ai-call-call-late",
    )
    takeover_at = received_at + timedelta(seconds=2)
    async with webhook_session_factory() as session, session.begin():
        new_claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "new-quarantine-token",
            database_clock=lambda _session: _constant_time(takeover_at),
        ).claim_quarantine("new-worker")

    async with webhook_session_factory() as session:
        with pytest.raises(StaleWebhookClaimError):
            async with session.begin():
                await RuntimeWebhookRepository(
                    session,
                    id_generator=lambda: 9501,
                    database_clock=lambda _session: _constant_time(takeover_at),
                ).resolve_quarantine(old_claim)
    async with webhook_session_factory() as session:
        assert await session.scalar(
            select(func.count()).select_from(AiCallWebhookInboxModel)
        ) == 0

    async with webhook_session_factory() as session, session.begin():
        decision = await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9502,
            database_clock=lambda _session: _constant_time(takeover_at),
        ).resolve_quarantine(new_claim)
    async with webhook_session_factory() as session:
        quarantine = await session.get(AiCallWebhookQuarantineModel, 9500)
        inbox = await session.scalar(select(AiCallWebhookInboxModel))
    assert decision.disposition == "INBOX"
    assert quarantine.status == "RESOLVED"
    assert quarantine.resolved_tenant_id == "tenant-a"
    assert quarantine.resolved_call_id == "call-late"
    assert inbox.call_id == "call-late"


@pytest.mark.anyio
async def test_quarantine_resolution_rechecks_database_time_after_downstream_locks(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
        StaleWebhookClaimError,
    )

    claimed_at = datetime(2026, 8, 2, 3, 6, tzinfo=timezone.utc)
    intent = _receive_intent(
        dedupe_key="EV_expire_during_resolve",
        room_name="ai-call-call-expiring",
        payload={
            "event": "participant_joined",
            "id": "EV_expire_during_resolve",
            "room": {"name": "ai-call-call-expiring"},
            "participant": {"identity": "human-agent-handoff-expiring"},
        },
    )
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=lambda: 9600,
            database_clock=lambda _session: _constant_time(claimed_at),
        ).receive(intent)
    async with webhook_session_factory() as session, session.begin():
        claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "expiring-token",
            processing_lease_ttl=timedelta(seconds=1),
            database_clock=lambda _session: _constant_time(claimed_at),
        ).claim_quarantine("worker-1")
    await _seed_owner_record(
        webhook_session_factory,
        with_handoff=False,
        record_id=103,
        call_id="call-expiring",
        room_name="ai-call-call-expiring",
    )

    clock_values = iter(
        (
            claimed_at + timedelta(milliseconds=500),
            claimed_at + timedelta(seconds=2),
        )
    )

    async def advancing_database_time(_session) -> datetime:
        return next(clock_values)

    async with webhook_session_factory() as session:
        with pytest.raises(StaleWebhookClaimError):
            async with session.begin():
                await RuntimeWebhookRepository(
                    session,
                    id_generator=lambda: 9601,
                    database_clock=advancing_database_time,
                ).resolve_quarantine(claim)

    async with webhook_session_factory() as session:
        assert await session.scalar(
            select(func.count()).select_from(AiCallWebhookInboxModel)
        ) == 0


@pytest.mark.anyio
async def test_media_event_after_terminal_barrier_is_immediately_superseded(
    webhook_session_factory,
) -> None:
    from app.services.ai_call.runtime_control.webhook_repository import (
        RuntimeWebhookRepository,
    )

    await _seed_owner_record(webhook_session_factory)
    now = datetime(2026, 8, 2, 3, 7, tzinfo=timezone.utc)
    async with webhook_session_factory() as session, session.begin():
        record = await session.scalar(select(AiCallRecordModel).with_for_update())
        record.status = "ending"
        record.terminal_requested_at = now

    ids = iter((9700, 9701, 9702))
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=ids.__next__,
            database_clock=lambda _session: _constant_time(now),
        ).receive(_receive_intent(dedupe_key="EV_after_terminal"))
    async with webhook_session_factory() as session, session.begin():
        claim = await RuntimeWebhookRepository(
            session,
            processing_token_generator=lambda: "terminal-token",
            database_clock=lambda _session: _constant_time(now),
        ).claim_inbox("jobs-1")
    async with webhook_session_factory() as session, session.begin():
        await RuntimeWebhookRepository(
            session,
            id_generator=ids.__next__,
            database_clock=lambda _session: _constant_time(now),
        ).apply_inbox_media(claim)

    async with webhook_session_factory() as session:
        command = await session.scalar(select(AiCallRuntimeCommandModel))
        evidence = await session.scalar(select(AiCallHandoffMediaEvidenceModel))
        handoff = await session.scalar(select(AiCallHandoffModel))
    assert evidence is not None
    assert handoff.media_state_version == 1
    assert command.command_type == "AGENT_MEDIA_READY"
    assert command.status == "SUPERSEDED"
    assert command.finished_at == now.replace(tzinfo=None)


async def _constant_time(value: datetime) -> datetime:
    return value
