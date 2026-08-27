from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallHandoffModel, AiCallRecordModel
from app.services.ai_call.runtime_control.command_repository import (
    EndCallIntent,
    RuntimeCommandRepository,
    canonical_request_fingerprint,
)
from app.services.ai_call.runtime_control.models import (
    AiCallHandoffMediaEvidenceModel,
    AiCallRuntimeCommandModel,
    AiCallWebhookInboxModel,
    AiCallWebhookQuarantineModel,
)
from app.services.ai_call.runtime_control.timing import read_database_time
from app.services.ai_call.runtime_control.types import CommandStatus
from app.utils.id_util import generate_snowflake_id


@dataclass(frozen=True, slots=True)
class WebhookReceiveIntent:
    provider: str
    provider_namespace: str
    dedupe_key: str
    event_type: str
    room_name: str | None
    participant_identity: str | None
    payload: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class WebhookReceiveDecision:
    disposition: str
    row_id: int | None
    status: str | None


@dataclass(frozen=True, slots=True)
class InboxClaim:
    inbox_id: int
    tenant_id: str
    call_id: str | None
    provider: str
    provider_namespace: str
    dedupe_key: str
    event_type: str
    payload_json: str | None
    processing_owner_id: str
    processing_token: str
    processing_expires_at: datetime
    attempt_count: int


@dataclass(frozen=True, slots=True)
class QuarantineClaim:
    quarantine_id: int
    provider: str
    provider_namespace: str
    dedupe_key: str
    room_name: str | None
    participant_identity: str | None
    event_type: str
    payload_json: str | None
    processing_owner_id: str
    processing_generation: int
    processing_token: str
    processing_expires_at: datetime
    attempt_count: int


@dataclass(frozen=True, slots=True)
class MediaEventDecision:
    inbox_id: int
    handoff_id: str | None
    media_state_version: int | None
    evidence_id: int | None
    command_id: int | None
    command_type: str | None


class StaleWebhookClaimError(RuntimeError):
    pass


class WebhookMediaContractError(RuntimeError):
    pass


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def sanitize_webhook_payload(
    *,
    event_type: str,
    payload: Mapping[str, object] | None,
) -> dict[str, object]:
    source = payload or {}
    sanitized: dict[str, object] = {"event": event_type}
    for key in ("id", "createdAt", "created_at"):
        value = source.get(key)
        if isinstance(value, (str, int, float)):
            sanitized[key] = value
    for key, allowed in (
        ("room", ("name", "sid")),
        ("participant", ("identity", "sid", "kind", "disconnectReason")),
        ("track", ("sid", "type", "source", "muted")),
    ):
        value = source.get(key)
        if not isinstance(value, Mapping):
            continue
        child = {
            field: field_value
            for field in allowed
            if isinstance((field_value := value.get(field)), (str, int, float, bool))
        }
        if child:
            sanitized[key] = child
    return sanitized


class RuntimeWebhookRepository:
    """持久接收和短租约认领；Provider 调用不进入数据库事务。"""

    def __init__(
        self,
        session: AsyncSession,
        *,
        id_generator: Callable[[], int] = generate_snowflake_id,
        processing_token_generator: Callable[[], str] = lambda: secrets.token_urlsafe(24),
        processing_lease_ttl: timedelta = timedelta(seconds=30),
        database_clock: Callable[[AsyncSession], Awaitable[datetime]] = read_database_time,
        managed_room_prefix: str = "ai-call-",
    ) -> None:
        self._session = session
        self._id_generator = id_generator
        self._processing_token_generator = processing_token_generator
        self._processing_lease_ttl = processing_lease_ttl
        self._database_clock = database_clock
        self._managed_room_prefix = managed_room_prefix

    async def receive(self, request: WebhookReceiveIntent) -> WebhookReceiveDecision:
        record = await self._lock_record_by_room(request.room_name)
        if record is not None:
            if record.runtime_control_mode != "owner_command_v1":
                return WebhookReceiveDecision("LEGACY", None, None)
            if not record.tenant_id:
                raise RuntimeError("owner mode webhook record is missing tenant_id")
            return await self._receive_inbox(request, record)
        if not request.room_name or not request.room_name.startswith(
            self._managed_room_prefix
        ):
            return WebhookReceiveDecision("IGNORED", None, None)
        return await self._receive_quarantine(request)

    async def claim_inbox(self, worker_id: str) -> InboxClaim | None:
        candidate_time = await self._database_clock(self._session)
        row = await self._session.scalar(
            select(AiCallWebhookInboxModel)
            .where(
                or_(
                    AiCallWebhookInboxModel.status == "RECEIVED",
                    and_(
                        AiCallWebhookInboxModel.status == "RETRY_WAIT",
                        AiCallWebhookInboxModel.next_retry_at.is_not(None),
                        AiCallWebhookInboxModel.next_retry_at <= candidate_time,
                    ),
                    and_(
                        AiCallWebhookInboxModel.status == "PROCESSING",
                        AiCallWebhookInboxModel.processing_expires_at.is_not(None),
                        AiCallWebhookInboxModel.processing_expires_at <= candidate_time,
                    ),
                )
            )
            .order_by(AiCallWebhookInboxModel.received_at, AiCallWebhookInboxModel.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        now = await self._database_clock(self._session)
        if not self._inbox_claimable(row, now):
            return None
        token = self._processing_token_generator()
        row.status = "PROCESSING"
        row.processing_owner_id = worker_id
        row.processing_token = token
        row.processing_expires_at = now + self._processing_lease_ttl
        row.attempt_count += 1
        row.claimed_at = now
        row.next_retry_at = None
        row.error_message = None
        await self._session.flush()
        return InboxClaim(
            inbox_id=row.id,
            tenant_id=row.tenant_id,
            call_id=row.call_id,
            provider=row.provider,
            provider_namespace=row.provider_namespace,
            dedupe_key=row.dedupe_key,
            event_type=row.event_type,
            payload_json=row.payload_json,
            processing_owner_id=worker_id,
            processing_token=token,
            processing_expires_at=row.processing_expires_at,
            attempt_count=row.attempt_count,
        )

    async def apply_inbox_media(self, claim: InboxClaim) -> MediaEventDecision:
        if claim.call_id is None:
            raise WebhookMediaContractError("media inbox is missing call_id")
        payload = self._decode_payload(claim.payload_json)
        participant = payload.get("participant")
        track = payload.get("track")
        participant_data = participant if isinstance(participant, dict) else {}
        track_data = track if isinstance(track, dict) else {}
        participant_identity = self._optional_string(participant_data.get("identity"))
        disconnect_reason = self._optional_string(
            participant_data.get("disconnectReason")
        )

        record = await self._session.scalar(
            select(AiCallRecordModel)
            .where(
                AiCallRecordModel.tenant_id == claim.tenant_id,
                AiCallRecordModel.call_id == claim.call_id,
            )
            .with_for_update()
        )
        if record is None or record.runtime_control_mode != "owner_command_v1":
            raise WebhookMediaContractError("media inbox record is missing or not owner mode")
        if (
            claim.event_type == "participant_left"
            and participant_identity
            and participant_identity == record.participant_identity
        ):
            user_unavailable = (
                record.entry_type != "web"
                and str(disconnect_reason or "").strip().upper()
                == "USER_UNAVAILABLE"
            )
            if user_unavailable:
                record.failure_stage = record.failure_stage or "sip"
                record.failure_message = record.failure_message or (
                    "SIP 480 Temporarily Unavailable; "
                    "hangup_cause=USER_UNAVAILABLE"
                )
            decision = await RuntimeCommandRepository(
                self._session,
                id_generator=self._id_generator,
                database_clock=self._database_clock,
            ).request_end(
                EndCallIntent(
                    tenant_id=claim.tenant_id,
                    call_id=claim.call_id,
                    source="livekit_webhook",
                    end_reason=(
                        "browser_disconnect"
                        if record.entry_type == "web"
                        else (
                            "user_unavailable"
                            if user_unavailable
                            else "sip_participant_left"
                        )
                    ),
                    dedupe_key=self._end_dedupe_key(claim),
                    provider=claim.provider,
                    provider_namespace=claim.provider_namespace,
                    provider_event_id=claim.dedupe_key,
                    event_at=self._event_at(payload),
                    evidence=payload,
                )
            )
            inbox = await self._lock_claimed_inbox(claim)
            self._mark_inbox_succeeded(inbox, await self._database_clock(self._session))
            await self._session.flush()
            return MediaEventDecision(
                claim.inbox_id,
                None,
                None,
                decision.evidence_id,
                decision.command_id,
                "END_CALL",
            )
        handoffs = list(
            (
                await self._session.scalars(
                    select(AiCallHandoffModel)
                    .where(
                        AiCallHandoffModel.tenant_id == claim.tenant_id,
                        AiCallHandoffModel.call_id == claim.call_id,
                        AiCallHandoffModel.status.in_(
                            ("requested", "accepted", "connected", "reconnecting")
                        ),
                    )
                    .order_by(AiCallHandoffModel.handoff_id)
                    .with_for_update()
                )
            ).all()
        )
        handoff = next(
            (
                candidate
                for candidate in handoffs
                if participant_identity
                == (
                    candidate.participant_identity
                    or f"human-agent-{candidate.handoff_id}"
                )
            ),
            None,
        )
        event_kind = self._media_event_kind(claim.event_type, track_data)
        if handoff is None or event_kind is None:
            inbox = await self._lock_claimed_inbox(claim)
            self._mark_inbox_succeeded(inbox, await self._database_clock(self._session))
            await self._session.flush()
            return MediaEventDecision(claim.inbox_id, None, None, None, None, None)

        version = handoff.media_state_version + 1
        command_type = (
            "AGENT_MEDIA_READY"
            if event_kind == "ready"
            else "AGENT_MEDIA_INVALIDATED"
        )
        evidence_id = self._id_generator()
        command_payload = {
            "evidence_id": str(evidence_id),
            "handoff_id": handoff.handoff_id,
            "media_state_version": version,
        }
        idempotency_key = self._media_command_idempotency_key(claim, command_type)
        fingerprint = canonical_request_fingerprint(
            {
                "tenant_id": claim.tenant_id,
                "call_id": claim.call_id,
                "command_type": command_type,
                "payload": command_payload,
            }
        )
        command = await self._session.scalar(
            select(AiCallRuntimeCommandModel)
            .where(
                AiCallRuntimeCommandModel.tenant_id == claim.tenant_id,
                AiCallRuntimeCommandModel.idempotency_key == idempotency_key,
            )
            .with_for_update()
        )
        evidence = await self._session.scalar(
            select(AiCallHandoffMediaEvidenceModel)
            .where(
                AiCallHandoffMediaEvidenceModel.tenant_id == claim.tenant_id,
                AiCallHandoffMediaEvidenceModel.provider_namespace
                == claim.provider_namespace,
                AiCallHandoffMediaEvidenceModel.dedupe_key == claim.dedupe_key,
            )
            .with_for_update()
        )
        inbox = await self._lock_claimed_inbox(claim)
        now = await self._database_clock(self._session)
        if command is not None or evidence is not None:
            if (
                command is None
                or evidence is None
                or command.command_type != command_type
                or command.request_fingerprint != fingerprint
                or evidence.handoff_id != handoff.handoff_id
                or evidence.media_state_version != version
            ):
                raise WebhookMediaContractError(
                    "media event has a partial or conflicting persisted result"
                )
            self._mark_inbox_succeeded(inbox, now)
            await self._session.flush()
            return MediaEventDecision(
                claim.inbox_id,
                handoff.handoff_id,
                version,
                evidence.id,
                command.id,
                command.command_type,
            )

        participant_sid = self._optional_string(participant_data.get("sid"))
        track_sid = self._optional_string(track_data.get("sid"))
        evidence = AiCallHandoffMediaEvidenceModel(
            id=evidence_id,
            tenant_id=claim.tenant_id,
            call_id=claim.call_id,
            handoff_id=handoff.handoff_id,
            provider_namespace=claim.provider_namespace,
            dedupe_key=claim.dedupe_key,
            participant_identity=participant_identity,
            participant_sid=participant_sid,
            track_sid=track_sid,
            event_type=claim.event_type,
            media_state_version=version,
            provider_event_id=claim.dedupe_key,
            event_at=self._event_at(payload),
            received_at=inbox.received_at,
            evidence_json=inbox.payload_json,
        )
        command = AiCallRuntimeCommandModel(
            id=self._id_generator(),
            tenant_id=claim.tenant_id,
            call_id=claim.call_id,
            command_seq=record.next_command_seq,
            command_type=command_type,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            dispatch_priority=100,
            payload_json=_canonical_json(command_payload),
            expected_fencing_token=record.runtime_fencing_token,
            target_owner_id=record.runtime_owner_id,
            status=(
                CommandStatus.SUPERSEDED
                if record.terminal_requested_at is not None
                else CommandStatus.PENDING
            ),
            finished_at=(now if record.terminal_requested_at is not None else None),
            created_at=now,
            updated_at=now,
        )
        handoff.media_state_version = version
        handoff.participant_identity = participant_identity
        if participant_sid is not None:
            handoff.participant_sid = participant_sid
        if track_sid is not None:
            handoff.track_sid = track_sid
        handoff.evidence_source = "livekit_webhook"
        handoff.last_media_event_key = claim.dedupe_key
        if event_kind == "invalidated":
            handoff.media_invalidated_at = now
        record.next_command_seq += 1
        self._session.add_all((evidence, command))
        self._mark_inbox_succeeded(inbox, now)
        await self._session.flush()
        return MediaEventDecision(
            claim.inbox_id,
            handoff.handoff_id,
            version,
            evidence.id,
            command.id,
            command.command_type,
        )

    async def claim_quarantine(self, worker_id: str) -> QuarantineClaim | None:
        candidate_time = await self._database_clock(self._session)
        row = await self._session.scalar(
            select(AiCallWebhookQuarantineModel)
            .where(
                or_(
                    AiCallWebhookQuarantineModel.status == "UNMATCHED",
                    and_(
                        AiCallWebhookQuarantineModel.status == "RETRY_WAIT",
                        AiCallWebhookQuarantineModel.next_retry_at.is_not(None),
                        AiCallWebhookQuarantineModel.next_retry_at <= candidate_time,
                    ),
                    and_(
                        AiCallWebhookQuarantineModel.status == "PROCESSING",
                        AiCallWebhookQuarantineModel.processing_expires_at.is_not(None),
                        AiCallWebhookQuarantineModel.processing_expires_at
                        <= candidate_time,
                    ),
                )
            )
            .order_by(
                AiCallWebhookQuarantineModel.received_at,
                AiCallWebhookQuarantineModel.id,
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return None
        now = await self._database_clock(self._session)
        if not self._quarantine_claimable(row, now):
            return None
        token = self._processing_token_generator()
        row.status = "PROCESSING"
        row.processing_owner_id = worker_id
        row.processing_generation += 1
        row.processing_token = token
        row.processing_expires_at = now + self._processing_lease_ttl
        row.attempt_count += 1
        row.claimed_at = now
        row.next_retry_at = None
        row.error_message = None
        await self._session.flush()
        return QuarantineClaim(
            quarantine_id=row.id,
            provider=row.provider,
            provider_namespace=row.provider_namespace,
            dedupe_key=row.dedupe_key,
            room_name=row.room_name,
            participant_identity=row.participant_identity,
            event_type=row.event_type,
            payload_json=row.payload_json,
            processing_owner_id=worker_id,
            processing_generation=row.processing_generation,
            processing_token=token,
            processing_expires_at=row.processing_expires_at,
            attempt_count=row.attempt_count,
        )

    async def resolve_quarantine(
        self,
        claim: QuarantineClaim,
    ) -> WebhookReceiveDecision:
        record = await self._lock_record_by_room(claim.room_name)
        now = await self._database_clock(self._session)
        if record is None:
            quarantine, now = await self._lock_claimed_quarantine(claim)
            quarantine.status = "RETRY_WAIT"
            quarantine.next_retry_at = now + timedelta(seconds=1)
            quarantine.processing_owner_id = None
            quarantine.processing_token = None
            quarantine.processing_expires_at = None
            await self._session.flush()
            return WebhookReceiveDecision("QUARANTINE", quarantine.id, quarantine.status)

        await self._validate_quarantine_claim_snapshot(claim, now)
        if record.runtime_control_mode == "owner_command_v1":
            if not record.tenant_id:
                raise RuntimeError("owner mode webhook record is missing tenant_id")
            request = WebhookReceiveIntent(
                provider=claim.provider,
                provider_namespace=claim.provider_namespace,
                dedupe_key=claim.dedupe_key,
                event_type=claim.event_type,
                room_name=claim.room_name,
                participant_identity=claim.participant_identity,
                payload=self._decode_payload(claim.payload_json),
            )
            inbox = await self._find_inbox(request)
            quarantine, now = await self._lock_claimed_quarantine(claim)
            if inbox is None:
                inbox = self._new_inbox_row(
                    request,
                    record,
                    received_at=quarantine.received_at,
                )
                self._session.add(inbox)
            quarantine.status = "RESOLVED"
            quarantine.resolved_tenant_id = record.tenant_id
            quarantine.resolved_call_id = record.call_id
            quarantine.resolved_at = now
            quarantine.next_retry_at = None
            quarantine.processing_owner_id = None
            quarantine.processing_token = None
            quarantine.processing_expires_at = None
            quarantine.error_message = None
            await self._session.flush()
            return WebhookReceiveDecision("INBOX", inbox.id, inbox.status)

        quarantine, now = await self._lock_claimed_quarantine(claim)
        quarantine.status = "IGNORED"
        quarantine.resolved_at = now
        quarantine.next_retry_at = None
        quarantine.processing_owner_id = None
        quarantine.processing_token = None
        quarantine.processing_expires_at = None
        quarantine.error_message = None
        await self._session.flush()
        return WebhookReceiveDecision("LEGACY", None, quarantine.status)

    async def _lock_record_by_room(
        self,
        room_name: str | None,
    ) -> AiCallRecordModel | None:
        if not room_name:
            return None
        return await self._session.scalar(
            select(AiCallRecordModel)
            .where(AiCallRecordModel.room_name == room_name)
            .with_for_update()
        )

    async def _lock_claimed_inbox(
        self,
        claim: InboxClaim,
    ) -> AiCallWebhookInboxModel:
        inbox = await self._session.scalar(
            select(AiCallWebhookInboxModel)
            .where(AiCallWebhookInboxModel.id == claim.inbox_id)
            .with_for_update()
        )
        now = await self._database_clock(self._session)
        if (
            inbox is None
            or inbox.status != "PROCESSING"
            or inbox.processing_owner_id != claim.processing_owner_id
            or inbox.processing_token != claim.processing_token
            or inbox.processing_expires_at is None
            or _ensure_utc(inbox.processing_expires_at) <= _ensure_utc(now)
            or inbox.tenant_id != claim.tenant_id
            or inbox.call_id != claim.call_id
            or inbox.provider != claim.provider
            or inbox.provider_namespace != claim.provider_namespace
            or inbox.dedupe_key != claim.dedupe_key
            or inbox.event_type != claim.event_type
        ):
            raise StaleWebhookClaimError("webhook inbox claim is stale")
        return inbox

    async def _lock_claimed_quarantine(
        self,
        claim: QuarantineClaim,
    ) -> tuple[AiCallWebhookQuarantineModel, datetime]:
        quarantine = await self._session.scalar(
            select(AiCallWebhookQuarantineModel)
            .where(AiCallWebhookQuarantineModel.id == claim.quarantine_id)
            .with_for_update()
        )
        now = await self._database_clock(self._session)
        if not self._quarantine_claim_matches(quarantine, claim, now):
            raise StaleWebhookClaimError("webhook quarantine claim is stale")
        assert quarantine is not None
        return quarantine, now

    async def _validate_quarantine_claim_snapshot(
        self,
        claim: QuarantineClaim,
        now: datetime,
    ) -> None:
        quarantine = await self._session.get(
            AiCallWebhookQuarantineModel,
            claim.quarantine_id,
        )
        if not self._quarantine_claim_matches(quarantine, claim, now):
            raise StaleWebhookClaimError("webhook quarantine claim is stale")

    @staticmethod
    def _quarantine_claim_matches(
        quarantine: AiCallWebhookQuarantineModel | None,
        claim: QuarantineClaim,
        now: datetime,
    ) -> bool:
        return bool(
            quarantine is not None
            and quarantine.status == "PROCESSING"
            and quarantine.processing_owner_id == claim.processing_owner_id
            and quarantine.processing_generation == claim.processing_generation
            and quarantine.processing_token == claim.processing_token
            and quarantine.processing_expires_at is not None
            and _ensure_utc(quarantine.processing_expires_at) > _ensure_utc(now)
            and quarantine.provider == claim.provider
            and quarantine.provider_namespace == claim.provider_namespace
            and quarantine.dedupe_key == claim.dedupe_key
            and quarantine.room_name == claim.room_name
            and quarantine.participant_identity == claim.participant_identity
            and quarantine.event_type == claim.event_type
            and quarantine.payload_json == claim.payload_json
        )

    @staticmethod
    def _mark_inbox_succeeded(inbox: AiCallWebhookInboxModel, now: datetime) -> None:
        inbox.status = "SUCCEEDED"
        inbox.processed_at = now
        inbox.processing_owner_id = None
        inbox.processing_token = None
        inbox.processing_expires_at = None
        inbox.next_retry_at = None
        inbox.error_message = None

    @staticmethod
    def _decode_payload(payload_json: str | None) -> dict[str, object]:
        if payload_json is None:
            return {}
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as exc:
            raise WebhookMediaContractError("webhook payload_json is invalid") from exc
        if not isinstance(payload, dict):
            raise WebhookMediaContractError("webhook payload_json must be an object")
        return payload

    @staticmethod
    def _optional_string(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _media_event_kind(
        event_type: str,
        track: Mapping[str, object],
    ) -> str | None:
        if event_type in {"participant_left", "track_unpublished", "track_muted"}:
            return "invalidated"
        if event_type == "participant_joined":
            return "ready"
        if event_type in {"track_published", "track_unmuted"}:
            track_type = str(track.get("type") or "").upper()
            if track_type and track_type != "AUDIO":
                return None
            if event_type == "track_published" and track.get("muted") is True:
                return None
            return "ready"
        return None

    @staticmethod
    def _media_command_idempotency_key(
        claim: InboxClaim,
        command_type: str,
    ) -> str:
        digest = hashlib.sha256(
            (
                f"{claim.tenant_id}|{claim.call_id}|{claim.provider_namespace}|"
                f"{claim.dedupe_key}|{command_type}"
            ).encode()
        ).hexdigest()
        return f"media:{digest}"

    @staticmethod
    def _end_dedupe_key(claim: InboxClaim) -> str:
        digest = hashlib.sha256(
            (
                f"{claim.provider}|{claim.provider_namespace}|{claim.dedupe_key}"
            ).encode()
        ).hexdigest()
        return f"webhook:{digest}"

    @staticmethod
    def _event_at(payload: Mapping[str, object]) -> datetime | None:
        value = payload.get("createdAt", payload.get("created_at"))
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return None

    async def _receive_inbox(
        self,
        request: WebhookReceiveIntent,
        record: AiCallRecordModel,
    ) -> WebhookReceiveDecision:
        existing = await self._find_inbox(request)
        if existing is not None:
            return WebhookReceiveDecision("INBOX", existing.id, existing.status)
        now = await self._database_clock(self._session)
        row = self._new_inbox_row(request, record, received_at=now)
        return await self._insert_inbox_or_read_winner(request, row)

    def _new_inbox_row(
        self,
        request: WebhookReceiveIntent,
        record: AiCallRecordModel,
        *,
        received_at: datetime,
    ) -> AiCallWebhookInboxModel:
        return AiCallWebhookInboxModel(
            id=self._id_generator(),
            provider=request.provider,
            provider_namespace=request.provider_namespace,
            dedupe_key=request.dedupe_key,
            tenant_id=record.tenant_id,
            call_id=record.call_id,
            event_type=request.event_type,
            payload_json=_canonical_json(
                sanitize_webhook_payload(
                    event_type=request.event_type,
                    payload=request.payload,
                )
            ),
            status="RECEIVED",
            received_at=received_at,
        )

    async def _receive_quarantine(
        self,
        request: WebhookReceiveIntent,
    ) -> WebhookReceiveDecision:
        existing = await self._find_quarantine(request)
        if existing is not None:
            return WebhookReceiveDecision("QUARANTINE", existing.id, existing.status)
        now = await self._database_clock(self._session)
        row = AiCallWebhookQuarantineModel(
            id=self._id_generator(),
            provider=request.provider,
            provider_namespace=request.provider_namespace,
            dedupe_key=request.dedupe_key,
            room_name=request.room_name,
            participant_identity=request.participant_identity,
            event_type=request.event_type,
            payload_json=_canonical_json(
                sanitize_webhook_payload(
                    event_type=request.event_type,
                    payload=request.payload,
                )
            ),
            status="UNMATCHED",
            received_at=now,
        )
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError:
            winner = await self._find_quarantine(request)
            if winner is None:
                raise
            return WebhookReceiveDecision("QUARANTINE", winner.id, winner.status)
        return WebhookReceiveDecision("QUARANTINE", row.id, row.status)

    async def _insert_inbox_or_read_winner(
        self,
        request: WebhookReceiveIntent,
        row: AiCallWebhookInboxModel,
    ) -> WebhookReceiveDecision:
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError:
            winner = await self._find_inbox(request)
            if winner is None:
                raise
            return WebhookReceiveDecision("INBOX", winner.id, winner.status)
        return WebhookReceiveDecision("INBOX", row.id, row.status)

    async def _find_inbox(
        self,
        request: WebhookReceiveIntent,
    ) -> AiCallWebhookInboxModel | None:
        return await self._session.scalar(
            select(AiCallWebhookInboxModel)
            .where(
                AiCallWebhookInboxModel.provider == request.provider,
                AiCallWebhookInboxModel.provider_namespace
                == request.provider_namespace,
                AiCallWebhookInboxModel.dedupe_key == request.dedupe_key,
            )
            .with_for_update()
        )

    async def _find_quarantine(
        self,
        request: WebhookReceiveIntent,
    ) -> AiCallWebhookQuarantineModel | None:
        return await self._session.scalar(
            select(AiCallWebhookQuarantineModel)
            .where(
                AiCallWebhookQuarantineModel.provider == request.provider,
                AiCallWebhookQuarantineModel.provider_namespace
                == request.provider_namespace,
                AiCallWebhookQuarantineModel.dedupe_key == request.dedupe_key,
            )
            .with_for_update()
        )

    @staticmethod
    def _inbox_claimable(row: AiCallWebhookInboxModel, now: datetime) -> bool:
        if row.status == "RECEIVED":
            return True
        if row.status == "RETRY_WAIT":
            return (
                row.next_retry_at is not None
                and _ensure_utc(row.next_retry_at) <= _ensure_utc(now)
            )
        return (
            row.status == "PROCESSING"
            and row.processing_expires_at is not None
            and _ensure_utc(row.processing_expires_at) <= _ensure_utc(now)
        )

    @staticmethod
    def _quarantine_claimable(
        row: AiCallWebhookQuarantineModel,
        now: datetime,
    ) -> bool:
        if row.status == "UNMATCHED":
            return True
        if row.status == "RETRY_WAIT":
            return (
                row.next_retry_at is not None
                and _ensure_utc(row.next_retry_at) <= _ensure_utc(now)
            )
        return (
            row.status == "PROCESSING"
            and row.processing_expires_at is not None
            and _ensure_utc(row.processing_expires_at) <= _ensure_utc(now)
        )
