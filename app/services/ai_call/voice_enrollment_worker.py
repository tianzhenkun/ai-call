from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from sqlalchemy import and_, case, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceEnrollmentModel,
    AiCallVoiceSampleCleanupModel,
)
from app.services.ai_call.providers.qwen_voice_enrollment import (
    VoiceCreateResult,
    VoiceListItem,
    VoiceProviderProtocolError,
    VoiceProviderRejectedError,
    VoiceProviderResultUnknownError,
    VoiceProviderRetryableError,
)
from app.services.ai_call.voice_sample import VoiceSampleStorage

RETRY_DELAYS = (5, 30, 120)
MAX_BATCH_SIZE = 100
DEFAULT_LIST_PAGE_SIZE = 1000
ENROLLMENT_FAILURE_MESSAGE = "音色创建失败，请重新上传声音样本"
ENROLLMENT_RETRY_MESSAGE = "音色创建暂时失败，等待自动重试"
SAMPLE_CLEANUP_ERROR_MESSAGE = "声音样本清理失败，等待后台重试"

_TERMINAL_ENROLLMENT_STATUSES = ("SUCCEEDED", "FAILED")
_SAMPLE_CONTENT_TYPES = {
    ".wav": "audio/wav",
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
}


class VoiceEnrollmentProvider(Protocol):
    async def create(
        self,
        *,
        preferred_name: str,
        audio_data_url: str,
    ) -> VoiceCreateResult:
        raise NotImplementedError

    async def list(
        self,
        *,
        page_index: int = 0,
        page_size: int = DEFAULT_LIST_PAGE_SIZE,
    ) -> list[VoiceListItem]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class EnrollmentWorkItem:
    id: int
    tenant_id: str
    voice_profile_id: int
    preferred_name: str
    target_model: str
    sample_object_key: str | None
    status: str
    attempt_count: int
    lease_owner: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class CleanupWorkItem:
    id: int
    tenant_id: str
    object_key: str
    attempt_count: int
    lease_owner: str


class VoiceEnrollmentWorker:
    def __init__(
        self,
        *,
        session_factory: Callable[
            [],
            AbstractAsyncContextManager[AsyncSession],
        ],
        provider: VoiceEnrollmentProvider,
        storage: VoiceSampleStorage,
        worker_id: str,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        batch_size: int = 20,
        lease_seconds: int = 60,
    ) -> None:
        self.session_factory = session_factory
        self.provider = provider
        self.storage = storage
        self.worker_id = worker_id
        self.now = now
        self.batch_size = min(MAX_BATCH_SIZE, max(1, batch_size))
        self.lease_seconds = max(1, lease_seconds)

    async def run_once(self) -> None:
        await self._cleanup_terminal_samples()
        enrollments = await self._claim_enrollments()
        await asyncio.gather(
            *(self._process_enrollment(item) for item in enrollments),
            return_exceptions=True,
        )
        cleanup_records = await self._claim_cleanup_records()
        await asyncio.gather(
            *(self._process_cleanup_record(item) for item in cleanup_records),
            return_exceptions=True,
        )

    @staticmethod
    def _enrollment_claim_select(
        *,
        now: datetime,
        batch_size: int,
    ) -> Select:
        return (
            select(AiCallVoiceEnrollmentModel)
            .where(VoiceEnrollmentWorker._enrollment_is_claimable(now))
            .order_by(AiCallVoiceEnrollmentModel.id)
            .limit(min(MAX_BATCH_SIZE, max(1, batch_size)))
            .with_for_update(skip_locked=True)
        )

    @staticmethod
    def _enrollment_is_claimable(now: datetime):
        return or_(
            AiCallVoiceEnrollmentModel.status == "PENDING",
            and_(
                AiCallVoiceEnrollmentModel.status == "RETRY_WAIT",
                AiCallVoiceEnrollmentModel.next_retry_at <= now,
            ),
            and_(
                AiCallVoiceEnrollmentModel.status.in_(("PROCESSING", "RECONCILING")),
                AiCallVoiceEnrollmentModel.lease_expires_at < now,
            ),
        )

    async def _claim_enrollments(self) -> list[EnrollmentWorkItem]:
        now = self.now()
        lease_expires_at = now + timedelta(seconds=self.lease_seconds)
        async with self.session_factory() as database:
            dialect_name = database.bind.dialect.name if database.bind else ""
            if dialect_name == "postgresql":
                rows = (
                    await database.scalars(
                        self._enrollment_claim_select(
                            now=now,
                            batch_size=self.batch_size,
                        )
                    )
                ).all()
                for row in rows:
                    self._claim_enrollment_row(
                        row,
                        now=now,
                        lease_expires_at=lease_expires_at,
                    )
            else:
                candidate_ids = (
                    select(AiCallVoiceEnrollmentModel.id)
                    .where(self._enrollment_is_claimable(now))
                    .order_by(AiCallVoiceEnrollmentModel.id)
                    .limit(self.batch_size)
                )
                rows = (
                    await database.scalars(
                        update(AiCallVoiceEnrollmentModel)
                        .where(
                            AiCallVoiceEnrollmentModel.id.in_(candidate_ids),
                            self._enrollment_is_claimable(now),
                        )
                        .values(
                            status=case(
                                (
                                    AiCallVoiceEnrollmentModel.status == "RECONCILING",
                                    "RECONCILING",
                                ),
                                else_="PROCESSING",
                            ),
                            attempt_count=(AiCallVoiceEnrollmentModel.attempt_count + 1),
                            lease_owner=self.worker_id,
                            lease_expires_at=lease_expires_at,
                            started_at=func.coalesce(
                                AiCallVoiceEnrollmentModel.started_at,
                                now,
                            ),
                            next_retry_at=None,
                            updated_at=now,
                        )
                        .returning(AiCallVoiceEnrollmentModel)
                    )
                ).all()
            profile_target_models: dict[int, str] = {}
            if rows:
                profile_target_models = dict(
                    (
                        await database.execute(
                            select(
                                AiCallTenantVoiceProfileModel.id,
                                AiCallTenantVoiceProfileModel.target_model,
                            ).where(
                                AiCallTenantVoiceProfileModel.id.in_({
                                    row.voice_profile_id for row in rows
                                })
                            )
                        )
                    ).all()
                )
            await database.commit()
            return sorted(
                (
                    self._enrollment_work_item(
                        row,
                        target_model=profile_target_models.get(
                            row.voice_profile_id,
                            "",
                        ),
                    )
                    for row in rows
                ),
                key=lambda item: item.id,
            )

    def _claim_enrollment_row(
        self,
        row: AiCallVoiceEnrollmentModel,
        *,
        now: datetime,
        lease_expires_at: datetime,
    ) -> None:
        if row.status != "RECONCILING":
            row.status = "PROCESSING"
        row.attempt_count += 1
        row.lease_owner = self.worker_id
        row.lease_expires_at = lease_expires_at
        row.started_at = row.started_at or now
        row.next_retry_at = None
        row.updated_at = now

    @staticmethod
    def _enrollment_work_item(
        row: AiCallVoiceEnrollmentModel,
        *,
        target_model: str,
    ) -> EnrollmentWorkItem:
        return EnrollmentWorkItem(
            id=row.id,
            tenant_id=row.tenant_id,
            voice_profile_id=row.voice_profile_id,
            preferred_name=row.preferred_name,
            target_model=target_model,
            sample_object_key=row.sample_object_key,
            status=row.status,
            attempt_count=row.attempt_count,
            lease_owner=row.lease_owner or "",
            lease_expires_at=_as_utc(row.lease_expires_at),
        )

    async def _process_enrollment(self, item: EnrollmentWorkItem) -> None:
        if item.status == "RECONCILING":
            await self._reconcile(item)
            return
        if item.sample_object_key is None:
            await self._fail_enrollment(item)
            return
        try:
            sample = await self.storage.get(item.sample_object_key)
        except Exception:
            await self._retry_or_fail(item, reconciliation=False)
            return
        try:
            result = await self.provider.create(
                preferred_name=item.preferred_name,
                audio_data_url=_audio_data_url(
                    sample,
                    item.sample_object_key,
                ),
            )
        except VoiceProviderResultUnknownError:
            await self._retry_or_fail(item, reconciliation=True)
            return
        except VoiceProviderRetryableError:
            await self._retry_or_fail(item, reconciliation=False)
            return
        except (VoiceProviderRejectedError, VoiceProviderProtocolError):
            await self._fail_enrollment(item)
            return
        except Exception:
            await self._retry_or_fail(item, reconciliation=False)
            return
        await self._publish_success(
            item,
            voice=result.voice,
            request_id=result.request_id,
        )

    async def _reconcile(self, item: EnrollmentWorkItem) -> None:
        try:
            voices = await self.provider.list(
                page_index=0,
                page_size=DEFAULT_LIST_PAGE_SIZE,
            )
        except (VoiceProviderRetryableError, VoiceProviderProtocolError):
            await self._retry_or_fail(item, reconciliation=True)
            return
        except VoiceProviderRejectedError:
            await self._fail_enrollment(item)
            return
        except Exception:
            await self._retry_or_fail(item, reconciliation=True)
            return
        voice = _matching_voice(
            voices,
            preferred_name=item.preferred_name,
            target_model=item.target_model,
        )
        if voice is None:
            await self._retry_or_fail(item, reconciliation=True)
            return
        await self._publish_success(item, voice=voice, request_id=None)

    async def _retry_or_fail(
        self,
        item: EnrollmentWorkItem,
        *,
        reconciliation: bool,
    ) -> None:
        if item.attempt_count > len(RETRY_DELAYS):
            await self._fail_enrollment(item)
            return
        now = self.now()
        retry_at = now + timedelta(seconds=RETRY_DELAYS[item.attempt_count - 1])
        next_status = "RECONCILING" if reconciliation else "RETRY_WAIT"
        values: dict[str, object] = {
            "status": next_status,
            "next_retry_at": retry_at,
            "lease_owner": None,
            "lease_expires_at": retry_at if reconciliation else None,
            "error_message": ENROLLMENT_RETRY_MESSAGE,
            "updated_at": now,
        }
        async with self.session_factory() as database:
            await database.execute(
                update(AiCallVoiceEnrollmentModel)
                .where(self._owned_enrollment(item))
                .values(**values)
            )
            await database.commit()

    async def _publish_success(
        self,
        item: EnrollmentWorkItem,
        *,
        voice: str,
        request_id: str | None,
    ) -> None:
        now = self.now()
        published = False
        async with self.session_factory() as database:
            profile_is_current = exists(
                select(AiCallTenantVoiceProfileModel.id).where(
                    AiCallTenantVoiceProfileModel.id == item.voice_profile_id,
                    AiCallTenantVoiceProfileModel.tenant_id == item.tenant_id,
                    AiCallTenantVoiceProfileModel.latest_enrollment_id == item.id,
                )
            )
            result = await database.execute(
                update(AiCallVoiceEnrollmentModel)
                .where(
                    self._owned_enrollment(item),
                    profile_is_current,
                )
                .values(
                    status="SUCCEEDED",
                    provider_voice=voice,
                    provider_request_id=request_id,
                    next_retry_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    error_message=None,
                    finished_at=now,
                    updated_at=now,
                )
            )
            if result.rowcount == 1:
                await database.execute(
                    update(AiCallTenantVoiceProfileModel)
                    .where(
                        AiCallTenantVoiceProfileModel.id == item.voice_profile_id,
                        AiCallTenantVoiceProfileModel.tenant_id == item.tenant_id,
                        AiCallTenantVoiceProfileModel.latest_enrollment_id == item.id,
                    )
                    .values(
                        voice=voice,
                        status="ENABLED",
                        provider_created_at=now,
                        error_message=None,
                        updated_at=now,
                    )
                )
                await database.commit()
                published = True
            else:
                await database.rollback()
        if published:
            await self._cleanup_enrollment_sample(
                enrollment_id=item.id,
                object_key=item.sample_object_key,
            )

    async def _fail_enrollment(self, item: EnrollmentWorkItem) -> None:
        now = self.now()
        failed = False
        async with self.session_factory() as database:
            result = await database.execute(
                update(AiCallVoiceEnrollmentModel)
                .where(self._owned_enrollment(item))
                .values(
                    status="FAILED",
                    next_retry_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    error_message=ENROLLMENT_FAILURE_MESSAGE,
                    finished_at=now,
                    updated_at=now,
                )
            )
            if result.rowcount == 1:
                await database.execute(
                    update(AiCallTenantVoiceProfileModel)
                    .where(
                        AiCallTenantVoiceProfileModel.id == item.voice_profile_id,
                        AiCallTenantVoiceProfileModel.tenant_id == item.tenant_id,
                        AiCallTenantVoiceProfileModel.latest_enrollment_id == item.id,
                    )
                    .values(
                        status="CREATE_FAILED",
                        error_message=ENROLLMENT_FAILURE_MESSAGE,
                        updated_at=now,
                    )
                )
                await database.commit()
                failed = True
            else:
                await database.rollback()
        if failed:
            await self._cleanup_enrollment_sample(
                enrollment_id=item.id,
                object_key=item.sample_object_key,
            )

    def _owned_enrollment(self, item: EnrollmentWorkItem):
        return and_(
            AiCallVoiceEnrollmentModel.id == item.id,
            AiCallVoiceEnrollmentModel.tenant_id == item.tenant_id,
            AiCallVoiceEnrollmentModel.lease_owner == self.worker_id,
            AiCallVoiceEnrollmentModel.status == item.status,
        )

    async def _cleanup_terminal_samples(self) -> None:
        async with self.session_factory() as database:
            rows = (
                await database.execute(
                    select(
                        AiCallVoiceEnrollmentModel.id,
                        AiCallVoiceEnrollmentModel.sample_object_key,
                    )
                    .where(
                        AiCallVoiceEnrollmentModel.status.in_(_TERMINAL_ENROLLMENT_STATUSES),
                        AiCallVoiceEnrollmentModel.sample_object_key.is_not(None),
                    )
                    .order_by(AiCallVoiceEnrollmentModel.id)
                    .limit(self.batch_size)
                )
            ).all()
            await database.rollback()
        for enrollment_id, object_key in rows:
            await self._cleanup_enrollment_sample(
                enrollment_id=enrollment_id,
                object_key=object_key,
            )

    async def _cleanup_enrollment_sample(
        self,
        *,
        enrollment_id: int,
        object_key: str | None,
    ) -> None:
        if object_key is None:
            return
        delete_failed = False
        try:
            await self.storage.delete(object_key)
        except Exception:
            delete_failed = True
        now = self.now()
        async with self.session_factory() as database:
            if delete_failed:
                values = {
                    "cleanup_error_message": SAMPLE_CLEANUP_ERROR_MESSAGE,
                    "updated_at": now,
                }
            else:
                values = {
                    "sample_object_key": None,
                    "cleanup_error_message": None,
                    "updated_at": now,
                }
            await database.execute(
                update(AiCallVoiceEnrollmentModel)
                .where(
                    AiCallVoiceEnrollmentModel.id == enrollment_id,
                    AiCallVoiceEnrollmentModel.status.in_(_TERMINAL_ENROLLMENT_STATUSES),
                    AiCallVoiceEnrollmentModel.sample_object_key == object_key,
                )
                .values(**values)
            )
            await database.commit()

    @staticmethod
    def _cleanup_is_claimable(now: datetime):
        return or_(
            AiCallVoiceSampleCleanupModel.status == "PENDING",
            and_(
                AiCallVoiceSampleCleanupModel.status == "RETRY_WAIT",
                AiCallVoiceSampleCleanupModel.next_retry_at <= now,
            ),
            and_(
                AiCallVoiceSampleCleanupModel.status == "PROCESSING",
                AiCallVoiceSampleCleanupModel.lease_expires_at < now,
            ),
        )

    async def _claim_cleanup_records(self) -> list[CleanupWorkItem]:
        now = self.now()
        lease_expires_at = now + timedelta(seconds=self.lease_seconds)
        async with self.session_factory() as database:
            dialect_name = database.bind.dialect.name if database.bind else ""
            if dialect_name == "postgresql":
                rows = (
                    await database.scalars(
                        select(AiCallVoiceSampleCleanupModel)
                        .where(self._cleanup_is_claimable(now))
                        .order_by(AiCallVoiceSampleCleanupModel.id)
                        .limit(self.batch_size)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                for row in rows:
                    row.status = "PROCESSING"
                    row.attempt_count += 1
                    row.lease_owner = self.worker_id
                    row.lease_expires_at = lease_expires_at
                    row.next_retry_at = None
                    row.updated_at = now
            else:
                candidate_ids = (
                    select(AiCallVoiceSampleCleanupModel.id)
                    .where(self._cleanup_is_claimable(now))
                    .order_by(AiCallVoiceSampleCleanupModel.id)
                    .limit(self.batch_size)
                )
                rows = (
                    await database.scalars(
                        update(AiCallVoiceSampleCleanupModel)
                        .where(
                            AiCallVoiceSampleCleanupModel.id.in_(candidate_ids),
                            self._cleanup_is_claimable(now),
                        )
                        .values(
                            status="PROCESSING",
                            attempt_count=(AiCallVoiceSampleCleanupModel.attempt_count + 1),
                            lease_owner=self.worker_id,
                            lease_expires_at=lease_expires_at,
                            next_retry_at=None,
                            updated_at=now,
                        )
                        .returning(AiCallVoiceSampleCleanupModel)
                    )
                ).all()
            await database.commit()
            return sorted(
                (
                    CleanupWorkItem(
                        id=row.id,
                        tenant_id=row.tenant_id,
                        object_key=row.object_key,
                        attempt_count=row.attempt_count,
                        lease_owner=row.lease_owner or "",
                    )
                    for row in rows
                ),
                key=lambda item: item.id,
            )

    async def _process_cleanup_record(self, item: CleanupWorkItem) -> None:
        if await self._sample_is_referenced(item.object_key):
            await self._finish_cleanup(item)
            return
        delete_failed = False
        try:
            await self.storage.delete(item.object_key)
        except Exception:
            delete_failed = True
        if delete_failed:
            await self._retry_cleanup(item)
            return
        await self._finish_cleanup(item)

    async def _sample_is_referenced(self, object_key: str) -> bool:
        async with self.session_factory() as database:
            referenced = bool(
                await database.scalar(
                    select(
                        exists().where(AiCallVoiceEnrollmentModel.sample_object_key == object_key)
                    )
                )
            )
            await database.rollback()
            return referenced

    async def _finish_cleanup(self, item: CleanupWorkItem) -> None:
        now = self.now()
        async with self.session_factory() as database:
            await database.execute(
                update(AiCallVoiceSampleCleanupModel)
                .where(self._owned_cleanup(item))
                .values(
                    status="SUCCEEDED",
                    next_retry_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    error_message=None,
                    updated_at=now,
                )
            )
            await database.commit()

    async def _retry_cleanup(self, item: CleanupWorkItem) -> None:
        now = self.now()
        delay = RETRY_DELAYS[min(item.attempt_count - 1, len(RETRY_DELAYS) - 1)]
        async with self.session_factory() as database:
            await database.execute(
                update(AiCallVoiceSampleCleanupModel)
                .where(self._owned_cleanup(item))
                .values(
                    status="RETRY_WAIT",
                    next_retry_at=now + timedelta(seconds=delay),
                    lease_owner=None,
                    lease_expires_at=None,
                    error_message=SAMPLE_CLEANUP_ERROR_MESSAGE,
                    updated_at=now,
                )
            )
            await database.commit()

    def _owned_cleanup(self, item: CleanupWorkItem):
        return and_(
            AiCallVoiceSampleCleanupModel.id == item.id,
            AiCallVoiceSampleCleanupModel.tenant_id == item.tenant_id,
            AiCallVoiceSampleCleanupModel.lease_owner == self.worker_id,
            AiCallVoiceSampleCleanupModel.status == "PROCESSING",
        )


def _matching_voice(
    voices: list[VoiceListItem],
    *,
    preferred_name: str,
    target_model: str,
) -> str | None:
    expected_suffix = f"-{preferred_name}"
    for item in voices:
        if item.target_model != target_model:
            continue
        if item.voice == preferred_name or item.voice.endswith(expected_suffix):
            return item.voice
    return None


def _audio_data_url(data: bytes, object_key: str) -> str:
    extension = Path(object_key).suffix.lower()
    content_type = _SAMPLE_CONTENT_TYPES.get(extension, "application/octet-stream")
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def _as_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
