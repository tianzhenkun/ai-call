from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Never

from fastapi import UploadFile, status
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.voice_sample import (
    VoiceSampleMetadata,
    VoiceSampleStorage,
    VoiceSampleValidationError,
    inspect_sample,
)
from app.utils.id_util import generate_snowflake_id

from .model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceEnrollmentModel,
    AiCallVoiceSampleCleanupModel,
)
from .schema import VoiceEnrollmentAcceptedOut, VoiceEnrollmentRequest

MAX_SAMPLE_BYTES = 10 * 1024 * 1024
PROVIDER = "aliyun_qwen"
CLEANUP_ERROR_MESSAGE = "即时删除声音样本失败，等待后台重试"
CLEANUP_PERSISTENCE_LOG = "音色样本清理补偿持久化失败，需人工检查后台回收"
SAMPLE_EXTENSION_BY_CONTENT_TYPE = {
    "audio/wav": ".wav",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class _Reconciliation:
    state: str
    accepted: VoiceEnrollmentAcceptedOut | None = None
    sample_object_key: str | None = None


class VoiceEnrollmentService:
    """受理租户自定义音色创建和失败重传。"""

    def __init__(
        self,
        *,
        storage: VoiceSampleStorage,
        cleanup_session_factory: Callable[
            [],
            AbstractAsyncContextManager[AsyncSession],
        ],
        target_model: str,
        now: Callable[[], datetime] = _utc_now,
        id_generator: Callable[[], int] = generate_snowflake_id,
        cleanup_id_generator: Callable[[], int] = generate_snowflake_id,
    ) -> None:
        self.storage = storage
        self.cleanup_session_factory = cleanup_session_factory
        self.target_model = target_model
        self.now = now
        self.id_generator = id_generator
        self.cleanup_id_generator = cleanup_id_generator

    async def create(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        idempotency_key: str,
        request: VoiceEnrollmentRequest,
        sample: UploadFile,
    ) -> VoiceEnrollmentAcceptedOut:
        return await self._accept_enrollment(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            idempotency_key=idempotency_key,
            request=request,
            sample=sample,
            existing_profile_id=None,
        )

    async def reenroll(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        profile_id: int,
        idempotency_key: str,
        request: VoiceEnrollmentRequest,
        sample: UploadFile,
    ) -> VoiceEnrollmentAcceptedOut:
        return await self._accept_enrollment(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            idempotency_key=idempotency_key,
            request=request,
            sample=sample,
            existing_profile_id=profile_id,
        )

    async def _accept_enrollment(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        idempotency_key: str,
        request: VoiceEnrollmentRequest,
        sample: UploadFile,
        existing_profile_id: int | None,
    ) -> VoiceEnrollmentAcceptedOut:
        command_key = idempotency_key.strip()
        if not command_key or len(command_key) > 128:
            raise CustomException(
                msg="Idempotency-Key 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if not request.consent_confirmed:
            raise CustomException(
                msg="请确认已获得声音权利人的明确授权",
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            )

        data, metadata = await self._read_and_inspect(sample)
        request_hash = self._request_hash(request, metadata.sha256)
        existing, lookup_failed = await self._lookup_enrollment(
            db,
            tenant_id,
            command_key,
        )
        if lookup_failed:
            await self._safe_rollback(db)
            self._raise_persistence_failure()
        if existing is not None:
            return await self._resolve_idempotent(
                db,
                tenant_id=tenant_id,
                enrollment=existing,
                request_hash=request_hash,
                expected_profile_id=existing_profile_id,
            )

        profile_id: int | None = existing_profile_id
        if existing_profile_id is not None:
            profile, profile_lookup_failed = await self._lookup_profile(
                db,
                tenant_id,
                existing_profile_id,
            )
            if profile_lookup_failed:
                await self._safe_rollback(db)
                self._raise_persistence_failure()
            if profile is None:
                raise CustomException(
                    msg="音色资产不存在",
                    status_code=status.HTTP_404_NOT_FOUND,
                )
            if profile.status != "CREATE_FAILED":
                raise CustomException(
                    msg="只有创建失败的音色可以重新上传",
                    status_code=status.HTTP_409_CONFLICT,
                )

        await self._safe_rollback(db)

        generated_ids = self._generate_ids(profile_id)
        if generated_ids is None:
            self._raise_persistence_failure()
        profile_id, enrollment_id = generated_ids

        object_key = self._sample_object_key(
            tenant_id=tenant_id,
            enrollment_id=enrollment_id,
            content_type=metadata.content_type,
        )
        uploaded = await self._put_sample(
            object_key=object_key,
            data=data,
            content_type=metadata.content_type,
        )
        if not uploaded:
            await self._delete_uploaded_sample(
                tenant_id=tenant_id,
                object_key=object_key,
            )
            raise CustomException(
                msg="声音样本暂存失败，请稍后重试",
                status_code=status.HTTP_502_BAD_GATEWAY,
            )

        if existing_profile_id is not None:
            reserved, reservation_failed = await self._reserve_profile(
                db,
                tenant_id=tenant_id,
                profile_id=existing_profile_id,
                enrollment_id=enrollment_id,
                request=request,
            )
            if reservation_failed:
                await self._safe_rollback(db)
                await self._delete_uploaded_sample(
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
                self._raise_persistence_failure()
            if not reserved:
                await self._safe_rollback(db)
                await self._delete_uploaded_sample(
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
                reconciled = await self._reconcile_enrollment(
                    tenant_id=tenant_id,
                    command_key=command_key,
                    request_hash=request_hash,
                    expected_profile_id=existing_profile_id,
                )
                if reconciled.state == "accepted":
                    return self._accepted_result(reconciled)
                if reconciled.state == "failed":
                    self._raise_persistence_failure()
                raise CustomException(
                    msg="只有创建失败的音色可以重新上传",
                    status_code=status.HTTP_409_CONFLICT,
                )

        flush_state = await self._stage_and_flush(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            command_key=command_key,
            request=request,
            request_hash=request_hash,
            sample_object_key=object_key,
            sample_sha256=metadata.sha256,
            profile_id=profile_id,
            enrollment_id=enrollment_id,
            create_profile=existing_profile_id is None,
        )
        if flush_state != "success":
            await self._safe_rollback(db)
            reconciled = await self._reconcile_enrollment(
                tenant_id=tenant_id,
                command_key=command_key,
                request_hash=request_hash,
                expected_profile_id=existing_profile_id,
            )
            await self._delete_if_not_referenced(
                reconciled=reconciled,
                tenant_id=tenant_id,
                object_key=object_key,
            )
            if flush_state == "integrity" and reconciled.state == "accepted":
                return self._accepted_result(reconciled)
            if flush_state == "integrity" and reconciled.state == "conflict":
                raise CustomException(
                    msg="Idempotency-Key 已用于不同请求",
                    status_code=status.HTTP_409_CONFLICT,
                )
            self._raise_persistence_failure()

        if not await self._commit(db):
            await self._safe_rollback(db)
            reconciled = await self._reconcile_enrollment(
                tenant_id=tenant_id,
                command_key=command_key,
                request_hash=request_hash,
                expected_profile_id=existing_profile_id,
            )
            if reconciled.state == "accepted":
                await self._delete_if_not_referenced(
                    reconciled=reconciled,
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
                return self._accepted_result(reconciled)
            if reconciled.state == "conflict":
                await self._delete_if_not_referenced(
                    reconciled=reconciled,
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
                raise CustomException(
                    msg="Idempotency-Key 已用于不同请求",
                    status_code=status.HTTP_409_CONFLICT,
                )
            if reconciled.state == "missing":
                await self._delete_uploaded_sample(
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
            else:
                await self._persist_cleanup_compensation(
                    tenant_id=tenant_id,
                    object_key=object_key,
                )
            self._raise_persistence_failure()

        return VoiceEnrollmentAcceptedOut(
            voice_profile_id=profile_id,
            enrollment_id=enrollment_id,
            status="CREATING",
            display_name=request.display_name,
        )

    def _generate_ids(
        self,
        profile_id: int | None,
    ) -> tuple[int, int] | None:
        try:
            generated_profile_id = profile_id if profile_id is not None else self.id_generator()
            enrollment_id = self.id_generator()
        except Exception:
            return None
        return generated_profile_id, enrollment_id

    async def _put_sample(
        self,
        *,
        object_key: str,
        data: bytes,
        content_type: str,
    ) -> bool:
        try:
            await self.storage.put(
                object_key=object_key,
                data=data,
                content_type=content_type,
            )
        except Exception:
            return False
        return True

    async def _stage_and_flush(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        command_key: str,
        request: VoiceEnrollmentRequest,
        request_hash: str,
        sample_object_key: str,
        sample_sha256: str,
        profile_id: int,
        enrollment_id: int,
        create_profile: bool,
    ) -> str:
        try:
            self._stage_enrollment(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                command_key=command_key,
                request=request,
                request_hash=request_hash,
                sample_object_key=sample_object_key,
                sample_sha256=sample_sha256,
                profile_id=profile_id,
                enrollment_id=enrollment_id,
                create_profile=create_profile,
            )
            await db.flush()
        except IntegrityError:
            return "integrity"
        except Exception:
            return "failed"
        return "success"

    @staticmethod
    async def _commit(db: AsyncSession) -> bool:
        try:
            await db.commit()
        except Exception:
            return False
        return True

    def _stage_enrollment(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        command_key: str,
        request: VoiceEnrollmentRequest,
        request_hash: str,
        sample_object_key: str,
        sample_sha256: str,
        profile_id: int,
        enrollment_id: int,
        create_profile: bool,
    ) -> None:
        now = self.now()
        if create_profile:
            profile = AiCallTenantVoiceProfileModel(
                id=profile_id,
                tenant_id=tenant_id,
                display_name=request.display_name,
                voice=None,
                voice_type="自定义复刻",
                gender=request.gender,
                language=request.language,
                target_model=self.target_model,
                provider=PROVIDER,
                status="CREATING",
                latest_enrollment_id=None,
                provider_created_at=None,
                error_message=None,
                created_by=user_id,
                deleted_by=None,
                deleted_at=None,
                created_at=now,
                updated_at=now,
            )
            db.add(profile)

        enrollment = AiCallVoiceEnrollmentModel(
            id=enrollment_id,
            tenant_id=tenant_id,
            voice_profile_id=profile_id,
            idempotency_key=command_key,
            request_hash=request_hash,
            preferred_name=f"vc{profile_id}"[-16:],
            language=request.language,
            transcript=request.transcript,
            sample_object_key=sample_object_key,
            sample_sha256=sample_sha256,
            status="PENDING",
            provider_voice=None,
            provider_request_id=None,
            attempt_count=0,
            next_retry_at=None,
            lease_owner=None,
            lease_expires_at=None,
            error_message=None,
            cleanup_error_message=None,
            consent_user_id=user_id,
            consent_at=now,
            started_at=None,
            finished_at=None,
            created_at=now,
            updated_at=now,
        )
        db.add(enrollment)
        if create_profile:
            profile.latest_enrollment_id = enrollment_id

    async def _reserve_failed_profile(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        profile_id: int,
        enrollment_id: int,
        request: VoiceEnrollmentRequest,
    ) -> bool:
        result = await db.execute(
            update(AiCallTenantVoiceProfileModel)
            .where(
                AiCallTenantVoiceProfileModel.tenant_id == tenant_id,
                AiCallTenantVoiceProfileModel.id == profile_id,
                AiCallTenantVoiceProfileModel.status == "CREATE_FAILED",
            )
            .values(
                display_name=request.display_name,
                gender=request.gender,
                language=request.language,
                status="CREATING",
                latest_enrollment_id=enrollment_id,
                error_message=None,
                updated_at=self.now(),
            )
            .execution_options(synchronize_session=False)
        )
        return result.rowcount == 1

    async def _reserve_profile(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        profile_id: int,
        enrollment_id: int,
        request: VoiceEnrollmentRequest,
    ) -> tuple[bool, bool]:
        try:
            reserved = await self._reserve_failed_profile(
                db,
                tenant_id=tenant_id,
                profile_id=profile_id,
                enrollment_id=enrollment_id,
                request=request,
            )
        except Exception:
            return False, True
        return reserved, False

    async def _lookup_enrollment(
        self,
        db: AsyncSession,
        tenant_id: str,
        command_key: str,
    ) -> tuple[AiCallVoiceEnrollmentModel | None, bool]:
        try:
            enrollment = await self._find_enrollment(db, tenant_id, command_key)
        except Exception:
            return None, True
        return enrollment, False

    async def _lookup_profile(
        self,
        db: AsyncSession,
        tenant_id: str,
        profile_id: int,
    ) -> tuple[AiCallTenantVoiceProfileModel | None, bool]:
        try:
            profile = await self._find_profile(db, tenant_id, profile_id)
        except Exception:
            return None, True
        return profile, False

    @staticmethod
    def _sample_object_key(
        *,
        tenant_id: str,
        enrollment_id: int,
        content_type: str,
    ) -> str:
        tenant_digest = hashlib.sha256(tenant_id.encode()).hexdigest()[:12]
        extension = SAMPLE_EXTENSION_BY_CONTENT_TYPE[content_type]
        return f"ai-call/voice-samples/{tenant_digest}/{enrollment_id}{extension}"

    async def _read_and_inspect(
        self,
        sample: UploadFile,
    ) -> tuple[bytes, VoiceSampleMetadata]:
        read_failed = False
        data = b""
        try:
            await sample.seek(0)
            data = await sample.read(MAX_SAMPLE_BYTES + 1)
        except Exception:
            read_failed = True
        if read_failed:
            raise CustomException(
                msg="声音样本读取失败",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if len(data) > MAX_SAMPLE_BYTES:
            raise CustomException(
                msg="声音样本必须小于 10 MB",
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            )
        validation_message: str | None = None
        metadata: VoiceSampleMetadata | None = None
        try:
            metadata = inspect_sample(
                data,
                filename=(sample.filename or "").strip(),
                content_type=(sample.content_type or "").strip(),
            )
        except VoiceSampleValidationError as exc:
            validation_message = str(exc)
        if validation_message is not None:
            raise CustomException(
                msg=validation_message,
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            )
        if metadata is None:
            raise CustomException(
                msg="声音样本校验失败",
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            )
        return data, metadata

    @staticmethod
    def _request_hash(request: VoiceEnrollmentRequest, sample_sha256: str) -> str:
        canonical = json.dumps(
            {
                "consentConfirmed": request.consent_confirmed,
                "displayName": request.display_name,
                "gender": request.gender,
                "language": request.language,
                "sampleSha256": sample_sha256,
                "transcript": request.transcript,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    async def _find_enrollment(
        db: AsyncSession,
        tenant_id: str,
        idempotency_key: str,
    ) -> AiCallVoiceEnrollmentModel | None:
        return await db.scalar(
            select(AiCallVoiceEnrollmentModel)
            .where(
                AiCallVoiceEnrollmentModel.tenant_id == tenant_id,
                AiCallVoiceEnrollmentModel.idempotency_key == idempotency_key,
            )
            .limit(1)
        )

    @staticmethod
    async def _find_profile(
        db: AsyncSession,
        tenant_id: str,
        profile_id: int,
    ) -> AiCallTenantVoiceProfileModel | None:
        return await db.scalar(
            select(AiCallTenantVoiceProfileModel)
            .where(
                AiCallTenantVoiceProfileModel.tenant_id == tenant_id,
                AiCallTenantVoiceProfileModel.id == profile_id,
            )
            .limit(1)
        )

    async def _resolve_idempotent(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        enrollment: AiCallVoiceEnrollmentModel,
        request_hash: str,
        expected_profile_id: int | None,
    ) -> VoiceEnrollmentAcceptedOut:
        if enrollment.request_hash != request_hash or (
            expected_profile_id is not None and enrollment.voice_profile_id != expected_profile_id
        ):
            raise CustomException(
                msg="Idempotency-Key 已用于不同请求",
                status_code=status.HTTP_409_CONFLICT,
            )
        profile_lookup_failed = False
        profile = None
        try:
            profile = await self._find_profile(
                db,
                tenant_id,
                enrollment.voice_profile_id,
            )
        except Exception:
            profile_lookup_failed = True
        if profile_lookup_failed:
            await self._safe_rollback(db)
            raise CustomException(
                msg="音色复刻任务受理失败，请稍后重试",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        if profile is None:
            raise CustomException(
                msg="幂等请求对应的音色资产不存在",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        return VoiceEnrollmentAcceptedOut(
            voice_profile_id=profile.id,
            enrollment_id=enrollment.id,
            status="CREATING",
            display_name=profile.display_name,
        )

    async def _reconcile_enrollment(
        self,
        *,
        tenant_id: str,
        command_key: str,
        request_hash: str,
        expected_profile_id: int | None,
    ) -> _Reconciliation:
        failed = False
        enrollment: AiCallVoiceEnrollmentModel | None = None
        profile: AiCallTenantVoiceProfileModel | None = None
        try:
            async with self.cleanup_session_factory() as reconcile_db:
                enrollment = await self._find_enrollment(
                    reconcile_db,
                    tenant_id,
                    command_key,
                )
                if enrollment is not None:
                    profile = await self._find_profile(
                        reconcile_db,
                        tenant_id,
                        enrollment.voice_profile_id,
                    )
        except Exception:
            failed = True
        if failed:
            return _Reconciliation(state="failed")
        if enrollment is None:
            return _Reconciliation(state="missing")
        if enrollment.request_hash != request_hash or (
            expected_profile_id is not None and enrollment.voice_profile_id != expected_profile_id
        ):
            return _Reconciliation(
                state="conflict",
                sample_object_key=enrollment.sample_object_key,
            )
        if profile is None:
            return _Reconciliation(
                state="failed",
                sample_object_key=enrollment.sample_object_key,
            )
        return _Reconciliation(
            state="accepted",
            accepted=VoiceEnrollmentAcceptedOut(
                voice_profile_id=profile.id,
                enrollment_id=enrollment.id,
                status="CREATING",
                display_name=profile.display_name,
            ),
            sample_object_key=enrollment.sample_object_key,
        )

    async def _delete_if_not_referenced(
        self,
        *,
        reconciled: _Reconciliation,
        tenant_id: str,
        object_key: str,
    ) -> None:
        if reconciled.sample_object_key == object_key:
            return
        await self._delete_uploaded_sample(
            tenant_id=tenant_id,
            object_key=object_key,
        )

    async def _delete_uploaded_sample(
        self,
        *,
        tenant_id: str,
        object_key: str,
    ) -> None:
        delete_failed = False
        try:
            await self.storage.delete(object_key)
        except Exception:
            delete_failed = True
        if delete_failed:
            await self._persist_cleanup_compensation(
                tenant_id=tenant_id,
                object_key=object_key,
            )

    async def _persist_cleanup_compensation(
        self,
        *,
        tenant_id: str,
        object_key: str,
    ) -> None:
        for _attempt in range(2):
            outcome = await self._try_persist_cleanup(
                tenant_id=tenant_id,
                object_key=object_key,
            )
            if outcome == "success":
                return
            if outcome != "integrity":
                log.warning(CLEANUP_PERSISTENCE_LOG)
                return
            exists = await self._cleanup_exists(object_key)
            if exists is True:
                return
            if exists is None:
                log.warning(CLEANUP_PERSISTENCE_LOG)
                return
        log.warning(CLEANUP_PERSISTENCE_LOG)

    async def _try_persist_cleanup(
        self,
        *,
        tenant_id: str,
        object_key: str,
    ) -> str:
        cleanup_db: AsyncSession | None = None
        outcome = "success"
        try:
            async with self.cleanup_session_factory() as cleanup_db:
                now = self.now()
                cleanup_db.add(
                    AiCallVoiceSampleCleanupModel(
                        id=self.cleanup_id_generator(),
                        tenant_id=tenant_id,
                        object_key=object_key,
                        status="PENDING",
                        attempt_count=0,
                        next_retry_at=None,
                        lease_owner=None,
                        lease_expires_at=None,
                        error_message=CLEANUP_ERROR_MESSAGE,
                        created_at=now,
                        updated_at=now,
                    )
                )
                await cleanup_db.flush()
                await cleanup_db.commit()
        except IntegrityError:
            outcome = "integrity"
        except Exception:
            outcome = "failed"
        if outcome != "success" and cleanup_db is not None:
            await self._safe_rollback(cleanup_db)
        return outcome

    async def _cleanup_exists(self, object_key: str) -> bool | None:
        failed = False
        existing: AiCallVoiceSampleCleanupModel | None = None
        try:
            async with self.cleanup_session_factory() as cleanup_db:
                existing = await cleanup_db.scalar(
                    select(AiCallVoiceSampleCleanupModel)
                    .where(AiCallVoiceSampleCleanupModel.object_key == object_key)
                    .limit(1)
                )
        except Exception:
            failed = True
        if failed:
            return None
        return existing is not None

    @staticmethod
    def _raise_persistence_failure() -> Never:
        raise CustomException(
            msg="音色复刻任务受理失败，请稍后重试",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    @classmethod
    def _accepted_result(
        cls,
        reconciled: _Reconciliation,
    ) -> VoiceEnrollmentAcceptedOut:
        if reconciled.accepted is None:
            cls._raise_persistence_failure()
        return reconciled.accepted

    @staticmethod
    async def _safe_rollback(db: AsyncSession) -> None:
        try:
            await db.rollback()
        except Exception:
            pass
