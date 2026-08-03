from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAsrJobModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.oss.service import OssService
from app.core.logger import log
from app.services.ai_call.livekit_egress import (
    LiveKitEgressManager,
    LiveKitEgressRequestTimeout,
)
from app.services.ai_call.session_registry import utc_now


class AiCallRecordingService:
    """B2 录音业务服务，录音失败不能阻断通话。"""

    _PARTICIPANT_EGRESS_RETRY_DELAYS_SECONDS = (0.0, 0.5, 1.0, 2.0)
    _VERIFY_PENDING_STATUSES = frozenset({"starting", "recording", "stopping", "verifying"})

    def __init__(
        self,
        repository: AiCallRecordRepository,
        *,
        enabled: bool,
        egress_manager: LiveKitEgressManager | None = None,
        participant_recording_enabled: bool = False,
        verify_deadline_seconds: int = 900,
        stop_session_factory: async_sessionmaker[AsyncSession] | None = None,
        transaction_checkpoint: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.repository = repository
        self.enabled = enabled
        self.egress_manager = egress_manager
        self.participant_recording_enabled = participant_recording_enabled
        self.verify_deadline_seconds = max(30, verify_deadline_seconds)
        self.stop_session_factory = stop_session_factory
        self.transaction_checkpoint = transaction_checkpoint

    async def start_for_session(
        self,
        *,
        tenant_id: str,
        call_id: str,
        room_name: str,
        customer_participant_identity: str | None = None,
        ai_participant_identity: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        started_at = utc_now()
        object_name = (
            self.egress_manager.build_object_name(call_id) if self.egress_manager else None
        )
        recording = await self.repository.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is None:
            recording = await self.repository.create_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                room_name=room_name,
                status="starting",
                object_name=object_name,
                started_at=started_at,
            )
        if self.egress_manager is None:
            await self.fail_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                failure_stage="egress_config",
                failure_message="录音组件未配置",
            )
            return
        oss_config = OssService.active_config()
        if not oss_config:
            await self.fail_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                failure_stage="oss_config",
                failure_message="未找到可用的OSS配置",
            )
            return

        try:
            result = await self.egress_manager.start_room_audio_recording(
                room_name=room_name,
                call_id=call_id,
                oss_config=oss_config,
            )
            await self.repository.update_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                status="recording",
                egress_id=result.egress_id or None,
                object_name=result.object_name or recording.object_name,
                started_at=result.started_at or recording.started_at,
                failure_stage=None,
                failure_message=None,
            )
        except Exception as exc:
            log.warning(
                "AI Call 录音启动失败: callId={}, errorType={}, message={}",
                call_id,
                type(exc).__name__,
                str(exc),
            )
            await self.fail_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                failure_stage="egress_start",
                failure_message="LiveKit Egress 启动失败",
            )
            return

        if customer_participant_identity or ai_participant_identity:
            log.debug(
                "AI Call 主录音已启动，分轨录音等待参与方 ready 后启动: callId={}",
                call_id,
            )

    async def start_session_participant_recordings(
        self,
        *,
        call_id: str,
        room_name: str,
        customer_participant_identity: str | None = None,
        ai_participant_identity: str | None = None,
    ) -> None:
        if not self.enabled or not self.participant_recording_enabled:
            return
        if self.egress_manager is None:
            return
        oss_config = OssService.active_config()
        if not oss_config:
            return
        await self._start_initial_participant_recordings(
            call_id=call_id,
            room_name=room_name,
            customer_participant_identity=customer_participant_identity,
            ai_participant_identity=ai_participant_identity,
            oss_config=oss_config,
        )

    async def start_human_agent_recording(
        self,
        *,
        call_id: str,
        room_name: str,
        handoff_id: str,
        participant_identity: str,
    ) -> None:
        if not self.enabled or not self.participant_recording_enabled:
            return
        if self.egress_manager is None:
            return
        oss_config = OssService.active_config()
        if not oss_config:
            return
        await self._start_participant_recording(
            call_id=call_id,
            room_name=room_name,
            track_role="human_agent",
            participant_identity=participant_identity,
            oss_config=oss_config,
            handoff_id=handoff_id,
        )

    async def stop_for_session(self, *, tenant_id: str, call_id: str) -> None:
        if not self.enabled:
            return
        if (
            self.stop_session_factory is not None
            and self.transaction_checkpoint is None
        ):
            await self._stop_for_session_in_isolated_session(
                tenant_id=tenant_id,
                call_id=call_id,
            )
            return
        await self._stop_for_session_in_current_session(
            tenant_id=tenant_id,
            call_id=call_id,
        )

    async def _stop_for_session_in_isolated_session(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> None:
        if self.stop_session_factory is None:
            return
        async with self.stop_session_factory() as db:
            isolated_service = AiCallRecordingService(
                AiCallRecordRepository(db),
                enabled=self.enabled,
                egress_manager=self.egress_manager,
                participant_recording_enabled=self.participant_recording_enabled,
                verify_deadline_seconds=self.verify_deadline_seconds,
                transaction_checkpoint=db.commit,
            )
            try:
                await isolated_service._stop_for_session_in_current_session(
                    tenant_id=tenant_id,
                    call_id=call_id,
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def _stop_for_session_in_current_session(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> None:
        await self._stop_main_recording(tenant_id=tenant_id, call_id=call_id)
        await self._stop_participant_recordings(call_id)

    async def _stop_main_recording(self, *, tenant_id: str, call_id: str) -> None:
        recording = await self.repository.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is None or not recording.egress_id:
            return
        if recording.status in {"completed", "failed", "verifying"}:
            return

        stop_requested_at = utc_now()
        await self.repository.update_recording(
            tenant_id=tenant_id,
            call_id=call_id,
            status="stopping",
            stop_requested_at=stop_requested_at,
        )
        await self._checkpoint_before_external_io()
        try:
            if self.egress_manager is None:
                raise RuntimeError("录音组件未配置")
            result = await self.egress_manager.stop_egress(recording.egress_id)
            if result.error or self._is_failed_egress_status(result.status):
                log.warning(
                    "AI Call 录音停止返回失败状态: callId={}, egressId={}, status={}, error={}",
                    call_id,
                    recording.egress_id,
                    result.status,
                    result.error,
                )
                await self.fail_recording(
                    tenant_id=tenant_id,
                    call_id=call_id,
                    failure_stage="egress_stop",
                    failure_message="LiveKit Egress 停止失败",
                )
                return
            object_name = result.object_name or recording.object_name
            if not object_name:
                raise RuntimeError("Egress 未返回录音文件名")
        except LiveKitEgressRequestTimeout as exc:
            log.warning(
                "AI Call 录音停止超时，开始回查OSS确认最终结果: "
                "callId={}, egressId={}, timeoutSeconds={}, message={}",
                call_id,
                recording.egress_id,
                exc.timeout_seconds,
                str(exc),
            )
            await self._mark_main_recording_verifying(
                recording,
                exc,
                stop_requested_at=stop_requested_at,
            )
            return
        except Exception as exc:
            if self._is_already_completed_stop_error(exc):
                log.info(
                    "AI Call 录音已由 LiveKit 提前完成，开始回查OSS确认最终结果: "
                    "callId={}, egressId={}, errorType={}, message={}",
                    call_id,
                    recording.egress_id,
                    type(exc).__name__,
                    str(exc),
                )
                if await self._complete_main_recording_from_existing_object(
                    recording,
                    exc,
                    verified_at=stop_requested_at,
                ):
                    return
                await self._mark_main_recording_verifying(
                    recording,
                    exc,
                    stop_requested_at=stop_requested_at,
                )
                return
            log.warning(
                "AI Call 录音停止失败: callId={}, errorType={}, message={}",
                call_id,
                type(exc).__name__,
                str(exc),
            )
            await self.fail_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                failure_stage="egress_stop",
                failure_message="LiveKit Egress 停止失败",
            )
            return

        ended_at = result.ended_at or utc_now()
        duration_ms = result.duration_ms or self._duration_ms(recording.started_at, ended_at)
        try:
            oss_id = await self._register_recording_object(object_name, result.file_size)
        except Exception as exc:
            log.warning(
                "AI Call 录音文件索引登记失败: callId={}, objectName={}, errorType={}, message={}",
                call_id,
                object_name,
                type(exc).__name__,
                str(exc),
            )
            await self.repository.update_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                status="failed",
                object_name=object_name,
                ended_at=ended_at,
                duration_ms=duration_ms,
                next_verify_at=None,
                last_verify_error=None,
                failure_stage="oss_register",
                failure_message="录音文件索引登记失败",
            )
            return

        try:
            await self.repository.update_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                status="completed",
                object_name=object_name,
                oss_id=oss_id,
                ended_at=ended_at,
                duration_ms=duration_ms,
                next_verify_at=None,
                last_verify_at=None,
                last_verify_error=None,
                failure_stage=None,
                failure_message=None,
            )
        except Exception as exc:
            log.warning(
                "AI Call 录音完成状态更新失败: callId={}, errorType={}, message={}",
                call_id,
                type(exc).__name__,
                str(exc),
            )
            await self.fail_recording(
                tenant_id=tenant_id,
                call_id=call_id,
                failure_stage="recording_update",
                failure_message="录音完成状态更新失败",
            )

    async def _start_initial_participant_recordings(
        self,
        *,
        call_id: str,
        room_name: str,
        customer_participant_identity: str | None,
        ai_participant_identity: str | None,
        oss_config: dict,
    ) -> None:
        for track_role, participant_identity in (
            ("customer", customer_participant_identity),
            ("ai", ai_participant_identity),
        ):
            if not participant_identity:
                continue
            await self._start_participant_recording(
                call_id=call_id,
                room_name=room_name,
                track_role=track_role,
                participant_identity=participant_identity,
                oss_config=oss_config,
            )

    async def _start_participant_recording(
        self,
        *,
        call_id: str,
        room_name: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
        handoff_id: str | None = None,
    ) -> AiCallRecordingTrackModel | None:
        if self.egress_manager is None:
            return None
        existing = await self.repository.get_recording_track(
            call_id=call_id,
            track_role=track_role,
            participant_identity=participant_identity,
        )
        if existing is not None and not self._can_retry_participant_start(existing):
            return existing

        started_at = utc_now()
        object_name = self.egress_manager.build_participant_object_name(
            call_id=call_id,
            track_role=track_role,
            participant_identity=participant_identity,
        )
        if existing is None:
            track = await self.repository.create_recording_track(
                call_id=call_id,
                room_name=room_name,
                track_role=track_role,
                participant_identity=participant_identity,
                handoff_id=handoff_id,
                status="starting",
                object_name=object_name,
                started_at=started_at,
            )
        else:
            track = await self.repository.update_recording_track(
                existing.id,
                status="starting",
                egress_id=None,
                object_name=object_name or existing.object_name,
                started_at=started_at,
                ended_at=None,
                duration_ms=None,
                failure_stage=None,
                failure_message=None,
            )
            if track is None:
                return None

        last_exc: Exception | None = None
        max_attempts = len(self._PARTICIPANT_EGRESS_RETRY_DELAYS_SECONDS)
        for attempt, delay_seconds in enumerate(
            self._PARTICIPANT_EGRESS_RETRY_DELAYS_SECONDS,
            start=1,
        ):
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            try:
                result = await self.egress_manager.start_participant_audio_recording(
                    room_name=room_name,
                    call_id=call_id,
                    track_role=track_role,
                    participant_identity=participant_identity,
                    oss_config=oss_config,
                )
                return await self.repository.update_recording_track(
                    track.id,
                    status="recording",
                    egress_id=result.egress_id or None,
                    object_name=result.object_name or track.object_name,
                    started_at=result.started_at or track.started_at,
                    failure_stage=None,
                    failure_message=None,
                )
            except Exception as exc:
                last_exc = exc
                log.warning(
                    "AI Call 分轨录音启动失败: callId={}, trackRole={}, participantIdentity={}, "
                    "attempt={}/{}, errorType={}, message={}",
                    call_id,
                    track_role,
                    participant_identity,
                    attempt,
                    max_attempts,
                    type(exc).__name__,
                    str(exc),
                )

        failure_message = self._failure_message(
            "LiveKit Track Egress 启动失败",
            last_exc,
        )
        return await self.repository.update_recording_track(
            track.id,
            status="failed",
            ended_at=utc_now(),
            failure_stage="egress_start",
            failure_message=failure_message,
        )

    @staticmethod
    def _can_retry_participant_start(track: AiCallRecordingTrackModel) -> bool:
        return track.status == "failed" and track.failure_stage == "egress_start"

    @staticmethod
    def _failure_message(prefix: str, exc: Exception | None) -> str:
        if exc is None:
            return prefix
        detail = str(exc).strip()
        if not detail:
            return prefix
        message = f"{prefix}: {detail}"
        return message[:500]

    async def _stop_participant_recordings(self, call_id: str) -> None:
        tracks = await self.repository.list_recording_tracks(call_id)
        for track in tracks:
            await self._stop_participant_recording(track)

    async def _stop_participant_recording(self, track: AiCallRecordingTrackModel) -> None:
        if track.status in {"completed", "failed", "verifying"}:
            return
        if not track.egress_id:
            return

        stop_requested_at = utc_now()
        await self.repository.update_recording_track(
            track.id,
            status="stopping",
            stop_requested_at=stop_requested_at,
        )
        await self._checkpoint_before_external_io()
        try:
            if self.egress_manager is None:
                raise RuntimeError("录音组件未配置")
            result = await self.egress_manager.stop_egress(track.egress_id)
            if result.error or self._is_failed_egress_status(result.status):
                log.warning(
                    "AI Call 分轨录音停止返回失败状态: callId={}, trackRole={}, "
                    "participantIdentity={}, egressId={}, status={}, error={}",
                    track.call_id,
                    track.track_role,
                    track.participant_identity,
                    track.egress_id,
                    result.status,
                    result.error,
                )
                await self.repository.update_recording_track(
                    track.id,
                    status="failed",
                    ended_at=utc_now(),
                    failure_stage="egress_stop",
                    failure_message="LiveKit Track Egress 停止失败",
                )
                return
            object_name = result.object_name or track.object_name
            if not object_name:
                raise RuntimeError("Egress 未返回录音文件名")
        except LiveKitEgressRequestTimeout as exc:
            log.warning(
                "AI Call 分轨录音停止超时，开始回查OSS确认最终结果: "
                "callId={}, trackRole={}, participantIdentity={}, egressId={}, "
                "timeoutSeconds={}, message={}",
                track.call_id,
                track.track_role,
                track.participant_identity,
                track.egress_id,
                exc.timeout_seconds,
                str(exc),
            )
            await self._mark_participant_recording_verifying(
                track,
                exc,
                stop_requested_at=stop_requested_at,
            )
            return
        except Exception as exc:
            if self._is_already_completed_stop_error(exc):
                log.info(
                    "AI Call 分轨录音已由 LiveKit 提前完成，开始回查OSS确认最终结果: "
                    "callId={}, trackRole={}, participantIdentity={}, egressId={}, "
                    "errorType={}, message={}",
                    track.call_id,
                    track.track_role,
                    track.participant_identity,
                    track.egress_id,
                    type(exc).__name__,
                    str(exc),
                )
                if await self._complete_participant_recording_from_existing_object(
                    track,
                    exc,
                    verified_at=stop_requested_at,
                ):
                    return
                await self._mark_participant_recording_verifying(
                    track,
                    exc,
                    stop_requested_at=stop_requested_at,
                )
                return
            log.warning(
                "AI Call 分轨录音停止失败: callId={}, trackRole={}, participantIdentity={}, "
                "errorType={}, message={}",
                track.call_id,
                track.track_role,
                track.participant_identity,
                type(exc).__name__,
                str(exc),
            )
            await self.repository.update_recording_track(
                track.id,
                status="failed",
                ended_at=utc_now(),
                failure_stage="egress_stop",
                failure_message="LiveKit Track Egress 停止失败",
            )
            return

        ended_at = result.ended_at or utc_now()
        duration_ms = result.duration_ms or self._duration_ms(track.started_at, ended_at)
        try:
            oss_id = await self._register_recording_object(object_name, result.file_size)
        except Exception as exc:
            log.warning(
                "AI Call 分轨录音文件索引登记失败: callId={}, trackRole={}, "
                "participantIdentity={}, objectName={}, errorType={}, message={}",
                track.call_id,
                track.track_role,
                track.participant_identity,
                object_name,
                type(exc).__name__,
                str(exc),
            )
            await self.repository.update_recording_track(
                track.id,
                status="failed",
                object_name=object_name,
                ended_at=ended_at,
                duration_ms=duration_ms,
                next_verify_at=None,
                last_verify_error=None,
                failure_stage="oss_register",
                failure_message="分轨录音文件索引登记失败",
            )
            return

        await self.repository.update_recording_track(
            track.id,
            status="completed",
            object_name=object_name,
            oss_id=oss_id,
            ended_at=ended_at,
            duration_ms=duration_ms,
            next_verify_at=None,
            last_verify_at=None,
            last_verify_error=None,
            failure_stage=None,
            failure_message=None,
        )

    async def reconcile_due_recordings(self, *, limit: int = 50) -> set[str]:
        if not self.enabled:
            return set()
        now = utc_now()
        safe_limit = max(1, limit)
        main_recordings = await self.repository.list_due_recording_verifications(
            now=now,
            limit=safe_limit,
        )
        remaining_limit = safe_limit - len(main_recordings)
        tracks = (
            await self.repository.list_due_recording_track_verifications(
                now=now,
                limit=remaining_limit,
            )
            if remaining_limit > 0
            else []
        )

        touched_calls: dict[str, str] = {}
        for recording in main_recordings:
            if await self._verify_main_recording(recording, now=now):
                touched_calls[recording.call_id] = recording.tenant_id
        for track in tracks:
            if await self._verify_participant_recording(track, now=now):
                record = await self.repository.get_record(track.call_id)
                if record is not None and record.tenant_id:
                    touched_calls[track.call_id] = record.tenant_id

        ready_call_ids: set[str] = set()
        for call_id, tenant_id in touched_calls.items():
            if await self.is_ready_for_offline_asr(
                tenant_id=tenant_id,
                call_id=call_id,
            ):
                ready_call_ids.add(call_id)
        return ready_call_ids

    async def is_ready_for_offline_asr(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> bool:
        recording = await self.repository.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is not None and recording.status in self._VERIFY_PENDING_STATUSES:
            return False
        tracks = await self.repository.list_recording_tracks(call_id)
        return not any(track.status in self._VERIFY_PENDING_STATUSES for track in tracks)

    async def _mark_main_recording_verifying(
        self,
        recording: AiCallRecordingModel,
        exc: Exception,
        *,
        stop_requested_at: datetime,
    ) -> None:
        await self.repository.update_recording(
            tenant_id=recording.tenant_id,
            call_id=recording.call_id,
            status="verifying",
            stop_requested_at=stop_requested_at,
            verify_attempts=0,
            next_verify_at=stop_requested_at,
            verify_deadline_at=self._verify_deadline_at(stop_requested_at),
            last_verify_at=None,
            last_verify_error=self._failure_message("StopEgress 停止结果待确认", exc),
            failure_stage=None,
            failure_message=None,
        )

    async def _mark_participant_recording_verifying(
        self,
        track: AiCallRecordingTrackModel,
        exc: Exception,
        *,
        stop_requested_at: datetime,
    ) -> None:
        await self.repository.update_recording_track(
            track.id,
            status="verifying",
            stop_requested_at=stop_requested_at,
            verify_attempts=0,
            next_verify_at=stop_requested_at,
            verify_deadline_at=self._verify_deadline_at(stop_requested_at),
            last_verify_at=None,
            last_verify_error=self._failure_message("StopEgress 停止结果待确认", exc),
            failure_stage=None,
            failure_message=None,
        )

    async def _verify_main_recording(
        self,
        recording: AiCallRecordingModel,
        *,
        now: datetime,
    ) -> bool:
        if recording.status != "verifying":
            return False
        attempts = int(recording.verify_attempts or 0) + 1
        object_name = recording.object_name
        if not object_name:
            await self.repository.update_recording(
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                status="failed",
                ended_at=now,
                verify_attempts=attempts,
                last_verify_at=now,
                last_verify_error="录音对象名为空，无法确认停止结果",
                next_verify_at=None,
                failure_stage="recording_object",
                failure_message="录音对象名为空，无法确认停止结果",
            )
            return True

        file_size = await self._resolve_existing_recording_object_size(object_name)
        if file_size is not None:
            return await self._complete_main_recording_from_existing_object(
                recording,
                RuntimeError("recording verification"),
                file_size=file_size,
                verify_attempts=attempts,
                verified_at=now,
            )

        deadline_at = self._aware_datetime(recording.verify_deadline_at)
        if deadline_at is not None and now >= deadline_at:
            duration_ms = self._duration_ms(recording.started_at, now)
            await self.repository.update_recording(
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                status="failed",
                ended_at=now,
                duration_ms=duration_ms,
                verify_attempts=attempts,
                last_verify_at=now,
                last_verify_error="确认窗口内未发现录音文件",
                next_verify_at=None,
                failure_stage="oss_missing",
                failure_message="录音停止后确认超时，未发现录音文件",
            )
            return True

        await self.repository.update_recording(
            tenant_id=recording.tenant_id,
            call_id=recording.call_id,
            status="verifying",
            verify_attempts=attempts,
            last_verify_at=now,
            last_verify_error="录音文件尚未在OSS可见",
            next_verify_at=self._next_verify_at(now, attempts, deadline_at),
        )
        return False

    async def _verify_participant_recording(
        self,
        track: AiCallRecordingTrackModel,
        *,
        now: datetime,
    ) -> bool:
        if track.status != "verifying":
            return False
        attempts = int(track.verify_attempts or 0) + 1
        object_name = track.object_name
        if not object_name:
            await self.repository.update_recording_track(
                track.id,
                status="failed",
                ended_at=now,
                verify_attempts=attempts,
                last_verify_at=now,
                last_verify_error="分轨录音对象名为空，无法确认停止结果",
                next_verify_at=None,
                failure_stage="recording_object",
                failure_message="分轨录音对象名为空，无法确认停止结果",
            )
            return True

        file_size = await self._resolve_existing_recording_object_size(object_name)
        if file_size is not None:
            return await self._complete_participant_recording_from_existing_object(
                track,
                RuntimeError("recording verification"),
                file_size=file_size,
                verify_attempts=attempts,
                verified_at=now,
            )

        deadline_at = self._aware_datetime(track.verify_deadline_at)
        if deadline_at is not None and now >= deadline_at:
            duration_ms = self._duration_ms(track.started_at, now)
            await self.repository.update_recording_track(
                track.id,
                status="failed",
                ended_at=now,
                duration_ms=duration_ms,
                verify_attempts=attempts,
                last_verify_at=now,
                last_verify_error="确认窗口内未发现分轨录音文件",
                next_verify_at=None,
                failure_stage="oss_missing",
                failure_message="分轨录音停止后确认超时，未发现录音文件",
            )
            return True

        await self.repository.update_recording_track(
            track.id,
            status="verifying",
            verify_attempts=attempts,
            last_verify_at=now,
            last_verify_error="分轨录音文件尚未在OSS可见",
            next_verify_at=self._next_verify_at(now, attempts, deadline_at),
        )
        return False

    async def fail_recording(
        self,
        *,
        tenant_id: str,
        call_id: str,
        failure_stage: str,
        failure_message: str,
    ) -> AiCallRecordingModel | None:
        return await self.repository.update_recording(
            tenant_id=tenant_id,
            call_id=call_id,
            status="failed",
            ended_at=utc_now(),
            next_verify_at=None,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )

    @staticmethod
    def _is_already_completed_stop_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return "egress_complete" in message and "cannot be stopped" in message

    async def get_recording(
        self,
        *,
        tenant_id: str,
        call_id: str,
    ) -> AiCallRecordingModel | None:
        return await self.repository.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )

    async def recording_to_dict(self, recording: AiCallRecordingModel) -> dict[str, Any]:
        play_url = await self._play_url(recording.oss_id)
        tracks = await self.repository.list_recording_tracks(recording.call_id)
        asr_jobs = await self.repository.list_asr_jobs(recording.call_id)
        return {
            "id": str(recording.id),
            "callId": recording.call_id,
            "roomName": recording.room_name,
            "status": recording.status,
            "egressId": recording.egress_id,
            "ossId": str(recording.oss_id) if recording.oss_id is not None else None,
            "objectName": recording.object_name,
            "playUrl": play_url,
            "startedAt": recording.started_at,
            "endedAt": recording.ended_at,
            "durationMs": recording.duration_ms,
            "failureStage": recording.failure_stage,
            "failureMessage": recording.failure_message,
            "stopRequestedAt": recording.stop_requested_at,
            "verifyAttempts": recording.verify_attempts,
            "nextVerifyAt": recording.next_verify_at,
            "verifyDeadlineAt": recording.verify_deadline_at,
            "lastVerifyAt": recording.last_verify_at,
            "lastVerifyError": recording.last_verify_error,
            "tracks": [await self._track_to_dict(track) for track in tracks],
            "asrJobs": [self._asr_job_to_dict(job) for job in asr_jobs],
        }

    async def _track_to_dict(self, track: AiCallRecordingTrackModel) -> dict[str, Any]:
        play_url = await self._play_url(track.oss_id)
        return {
            "id": str(track.id),
            "callId": track.call_id,
            "roomName": track.room_name,
            "trackRole": track.track_role,
            "participantIdentity": track.participant_identity,
            "handoffId": track.handoff_id,
            "status": track.status,
            "egressId": track.egress_id,
            "ossId": str(track.oss_id) if track.oss_id is not None else None,
            "objectName": track.object_name,
            "playUrl": play_url,
            "startedAt": track.started_at,
            "endedAt": track.ended_at,
            "durationMs": track.duration_ms,
            "failureStage": track.failure_stage,
            "failureMessage": track.failure_message,
            "stopRequestedAt": track.stop_requested_at,
            "verifyAttempts": track.verify_attempts,
            "nextVerifyAt": track.next_verify_at,
            "verifyDeadlineAt": track.verify_deadline_at,
            "lastVerifyAt": track.last_verify_at,
            "lastVerifyError": track.last_verify_error,
        }

    @staticmethod
    def _asr_job_to_dict(job: AiCallAsrJobModel) -> dict[str, Any]:
        return {
            "id": str(job.id),
            "callId": job.call_id,
            "trackId": str(job.track_id),
            "trackRole": job.track_role,
            "participantIdentity": job.participant_identity,
            "provider": job.provider,
            "model": job.model,
            "status": job.status,
            "taskId": job.task_id,
            "sourceUrl": job.source_url,
            "transcriptionUrl": job.transcription_url,
            "submittedAt": job.submitted_at,
            "completedAt": job.completed_at,
            "segmentCount": job.segment_count,
            "failureStage": job.failure_stage,
            "failureMessage": job.failure_message,
        }

    async def _play_url(self, oss_id: int | None) -> str | None:
        if oss_id is None:
            return None
        auth = AuthSchema(user=None, check_data_scope=False, db=self.repository.db)
        return await OssService.get_presigned_url_by_oss_id_service(auth, oss_id)

    async def _register_recording_object(self, object_name: str, file_size: int | None) -> int:
        return await OssService.register_existing_object_service(
            self.repository.db,
            object_name=object_name,
            original_filename=Path(object_name).name,
            content_type=self._content_type(object_name),
            file_size=file_size,
        )

    async def _complete_main_recording_from_existing_object(
        self,
        recording: AiCallRecordingModel,
        exc: Exception,
        *,
        file_size: int | None = None,
        verify_attempts: int | None = None,
        verified_at: datetime | None = None,
    ) -> bool:
        object_name = recording.object_name
        if not object_name:
            return False
        if file_size is None:
            file_size = await self._resolve_existing_recording_object_size(object_name)
        if file_size is None:
            return False

        ended_at = verified_at or utc_now()
        duration_ms = self._duration_ms(recording.started_at, ended_at)
        log.info(
            "AI Call 录音停止结果不确定，已通过OSS回查恢复完成状态: "
            "callId={}, objectName={}, fileSize={}, errorType={}",
            recording.call_id,
            object_name,
            file_size,
            type(exc).__name__,
        )
        try:
            oss_id = await self._register_recording_object(object_name, file_size)
        except Exception as register_exc:
            log.warning(
                "AI Call 录音停止超时后OSS索引补偿失败: callId={}, objectName={}, "
                "errorType={}, message={}",
                recording.call_id,
                object_name,
                type(register_exc).__name__,
                str(register_exc),
            )
            await self.repository.update_recording(
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                status="failed",
                object_name=object_name,
                ended_at=ended_at,
                duration_ms=duration_ms,
                verify_attempts=verify_attempts,
                last_verify_at=ended_at,
                last_verify_error=str(register_exc)[:500],
                next_verify_at=None,
                failure_stage="oss_register",
                failure_message="录音文件索引登记失败",
            )
            return True

        try:
            await self.repository.update_recording(
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                status="completed",
                object_name=object_name,
                oss_id=oss_id,
                ended_at=ended_at,
                duration_ms=duration_ms,
                verify_attempts=verify_attempts,
                last_verify_at=ended_at,
                last_verify_error=None,
                next_verify_at=None,
                failure_stage=None,
                failure_message=None,
            )
        except Exception as update_exc:
            log.warning(
                "AI Call 录音停止超时后完成状态补偿失败: callId={}, errorType={}, message={}",
                recording.call_id,
                type(update_exc).__name__,
                str(update_exc),
            )
            await self.fail_recording(
                tenant_id=recording.tenant_id,
                call_id=recording.call_id,
                failure_stage="recording_update",
                failure_message="录音完成状态更新失败",
            )
        return True

    async def _complete_participant_recording_from_existing_object(
        self,
        track: AiCallRecordingTrackModel,
        exc: Exception,
        *,
        file_size: int | None = None,
        verify_attempts: int | None = None,
        verified_at: datetime | None = None,
    ) -> bool:
        object_name = track.object_name
        if not object_name:
            return False
        if file_size is None:
            file_size = await self._resolve_existing_recording_object_size(object_name)
        if file_size is None:
            return False

        ended_at = verified_at or utc_now()
        duration_ms = self._duration_ms(track.started_at, ended_at)
        log.info(
            "AI Call 分轨录音停止结果不确定，已通过OSS回查恢复完成状态: "
            "callId={}, trackRole={}, participantIdentity={}, objectName={}, "
            "fileSize={}, errorType={}",
            track.call_id,
            track.track_role,
            track.participant_identity,
            object_name,
            file_size,
            type(exc).__name__,
        )
        try:
            oss_id = await self._register_recording_object(object_name, file_size)
        except Exception as register_exc:
            log.warning(
                "AI Call 分轨录音停止超时后OSS索引补偿失败: callId={}, trackRole={}, "
                "participantIdentity={}, objectName={}, errorType={}, message={}",
                track.call_id,
                track.track_role,
                track.participant_identity,
                object_name,
                type(register_exc).__name__,
                str(register_exc),
            )
            await self.repository.update_recording_track(
                track.id,
                status="failed",
                object_name=object_name,
                ended_at=ended_at,
                duration_ms=duration_ms,
                verify_attempts=verify_attempts,
                last_verify_at=ended_at,
                last_verify_error=str(register_exc)[:500],
                next_verify_at=None,
                failure_stage="oss_register",
                failure_message="分轨录音文件索引登记失败",
            )
            return True

        await self.repository.update_recording_track(
            track.id,
            status="completed",
            object_name=object_name,
            oss_id=oss_id,
            ended_at=ended_at,
            duration_ms=duration_ms,
            verify_attempts=verify_attempts,
            last_verify_at=ended_at,
            last_verify_error=None,
            next_verify_at=None,
            failure_stage=None,
            failure_message=None,
        )
        return True

    async def _resolve_existing_recording_object_size(self, object_name: str) -> int | None:
        oss_config = OssService.active_config()
        if not oss_config:
            return None
        try:
            file_size = await OssService.resolve_existing_object_size(
                oss_config,
                object_name,
            )
        except Exception as exc:
            log.warning(
                "AI Call 录音停止结果OSS对象回查失败: objectName={}, errorType={}, message={}",
                object_name,
                type(exc).__name__,
                str(exc),
            )
            return None
        if file_size is not None and file_size >= 0:
            return file_size
        return None

    async def _checkpoint_before_external_io(self) -> None:
        if self.transaction_checkpoint is not None:
            await self.transaction_checkpoint()

    def _verify_deadline_at(self, stop_requested_at: datetime) -> datetime:
        return self._aware_datetime(stop_requested_at) + timedelta(
            seconds=self.verify_deadline_seconds,
        )

    @staticmethod
    def _next_verify_at(
        now: datetime,
        attempts: int,
        deadline_at: datetime | None,
    ) -> datetime:
        delay_seconds = min(60, max(5, 5 * (2 ** max(0, attempts - 1))))
        candidate = now + timedelta(seconds=delay_seconds)
        if deadline_at is not None and candidate > deadline_at:
            return deadline_at
        return candidate

    @staticmethod
    def _aware_datetime(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    @staticmethod
    def _duration_ms(started_at: datetime | None, ended_at: datetime) -> int | None:
        if started_at is None:
            return None
        start = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
        end = ended_at if ended_at.tzinfo else ended_at.replace(tzinfo=timezone.utc)
        return max(0, int((end - start).total_seconds() * 1000))

    @staticmethod
    def _content_type(object_name: str) -> str:
        suffix = Path(object_name).suffix.lower()
        if suffix == ".mp4":
            return "video/mp4"
        if suffix == ".mp3":
            return "audio/mpeg"
        if suffix == ".ogg":
            return "audio/ogg"
        return "application/octet-stream"

    @staticmethod
    def _is_failed_egress_status(status: str) -> bool:
        normalized = str(status or "").upper()
        return normalized in {
            "EGRESS_FAILED",
            "EGRESS_ABORTED",
            "EGRESS_LIMIT_REACHED",
            "FAILED",
            "ABORTED",
            "LIMIT_REACHED",
        }


class AiCallRecordingReconcileWorker:
    """后台确认 StopEgress 超时后的录音最终状态。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        service_factory: Callable[[AiCallRecordRepository], AiCallRecordingService],
        *,
        enabled: bool = True,
        interval_seconds: float = 5.0,
        batch_size: int = 50,
        on_call_ready_for_asr: Callable[[str], None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.service_factory = service_factory
        self.enabled = enabled
        self.interval_seconds = max(0.5, interval_seconds)
        self.batch_size = max(1, batch_size)
        self.on_call_ready_for_asr = on_call_ready_for_asr
        self.processed_count = 0
        self.failed_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-recording-reconcile-worker",
        )

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 录音对账 worker 关闭超时")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        finally:
            self._task = None

    async def flush_once(self) -> set[str]:
        return await self._reconcile_once()

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._reconcile_once()
            except Exception as exc:
                self.failed_count += 1
                log.warning(
                    "AI Call 录音对账失败: errorType={}, message={}",
                    type(exc).__name__,
                    str(exc),
                )
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self.interval_seconds,
                )
            except TimeoutError:
                continue

    async def _reconcile_once(self) -> set[str]:
        if not self.enabled:
            return set()
        async with self.session_factory() as db:
            async with db.begin():
                repository = AiCallRecordRepository(db)
                service = self.service_factory(repository)
                ready_call_ids = await service.reconcile_due_recordings(
                    limit=self.batch_size,
                )
            for call_id in sorted(ready_call_ids):
                if self.on_call_ready_for_asr is not None:
                    self.on_call_ready_for_asr(call_id)
            self.processed_count += len(ready_call_ids)
            return ready_call_ids
