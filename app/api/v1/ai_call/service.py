from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from fastapi import status
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.common.constant import RET
from app.config.setting import settings
from app.core.database import async_db_session
from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.dialogue_service import (
    AiCallDialoguePersistenceWorker,
    AiCallDialogueRuntimeStore,
    AiCallDialogueService,
)
from app.services.ai_call.event_store import AiCallEvent
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.handoff_exception_manager import AiCallHandoffExceptionManager
from app.services.ai_call.handoff_service import AiCallHandoffService
from app.services.ai_call.handoff_unanswered_service import (
    AiCallHandoffUnansweredService,
)
from app.services.ai_call.interrupt_summary import build_interrupt_summary
from app.services.ai_call.livekit_egress import LiveKitEgressManager
from app.services.ai_call.livekit_sip import (
    CreateSipParticipantResult,
    LiveKitSipClient,
    SipOutboundConfig,
)
from app.services.ai_call.orchestrator import (
    AiCallOrchestrator,
    BrowserEventReportResult,
    CreateSessionResult,
    EndSessionResult,
    EventListResult,
    ReissueTokenResult,
    SessionStatusResult,
)
from app.services.ai_call.prompt_config import (
    CALL_END_TOOL_INSTRUCTIONS,
    PROMPT_PROVIDER_STATIC_PROFILE,
    BusinessPromptResolver,
    DebugPromptProvider,
    DefaultPromptProvider,
    PromptComponent,
    PromptComposer,
    PromptEffectiveConfig,
    PromptResolveContext,
    normalize_scene_code,
)
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.recording_service import AiCallRecordingService
from app.services.ai_call.recov_collection_prompt import RecovCollectionPostgresPromptStore
from app.services.ai_call.runtime_control.command_repository import (
    EndCallIntent,
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.roles import runtime_control_mode_for_entry
from app.services.ai_call.semantic_analysis import (
    AiCallSemanticAnalysisService,
    build_default_semantic_analyzer,
)
from app.services.ai_call.session_registry import RUNNING_STATUSES, CallSessionStatus
from app.services.ai_call.system_prompt_player import LiveKitSystemPromptPlayer
from app.services.ai_call.voice_profile import (
    VOICE_GENDER_FEMALE,
    VOICE_GENDER_MALE,
    VOICE_GENDER_UNKNOWN,
    VOICE_TYPE_CUSTOM_CLONE,
    builtin_voice_profile_values,
)
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.services.ai_call.event_persistence import AiCallEventPersistenceWorker
    from app.services.ai_call.handoff_trigger_service import AiCallHandoffTriggerWorker
    from app.services.ai_call.offline_asr_service import AiCallOfflineAsrWorker
    from app.services.ai_call.semantic_analysis import AiCallSemanticAnalysisWorker


@dataclass(frozen=True, slots=True)
class CreateSipSessionResult:
    call_id: str
    room_name: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: Any
    sip_call_id: str | None
    sip_call_id_full: str | None
    sip_trunk_id: str | None
    sip_call_status: str | None


class AiCallService:
    def __init__(
        self,
        orchestrator: AiCallOrchestrator,
        record_service: AiCallRecordService | None = None,
        recording_service: AiCallRecordingService | None = None,
        dialogue_service: AiCallDialogueService | None = None,
        handoff_service: AiCallHandoffService | None = None,
        handoff_exception_manager: AiCallHandoffExceptionManager | None = None,
        prompt_repository: AiCallRecordRepository | None = None,
        prompt_resolver: BusinessPromptResolver | None = None,
        prompt_composer: PromptComposer | None = None,
        sip_client: LiveKitSipClient | None = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.record_service = record_service
        self.recording_service = recording_service
        self.dialogue_service = dialogue_service
        self.handoff_service = handoff_service
        self.handoff_exception_manager = handoff_exception_manager
        self.prompt_repository = prompt_repository or (
            record_service.repository if record_service is not None else None
        )
        self.prompt_resolver = prompt_resolver
        self.prompt_composer = prompt_composer
        self.sip_client = sip_client

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
        business_id: str | None = None,
        scene_code: str | None = None,
        business_params: dict | None = None,
        tenant_id: str | None = None,
    ) -> CreateSessionResult:
        self._ensure_legacy_entry_allowed("web")
        if self.record_service is None:
            try:
                return await self.orchestrator.create_web_session(voice=voice, prompt=prompt)
            except AiCallError as exc:
                raise self._to_custom_exception(exc) from exc

        if self.prompt_resolver is not None or self.prompt_composer is not None:
            scene_code = self._require_scene_code(scene_code)
            if _strip_or_none(prompt):
                raise CustomException(
                    msg="调试提示词已下线，请使用业务场景提示词配置",
                    code=RET.ERROR.code,
                    status_code=400,
                )

        resolved_voice = await self._resolve_voice(voice)
        call_id = f"call_{generate_snowflake_id()}"
        room_name = f"ai-call-{call_id}"
        participant_identity = f"browser-{call_id}"
        await self.record_service.create_web_record(
            tenant_id=tenant_id,
            call_id=call_id,
            business_id=business_id,
            room_name=room_name,
            participant_identity=participant_identity,
        )
        try:
            prompt_effective_config = await self._resolve_prompt_effective_config(
                call_id=call_id,
                business_id=business_id,
                scene_code=scene_code,
                business_params=business_params or {},
                debug_prompt=None,
            )
            await self.record_service.update_prompt_context(
                call_id,
                scene_code=scene_code,
                prompt_source_key=(
                    prompt_effective_config.prompt_source_key
                    if prompt_effective_config is not None
                    else None
                ),
            )
            result = await self.orchestrator.create_web_session(
                voice=resolved_voice,
                prompt=None,
                call_id=call_id,
                prompt_effective_config=prompt_effective_config,
            )
        except AiCallError as exc:
            await self.record_service.fail_session(
                call_id,
                end_reason=exc.error_id,
                failure_stage=self._failure_stage_for_end_reason(exc.error_id),
                failure_message=exc.msg,
            )
            raise self._to_custom_exception(exc) from exc
        await self.record_service.mark_status(call_id, result.status)
        if self.recording_service is not None:
            await self.recording_service.start_for_session(
                tenant_id=self._require_recording_tenant(tenant_id),
                call_id=result.call_id,
                room_name=result.room_name,
                customer_participant_identity=result.participant_identity,
                ai_participant_identity=f"agent-{result.call_id}",
            )
        return result

    async def create_sip_session(
        self,
        *,
        callee_phone_number: str,
        voice: str | None,
        call_id: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        scene_code: str | None = None,
        business_params: dict | None = None,
        ringing_timeout_seconds: int | None = None,
        before_sip_invite: Callable[[], Awaitable[None]] | None = None,
        tenant_id: str | None = None,
    ) -> CreateSipSessionResult:
        self._ensure_legacy_entry_allowed("direct_sip")
        self._ensure_record_service()
        if self.sip_client is None:
            raise CustomException(
                msg="SIP 外呼服务未启用",
                code=RET.ERROR.code,
                status_code=500,
            )
        if self.prompt_resolver is not None or self.prompt_composer is not None:
            scene_code = self._require_scene_code(scene_code)

        callee_phone_number_masked = self._mask_phone_number(callee_phone_number)
        callee_phone_number_hash = self._callee_phone_number_hash(callee_phone_number)
        await self._ensure_no_active_sip_outbound_for_callee(
            callee_phone_number_hash=callee_phone_number_hash,
            callee_phone_number_masked=callee_phone_number_masked,
        )

        resolved_voice = await self._resolve_voice(voice)
        resolved_call_id = call_id or f"call_{generate_snowflake_id()}"
        room_name = f"ai-call-{resolved_call_id}"
        participant_identity = f"sip-{resolved_call_id}"
        await self.record_service.create_sip_record(
            tenant_id=tenant_id,
            call_id=resolved_call_id,
            business_type=business_type,
            business_id=business_id,
            room_name=room_name,
            participant_identity=participant_identity,
            callee_phone_number_hash=callee_phone_number_hash,
            callee_phone_number_masked=callee_phone_number_masked,
        )
        sip_invite_sent = False
        try:
            self._record_sip_preflight(
                call_id=resolved_call_id,
                callee_phone_number=callee_phone_number,
            )
            prompt_effective_config = await self._resolve_prompt_effective_config(
                call_id=resolved_call_id,
                business_id=business_id,
                scene_code=scene_code,
                business_params=business_params or {},
                debug_prompt=None,
            )
            await self.record_service.update_prompt_context(
                resolved_call_id,
                scene_code=scene_code,
                prompt_source_key=(
                    prompt_effective_config.prompt_source_key
                    if prompt_effective_config is not None
                    else None
                ),
            )
            room_session = await self.orchestrator.create_sip_session(
                voice=resolved_voice,
                prompt=None,
                call_id=resolved_call_id,
                prompt_effective_config=prompt_effective_config,
            )
            await self.record_service.mark_status(resolved_call_id, room_session.status)
            self._record_sip_event(
                call_id=resolved_call_id,
                event_type="sip_invite_sent",
                payload={
                    "participantIdentity": participant_identity,
                    "calleePhoneNumberMasked": callee_phone_number_masked,
                    "calleePhoneNumberHash": callee_phone_number_hash,
                    "ringingTimeoutSeconds": ringing_timeout_seconds,
                },
            )
            if before_sip_invite is not None:
                await before_sip_invite()
            sip_invite_sent = True
            sip_participant = await self.sip_client.create_participant(
                room_name=room_name,
                participant_identity=participant_identity,
                callee_phone_number=callee_phone_number,
                ringing_timeout_seconds=ringing_timeout_seconds,
                wait_until_answered=True,
            )
            self._record_successful_sip_participant(
                call_id=resolved_call_id,
                sip_participant=sip_participant,
            )
        except AiCallError as exc:
            cleanup_error: Exception | None = None
            if sip_invite_sent:
                try:
                    await self._cleanup_sip_resources(
                        call_id=resolved_call_id,
                        room_name=room_name,
                    )
                except Exception as cleanup_exc:
                    cleanup_error = cleanup_exc
                    exc = AiCallError(
                        error_id=exc.error_id,
                        msg=f"{exc.msg}；SIP 资源清理失败，保持待对账",
                        status_code=exc.status_code,
                        details={
                            **exc.details,
                            "cleanupFailed": True,
                            "cleanupErrorType": type(cleanup_exc).__name__,
                            "cleanupMessage": str(cleanup_exc)[:200],
                        },
                    )
            if sip_invite_sent:
                failure_payload = {
                    "errorId": exc.error_id,
                    "message": exc.msg,
                    "calleePhoneNumberMasked": callee_phone_number_masked,
                    "calleePhoneNumberHash": callee_phone_number_hash,
                }
                failure_payload.update(exc.details)
                self._record_sip_event(
                    call_id=resolved_call_id,
                    event_type="sip_failed",
                    payload=failure_payload,
                )
            if cleanup_error is None:
                await self.record_service.fail_session(
                    resolved_call_id,
                    end_reason=exc.error_id,
                    failure_stage=self._failure_stage_for_end_reason(exc.error_id),
                    failure_message=exc.msg,
                )
            raise self._to_custom_exception(exc) from exc

        await self.record_service.mark_answered(
            resolved_call_id,
            datetime.now(timezone.utc),
        )
        if self.recording_service is not None:
            await self.recording_service.start_for_session(
                tenant_id=self._require_recording_tenant(tenant_id),
                call_id=resolved_call_id,
                room_name=room_name,
                customer_participant_identity=participant_identity,
                ai_participant_identity=f"agent-{resolved_call_id}",
            )
        await self.orchestrator.start_opening(resolved_call_id)
        if self.recording_service is not None:
            await self.recording_service.start_session_participant_recordings(
                tenant_id=self._require_recording_tenant(tenant_id),
                call_id=resolved_call_id,
                room_name=room_name,
                customer_participant_identity=participant_identity,
                ai_participant_identity=f"agent-{resolved_call_id}",
            )
        return CreateSipSessionResult(
            call_id=resolved_call_id,
            room_name=room_name,
            participant_identity=participant_identity,
            status=room_session.status,
            effective_config=room_session.effective_config,
            sip_call_id=sip_participant.sip_call_id,
            sip_call_id_full=sip_participant.sip_call_id_full,
            sip_trunk_id=sip_participant.sip_trunk_id,
            sip_call_status=sip_participant.sip_call_status,
        )

    @staticmethod
    def _ensure_legacy_entry_allowed(entry: str) -> None:
        if runtime_control_mode_for_entry(settings, entry) == "owner_command_v1":
            raise CustomException(
                msg=f"{entry} 入口已切换为异步 START_CALL，请使用运行时入口",
                code=RET.ERROR.code,
                status_code=409,
            )

    async def list_voice_profiles(
        self,
        *,
        voice_type: str | None = None,
        gender: str | None = None,
        target_model: str | None = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> dict:
        repository = self._ensure_prompt_repository()
        resolved_target_model = (
            target_model or self.orchestrator.config.qwen_realtime_model
        ).strip() or self.orchestrator.config.qwen_realtime_model
        await self._ensure_builtin_voice_profiles(repository, resolved_target_model)
        rows, total = await repository.list_voice_profiles(
            target_model=resolved_target_model,
            voice_type=_strip_or_none(voice_type),
            gender=_strip_or_none(gender),
            page_num=page_num,
            page_size=page_size,
        )
        return {
            "rows": [self._voice_profile_to_dict(row) for row in rows],
            "total": total,
        }

    async def create_voice_profile(self, values: dict) -> dict:
        repository = self._ensure_prompt_repository()
        values = self._normalize_voice_profile_values(values)
        existing = await repository.get_voice_profile_by_voice(
            voice=values["voice"],
            target_model=values["target_model"],
        )
        if existing is not None:
            raise CustomException(
                msg="该模型下音色已存在",
                code=RET.ERROR.code,
                status_code=400,
            )
        profile = await repository.create_voice_profile(**values)
        return self._voice_profile_to_dict(profile)

    async def list_prompt_profiles(
        self,
        *,
        scene_code: str | None = None,
        page_num: int = 1,
        page_size: int = 20,
    ) -> dict:
        repository = self._ensure_prompt_repository()
        rows, total = await repository.list_prompt_profiles(
            scene_code=normalize_scene_code(scene_code),
            page_num=page_num,
            page_size=page_size,
        )
        return {
            "rows": [self._prompt_profile_to_dict(row) for row in rows],
            "total": total,
        }

    async def get_prompt_profile(self, profile_id: int) -> dict:
        repository = self._ensure_prompt_repository()
        profile = await repository.get_prompt_profile(profile_id)
        if profile is None:
            raise CustomException(msg="提示词配置不存在", code=RET.ERROR.code, status_code=404)
        return self._prompt_profile_to_dict(profile)

    async def create_prompt_profile(self, values: dict) -> dict:
        repository = self._ensure_prompt_repository()
        values = self._normalize_prompt_profile_values(values)
        await self._ensure_prompt_profile_unique(repository, values)
        profile = await repository.create_prompt_profile(**values)
        return self._prompt_profile_to_dict(profile)

    async def update_prompt_profile(self, profile_id: int, values: dict) -> dict:
        repository = self._ensure_prompt_repository()
        values = self._normalize_prompt_profile_values(values)
        existing = await repository.get_prompt_profile(profile_id)
        if existing is None:
            raise CustomException(msg="提示词配置不存在", code=RET.ERROR.code, status_code=404)
        await self._ensure_prompt_profile_unique(repository, values, current_id=profile_id)
        profile = await repository.update_prompt_profile(profile_id, **values)
        if profile is None:
            raise CustomException(msg="提示词配置不存在", code=RET.ERROR.code, status_code=404)
        return self._prompt_profile_to_dict(profile)

    async def list_prompt_components(self) -> dict:
        composer = self._ensure_prompt_composer()
        components = [
            *composer.public_components(),
            PromptComponent(
                component_key="call_end_tool",
                name="结束通话工具约束",
                content=CALL_END_TOOL_INSTRUCTIONS,
            ),
        ]
        rows = [
            {
                "componentKey": component.component_key,
                "name": component.name,
                "content": component.content,
            }
            for component in components
        ]
        return {"rows": rows, "total": len(rows)}

    async def preview_prompt_profile(
        self,
        *,
        business_id: str | None,
        scene_code: str | None,
        business_params: dict | None,
        prompt: str | None,
    ) -> dict:
        scene_code = self._require_scene_code(scene_code)
        if _strip_or_none(prompt):
            raise CustomException(
                msg="调试提示词已下线，请使用业务场景提示词配置",
                code=RET.ERROR.code,
                status_code=400,
            )
        try:
            effective_config = await self._resolve_prompt_effective_config(
                call_id="preview",
                business_id=business_id,
                scene_code=scene_code,
                business_params=business_params or {},
                debug_prompt=None,
            )
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        return {
            "instructions": effective_config.instructions,
            "openingMessage": effective_config.opening_message,
            "promptHash": effective_config.prompt_hash,
            "openingMessageHash": effective_config.opening_message_hash,
            "promptSourceKey": effective_config.prompt_source_key,
            "bargeInEnabled": self.orchestrator.config.barge_in_enabled,
        }

    async def reissue_browser_token(self, call_id: str) -> ReissueTokenResult:
        try:
            result = await self.orchestrator.reissue_browser_token(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        return result

    async def get_session(self, call_id: str) -> SessionStatusResult:
        try:
            return await self.orchestrator.get_session(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc

    async def list_events(
        self,
        call_id: str,
        limit: int,
        after_event_id: str | None,
    ) -> EventListResult:
        try:
            return await self.orchestrator.list_events(
                call_id=call_id,
                limit=limit,
                after_event_id=after_event_id,
            )
        except AiCallError as exc:
            if self.record_service is not None:
                record = await self.record_service.get_record(call_id)
                if record is not None:
                    rows = await self.record_service.list_events(
                        call_id,
                        limit=limit,
                        after_event_id=after_event_id,
                    )
                    return self._event_rows_to_runtime_result(rows)
            raise self._to_custom_exception(exc) from exc

    async def report_browser_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime | None,
        payload: dict[str, Any] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> BrowserEventReportResult:
        try:
            tenant_id = self._require_browser_event_tenant(tenant_id)
            if self.record_service is None:
                raise CustomException(
                    msg="通话事件租户上下文不匹配",
                    code=RET.ERROR.code,
                    status_code=403,
                )
            record = await self.record_service.get_record_for_tenant(
                tenant_id=tenant_id,
                call_id=call_id,
            )
            if record is None:
                raise CustomException(
                    msg="通话事件租户上下文不匹配",
                    code=RET.ERROR.code,
                    status_code=403,
                )
            owner_mode = record.runtime_control_mode == "owner_command_v1"
            if (
                event_type == "browser_disconnect"
                and self.recording_service is not None
                and not owner_mode
            ):
                session = await self.orchestrator.get_session(call_id)
                if session.status in RUNNING_STATUSES:
                    try:
                        await self.recording_service.stop_for_session(
                            tenant_id=tenant_id,
                            call_id=call_id,
                        )
                    except OperationalError as exc:
                        if not _is_sqlite_database_locked(exc):
                            raise
                        log.warning(
                            "AI Call 浏览器断开录音停止遇到 SQLite 写锁，已降级为后台收尾: "
                            "callId={}, message={}",
                            call_id,
                            str(exc),
                        )
            elif event_type == "browser_ready" and not owner_mode:
                await self._start_browser_ready_recording_tracks(
                    tenant_id=tenant_id,
                    call_id=call_id,
                    record=record,
                )
            if owner_mode and event_type in {
                "browser_ready",
                "browser_disconnect",
                "browser_first_audio",
                "browser_audio_input_diagnostics",
            }:
                reported_at = timestamp or datetime.now(timezone.utc)
                event_payload = dict(payload or {})
                event_payload["reportedAt"] = reported_at.isoformat()
                event = self.orchestrator.event_store.append(
                    call_id=call_id,
                    type=event_type,
                    source="browser",
                    payload=event_payload,
                    timestamp=reported_at,
                )
                result = BrowserEventReportResult(
                    event_id=event.event_id,
                    call_id=event.call_id,
                    type=event.type,
                    timestamp=event.timestamp,
                    source=event.source,
                    payload=event.payload,
                )
            else:
                result = await self.orchestrator.report_browser_event(
                    call_id=call_id,
                    event_type=event_type,
                    timestamp=timestamp,
                    payload=payload,
                )
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        if self.record_service is not None:
            if event_type == "browser_ready":
                if record.runtime_control_mode == "owner_command_v1":
                    await self.record_service.mark_owner_customer_ready(
                        tenant_id=tenant_id,
                        call_id=call_id,
                    )
                else:
                    await self.record_service.mark_answered(call_id, result.timestamp)
            elif event_type == "browser_disconnect" and record.runtime_control_mode == (
                "owner_command_v1"
            ):
                await RuntimeCommandRepository(
                    self.record_service.repository.db
                ).request_end(
                    EndCallIntent(
                        tenant_id=tenant_id,
                        call_id=call_id,
                        source="browser_client",
                        end_reason="browser_disconnect",
                        dedupe_key=f"browser_disconnect:{call_id}",
                        event_at=result.timestamp,
                        evidence={
                            "eventId": result.event_id,
                            "eventType": result.type,
                        },
                    )
                )
            elif (
                event_type == "browser_disconnect"
                and result.payload.get("terminalSessionStatus") is None
            ):
                await self.record_service.complete_session(
                    call_id,
                    end_reason="browser_disconnect",
                    ended_at=result.timestamp,
                )
                await self._finalize_handoffs_for_call(
                    call_id,
                    end_reason="browser_disconnect",
                )
                await self._enqueue_offline_asr_if_recordings_closed(
                    call_id,
                    tenant_id=tenant_id,
                )
        return result

    async def handle_livekit_webhook_event(
        self,
        *,
        event_type: str,
        room_name: str | None,
        participant_identity: str | None,
        payload: dict[str, Any] | None = None,
    ) -> dict:
        if event_type not in {"participant_left", "track_published"}:
            return {"handled": False, "reason": "unsupported_event"}
        call_id = self._call_id_from_sip_participant(participant_identity)
        if call_id is None:
            return {"handled": False, "reason": "non_sip_participant"}
        if room_name and room_name != f"ai-call-{call_id}":
            return {"handled": False, "reason": "room_mismatch"}

        if event_type == "track_published":
            track = (payload or {}).get("track")
            track_type = track.get("type") if isinstance(track, dict) else None
            if str(track_type or "").upper() != "AUDIO":
                return {"handled": False, "reason": "non_audio_track"}
            if self.record_service is None:
                return {"handled": False, "reason": "record_service_unavailable"}
            record = await self.record_service.get_record(call_id)
            if record is None:
                return {"handled": False, "reason": "record_not_found"}
            if record.entry_type != "sip_outbound":
                return {"handled": False, "reason": "not_sip_outbound"}
            if room_name and record.room_name != room_name:
                return {"handled": False, "reason": "record_room_mismatch"}
            if participant_identity != record.participant_identity:
                return {"handled": False, "reason": "record_participant_mismatch"}
            if str(record.status or "").lower() in {"completed", "failed"}:
                return {"handled": False, "reason": "record_terminal"}
            event = self._record_sip_event(
                call_id=call_id,
                event_type="media_connected",
                source="livekit",
                payload={
                    "participantIdentity": participant_identity,
                    "trackSid": track.get("sid"),
                    "evidence": "audio_track_published",
                },
            )
            if event is None:
                return {"handled": False, "reason": "event_record_failed"}
            await self.record_service.mirror_runtime_events([event])
            return {
                "handled": True,
                "action": "record_media_connected",
                "callId": call_id,
            }

        try:
            session = await self.orchestrator.get_session(call_id)
        except AiCallError:
            if self.record_service is None:
                return {"handled": False, "reason": "record_service_unavailable"}
            record = await self.record_service.get_record(call_id)
            if record is None:
                return {"handled": False, "reason": "record_not_found"}
            if record.entry_type != "sip_outbound":
                return {"handled": False, "reason": "not_sip_outbound"}
            if room_name and record.room_name != room_name:
                return {"handled": False, "reason": "record_room_mismatch"}
            if participant_identity != record.participant_identity:
                return {"handled": False, "reason": "record_participant_mismatch"}
            if str(record.status or "").lower() in {"completed", "failed"}:
                return {"handled": False, "reason": "record_terminal"}
            return await self._end_persisted_sip_session_after_remote_hangup(
                call_id=call_id,
                room_name=record.room_name,
                participant_identity=participant_identity,
                event_type=event_type,
                payload=payload,
            )
        if session.status not in RUNNING_STATUSES:
            return {"handled": False, "reason": "session_not_running"}

        self._record_sip_event(
            call_id=call_id,
            event_type="sip_hangup",
            source="livekit",
            payload=self._sip_hangup_payload(
                room_name=room_name,
                participant_identity=participant_identity,
                event_type=event_type,
                payload=payload,
            ),
        )
        await self.end_session(call_id, end_reason="remote_hangup")
        return {
            "handled": True,
            "action": "end_session",
            "callId": call_id,
            "endReason": "remote_hangup",
        }

    async def _end_persisted_sip_session_after_remote_hangup(
        self,
        *,
        call_id: str,
        room_name: str,
        participant_identity: str,
        event_type: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        assert self.record_service is not None
        hangup_event = self.orchestrator.event_store.append(
            call_id=call_id,
            type="sip_hangup",
            source="livekit",
            payload=self._sip_hangup_payload(
                room_name=room_name,
                participant_identity=participant_identity,
                event_type=event_type,
                payload=payload,
            ),
        )
        await self.record_service.mirror_runtime_events([hangup_event])
        await self._cleanup_sip_resources(
            call_id=call_id,
            room_name=room_name,
        )
        await self.record_service.complete_session(
            call_id,
            end_reason="remote_hangup",
        )
        await self._finalize_handoffs_for_call(
            call_id,
            end_reason="remote_hangup",
        )
        completed_event = self.orchestrator.event_store.append(
            call_id=call_id,
            type="session_completed",
            source="orchestrator",
            payload={"endReason": "remote_hangup"},
        )
        await self.record_service.mirror_runtime_events([completed_event])
        await self._enqueue_offline_asr_if_recordings_closed(call_id)
        return {
            "handled": True,
            "action": "end_persisted_session",
            "callId": call_id,
            "endReason": "remote_hangup",
        }

    async def terminate_sip_session(
        self,
        call_id: str,
        *,
        end_reason: str,
    ) -> None:
        room_name = f"ai-call-{call_id}"
        if self.record_service is not None:
            record = await self.record_service.get_record(call_id)
            if record is not None and record.room_name:
                room_name = record.room_name
        await self._cleanup_sip_resources(
            call_id=call_id,
            room_name=room_name,
        )
        if self.record_service is not None:
            await self.record_service.fail_session(
                call_id,
                end_reason=end_reason,
                failure_stage="reconciliation",
                failure_message="SIP 通话状态对账超时并已终止资源",
            )

    async def _cleanup_sip_resources(
        self,
        *,
        call_id: str,
        room_name: str,
    ) -> None:
        await self.orchestrator.livekit_room_manager.delete_room(room_name)
        cleanup_errors: list[str] = []
        try:
            await self.orchestrator.agent_runner.stop(call_id)
        except Exception as exc:
            cleanup_errors.append(
                f"agent_runner.stop: {type(exc).__name__}: {str(exc)[:160]}"
            )
        if self.recording_service is not None:
            try:
                await self.recording_service.stop_for_session(
                    tenant_id=await self._recording_tenant_for_call(call_id),
                    call_id=call_id,
                )
            except Exception as exc:
                cleanup_errors.append(
                    "recording_service.stop_for_session: "
                    f"{type(exc).__name__}: {str(exc)[:160]}"
                )
        if cleanup_errors:
            raise AiCallError(
                error_id="sip_cleanup_incomplete",
                msg="SIP 房间已终止，但本地资源清理未完成",
                details={"cleanupErrors": cleanup_errors},
            )
        try:
            session = self.orchestrator.registry.get(call_id)
        except AiCallError:
            return
        if session.status in RUNNING_STATUSES:
            self.orchestrator.registry.transition(
                call_id,
                CallSessionStatus.FAILED,
            )

    async def end_session(
        self,
        call_id: str,
        *,
        end_reason: str = "web_user_end",
    ) -> EndSessionResult:
        try:
            session = await self.orchestrator.get_session(call_id)
            if self.recording_service is not None and session.status != CallSessionStatus.COMPLETED:
                await self.recording_service.stop_for_session(
                    tenant_id=await self._recording_tenant_for_call(call_id),
                    call_id=call_id,
                )
            result = await self.orchestrator.end_session(call_id, end_reason=end_reason)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        if self.record_service is not None:
            if result.status == CallSessionStatus.COMPLETED:
                await self.record_service.complete_session(
                    call_id,
                    end_reason=end_reason,
                )
                await self._finalize_handoffs_for_call(
                    call_id,
                    end_reason=end_reason,
                )
                await self._enqueue_offline_asr_if_recordings_closed(call_id)
            elif result.status == CallSessionStatus.FAILED:
                await self.record_service.fail_session(
                    call_id,
                    end_reason="unknown",
                    failure_stage="runtime",
                    failure_message="会话已失败",
                )
                await self._finalize_handoffs_for_call(call_id, end_reason="runtime_failed")
                await self._enqueue_offline_asr_if_recordings_closed(call_id)
        return result

    async def list_records(
        self,
        *,
        tenant_id: str,
        call_id: str | None = None,
        task_id: int | None = None,
        target_id: int | None = None,
        phone_number: str | None = None,
        customer_name: str | None = None,
        call_result: str | None = None,
        customer_intent: str | None = None,
        follow_up_status: str | None = None,
        business_type: str | None = None,
        business_id: str | None = None,
        status: str | None = None,
        entry_type: str | None = None,
        formal_outbound_only: bool = False,
        started_at_begin: datetime | None = None,
        started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> dict:
        self._ensure_record_service()
        rows, total = await self.record_service.list_records(
            tenant_id=tenant_id,
            call_id=call_id,
            task_id=task_id,
            target_id=target_id,
            phone_number=phone_number,
            customer_name=customer_name,
            call_result=call_result,
            customer_intent=customer_intent,
            follow_up_status=follow_up_status,
            business_type=business_type,
            business_id=business_id,
            status=status,
            entry_type=entry_type,
            formal_outbound_only=formal_outbound_only,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
            page_num=page_num,
            page_size=page_size,
        )
        return {
            "rows": [self.record_service.record_to_dict(row) for row in rows],
            "total": total,
        }

    async def get_record_detail(self, call_id: str) -> dict:
        self._ensure_record_service()
        record = await self.record_service.get_record(call_id)
        if record is None:
            raise CustomException(msg="通话记录不存在", code=RET.ERROR.code, status_code=404)
        last_event = await self.record_service.get_last_event(call_id)
        execution_config = await self.record_service.get_execution_config(record)
        return {
            "record": self.record_service.record_to_dict(record),
            "lastEvent": self.record_service.event_to_dict(last_event) if last_event else None,
            "executionConfig": execution_config,
        }

    async def list_record_events(
        self,
        call_id: str,
        *,
        limit: int = 200,
        after_event_id: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> dict:
        self._ensure_record_service()
        rows = await self.record_service.list_events(
            call_id,
            limit=limit,
            after_event_id=after_event_id,
            event_type=event_type,
            source=source,
        )
        return {
            "rows": [self.record_service.event_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def get_record_interrupt_summary(self, call_id: str) -> dict:
        self._ensure_record_service()
        record = await self.record_service.get_record(call_id)
        if record is None:
            raise CustomException(msg="通话记录不存在", code=RET.ERROR.code, status_code=404)
        rows = await self.record_service.list_events(call_id, limit=1000)
        return build_interrupt_summary(
            call_id,
            [self.record_service.event_to_dict(row) for row in rows],
        )

    async def get_record_semantic_analysis(self, call_id: str) -> dict | None:
        repository = self._ensure_prompt_repository()
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        if analysis is None:
            return None
        return AiCallSemanticAnalysisService.analysis_to_dict(analysis)

    async def reanalyze_record_semantic_analysis(self, call_id: str) -> dict:
        repository = self._ensure_prompt_repository()
        record = await repository.get_record(call_id)
        if record is None:
            raise CustomException(msg="通话记录不存在", code=RET.ERROR.code, status_code=404)
        service = AiCallSemanticAnalysisService(
            repository,
            analyzer=_manual_semantic_analyzer(),
        )
        analysis = await service.reanalyze_call_once(
            call_id=call_id,
            scene_code=record.scene_code,
            reference_date=date.today().isoformat(),
            now=datetime.now(timezone.utc),
        )
        return AiCallSemanticAnalysisService.analysis_to_dict(analysis)

    async def get_recording(self, *, tenant_id: str, call_id: str) -> dict | None:
        self._ensure_recording_service()
        recording = await self.recording_service.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is None:
            return None
        return await self.recording_service.recording_to_dict(recording)

    async def list_dialogue_preview(self, call_id: str) -> dict:
        self._ensure_dialogue_service()
        return await self.dialogue_service.list_preview_segments(call_id)

    async def list_record_dialogue_segments(
        self,
        call_id: str,
        *,
        speaker_type: str | None = None,
        limit: int = 1000,
    ) -> dict:
        self._ensure_dialogue_service()
        rows = await self.dialogue_service.list_persisted_segments(
            call_id,
            speaker_type=speaker_type,
            limit=limit,
        )
        return {
            "rows": [self.dialogue_service.segment_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def create_handoff(
        self,
        *,
        call_id: str,
        source: str,
        reason: str | None,
        request_message: str | None,
        waiting_prompt_kind: str = "default",
    ) -> dict:
        self._ensure_handoff_service()
        try:
            session = await self.orchestrator.get_session(call_id)
        except AiCallError as exc:
            raise self._to_custom_exception(exc) from exc
        handoff, created = await self.handoff_service.create_request(
            call_id=call_id,
            room_name=session.room_name,
            source=source,
            reason=reason,
            request_message=request_message,
        )
        self._record_expired_handoff_events()
        if created:
            try:
                await self.orchestrator.suspend_for_handoff(
                    call_id=call_id,
                    handoff_id=handoff.handoff_id,
                    request_source=handoff.request_source,
                    request_reason=handoff.request_reason,
                )
            except AiCallError as exc:
                failed = await self.handoff_service.fail_request(
                    handoff_id=handoff.handoff_id,
                    failure_stage="agent_suspend",
                    failure_message=exc.msg,
                )
                if failed is not None:
                    self._trigger_handoff_exception_close(
                        failed,
                        call_end_reason="handoff_failed",
                    )
                raise self._to_custom_exception(exc) from exc
            except Exception as exc:
                failed = await self.handoff_service.fail_request(
                    handoff_id=handoff.handoff_id,
                    failure_stage="agent_suspend",
                    failure_message="Agent 挂起失败",
                )
                if failed is not None:
                    self._trigger_handoff_exception_close(
                        failed,
                        call_end_reason="handoff_failed",
                    )
                raise CustomException(
                    msg="Agent 挂起失败",
                    code=RET.ERROR.code,
                    status_code=500,
                ) from exc
            refreshed = await self.handoff_service.get_current(call_id)
            if refreshed is not None:
                handoff = refreshed
        if waiting_prompt_kind != "none":
            self._schedule_handoff_timeout(
                handoff,
                waiting_prompt_kind=waiting_prompt_kind,
            )
        if created:
            from app.services.ai_call.agent_console_reconciler import (
                publish_agent_console_event,
            )

            await publish_agent_console_event(
                handoff.tenant_id,
                "handoff.requested",
                {
                    "handoff_id": handoff.handoff_id,
                    "call_id": handoff.call_id,
                    "scene_code": handoff.scene_code,
                    "status": handoff.status,
                },
            )
        return self.handoff_service.handoff_to_dict(handoff)

    async def get_current_handoff(self, call_id: str) -> dict | None:
        self._ensure_handoff_service()
        handoff = await self.handoff_service.get_current(call_id)
        self._record_expired_handoff_events()
        return self.handoff_service.handoff_to_dict(handoff) if handoff else None

    async def list_handoffs(self, call_id: str) -> dict:
        self._ensure_handoff_service()
        rows = await self.handoff_service.list_handoffs(call_id)
        return {
            "rows": [self.handoff_service.handoff_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def list_joinable_handoffs(self, *, limit: int = 50) -> dict:
        self._ensure_handoff_service()
        rows = await self.handoff_service.list_joinable_handoffs(limit=limit)
        return {
            "rows": [self.handoff_service.handoff_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def get_handoff_agent_status(self, human_agent_identity: str) -> dict:
        self._ensure_handoff_service()
        agent = await self.handoff_service.get_agent_status(human_agent_identity)
        if agent is None:
            return self.handoff_service.default_handoff_agent_to_dict(human_agent_identity)
        return self.handoff_service.handoff_agent_to_dict(agent)

    async def set_handoff_agent_status(
        self,
        *,
        human_agent_identity: str,
        status: str,
        skill_group: str | None = None,
    ) -> dict:
        self._ensure_handoff_service()
        agent = await self.handoff_service.set_agent_status(
            human_agent_identity=human_agent_identity,
            agent_status=status,
            skill_group=skill_group,
        )
        return self.handoff_service.handoff_agent_to_dict(agent)

    async def accept_handoff(
        self,
        *,
        handoff_id: str,
        human_agent_identity: str,
    ) -> dict:
        self._ensure_handoff_service()
        try:
            handoff = await self.handoff_service.accept(
                handoff_id=handoff_id,
                human_agent_identity=human_agent_identity,
            )
        finally:
            self._record_expired_handoff_events()
        try:
            token = self.orchestrator.issue_handoff_token(
                call_id=handoff.call_id,
                handoff_id=handoff.handoff_id,
                human_agent_identity=human_agent_identity,
            )
        except AiCallError as exc:
            failed = await self.handoff_service.fail_request(
                handoff_id=handoff.handoff_id,
                failure_stage="token_issue",
                failure_message=exc.msg,
            )
            if failed is not None:
                self._record_handoff_event_best_effort(
                    call_id=failed.call_id,
                    event_type="handoff_failed",
                    handoff_id=failed.handoff_id,
                    handoff_status=failed.status,
                    payload={
                        "failureStage": failed.failure_stage,
                        "failureMessage": failed.failure_message,
                    },
                )
                self._trigger_handoff_exception_close(
                    failed,
                    call_end_reason="handoff_failed",
                )
            raise self._to_custom_exception(exc) from exc
        handoff = await self.handoff_service.confirm_accepted(
            handoff_id=handoff.handoff_id,
            human_agent_identity=human_agent_identity,
        )
        self._schedule_handoff_timeout(handoff)
        return {
            "handoff": self.handoff_service.handoff_to_dict(handoff),
            "seatToken": {
                "callId": token.call_id,
                "handoffId": token.handoff_id,
                "roomName": token.room_name,
                "livekitUrl": token.livekit_url,
                "participantToken": token.participant_token,
                "participantIdentity": token.participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        }

    async def mark_handoff_connected(self, handoff_id: str) -> dict:
        self._ensure_handoff_service()
        try:
            handoff = await self.handoff_service.mark_connected(handoff_id)
        finally:
            self._record_expired_handoff_events()
        self._record_handoff_event_best_effort(
            call_id=handoff.call_id,
            event_type="handoff_connected",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={"humanAgentIdentity": handoff.human_agent_identity},
        )
        if self.recording_service is not None:
            await self.recording_service.start_human_agent_recording(
                tenant_id=handoff.tenant_id,
                call_id=handoff.call_id,
                room_name=handoff.room_name,
                handoff_id=handoff.handoff_id,
                participant_identity=f"human-agent-{handoff.handoff_id}",
            )
        self._cancel_handoff_timeout(handoff, reason="connected")
        return self.handoff_service.handoff_to_dict(handoff)

    async def complete_handoff(self, *, handoff_id: str, reason: str | None) -> dict:
        self._ensure_handoff_service()
        handoff = await self.handoff_service.complete(
            handoff_id=handoff_id,
            reason=reason,
        )
        self._record_handoff_event_best_effort(
            call_id=handoff.call_id,
            event_type="handoff_completed",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={"reason": handoff.end_reason},
        )
        self._cancel_handoff_timeout(handoff, reason="completed")
        await self._end_running_session_after_handoff(handoff.call_id, handoff.end_reason)
        return self.handoff_service.handoff_to_dict(handoff)

    async def cancel_handoff(self, *, handoff_id: str, reason: str | None) -> dict:
        self._ensure_handoff_service()
        try:
            handoff = await self.handoff_service.cancel(
                handoff_id=handoff_id,
                reason=reason,
            )
        finally:
            self._record_expired_handoff_events()
        self._record_handoff_event_best_effort(
            call_id=handoff.call_id,
            event_type="handoff_canceled",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={"reason": handoff.end_reason},
        )
        self._cancel_handoff_timeout(handoff, reason="canceled")
        if handoff.connected_at is None:
            self._trigger_handoff_exception_close(
                handoff,
                call_end_reason=handoff.end_reason or "handoff_canceled",
            )
        else:
            await self._end_running_session_after_handoff(
                handoff.call_id,
                handoff.end_reason,
            )
        return self.handoff_service.handoff_to_dict(handoff)

    async def fail_handoff(
        self,
        *,
        handoff_id: str,
        failure_stage: str,
        failure_message: str | None,
        end_reason: str | None = None,
    ) -> dict:
        self._ensure_handoff_service()
        handoff = await self.handoff_service.fail(
            handoff_id=handoff_id,
            failure_stage=failure_stage,
            failure_message=failure_message,
        )
        if end_reason:
            updated = await self.handoff_service.repository.update_handoff(
                handoff.handoff_id,
                end_reason=end_reason,
            )
            if updated is not None:
                handoff = updated
        unanswered_reasons = {
            "no_online_agent": "转人工时当前场景没有在线坐席",
            "handoff_service_unavailable": "转人工服务暂时不可用",
        }
        if end_reason in unanswered_reasons:
            await AiCallHandoffUnansweredService(
                self.handoff_service.repository
            ).ensure_for_handoff(
                handoff,
                reason=unanswered_reasons[end_reason],
            )
        self._record_handoff_event_best_effort(
            call_id=handoff.call_id,
            event_type="handoff_failed",
            handoff_id=handoff.handoff_id,
            handoff_status=handoff.status,
            payload={
                "failureStage": handoff.failure_stage,
                "failureMessage": handoff.failure_message,
                "endReason": handoff.end_reason,
            },
        )
        self._trigger_handoff_exception_close(
            handoff,
            call_end_reason=end_reason or "handoff_failed",
        )
        return self.handoff_service.handoff_to_dict(handoff)

    @staticmethod
    def _event_rows_to_runtime_result(rows) -> EventListResult:
        runtime_rows = [
            AiCallEvent(
                event_id=row.event_id,
                call_id=row.call_id,
                type=row.event_type,
                timestamp=row.event_time,
                source=row.source,
                payload=row.payload,
            )
            for row in rows
        ]
        return EventListResult(rows=runtime_rows, total=len(runtime_rows))

    @staticmethod
    def _failure_stage_for_end_reason(end_reason: str) -> str:
        return {
            "agent_start_failed": "agent_start",
            "room_create_failed": "room_create",
            "provider_connect_failed": "provider_connect",
            "sip_caller_number_missing": "sip_trunk",
            "sip_create_participant_failed": "sip",
            "sip_duplicate_active_callee": "sip_concurrency",
            "invalid_callee_number": "callee_number",
            "callee_prefix_not_allowed": "callee_number",
            "sip_outbound_disabled": "sip_config",
            "sip_preflight_failed": "sip_config",
            "sip_public_ip_missing": "sip_network",
            "sip_rtp_range_invalid": "sip_network",
            "sip_sdk_config_missing": "sip_sdk",
            "sip_sdk_unavailable": "sip_sdk",
            "sip_signaling_port_invalid": "sip_network",
            "sip_trunk_missing": "sip_trunk",
        }.get(end_reason, end_reason)

    def _record_sip_event(
        self,
        *,
        call_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        source: str = "sip",
    ) -> AiCallEvent | None:
        try:
            self.orchestrator.record_sip_event(
                call_id=call_id,
                event_type=event_type,
                payload=payload,
                source=source,
            )
            return self.orchestrator.event_store.list_all(call_id)[-1]
        except AiCallError as exc:
            if exc.error_id != "session_not_found":
                return None
            events = self.orchestrator.event_store.list_all(call_id)
            return events[-1] if events else None
        except Exception:
            return None

    def _record_sip_preflight(
        self,
        *,
        call_id: str,
        callee_phone_number: str,
    ) -> None:
        if self.sip_client is None:
            return
        callee_phone_number_masked = self._mask_phone_number(callee_phone_number)
        callee_phone_number_hash = self._callee_phone_number_hash(callee_phone_number)
        preflight = self.sip_client.preflight(callee_phone_number=callee_phone_number)
        if preflight.ok:
            self._record_sip_event(
                call_id=call_id,
                event_type="sip_preflight_passed",
                payload={
                    "calleePhoneNumberMasked": callee_phone_number_masked,
                    "calleePhoneNumberHash": callee_phone_number_hash,
                },
            )
            return
        self._record_sip_event(
            call_id=call_id,
            event_type="sip_preflight_failed",
            payload={
                "errorId": preflight.failure_reason or "sip_preflight_failed",
                "message": preflight.message or "SIP 外呼预检失败",
                "calleePhoneNumberMasked": callee_phone_number_masked,
                "calleePhoneNumberHash": callee_phone_number_hash,
            },
        )
        raise AiCallError(
            error_id=preflight.failure_reason or "sip_preflight_failed",
            msg=preflight.message or "SIP 外呼预检失败",
            status_code=400,
        )

    def _record_successful_sip_participant(
        self,
        *,
        call_id: str,
        sip_participant: CreateSipParticipantResult,
    ) -> None:
        sip_call_status = str(sip_participant.sip_call_status or "").lower()
        if sip_call_status not in {"active", "answered", "connected"}:
            raise AiCallError(
                error_id="sip_answer_not_confirmed",
                msg="SIP Participant 未返回已接听状态",
                status_code=status.HTTP_502_BAD_GATEWAY,
                details={
                    "sipCallStatus": sip_participant.sip_call_status or "",
                },
            )
        payload = {
            "participantIdentity": sip_participant.participant_identity,
            "sipCallId": sip_participant.sip_call_id,
            "sipCallIdFull": sip_participant.sip_call_id_full,
            "sipTrunkId": sip_participant.sip_trunk_id,
            "sipCallStatus": sip_participant.sip_call_status,
        }
        self._record_sip_event(
            call_id=call_id,
            event_type="sip_answered",
            payload=payload,
        )

    @staticmethod
    def _call_id_from_sip_participant(participant_identity: str | None) -> str | None:
        if not participant_identity or not participant_identity.startswith("sip-"):
            return None
        call_id = participant_identity.removeprefix("sip-")
        return call_id or None

    @staticmethod
    def _sip_hangup_payload(
        *,
        room_name: str | None,
        participant_identity: str,
        event_type: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        source_payload = payload or {}
        result: dict[str, Any] = {
            "roomName": room_name,
            "participantIdentity": participant_identity,
            "livekitEventType": event_type,
        }
        for source_key, target_key in (
            ("id", "livekitEventId"),
            ("disconnectReason", "disconnectReason"),
            ("disconnect_reason", "disconnectReason"),
            ("createdAt", "createdAt"),
            ("created_at", "createdAt"),
        ):
            value = source_payload.get(source_key)
            if value not in (None, ""):
                result[target_key] = value
        return result

    @staticmethod
    def _mask_phone_number(phone_number: str) -> str:
        normalized = "".join(ch for ch in str(phone_number or "") if ch.isdigit())
        if len(normalized) <= 7:
            return "***"
        return f"{normalized[:3]}****{normalized[-4:]}"

    @staticmethod
    def _callee_phone_number_hash(phone_number: str) -> str:
        normalized = "".join(ch for ch in str(phone_number or "") if ch.isdigit())
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    async def _ensure_no_active_sip_outbound_for_callee(
        self,
        *,
        callee_phone_number_hash: str,
        callee_phone_number_masked: str,
    ) -> None:
        if self.record_service is None:
            return
        active_record = await self.record_service.get_active_sip_record_by_callee_hash(
            callee_phone_number_hash,
        )
        if active_record is None:
            return
        active_status = getattr(active_record, "status", None)
        active_call_id = getattr(active_record, "call_id", None)
        raise CustomException(
            msg="该号码已有进行中的电话外呼，请先结束当前通话后再重试",
            code=RET.CONFLICT.code,
            status_code=status.HTTP_409_CONFLICT,
            data={
                "activeCallId": active_call_id,
                "activeStatus": active_status,
                "calleePhoneNumberMasked": callee_phone_number_masked,
            },
        )

    def _ensure_record_service(self) -> None:
        if self.record_service is None:
            raise CustomException(msg="通话记录服务未启用", code=RET.ERROR.code, status_code=500)

    def _ensure_prompt_repository(self) -> AiCallRecordRepository:
        if self.prompt_repository is None:
            raise CustomException(msg="提示词配置服务未启用", code=RET.ERROR.code, status_code=500)
        return self.prompt_repository

    def _ensure_prompt_resolver(self) -> BusinessPromptResolver:
        if self.prompt_resolver is None:
            raise CustomException(msg="提示词解析服务未启用", code=RET.ERROR.code, status_code=500)
        return self.prompt_resolver

    def _ensure_prompt_composer(self) -> PromptComposer:
        if self.prompt_composer is None:
            raise CustomException(msg="提示词组装服务未启用", code=RET.ERROR.code, status_code=500)
        return self.prompt_composer

    async def _resolve_voice(self, voice: str | None) -> str | None:
        normalized_voice = _strip_or_none(voice)
        if not normalized_voice:
            return None
        repository = self._ensure_prompt_repository()
        target_model = self.orchestrator.config.qwen_realtime_model
        await self._ensure_builtin_voice_profiles(repository, target_model)
        profile = await repository.get_voice_profile_by_voice(
            voice=normalized_voice,
            target_model=target_model,
        )
        if profile is None:
            raise CustomException(
                msg="音色不存在或不适用于当前模型",
                code=RET.ERROR.code,
                status_code=400,
            )
        return profile.voice

    @staticmethod
    async def _ensure_builtin_voice_profiles(
        repository: AiCallRecordRepository,
        target_model: str,
    ) -> None:
        await repository.ensure_builtin_voice_profiles(
            target_model=target_model,
            profiles=builtin_voice_profile_values(target_model),
        )

    def _normalize_voice_profile_values(self, values: dict) -> dict:
        voice = _strip_or_none(values.get("voice"))
        if not voice:
            raise CustomException(msg="voice 不能为空", code=RET.ERROR.code, status_code=400)
        display_name = _strip_or_none(values.get("display_name"))
        if not display_name:
            raise CustomException(msg="displayName 不能为空", code=RET.ERROR.code, status_code=400)
        target_model = (
            _strip_or_none(values.get("target_model"))
            or self.orchestrator.config.qwen_realtime_model
        )
        gender = _strip_or_none(values.get("gender")) or VOICE_GENDER_UNKNOWN
        if gender not in {VOICE_GENDER_UNKNOWN, VOICE_GENDER_FEMALE, VOICE_GENDER_MALE}:
            raise CustomException(msg="gender 不合法", code=RET.ERROR.code, status_code=400)
        sort_order = values.get("sort_order")
        return {
            "voice": voice,
            "display_name": display_name,
            "voice_type": VOICE_TYPE_CUSTOM_CLONE,
            "gender": gender,
            "target_model": target_model,
            "description": _strip_or_none(values.get("description")),
            "sort_order": int(sort_order) if sort_order is not None else 1000,
            "remark": _strip_or_none(values.get("remark")),
        }

    @staticmethod
    def _require_scene_code(scene_code: str | None) -> str:
        normalized_scene_code = _strip_or_none(scene_code)
        if not normalized_scene_code:
            raise CustomException(msg="sceneCode 不能为空", code=RET.ERROR.code, status_code=400)
        return normalize_scene_code(normalized_scene_code) or normalized_scene_code

    async def _resolve_prompt_effective_config(
        self,
        *,
        call_id: str,
        business_id: str | None,
        scene_code: str | None,
        business_params: dict,
        debug_prompt: str | None,
    ) -> PromptEffectiveConfig | None:
        if self.prompt_resolver is None or self.prompt_composer is None:
            return None
        prompt_result = await self.prompt_resolver.resolve(
            PromptResolveContext(
                call_id=call_id,
                business_id=business_id,
                scene_code=scene_code,
                business_params=business_params,
                debug_prompt=debug_prompt,
            )
        )
        return self.prompt_composer.compose(prompt_result)

    @staticmethod
    def _normalize_prompt_profile_values(values: dict) -> dict:
        normalized = {
            "scene_code": (normalize_scene_code(str(values.get("scene_code") or "").strip()) or ""),
            "name": str(values.get("name") or "").strip(),
            "provider_key": str(
                values.get("provider_key") or PROMPT_PROVIDER_STATIC_PROFILE
            ).strip(),
            "prompt_text": _strip_or_none(values.get("prompt_text")),
            "opening_message": _strip_or_none(values.get("opening_message")),
        }
        if not normalized["scene_code"]:
            raise CustomException(msg="sceneCode 不能为空", code=RET.ERROR.code, status_code=400)
        if not normalized["name"]:
            raise CustomException(msg="name 不能为空", code=RET.ERROR.code, status_code=400)
        if (
            normalized["provider_key"] == PROMPT_PROVIDER_STATIC_PROFILE
            and not normalized["prompt_text"]
        ):
            raise CustomException(msg="固定提示词不能为空", code=RET.ERROR.code, status_code=400)
        if (
            normalized["provider_key"] == PROMPT_PROVIDER_STATIC_PROFILE
            and not normalized["opening_message"]
        ):
            raise CustomException(msg="固定开场白不能为空", code=RET.ERROR.code, status_code=400)
        return normalized

    @staticmethod
    async def _ensure_prompt_profile_unique(
        repository: AiCallRecordRepository,
        values: dict,
        *,
        current_id: int | None = None,
    ) -> None:
        existing_scene = await repository.get_prompt_profile_by_scene(
            scene_code=values["scene_code"],
        )
        if existing_scene is not None and existing_scene.id != current_id:
            raise CustomException(msg="场景编码已存在配置", code=RET.ERROR.code, status_code=409)

    @staticmethod
    def _prompt_profile_to_dict(profile) -> dict:
        return {
            "id": str(profile.id),
            "sceneCode": profile.scene_code,
            "name": profile.name,
            "providerKey": profile.provider_key,
            "promptText": profile.prompt_text,
            "openingMessage": profile.opening_message,
            "createdAt": profile.created_at,
            "updatedAt": profile.updated_at,
        }

    @staticmethod
    def _voice_profile_to_dict(profile) -> dict:
        return {
            "id": str(profile.id),
            "voice": profile.voice,
            "displayName": profile.display_name,
            "voiceType": profile.voice_type,
            "gender": profile.gender,
            "targetModel": profile.target_model,
            "description": profile.description,
            "sortOrder": profile.sort_order,
            "remark": profile.remark,
            "createdAt": profile.created_at,
            "updatedAt": profile.updated_at,
        }

    def _ensure_recording_service(self) -> None:
        if self.recording_service is None:
            raise CustomException(msg="通话录音服务未启用", code=RET.ERROR.code, status_code=500)

    @staticmethod
    def _require_recording_tenant(tenant_id: str | None) -> str:
        normalized = str(tenant_id or "").strip()
        if not normalized:
            raise CustomException(
                msg="通话录音缺少租户上下文",
                code=RET.ERROR.code,
                status_code=500,
            )
        return normalized

    @staticmethod
    def _require_browser_event_tenant(tenant_id: str | None) -> str:
        normalized = str(tenant_id or "").strip()
        if not normalized:
            raise CustomException(
                msg="通话事件缺少租户上下文",
                code=RET.ERROR.code,
                status_code=403,
            )
        return normalized

    async def _recording_tenant_for_call(self, call_id: str) -> str:
        if self.record_service is None:
            return self._require_recording_tenant(None)
        record = await self.record_service.get_record(call_id)
        if record is None:
            raise CustomException(
                msg="通话录音对应的通话记录不存在",
                code=RET.ERROR.code,
                status_code=500,
            )
        return self._require_recording_tenant(record.tenant_id)

    def _ensure_dialogue_service(self) -> None:
        if self.dialogue_service is None:
            raise CustomException(msg="对话文本服务未启用", code=RET.ERROR.code, status_code=500)

    def _ensure_handoff_service(self) -> None:
        if self.handoff_service is None:
            raise CustomException(msg="转人工服务未启用", code=RET.ERROR.code, status_code=500)

    async def _start_browser_ready_recording_tracks(
        self,
        *,
        tenant_id: str,
        call_id: str,
        record: Any | None = None,
    ) -> None:
        if self.recording_service is None:
            return
        if record is not None and record.runtime_control_mode == "owner_command_v1":
            return
        tenant_id = self._require_recording_tenant(tenant_id)
        if record is None:
            if self.record_service is None:
                raise CustomException(
                    msg="通话录音对应的通话记录不存在",
                    code=RET.ERROR.code,
                    status_code=500,
                )
            record = await self.record_service.get_record_for_tenant(
                tenant_id=tenant_id,
                call_id=call_id,
            )
            if record is None:
                raise CustomException(
                    msg="通话录音对应的通话记录不存在",
                    code=RET.ERROR.code,
                    status_code=500,
                )
        record_tenant_id = self._require_recording_tenant(record.tenant_id)
        if record_tenant_id != tenant_id:
            raise CustomException(
                msg="通话录音租户上下文不匹配",
                code=RET.ERROR.code,
                status_code=403,
            )
        session = await self.orchestrator.get_session(call_id)
        if session.status not in RUNNING_STATUSES:
            return
        room_name = record.room_name or session.room_name
        customer_participant_identity = (
            record.participant_identity or f"browser-{call_id}"
        )
        await self.recording_service.start_session_participant_recordings(
            tenant_id=tenant_id,
            call_id=call_id,
            room_name=room_name,
            customer_participant_identity=customer_participant_identity,
            ai_participant_identity=f"agent-{call_id}",
        )

    @staticmethod
    def _enqueue_offline_asr(call_id: str) -> None:
        enqueue_ai_call_offline_asr(call_id)

    async def _enqueue_offline_asr_if_recordings_closed(
        self,
        call_id: str,
        *,
        tenant_id: str | None = None,
    ) -> None:
        if self.recording_service is not None:
            if not await self.recording_service.is_ready_for_offline_asr(
                tenant_id=(
                    self._require_recording_tenant(tenant_id)
                    if tenant_id is not None
                    else await self._recording_tenant_for_call(call_id)
                ),
                call_id=call_id,
            ):
                return
        self._enqueue_offline_asr(call_id)

    async def _end_running_session_after_handoff(
        self,
        call_id: str,
        end_reason: str | None,
    ) -> None:
        try:
            session = await self.orchestrator.get_session(call_id)
        except AiCallError:
            return
        if session.status not in RUNNING_STATUSES:
            return
        await self.end_session(call_id, end_reason=end_reason or "agent_completed")

    async def end_running_session_after_handoff(
        self,
        call_id: str,
        end_reason: str | None,
    ) -> None:
        await self._end_running_session_after_handoff(call_id, end_reason)

    async def _finalize_handoffs_for_call(self, call_id: str, *, end_reason: str) -> None:
        if self.handoff_service is None:
            return
        rows = await self.handoff_service.finalize_active_for_call(
            call_id,
            end_reason=end_reason,
        )
        for handoff in rows:
            self._cancel_handoff_timeout(handoff, reason=end_reason)
            event_type = (
                "handoff_completed" if handoff.status == "completed" else "handoff_canceled"
            )
            self._record_handoff_event_best_effort(
                call_id=handoff.call_id,
                event_type=event_type,
                handoff_id=handoff.handoff_id,
                handoff_status=handoff.status,
                payload={"reason": handoff.end_reason},
            )

    def _record_handoff_event_best_effort(
        self,
        *,
        call_id: str,
        event_type: str,
        handoff_id: str,
        handoff_status: str,
        payload: dict | None = None,
    ) -> None:
        try:
            self.orchestrator.record_handoff_event(
                call_id=call_id,
                event_type=event_type,
                handoff_id=handoff_id,
                handoff_status=handoff_status,
                payload=payload,
            )
        except AiCallError:
            return

    def _record_expired_handoff_events(self) -> None:
        if self.handoff_service is None:
            return
        for handoff in self.handoff_service.consume_expired_handoffs():
            self._record_handoff_event_best_effort(
                call_id=handoff.call_id,
                event_type="handoff_expired",
                handoff_id=handoff.handoff_id,
                handoff_status=handoff.status,
                payload={"reason": handoff.end_reason},
            )
            self._trigger_handoff_exception_close(
                handoff,
                call_end_reason="handoff_timeout",
            )

    def _schedule_handoff_timeout(
        self,
        handoff,
        *,
        waiting_prompt_kind: str = "default",
    ) -> None:
        if self.handoff_exception_manager is None:
            return
        self.handoff_exception_manager.schedule_timeout(handoff)
        self.handoff_exception_manager.start_waiting_tone(
            handoff,
            prompt_kind=waiting_prompt_kind,
        )

    def _cancel_handoff_timeout(self, handoff, *, reason: str) -> None:
        if self.handoff_exception_manager is None:
            return
        self.handoff_exception_manager.cancel_timeout(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason=reason,
        )
        self.handoff_exception_manager.stop_waiting_tone(
            handoff.handoff_id,
            call_id=handoff.call_id,
            handoff_status=handoff.status,
            reason=reason,
        )

    def _trigger_handoff_exception_close(self, handoff, *, call_end_reason: str) -> None:
        if self.handoff_exception_manager is None:
            return
        self.handoff_exception_manager.trigger_exception_close(
            handoff,
            call_end_reason=call_end_reason,
        )

    @staticmethod
    def _to_custom_exception(exc: AiCallError) -> CustomException:
        return CustomException(
            msg=exc.msg,
            code=RET.ERROR.code,
            status_code=exc.status_code or status.HTTP_500_INTERNAL_SERVER_ERROR,
            data=None,
        )


_default_orchestrator: AiCallOrchestrator | None = None
_default_event_persistence_worker: AiCallEventPersistenceWorker | None = None
_default_dialogue_runtime_store: AiCallDialogueRuntimeStore | None = None
_default_dialogue_persistence_worker: AiCallDialoguePersistenceWorker | None = None
_default_handoff_exception_manager: AiCallHandoffExceptionManager | None = None
_default_handoff_trigger_worker: AiCallHandoffTriggerWorker | None = None
_default_offline_asr_worker: AiCallOfflineAsrWorker | None = None
_default_semantic_analysis_worker: AiCallSemanticAnalysisWorker | None = None


def configure_ai_call_event_persistence(
    worker: AiCallEventPersistenceWorker | None,
) -> None:
    global _default_event_persistence_worker
    _default_event_persistence_worker = worker
    if worker is not None and _default_orchestrator is not None:
        worker.attach_event_store(_default_orchestrator.event_store)


def configure_ai_call_dialogue_persistence(
    worker: AiCallDialoguePersistenceWorker | None,
) -> None:
    global _default_dialogue_persistence_worker
    runtime_store = _ensure_default_dialogue_runtime_store()
    _default_dialogue_persistence_worker = worker
    if worker is not None:
        worker.attach_runtime_store(runtime_store)
    if _default_orchestrator is not None:
        runtime_store.attach_event_store(_default_orchestrator.event_store)


def configure_ai_call_handoff_trigger(
    worker: AiCallHandoffTriggerWorker | None,
) -> None:
    global _default_handoff_trigger_worker
    _default_handoff_trigger_worker = worker
    if worker is not None and _default_orchestrator is not None:
        worker.attach_event_store(_default_orchestrator.event_store)


def configure_ai_call_offline_asr(worker: AiCallOfflineAsrWorker | None) -> None:
    global _default_offline_asr_worker
    _default_offline_asr_worker = worker


def configure_ai_call_semantic_analysis(worker: AiCallSemanticAnalysisWorker | None) -> None:
    global _default_semantic_analysis_worker
    _default_semantic_analysis_worker = worker


def _manual_semantic_analyzer() -> Any | None:
    if _default_semantic_analysis_worker is not None:
        return _default_semantic_analysis_worker.analyzer
    return build_default_semantic_analyzer(
        base_url=settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL,
        api_key=settings.EFFECTIVE_POST_ANALYSIS_API_KEY or settings.EFFECTIVE_LLM_API_KEY,
        model=(
            settings.AI_CALL_SEMANTIC_ANALYSIS_MODEL
            or settings.POST_ANALYSIS_MODEL
            or settings.LLM_MODEL
            or "qwen-plus"
        ),
        timeout_seconds=settings.AI_CALL_SEMANTIC_ANALYSIS_TIMEOUT_SECONDS,
    )


def enqueue_ai_call_offline_asr(call_id: str) -> None:
    if _default_offline_asr_worker is not None:
        _default_offline_asr_worker.enqueue(call_id)


def enqueue_ai_call_semantic_analysis(call_id: str, scene_code: str | None = None) -> None:
    if _default_semantic_analysis_worker is not None:
        _default_semantic_analysis_worker.enqueue(call_id, scene_code=scene_code)


def get_default_ai_call_service(
    db: AsyncSession | None = None,
    *,
    sip_config: SipOutboundConfig | None = None,
) -> AiCallService:
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = AiCallOrchestrator.from_settings(settings)
        if _default_event_persistence_worker is not None:
            _default_event_persistence_worker.attach_event_store(_default_orchestrator.event_store)
        if _default_handoff_trigger_worker is not None:
            _default_handoff_trigger_worker.attach_event_store(_default_orchestrator.event_store)
    _ensure_default_dialogue_runtime_store().attach_event_store(_default_orchestrator.event_store)

    if db is None:
        return AiCallService(_default_orchestrator)

    repository = AiCallRecordRepository(db)
    record_service = AiCallRecordService(repository)
    recording_service = _build_recording_service(repository)
    handoff_exception_manager = _ensure_default_handoff_exception_manager(_default_orchestrator)
    dialogue_service = AiCallDialogueService(
        repository,
        runtime_store=_ensure_default_dialogue_runtime_store(),
    )
    return AiCallService(
        _default_orchestrator,
        record_service,
        recording_service=recording_service,
        dialogue_service=dialogue_service,
        handoff_service=AiCallHandoffService(
            repository,
            request_timeout_seconds=settings.AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS,
        ),
        handoff_exception_manager=handoff_exception_manager,
        prompt_repository=repository,
        prompt_resolver=_build_prompt_resolver(repository, _default_orchestrator),
        prompt_composer=_build_prompt_composer(_default_orchestrator),
        sip_client=_build_sip_client(config=sip_config),
    )


async def end_agent_handoff_session_background(
    call_id: str,
    end_reason: str | None,
) -> None:
    try:
        async with async_db_session() as db:
            async with db.begin():
                await get_default_ai_call_service(
                    db
                ).end_running_session_after_handoff(call_id, end_reason)
    except Exception as exc:
        log.exception(
            "AI Call 坐席通话后台收尾失败: "
            f"callId={call_id}, endReason={end_reason}, "
            f"errorType={type(exc).__name__}, message={exc!s}"
        )


def schedule_livekit_webhook_event(
    *,
    event_type: str,
    room_name: str | None,
    participant_identity: str | None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = asyncio.create_task(
        _handle_livekit_webhook_event_background(
            event_type=event_type,
            room_name=room_name,
            participant_identity=participant_identity,
            payload=payload,
        ),
        name=f"ai-call-livekit-webhook-{event_type or 'event'}",
    )
    task.add_done_callback(_consume_livekit_webhook_task)
    return {
        "queued": True,
        "eventType": event_type,
        "roomName": room_name,
        "participantIdentity": participant_identity,
    }


async def _handle_livekit_webhook_event_background(
    *,
    event_type: str,
    room_name: str | None,
    participant_identity: str | None,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    async with async_db_session() as db:
        async with db.begin():
            service = get_default_ai_call_service(db)
            result = await service.handle_livekit_webhook_event(
                event_type=event_type,
                room_name=room_name,
                participant_identity=participant_identity,
                payload=payload,
            )
            if result.get("handled"):
                return result
            from app.services.ai_call.follow_up_service import AiCallFollowUpService

            return await AiCallFollowUpService(db).handle_livekit_webhook_event(
                event_type=event_type,
                room_name=room_name,
                participant_identity=participant_identity,
                payload=payload,
            )


def _consume_livekit_webhook_task(task: asyncio.Task[dict[str, Any]]) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        log.exception(f"AI Call LiveKit webhook 后台处理失败: {exc!s}")


def _ensure_default_dialogue_runtime_store() -> AiCallDialogueRuntimeStore:
    global _default_dialogue_runtime_store
    if _default_dialogue_runtime_store is None:
        _default_dialogue_runtime_store = AiCallDialogueRuntimeStore()
        if _default_dialogue_persistence_worker is not None:
            _default_dialogue_persistence_worker.attach_runtime_store(
                _default_dialogue_runtime_store
            )
    return _default_dialogue_runtime_store


def _build_recording_service(repository: AiCallRecordRepository) -> AiCallRecordingService:
    manager: LiveKitEgressManager | None = None
    if settings.AI_CALL_RECORDING_ENABLED:
        manager = LiveKitEgressManager(
            livekit_url=settings.LIVEKIT_URL,
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
            timeout_seconds=settings.AI_CALL_EGRESS_TIMEOUT_SECONDS,
            stop_timeout_seconds=settings.AI_CALL_EGRESS_STOP_TIMEOUT_SECONDS,
            object_prefix=settings.AI_CALL_RECORDING_OBJECT_PREFIX,
            file_type=settings.AI_CALL_RECORDING_FORMAT,
            participant_file_type=settings.AI_CALL_PARTICIPANT_RECORDING_FORMAT,
        )
    return AiCallRecordingService(
        repository,
        enabled=settings.AI_CALL_RECORDING_ENABLED,
        egress_manager=manager,
        participant_recording_enabled=settings.AI_CALL_PARTICIPANT_RECORDING_ENABLED,
        verify_deadline_seconds=settings.AI_CALL_RECORDING_VERIFY_DEADLINE_SECONDS,
        stop_session_factory=async_db_session,
    )


def _build_sip_client(
    config: SipOutboundConfig | None = None,
) -> LiveKitSipClient:
    return LiveKitSipClient(
        config=config or SipOutboundConfig.from_settings(settings),
        livekit_url=settings.LIVEKIT_URL,
        api_key=settings.LIVEKIT_API_KEY,
        api_secret=settings.LIVEKIT_API_SECRET,
    )


def _build_prompt_resolver(
    repository: AiCallRecordRepository,
    orchestrator: AiCallOrchestrator,
) -> BusinessPromptResolver:
    return BusinessPromptResolver(
        repository=repository,
        default_provider=DefaultPromptProvider(
            default_prompt=orchestrator.config.default_prompt,
            opening_message=orchestrator.config.opening_message,
        ),
        debug_provider=DebugPromptProvider(
            opening_message=orchestrator.config.opening_message,
        ),
        collection_prompt_store=_build_collection_prompt_store(),
        timeout_seconds=settings.AI_CALL_PROMPT_RESOLVE_TIMEOUT_SECONDS,
        debug_override_enabled=settings.AI_CALL_DEBUG_PROMPT_OVERRIDE_ENABLED,
    )


def _build_collection_prompt_store() -> RecovCollectionPostgresPromptStore | None:
    dsn = (settings.AI_CALL_COLLECTION_POSTGRES_DSN or "").strip()
    if not dsn:
        return None
    return RecovCollectionPostgresPromptStore(
        dsn=dsn,
        timeout_seconds=settings.AI_CALL_COLLECTION_POSTGRES_TIMEOUT_SECONDS,
    )


def _build_prompt_composer(orchestrator: AiCallOrchestrator) -> PromptComposer:
    return PromptComposer(
        handoff_component_enabled=orchestrator.config.handoff_prompt_constraint_enabled,
    )


def _ensure_default_handoff_exception_manager(
    orchestrator: AiCallOrchestrator,
) -> AiCallHandoffExceptionManager:
    global _default_handoff_exception_manager
    if _default_handoff_exception_manager is None:
        prompt_player = LiveKitSystemPromptPlayer(
            livekit_url=settings.LIVEKIT_URL,
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
        )
        _default_handoff_exception_manager = AiCallHandoffExceptionManager(
            orchestrator=orchestrator,
            session_factory=async_db_session,
            recording_service_factory=_build_recording_service,
            system_prompt_player=prompt_player,
            timeout_seconds=settings.AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS,
            exception_close_enabled=settings.AI_CALL_HANDOFF_EXCEPTION_CLOSE_ENABLED,
            waiting_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_WAITING_PROMPT_AUDIO_PATH
            ),
            waiting_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_WAITING_PROMPT_TEXT
            ),
            busy_waiting_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_BUSY_WAITING_PROMPT_AUDIO_PATH
            ),
            busy_waiting_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_BUSY_WAITING_PROMPT_TEXT
            ),
            waiting_tone_enabled=settings.AI_CALL_HANDOFF_WAITING_TONE_ENABLED,
            waiting_tone_audio_path=settings.AI_CALL_HANDOFF_WAITING_TONE_AUDIO_PATH,
            waiting_tone_interval_seconds=settings.AI_CALL_HANDOFF_WAITING_TONE_INTERVAL_SECONDS,
            unavailable_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_UNAVAILABLE_PROMPT_AUDIO_PATH
            ),
            unavailable_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_UNAVAILABLE_PROMPT_TEXT
            ),
            no_online_agent_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_NO_ONLINE_AGENT_PROMPT_AUDIO_PATH
            ),
            no_online_agent_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_NO_ONLINE_AGENT_PROMPT_TEXT
            ),
            busy_timeout_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_BUSY_TIMEOUT_PROMPT_AUDIO_PATH
            ),
            busy_timeout_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_BUSY_TIMEOUT_PROMPT_TEXT
            ),
            service_unavailable_prompt_audio_path=_strip_or_none(
                settings.AI_CALL_HANDOFF_SERVICE_UNAVAILABLE_PROMPT_AUDIO_PATH
            ),
            service_unavailable_prompt_text=_strip_or_none(
                settings.AI_CALL_HANDOFF_SERVICE_UNAVAILABLE_PROMPT_TEXT
            ),
        )
    return _default_handoff_exception_manager


def _strip_or_none(value) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def _is_sqlite_database_locked(exc: OperationalError) -> bool:
    return "database is locked" in str(exc).lower()
