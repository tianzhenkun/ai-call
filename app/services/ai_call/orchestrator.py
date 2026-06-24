from __future__ import annotations

import asyncio
import hashlib
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from fastapi import status

from app.config.setting import Settings
from app.core.logger import log
from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_audio_transport import LiveKitRoomAudioTransport
from app.services.ai_call.livekit_room import BrowserRoomToken, LiveKitRoomManager
from app.services.ai_call.metrics import CallMetrics
from app.services.ai_call.prompt_config import PromptEffectiveConfig
from app.services.ai_call.providers.aliyun_qwen_realtime import AliyunQwenRealtimeProvider
from app.services.ai_call.session_registry import (
    ALLOWED_TRANSITIONS,
    RUNNING_STATUSES,
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
    utc_now,
)
from app.utils.id_util import generate_snowflake_id


class LiveKitRoomManagerProtocol(Protocol):
    async def create_room(self, room_name: str) -> None: ...

    def issue_browser_token(
        self, room_name: str, participant_identity: str
    ) -> BrowserRoomToken: ...

    def issue_handoff_token(
        self,
        room_name: str,
        participant_identity: str,
        expires_in_seconds: int | None = None,
    ) -> BrowserRoomToken: ...

    async def delete_room(self, room_name: str) -> None: ...


class RealtimeAgentRunnerProtocol(Protocol):
    async def start(self, session: CallSession) -> None: ...

    async def start_opening(self, call_id: str) -> None: ...

    async def record_browser_speech_candidate(
        self,
        call_id: str,
        trigger_timestamp: datetime,
    ) -> bool: ...

    async def record_browser_speech_segment(
        self,
        call_id: str,
        trigger_timestamp: datetime,
        payload: dict[str, Any],
    ) -> bool: ...

    async def suspend_for_handoff(self, call_id: str) -> None: ...

    async def stop(self, call_id: str) -> None: ...


@dataclass(frozen=True, slots=True)
class AiCallRuntimeConfig:
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    browser_token_ttl_seconds: int
    dashscope_api_key: str
    dashscope_realtime_url: str
    qwen_realtime_model: str
    qwen_realtime_voice: str
    default_prompt: str
    opening_message: str
    web_audio_echo_cancellation: bool
    web_audio_noise_suppression: bool
    web_audio_auto_gain_control: bool
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    user_turn_stability_delay_seconds: float = 0.35
    handoff_prompt_constraint_enabled: bool = True

    @classmethod
    def from_settings(cls, settings: Settings) -> AiCallRuntimeConfig:
        return cls(
            livekit_url=settings.LIVEKIT_URL,
            livekit_api_key=settings.LIVEKIT_API_KEY,
            livekit_api_secret=settings.LIVEKIT_API_SECRET,
            browser_token_ttl_seconds=settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS,
            dashscope_api_key=settings.DASHSCOPE_API_KEY,
            dashscope_realtime_url=settings.DASHSCOPE_REALTIME_URL,
            qwen_realtime_model=settings.QWEN_REALTIME_MODEL,
            qwen_realtime_voice=settings.QWEN_REALTIME_VOICE,
            default_prompt=settings.AI_CALL_DEFAULT_PROMPT,
            opening_message=settings.AI_CALL_OPENING_MESSAGE,
            web_audio_echo_cancellation=settings.WEB_AUDIO_ECHO_CANCELLATION,
            web_audio_noise_suppression=settings.WEB_AUDIO_NOISE_SUPPRESSION,
            web_audio_auto_gain_control=settings.WEB_AUDIO_AUTO_GAIN_CONTROL,
            vad_type=settings.QWEN_REALTIME_TURN_DETECTION_TYPE,
            vad_threshold=settings.QWEN_REALTIME_VAD_THRESHOLD,
            vad_silence_duration_ms=settings.QWEN_REALTIME_VAD_SILENCE_DURATION_MS,
            user_turn_stability_delay_seconds=(settings.AI_CALL_USER_TURN_STABILITY_DELAY_SECONDS),
            handoff_prompt_constraint_enabled=(settings.AI_CALL_HANDOFF_PROMPT_CONSTRAINT_ENABLED),
        )

    def ensure_ready(self) -> None:
        if not self.livekit_url or not self.livekit_api_key or not self.livekit_api_secret:
            raise AiCallError(
                error_id="missing_livekit_config",
                msg="LiveKit 配置缺失",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        if not self.dashscope_api_key:
            raise AiCallError(
                error_id="missing_model_config",
                msg="模型配置缺失",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    model: str
    voice: str
    prompt: str = field(repr=False)
    prompt_hash: str
    opening_message: str = field(repr=False)
    opening_message_hash: str
    prompt_source_key: str
    vad_type: str
    vad_threshold: float
    vad_silence_duration_ms: int
    instructions: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class WebAudioConstraints:
    echo_cancellation: bool
    noise_suppression: bool
    auto_gain_control: bool


@dataclass(frozen=True, slots=True)
class CreateSessionResult:
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    status: CallSessionStatus
    effective_config: EffectiveConfig
    web_audio_constraints: WebAudioConstraints


@dataclass(frozen=True, slots=True)
class ReissueTokenResult:
    call_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


@dataclass(frozen=True, slots=True)
class SessionStatusResult:
    call_id: str
    status: CallSessionStatus
    room_name: str
    effective_config: Any
    started_at: datetime
    last_event_at: datetime
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventListResult:
    rows: list[AiCallEvent]
    total: int


@dataclass(frozen=True, slots=True)
class EndSessionResult:
    call_id: str
    status: CallSessionStatus


@dataclass(frozen=True, slots=True)
class BrowserEventReportResult:
    event_id: str
    call_id: str
    type: str
    timestamp: datetime
    source: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HandoffTokenResult:
    call_id: str
    handoff_id: str
    room_name: str
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


class AiCallOrchestrator:
    def __init__(
        self,
        config: AiCallRuntimeConfig,
        livekit_room_manager: LiveKitRoomManagerProtocol | None = None,
        agent_runner: RealtimeAgentRunnerProtocol | None = None,
        registry: InMemorySessionRegistry | None = None,
        event_store: InMemoryEventStore | None = None,
        metrics_by_call_id: dict[str, CallMetrics] | None = None,
    ) -> None:
        self.config = config
        self.registry = registry or InMemorySessionRegistry()
        self.event_store = event_store or InMemoryEventStore()
        self.metrics_by_call_id = metrics_by_call_id if metrics_by_call_id is not None else {}
        self.livekit_room_manager = livekit_room_manager or LiveKitRoomManager(
            livekit_url=config.livekit_url,
            api_key=config.livekit_api_key,
            api_secret=config.livekit_api_secret,
            browser_token_ttl_seconds=config.browser_token_ttl_seconds,
        )
        self._auto_end_tasks: set[asyncio.Task[None]] = set()
        self.agent_runner = agent_runner or self._build_default_agent_runner()

    @classmethod
    def from_settings(cls, settings: Settings) -> AiCallOrchestrator:
        return cls(config=AiCallRuntimeConfig.from_settings(settings))

    def _build_default_agent_runner(self) -> RealtimeAgentRunnerProtocol:
        audio_transport = LiveKitRoomAudioTransport(
            livekit_url=self.config.livekit_url,
            api_key=self.config.livekit_api_key,
            api_secret=self.config.livekit_api_secret,
        )
        return RealtimeCallAgentRunner(
            provider_factory=lambda _session: AliyunQwenRealtimeProvider(
                realtime_url=self.config.dashscope_realtime_url,
                api_key=self.config.dashscope_api_key,
                model=self.config.qwen_realtime_model,
            ),
            registry=self.registry,
            event_store=self.event_store,
            metrics_by_call_id=self.metrics_by_call_id,
            audio_transport=audio_transport,
            user_turn_stability_delay_seconds=(self.config.user_turn_stability_delay_seconds),
            handoff_prompt_constraint_enabled=(self.config.handoff_prompt_constraint_enabled),
            call_end_scheduler=self._schedule_auto_end_session,
        )

    async def create_web_session(
        self,
        voice: str | None,
        prompt: str | None,
        call_id: str | None = None,
        prompt_effective_config: PromptEffectiveConfig | None = None,
    ) -> CreateSessionResult:
        self.config.ensure_ready()
        call_id = call_id or self._new_call_id()
        room_name = f"ai-call-{call_id}"
        participant_identity = f"browser-{call_id}"
        effective_config = self._build_effective_config(
            voice=voice,
            prompt=prompt,
            prompt_effective_config=prompt_effective_config,
        )
        session = CallSession(
            call_id=call_id,
            room_name=room_name,
            participant_identity=participant_identity,
            status=CallSessionStatus.CREATED,
            effective_config=effective_config,
        )
        self.registry.add(session)
        self.metrics_by_call_id[call_id] = CallMetrics()
        self._append_event(
            call_id,
            "session_created",
            "orchestrator",
            {
                "promptHash": effective_config.prompt_hash,
                "openingMessageHash": effective_config.opening_message_hash,
                "promptSourceKey": effective_config.prompt_source_key,
            },
        )

        self.registry.transition(call_id, CallSessionStatus.PREPARING)
        self._append_event(call_id, "session_preparing", "orchestrator")

        try:
            await self.livekit_room_manager.create_room(room_name)
        except AiCallError as exc:
            self._append_failed_terminal_event(
                call_id,
                end_reason=exc.error_id,
                failure_stage=self._failure_stage_for_end_reason(exc.error_id),
                failure_message=exc.msg,
            )
            raise
        except Exception as exc:
            self._append_failed_terminal_event(
                call_id,
                end_reason="room_create_failed",
                failure_stage="room_create",
                failure_message="LiveKit Room 创建失败",
            )
            raise AiCallError(
                error_id="room_create_failed",
                msg="LiveKit Room 创建失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from exc
        self._append_event(call_id, "room_created", "livekit", {"roomName": room_name})

        token = self.livekit_room_manager.issue_browser_token(room_name, participant_identity)
        self._append_event(
            call_id,
            "browser_token_issued",
            "livekit",
            {
                "participantIdentity": participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        )

        try:
            await self.agent_runner.start(session)
        except Exception as exc:
            await self._handle_agent_start_failed(call_id, room_name, exc)
        self._append_event(
            call_id,
            "agent_started",
            "agent",
            self._agent_runner_runtime_diagnostics(),
        )

        self.registry.transition(call_id, CallSessionStatus.READY)
        self._append_event(call_id, "session_ready", "orchestrator")

        return CreateSessionResult(
            call_id=call_id,
            room_name=room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            status=CallSessionStatus.READY,
            effective_config=effective_config,
            web_audio_constraints=self._web_audio_constraints(),
        )

    async def _handle_agent_start_failed(
        self,
        call_id: str,
        room_name: str,
        exc: Exception,
    ) -> None:
        error_payload: dict[str, Any] = {
            "errorType": type(exc).__name__,
            "errorMessage": str(exc),
        }
        if exc.__cause__ is not None:
            error_payload["causeType"] = type(exc.__cause__).__name__
            error_payload["causeMessage"] = str(exc.__cause__)
        log.error(
            "AI Call Agent 启动失败 call_id={} room_name={} error_type={} error_message={}",
            call_id,
            room_name,
            error_payload["errorType"],
            error_payload["errorMessage"],
        )
        self._append_event(
            call_id,
            "agent_start_failed",
            "agent",
            error_payload,
        )
        self._append_failed_terminal_event(
            call_id,
            end_reason="agent_start_failed",
            failure_stage="agent_start",
            failure_message="Agent 启动失败",
        )
        with suppress(Exception):
            await self.agent_runner.stop(call_id)
        with suppress(Exception):
            await self.livekit_room_manager.delete_room(room_name)
        raise AiCallError(
            error_id="agent_start_failed",
            msg="Agent 启动失败",
            status_code=status.HTTP_502_BAD_GATEWAY,
        ) from exc

    async def reissue_browser_token(self, call_id: str) -> ReissueTokenResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        token = self.livekit_room_manager.issue_browser_token(
            session.room_name,
            session.participant_identity,
        )
        self._append_event(
            call_id,
            "browser_token_issued",
            "livekit",
            {
                "participantIdentity": session.participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
                "reason": "reissue",
            },
        )
        return ReissueTokenResult(
            call_id=call_id,
            room_name=session.room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            expires_in_seconds=token.expires_in_seconds,
        )

    async def suspend_for_handoff(
        self,
        *,
        call_id: str,
        handoff_id: str,
        request_source: str,
        request_reason: str | None,
    ) -> None:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        self._append_event(
            call_id,
            "handoff_requested",
            "handoff",
            {
                "handoffId": handoff_id,
                "status": "requested",
                "source": request_source,
                "reason": request_reason,
            },
        )
        await self.agent_runner.suspend_for_handoff(call_id)
        self._move_session_to_waiting_if_possible(call_id)
        self._append_event(
            call_id,
            "agent_suspended_for_handoff",
            "agent",
            {
                "handoffId": handoff_id,
                "reason": "handoff_requested",
            },
        )

    def issue_handoff_token(
        self,
        *,
        call_id: str,
        handoff_id: str,
        human_agent_identity: str,
    ) -> HandoffTokenResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        participant_identity = f"human-agent-{handoff_id}"
        token = self.livekit_room_manager.issue_handoff_token(
            session.room_name,
            participant_identity,
            self.config.browser_token_ttl_seconds,
        )
        self._append_event(
            call_id,
            "handoff_accepted",
            "handoff",
            {
                "handoffId": handoff_id,
                "status": "accepted",
                "humanAgentIdentity": human_agent_identity,
                "participantIdentity": participant_identity,
                "expiresInSeconds": token.expires_in_seconds,
            },
        )
        return HandoffTokenResult(
            call_id=call_id,
            handoff_id=handoff_id,
            room_name=session.room_name,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            participant_identity=token.participant_identity,
            expires_in_seconds=token.expires_in_seconds,
        )

    def record_handoff_event(
        self,
        *,
        call_id: str,
        event_type: str,
        handoff_id: str,
        handoff_status: str,
        payload: dict[str, Any] | None = None,
        source: str = "handoff",
    ) -> None:
        event_payload = {
            "handoffId": handoff_id,
            "status": handoff_status,
        }
        if payload:
            event_payload.update(payload)
        self._append_event(call_id, event_type, source, event_payload)

    async def get_session(self, call_id: str) -> SessionStatusResult:
        session = self.registry.get(call_id)
        return SessionStatusResult(
            call_id=session.call_id,
            status=session.status,
            room_name=session.room_name,
            effective_config=session.effective_config,
            started_at=session.started_at,
            last_event_at=session.last_event_at,
            metrics=session.metrics,
        )

    async def list_events(
        self,
        call_id: str,
        limit: int = 200,
        after_event_id: str | None = None,
    ) -> EventListResult:
        self.registry.get(call_id)
        rows = self.event_store.list(
            call_id=call_id,
            limit=limit,
            after_event_id=after_event_id,
        )
        return EventListResult(rows=rows, total=len(rows))

    async def end_session(
        self,
        call_id: str,
        *,
        end_reason: str = "web_user_end",
    ) -> EndSessionResult:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.COMPLETED:
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)
        if session.status == CallSessionStatus.FAILED:
            await self.agent_runner.stop(call_id)
            await self.livekit_room_manager.delete_room(session.room_name)
            return EndSessionResult(call_id=call_id, status=CallSessionStatus.FAILED)

        self.registry.transition(call_id, CallSessionStatus.ENDING)
        self._append_event(call_id, "session_ending", "orchestrator")

        await self.agent_runner.stop(call_id)
        await self.livekit_room_manager.delete_room(session.room_name)

        self.registry.transition(call_id, CallSessionStatus.COMPLETED)
        self._append_event(
            call_id,
            "session_completed",
            "orchestrator",
            {"endReason": end_reason},
        )
        return EndSessionResult(call_id=call_id, status=CallSessionStatus.COMPLETED)

    def _schedule_auto_end_session(self, call_id: str, end_reason: str) -> None:
        task = asyncio.create_task(
            self._auto_end_session(call_id, end_reason),
            name=f"ai-call-auto-end-{call_id}",
        )
        self._auto_end_tasks.add(task)
        task.add_done_callback(self._consume_auto_end_task)

    def _consume_auto_end_task(self, task: asyncio.Task[None]) -> None:
        self._auto_end_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            call_id = self._auto_end_task_call_id(task)
            if call_id is None:
                return
            with suppress(Exception):
                self._append_event(
                    call_id,
                    "call_end_auto_failed",
                    "orchestrator",
                    {
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    },
                )

    async def _auto_end_session(self, call_id: str, end_reason: str) -> None:
        try:
            await self.end_session(call_id, end_reason=end_reason)
        except AiCallError as exc:
            with suppress(Exception):
                self._append_event(
                    call_id,
                    "call_end_auto_failed",
                    "orchestrator",
                    {
                        "errorId": exc.error_id,
                        "message": exc.msg,
                        "endReason": end_reason,
                    },
                )

    @staticmethod
    def _auto_end_task_call_id(task: asyncio.Task[None]) -> str | None:
        prefix = "ai-call-auto-end-"
        name = task.get_name()
        if not name.startswith(prefix):
            return None
        return name[len(prefix) :]

    async def report_browser_event(
        self,
        call_id: str,
        event_type: str,
        timestamp: datetime | None = None,
        payload: dict[str, Any] | None = None,
    ) -> BrowserEventReportResult:
        session = self.registry.get(call_id)
        if session.status not in RUNNING_STATUSES:
            raise AiCallError(
                error_id="invalid_session_state",
                msg="当前会话状态不允许该操作",
                status_code=status.HTTP_409_CONFLICT,
            )
        reported_at = timestamp or utc_now()
        event_payload = dict(payload or {})
        event_payload["reportedAt"] = reported_at.isoformat()
        should_start_opening = False
        should_record_browser_speech_candidate = False
        should_record_browser_speech_segment = False
        if event_type == "browser_disconnect":
            event = self.event_store.append(
                call_id=call_id,
                type=event_type,
                source="browser",
                payload=event_payload,
            )
            session.last_event_at = event.timestamp
            await self.end_session(call_id, end_reason="browser_disconnect")
            return BrowserEventReportResult(
                event_id=event.event_id,
                call_id=event.call_id,
                type=event.type,
                timestamp=event.timestamp,
                source=event.source,
                payload=event.payload,
            )
        if event_type == "browser_first_audio":
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_browser_first_audio(reported_at)
            session.metrics = metrics.snapshot()
        elif event_type == "browser_ready":
            if session.status == CallSessionStatus.READY:
                self.registry.transition(call_id, CallSessionStatus.CONNECTED)
                should_start_opening = bool(
                    str(self._config_value(session.effective_config, "opening_message", "")).strip()
                )
        elif event_type == "browser_audio_input_diagnostics":
            pass
        elif event_type == "browser_user_speech_started":
            should_record_browser_speech_candidate = True
        elif event_type == "browser_user_speech_segment":
            should_record_browser_speech_segment = True
        else:
            raise AiCallError(
                error_id="unsupported_browser_event",
                msg="不支持的浏览器事件",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        event = self.event_store.append(
            call_id=call_id,
            type=event_type,
            source="browser",
            payload=event_payload,
        )
        session.last_event_at = event.timestamp
        if should_record_browser_speech_candidate:
            await self.agent_runner.record_browser_speech_candidate(call_id, event.timestamp)
        if should_record_browser_speech_segment:
            await self.agent_runner.record_browser_speech_segment(
                call_id,
                event.timestamp,
                event.payload,
            )
        if should_start_opening:
            opening_started_at = self._append_event(
                call_id,
                "opening_started",
                "agent",
                {
                    "openingMessageHash": self._config_value(
                        session.effective_config,
                        "opening_message_hash",
                        "",
                    )
                },
            )
            metrics = self.metrics_by_call_id.setdefault(call_id, CallMetrics())
            metrics.mark_model_response_requested(opening_started_at)
            session.metrics = metrics.snapshot()
            await self.agent_runner.start_opening(call_id)
        return BrowserEventReportResult(
            event_id=event.event_id,
            call_id=event.call_id,
            type=event.type,
            timestamp=event.timestamp,
            source=event.source,
            payload=event.payload,
        )

    def _append_event(
        self,
        call_id: str,
        event_type: str,
        source: str,
        payload: dict[str, Any] | None = None,
    ) -> datetime:
        event = self.event_store.append(
            call_id=call_id,
            type=event_type,
            source=source,
            payload=payload,
        )
        session = self.registry.get(call_id)
        session.last_event_at = event.timestamp
        return event.timestamp

    def _agent_runner_runtime_diagnostics(self) -> dict[str, Any]:
        diagnostics = getattr(self.agent_runner, "runtime_diagnostics", None)
        if not callable(diagnostics):
            return {}
        try:
            payload = diagnostics()
        except Exception as exc:
            return {
                "diagnosticsVersion": "runtime-diagnostics-error",
                "diagnosticsErrorType": type(exc).__name__,
                "diagnosticsErrorMessage": str(exc),
            }
        return payload if isinstance(payload, dict) else {}

    def _append_failed_terminal_event(
        self,
        call_id: str,
        *,
        end_reason: str,
        failure_stage: str,
        failure_message: str,
    ) -> None:
        if self.registry.get(call_id).status != CallSessionStatus.FAILED:
            self.registry.transition(call_id, CallSessionStatus.FAILED)
        self._append_event(
            call_id,
            "session_failed",
            "orchestrator",
            {
                "endReason": end_reason,
                "failureStage": failure_stage,
                "failureMessage": failure_message,
            },
        )

    def _move_session_to_waiting_if_possible(self, call_id: str) -> None:
        session = self.registry.get(call_id)
        if session.status == CallSessionStatus.WAITING:
            return
        if CallSessionStatus.WAITING not in ALLOWED_TRANSITIONS.get(session.status, set()):
            return
        self.registry.transition(call_id, CallSessionStatus.WAITING)

    def _build_effective_config(
        self,
        voice: str | None,
        prompt: str | None,
        prompt_effective_config: PromptEffectiveConfig | None = None,
    ) -> EffectiveConfig:
        resolved_voice = voice or self.config.qwen_realtime_voice
        if prompt_effective_config is not None:
            if not prompt_effective_config.instructions.strip():
                raise AiCallError(
                    error_id="prompt_empty",
                    msg="提示词不能为空",
                    status_code=status.HTTP_400_BAD_REQUEST,
                )
            if not prompt_effective_config.opening_message.strip():
                raise AiCallError(
                    error_id="opening_message_empty",
                    msg="开场白不能为空",
                    status_code=status.HTTP_400_BAD_REQUEST,
                )
            return EffectiveConfig(
                model=self.config.qwen_realtime_model,
                voice=resolved_voice,
                prompt=prompt_effective_config.instructions,
                prompt_hash=prompt_effective_config.prompt_hash,
                opening_message=prompt_effective_config.opening_message,
                opening_message_hash=prompt_effective_config.opening_message_hash,
                prompt_source_key=prompt_effective_config.prompt_source_key,
                vad_type=self.config.vad_type,
                vad_threshold=self.config.vad_threshold,
                vad_silence_duration_ms=self.config.vad_silence_duration_ms,
                instructions=prompt_effective_config.instructions,
            )

        resolved_prompt = (prompt or self.config.default_prompt).strip()
        resolved_opening_message = self.config.opening_message.strip()
        if not resolved_prompt:
            raise AiCallError(
                error_id="prompt_empty",
                msg="提示词不能为空",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if not resolved_opening_message:
            raise AiCallError(
                error_id="opening_message_empty",
                msg="开场白不能为空",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return EffectiveConfig(
            model=self.config.qwen_realtime_model,
            voice=resolved_voice,
            prompt=resolved_prompt,
            prompt_hash=self._hash_text(resolved_prompt),
            opening_message=resolved_opening_message,
            opening_message_hash=self._hash_text(resolved_opening_message),
            prompt_source_key="debug.prompt" if prompt else "default",
            vad_type=self.config.vad_type,
            vad_threshold=self.config.vad_threshold,
            vad_silence_duration_ms=self.config.vad_silence_duration_ms,
        )

    def _web_audio_constraints(self) -> WebAudioConstraints:
        return WebAudioConstraints(
            echo_cancellation=self.config.web_audio_echo_cancellation,
            noise_suppression=self.config.web_audio_noise_suppression,
            auto_gain_control=self.config.web_audio_auto_gain_control,
        )

    @staticmethod
    def _hash_text(text: str) -> str:
        return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _config_value(config: Any, key: str, default: Any) -> Any:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)

    @staticmethod
    def _failure_stage_for_end_reason(end_reason: str) -> str:
        return {
            "agent_start_failed": "agent_start",
            "room_create_failed": "room_create",
            "provider_connect_failed": "provider_connect",
        }.get(end_reason, end_reason)

    @staticmethod
    def _new_call_id() -> str:
        return f"call_{generate_snowflake_id()}"
