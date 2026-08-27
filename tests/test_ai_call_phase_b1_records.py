from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import jwt
import pytest
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.controller import get_ai_call_service
from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallEventModel,
    AiCallFollowUpDataModel,
    AiCallFollowUpHandlingResultModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordingModel,
    AiCallRecordingTrackModel,
    AiCallRecordModel,
    AiCallVoiceProfileModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.schema import HandoffListOut, RecordDetailOut
from app.api.v1.ai_call.service import AiCallService, configure_ai_call_offline_asr
from app.api.v1.system.oss.service import OssService
from app.config.setting import Settings
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException
from app.services.ai_call.dialogue_service import (
    AiCallDialoguePersistenceWorker,
    AiCallDialogueRuntimeStore,
    AiCallDialogueService,
    DialogueSegmentSnapshot,
)
from app.services.ai_call.event_persistence import AiCallEventPersistenceWorker
from app.services.ai_call.event_store import InMemoryEventStore
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.handoff_availability_service import HandoffAgentAvailability
from app.services.ai_call.handoff_exception_manager import AiCallHandoffExceptionManager
from app.services.ai_call.handoff_service import AiCallHandoffService
from app.services.ai_call.handoff_trigger_service import (
    AiCallHandoffTriggerService,
    AiCallHandoffTriggerWorker,
    CompositeHandoffIntentClassifier,
    HandoffIntentResult,
    RuleBasedHandoffIntentClassifier,
)
from app.services.ai_call.livekit_egress import (
    LiveKitEgressManager,
    LiveKitEgressRequestTimeout,
    LiveKitEgressStartResult,
    LiveKitEgressStopResult,
)
from app.services.ai_call.livekit_room import BrowserRoomToken
from app.services.ai_call.offline_asr_service import (
    AiCallOfflineAsrService,
    OfflineAsrResult,
    OfflineAsrSegment,
)
from app.services.ai_call.orchestrator import AiCallOrchestrator, AiCallRuntimeConfig
from app.services.ai_call.record_service import AiCallRecordService
from app.services.ai_call.recording_service import (
    AiCallRecordingReconcileWorker,
    AiCallRecordingService,
)
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    InMemorySessionRegistry,
)
from app.utils.minio_util import MinioUtil


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class FakeLiveKitRoomManager:
    def __init__(self) -> None:
        self.created_rooms: list[str] = []
        self.deleted_rooms: list[str] = []
        self.issued_handoff_tokens: list[tuple[str, str, int | None]] = []

    async def create_room(self, room_name: str) -> None:
        self.created_rooms.append(room_name)

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"browser-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=600,
        )

    def issue_handoff_token(
        self,
        room_name: str,
        participant_identity: str,
        expires_in_seconds: int | None = None,
    ) -> BrowserRoomToken:
        self.issued_handoff_tokens.append((room_name, participant_identity, expires_in_seconds))
        return BrowserRoomToken(
            livekit_url="wss://livekit.test",
            participant_token=f"handoff-token-for-{participant_identity}",
            participant_identity=participant_identity,
            expires_in_seconds=expires_in_seconds or 600,
        )

    async def delete_room(self, room_name: str) -> None:
        self.deleted_rooms.append(room_name)


class FakeAgentRunner:
    def __init__(self) -> None:
        self.suspended_call_ids: list[str] = []

    async def start(self, session: CallSession) -> None:
        _ = session

    async def start_opening(self, call_id: str) -> None:
        _ = call_id

    async def record_browser_speech_candidate(self, call_id: str, trigger_timestamp) -> bool:
        _ = call_id, trigger_timestamp
        return False

    async def stop(self, call_id: str) -> None:
        _ = call_id

    async def suspend_for_handoff(self, call_id: str) -> None:
        self.suspended_call_ids.append(call_id)


class FakeSystemPromptPlayer:
    def __init__(self) -> None:
        self.played: list[tuple[str, str, str]] = []

    async def play(self, *, call_id: str, room_name: str, audio_path) -> None:
        self.played.append((call_id, room_name, str(audio_path)))


class BlockingSystemPromptPlayer(FakeSystemPromptPlayer):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def play(self, *, call_id: str, room_name: str, audio_path) -> None:
        self.played.append((call_id, room_name, str(audio_path)))
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class SecondPromptBlockingSystemPromptPlayer(FakeSystemPromptPlayer):
    def __init__(self) -> None:
        super().__init__()
        self.second_started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def play(self, *, call_id: str, room_name: str, audio_path) -> None:
        self.played.append((call_id, room_name, str(audio_path)))
        if len(self.played) != 2:
            return
        self.second_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class FakeHandoffIntentClassifier:
    def __init__(self, result: HandoffIntentResult) -> None:
        self.result = result
        self.transcripts: list[str] = []

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        self.transcripts.append(transcript)
        return self.result


class FakeHandoffAvailabilityService:
    def __init__(self, result: HandoffAgentAvailability) -> None:
        self.result = result
        self.call_ids: list[str] = []

    async def get_for_call(self, call_id: str) -> HandoffAgentAvailability:
        self.call_ids.append(call_id)
        return self.result


class FailingHandoffAvailabilityService:
    async def get_for_call(self, call_id: str) -> HandoffAgentAvailability:
        raise RuntimeError(f"availability failed for {call_id}")


class DatabaseFailingHandoffAvailabilityService:
    def __init__(self, db) -> None:
        self.db = db

    async def get_for_call(self, call_id: str) -> HandoffAgentAvailability:
        _ = call_id
        try:
            await self.db.execute(
                text("SELECT * FROM ai_call_missing_handoff_availability_table")
            )
        except Exception:
            await self.db.rollback()
            raise
        raise AssertionError("缺失表查询必须失败")


class SlowHandoffIntentClassifier:
    def __init__(self) -> None:
        self.transcripts: list[str] = []

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        self.transcripts.append(transcript)
        await asyncio.sleep(1)
        return HandoffIntentResult(
            matched=False,
            confidence=0.3,
            reason="not_handoff",
            summary="主分类器未识别",
            source="slow_primary",
        )


@pytest.mark.anyio
async def test_rule_based_handoff_classifier_matches_customer_manager_request() -> None:
    classifier = RuleBasedHandoffIntentClassifier()

    matched = await classifier.classify(transcript="给我找你们客户经理。")
    owner_matched = await classifier.classify(transcript="帮我转给负责人。")
    customer_manager_question = await classifier.classify(transcript="客户经理是做什么的？")
    customer_manager_name_question = await classifier.classify(transcript="客户经理叫什么？")
    ai_concept_question = await classifier.classify(transcript="人工智能是什么意思？")

    assert matched.matched is True
    assert matched.reason == "customer_request"
    assert matched.source == "rule_fallback"
    assert owner_matched.matched is True
    assert customer_manager_question.matched is False
    assert customer_manager_name_question.matched is False
    assert ai_concept_question.matched is False


@pytest.mark.anyio
async def test_rule_based_handoff_classifier_matches_product_follow_up_intent() -> None:
    classifier = RuleBasedHandoffIntentClassifier()

    contact = await classifier.classify(transcript="可以，你们怎么联系我呢？你们有 demo 吗？")
    generic_contact = await classifier.classify(transcript="不啊，怎么联系？")
    contact_method = await classifier.classify(transcript="联系方式是什么？")
    advisor_contact = await classifier.classify(transcript="安排你们产品顾问联系我。")
    schedule = await classifier.classify(transcript="等你们客服到时候我们一起聊吧。")
    demo_question = await classifier.classify(transcript="你们有 demo 吗？")
    price_question = await classifier.classify(transcript="你们这个多少钱呢？")
    trial_question = await classifier.classify(transcript="可以试用吗？")
    off_topic = await classifier.classify(transcript="明天几号？")

    assert contact.matched is True
    assert contact.reason == "business_escalation"
    assert contact.source == "rule_fallback"
    assert generic_contact.matched is True
    assert generic_contact.reason == "business_escalation"
    assert contact_method.matched is True
    assert contact_method.reason == "business_escalation"
    assert advisor_contact.matched is True
    assert schedule.matched is True
    assert demo_question.matched is False
    assert demo_question.reason == "not_handoff"
    assert demo_question.confidence >= 0.9
    assert price_question.matched is False
    assert price_question.confidence >= 0.9
    assert trial_question.matched is False
    assert trial_question.confidence >= 0.9
    assert off_topic.matched is False


@pytest.mark.anyio
async def test_composite_handoff_classifier_uses_strong_rule_before_primary() -> None:
    primary = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.4,
            reason="not_handoff",
            summary="主分类器未识别",
            source="test_primary",
        )
    )
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
    )

    result = await classifier.classify(transcript="麻烦给我找你们客户经理。")

    assert result.matched is True
    assert result.source == "rule_fallback"
    assert primary.transcripts == []


@pytest.mark.anyio
async def test_composite_handoff_classifier_uses_product_follow_up_rule_before_slow_primary() -> None:
    primary = SlowHandoffIntentClassifier()
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
    )

    result = await asyncio.wait_for(
        classifier.classify(transcript="可以，你们怎么联系我呢？你们有 demo 吗？"),
        timeout=0.2,
    )

    assert result.matched is True
    assert result.reason == "business_escalation"
    assert result.source == "rule_fallback"
    assert primary.transcripts == []


@pytest.mark.anyio
async def test_composite_handoff_classifier_uses_product_consultation_rule_before_slow_primary() -> None:
    primary = SlowHandoffIntentClassifier()
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
    )

    result = await asyncio.wait_for(
        classifier.classify(transcript="你们有 demo 吗？"),
        timeout=0.2,
    )

    assert result.matched is False
    assert result.reason == "not_handoff"
    assert result.confidence >= 0.9
    assert result.source == "rule_fallback"
    assert primary.transcripts == []


@pytest.mark.anyio
async def test_composite_handoff_classifier_falls_back_when_primary_times_out() -> None:
    primary = SlowHandoffIntentClassifier()
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
        primary_timeout_seconds=0.05,
    )

    result = await asyncio.wait_for(
        classifier.classify(transcript="明天几号？"),
        timeout=0.2,
    )

    assert result.matched is False
    assert result.reason == "not_handoff"
    assert result.source == "rule_fallback"
    assert primary.transcripts == ["明天几号？"]


@pytest.mark.anyio
async def test_app_handoff_trigger_worker_enables_transcript_trigger(monkeypatch) -> None:
    from app.plugin import init_app

    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED", True)

    worker = await init_app._start_ai_call_handoff_trigger_worker()
    try:
        assert worker is not None
        assert worker.transcript_trigger_enabled is True
        assert worker.trigger_service.availability_service_factory is not None
    finally:
        await init_app._stop_ai_call_handoff_trigger_worker(worker)


class FailingAgentRunner(FakeAgentRunner):
    async def start(self, session: CallSession) -> None:
        await super().start(session)
        raise RuntimeError("agent boom")


class FakeEgressManager:
    def __init__(
        self,
        room_manager: FakeLiveKitRoomManager | None = None,
        *,
        file_size: int | None = 2048,
    ) -> None:
        self.room_manager = room_manager
        self.file_size = file_size
        self.started: list[tuple[str, str]] = []
        self.started_participants: list[tuple[str, str, str, str]] = []
        self.stopped: list[str] = []
        self.stop_saw_deleted_room: bool | None = None

    def build_object_name(self, call_id: str) -> str:
        return f"ai-call/recordings/{call_id}.mp3"

    def build_participant_object_name(
        self,
        *,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> str:
        return f"ai-call/recordings/tracks/{call_id}/{track_role}-{participant_identity}.mp3"

    async def start_room_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        assert oss_config["bucket_name"] == "recordings"
        self.started.append((room_name, call_id))
        return LiveKitEgressStartResult(
            egress_id=f"EG_{call_id}",
            object_name=self.build_object_name(call_id),
            status="EGRESS_ACTIVE",
        )

    async def start_participant_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        assert oss_config["bucket_name"] == "recordings"
        self.started_participants.append((room_name, call_id, track_role, participant_identity))
        return LiveKitEgressStartResult(
            egress_id=f"EG_{call_id}_{track_role}_{participant_identity}",
            object_name=self.build_participant_object_name(
                call_id=call_id,
                track_role=track_role,
                participant_identity=participant_identity,
            ),
            status="EGRESS_ACTIVE",
        )

    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raw = egress_id.removeprefix("EG_")
        call_id = self._call_id_from_egress_id(egress_id, raw)
        if self.room_manager is not None:
            room_name = f"ai-call-{call_id}"
            self.stop_saw_deleted_room = room_name in self.room_manager.deleted_rooms
        object_name = self._object_name_from_egress_id(egress_id, call_id)
        return LiveKitEgressStopResult(
            egress_id=egress_id,
            status="EGRESS_COMPLETE",
            object_name=object_name,
            duration_ms=1200,
            file_size=self.file_size,
            location=f"s3://recordings/{object_name}",
        )

    def _call_id_from_egress_id(self, egress_id: str, fallback: str) -> str:
        for (
            _room_name,
            started_call_id,
            track_role,
            participant_identity,
        ) in self.started_participants:
            if egress_id == f"EG_{started_call_id}_{track_role}_{participant_identity}":
                return started_call_id
        return fallback

    def _object_name_from_egress_id(self, egress_id: str, call_id: str) -> str:
        for (
            _room_name,
            started_call_id,
            track_role,
            participant_identity,
        ) in self.started_participants:
            if egress_id == f"EG_{started_call_id}_{track_role}_{participant_identity}":
                return self.build_participant_object_name(
                    call_id=started_call_id,
                    track_role=track_role,
                    participant_identity=participant_identity,
                )
        return self.build_object_name(call_id)


class FlakyParticipantEgressManager(FakeEgressManager):
    def __init__(self, *, failures_before_success: int = 1) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success
        self.participant_start_attempts: dict[tuple[str, str], int] = {}

    async def start_participant_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        key = (track_role, participant_identity)
        attempts = self.participant_start_attempts.get(key, 0) + 1
        self.participant_start_attempts[key] = attempts
        if attempts <= self.failures_before_success:
            raise RuntimeError("participant not ready")
        return await super().start_participant_audio_recording(
            room_name=room_name,
            call_id=call_id,
            track_role=track_role,
            participant_identity=participant_identity,
            oss_config=oss_config,
        )


class FailedStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        call_id = egress_id.removeprefix("EG_")
        if self.room_manager is not None:
            room_name = f"ai-call-{call_id}"
            self.stop_saw_deleted_room = room_name in self.room_manager.deleted_rooms
        return LiveKitEgressStopResult(
            egress_id=egress_id,
            status="EGRESS_FAILED",
            object_name=self.build_object_name(call_id),
            error="egress failed",
        )


class TimeoutMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise LiveKitEgressRequestTimeout(method="StopEgress", timeout_seconds=2.0)


class BrokenMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise RuntimeError("stop failed before result was accepted")


class AlreadyCompletedMainStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        self.stopped.append(egress_id)
        raise RuntimeError(
            'StopEgress HTTP 412: {"code":"failed_precondition",'
            '"msg":"egress with status EGRESS_COMPLETE cannot be stopped"}'
        )


class TimeoutAiTrackStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        if "_ai_" in egress_id:
            self.stopped.append(egress_id)
            raise LiveKitEgressRequestTimeout(method="StopEgress", timeout_seconds=2.0)
        return await super().stop_egress(egress_id)


class AlreadyCompletedAiTrackStopEgressManager(FakeEgressManager):
    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        if "_ai_" in egress_id:
            self.stopped.append(egress_id)
            raise RuntimeError(
                'StopEgress HTTP 412: {"code":"failed_precondition",'
                '"msg":"egress with status EGRESS_COMPLETE cannot be stopped"}'
            )
        return await super().stop_egress(egress_id)


class FakeOfflineAsrProvider:
    provider_name = "fake_asr"
    model_name = "fake-model"

    def __init__(self) -> None:
        self.audio_urls: list[str] = []

    async def transcribe(self, *, audio_url: str) -> OfflineAsrResult:
        self.audio_urls.append(audio_url)
        if "/customer-" in audio_url:
            return OfflineAsrResult(
                task_id="task-customer",
                transcription_url="https://asr.test/customer.json",
                segments=[
                    OfflineAsrSegment(
                        text="客户需要转人工",
                        begin_time_ms=100,
                        end_time_ms=520,
                    )
                ],
            )
        return OfflineAsrResult(
            task_id="task-agent",
            transcription_url="https://asr.test/agent.json",
            segments=[
                OfflineAsrSegment(
                    text="您好，我帮您接入人工",
                    begin_time_ms=600,
                    end_time_ms=1200,
                )
            ],
        )


class FakeOfflineAsrWorker:
    def __init__(self) -> None:
        self.call_ids: list[str] = []

    def enqueue(self, call_id: str) -> None:
        self.call_ids.append(call_id)


@dataclass(slots=True)
class B1ServiceContext:
    service: AiCallService
    record_service: AiCallRecordService
    event_worker: AiCallEventPersistenceWorker
    agent_runner: FakeAgentRunner
    room_manager: FakeLiveKitRoomManager
    session_maker: async_sessionmaker

    def __iter__(self):
        yield self.service
        yield self.record_service

    async def flush_events(self) -> None:
        await self.event_worker.flush_pending()


def build_b1_orchestrator(
    agent_runner: FakeAgentRunner | None = None,
    livekit_room_manager: FakeLiveKitRoomManager | None = None,
) -> AiCallOrchestrator:
    return AiCallOrchestrator(
        config=AiCallRuntimeConfig(
            livekit_url="wss://livekit.test",
            livekit_api_key="livekit-key",
            livekit_api_secret="livekit-secret",
            browser_token_ttl_seconds=600,
            dashscope_api_key="dashscope-secret",
            dashscope_realtime_url="wss://dashscope.test/api-ws/v1/realtime",
            qwen_realtime_model="qwen3.5-omni-plus-realtime",
            qwen_realtime_voice="Tina",
            default_prompt="你是一个电话外呼助手，回答要简短自然。",
            opening_message="您好，我是灵宸智能助手，请问现在方便简单沟通一下吗？",
            web_audio_echo_cancellation=True,
            web_audio_noise_suppression=True,
            web_audio_auto_gain_control=True,
            vad_type="server_vad",
            vad_threshold=0.5,
            vad_silence_duration_ms=800,
        ),
        livekit_room_manager=livekit_room_manager or FakeLiveKitRoomManager(),
        agent_runner=agent_runner or FakeAgentRunner(),
        registry=InMemorySessionRegistry(),
        event_store=InMemoryEventStore(),
    )


async def wait_until(predicate, *, attempts: int = 40, delay_seconds: float = 0.05) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(delay_seconds)
    raise AssertionError("condition was not met in time")


async def append_record_event(
    record_service: AiCallRecordService,
    *,
    call_id: str,
    event_type: str,
    event_time: datetime,
    source: str = "agent",
    payload: dict | None = None,
) -> None:
    await record_service.repository.append_event(
        event_id=f"evt_{call_id}_{event_type}_{event_time.timestamp()}",
        call_id=call_id,
        event_type=event_type,
        source=source,
        event_time=event_time,
        payload_json=json.dumps(payload, ensure_ascii=False) if payload else None,
    )


def _availability_agent_rows(
    *,
    row_id: int,
    agent_identity: str,
    scene_code: str,
    status: str,
    last_seen_at: datetime,
    enabled: bool = True,
    active_handoff_id: str | None = None,
) -> tuple[
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
]:
    now = datetime.now(timezone.utc)
    return (
        AiCallAgentProfileModel(
            id=row_id,
            tenant_id="000000",
            agent_identity=agent_identity,
            user_id=row_id,
            enabled=enabled,
            created_by=1,
            created_at=now,
            updated_by=1,
            updated_at=now,
        ),
        AiCallAgentSceneScopeModel(
            id=row_id,
            tenant_id="000000",
            agent_identity=agent_identity,
            scene_code=scene_code,
            created_by=1,
            created_at=now,
        ),
        AiCallHandoffAgentModel(
            id=row_id,
            tenant_id="000000",
            agent_identity=agent_identity,
            skill_group="default",
            status=status,
            active_handoff_id=active_handoff_id,
            active_call_id=None,
            console_session_id=None,
            last_seen_at=last_seen_at,
            status_updated_at=now,
        ),
    )


@pytest.fixture
async def b1_service():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        agent_runner = FakeAgentRunner()
        room_manager = FakeLiveKitRoomManager()
        orchestrator = build_b1_orchestrator(
            agent_runner=agent_runner,
            livekit_room_manager=room_manager,
        )
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        event_worker = AiCallEventPersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await event_worker.start()
        event_worker.attach_event_store(orchestrator.event_store)
        try:
            yield B1ServiceContext(
                service=AiCallService(
                    orchestrator,
                    record_service,
                    handoff_service=AiCallHandoffService(repository),
                ),
                record_service=record_service,
                event_worker=event_worker,
                agent_runner=agent_runner,
                room_manager=room_manager,
                session_maker=session_maker,
            )
        finally:
            event_worker.detach_all()
            await event_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_phase_b1_models_do_not_use_physical_foreign_keys_or_relationships() -> None:
    assert not AiCallRecordModel.__table__.foreign_keys
    assert not AiCallEventModel.__table__.foreign_keys
    assert not AiCallRecordingModel.__table__.foreign_keys
    assert not AiCallRecordingTrackModel.__table__.foreign_keys
    assert not AiCallDialogueSegmentModel.__table__.foreign_keys
    assert not AiCallAsrJobModel.__table__.foreign_keys
    assert not AiCallHandoffModel.__table__.foreign_keys
    assert not AiCallHandoffAgentModel.__table__.foreign_keys
    assert not AiCallVoiceProfileModel.__table__.foreign_keys
    assert not sa_inspect(AiCallRecordModel).relationships
    assert not sa_inspect(AiCallEventModel).relationships
    assert not sa_inspect(AiCallRecordingModel).relationships
    assert not sa_inspect(AiCallRecordingTrackModel).relationships
    assert not sa_inspect(AiCallDialogueSegmentModel).relationships
    assert not sa_inspect(AiCallAsrJobModel).relationships
    assert not sa_inspect(AiCallHandoffModel).relationships
    assert not sa_inspect(AiCallHandoffAgentModel).relationships
    assert not sa_inspect(AiCallVoiceProfileModel).relationships


def test_handoff_timeout_default_is_commercial_wait_window() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_HANDOFF_TIMEOUT_SECONDS == 30


@pytest.mark.anyio
async def test_issue_handoff_token_for_persisted_room_without_runtime_session(
    b1_service,
) -> None:
    orchestrator = b1_service.service.orchestrator

    token = orchestrator.issue_handoff_token_for_room(
        call_id="worker-owned-call",
        handoff_id="handoff-cross-process",
        human_agent_identity="agent-admin",
        room_name="worker-owned-room",
    )

    assert token.room_name == "worker-owned-room"
    assert token.participant_token.startswith("handoff-token-for-")
    assert [
        event.type
        for event in orchestrator.event_store.list_all("worker-owned-call")
    ] == ["handoff_accepted"]


def test_offline_asr_defaults_to_qwen_filetrans_with_chinese_language() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_OFFLINE_ASR_PROVIDER == "dashscope_qwen_filetrans"
    assert settings.AI_CALL_OFFLINE_ASR_MODEL == "qwen3-asr-flash-filetrans"
    assert settings.AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS == "zh"


def test_livekit_egress_uses_ogg_participant_format_when_mp3_is_requested() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
        file_type="MP3",
        participant_file_type="MP3",
    )

    assert manager.build_object_name("call_format") == "ai-call/recordings/call_format.mp3"
    assert (
        manager.build_participant_object_name(
            call_id="call_format",
            track_role="human_agent",
            participant_identity="human-agent-001",
        )
        == "ai-call/recordings/tracks/call_format/human_agent-human-agent-001.ogg"
    )


def test_livekit_egress_uses_separate_stop_timeout() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=2,
        stop_timeout_seconds=10,
        object_prefix="ai-call/recordings",
    )

    assert manager.timeout_seconds == 2
    assert manager.stop_timeout_seconds == 10


def test_livekit_egress_room_admin_token_scopes_to_room_for_participant_lookup() -> None:
    manager = LiveKitEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
    )

    token = manager._issue_room_admin_token(room_name="room_format")
    payload = jwt.decode(token, "secret", algorithms=["HS256"])

    assert payload["video"]["roomAdmin"] is True
    assert payload["video"]["room"] == "room_format"


@pytest.mark.anyio
async def test_livekit_egress_uses_track_egress_for_participant_audio() -> None:
    class CapturingEgressManager(LiveKitEgressManager):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.calls: list[tuple[str, dict]] = []
            self.room_calls: list[tuple[str, dict]] = []

        async def _post_egress(
            self,
            method: str,
            payload: dict,
            *,
            timeout_seconds: float | None = None,
        ) -> dict:
            _ = timeout_seconds
            self.calls.append((method, payload))
            return {"egress_id": f"EG_{method}", "status": "EGRESS_ACTIVE"}

        async def _post_room_service(self, method: str, payload: dict) -> dict:
            self.room_calls.append((method, payload))
            return {
                "identity": payload["identity"],
                "tracks": [
                    {"sid": "TR_video", "type": "VIDEO", "source": "CAMERA"},
                    {"sid": "TR_audio", "type": "AUDIO", "source": "MICROPHONE"},
                ],
            }

    manager = CapturingEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
        file_type="MP4",
        participant_file_type="OGG",
    )

    await manager.start_room_audio_recording(
        room_name="room_format",
        call_id="call_format",
        oss_config={},
    )
    await manager.start_participant_audio_recording(
        room_name="room_format",
        call_id="call_format",
        track_role="customer",
        participant_identity="browser-call_format",
        oss_config={},
    )

    assert manager.calls[0][0] == "StartRoomCompositeEgress"
    assert manager.calls[0][1]["file_outputs"][0]["file_type"] == "MP4"
    assert manager.calls[0][1]["file_outputs"][0]["filepath"].endswith(".mp4")
    assert manager.room_calls == [
        (
            "GetParticipant",
            {"room": "room_format", "identity": "browser-call_format"},
        )
    ]
    assert manager.calls[1][0] == "StartTrackEgress"
    assert manager.calls[1][1]["track_id"] == "TR_audio"
    assert manager.calls[1][1]["file"]["filepath"].endswith(".ogg")
    assert "file_type" not in manager.calls[1][1]["file"]


@pytest.mark.anyio
async def test_livekit_egress_queries_exact_stable_recording_object() -> None:
    class QueryingEgressManager(LiveKitEgressManager):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.calls: list[tuple[str, dict]] = []

        async def _post_egress(
            self,
            method: str,
            payload: dict,
            *,
            timeout_seconds: float | None = None,
        ) -> dict:
            _ = timeout_seconds
            self.calls.append((method, payload))
            return {
                "items": [
                    {
                        "egress_id": "EG_wrong",
                        "room_name": "room_format",
                        "status": "EGRESS_COMPLETE",
                        "file_results": [
                            {"filename": "ai-call/recordings/other.ogg"}
                        ],
                    },
                    {
                        "egress_id": "EG_target",
                        "room_name": "room_format",
                        "status": "EGRESS_COMPLETE",
                        "started_at": 1_000_000_000,
                        "ended_at": 61_000_000_000,
                        "file_results": [
                            {
                                "filename": "ai-call/recordings/call_format.ogg",
                                "duration": 60_000_000_000,
                                "size": 1024,
                            }
                        ],
                    },
                ]
            }

    manager = QueryingEgressManager(
        livekit_url="ws://livekit.test",
        api_key="key",
        api_secret="secret",
        timeout_seconds=1,
        object_prefix="ai-call/recordings",
        file_type="OGG",
    )

    by_id = await manager.get_egress("EG_target")
    by_object = await manager.find_room_audio_recording(
        "room_format",
        "ai-call/recordings/call_format.ogg",
    )

    assert by_id == by_object
    assert by_object is not None
    assert by_object.egress_id == "EG_target"
    assert by_object.duration_ms == 60_000
    assert by_object.file_size == 1024
    assert manager.calls == [
        ("ListEgress", {"egress_id": "EG_target"}),
        ("ListEgress", {"room_name": "room_format"}),
    ]


def test_livekit_egress_selects_first_audio_track_when_source_is_numeric() -> None:
    assert LiveKitEgressManager._select_audio_track([
        {"sid": "TR_video", "type": 1, "source": 1},
        {"sid": "TR_audio", "type": 0, "source": 2},
    ]) == {"sid": "TR_audio", "type": 0, "source": 2}


@pytest.mark.anyio
async def test_create_web_session_persists_record_and_key_events(b1_service) -> None:
    service, record_service = b1_service

    result = await service.create_web_session(
        voice="Cindy",
        prompt=None,
        business_id="324800000000000001",
    )

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.id is not None
    assert record.call_id == result.call_id
    assert record.business_type is None
    assert record.business_id == "324800000000000001"
    assert record.entry_type == "web"
    assert record.room_name == result.room_name
    assert record.participant_identity == result.participant_identity
    assert record.status == CallSessionStatus.READY.value

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events] == [
        "session_created",
        "session_preparing",
        "room_created",
        "browser_token_issued",
        "agent_started",
        "session_ready",
    ]
    assert all(event.call_id == result.call_id for event in events)


@pytest.mark.anyio
async def test_call_end_interrupted_runtime_event_is_persisted(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="call_end_interrupted",
        source="agent",
        payload={
            "reason": "user_transcript_after_call_end_tool",
            "toolCallId": "tool_pending_end",
            "toolReason": "customer_end",
            "endReason": "customer_end",
        },
    )

    await b1_service.flush_events()

    events = await record_service.list_events(
        result.call_id,
        event_type="call_end_interrupted",
    )
    assert len(events) == 1
    assert events[0].source == "agent"
    assert events[0].payload == {
        "endReason": "customer_end",
        "reason": "user_transcript_after_call_end_tool",
        "toolCallId": "tool_pending_end",
        "toolReason": "customer_end",
    }


@pytest.mark.anyio
async def test_end_session_updates_record_terminal_state_and_reason(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        tenant_id="000000",
        voice=None,
        prompt=None,
        business_id=None,
    )

    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
        tenant_id="000000",
    )
    await service.end_session(result.call_id)

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "web_user_end"
    assert record.answered_at is not None
    assert record.ended_at is not None
    assert record.duration_ms is not None
    assert record.duration_ms >= 0

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events].count("session_completed") == 1
    terminal_event = events[-1]
    assert terminal_event.event_type == "session_completed"
    assert terminal_event.payload == {"endReason": "web_user_end"}


@pytest.mark.anyio
async def test_repeated_end_session_preserves_first_terminal_reason(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        tenant_id="000000",
        voice=None,
        prompt=None,
        business_id=None,
    )
    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
        tenant_id="000000",
    )

    await service.end_session(result.call_id, end_reason="handoff_timeout")
    first_record = await record_service.get_record(result.call_id)
    assert first_record is not None
    assert first_record.end_reason == "handoff_timeout"
    first_ended_at = first_record.ended_at
    first_duration_ms = first_record.duration_ms

    await service.end_session(result.call_id)

    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "handoff_timeout"
    assert record.ended_at == first_ended_at
    assert record.duration_ms == first_duration_ms


@pytest.mark.anyio
async def test_browser_disconnect_after_runtime_terminal_preserves_terminal_reason(
    b1_service,
) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        tenant_id="000000",
        voice=None,
        prompt=None,
        business_id=None,
    )
    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
        tenant_id="000000",
    )

    await service.orchestrator.end_session(result.call_id, end_reason="handoff_timeout")
    browser_event = await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_disconnect",
        timestamp=None,
        tenant_id="000000",
    )
    await b1_service.flush_events()

    assert browser_event.payload["terminalSessionStatus"] == "completed"
    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "handoff_timeout"


@pytest.mark.anyio
async def test_browser_disconnect_completes_record_with_disconnect_reason(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        tenant_id="000000",
        voice=None,
        prompt=None,
        business_id=None,
    )
    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
        tenant_id="000000",
    )

    browser_event = await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_disconnect",
        timestamp=None,
        tenant_id="000000",
    )

    assert browser_event.type == "browser_disconnect"
    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "browser_disconnect"
    assert record.ended_at is not None
    assert record.duration_ms is not None

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events][-3:] == [
        "browser_disconnect",
        "session_ending",
        "session_completed",
    ]
    assert events[-1].payload == {"endReason": "browser_disconnect"}


@pytest.mark.anyio
async def test_agent_start_failure_persists_failed_record_and_terminal_payload() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        orchestrator = build_b1_orchestrator(agent_runner=FailingAgentRunner())
        event_worker = AiCallEventPersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await event_worker.start()
        event_worker.attach_event_store(orchestrator.event_store)
        record_service = AiCallRecordService(AiCallRecordRepository(db))
        service = AiCallService(
            orchestrator,
            record_service,
        )
        try:
            with pytest.raises(CustomException) as exc_info:
                await service.create_web_session(
                    voice=None,
                    prompt=None,
                    business_id=None,
                )
            assert exc_info.value.msg == "Agent 启动失败"

            await event_worker.flush_pending()
            rows, total = await record_service.list_records()
            assert total == 1
            record = rows[0]
            assert record.status == CallSessionStatus.FAILED.value
            assert record.end_reason == "agent_start_failed"
            assert record.failure_stage == "agent_start"
            assert record.failure_message == "Agent 启动失败"

            events = await record_service.list_events(record.call_id)
            assert events[-1].event_type == "session_failed"
            assert events[-1].payload == {
                "endReason": "agent_start_failed",
                "failureMessage": "Agent 启动失败",
                "failureStage": "agent_start",
            }
        finally:
            event_worker.detach_all()
            await event_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_record_query_outputs_bigint_ids_as_strings(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id="324800000000000002",
    )

    await b1_service.flush_events()
    detail = await service.get_record_detail(result.call_id)
    assert detail["record"]["id"].isdigit()
    assert isinstance(detail["record"]["id"], str)
    assert detail["record"]["businessId"] == "324800000000000002"
    assert detail["lastEvent"]["id"].isdigit()
    assert isinstance(detail["lastEvent"]["id"], str)
    assert detail["executionConfig"] is None

    events = await service.list_record_events(result.call_id)
    assert events["total"] == 6
    assert isinstance(events["rows"][0]["id"], str)
    assert events["rows"][0]["eventType"] == "session_created"


@pytest.mark.anyio
async def test_record_access_is_scoped_to_current_tenant(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
        tenant_id="tenant-a",
    )

    await service.require_record_for_tenant(
        tenant_id="tenant-a",
        call_id=result.call_id,
    )
    with pytest.raises(CustomException) as exc_info:
        await service.require_record_for_tenant(
            tenant_id="tenant-b",
            call_id=result.call_id,
        )

    assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_record_detail_returns_seat_after_call_disposition(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
        tenant_id="000000",
    )
    record = await record_service.get_record(result.call_id)
    assert record is not None
    now = datetime(2026, 8, 4, 8, 6, tzinfo=timezone.utc)
    handoff_id = "handoff-record-detail-seat-disposition"

    async with b1_service.session_maker() as db:
        db.add_all([
            AiCallAfterCallWorkModel(
                id=324800000000000201,
                work_id="work-record-detail-seat-disposition",
                tenant_id=record.tenant_id,
                call_id=result.call_id,
                handoff_id=handoff_id,
                agent_identity="agent-admin",
                disposition_code="follow_up_required",
                summary="请继续跟进试用方案",
                needs_follow_up=True,
                submitted_at=now,
                created_at=now,
                updated_at=now,
            ),
            AiCallFollowUpTaskModel(
                id=324800000000000202,
                tenant_id=record.tenant_id,
                source_type="after_call_work",
                source_key=f"handoff:{handoff_id}",
                source_call_id=result.call_id,
                source_handoff_id=handoff_id,
                scene_code="intro_geo",
                business_type=None,
                business_id=None,
                contact_ref="record-detail-seat-disposition",
                masked_contact="138****0000",
                owner_agent_identity="agent-admin",
                status="pending",
                follow_up_reason="人工通话后续跟进",
                customer_callback_at=None,
                summary="请继续跟进试用方案",
                closed_reason=None,
                closed_remark=None,
                completed_at=None,
                closed_at=None,
                created_at=now,
                updated_at=now,
            ),
            AiCallRecordModel(
                id=324800000000000203,
                tenant_id=record.tenant_id,
                call_id="call-record-detail-callback",
                follow_up_id=324800000000000202,
                business_type=None,
                business_id=None,
                scene_code="intro_geo",
                entry_type="sip_callback",
                room_name="room-record-detail-callback",
                participant_identity="customer-record-detail-callback",
                status="completed",
                end_reason="callback_ended_by_agent",
                started_at=now,
                answered_at=now,
                ended_at=now,
                duration_ms=0,
            ),
        ])
        await db.commit()

    response = RecordDetailOut.model_validate(
        await service.get_record_detail(result.call_id)
    ).model_dump(mode="json", by_alias=True)

    after_call_work = response["afterCallWork"]
    assert after_call_work["agentIdentity"] == "agent-admin"
    assert after_call_work["dispositionCode"] == "follow_up_required"
    assert after_call_work["summary"] == "请继续跟进试用方案"
    assert after_call_work["needsFollowUp"] is True
    assert after_call_work["submittedAt"].startswith("2026-08-04T08:06:00")
    follow_up = response["followUp"]
    assert follow_up["id"] == "324800000000000202"
    assert follow_up["status"] == "pending"
    assert follow_up["reason"] == "人工通话后续跟进"
    assert follow_up["sourceCallId"] == result.call_id
    assert follow_up["sourceRecord"]["callId"] == result.call_id
    assert [row["callId"] for row in follow_up["callbackRecords"]] == [
        "call-record-detail-callback"
    ]


@pytest.mark.anyio
async def test_outbound_record_detail_uses_frozen_task_execution_config(
    b1_service,
) -> None:
    task_id = 324800000000000101
    now = datetime(2026, 7, 30, 2, 0, tzinfo=timezone.utc)
    frozen_snapshot = {
        "prompt": {
            "id": "prompt-frozen",
            "name": "冻结提示词",
            "sceneCode": "intro_frozen",
        },
        "voice": {
            "voice": "Cherry",
            "voiceName": "芊悦",
        },
        "rule": {
            "ruleName": "冻结规则",
        },
    }
    async with b1_service.session_maker() as db:
        db.add_all([
            AiCallOutboundTaskModel(
                id=task_id,
                tenant_id="000000",
                validation_id=324800000000000102,
                idempotency_key="record-detail-frozen-config",
                request_fingerprint="a" * 64,
                task_name="正式外呼任务",
                task_mode="single",
                status="COMPLETED",
                total_targets=1,
                completed_targets=1,
                connected_targets=1,
                failed_targets=0,
                execution_mode="immediate",
                scheduled_at=None,
                next_dispatch_at=None,
                last_dispatched_at=now,
                started_at=now,
                ended_at=now,
                prompt_profile_id="prompt-current",
                prompt_name="不能使用的当前提示词",
                scene_code="intro_current",
                voice="Tina",
                voice_name="不能使用的当前音色",
                rule_id=324800000000000103,
                rule_name="不能使用的当前规则",
                rule_summary="当前规则摘要",
                line_id=None,
                line_name=None,
                config_snapshot_json=json.dumps(frozen_snapshot, ensure_ascii=False),
                error_message=None,
                created_by=1,
                created_by_name="管理员",
                created_at=now,
                updated_at=now,
            ),
            AiCallOutboundTargetModel(
                id=324800000000000105,
                tenant_id="000000",
                task_id=task_id,
                validation_id=324800000000000102,
                source_validation_row_id=324800000000000106,
                source_row_number=1,
                phone_number="19900001001",
                customer_name="测试客户",
                status="COMPLETED",
                attempt_count=1,
                latest_result="connected",
                created_at=now,
                updated_at=now,
            ),
        ])
        await db.commit()

    await b1_service.record_service.create_sip_record(
        call_id="call_outbound_frozen_config",
        business_type="outbound_task",
        business_id=str(task_id),
        room_name="room-outbound-frozen-config",
        participant_identity="sip-outbound-frozen-config",
        started_at=now,
    )

    detail = await b1_service.service.get_record_detail(
        "call_outbound_frozen_config"
    )
    response = RecordDetailOut.model_validate(detail).model_dump(
        mode="json",
        by_alias=True,
    )

    assert response["executionConfig"] == {
        "promptProfileId": "prompt-frozen",
        "promptName": "冻结提示词",
        "sceneCode": "intro_frozen",
        "voice": "Cherry",
        "voiceName": "芊悦",
        "ruleName": "冻结规则",
    }

    attempt_id = 324800000000000104
    attempt_call_id = "call_outbound_attempt_frozen_config"
    async with b1_service.session_maker() as db:
        db.add(
            AiCallOutboundAttemptModel(
                id=attempt_id,
                tenant_id="000000",
                task_id=task_id,
                target_id=324800000000000105,
                attempt_no=1,
                call_id=attempt_call_id,
                status="COMPLETED",
                call_result="connected",
                started_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        await db.commit()

    await b1_service.record_service.create_sip_record(
        call_id=attempt_call_id,
        tenant_id="000000",
        business_type="outbound_attempt",
        business_id=str(attempt_id),
        room_name="room-outbound-attempt-frozen-config",
        participant_identity="sip-outbound-attempt-frozen-config",
        started_at=now,
    )
    attempt_detail = await b1_service.service.get_record_detail(attempt_call_id)
    assert attempt_detail["executionConfig"] == response["executionConfig"]
    assert attempt_detail["record"]["taskId"] == str(task_id)
    attempt_response = RecordDetailOut.model_validate(attempt_detail).model_dump(
        mode="json",
        by_alias=True,
    )
    assert attempt_response["record"]["taskId"] == str(task_id)


@pytest.mark.anyio
async def test_record_list_includes_successful_semantic_summary(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    repository = record_service.repository
    await repository.ensure_semantic_analysis_record(
        call_id=result.call_id,
        scene_code="intro_geo",
    )
    await repository.update_semantic_analysis_success(
        call_id=result.call_id,
        analysis_result={
            "summary": "客户希望进一步了解服务效果。",
            "feedback_type": "中性",
            "key_points": [],
            "time_hint": {},
            "tags": [],
        },
        transcript_snapshot_json="{}",
        transcript_hash="hash-1",
    )

    page = await service.list_records(tenant_id="000000")

    assert page["rows"][0]["summary"] == "客户希望进一步了解服务效果。"


@pytest.mark.anyio
async def test_record_list_filters_and_projects_after_call_result_status(
    b1_service,
) -> None:
    now = datetime.now(timezone.utc)

    def record(
        row_id: int,
        call_id: str,
        operator: str | None,
        *,
        answered: bool = True,
    ):
        return AiCallRecordModel(
            id=row_id,
            tenant_id="000000",
            call_id=call_id,
            follow_up_data_id=9001 if call_id == "call-result-pending" else None,
            operator_agent_identity=operator,
            entry_type="sip_callback",
            room_name=f"room-{call_id}",
            participant_identity=f"sip-{call_id}",
            status="completed",
            started_at=now,
            answered_at=now if answered else None,
            ended_at=now,
            end_reason="callback_no_answer" if not answered and operator else None,
        )

    async with b1_service.session_maker.begin() as db:
        db.add_all(
            [
                record(9101, "call-result-pending", "agent-a"),
                record(9102, "call-result-acw", "agent-a"),
                record(9103, "call-result-handling", "agent-a"),
                record(9104, "call-result-na", None, answered=False),
                record(9105, "call-result-unanswered", "agent-a", answered=False),
                AiCallFollowUpDataModel(
                    id=9001,
                    tenant_id="000000",
                    task_id=9201,
                    target_id=9202,
                    source_call_id="call-result-pending",
                    classification="interested",
                    classification_reason="客户有明确需求",
                    classification_source="human",
                    classification_confidence=None,
                    suggest_review=False,
                    low_value_reason=None,
                    latest_conclusion="等待人工提交本次结果",
                    last_contact_at=now,
                    blocking_human_call_id="call-result-pending",
                    version=3,
                    classification_updated_at=now,
                    classification_updated_by="agent-a",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallAfterCallWorkModel(
                    id=9301,
                    work_id="acw-result-submitted",
                    tenant_id="000000",
                    call_id="call-result-acw",
                    handoff_id="handoff-result-acw",
                    agent_identity="agent-a",
                    disposition_code="resolved",
                    summary="已提交转人工结果",
                    needs_follow_up=False,
                    submitted_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallFollowUpHandlingResultModel(
                    id=9401,
                    tenant_id="000000",
                    follow_up_id=9402,
                    follow_up_data_id=None,
                    idempotency_key="handling-result-submitted",
                    request_fingerprint=None,
                    related_call_id="call-result-handling",
                    contact_channel="manual_phone",
                    contact_result="connected",
                    remark="已提交人工回拨结果",
                    next_action="complete",
                    next_follow_up_at=None,
                    closed_reason=None,
                    classification=None,
                    low_value_reason=None,
                    result_version=None,
                    agent_identity="agent-a",
                    handled_at=now,
                    created_at=now,
                ),
            ]
        )

    pending = await b1_service.service.list_records(
        tenant_id="000000",
        after_call_result_status="pending",
        operator_agent_identity="agent-a",
    )
    submitted = await b1_service.service.list_records(
        tenant_id="000000",
        after_call_result_status="submitted",
        operator_agent_identity="agent-a",
    )
    not_applicable = await b1_service.service.list_records(
        tenant_id="000000",
        after_call_result_status="not_applicable",
    )

    assert [row["callId"] for row in pending["rows"]] == ["call-result-pending"]
    assert pending["rows"][0]["afterCallResultStatus"] == "pending"
    assert pending["rows"][0]["afterCallResultType"] == "follow_up_data"
    assert {row["callId"] for row in submitted["rows"]} == {
        "call-result-acw",
        "call-result-handling",
    }
    assert "call-result-na" in {row["callId"] for row in not_applicable["rows"]}
    assert "call-result-unanswered" in {
        row["callId"] for row in not_applicable["rows"]
    }

    pending_detail = RecordDetailOut.model_validate(
        await b1_service.service.get_record_detail("call-result-pending")
    ).model_dump(mode="json", by_alias=True)
    handling_detail = RecordDetailOut.model_validate(
        await b1_service.service.get_record_detail("call-result-handling")
    ).model_dump(mode="json", by_alias=True)
    assert pending_detail["record"]["afterCallResultStatus"] == "pending"
    assert pending_detail["followUpData"]["classification"] == "interested"
    assert pending_detail["followUpData"]["version"] == 3
    assert handling_detail["record"]["afterCallResultStatus"] == "submitted"
    assert handling_detail["handlingResult"]["remark"] == "已提交人工回拨结果"


@pytest.mark.anyio
async def test_callback_record_inherits_source_outbound_context(b1_service) -> None:
    now = datetime.now(timezone.utc)
    task_id = 324800000000000301
    target_id = 324800000000000302
    attempt_id = 324800000000000303
    callback_call_id = "call-record-list-callback"
    async with b1_service.session_maker.begin() as db:
        db.add_all(
            [
                AiCallOutboundTaskModel(
                    id=task_id,
                    tenant_id="000000",
                    validation_id=1,
                    idempotency_key="callback-record-context-task",
                    request_fingerprint="callback-record-context-fingerprint",
                    task_name="回拨来源任务",
                    task_mode="single",
                    status="COMPLETED",
                    total_targets=1,
                    completed_targets=1,
                    connected_targets=1,
                    failed_targets=0,
                    execution_mode="immediate",
                    started_at=now,
                    ended_at=now,
                    prompt_name="GEO 产品介绍",
                    scene_code="intro_geo",
                    voice="Tina",
                    voice_name="甜甜 Tina",
                    rule_id=1,
                    rule_name="工作日规则",
                    rule_summary="00:00-23:55",
                    config_snapshot_json="{}",
                    created_by=1,
                    created_by_name="管理员",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundTargetModel(
                    id=target_id,
                    tenant_id="000000",
                    task_id=task_id,
                    validation_id=1,
                    source_validation_row_id=1,
                    source_row_number=1,
                    phone_number="19900001001",
                    customer_name="刘先生",
                    status="COMPLETED",
                    attempt_count=1,
                    latest_result="connected",
                    created_at=now,
                    updated_at=now,
                ),
                AiCallOutboundAttemptModel(
                    id=attempt_id,
                    tenant_id="000000",
                    task_id=task_id,
                    target_id=target_id,
                    attempt_no=1,
                    call_id="call-record-list-source",
                    status="COMPLETED",
                    call_result="connected",
                    started_at=now,
                    ended_at=now,
                    created_at=now,
                    updated_at=now,
                ),
                AiCallRecordModel(
                    id=324800000000000304,
                    tenant_id="000000",
                    call_id=callback_call_id,
                    follow_up_id=324800000000000305,
                    business_type="outbound_attempt",
                    business_id=str(attempt_id),
                    scene_code="intro_geo",
                    entry_type="sip_callback",
                    room_name="room-record-list-callback",
                    participant_identity="sip-record-list-callback",
                    status="completed",
                    started_at=now,
                    ended_at=now,
                    duration_ms=0,
                ),
            ]
        )

    page = await b1_service.service.list_records(
        tenant_id="000000",
        task_id=task_id,
        customer_name="刘先生",
    )

    assert page["total"] == 1
    row = page["rows"][0]
    assert row["callId"] == callback_call_id
    assert row["taskId"] == str(task_id)
    assert row["targetId"] == str(target_id)
    assert row["taskName"] == "回拨来源任务"
    assert row["customerName"] == "刘先生"
    assert row["phoneNumber"] == "19900001001"


@pytest.mark.anyio
async def test_handoff_availability_counts_online_and_available_agents(
    b1_service,
) -> None:
    try:
        from app.services.ai_call.handoff_availability_service import (
            AiCallHandoffAvailabilityService,
        )
    except ModuleNotFoundError:
        pytest.fail("handoff availability service is missing")

    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await record_service.repository.update_record(
        result.call_id,
        scene_code="intro_geo",
    )
    now = datetime.now(timezone.utc)
    record_service.repository.db.add_all(
        [
            *_availability_agent_rows(
                row_id=101,
                agent_identity="agent-available",
                scene_code="intro_geo",
                status="available",
                last_seen_at=now,
            ),
            *_availability_agent_rows(
                row_id=102,
                agent_identity="agent-busy",
                scene_code="intro_geo",
                status="in_call",
                last_seen_at=now,
                active_handoff_id="handoff-busy",
            ),
        ]
    )
    await record_service.repository.db.flush()

    snapshot = await AiCallHandoffAvailabilityService(
        record_service.repository.db
    ).get_for_call(result.call_id)

    assert snapshot.online_agent_count == 2
    assert snapshot.available_agent_count == 1


@pytest.mark.anyio
async def test_handoff_availability_excludes_stale_paused_and_wrong_scene_agents(
    b1_service,
) -> None:
    try:
        from app.services.ai_call.handoff_availability_service import (
            AiCallHandoffAvailabilityService,
        )
    except ModuleNotFoundError:
        pytest.fail("handoff availability service is missing")

    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await record_service.repository.update_record(
        result.call_id,
        scene_code="intro_geo",
    )
    now = datetime.now(timezone.utc)
    record_service.repository.db.add_all(
        [
            *_availability_agent_rows(
                row_id=201,
                agent_identity="agent-stale",
                scene_code="intro_geo",
                status="available",
                last_seen_at=now - timedelta(seconds=31),
            ),
            *_availability_agent_rows(
                row_id=202,
                agent_identity="agent-paused",
                scene_code="intro_geo",
                status="paused",
                last_seen_at=now,
            ),
            *_availability_agent_rows(
                row_id=203,
                agent_identity="agent-other-scene",
                scene_code="intro_contract",
                status="available",
                last_seen_at=now,
            ),
        ]
    )
    await record_service.repository.db.flush()

    snapshot = await AiCallHandoffAvailabilityService(
        record_service.repository.db
    ).get_for_call(result.call_id)

    assert snapshot.online_agent_count == 0
    assert snapshot.available_agent_count == 0


@pytest.mark.anyio
async def test_handoff_availability_requires_existing_call(b1_service) -> None:
    try:
        from app.services.ai_call.handoff_availability_service import (
            AiCallHandoffAvailabilityService,
        )
    except ModuleNotFoundError:
        pytest.fail("handoff availability service is missing")

    _service, record_service = b1_service
    with pytest.raises(CustomException, match="通话记录不存在"):
        await AiCallHandoffAvailabilityService(
            record_service.repository.db
        ).get_for_call("missing-call")


@pytest.mark.anyio
async def test_create_handoff_persists_record_and_suspends_agent(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message="验证页手工发起转人工",
    )

    assert handoff["id"].isdigit()
    assert isinstance(handoff["id"], str)
    assert handoff["handoffId"].startswith("handoff_")
    assert handoff["callId"] == result.call_id
    assert handoff["roomName"] == result.room_name
    assert handoff["status"] == "requested"
    assert handoff["requestSource"] == "operator"
    assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
    assert service.orchestrator.registry.get(result.call_id).status == CallSessionStatus.WAITING

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    event_types = [event.event_type for event in events]
    assert "handoff_requested" in event_types
    assert "agent_suspended_for_handoff" in event_types


@pytest.mark.anyio
async def test_handoff_requested_event_is_published_after_commit(
    tmp_path,
    monkeypatch,
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'handoff-after-commit.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallService(
            build_b1_orchestrator(),
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await db.commit()

        committed_when_published: list[bool] = []

        async def capture_requested_event(_tenant_id, _event_type, payload):
            async with session_maker() as read_db:
                handoff = await AiCallRecordRepository(read_db).get_handoff_by_id(
                    payload["handoff_id"]
                )
            committed_when_published.append(handoff is not None)

        monkeypatch.setattr(
            "app.services.ai_call.agent_console_reconciler.publish_agent_console_event",
            capture_requested_event,
        )

        async with db.begin():
            await service.create_handoff(
                call_id=result.call_id,
                source="customer",
                reason="customer_request",
                request_message="转人工",
            )

        await wait_until(lambda: bool(committed_when_published))
        assert committed_when_published == [True]

    await engine.dispose()


@pytest.mark.anyio
async def test_duplicate_handoff_request_returns_active_one(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    first = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    second = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    assert second["handoffId"] == first["handoffId"]
    assert b1_service.agent_runner.suspended_call_ids == [result.call_id]


@pytest.mark.anyio
async def test_handoff_trigger_worker_creates_customer_handoff_from_tool_request(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_1",
                "reason": "customer_request",
            },
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "customer"
        assert handoffs["rows"][0]["requestReason"] == "customer_request"
        assert handoffs["rows"][0]["status"] == "requested"
        assert handoffs["rows"][0]["requestMessage"] == "模型判断用户需要转人工"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_tool_requested" in event_types
        assert "handoff_intent_detected" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_customer_partial_handoff_is_created_only_after_affirmative_confirmation(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_partial",
                "reason": "customer_request",
                "confirmationRequired": True,
            },
        )
        await worker.flush_pending()

        assert await service.list_handoffs(result.call_id) == {"rows": [], "total": 0}

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "confirm_partial", "transcript": "是的，转人工。"},
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["status"] == "requested"
        assert handoffs["rows"][0]["requestReason"] == "customer_request"

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_confirmation_confirmed" in event_types
        assert "handoff_auto_triggered" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_customer_partial_handoff_rejects_negative_confirmation(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_negative_confirmation",
                "reason": "customer_request",
                "confirmationRequired": True,
            },
        )
        await worker.flush_pending()

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={
                "item_id": "negative_confirmation",
                "transcript": "不是，我不想转人工。",
            },
        )
        await worker.flush_pending()

        assert await service.list_handoffs(result.call_id) == {"rows": [], "total": 0}

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_declined" in event_types
        assert "handoff_confirmation_confirmed" not in event_types
        assert "handoff_auto_triggered" not in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("snapshot", "expected_prompt_kind"),
    [
        (HandoffAgentAvailability(online_agent_count=1, available_agent_count=1), "available"),
        (HandoffAgentAvailability(online_agent_count=1, available_agent_count=0), "busy"),
    ],
)
async def test_handoff_availability_routes_requested_pool(
    b1_service,
    snapshot: HandoffAgentAvailability,
    expected_prompt_kind: str,
) -> None:
    service, _record_service = b1_service
    availability = FakeHandoffAvailabilityService(snapshot)
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        availability_service_factory=lambda _db: availability,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await b1_service.event_worker.flush_pending()
        b1_service.event_worker.detach_all()
        service.orchestrator.event_store.append(
            result.call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": "handoff_tool_available",
                "reason": "customer_request",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["rows"][0]["status"] == "requested"
        assert availability.call_ids == [result.call_id]
        triggered = next(
            event
            for event in service.orchestrator.event_store.list_all(result.call_id)
            if event.type == "handoff_auto_triggered"
        )
        assert triggered.payload["waitingPromptKind"] == expected_prompt_kind
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_without_online_agent_fails_without_creating_follow_up(
    b1_service,
) -> None:
    service, record_service = b1_service
    availability = FakeHandoffAvailabilityService(
        HandoffAgentAvailability(online_agent_count=0, available_agent_count=0)
    )
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        availability_service_factory=lambda _db: availability,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await b1_service.event_worker.flush_pending()
        b1_service.event_worker.detach_all()
        service.orchestrator.event_store.append(
            result.call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": "handoff_tool_no_online",
                "reason": "customer_request",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["status"] == "failed"
        assert handoffs["rows"][0]["endReason"] == "no_online_agent"
        assert handoffs["rows"][0]["failureStage"] == "availability_check"

        follow_ups = list(
            (
                await record_service.repository.db.execute(
                    select(AiCallFollowUpTaskModel).where(
                        AiCallFollowUpTaskModel.source_handoff_id
                        == handoffs["rows"][0]["handoffId"]
                    )
                )
            )
            .scalars()
            .all()
        )
        assert follow_ups == []
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_same_turn_transcript_and_tool_create_only_one_terminal_handoff(
    b1_service,
) -> None:
    service, record_service = b1_service
    availability = FakeHandoffAvailabilityService(
        HandoffAgentAvailability(online_agent_count=0, available_agent_count=0)
    )
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.99,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        availability_service_factory=lambda _db: availability,
    )
    worker = AiCallHandoffTriggerWorker(
        trigger_service,
        transcript_trigger_enabled=True,
    )
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await b1_service.event_worker.flush_pending()
        b1_service.event_worker.detach_all()
        service.orchestrator.event_store.append(
            result.call_id,
            "user_transcript_done",
            "provider",
            {"item_id": "handoff_same_turn", "transcript": "我要转人工。"},
        )
        service.orchestrator.event_store.append(
            result.call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": "handoff_tool_same_turn",
                "reason": "customer_request",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["status"] == "failed"
        assert handoffs["rows"][0]["endReason"] == "no_online_agent"

        follow_ups = list(
            (await record_service.repository.db.execute(select(AiCallFollowUpTaskModel)))
            .scalars()
            .all()
        )
        assert follow_ups == []
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_availability_failure_is_not_reported_as_agent_busy(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        availability_service_factory=lambda _db: FailingHandoffAvailabilityService(),
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await b1_service.event_worker.flush_pending()
        b1_service.event_worker.detach_all()
        service.orchestrator.event_store.append(
            result.call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": "handoff_tool_availability_error",
                "reason": "customer_request",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["status"] == "failed"
        assert handoffs["rows"][0]["endReason"] == "handoff_service_unavailable"
        assert handoffs["rows"][0]["failureStage"] == "availability_check"

        follow_ups = list(
            (
                await record_service.repository.db.execute(
                    select(AiCallFollowUpTaskModel).where(
                        AiCallFollowUpTaskModel.source_handoff_id
                        == handoffs["rows"][0]["handoffId"]
                    )
                )
            )
            .scalars()
            .all()
        )
        assert follow_ups == []
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_availability_database_error_uses_fresh_transaction(
    b1_service,
) -> None:
    service, _record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        availability_service_factory=DatabaseFailingHandoffAvailabilityService,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await b1_service.event_worker.flush_pending()
        b1_service.event_worker.detach_all()
        service.orchestrator.event_store.append(
            result.call_id,
            "handoff_tool_requested",
            "agent",
            {
                "toolCallId": "handoff_tool_database_error",
                "reason": "customer_request",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["status"] == "failed"
        assert handoffs["rows"][0]["endReason"] == "handoff_service_unavailable"
        assert handoffs["rows"][0]["failureStage"] == "availability_check"
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_waits_for_confirmation_on_business_escalation_tool_request(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_business",
                "reason": "business_escalation",
            },
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_tool_requested" in event_types
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_auto_triggered" not in event_types
        assert "handoff_requested" not in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_creates_business_escalation_after_user_confirms(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_business_confirm",
                "reason": "business_escalation",
            },
        )
        await worker.flush_pending()

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_confirm_handoff", "transcript": "可以，转吧。"},
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "customer"
        assert handoffs["rows"][0]["requestReason"] == "business_escalation"
        assert handoffs["rows"][0]["status"] == "requested"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_confirmation_confirmed" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
@pytest.mark.parametrize("transcript", ["转啊。", "你不转。"])
async def test_handoff_trigger_worker_treats_transfer_urge_as_confirmation(
    b1_service,
    transcript: str,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_business_urge",
                "reason": "business_escalation",
            },
        )
        await worker.flush_pending()

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_urge_handoff", "transcript": transcript},
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestReason"] == "business_escalation"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_confirmation_confirmed" in event_types
        assert "handoff_confirmation_declined" not in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_uses_recent_affirmation_for_late_business_escalation_tool(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_recent_handoff_yes", "transcript": "好的。"},
        )
        await worker.flush_pending()

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_business_late",
                "reason": "business_escalation",
            },
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestReason"] == "business_escalation"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == ["好的。"]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_confirmation_confirmed" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_declines_business_escalation_confirmation(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.0,
            reason="not_handoff",
            summary="不应进入分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="handoff_tool_requested",
            source="agent",
            payload={
                "toolCallId": "handoff_tool_business_decline",
                "reason": "business_escalation",
            },
        )
        await worker.flush_pending()

        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_decline_handoff", "transcript": "不用了，继续说。"},
        )
        await worker.flush_pending()

        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_confirmation_requested" in event_types
        assert "handoff_confirmation_declined" in event_types
        assert "handoff_auto_triggered" not in event_types
        assert "handoff_requested" not in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_auto_creates_customer_handoff(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.92,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_handoff", "transcript": "我想找真人客服"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "customer"
        assert handoffs["rows"][0]["requestReason"] == "customer_request"
        assert handoffs["rows"][0]["status"] == "requested"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == ["我想找真人客服"]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_detected" in event_types
        assert "handoff_auto_triggered" in event_types
        assert "handoff_requested" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_uses_explicit_transfer_delta_when_done_is_missing(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.2,
            reason="not_handoff",
            summary="不应调用通用分类器",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_delta",
            source="provider",
            payload={
                "item_id": "item_explicit_transfer",
                "text": "转人工",
                "stash": "",
            },
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestReason"] == "customer_request"
        assert classifier.transcripts == []

        await b1_service.flush_events()
        events = await record_service.list_events(result.call_id)
        detected = next(
            event for event in events if event.event_type == "handoff_intent_detected"
        )
        assert json.loads(detected.payload_json)["classifierSource"] == (
            "realtime_delta_guard"
        )
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_ignores_low_confidence_text(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=False,
            confidence=0.3,
            reason="not_handoff",
            summary="未识别到明确转人工意图",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_no_handoff", "transcript": "人工智能是什么意思"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_ignored" in event_types
        assert "handoff_requested" not in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_ignores_product_consultation_without_classifier_timeout(
    b1_service,
) -> None:
    service, record_service = b1_service
    primary = SlowHandoffIntentClassifier()
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_product_consultation", "transcript": "你们有 demo 吗？"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []
        assert primary.transcripts == []

        await b1_service.flush_events()
        events = await record_service.list_events(result.call_id)
        event_types = [event.event_type for event in events]
        assert "handoff_intent_ignored" in event_types
        assert "handoff_requested" not in event_types
        ignored = next(event for event in events if event.event_type == "handoff_intent_ignored")
        assert ignored.payload["reason"] == "not_handoff"
        assert ignored.payload["classifierSource"] == "rule_fallback"
        assert ignored.payload["confidence"] >= 0.9
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_creates_handoff_for_product_contact_question(
    b1_service,
) -> None:
    service, record_service = b1_service
    primary = SlowHandoffIntentClassifier()
    classifier = CompositeHandoffIntentClassifier(
        primary=primary,
        fallback=RuleBasedHandoffIntentClassifier(),
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_product_contact", "transcript": "怎么联系啊？"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestReason"] == "business_escalation"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert primary.transcripts == []

        await b1_service.flush_events()
        events = await record_service.list_events(result.call_id)
        event_types = [event.event_type for event in events]
        assert "handoff_intent_detected" in event_types
        assert "handoff_requested" in event_types
        detected = next(event for event in events if event.event_type == "handoff_intent_detected")
        assert detected.payload["classifierSource"] == "rule_fallback"
        assert detected.payload["reason"] == "business_escalation"
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_skips_semantically_rejected_transcript(
    b1_service,
) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.95,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={
                "item_id": "item_rejected_handoff",
                "transcript": "帮我转人工",
                "transcriptTrust": "low_confidence",
                "semanticAction": "reject",
                "semanticRejectReason": "opening_double_talk_low_confidence_transcript",
            },
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs == {"rows": [], "total": 0}
        assert b1_service.agent_runner.suspended_call_ids == []
        assert classifier.transcripts == []

        await b1_service.flush_events()
        events = await record_service.list_events(result.call_id)
        event_types = [event.event_type for event in events]
        assert "handoff_intent_ignored" in event_types
        assert "handoff_requested" not in event_types
        ignored = next(event for event in events if event.event_type == "handoff_intent_ignored")
        assert ignored.payload["reason"] == "low_confidence_transcript"
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_handoff_trigger_worker_does_not_duplicate_active_handoff(b1_service) -> None:
    service, record_service = b1_service
    classifier = FakeHandoffIntentClassifier(
        HandoffIntentResult(
            matched=True,
            confidence=0.95,
            reason="customer_request",
            summary="用户明确要求转人工",
            source="test_classifier",
        )
    )

    def service_factory(db):
        repository = AiCallRecordRepository(db)
        return AiCallService(
            service.orchestrator,
            AiCallRecordService(repository),
            handoff_service=AiCallHandoffService(repository),
        )

    trigger_service = AiCallHandoffTriggerService(
        b1_service.session_maker,
        service_factory,
        classifier,
        threshold=0.8,
        timeout_seconds=0.2,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    worker.attach_event_store(service.orchestrator.event_store)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message="验证页手工发起转人工",
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="user_transcript_done",
            source="provider",
            payload={"item_id": "item_duplicate", "transcript": "帮我转人工"},
        )

        await worker.flush_pending()
        handoffs = await service.list_handoffs(result.call_id)
        assert handoffs["total"] == 1
        assert handoffs["rows"][0]["requestSource"] == "operator"
        assert b1_service.agent_runner.suspended_call_ids == [result.call_id]
        assert classifier.transcripts == []

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_intent_ignored" in event_types
    finally:
        worker.detach_all()
        await worker.stop()


@pytest.mark.anyio
async def test_joinable_handoffs_only_return_requested_unexpired_rows(b1_service) -> None:
    service, _record_service = b1_service
    first_session = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    first_handoff = await service.create_handoff(
        call_id=first_session.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 1
    assert joinable["rows"][0]["handoffId"] == first_handoff["handoffId"]

    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.accept_handoff(
        handoff_id=first_handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 0

    second_session = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    second_handoff = await service.create_handoff(
        call_id=second_session.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.handoff_service.repository.update_handoff(
        second_handoff["handoffId"],
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    joinable = await service.list_joinable_handoffs()
    assert joinable["total"] == 0


@pytest.mark.anyio
async def test_handoff_joinable_timestamps_serialize_with_utc_offset(b1_service) -> None:
    service, _record_service = b1_service
    session = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=session.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    requested_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    expires_at = requested_at + timedelta(seconds=90)
    await service.handoff_service.repository.update_handoff(
        handoff["handoffId"],
        requested_at=requested_at,
        expires_at=expires_at,
    )

    joinable = await service.list_joinable_handoffs()
    encoded = jsonable_encoder(HandoffListOut.model_validate(joinable), by_alias=True)

    assert encoded["rows"][0]["requestedAt"].endswith(("Z", "+00:00"))
    assert encoded["rows"][0]["expiresAt"].endswith(("Z", "+00:00"))


@pytest.mark.anyio
async def test_handoff_accept_connect_and_complete_flow(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )

    accepted = await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    assert accepted["handoff"]["status"] == "accepted"
    assert accepted["handoff"]["humanAgentIdentity"] == "agent-debug-001"
    assert accepted["seatToken"]["callId"] == result.call_id
    assert accepted["seatToken"]["roomName"] == result.room_name
    assert accepted["seatToken"]["participantIdentity"].startswith("human-agent-handoff_")
    assert accepted["seatToken"]["participantToken"].startswith("handoff-token-for-")

    connected = await service.mark_handoff_connected(handoff["handoffId"])
    assert connected["status"] == "connected"
    assert connected["connectedAt"] is not None

    completed = await service.complete_handoff(
        handoff_id=handoff["handoffId"],
        reason="agent_completed",
    )
    assert completed["status"] == "completed"
    assert completed["endReason"] == "agent_completed"
    assert completed["endedAt"] is not None

    history = await service.list_handoffs(result.call_id)
    assert history["total"] == 1
    assert history["rows"][0]["status"] == "completed"

    assert service.orchestrator.registry.get(result.call_id).status == CallSessionStatus.COMPLETED
    record_service.repository.db.expire_all()
    record = await record_service.get_record(result.call_id)
    assert record is not None
    assert record.status == CallSessionStatus.COMPLETED.value
    assert record.end_reason == "agent_completed"
    assert b1_service.room_manager.deleted_rooms == [result.room_name]

    await b1_service.flush_events()
    event_types = [event.event_type for event in await record_service.list_events(result.call_id)]
    assert "handoff_accepted" in event_types
    assert "handoff_connected" in event_types
    assert "handoff_completed" in event_types
    assert "session_completed" in event_types
    assert event_types.index("handoff_completed") < event_types.index("session_completed")


@pytest.mark.anyio
async def test_handoff_accept_requires_existing_agent_status(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    with pytest.raises(CustomException) as missing_agent_exc:
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
    assert missing_agent_exc.value.status_code == 409

    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "requested"


@pytest.mark.anyio
async def test_handoff_accept_requires_available_agent_and_marks_busy(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )

    offline = await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="offline",
    )
    assert offline["status"] == "offline"

    with pytest.raises(CustomException) as exc_info:
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
    assert exc_info.value.status_code == 409

    online = await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    assert online["status"] == "online"

    accepted = await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    assert accepted["handoff"]["status"] == "accepted"

    busy = await service.get_handoff_agent_status("agent-debug-001")
    assert busy["status"] == "busy"
    assert busy["activeHandoffId"] == handoff["handoffId"]


@pytest.mark.anyio
async def test_handoff_agent_status_expires_stale_online_presence(b1_service) -> None:
    service, _record_service = b1_service
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    stale_seen_at = datetime.now(timezone.utc) - timedelta(days=1)
    await service.handoff_service.repository.upsert_handoff_agent(
        agent_identity="agent-debug-001",
        skill_group="default",
        status="online",
        active_handoff_id=None,
        last_seen_at=stale_seen_at,
        status_updated_at=stale_seen_at,
    )

    agent = await service.get_handoff_agent_status("agent-debug-001")

    assert agent["status"] == "offline"
    assert agent["activeHandoffId"] is None
    assert _as_utc(agent["lastSeenAt"]) == stale_seen_at


@pytest.mark.anyio
async def test_handoff_agent_status_refreshes_online_presence(b1_service) -> None:
    service, _record_service = b1_service
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    previous_seen_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    await service.handoff_service.repository.upsert_handoff_agent(
        agent_identity="agent-debug-001",
        skill_group="default",
        status="online",
        active_handoff_id=None,
        last_seen_at=previous_seen_at,
        status_updated_at=previous_seen_at,
    )

    agent = await service.get_handoff_agent_status("agent-debug-001")

    assert agent["status"] == "online"
    assert _as_utc(agent["lastSeenAt"]) > previous_seen_at


@pytest.mark.anyio
async def test_handoff_accept_rejects_stale_online_agent(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    stale_seen_at = datetime.now(timezone.utc) - timedelta(days=1)
    await service.handoff_service.repository.upsert_handoff_agent(
        agent_identity="agent-debug-001",
        skill_group="default",
        status="online",
        active_handoff_id=None,
        last_seen_at=stale_seen_at,
        status_updated_at=stale_seen_at,
    )

    with pytest.raises(CustomException) as exc_info:
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )

    assert exc_info.value.status_code == 409
    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "requested"
    agent = await service.get_handoff_agent_status("agent-debug-001")
    assert agent["status"] == "offline"
    assert agent["activeHandoffId"] is None


@pytest.mark.anyio
async def test_handoff_accept_rejects_request_too_close_to_expiry(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.handoff_service.repository.update_handoff(
        handoff["handoffId"],
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=1),
    )

    with pytest.raises(CustomException) as exc_info:
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )

    assert exc_info.value.status_code == 409
    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "requested"
    agent = await service.get_handoff_agent_status("agent-debug-001")
    assert agent["status"] == "online"
    assert agent["activeHandoffId"] is None


@pytest.mark.anyio
async def test_handoff_complete_releases_busy_agent(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    await service.mark_handoff_connected(handoff["handoffId"])

    await service.complete_handoff(
        handoff_id=handoff["handoffId"],
        reason="agent_completed",
    )

    agent = await service.get_handoff_agent_status("agent-debug-001")
    assert agent["status"] == "online"
    assert agent["activeHandoffId"] is None


@pytest.mark.anyio
async def test_handoff_accept_token_failure_releases_busy_agent(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )

    def fail_issue_handoff_token(*_args, **_kwargs):
        raise AiCallError(
            error_id="handoff_token_failed",
            msg="坐席令牌签发失败",
            status_code=503,
        )

    b1_service.room_manager.issue_handoff_token = fail_issue_handoff_token

    with pytest.raises(CustomException) as exc_info:
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )

    assert exc_info.value.status_code == 503
    agent = await service.get_handoff_agent_status("agent-debug-001")
    assert agent["status"] == "online"
    assert agent["activeHandoffId"] is None
    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "failed"
    assert history["rows"][0]["failureStage"] == "token_issue"


@pytest.mark.anyio
async def test_end_session_finalizes_active_handoff(b1_service) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    await service.mark_handoff_connected(handoff["handoffId"])
    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    presence.active_call_id = result.call_id
    presence.console_session_id = "8ed3e232-907f-49cc-b365-6a9cc5c9aa0a"
    await service.handoff_service.repository.db.flush()

    await service.end_session(result.call_id)

    assert await service.get_current_handoff(result.call_id) is None
    history = await service.list_handoffs(result.call_id)
    assert history["total"] == 1
    assert history["rows"][0]["handoffId"] == handoff["handoffId"]
    assert history["rows"][0]["status"] == "completed"
    assert history["rows"][0]["endReason"] == "web_user_end"
    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    assert presence.status == "wrap_up_quick"
    assert presence.active_handoff_id == handoff["handoffId"]
    assert presence.active_call_id == result.call_id


@pytest.mark.anyio
async def test_end_session_keeps_connected_reconnecting_handoff_in_wrap_up(
    b1_service,
) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    await service.mark_handoff_connected(handoff["handoffId"])
    handoff_row = await service.handoff_service.repository.get_handoff_by_id(
        handoff["handoffId"]
    )
    handoff_row.status = "reconnecting"
    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    presence.status = "reconnecting"
    presence.active_call_id = result.call_id
    presence.console_session_id = "8ed3e232-907f-49cc-b365-6a9cc5c9aa0a"
    await service.handoff_service.repository.db.flush()

    await service.end_session(result.call_id, end_reason="remote_hangup")

    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "completed"
    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    assert presence.status == "wrap_up_quick"
    assert presence.active_handoff_id == handoff["handoffId"]
    assert presence.active_call_id == result.call_id


@pytest.mark.anyio
async def test_unconnected_handoff_cancel_releases_console_agent_as_available(
    b1_service,
) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.set_handoff_agent_status(
        human_agent_identity="agent-debug-001",
        status="online",
    )
    await service.accept_handoff(
        handoff_id=handoff["handoffId"],
        human_agent_identity="agent-debug-001",
    )
    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    presence.active_call_id = result.call_id
    presence.console_session_id = "8ed3e232-907f-49cc-b365-6a9cc5c9aa0a"
    await service.handoff_service.repository.db.flush()

    await service.cancel_handoff(
        handoff_id=handoff["handoffId"],
        reason="customer_hangup",
    )

    presence = await service.handoff_service.repository.get_handoff_agent(
        "agent-debug-001"
    )
    assert presence.status == "available"
    assert presence.active_handoff_id is None
    assert presence.active_call_id is None


@pytest.mark.anyio
async def test_handoff_lazy_expire_records_expired_event(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    handoff = await service.create_handoff(
        call_id=result.call_id,
        source="operator",
        reason="customer_request",
        request_message=None,
    )
    await service.handoff_service.repository.update_handoff(
        handoff["handoffId"],
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    assert await service.get_current_handoff(result.call_id) is None
    history = await service.list_handoffs(result.call_id)
    assert history["rows"][0]["status"] == "expired"

    await b1_service.flush_events()
    event_types = [event.event_type for event in await record_service.list_events(result.call_id)]
    assert "handoff_expired" in event_types


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("reason", "expected_file", "expected_text"),
    [
        (
            "no_online_agent",
            "handoff-no-online-agent.wav",
            "当前暂无人工坐席在线，我先为您记录需求，稍后安排工作人员联系您。",
        ),
        (
            "handoff_timeout",
            "handoff-busy-timeout.wav",
            "当前人工坐席繁忙，暂未接通，我先为您记录需求。",
        ),
        (
            "handoff_service_unavailable",
            "handoff-service-unavailable.wav",
            "人工转接服务暂时不可用，我先为您记录需求。",
        ),
    ],
)
async def test_handoff_exception_prompt_is_selected_by_reason(
    b1_service,
    tmp_path,
    reason: str,
    expected_file: str,
    expected_text: str,
) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        system_prompt_player=prompt_player,
        unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
        no_online_agent_prompt_audio_path=tmp_path / "handoff-no-online-agent.wav",
        no_online_agent_prompt_text=(
            "当前暂无人工坐席在线，我先为您记录需求，稍后安排工作人员联系您。"
        ),
        busy_timeout_prompt_audio_path=tmp_path / "handoff-busy-timeout.wav",
        busy_timeout_prompt_text="当前人工坐席繁忙，暂未接通，我先为您记录需求。",
        service_unavailable_prompt_audio_path=(
            tmp_path / "handoff-service-unavailable.wav"
        ),
        service_unavailable_prompt_text="人工转接服务暂时不可用，我先为您记录需求。",
    )

    await manager._play_unavailable_prompt(
        call_id=result.call_id,
        room_name=result.room_name,
        handoff_id="handoff-prompt-selection",
        handoff_status="failed",
        call_end_reason=reason,
    )

    assert prompt_player.played == [
        (result.call_id, result.room_name, str(tmp_path / expected_file))
    ]
    started = next(
        event
        for event in service.orchestrator.event_store.list_all(result.call_id)
        if event.type == "handoff_unavailable_prompt_started"
    )
    assert started.payload["promptText"] == expected_text


@pytest.mark.anyio
async def test_handoff_busy_waiting_prompt_is_selected(
    b1_service,
    tmp_path,
) -> None:
    service, _record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        system_prompt_player=prompt_player,
        timeout_seconds=60,
        waiting_prompt_audio_path=tmp_path / "handoff-waiting.wav",
        busy_waiting_prompt_audio_path=tmp_path / "handoff-busy-waiting.wav",
        busy_waiting_prompt_text="当前人工坐席繁忙，正在为您排队转接，请稍候。",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.create_handoff(
            call_id=result.call_id,
            source="customer",
            reason="customer_request",
            request_message="转人工",
            waiting_prompt_kind="busy",
        )
        await wait_until(lambda: len(prompt_player.played) == 1)

        assert prompt_player.played == [
            (
                result.call_id,
                result.room_name,
                str(tmp_path / "handoff-busy-waiting.wav"),
            )
        ]
        started = next(
            event
            for event in service.orchestrator.event_store.list_all(result.call_id)
            if event.type == "handoff_prompt_started"
        )
        assert started.payload["promptText"] == (
            "当前人工坐席繁忙，正在为您排队转接，请稍候。"
        )
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_timeout_plays_unavailable_prompt_before_auto_end(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=1,
        unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 1
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        await wait_until(
            lambda: (
                service.orchestrator.registry.get(result.call_id).status
                == CallSessionStatus.COMPLETED
            ),
            attempts=50,
            delay_seconds=0.05,
        )
        record = None
        for _ in range(50):
            record_service.repository.db.expire_all()
            record = await record_service.get_record(result.call_id)
            if record is not None and record.status == "completed":
                break
            await asyncio.sleep(0.05)
        history = await service.list_handoffs(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "handoff_timeout"
        assert history["rows"][0]["handoffId"] == handoff["handoffId"]
        assert history["rows"][0]["status"] == "expired"
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-unavailable.wav"))
        ]
        assert b1_service.room_manager.deleted_rooms == [result.room_name]

        await b1_service.flush_events()
        events = await record_service.list_events(result.call_id)
        event_types = [event.event_type for event in events]
        assert "handoff_expired" in event_types
        assert "handoff_unavailable_prompt_started" in event_types
        assert "handoff_unavailable_prompt_done" in event_types
        assert event_types.index("handoff_unavailable_prompt_done") < event_types.index(
            "handoff_auto_ended"
        )
        assert "handoff_auto_ended" in event_types
        expected_prompt_text = "当前暂时没有人工接入，我先帮您记录需求，稍后安排顾问联系您。"
        started_event = next(
            event for event in events if event.event_type == "handoff_unavailable_prompt_started"
        )
        done_event = next(
            event for event in events if event.event_type == "handoff_unavailable_prompt_done"
        )
        assert started_event.payload["promptText"] == expected_prompt_text
        assert done_event.payload["promptText"] == expected_prompt_text
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_timeout_closes_room_by_name_when_runtime_session_is_missing(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=1,
        unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 1
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        service.orchestrator.registry._sessions.pop(result.call_id)

        await wait_until(
            lambda: b1_service.room_manager.deleted_rooms == [result.room_name],
            attempts=50,
            delay_seconds=0.05,
        )
        await wait_until(
            lambda: "handoff_auto_ended"
            in [
                event.type
                for event in service.orchestrator.event_store.list_all(result.call_id)
            ],
            attempts=50,
            delay_seconds=0.05,
        )
        await manager.shutdown()
        await b1_service.flush_events()

        record_service.repository.db.expire_all()
        record = await record_service.get_record(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "handoff_timeout"
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-unavailable.wav"))
        ]

        event_types = [
            event.type for event in service.orchestrator.event_store.list_all(result.call_id)
        ]
        assert "handoff_runtime_close_fallback" in event_types
        assert "handoff_auto_ended" in event_types
        assert "session_completed" in event_types
        assert event_types.index("handoff_runtime_close_fallback") < event_types.index(
            "handoff_auto_ended"
        )
        session_completed = next(
            event
            for event in service.orchestrator.event_store.list_all(result.call_id)
            if event.type == "session_completed"
        )
        assert session_completed.payload == {"endReason": "handoff_timeout"}
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_timeout_does_not_mark_auto_ended_when_room_fallback_fails(
    b1_service,
    monkeypatch,
) -> None:
    service, record_service = b1_service
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        timeout_seconds=1,
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 1

    async def fail_delete_room(_room_name: str) -> None:
        raise AiCallError(
            error_id="room_delete_failed",
            msg="LiveKit Room 删除失败",
        )

    monkeypatch.setattr(b1_service.room_manager, "delete_room", fail_delete_room)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        service.orchestrator.registry._sessions.pop(result.call_id)

        await wait_until(
            lambda: "handoff_auto_end_runtime_failed"
            in [
                event.type
                for event in service.orchestrator.event_store.list_all(result.call_id)
            ],
            attempts=50,
            delay_seconds=0.05,
        )
        record_service.repository.db.expire_all()
        record = await record_service.get_record(result.call_id)

        assert record is not None
        assert record.status != "completed"

        event_types = [
            event.type for event in service.orchestrator.event_store.list_all(result.call_id)
        ]
        assert "handoff_auto_ended" not in event_types
        await b1_service.flush_events()
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_connected_cancels_timeout_auto_end(
    b1_service,
) -> None:
    service, _record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=1,
    )
    service.handoff_exception_manager = manager
    service.handoff_service.request_timeout_seconds = 4
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        await service.set_handoff_agent_status(
            human_agent_identity="agent-debug-001",
            status="online",
        )
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        connected = await service.mark_handoff_connected(handoff["handoffId"])

        await asyncio.sleep(4.2)
        assert connected["status"] == "connected"
        assert (
            service.orchestrator.registry.get(result.call_id).status != CallSessionStatus.COMPLETED
        )
        assert prompt_player.played == []
        assert b1_service.room_manager.deleted_rooms == []
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_waiting_tone_stops_when_agent_connected(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = BlockingSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        waiting_tone_enabled=True,
        waiting_tone_audio_path=tmp_path / "handoff-ringback.wav",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        await wait_until(lambda: prompt_player.started.is_set())
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-ringback.wav"))
        ]

        await service.set_handoff_agent_status(
            human_agent_identity="agent-debug-001",
            status="online",
        )
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        await service.mark_handoff_connected(handoff["handoffId"])
        await wait_until(lambda: prompt_player.cancelled.is_set())

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_waiting_tone_started" in event_types
        assert "handoff_waiting_tone_stopped" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_waiting_tone_owner_observes_cross_process_connected_state(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = BlockingSystemPromptPlayer()
    owner_manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        waiting_tone_enabled=True,
        waiting_tone_audio_path=tmp_path / "handoff-ringback.wav",
    )
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        await service.handoff_service.repository.db.commit()
        handoff_row = await service.handoff_service.repository.get_handoff_by_id(
            handoff["handoffId"]
        )
        owner_manager.start_waiting_tone(handoff_row)
        await wait_until(lambda: prompt_player.started.is_set())

        async with b1_service.session_maker() as other_process_db:
            async with other_process_db.begin():
                other_process_repository = AiCallRecordRepository(other_process_db)
                other_process_handoff = (
                    await other_process_repository.get_handoff_by_id(handoff["handoffId"])
                )
                other_process_handoff.status = "connected"
                other_process_handoff.connected_at = datetime.now(timezone.utc)

        await wait_until(
            lambda: prompt_player.cancelled.is_set(),
            attempts=80,
            delay_seconds=0.05,
        )
        await wait_until(
            lambda: any(
                event.type == "handoff_waiting_tone_stopped"
                for event in service.orchestrator.event_store.list(result.call_id)
            ),
            attempts=80,
            delay_seconds=0.05,
        )
        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_waiting_tone_stopped" in event_types
    finally:
        await owner_manager.shutdown()


@pytest.mark.anyio
async def test_handoff_prompt_plays_before_waiting_tone_and_stops_when_agent_connected(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = SecondPromptBlockingSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        waiting_tone_enabled=True,
        waiting_prompt_audio_path=tmp_path / "handoff-waiting.wav",
        waiting_tone_audio_path=tmp_path / "handoff-ringback.wav",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        await wait_until(lambda: prompt_player.second_started.is_set())
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-waiting.wav")),
            (result.call_id, result.room_name, str(tmp_path / "handoff-ringback.wav")),
        ]

        await service.set_handoff_agent_status(
            human_agent_identity="agent-debug-001",
            status="online",
        )
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        await service.mark_handoff_connected(handoff["handoffId"])
        await wait_until(lambda: prompt_player.cancelled.is_set())

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_prompt_started" in event_types
        assert "handoff_prompt_done" in event_types
        assert "handoff_waiting_tone_started" in event_types
        assert event_types.index("handoff_prompt_done") < event_types.index(
            "handoff_waiting_tone_started"
        )
        assert "handoff_waiting_tone_stopped" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_fail_plays_unavailable_prompt_before_auto_end(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        failed = await service.fail_handoff(
            handoff_id=handoff["handoffId"],
            failure_stage="agent_join",
            failure_message="坐席加入 Room 失败",
        )
        assert failed["status"] == "failed"

        await wait_until(
            lambda: any(
                event.type == "handoff_auto_ended"
                for event in service.orchestrator.event_store.list(result.call_id)
            ),
            attempts=40,
            delay_seconds=0.05,
        )
        record_service.repository.db.expire_all()
        record = await record_service.get_record(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "handoff_failed"
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-unavailable.wav"))
        ]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_failed" in event_types
        assert "handoff_unavailable_prompt_started" in event_types
        assert "handoff_unavailable_prompt_done" in event_types
        assert event_types.index("handoff_unavailable_prompt_done") < event_types.index(
            "handoff_auto_ended"
        )
        assert "handoff_auto_ended" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_handoff_cancel_plays_unavailable_prompt_before_auto_end(
    b1_service,
    tmp_path,
) -> None:
    service, record_service = b1_service
    prompt_player = FakeSystemPromptPlayer()
    manager = AiCallHandoffExceptionManager(
        orchestrator=service.orchestrator,
        session_factory=b1_service.session_maker,
        recording_service_factory=lambda _repository: None,
        system_prompt_player=prompt_player,
        timeout_seconds=30,
        unavailable_prompt_audio_path=tmp_path / "handoff-unavailable.wav",
    )
    service.handoff_exception_manager = manager
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )

        canceled = await service.cancel_handoff(
            handoff_id=handoff["handoffId"],
            reason="operator_cancelled",
        )
        assert canceled["status"] == "canceled"

        await wait_until(
            lambda: any(
                event.type == "handoff_auto_ended"
                for event in service.orchestrator.event_store.list(result.call_id)
            ),
            attempts=40,
            delay_seconds=0.05,
        )
        record_service.repository.db.expire_all()
        record = await record_service.get_record(result.call_id)

        assert record is not None
        assert record.status == "completed"
        assert record.end_reason == "operator_cancelled"
        assert prompt_player.played == [
            (result.call_id, result.room_name, str(tmp_path / "handoff-unavailable.wav"))
        ]

        await b1_service.flush_events()
        event_types = [
            event.event_type for event in await record_service.list_events(result.call_id)
        ]
        assert "handoff_canceled" in event_types
        assert "handoff_unavailable_prompt_started" in event_types
        assert "handoff_unavailable_prompt_done" in event_types
        assert event_types.index("handoff_unavailable_prompt_done") < event_types.index(
            "handoff_auto_ended"
        )
        assert "handoff_auto_ended" in event_types
    finally:
        await manager.shutdown()


@pytest.mark.anyio
async def test_append_event_is_idempotent_by_event_id(b1_service) -> None:
    _service, record_service = b1_service
    event_time = datetime.now(timezone.utc)

    first = await record_service.repository.append_event(
        event_id="evt_duplicate",
        call_id="call_duplicate",
        event_type="provider_event",
        source="provider",
        event_time=event_time,
        payload_json='{"sequence": 1}',
    )
    second = await record_service.repository.append_event(
        event_id="evt_duplicate",
        call_id="call_duplicate",
        event_type="provider_event_changed",
        source="provider",
        event_time=event_time,
        payload_json='{"sequence": 2}',
    )

    rows = await record_service.list_events("call_duplicate")
    assert len(rows) == 1
    assert first.id == second.id == rows[0].id
    assert rows[0].event_type == "provider_event"
    assert rows[0].payload == {"sequence": 1}


def test_record_service_persists_promoted_browser_interrupt_candidate() -> None:
    event = InMemoryEventStore().append(
        call_id="call_browser_promoted",
        type="browser_interrupt_candidate_promoted",
        source="agent",
    )

    assert AiCallRecordService.should_persist_event(event) is True


@pytest.mark.parametrize(
    "event_type",
    [
        "sip_interrupt_candidate",
        "sip_impulse_noise_ignored",
        "sip_interrupt_candidate_confirmed",
        "sip_interrupt_candidate_expired",
        "sip_interrupt_confirmed",
        "sip_interrupt_rejected",
        "sip_ai_playback_echo_deferred",
        "sip_provider_speech_started_deferred",
        "sip_pre_stop_deferred",
        "sip_pre_stop",
        "sip_recovery_started",
        "sip_vad_shadow_started",
        "sip_vad_shadow_ended",
        "sip_vad_shadow_error",
        "browser_pre_stop_requested",
        "browser_pre_stop_completed",
        "browser_pre_stop_confirmed",
        "browser_pre_stop_expired",
        "browser_pre_stop_rejected_echo",
        "browser_pre_stop_skipped",
        "browser_audio_hold_requested",
        "browser_audio_hold_completed",
        "browser_audio_hold_confirmed",
        "browser_audio_hold_expired",
        "browser_audio_hold_rejected_echo",
    ],
)
def test_record_service_persists_interrupt_observation_diagnostics(event_type: str) -> None:
    event = InMemoryEventStore().append(
        call_id="call_browser_pre_stop_persist",
        type=event_type,
        source="agent",
    )

    assert AiCallRecordService.should_persist_event(event) is True


def test_record_service_persists_browser_speech_segment() -> None:
    event = InMemoryEventStore().append(
        call_id="call_browser_segment",
        type="browser_user_speech_segment",
        source="browser",
        payload={
            "segmentId": "browser-seg-1",
            "phase": "updated",
            "durationMs": 440,
            "snrDb": 15.0,
            "hotFrameCount": 11,
        },
    )

    assert AiCallRecordService.should_persist_event(event) is True


def test_record_service_persists_browser_audio_input_diagnostics() -> None:
    event = InMemoryEventStore().append(
        call_id="call_browser_audio_input",
        type="browser_audio_input_diagnostics",
        source="browser",
        payload={
            "diagnosticsVersion": "browser-audio-input-v1",
            "trackSettings": {"echoCancellation": True},
        },
    )

    assert AiCallRecordService.should_persist_event(event) is True


@pytest.mark.anyio
async def test_interrupt_summary_reports_normal_interrupt_metrics(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 22, 4, 0, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="browser_user_speech_started",
        event_time=base,
        source="browser",
        payload={"reportedAt": base.isoformat()},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base + timedelta(milliseconds=1),
        payload={"source": "browser", "reason": "browser_user_speech_started_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_pending",
        event_time=base + timedelta(milliseconds=2),
        payload={"source": "browser", "suppressMs": 600},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="stale_audio_dropped",
        event_time=base + timedelta(milliseconds=20),
        payload={"reason": "interrupt_pending", "deltaBytes": 15360},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="stale_audio_dropped",
        event_time=base + timedelta(milliseconds=40),
        payload={"reason": "interrupt_pending", "deltaBytes": 3840},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="user_speech_started",
        event_time=base + timedelta(milliseconds=100),
        source="provider",
        payload={"audio_start_ms": 1000},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_audio_stop_requested",
        event_time=base + timedelta(milliseconds=105),
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="playout_queue_flushed",
        event_time=base + timedelta(milliseconds=110),
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_audio_stop_completed",
        event_time=base + timedelta(milliseconds=125),
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_confirmed",
        event_time=base + timedelta(milliseconds=230),
        payload={"reason": "user_speech_started_during_ai_audio"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["callId"] == result.call_id
    assert summary["interruptCandidateCount"] == 1
    assert summary["interruptConfirmedCount"] == 1
    assert summary["candidateNotConfirmedCount"] == 0
    assert summary["browserToProviderMs"] == 100
    assert summary["providerToConfirmedMs"] == 130
    assert summary["browserToConfirmedMs"] == 230
    assert summary["staleAudioDroppedCount"] == 2
    assert summary["staleAudioDroppedBytes"] == 19200
    assert summary["playoutFlushCount"] == 1
    assert summary["duplicateEndRequest"] is False
    assert summary["agentStartFailed"] is False
    assert summary["verdict"] == "normal"
    assert summary["issues"] == []


@pytest.mark.anyio
async def test_interrupt_summary_flags_candidate_without_confirm(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 22, 4, 1, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="browser_user_speech_started",
        event_time=base,
        source="browser",
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base + timedelta(milliseconds=1),
        payload={"source": "browser"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["interruptCandidateCount"] == 1
    assert summary["interruptConfirmedCount"] == 0
    assert summary["candidateNotConfirmedCount"] == 1
    assert summary["verdict"] == "candidate_not_confirmed"
    assert summary["issues"] == ["candidate_not_confirmed"]


@pytest.mark.anyio
async def test_interrupt_summary_counts_sip_candidate_confirmation(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 25, 11, 23, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base,
        payload={"source": "sip", "reason": "sip_uplink_speech_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="sip_interrupt_candidate",
        event_time=base + timedelta(milliseconds=1),
        payload={"reason": "sip_uplink_speech_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="sip_interrupt_candidate_confirmed",
        event_time=base + timedelta(milliseconds=300),
        payload={"confirmedBy": "transcript", "reason": "sip_uplink_speech_during_ai_audio"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["interruptCandidateCount"] == 1
    assert summary["interruptConfirmedCount"] == 1
    assert summary["candidateNotConfirmedCount"] == 0
    assert summary["verdict"] == "normal"
    assert summary["issues"] == []


@pytest.mark.anyio
async def test_interrupt_summary_counts_sip_p1_confirmation(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 30, 11, 23, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base,
        payload={"source": "sip", "reason": "sip_uplink_speech_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="sip_interrupt_confirmed",
        event_time=base + timedelta(milliseconds=300),
        payload={"decision": "confirmed", "reason": "sip_uplink_speech_during_ai_audio"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["interruptCandidateCount"] == 1
    assert summary["interruptConfirmedCount"] == 1
    assert summary["candidateNotConfirmedCount"] == 0
    assert summary["verdict"] == "normal"
    assert summary["issues"] == []


@pytest.mark.anyio
async def test_interrupt_summary_deduplicates_sip_and_generic_confirmation(
    b1_service,
) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 25, 11, 51, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base,
        payload={"source": "sip", "reason": "sip_uplink_speech_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="sip_interrupt_candidate",
        event_time=base + timedelta(milliseconds=1),
        payload={"reason": "sip_uplink_speech_during_ai_audio"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="user_speech_started",
        event_time=base + timedelta(milliseconds=500),
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="sip_interrupt_candidate_confirmed",
        event_time=base + timedelta(milliseconds=501),
        payload={"confirmedBy": "provider_speech_started"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_confirmed",
        event_time=base + timedelta(milliseconds=504),
        payload={"reason": "user_speech_started_during_ai_audio"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["interruptCandidateCount"] == 1
    assert summary["interruptConfirmedCount"] == 1
    assert summary["candidateNotConfirmedCount"] == 0
    assert summary["providerToConfirmedMs"] == 1
    assert summary["verdict"] == "normal"
    assert summary["issues"] == []


@pytest.mark.anyio
async def test_interrupt_summary_flags_slow_confirm(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 22, 4, 2, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="browser_user_speech_started",
        event_time=base,
        source="browser",
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_candidate",
        event_time=base + timedelta(milliseconds=1),
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="user_speech_started",
        event_time=base + timedelta(milliseconds=100),
        source="provider",
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="interrupt_confirmed",
        event_time=base + timedelta(milliseconds=1100),
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["browserToProviderMs"] == 100
    assert summary["providerToConfirmedMs"] == 1000
    assert summary["browserToConfirmedMs"] == 1100
    assert summary["verdict"] == "slow_confirm"
    assert summary["issues"] == ["slow_confirm"]


@pytest.mark.anyio
async def test_interrupt_summary_flags_agent_start_failure(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(voice=None, prompt=None, business_id=None)
    await b1_service.flush_events()
    base = datetime(2026, 6, 22, 4, 3, tzinfo=timezone.utc)

    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="agent_start_failed",
        event_time=base,
        payload={"errorType": "ImportError", "errorMessage": "python-socks is required"},
    )
    await append_record_event(
        record_service,
        call_id=result.call_id,
        event_type="session_failed",
        event_time=base + timedelta(milliseconds=1),
        payload={"endReason": "agent_start_failed"},
    )

    summary = await service.get_record_interrupt_summary(result.call_id)

    assert summary["agentStartFailed"] is True
    assert summary["verdict"] == "session_failed"
    assert summary["issues"] == ["agent_start_failed", "session_failed"]


def test_interrupt_summary_api_returns_camel_case_response() -> None:
    class FakeInterruptSummaryService:
        async def require_record_for_tenant(self, **query) -> None:
            assert query == {
                "tenant_id": "tenant-a",
                "call_id": "call_interrupt",
            }

        async def get_record_interrupt_summary(self, call_id: str) -> dict:
            return {
                "callId": call_id,
                "interruptCandidateCount": 1,
                "interruptConfirmedCount": 1,
                "candidateNotConfirmedCount": 0,
                "browserToProviderMs": 120,
                "providerToConfirmedMs": 180,
                "browserToConfirmedMs": 300,
                "staleAudioDroppedCount": 2,
                "staleAudioDroppedBytes": 6400,
                "playoutFlushCount": 1,
                "duplicateEndRequest": False,
                "agentStartFailed": False,
                "verdict": "normal",
                "issues": [],
            }

    app = FastAPI()
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_ai_call_service] = lambda: FakeInterruptSummaryService()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=1),
    )

    with TestClient(app) as client:
        response = client.get("/ai-call/records/call_interrupt/interrupt-summary")

    assert response.status_code == 200
    body = response.json()
    assert body["code"] == 200
    assert body["msg"] == "查询成功"
    assert body["data"] == {
        "callId": "call_interrupt",
        "interruptCandidateCount": 1,
        "interruptConfirmedCount": 1,
        "candidateNotConfirmedCount": 0,
        "browserToProviderMs": 120,
        "providerToConfirmedMs": 180,
        "browserToConfirmedMs": 300,
        "staleAudioDroppedCount": 2,
        "staleAudioDroppedBytes": 6400,
        "playoutFlushCount": 1,
        "duplicateEndRequest": False,
        "agentStartFailed": False,
        "verdict": "normal",
        "issues": [],
    }


@pytest.mark.anyio
async def test_mirror_runtime_events_only_persists_new_events(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()

    runtime_events = service.orchestrator.event_store.list_all(call_id=result.call_id)
    assert await record_service.mirror_runtime_events(runtime_events) == []

    new_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"sequence": 1},
    )
    mirrored = await record_service.mirror_runtime_events(
        service.orchestrator.event_store.list_all(call_id=result.call_id)
    )

    assert [event.event_id for event in mirrored] == [new_event.event_id]
    rows = await record_service.list_events(result.call_id)
    assert [event.event_id for event in rows].count(new_event.event_id) == 1


@pytest.mark.anyio
async def test_live_event_query_does_not_persist_runtime_events(b1_service) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    new_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"message": "provider failed"},
    )

    live_events = await service.list_events(
        call_id=result.call_id,
        limit=1000,
        after_event_id=None,
    )
    persisted_rows = await record_service.list_events(
        result.call_id,
        event_type="model_error",
    )

    assert [event.event_id for event in live_events.rows].count(new_event.event_id) == 1
    assert persisted_rows == []


@pytest.mark.anyio
async def test_mirror_runtime_events_filters_high_frequency_events_after_first_1000(
    b1_service,
) -> None:
    service, record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    await b1_service.flush_events()
    b1_service.event_worker.detach_all()
    for index in range(1005):
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="model_audio_delta",
            source="provider",
            payload={"deltaBytes": index},
        )
    key_event = service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"message": "provider failed"},
    )

    await record_service.mirror_runtime_events(
        service.orchestrator.event_store.list_all(call_id=result.call_id)
    )

    persisted_key_events = await record_service.list_events(
        result.call_id,
        event_type="model_error",
    )
    persisted_audio_events = await record_service.list_events(
        result.call_id,
        event_type="model_audio_delta",
    )
    assert [event.event_id for event in persisted_key_events] == [key_event.event_id]
    assert persisted_audio_events == []


@pytest.mark.anyio
async def test_event_persistence_worker_flushes_events_without_service_mirror(
    b1_service,
) -> None:
    service, record_service = b1_service

    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )

    await b1_service.flush_events()
    events = await record_service.list_events(result.call_id)
    assert [event.event_type for event in events] == [
        "session_created",
        "session_preparing",
        "room_created",
        "browser_token_issued",
        "agent_started",
        "session_ready",
    ]


@pytest.mark.anyio
async def test_event_persistence_worker_closes_record_on_model_error(
    b1_service,
) -> None:
    service, _record_service = b1_service

    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="model_error",
        source="provider",
        payload={"error": {"message": "Conversation already has an active response"}},
    )
    await b1_service.flush_events()

    async with b1_service.event_worker.session_factory() as db:
        record = await AiCallRecordRepository(db).get_record(result.call_id)
        assert record is not None
        assert record.status == "failed"
        assert record.end_reason == "model_error"
        assert record.failure_stage == "model"
        assert record.failure_message == "Conversation already has an active response"
        assert record.ended_at is not None
        assert record.duration_ms is None


@pytest.mark.anyio
async def test_event_persistence_worker_completes_connected_reconnecting_handoff(
    b1_service,
) -> None:
    service, _record_service = b1_service
    result = await service.create_web_session(
        voice=None,
        prompt=None,
        business_id=None,
    )
    now = datetime.now(timezone.utc)
    async with b1_service.session_maker.begin() as db:
        db.add(
            AiCallHandoffModel(
                id=999_999,
                tenant_id="000000",
                handoff_id="handoff-terminal-reconnect",
                call_id=result.call_id,
                room_name=result.room_name,
                scene_code="default",
                status="reconnecting",
                request_source="customer",
                human_agent_identity="agent-debug-001",
                requested_at=now,
                connected_at=now,
                reconnect_expires_at=now + timedelta(seconds=15),
            )
        )
        db.add(
            AiCallHandoffAgentModel(
                id=999_998,
                tenant_id="000000",
                agent_identity="agent-debug-001",
                skill_group="default",
                status="reconnecting",
                active_handoff_id="handoff-terminal-reconnect",
                active_call_id=result.call_id,
                console_session_id="8ed3e232-907f-49cc-b365-6a9cc5c9aa0a",
                last_seen_at=now,
                status_updated_at=now,
            )
        )

    service.orchestrator.event_store.append(
        call_id=result.call_id,
        type="session_completed",
        source="orchestrator",
        payload={"endReason": "sip_participant_left"},
    )
    await b1_service.flush_events()

    async with b1_service.session_maker() as db:
        handoff = await db.scalar(
            select(AiCallHandoffModel).where(
                AiCallHandoffModel.handoff_id == "handoff-terminal-reconnect"
            )
        )
        assert handoff is not None
        assert handoff.status == "completed"
        assert handoff.end_reason == "sip_participant_left"
        presence = await db.scalar(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.agent_identity == "agent-debug-001"
            )
        )
        assert presence is not None
        assert presence.status == "wrap_up_quick"
        assert presence.active_handoff_id == handoff.handoff_id
        assert presence.active_call_id == result.call_id


@pytest.mark.anyio
async def test_runtime_event_worker_persists_without_legacy_terminal_projection(
    b1_service,
) -> None:
    service, record_service = b1_service
    b1_service.event_worker.detach_all()
    worker = AiCallEventPersistenceWorker(
        b1_service.event_worker.session_factory,
        project_terminal_records=False,
    )
    worker.attach_event_store(service.orchestrator.event_store)
    await worker.start()
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        service.orchestrator.event_store.append(
            call_id=result.call_id,
            type="model_error",
            source="provider",
            payload={"message": "owner runtime error"},
        )
        await worker.flush_pending()

        events = await record_service.list_events(
            result.call_id,
            event_type="model_error",
        )
        async with worker.session_factory() as db:
            record = await AiCallRecordRepository(db).get_record(result.call_id)
        assert len(events) == 1
        assert record is not None
        assert record.ended_at is None
    finally:
        await worker.stop()


@pytest.mark.anyio
async def test_event_worker_queue_full_does_not_block_runtime_path(b1_service) -> None:
    service, _record_service = b1_service
    b1_service.event_worker.detach_all()
    worker = AiCallEventPersistenceWorker(
        b1_service.event_worker.session_factory,
        queue_max_size=1,
    )
    service.orchestrator.event_store.add_listener(worker.enqueue)

    first = service.orchestrator.event_store.append(
        call_id="call_queue_full",
        type="model_error",
        source="provider",
        payload={"sequence": 1},
    )
    second = service.orchestrator.event_store.append(
        call_id="call_queue_full",
        type="model_error",
        source="provider",
        payload={"sequence": 2},
    )

    assert first.event_id != second.event_id
    assert worker.queue.qsize() == 1
    assert worker.dropped_count == 1
    service.orchestrator.event_store.remove_listener(worker.enqueue)


@pytest.mark.anyio
async def test_dialogue_worker_queue_full_does_not_block_runtime_path(b1_service) -> None:
    worker = AiCallDialoguePersistenceWorker(
        b1_service.event_worker.session_factory,
        queue_max_size=1,
    )
    now = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    first = DialogueSegmentSnapshot(
        call_id="call_dialogue_queue_full",
        segment_no=1,
        speaker_type="customer",
        speaker_identity=None,
        source="qwen_realtime",
        source_segment_id="item_1",
        text="第一句",
        segment_status="final",
        started_at=now,
        ended_at=now + timedelta(seconds=1),
        duration_ms=1000,
    )
    second = DialogueSegmentSnapshot(
        call_id="call_dialogue_queue_full",
        segment_no=2,
        speaker_type="customer",
        speaker_identity=None,
        source="qwen_realtime",
        source_segment_id="item_2",
        text="第二句",
        segment_status="final",
        started_at=now + timedelta(seconds=2),
        ended_at=now + timedelta(seconds=3),
        duration_ms=1000,
    )

    worker.enqueue(first)
    worker.enqueue(second)

    assert worker.queue.qsize() == 1
    assert worker.dropped_count == 1


@pytest.mark.anyio
async def test_recording_closure_starts_stops_and_registers_oss(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        room_manager = FakeLiveKitRoomManager()
        fake_egress = FakeEgressManager(room_manager)
        service = AiCallService(
            build_b1_orchestrator(livekit_room_manager=room_manager),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert recording["status"] == "recording"
        assert recording["egressId"] == f"EG_{result.call_id}"

        await service.end_session(result.call_id)
        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["ossId"].isdigit()
        play_url = urlparse(completed["playUrl"])
        assert play_url.path == f"/recordings/ai-call/recordings/{result.call_id}.mp3"
        assert parse_qs(play_url.query)["X-Amz-SignedHeaders"] == ["host"]
        assert completed["durationMs"] == 1200
        assert fake_egress.started == [(result.room_name, result.call_id)]
        assert fake_egress.stopped == [f"EG_{result.call_id}"]
        assert fake_egress.stop_saw_deleted_room is False
        assert room_manager.deleted_rooms == [result.room_name]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("recording_kind", ["main", "participant"])
async def test_recording_stop_releases_sqlite_write_lock_before_egress_io(
    monkeypatch,
    tmp_path,
    recording_kind,
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'recording-stop-boundary.db'}",
        connect_args={"timeout": 0.05},
    )
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    class BlockingStopEgressManager(FakeEgressManager):
        def __init__(self) -> None:
            super().__init__()
            self.stop_started = asyncio.Event()
            self.release_stop = asyncio.Event()

        async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
            self.stop_started.set()
            await self.release_stop.wait()
            return await super().stop_egress(egress_id)

    call_id = "call_recording_stop_boundary"
    now = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    fake_egress = BlockingStopEgressManager()
    participant_identity = f"customer-{call_id}"
    if recording_kind == "participant":
        fake_egress.started_participants.append(
            (
                f"ai-call-{call_id}",
                call_id,
                "customer",
                participant_identity,
            )
        )

    try:
        async with session_maker() as db:
            db.add(
                AiCallRecordModel(
                    id=1,
                    tenant_id="000000",
                    call_id=call_id,
                    business_type=None,
                    business_id=None,
                    entry_type="web",
                    room_name=f"ai-call-{call_id}",
                    participant_identity=f"customer-{call_id}",
                    status="created",
                    started_at=now,
                )
            )
            if recording_kind == "main":
                db.add(
                    AiCallRecordingModel(
                        id=2,
                        tenant_id="000000",
                        call_id=call_id,
                        room_name=f"ai-call-{call_id}",
                        status="recording",
                        egress_id=f"EG_{call_id}",
                        object_name=f"ai-call/recordings/{call_id}.mp3",
                        started_at=now,
                    )
                )
            else:
                db.add(
                    AiCallRecordingTrackModel(
                        id=2,
                        tenant_id="000000",
                        call_id=call_id,
                        room_name=f"ai-call-{call_id}",
                        track_role="customer",
                        participant_identity=participant_identity,
                        status="recording",
                        egress_id=(
                            f"EG_{call_id}_customer_{participant_identity}"
                        ),
                        object_name=(
                            f"ai-call/recordings/tracks/{call_id}/"
                            f"customer-{participant_identity}.mp3"
                        ),
                        started_at=now,
                    )
                )
            await db.commit()

            service = AiCallRecordingService(
                AiCallRecordRepository(db),
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
                stop_session_factory=session_maker,
            )
            stop_task = asyncio.create_task(
                service.stop_for_session(tenant_id="000000", call_id=call_id)
            )
            await asyncio.wait_for(fake_egress.stop_started.wait(), timeout=0.5)

            async with session_maker() as concurrent_db:
                if recording_kind == "main":
                    recording = await concurrent_db.scalar(
                        select(AiCallRecordingModel).where(
                            AiCallRecordingModel.call_id == call_id
                        )
                    )
                else:
                    recording = await concurrent_db.scalar(
                        select(AiCallRecordingTrackModel).where(
                            AiCallRecordingTrackModel.call_id == call_id
                        )
                    )
                record = await concurrent_db.scalar(
                    select(AiCallRecordModel).where(
                        AiCallRecordModel.call_id == call_id
                    )
                )
                assert recording is not None and recording.status == "stopping"
                assert record is not None
                record.status = "concurrent_write"
                await concurrent_db.commit()

            fake_egress.release_stop.set()
            await asyncio.wait_for(stop_task, timeout=0.5)

        async with session_maker() as db:
            if recording_kind == "main":
                recording = await db.scalar(
                    select(AiCallRecordingModel).where(
                        AiCallRecordingModel.call_id == call_id
                    )
                )
            else:
                recording = await db.scalar(
                    select(AiCallRecordingTrackModel).where(
                        AiCallRecordingTrackModel.call_id == call_id
                    )
                )
            record = await db.scalar(
                select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
            )
        assert recording is not None and recording.status == "completed"
        assert record is not None and record.status == "concurrent_write"
    finally:
        fake_egress.release_stop.set()
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("recording_kind", ["main", "participant"])
async def test_isolated_recording_stop_timeout_commits_verifying_state(
    tmp_path,
    recording_kind,
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'recording-stop-timeout.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    call_id = "call_recording_stop_timeout"
    now = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)
    participant_identity = f"customer-{call_id}"
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with session_maker() as db:
            db.add(
                AiCallRecordModel(
                    id=1,
                    tenant_id="000000",
                    call_id=call_id,
                    business_type=None,
                    business_id=None,
                    entry_type="web",
                    room_name=f"ai-call-{call_id}",
                    participant_identity=participant_identity,
                    status="created",
                    started_at=now,
                )
            )
            if recording_kind == "main":
                db.add(
                    AiCallRecordingModel(
                        id=2,
                        tenant_id="000000",
                        call_id=call_id,
                        room_name=f"ai-call-{call_id}",
                        status="recording",
                        egress_id=f"EG_{call_id}",
                        object_name=f"ai-call/recordings/{call_id}.mp3",
                        started_at=now,
                    )
                )
            else:
                db.add(
                    AiCallRecordingTrackModel(
                        id=2,
                        tenant_id="000000",
                        call_id=call_id,
                        room_name=f"ai-call-{call_id}",
                        track_role="customer",
                        participant_identity=participant_identity,
                        status="recording",
                        egress_id=f"EG_{call_id}_customer_{participant_identity}",
                        object_name=(
                            f"ai-call/recordings/tracks/{call_id}/"
                            f"customer-{participant_identity}.mp3"
                        ),
                        started_at=now,
                    )
                )
            await db.commit()

            service = AiCallRecordingService(
                AiCallRecordRepository(db),
                enabled=True,
                egress_manager=TimeoutMainStopEgressManager(),
                participant_recording_enabled=True,
                stop_session_factory=session_maker,
            )
            await service.stop_for_session(tenant_id="000000", call_id=call_id)

        async with session_maker() as db:
            if recording_kind == "main":
                recording = await db.scalar(
                    select(AiCallRecordingModel).where(
                        AiCallRecordingModel.call_id == call_id
                    )
                )
            else:
                recording = await db.scalar(
                    select(AiCallRecordingTrackModel).where(
                        AiCallRecordingTrackModel.call_id == call_id
                    )
                )

        assert recording is not None
        assert recording.status == "verifying"
        assert recording.stop_requested_at is not None
        assert recording.next_verify_at is not None
        assert recording.verify_deadline_at is not None
        assert recording.last_verify_error is not None
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_participant_recording_closure_records_customer_and_ai_tracks(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert recording["tracks"] == []
        assert fake_egress.started_participants == []

        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert [track["trackRole"] for track in recording["tracks"]] == ["customer", "ai"]
        assert fake_egress.started_participants == [
            (result.room_name, result.call_id, "customer", result.participant_identity),
            (result.room_name, result.call_id, "ai", f"agent-{result.call_id}"),
        ]

        await service.end_session(result.call_id)
        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert {track["status"] for track in completed["tracks"]} == {"completed"}
        assert {track["trackRole"] for track in completed["tracks"]} == {"customer", "ai"}
        assert all(track["ossId"] and track["playUrl"] for track in completed["tracks"])
        assert fake_egress.stopped == [
            f"EG_{result.call_id}",
            f"EG_{result.call_id}_customer_{result.participant_identity}",
            f"EG_{result.call_id}_ai_agent-{result.call_id}",
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_recording_retries_until_participant_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        AiCallRecordingService,
        "_PARTICIPANT_EGRESS_RETRY_DELAYS_SECONDS",
        (0.0, 0.0),
    )
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FlakyParticipantEgressManager(failures_before_success=1)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert {track["status"] for track in recording["tracks"]} == {"recording"}
        assert (
            fake_egress.participant_start_attempts[("customer", result.participant_identity)] == 2
        )
        assert fake_egress.participant_start_attempts[("ai", f"agent-{result.call_id}")] == 2

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_handoff_connected_starts_human_agent_participant_recording(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
            handoff_service=AiCallHandoffService(repository),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        await service.set_handoff_agent_status(
            human_agent_identity="agent-debug-001",
            status="online",
        )
        accepted = await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        human_participant_identity = accepted["seatToken"]["participantIdentity"]

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert not [track for track in recording["tracks"] if track["trackRole"] == "human_agent"]

        await service.mark_handoff_connected(handoff["handoffId"])
        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        human_tracks = [
            track for track in recording["tracks"] if track["trackRole"] == "human_agent"
        ]
        assert len(human_tracks) == 1
        assert human_tracks[0]["participantIdentity"] == human_participant_identity
        assert human_tracks[0]["handoffId"] == handoff["handoffId"]

        await service.complete_handoff(
            handoff_id=handoff["handoffId"],
            reason="agent_completed",
        )
        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert {track["trackRole"] for track in completed["tracks"]} == {
            "customer",
            "ai",
            "human_agent",
        }
        assert {track["status"] for track in completed["tracks"]} == {"completed"}

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_persists_split_track_dialogue_segments(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
            dialogue_service=AiCallDialogueService(repository),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        await service.dialogue_service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id=result.call_id,
                segment_no=1,
                speaker_type="customer",
                speaker_identity=result.participant_identity,
                source="qwen_realtime",
                source_segment_id="item_realtime_1",
                text="实时识别文本",
                segment_status="final",
                started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
                ended_at=datetime(2026, 6, 16, 10, 0, 1, tzinfo=timezone.utc),
                duration_ms=1000,
            )
        )
        await service.end_session(result.call_id)
        await db.commit()

        provider = FakeOfflineAsrProvider()
        asr_service = AiCallOfflineAsrService(repository, provider=provider)
        stats = await asr_service.process_call(result.call_id)

        assert stats["jobs"] == 1
        assert stats["segments"] == 1
        assert stats["skipped"] == 1
        assert len(provider.audio_urls) == 1
        rows = await service.list_record_dialogue_segments(result.call_id)
        assert rows["total"] == 2
        assert [(row["segmentNo"], row["source"], row["speakerType"]) for row in rows["rows"]] == [
            (1, "qwen_realtime", "customer"),
            (2, "offline_asr", "customer"),
        ]
        assert rows["rows"][1]["text"] == "客户需要转人工"
        assert rows["rows"][1]["audioStartMs"] == 100

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert {job["status"] for job in recording["asrJobs"]} == {"completed"}
        assert {job["segmentCount"] for job in recording["asrJobs"]} == {1}
        assert {job["trackRole"] for job in recording["asrJobs"]} == {"customer"}

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_persists_human_agent_track_dialogue_segments(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=FakeEgressManager(),
                participant_recording_enabled=True,
            ),
            handoff_service=AiCallHandoffService(repository),
            dialogue_service=AiCallDialogueService(repository),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        handoff = await service.create_handoff(
            call_id=result.call_id,
            source="operator",
            reason="customer_request",
            request_message=None,
        )
        await service.set_handoff_agent_status(
            human_agent_identity="agent-debug-001",
            status="online",
        )
        await service.accept_handoff(
            handoff_id=handoff["handoffId"],
            human_agent_identity="agent-debug-001",
        )
        await service.mark_handoff_connected(handoff["handoffId"])
        await service.complete_handoff(
            handoff_id=handoff["handoffId"],
            reason="agent_completed",
        )
        await service.end_session(result.call_id)
        await db.commit()

        provider = FakeOfflineAsrProvider()
        asr_service = AiCallOfflineAsrService(repository, provider=provider)
        stats = await asr_service.process_call(result.call_id)

        assert stats["jobs"] == 2
        assert stats["segments"] == 2
        assert stats["skipped"] == 1
        assert len(provider.audio_urls) == 2

        rows = await service.list_record_dialogue_segments(result.call_id)
        assert rows["total"] == 2
        assert [(row["source"], row["speakerType"], row["text"]) for row in rows["rows"]] == [
            ("offline_asr", "customer", "客户需要转人工"),
            ("offline_asr", "human_agent", "您好，我帮您接入人工"),
        ]

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert {job["status"] for job in recording["asrJobs"]} == {"completed"}
        assert {job["trackRole"] for job in recording["asrJobs"]} == {
            "customer",
            "human_agent",
        }

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_offline_asr_skips_duplicate_realtime_customer_segment(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=FakeEgressManager(),
                participant_recording_enabled=True,
            ),
            dialogue_service=AiCallDialogueService(repository),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        now = datetime.now(timezone.utc)
        await service.dialogue_service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id=result.call_id,
                segment_no=1,
                speaker_type="customer",
                speaker_identity=result.participant_identity,
                source="qwen_realtime",
                source_segment_id="item_realtime_duplicate",
                text="客户需要转人工",
                segment_status="final",
                started_at=now - timedelta(seconds=10),
                ended_at=now + timedelta(seconds=10),
                duration_ms=20000,
            )
        )
        await service.end_session(result.call_id)
        await db.commit()

        provider = FakeOfflineAsrProvider()
        asr_service = AiCallOfflineAsrService(repository, provider=provider)
        stats = await asr_service.process_call(result.call_id)

        assert stats["jobs"] == 1
        assert stats["segments"] == 0
        assert stats["skipped"] == 1
        assert len(provider.audio_urls) == 1
        assert len(await repository.list_dialogue_segments(result.call_id)) == 1

        rows = await service.list_record_dialogue_segments(result.call_id)
        assert rows["total"] == 1
        assert rows["rows"][0]["source"] == "qwen_realtime"
        assert rows["rows"][0]["text"] == "客户需要转人工"

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert len(recording["asrJobs"]) == 1
        assert recording["asrJobs"][0]["trackRole"] == "customer"
        assert recording["asrJobs"][0]["segmentCount"] == 0

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_end_session_enqueues_offline_asr(b1_service) -> None:
    service = b1_service.service
    worker = FakeOfflineAsrWorker()
    configure_ai_call_offline_asr(worker)
    try:
        result = await service.create_web_session(
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)
        assert result.call_id in worker.call_ids
    finally:
        configure_ai_call_offline_asr(None)


@pytest.mark.anyio
async def test_end_session_does_not_enqueue_offline_asr_while_recording_verifies(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    worker = FakeOfflineAsrWorker()
    configure_ai_call_offline_asr(worker)
    try:
        async with session_maker() as db:
            repository = AiCallRecordRepository(db)
            record_service = AiCallRecordService(repository)
            service = AiCallService(
                build_b1_orchestrator(),
                record_service,
                recording_service=AiCallRecordingService(
                    repository,
                    enabled=True,
                    egress_manager=TimeoutMainStopEgressManager(),
                ),
            )

            result = await service.create_web_session(
                tenant_id="000000",
                voice=None,
                prompt=None,
                business_id=None,
            )
            await service.end_session(result.call_id)

            recording = await service.get_recording(
                tenant_id="000000",
                call_id=result.call_id,
            )
            assert recording is not None
            assert recording["status"] == "verifying"
            assert worker.call_ids == []
    finally:
        configure_ai_call_offline_asr(None)

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_reconcile_worker_enqueues_asr_after_verification(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 4096

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    ready_for_asr: list[str] = []
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=TimeoutMainStopEgressManager(),
            ),
        )
        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)
        await db.commit()

    def service_factory(repository: AiCallRecordRepository) -> AiCallRecordingService:
        return AiCallRecordingService(repository, enabled=True)

    worker = AiCallRecordingReconcileWorker(
        session_maker,
        service_factory,
        on_call_ready_for_asr=ready_for_asr.append,
    )
    ready_call_ids = await worker.flush_once()

    assert ready_call_ids == {result.call_id}
    assert ready_for_asr == [result.call_id]

    async with session_maker() as db:
        service = AiCallRecordingService(AiCallRecordRepository(db), enabled=True)
        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        assert recording.status == "completed"
        assert recording.oss_id is not None
        assert recording.verify_attempts == 1

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_registers_head_object_size_when_egress_size_is_zero(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_head_object_size(config, object_name, timeout=5.0):
        _ = timeout
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 4096

    monkeypatch.setattr(MinioUtil, "head_object_size", fake_head_object_size)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager(file_size=0)
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 4096

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_browser_disconnect_stops_recording_before_room_delete(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        room_manager = FakeLiveKitRoomManager()
        fake_egress = FakeEgressManager(room_manager)
        service = AiCallService(
            build_b1_orchestrator(livekit_room_manager=room_manager),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_disconnect",
            timestamp=None,
            tenant_id="000000",
        )

        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert fake_egress.stopped == [f"EG_{result.call_id}"]
        assert fake_egress.stop_saw_deleted_room is False
        assert room_manager.deleted_rooms == [result.room_name]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_failed_status_marks_recording_failed(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FailedStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "egress_stop"
        assert failed["ossId"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_timeout_enters_verifying_then_reconciles(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 358253

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = TimeoutMainStopEgressManager()
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=fake_egress,
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        pending = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert pending is not None
        assert pending["status"] == "verifying"
        assert pending["ossId"] is None
        assert pending["nextVerifyAt"] is not None
        assert pending["verifyDeadlineAt"] is not None
        assert pending["lastVerifyError"] is not None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["failureStage"] is None
        assert completed["failureMessage"] is None
        assert completed["ossId"] is not None
        play_url = urlparse(completed["playUrl"])
        assert play_url.path == (
            f"/recordings/ai-call/recordings/{result.call_id}.mp3"
        )
        assert parse_qs(play_url.query)["X-Amz-SignedHeaders"] == ["host"]
        assert completed["verifyAttempts"] == 1
        assert completed["nextVerifyAt"] is None

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 358253

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_already_completed_recovers_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert object_name.startswith("ai-call/recordings/")
        return 245760

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = AlreadyCompletedMainStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["failureStage"] is None
        assert completed["failureMessage"] is None
        assert completed["ossId"] is not None
        assert completed["lastVerifyError"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(completed["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 245760

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_stop_non_timeout_exception_does_not_recover_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fail_if_resolved(config, object_name):
        _ = config, object_name
        raise AssertionError("non-timeout stop failure must not be recovered from OSS")

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fail_if_resolved),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = BrokenMainStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "egress_stop"
        assert failed["ossId"] is None
        assert fake_egress.stopped == [f"EG_{result.call_id}"]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_verification_deadline_marks_missing_object_failed(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def missing_object(config, object_name):
        _ = config, object_name
        return None

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(missing_object),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=TimeoutMainStopEgressManager(),
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        expired_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await repository.update_recording(
            tenant_id="000000",
            call_id=result.call_id,
            next_verify_at=expired_at,
            verify_deadline_at=expired_at,
        )

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        failed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "oss_missing"
        assert failed["verifyAttempts"] == 1
        assert failed["nextVerifyAt"] is None

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_stop_timeout_enters_verifying_then_reconciles(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert "/tracks/" in object_name
        return 102080

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = TimeoutAiTrackStopEgressManager()
        recording_service = AiCallRecordingService(
            repository,
            enabled=True,
            egress_manager=fake_egress,
            participant_recording_enabled=True,
        )
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=recording_service,
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        await service.end_session(result.call_id)

        pending = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert pending is not None
        tracks = {track["trackRole"]: track for track in pending["tracks"]}
        assert tracks["customer"]["status"] == "completed"
        assert tracks["ai"]["status"] == "verifying"
        assert tracks["ai"]["ossId"] is None
        assert tracks["ai"]["nextVerifyAt"] is not None

        ready_call_ids = await recording_service.reconcile_due_recordings()
        assert ready_call_ids == {result.call_id}

        completed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert completed is not None
        assert completed["status"] == "completed"
        tracks = {track["trackRole"]: track for track in completed["tracks"]}
        assert tracks["ai"]["status"] == "completed"
        assert tracks["ai"]["failureStage"] is None
        assert tracks["ai"]["failureMessage"] is None
        assert tracks["ai"]["ossId"] is not None
        play_url = urlparse(tracks["ai"]["playUrl"])
        assert play_url.path == (
            f"/recordings/ai-call/recordings/tracks/"
            f"{result.call_id}/ai-agent-{result.call_id}.mp3"
        )
        assert parse_qs(play_url.query)["X-Amz-SignedHeaders"] == ["host"]

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(tracks["ai"]["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 102080

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_participant_stop_already_completed_recovers_from_oss(
    monkeypatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fake_resolve_existing_object_size(config, object_name):
        assert config["bucket_name"] == "recordings"
        assert "/tracks/" in object_name
        return 88064

    monkeypatch.setattr(
        OssService,
        "resolve_existing_object_size",
        staticmethod(fake_resolve_existing_object_size),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = AlreadyCompletedAiTrackStopEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
                participant_recording_enabled=True,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.report_browser_event(
            call_id=result.call_id,
            event_type="browser_ready",
            timestamp=None,
            tenant_id="000000",
        )
        await service.end_session(result.call_id)

        recording = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert recording is not None
        tracks = {track["trackRole"]: track for track in recording["tracks"]}
        assert tracks["customer"]["status"] == "completed"
        assert tracks["ai"]["status"] == "completed"
        assert tracks["ai"]["failureStage"] is None
        assert tracks["ai"]["failureMessage"] is None
        assert tracks["ai"]["ossId"] is not None
        assert tracks["ai"]["lastVerifyError"] is None

        oss_row = await db.execute(
            text("select ext1 from sys_oss where oss_id = :oss_id"),
            {"oss_id": int(tracks["ai"]["ossId"])},
        )
        ext1 = json.loads(oss_row.scalar_one())
        assert ext1["fileSize"] == 88064

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_recording_oss_register_failure_keeps_object_name(monkeypatch) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "bucket_name": "recordings",
            "endpoint": "minio.test:9000",
            "domain": "https://files.test",
            "is_https": "N",
            "access_key": "minio",
            "secret_key": "secret",
            "region": "",
        },
    )

    async def fail_register(*args, **kwargs):
        _ = args, kwargs
        raise RuntimeError("oss register failed")

    monkeypatch.setattr(
        OssService,
        "register_existing_object_service",
        staticmethod(fail_register),
    )

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        fake_egress = FakeEgressManager()
        service = AiCallService(
            build_b1_orchestrator(),
            record_service,
            recording_service=AiCallRecordingService(
                repository,
                enabled=True,
                egress_manager=fake_egress,
            ),
        )

        result = await service.create_web_session(
            tenant_id="000000",
            voice=None,
            prompt=None,
            business_id=None,
        )
        await service.end_session(result.call_id)

        failed = await service.get_recording(tenant_id="000000", call_id=result.call_id)
        assert failed is not None
        assert failed["status"] == "failed"
        assert failed["failureStage"] == "oss_register"
        assert failed["objectName"] == f"ai-call/recordings/{result.call_id}.mp3"
        assert failed["ossId"] is None
        assert failed["durationMs"] == 1200

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_preview_keeps_partial_in_memory_and_persists_final() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        orchestrator = build_b1_orchestrator()
        repository = AiCallRecordRepository(db)
        record_service = AiCallRecordService(repository)
        runtime_store = AiCallDialogueRuntimeStore()
        runtime_store.attach_event_store(orchestrator.event_store)
        dialogue_worker = AiCallDialoguePersistenceWorker(
            session_maker,
            flush_interval_seconds=0.01,
        )
        await dialogue_worker.start()
        dialogue_worker.attach_runtime_store(runtime_store)
        service = AiCallService(
            orchestrator,
            record_service,
            dialogue_service=AiCallDialogueService(repository, runtime_store),
        )
        try:
            result = await service.create_web_session(
                tenant_id="000000",
                voice=None,
                prompt=None,
                business_id=None,
            )
            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_speech_started",
                source="provider",
                payload={"item_id": "item_customer_1", "audio_start_ms": 1000},
            )
            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_delta",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "text": "今天",
                    "stash": "需要还款吗",
                },
            )

            preview = await service.list_dialogue_preview(result.call_id)
            assert preview["total"] == 1
            assert preview["rows"][0]["segmentStatus"] == "partial"
            assert preview["rows"][0]["text"] == "今天需要还款吗"
            assert await service.list_record_dialogue_segments(result.call_id) == {
                "rows": [],
                "total": 0,
            }

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_speech_stopped",
                source="provider",
                payload={"item_id": "item_customer_1", "audio_end_ms": 2200},
            )
            await dialogue_worker.flush_pending()
            assert await service.list_record_dialogue_segments(result.call_id) == {
                "rows": [],
                "total": 0,
            }

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_done",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "transcript": "今天需要还款吗",
                },
            )
            await dialogue_worker.flush_pending()

            persisted = await service.list_record_dialogue_segments(result.call_id)
            assert persisted["total"] == 1
            assert persisted["rows"][0]["id"].isdigit()
            assert persisted["rows"][0]["speakerType"] == "customer"
            assert persisted["rows"][0]["source"] == "qwen_realtime"
            assert persisted["rows"][0]["sourceSegmentId"] == "item_customer_1"
            assert persisted["rows"][0]["segmentStatus"] == "final"
            assert persisted["rows"][0]["text"] == "今天需要还款吗"
            assert persisted["rows"][0]["audioStartMs"] == 1000
            assert persisted["rows"][0]["audioEndMs"] == 2200

            orchestrator.event_store.append(
                call_id=result.call_id,
                type="user_transcript_done",
                source="provider",
                payload={
                    "item_id": "item_customer_1",
                    "transcript": "今天需要还款吗",
                },
            )
            await dialogue_worker.flush_pending()

            persisted = await service.list_record_dialogue_segments(result.call_id)
            assert persisted["total"] == 1
        finally:
            dialogue_worker.detach_all()
            runtime_store.detach_all()
            await dialogue_worker.stop()

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_persistence_upserts_by_source_segment_id() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        first = DialogueSegmentSnapshot(
            call_id="call_dialogue_upsert",
            segment_no=1,
            speaker_type="customer",
            speaker_identity=None,
            source="qwen_realtime",
            source_segment_id="item_same",
            text="你叫什么？",
            segment_status="final",
            started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 6, 16, 10, 0, 1, tzinfo=timezone.utc),
            duration_ms=1000,
        )
        second = DialogueSegmentSnapshot(
            call_id="call_dialogue_upsert",
            segment_no=99,
            speaker_type="customer",
            speaker_identity=None,
            source="qwen_realtime",
            source_segment_id="item_same",
            text="你叫什么呀",
            segment_status="final",
            started_at=datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 6, 16, 10, 0, 2, tzinfo=timezone.utc),
            duration_ms=2000,
        )

        await service.persist_snapshot(first)
        await service.persist_snapshot(second)

        rows = await service.list_persisted_segments("call_dialogue_upsert")
        assert len(rows) == 1
        assert rows[0].segment_no == 1
        assert rows[0].source_segment_id == "item_same"
        assert rows[0].segment_text == "你叫什么呀"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_persistence_separates_same_source_segment_id_by_speaker() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        now = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_same_item_id",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_reused",
                text="可以啊",
                segment_status="final",
                started_at=now,
                ended_at=now + timedelta(milliseconds=500),
                duration_ms=500,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_same_item_id",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_reused",
                text="好的，那我简单问两句。",
                segment_status="final",
                started_at=now + timedelta(milliseconds=600),
                ended_at=now + timedelta(milliseconds=1600),
                duration_ms=1000,
            )
        )

        rows = await service.list_persisted_segments("call_same_item_id")
        assert [(row.speaker_type, row.source_segment_id) for row in rows] == [
            ("customer", "item_reused"),
            ("ai", "item_reused"),
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_duplicate_offline_asr_segment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
        ended_at = started_at + timedelta(milliseconds=1600)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_dialogue_duplicate_sources",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_customer_1",
                text="你能做什么？",
                segment_status="final",
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=1600,
            )
        )
        await repository.upsert_dialogue_segment(
            call_id="call_dialogue_duplicate_sources",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call_dialogue_duplicate_sources",
            source="offline_asr",
            source_segment_id="track_customer_1",
            segment_text="你能做什么",
            segment_status="final",
            started_at=started_at - timedelta(milliseconds=400),
            ended_at=ended_at - timedelta(milliseconds=100),
            duration_ms=1500,
        )

        raw_rows = await repository.list_dialogue_segments("call_dialogue_duplicate_sources")
        rows = await service.list_persisted_segments("call_dialogue_duplicate_sources")

        assert len(raw_rows) == 2
        assert len(rows) == 1
        assert rows[0].source == "qwen_realtime"
        assert rows[0].segment_text == "你能做什么？"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_overlapping_offline_asr_when_realtime_exists() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_dialogue_shadowed_offline_asr",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_customer_ok",
                text="行。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=2),
                ended_at=started_at + timedelta(seconds=3),
                duration_ms=1000,
                audio_start_ms=2000,
                audio_end_ms=3000,
            )
        )
        await repository.upsert_dialogue_segment(
            call_id="call_dialogue_shadowed_offline_asr",
            segment_no=2,
            speaker_type="customer",
            speaker_identity="browser-call_dialogue_shadowed_offline_asr",
            source="offline_asr",
            source_segment_id="track_customer_1",
            segment_text="So.",
            segment_status="final",
            started_at=started_at + timedelta(milliseconds=1500),
            ended_at=started_at + timedelta(milliseconds=3500),
            duration_ms=2000,
        )

        raw_rows = await repository.list_dialogue_segments(
            "call_dialogue_shadowed_offline_asr"
        )
        rows = await service.list_persisted_segments("call_dialogue_shadowed_offline_asr")

        assert len(raw_rows) == 2
        assert [(row.source, row.segment_text) for row in rows] == [
            ("qwen_realtime", "行。")
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_realtime_customer_prefix_fragment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 24, 7, 26, 45, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_realtime_customer_prefix",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_prefix",
                text="没有啊。",
                segment_status="final",
                started_at=started_at,
                ended_at=started_at + timedelta(minutes=1),
                duration_ms=60000,
                audio_start_ms=25280,
                audio_end_ms=None,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_realtime_customer_prefix",
                segment_no=2,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_full",
                text="没有啊，我不知道啊。",
                segment_status="final",
                started_at=started_at + timedelta(milliseconds=990),
                ended_at=started_at + timedelta(milliseconds=1750),
                duration_ms=760,
                audio_start_ms=None,
                audio_end_ms=26980,
            )
        )

        raw_rows = await repository.list_dialogue_segments("call_realtime_customer_prefix")
        rows = await service.list_persisted_segments("call_realtime_customer_prefix")

        assert len(raw_rows) == 2
        assert [(row.source_segment_id, row.segment_text) for row in rows] == [
            ("item_full", "没有啊，我不知道啊。")
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_duplicate_realtime_ai_segment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
        opening = "您好张总，我是灵宸智能助手，想简单介绍一下GEO生成式引擎优化服务，请问现在方便吗？"

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_dialogue_duplicate_ai_opening",
                segment_no=1,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_opening_audio",
                text=opening,
                segment_status="final",
                started_at=started_at,
                ended_at=started_at + timedelta(seconds=29),
                duration_ms=29000,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_dialogue_duplicate_ai_opening",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_opening_done",
                text="您好张总，我是灵宸智能助手，想简单介绍一下 GEO 生成式引擎优化服务，请问现在方便吗？",
                segment_status="final",
                started_at=started_at + timedelta(seconds=8),
                ended_at=started_at + timedelta(seconds=8),
                duration_ms=0,
            )
        )

        raw_rows = await repository.list_dialogue_segments("call_dialogue_duplicate_ai_opening")
        rows = await service.list_persisted_segments("call_dialogue_duplicate_ai_opening")

        assert len(raw_rows) == 2
        assert len(rows) == 1
        assert rows[0].source_segment_id == "item_opening_audio"
        assert rows[0].segment_text == opening
        assert rows[0].duration_ms == 29000

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_keeps_short_customer_ack_after_duplicate_ai_done() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
        question = "好的，明白。那您看，我们是否需要安排一位产品顾问后续联系您？"

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_short_ack_after_duplicate_ai",
                segment_no=1,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_added",
                text=question,
                segment_status="final",
                started_at=started_at,
                ended_at=started_at + timedelta(seconds=60),
                duration_ms=60000,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_short_ack_after_duplicate_ai",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_done",
                text=question,
                segment_status="final",
                started_at=started_at + timedelta(seconds=8),
                ended_at=started_at + timedelta(seconds=8),
                duration_ms=0,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_short_ack_after_duplicate_ai",
                segment_no=3,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_customer_ok",
                text="行。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=20),
                ended_at=started_at + timedelta(seconds=21),
                duration_ms=1000,
                audio_start_ms=20000,
                audio_end_ms=21000,
            )
        )

        rows = await service.list_persisted_segments("call_short_ack_after_duplicate_ai")

        assert [(row.speaker_type, row.source_segment_id, row.segment_text) for row in rows] == [
            ("ai", "item_ai_added", question),
            ("customer", "item_customer_ok", "行。"),
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_preview_hides_duplicate_realtime_ai_segment() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    call_id = "call_dialogue_preview_duplicate_ai"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    message = (
        "我是灵宸智能助手，主要负责协助确认款项的归还安排。"
        "请问您现在方便沟通一下预计什么时候可以归还吗？"
        "如果暂时不确定，也可以告诉我一个大概的时间范围。"
    )

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={"item_id": "item_ai_audio", "delta": message},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="model_response_done",
        source="provider",
        payload={"response": {"id": "item_ai_audio", "status": "completed"}},
        timestamp=started_at + timedelta(seconds=29),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={"item_id": "item_ai_done", "transcript": message},
        timestamp=started_at + timedelta(seconds=8),
    )

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        service = AiCallDialogueService(AiCallRecordRepository(db), runtime_store)
        preview = await service.list_preview_segments(call_id)

        assert preview["total"] == 1
        assert preview["rows"][0]["sourceSegmentId"] == "item_ai_audio"
        assert preview["rows"][0]["text"] == message

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_preview_uses_persisted_segments_after_call_completed() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    call_id = "call_dialogue_preview_completed_uses_persisted"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "runtime_customer_stale"},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "runtime_customer_stale", "transcript": "运行态残缺文本。"},
        timestamp=started_at + timedelta(seconds=1),
    )

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        repository = AiCallRecordRepository(db)
        await repository.create_record(
            call_id=call_id,
            business_type=None,
            business_id=None,
            entry_type="web",
            room_name=f"ai-call-{call_id}",
            participant_identity=f"browser-{call_id}",
            status=CallSessionStatus.COMPLETED.value,
            started_at=started_at,
        )
        service = AiCallDialogueService(repository, runtime_store)
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id=call_id,
                segment_no=1,
                speaker_type="customer",
                speaker_identity=f"browser-{call_id}",
                source="qwen_realtime",
                source_segment_id="persisted_customer_final",
                text="你们都会使用哪些AI平台？",
                segment_status="final",
                started_at=started_at + timedelta(seconds=2),
                ended_at=started_at + timedelta(seconds=3),
                duration_ms=1000,
            )
        )

        preview = await service.list_preview_segments(call_id)

        assert preview["total"] == 1
        assert preview["rows"][0]["sourceSegmentId"] == "persisted_customer_final"
        assert preview["rows"][0]["text"] == "你们都会使用哪些AI平台？"

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_only_obvious_short_realtime_asr_noise() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

        snapshots = [
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=1,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_noise",
                text="嘿嘿。",
                segment_status="final",
                started_at=started_at,
                ended_at=started_at + timedelta(milliseconds=1),
                duration_ms=1,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=2,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ack",
                text="嗯。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=1),
                ended_at=started_at + timedelta(seconds=1, milliseconds=1),
                duration_ms=1,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=3,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_sentence",
                text="你好，我是林晨晨。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=2),
                ended_at=started_at + timedelta(seconds=2),
                duration_ms=0,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=4,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_normal_short",
                text="对呀。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=3),
                ended_at=started_at + timedelta(seconds=3, milliseconds=667),
                duration_ms=667,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=5,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_late_noise",
                text="法人。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=4),
                ended_at=started_at + timedelta(minutes=1),
                duration_ms=56000,
                audio_start_ms=30060,
                audio_end_ms=None,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=6,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_greeting_with_audio_span",
                text="你好。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=5),
                ended_at=started_at + timedelta(seconds=5),
                duration_ms=0,
                audio_start_ms=2440,
                audio_end_ms=3300,
            ),
            DialogueSegmentSnapshot(
                call_id="call_short_asr_noise",
                segment_no=7,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_short_number_tail",
                text="五七一八五。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=6),
                ended_at=started_at + timedelta(seconds=6, milliseconds=37),
                duration_ms=37,
                audio_start_ms=14600,
                audio_end_ms=15840,
            ),
        ]
        for snapshot in snapshots:
            await service.persist_snapshot(snapshot)

        raw_rows = await repository.list_dialogue_segments("call_short_asr_noise")
        rows = await service.list_persisted_segments("call_short_asr_noise")

        assert len(raw_rows) == 7
        assert [row.source_segment_id for row in rows] == [
            "item_ack",
            "item_sentence",
            "item_normal_short",
            "item_greeting_with_audio_span",
        ]
        assert [row.segment_text for row in rows] == [
            "嗯。",
            "你好，我是林晨晨。",
            "对呀。",
            "你好。",
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_double_talk_single_char_realtime_asr() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 24, 6, 37, 16, tzinfo=timezone.utc)
        opening = "您好，我是灵宸智能助手，想和您确认一下当前款项的归还安排，请问您现在方便简单沟通一下吗？"

        snapshots = [
            DialogueSegmentSnapshot(
                call_id="call_double_talk_short_asr",
                segment_no=1,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_opening",
                text=opening,
                segment_status="final",
                started_at=started_at,
                ended_at=started_at + timedelta(seconds=8),
                duration_ms=8000,
            ),
            DialogueSegmentSnapshot(
                call_id="call_double_talk_short_asr",
                segment_no=2,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_misheard_ack",
                text="对。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=6),
                ended_at=started_at + timedelta(seconds=8),
                duration_ms=2320,
                audio_start_ms=1300,
                audio_end_ms=1920,
            ),
            DialogueSegmentSnapshot(
                call_id="call_double_talk_short_asr",
                segment_no=3,
                speaker_type="customer",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_real_greeting",
                text="你好。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=8),
                ended_at=started_at + timedelta(seconds=8),
                duration_ms=0,
            ),
        ]
        for snapshot in snapshots:
            await service.persist_snapshot(snapshot)

        raw_rows = await repository.list_dialogue_segments("call_double_talk_short_asr")
        rows = await service.list_persisted_segments("call_double_talk_short_asr")

        assert len(raw_rows) == 3
        assert [(row.speaker_type, row.segment_text) for row in rows] == [
            ("ai", opening),
            ("customer", "你好。"),
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_preview_hides_semantically_rejected_customer_transcript() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    call_id = "call_dialogue_rejected_transcript"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_rejected", "audio_start_ms": 1400},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={
            "item_id": "item_rejected",
            "transcript": "我。",
            "transcriptTrust": "low_confidence",
            "semanticAction": "reject",
            "semanticRejectReason": "opening_double_talk_low_confidence_transcript",
        },
        timestamp=started_at + timedelta(milliseconds=640),
    )

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        service = AiCallDialogueService(AiCallRecordRepository(db), runtime_store)
        preview = await service.list_preview_segments(call_id)

        assert preview == {"rows": [], "total": 0}

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_keeps_interrupted_realtime_ai_segment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_interrupted_ai_text",
                segment_no=1,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_interrupted",
                text="归还呢？如果暂时不方便确定具体时间",
                segment_status="interrupted",
                started_at=started_at,
                ended_at=started_at + timedelta(milliseconds=200),
                duration_ms=200,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_interrupted_ai_text",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_final",
                text="我这边预计大概下周内可以安排归还，如果时间有变化我再提前跟您说。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=2),
                ended_at=started_at + timedelta(seconds=6),
                duration_ms=4000,
            )
        )

        raw_rows = await repository.list_dialogue_segments("call_interrupted_ai_text")
        rows = await service.list_persisted_segments("call_interrupted_ai_text")

        assert len(raw_rows) == 2
        assert [(row.source_segment_id, row.segment_status, row.segment_text) for row in rows] == [
            (
                "item_ai_interrupted",
                "interrupted",
                "归还呢？如果暂时不方便确定具体时间",
            ),
            (
                "item_ai_final",
                "final",
                "我这边预计大概下周内可以安排归还，如果时间有变化我再提前跟您说。",
            )
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.anyio
async def test_dialogue_query_hides_unplayed_interrupted_ai_segment() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.create_all)

    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        repository = AiCallRecordRepository(db)
        service = AiCallDialogueService(repository)
        started_at = datetime(2026, 7, 6, 8, 53, 13, tzinfo=timezone.utc)

        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_unplayed_interrupted_ai",
                segment_no=1,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_cancelled_before_playout",
                text="谢谢张总，那我简单说一下。GEO 主要是帮助品牌和产品。",
                segment_status="interrupted",
                started_at=started_at,
                ended_at=started_at,
                duration_ms=0,
            )
        )
        await service.persist_snapshot(
            DialogueSegmentSnapshot(
                call_id="call_unplayed_interrupted_ai",
                segment_no=2,
                speaker_type="ai",
                speaker_identity=None,
                source="qwen_realtime",
                source_segment_id="item_ai_final",
                text="好的张总，那我先不打扰您了，祝您工作顺利，再见。",
                segment_status="final",
                started_at=started_at + timedelta(seconds=20),
                ended_at=started_at + timedelta(seconds=24),
                duration_ms=4000,
            )
        )

        raw_rows = await repository.list_dialogue_segments("call_unplayed_interrupted_ai")
        rows = await service.list_persisted_segments("call_unplayed_interrupted_ai")

        assert len(raw_rows) == 2
        assert [(row.source_segment_id, row.segment_status, row.segment_text) for row in rows] == [
            (
                "item_ai_final",
                "final",
                "好的张总，那我先不打扰您了，祝您工作顺利，再见。",
            )
        ]

    async with engine.begin() as conn:
        await conn.run_sync(MappedBase.metadata.drop_all)
    await engine.dispose()


def test_dialogue_runtime_finalizes_customer_segment_when_handoff_is_requested() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_handoff_customer_segment"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    handoff_at = started_at + timedelta(milliseconds=380)

    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_handoff"},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_delta",
        source="provider",
        payload={"item_id": "item_handoff", "stash": "转人工。"},
        timestamp=started_at + timedelta(milliseconds=370),
    )
    event_store.append(
        call_id=call_id,
        type="handoff_requested",
        source="handoff",
        payload={"handoffId": "handoff_1"},
        timestamp=handoff_at,
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(seconds=104),
    )

    assert len(persisted) == 1
    assert persisted[0].text == "转人工。"
    assert persisted[0].segment_status == "final"
    assert persisted[0].ended_at == handoff_at
    assert persisted[0].duration_ms == 380


def test_dialogue_runtime_merges_adjacent_customer_fragments() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_fragment_merge"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_1", "audio_start_ms": 1000},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_delta",
        source="provider",
        payload={"item_id": "item_1", "stash": "你叫什么？"},
        timestamp=started_at + timedelta(milliseconds=2),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_stopped",
        source="provider",
        payload={"item_id": "item_1", "audio_end_ms": 1500},
        timestamp=started_at + timedelta(milliseconds=4),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item_1", "transcript": "你叫什么？"},
        timestamp=started_at + timedelta(milliseconds=5),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_started",
        source="provider",
        payload={"item_id": "item_2", "audio_start_ms": 1501},
        timestamp=started_at + timedelta(milliseconds=6),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_delta",
        source="provider",
        payload={"item_id": "item_2", "stash": "你叫什么呀"},
        timestamp=started_at + timedelta(milliseconds=8),
    )
    event_store.append(
        call_id=call_id,
        type="user_speech_stopped",
        source="provider",
        payload={"item_id": "item_2", "audio_end_ms": 1800},
        timestamp=started_at + timedelta(milliseconds=10),
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(milliseconds=20),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_1"
    assert preview[0].text == "你叫什么呀"
    assert preview[0].audio_start_ms == 1000
    assert preview[0].audio_end_ms == 1800
    assert persisted[-1].text == "你叫什么呀"


def test_dialogue_runtime_suppresses_interrupted_ai_done_duplicate() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_ai_duplicate_after_interrupt"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={"item_id": "item_ai_interrupted", "delta": "您好，我是灵宸智能助手。"},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="interrupt_confirmed",
        source="agent",
        payload={},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={"item_id": "item_ai_late", "transcript": "您好，我是灵宸智能助手。"},
        timestamp=started_at + timedelta(milliseconds=800),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_ai_interrupted"
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == "您好，我是灵宸智能助手。"
    assert len(persisted) == 1


def test_dialogue_runtime_keeps_ai_done_duplicate_after_customer_turn() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_ai_duplicate_after_customer_turn"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    ai_text = "您看是约个时间给您做个简短演示更合适，还是我先安排同事跟您对接详细需求？"

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={"item_id": "item_ai_interrupted", "delta": ai_text},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="interrupt_confirmed",
        source="agent",
        payload={},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item_customer", "transcript": "行。"},
        timestamp=started_at + timedelta(milliseconds=700),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={"item_id": "item_ai_next", "transcript": ai_text},
        timestamp=started_at + timedelta(milliseconds=900),
    )

    preview = runtime_store.list_preview(call_id)
    assert [(row.speaker_type, row.source_segment_id) for row in preview] == [
        ("ai", "item_ai_interrupted"),
        ("customer", "item_customer"),
        ("ai", "item_ai_next"),
    ]
    assert preview[0].segment_status == "interrupted"
    assert preview[2].segment_status == "final"
    assert preview[2].text == ai_text
    assert len(persisted) == 3


def test_dialogue_runtime_suppresses_interrupted_response_duplicate_after_customer_turn() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_interrupted_response_duplicate_after_customer_turn"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    ai_text = "张总您好，那我跟您简单说一下。GEO主要是帮助品牌和产品被更准确地引用。"

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_ai_interrupted",
            "response_id": "resp_interrupted",
            "delta": ai_text,
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="response_generation_invalidated",
        source="agent",
        payload={"responseId": "resp_interrupted"},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item_customer", "transcript": "行，可以的。"},
        timestamp=started_at + timedelta(milliseconds=700),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={
            "item_id": "item_ai_late",
            "response_id": "resp_interrupted",
            "transcript": ai_text,
        },
        timestamp=started_at + timedelta(milliseconds=900),
    )

    preview = runtime_store.list_preview(call_id)
    assert [(row.speaker_type, row.source_segment_id) for row in preview] == [
        ("ai", "item_ai_interrupted"),
        ("customer", "item_customer"),
    ]
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == ai_text
    assert preview[1].segment_status == "final"
    assert len(persisted) == 2


def test_dialogue_runtime_marks_late_ai_done_for_invalidated_response_interrupted() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_late_ai_done_after_interrupt"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="response_generation_invalidated",
        source="agent",
        payload={"responseId": "resp_interrupted"},
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="interrupt_confirmed",
        source="agent",
        payload={},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={"response_id": "resp_interrupted", "transcript": "这个是被打断的旧回答。"},
        timestamp=started_at + timedelta(milliseconds=800),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "resp_interrupted"
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == "这个是被打断的旧回答。"
    assert len(persisted) == 1
    assert persisted[0].segment_status == "interrupted"


def test_dialogue_runtime_suppresses_late_ai_done_when_response_item_changes() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_late_ai_done_item_changed_after_interrupt"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_ai_old",
            "response_id": "resp_interrupted",
            "delta": "谢谢张总。我简单说一下，GEO主要是帮助品牌和产品。",
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="response_generation_invalidated",
        source="provider",
        payload={"responseId": "resp_interrupted"},
        timestamp=started_at + timedelta(milliseconds=300),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={
            "item_id": "item_ai_new",
            "response_id": "resp_interrupted",
            "transcript": "谢谢张总。我简单说一下，GEO主要是帮助品牌和产品。",
        },
        timestamp=started_at + timedelta(milliseconds=800),
    )
    event_store.append(
        call_id=call_id,
        type="interrupt_confirmed",
        source="agent",
        payload={},
        timestamp=started_at + timedelta(milliseconds=850),
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(seconds=10),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_ai_old"
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == "谢谢张总。我简单说一下，GEO主要是帮助品牌和产品。"
    assert len(persisted) == 1
    assert persisted[0].segment_status == "interrupted"


def test_dialogue_runtime_marks_finalized_ai_interrupted_when_invalidation_arrives_late() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted_statuses: list[str] = []
    runtime_store.add_persist_listener(
        lambda snapshot: persisted_statuses.append(snapshot.segment_status)
    )
    call_id = "call_invalidation_after_model_done"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    transcript = "好的张总，那我再跟您具体说说。GEO的核心就是让品牌被更准确地引用。"

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_ai_interrupted",
            "response_id": "resp_interrupted",
            "delta": transcript,
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="model_response_done",
        source="provider",
        payload={
            "response": {
                "id": "resp_interrupted",
                "status": "completed",
                "output": [
                    {
                        "id": "item_ai_interrupted",
                        "role": "assistant",
                        "type": "message",
                        "content": [{"type": "audio", "transcript": transcript}],
                    }
                ],
            }
        },
        timestamp=started_at + timedelta(milliseconds=700),
    )
    event_store.append(
        call_id=call_id,
        type="response_generation_invalidated",
        source="provider",
        payload={"responseId": "resp_interrupted"},
        timestamp=started_at + timedelta(milliseconds=720),
    )

    preview = runtime_store.list_preview(call_id)
    assert len(preview) == 1
    assert preview[0].source_segment_id == "item_ai_interrupted"
    assert preview[0].segment_status == "interrupted"
    assert preview[0].text == transcript
    assert persisted_statuses == ["final", "interrupted"]


def test_dialogue_runtime_suppresses_unheard_ai_response_after_stale_audio_drop() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_unheard_ai_response"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    transcript = "您是想先了解我们具体怎么观测AI对品牌的介绍吗？"

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_unheard_ai",
            "response_id": "resp_unheard_ai",
            "delta": "您",
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="stale_audio_dropped",
        source="agent",
        payload={
            "responseId": "resp_unheard_ai",
            "reason": "session_not_ai_speaking",
        },
        timestamp=started_at + timedelta(milliseconds=200),
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={
            "item_id": "item_unheard_ai",
            "response_id": "resp_unheard_ai",
            "transcript": transcript,
        },
        timestamp=started_at + timedelta(milliseconds=700),
    )
    event_store.append(
        call_id=call_id,
        type="model_audio_done",
        source="provider",
        payload={
            "item_id": "item_unheard_ai",
            "response_id": "resp_unheard_ai",
        },
        timestamp=started_at + timedelta(milliseconds=720),
    )
    event_store.append(
        call_id=call_id,
        type="model_response_done",
        source="provider",
        payload={
            "response": {
                "id": "resp_unheard_ai",
                "status": "completed",
                "output": [
                    {
                        "id": "item_unheard_ai",
                        "role": "assistant",
                        "type": "message",
                        "content": [{"type": "audio", "transcript": transcript}],
                    }
                ],
            }
        },
        timestamp=started_at + timedelta(milliseconds=750),
    )

    assert runtime_store.list_preview(call_id) == []
    assert persisted == []


def test_dialogue_runtime_does_not_finalize_unheard_ai_fragment_on_session_completed() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    persisted: list[DialogueSegmentSnapshot] = []
    runtime_store.add_persist_listener(persisted.append)
    call_id = "call_unheard_ai_fragment"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_unheard_fragment",
            "response_id": "resp_unheard_fragment",
            "delta": "您",
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="stale_audio_dropped",
        source="agent",
        payload={
            "responseId": "resp_unheard_fragment",
            "reason": "session_not_ai_speaking",
        },
        timestamp=started_at + timedelta(milliseconds=200),
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(seconds=10),
    )

    assert runtime_store.list_preview(call_id) == []
    assert persisted == []


def test_dialogue_runtime_suppresses_orphan_ai_item_when_response_done_uses_final_item() -> None:
    runtime_store = AiCallDialogueRuntimeStore()
    event_store = InMemoryEventStore()
    runtime_store.attach_event_store(event_store)
    call_id = "call_response_done_final_item"
    started_at = datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc)
    ai_text = "好的，明白。那您看，我们是否需要安排一位产品顾问后续联系您？"

    event_store.append(
        call_id=call_id,
        type="ai_transcript_delta",
        source="provider",
        payload={
            "item_id": "item_ai_added",
            "response_id": "resp_ai",
            "delta": ai_text,
        },
        timestamp=started_at,
    )
    event_store.append(
        call_id=call_id,
        type="ai_transcript_done",
        source="provider",
        payload={
            "item_id": "item_ai_done",
            "response_id": "resp_ai",
            "transcript": ai_text,
        },
        timestamp=started_at + timedelta(seconds=8),
    )
    event_store.append(
        call_id=call_id,
        type="model_response_done",
        source="provider",
        payload={
            "response": {
                "id": "resp_ai",
                "status": "completed",
                "output": [
                    {
                        "id": "item_ai_done",
                        "role": "assistant",
                        "type": "message",
                        "content": [{"type": "audio", "transcript": ai_text}],
                    }
                ],
            }
        },
        timestamp=started_at + timedelta(seconds=8),
    )
    event_store.append(
        call_id=call_id,
        type="user_transcript_done",
        source="provider",
        payload={"item_id": "item_customer_ok", "transcript": "行。"},
        timestamp=started_at + timedelta(seconds=20),
    )
    event_store.append(
        call_id=call_id,
        type="session_completed",
        source="orchestrator",
        payload={},
        timestamp=started_at + timedelta(seconds=60),
    )

    preview = AiCallDialogueService._canonical_preview_segments(
        runtime_store.list_preview(call_id)
    )

    assert [(row.speaker_type, row.source_segment_id, row.text) for row in preview] == [
        ("ai", "item_ai_done", ai_text),
        ("customer", "item_customer_ok", "行。"),
    ]


@pytest.mark.anyio
async def test_owner_browser_ready_recording_writes_ready_without_legacy_track_start(
    b1_service,
    monkeypatch,
) -> None:
    from app.services.ai_call.runtime_control import customer_media_repository

    service, record_service = b1_service
    opening_commands = []

    class CommandRepositorySpy:
        def __init__(self, _session, **_kwargs) -> None:
            pass

        async def append_command(self, request) -> None:
            opening_commands.append(request)

    monkeypatch.setattr(
        customer_media_repository,
        "RuntimeCommandRepository",
        CommandRepositorySpy,
    )
    result = await service.create_web_session(
        tenant_id="000000",
        voice=None,
        prompt=None,
        business_id=None,
    )
    record = await record_service.get_record(result.call_id)
    assert record is not None
    record.dialogue_persistence_status = "pending"
    record.runtime_control_mode = "owner_command_v1"
    await record_service.repository.db.flush()

    class RecordingSpy:
        def __init__(self) -> None:
            self.start_calls: list[dict[str, object]] = []

        async def start_session_participant_recordings(self, **kwargs: object) -> None:
            self.start_calls.append(kwargs)

    spy = RecordingSpy()
    service.recording_service = spy

    await service.report_browser_event(
        call_id=result.call_id,
        event_type="browser_ready",
        timestamp=None,
        tenant_id="000000",
    )

    assert spy.start_calls == []
    assert record.answered_at is not None
    assert record.status == CallSessionStatus.CONNECTED.value
    assert [command.command_type for command in opening_commands] == ["START_OPENING"]


@pytest.mark.anyio
async def test_owner_browser_ready_recording_after_terminal_barrier_updates_zero_rows(
    b1_service,
) -> None:
    _service, record_service = b1_service
    result = await record_service.create_web_record(
        tenant_id="000000",
        call_id="owner-ready-terminal",
        business_id=None,
        room_name="room-owner-ready-terminal",
        participant_identity="browser-owner-ready-terminal",
    )
    result.dialogue_persistence_status = "pending"
    result.runtime_control_mode = "owner_command_v1"
    result.terminal_requested_at = datetime.now(timezone.utc)
    result.status = CallSessionStatus.ENDING.value
    await record_service.repository.db.flush()

    marked = await record_service.mark_owner_customer_ready(
        tenant_id="000000",
        call_id=result.call_id,
    )

    assert marked is False
    assert result.answered_at is None


@pytest.mark.anyio
async def test_failed_owner_customer_track_keeps_main_recording_closed_and_asr_ready(
    b1_service,
) -> None:
    _service, record_service = b1_service
    result = await record_service.create_web_record(
        tenant_id="000000",
        call_id="owner-track-failed",
        business_id=None,
        room_name="room-owner-track-failed",
        participant_identity="browser-owner-track-failed",
    )
    result.dialogue_persistence_status = "pending"
    result.runtime_control_mode = "owner_command_v1"
    result.status = CallSessionStatus.COMPLETED.value
    result.terminal_requested_at = datetime.now(timezone.utc)
    await record_service.repository.db.flush()
    await record_service.repository.create_recording(
        tenant_id="000000",
        call_id=result.call_id,
        room_name=result.room_name,
        status="completed",
        started_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )
    await record_service.repository.create_recording_track(
        tenant_id="000000",
        call_id=result.call_id,
        room_name=result.room_name,
        track_role="customer",
        participant_identity="browser-owner-track-failed",
        status="failed",
        started_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )
    track = await record_service.repository.get_recording_track(
        tenant_id="000000",
        call_id=result.call_id,
        track_role="customer",
        participant_identity="browser-owner-track-failed",
    )
    assert track is not None
    track.failure_stage = "oss_missing"
    track.failure_message = "录音停止后确认超时，未发现录音文件"
    await record_service.repository.db.flush()

    recording_service = AiCallRecordingService(
        record_service.repository,
        enabled=True,
    )
    assert await recording_service.is_ready_for_offline_asr(
        tenant_id="000000",
        call_id=result.call_id,
    )
    main_recording = await record_service.repository.get_recording(
        tenant_id="000000",
        call_id=result.call_id,
    )
    assert main_recording is not None
    assert main_recording.status == "completed"
    assert track.failure_message is not None
