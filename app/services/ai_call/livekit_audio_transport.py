from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt

from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.session_registry import CallSession

RoomFactory = Callable[[], Any]
SipConnectedObserver = Callable[[str], Awaitable[bool]]

_SIP_CONNECTED_STATUSES = frozenset({"active", "answered", "connected"})
_LOGGER = logging.getLogger(__name__)


class LiveKitRoomAudioTransport:
    def __init__(
        self,
        livekit_url: str,
        api_key: str,
        api_secret: str,
        rtc_module: Any | None = None,
        room_factory: RoomFactory | None = None,
        output_sample_rate_hz: int = 24000,
        output_channels: int = 1,
        output_queue_size_ms: int = 200,
        input_sample_rate_hz: int = 48000,
        input_channels: int = 1,
        frame_size_ms: int = 20,
    ) -> None:
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.rtc = rtc_module or self._load_rtc_module()
        self.room_factory = room_factory or self.rtc.Room
        self.output_sample_rate_hz = output_sample_rate_hz
        self.output_channels = output_channels
        self.output_queue_size_ms = output_queue_size_ms
        self.input_sample_rate_hz = input_sample_rate_hz
        self.input_channels = input_channels
        self.frame_size_ms = frame_size_ms
        self._rooms: dict[str, Any] = {}
        self._sources: dict[str, Any] = {}
        self._track_streams: dict[str, Any] = {}
        self._last_output_frames: dict[str, PcmAudioFrame] = {}
        self._target_participant_identities: dict[str, str] = {}
        self._sip_connected_observers: dict[str, SipConnectedObserver] = {}
        self._sip_connected_tasks: dict[str, asyncio.Task[None]] = {}
        self._reported_sip_connected: set[str] = set()

    def bind_sip_connected_observer(
        self,
        call_id: str,
        observer: SipConnectedObserver,
    ) -> None:
        self.unbind_sip_connected_observer(call_id)
        self._sip_connected_observers[call_id] = observer

    def unbind_sip_connected_observer(self, call_id: str) -> None:
        self._sip_connected_observers.pop(call_id, None)
        self._reported_sip_connected.discard(call_id)
        self._sip_connected_tasks.pop(call_id, None)

    async def start(self, session: CallSession) -> None:
        room = self.room_factory()
        source = self.rtc.AudioSource(
            self.output_sample_rate_hz,
            self.output_channels,
            queue_size_ms=self.output_queue_size_ms,
        )
        track = self.rtc.LocalAudioTrack.create_audio_track("ai_audio", source)
        options = self.rtc.TrackPublishOptions(source=self.rtc.TrackSource.SOURCE_MICROPHONE)
        self._track_streams[session.call_id] = _StreamQueue()
        self._target_participant_identities[session.call_id] = session.participant_identity

        room.on(
            "track_subscribed",
            lambda track, publication, participant: self._on_track_subscribed(
                session.call_id,
                track,
                publication,
                participant,
            ),
        )
        room.on(
            "participant_connected",
            lambda participant: self._on_participant_updated(
                session.call_id,
                participant,
            ),
        )
        room.on(
            "participant_attributes_changed",
            lambda changed_attributes, participant: self._on_participant_updated(
                session.call_id,
                participant,
                changed_attributes,
            ),
        )
        await room.connect(
            self.livekit_url,
            self._issue_agent_token(
                session.room_name,
                session.local_participant_identity or f"agent-{session.call_id}",
            ),
        )
        await room.local_participant.publish_track(track, options)

        self._rooms[session.call_id] = room
        self._sources[session.call_id] = source
        for participant in getattr(room, "remote_participants", {}).values():
            self._on_participant_updated(session.call_id, participant)

    async def publish_audio(self, call_id: str, frame: PcmAudioFrame) -> None:
        source = self._sources[call_id]
        self._last_output_frames[call_id] = frame
        await source.capture_frame(self._to_rtc_audio_frame(frame))

    async def stop_audio(self, call_id: str) -> None:
        source = self._sources.get(call_id)
        if source is None:
            return
        clear_queue = getattr(source, "clear_queue", None)
        if clear_queue is not None:
            await self._maybe_await(clear_queue())
        self._last_output_frames.pop(call_id, None)

    async def wait_for_playout(self, call_id: str) -> None:
        source = self._sources.get(call_id)
        if source is None:
            return
        wait_for_playout = getattr(source, "wait_for_playout", None)
        if wait_for_playout is not None:
            await self._maybe_await(wait_for_playout())

    async def receive_audio_frames(self, call_id: str) -> AsyncIterator[PcmAudioFrame]:
        stream_queue = self._track_streams[call_id]
        while True:
            stream = await stream_queue.get()
            if stream is None:
                return
            async for audio_event in stream:
                yield self._from_rtc_audio_frame(audio_event.frame)

    async def close(self, call_id: str) -> None:
        self.unbind_sip_connected_observer(call_id)
        with suppress(Exception):
            await self.stop_audio(call_id)
        stream_queue = self._track_streams.pop(call_id, None)
        if stream_queue is not None:
            await stream_queue.close()

        room = self._rooms.pop(call_id, None)
        if room is not None:
            await self._maybe_await(room.disconnect())
        self._sources.pop(call_id, None)
        self._last_output_frames.pop(call_id, None)
        self._target_participant_identities.pop(call_id, None)

    def _on_participant_updated(
        self,
        call_id: str,
        participant: Any,
        changed_attributes: Mapping[str, str] | None = None,
    ) -> None:
        target_identity = self._target_participant_identities.get(call_id)
        if not target_identity or getattr(participant, "identity", None) != target_identity:
            return
        attributes = getattr(participant, "attributes", None)
        status = None
        if changed_attributes is not None:
            status = changed_attributes.get("sip.callStatus")
        if status is None and isinstance(attributes, Mapping):
            status = attributes.get("sip.callStatus")
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in _SIP_CONNECTED_STATUSES:
            return
        if call_id in self._reported_sip_connected:
            return
        active_task = self._sip_connected_tasks.get(call_id)
        if active_task is not None and not active_task.done():
            return
        observer = self._sip_connected_observers.get(call_id)
        if observer is None:
            return
        task = asyncio.create_task(
            self._notify_sip_connected(call_id, normalized_status, observer)
        )
        self._sip_connected_tasks[call_id] = task

    async def _notify_sip_connected(
        self,
        call_id: str,
        status: str,
        observer: SipConnectedObserver,
    ) -> None:
        try:
            accepted = await observer(status)
            if accepted:
                self._reported_sip_connected.add(call_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("failed to persist SIP connected fact for call %s", call_id)
        finally:
            if self._sip_connected_tasks.get(call_id) is asyncio.current_task():
                self._sip_connected_tasks.pop(call_id, None)

    def _on_track_subscribed(
        self,
        call_id: str,
        track: Any,
        _publication: Any,
        participant: Any,
    ) -> None:
        stream_queue = self._track_streams.get(call_id)
        if stream_queue is None:
            return
        target_identity = self._target_participant_identities.get(call_id)
        participant_identity = getattr(participant, "identity", None)
        if target_identity and participant_identity != target_identity:
            return
        stream_queue.put(
            self.rtc.AudioStream(
                track,
                sample_rate=self.input_sample_rate_hz,
                num_channels=self.input_channels,
                frame_size_ms=self.frame_size_ms,
            )
        )

    def _to_rtc_audio_frame(self, frame: PcmAudioFrame) -> Any:
        samples_per_channel = len(frame.data) // (frame.sample_width_bytes * frame.channels)
        return self.rtc.AudioFrame(
            data=frame.data,
            sample_rate=frame.sample_rate_hz,
            num_channels=frame.channels,
            samples_per_channel=samples_per_channel,
        )

    @staticmethod
    def _from_rtc_audio_frame(frame: Any) -> PcmAudioFrame:
        return PcmAudioFrame(
            data=_memoryview_to_bytes(frame.data),
            sample_rate_hz=frame.sample_rate,
            channels=frame.num_channels,
            sample_width_bytes=2,
        )

    def _issue_agent_token(self, room_name: str, participant_identity: str) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self.api_key,
            "sub": participant_identity,
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=30)).timestamp()),
            "video": {
                "roomJoin": True,
                "room": room_name,
                "canPublish": True,
                "canSubscribe": True,
                "canPublishData": True,
            },
        }
        return jwt.encode(payload, self.api_secret, algorithm="HS256")

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    @staticmethod
    def _load_rtc_module() -> Any:
        try:
            from livekit import rtc
        except ImportError as exc:
            raise RuntimeError("缺少 livekit 依赖，无法接入 LiveKit Room 音频") from exc
        return rtc


class _StreamQueue:
    def __init__(self) -> None:
        self._queue: Any = _new_asyncio_queue()

    async def get(self) -> Any:
        return await self._queue.get()

    def put(self, value: Any) -> None:
        self._queue.put_nowait(value)

    async def close(self) -> None:
        self.put(None)


def _new_asyncio_queue() -> Any:
    import asyncio

    return asyncio.Queue()


def _memoryview_to_bytes(data: Any) -> bytes:
    if isinstance(data, memoryview):
        try:
            return data.cast("B").tobytes()
        except TypeError:
            return data.tobytes()
    return bytes(data)
