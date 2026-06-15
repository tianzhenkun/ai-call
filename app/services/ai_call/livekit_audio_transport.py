from __future__ import annotations

import inspect
import struct
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt

from app.services.ai_call.audio_bridge import PcmAudioFrame
from app.services.ai_call.session_registry import CallSession

RoomFactory = Callable[[], Any]


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
        input_sample_rate_hz: int = 48000,
        input_channels: int = 1,
        frame_size_ms: int = 20,
        fade_out_ms: int = 80,
    ) -> None:
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.rtc = rtc_module or self._load_rtc_module()
        self.room_factory = room_factory or self.rtc.Room
        self.output_sample_rate_hz = output_sample_rate_hz
        self.output_channels = output_channels
        self.input_sample_rate_hz = input_sample_rate_hz
        self.input_channels = input_channels
        self.frame_size_ms = frame_size_ms
        self.fade_out_ms = fade_out_ms
        self._rooms: dict[str, Any] = {}
        self._sources: dict[str, Any] = {}
        self._track_streams: dict[str, Any] = {}
        self._last_output_frames: dict[str, PcmAudioFrame] = {}

    async def start(self, session: CallSession) -> None:
        room = self.room_factory()
        source = self.rtc.AudioSource(self.output_sample_rate_hz, self.output_channels)
        track = self.rtc.LocalAudioTrack.create_audio_track("ai_audio", source)
        options = self.rtc.TrackPublishOptions(source=self.rtc.TrackSource.SOURCE_MICROPHONE)
        self._track_streams[session.call_id] = _StreamQueue()

        room.on(
            "track_subscribed",
            lambda track, publication, participant: self._on_track_subscribed(
                session.call_id,
                track,
                publication,
                participant,
            ),
        )
        await room.connect(
            self.livekit_url,
            self._issue_agent_token(session.room_name, f"agent-{session.call_id}"),
        )
        await room.local_participant.publish_track(track, options)

        self._rooms[session.call_id] = room
        self._sources[session.call_id] = source

    async def publish_audio(self, call_id: str, frame: PcmAudioFrame) -> None:
        source = self._sources[call_id]
        self._last_output_frames[call_id] = frame
        await source.capture_frame(self._to_rtc_audio_frame(frame))

    async def stop_audio(self, call_id: str) -> None:
        source = self._sources.get(call_id)
        if source is None:
            return
        # 打断时先清空播放队列，再补短淡出帧，减少残留音频和爆音。
        clear_queue = getattr(source, "clear_queue", None)
        if clear_queue is not None:
            await self._maybe_await(clear_queue())
        fade_frame = self._build_fade_out_frame(self._last_output_frames.get(call_id))
        if fade_frame is not None:
            with suppress(Exception):
                await source.capture_frame(self._to_rtc_audio_frame(fade_frame))

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

    def _on_track_subscribed(
        self,
        call_id: str,
        track: Any,
        _publication: Any,
        _participant: Any,
    ) -> None:
        stream_queue = self._track_streams.get(call_id)
        if stream_queue is None:
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

    def _build_fade_out_frame(self, frame: PcmAudioFrame | None) -> PcmAudioFrame | None:
        if (
            frame is None
            or self.fade_out_ms <= 0
            or frame.channels != 1
            or frame.sample_width_bytes != 2
        ):
            return None

        sample_count = len(frame.data) // frame.sample_width_bytes
        fade_sample_count = frame.sample_rate_hz * self.fade_out_ms // 1000
        fade_sample_count = min(sample_count, fade_sample_count)
        if fade_sample_count <= 0:
            return None

        source_offset = (sample_count - fade_sample_count) * frame.sample_width_bytes
        samples = struct.unpack(
            "<" + "h" * fade_sample_count,
            frame.data[source_offset:],
        )
        faded = bytearray(fade_sample_count * frame.sample_width_bytes)
        denominator = max(1, fade_sample_count - 1)
        for index, sample in enumerate(samples):
            scale = (denominator - index) / denominator
            struct.pack_into("<h", faded, index * frame.sample_width_bytes, round(sample * scale))

        return PcmAudioFrame(
            data=bytes(faded),
            sample_rate_hz=frame.sample_rate_hz,
            channels=frame.channels,
            sample_width_bytes=frame.sample_width_bytes,
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
