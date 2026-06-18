from __future__ import annotations

import inspect
import wave
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import jwt

from app.services.ai_call.audio_bridge import PcmAudioFrame

RoomFactory = Callable[[], Any]


class LiveKitSystemPromptPlayer:
    """播放系统固定提示音，不依赖 AI Agent 或模型会话。"""

    def __init__(
        self,
        *,
        livekit_url: str,
        api_key: str,
        api_secret: str,
        rtc_module: Any | None = None,
        room_factory: RoomFactory | None = None,
        frame_duration_ms: int = 40,
    ) -> None:
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.rtc = rtc_module or self._load_rtc_module()
        self.room_factory = room_factory or self.rtc.Room
        self.frame_duration_ms = max(10, frame_duration_ms)

    async def play(
        self,
        *,
        call_id: str,
        room_name: str,
        audio_path: str | Path,
    ) -> None:
        frames = self._load_wav_frames(audio_path)
        if not frames:
            return

        first = frames[0]
        room = self.room_factory()
        source = self.rtc.AudioSource(first.sample_rate_hz, first.channels)
        track = self.rtc.LocalAudioTrack.create_audio_track("system_audio", source)
        options = self.rtc.TrackPublishOptions(source=self.rtc.TrackSource.SOURCE_MICROPHONE)
        try:
            await room.connect(
                self.livekit_url,
                self._issue_token(room_name, f"system-audio-{call_id}"),
            )
            await room.local_participant.publish_track(track, options)
            for frame in frames:
                await source.capture_frame(self._to_rtc_audio_frame(frame))
            wait_for_playout = getattr(source, "wait_for_playout", None)
            if wait_for_playout is not None:
                await self._maybe_await(wait_for_playout())
        finally:
            with suppress(Exception):
                await self._maybe_await(room.disconnect())

    def _load_wav_frames(self, audio_path: str | Path) -> list[PcmAudioFrame]:
        path = Path(audio_path).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            raise FileNotFoundError(f"系统提示音不存在: {path}")

        with wave.open(str(path), "rb") as wav_file:
            channels = wav_file.getnchannels()
            sample_width = wav_file.getsampwidth()
            sample_rate = wav_file.getframerate()
            if channels != 1:
                raise ValueError("系统提示音必须是 mono wav")
            if sample_width != 2:
                raise ValueError("系统提示音必须是 16-bit PCM wav")
            if sample_rate <= 0:
                raise ValueError("系统提示音采样率无效")
            pcm = wav_file.readframes(wav_file.getnframes())

        chunk_size = sample_rate * self.frame_duration_ms // 1000 * channels * sample_width
        chunk_size = max(sample_width, chunk_size)
        return [
            PcmAudioFrame(
                data=pcm[offset : offset + chunk_size],
                sample_rate_hz=sample_rate,
                channels=channels,
                sample_width_bytes=sample_width,
            )
            for offset in range(0, len(pcm), chunk_size)
            if pcm[offset : offset + chunk_size]
        ]

    def _to_rtc_audio_frame(self, frame: PcmAudioFrame) -> Any:
        samples_per_channel = len(frame.data) // (frame.sample_width_bytes * frame.channels)
        return self.rtc.AudioFrame(
            data=frame.data,
            sample_rate=frame.sample_rate_hz,
            num_channels=frame.channels,
            samples_per_channel=samples_per_channel,
        )

    def _issue_token(self, room_name: str, participant_identity: str) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self.api_key,
            "sub": participant_identity,
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "video": {
                "roomJoin": True,
                "room": room_name,
                "canPublish": True,
                "canSubscribe": False,
                "canPublishData": False,
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
            raise RuntimeError("缺少 livekit 依赖，无法播放系统提示音") from exc
        return rtc
