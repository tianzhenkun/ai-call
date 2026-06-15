from __future__ import annotations

import base64
import binascii
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PcmAudioFrame:
    data: bytes
    sample_rate_hz: int
    channels: int
    sample_width_bytes: int = 2


class AudioBridgeError(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class PcmAudioBridge:
    def __init__(
        self,
        qwen_input_sample_rate_hz: int = 16000,
        qwen_output_sample_rate_hz: int = 24000,
        input_frame_duration_ms: int = 20,
        output_frame_duration_ms: int = 40,
        sample_width_bytes: int = 2,
    ) -> None:
        self.qwen_input_sample_rate_hz = qwen_input_sample_rate_hz
        self.qwen_output_sample_rate_hz = qwen_output_sample_rate_hz
        self.input_frame_duration_ms = input_frame_duration_ms
        self.output_frame_duration_ms = output_frame_duration_ms
        self.sample_width_bytes = sample_width_bytes

    def decode_qwen_output_delta(self, delta: str) -> PcmAudioFrame:
        try:
            pcm = base64.b64decode(delta, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise AudioBridgeError(
                reason="invalid_model_audio_delta",
                message="Qwen Realtime 输出音频不是合法 base64 PCM",
            ) from exc

        frame = PcmAudioFrame(
            data=pcm,
            sample_rate_hz=self.qwen_output_sample_rate_hz,
            channels=1,
            sample_width_bytes=self.sample_width_bytes,
        )
        self._validate_frame(frame)
        return frame

    def iter_qwen_input_chunks(self, frame: PcmAudioFrame) -> Iterator[bytes]:
        pcm = self.normalize_qwen_input(frame)
        chunk_size = self._qwen_input_chunk_size()
        for offset in range(0, len(pcm), chunk_size):
            chunk = pcm[offset : offset + chunk_size]
            if chunk:
                yield chunk

    def iter_output_playout_frames(self, frame: PcmAudioFrame) -> Iterator[PcmAudioFrame]:
        self._validate_frame(frame)
        chunk_size = self._output_playout_chunk_size(frame)
        for offset in range(0, len(frame.data), chunk_size):
            chunk = frame.data[offset : offset + chunk_size]
            if chunk:
                yield PcmAudioFrame(
                    data=chunk,
                    sample_rate_hz=frame.sample_rate_hz,
                    channels=frame.channels,
                    sample_width_bytes=frame.sample_width_bytes,
                )

    def normalize_qwen_input(self, frame: PcmAudioFrame) -> bytes:
        self._validate_frame(frame)
        if frame.sample_rate_hz == self.qwen_input_sample_rate_hz:
            return frame.data
        return self._rate_convert_pcm16_mono(frame)

    def _validate_frame(self, frame: PcmAudioFrame) -> None:
        if frame.channels != 1:
            raise AudioBridgeError(
                reason="unsupported_channel_count",
                message="Phase A 音频桥当前只支持 mono PCM",
            )
        if frame.sample_width_bytes != self.sample_width_bytes:
            raise AudioBridgeError(
                reason="unsupported_sample_width",
                message="Phase A 音频桥当前只支持 16-bit PCM",
            )
        if len(frame.data) % frame.sample_width_bytes != 0:
            raise AudioBridgeError(
                reason="invalid_pcm_frame_size",
                message="PCM 字节长度必须按 sample width 对齐",
            )

    def _rate_convert_pcm16_mono(self, frame: PcmAudioFrame) -> bytes:
        if frame.sample_rate_hz <= 0 or self.qwen_input_sample_rate_hz <= 0:
            raise AudioBridgeError(
                reason="unsupported_sample_rate",
                message="当前 PCM 采样率无法转换到 Qwen Realtime 输入采样率",
            )

        input_sample_count = len(frame.data) // frame.sample_width_bytes
        output_sample_count = (
            input_sample_count * self.qwen_input_sample_rate_hz // frame.sample_rate_hz
        )
        output = bytearray(output_sample_count * frame.sample_width_bytes)

        for output_index in range(output_sample_count):
            input_index = output_index * frame.sample_rate_hz // self.qwen_input_sample_rate_hz
            input_offset = input_index * frame.sample_width_bytes
            output_offset = output_index * frame.sample_width_bytes
            output[output_offset : output_offset + frame.sample_width_bytes] = frame.data[
                input_offset : input_offset + frame.sample_width_bytes
            ]

        return bytes(output)

    def _qwen_input_chunk_size(self) -> int:
        samples = self.qwen_input_sample_rate_hz * self.input_frame_duration_ms // 1000
        return samples * self.sample_width_bytes

    def _output_playout_chunk_size(self, frame: PcmAudioFrame) -> int:
        samples = frame.sample_rate_hz * self.output_frame_duration_ms // 1000
        return samples * frame.channels * frame.sample_width_bytes
