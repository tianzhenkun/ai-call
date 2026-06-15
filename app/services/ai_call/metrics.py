from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from math import ceil, floor
from typing import Any


def _elapsed_ms(start: datetime, end: datetime) -> int:
    return max(0, round((end - start).total_seconds() * 1000))


def _percentile(samples: list[int], percentile: int) -> int | None:
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile / 100
    lower_index = floor(rank)
    upper_index = ceil(rank)
    if lower_index == upper_index:
        return ordered[lower_index]
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    return round(lower + (upper - lower) * (rank - lower_index))


@dataclass(slots=True)
class CallMetrics:
    """单通会话的内存态观测指标，用于 Phase A 延迟验收。"""

    last_user_speech_stopped_at: datetime | None = None
    last_model_response_requested_at: datetime | None = None
    last_model_audio_delta_at: datetime | None = None
    last_model_first_audio_ms: int | None = None
    last_browser_first_audio_ms: int | None = None
    last_publish_delay_ms: int | None = None
    last_interrupt_confirmed_at: datetime | None = None
    last_interrupt_stop_ms: int | None = None
    audio_queue_depth: int = 0
    model_first_audio_samples_ms: list[int] = field(default_factory=list)
    browser_first_audio_samples_ms: list[int] = field(default_factory=list)

    def mark_user_speech_stopped(self, timestamp: datetime) -> None:
        self.last_user_speech_stopped_at = timestamp
        self.mark_model_response_requested(timestamp)

    def mark_model_response_requested(self, timestamp: datetime) -> None:
        self.last_model_response_requested_at = timestamp
        self.last_model_audio_delta_at = None
        self.last_model_first_audio_ms = None
        self.last_browser_first_audio_ms = None
        self.last_publish_delay_ms = None

    def mark_model_audio_delta(self, timestamp: datetime) -> None:
        self.last_model_audio_delta_at = timestamp
        if self.last_model_response_requested_at and self.last_model_first_audio_ms is None:
            self.last_model_first_audio_ms = _elapsed_ms(
                self.last_model_response_requested_at, timestamp
            )
            self.model_first_audio_samples_ms.append(self.last_model_first_audio_ms)

    def mark_audio_published(self, timestamp: datetime) -> None:
        if self.last_model_audio_delta_at and self.last_publish_delay_ms is None:
            self.last_publish_delay_ms = _elapsed_ms(self.last_model_audio_delta_at, timestamp)

    def mark_browser_first_audio(self, timestamp: datetime) -> None:
        if self.last_model_response_requested_at and self.last_browser_first_audio_ms is None:
            self.last_browser_first_audio_ms = _elapsed_ms(
                self.last_model_response_requested_at, timestamp
            )
            self.browser_first_audio_samples_ms.append(self.last_browser_first_audio_ms)

    def mark_interrupt_confirmed(self, timestamp: datetime) -> None:
        self.last_interrupt_confirmed_at = timestamp
        self.last_interrupt_stop_ms = None

    def mark_ai_audio_stopped(self, timestamp: datetime) -> None:
        if self.last_interrupt_confirmed_at:
            self.last_interrupt_stop_ms = _elapsed_ms(self.last_interrupt_confirmed_at, timestamp)

    def snapshot(self) -> dict[str, Any]:
        return {
            "lastModelFirstAudioMs": self.last_model_first_audio_ms,
            "lastBrowserFirstAudioMs": self.last_browser_first_audio_ms,
            "lastPublishDelayMs": self.last_publish_delay_ms,
            "lastInterruptStopMs": self.last_interrupt_stop_ms,
            "audioQueueDepth": self.audio_queue_depth,
            "modelFirstAudioCount": len(self.model_first_audio_samples_ms),
            "modelFirstAudioP50Ms": _percentile(self.model_first_audio_samples_ms, 50),
            "modelFirstAudioP90Ms": _percentile(self.model_first_audio_samples_ms, 90),
            "modelFirstAudioMaxMs": max(self.model_first_audio_samples_ms, default=None),
            "browserFirstAudioCount": len(self.browser_first_audio_samples_ms),
            "browserFirstAudioP50Ms": _percentile(self.browser_first_audio_samples_ms, 50),
            "browserFirstAudioP90Ms": _percentile(self.browser_first_audio_samples_ms, 90),
            "browserFirstAudioMaxMs": max(self.browser_first_audio_samples_ms, default=None),
        }
