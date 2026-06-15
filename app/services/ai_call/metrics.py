from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


def _elapsed_ms(start: datetime, end: datetime) -> int:
    return max(0, round((end - start).total_seconds() * 1000))


@dataclass(slots=True)
class CallMetrics:
    last_user_speech_stopped_at: datetime | None = None
    last_model_response_requested_at: datetime | None = None
    last_model_audio_delta_at: datetime | None = None
    last_model_first_audio_ms: int | None = None
    last_browser_first_audio_ms: int | None = None
    last_publish_delay_ms: int | None = None
    last_interrupt_confirmed_at: datetime | None = None
    last_interrupt_stop_ms: int | None = None
    audio_queue_depth: int = 0

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

    def mark_audio_published(self, timestamp: datetime) -> None:
        if self.last_model_audio_delta_at and self.last_publish_delay_ms is None:
            self.last_publish_delay_ms = _elapsed_ms(self.last_model_audio_delta_at, timestamp)

    def mark_browser_first_audio(self, timestamp: datetime) -> None:
        if self.last_model_response_requested_at and self.last_browser_first_audio_ms is None:
            self.last_browser_first_audio_ms = _elapsed_ms(
                self.last_model_response_requested_at, timestamp
            )

    def mark_interrupt_confirmed(self, timestamp: datetime) -> None:
        self.last_interrupt_confirmed_at = timestamp
        self.last_interrupt_stop_ms = None

    def mark_ai_audio_stopped(self, timestamp: datetime) -> None:
        if self.last_interrupt_confirmed_at:
            self.last_interrupt_stop_ms = _elapsed_ms(self.last_interrupt_confirmed_at, timestamp)

    def snapshot(self) -> dict[str, int | None]:
        return {
            "lastModelFirstAudioMs": self.last_model_first_audio_ms,
            "lastBrowserFirstAudioMs": self.last_browser_first_audio_ms,
            "lastPublishDelayMs": self.last_publish_delay_ms,
            "lastInterruptStopMs": self.last_interrupt_stop_ms,
            "audioQueueDepth": self.audio_queue_depth,
        }
