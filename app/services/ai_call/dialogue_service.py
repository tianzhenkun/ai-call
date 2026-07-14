from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import AiCallDialogueSegmentModel
from app.core.logger import log
from app.services.ai_call.dialogue_merge import (
    OFFLINE_ASR_SOURCE,
    QWEN_REALTIME_SOURCE,
    is_duplicate_dialogue_segment,
    normalize_dialogue_text,
)
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.transcript_trust import (
    is_realtime_transcript_semantically_rejected,
)

PERSISTED_DIALOGUE_STATUSES = frozenset({"final", "interrupted", "failed"})
TERMINAL_DIALOGUE_RECORD_STATUSES = frozenset({"completed", "failed"})
SHORT_ASR_NOISE_MAX_DURATION_MS = 100
SHORT_ASR_NOISE_MAX_TEXT_LENGTH = 2
SHORT_ASR_REAL_AUDIO_MIN_SPAN_MS = 300
REALTIME_ASR_PREFIX_DUPLICATE_START_GAP_MS = 1500
UNHEARD_AUDIO_DROP_REASONS = frozenset({"session_not_ai_speaking"})
VALID_SHORT_CUSTOMER_UTTERANCES = frozenset({
    "嗯",
    "好",
    "好的",
    "行",
    "可以",
    "对",
    "对的",
    "对呀",
    "是",
    "不是",
    "不行",
    "不要",
    "不用",
    "你好",
})


@dataclass(slots=True)
class DialogueSegmentSnapshot:
    call_id: str
    segment_no: int
    speaker_type: str
    speaker_identity: str | None
    source: str
    source_segment_id: str
    text: str
    segment_status: str
    started_at: datetime | None
    ended_at: datetime | None
    duration_ms: int | None = None
    audio_start_ms: int | None = None
    audio_end_ms: int | None = None
    failure_stage: str | None = None
    failure_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "callId": self.call_id,
            "segmentNo": self.segment_no,
            "speakerType": self.speaker_type,
            "speakerIdentity": self.speaker_identity,
            "source": self.source,
            "sourceSegmentId": self.source_segment_id,
            "text": self.text,
            "segmentStatus": self.segment_status,
            "startedAt": self.started_at,
            "endedAt": self.ended_at,
            "durationMs": self.duration_ms,
            "audioStartMs": self.audio_start_ms,
            "audioEndMs": self.audio_end_ms,
            "failureStage": self.failure_stage,
            "failureMessage": self.failure_message,
        }


@dataclass(slots=True)
class _PendingDialogueSegment:
    snapshot: DialogueSegmentSnapshot
    replace_on_next_text: bool = False
    finalized: bool = False
    suppress_persist: bool = False


class AiCallDialogueRuntimeStore:
    """运行态对话文本聚合，只保存轻量预览。"""

    MERGE_WINDOW_MS = 500
    AI_INTERRUPTED_DUPLICATE_WINDOW_MS = 5000

    def __init__(self) -> None:
        self._segments_by_call: dict[str, list[DialogueSegmentSnapshot]] = {}
        self._segments_by_source_key: dict[tuple[str, str, str], _PendingDialogueSegment] = {}
        self._active_key_by_call_speaker: dict[tuple[str, str], tuple[str, str, str]] = {}
        self._merged_key_targets: dict[tuple[str, str, str], tuple[str, str, str]] = {}
        self._interrupted_ai_source_ids_by_call: dict[str, set[str]] = {}
        self._interrupted_ai_response_ids_by_call: dict[str, set[str]] = {}
        self._unheard_ai_source_ids_by_call: dict[str, set[str]] = {}
        self._unheard_ai_response_ids_by_call: dict[str, set[str]] = {}
        self._ai_source_ids_by_response_id: dict[str, dict[str, set[str]]] = {}
        self._next_no_by_call: dict[str, int] = {}
        self._persist_listeners: list[Callable[[DialogueSegmentSnapshot], None]] = []
        self._attached_stores: dict[int, InMemoryEventStore] = {}

    def attach_event_store(self, event_store: InMemoryEventStore) -> None:
        store_id = id(event_store)
        if store_id in self._attached_stores:
            return
        event_store.add_listener(self.handle_event)
        self._attached_stores[store_id] = event_store

    def detach_all(self) -> None:
        for event_store in self._attached_stores.values():
            event_store.remove_listener(self.handle_event)
        self._attached_stores.clear()

    def add_persist_listener(self, listener: Callable[[DialogueSegmentSnapshot], None]) -> None:
        if listener not in self._persist_listeners:
            self._persist_listeners.append(listener)

    def remove_persist_listener(self, listener: Callable[[DialogueSegmentSnapshot], None]) -> None:
        if listener in self._persist_listeners:
            self._persist_listeners.remove(listener)

    def handle_event(self, event: AiCallEvent) -> None:
        if event.type == "user_speech_started":
            self._ensure_pending(
                event,
                speaker_type="customer",
                source=QWEN_REALTIME_SOURCE,
                started_at=event.timestamp,
            )
        elif event.type == "user_transcript_delta":
            self._apply_text_delta(
                event,
                speaker_type="customer",
                source=QWEN_REALTIME_SOURCE,
                text=self._user_transcript_text(event.payload, event_type=event.type),
                replace=self._is_qwen_stash_payload(event.payload),
            )
        elif event.type == "user_transcript_done":
            pending = self._apply_text_delta(
                event,
                speaker_type="customer",
                source=QWEN_REALTIME_SOURCE,
                text=self._user_transcript_text(event.payload, event_type=event.type),
                replace=True,
            )
            if pending is not None:
                self._finalize_segment(pending, event.timestamp, "final")
        elif event.type == "user_speech_stopped":
            self._mark_speech_stopped(event)
        elif event.type == "user_transcript_failed":
            self._fail_customer_transcript(event)
        elif event.type == "ai_transcript_delta":
            if self._is_unheard_ai_source_event(event):
                self._suppress_unheard_ai_pending(event)
                return
            self._apply_text_delta(
                event,
                speaker_type="ai",
                source=QWEN_REALTIME_SOURCE,
                text=self._ai_transcript_text(event.payload),
                replace=False,
            )
        elif event.type == "ai_transcript_done":
            if self._is_unheard_ai_source_event(event):
                self._suppress_unheard_ai_pending(event)
                return
            done_text = self._ai_transcript_text(event.payload)
            pending = None
            if done_text:
                pending = self._apply_text_delta(
                    event,
                    speaker_type="ai",
                    source=QWEN_REALTIME_SOURCE,
                    text=done_text,
                    replace=True,
                )
            if pending is not None:
                status = (
                    "interrupted" if self._is_interrupted_ai_source_event(event) else "final"
                )
                self._finalize_segment(pending, event.timestamp, status)
        elif event.type == "model_response_started":
            self._finalize_stable_customer_turn(event.call_id, event.timestamp)
        elif event.type == "model_response_done":
            self._finalize_stable_customer_turn(event.call_id, event.timestamp)
            if self._is_unheard_ai_source_event(event):
                self._suppress_unheard_ai_pending(event)
                return
            if not self._should_ignore_model_response_done(event):
                status = (
                    "interrupted" if self._is_interrupted_ai_source_event(event) else "final"
                )
                self._finalize_event_pending(event, "ai", status)
        elif event.type == "stale_audio_dropped":
            self._mark_unheard_ai_source(event)
        elif event.type == "response_generation_invalidated":
            self._mark_interrupted_ai_source(event)
            self._mark_finalized_interrupted_ai_sources(event)
            self._finalize_interrupted_ai_pending(event)
        elif event.type == "interrupt_confirmed":
            self._finalize_pending(event.call_id, "ai", event.timestamp, "interrupted")
        elif event.type in {"session_completed", "session_failed"}:
            self._finalize_all_pending(event.call_id, event.timestamp)

    def list_preview(self, call_id: str) -> list[DialogueSegmentSnapshot]:
        return [
            segment
            for segment in self._segments_by_call.get(call_id, [])
            if segment.text or segment.segment_status == "failed"
            if not self._is_unheard_ai_source_id(call_id, segment.source_segment_id)
        ]

    def _apply_text_delta(
        self,
        event: AiCallEvent,
        *,
        speaker_type: str,
        source: str,
        text: str,
        replace: bool,
    ) -> _PendingDialogueSegment | None:
        if not text:
            return None
        pending = self._ensure_pending(
            event,
            speaker_type=speaker_type,
            source=source,
            started_at=None,
        )
        if pending.finalized:
            return None
        snapshot = pending.snapshot
        if replace or pending.replace_on_next_text:
            snapshot.text = text
        else:
            snapshot.text = f"{snapshot.text}{text}"
        pending.replace_on_next_text = False
        snapshot.segment_status = "partial"
        return pending

    def _finalize_pending(
        self,
        call_id: str,
        speaker_type: str,
        ended_at: datetime,
        status: str,
    ) -> None:
        key = self._active_key_by_call_speaker.get((call_id, speaker_type))
        if key is None:
            return
        pending = self._pending_for_key(key)
        if pending is None:
            return
        self._finalize_segment(pending, ended_at, status)

    def _finalize_event_pending(
        self,
        event: AiCallEvent,
        speaker_type: str,
        status: str,
    ) -> None:
        source_segment_id = self._source_segment_id(event)
        if source_segment_id:
            key = self._resolve_key((event.call_id, speaker_type, source_segment_id))
            pending = self._pending_for_key(key)
            if pending is not None:
                if pending.finalized:
                    self._suppress_unfinalized_response_siblings(
                        event,
                        speaker_type=speaker_type,
                        keep_source_segment_id=source_segment_id,
                        keep_text=pending.snapshot.text,
                    )
                    return
                self._finalize_segment(pending, event.timestamp, status)
                self._suppress_unfinalized_response_siblings(
                    event,
                    speaker_type=speaker_type,
                    keep_source_segment_id=source_segment_id,
                    keep_text=pending.snapshot.text,
                )
                return
        self._finalize_pending(event.call_id, speaker_type, event.timestamp, status)

    def _finalize_stable_customer_turn(self, call_id: str, ended_at: datetime) -> None:
        key = self._active_key_by_call_speaker.get((call_id, "customer"))
        if key is None:
            return
        pending = self._pending_for_key(key)
        if pending is None or pending.finalized:
            return
        snapshot = pending.snapshot
        if snapshot.text and snapshot.ended_at is not None:
            self._finalize_segment(pending, ended_at, "final")

    def _finalize_segment(
        self,
        pending: _PendingDialogueSegment,
        ended_at: datetime,
        status: str,
    ) -> None:
        if pending.finalized:
            return
        snapshot = pending.snapshot
        if not snapshot.text:
            return
        final_ended_at = snapshot.ended_at or ended_at
        snapshot.segment_status = status
        snapshot.ended_at = final_ended_at
        snapshot.duration_ms = self._duration_ms(snapshot.started_at, final_ended_at)
        pending.finalized = True
        merged_snapshot = self._merge_into_previous_if_needed(pending)
        if pending.suppress_persist:
            return
        if merged_snapshot is not None:
            self._notify_persist(merged_snapshot)
            return
        self._notify_persist(snapshot)

    def _fail_customer_transcript(self, event: AiCallEvent) -> None:
        pending = self._ensure_pending(
            event,
            speaker_type="customer",
            source=QWEN_REALTIME_SOURCE,
            started_at=None,
        )
        snapshot = pending.snapshot
        snapshot.segment_status = "failed"
        snapshot.ended_at = event.timestamp
        snapshot.duration_ms = self._duration_ms(snapshot.started_at, event.timestamp)
        snapshot.failure_stage = "asr"
        snapshot.failure_message = self._failure_message(event.payload)
        pending.finalized = True
        self._notify_persist(snapshot)

    def _ensure_pending(
        self,
        event: AiCallEvent,
        *,
        speaker_type: str,
        source: str,
        started_at: datetime | None,
    ) -> _PendingDialogueSegment:
        source_segment_id = self._source_segment_id(event)
        active_key = self._active_key_by_call_speaker.get((event.call_id, speaker_type))
        if source_segment_id is None and active_key is not None:
            key = self._resolve_key(active_key)
            source_segment_id = key[2]
        else:
            source_segment_id = source_segment_id or event.event_id[:128]
            key = self._resolve_key((event.call_id, speaker_type, source_segment_id))
        pending = self._pending_for_key(key)
        if pending is not None:
            if started_at and pending.snapshot.started_at is None:
                pending.snapshot.started_at = started_at
            if event.type == "user_speech_started":
                pending.snapshot.audio_start_ms = self._payload_int(
                    event.payload,
                    "audio_start_ms",
                )
            self._active_key_by_call_speaker[(event.call_id, speaker_type)] = key
            self._remember_ai_response_source(event, speaker_type, source_segment_id)
            return pending

        segment_no = self._next_segment_no(event.call_id)
        snapshot = DialogueSegmentSnapshot(
            call_id=event.call_id,
            segment_no=segment_no,
            speaker_type=speaker_type,
            speaker_identity=self._speaker_identity(event.payload),
            source=source,
            source_segment_id=source_segment_id,
            text="",
            segment_status="partial",
            started_at=started_at or event.timestamp,
            ended_at=None,
            audio_start_ms=self._payload_int(event.payload, "audio_start_ms"),
        )
        pending = _PendingDialogueSegment(snapshot=snapshot)
        self._segments_by_source_key[key] = pending
        self._active_key_by_call_speaker[(event.call_id, speaker_type)] = key
        self._segments_by_call.setdefault(event.call_id, []).append(snapshot)
        self._remember_ai_response_source(event, speaker_type, source_segment_id)
        return pending

    def _mark_speech_stopped(self, event: AiCallEvent) -> None:
        pending = self._ensure_pending(
            event,
            speaker_type="customer",
            source=QWEN_REALTIME_SOURCE,
            started_at=None,
        )
        snapshot = pending.snapshot
        snapshot.ended_at = event.timestamp
        snapshot.audio_end_ms = self._payload_int(event.payload, "audio_end_ms")
        snapshot.duration_ms = self._duration_ms(snapshot.started_at, event.timestamp)

    def _finalize_all_pending(self, call_id: str, ended_at: datetime) -> None:
        for key, pending in list(self._segments_by_source_key.items()):
            if key[0] == call_id:
                status = pending.snapshot.segment_status
                if status not in {"interrupted", "failed"}:
                    status = (
                        "interrupted"
                        if self._is_interrupted_ai_source_id(
                            call_id,
                            pending.snapshot.source_segment_id,
                        )
                        else "final"
                    )
                self._finalize_segment(pending, ended_at, status)

    def _pending_for_key(
        self,
        key: tuple[str, str, str],
    ) -> _PendingDialogueSegment | None:
        return self._segments_by_source_key.get(self._resolve_key(key))

    def _resolve_key(self, key: tuple[str, str, str]) -> tuple[str, str, str]:
        while key in self._merged_key_targets:
            key = self._merged_key_targets[key]
        return key

    def _merge_into_previous_if_needed(
        self,
        pending: _PendingDialogueSegment,
    ) -> DialogueSegmentSnapshot | None:
        snapshot = pending.snapshot
        rows = self._segments_by_call.get(snapshot.call_id, [])
        try:
            current_index = rows.index(snapshot)
        except ValueError:
            return None
        if snapshot.speaker_type == "ai":
            return self._suppress_interrupted_ai_duplicate(
                rows=rows,
                current_index=current_index,
                snapshot=snapshot,
            )
        if snapshot.speaker_type != "customer":
            return None
        for candidate in reversed(rows[:current_index]):
            if self._can_merge_adjacent(candidate, snapshot):
                self._merge_snapshots(candidate, snapshot)
                rows.remove(snapshot)
                current_key = (
                    snapshot.call_id,
                    snapshot.speaker_type,
                    snapshot.source_segment_id,
                )
                target_key = (
                    candidate.call_id,
                    candidate.speaker_type,
                    candidate.source_segment_id,
                )
                self._merged_key_targets[current_key] = target_key
                return candidate
            if candidate.speaker_type == snapshot.speaker_type:
                break
        return None

    def _suppress_interrupted_ai_duplicate(
        self,
        *,
        rows: list[DialogueSegmentSnapshot],
        current_index: int,
        snapshot: DialogueSegmentSnapshot,
    ) -> DialogueSegmentSnapshot | None:
        for candidate in reversed(rows[:current_index]):
            if candidate.speaker_type != "ai":
                break
            if not self._can_suppress_interrupted_ai_duplicate(candidate, snapshot):
                break
            rows.remove(snapshot)
            current_key = (
                snapshot.call_id,
                snapshot.speaker_type,
                snapshot.source_segment_id,
            )
            target_key = (
                candidate.call_id,
                candidate.speaker_type,
                candidate.source_segment_id,
            )
            self._merged_key_targets[current_key] = target_key
            pending = self._segments_by_source_key.get(current_key)
            if pending is not None:
                pending.suppress_persist = True
            return candidate
        return None

    def _can_merge_adjacent(
        self,
        previous: DialogueSegmentSnapshot,
        current: DialogueSegmentSnapshot,
    ) -> bool:
        if previous.speaker_type != current.speaker_type or previous.source != current.source:
            return False
        previous_text = self._normalize_text(previous.text)
        current_text = self._normalize_text(current.text)
        if not previous_text or not current_text:
            return False
        if (
            previous_text != current_text
            and previous_text not in current_text
            and current_text not in previous_text
        ):
            return False
        previous_end = previous.ended_at or previous.started_at
        current_start = current.started_at
        if previous_end is None or current_start is None:
            return False
        gap_ms = self._duration_ms(previous_end, current_start)
        return gap_ms is not None and 0 <= gap_ms <= self.MERGE_WINDOW_MS

    def _can_suppress_interrupted_ai_duplicate(
        self,
        previous: DialogueSegmentSnapshot,
        current: DialogueSegmentSnapshot,
    ) -> bool:
        if (
            previous.speaker_type != "ai"
            or current.speaker_type != "ai"
            or previous.source != current.source
            or previous.segment_status != "interrupted"
        ):
            return False
        previous_text = self._normalize_text(previous.text)
        current_text = self._normalize_text(current.text)
        if not previous_text or not current_text:
            return False
        if (
            previous_text != current_text
            and previous_text not in current_text
            and current_text not in previous_text
        ):
            return False
        previous_end = previous.ended_at or previous.started_at
        current_start = current.started_at
        if previous_end is None or current_start is None:
            return False
        gap_ms = self._duration_ms(previous_end, current_start)
        return gap_ms is not None and 0 <= gap_ms <= self.AI_INTERRUPTED_DUPLICATE_WINDOW_MS

    def _merge_snapshots(
        self,
        previous: DialogueSegmentSnapshot,
        current: DialogueSegmentSnapshot,
    ) -> None:
        if len(self._normalize_text(current.text)) >= len(self._normalize_text(previous.text)):
            previous.text = current.text
        if current.ended_at and (previous.ended_at is None or current.ended_at > previous.ended_at):
            previous.ended_at = current.ended_at
        if previous.audio_start_ms is None:
            previous.audio_start_ms = current.audio_start_ms
        if current.audio_end_ms is not None:
            previous.audio_end_ms = current.audio_end_ms
        previous.duration_ms = self._duration_ms(previous.started_at, previous.ended_at)

    def _next_segment_no(self, call_id: str) -> int:
        value = self._next_no_by_call.get(call_id, 1)
        self._next_no_by_call[call_id] = value + 1
        return value

    def _notify_persist(self, snapshot: DialogueSegmentSnapshot) -> None:
        if snapshot.segment_status not in PERSISTED_DIALOGUE_STATUSES:
            return
        for listener in tuple(self._persist_listeners):
            try:
                listener(snapshot)
            except Exception:
                continue

    @staticmethod
    def _speaker_identity(payload: dict[str, Any]) -> str | None:
        value = payload.get("participantIdentity") or payload.get("identity")
        return str(value) if value else None

    @staticmethod
    def _user_transcript_text(payload: dict[str, Any], *, event_type: str) -> str:
        if is_realtime_transcript_semantically_rejected(payload):
            return ""
        if event_type == "user_transcript_done":
            value = payload.get("transcript") or payload.get("text") or payload.get("delta")
            return value.strip() if isinstance(value, str) else ""
        text = payload.get("text")
        stash = payload.get("stash")
        if isinstance(text, str) or isinstance(stash, str):
            return f"{text or ''}{stash or ''}".strip()
        value = payload.get("delta")
        return value.strip() if isinstance(value, str) else ""

    @staticmethod
    def _ai_transcript_text(payload: dict[str, Any]) -> str:
        value = payload.get("transcript") or payload.get("text") or payload.get("delta")
        return value.strip() if isinstance(value, str) else ""

    @staticmethod
    def _is_qwen_stash_payload(payload: dict[str, Any]) -> bool:
        return "text" in payload or "stash" in payload

    @staticmethod
    def _should_ignore_model_response_done(event: AiCallEvent) -> bool:
        response = event.payload.get("response")
        if not isinstance(response, dict):
            return False
        status = response.get("status")
        if isinstance(status, str) and status.lower() in {
            "cancelled",
            "canceled",
            "failed",
            "incomplete",
        }:
            return True
        return False

    def _mark_interrupted_ai_source(self, event: AiCallEvent) -> None:
        source_segment_id = self._source_segment_id(event)
        if source_segment_id:
            self._interrupted_ai_source_ids_by_call.setdefault(event.call_id, set()).add(
                source_segment_id
            )
        response_id = self._response_id(event)
        if response_id:
            self._interrupted_ai_response_ids_by_call.setdefault(event.call_id, set()).add(
                response_id
            )
            source_ids = self._ai_source_ids_by_response_id.get(event.call_id, {}).get(
                response_id,
                set(),
            )
            self._interrupted_ai_source_ids_by_call.setdefault(event.call_id, set()).update(
                source_ids
            )

    def _mark_unheard_ai_source(self, event: AiCallEvent) -> None:
        if not self._is_unheard_audio_drop(event):
            return
        response_id = self._response_id(event)
        if response_id:
            self._unheard_ai_response_ids_by_call.setdefault(event.call_id, set()).add(
                response_id
            )
            for source_id in self._ai_source_ids_by_response_id.get(event.call_id, {}).get(
                response_id,
                set(),
            ):
                self._mark_unheard_ai_source_id(event.call_id, source_id)
        source_segment_id = self._item_source_segment_id(event.payload)
        if source_segment_id:
            self._mark_unheard_ai_source_id(event.call_id, source_segment_id)

    def _suppress_unheard_ai_pending(self, event: AiCallEvent) -> None:
        source_segment_id = self._source_segment_id(event)
        if source_segment_id:
            self._mark_unheard_ai_source_id(event.call_id, source_segment_id)
        response_id = self._response_id(event)
        if not response_id:
            return
        self._unheard_ai_response_ids_by_call.setdefault(event.call_id, set()).add(response_id)
        for source_id in self._ai_source_ids_by_response_id.get(event.call_id, {}).get(
            response_id,
            set(),
        ):
            self._mark_unheard_ai_source_id(event.call_id, source_id)

    def _mark_unheard_ai_source_id(self, call_id: str, source_segment_id: str) -> None:
        self._unheard_ai_source_ids_by_call.setdefault(call_id, set()).add(source_segment_id)
        pending = self._pending_for_key((call_id, "ai", source_segment_id))
        if pending is not None:
            pending.suppress_persist = True

    def _suppress_unfinalized_response_siblings(
        self,
        event: AiCallEvent,
        *,
        speaker_type: str,
        keep_source_segment_id: str,
        keep_text: str,
    ) -> None:
        if speaker_type != "ai":
            return
        response_id = self._response_id(event)
        if not response_id:
            return
        source_ids = self._ai_source_ids_by_response_id.get(event.call_id, {}).get(
            response_id,
            set(),
        )
        for source_id in tuple(source_ids):
            if source_id == keep_source_segment_id:
                continue
            pending = self._pending_for_key((event.call_id, "ai", source_id))
            if pending is None or pending.finalized:
                continue
            if not self._can_suppress_ai_response_sibling(pending.snapshot.text, keep_text):
                continue
            pending.suppress_persist = True
            self._remove_snapshot(pending.snapshot)
            active_key = self._active_key_by_call_speaker.get((event.call_id, "ai"))
            if active_key == (event.call_id, "ai", source_id):
                self._active_key_by_call_speaker.pop((event.call_id, "ai"), None)

    def _remove_snapshot(self, snapshot: DialogueSegmentSnapshot) -> None:
        rows = self._segments_by_call.get(snapshot.call_id)
        if not rows:
            return
        try:
            rows.remove(snapshot)
        except ValueError:
            return

    @classmethod
    def _can_suppress_ai_response_sibling(cls, sibling_text: str, keep_text: str) -> bool:
        sibling = cls._normalize_text(sibling_text)
        keep = cls._normalize_text(keep_text)
        if not sibling or not keep:
            return True
        return sibling == keep or sibling in keep or keep in sibling

    @staticmethod
    def _is_unheard_audio_drop(event: AiCallEvent) -> bool:
        reason = event.payload.get("reason")
        return isinstance(reason, str) and reason in UNHEARD_AUDIO_DROP_REASONS

    def _finalize_interrupted_ai_pending(self, event: AiCallEvent) -> None:
        key = self._active_key_by_call_speaker.get((event.call_id, "ai"))
        if key is None:
            return
        pending = self._pending_for_key(key)
        if pending is None or pending.finalized:
            return
        response_id = self._response_id(event)
        mapped_source_ids = (
            self._ai_source_ids_by_response_id.get(event.call_id, {}).get(response_id, set())
            if response_id
            else set()
        )
        source_id = pending.snapshot.source_segment_id
        if mapped_source_ids and source_id not in mapped_source_ids:
            return
        self._finalize_segment(pending, event.timestamp, "interrupted")

    def _mark_finalized_interrupted_ai_sources(self, event: AiCallEvent) -> None:
        source_ids = self._interrupted_ai_source_ids_by_call.get(event.call_id, set())
        if not source_ids:
            return
        for source_id in tuple(source_ids):
            pending = self._pending_for_key((event.call_id, "ai", source_id))
            if pending is None or not pending.finalized or pending.suppress_persist:
                continue
            snapshot = pending.snapshot
            if snapshot.segment_status == "interrupted":
                continue
            snapshot.segment_status = "interrupted"
            self._notify_persist(snapshot)

    def _is_interrupted_ai_source_event(self, event: AiCallEvent) -> bool:
        source_segment_id = self._source_segment_id(event)
        if source_segment_id and self._is_interrupted_ai_source_id(
            event.call_id,
            source_segment_id,
        ):
            return True
        response_id = self._response_id(event)
        if not response_id:
            return False
        return response_id in self._interrupted_ai_response_ids_by_call.get(
            event.call_id,
            set(),
        )

    def _is_interrupted_ai_source_id(self, call_id: str, source_segment_id: str) -> bool:
        return source_segment_id in self._interrupted_ai_source_ids_by_call.get(call_id, set())

    def _is_unheard_ai_source_event(self, event: AiCallEvent) -> bool:
        source_segment_id = self._source_segment_id(event)
        if source_segment_id and self._is_unheard_ai_source_id(
            event.call_id,
            source_segment_id,
        ):
            return True
        response_id = self._response_id(event)
        if not response_id:
            return False
        return response_id in self._unheard_ai_response_ids_by_call.get(event.call_id, set())

    def _is_unheard_ai_source_id(self, call_id: str, source_segment_id: str) -> bool:
        return source_segment_id in self._unheard_ai_source_ids_by_call.get(call_id, set())

    def _remember_ai_response_source(
        self,
        event: AiCallEvent,
        speaker_type: str,
        source_segment_id: str | None,
    ) -> None:
        if speaker_type != "ai" or not source_segment_id:
            return
        response_id = self._response_id(event)
        if not response_id:
            return
        self._ai_source_ids_by_response_id.setdefault(event.call_id, {}).setdefault(
            response_id,
            set(),
        ).add(source_segment_id)
        if self._is_unheard_ai_source_event(event):
            self._mark_unheard_ai_source_id(event.call_id, source_segment_id)

    @staticmethod
    def _source_segment_id(event: AiCallEvent) -> str | None:
        payload = event.payload
        for key in ("item_id", "itemId", "response_id", "responseId"):
            value = payload.get(key)
            if value:
                return str(value)[:128]
        item = payload.get("item")
        if isinstance(item, dict) and item.get("id"):
            return str(item["id"])[:128]
        response = payload.get("response")
        if isinstance(response, dict):
            output = response.get("output")
            if isinstance(output, list):
                for item in output:
                    if isinstance(item, dict) and item.get("id"):
                        return str(item["id"])[:128]
            if response.get("id"):
                return str(response["id"])[:128]
        return None

    @staticmethod
    def _item_source_segment_id(payload: dict[str, Any]) -> str | None:
        for key in ("item_id", "itemId"):
            value = payload.get(key)
            if value:
                return str(value)[:128]
        item = payload.get("item")
        if isinstance(item, dict) and item.get("id"):
            return str(item["id"])[:128]
        return None

    @staticmethod
    def _response_id(event: AiCallEvent) -> str | None:
        payload = event.payload
        for key in ("response_id", "responseId"):
            value = payload.get(key)
            if value:
                return str(value)[:128]
        response = payload.get("response")
        if isinstance(response, dict) and response.get("id"):
            return str(response["id"])[:128]
        return None

    @staticmethod
    def _payload_int(payload: dict[str, Any], key: str) -> int | None:
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def _normalize_text(text: str) -> str:
        return "".join(char for char in text.strip() if char not in "，。！？,.!?；;：:、 ")

    @staticmethod
    def _failure_message(payload: dict[str, Any]) -> str | None:
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("code")
            return str(message) if message else None
        if isinstance(error, str):
            return error
        return None

    @staticmethod
    def _duration_ms(started_at: datetime | None, ended_at: datetime) -> int | None:
        if started_at is None:
            return None
        start = started_at if started_at.tzinfo else started_at.replace(tzinfo=timezone.utc)
        end = ended_at if ended_at.tzinfo else ended_at.replace(tzinfo=timezone.utc)
        return max(0, int((end - start).total_seconds() * 1000))


@dataclass(slots=True)
class QueuedDialogueSegment:
    segment: DialogueSegmentSnapshot
    attempts: int = 0


class AiCallDialoguePersistenceWorker:
    """后台持久化最终对话段，避免数据库 I/O 进入实时通话路径。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        queue_max_size: int = 5000,
        batch_size: int = 50,
        flush_interval_seconds: float = 0.2,
        max_retries: int = 2,
    ) -> None:
        self.session_factory = session_factory
        self.batch_size = max(1, batch_size)
        self.flush_interval_seconds = max(0.05, flush_interval_seconds)
        self.max_retries = max(0, max_retries)
        self.queue: asyncio.Queue[QueuedDialogueSegment] = asyncio.Queue(maxsize=queue_max_size)
        self.dropped_count = 0
        self.failed_count = 0
        self.persisted_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._attached_runtime_stores: dict[int, AiCallDialogueRuntimeStore] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-dialogue-persistence-worker",
        )

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 对话文本持久化队列关闭超时，仍有片段未落库")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def attach_runtime_store(self, runtime_store: AiCallDialogueRuntimeStore) -> None:
        store_id = id(runtime_store)
        if store_id in self._attached_runtime_stores:
            return
        runtime_store.add_persist_listener(self.enqueue)
        self._attached_runtime_stores[store_id] = runtime_store

    def detach_all(self) -> None:
        for runtime_store in self._attached_runtime_stores.values():
            runtime_store.remove_persist_listener(self.enqueue)
        self._attached_runtime_stores.clear()

    def enqueue(self, segment: DialogueSegmentSnapshot) -> None:
        if segment.segment_status not in PERSISTED_DIALOGUE_STATUSES:
            return
        try:
            self.queue.put_nowait(QueuedDialogueSegment(segment=segment))
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning(
                "AI Call 对话文本持久化队列已满，丢弃片段: callId={}, segmentNo={}",
                segment.call_id,
                segment.segment_no,
            )

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            batch = await self._next_batch()
            if not batch:
                continue
            await self._persist_batch(batch)

    async def _next_batch(self) -> list[QueuedDialogueSegment]:
        try:
            first = await asyncio.wait_for(
                self.queue.get(),
                timeout=self.flush_interval_seconds,
            )
        except TimeoutError:
            return []
        batch = [first]
        while len(batch) < self.batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _persist_batch(self, batch: list[QueuedDialogueSegment]) -> None:
        try:
            async with self.session_factory() as db:
                async with db.begin():
                    service = AiCallDialogueService(AiCallRecordRepository(db))
                    for item in batch:
                        await service.persist_snapshot(item.segment)
                        self.persisted_count += 1
        except Exception as exc:
            self.failed_count += len(batch)
            log.error(
                "AI Call 对话文本后台落库失败: count={}, errorType={}, message={}",
                len(batch),
                type(exc).__name__,
                str(exc),
            )
            await self._retry_batch(batch)
        finally:
            for _item in batch:
                self.queue.task_done()

    async def _retry_batch(self, batch: list[QueuedDialogueSegment]) -> None:
        retry_items = [
            QueuedDialogueSegment(segment=item.segment, attempts=item.attempts + 1)
            for item in batch
            if item.attempts < self.max_retries
        ]
        if not retry_items:
            return
        await asyncio.sleep(min(0.2 * retry_items[0].attempts, 1.0))
        for item in retry_items:
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                self.dropped_count += 1


class AiCallDialogueService:
    """B2.5 对话文本查询和持久化服务。"""

    def __init__(
        self,
        repository: AiCallRecordRepository,
        runtime_store: AiCallDialogueRuntimeStore | None = None,
    ) -> None:
        self.repository = repository
        self.runtime_store = runtime_store

    async def list_preview_segments(self, call_id: str) -> dict[str, Any]:
        if self.runtime_store is not None:
            record = await self.repository.get_record(call_id)
            if record is not None and record.status in TERMINAL_DIALOGUE_RECORD_STATUSES:
                return await self._persisted_segment_list(call_id)
            rows = self.runtime_store.list_preview(call_id)
            if rows:
                rows = self._canonical_preview_segments(rows)
                return {
                    "rows": [row.as_dict() for row in rows],
                    "total": len(rows),
                }
        return await self._persisted_segment_list(call_id)

    async def _persisted_segment_list(self, call_id: str) -> dict[str, Any]:
        rows = await self.list_persisted_segments(call_id)
        return {
            "rows": [self.segment_to_dict(row) for row in rows],
            "total": len(rows),
        }

    async def list_persisted_segments(
        self,
        call_id: str,
        *,
        speaker_type: str | None = None,
        limit: int = 1000,
    ) -> list[AiCallDialogueSegmentModel]:
        rows = await self.repository.list_dialogue_segments(
            call_id,
            speaker_type=speaker_type,
            limit=limit,
        )
        return self._canonical_segments(rows)

    async def persist_snapshot(
        self,
        snapshot: DialogueSegmentSnapshot,
    ) -> AiCallDialogueSegmentModel:
        return await self.repository.upsert_dialogue_segment(
            call_id=snapshot.call_id,
            segment_no=snapshot.segment_no,
            speaker_type=snapshot.speaker_type,
            speaker_identity=snapshot.speaker_identity,
            source=snapshot.source,
            source_segment_id=snapshot.source_segment_id,
            segment_text=snapshot.text,
            segment_status=snapshot.segment_status,
            started_at=snapshot.started_at,
            ended_at=snapshot.ended_at,
            duration_ms=snapshot.duration_ms,
            audio_start_ms=snapshot.audio_start_ms,
            audio_end_ms=snapshot.audio_end_ms,
            failure_stage=snapshot.failure_stage,
            failure_message=snapshot.failure_message,
        )

    @staticmethod
    def _canonical_segments(
        rows: list[AiCallDialogueSegmentModel],
    ) -> list[AiCallDialogueSegmentModel]:
        realtime_rows = [row for row in rows if row.source == QWEN_REALTIME_SOURCE]
        canonical: list[AiCallDialogueSegmentModel] = []
        for row in rows:
            if AiCallDialogueService._is_obvious_short_realtime_asr_noise(
                row
            ) or AiCallDialogueService._is_double_talk_single_char_customer_asr(
                row,
                rows,
            ) or AiCallDialogueService._is_realtime_customer_prefix_fragment(
                row,
                rows,
            ) or AiCallDialogueService._is_offline_customer_shadowed_by_realtime(
                row,
                realtime_rows,
            ):
                continue
            if row.source == QWEN_REALTIME_SOURCE and row.speaker_type == "ai" and any(
                candidate.source == QWEN_REALTIME_SOURCE
                and is_duplicate_dialogue_segment(
                    speaker_type=row.speaker_type,
                    text=row.segment_text,
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    candidate_speaker_type=candidate.speaker_type,
                    candidate_text=candidate.segment_text,
                    candidate_started_at=candidate.started_at,
                    candidate_ended_at=candidate.ended_at,
                )
                for candidate in canonical
            ):
                continue
            if row.source == OFFLINE_ASR_SOURCE and any(
                is_duplicate_dialogue_segment(
                    speaker_type=row.speaker_type,
                    text=row.segment_text,
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    candidate_speaker_type=candidate.speaker_type,
                    candidate_text=candidate.segment_text,
                    candidate_started_at=candidate.started_at,
                    candidate_ended_at=candidate.ended_at,
                )
                for candidate in realtime_rows
            ):
                continue
            canonical.append(row)
        return canonical

    @staticmethod
    def _canonical_preview_segments(
        rows: list[DialogueSegmentSnapshot],
    ) -> list[DialogueSegmentSnapshot]:
        realtime_rows = [row for row in rows if row.source == QWEN_REALTIME_SOURCE]
        canonical: list[DialogueSegmentSnapshot] = []
        for row in rows:
            if AiCallDialogueService._is_obvious_short_realtime_asr_noise(
                row
            ) or AiCallDialogueService._is_double_talk_single_char_customer_asr(
                row,
                rows,
            ) or AiCallDialogueService._is_realtime_customer_prefix_fragment(
                row,
                rows,
            ) or AiCallDialogueService._is_offline_customer_shadowed_by_realtime(
                row,
                realtime_rows,
            ):
                continue
            if row.source == QWEN_REALTIME_SOURCE and row.speaker_type == "ai" and any(
                candidate.source == QWEN_REALTIME_SOURCE
                and is_duplicate_dialogue_segment(
                    speaker_type=row.speaker_type,
                    text=AiCallDialogueService._dialogue_text(row),
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    candidate_speaker_type=candidate.speaker_type,
                    candidate_text=AiCallDialogueService._dialogue_text(candidate),
                    candidate_started_at=candidate.started_at,
                    candidate_ended_at=candidate.ended_at,
                )
                for candidate in canonical
            ):
                continue
            if row.source == OFFLINE_ASR_SOURCE and any(
                is_duplicate_dialogue_segment(
                    speaker_type=row.speaker_type,
                    text=AiCallDialogueService._dialogue_text(row),
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    candidate_speaker_type=candidate.speaker_type,
                    candidate_text=AiCallDialogueService._dialogue_text(candidate),
                    candidate_started_at=candidate.started_at,
                    candidate_ended_at=candidate.ended_at,
                )
                for candidate in realtime_rows
            ):
                continue
            canonical.append(row)
        return canonical

    @staticmethod
    def _is_obvious_short_realtime_asr_noise(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
    ) -> bool:
        if row.source != QWEN_REALTIME_SOURCE or row.speaker_type != "customer":
            return False
        if row.audio_start_ms is not None and row.audio_end_ms is not None:
            audio_span_ms = row.audio_end_ms - row.audio_start_ms
            if audio_span_ms >= SHORT_ASR_REAL_AUDIO_MIN_SPAN_MS:
                return False
        if row.duration_ms is None:
            return False
        if (
            row.audio_end_ms is not None
            and row.duration_ms > SHORT_ASR_NOISE_MAX_DURATION_MS
        ):
            return False
        normalized_text = normalize_dialogue_text(AiCallDialogueService._dialogue_text(row))
        if not normalized_text or normalized_text in VALID_SHORT_CUSTOMER_UTTERANCES:
            return False
        if len(normalized_text) > SHORT_ASR_NOISE_MAX_TEXT_LENGTH:
            return False
        return row.audio_end_ms is None or row.duration_ms <= SHORT_ASR_NOISE_MAX_DURATION_MS

    @staticmethod
    def _is_double_talk_single_char_customer_asr(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        rows: list[AiCallDialogueSegmentModel] | list[DialogueSegmentSnapshot],
    ) -> bool:
        if row.source != QWEN_REALTIME_SOURCE or row.speaker_type != "customer":
            return False
        normalized_text = normalize_dialogue_text(AiCallDialogueService._dialogue_text(row))
        if len(normalized_text) != 1:
            return False
        return any(
            candidate.source == QWEN_REALTIME_SOURCE
            and candidate.speaker_type == "ai"
            and not AiCallDialogueService._is_interrupted_realtime_ai_fragment(candidate)
            and not AiCallDialogueService._has_duplicate_ai_finished_before(
                row,
                candidate,
                rows,
            )
            and AiCallDialogueService._time_ranges_overlap(row, candidate)
            for candidate in rows
        )

    @staticmethod
    def _has_duplicate_ai_finished_before(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        candidate: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        rows: list[AiCallDialogueSegmentModel] | list[DialogueSegmentSnapshot],
    ) -> bool:
        row_start = row.started_at
        if row_start is None:
            return False
        for duplicate in rows:
            if duplicate is candidate:
                continue
            if (
                duplicate.source != QWEN_REALTIME_SOURCE
                or duplicate.speaker_type != "ai"
                or AiCallDialogueService._is_interrupted_realtime_ai_fragment(duplicate)
            ):
                continue
            if not is_duplicate_dialogue_segment(
                speaker_type=candidate.speaker_type,
                text=AiCallDialogueService._dialogue_text(candidate),
                started_at=candidate.started_at,
                ended_at=candidate.ended_at,
                candidate_speaker_type=duplicate.speaker_type,
                candidate_text=AiCallDialogueService._dialogue_text(duplicate),
                candidate_started_at=duplicate.started_at,
                candidate_ended_at=duplicate.ended_at,
            ):
                continue
            duplicate_end = duplicate.ended_at or duplicate.started_at
            if duplicate_end is not None and duplicate_end <= row_start:
                return True
        return False

    @staticmethod
    def _is_realtime_customer_prefix_fragment(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        rows: list[AiCallDialogueSegmentModel] | list[DialogueSegmentSnapshot],
    ) -> bool:
        if row.source != QWEN_REALTIME_SOURCE or row.speaker_type != "customer":
            return False
        normalized_text = normalize_dialogue_text(AiCallDialogueService._dialogue_text(row))
        if len(normalized_text) < 2:
            return False
        for candidate in rows:
            if candidate is row:
                continue
            if (
                candidate.source != QWEN_REALTIME_SOURCE
                or candidate.speaker_type != "customer"
            ):
                continue
            candidate_text = normalize_dialogue_text(
                AiCallDialogueService._dialogue_text(candidate)
            )
            if len(candidate_text) <= len(normalized_text):
                continue
            if not candidate_text.startswith(normalized_text):
                continue
            start_gap_ms = AiCallDialogueService._absolute_start_gap_ms(row, candidate)
            if (
                start_gap_ms is not None
                and start_gap_ms <= REALTIME_ASR_PREFIX_DUPLICATE_START_GAP_MS
            ):
                return True
        return False

    @staticmethod
    def _is_offline_customer_shadowed_by_realtime(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        realtime_rows: list[AiCallDialogueSegmentModel] | list[DialogueSegmentSnapshot],
    ) -> bool:
        if row.source != OFFLINE_ASR_SOURCE or row.speaker_type != "customer":
            return False
        return any(
            candidate.source == QWEN_REALTIME_SOURCE
            and candidate.speaker_type == "customer"
            and AiCallDialogueService._time_ranges_overlap(row, candidate)
            for candidate in realtime_rows
        )

    @staticmethod
    def _is_interrupted_realtime_ai_fragment(
        row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
    ) -> bool:
        return (
            row.source == QWEN_REALTIME_SOURCE
            and row.speaker_type == "ai"
            and row.segment_status == "interrupted"
        )

    @staticmethod
    def _dialogue_text(row: AiCallDialogueSegmentModel | DialogueSegmentSnapshot) -> str:
        if isinstance(row, DialogueSegmentSnapshot):
            return row.text
        return row.segment_text

    @staticmethod
    def _time_ranges_overlap(
        left: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        right: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
    ) -> bool:
        left_start = left.started_at
        left_end = left.ended_at or left.started_at
        right_start = right.started_at
        right_end = right.ended_at or right.started_at
        if not all((left_start, left_end, right_start, right_end)):
            return False
        assert left_start is not None
        assert left_end is not None
        assert right_start is not None
        assert right_end is not None
        if left_start > left_end:
            left_start, left_end = left_end, left_start
        if right_start > right_end:
            right_start, right_end = right_end, right_start
        return left_start <= right_end and right_start <= left_end

    @staticmethod
    def _absolute_start_gap_ms(
        left: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
        right: AiCallDialogueSegmentModel | DialogueSegmentSnapshot,
    ) -> int | None:
        if left.started_at is None or right.started_at is None:
            return None
        left_start = (
            left.started_at
            if left.started_at.tzinfo
            else left.started_at.replace(tzinfo=timezone.utc)
        )
        right_start = (
            right.started_at
            if right.started_at.tzinfo
            else right.started_at.replace(tzinfo=timezone.utc)
        )
        return abs(int((right_start - left_start).total_seconds() * 1000))

    @staticmethod
    def segment_to_dict(segment: AiCallDialogueSegmentModel) -> dict[str, Any]:
        return {
            "id": str(segment.id),
            "callId": segment.call_id,
            "segmentNo": segment.segment_no,
            "speakerType": segment.speaker_type,
            "speakerIdentity": segment.speaker_identity,
            "source": segment.source,
            "sourceSegmentId": segment.source_segment_id,
            "text": segment.segment_text,
            "segmentStatus": segment.segment_status,
            "startedAt": segment.started_at,
            "endedAt": segment.ended_at,
            "durationMs": segment.duration_ms,
            "audioStartMs": segment.audio_start_ms,
            "audioEndMs": segment.audio_end_ms,
            "failureStage": segment.failure_stage,
            "failureMessage": segment.failure_message,
        }
