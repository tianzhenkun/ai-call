from __future__ import annotations

import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher

QWEN_REALTIME_SOURCE = "qwen_realtime"
OFFLINE_ASR_SOURCE = "offline_asr"
CUSTOMER_SPEAKER_TYPE = "customer"
DUPLICATE_TIME_GAP_MS = 1200
CROSS_SOURCE_CONFLICT_TIME_GAP_MS = 500
CROSS_SOURCE_CONFLICT_MIN_TEXT_SIMILARITY = 1 / 3
CROSS_SOURCE_CONFLICT_MIN_LENGTH_RATIO = 0.5


def normalize_dialogue_text(text: str | None) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKC", text).lower()
    return "".join(
        char
        for char in normalized
        if not char.isspace() and not unicodedata.category(char).startswith("P")
    )


def is_duplicate_dialogue_segment(
    *,
    speaker_type: str,
    text: str,
    started_at: datetime | None,
    ended_at: datetime | None,
    candidate_speaker_type: str,
    candidate_text: str,
    candidate_started_at: datetime | None,
    candidate_ended_at: datetime | None,
    max_gap_ms: int = DUPLICATE_TIME_GAP_MS,
) -> bool:
    if speaker_type != candidate_speaker_type:
        return False
    if normalize_dialogue_text(text) != normalize_dialogue_text(candidate_text):
        return False
    return _time_ranges_touch(
        started_at,
        ended_at,
        candidate_started_at,
        candidate_ended_at,
        max_gap_ms=max_gap_ms,
    )


def is_cross_source_customer_transcript_conflict(
    *,
    source: str,
    speaker_type: str,
    text: str,
    started_at: datetime | None,
    ended_at: datetime | None,
    candidate_source: str,
    candidate_speaker_type: str,
    candidate_text: str,
    candidate_started_at: datetime | None,
    candidate_ended_at: datetime | None,
    max_gap_ms: int = CROSS_SOURCE_CONFLICT_TIME_GAP_MS,
) -> bool:
    if speaker_type != CUSTOMER_SPEAKER_TYPE or candidate_speaker_type != speaker_type:
        return False
    if {source, candidate_source} != {OFFLINE_ASR_SOURCE, QWEN_REALTIME_SOURCE}:
        return False

    normalized = normalize_dialogue_text(text)
    candidate_normalized = normalize_dialogue_text(candidate_text)
    if not normalized or not candidate_normalized or normalized == candidate_normalized:
        return False
    if normalized in candidate_normalized or candidate_normalized in normalized:
        return False
    if not _time_ranges_touch(
        started_at,
        ended_at,
        candidate_started_at,
        candidate_ended_at,
        max_gap_ms=max_gap_ms,
    ):
        return False

    length_ratio = min(len(normalized), len(candidate_normalized)) / max(
        len(normalized),
        len(candidate_normalized),
    )
    if length_ratio < CROSS_SOURCE_CONFLICT_MIN_LENGTH_RATIO:
        return False
    return (
        SequenceMatcher(None, normalized, candidate_normalized).ratio()
        >= CROSS_SOURCE_CONFLICT_MIN_TEXT_SIMILARITY
    )


def _time_ranges_touch(
    left_started_at: datetime | None,
    left_ended_at: datetime | None,
    right_started_at: datetime | None,
    right_ended_at: datetime | None,
    *,
    max_gap_ms: int,
) -> bool:
    if not all((left_started_at, left_ended_at, right_started_at, right_ended_at)):
        return False

    left_start = _timestamp_ms(left_started_at)
    left_end = _timestamp_ms(left_ended_at)
    right_start = _timestamp_ms(right_started_at)
    right_end = _timestamp_ms(right_ended_at)
    if left_start > left_end:
        left_start, left_end = left_end, left_start
    if right_start > right_end:
        right_start, right_end = right_end, right_start

    if left_end < right_start:
        return right_start - left_end <= max_gap_ms
    if right_end < left_start:
        return left_start - right_end <= max_gap_ms
    return True


def _timestamp_ms(value: datetime | None) -> int:
    assert value is not None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)
