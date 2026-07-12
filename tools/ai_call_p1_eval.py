from __future__ import annotations

import argparse
import asyncio
import json
import math
import struct
import sys
import urllib.error
import urllib.request
import wave
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.interrupt_p1_evaluation import (
    build_p1_evaluation,
    build_p1_evaluation_suite,
)
from app.services.ai_call.interrupt_p1_sample_matrix import (
    build_p1_sample_matrix_evaluation,
)
from app.services.ai_call.sip_barge_in import (
    SipBargeInConfig,
    SipBargeInDetector,
    SipBargeInObservation,
)
from app.services.ai_call.sip_barge_in_replay import (
    SipBargeInProviderReplayReport,
    SipVadReplayProvider,
    SpeechWindow,
    WindowVoiceActivityDetector,
    pcm16_mono_to_replay_frames,
    replay_sip_barge_in_vad_providers,
)

GetJson = Callable[[str, float], dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="P1 SIP barge-in evaluation from AI Call events.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011",
        help="AI Call API base URL, without trailing slash.",
    )
    parser.add_argument("--call-id", help="Call ID to evaluate.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Evaluate the latest N sip_outbound calls instead of a single call.",
    )
    parser.add_argument(
        "--sample-matrix",
        help="Evaluate a JSON sample matrix with per-call P1 expectations.",
    )
    parser.add_argument(
        "--fixture-only",
        action="store_true",
        help="With --sample-matrix, evaluate only fixtureReports/audioFixtures samples.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    get_json: GetJson | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    get_json = get_json or _get_json
    base_url = args.base_url.rstrip("/")

    selected_modes = sum(
        1
        for value in (args.call_id, args.recent, args.sample_matrix)
        if value is not None
    )
    if selected_modes != 1:
        parser.error("exactly one of --call-id, --recent, or --sample-matrix is required")
    if args.recent is not None and args.recent < 1:
        parser.error("--recent must be greater than 0")
    if args.fixture_only and args.sample_matrix is None:
        parser.error("--fixture-only requires --sample-matrix")

    try:
        report = (
            _build_recent_report(
                base_url=base_url,
                recent=args.recent,
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
            )
            if args.recent is not None
            else _build_matrix_report(
                base_url=base_url,
                matrix_path=Path(str(args.sample_matrix)),
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
                fixture_only=args.fixture_only,
            )
            if args.sample_matrix is not None
            else _build_single_report(
                base_url=base_url,
                call_id=str(args.call_id),
                timeout_seconds=args.timeout_seconds,
                get_json=get_json,
            )
        )
    except Exception as exc:
        print(f"fetch failed: {exc!s}", file=stderr)
        return 1

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        _print_text_report(report, stdout)
    if report.get("mode") == "sample_matrix" and _sample_matrix_has_failures(report):
        return 2
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _build_single_report(
    *,
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    record = _record_for_call(base_url, call_id, timeout_seconds, get_json)
    events = _events_for_call(base_url, call_id, timeout_seconds, get_json)
    dialogue_segments = _dialogue_segments_for_call(
        base_url,
        call_id,
        timeout_seconds,
        get_json,
    )
    return build_p1_evaluation(
        call_id=call_id,
        record=record,
        events=events,
        dialogue_segments=dialogue_segments,
    )


def _build_recent_report(
    *,
    base_url: str,
    recent: int,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    records_response = get_json(
        f"{base_url}/ai-call/records?entryType=sip_outbound&pageSize={recent}",
        timeout_seconds,
    )
    reports: list[dict[str, Any]] = []
    failed_calls: list[dict[str, str]] = []
    for row in _record_rows(records_response):
        call_id = str(row.get("callId") or "")
        if not call_id:
            continue
        try:
            reports.append(
                _build_single_report(
                    base_url=base_url,
                    call_id=call_id,
                    timeout_seconds=timeout_seconds,
                    get_json=get_json,
                )
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})
    report = build_p1_evaluation_suite(reports, failed_calls)
    report["mode"] = "recent"
    report["requested"] = recent
    report["sourceTotal"] = records_response.get("total")
    return report


def _build_matrix_report(
    *,
    base_url: str,
    matrix_path: Path,
    timeout_seconds: float,
    get_json: GetJson,
    fixture_only: bool = False,
) -> dict[str, Any]:
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    samples = matrix.get("samples")
    if not isinstance(samples, list):
        raise RuntimeError("sample matrix missing samples list")
    coverage_gates = matrix.get("fixtureCoverageGates" if fixture_only else "coverageGates")
    if coverage_gates is not None and not isinstance(coverage_gates, dict):
        raise RuntimeError("sample matrix coverageGates must be an object")
    fixture_reports = _fixture_reports_from_matrix(matrix)
    reports_by_call_id = dict(fixture_reports)
    report_sources_by_call_id = dict.fromkeys(fixture_reports, "fixture_report")
    audio_fixture_reports, audio_fixture_sources = _audio_fixture_reports_from_matrix(
        matrix,
        base_dir=matrix_path.parent,
    )
    _add_audio_fixture_reports(
        reports_by_call_id,
        report_sources_by_call_id,
        audio_fixture_reports=audio_fixture_reports,
        audio_fixture_sources=audio_fixture_sources,
    )
    authority_fixture_reports = _authority_fixture_reports_from_matrix(matrix)
    _add_authority_fixture_reports(
        reports_by_call_id,
        report_sources_by_call_id,
        authority_fixture_reports=authority_fixture_reports,
    )
    matrix_samples = [
        _sample_with_evaluation_source(
            sample,
            report_sources_by_call_id=report_sources_by_call_id,
        )
        for sample in samples
        if isinstance(sample, dict)
        and (
            not fixture_only
            or str(sample.get("callId") or "") in reports_by_call_id
        )
    ]
    call_ids = sorted(
        {
            str(sample.get("callId") or "")
            for sample in matrix_samples
            if str(sample.get("callId") or "") not in reports_by_call_id
        }
    )
    failed_calls: list[dict[str, str]] = []
    for call_id in call_ids:
        if not call_id:
            continue
        try:
            reports_by_call_id[call_id] = _build_single_report(
                base_url=base_url,
                call_id=call_id,
                timeout_seconds=timeout_seconds,
                get_json=get_json,
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})
    report = build_p1_sample_matrix_evaluation(
        reports_by_call_id=reports_by_call_id,
        samples=matrix_samples,
        coverage_gates=coverage_gates,
    )
    report["failedCalls"] = failed_calls
    return report


def _add_audio_fixture_reports(
    reports_by_call_id: dict[str, dict[str, Any]],
    report_sources_by_call_id: dict[str, str],
    *,
    audio_fixture_reports: dict[str, dict[str, Any]],
    audio_fixture_sources: dict[str, str],
) -> None:
    for call_id, report in audio_fixture_reports.items():
        if call_id in reports_by_call_id:
            raise RuntimeError(f"sample matrix duplicate fixture callId: {call_id}")
        reports_by_call_id[call_id] = report
        report_sources_by_call_id[call_id] = audio_fixture_sources.get(
            call_id,
            "audio_fixture",
        )


def _add_authority_fixture_reports(
    reports_by_call_id: dict[str, dict[str, Any]],
    report_sources_by_call_id: dict[str, str],
    *,
    authority_fixture_reports: dict[str, dict[str, Any]],
) -> None:
    for call_id, report in authority_fixture_reports.items():
        if call_id in reports_by_call_id:
            raise RuntimeError(f"sample matrix duplicate fixture callId: {call_id}")
        reports_by_call_id[call_id] = report
        report_sources_by_call_id[call_id] = "authority_fixture"


def _sample_with_evaluation_source(
    sample: dict[str, Any],
    *,
    report_sources_by_call_id: dict[str, str],
) -> dict[str, Any]:
    call_id = str(sample.get("callId") or "")
    if not call_id:
        return sample
    evaluation_source = report_sources_by_call_id.get(call_id, "api_history")
    enriched = {**sample, "evaluationSource": evaluation_source}
    if not isinstance(enriched.get("sourceType"), str) or not enriched["sourceType"]:
        enriched["sourceType"] = evaluation_source
    return enriched


def _audio_fixture_reports_from_matrix(
    matrix: dict[str, Any],
    *,
    base_dir: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    raw_fixtures = matrix.get("audioFixtures")
    if raw_fixtures is None:
        return {}, {}
    if not isinstance(raw_fixtures, dict):
        raise RuntimeError("sample matrix audioFixtures must be an object")

    reports: dict[str, dict[str, Any]] = {}
    sources: dict[str, str] = {}
    for raw_call_id, raw_fixture in raw_fixtures.items():
        call_id = str(raw_call_id or "")
        if not call_id:
            continue
        reports[call_id] = _build_audio_fixture_report(
            call_id=call_id,
            fixture=raw_fixture,
            base_dir=base_dir,
        )
        sources[call_id] = (
            "audio_authority_fixture"
            if isinstance(raw_fixture, dict) and bool(raw_fixture.get("authorityReplay"))
            else "audio_fixture"
        )
    return reports, sources


class _AuthorityFixtureDetector:
    def __init__(self, raw_detector: Any) -> None:
        detector = raw_detector if isinstance(raw_detector, dict) else {}
        self._single_short = bool(detector.get("singleShort"))
        self._fast_local = bool(detector.get("fastLocal"))
        self._pre_stop_local = bool(detector.get("preStopLocal"))
        payload = detector.get("payload")
        self._payload = payload if isinstance(payload, dict) else {}

    def has_single_short_pre_stop_local_speech(
        self,
        _call_id: str,
        *,
        min_rms_dbfs: float,
        max_rms_dbfs: float,
        min_snr_db: float,
    ) -> bool:
        del min_rms_dbfs, max_rms_dbfs, min_snr_db
        return self._single_short

    def has_fast_pre_stop_local_speech(self, _call_id: str) -> bool:
        return self._fast_local

    def has_pre_stop_local_speech(self, _call_id: str) -> bool:
        return self._pre_stop_local

    def latest_observation_payload(self, _call_id: str) -> dict[str, Any]:
        return dict(self._payload)

    def reset(self, _call_id: str) -> None:
        return

    def reset_activity(self, _call_id: str) -> None:
        return


def _authority_fixture_reports_from_matrix(matrix: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_fixtures = matrix.get("authorityFixtures")
    if raw_fixtures is None:
        return {}
    if not isinstance(raw_fixtures, dict):
        raise RuntimeError("sample matrix authorityFixtures must be an object")

    reports: dict[str, dict[str, Any]] = {}
    for raw_call_id, raw_fixture in raw_fixtures.items():
        call_id = str(raw_call_id or "")
        if not call_id:
            continue
        reports[call_id] = _build_authority_fixture_report(
            call_id=call_id,
            fixture=raw_fixture,
        )
    return reports


def _build_authority_fixture_report(*, call_id: str, fixture: Any) -> dict[str, Any]:
    from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
    from app.services.ai_call.event_store import InMemoryEventStore
    from app.services.ai_call.session_registry import InMemorySessionRegistry

    if not isinstance(fixture, dict):
        raise RuntimeError(f"sample matrix authorityFixture {call_id} must be an object")

    if isinstance(fixture.get("observations"), list):
        return asyncio.run(
            _build_authority_fixture_observation_episode_report(
                call_id=call_id,
                fixture=fixture,
            )
        )

    observation = _authority_fixture_observation(
        fixture.get("observation"),
        call_id=call_id,
    )
    started_at = _parse_fixture_time(fixture.get("startedAt"))
    decision_offset_ms = _positive_int(
        fixture.get("decisionOffsetMs"),
        default=observation.candidate_duration_ms,
    )
    trigger_timestamp = started_at + timedelta(milliseconds=decision_offset_ms)
    response_id = str(fixture.get("responseId") or f"resp_{call_id}")
    generation = _positive_int(fixture.get("generation"), default=0)

    event_store = InMemoryEventStore()
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: None,
        registry=InMemorySessionRegistry(),
        event_store=event_store,
        sip_barge_in_config=SipBargeInConfig(),
    )
    if bool(fixture.get("openingStarted")):
        event_store.append(
            call_id,
            "opening_started",
            "agent",
            {"openingMessageHash": "sha256:authority-fixture"},
            timestamp=started_at,
        )
    runner._sip_barge_in_detector = _AuthorityFixtureDetector(fixture.get("detector"))

    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_generation = generation
    guard.generation = generation
    guard.current_response_audio_published = bool(fixture.get("playbackTarget", True))

    lifecycle = runner._response_lifecycle(call_id)
    lifecycle.current_response_is_opening = bool(fixture.get("opening"))

    _apply_authority_fixture_ai_playback(
        runner=runner,
        call_id=call_id,
        fixture=fixture,
        entry=None,
        started_at=started_at,
        current_timestamp=trigger_timestamp,
        offset_ms=decision_offset_ms,
    )

    turn = _authority_fixture_turn(
        fixture.get("turn"),
        started_at=started_at,
        response_id=response_id,
        generation=generation,
        observation=observation,
    )
    runner._pending_user_turns[call_id] = turn

    decision = runner._decide_sip_pre_stop_authority(
        call_id=call_id,
        turn=turn,
        trigger_timestamp=trigger_timestamp,
        observation=observation,
    )
    authority_payload = runner._sip_pre_stop_authority_payload(decision)
    return build_p1_evaluation(
        call_id=call_id,
        record={
            "callId": call_id,
            "startedAt": _format_fixture_time(started_at),
        },
        events=_authority_fixture_events(
            call_id=call_id,
            trigger_timestamp=trigger_timestamp,
            response_id=response_id,
            generation=generation,
            observation=observation,
            decision=decision,
            authority_payload=authority_payload,
        ),
    )


class _AuthorityFixtureProvider:
    def __init__(self) -> None:
        self.cancelled_response_count = 0
        self.cleared_input_count = 0

    async def cancel_response(self) -> None:
        self.cancelled_response_count += 1

    async def clear_input_audio(self) -> None:
        self.cleared_input_count += 1


async def _build_authority_fixture_observation_episode_report(
    *,
    call_id: str,
    fixture: dict[str, Any],
) -> dict[str, Any]:
    from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
    from app.services.ai_call.event_store import InMemoryEventStore
    from app.services.ai_call.session_registry import (
        CallSession,
        CallSessionStatus,
        InMemorySessionRegistry,
    )

    started_at = _parse_fixture_time(fixture.get("startedAt"))
    response_id = str(fixture.get("responseId") or f"resp_{call_id}")
    generation = _positive_int(fixture.get("generation"), default=0)
    event_store = InMemoryEventStore()
    registry = InMemorySessionRegistry()
    provider = _AuthorityFixtureProvider()
    registry.add(
        CallSession(
            call_id=call_id,
            room_name=f"ai-call-{call_id}",
            participant_identity=f"sip-{call_id}",
            status=CallSessionStatus.AI_SPEAKING,
            effective_config={},
        )
    )
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=event_store,
        sip_barge_in_enabled=True,
        sip_barge_in_fast_stop_enabled=True,
        sip_barge_in_config=SipBargeInConfig(),
    )
    current_event_timestamp = started_at

    def append_fixture_event(
        event_call_id: str,
        event_type: str,
        source: str,
        payload: dict[str, Any] | None = None,
    ) -> datetime:
        event = event_store.append(
            call_id=event_call_id,
            type=event_type,
            source=source,
            payload=payload,
            timestamp=current_event_timestamp,
        )
        registry.get(event_call_id).last_event_at = event.timestamp
        return event.timestamp

    runner._append_event = append_fixture_event

    if bool(fixture.get("openingStarted")):
        event_store.append(
            call_id,
            "opening_started",
            "agent",
            {"openingMessageHash": "sha256:authority-fixture"},
            timestamp=started_at,
        )

    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_generation = generation
    guard.generation = generation
    guard.current_response_audio_published = bool(fixture.get("playbackTarget", True))

    lifecycle = runner._response_lifecycle(call_id)
    lifecycle.current_response_is_opening = bool(fixture.get("opening"))

    for raw_entry in fixture["observations"]:
        if not isinstance(raw_entry, dict):
            raise RuntimeError(
                f"sample matrix authorityFixture {call_id} observations must be objects"
            )
        observation = _authority_fixture_observation(
            raw_entry.get("observation"),
            call_id=call_id,
        )
        offset_ms = _positive_int(raw_entry.get("offsetMs"), default=0)
        current_event_timestamp = started_at + timedelta(milliseconds=offset_ms)
        _apply_authority_fixture_ai_playback(
            runner=runner,
            call_id=call_id,
            fixture=fixture,
            entry=raw_entry,
            started_at=started_at,
            current_timestamp=current_event_timestamp,
            offset_ms=offset_ms,
        )
        runner._sip_barge_in_detector = _AuthorityFixtureDetector(
            raw_entry.get("detector", fixture.get("detector"))
        )
        await runner._handle_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=current_event_timestamp,
            observation=observation,
        )

    return build_p1_evaluation(
        call_id=call_id,
        record={
            "callId": call_id,
            "startedAt": _format_fixture_time(started_at),
        },
        events=_authority_fixture_events_from_store(event_store.list_all(call_id)),
    )


def _apply_authority_fixture_ai_playback(
    *,
    runner: Any,
    call_id: str,
    fixture: dict[str, Any],
    entry: dict[str, Any] | None,
    started_at: datetime,
    current_timestamp: datetime,
    offset_ms: int,
) -> None:
    recent_ai_audio = entry.get("recentAiAudio") if isinstance(entry, dict) else None
    if isinstance(recent_ai_audio, dict):
        _apply_authority_fixture_recent_ai_audio(
            runner=runner,
            call_id=call_id,
            recent_ai_audio=recent_ai_audio,
            current_timestamp=current_timestamp,
        )
        return

    ai_frame = _authority_fixture_ai_playback_frame_at(
        fixture.get("aiPlaybackFrames"),
        offset_ms=offset_ms,
    )
    if ai_frame is not None:
        ai_rms_dbfs = ai_frame["frame"].get("rmsDbfs")
        if ai_rms_dbfs is not None:
            runner._last_ai_audio_rms_dbfs[call_id] = float(ai_rms_dbfs)
        runner._last_ai_audio_published_at[call_id] = started_at + timedelta(
            milliseconds=ai_frame["offsetMs"],
        )
        return

    recent_ai_audio = fixture.get("recentAiAudio")
    if isinstance(recent_ai_audio, dict):
        _apply_authority_fixture_recent_ai_audio(
            runner=runner,
            call_id=call_id,
            recent_ai_audio=recent_ai_audio,
            current_timestamp=current_timestamp,
        )


def _apply_authority_fixture_recent_ai_audio(
    *,
    runner: Any,
    call_id: str,
    recent_ai_audio: dict[str, Any],
    current_timestamp: datetime,
) -> None:
    ai_rms_dbfs = recent_ai_audio.get("rmsDbfs")
    if ai_rms_dbfs is not None:
        runner._last_ai_audio_rms_dbfs[call_id] = float(ai_rms_dbfs)
    age_ms = _positive_int(recent_ai_audio.get("ageMs"), default=0)
    runner._last_ai_audio_published_at[call_id] = (
        current_timestamp - timedelta(milliseconds=age_ms)
    )


def _authority_fixture_ai_playback_frame_at(
    raw_frames: Any,
    *,
    offset_ms: int,
) -> dict[str, Any] | None:
    if raw_frames is None:
        return None
    if not isinstance(raw_frames, list):
        raise RuntimeError("sample matrix authorityFixture aiPlaybackFrames must be a list")

    latest: dict[str, Any] | None = None
    latest_offset_ms = -1
    for index, raw_frame in enumerate(raw_frames):
        if not isinstance(raw_frame, dict):
            raise RuntimeError(
                f"sample matrix authorityFixture aiPlaybackFrames[{index}] must be an object"
            )
        frame_offset_ms = _positive_int(raw_frame.get("offsetMs"), default=0)
        if frame_offset_ms > offset_ms or frame_offset_ms < latest_offset_ms:
            continue
        latest = {
            "offsetMs": frame_offset_ms,
            "frame": raw_frame,
        }
        latest_offset_ms = frame_offset_ms
    return latest


def _authority_fixture_observation(
    raw_observation: Any,
    *,
    call_id: str,
) -> SipBargeInObservation:
    if not isinstance(raw_observation, dict):
        raise RuntimeError(f"sample matrix authorityFixture {call_id} requires observation")
    return SipBargeInObservation(
        active=bool(raw_observation.get("active", True)),
        candidate=bool(raw_observation.get("candidate", True)),
        rms_dbfs=_optional_float(raw_observation.get("rmsDbfs")),
        noise_floor_dbfs=_optional_float(raw_observation.get("noiseFloorDbfs")),
        snr_db=_optional_float(raw_observation.get("snrDb")),
        peak_dbfs=_optional_float(raw_observation.get("peakDbfs")),
        vad_voiced_ms=_positive_int(raw_observation.get("vadVoicedMs"), default=0),
        candidate_duration_ms=_positive_int(
            raw_observation.get("candidateDurationMs"),
            default=0,
        ),
        speech_duration_ms=_positive_int(raw_observation.get("speechDurationMs"), default=0),
        frame_duration_ms=_positive_int(raw_observation.get("frameDurationMs"), default=20),
        candidate_class=_optional_str(raw_observation.get("candidateClass")),
        reason=str(raw_observation.get("reason") or "authority_fixture"),
    )


def _authority_fixture_turn(
    raw_turn: Any,
    *,
    started_at: datetime,
    response_id: str,
    generation: int,
    observation: SipBargeInObservation,
) -> Any:
    from app.services.ai_call.agent_runner import PendingUserTurn

    turn_values = raw_turn if isinstance(raw_turn, dict) else {}
    turn = PendingUserTurn(started_at=started_at)
    turn.interrupt_candidate = True
    turn.interrupt_trigger_at = started_at
    turn.sip_candidate_class = observation.candidate_class
    turn.sip_candidate_response_id = response_id
    turn.sip_candidate_generation = generation
    turn.sip_single_short_pre_stop_evidence = bool(
        turn_values.get("singleShortPreStopEvidence")
    )
    deferred_episode = turn_values.get("deferredEpisode")
    if isinstance(deferred_episode, dict):
        turn.sip_deferred_episode_response_id = str(
            deferred_episode.get("responseId") or response_id
        )
        turn.sip_deferred_episode_generation = _positive_int(
            deferred_episode.get("generation"),
            default=generation,
        )
        first_offset_ms = _positive_int(
            deferred_episode.get("firstOffsetMs"),
            default=0,
        )
        last_offset_ms = _positive_int(
            deferred_episode.get("lastOffsetMs"),
            default=first_offset_ms,
        )
        turn.sip_deferred_episode_first_at = started_at + timedelta(
            milliseconds=first_offset_ms,
        )
        turn.sip_deferred_episode_last_at = started_at + timedelta(
            milliseconds=last_offset_ms,
        )
        turn.sip_deferred_episode_burst_count = _positive_int(
            deferred_episode.get("burstCount"),
            default=0,
        )
        turn.sip_deferred_episode_voiced_ms = _positive_int(
            deferred_episode.get("voicedMs"),
            default=0,
        )
        turn.sip_deferred_episode_current_burst_voiced_ms = _positive_int(
            deferred_episode.get("currentBurstVoicedMs"),
            default=0,
        )
        turn.sip_deferred_episode_min_rms_dbfs = _optional_float(
            deferred_episode.get("minRmsDbfs")
        )
        turn.sip_deferred_episode_max_rms_dbfs = _optional_float(
            deferred_episode.get("maxRmsDbfs")
        )
        turn.sip_deferred_episode_max_snr_db = _optional_float(
            deferred_episode.get("maxSnrDb")
        )
        turn.sip_deferred_episode_max_rms_range_db = _optional_float(
            deferred_episode.get("maxRmsRangeDb")
        )
        turn.sip_deferred_episode_max_gap_ms = _positive_int(
            deferred_episode.get("maxGapMs"),
            default=0,
        )
    echo_guarded_turn = turn_values.get("echoGuardedTurn")
    if isinstance(echo_guarded_turn, dict):
        turn.sip_echo_guarded_turn_response_id = str(
            echo_guarded_turn.get("responseId") or response_id
        )
        turn.sip_echo_guarded_turn_generation = _positive_int(
            echo_guarded_turn.get("generation"),
            default=generation,
        )
        first_offset_ms = _positive_int(
            echo_guarded_turn.get("firstOffsetMs"),
            default=0,
        )
        last_offset_ms = _positive_int(
            echo_guarded_turn.get("lastOffsetMs"),
            default=first_offset_ms,
        )
        turn.sip_echo_guarded_turn_first_at = started_at + timedelta(
            milliseconds=first_offset_ms,
        )
        turn.sip_echo_guarded_turn_last_at = started_at + timedelta(
            milliseconds=last_offset_ms,
        )
        turn.sip_echo_guarded_turn_burst_count = _positive_int(
            echo_guarded_turn.get("burstCount"),
            default=0,
        )
        turn.sip_echo_guarded_turn_voiced_ms = _positive_int(
            echo_guarded_turn.get("voicedMs"),
            default=0,
        )
        turn.sip_echo_guarded_turn_current_burst_voiced_ms = _positive_int(
            echo_guarded_turn.get("currentBurstVoicedMs"),
            default=0,
        )
        turn.sip_echo_guarded_turn_min_rms_dbfs = _optional_float(
            echo_guarded_turn.get("minRmsDbfs")
        )
        turn.sip_echo_guarded_turn_max_rms_dbfs = _optional_float(
            echo_guarded_turn.get("maxRmsDbfs")
        )
        turn.sip_echo_guarded_turn_max_snr_db = _optional_float(
            echo_guarded_turn.get("maxSnrDb")
        )
        turn.sip_echo_guarded_turn_max_rms_range_db = _optional_float(
            echo_guarded_turn.get("maxRmsRangeDb")
        )
    return turn


def _authority_fixture_events_from_store(events: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {
            "eventType": event.type,
            "source": event.source,
            "eventTime": _format_fixture_time(event.timestamp),
            "payload": event.payload,
        }
        for event in events
    ]


def _authority_fixture_events(
    *,
    call_id: str,
    trigger_timestamp: datetime,
    response_id: str,
    generation: int,
    observation: SipBargeInObservation,
    decision: Any,
    authority_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    candidate_payload = _authority_fixture_event_payload(
        call_id=call_id,
        response_id=response_id,
        generation=generation,
        observation=observation,
    )
    events = [
        {
            "eventType": "sip_interrupt_candidate",
            "source": "agent",
            "eventTime": _format_fixture_time(trigger_timestamp),
            "payload": candidate_payload,
        }
    ]
    decision_payload = {
        **candidate_payload,
        "reason": decision.reason,
        "requiredDurationMs": decision.required_duration_ms,
    }
    decision_payload.update(authority_payload)
    if decision.allowed:
        events.append(
            {
                "eventType": "sip_pre_stop",
                "source": "agent",
                "eventTime": _format_fixture_time(trigger_timestamp),
                "payload": {
                    **decision_payload,
                    "candidateToStopMs": 0,
                    "generation": generation + 1,
                },
            }
        )
        return events

    event_type = (
        "sip_ai_playback_echo_deferred"
        if decision.reason == "awaiting_ai_playback_echo_guard"
        else "sip_pre_stop_deferred"
    )
    events.append(
        {
            "eventType": event_type,
            "source": "agent",
            "eventTime": _format_fixture_time(trigger_timestamp),
            "payload": decision_payload,
        }
    )
    return events


def _authority_fixture_event_payload(
    *,
    call_id: str,
    response_id: str,
    generation: int,
    observation: SipBargeInObservation,
) -> dict[str, Any]:
    return {
        "callId": call_id,
        "responseId": response_id,
        "generation": generation,
        "reason": observation.reason,
        "candidateClass": observation.candidate_class,
        "candidateDurationMs": observation.candidate_duration_ms,
        "wallClockSpeechMs": observation.speech_duration_ms,
        "vadVoicedMs": observation.vad_voiced_ms,
        "rmsDbfs": observation.rms_dbfs,
        "noiseFloorDbfs": observation.noise_floor_dbfs,
        "snrDb": observation.snr_db,
        "peakDbfs": observation.peak_dbfs,
    }


def _build_audio_fixture_report(
    *,
    call_id: str,
    fixture: Any,
    base_dir: Path,
) -> dict[str, Any]:
    if not isinstance(fixture, dict):
        raise RuntimeError(f"sample matrix audioFixture {call_id} must be an object")

    pcm16_mono, sample_rate_hz = _audio_fixture_pcm16_mono(
        fixture,
        base_dir=base_dir,
    )
    frame_ms = _positive_int(fixture.get("frameMs"), default=20)
    frames = pcm16_mono_to_replay_frames(
        pcm16_mono,
        sample_rate_hz=sample_rate_hz,
        frame_duration_ms=frame_ms,
    )
    ai_frames = _audio_fixture_ai_replay_frames(
        fixture,
        base_dir=base_dir,
        sample_rate_hz=sample_rate_hz,
        frame_ms=frame_ms,
    )
    speech_windows = _audio_fixture_vad_windows(fixture, call_id=call_id)
    started_at = _parse_fixture_time(fixture.get("startedAt"))
    if bool(fixture.get("authorityReplay")):
        return asyncio.run(
            _build_audio_authority_fixture_report(
                call_id=call_id,
                fixture=fixture,
                frames=frames,
                ai_frames=ai_frames,
                speech_windows=speech_windows,
                started_at=started_at,
            )
        )

    provider_name = str(fixture.get("provider") or "fixture_vad")
    replay_report = replay_sip_barge_in_vad_providers(
        call_id=call_id,
        frames=frames,
        providers=[
            SipVadReplayProvider(
                name=provider_name,
                vad_factory=lambda: WindowVoiceActivityDetector(speech_windows),
            )
        ],
        config=SipBargeInConfig(),
        started_at=started_at,
    )
    provider_report = replay_report.provider_reports[provider_name]
    return build_p1_evaluation(
        call_id=call_id,
        record={
            "callId": call_id,
            "startedAt": _format_fixture_time(started_at),
        },
        events=_audio_replay_events(
            call_id=call_id,
            started_at=started_at,
            provider_report=provider_report,
        ),
    )


async def _build_audio_authority_fixture_report(
    *,
    call_id: str,
    fixture: dict[str, Any],
    frames: Sequence[Any],
    ai_frames: Sequence[Any],
    speech_windows: Sequence[SpeechWindow],
    started_at: datetime,
) -> dict[str, Any]:
    from app.services.ai_call.agent_runner import RealtimeCallAgentRunner
    from app.services.ai_call.event_store import InMemoryEventStore
    from app.services.ai_call.session_registry import (
        CallSession,
        CallSessionStatus,
        InMemorySessionRegistry,
    )

    response_id = str(fixture.get("responseId") or f"resp_{call_id}")
    generation = _positive_int(fixture.get("generation"), default=0)
    event_store = InMemoryEventStore()
    registry = InMemorySessionRegistry()
    provider = _AuthorityFixtureProvider()
    registry.add(
        CallSession(
            call_id=call_id,
            room_name=f"ai-call-{call_id}",
            participant_identity=f"sip-{call_id}",
            status=CallSessionStatus.AI_SPEAKING,
            effective_config={},
        )
    )
    detector = SipBargeInDetector(
        config=SipBargeInConfig(),
        vad=WindowVoiceActivityDetector(speech_windows),
    )
    runner = RealtimeCallAgentRunner(
        provider_factory=lambda _session: provider,
        registry=registry,
        event_store=event_store,
        sip_barge_in_enabled=True,
        sip_barge_in_fast_stop_enabled=True,
        sip_barge_in_config=detector.config,
        sip_barge_in_vad=detector.vad,
    )
    runner._sip_barge_in_detector = detector
    current_event_timestamp = started_at

    def append_fixture_event(
        event_call_id: str,
        event_type: str,
        source: str,
        payload: dict[str, Any] | None = None,
    ) -> datetime:
        event = event_store.append(
            call_id=event_call_id,
            type=event_type,
            source=source,
            payload=payload,
            timestamp=current_event_timestamp,
        )
        registry.get(event_call_id).last_event_at = event.timestamp
        return event.timestamp

    runner._append_event = append_fixture_event

    if bool(fixture.get("openingStarted")):
        event_store.append(
            call_id,
            "opening_started",
            "agent",
            {"openingMessageHash": "sha256:audio-authority-fixture"},
            timestamp=started_at,
        )

    guard = runner._playback_guard(call_id)
    guard.current_response_id = response_id
    guard.current_response_generation = generation
    guard.generation = generation
    guard.current_response_audio_published = bool(fixture.get("playbackTarget", True))

    lifecycle = runner._response_lifecycle(call_id)
    lifecycle.current_response_is_opening = bool(fixture.get("opening"))
    ai_frames_by_offset = {frame.offset_ms: frame for frame in ai_frames}

    for replay_frame in frames:
        current_event_timestamp = started_at + timedelta(
            milliseconds=replay_frame.offset_ms
        )
        ai_replay_frame = ai_frames_by_offset.get(replay_frame.offset_ms)
        if ai_replay_frame is not None:
            ai_rms_dbfs = _pcm16_rms_dbfs(ai_replay_frame.frame.data)
            if ai_rms_dbfs is not None:
                runner._last_ai_audio_rms_dbfs[call_id] = ai_rms_dbfs
                runner._last_ai_audio_published_at[call_id] = current_event_timestamp
        elif isinstance(fixture.get("recentAiAudio"), dict):
            recent_ai_audio = fixture["recentAiAudio"]
            ai_rms_dbfs = recent_ai_audio.get("rmsDbfs")
            if ai_rms_dbfs is not None:
                runner._last_ai_audio_rms_dbfs[call_id] = float(ai_rms_dbfs)
            age_ms = _positive_int(recent_ai_audio.get("ageMs"), default=0)
            runner._last_ai_audio_published_at[call_id] = (
                current_event_timestamp - timedelta(milliseconds=age_ms)
            )
        observation = detector.observe(
            call_id,
            replay_frame.frame,
            now=current_event_timestamp,
            interruptible=replay_frame.interruptible,
        )
        turn = runner._pending_user_turns.get(call_id)
        if not observation.candidate and not (
            observation.active
            and turn is not None
            and turn.sip_barge_in_requested
            and not turn.sip_pre_stop_requested
        ):
            continue
        await runner._handle_sip_barge_in_candidate(
            call_id=call_id,
            provider=provider,
            trigger_timestamp=current_event_timestamp,
            observation=observation,
        )

    return build_p1_evaluation(
        call_id=call_id,
        record={
            "callId": call_id,
            "startedAt": _format_fixture_time(started_at),
        },
        events=_authority_fixture_events_from_store(event_store.list_all(call_id)),
    )


def _audio_fixture_ai_replay_frames(
    fixture: dict[str, Any],
    *,
    base_dir: Path,
    sample_rate_hz: int,
    frame_ms: int,
) -> list[Any]:
    ai_wav_path = fixture.get("aiWavPath")
    if isinstance(ai_wav_path, str) and ai_wav_path:
        pcm16_mono, ai_sample_rate_hz = _read_pcm16_mono_wav(
            base_dir / ai_wav_path,
            fixture=fixture,
        )
        if ai_sample_rate_hz != sample_rate_hz:
            raise RuntimeError("sample matrix audioFixture aiWavPath sampleRateHz mismatch")
        return list(
            pcm16_mono_to_replay_frames(
                pcm16_mono,
                sample_rate_hz=sample_rate_hz,
                frame_duration_ms=frame_ms,
            )
        )

    ai_frame_amplitudes = fixture.get("aiFrameAmplitudes")
    if isinstance(ai_frame_amplitudes, list):
        pcm16_mono = _pcm16_mono_from_frame_amplitudes(
            ai_frame_amplitudes,
            sample_rate_hz=sample_rate_hz,
            frame_ms=frame_ms,
        )
        return list(
            pcm16_mono_to_replay_frames(
                pcm16_mono,
                sample_rate_hz=sample_rate_hz,
                frame_duration_ms=frame_ms,
            )
        )
    return []


def _audio_fixture_pcm16_mono(
    fixture: dict[str, Any],
    *,
    base_dir: Path,
) -> tuple[bytes, int]:
    wav_path = fixture.get("wavPath")
    if isinstance(wav_path, str) and wav_path:
        return _read_pcm16_mono_wav(base_dir / wav_path, fixture=fixture)

    frame_amplitudes = fixture.get("frameAmplitudes")
    if isinstance(frame_amplitudes, list):
        sample_rate_hz = _positive_int(fixture.get("sampleRateHz"), default=16_000)
        frame_ms = _positive_int(fixture.get("frameMs"), default=20)
        return _pcm16_mono_from_frame_amplitudes(
            frame_amplitudes,
            sample_rate_hz=sample_rate_hz,
            frame_ms=frame_ms,
        ), sample_rate_hz

    segments = fixture.get("pcmSegments")
    if not isinstance(segments, list):
        raise RuntimeError(
            "sample matrix audioFixture requires pcmSegments, frameAmplitudes, or wavPath"
        )
    sample_rate_hz = _positive_int(fixture.get("sampleRateHz"), default=16_000)
    return _pcm16_mono_from_segments(
        segments,
        sample_rate_hz=sample_rate_hz,
    ), sample_rate_hz


def _read_pcm16_mono_wav(path: Path, *, fixture: dict[str, Any]) -> tuple[bytes, int]:
    with wave.open(str(path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate_hz = wav_file.getframerate()
        if channels != 1 or sample_width != 2:
            raise RuntimeError("sample matrix audioFixture wavPath must be PCM16 mono")
        configured_sample_rate = fixture.get("sampleRateHz")
        if configured_sample_rate is not None and int(configured_sample_rate) != sample_rate_hz:
            raise RuntimeError("sample matrix audioFixture wavPath sampleRateHz mismatch")
        return wav_file.readframes(wav_file.getnframes()), sample_rate_hz


def _pcm16_rms_dbfs(pcm16_mono: bytes) -> float | None:
    if not pcm16_mono:
        return None
    samples = [sample for (sample,) in struct.iter_unpack("<h", pcm16_mono)]
    if not samples:
        return None
    rms = math.sqrt(sum(sample * sample for sample in samples) / len(samples))
    if rms <= 0:
        return None
    return 20 * math.log10(rms / 32768.0)


def _pcm16_mono_from_segments(
    segments: list[Any],
    *,
    sample_rate_hz: int,
) -> bytes:
    chunks: list[bytes] = []
    for index, raw_segment in enumerate(segments):
        if not isinstance(raw_segment, dict):
            raise RuntimeError(f"sample matrix pcmSegments[{index}] must be an object")
        duration_ms = _positive_int(raw_segment.get("durationMs"))
        amplitude = _pcm16_amplitude(raw_segment.get("amplitude"))
        sample_count = sample_rate_hz * duration_ms // 1000
        chunks.append(struct.pack("<h", amplitude) * sample_count)
    return b"".join(chunks)


def _pcm16_mono_from_frame_amplitudes(
    frame_amplitudes: list[Any],
    *,
    sample_rate_hz: int,
    frame_ms: int,
) -> bytes:
    samples_per_frame = sample_rate_hz * frame_ms // 1000
    if samples_per_frame <= 0:
        raise RuntimeError("sample matrix frameMs is too small for sampleRateHz")
    chunks: list[bytes] = []
    for index, raw_amplitude in enumerate(frame_amplitudes):
        try:
            amplitude = _pcm16_amplitude(raw_amplitude)
        except Exception as exc:
            raise RuntimeError(
                f"sample matrix frameAmplitudes[{index}] must fit PCM16"
            ) from exc
        chunks.append(struct.pack("<h", amplitude) * samples_per_frame)
    return b"".join(chunks)


def _audio_fixture_vad_windows(
    fixture: dict[str, Any],
    *,
    call_id: str,
) -> list[SpeechWindow]:
    raw_windows = fixture.get("vadWindows")
    if not isinstance(raw_windows, list):
        raise RuntimeError(f"sample matrix audioFixture {call_id} requires vadWindows")
    windows: list[SpeechWindow] = []
    for index, raw_window in enumerate(raw_windows):
        if not isinstance(raw_window, dict):
            raise RuntimeError(f"sample matrix vadWindows[{index}] must be an object")
        windows.append(
            SpeechWindow(
                start_ms=_positive_int(raw_window.get("startMs"), default=0),
                end_ms=_positive_int(raw_window.get("endMs")),
            )
        )
    return windows


def _audio_replay_events(
    *,
    call_id: str,
    started_at: datetime,
    provider_report: SipBargeInProviderReplayReport,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    response_id = f"resp_{call_id}"
    for event in provider_report.candidate_events:
        events.append(
            _audio_replay_event(
                event_type="sip_interrupt_candidate",
                event=event,
                started_at=started_at,
                response_id=response_id,
                generation=0,
            )
        )
    for event in provider_report.pre_stop_events:
        events.append(
            _audio_replay_event(
                event_type="sip_pre_stop",
                event=event,
                started_at=started_at,
                response_id=response_id,
                generation=1,
            )
        )
    events.sort(key=lambda event: str(event.get("eventTime") or ""))
    return events


def _audio_replay_event(
    *,
    event_type: str,
    event: Any,
    started_at: datetime,
    response_id: str,
    generation: int,
) -> dict[str, Any]:
    observation = event.observation
    return {
        "eventType": event_type,
        "source": "agent",
        "eventTime": _format_fixture_time(
            started_at + timedelta(milliseconds=int(event.offset_ms))
        ),
        "payload": {
            "responseId": response_id,
            "generation": generation,
            "reason": event.reason,
            "candidateClass": event.candidate_class,
            "candidateDurationMs": observation.candidate_duration_ms,
            "wallClockSpeechMs": observation.speech_duration_ms,
            "vadVoicedMs": observation.vad_voiced_ms,
            "rmsDbfs": observation.rms_dbfs,
            "snrDb": observation.snr_db,
            "peakDbfs": observation.peak_dbfs,
        },
    }


def _positive_int(value: Any, *, default: int | None = None) -> int:
    if value is None:
        if default is None:
            raise RuntimeError("sample matrix expected a positive integer")
        return default
    if isinstance(value, bool):
        raise RuntimeError("sample matrix expected a positive integer")
    result = int(value)
    if result < 0:
        raise RuntimeError("sample matrix expected a positive integer")
    return result


def _pcm16_amplitude(value: Any) -> int:
    if isinstance(value, bool):
        raise RuntimeError("sample matrix pcmSegments amplitude must be an integer")
    amplitude = int(value)
    if amplitude < -32768 or amplitude > 32767:
        raise RuntimeError("sample matrix pcmSegments amplitude must fit PCM16")
    return amplitude


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RuntimeError("sample matrix expected a number")
    return float(value)


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _parse_fixture_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_fixture_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00",
        "Z",
    )


def _fixture_reports_from_matrix(matrix: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_fixtures = matrix.get("fixtureReports")
    if raw_fixtures is None:
        return {}
    if not isinstance(raw_fixtures, dict):
        raise RuntimeError("sample matrix fixtureReports must be an object")

    reports: dict[str, dict[str, Any]] = {}
    for raw_call_id, raw_fixture in raw_fixtures.items():
        call_id = str(raw_call_id or "")
        if not call_id:
            continue
        reports[call_id] = _build_fixture_report(call_id=call_id, fixture=raw_fixture)
    return reports


def _build_fixture_report(*, call_id: str, fixture: Any) -> dict[str, Any]:
    if not isinstance(fixture, dict):
        raise RuntimeError(f"sample matrix fixtureReport {call_id} must be an object")

    record = fixture.get("record") or {"callId": call_id}
    if not isinstance(record, dict):
        raise RuntimeError(f"sample matrix fixtureReport {call_id} record must be an object")

    events = fixture.get("events")
    if not isinstance(events, list):
        raise RuntimeError(f"sample matrix fixtureReport {call_id} missing events list")

    dialogue_segments = fixture.get("dialogueSegments") or []
    if not isinstance(dialogue_segments, list):
        raise RuntimeError(
            f"sample matrix fixtureReport {call_id} dialogueSegments must be a list"
        )

    return build_p1_evaluation(
        call_id=call_id,
        record=record,
        events=[event for event in events if isinstance(event, dict)],
        dialogue_segments=[
            segment for segment in dialogue_segments if isinstance(segment, dict)
        ],
    )


def _sample_matrix_has_failures(report: dict[str, Any]) -> bool:
    summary = report.get("summary")
    if not isinstance(summary, dict):
        return False
    if summary.get("failed"):
        return True
    coverage = summary.get("coverage")
    return isinstance(coverage, dict) and bool(coverage.get("failureCount"))


def _record_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    detail = _unwrap_data(get_json(f"{base_url}/ai-call/records/{call_id}", timeout_seconds))
    record = detail.get("record") if isinstance(detail, dict) else None
    if not isinstance(record, dict):
        raise RuntimeError("record detail response missing data.record")
    return record


def _events_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> list[dict[str, Any]]:
    response = _unwrap_data(
        get_json(f"{base_url}/ai-call/records/{call_id}/events?limit=1000", timeout_seconds)
    )
    rows = response.get("rows") if isinstance(response, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("event response missing data.rows")
    return [row for row in rows if isinstance(row, dict)]


def _dialogue_segments_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> list[dict[str, Any]]:
    response = _unwrap_data(
        get_json(
            f"{base_url}/ai-call/records/{call_id}/dialogue-segments?limit=1000",
            timeout_seconds,
        )
    )
    rows = response.get("rows") if isinstance(response, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("dialogue segment response missing data.rows")
    return [row for row in rows if isinstance(row, dict)]


def _record_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("rows")
    if rows is None and isinstance(response.get("data"), dict):
        rows = response["data"].get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("record list response missing rows")
    return [row for row in rows if isinstance(row, dict)]


def _get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(raw_body) from exc


def _unwrap_data(response: dict[str, Any]) -> Any:
    if response.get("code") not in (None, 200):
        raise RuntimeError(json.dumps(response, ensure_ascii=False))
    return response.get("data")


def _print_text_report(report: dict[str, Any], stdout: TextIO) -> None:
    if report.get("mode") == "sample_matrix":
        _print_matrix_report(report, stdout)
        return
    if report.get("mode") == "recent":
        _print_recent_report(report, stdout)
        return
    _print_call_report(report, stdout)


def _print_matrix_report(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report["summary"]
    print(
        "p1_sample_matrix "
        f"samples={summary['samples']} "
        f"passed={summary['passed']} "
        f"failed={summary['failed']} "
        f"missingReports={summary['missingReports']}",
        file=stdout,
    )
    for sample in report.get("samples", []):
        status = "pass" if sample.get("passed") else "fail"
        evidence = sample.get("evidence") if isinstance(sample.get("evidence"), dict) else {}
        print(
            "sample "
            f"id={sample.get('id')} "
            f"callId={sample.get('callId')} "
            f"sourceType={sample.get('sourceType')} "
            f"evaluationSource={sample.get('evaluationSource')} "
            f"category={sample.get('category')} "
            f"expectation={sample.get('expectation')} "
            f"status={status} "
            f"reason={sample.get('reason')} "
            f"candidateTime={evidence.get('candidateTime')} "
            f"preStopTime={evidence.get('preStopTime')} "
            f"outcome={evidence.get('outcome')} "
            f"decisionReason={evidence.get('decisionReason')}",
            file=stdout,
        )
    coverage = summary.get("coverage")
    if not isinstance(coverage, dict):
        return
    status = "pass" if coverage.get("passed") else "fail"
    print(
        f"coverage status={status} failures={coverage.get('failureCount', 0)}",
        file=stdout,
    )
    for failure in coverage.get("failures") or []:
        if not isinstance(failure, dict):
            continue
        print(
            "coverage_failure "
            f"gate={failure.get('gate')} "
            f"key={failure.get('key')} "
            f"required={failure.get('required')} "
            f"actual={failure.get('actual')}",
            file=stdout,
        )


def _print_recent_report(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report["summary"]
    print(
        "p1_eval "
        f"calls={summary['calls']} "
        f"windows={summary['windows']} "
        f"confirmedPreStops={summary['confirmedPreStops']} "
        f"falsePreStops={summary['falsePreStops']} "
        f"candidateOnly={summary['candidateOnly']} "
        f"preStopPending={summary['preStopPending']} "
        f"providerSpeechStarted={summary['providerSpeechStarted']} "
        f"failed={summary['failedCalls']}",
        file=stdout,
    )
    for call in report.get("calls", []):
        _print_call_summary(call, stdout)
    for failed in report.get("failedCalls", []):
        print(
            f"failed callId={failed.get('callId')} error={failed.get('error')}",
            file=stdout,
        )


def _print_call_report(report: dict[str, Any], stdout: TextIO) -> None:
    _print_call_summary(report, stdout)
    for window in report.get("windows", []):
        print(
            "window "
            f"outcome={window.get('outcome')} "
            f"severity={window.get('severity')} "
            f"responseId={window.get('responseId')} "
            f"candidateTime={window.get('candidateTime')} "
            f"preStopTime={window.get('preStopTime')} "
            f"candidateToPreStopMs={window.get('candidateToPreStopMs')} "
            f"decision={window.get('decisionEventType')} "
            f"reason={window.get('decisionReason')} "
            f"rmsDbfs={window.get('rmsDbfs')} "
            f"snrDb={window.get('snrDb')} "
            f"quality={window.get('speechQualityRejection')}",
            file=stdout,
        )


def _print_call_summary(report: dict[str, Any], stdout: TextIO) -> None:
    summary = report.get("summary") or {}
    print(
        "call "
        f"callId={report.get('callId')} "
        f"windows={len(report.get('windows') or [])} "
        f"confirmedPreStops={summary.get('confirmedPreStops', 0)} "
        f"falsePreStops={summary.get('falsePreStops', 0)} "
        f"candidateOnly={summary.get('candidateOnly', 0)} "
        f"preStopPending={summary.get('preStopPending', 0)} "
        f"providerSpeechStarted={summary.get('providerSpeechStarted', 0)}",
        file=stdout,
    )


if __name__ == "__main__":
    main()
