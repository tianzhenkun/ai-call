from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_call.timeline_audit import build_timeline_audit

DEFAULT_BASE_URL = "http://127.0.0.1:19012"
ASSISTANT_CUSTOMER_VOICE_PATTERN = re.compile(
    r"(我们这边|我这边|我司|我们公司).{0,24}"
    r"(合同量|法务|业务|需求|风险|痛点|获客|增长|人手|漏审|担心)"
)
TEXT_NORMALIZE_PATTERN = re.compile(r"[\W_]+", re.UNICODE)
CJK_PATTERN = re.compile(r"[\u3400-\u9fff]")
LATIN_PATTERN = re.compile(r"[A-Za-z]")
RECORD_ONLY_SHORT_BLOCK_PATTERN = re.compile(r"(不帮我|别打|别联系|你管我|不聊|别管)")
SHORT_BACKGROUND_TEXTS = {
    "你好",
    "知道了",
    "行",
    "好",
    "好的",
    "可以",
    "方便",
    "嗯",
    "嗯嗯",
    "哦",
    "是的",
}
GetJson = Callable[[str, float], dict[str, Any]]
_SEMANTIC_MODULE: Any | None = None


@dataclass(frozen=True)
class DialogueSegmentRow:
    call_id: str
    segment_no: int
    speaker_type: str
    speaker_identity: str | None
    source: str
    source_segment_id: str
    segment_text: str
    segment_status: str
    started_at: datetime | None
    ended_at: datetime | None
    duration_ms: int | None
    failure_stage: str | None = None
    failure_message: str | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch acceptance report for AI Call semantic P1 closure.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--call-id",
        action="append",
        default=[],
        help="Call ID to verify. Can be repeated.",
    )
    parser.add_argument(
        "--recent",
        type=int,
        help="Verify the latest N calls instead of explicit call IDs.",
    )
    parser.add_argument("--entry-type", default="web")
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--output", help="Optional markdown report path.")
    parser.add_argument("--json-output", help="Optional JSON report path.")
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
    if bool(args.call_id) == bool(args.recent):
        parser.error("exactly one of --call-id or --recent is required")
    if args.recent is not None and args.recent < 1:
        parser.error("--recent must be greater than 0")

    get_json = get_json or _get_json
    try:
        report = _build_report(
            base_url=str(args.base_url).rstrip("/"),
            call_ids=[str(call_id) for call_id in args.call_id],
            recent=args.recent,
            entry_type=str(args.entry_type),
            timeout_seconds=float(args.timeout_seconds),
            get_json=get_json,
        )
    except Exception as exc:
        print(f"semantic acceptance failed: {exc!s}", file=stderr)
        return 1

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(_markdown_report(report), encoding="utf-8")
    if args.json_output:
        json_path = Path(args.json_output)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2), file=stdout)
    else:
        print(_text_summary(report), file=stdout)
    return 0 if report["summary"]["verdict"] == "PASS" else 1


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _build_report(
    *,
    base_url: str,
    call_ids: list[str],
    recent: int | None,
    entry_type: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any]:
    source_total: Any = None
    requested = len(call_ids) if call_ids else recent
    if recent is not None:
        response = get_json(
            f"{base_url}/ai-call/records?entryType={entry_type}&pageSize={recent}",
            timeout_seconds,
        )
        rows = _record_rows(response)
        source_total = _source_total(response)
        call_ids = [str(row.get("callId") or "") for row in rows]

    call_reports: list[dict[str, Any]] = []
    failed_calls: list[dict[str, str]] = []
    for call_id in call_ids:
        if not call_id:
            continue
        try:
            call_reports.append(
                _build_call_report(
                    base_url=base_url,
                    call_id=call_id,
                    timeout_seconds=timeout_seconds,
                    get_json=get_json,
                )
            )
        except Exception as exc:
            failed_calls.append({"callId": call_id, "error": str(exc)})

    summary = _suite_summary(call_reports, failed_calls)
    return {
        "mode": "semantic_acceptance",
        "baseUrl": base_url,
        "entryType": entry_type,
        "requested": requested,
        "sourceTotal": source_total,
        "summary": summary,
        "calls": call_reports,
        "failedCalls": failed_calls,
    }


def _build_call_report(
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
    semantic_analysis = _semantic_analysis_for_call(
        base_url,
        call_id,
        timeout_seconds,
        get_json,
    )
    segment_rows = [
        _segment_row(row, fallback_call_id=call_id, index=index)
        for index, row in enumerate(dialogue_segments)
    ]
    semantic_module = _semantic_module()
    rebuilt_snapshot = semantic_module.SemanticTranscriptBuilder().build(
        call_id=call_id,
        scene_code=_optional_text(record.get("sceneCode") or record.get("scene_code")),
        rows=segment_rows,
    )
    stored_snapshot = _stored_transcript_snapshot(semantic_analysis)
    semantic_snapshot = stored_snapshot or rebuilt_snapshot
    semantic_snapshot_source = (
        "stored_semantic_analysis"
        if stored_snapshot is not None
        else "rebuilt_dialogue_segments"
    )
    timeline_report = build_timeline_audit(
        call_id=call_id,
        record=record,
        events=events,
        dialogue_segments=dialogue_segments,
    )
    quality_issues = [
        *_assistant_customer_voice_issues(rebuilt_snapshot),
        *_human_agent_crosstalk_contract_issues(segment_rows, rebuilt_snapshot),
        *_semantic_result_issues(
            semantic_analysis,
            semantic_snapshot,
            assistant_snapshot=rebuilt_snapshot,
        ),
    ]
    timeline_high = int((timeline_report.get("summary") or {}).get("highSeverityCount") or 0)
    high_count = timeline_high + _count_issues(quality_issues, "high")
    review_count = _count_issues(quality_issues, "review")
    verdict = "FAIL" if high_count else "REVIEW" if review_count else "PASS"
    return {
        "callId": call_id,
        "record": _record_summary(record),
        "verdict": verdict,
        "highIssueCount": high_count,
        "reviewIssueCount": review_count,
        "timelineAudit": timeline_report,
        "semanticSnapshot": _snapshot_summary(
            semantic_snapshot,
            source=semantic_snapshot_source,
        ),
        "snapshotComparison": _snapshot_comparison(stored_snapshot, rebuilt_snapshot),
        "storedSemanticAnalysis": _semantic_analysis_summary(semantic_analysis),
        "qualityIssues": quality_issues,
    }


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


def _semantic_analysis_for_call(
    base_url: str,
    call_id: str,
    timeout_seconds: float,
    get_json: GetJson,
) -> dict[str, Any] | None:
    response = _unwrap_data(
        get_json(
            f"{base_url}/ai-call/records/{call_id}/semantic-analysis",
            timeout_seconds,
        )
    )
    return response if isinstance(response, dict) else None


def _stored_transcript_snapshot(analysis: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(analysis, dict):
        return None
    snapshot = analysis.get("transcriptSnapshot") or analysis.get("transcript_snapshot")
    if isinstance(snapshot, dict) and isinstance(snapshot.get("turns"), list):
        return snapshot
    return None


def _assistant_customer_voice_issues(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for turn in snapshot.get("turns") or []:
        if not isinstance(turn, dict) or turn.get("role") != "assistant":
            continue
        text = str(turn.get("text") or "").strip()
        if not ASSISTANT_CUSTOMER_VOICE_PATTERN.search(text):
            continue
        issues.append({
            "type": "assistant_customer_voice_risk",
            "severity": "high",
            "turnSeq": turn.get("seq"),
            "source": turn.get("source"),
            "text": _truncate(text),
            "reason": "assistant turn contains customer-side first-person business pain",
        })
    return issues


def _semantic_result_issues(
    analysis: dict[str, Any] | None,
    snapshot: dict[str, Any],
    *,
    assistant_snapshot: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if analysis is None:
        return [{
            "type": "semantic_analysis_missing",
            "severity": "review",
            "reason": "stored semantic analysis record is missing",
        }]

    issues: list[dict[str, Any]] = []
    status = str(analysis.get("analysisStatus") or analysis.get("analysis_status") or "")
    if status != "2":
        issues.append({
            "type": "semantic_analysis_not_succeeded",
            "severity": "review",
            "analysisStatus": status,
            "reason": "stored semantic analysis is not succeeded",
        })

    stored_snapshot = _stored_transcript_snapshot(analysis)
    raw_result = analysis.get("analysisResult") or analysis.get("analysis_result")
    if not isinstance(raw_result, dict):
        issues.append({
            "type": "semantic_analysis_result_missing",
            "severity": "review",
            "reason": "stored semantic analysis result is missing",
        })
    else:
        semantic_module = _semantic_module()
        normalized_result = semantic_module.normalize_analysis_result(raw_result)
        enforced_result = semantic_module.enforce_semantic_evidence_on_result(
            normalized_result,
            snapshot,
        )
        if enforced_result != normalized_result:
            issues.append({
                "type": "semantic_result_changed_by_evidence_gate",
                "severity": "high",
                "reason": "current semantic evidence gate would change stored result",
            })
        issues.extend(_assistant_text_leak_issues(normalized_result, assistant_snapshot or snapshot))
        issues.extend(_record_only_leak_issues(normalized_result, snapshot))
        issues.extend(
            _absent_user_quote_issues(
                normalized_result,
                snapshot,
                stored_snapshot=stored_snapshot,
            )
        )
    return issues


def _assistant_text_leak_issues(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    rendered = _normalized_result_text(result)
    issues: list[dict[str, Any]] = []
    for turn in snapshot.get("turns") or []:
        if not isinstance(turn, dict) or turn.get("role") != "assistant":
            continue
        text = str(turn.get("text") or "")
        for phrase in _salient_phrases(text):
            if phrase in rendered:
                issues.append({
                    "type": "assistant_text_leaked_into_semantic_result",
                    "severity": "high",
                    "turnSeq": turn.get("seq"),
                    "matchedText": _truncate(phrase, limit=40),
                    "reason": "stored semantic result contains text from assistant turn",
                })
                return issues
    return issues


def _record_only_leak_issues(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    rendered = _normalized_result_text(result)
    issues: list[dict[str, Any]] = []
    for turn in snapshot.get("turns") or []:
        if not isinstance(turn, dict) or turn.get("role") != "user":
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict):
            continue
        if evidence.get("analysis_usage") != "record_only":
            continue
        normalized_text = _normalize_text(str(turn.get("text") or ""))
        if (
            normalized_text in SHORT_BACKGROUND_TEXTS
            or (
                len(normalized_text) < 6
                and not RECORD_ONLY_SHORT_BLOCK_PATTERN.search(normalized_text)
            )
            or normalized_text not in rendered
        ):
            continue
        issues.append({
            "type": "record_only_user_text_leaked_into_semantic_result",
            "severity": "high",
            "turnSeq": turn.get("seq"),
            "matchedText": _truncate(normalized_text, limit=40),
            "reason": "stored semantic result contains record_only user text",
        })
    return issues


def _human_agent_crosstalk_contract_issues(
    rows: list[DialogueSegmentRow],
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    customer_rows = [
        row
        for row in rows
        if row.speaker_type == "customer"
        and row.segment_status == "final"
        and row.segment_text.strip()
    ]
    issues: list[dict[str, Any]] = []
    for row in rows:
        if row.speaker_type != "human_agent" or row.source != "offline_asr":
            continue
        if not _looks_like_language_mismatch(row.segment_text):
            continue
        if not any(_time_ranges_overlap(row, customer_row) for customer_row in customer_rows):
            continue
        turn = _matching_human_agent_turn(snapshot, row)
        if _has_human_agent_crosstalk_marker(turn):
            continue
        issues.append({
            "type": "human_agent_crosstalk_not_marked_low_confidence",
            "severity": "high",
            "turnSeq": (turn or {}).get("seq") or row.segment_no,
            "text": _truncate(row.segment_text),
            "reason": (
                "human_agent offline ASR has English-like overlap with customer audio "
                "but snapshot lacks low-confidence crosstalk marker"
            ),
        })
    return issues


def _matching_human_agent_turn(
    snapshot: dict[str, Any],
    row: DialogueSegmentRow,
) -> dict[str, Any] | None:
    for turn in snapshot.get("turns") or []:
        if not isinstance(turn, dict):
            continue
        if turn.get("speaker_type") != "human_agent":
            continue
        if turn.get("source") != row.source:
            continue
        if str(turn.get("text") or "").strip() == row.segment_text.strip():
            return turn
    return None


def _has_human_agent_crosstalk_marker(turn: dict[str, Any] | None) -> bool:
    if not isinstance(turn, dict):
        return False
    quality = turn.get("transcript_quality")
    if not isinstance(quality, dict) or not quality.get("low_confidence_source"):
        return False
    reason_codes = quality.get("reason_codes")
    return isinstance(reason_codes, list) and "human_agent_track_customer_overlap" in reason_codes


def _absent_user_quote_issues(
    result: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    stored_snapshot: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    current_user_texts = _snapshot_user_texts(snapshot)
    if isinstance(stored_snapshot, dict):
        current_user_texts.update(_snapshot_user_texts(stored_snapshot))
    issues: list[dict[str, Any]] = []
    for quoted_text in _quoted_result_texts(result):
        normalized_text = _normalize_text(quoted_text)
        if len(normalized_text) < 3 or _user_quote_present(
            normalized_text,
            current_user_texts,
        ):
            continue
        issues.append({
            "type": "semantic_result_quote_absent_from_rebuilt_user_turns",
            "severity": "review",
            "matchedText": _truncate(quoted_text, limit=40),
            "reason": "stored semantic result quotes user text absent from rebuilt snapshot",
        })
    return issues


def _snapshot_user_texts(snapshot: dict[str, Any]) -> set[str]:
    return {
        _normalize_text(str(turn.get("text") or ""))
        for turn in snapshot.get("turns") or []
        if isinstance(turn, dict) and turn.get("role") == "user"
    }


def _user_quote_present(
    normalized_quote: str,
    normalized_user_texts: set[str],
) -> bool:
    return any(
        normalized_quote == user_text
        or normalized_quote in user_text
        or user_text in normalized_quote
        for user_text in normalized_user_texts
    )


def _suite_summary(
    calls: list[dict[str, Any]],
    failed_calls: list[dict[str, str]],
) -> dict[str, Any]:
    high_count = sum(int(call.get("highIssueCount") or 0) for call in calls)
    review_count = sum(int(call.get("reviewIssueCount") or 0) for call in calls)
    verdict = (
        "FAIL"
        if high_count or failed_calls
        else "REVIEW"
        if review_count
        else "PASS"
    )
    return {
        "calls": len(calls),
        "passed": sum(1 for call in calls if call.get("verdict") == "PASS"),
        "review": sum(1 for call in calls if call.get("verdict") == "REVIEW"),
        "failed": sum(1 for call in calls if call.get("verdict") == "FAIL"),
        "high": high_count,
        "reviewIssues": review_count,
        "fetchFailed": len(failed_calls),
        "verdict": verdict,
    }


def _snapshot_summary(
    snapshot: dict[str, Any],
    *,
    source: str | None = None,
) -> dict[str, Any]:
    turns = [turn for turn in snapshot.get("turns") or [] if isinstance(turn, dict)]
    user_turns = [turn for turn in turns if turn.get("role") == "user"]
    assistant_turns = [turn for turn in turns if turn.get("role") == "assistant"]
    record_only_turns = [
        turn
        for turn in user_turns
        if isinstance(turn.get("semantic_evidence"), dict)
        and turn["semantic_evidence"].get("analysis_usage") == "record_only"
    ]
    usable_turns = [
        turn
        for turn in user_turns
        if isinstance(turn.get("semantic_evidence"), dict)
        and turn["semantic_evidence"].get("analysis_usage") == "use_as_customer_signal"
    ]
    unsupported_fact_turns = [
        turn
        for turn in user_turns
        if isinstance(turn.get("semantic_evidence"), dict)
        and turn["semantic_evidence"].get("unsupported_strong_fact_types")
    ]
    metadata = snapshot.get("metadata") if isinstance(snapshot.get("metadata"), dict) else {}
    transcript_quality = (
        metadata.get("transcript_quality")
        if isinstance(metadata.get("transcript_quality"), dict)
        else {}
    )
    summary = {
        "callId": snapshot.get("call_id"),
        "sceneCode": snapshot.get("scene_code"),
        "turnCount": len(turns),
        "userTurnCount": len(user_turns),
        "assistantTurnCount": len(assistant_turns),
        "recordOnlyUserTurnCount": len(record_only_turns),
        "usableUserSignalCount": len(usable_turns),
        "unsupportedStrongFactTurnCount": len(unsupported_fact_turns),
        "fallbackToRealtime": metadata.get("fallback_to_realtime"),
        "realtimeSupplementedCount": metadata.get("realtime_supplemented_count"),
        "offlineAsrQualityRejectedCount": metadata.get("offline_asr_quality_rejected_count"),
        "qualitySignals": transcript_quality.get("signals") or [],
        "qualityReasons": transcript_quality.get("reasons") or [],
    }
    if source is not None:
        summary["source"] = source
    return summary


def _snapshot_comparison(
    stored_snapshot: dict[str, Any] | None,
    rebuilt_snapshot: dict[str, Any],
) -> dict[str, Any]:
    if stored_snapshot is None:
        return {
            "storedPresent": False,
            "storedDiffersFromRebuilt": None,
        }
    return {
        "storedPresent": True,
        "storedDiffersFromRebuilt": (
            _snapshot_signature(stored_snapshot) != _snapshot_signature(rebuilt_snapshot)
        ),
    }


def _semantic_analysis_summary(analysis: dict[str, Any] | None) -> dict[str, Any]:
    if analysis is None:
        return {"present": False}
    result = analysis.get("analysisResult") or analysis.get("analysis_result")
    snapshot = analysis.get("transcriptSnapshot") or analysis.get("transcript_snapshot")
    return {
        "present": True,
        "analysisStatus": analysis.get("analysisStatus") or analysis.get("analysis_status"),
        "hasResult": isinstance(result, dict),
        "hasTranscriptSnapshot": isinstance(snapshot, dict),
        "transcriptHash": analysis.get("transcriptHash") or analysis.get("transcript_hash"),
    }


def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "callId",
        "entryType",
        "sceneCode",
        "promptSourceKey",
        "status",
        "endReason",
        "startedAt",
        "endedAt",
        "durationMs",
    )
    return {key: record.get(key) for key in keys if record.get(key) is not None}


def _segment_row(row: dict[str, Any], *, fallback_call_id: str, index: int) -> DialogueSegmentRow:
    segment_no = _int_value(row.get("segmentNo") or row.get("segment_no")) or index + 1
    source = str(row.get("source") or "unknown")
    return DialogueSegmentRow(
        call_id=str(row.get("callId") or row.get("call_id") or fallback_call_id),
        segment_no=segment_no,
        speaker_type=str(row.get("speakerType") or row.get("speaker_type") or ""),
        speaker_identity=_optional_text(row.get("speakerIdentity") or row.get("speaker_identity")),
        source=source,
        source_segment_id=str(
            row.get("sourceSegmentId")
            or row.get("source_segment_id")
            or f"{source}_{segment_no}"
        ),
        segment_text=str(row.get("segmentText") or row.get("segment_text") or row.get("text") or ""),
        segment_status=str(row.get("segmentStatus") or row.get("segment_status") or ""),
        started_at=_parse_optional_time(row.get("startedAt") or row.get("started_at")),
        ended_at=_parse_optional_time(row.get("endedAt") or row.get("ended_at")),
        duration_ms=_int_value(row.get("durationMs") or row.get("duration_ms")),
        failure_stage=_optional_text(row.get("failureStage") or row.get("failure_stage")),
        failure_message=_optional_text(row.get("failureMessage") or row.get("failure_message")),
    )


def _looks_like_language_mismatch(text: str) -> bool:
    latin_count = len(LATIN_PATTERN.findall(text))
    cjk_count = len(CJK_PATTERN.findall(text))
    if latin_count >= 2 and cjk_count == 0:
        return True
    return bool(latin_count >= 4 and cjk_count <= 1 and latin_count > cjk_count * 2)


def _time_ranges_overlap(left: DialogueSegmentRow, right: DialogueSegmentRow) -> bool:
    if left.started_at is None or right.started_at is None:
        return False
    left_end = left.ended_at or left.started_at
    right_end = right.ended_at or right.started_at
    return left.started_at <= right_end and right.started_at <= left_end


def _record_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("rows")
    if rows is None and isinstance(response.get("data"), dict):
        rows = response["data"].get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("record list response missing rows")
    return [row for row in rows if isinstance(row, dict)]


def _source_total(response: dict[str, Any]) -> Any:
    if "total" in response:
        return response.get("total")
    data = response.get("data")
    if isinstance(data, dict):
        return data.get("total")
    return None


def _unwrap_data(response: dict[str, Any]) -> Any:
    if response.get("code") not in (None, 200):
        raise RuntimeError(json.dumps(response, ensure_ascii=False))
    return response.get("data")


def _get_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(raw_body) from exc


def _semantic_module() -> Any:
    global _SEMANTIC_MODULE
    if _SEMANTIC_MODULE is None:
        importlib.import_module("app.api.v1.ai_call.controller")
        _SEMANTIC_MODULE = importlib.import_module("app.services.ai_call.semantic_analysis")
    return _SEMANTIC_MODULE


def _text_summary(report: dict[str, Any]) -> str:
    summary = report["summary"]
    return (
        "semantic_acceptance "
        f"calls={summary['calls']} "
        f"verdict={summary['verdict']} "
        f"high={summary['high']} "
        f"review={summary['reviewIssues']} "
        f"fetchFailed={summary['fetchFailed']}"
    )


def _markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# AI Call 语义分析 P1 验收报告",
        "",
        "> 本报告优先使用语义分析入库时保存的 transcript snapshot 复核结果；timeline 与 AI 口吻检查仍基于当前展示分段重建。历史 analysis_result 不会因代码修复自动回填。",
        "",
        f"- 基地址：`{report['baseUrl']}`",
        f"- 入口类型：`{report.get('entryType')}`",
        f"- 请求通话数：`{report.get('requested')}`",
        f"- 结论：`{_zh_verdict(summary['verdict'])}`",
        f"- 高危问题：`{summary['high']}`",
        f"- 需复核问题：`{summary['reviewIssues']}`",
        f"- 拉取失败：`{summary['fetchFailed']}`",
        "",
        "| Call ID | 场景 | 结论 | 高危 | 需复核 | Timeline 高危 | 用户轮次 | record_only | 质量原因 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for call in report.get("calls", []):
        timeline_summary = (call.get("timelineAudit") or {}).get("summary") or {}
        snapshot_summary = call.get("semanticSnapshot") or {}
        quality_reasons = snapshot_summary.get("qualityReasons") or []
        lines.append(
            "| "
            f"`{call.get('callId')}` | "
            f"`{(call.get('record') or {}).get('sceneCode') or '-'}` | "
            f"{_zh_verdict(call.get('verdict'))} | "
            f"{call.get('highIssueCount', 0)} | "
            f"{call.get('reviewIssueCount', 0)} | "
            f"{timeline_summary.get('highSeverityCount', 0)} | "
            f"{snapshot_summary.get('userTurnCount', 0)} | "
            f"{snapshot_summary.get('recordOnlyUserTurnCount', 0)} | "
            f"{_join_inline(quality_reasons)} |"
        )
    if report.get("failedCalls"):
        lines.extend(["", "## 拉取失败", ""])
        for failed in report["failedCalls"]:
            lines.append(f"- `{failed.get('callId')}`: {failed.get('error')}")
    lines.extend(["", "## 问题明细", ""])
    has_issue = False
    for call in report.get("calls", []):
        timeline_issues = (call.get("timelineAudit") or {}).get("issues") or []
        quality_issues = call.get("qualityIssues") or []
        for issue in [*timeline_issues, *quality_issues]:
            has_issue = True
            lines.append(
                "- "
                f"`{call.get('callId')}` "
                f"type=`{issue.get('type')}` "
                f"severity=`{issue.get('severity')}` "
                f"reason={issue.get('reason') or '-'} "
                f"text={issue.get('text') or issue.get('matchedText') or '-'}"
            )
    if not has_issue:
        lines.append("- 未发现高危或需复核问题。")
    return "\n".join(lines) + "\n"


def _snapshot_signature(snapshot: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    signature: list[tuple[str, str, str, str]] = []
    for turn in snapshot.get("turns") or []:
        if not isinstance(turn, dict):
            continue
        signature.append((
            str(turn.get("role") or ""),
            str(turn.get("text") or ""),
            str(turn.get("source") or ""),
            str(turn.get("segment_status") or turn.get("segmentStatus") or ""),
        ))
    return signature


def _normalized_result_text(result: dict[str, Any]) -> str:
    return _normalize_text(json.dumps(result, ensure_ascii=False, sort_keys=True))


def _quoted_result_texts(result: dict[str, Any]) -> list[str]:
    rendered_parts = [str(result.get("summary") or "")]
    key_points = result.get("key_points")
    if isinstance(key_points, list):
        rendered_parts.extend(str(point) for point in key_points)
    rendered = "\n".join(rendered_parts)
    values: list[str] = []
    for match in re.finditer(r"[‘“\"']([^’”\"']{2,30})[’”\"']", rendered):
        value = match.group(1).strip()
        if value:
            values.append(value)
    return values


def _salient_phrases(text: str) -> list[str]:
    phrases: list[str] = []
    for raw_part in re.split(r"[，。！？；,.!?;\s]+", text):
        value = _normalize_text(raw_part)
        if len(value) >= 6:
            phrases.append(value)
    full = _normalize_text(text)
    if len(full) >= 8:
        phrases.append(full)
    return phrases


def _normalize_text(text: str) -> str:
    return TEXT_NORMALIZE_PATTERN.sub("", text).lower()


def _count_issues(issues: list[dict[str, Any]], severity: str) -> int:
    return sum(1 for issue in issues if issue.get("severity") == severity)


def _zh_verdict(value: Any) -> str:
    return {
        "PASS": "通过",
        "REVIEW": "需复核",
        "FAIL": "失败",
    }.get(str(value), str(value))


def _join_inline(values: Any) -> str:
    if not isinstance(values, list) or not values:
        return "-"
    return "<br>".join(f"`{value}`" for value in values)


def _truncate(value: str, *, limit: int = 80) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def _parse_optional_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


if __name__ == "__main__":
    main()
