from __future__ import annotations

from typing import Any

VOICEMAIL_MARKERS = (
    "语音信箱",
    "语音留言",
    "提示音后录制留言",
    "录音完成后挂断",
)


def detect_answer_type(
    *,
    call_result: str | None,
    analysis_status: str | None,
    analysis_result: dict[str, Any] | None,
) -> str | None:
    if call_result != "connected":
        return None
    if analysis_status != "2" or not isinstance(analysis_result, dict):
        return "transport"
    if is_voicemail_analysis(analysis_result):
        return "voicemail"
    return "human" if analysis_result.get("valid_dialogue") is True else "transport"


def business_call_result(call_result: str | None, answer_type: str | None) -> str | None:
    """业务接通不包含语音信箱，保留底层呼叫结果用于信令排查。"""
    return "no_answer" if call_result in {"connected", "early_hangup"} and answer_type == "voicemail" else call_result


def is_voicemail_analysis(analysis_result: dict[str, Any]) -> bool:
    if analysis_result.get("valid_dialogue") is True:
        return False
    values = [
        analysis_result.get("summary"),
        analysis_result.get("reason"),
        *(analysis_result.get("tags") or []),
        *(analysis_result.get("key_points") or []),
    ]
    text = " ".join(str(value) for value in values if value)
    return any(marker in text for marker in VOICEMAIL_MARKERS)
