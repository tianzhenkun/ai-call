from __future__ import annotations

import re
from typing import Any

from app.services.ai_call.dialogue_merge import normalize_dialogue_text

VOICEMAIL_MARKERS = (
    "语音信箱",
    "语音留言",
    "录音留言",
    "无法接听，请留言",
    "嘟声后留言",
    "提示音后录制留言",
    "录音完成后挂断",
)

AUTOMATED_VOICEMAIL_PROMPTS = (
    "请在提示音后留言",
    "请在嘟声后留言",
    "请在哔声后留言",
    "请在听到提示音后留言",
    "请在提示音后录制留言",
    "please leave a message after the tone",
    "please leave your message after the tone",
)


def is_clear_voicemail_prompt(text: str) -> bool:
    # 转述以前听到的提示语，不是当前正在播放自动留言提示。
    if re.search(r"(?:上次|之前|刚才|以前|曾经).{0,24}(?:听到|听见|播放|说)|\b(?:i|you) (?:heard|said)\b", text, re.I):
        return False
    normalized = re.sub(r"[\s，。,.！!？?：:]", "", text).lower()
    if any(
        re.sub(r"[\s，。,.！!？?：:]", "", marker).lower() in normalized
        for marker in AUTOMATED_VOICEMAIL_PROMPTS
    ):
        return True
    # 容许转写中的措辞变化；仍要求录音提示，不把一般的“请留言”当作语音信箱。
    return bool(re.search(
        r"(?:提示音|嘟(?:的)?(?:一)?声|哔声|蜂鸣声).{0,4}后.{0,20}(?:留言|留下|录制)"
        r"|leave.{0,35}(?:message|name|number).{0,15}after(?:the)?(?:beep|tone)"
        r"|(?:无法接听|不方便接听|暂时不在).{0,30}(?:录下|录制).{0,20}(?:留言|姓名|电话|来电事由)",
        normalized,
    ))


def realtime_answer_type(payload: dict[str, Any]) -> str | None:
    """只消费运行时经原话校验的事件，不将模型声明直接当作收费依据。"""
    if (
        payload.get("verificationVersion") != 1
        or not isinstance(payload.get("evidenceQuote"), str)
        or not payload["evidenceQuote"].strip()
        or not isinstance(payload.get("customerTranscript"), str)
        or normalize_dialogue_text(payload["evidenceQuote"]) not in normalize_dialogue_text(payload["customerTranscript"])
        or not payload.get("customerTranscriptEventId")
    ):
        return None
    if (
        payload.get("answerType") == "human" and payload.get("contextEventId")
        and isinstance(payload.get("contextQuote"), str) and payload["contextQuote"].strip()
        and not is_clear_voicemail_prompt(payload["customerTranscript"])
    ):
        return "human"
    return None


def detect_answer_type(
    *,
    call_result: str | None,
    analysis_status: str | None,
    analysis_result: dict[str, Any] | None,
) -> str | None:
    if call_result not in {"connected", "early_hangup"}:
        return None
    if analysis_status != "2" or not isinstance(analysis_result, dict):
        return "transport"
    verified_type = analysis_result.get("answer_type")
    if verified_type in {"human", "voicemail"}:
        return verified_type
    if "answer_type" in analysis_result:
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
