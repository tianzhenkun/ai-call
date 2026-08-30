from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CallEndDecisionAction = Literal["explicit_end", "not_end", "uncertain"]


@dataclass(frozen=True, slots=True)
class CallEndDecision:
    action: CallEndDecisionAction
    confidence: float
    reason: str
    summary: str
    source: str = "rule_fallback"


class RuleBasedCallEndDecisionService:
    """High-confidence local guard for hard call-end control commands."""

    NEGATIVE_PATTERNS = (
        "别挂",
        "不要挂",
        "先别挂",
        "先不要挂",
        "不用挂",
        "别结束",
        "不要结束",
        "先别结束",
        "先不要结束",
    )
    QUESTION_PATTERNS = (
        "挂了吗",
        "挂了没",
        "要挂吗",
        "能挂吗",
        "可以挂吗",
        "是不是要挂",
        "会自动挂",
        "自动挂吗",
        "结束了吗",
        "要结束吗",
    )
    EXPLICIT_END_PATTERNS = (
        "挂了吧",
        "挂断吧",
        "挂电话吧",
        "挂吧",
        "可以挂了",
        "结束吧",
        "结束通话吧",
        "不聊了",
        "不用聊了",
        "不用说了",
        "别说了",
        "先这样吧",
        "就这样吧",
        "到这吧",
    )
    POLITE_END_PATTERNS = (
        "再见",
        "拜拜",
    )

    def decide(self, transcript: str) -> CallEndDecision:
        normalized = self._normalize(transcript)
        if not normalized:
            return CallEndDecision(
                action="uncertain",
                confidence=0.0,
                reason="empty_transcript",
                summary="用户文本为空",
            )
        if any(pattern in normalized for pattern in self.NEGATIVE_PATTERNS):
            return CallEndDecision(
                action="not_end",
                confidence=0.95,
                reason="negated_call_end",
                summary="用户明确表示不要挂断",
            )
        if self._looks_like_question(transcript, normalized):
            return CallEndDecision(
                action="not_end",
                confidence=0.9,
                reason="call_end_question",
                summary="用户在询问挂断状态或可能性",
            )
        if any(pattern in normalized for pattern in self.EXPLICIT_END_PATTERNS):
            return CallEndDecision(
                action="explicit_end",
                confidence=0.95,
                reason="explicit_customer_end",
                summary="用户明确要求结束通话",
            )
        if any(pattern in normalized for pattern in self.POLITE_END_PATTERNS):
            return CallEndDecision(
                action="explicit_end",
                confidence=0.8,
                reason="polite_customer_end",
                summary="用户使用礼貌结束语，需要模型结合上下文确认",
            )
        return CallEndDecision(
            action="uncertain",
            confidence=0.4,
            reason="not_explicit_call_end",
            summary="未识别到明确结束通话意图",
        )

    @staticmethod
    def _normalize(text: str) -> str:
        return "".join(
            char.lower() for char in text.strip() if char not in " \t\r\n，。！？,.!?；;：:、"
        )

    @classmethod
    def _looks_like_question(cls, transcript: str, normalized: str) -> bool:
        stripped = transcript.strip()
        return stripped.endswith(("?", "？")) or any(
            pattern in normalized for pattern in cls.QUESTION_PATTERNS
        )
