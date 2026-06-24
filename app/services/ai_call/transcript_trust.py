from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from app.services.ai_call.dialogue_merge import normalize_dialogue_text

TranscriptTrust = Literal["trusted", "low_confidence"]
SemanticAction = Literal["accept", "reject"]
CommitDecision = Literal["commit", "candidate", "reject"]

TRUSTED_TRANSCRIPT = "trusted"
LOW_CONFIDENCE_TRANSCRIPT = "low_confidence"
SEMANTIC_ACCEPT = "accept"
SEMANTIC_REJECT = "reject"
COMMIT_TRANSCRIPT = "commit"
CANDIDATE_TRANSCRIPT = "candidate"
REJECT_TRANSCRIPT = "reject"

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
    "不",
    "不行",
    "不要",
    "不用",
    "喂",
    "你好",
})


@dataclass(frozen=True, slots=True)
class RealtimeTranscriptTrustDecision:
    trust: TranscriptTrust
    semantic_action: SemanticAction
    commit_decision: CommitDecision
    reason: str
    confidence: float

    @property
    def accepted(self) -> bool:
        return self.semantic_action == SEMANTIC_ACCEPT and self.commit_decision == COMMIT_TRANSCRIPT

    def as_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "transcriptTrust": self.trust,
            "semanticAction": self.semantic_action,
            "commitDecision": self.commit_decision,
            "semanticConfidence": self.confidence,
        }
        if not self.accepted:
            payload["semanticRejectReason"] = self.reason
        return payload


def decide_realtime_transcript_trust(
    transcript: str,
    *,
    during_ai_audio: bool,
    has_interrupt_candidate: bool,
    has_reliable_user_audio: bool = False,
    payload: dict[str, Any] | None = None,
) -> RealtimeTranscriptTrustDecision:
    payload = payload or {}
    if is_realtime_transcript_semantically_rejected(payload):
        return RealtimeTranscriptTrustDecision(
            trust=LOW_CONFIDENCE_TRANSCRIPT,
            semantic_action=SEMANTIC_REJECT,
            commit_decision=_payload_commit_decision(payload) or REJECT_TRANSCRIPT,
            reason=_payload_reason(payload) or "low_confidence_transcript",
            confidence=0.2,
        )

    normalized = normalize_dialogue_text(transcript)
    if not normalized:
        return RealtimeTranscriptTrustDecision(
            trust=LOW_CONFIDENCE_TRANSCRIPT,
            semantic_action=SEMANTIC_REJECT,
            commit_decision=REJECT_TRANSCRIPT,
            reason="empty_transcript",
            confidence=0.0,
        )

    if (
        during_ai_audio
        and has_interrupt_candidate
        and len(normalized) <= 2
        and not has_reliable_user_audio
    ):
        return RealtimeTranscriptTrustDecision(
            trust=LOW_CONFIDENCE_TRANSCRIPT,
            semantic_action=SEMANTIC_REJECT,
            commit_decision=CANDIDATE_TRANSCRIPT,
            reason="ai_audio_short_overlap_candidate_transcript",
            confidence=0.35,
        )

    reason = (
        "trusted_short_overlap_reliable_audio"
        if during_ai_audio and has_interrupt_candidate and len(normalized) <= 2
        else "trusted_realtime_transcript"
    )
    return RealtimeTranscriptTrustDecision(
        trust=TRUSTED_TRANSCRIPT,
        semantic_action=SEMANTIC_ACCEPT,
        commit_decision=COMMIT_TRANSCRIPT,
        reason=reason,
        confidence=0.9,
    )


def is_realtime_transcript_semantically_rejected(payload: dict[str, Any]) -> bool:
    semantic_action = payload.get("semanticAction")
    if isinstance(semantic_action, str) and semantic_action.lower() == SEMANTIC_REJECT:
        return True
    transcript_trust = payload.get("transcriptTrust")
    return isinstance(transcript_trust, str) and transcript_trust.lower() in {
        LOW_CONFIDENCE_TRANSCRIPT,
        "noise",
    }


def _payload_reason(payload: dict[str, Any]) -> str | None:
    value = payload.get("semanticRejectReason") or payload.get("semanticReason")
    return value if isinstance(value, str) and value else None


def _payload_commit_decision(payload: dict[str, Any]) -> CommitDecision | None:
    value = payload.get("commitDecision")
    if isinstance(value, str) and value in {
        COMMIT_TRANSCRIPT,
        CANDIDATE_TRANSCRIPT,
        REJECT_TRANSCRIPT,
    }:
        return value
    return None
