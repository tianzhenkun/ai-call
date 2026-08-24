from __future__ import annotations

from typing import Any

AI_CLASSIFICATIONS = {"interested", "nurturing", "low_value"}


def has_reviewable_classification(
    *,
    analysis_status: str,
    analysis_result: dict[str, Any] | None,
    review_status: str | None,
) -> bool:
    result = analysis_result or {}
    return bool(
        analysis_status == "2"
        and review_status not in {"confirmed", "adjusted"}
        and result.get("valid_dialogue") is True
        and result.get("classification") in AI_CLASSIFICATIONS
        and str(result.get("reason") or "").strip()
    )


def requires_classification_review(
    *,
    analysis_status: str,
    analysis_result: dict[str, Any] | None,
    review_status: str | None,
    current_classification: str | None = None,
) -> bool:
    result = analysis_result or {}
    classification_conflict = bool(
        current_classification
        and result.get("classification") in AI_CLASSIFICATIONS
        and current_classification != result.get("classification")
    )
    if classification_conflict:
        return bool(
            analysis_status == "2"
            and review_status not in {"confirmed", "adjusted"}
            and str(result.get("reason") or "").strip()
        )
    return bool(
        has_reviewable_classification(
            analysis_status=analysis_status,
            analysis_result=result,
            review_status=review_status,
        )
        and (result.get("confidence") != "high" or result.get("evidence_conflict") is True)
    )
