from __future__ import annotations

from typing import Any

AI_CLASSIFICATIONS = {"interested", "nurturing", "low_value"}


def requires_classification_review(
    *,
    analysis_status: str,
    analysis_result: dict[str, Any] | None,
    review_status: str | None,
) -> bool:
    result = analysis_result or {}
    return bool(
        analysis_status == "2"
        and review_status is None
        and result.get("valid_dialogue") is True
        and result.get("classification") in AI_CLASSIFICATIONS
        and str(result.get("reason") or "").strip()
        and (
            result.get("confidence") != "high"
            or result.get("evidence_conflict") is True
        )
    )
