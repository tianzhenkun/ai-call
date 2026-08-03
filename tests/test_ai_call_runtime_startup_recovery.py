from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.services.ai_call.runtime_control.startup_recovery import (
    StartupReconcileDecision,
    decide_startup_reconcile,
    startup_reconcile_due,
)
from app.services.ai_call.runtime_control.types import EffectStatus


def test_startup_reconcile_decision_is_no_resource_when_all_creates_are_no_resource() -> None:
    assert (
        decide_startup_reconcile(
            (
                (EffectStatus.FAILED, "no_resource"),
                (EffectStatus.FAILED, "no_resource"),
            )
        )
        == StartupReconcileDecision.NO_RESOURCE
    )


def test_startup_reconcile_decision_is_resource_present_if_any_create_applied() -> None:
    assert (
        decide_startup_reconcile(
            (
                (EffectStatus.RECONCILE_REQUIRED, "provider timeout"),
                (EffectStatus.APPLIED, None),
            )
        )
        == StartupReconcileDecision.RESOURCE_PRESENT
    )


def test_startup_reconcile_ignores_auxiliary_egress_fact() -> None:
    assert (
        decide_startup_reconcile(
            (
                ("CREATE_ROOM", EffectStatus.FAILED, "no_resource"),
                ("ATTACH_AGENT_PARTICIPANT", EffectStatus.FAILED, "no_resource"),
                ("START_EGRESS", EffectStatus.APPLIED, None),
            )
        )
        == StartupReconcileDecision.NO_RESOURCE
    )


@pytest.mark.parametrize(
    "effects",
    [
        (),
        ((EffectStatus.RECONCILE_REQUIRED, "provider timeout"),),
        ((EffectStatus.FAILED, "provider rejected"),),
        ((EffectStatus.PENDING, None),),
    ],
)
def test_startup_reconcile_decision_is_unknown_when_resource_existence_is_not_proven(
    effects: tuple[tuple[EffectStatus, str | None], ...],
) -> None:
    assert decide_startup_reconcile(effects) == StartupReconcileDecision.UNKNOWN


def test_startup_reconcile_due_uses_database_deadline_without_clock_skew() -> None:
    deadline = datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc)
    assert startup_reconcile_due(deadline, deadline + timedelta(microseconds=1))
    assert not startup_reconcile_due(deadline, deadline - timedelta(microseconds=1))
    assert not startup_reconcile_due(None, deadline)
