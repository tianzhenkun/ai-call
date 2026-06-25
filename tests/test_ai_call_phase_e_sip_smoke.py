from __future__ import annotations

from io import StringIO

import pytest

from tools.ai_call_sip_smoke import main, mask_phone_number, run


def test_sip_smoke_masks_phone_number() -> None:
    assert mask_phone_number("13800000000") == "138****0000"
    assert mask_phone_number("+8613800000000") == "861****0000"
    assert mask_phone_number("1234567") == "***"


def test_sip_smoke_requires_explicit_confirmation_for_real_call(capsys) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([
            "--base-url",
            "http://127.0.0.1:19011",
            "--callee-phone-number",
            "13800000000",
            "--scene-code",
            "intro_geo",
        ])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "--confirm-real-call" in captured.err
    assert "13800000000" not in captured.err
    assert "138****0000" in captured.err


def test_sip_smoke_dry_run_does_not_post_or_print_full_phone() -> None:
    calls = []

    def fake_post_json(url, payload, timeout_seconds):
        calls.append((url, payload, timeout_seconds))
        return {"code": 200}

    stdout = StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--callee-phone-number",
            "13800000000",
            "--scene-code",
            "intro_geo",
            "--dry-run",
        ],
        post_json=fake_post_json,
        stdout=stdout,
    )

    assert exit_code == 0
    assert calls == []
    output = stdout.getvalue()
    assert "13800000000" not in output
    assert "138****0000" in output


def test_sip_smoke_confirmed_call_posts_to_sip_sessions_and_masks_output() -> None:
    calls = []

    def fake_post_json(url, payload, timeout_seconds):
        calls.append((url, payload, timeout_seconds))
        return {
            "code": 200,
            "msg": "创建成功",
            "data": {
                "callId": "call_1",
                "roomName": "ai-call-call_1",
                "participantIdentity": "sip-call_1",
                "status": "ready",
                "sipCallId": "short-call-id",
                "sipTrunkId": "trunk_123",
                "sipCallStatus": "active",
            },
        }

    stdout = StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--callee-phone-number",
            "13800000000",
            "--scene-code",
            "intro_geo",
            "--business-id",
            "geo_task_001",
            "--ringing-timeout-seconds",
            "30",
            "--confirm-real-call",
        ],
        post_json=fake_post_json,
        stdout=stdout,
    )

    assert exit_code == 0
    assert calls == [
        (
            "http://127.0.0.1:19011/ai-call/sip-sessions",
            {
                "calleePhoneNumber": "13800000000",
                "sceneCode": "intro_geo",
                "businessParams": {},
                "businessId": "geo_task_001",
                "ringingTimeoutSeconds": 30,
            },
            30.0,
        )
    ]
    output = stdout.getvalue()
    assert "13800000000" not in output
    assert "138****0000" in output
    assert "call_1" in output


def test_sip_smoke_masks_full_phone_in_error_response() -> None:
    def fake_post_json(url, payload, timeout_seconds):
        _ = url, payload, timeout_seconds
        return {
            "code": 500,
            "msg": "callee 13800000000 is not allowed",
            "data": None,
        }

    stdout = StringIO()
    stderr = StringIO()
    exit_code = run(
        [
            "--base-url",
            "http://127.0.0.1:19011",
            "--callee-phone-number",
            "13800000000",
            "--scene-code",
            "intro_geo",
            "--confirm-real-call",
        ],
        post_json=fake_post_json,
        stdout=stdout,
        stderr=stderr,
    )

    assert exit_code == 1
    assert "13800000000" not in stderr.getvalue()
    assert "138****0000" in stderr.getvalue()
