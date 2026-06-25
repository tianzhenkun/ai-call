from __future__ import annotations

from pathlib import Path

import pytest

from app.config.setting import Settings
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_sip import (
    CreateSipParticipantPayload,
    LiveKitSipClient,
    SipOutboundConfig,
    validate_sip_outbound_preflight,
)


def test_livekit_api_dependency_is_declared() -> None:
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert '"livekit-api>=1.0,<2.0"' in pyproject


def test_self_hosted_livekit_sip_templates_are_declared() -> None:
    compose = Path("deploy/livekit-egress/docker-compose.yml").read_text(encoding="utf-8")
    sip_config = Path("deploy/livekit-egress/sip.yaml.example").read_text(encoding="utf-8")

    assert "livekit-sip:" in compose
    assert "livekit/sip:latest" in compose
    assert "./sip.yaml:/etc/sip.yaml:ro" in compose
    assert "${SIP_SIGNALING_PORT:-5060}:${SIP_SIGNALING_PORT:-5060}/udp" in compose
    assert "${SIP_RTP_RANGE:-10000-20000}:${SIP_RTP_RANGE:-10000-20000}/udp" in compose
    assert "redis:" in sip_config
    assert "sip_port: 5060" in sip_config
    assert "rtp_port: 10000-20000" in sip_config
    assert "use_external_ip: true" in sip_config


def test_settings_expose_sip_outbound_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_SIP_OUTBOUND_ENABLED is False
    assert settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES == ""
    assert settings.AI_CALL_SIP_DEFAULT_RINGING_TIMEOUT_SECONDS == 45
    assert settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS == 120
    assert settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS == 600
    assert settings.LIVEKIT_SIP_OUTBOUND_TRUNK_ID == ""
    assert settings.LIVEKIT_SIP_OUTBOUND_TRUNK_HOSTNAME == ""
    assert settings.LIVEKIT_SIP_OUTBOUND_DESTINATION_COUNTRY == "CN"
    assert settings.LIVEKIT_SIP_AUTH_USERNAME == ""
    assert settings.LIVEKIT_SIP_AUTH_PASSWORD == ""
    assert settings.SIP_PUBLIC_IP == ""
    assert settings.SIP_USE_EXTERNAL_IP is True


def test_sip_preflight_accepts_ip_allowlist_trunk_without_password() -> None:
    config = SipOutboundConfig(
        enabled=True,
        allowed_callee_prefixes="13,15,+86",
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        auth_username="037100000000",
        auth_password="",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
        use_external_ip=True,
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="13800000000",
    )

    assert result.ok is True
    assert result.failure_reason is None


def test_sip_preflight_rejects_disabled_outbound_before_real_call() -> None:
    config = SipOutboundConfig(
        enabled=False,
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="13800000000",
    )

    assert result.ok is False
    assert result.failure_reason == "sip_outbound_disabled"
    assert result.stage == "sip_config"


def test_sip_preflight_rejects_callee_outside_allowed_prefixes() -> None:
    config = SipOutboundConfig(
        enabled=True,
        allowed_callee_prefixes="13,15",
        trunk_hostname="sip-provider.example.com:5060",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )

    result = validate_sip_outbound_preflight(
        config,
        callee_phone_number="18800000000",
    )

    assert result.ok is False
    assert result.failure_reason == "callee_prefix_not_allowed"
    assert result.stage == "callee_number"


@pytest.mark.anyio
async def test_livekit_sip_client_builds_create_participant_payload_for_fake_sdk() -> None:
    captured_payloads: list[CreateSipParticipantPayload] = []

    async def fake_create_participant(payload: CreateSipParticipantPayload) -> dict:
        captured_payloads.append(payload)
        return {
            "identity": payload.participant_identity,
            "attributes": {
                "sip.callID": "short-call-id",
                "sip.callIDFull": "full-call-id",
                "sip.trunkID": "trunk_123",
                "sip.callStatus": "active",
            },
        }

    config = SipOutboundConfig(
        enabled=True,
        trunk_id="trunk_123",
        caller_number="037100000000",
        signaling_port=5060,
        rtp_range="10000-20000",
        public_ip="203.0.113.10",
    )
    client = LiveKitSipClient(
        config=config,
        create_participant=fake_create_participant,
    )

    result = await client.create_participant(
        room_name="ai-call-call_1",
        participant_identity="sip-call_1",
        callee_phone_number="13800000000",
        ringing_timeout_seconds=30,
    )

    assert captured_payloads == [
        CreateSipParticipantPayload(
            room_name="ai-call-call_1",
            participant_identity="sip-call_1",
            sip_call_to="13800000000",
            sip_number="037100000000",
            sip_trunk_id="trunk_123",
            trunk_hostname="",
            auth_username="",
            auth_password="",
            destination_country="CN",
            wait_until_answered=True,
            ringing_timeout_seconds=30,
        )
    ]
    assert result.participant_identity == "sip-call_1"
    assert result.sip_call_id == "short-call-id"
    assert result.sip_call_id_full == "full-call-id"
    assert result.sip_trunk_id == "trunk_123"
    assert result.sip_call_status == "active"


@pytest.mark.anyio
async def test_livekit_sip_client_raises_aicall_error_when_preflight_fails() -> None:
    client = LiveKitSipClient(
        config=SipOutboundConfig(enabled=False),
        create_participant=lambda payload: None,
    )

    with pytest.raises(AiCallError) as exc_info:
        await client.create_participant(
            room_name="ai-call-call_1",
            participant_identity="sip-call_1",
            callee_phone_number="13800000000",
        )

    assert exc_info.value.error_id == "sip_outbound_disabled"
