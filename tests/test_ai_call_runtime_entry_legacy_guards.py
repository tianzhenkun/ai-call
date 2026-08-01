from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.api.v1.ai_call.service import AiCallService
from app.api.v1.ai_call.voice.controller import VoicePreviewRequest
from app.core.exceptions import CustomException


@pytest.mark.anyio
async def test_legacy_web_service_is_blocked_when_web_owner_entry_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.api.v1.ai_call.service as service_module

    monkeypatch.setattr(
        service_module,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="web"),
    )
    orchestrator = Mock()

    with pytest.raises(CustomException) as exc_info:
        await AiCallService(orchestrator).create_web_session(
            voice="v1",
            prompt=None,
            scene_code="collection",
        )

    assert exc_info.value.status_code == 409
    orchestrator.create_web_session.assert_not_called()


@pytest.mark.anyio
async def test_legacy_direct_sip_service_is_blocked_when_direct_sip_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.api.v1.ai_call.service as service_module

    monkeypatch.setattr(
        service_module,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="direct_sip"),
    )
    orchestrator = Mock()

    with pytest.raises(CustomException) as exc_info:
        await AiCallService(orchestrator).create_sip_session(
            callee_phone_number="13800000000",
            voice="v1",
            scene_code="collection",
        )

    assert exc_info.value.status_code == 409
    orchestrator.create_sip_session.assert_not_called()


@pytest.mark.anyio
async def test_legacy_preview_controller_is_blocked_when_preview_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.api.v1.ai_call.voice.controller as controller

    monkeypatch.setattr(
        controller,
        "settings",
        SimpleNamespace(AI_CALL_OWNER_COMMAND_V1_ENTRIES="preview"),
    )
    service = Mock()
    auth = SimpleNamespace(
        user=SimpleNamespace(tenant_id="tenant-a", user_id=7),
    )

    with pytest.raises(CustomException) as exc_info:
        await controller.create_voice_preview_controller(
            request=VoicePreviewRequest(voice="v1"),
            auth=auth,
            service=service,
        )

    assert exc_info.value.status_code == 409
    service.create_preview_session.assert_not_called()
