from __future__ import annotations

import httpx
import pytest
from google.protobuf.json_format import MessageToDict
from httpx import AsyncClient
from livekit.protocol import models

from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.livekit_room import LiveKitRoomManager


def _room_manager(monkeypatch, response: httpx.Response) -> LiveKitRoomManager:
    transport = httpx.MockTransport(lambda _request: response)
    monkeypatch.setattr(
        "app.services.ai_call.livekit_room.httpx.AsyncClient",
        lambda **kwargs: AsyncClient(transport=transport, **kwargs),
    )
    return LiveKitRoomManager("ws://livekit.test", "key", "test-secret-" * 3, 60)


@pytest.mark.anyio
async def test_livekit_protojson_microphone_is_ready_when_audio_type_is_omitted(
    monkeypatch,
) -> None:
    participant = models.ParticipantInfo(
        identity="human-agent-1",
        sid="PA_1",
        tracks=[
            models.TrackInfo(
                sid="TR_1",
                type=models.AUDIO,
                source=models.MICROPHONE,
                muted=False,
            )
        ],
    )
    payload = MessageToDict(participant)
    assert "type" not in payload["tracks"][0]
    manager = _room_manager(monkeypatch, httpx.Response(200, json=payload))

    fact = await manager.get_participant_media("room-1", "human-agent-1")

    assert fact is not None
    assert fact.microphone_ready is True
    assert fact.participant_sid == "PA_1"
    assert fact.track_sid == "TR_1"
    assert await manager.has_published_microphone("room-1", "human-agent-1") is True


@pytest.mark.anyio
@pytest.mark.parametrize("numeric_enums", [False, True])
@pytest.mark.parametrize("emit_defaults", [False, True])
async def test_livekit_microphone_accepts_protojson_enum_encodings(
    monkeypatch, numeric_enums: bool, emit_defaults: bool
) -> None:
    participant = models.ParticipantInfo(
        identity="human-agent-1",
        sid="PA_1",
        tracks=[models.TrackInfo(sid="TR_1", type=models.AUDIO, source=models.MICROPHONE)],
        attributes={"sip.callStatus": "active"},
    )
    payload = MessageToDict(
        participant,
        use_integers_for_enums=numeric_enums,
        always_print_fields_with_no_presence=emit_defaults,
    )
    manager = _room_manager(monkeypatch, httpx.Response(200, json=payload))

    fact = await manager.get_participant_media("room-1", "human-agent-1")

    assert fact is not None
    assert fact.microphone_ready is True
    assert fact.track_sid == "TR_1"
    assert fact.sip_call_status == "active"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "tracks",
    [
        None,
        [],
        [{}],
        [{"sid": "TR_1", "source": "MICROPHONE", "muted": True}],
        [{"sid": "TR_1", "type": "VIDEO", "source": "MICROPHONE"}],
        [{"sid": "TR_1", "type": 1, "source": 2}],
        [{"sid": "TR_1", "type": "DATA", "source": "MICROPHONE"}],
        [{"sid": "TR_1", "source": "SCREEN_SHARE_AUDIO"}],
        [{"sid": "TR_1", "source": 4}],
        [{"sid": "TR_1", "source": "UNKNOWN"}],
        [{"sid": "TR_1", "source": 0}],
        [{"sid": "TR_1"}],
    ],
)
async def test_livekit_participant_without_unmuted_microphone_is_not_ready(
    monkeypatch, tracks
) -> None:
    manager = _room_manager(
        monkeypatch,
        httpx.Response(200, json={"identity": "human-agent-1", "tracks": tracks}),
    )

    fact = await manager.get_participant_media("room-1", "human-agent-1")

    assert fact is not None
    assert fact.microphone_ready is False
    assert fact.track_sid is None
    assert await manager.has_published_microphone("room-1", "human-agent-1") is False
    assert await manager.participant_exists("room-1", "human-agent-1") is True


@pytest.mark.anyio
async def test_livekit_missing_participant_is_absent_and_not_media_ready(monkeypatch) -> None:
    manager = _room_manager(monkeypatch, httpx.Response(404, json={"code": "not_found"}))

    assert await manager.get_participant_media("room-1", "human-agent-1") is None
    assert await manager.has_published_microphone("room-1", "human-agent-1") is False
    assert await manager.participant_exists("room-1", "human-agent-1") is False


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [401, 403, 500, 503])
async def test_livekit_lookup_failure_is_not_mistaken_for_missing_media(
    monkeypatch, status_code: int
) -> None:
    manager = _room_manager(monkeypatch, httpx.Response(status_code, json={"code": "error"}))

    with pytest.raises(AiCallError) as exc:
        await manager.has_published_microphone("room-1", "human-agent-1")

    assert exc.value.error_id == "participant_lookup_failed"
    assert exc.value.status_code == 502
