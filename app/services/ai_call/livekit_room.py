from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

import httpx
import jwt
from fastapi import status

from app.services.ai_call.exceptions import AiCallError


@dataclass(frozen=True, slots=True)
class BrowserRoomToken:
    livekit_url: str
    participant_token: str
    participant_identity: str
    expires_in_seconds: int


@dataclass(frozen=True, slots=True)
class LiveKitParticipantMediaFact:
    participant_identity: str
    participant_sid: str | None
    track_sid: str | None
    microphone_ready: bool


class LiveKitRoomManager:
    def __init__(
        self,
        livekit_url: str,
        api_key: str,
        api_secret: str,
        browser_token_ttl_seconds: int,
    ) -> None:
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.browser_token_ttl_seconds = browser_token_ttl_seconds

    async def create_room(self, room_name: str) -> None:
        await self._post_room_service(
            method="CreateRoom",
            payload={"name": room_name, "emptyTimeout": 60, "maxParticipants": 8},
            error_id="room_create_failed",
            msg="LiveKit Room 创建失败",
        )

    async def delete_room(self, room_name: str) -> None:
        try:
            await self._post_room_service(
                method="DeleteRoom",
                payload={"room": room_name},
                error_id="room_delete_failed",
                msg="LiveKit Room 删除失败",
            )
        except AiCallError as exc:
            cause = exc.__cause__
            if (
                isinstance(cause, httpx.HTTPStatusError)
                and cause.response.status_code == status.HTTP_404_NOT_FOUND
            ):
                return
            raise

    def issue_browser_token(
        self,
        room_name: str,
        participant_identity: str,
        *,
        metadata: Mapping[str, object] | None = None,
    ) -> BrowserRoomToken:
        return self.issue_participant_token(
            room_name,
            participant_identity,
            expires_in_seconds=self.browser_token_ttl_seconds,
            metadata=metadata,
        )

    def issue_handoff_token(
        self,
        room_name: str,
        participant_identity: str,
        expires_in_seconds: int | None = None,
    ) -> BrowserRoomToken:
        return self.issue_participant_token(
            room_name,
            participant_identity,
            expires_in_seconds=expires_in_seconds or self.browser_token_ttl_seconds,
        )

    def issue_participant_token(
        self,
        room_name: str,
        participant_identity: str,
        *,
        expires_in_seconds: int,
        metadata: Mapping[str, object] | None = None,
    ) -> BrowserRoomToken:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=expires_in_seconds)
        payload = {
            "iss": self.api_key,
            "sub": participant_identity,
            "nbf": int(now.timestamp()),
            "exp": int(expires_at.timestamp()),
            "video": {
                "roomJoin": True,
                "room": room_name,
                "canPublish": True,
                "canSubscribe": True,
                "canPublishData": True,
            },
        }
        if metadata:
            payload["metadata"] = json.dumps(
                metadata,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        token = jwt.encode(payload, self.api_secret, algorithm="HS256")
        return BrowserRoomToken(
            livekit_url=self.livekit_url,
            participant_token=token,
            participant_identity=participant_identity,
            expires_in_seconds=expires_in_seconds,
        )

    async def has_published_microphone(
        self,
        room_name: str,
        participant_identity: str,
    ) -> bool:
        fact = await self.get_participant_media(room_name, participant_identity)
        return bool(fact and fact.microphone_ready)

    async def get_participant_media(
        self,
        room_name: str,
        participant_identity: str,
    ) -> LiveKitParticipantMediaFact | None:
        try:
            participant = await self._post_room_service(
                method="GetParticipant",
                payload={"room": room_name, "identity": participant_identity},
                error_id="participant_lookup_failed",
                msg="LiveKit 参与方核验失败",
            )
        except AiCallError as exc:
            if self._is_not_found(exc):
                return None
            raise
        tracks = participant.get("tracks")
        microphone_track = next(
            (
                track
                for track in tracks
                if isinstance(track, dict)
                and track.get("type") in {0, "AUDIO"}
                and track.get("source") in {2, "MICROPHONE"}
                and not bool(track.get("muted"))
            ),
            None,
        ) if isinstance(tracks, list) else None
        return LiveKitParticipantMediaFact(
            participant_identity=str(
                participant.get("identity") or participant_identity
            ),
            participant_sid=str(participant.get("sid") or "") or None,
            track_sid=(
                str(microphone_track.get("sid") or "") or None
                if microphone_track is not None
                else None
            ),
            microphone_ready=microphone_track is not None,
        )

    async def participant_exists(self, room_name: str, participant_identity: str) -> bool:
        return (
            await self.get_participant_media(room_name, participant_identity)
        ) is not None

    async def remove_participant(
        self,
        room_name: str,
        participant_identity: str,
    ) -> None:
        try:
            await self._post_room_service(
                method="RemoveParticipant",
                payload={"room": room_name, "identity": participant_identity},
                error_id="participant_remove_failed",
                msg="LiveKit 参与方移除失败",
            )
        except AiCallError as exc:
            if self._is_not_found(exc):
                return
            raise

    async def room_exists(self, room_name: str) -> bool:
        result = await self._post_room_service(
            method="ListRooms",
            payload={"names": [room_name]},
            error_id="room_lookup_failed",
            msg="LiveKit Room 核验失败",
        )
        rooms = result.get("rooms")
        if not isinstance(rooms, list):
            return False
        return any(isinstance(room, dict) and room.get("name") == room_name for room in rooms)

    @staticmethod
    def _is_not_found(exc: AiCallError) -> bool:
        cause = exc.__cause__
        return bool(
            isinstance(cause, httpx.HTTPStatusError)
            and cause.response.status_code == status.HTTP_404_NOT_FOUND
        )

    async def _post_room_service(
        self,
        method: str,
        payload: dict,
        error_id: str,
        msg: str,
    ) -> dict:
        service_url = f"{self._http_base_url()}/twirp/livekit.RoomService/{method}"
        room_name = payload.get("room")
        token = self._issue_room_admin_token(
            room_name=str(room_name).strip() if room_name else None,
        )
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    service_url,
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )
            response.raise_for_status()
        except Exception as exc:
            raise AiCallError(
                error_id=error_id,
                msg=msg,
                status_code=status.HTTP_502_BAD_GATEWAY,
            ) from exc
        data = response.json()
        return data if isinstance(data, dict) else {}

    def _issue_room_admin_token(self, *, room_name: str | None = None) -> str:
        now = datetime.now(timezone.utc)
        video_grant = {
            "roomCreate": True,
            "roomList": True,
            "roomAdmin": True,
        }
        if room_name:
            video_grant["room"] = room_name
        payload = {
            "iss": self.api_key,
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "video": video_grant,
        }
        return jwt.encode(payload, self.api_secret, algorithm="HS256")

    def _http_base_url(self) -> str:
        parsed = urlparse(self.livekit_url)
        scheme = "https" if parsed.scheme in {"wss", "https"} else "http"
        return urlunparse((scheme, parsed.netloc, "", "", "", "")).rstrip("/")
