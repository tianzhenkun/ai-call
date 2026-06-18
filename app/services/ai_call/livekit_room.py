from __future__ import annotations

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
        await self._post_room_service(
            method="DeleteRoom",
            payload={"room": room_name},
            error_id="room_delete_failed",
            msg="LiveKit Room 删除失败",
        )

    def issue_browser_token(self, room_name: str, participant_identity: str) -> BrowserRoomToken:
        return self.issue_participant_token(
            room_name,
            participant_identity,
            expires_in_seconds=self.browser_token_ttl_seconds,
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
        token = jwt.encode(payload, self.api_secret, algorithm="HS256")
        return BrowserRoomToken(
            livekit_url=self.livekit_url,
            participant_token=token,
            participant_identity=participant_identity,
            expires_in_seconds=expires_in_seconds,
        )

    async def _post_room_service(
        self,
        method: str,
        payload: dict,
        error_id: str,
        msg: str,
    ) -> None:
        service_url = f"{self._http_base_url()}/twirp/livekit.RoomService/{method}"
        token = self._issue_room_admin_token()
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

    def _issue_room_admin_token(self) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self.api_key,
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "video": {
                "roomCreate": True,
                "roomList": True,
                "roomAdmin": True,
            },
        }
        return jwt.encode(payload, self.api_secret, algorithm="HS256")

    def _http_base_url(self) -> str:
        parsed = urlparse(self.livekit_url)
        scheme = "https" if parsed.scheme in {"wss", "https"} else "http"
        return urlunparse((scheme, parsed.netloc, "", "", "", "")).rstrip("/")
