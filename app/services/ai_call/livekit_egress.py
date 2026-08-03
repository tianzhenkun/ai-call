from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

import httpx
import jwt


class LiveKitEgressRequestTimeout(TimeoutError):
    """LiveKit Egress HTTP request timed out before the server result was known."""

    def __init__(self, *, method: str, timeout_seconds: float) -> None:
        self.method = method
        self.timeout_seconds = timeout_seconds
        super().__init__(f"{method} timed out after {timeout_seconds:g}s")


class LiveKitEgressNotFoundError(LookupError):
    pass


class LiveKitEgressAlreadyCompleteError(RuntimeError):
    """StopEgress was rejected because the Egress is already terminal."""

    def __init__(self, egress_id: str) -> None:
        self.egress_id = egress_id
        super().__init__(f"egress {egress_id} is already complete")


@dataclass(frozen=True, slots=True)
class LiveKitEgressStartResult:
    egress_id: str
    object_name: str
    status: str
    started_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class LiveKitEgressStopResult:
    egress_id: str
    status: str
    object_name: str | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    file_size: int | None = None
    location: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class LiveKitEgressObservation:
    egress_id: str
    status: str
    object_name: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    file_size: int | None = None


class LiveKitEgressManager:
    """LiveKit Egress Twirp 控制器。"""

    def __init__(
        self,
        *,
        livekit_url: str,
        api_key: str,
        api_secret: str,
        timeout_seconds: float,
        object_prefix: str,
        file_type: str = "MP4",
        participant_file_type: str | None = None,
        stop_timeout_seconds: float | None = None,
    ) -> None:
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout_seconds = max(0.5, timeout_seconds)
        self.stop_timeout_seconds = max(
            self.timeout_seconds,
            stop_timeout_seconds if stop_timeout_seconds is not None else timeout_seconds,
        )
        self.object_prefix = object_prefix.strip("/")
        self.file_type = self._normalize_file_type(file_type)
        self.participant_file_type = self._normalize_participant_file_type(
            participant_file_type or file_type
        )

    async def start_room_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        object_name = self.build_object_name(call_id)
        payload = {
            "room_name": room_name,
            "audio_only": True,
            "file_outputs": [
                {
                    "file_type": self.file_type,
                    "filepath": object_name,
                    "disable_manifest": True,
                    "s3": self._s3_upload_payload(oss_config),
                }
            ],
        }
        data = await self._post_egress("StartRoomCompositeEgress", payload)
        return LiveKitEgressStartResult(
            egress_id=str(data.get("egress_id") or ""),
            object_name=object_name,
            status=str(data.get("status") or ""),
            started_at=self._nanos_to_datetime(data.get("started_at")),
        )

    async def start_participant_audio_recording(
        self,
        *,
        room_name: str,
        call_id: str,
        track_role: str,
        participant_identity: str,
        oss_config: dict,
    ) -> LiveKitEgressStartResult:
        object_name = self.build_participant_object_name(
            call_id=call_id,
            track_role=track_role,
            participant_identity=participant_identity,
        )
        track_id = await self._resolve_participant_audio_track_id(
            room_name=room_name,
            participant_identity=participant_identity,
        )
        payload = {
            "room_name": room_name,
            "track_id": track_id,
            "file": {
                "filepath": object_name,
                "disable_manifest": True,
                "s3": self._s3_upload_payload(oss_config),
            },
        }
        data = await self._post_egress("StartTrackEgress", payload)
        return LiveKitEgressStartResult(
            egress_id=str(data.get("egress_id") or ""),
            object_name=object_name,
            status=str(data.get("status") or ""),
            started_at=self._nanos_to_datetime(data.get("started_at")),
        )

    async def stop_egress(self, egress_id: str) -> LiveKitEgressStopResult:
        data = await self._post_egress(
            "StopEgress",
            {"egress_id": egress_id},
            timeout_seconds=self.stop_timeout_seconds,
        )
        file_result = self._first_file_result(data)
        return LiveKitEgressStopResult(
            egress_id=str(data.get("egress_id") or egress_id),
            status=str(data.get("status") or ""),
            object_name=self._file_object_name(file_result),
            ended_at=self._nanos_to_datetime(data.get("ended_at")),
            duration_ms=self._nanos_to_millis(file_result.get("duration")) if file_result else None,
            file_size=self._safe_int(file_result.get("size")) if file_result else None,
            location=str(file_result.get("location") or "") if file_result else None,
            error=str(data.get("error") or "") or None,
        )

    async def get_egress(self, egress_id: str) -> LiveKitEgressObservation | None:
        try:
            data = await self._post_egress(
                "ListEgress",
                {"egress_id": egress_id},
            )
        except LiveKitEgressNotFoundError:
            return None
        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError("ListEgress response is missing items")
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("egress_id") or "") == egress_id:
                return self._egress_observation(item)
        return None

    async def find_room_audio_recording(
        self,
        room_name: str,
        object_name: str,
    ) -> LiveKitEgressObservation | None:
        try:
            data = await self._post_egress(
                "ListEgress",
                {"room_name": room_name},
            )
        except LiveKitEgressNotFoundError:
            return None
        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError("ListEgress response is missing items")
        for item in items:
            if not isinstance(item, dict):
                continue
            item_room = str(item.get("room_name") or "")
            if item_room and item_room != room_name:
                continue
            observation = self._egress_observation(item)
            if observation.object_name == object_name:
                return observation
        return None

    async def get_egress_status(self, egress_id: str) -> str | None:
        observation = await self.get_egress(egress_id)
        return observation.status if observation is not None else None

    def build_object_name(self, call_id: str) -> str:
        suffix = self.file_type.lower()
        filename = f"{call_id}.{suffix}"
        return f"{self.object_prefix}/{filename}" if self.object_prefix else filename

    def build_participant_object_name(
        self,
        *,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> str:
        suffix = self.participant_file_type.lower()
        safe_role = self._safe_object_part(track_role)
        safe_identity = self._safe_object_part(participant_identity)
        filename = f"{safe_role}-{safe_identity}.{suffix}"
        if self.object_prefix:
            return f"{self.object_prefix}/tracks/{call_id}/{filename}"
        return f"tracks/{call_id}/{filename}"

    async def _post_egress(
        self,
        method: str,
        payload: dict,
        *,
        timeout_seconds: float | None = None,
    ) -> dict:
        service_url = f"{self._http_base_url()}/twirp/livekit.Egress/{method}"
        token = self._issue_egress_token()
        request_timeout_seconds = (
            self.timeout_seconds if timeout_seconds is None else max(0.5, timeout_seconds)
        )
        try:
            async with httpx.AsyncClient(timeout=request_timeout_seconds) as client:
                response = await client.post(
                    service_url,
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )
        except httpx.TimeoutException as exc:
            raise LiveKitEgressRequestTimeout(
                method=method,
                timeout_seconds=request_timeout_seconds,
            ) from exc
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if response.status_code == 404:
                raise LiveKitEgressNotFoundError(
                    str(payload.get("egress_id") or "")
                ) from exc
            if (
                method == "StopEgress"
                and response.status_code == 412
                and self._is_already_complete_response(response)
            ):
                raise LiveKitEgressAlreadyCompleteError(
                    str(payload.get("egress_id") or "")
                ) from exc
            raise RuntimeError(
                self._http_error_message(method, response.status_code, response.text)
            ) from exc
        data = response.json()
        return data if isinstance(data, dict) else {}

    async def _post_room_service(self, method: str, payload: dict) -> dict:
        service_url = f"{self._http_base_url()}/twirp/livekit.RoomService/{method}"
        room_name = payload.get("room")
        token = self._issue_room_admin_token(
            room_name=str(room_name).strip() if room_name else None,
        )
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                service_url,
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                self._http_error_message(method, response.status_code, response.text)
            ) from exc
        data = response.json()
        return data if isinstance(data, dict) else {}

    async def _resolve_participant_audio_track_id(
        self,
        *,
        room_name: str,
        participant_identity: str,
    ) -> str:
        participant = await self._post_room_service(
            "GetParticipant",
            {"room": room_name, "identity": participant_identity},
        )
        track = self._select_audio_track(participant.get("tracks"))
        track_id = str(track.get("sid") or "").strip()
        if not track_id:
            raise RuntimeError(f"参与方未发布可录制的音频轨: {participant_identity}")
        return track_id

    def _issue_egress_token(self) -> str:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self.api_key,
            "nbf": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "video": {
                "roomRecord": True,
            },
        }
        return jwt.encode(payload, self.api_secret, algorithm="HS256")

    def _issue_room_admin_token(self, *, room_name: str | None = None) -> str:
        now = datetime.now(timezone.utc)
        video_grant = {
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

    @staticmethod
    def _is_already_complete_response(response: httpx.Response) -> bool:
        try:
            payload = response.json()
        except ValueError:
            return False
        return payload == {
            "code": "failed_precondition",
            "msg": "egress with status EGRESS_COMPLETE cannot be stopped",
        }

    @staticmethod
    def _s3_upload_payload(oss_config: dict) -> dict:
        endpoint = str(oss_config.get("endpoint") or "")
        if endpoint and not endpoint.startswith(("http://", "https://")):
            scheme = "https" if str(oss_config.get("is_https") or "").upper() == "Y" else "http"
            endpoint = f"{scheme}://{endpoint}"
        return {
            "access_key": oss_config.get("access_key") or "",
            "secret": oss_config.get("secret_key") or "",
            "region": oss_config.get("region") or "",
            "endpoint": endpoint,
            "bucket": oss_config.get("bucket_name") or "",
            "force_path_style": True,
        }

    @staticmethod
    def _safe_object_part(value: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
        return normalized.strip("._-") or "unknown"

    @staticmethod
    def _http_error_message(method: str, status_code: int, response_text: str) -> str:
        body = str(response_text or "").strip().replace("\n", " ")
        if len(body) > 300:
            body = f"{body[:300]}..."
        return f"{method} HTTP {status_code}: {body}" if body else f"{method} HTTP {status_code}"

    @staticmethod
    def _normalize_file_type(value: str) -> str:
        normalized = str(value or "MP4").strip().upper()
        return normalized if normalized in {"MP4", "OGG", "MP3"} else "MP4"

    @classmethod
    def _normalize_participant_file_type(cls, value: str) -> str:
        normalized = cls._normalize_file_type(value)
        return "OGG" if normalized == "MP3" else normalized

    @classmethod
    def _select_audio_track(cls, tracks) -> dict:
        if not isinstance(tracks, list):
            return {}
        audio_tracks = [track for track in tracks if cls._is_audio_track(track)]
        if not audio_tracks:
            return {}
        for track in audio_tracks:
            if cls._track_source(track) == "MICROPHONE":
                return track
        return audio_tracks[0]

    @staticmethod
    def _is_audio_track(track) -> bool:
        if not isinstance(track, dict):
            return False
        value = track.get("type")
        if isinstance(value, str):
            return value.upper().endswith("AUDIO")
        return value == 0

    @staticmethod
    def _track_source(track: dict) -> str:
        value = track.get("source")
        if isinstance(value, str):
            return value.upper().split(".")[-1]
        if value == 2:
            return "MICROPHONE"
        return ""

    @classmethod
    def _first_file_result(cls, data: dict) -> dict:
        file_results = data.get("file_results")
        if isinstance(file_results, list) and file_results:
            first = file_results[0]
            return first if isinstance(first, dict) else {}
        return {}

    @classmethod
    def _egress_observation(cls, data: dict) -> LiveKitEgressObservation:
        file_result = cls._first_file_result(data)
        return LiveKitEgressObservation(
            egress_id=str(data.get("egress_id") or ""),
            status=str(data.get("status") or ""),
            object_name=cls._file_object_name(file_result),
            started_at=cls._nanos_to_datetime(data.get("started_at")),
            ended_at=cls._nanos_to_datetime(data.get("ended_at")),
            duration_ms=(
                cls._nanos_to_millis(file_result.get("duration"))
                if file_result
                else None
            ),
            file_size=cls._safe_int(file_result.get("size")) if file_result else None,
        )

    @staticmethod
    def _file_object_name(file_result: dict) -> str | None:
        filename = file_result.get("filename") if file_result else None
        return str(filename) if filename else None

    @classmethod
    def _nanos_to_datetime(cls, value) -> datetime | None:
        nanos = cls._safe_int(value)
        if not nanos:
            return None
        return datetime.fromtimestamp(nanos / 1_000_000_000, timezone.utc)

    @classmethod
    def _nanos_to_millis(cls, value) -> int | None:
        nanos = cls._safe_int(value)
        if nanos is None:
            return None
        return max(0, int(nanos / 1_000_000))

    @staticmethod
    def _safe_int(value) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
