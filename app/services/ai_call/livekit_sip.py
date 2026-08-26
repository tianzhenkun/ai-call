from __future__ import annotations

import inspect
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import status

from app.config.setting import Settings
from app.services.ai_call.exceptions import AiCallError

CreateParticipantCallable = Callable[["CreateSipParticipantPayload"], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class SipOutboundConfig:
    enabled: bool = False
    allowed_callee_prefixes: str = ""
    default_ringing_timeout_seconds: int = 45
    max_ringing_timeout_seconds: int = 120
    max_call_duration_seconds: int = 600
    trunk_id: str = ""
    trunk_hostname: str = ""
    destination_country: str = "CN"
    auth_username: str = ""
    auth_password: str = ""
    caller_number: str = ""
    signaling_port: int = 5080
    rtp_range: str = "16384-16484"
    public_ip: str = ""
    use_external_ip: bool = True

    @classmethod
    def from_settings(cls, settings: Settings) -> SipOutboundConfig:
        return cls(
            enabled=settings.AI_CALL_SIP_OUTBOUND_ENABLED,
            allowed_callee_prefixes=settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES,
            default_ringing_timeout_seconds=settings.AI_CALL_SIP_DEFAULT_RINGING_TIMEOUT_SECONDS,
            max_ringing_timeout_seconds=settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS,
            max_call_duration_seconds=settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS,
            trunk_id=settings.LIVEKIT_SIP_OUTBOUND_TRUNK_ID,
            trunk_hostname=settings.LIVEKIT_SIP_OUTBOUND_TRUNK_HOSTNAME or settings.SIP_PROXY,
            destination_country=settings.LIVEKIT_SIP_OUTBOUND_DESTINATION_COUNTRY,
            auth_username=settings.LIVEKIT_SIP_AUTH_USERNAME,
            auth_password=settings.LIVEKIT_SIP_AUTH_PASSWORD,
            caller_number=settings.SIP_CALLER_NUMBER,
            signaling_port=settings.SIP_SIGNALING_PORT,
            rtp_range=settings.SIP_RTP_RANGE,
            public_ip=settings.SIP_PUBLIC_IP,
            use_external_ip=settings.SIP_USE_EXTERNAL_IP,
        )


@dataclass(frozen=True, slots=True)
class SipOutboundPreflightResult:
    ok: bool
    failure_reason: str | None = None
    stage: str | None = None
    message: str | None = None


@dataclass(frozen=True, slots=True)
class CreateSipParticipantPayload:
    room_name: str
    participant_identity: str
    sip_call_to: str
    sip_number: str
    sip_trunk_id: str
    trunk_hostname: str
    auth_username: str
    auth_password: str
    destination_country: str
    wait_until_answered: bool
    ringing_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class CreateSipParticipantResult:
    room_name: str
    participant_identity: str
    sip_call_id: str | None
    sip_call_id_full: str | None
    sip_trunk_id: str | None
    sip_call_status: str | None
    raw_status: str


@dataclass(frozen=True, slots=True)
class HumanCallbackSessionResult:
    call_id: str
    room_name: str
    customer_participant_identity: str
    agent_participant_identity: str
    livekit_url: str
    participant_token: str
    expires_in_seconds: int


class HumanOnlySipSessionFactory:
    """创建不启动 AI Runner 的浏览器人工回拨 Room。"""

    def __init__(self, *, room_manager: Any, sip_client: LiveKitSipClient) -> None:
        self.room_manager = room_manager
        self.sip_client = sip_client

    async def create(
        self,
        *,
        call_id: str,
        callee_phone_number: str,
        config: SipOutboundConfig | None = None,
    ) -> HumanCallbackSessionResult:
        room_name = f"ai-call-{call_id}"
        customer_identity = f"sip-{call_id}"
        agent_identity = f"human-callback-{call_id}"
        await self.room_manager.create_room(room_name)
        try:
            token = self.room_manager.issue_browser_token(room_name, agent_identity)
            await self.sip_client.create_participant(
                room_name=room_name,
                participant_identity=customer_identity,
                callee_phone_number=callee_phone_number,
                wait_until_answered=False,
                config=config,
            )
        except Exception:
            await self.room_manager.delete_room(room_name)
            raise
        return HumanCallbackSessionResult(
            call_id=call_id,
            room_name=room_name,
            customer_participant_identity=customer_identity,
            agent_participant_identity=agent_identity,
            livekit_url=token.livekit_url,
            participant_token=token.participant_token,
            expires_in_seconds=token.expires_in_seconds,
        )

    async def end(self, *, call_id: str) -> None:
        await self.room_manager.remove_participant(
            f"ai-call-{call_id}",
            f"sip-{call_id}",
        )

    async def get_call_status(self, *, call_id: str) -> str | None:
        fact = await self.room_manager.get_participant_media(
            f"ai-call-{call_id}",
            f"sip-{call_id}",
        )
        return fact.sip_call_status if fact else None


class LiveKitSipClient:
    def __init__(
        self,
        *,
        config: SipOutboundConfig,
        livekit_url: str = "",
        api_key: str = "",
        api_secret: str = "",
        timeout_seconds: float = 10.0,
        create_participant: CreateParticipantCallable | None = None,
    ) -> None:
        self.config = config
        self.livekit_url = livekit_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout_seconds = max(1.0, timeout_seconds)
        self._create_participant = create_participant

    def preflight(
        self,
        *,
        callee_phone_number: str,
        config: SipOutboundConfig | None = None,
    ) -> SipOutboundPreflightResult:
        return validate_sip_outbound_preflight(
            config or self.config,
            callee_phone_number=callee_phone_number,
        )

    async def create_participant(
        self,
        *,
        room_name: str,
        participant_identity: str,
        callee_phone_number: str,
        ringing_timeout_seconds: int | None = None,
        wait_until_answered: bool = True,
        config: SipOutboundConfig | None = None,
    ) -> CreateSipParticipantResult:
        effective_config = config or self.config
        preflight = self.preflight(
            callee_phone_number=callee_phone_number,
            config=effective_config,
        )
        if not preflight.ok:
            raise AiCallError(
                error_id=preflight.failure_reason or "sip_preflight_failed",
                msg=preflight.message or "SIP 外呼预检失败",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        timeout_seconds = self._ringing_timeout_seconds(
            ringing_timeout_seconds,
            effective_config,
        )
        payload = CreateSipParticipantPayload(
            room_name=room_name,
            participant_identity=participant_identity,
            sip_call_to=_normalize_required(callee_phone_number),
            sip_number=_normalize_required(effective_config.caller_number),
            sip_trunk_id=_normalize_optional(effective_config.trunk_id),
            trunk_hostname=_normalize_optional(effective_config.trunk_hostname),
            auth_username=_normalize_optional(effective_config.auth_username),
            auth_password=_normalize_optional(effective_config.auth_password),
            destination_country=(
                _normalize_optional(effective_config.destination_country) or "CN"
            ),
            wait_until_answered=wait_until_answered,
            ringing_timeout_seconds=timeout_seconds,
        )
        raw_result = await self._create(payload)
        return self._coerce_result(raw_result, payload)

    async def _create(self, payload: CreateSipParticipantPayload) -> Any:
        if self._create_participant is not None:
            result = self._create_participant(payload)
            if inspect.isawaitable(result):
                return await result
            return result
        return await self._create_with_official_sdk(payload)

    async def _create_with_official_sdk(self, payload: CreateSipParticipantPayload) -> Any:
        if not (self.livekit_url and self.api_key and self.api_secret):
            raise AiCallError(
                error_id="sip_sdk_config_missing",
                msg="LiveKit SIP SDK 配置不完整",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        try:
            from livekit import api

            request = _build_official_create_sip_participant_request(payload)
        except Exception as exc:
            raise AiCallError(
                error_id="sip_sdk_unavailable",
                msg="LiveKit SIP SDK 不可用",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) from exc

        client = api.LiveKitAPI(
            url=self.livekit_url,
            api_key=self.api_key,
            api_secret=self.api_secret,
        )
        try:
            return await client.sip.create_sip_participant(
                request,
                timeout=self._request_timeout_seconds(payload),
            )
        except AiCallError:
            raise
        except Exception as exc:
            raise AiCallError(
                error_id="sip_create_participant_failed",
                msg="LiveKit SIP Participant 创建失败",
                status_code=status.HTTP_502_BAD_GATEWAY,
                details=_safe_exception_details(exc),
            ) from exc
        finally:
            await client.aclose()

    @staticmethod
    def _ringing_timeout_seconds(
        value: int | None,
        config: SipOutboundConfig,
    ) -> int:
        timeout = config.default_ringing_timeout_seconds if value is None else int(value)
        return min(max(1, timeout), config.max_ringing_timeout_seconds)

    def _request_timeout_seconds(self, payload: CreateSipParticipantPayload) -> float:
        if not payload.wait_until_answered:
            return self.timeout_seconds
        return max(self.timeout_seconds, float(payload.ringing_timeout_seconds + 5))

    @staticmethod
    def _coerce_result(
        raw_result: Any,
        payload: CreateSipParticipantPayload,
    ) -> CreateSipParticipantResult:
        attributes = _extract_attributes(raw_result)
        return CreateSipParticipantResult(
            room_name=_extract_value(raw_result, "room_name") or payload.room_name,
            participant_identity=(
                _extract_value(raw_result, "participant_identity")
                or _extract_value(raw_result, "identity")
                or payload.participant_identity
            ),
            sip_call_id=(_extract_value(raw_result, "sip_call_id") or attributes.get("sip.callID")),
            sip_call_id_full=attributes.get("sip.callIDFull"),
            sip_trunk_id=attributes.get("sip.trunkID") or payload.sip_trunk_id or None,
            sip_call_status=(
                attributes.get("sip.callStatus")
                or ("answered" if payload.wait_until_answered else None)
            ),
            raw_status=_extract_value(raw_result, "status") or "created",
        )


def validate_sip_outbound_preflight(
    config: SipOutboundConfig,
    *,
    callee_phone_number: str,
) -> SipOutboundPreflightResult:
    line_preflight = validate_sip_outbound_line_config(config)
    if not line_preflight.ok:
        return line_preflight

    callee = _normalize_optional(callee_phone_number)
    if not callee or not re.fullmatch(r"\+?\d{5,20}", callee):
        return _preflight_failed(
            "invalid_callee_number",
            "callee_number",
            "被叫号码格式不合法",
        )
    allowed_prefixes = _split_csv(config.allowed_callee_prefixes)
    if allowed_prefixes and not any(callee.startswith(prefix) for prefix in allowed_prefixes):
        return _preflight_failed(
            "callee_prefix_not_allowed",
            "callee_number",
            "被叫号码不在允许拨打前缀内",
        )
    return SipOutboundPreflightResult(ok=True)


def validate_sip_outbound_line_config(
    config: SipOutboundConfig,
) -> SipOutboundPreflightResult:
    if not config.enabled:
        return _preflight_failed(
            "sip_outbound_disabled",
            "sip_config",
            "SIP 真实外呼未启用",
        )
    if not (_normalize_optional(config.trunk_id) or _normalize_optional(config.trunk_hostname)):
        return _preflight_failed(
            "sip_trunk_missing",
            "sip_trunk",
            "SIP trunk 配置缺失",
        )
    if not _normalize_optional(config.caller_number):
        return _preflight_failed(
            "sip_caller_number_missing",
            "sip_trunk",
            "SIP 主叫显号缺失",
        )
    if _normalize_optional(config.trunk_id):
        return SipOutboundPreflightResult(ok=True)
    if config.signaling_port <= 0 or config.signaling_port > 65535:
        return _preflight_failed(
            "sip_signaling_port_invalid",
            "sip_network",
            "SIP signaling 端口不合法",
        )
    if not _valid_port_range(config.rtp_range):
        return _preflight_failed(
            "sip_rtp_range_invalid",
            "sip_network",
            "SIP RTP 端口范围不合法",
        )
    if not _normalize_optional(config.public_ip):
        return _preflight_failed(
            "sip_public_ip_missing",
            "sip_network",
            "SIP 公网地址缺失",
        )
    return SipOutboundPreflightResult(ok=True)


def _build_official_create_sip_participant_request(payload: CreateSipParticipantPayload):
    from google.protobuf.duration_pb2 import Duration
    from livekit.protocol import sip

    request = sip.CreateSIPParticipantRequest(
        room_name=payload.room_name,
        participant_identity=payload.participant_identity,
        sip_call_to=payload.sip_call_to,
        sip_number=payload.sip_number,
        wait_until_answered=payload.wait_until_answered,
        ringing_timeout=Duration(seconds=payload.ringing_timeout_seconds),
    )
    if payload.sip_trunk_id:
        request.sip_trunk_id = payload.sip_trunk_id
    else:
        request.trunk.CopyFrom(
            sip.SIPOutboundConfig(
                hostname=payload.trunk_hostname,
                destination_country=payload.destination_country,
                auth_username=payload.auth_username,
                auth_password=payload.auth_password,
            )
        )
    return request


def _preflight_failed(
    failure_reason: str,
    stage: str,
    message: str,
) -> SipOutboundPreflightResult:
    return SipOutboundPreflightResult(
        ok=False,
        failure_reason=failure_reason,
        stage=stage,
        message=message,
    )


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _safe_exception_details(exc: Exception) -> dict[str, str]:
    raw_message = str(exc).strip()
    details = {"rawErrorType": exc.__class__.__name__}
    if raw_message:
        safe_message = _sanitize_error_message(raw_message)[:500]
        details["rawErrorMessage"] = safe_message
        status_match = re.search(r"(?i)\bSIP[\s_:-]?([1-6]\d{2})\b", safe_message)
        if status_match:
            details["providerStatusCode"] = status_match.group(1)
        hangup_match = re.search(
            r"(?i)\bhangup[_\s-]?cause\s*[:=]\s*([A-Z0-9_-]+)",
            safe_message,
        )
        if hangup_match:
            details["hangupCause"] = hangup_match.group(1)
        provider_reason = re.split(
            r"(?i)\s*[;,]\s*hangup[_\s-]?cause\s*[:=]",
            safe_message,
            maxsplit=1,
        )[0].strip()
        if "sip request timed out" in safe_message.casefold():
            details["providerReason"] = "线路无响应（SIP 请求超时）"
        elif status_match and provider_reason:
            details["providerReason"] = provider_reason
    return details


def _sanitize_error_message(value: str) -> str:
    sanitized = re.sub(r"\+?\d{5,20}", lambda match: _mask_digits(match.group(0)), value)
    return re.sub(
        r"(?i)(authorization|token|api[_-]?key|secret|password)=\S+",
        r"\1=<redacted>",
        sanitized,
    )


def _mask_digits(value: str) -> str:
    prefix = "+" if value.startswith("+") else ""
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) <= 7:
        return "***"
    return f"{prefix}{digits[:3]}****{digits[-4:]}"


def _valid_port_range(value: str) -> bool:
    match = re.fullmatch(r"\s*(\d{1,5})\s*-\s*(\d{1,5})\s*", str(value or ""))
    if not match:
        return False
    start = int(match.group(1))
    end = int(match.group(2))
    return 0 < start <= end <= 65535


def _normalize_required(value: str) -> str:
    return str(value or "").strip()


def _normalize_optional(value: str | None) -> str:
    return str(value or "").strip() if value is not None else ""


def _extract_attributes(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        attributes = value.get("attributes")
    else:
        attributes = getattr(value, "attributes", None)
    if not isinstance(attributes, dict):
        return {}
    return {str(key): str(item) for key, item in attributes.items()}


def _extract_value(value: Any, key: str) -> str:
    if isinstance(value, dict):
        raw = value.get(key)
    else:
        raw = getattr(value, key, None)
    return str(raw or "").strip()
