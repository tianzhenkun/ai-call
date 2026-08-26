from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.service import get_default_ai_call_service
from app.config.setting import Settings, settings
from app.services.ai_call.livekit_sip import SipOutboundConfig

from .attempt_projection import (
    BUSY_END_REASONS,
    INVALID_NUMBER_END_REASONS,
    NO_ANSWER_END_REASONS,
    REJECTED_END_REASONS,
)
from .media_evidence import has_persisted_media_evidence
from .sip_line_schema import SipLineSnapshot
from .task_executor import ConnectedCallback, DialResult, OutboundDialRequest

TERMINAL_STATUSES = {"completed", "failed"}


def build_sip_line_config(
    settings: Settings,
    line: SipLineSnapshot,
) -> SipOutboundConfig:
    trunk_hostname = ""
    if line.route_mode == "inline_hostname":
        if not line.proxy_host or line.proxy_port is None:
            raise ValueError("内联 SIP 线路缺少代理地址或端口")
        trunk_hostname = f"{line.proxy_host}:{line.proxy_port}"
    return SipOutboundConfig(
        enabled=settings.AI_CALL_SIP_OUTBOUND_ENABLED,
        allowed_callee_prefixes=settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES,
        default_ringing_timeout_seconds=line.originate_timeout_seconds,
        max_ringing_timeout_seconds=settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS,
        max_call_duration_seconds=settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS,
        trunk_id=line.trunk_id or "",
        trunk_hostname=trunk_hostname,
        destination_country=line.destination_country,
        auth_username="",
        auth_password="",
        caller_number=line.caller_number,
        signaling_port=settings.SIP_SIGNALING_PORT,
        rtp_range=settings.SIP_RTP_RANGE,
        public_ip=settings.SIP_PUBLIC_IP,
        use_external_ip=settings.SIP_USE_EXTERNAL_IP,
    )


class AiCallServiceLike(Protocol):
    async def create_sip_session(self, **kwargs): ...

    async def terminate_sip_session(
        self,
        call_id: str,
        *,
        end_reason: str,
    ) -> None: ...


AiCallServiceFactory = Callable[
    [AsyncSession, SipOutboundConfig],
    AiCallServiceLike,
]


def _default_service_factory(
    db: AsyncSession,
    config: SipOutboundConfig,
) -> AiCallServiceLike:
    return get_default_ai_call_service(db, sip_config=config)


class SipOutboundDialer:
    """正式 SIP 外呼适配器，只以持久化的接听和媒体事件认定接通。"""

    dialer_type = "sip"
    manages_call_record = True

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        ai_call_service_factory: AiCallServiceFactory = _default_service_factory,
        settings: Settings = settings,
        poll_seconds: float = 0.5,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        reconciliation_grace_seconds: int = 60,
    ) -> None:
        self.session_factory = session_factory
        self.ai_call_service_factory = ai_call_service_factory
        self.settings = settings
        self.poll_seconds = max(0.0, poll_seconds)
        self.sleep = sleep
        self.monotonic = monotonic
        self.reconciliation_grace_seconds = max(0, reconciliation_grace_seconds)

    async def dial(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        on_connected: ConnectedCallback,
    ) -> DialResult:
        line = self._request_line(request)
        config = self.build_sip_config(line)
        reconciliation_deadline = self.monotonic() + (
            line.originate_timeout_seconds
            + config.max_call_duration_seconds
            + self.reconciliation_grace_seconds
        )
        create_error: Exception | None = None

        async with self.session_factory() as db:
            service = self.ai_call_service_factory(db, config)
            try:
                await service.create_sip_session(
                    tenant_id=request.tenant_id,
                    callee_phone_number=request.phone_number,
                    voice=request.voice,
                    call_id=call_id,
                    business_type="outbound_task",
                    business_id=str(request.task_id),
                    scene_code=request.scene_code,
                    business_params={
                        **request.business_params,
                        "targetId": str(request.target_id),
                        "attemptNo": request.attempt_no,
                        "lineId": line.line_id,
                        "lineCode": line.line_code,
                    },
                    ringing_timeout_seconds=line.originate_timeout_seconds,
                    before_sip_invite=db.commit,
                    prompt_snapshot=request.prompt_snapshot,
                )
            except Exception as exc:
                create_error = exc
                try:
                    await db.commit()
                except Exception:
                    await db.rollback()
            else:
                try:
                    await db.commit()
                except Exception as exc:
                    await db.rollback()
                    create_error = exc

        if create_error is not None:
            try:
                record, media_connected = await self._read_evidence(call_id)
            except Exception as evidence_error:
                return await self._handle_uncertain_error(
                    request,
                    call_id=call_id,
                    error=evidence_error,
                    end_reason="outbound_create_reconciliation_error",
                )
            if self._is_terminal(record):
                if self._is_connected(record, media_connected):
                    await on_connected()
                return self._with_exception_diagnostics(
                    self._map_evidenced_terminal(record, media_connected),
                    create_error,
                )
            return await self._handle_uncertain_error(
                request,
                call_id=call_id,
                error=create_error,
                end_reason="outbound_create_error",
            )

        connected_notified = False
        try:
            while True:
                record, media_connected = await self._read_evidence(call_id)
                if (
                    not connected_notified
                    and self._is_connected(record, media_connected)
                ):
                    await on_connected()
                    connected_notified = True
                if self._is_terminal(record):
                    return self._map_evidenced_terminal(record, media_connected)
                if self.monotonic() >= reconciliation_deadline:
                    connected_before_deadline = self._is_connected(
                        record,
                        media_connected,
                    )
                    cleaned = await self.terminate(
                        request,
                        call_id=call_id,
                        end_reason="outbound_reconcile_timeout",
                    )
                    if not cleaned:
                        return DialResult(
                            call_result="call_failed",
                            error_message=(
                                "SIP 通话状态对账超时，资源清理失败，保持待对账"
                            ),
                            retry_allowed=False,
                            settle_attempt=False,
                        )
                    if connected_before_deadline:
                        final_record = record
                        final_media_connected = media_connected
                        try:
                            refreshed_record, refreshed_media_connected = (
                                await self._read_evidence(call_id)
                            )
                            if refreshed_record is not None:
                                final_record = refreshed_record
                                final_media_connected = refreshed_media_connected
                        except Exception:
                            pass
                        if final_record is not None:
                            return replace(
                                self._map_evidenced_terminal(
                                    final_record,
                                    final_media_connected,
                                ),
                                retry_allowed=False,
                            )
                    return DialResult(
                        call_result="call_failed",
                        error_message="SIP 通话状态对账超时，禁止自动重拨",
                        retry_allowed=False,
                    )
                await self.sleep(self.poll_seconds)
        except Exception as exc:
            return await self._handle_uncertain_error(
                request,
                call_id=call_id,
                error=exc,
                end_reason="outbound_reconcile_error",
                retry_allowed=False,
            )

    async def _handle_uncertain_error(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        error: Exception,
        end_reason: str,
        retry_allowed: bool = True,
    ) -> DialResult:
        result = self._with_exception_diagnostics(
            DialResult(
                call_result="call_failed",
                error_message=self._error_message(error),
                retry_allowed=retry_allowed,
            ),
            error,
        )
        if await self.terminate(
            request,
            call_id=call_id,
            end_reason=end_reason,
        ):
            return result
        return replace(
            result,
            error_message=(
                f"{self._error_message(error)}；SIP 资源清理失败，保持待对账"
            ),
            retry_allowed=False,
            settle_attempt=False,
        )

    async def terminate(
        self,
        request: OutboundDialRequest,
        *,
        call_id: str,
        end_reason: str,
    ) -> bool:
        line = self._request_line(request)
        config = self.build_sip_config(line)
        async with self.session_factory() as db:
            service = self.ai_call_service_factory(db, config)
            try:
                await service.terminate_sip_session(
                    call_id,
                    end_reason=end_reason,
                )
                await db.commit()
            except Exception:
                await db.rollback()
                return False
        return True

    def build_sip_config(self, line: SipLineSnapshot) -> SipOutboundConfig:
        return build_sip_line_config(self.settings, line)

    async def _read_evidence(
        self,
        call_id: str,
    ) -> tuple[AiCallRecordModel | None, bool]:
        async with self.session_factory() as db:
            record = await db.scalar(
                select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
            )
            media_connected = await has_persisted_media_evidence(db, call_id)
        return record, media_connected

    @staticmethod
    def _request_line(request: OutboundDialRequest) -> SipLineSnapshot:
        line = getattr(request, "line", None)
        if isinstance(line, SipLineSnapshot):
            return line
        if isinstance(line, dict):
            return SipLineSnapshot.model_validate(line)
        raise ValueError("正式 SIP 外呼请求缺少线路快照")

    @staticmethod
    def _is_connected(
        record: AiCallRecordModel | None,
        media_connected: bool,
    ) -> bool:
        return (
            record is not None
            and record.answered_at is not None
            and media_connected
        )

    @staticmethod
    def _is_terminal(record: AiCallRecordModel | None) -> bool:
        return (
            record is not None
            and str(record.status or "").lower() in TERMINAL_STATUSES
        )

    @staticmethod
    def map_terminal_record(record: AiCallRecordModel) -> DialResult:
        provider_reason = record.failure_message or record.end_reason
        provider_status_code = SipOutboundDialer._provider_status_code(
            record.end_reason,
            record.failure_message,
        )
        hangup_cause = SipOutboundDialer._hangup_cause(record.failure_message)
        if record.answered_at is not None:
            return DialResult(
                call_result="connected",
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=provider_status_code,
                provider_reason=provider_reason,
                hangup_cause=hangup_cause,
            )

        reason = str(record.end_reason or "").strip().lower()
        error_message = provider_reason
        if reason in BUSY_END_REASONS:
            return DialResult(
                call_result="busy",
                error_message=error_message,
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=provider_status_code,
                provider_reason=provider_reason,
                hangup_cause=hangup_cause,
            )
        if reason in NO_ANSWER_END_REASONS:
            return DialResult(
                call_result="no_answer",
                error_message=error_message,
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=provider_status_code,
                provider_reason=provider_reason,
                hangup_cause=hangup_cause,
            )
        if (
            reason in REJECTED_END_REASONS
            or provider_status_code == "603"
            or hangup_cause in {"CALL_REJECTED", "21"}
        ):
            return DialResult(
                call_result="rejected",
                error_message=error_message,
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=provider_status_code,
                provider_reason=provider_reason,
                hangup_cause=hangup_cause,
            )
        if (
            reason in INVALID_NUMBER_END_REASONS
            or provider_status_code in {"404", "410", "484", "604"}
            or hangup_cause
            in {
                "ADDRESS_INCOMPLETE",
                "SUBSCRIBER_ABSENT",
                "UNALLOCATED_NUMBER",
                "USER_NOT_REGISTERED",
            }
        ):
            return DialResult(
                call_result="invalid_number",
                error_message=error_message,
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=provider_status_code,
                provider_reason=provider_reason,
                hangup_cause=hangup_cause,
            )
        return DialResult(
            call_result="call_failed",
            error_message=error_message or "SIP 外呼失败",
            duration_ms=max(0, int(record.duration_ms or 0)),
            provider_status_code=provider_status_code,
            provider_reason=provider_reason,
            hangup_cause=hangup_cause,
        )

    @staticmethod
    def _map_evidenced_terminal(
        record: AiCallRecordModel,
        media_connected: bool,
    ) -> DialResult:
        if record.answered_at is not None and not media_connected:
            return DialResult(
                call_result="call_failed",
                error_message="未检测到媒体接通证据",
                duration_ms=max(0, int(record.duration_ms or 0)),
                provider_status_code=SipOutboundDialer._provider_status_code(
                    record.end_reason,
                    record.failure_message,
                ),
                provider_reason=record.failure_message or record.end_reason,
                hangup_cause=SipOutboundDialer._hangup_cause(
                    record.failure_message
                ),
                retry_allowed=False,
            )
        return SipOutboundDialer.map_terminal_record(record)

    @staticmethod
    def _provider_status_code(*values: str | None) -> str | None:
        for value in values:
            match = re.search(r"(?i)\bsip[_\s:-]?([1-6]\d{2})\b", value or "")
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _hangup_cause(value: str | None) -> str | None:
        match = re.search(
            r"(?i)\bhangup[_\s-]?cause\s*[:=]\s*([A-Z0-9_-]+)",
            value or "",
        )
        return match.group(1) if match else None

    @staticmethod
    def _error_message(exc: Exception) -> str:
        message = getattr(exc, "msg", None) or str(exc).strip()
        return str(message or exc.__class__.__name__)[:500]

    @staticmethod
    def _with_exception_diagnostics(
        result: DialResult,
        exc: Exception,
    ) -> DialResult:
        cause: BaseException | None = exc
        details: dict | None = None
        while cause is not None:
            candidate = getattr(cause, "details", None)
            if isinstance(candidate, dict) and candidate:
                details = candidate
                break
            cause = cause.__cause__
        if details is None:
            return result
        provider_status_code = (
            str(details["providerStatusCode"])
            if details.get("providerStatusCode")
            else result.provider_status_code
        )
        call_result = result.call_result
        if call_result == "call_failed":
            hangup_cause = str(details.get("hangupCause") or "").upper()
            if provider_status_code in {"486", "600"}:
                call_result = "busy"
            elif provider_status_code in {"408", "480"}:
                call_result = "no_answer"
            elif provider_status_code == "603" or hangup_cause in {"CALL_REJECTED", "21"}:
                call_result = "rejected"
            elif provider_status_code in {"404", "410", "484", "604"} or hangup_cause in {
                "ADDRESS_INCOMPLETE",
                "SUBSCRIBER_ABSENT",
                "UNALLOCATED_NUMBER",
                "USER_NOT_REGISTERED",
            }:
                call_result = "invalid_number"
        return replace(
            result,
            call_result=call_result,
            provider_status_code=provider_status_code,
            provider_reason=(
                str(details["providerReason"])
                if details.get("providerReason")
                else result.provider_reason
            ),
            hangup_cause=(
                str(details["hangupCause"])
                if details.get("hangupCause")
                else result.hangup_cause
            ),
            retry_allowed=(
                False
                if details.get("cleanupFailed")
                else result.retry_allowed
            ),
            settle_attempt=(
                False
                if details.get("cleanupFailed")
                else result.settle_attempt
            ),
        )
