from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Protocol

from fastapi import status
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.setting import Settings
from app.core.exceptions import CustomException
from app.services.ai_call.livekit_sip import (
    SipOutboundConfig,
    validate_sip_outbound_line_config,
)
from app.utils.id_util import generate_snowflake_id

from .sip_line_model import AiCallSipLineModel
from .sip_line_schema import (
    SipLineHealthOut,
    SipLineIn,
    SipLineOut,
    SipLineSnapshot,
)

DEFAULT_MARKER = "OUTBOUND"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class SipLinePreflightChecker(Protocol):
    async def check(self, config: SipOutboundConfig) -> None: ...


class LiveKitReadOnlyPreflightChecker:
    """只验证 LiveKit API 可访问，不创建 Room 或 SIP Participant。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def check(self, config: SipOutboundConfig) -> None:
        del config
        if not (
            self.settings.LIVEKIT_URL
            and self.settings.LIVEKIT_API_KEY
            and self.settings.LIVEKIT_API_SECRET
        ):
            raise RuntimeError("LiveKit API 配置不完整")

        from livekit import api

        async with api.LiveKitAPI(
            url=self.settings.LIVEKIT_URL,
            api_key=self.settings.LIVEKIT_API_KEY,
            api_secret=self.settings.LIVEKIT_API_SECRET,
        ) as livekit_api:
            await livekit_api.room.list_rooms(api.ListRoomsRequest(names=[]))


class SipLineService:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        preflight_checker: SipLinePreflightChecker | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.preflight_checker = preflight_checker or LiveKitReadOnlyPreflightChecker(
            self.settings
        )

    async def list_lines(
        self,
        db: AsyncSession,
        tenant_id: str,
        *,
        page_num: int,
        page_size: int,
    ) -> tuple[list[SipLineOut], int]:
        conditions = [
            AiCallSipLineModel.tenant_id == tenant_id,
            AiCallSipLineModel.deleted.is_(False),
        ]
        total = int(
            await db.scalar(select(func.count(AiCallSipLineModel.id)).where(*conditions))
            or 0
        )
        rows = (
            await db.scalars(
                select(AiCallSipLineModel)
                .where(*conditions)
                .order_by(AiCallSipLineModel.updated_at.desc(), AiCallSipLineModel.id)
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        return [self.line_out(row) for row in rows], total

    async def get_line(
        self,
        db: AsyncSession,
        tenant_id: str,
        line_id: int,
    ) -> AiCallSipLineModel:
        line = await db.scalar(
            select(AiCallSipLineModel).where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.id == line_id,
                AiCallSipLineModel.deleted.is_(False),
            )
        )
        if line is None:
            raise CustomException(
                msg="SIP 外呼线路不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return line

    async def create_line(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        request: SipLineIn,
    ) -> AiCallSipLineModel:
        duplicate = await db.scalar(
            select(AiCallSipLineModel.id).where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.line_code == request.line_code,
            )
        )
        if duplicate is not None:
            raise CustomException(
                msg="线路编码已存在",
                status_code=status.HTTP_409_CONFLICT,
            )
        now = _now()
        line = AiCallSipLineModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            default_marker=None,
            health_status="UNKNOWN",
            health_message=None,
            last_checked_at=None,
            deleted=False,
            deleted_at=None,
            created_by=user_id,
            updated_by=user_id,
            created_at=now,
            updated_at=now,
            **request.model_dump(),
        )
        db.add(line)
        await db.flush()
        return line

    async def update_line(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
        request: SipLineIn,
    ) -> AiCallSipLineModel:
        line = await self.get_line(db, tenant_id, line_id)
        if request.line_code != line.line_code:
            raise CustomException(
                msg="线路编码创建后不可修改",
                status_code=status.HTTP_409_CONFLICT,
            )
        if line.default_marker == DEFAULT_MARKER and not request.enabled:
            self._ensure_not_default(line)
        for field, value in request.model_dump().items():
            setattr(line, field, value)
        line.health_status = "UNKNOWN"
        line.health_message = None
        line.last_checked_at = None
        line.updated_by = user_id
        line.updated_at = _now()
        await db.flush()
        return line

    async def set_default(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
    ) -> AiCallSipLineModel:
        line = await self.get_line(db, tenant_id, line_id)
        if not line.enabled:
            raise CustomException(
                msg="停用线路不能设为默认线路",
                status_code=status.HTTP_409_CONFLICT,
            )
        await db.execute(
            select(AiCallSipLineModel.id)
            .where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.deleted.is_(False),
            )
            .with_for_update()
        )
        await db.execute(
            update(AiCallSipLineModel)
            .where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.default_marker == DEFAULT_MARKER,
                AiCallSipLineModel.id != line_id,
            )
            .values(default_marker=None, updated_by=user_id, updated_at=_now())
        )
        line.default_marker = DEFAULT_MARKER
        line.updated_by = user_id
        line.updated_at = _now()
        await db.flush()
        return line

    async def enable(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
    ) -> AiCallSipLineModel:
        line = await self.get_line(db, tenant_id, line_id)
        line.enabled = True
        line.updated_by = user_id
        line.updated_at = _now()
        await db.flush()
        return line

    async def disable(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
    ) -> AiCallSipLineModel:
        line = await self.get_line(db, tenant_id, line_id)
        self._ensure_not_default(line)
        line.enabled = False
        line.updated_by = user_id
        line.updated_at = _now()
        await db.flush()
        return line

    async def delete(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
    ) -> None:
        line = await self.get_line(db, tenant_id, line_id)
        self._ensure_not_default(line)
        now = _now()
        line.deleted = True
        line.deleted_at = now
        line.updated_by = user_id
        line.updated_at = now
        await db.flush()

    async def resolve_default(
        self,
        db: AsyncSession,
        tenant_id: str,
        *,
        require_available: bool = True,
    ) -> AiCallSipLineModel:
        line = await db.scalar(
            select(AiCallSipLineModel).where(
                AiCallSipLineModel.tenant_id == tenant_id,
                AiCallSipLineModel.default_marker == DEFAULT_MARKER,
                AiCallSipLineModel.deleted.is_(False),
                AiCallSipLineModel.enabled.is_(True),
            )
        )
        if line is None:
            raise CustomException(
                msg="当前租户没有可用的默认外呼线路",
                status_code=status.HTTP_409_CONFLICT,
            )
        if require_available and line.health_status != "AVAILABLE":
            message = line.health_message or {
                "UNKNOWN": "请先执行线路预检",
                "MISCONFIGURED": "默认外呼线路配置不完整",
                "UNAVAILABLE": "默认外呼线路当前不可用",
            }.get(line.health_status, "默认外呼线路当前不可用")
            raise CustomException(
                msg=message,
                status_code=status.HTTP_409_CONFLICT,
            )
        return line

    async def preflight(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        line_id: int,
    ) -> SipLineHealthOut:
        line = await self.get_line(db, tenant_id, line_id)
        checked_at = _now()
        config = self.to_sip_config(line)
        preflight = validate_sip_outbound_line_config(config)
        if not preflight.ok:
            line.health_status = "MISCONFIGURED"
            line.health_message = preflight.message or "SIP 线路配置不完整"
        else:
            try:
                await self.preflight_checker.check(config)
            except Exception as exc:
                line.health_status = "UNAVAILABLE"
                line.health_message = self._safe_health_message(exc)
            else:
                line.health_status = "AVAILABLE"
                line.health_message = (
                    "基础配置有效，LiveKit API 可连接；"
                    "未验证运营商 SIP trunk、号码路由、振铃、媒体或真实通话"
                )
        line.last_checked_at = checked_at
        line.updated_by = user_id
        line.updated_at = checked_at
        await db.flush()
        return SipLineHealthOut(
            line_id=str(line.id),
            health_status=line.health_status,
            health_message=line.health_message,
            last_checked_at=checked_at,
        )

    def snapshot(self, line: AiCallSipLineModel) -> SipLineSnapshot:
        return SipLineSnapshot(
            line_id=str(line.id),
            line_code=line.line_code,
            line_name=line.line_name,
            adapter_type=line.adapter_type,
            route_mode=line.route_mode,
            trunk_id=line.trunk_id,
            proxy_host=line.proxy_host,
            proxy_port=line.proxy_port,
            auth_mode=line.auth_mode,
            caller_number=line.caller_number,
            destination_country=line.destination_country,
            max_concurrency=line.max_concurrency,
            originate_timeout_seconds=line.originate_timeout_seconds,
        )

    def snapshot_json(self, line: AiCallSipLineModel) -> str:
        return json.dumps(
            self.snapshot(line).model_dump(mode="json", by_alias=True),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def line_out(self, line: AiCallSipLineModel) -> SipLineOut:
        return SipLineOut(
            **self.snapshot(line).model_dump(),
            enabled=line.enabled,
            is_default=line.default_marker == DEFAULT_MARKER,
            health_status=line.health_status,
            health_message=line.health_message,
            last_checked_at=line.last_checked_at,
            created_at=line.created_at,
            updated_at=line.updated_at,
        )

    def to_sip_config(self, line: AiCallSipLineModel) -> SipOutboundConfig:
        trunk_hostname = ""
        if line.route_mode == "inline_hostname" and line.proxy_host and line.proxy_port:
            trunk_hostname = f"{line.proxy_host}:{line.proxy_port}"
        return SipOutboundConfig(
            enabled=self.settings.AI_CALL_SIP_OUTBOUND_ENABLED,
            allowed_callee_prefixes=self.settings.AI_CALL_SIP_ALLOWED_CALLEE_PREFIXES,
            default_ringing_timeout_seconds=line.originate_timeout_seconds,
            max_ringing_timeout_seconds=self.settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS,
            max_call_duration_seconds=self.settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS,
            trunk_id=line.trunk_id or "",
            trunk_hostname=trunk_hostname,
            destination_country=line.destination_country,
            auth_username="",
            auth_password="",
            caller_number=line.caller_number,
            signaling_port=self.settings.SIP_SIGNALING_PORT,
            rtp_range=self.settings.SIP_RTP_RANGE,
            public_ip=self.settings.SIP_PUBLIC_IP,
            use_external_ip=self.settings.SIP_USE_EXTERNAL_IP,
        )

    @staticmethod
    def _ensure_not_default(line: AiCallSipLineModel) -> None:
        if line.default_marker == DEFAULT_MARKER:
            raise CustomException(
                msg="请先指定其他默认线路，再停用或删除当前线路",
                status_code=status.HTTP_409_CONFLICT,
            )

    @staticmethod
    def _safe_health_message(exc: Exception) -> str:
        return f"LiveKit API 连接失败（{exc.__class__.__name__}）"
