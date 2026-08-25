from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .schema import OutboundSchema

LineHealthStatus = Literal[
    "UNKNOWN",
    "AVAILABLE",
    "MISCONFIGURED",
    "UNAVAILABLE",
]
LineRouteMode = Literal["managed_trunk_id", "inline_hostname"]
LineAuthMode = Literal["managed_trunk", "ip_allowlist"]


class SipLineIn(OutboundSchema):
    line_code: str = Field(min_length=1, max_length=64)
    line_name: str = Field(min_length=1, max_length=100)
    description: str | None = Field(default=None, max_length=500)
    unit_price: Decimal | None = Field(
        default=None,
        ge=0,
        max_digits=12,
        decimal_places=4,
    )
    purpose: str | None = Field(default=None, max_length=200)
    expires_at: date | None = None
    enabled: bool = True
    adapter_type: Literal["livekit_sip"] = "livekit_sip"
    route_mode: LineRouteMode
    trunk_id: str | None = Field(default=None, max_length=128)
    proxy_host: str | None = Field(default=None, max_length=255)
    proxy_port: int | None = Field(default=None, ge=1, le=65535)
    auth_mode: LineAuthMode
    caller_number: str = Field(min_length=1, max_length=64)
    destination_country: str = Field(default="CN", min_length=2, max_length=8)
    max_concurrency: int = Field(default=1, ge=1, le=1000)
    originate_timeout_seconds: int = Field(default=45, ge=1, le=120)

    @field_validator(
        "line_code",
        "line_name",
        "trunk_id",
        "proxy_host",
        "caller_number",
        "destination_country",
        mode="before",
    )
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return str(value).strip()

    @field_validator("description", "purpose", mode="before")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def validate_route(self) -> SipLineIn:
        if self.route_mode == "managed_trunk_id":
            if not self.trunk_id or self.proxy_host or self.proxy_port:
                raise ValueError("托管线路必须且只能提供 trunkId")
            if self.auth_mode != "managed_trunk":
                raise ValueError("托管线路必须使用 managed_trunk 鉴权")
        else:
            if not self.proxy_host or self.proxy_port is None or self.trunk_id:
                raise ValueError("内联线路必须且只能提供 proxyHost 和 proxyPort")
            if self.auth_mode != "ip_allowlist":
                raise ValueError("V1 内联线路只支持 IP 白名单鉴权")
        return self


class SipLineSnapshot(OutboundSchema):
    line_id: str
    line_code: str
    line_name: str
    adapter_type: Literal["livekit_sip"]
    route_mode: LineRouteMode
    trunk_id: str | None = None
    proxy_host: str | None = None
    proxy_port: int | None = None
    auth_mode: LineAuthMode
    caller_number: str
    destination_country: str
    max_concurrency: int
    originate_timeout_seconds: int


class SipLineOut(SipLineSnapshot):
    description: str | None = None
    unit_price: Decimal | None = None
    purpose: str | None = None
    expires_at: date | None = None
    enabled: bool
    is_default: bool
    health_status: LineHealthStatus
    health_message: str | None = None
    last_checked_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class SipLineHealthOut(OutboundSchema):
    line_id: str
    health_status: LineHealthStatus
    health_message: str | None = None
    last_checked_at: datetime


class SipLineListOut(OutboundSchema):
    rows: list[SipLineOut]
    total: int
