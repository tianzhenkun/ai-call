from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import BigInteger, DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import or_, select

from app.config.setting import settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.core.logger import log
from app.utils.id_util import generate_snowflake_id

CREDIT_PRODUCT_CODE = "reach"
CREDIT_OWNER_TYPE = "USER"
CREDIT_SCENARIO_CODE = "reach_outbound_call"
CREDIT_METER_ITEM_CODE = "connected_call_seconds"
CREDIT_ELIGIBILITY_PATH = "/system/credit/external/v1/eligibility"
CREDIT_USAGE_PATH = "/system/credit/external/v1/usage-events"


class CreditAdmissionDenied(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


class CreditMeteringUnavailable(RuntimeError):
    pass


class CreditMeteringRejected(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class ReachCreditUsageOutboxModel(MappedBase):
    __tablename__ = "reach_credit_usage_outbox"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uk_reach_credit_usage_outbox_idempotency",
        ),
        Index(
            "idx_reach_credit_usage_outbox_dispatch",
            "status",
            "next_attempt_at",
            "created_at",
        ),
        {"comment": "Reach信用点用量事件可靠投递箱"},
    )
    __permission_strategy__ = None

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, default=generate_snowflake_id, autoincrement=False
    )
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str] = mapped_column(String(64), nullable=False)
    product_code: Mapped[str] = mapped_column(String(64), nullable=False)
    scenario_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meter_item_code: Mapped[str] = mapped_column(String(64), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(28, 8), nullable=False)
    source_id: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def build_signed_headers(
    *,
    method: str,
    path: str,
    body: str,
    client_id: str,
    secret: str,
    timestamp: str | None = None,
    nonce: str | None = None,
) -> dict[str, str]:
    resolved_timestamp = timestamp or str(int(time.time()))
    resolved_nonce = nonce or uuid.uuid4().hex
    digest = hashlib.sha256(body.encode()).hexdigest()
    canonical = "\n".join((method.upper(), path, resolved_timestamp, resolved_nonce, digest))
    signature = base64.b64encode(
        hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "Content-Type": "application/json",
        "X-LC-Client-Id": client_id,
        "X-LC-Timestamp": resolved_timestamp,
        "X-LC-Nonce": resolved_nonce,
        "X-LC-Signature": signature,
        "X-LC-Signature-Path": path,
    }


def _json_body(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _decimal_string(value: Decimal) -> str:
    plain = format(value, "f")
    return plain.rstrip("0").rstrip(".") if "." in plain else plain


class CreditMeteringClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        client_id: str = CREDIT_PRODUCT_CODE,
        secret: str | None = None,
        timeout_seconds: float = 10.0,
        verify_tls: bool = True,
        post: Callable[[str, dict[str, object]], Awaitable[dict[str, object]]] | None = None,
    ) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.client_id = client_id
        self.secret = secret or ""
        self.timeout_seconds = timeout_seconds
        self.verify_tls = verify_tls
        self._injected_post = post

    @classmethod
    def from_settings(cls) -> CreditMeteringClient:
        return cls(
            base_url=settings.REACH_CREDIT_GATEWAY_BASE_URL,
            client_id=settings.REACH_CREDIT_METERING_CLIENT_ID,
            secret=settings.REACH_CREDIT_METERING_SECRET,
            timeout_seconds=settings.REACH_CREDIT_METERING_REQUEST_TIMEOUT_SECONDS,
            verify_tls=settings.REACH_CREDIT_VERIFY_TLS,
        )

    async def post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        if self._injected_post is not None:
            return await self._injected_post(path, payload)
        if not self.base_url or not self.secret:
            raise CreditMeteringUnavailable("Reach计费客户端尚未配置")
        body = _json_body(payload)
        headers = build_signed_headers(
            method="POST",
            path=path,
            body=body,
            client_id=self.client_id,
            secret=self.secret,
        )
        try:
            async with httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout_seconds,
                verify=self.verify_tls,
            ) as client:
                response = await client.post(path, content=body.encode(), headers=headers)
        except httpx.HTTPError as exc:
            raise CreditMeteringUnavailable("计费服务暂不可用") from exc
        try:
            result = response.json()
        except ValueError as exc:
            raise CreditMeteringUnavailable(
                f"计费服务返回了无效响应（HTTP {response.status_code}）"
            ) from exc
        if response.is_error:
            message = str(result.get("msg") or result.get("message") or "计费服务请求失败")
            if response.status_code in {408, 429} or response.status_code >= 500:
                raise CreditMeteringUnavailable(f"{message}（HTTP {response.status_code}）")
            raise CreditMeteringRejected(response.status_code, message)
        data = result.get("data")
        if not isinstance(data, dict):
            raise CreditMeteringUnavailable("计费服务响应缺少data")
        return data

    async def require_eligible(self, *, tenant_id: str, owner_id: str) -> None:
        result = await self.post(
            CREDIT_ELIGIBILITY_PATH,
            {
                "tenantId": tenant_id,
                "ownerType": CREDIT_OWNER_TYPE,
                "ownerId": owner_id,
                "productCode": CREDIT_PRODUCT_CODE,
                "scenarioCode": CREDIT_SCENARIO_CODE,
                "meterItemCode": CREDIT_METER_ITEM_CODE,
            },
        )
        if bool(result.get("eligible")):
            return
        raise CreditAdmissionDenied(
            str(result.get("reasonCode") or "CREDIT_NOT_ELIGIBLE"),
            str(result.get("reasonMessage") or "当前信用点账户不能启动该任务"),
            dict(result),
        )


async def require_credit_eligible_for_request(
    client: CreditMeteringClient, *, tenant_id: str, owner_id: str
) -> None:
    try:
        await client.require_eligible(tenant_id=tenant_id, owner_id=owner_id)
    except CreditAdmissionDenied as exc:
        data: dict[str, object] = {"creditReason": exc.reason_code}
        for key in ("availablePoints", "minimumStartPoints"):
            if exc.details.get(key) is not None:
                data[key] = exc.details[key]
        raise CustomException(msg=str(exc), code=10402, status_code=402, data=data) from exc
    except (CreditMeteringUnavailable, CreditMeteringRejected) as exc:
        raise CustomException(msg=str(exc), code=10503, status_code=503) from exc


async def enqueue_connected_call_usage(
    session: AsyncSession,
    *,
    tenant_id: str,
    owner_id: str,
    attempt_id: int,
    call_id: str,
    duration_ms: int,
) -> None:
    if duration_ms <= 0:
        return
    existing = await session.scalar(
        select(ReachCreditUsageOutboxModel).where(
            ReachCreditUsageOutboxModel.tenant_id == tenant_id,
            ReachCreditUsageOutboxModel.idempotency_key == f"reach:call:{attempt_id}",
        )
    )
    if existing is not None:
        return
    now = datetime.now(timezone.utc)
    session.add(
        ReachCreditUsageOutboxModel(
            tenant_id=tenant_id,
            owner_id=owner_id,
            product_code=CREDIT_PRODUCT_CODE,
            scenario_code=CREDIT_SCENARIO_CODE,
            meter_item_code=CREDIT_METER_ITEM_CODE,
            quantity=Decimal(duration_ms) / Decimal(1000),
            source_id=call_id,
            idempotency_key=f"reach:call:{attempt_id}",
            occurred_at=now,
            payload_json=_json_body({
                "attemptId": str(attempt_id),
                "callId": call_id,
                "durationMs": duration_ms,
            }),
            status="pending",
            next_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
    )
    await session.flush()


class CreditUsageDispatcher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        client: CreditMeteringClient | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.client = client or CreditMeteringClient.from_settings()
        self.worker_id = worker_id or f"reach-credit:{uuid.uuid4().hex}"
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self.run_forever())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task

    async def run_once(self) -> bool:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                event = (
                    await session.execute(
                        select(ReachCreditUsageOutboxModel)
                        .where(
                            or_(
                                (ReachCreditUsageOutboxModel.status == "pending")
                                & (ReachCreditUsageOutboxModel.next_attempt_at <= now),
                                (ReachCreditUsageOutboxModel.status == "sending")
                                & (ReachCreditUsageOutboxModel.lease_expires_at <= now),
                            )
                        )
                        .order_by(ReachCreditUsageOutboxModel.created_at)
                        .with_for_update(skip_locked=True)
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if event is None:
                    return False
                event.status = "sending"
                event.attempt_count += 1
                event.lease_owner = self.worker_id
                event.lease_expires_at = now + timedelta(
                    seconds=settings.REACH_CREDIT_USAGE_LEASE_SECONDS
                )
                await session.flush()
                event_id = event.id
                payload = {
                    "tenantId": event.tenant_id,
                    "ownerType": CREDIT_OWNER_TYPE,
                    "ownerId": event.owner_id,
                    "productCode": event.product_code,
                    "scenarioCode": event.scenario_code,
                    "meterItemCode": event.meter_item_code,
                    "quantity": _decimal_string(event.quantity),
                    "sourceId": event.source_id,
                    "idempotencyKey": event.idempotency_key,
                    "occurredAt": round(event.occurred_at.timestamp() * 1000),
                    "payload": json.loads(event.payload_json or "{}"),
                }
        try:
            await self.client.post(CREDIT_USAGE_PATH, payload)
        except CreditMeteringRejected as exc:
            await self._finish(event_id, "failed", str(exc), retry=False)
        except Exception as exc:
            await self._finish(event_id, "pending", str(exc), retry=True)
        else:
            await self._finish(event_id, "sent", None, retry=False)
        return True

    async def _finish(self, event_id: int, status: str, error: str | None, *, retry: bool) -> None:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                event = await session.get(
                    ReachCreditUsageOutboxModel, event_id, with_for_update=True
                )
                if (
                    event is None
                    or event.status != "sending"
                    or event.lease_owner != self.worker_id
                ):
                    return
                event.status = status
                event.lease_owner = None
                event.lease_expires_at = None
                event.next_attempt_at = (
                    now + timedelta(seconds=min(300, 2 ** min(event.attempt_count, 8)))
                    if retry
                    else None
                )
                event.sent_at = now if status == "sent" else event.sent_at
                event.last_error = error[:2000] if error else None
                event.updated_at = now

    async def run_forever(self) -> None:
        log.info("Reach信用点用量投递Worker已启动")
        while not self._stop_event.is_set():
            try:
                handled = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Reach信用点用量投递失败")
                handled = False
            if not handled:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=settings.REACH_CREDIT_USAGE_POLL_INTERVAL_SECONDS,
                    )
                except TimeoutError:
                    pass


__all__ = [
    "CREDIT_ELIGIBILITY_PATH",
    "CreditAdmissionDenied",
    "CreditMeteringClient",
    "CreditUsageDispatcher",
    "ReachCreditUsageOutboxModel",
    "build_signed_headers",
    "enqueue_connected_call_usage",
    "require_credit_eligible_for_request",
]
