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
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import httpx
from sqlalchemy import BigInteger, DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import or_, select, update

from app.config.setting import settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.livekit_sip import MAX_TOTAL_CALL_DURATION_SECONDS
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.api.v1.ai_call.model import AiCallRecordModel

CREDIT_PRODUCT_CODE = "reach"
CREDIT_OWNER_TYPE = "USER"
CREDIT_SCENARIO_CODE = "reach_outbound_call"
CREDIT_METER_ITEM_CODE = "connected_call_seconds"
CREDIT_ELIGIBILITY_PATH = "/system/credit/external/v1/eligibility"
CREDIT_USAGE_PATH = "/system/credit/external/v1/usage-events"
MINIMUM_START_POINTS = "1"


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
    def __init__(self, status_code: int, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}


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
    admission_json: Mapped[str | None] = mapped_column(Text)
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
        post: Callable[[str, dict[str, object]], Awaitable[dict | list]] | None = None,
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

    async def post(self, path: str, payload: dict[str, object]) -> dict | list:
        # 存量业务数据使用本地分区；平台账务必须归属映射前的平台租户。
        platform_tenant = settings.AI_CALL_PLATFORM_TENANT_ID.strip()
        legacy_tenant = settings.AI_CALL_LEGACY_DATA_TENANT_ID.strip()
        if platform_tenant and legacy_tenant and payload.get("tenantId") == legacy_tenant:
            payload = {**payload, "tenantId": platform_tenant}
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
        if not isinstance(result, dict):
            raise CreditMeteringUnavailable("计费服务返回了无效响应结构")
        if response.is_error or result.get("code", 200) != 200:
            message = str(result.get("msg") or result.get("message") or "计费服务请求失败")
            status = response.status_code if response.is_error else result.get("code")
            if not isinstance(status, int) or status in {408, 429} or status >= 500:
                raise CreditMeteringUnavailable(f"{message}（code {status}）")
            raise CreditMeteringRejected(status, message, result.get("data") if isinstance(result.get("data"), dict) else None)
        data = result.get("data")
        if not isinstance(data, (dict, list)):
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
                # 任务启动至少需要 1 点，平台可配置更高门槛。
                "estimatedPoints": MINIMUM_START_POINTS,
            },
        )
        if not isinstance(result, dict) or not isinstance(result.get("eligible"), bool):
            raise CreditMeteringUnavailable("计费准入检查响应无效")
        if result["eligible"]:
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


async def prepare_outbound_credit_resume(
    session: AsyncSession, *, client: CreditMeteringClient, tenant_id: str, owner_id: str,
) -> None:
    from app.services.ai_call.credit_postpaid import reconcile_postpaid_adjustments

    await require_credit_eligible_for_request(client, tenant_id=tenant_id, owner_id=owner_id)
    await reconcile_postpaid_adjustments(session, client=client, tenant_id=tenant_id, owner_id=owner_id)
    unresolved = await session.scalar(
        select(ReachCreditUsageOutboxModel.status).where(
            ReachCreditUsageOutboxModel.tenant_id == tenant_id,
            ReachCreditUsageOutboxModel.owner_id == owner_id,
            ReachCreditUsageOutboxModel.scenario_code == CREDIT_SCENARIO_CODE,
            or_(
                ReachCreditUsageOutboxModel.status == "adjustment",
                (ReachCreditUsageOutboxModel.status == "sending")
                & ReachCreditUsageOutboxModel.admission_json.is_not(None),
                (ReachCreditUsageOutboxModel.status == "blocked")
                & (ReachCreditUsageOutboxModel.attempt_count > 0),
                (ReachCreditUsageOutboxModel.status == "admission")
                & ReachCreditUsageOutboxModel.last_error.is_not(None),
            ),
        ).limit(1)
    )
    if unresolved is not None:
        raise CustomException(
            msg="已有通话费用待调整，请核实后继续" if unresolved == "adjustment" else
            "已有通话计费回执待核实，请核实后继续",
            status_code=409,
        )
    now = datetime.now(timezone.utc)
    await session.execute(
        update(ReachCreditUsageOutboxModel).where(
            ReachCreditUsageOutboxModel.tenant_id == tenant_id,
            ReachCreditUsageOutboxModel.owner_id == owner_id,
            ReachCreditUsageOutboxModel.scenario_code == CREDIT_SCENARIO_CODE,
            ReachCreditUsageOutboxModel.status == "failed",
        ).values(status="pending", next_attempt_at=now, updated_at=now)
    )


async def enqueue_connected_call_usage(
    session: AsyncSession,
    *,
    tenant_id: str,
    owner_id: str,
    attempt_id: int,
    call_id: str,
    duration_ms: int,
) -> None:
    from app.api.v1.ai_call.model import AiCallRecordModel

    record = await session.scalar(
        select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        .with_for_update().execution_options(populate_existing=True)
    )
    if record is None or record.ended_at is None:
        return
    billing_state = await _call_billing_state(session, call_id, record=record)
    now = datetime.now(timezone.utc)
    duration_ms = int(record.duration_ms or 0)
    quantity = Decimal(min(max(0, duration_ms), MAX_TOTAL_CALL_DURATION_SECONDS * 1000)) / Decimal(1000)
    payload_json = _json_body({
        "attemptId": str(attempt_id), "callId": call_id, "durationMs": duration_ms,
    })
    occurred_at = record.answered_at or record.started_at
    existing = await session.scalar(
        select(ReachCreditUsageOutboxModel).where(
            ReachCreditUsageOutboxModel.tenant_id == tenant_id,
            ReachCreditUsageOutboxModel.idempotency_key == f"reach:call:{attempt_id}",
        ).with_for_update()
    )
    if existing is not None:
        if getattr(existing, "admission_json", None) is not None:
            from app.services.ai_call.credit_postpaid import update_postpaid_billing

            update_postpaid_billing(existing, billing_state, record, now)
            return
        if existing.status == "waived":
            return
        if billing_state.startswith("waived"):
            # 请求超时也可能已经入账，发过的平台用量必须核对原单后调整。
            existing.status = (
                "adjustment" if existing.attempt_count or existing.status in {"sent", "sending", "adjustment"}
                else "waived"
            )
            existing.last_error = (
                "接听依据已变更，原消费待核对调整" if existing.status == "adjustment" else
                "语音信箱免收" if billing_state == "waived_voicemail" else "接听类型无法核实免收"
            )
            existing.next_attempt_at = None
            if existing.status == "waived":
                existing.quantity = Decimal(0)
        elif billing_state == "chargeable" and existing.status == "blocked":
            existing.status = "pending"
            existing.next_attempt_at = now
            existing.last_error = None
            if not existing.attempt_count:
                existing.quantity = quantity
                existing.payload_json = payload_json
                existing.occurred_at = occurred_at
        elif billing_state == "pending_review" and existing.status == "pending":
            existing.status = "blocked"
            existing.last_error = "通话收费依据待核实"
            existing.next_attempt_at = None
        existing.updated_at = now
        return
    billable = billing_state == "chargeable"
    waiver_reason = (
        "语音信箱免收" if billing_state == "waived_voicemail" else
        "接听类型无法核实免收" if billing_state == "waived_unknown" else
        "通话收费依据待核实" if not billable else None
    )
    values = {
        "id": generate_snowflake_id(), "tenant_id": tenant_id, "owner_id": owner_id,
        "product_code": CREDIT_PRODUCT_CODE, "scenario_code": CREDIT_SCENARIO_CODE,
        "meter_item_code": CREDIT_METER_ITEM_CODE,
        "quantity": (
            quantity if billable else Decimal(0)
        ),
        "source_id": call_id, "idempotency_key": f"reach:call:{attempt_id}",
        "occurred_at": occurred_at,
        "payload_json": payload_json,
        "status": "pending" if billable else "blocked" if billing_state == "pending_review" else "waived",
        "next_attempt_at": now if billable else None,
        "last_error": waiver_reason,
        "created_at": now, "updated_at": now,
    }
    table = ReachCreditUsageOutboxModel.__table__
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = postgresql_insert(table).values(**values).on_conflict_do_nothing(
            index_elements=[table.c.tenant_id, table.c.idempotency_key]
        )
    elif dialect == "sqlite":
        statement = sqlite_insert(table).values(**values).on_conflict_do_nothing(
            index_elements=[table.c.tenant_id, table.c.idempotency_key]
        )
    elif dialect in {"mysql", "mariadb"}:
        statement = mysql_insert(table).values(**values).on_duplicate_key_update(id=table.c.id)
    else:
        raise CreditMeteringUnavailable(f"不支持的用量数据库类型: {dialect}")
    await session.execute(statement)


async def _call_billing_state(
    session: AsyncSession, call_id: str, *, record: AiCallRecordModel | None = None,
) -> str:
    from app.api.v1.ai_call.model import (
        AiCallEventModel,
        AiCallRecordModel,
        AiCallSemanticAnalysisModel,
    )
    from app.services.ai_call.call_outcome import realtime_answer_type

    if record is None:
        record = await session.scalar(
            select(AiCallRecordModel).where(AiCallRecordModel.call_id == call_id)
        )
    if record is None or record.ended_at is None:
        return "pending_review"
    if record.end_reason == "voicemail_detected":
        return "waived_voicemail"
    analysis = await session.scalar(
        select(AiCallSemanticAnalysisModel).where(
            AiCallSemanticAnalysisModel.call_id == call_id,
            AiCallSemanticAnalysisModel.analysis_scene_code == "ai_call_semantic_analysis",
        )
    )
    if analysis is None or analysis.analysis_status != "2":
        # 接听依据已在实时对话中核验时，挂断后结算无需等待销售摘要和意向分析。
        if record.answered_at is not None and record.duration_ms and record.duration_ms > 0:
            evidence = await session.scalar(select(AiCallEventModel).where(
                AiCallEventModel.call_id == call_id,
                AiCallEventModel.event_type == "call_answer_verified",
                AiCallEventModel.source == "agent",
                AiCallEventModel.event_time >= record.answered_at,
                AiCallEventModel.event_time <= record.ended_at,
            ).order_by(AiCallEventModel.id.desc()).limit(1))
            if evidence is not None and realtime_answer_type(evidence.payload) == "human":
                return "chargeable"
        if analysis is None:
            return "pending_review"
    if analysis.analysis_status == "4":
        return "waived_unknown"
    if analysis.analysis_status != "2":
        return "pending_review"
    result = analysis.analysis_result_dict or {}
    answer_type = result.get("answer_type")
    # 接听类型已核验引用的客户轮次；销售分类冲突不能否定独立的可靠接听证据。
    if result.get("evidence_conflict") is True and answer_type not in {"human", "voicemail"}:
        return "pending_review"
    if answer_type == "voicemail":
        return "waived_voicemail"
    if answer_type == "human" and (record.answered_at is None or not record.duration_ms or record.duration_ms <= 0):
        return "pending_review"
    if answer_type == "human":
        return "chargeable"
    return "waived_unknown" if answer_type == "unknown" else "pending_review"


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
                                (ReachCreditUsageOutboxModel.status.in_({"pending", "admission"}))
                                & (ReachCreditUsageOutboxModel.next_attempt_at <= now),
                                (ReachCreditUsageOutboxModel.status == "sending")
                                & (ReachCreditUsageOutboxModel.lease_expires_at <= now),
                                (ReachCreditUsageOutboxModel.status == "adjustment")
                                & ReachCreditUsageOutboxModel.admission_json.is_not(None)
                                & or_(
                                    ReachCreditUsageOutboxModel.next_attempt_at.is_(None),
                                    ReachCreditUsageOutboxModel.next_attempt_at <= now,
                                ),
                            )
                        )
                        .order_by(ReachCreditUsageOutboxModel.created_at)
                        .with_for_update(skip_locked=True)
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if event is None:
                    return False
                postpaid = getattr(event, "admission_json", None) is not None
                email_event = event.scenario_code == "reach_email_send" and postpaid
                email_refund = email_event and event.status == "adjustment"
                if email_event:
                    from app.services.reach_email.credit import (
                        freeze_email_refund,
                        prepare_email_completion,
                    )
                    if email_refund:
                        if not json.loads(event.admission_json).get("refundRequested"):
                            event.status, event.next_attempt_at = "blocked", None
                            event.last_error = "邮件发送结果与原收费不一致，需核对原单"
                            return True
                        freeze_email_refund(event)
                    else:
                        if not await prepare_email_completion(session, event, now):
                            return True
                        from app.services.ai_call.credit_postpaid import freeze_postpaid_completion
                        freeze_postpaid_completion(event)
                elif postpaid:
                    from app.services.ai_call.credit_postpaid import (
                        freeze_postpaid_completion,
                        prepare_postpaid_completion,
                    )

                    admission = json.loads(event.admission_json)
                    if event.status == "adjustment":
                        admission["adjustmentQueryOnly"] = True
                        event.admission_json = _json_body(admission)
                    # 查询用途先落库，持有租约时重启也不能再次提交收费或免收。
                    if not admission.get("adjustmentQueryOnly"):
                        if not await prepare_postpaid_completion(session, event, now):
                            return True
                        freeze_postpaid_completion(event)
                elif event.scenario_code == CREDIT_SCENARIO_CODE:
                    billing_state = await _call_billing_state(session, event.source_id)
                    if billing_state != "chargeable":
                        event.status = (
                            "adjustment" if billing_state.startswith("waived") and event.attempt_count else
                            "waived" if billing_state.startswith("waived") else "blocked"
                        )
                        event.lease_owner = None
                        event.lease_expires_at = None
                        event.last_error = (
                            "接听依据已变更，原消费待核对调整" if event.status == "adjustment" else
                            "语音信箱免收" if billing_state == "waived_voicemail" else
                            "接听类型无法核实免收" if billing_state == "waived_unknown" else
                            "通话收费依据待核实"
                        )
                        if event.status == "waived":
                            event.quantity = Decimal(0)
                        event.next_attempt_at = None
                        event.updated_at = now
                        return True
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
                    "occurredAt": round(
                        event.occurred_at.replace(tzinfo=timezone.utc).timestamp() * 1000
                        if event.occurred_at.tzinfo is None else event.occurred_at.timestamp() * 1000
                    ),
                    "payload": json.loads(event.payload_json or "{}"),
                }
                admission_json = event.admission_json if postpaid else None
        if email_refund:
            from app.services.reach_email.credit import dispatch_email_refund

            try:
                status, admission_json = await dispatch_email_refund(self.client, payload, admission_json)
                error = None
            except (CreditMeteringUnavailable, CreditMeteringRejected) as exc:
                status, error = "adjustment", str(exc)
            await self._finish(event_id, status, error, retry=status == "adjustment",
                               admission_json=admission_json)
            return True
        if postpaid:
            from app.services.ai_call.credit_postpaid import dispatch_postpaid_completion

            status, error, retry, admission_json = await dispatch_postpaid_completion(
                self.client, payload, admission_json,
            )
            await self._finish(event_id, status, error, retry=retry, admission_json=admission_json)
            return True
        try:
            receipt = await self.client.post(CREDIT_USAGE_PATH, payload)
        except CreditMeteringRejected as exc:
            await self._finish(event_id, "failed", str(exc), retry=False)
        except Exception as exc:
            await self._finish(event_id, "pending", str(exc), retry=True)
        else:
            if not isinstance(receipt, dict):
                await self._finish(event_id, "pending", "平台用量回执结构无效", retry=True)
                return True
            receipt_status = str(receipt.get("status") or "").upper()
            if receipt_status in {"SETTLED", "PARTIAL_OUTSTANDING", "OUTSTANDING"}:
                await self._finish(event_id, "sent", None, retry=False)
            elif receipt_status == "FAILED":
                await self._finish(
                    event_id, "failed", str(receipt.get("errorMessage") or "平台用量结算失败"), retry=False,
                )
            else:
                await self._finish(event_id, "pending", "平台结算结果待核实", retry=True)
        return True

    async def _finish(
        self, event_id: int, status: str, error: str | None, *, retry: bool,
        admission_json: str | None = None,
    ) -> None:
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
                immediate_refund = False
                if admission_json is not None:
                    completed = json.loads(admission_json)
                    current = json.loads(event.admission_json)
                    if current.get("billingState") is not None:
                        completed["billingState"] = current["billingState"]
                    if event.scenario_code == "reach_email_send":
                        if status == "sent":
                            try:
                                billed = Decimal(str(completed["receipt"]["billedPoints"]))
                            except (KeyError, TypeError, ValueError, InvalidOperation):
                                billed = None
                            if billed != 1:
                                event.status, retry = "blocked", False
                                error = "邮件原单应收金额不是 1 点，需核对平台计价"
                        for key in ("refundRequested", "refundRequest"):
                            if key in current:
                                completed[key] = current[key]
                        if event.status == "sent" and completed.get("refundRequested"):
                            event.status, retry = "adjustment", True
                            immediate_refund = True
                    else:
                        from app.services.ai_call.credit_postpaid import _terminal_status

                        receipt = completed.get("receipt")
                        if receipt is not None and status in {"sent", "waived"}:
                            event.status, error = _terminal_status(receipt, completed)
                    if event.status == "adjustment":
                        completed["adjustmentQueryOnly"] = True
                        retry = True
                    event.admission_json = _json_body(completed)
                event.lease_owner = None
                event.lease_expires_at = None
                event.next_attempt_at = (
                    now if immediate_refund else
                    now + timedelta(seconds=min(300, 2 ** min(event.attempt_count, 8)))
                    if retry
                    else None
                )
                event.sent_at = now if event.status == "sent" else event.sent_at
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
    "prepare_outbound_credit_resume",
    "require_credit_eligible_for_request",
]
