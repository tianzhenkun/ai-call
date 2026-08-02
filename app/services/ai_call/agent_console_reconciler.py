from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import status
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.model import (
    AiCallAfterCallWorkModel,
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallFollowUpAttemptModel,
    AiCallFollowUpTaskModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.common.constant import RET
from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.agent_console_service import AiCallAgentConsoleService
from app.services.ai_call.exceptions import AiCallError
from app.services.ai_call.follow_up_service import AiCallFollowUpService

RoomExistsCallable = Callable[[str], bool | Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class AgentConsoleEvent:
    sequence: int
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime


class AgentConsoleEventBroker:
    """进程内轻量推送；断线或进程重启后由 bootstrap 恢复数据库事实。"""

    def __init__(self, *, history_size: int = 500) -> None:
        self.history_size = max(1, history_size)
        self._sequence = 0
        self._events: dict[str, deque[AgentConsoleEvent]] = defaultdict(
            lambda: deque(maxlen=self.history_size)
        )
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition()

    async def publish(
        self,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> AgentConsoleEvent:
        async with self._lock:
            self._sequence += 1
            event = AgentConsoleEvent(
                sequence=self._sequence,
                event_type=event_type,
                payload=dict(payload),
                occurred_at=datetime.now(timezone.utc),
            )
            self._events[tenant_id].append(event)
        async with self._condition:
            self._condition.notify_all()
        return event

    def latest_sequence(self, tenant_id: str) -> int:
        events = self._events.get(tenant_id)
        return events[-1].sequence if events else 0

    def events_after(self, tenant_id: str, sequence: int) -> list[AgentConsoleEvent]:
        return [event for event in self._events.get(tenant_id, ()) if event.sequence > sequence]

    async def wait_for_events(
        self,
        tenant_id: str,
        sequence: int,
        *,
        timeout_seconds: float = 15.0,
    ) -> list[AgentConsoleEvent]:
        available = self.events_after(tenant_id, sequence)
        if available:
            return available
        try:
            async with self._condition:
                available = self.events_after(tenant_id, sequence)
                if available:
                    return available
                await asyncio.wait_for(
                    self._condition.wait(),
                    timeout=max(0.1, timeout_seconds),
                )
        except TimeoutError:
            return []
        return self.events_after(tenant_id, sequence)


@dataclass(frozen=True, slots=True)
class AgentConsoleStreamLease:
    tenant_id: str
    agent_identity: str
    replaced: asyncio.Event


class AgentConsoleStreamRegistry:
    """同一进程内每个坐席只保留最新的 SSE 连接。"""

    def __init__(self) -> None:
        self._current: dict[tuple[str, str], AgentConsoleStreamLease] = {}

    def replace(self, tenant_id: str, agent_identity: str) -> AgentConsoleStreamLease:
        key = (tenant_id, agent_identity)
        previous = self._current.get(key)
        if previous is not None:
            previous.replaced.set()
        lease = AgentConsoleStreamLease(
            tenant_id=tenant_id,
            agent_identity=agent_identity,
            replaced=asyncio.Event(),
        )
        self._current[key] = lease
        return lease

    def is_current(self, lease: AgentConsoleStreamLease) -> bool:
        return self._current.get((lease.tenant_id, lease.agent_identity)) is lease

    def release(self, lease: AgentConsoleStreamLease) -> None:
        key = (lease.tenant_id, lease.agent_identity)
        if self._current.get(key) is lease:
            self._current.pop(key, None)


agent_console_event_broker = AgentConsoleEventBroker()
agent_console_stream_registry = AgentConsoleStreamRegistry()


async def publish_agent_console_event(
    tenant_id: str,
    event_type: str,
    payload: dict[str, Any],
    *,
    broker: AgentConsoleEventBroker = agent_console_event_broker,
) -> AgentConsoleEvent | None:
    try:
        return await broker.publish(tenant_id, event_type, payload)
    except Exception as exc:
        log.warning(
            "AI Call 坐席中心推送失败，客户端将由 bootstrap/轮询补偿: "
            f"eventType={event_type}, message={exc!s}"
        )
        return None


class AiCallAgentConsoleReconciler:
    """坐席中心管理查询和只收敛异常状态的补偿操作。"""

    def __init__(
        self,
        db: AsyncSession,
        *,
        room_exists: RoomExistsCallable | None = None,
    ) -> None:
        self.db = db
        self.repository = AiCallRecordRepository(db)
        self.agent_service = AiCallAgentConsoleService(db)
        self.follow_up_service = AiCallFollowUpService(db)
        self.room_exists = room_exists

    async def list_agents(self, auth: AuthSchema) -> dict:
        _, tenant_id = self._identity(auth)
        profiles = list(
            (
                await self.db.execute(
                    select(AiCallAgentProfileModel)
                    .where(AiCallAgentProfileModel.tenant_id == tenant_id)
                    .order_by(AiCallAgentProfileModel.id)
                )
            )
            .scalars()
            .all()
        )
        presences = {
            row.agent_identity: row
            for row in (
                (
                    await self.db.execute(
                        select(AiCallHandoffAgentModel).where(
                            AiCallHandoffAgentModel.tenant_id == tenant_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        }
        scopes = await self._scene_scope_map(tenant_id)
        users = await self._user_map(tenant_id, [profile.user_id for profile in profiles])
        rows = [
            self._agent_payload(
                profile,
                presence=presences.get(profile.agent_identity),
                scene_codes=scopes.get(profile.agent_identity, []),
                user=users.get(profile.user_id),
            )
            for profile in profiles
        ]
        for row in rows:
            if not row["active_handoff_id"] and not row["active_call_id"]:
                continue
            handoff = await self._handoff_by_id(tenant_id, row["active_handoff_id"])
            record = (
                await self.repository.get_record(row["active_call_id"])
                if row["active_call_id"]
                else None
            )
            row["stale_occupied"] = bool(
                row["stale_occupied"]
                or (
                    handoff is not None
                    and handoff.status in {"completed", "expired", "canceled", "failed"}
                )
                or (record is not None and record.status in {"completed", "failed"})
            )
        return {
            "metrics": {
                "enabled": sum(row["enabled"] for row in rows),
                "online": sum(bool(row["online"]) for row in rows),
                "available": sum(row["status"] == "available" for row in rows),
                "in_call": sum(row["status"] == "in_call" for row in rows),
                "stale_occupied": sum(bool(row["stale_occupied"]) for row in rows),
            },
            "rows": rows,
        }

    async def get_agent_status(self, auth: AuthSchema, agent_id: int) -> dict:
        _, tenant_id = self._identity(auth)
        profile = await self._required_profile(tenant_id, agent_id)
        presence = await self._presence(tenant_id, profile.agent_identity)
        scopes = await self._scene_scope_map(tenant_id)
        users = await self._user_map(tenant_id, [profile.user_id])
        return self._agent_payload(
            profile,
            presence=presence,
            scene_codes=scopes.get(profile.agent_identity, []),
            user=users.get(profile.user_id),
        )

    async def release_stale_agent(
        self,
        auth: AuthSchema,
        *,
        agent_id: int,
        confirmed: bool,
        reason: str | None,
    ) -> dict:
        self._require_confirmation(confirmed)
        user, tenant_id = self._identity(auth)
        profile = await self._required_profile(tenant_id, agent_id)
        scene_codes = (await self._scene_scope_map(tenant_id)).get(profile.agent_identity, [])
        user_row = (await self._user_map(tenant_id, [profile.user_id])).get(profile.user_id)
        presence = await self._presence(tenant_id, profile.agent_identity)
        if presence is None:
            return self._agent_payload(
                profile,
                presence=None,
                scene_codes=scene_codes,
                user=user_row,
            )
        has_active_reference = bool(presence.active_call_id or presence.active_handoff_id)
        orphan_occupied = (
            presence.status in {"claiming", "in_call", "reconnecting", "wrap_up_quick"}
            and not has_active_reference
        )
        if not has_active_reference and not orphan_occupied:
            return self._agent_payload(
                profile,
                presence=presence,
                scene_codes=scene_codes,
                user=user_row,
            )

        call_id = presence.active_call_id
        handoff = await self._handoff_by_id(tenant_id, presence.active_handoff_id)
        if call_id is None and handoff is not None:
            call_id = handoff.call_id
        record = await self.repository.get_record(call_id) if call_id else None
        terminal_call = record is not None and record.status in {"completed", "failed"}
        room_name = (
            handoff.room_name if handoff is not None else record.room_name if record else None
        )
        if (
            has_active_reference
            and not terminal_call
            and (not room_name or await self._is_room_active(room_name))
        ):
            self._raise_conflict(
                "活动 Room 或通话仍存在，禁止强制释放",
                "STALE_RELEASE_NOT_ALLOWED",
            )

        previous = {
            "status": presence.status,
            "active_handoff_id": presence.active_handoff_id,
            "active_call_id": presence.active_call_id,
        }
        presence.status = "offline"
        presence.active_handoff_id = None
        presence.active_call_id = None
        presence.console_session_id = None
        presence.status_updated_at = datetime.now(timezone.utc)
        audit_call_id = call_id or f"admin-agent-{agent_id}"
        await self._append_audit_event(
            call_id=audit_call_id,
            event_type="agent_stale_released",
            entity_key=f"agent:{agent_id}:{audit_call_id}",
            payload={
                "operator_user_id": str(user.id),
                "agent_id": str(agent_id),
                "agent_identity": profile.agent_identity,
                "reason": (reason or "").strip() or None,
                "previous": previous,
            },
        )
        await self.db.flush()
        return self._agent_payload(
            profile,
            presence=presence,
            scene_codes=scene_codes,
            user=user_row,
        )

    async def list_handoffs(self, auth: AuthSchema) -> dict:
        _, tenant_id = self._identity(auth)
        handoffs = list(
            (
                await self.db.execute(
                    select(AiCallHandoffModel)
                    .where(AiCallHandoffModel.tenant_id == tenant_id)
                    .order_by(AiCallHandoffModel.requested_at.desc())
                )
            )
            .scalars()
            .all()
        )
        records = {
            call_id: await self.repository.get_record(call_id)
            for call_id in {row.call_id for row in handoffs}
        }
        follow_up_handoff_ids = (
            set(
                (
                    await self.db.execute(
                        select(AiCallFollowUpTaskModel.source_handoff_id).where(
                            AiCallFollowUpTaskModel.tenant_id == tenant_id,
                            AiCallFollowUpTaskModel.source_handoff_id.in_([
                                row.handoff_id for row in handoffs
                            ]),
                        )
                    )
                ).scalars()
            )
            if handoffs
            else set()
        )
        rows = []
        for handoff in handoffs:
            row = self._handoff_payload(handoff)
            record = records.get(handoff.call_id)
            row.update({
                "masked_contact": record.callee_phone_number_masked if record else None,
                "business_type": record.business_type if record else None,
                "business_id": record.business_id if record else None,
                "has_unanswered_follow_up": handoff.handoff_id in follow_up_handoff_ids,
            })
            rows.append(row)
        connected = [row for row in handoffs if row.connected_at is not None]
        waits = [
            max(0.0, (self._utc(row.connected_at) - self._utc(row.requested_at)).total_seconds())
            for row in connected
        ]
        connected_within_60 = sum(wait <= 60 for wait in waits)
        total = len(handoffs)
        return {
            "metrics": {
                "request_count": total,
                "connected_rate_within_60_seconds": (
                    round(connected_within_60 / total, 4) if total else 0.0
                ),
                "average_wait_seconds": round(sum(waits) / len(waits), 2) if waits else 0.0,
                "timeout_count": sum(
                    row.status == "expired" or row.end_reason == "handoff_unanswered"
                    for row in handoffs
                ),
                "media_failure_count": sum(
                    row.failure_stage in {"media", "media_ready", "claim_timeout"}
                    or row.end_reason in {"claim_timeout", "reconnect_timeout"}
                    for row in handoffs
                ),
            },
            "rows": rows,
        }

    async def get_handoff_detail(self, auth: AuthSchema, handoff_id: str) -> dict:
        _, tenant_id = self._identity(auth)
        handoff = await self._handoff_by_id(tenant_id, handoff_id)
        if handoff is None:
            raise CustomException(msg="转人工记录不存在", status_code=404)
        record = await self.repository.get_record(handoff.call_id)
        acw = await self._after_call_work(tenant_id, handoff_id)
        follow_up = await self._follow_up_by_handoff(tenant_id, handoff_id)
        return {
            "handoff": self._handoff_payload(handoff),
            "record": self._record_payload(record),
            "after_call_work": (
                self.follow_up_service.after_call_work_payload(acw) if acw else None
            ),
            "follow_up": (
                self.follow_up_service.follow_up_payload(follow_up) if follow_up else None
            ),
        }

    async def reconcile_handoff(
        self,
        auth: AuthSchema,
        *,
        handoff_id: str,
        confirmed: bool,
        reason: str | None,
        now: datetime | None = None,
    ) -> dict:
        self._require_confirmation(confirmed)
        user, tenant_id = self._identity(auth)
        handoff = await self._handoff_by_id(tenant_id, handoff_id)
        if handoff is None:
            raise CustomException(msg="转人工记录不存在", status_code=404)
        current = now or datetime.now(timezone.utc)
        if handoff.status in {"connected", "reconnecting"} and not await self._is_room_active(
            handoff.room_name
        ):
            handoff.status = "failed"
            handoff.ended_at = handoff.ended_at or current
            handoff.end_reason = handoff.end_reason or "room_missing"
            handoff.failure_stage = handoff.failure_stage or "livekit_room"
            presence = await self._presence(tenant_id, handoff.human_agent_identity or "")
            if presence is not None and presence.active_handoff_id == handoff.handoff_id:
                presence.status = "wrap_up_quick"
                presence.status_updated_at = current
            record = await self.repository.get_record(handoff.call_id)
            if record is not None and record.status not in {"completed", "failed"}:
                record.status = "completed"
                record.ended_at = record.ended_at or current
                record.end_reason = record.end_reason or "room_missing"
            await publish_agent_console_event(
                handoff.tenant_id,
                "handoff.changed",
                {
                    "handoff_id": handoff.handoff_id,
                    "call_id": handoff.call_id,
                    "status": handoff.status,
                },
            )
        else:
            handoff = await self.agent_service.reconcile_handoff_timeout(
                tenant_id,
                handoff_id,
                now=current,
            )
            if handoff is None:
                raise CustomException(msg="转人工记录不存在", status_code=404)
        await self._append_audit_event(
            call_id=handoff.call_id,
            event_type="handoff_reconciled",
            entity_key=f"handoff:{handoff_id}",
            payload={
                "operator_user_id": str(user.id),
                "handoff_id": handoff_id,
                "result_status": handoff.status,
                "reason": (reason or "").strip() or None,
            },
        )
        await self.db.flush()
        return self._handoff_payload(handoff)

    async def list_follow_ups(
        self,
        auth: AuthSchema,
        *,
        status: str | None = None,
        formal_outbound_only: bool = False,
        source_started_at_begin: datetime | None = None,
        source_started_at_end: datetime | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> dict:
        _, tenant_id = self._identity(auth)
        normalized_status = (status or "").strip() or None
        if normalized_status not in {
            None,
            "pending",
            "processing",
            "completed",
            "closed",
        }:
            raise CustomException(msg="跟进状态不合法", status_code=400)
        if (
            source_started_at_begin is not None
            and source_started_at_end is not None
            and source_started_at_begin >= source_started_at_end
        ):
            raise CustomException(msg="来源通话开始时间必须早于结束时间", status_code=400)

        filtered_stmt = select(AiCallFollowUpTaskModel).where(
            AiCallFollowUpTaskModel.tenant_id == tenant_id
        )
        formal_source_scope = None
        if normalized_status:
            filtered_stmt = filtered_stmt.where(AiCallFollowUpTaskModel.status == normalized_status)
        if (
            formal_outbound_only
            or source_started_at_begin is not None
            or source_started_at_end is not None
        ):
            record_conditions = [
                or_(
                    AiCallRecordModel.follow_up_id == AiCallFollowUpTaskModel.id,
                    AiCallRecordModel.call_id == AiCallFollowUpTaskModel.source_call_id,
                )
            ]
            if source_started_at_begin is not None:
                record_conditions.append(AiCallRecordModel.started_at >= source_started_at_begin)
            if source_started_at_end is not None:
                record_conditions.append(AiCallRecordModel.started_at < source_started_at_end)
            record_stmt = select(AiCallRecordModel.id)
            if formal_outbound_only:
                record_stmt = (
                    record_stmt
                    .join(
                        AiCallOutboundAttemptModel,
                        AiCallOutboundAttemptModel.call_id == AiCallRecordModel.call_id,
                    )
                    .join(
                        AiCallOutboundTargetModel,
                        and_(
                            AiCallOutboundTargetModel.tenant_id
                            == AiCallOutboundAttemptModel.tenant_id,
                            AiCallOutboundTargetModel.id
                            == AiCallOutboundAttemptModel.target_id,
                            AiCallOutboundTargetModel.task_id
                            == AiCallOutboundAttemptModel.task_id,
                        ),
                    )
                    .join(
                        AiCallOutboundTaskModel,
                        and_(
                            AiCallOutboundTaskModel.tenant_id
                            == AiCallOutboundAttemptModel.tenant_id,
                            AiCallOutboundTaskModel.id
                            == AiCallOutboundAttemptModel.task_id,
                        ),
                    )
                )
                record_conditions.extend([
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallRecordModel.entry_type == "sip_outbound",
                ])
            source_scope = record_stmt.where(*record_conditions).exists()
            filtered_stmt = filtered_stmt.where(source_scope)
            if formal_outbound_only:
                formal_source_scope = source_scope

        total = int(
            (
                await self.db.execute(
                    select(func.count()).select_from(filtered_stmt.order_by(None).subquery())
                )
            ).scalar_one()
        )
        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 100))
        tasks = list(
            (
                await self.db.execute(
                    filtered_stmt
                    .order_by(
                        AiCallFollowUpTaskModel.updated_at.desc(),
                        AiCallFollowUpTaskModel.id.desc(),
                    )
                    .offset((safe_page_num - 1) * safe_page_size)
                    .limit(safe_page_size)
                )
            )
            .scalars()
            .all()
        )
        attempts = await self._latest_attempt_map(
            tenant_id,
            [task.id for task in tasks],
        )
        now = datetime.now(timezone.utc)
        rows = []
        for task in tasks:
            row = self.follow_up_service.follow_up_payload(task)
            latest = attempts.get(task.id)
            row["latest_attempt"] = (
                self.follow_up_service.attempt_payload(latest) if latest else None
            )
            rows.append(row)
        metrics_stmt = select(
                    func.coalesce(
                        func.sum(case((AiCallFollowUpTaskModel.status == "pending", 1), else_=0)),
                        0,
                    ),
                    func.coalesce(
                        func.sum(
                            case(
                                (
                                    AiCallFollowUpTaskModel.status.in_({"pending", "processing"})
                                    & AiCallFollowUpTaskModel.customer_callback_at.is_not(None)
                                    & (AiCallFollowUpTaskModel.customer_callback_at >= now),
                                    1,
                                ),
                                else_=0,
                            )
                        ),
                        0,
                    ),
                    func.coalesce(
                        func.sum(
                            case(
                                (
                                    AiCallFollowUpTaskModel.status.in_({"pending", "processing"})
                                    & AiCallFollowUpTaskModel.customer_callback_at.is_not(None)
                                    & (AiCallFollowUpTaskModel.customer_callback_at < now),
                                    1,
                                ),
                                else_=0,
                            )
                        ),
                        0,
                    ),
                    func.coalesce(
                        func.sum(
                            case(
                                (
                                    AiCallFollowUpTaskModel.source_type == "handoff_unanswered",
                                    1,
                                ),
                                else_=0,
                            )
                        ),
                        0,
                    ),
                    func.coalesce(
                        func.sum(
                            case(
                                (AiCallFollowUpTaskModel.status == "completed", 1),
                                else_=0,
                            )
                        ),
                        0,
                    ),
                    func.coalesce(
                        func.sum(case((AiCallFollowUpTaskModel.status == "closed", 1), else_=0)),
                        0,
                    ),
                ).where(AiCallFollowUpTaskModel.tenant_id == tenant_id)
        if formal_source_scope is not None:
            metrics_stmt = metrics_stmt.where(formal_source_scope)
        metrics_row = (await self.db.execute(metrics_stmt)).one()
        return {
            "metrics": {
                "pending": int(metrics_row[0]),
                "scheduled": int(metrics_row[1]),
                "overdue": int(metrics_row[2]),
                "handoff_unanswered": int(metrics_row[3]),
                "completed": int(metrics_row[4]),
                "closed": int(metrics_row[5]),
            },
            "rows": rows,
            "total": total,
        }

    async def get_follow_up_detail(self, auth: AuthSchema, follow_up_id: int) -> dict:
        _, tenant_id = self._identity(auth)
        task = await self._follow_up_by_id(tenant_id, follow_up_id)
        if task is None:
            raise CustomException(msg="跟进任务不存在", status_code=404)
        attempts = list(
            (
                await self.db.execute(
                    select(AiCallFollowUpAttemptModel)
                    .where(
                        AiCallFollowUpAttemptModel.tenant_id == tenant_id,
                        AiCallFollowUpAttemptModel.follow_up_id == follow_up_id,
                    )
                    .order_by(AiCallFollowUpAttemptModel.contacted_at)
                )
            )
            .scalars()
            .all()
        )
        callback_records = list(
            (
                await self.db.execute(
                    select(AiCallRecordModel)
                    .where(AiCallRecordModel.follow_up_id == follow_up_id)
                    .order_by(AiCallRecordModel.started_at)
                )
            )
            .scalars()
            .all()
        )
        return {
            "task": self.follow_up_service.follow_up_payload(task),
            "attempts": [self.follow_up_service.attempt_payload(attempt) for attempt in attempts],
            "callback_records": [self._record_payload(record) for record in callback_records],
        }

    async def _append_audit_event(
        self,
        *,
        call_id: str,
        event_type: str,
        entity_key: str,
        payload: dict[str, Any],
    ) -> None:
        digest = hashlib.sha256(f"{event_type}:{entity_key}".encode()).hexdigest()[:24]
        await self.repository.append_event(
            event_id=f"audit_{event_type[:20]}_{digest}",
            call_id=call_id,
            event_type=event_type,
            source="agent_admin",
            event_time=datetime.now(timezone.utc),
            payload_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        )

    async def _is_room_active(self, room_name: str) -> bool:
        if self.room_exists is None:
            raise CustomException(msg="LiveKit Room 核验器未配置", status_code=503)
        try:
            result = self.room_exists(room_name)
            if inspect.isawaitable(result):
                result = await result
        except AiCallError as exc:
            raise CustomException(
                msg=exc.msg,
                status_code=exc.status_code,
                data={"errorCode": exc.error_id},
            ) from exc
        return bool(result)

    async def _scene_scope_map(self, tenant_id: str) -> dict[str, list[str]]:
        scopes = list(
            (
                await self.db.execute(
                    select(AiCallAgentSceneScopeModel).where(
                        AiCallAgentSceneScopeModel.tenant_id == tenant_id
                    )
                )
            )
            .scalars()
            .all()
        )
        result: dict[str, list[str]] = defaultdict(list)
        for scope in scopes:
            result[scope.agent_identity].append(scope.scene_code)
        return result

    async def _user_map(self, tenant_id: str, user_ids: list[int]) -> dict[int, UserModel]:
        if not user_ids:
            return {}
        rows = list(
            (
                await self.db.execute(
                    select(UserModel).where(
                        UserModel.tenant_id == tenant_id,
                        UserModel.user_id.in_(user_ids),
                    )
                )
            )
            .scalars()
            .all()
        )
        return {row.user_id: row for row in rows}

    async def _required_profile(
        self,
        tenant_id: str,
        agent_id: int,
    ) -> AiCallAgentProfileModel:
        result = await self.db.execute(
            select(AiCallAgentProfileModel).where(
                AiCallAgentProfileModel.tenant_id == tenant_id,
                AiCallAgentProfileModel.id == agent_id,
            )
        )
        profile = result.scalar_one_or_none()
        if profile is None:
            raise CustomException(msg="坐席档案不存在", status_code=404)
        return profile

    async def _presence(
        self,
        tenant_id: str,
        agent_identity: str,
    ) -> AiCallHandoffAgentModel | None:
        result = await self.db.execute(
            select(AiCallHandoffAgentModel).where(
                AiCallHandoffAgentModel.tenant_id == tenant_id,
                AiCallHandoffAgentModel.agent_identity == agent_identity,
            )
        )
        return result.scalar_one_or_none()

    async def _handoff_by_id(
        self,
        tenant_id: str,
        handoff_id: str | None,
    ) -> AiCallHandoffModel | None:
        if not handoff_id:
            return None
        result = await self.db.execute(
            select(AiCallHandoffModel).where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.handoff_id == handoff_id,
            )
        )
        return result.scalar_one_or_none()

    async def _after_call_work(
        self,
        tenant_id: str,
        handoff_id: str,
    ) -> AiCallAfterCallWorkModel | None:
        result = await self.db.execute(
            select(AiCallAfterCallWorkModel).where(
                AiCallAfterCallWorkModel.tenant_id == tenant_id,
                AiCallAfterCallWorkModel.handoff_id == handoff_id,
            )
        )
        return result.scalar_one_or_none()

    async def _follow_up_by_handoff(
        self,
        tenant_id: str,
        handoff_id: str,
    ) -> AiCallFollowUpTaskModel | None:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.source_handoff_id == handoff_id,
            )
        )
        return result.scalar_one_or_none()

    async def _follow_up_by_id(
        self,
        tenant_id: str,
        follow_up_id: int,
    ) -> AiCallFollowUpTaskModel | None:
        result = await self.db.execute(
            select(AiCallFollowUpTaskModel).where(
                AiCallFollowUpTaskModel.tenant_id == tenant_id,
                AiCallFollowUpTaskModel.id == follow_up_id,
            )
        )
        return result.scalar_one_or_none()

    async def _latest_attempt_map(
        self,
        tenant_id: str,
        follow_up_ids: list[int],
    ) -> dict[int, AiCallFollowUpAttemptModel]:
        if not follow_up_ids:
            return {}
        attempts = list(
            (
                await self.db.execute(
                    select(AiCallFollowUpAttemptModel)
                    .where(
                        AiCallFollowUpAttemptModel.tenant_id == tenant_id,
                        AiCallFollowUpAttemptModel.follow_up_id.in_(follow_up_ids),
                    )
                    .order_by(AiCallFollowUpAttemptModel.contacted_at)
                )
            )
            .scalars()
            .all()
        )
        return {attempt.follow_up_id: attempt for attempt in attempts}

    @classmethod
    def _agent_payload(
        cls,
        profile: AiCallAgentProfileModel,
        *,
        presence: AiCallHandoffAgentModel | None,
        scene_codes: list[str],
        user: UserModel | None,
    ) -> dict:
        status_value = presence.status if presence is not None else "offline"
        active_reference = bool(
            presence is not None and (presence.active_handoff_id or presence.active_call_id)
        )
        orphan_occupied = bool(
            presence is not None
            and presence.status in {"claiming", "in_call", "reconnecting", "wrap_up_quick"}
            and not active_reference
        )
        stale_heartbeat = bool(
            active_reference
            and presence is not None
            and presence.last_seen_at is not None
            and (datetime.now(timezone.utc) - cls._utc(presence.last_seen_at)).total_seconds() > 30
        )
        stale = orphan_occupied or stale_heartbeat
        online = bool(
            presence is not None
            and presence.status != "offline"
            and presence.last_seen_at is not None
            and (datetime.now(timezone.utc) - cls._utc(presence.last_seen_at)).total_seconds()
            <= 30
        )
        return {
            "id": str(profile.id),
            "user_id": str(profile.user_id),
            "user_name": user.user_name if user else None,
            "nick_name": user.nick_name if user else None,
            "agent_identity": profile.agent_identity,
            "enabled": profile.enabled,
            "scene_codes": scene_codes,
            "status": status_value,
            "online": online,
            "active_handoff_id": presence.active_handoff_id if presence else None,
            "active_call_id": presence.active_call_id if presence else None,
            "last_seen_at": cls._api_datetime(presence.last_seen_at if presence else None),
            "status_updated_at": cls._api_datetime(
                presence.status_updated_at if presence else None
            ),
            "stale_occupied": stale,
        }

    @classmethod
    def _handoff_payload(cls, handoff: AiCallHandoffModel) -> dict:
        return {
            "id": str(handoff.id),
            "handoff_id": handoff.handoff_id,
            "call_id": handoff.call_id,
            "room_name": handoff.room_name,
            "scene_code": handoff.scene_code,
            "status": handoff.status,
            "request_source": handoff.request_source,
            "request_reason": handoff.request_reason,
            "request_message": handoff.request_message,
            "human_agent_identity": handoff.human_agent_identity,
            "requested_at": cls._api_datetime(handoff.requested_at),
            "accepted_at": cls._api_datetime(handoff.accepted_at),
            "connected_at": cls._api_datetime(handoff.connected_at),
            "ended_at": cls._api_datetime(handoff.ended_at),
            "end_reason": handoff.end_reason,
            "failure_stage": handoff.failure_stage,
            "failure_message": handoff.failure_message,
        }

    @classmethod
    def _record_payload(cls, record: AiCallRecordModel | None) -> dict | None:
        if record is None:
            return None
        return {
            "id": str(record.id),
            "call_id": record.call_id,
            "follow_up_id": str(record.follow_up_id) if record.follow_up_id else None,
            "business_type": record.business_type,
            "business_id": record.business_id,
            "scene_code": record.scene_code,
            "prompt_source_key": record.prompt_source_key,
            "entry_type": record.entry_type,
            "masked_contact": record.callee_phone_number_masked,
            "status": record.status,
            "end_reason": record.end_reason,
            "failure_stage": record.failure_stage,
            "failure_message": record.failure_message,
            "started_at": cls._api_datetime(record.started_at),
            "answered_at": cls._api_datetime(record.answered_at),
            "ended_at": cls._api_datetime(record.ended_at),
        }

    @staticmethod
    def _identity(auth: AuthSchema):
        user = auth.user
        if user is None:
            raise CustomException(msg="认证已失效", code=10401, status_code=401)
        tenant_id = getattr(user, "tenant_id", None) or "000000"
        return user, str(tenant_id)

    @staticmethod
    def _require_confirmation(confirmed: bool) -> None:
        if not confirmed:
            raise CustomException(msg="请确认异常处理操作", status_code=422)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _api_datetime(cls, value: datetime | None) -> datetime | None:
        return cls._utc(value) if value is not None else None

    @staticmethod
    def _raise_conflict(message: str, error_code: str) -> None:
        raise CustomException(
            msg=message,
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
            data={"errorCode": error_code},
        )
