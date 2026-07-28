from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import status
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
    AiCallHandoffModel,
    AiCallRecordModel,
)
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.livekit_sip import (
    SipOutboundConfig,
    SipOutboundPreflightResult,
    validate_sip_outbound_preflight,
)

from .linphone_test_schema import (
    LinphoneTestAcceptedOut,
    LinphoneTestCapabilityOut,
    LinphoneTestStatusOut,
)
from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from .rule_task_schema import AcceptedCommandOut
from .task_executor import (
    ClaimedAttempt,
    DialResult,
    OutboundDialRequest,
    OutboundTaskExecutor,
    TaskKey,
)

DEFAULT_TENANT_ID = "000000"
LINPHONE_ACTIVE_SLOT = "linphone_test"
ACTIVE_ATTEMPT_STATUSES = ("DIALING", "IN_CALL")
AGENT_HEARTBEAT_SECONDS = 30


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def derive_test_phase(
    attempt_status: str,
    handoff_status: str | None,
) -> str:
    if attempt_status == "FAILED":
        return "failed"
    if attempt_status == "COMPLETED":
        return "completed"
    if handoff_status == "connected":
        return "human_call"
    if handoff_status in {"requested", "accepted"}:
        return "waiting_handoff"
    if attempt_status == "IN_CALL":
        return "ai_call"
    return "dialing"


def calculate_elapsed_seconds(
    answered_at: datetime | None,
    ended_at: datetime | None,
    now: datetime,
) -> int:
    if answered_at is None:
        return 0
    end = ended_at or now
    if answered_at.tzinfo is None and end.tzinfo is not None:
        answered_at = answered_at.replace(tzinfo=end.tzinfo)
    elif answered_at.tzinfo is not None and end.tzinfo is None:
        end = end.replace(tzinfo=answered_at.tzinfo)
    return max(0, int((end - answered_at).total_seconds()))


class LinphoneTestService:
    """评估正式外呼任务能否进入本机 Linphone 测试。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        settings_obj: Any = settings,
        now: Callable[[], datetime] = _utc_now,
        sip_preflight: Callable[[str], SipOutboundPreflightResult] | None = None,
        executor: OutboundTaskExecutor | None = None,
        dispatch: Callable[[ClaimedAttempt], None] | None = None,
        ai_call_service_factory: Callable[[AsyncSession], Any] | None = None,
        room_exists: Callable[[str], Awaitable[bool]] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings_obj
        self.now = now
        self.sip_preflight = sip_preflight or self._default_sip_preflight
        self.executor = executor or self._default_executor()
        self.dispatch = dispatch or self._dispatch_background
        self.ai_call_service_factory = (
            ai_call_service_factory or self._default_ai_call_service_factory
        )
        self.room_exists = room_exists or self._default_room_exists
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._end_command_results: dict[
            tuple[str, int, str],
            AcceptedCommandOut,
        ] = {}
        self._end_command_lock = asyncio.Lock()

    async def start_test(
        self,
        *,
        tenant_id: str,
        task_id: int,
        idempotency_key: str,
        scenario: str,
    ) -> LinphoneTestAcceptedOut:
        command_key = idempotency_key.strip()
        if not command_key or len(command_key) > 128:
            raise CustomException(
                msg="Idempotency-Key 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        existing = await self._read_attempt_by_command(tenant_id, command_key)
        if existing is not None:
            return self.accepted_out(existing)

        async with self.session_factory() as db:
            capability = await self.get_capability(db, tenant_id, task_id)
        if not capability.eligible:
            reason = capability.reasons[0]
            if "Linphone 测试通话" in reason:
                reason = "已有 Linphone 测试通话进行中"
            raise CustomException(
                msg=reason,
                status_code=status.HTTP_409_CONFLICT,
            )
        if scenario == "handoff" and capability.available_agent_count < 1:
            raise CustomException(
                msg="转人工测试至少需要一名可用坐席",
                status_code=status.HTTP_409_CONFLICT,
            )

        try:
            claimed = await self.executor.claim_manual_test(
                TaskKey(tenant_id, task_id),
                command_idempotency_key=command_key,
                test_scenario=scenario,
                active_slot=LINPHONE_ACTIVE_SLOT,
            )
        except (IntegrityError, ValueError):
            existing = await self._read_attempt_by_command(tenant_id, command_key)
            if existing is not None:
                return self.accepted_out(existing)
            raise CustomException(
                msg="已有 Linphone 测试通话进行中",
                status_code=status.HTTP_409_CONFLICT,
            ) from None

        attempt = await self._read_attempt_by_command(tenant_id, command_key)
        if attempt is None:
            raise CustomException(
                msg="Linphone 测试认领结果不存在",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        self.dispatch(claimed)
        return self.accepted_out(attempt)

    async def get_capability(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> LinphoneTestCapabilityOut:
        enabled = bool(self.settings.AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED)
        if not enabled:
            return self._unavailable(
                enabled=False,
                reason="Linphone 测试功能未启用",
            )
        if tenant_id != DEFAULT_TENANT_ID:
            return self._unavailable(
                enabled=True,
                reason="Linphone 测试仅支持默认租户 000000",
            )

        task = await db.scalar(
            select(AiCallOutboundTaskModel)
            .where(
                AiCallOutboundTaskModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.id == task_id,
            )
            .limit(1)
        )
        if task is None:
            return self._unavailable(enabled=True, reason="外呼任务不存在")

        available_agent_count = await self._available_agent_count(
            db,
            tenant_id=tenant_id,
            scene_code=task.scene_code,
        )
        same_task_active = await self._active_attempt(
            db,
            tenant_id=tenant_id,
            task_id=task_id,
        )
        if same_task_active is not None:
            return self._unavailable(
                enabled=True,
                reason="该任务已有进行中的 Linphone 测试通话",
                available_agent_count=available_agent_count,
                active_call_id=same_task_active.call_id,
                can_end_active_call=True,
            )

        if task.status != "SCHEDULED":
            return self._unavailable(
                enabled=True,
                reason="仅支持状态为 SCHEDULED 的外呼任务",
                available_agent_count=available_agent_count,
            )
        if task.task_mode != "single":
            return self._unavailable(
                enabled=True,
                reason="仅支持 single 模式的外呼任务",
                available_agent_count=available_agent_count,
            )

        targets = list(
            (
                await db.scalars(
                    select(AiCallOutboundTargetModel)
                    .where(
                        AiCallOutboundTargetModel.tenant_id == tenant_id,
                        AiCallOutboundTargetModel.task_id == task_id,
                    )
                    .order_by(AiCallOutboundTargetModel.id)
                    .limit(2)
                )
            ).all()
        )
        if len(targets) != 1:
            return self._unavailable(
                enabled=True,
                reason="任务必须且只能包含一条外呼对象",
                available_agent_count=available_agent_count,
            )

        target = targets[0]
        if target.status != "PENDING":
            return self._unavailable(
                enabled=True,
                reason="外呼对象必须处于 PENDING 状态",
                available_agent_count=available_agent_count,
            )
        if target.phone_number != self.settings.AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE:
            return self._unavailable(
                enabled=True,
                reason="外呼号码必须为允许的 Linphone 测试号码",
                available_agent_count=available_agent_count,
            )

        other_task_active = await db.scalar(
            select(AiCallOutboundAttemptModel.id)
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.task_id != task_id,
                AiCallOutboundAttemptModel.active_slot == LINPHONE_ACTIVE_SLOT,
                AiCallOutboundAttemptModel.status.in_(ACTIVE_ATTEMPT_STATUSES),
            )
            .limit(1)
        )
        if other_task_active is not None:
            return self._unavailable(
                enabled=True,
                reason="当前租户已有其他任务进行中的 Linphone 测试通话",
                available_agent_count=available_agent_count,
            )

        preflight = self.sip_preflight(target.phone_number)
        if not preflight.ok:
            return self._unavailable(
                enabled=True,
                reason=preflight.message or "SIP 外呼预检失败",
                available_agent_count=available_agent_count,
            )

        return LinphoneTestCapabilityOut(
            enabled=True,
            eligible=True,
            available_agent_count=available_agent_count,
        )

    async def get_status(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> LinphoneTestStatusOut:
        attempt = await db.scalar(
            select(AiCallOutboundAttemptModel)
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.task_id == task_id,
            )
            .order_by(
                AiCallOutboundAttemptModel.started_at.desc(),
                AiCallOutboundAttemptModel.id.desc(),
            )
            .limit(1)
        )
        if attempt is None:
            raise CustomException(
                msg="任务暂无测试拨打记录",
                status_code=status.HTTP_404_NOT_FOUND,
            )

        target = await db.scalar(
            select(AiCallOutboundTargetModel)
            .where(
                AiCallOutboundTargetModel.tenant_id == tenant_id,
                AiCallOutboundTargetModel.id == attempt.target_id,
            )
            .limit(1)
        )
        if target is None:
            raise CustomException(
                msg="测试拨打对象不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )

        record = await db.scalar(
            select(AiCallRecordModel)
            .where(AiCallRecordModel.call_id == attempt.call_id)
            .limit(1)
        )
        handoff = await db.scalar(
            select(AiCallHandoffModel)
            .where(
                AiCallHandoffModel.tenant_id == tenant_id,
                AiCallHandoffModel.call_id == attempt.call_id,
            )
            .order_by(
                AiCallHandoffModel.requested_at.desc(),
                AiCallHandoffModel.id.desc(),
            )
            .limit(1)
        )
        handoff_status = handoff.status if handoff is not None else None
        return LinphoneTestStatusOut(
            task_id=attempt.task_id,
            target_id=attempt.target_id,
            attempt_id=attempt.id,
            call_id=attempt.call_id,
            target_status=target.status,
            attempt_status=attempt.status,
            call_status=record.status if record is not None else None,
            handoff_status=handoff_status,
            phase=derive_test_phase(attempt.status, handoff_status),
            elapsed_seconds=calculate_elapsed_seconds(
                record.answered_at if record is not None else None,
                record.ended_at if record is not None else None,
                self.now(),
            ),
            end_reason=record.end_reason if record is not None else None,
            error_message=(
                attempt.error_message
                or (record.failure_message if record is not None else None)
            ),
            can_end_active_call=(
                attempt.active_slot == LINPHONE_ACTIVE_SLOT
                and attempt.status in ACTIVE_ATTEMPT_STATUSES
            ),
        )

    async def end_active_call(
        self,
        *,
        tenant_id: str,
        task_id: int,
        idempotency_key: str,
    ) -> AcceptedCommandOut:
        command_key = idempotency_key.strip()
        if not command_key or len(command_key) > 128:
            raise CustomException(
                msg="Idempotency-Key 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        result_key = (tenant_id, task_id, command_key)
        async with self._end_command_lock:
            existing = self._end_command_results.get(result_key)
            if existing is not None:
                return existing

            async with self.session_factory() as db:
                attempt = await self._active_attempt(
                    db,
                    tenant_id=tenant_id,
                    task_id=task_id,
                )
                if attempt is None:
                    raise CustomException(
                        msg="当前任务没有可结束的活动通话",
                        status_code=status.HTTP_409_CONFLICT,
                    )
                service = self.ai_call_service_factory(db)
                await service.end_session(
                    attempt.call_id,
                    end_reason="outbound_task_manual_end",
                )
                await db.commit()

            result = AcceptedCommandOut()
            self._end_command_results[result_key] = result
            return result

    async def reconcile_once(self) -> int:
        """恢复重启后遗留的本机 Linphone 活动测试。"""
        async with self.session_factory() as db:
            attempt_ids = list(
                (
                    await db.scalars(
                        select(AiCallOutboundAttemptModel.id)
                        .where(
                            AiCallOutboundAttemptModel.dialer_type
                            == LINPHONE_ACTIVE_SLOT,
                            AiCallOutboundAttemptModel.status.in_(
                                ACTIVE_ATTEMPT_STATUSES
                            ),
                            AiCallOutboundAttemptModel.active_slot
                            == LINPHONE_ACTIVE_SLOT,
                        )
                        .order_by(AiCallOutboundAttemptModel.started_at)
                        .limit(50)
                    )
                ).all()
            )

        recovered = 0
        for attempt_id in attempt_ids:
            if await self._reconcile_attempt(attempt_id):
                recovered += 1
        return recovered

    async def _reconcile_attempt(self, attempt_id: int) -> bool:
        now = self.now()
        async with self.session_factory() as db:
            attempt = await db.scalar(
                select(AiCallOutboundAttemptModel)
                .where(
                    AiCallOutboundAttemptModel.id == attempt_id,
                    AiCallOutboundAttemptModel.dialer_type
                    == LINPHONE_ACTIVE_SLOT,
                    AiCallOutboundAttemptModel.status.in_(ACTIVE_ATTEMPT_STATUSES),
                    AiCallOutboundAttemptModel.active_slot == LINPHONE_ACTIVE_SLOT,
                )
                .limit(1)
            )
            if attempt is None:
                return False
            task = await db.scalar(
                select(AiCallOutboundTaskModel)
                .where(
                    AiCallOutboundTaskModel.tenant_id == attempt.tenant_id,
                    AiCallOutboundTaskModel.id == attempt.task_id,
                )
                .limit(1)
            )
            target = await db.scalar(
                select(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id == attempt.tenant_id,
                    AiCallOutboundTargetModel.task_id == attempt.task_id,
                    AiCallOutboundTargetModel.id == attempt.target_id,
                )
                .limit(1)
            )
            if task is None or target is None:
                return False
            record = await db.scalar(
                select(AiCallRecordModel)
                .where(AiCallRecordModel.call_id == attempt.call_id)
                .limit(1)
            )

            if record is not None and str(record.status or "").lower() in {
                "completed",
                "failed",
            }:
                result = self._dial_result_from_record(record, now)
            else:
                if record is not None and await self.room_exists(record.room_name):
                    return False
                if not self._recovery_grace_elapsed(attempt.started_at, now):
                    return False
                if record is None:
                    result = DialResult(
                        call_result="call_failed",
                        error_message="Linphone 测试恢复失败：宽限期内未生成通话记录",
                    )
                else:
                    record.status = "failed"
                    record.end_reason = "linphone_test_recovery_room_missing"
                    record.failure_stage = "linphone_test_recovery"
                    record.failure_message = (
                        "Linphone 测试恢复失败：LiveKit Room 已不存在"
                    )
                    record.ended_at = now
                    if record.answered_at is not None:
                        record.duration_ms = self._duration_ms(
                            record.answered_at,
                            now,
                        )
                    await db.commit()
                    result = self._dial_result_from_record(record, now)

            request = OutboundDialRequest(
                tenant_id=attempt.tenant_id,
                task_id=task.id,
                target_id=target.id,
                attempt_no=attempt.attempt_no,
                phone_number=target.phone_number,
                customer_name=target.customer_name,
                scene_code=task.scene_code,
                voice=task.voice,
                prompt_profile_id=task.prompt_profile_id,
            )
            call_id = attempt.call_id

        await self.executor._finish_attempt(request, call_id, result, now)
        return True

    def _recovery_grace_elapsed(
        self,
        started_at: datetime,
        now: datetime,
    ) -> bool:
        grace_seconds = max(
            0,
            int(
                getattr(
                    self.settings,
                    "AI_CALL_OUTBOUND_LINPHONE_RECOVERY_GRACE_SECONDS",
                    30,
                )
            ),
        )
        return self._duration_ms(started_at, now) >= grace_seconds * 1000

    @classmethod
    def _dial_result_from_record(
        cls,
        record: AiCallRecordModel,
        now: datetime,
    ) -> DialResult:
        if record.answered_at is not None:
            return DialResult(
                call_result="connected",
                duration_ms=cls._duration_ms(
                    record.answered_at,
                    record.ended_at or now,
                ),
            )
        end_reason = str(record.end_reason or "").strip()
        normalized_reason = end_reason.lower()
        if normalized_reason in {
            "busy",
            "busy_here",
            "callee_busy",
            "sip_busy",
            "user_busy",
        }:
            call_result = "busy"
        elif normalized_reason in {
            "connect_timeout",
            "no_answer",
            "ringing_timeout",
            "sip_connect_timeout",
            "user_unavailable",
        }:
            call_result = "no_answer"
        else:
            call_result = "call_failed"
        return DialResult(
            call_result=call_result,
            error_message=(
                record.failure_message or end_reason or "Linphone 测试恢复失败"
            ),
        )

    @staticmethod
    def _duration_ms(started_at: datetime, ended_at: datetime) -> int:
        if started_at.tzinfo is None and ended_at.tzinfo is not None:
            started_at = started_at.replace(tzinfo=ended_at.tzinfo)
        elif started_at.tzinfo is not None and ended_at.tzinfo is None:
            ended_at = ended_at.replace(tzinfo=started_at.tzinfo)
        return max(0, int((ended_at - started_at).total_seconds() * 1000))

    async def _default_room_exists(self, room_name: str) -> bool:
        from app.services.ai_call.livekit_room import LiveKitRoomManager

        manager = LiveKitRoomManager(
            self.settings.LIVEKIT_URL,
            self.settings.LIVEKIT_API_KEY,
            self.settings.LIVEKIT_API_SECRET,
            self.settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS,
        )
        return await manager.room_exists(room_name)

    async def _active_attempt(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        task_id: int,
    ) -> AiCallOutboundAttemptModel | None:
        return await db.scalar(
            select(AiCallOutboundAttemptModel)
            .where(
                AiCallOutboundAttemptModel.tenant_id == tenant_id,
                AiCallOutboundAttemptModel.task_id == task_id,
                AiCallOutboundAttemptModel.active_slot == LINPHONE_ACTIVE_SLOT,
                AiCallOutboundAttemptModel.status.in_(ACTIVE_ATTEMPT_STATUSES),
            )
            .order_by(AiCallOutboundAttemptModel.started_at.desc())
            .limit(1)
        )

    async def _available_agent_count(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        scene_code: str,
    ) -> int:
        cutoff = self.now() - timedelta(seconds=AGENT_HEARTBEAT_SECONDS)
        count = await db.scalar(
            select(func.count(func.distinct(AiCallAgentProfileModel.agent_identity)))
            .select_from(AiCallAgentProfileModel)
            .join(
                AiCallAgentSceneScopeModel,
                and_(
                    AiCallAgentSceneScopeModel.tenant_id == AiCallAgentProfileModel.tenant_id,
                    AiCallAgentSceneScopeModel.agent_identity
                    == AiCallAgentProfileModel.agent_identity,
                ),
            )
            .join(
                AiCallHandoffAgentModel,
                and_(
                    AiCallHandoffAgentModel.tenant_id == AiCallAgentProfileModel.tenant_id,
                    AiCallHandoffAgentModel.agent_identity
                    == AiCallAgentProfileModel.agent_identity,
                ),
            )
            .where(
                AiCallAgentProfileModel.tenant_id == tenant_id,
                AiCallAgentProfileModel.enabled.is_(True),
                AiCallAgentSceneScopeModel.scene_code == scene_code,
                AiCallHandoffAgentModel.status == "available",
                AiCallHandoffAgentModel.active_handoff_id.is_(None),
                AiCallHandoffAgentModel.last_seen_at.is_not(None),
                AiCallHandoffAgentModel.last_seen_at >= cutoff,
            )
        )
        return int(count or 0)

    def _default_sip_preflight(
        self,
        callee_phone_number: str,
    ) -> SipOutboundPreflightResult:
        config = SipOutboundConfig.from_settings(self.settings)
        return validate_sip_outbound_preflight(
            config,
            callee_phone_number=callee_phone_number,
        )

    @staticmethod
    def _default_ai_call_service_factory(db: AsyncSession) -> Any:
        from app.api.v1.ai_call.service import get_default_ai_call_service

        return get_default_ai_call_service(db)

    def _default_executor(self) -> OutboundTaskExecutor:
        from .linphone_test_dialer import LinphoneTestDialer

        dialer = LinphoneTestDialer(
            self.session_factory,
            poll_seconds=getattr(
                self.settings,
                "AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS",
                1.0,
            ),
            now=self.now,
        )
        return OutboundTaskExecutor(
            self.session_factory,
            dialer,
            now_provider=self.now,
        )

    async def _read_attempt_by_command(
        self,
        tenant_id: str,
        command_key: str,
    ) -> AiCallOutboundAttemptModel | None:
        async with self.session_factory() as db:
            return await db.scalar(
                select(AiCallOutboundAttemptModel)
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.command_idempotency_key == command_key,
                )
                .limit(1)
            )

    def _dispatch_background(self, claimed: ClaimedAttempt) -> None:
        task = asyncio.create_task(self.executor.execute_claimed(claimed))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    @staticmethod
    def accepted_out(
        attempt: AiCallOutboundAttemptModel,
    ) -> LinphoneTestAcceptedOut:
        return LinphoneTestAcceptedOut(
            task_id=attempt.task_id,
            attempt_id=attempt.id,
            call_id=attempt.call_id,
        )

    @staticmethod
    def _unavailable(
        *,
        enabled: bool,
        reason: str,
        available_agent_count: int = 0,
        active_call_id: str | None = None,
        can_end_active_call: bool = False,
    ) -> LinphoneTestCapabilityOut:
        return LinphoneTestCapabilityOut(
            enabled=enabled,
            eligible=False,
            reasons=[reason],
            available_agent_count=available_agent_count,
            active_call_id=active_call_id,
            can_end_active_call=can_end_active_call,
        )


class LinphoneTestRecoveryWorker:
    """周期性恢复因 API 进程重启而遗留的 Linphone 测试。"""

    def __init__(
        self,
        service: LinphoneTestService,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self.service = service
        self.poll_interval_seconds = max(0.05, poll_interval_seconds)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-linphone-test-recovery-worker",
        )

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.service.reconcile_once()
            except Exception:
                log.exception("AI Call 本机 Linphone 测试恢复 worker 轮询失败")
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                continue
