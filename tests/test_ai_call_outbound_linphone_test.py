import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import String, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallAgentProfileModel,
    AiCallAgentSceneScopeModel,
    AiCallHandoffAgentModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.task_executor import OutboundDialRequest
from app.config.setting import Settings
from app.core.base_model import MappedBase


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'linphone.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def test_linphone_command_schema_uses_camel_case_and_string_ids() -> None:
    from app.api.v1.ai_call.outbound.linphone_test_schema import (
        LinphoneTestAcceptedOut,
        LinphoneTestCapabilityOut,
        LinphoneTestRunIn,
        LinphoneTestScenario,
    )

    ai_only = LinphoneTestRunIn.model_validate({"scenario": "ai_only"})
    handoff = LinphoneTestRunIn.model_validate({"scenario": "handoff"})
    capability = LinphoneTestCapabilityOut(
        enabled=True,
        eligible=False,
        reasons=["已有测试通话"],
        available_agent_count=2,
        active_call_id="call-1",
        can_end_active_call=True,
    )
    accepted = LinphoneTestAcceptedOut(
        task_id=1001,
        attempt_id=2001,
        call_id="call-1",
    )

    assert LinphoneTestScenario.__bases__ == (str, Enum)
    assert isinstance(ai_only.scenario.value, str)
    assert isinstance(handoff.scenario.value, str)
    assert ai_only.scenario == "ai_only"
    assert handoff.scenario == "handoff"
    assert ai_only.model_dump_json(by_alias=True) == '{"scenario":"ai_only"}'
    assert handoff.model_dump_json(by_alias=True) == '{"scenario":"handoff"}'
    assert capability.model_dump(by_alias=True) == {
        "enabled": True,
        "eligible": False,
        "reasons": ["已有测试通话"],
        "availableAgentCount": 2,
        "activeCallId": "call-1",
        "canEndActiveCall": True,
    }
    assert accepted.model_dump(by_alias=True) == {
        "accepted": True,
        "taskId": "1001",
        "attemptId": "2001",
        "callId": "call-1",
    }


def test_linphone_test_service_is_importable() -> None:
    from app.api.v1.ai_call.outbound.linphone_test_service import LinphoneTestService

    assert LinphoneTestService is not None


def test_linphone_safety_settings_are_closed_by_default() -> None:
    assert Settings.model_fields["AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED"].default is False
    assert (
        Settings.model_fields["AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE"].default
        == "19900001001"
    )
    assert Settings.model_fields["AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS"].default == 1.0
    assert (
        Settings.model_fields[
            "AI_CALL_OUTBOUND_LINPHONE_RECOVERY_GRACE_SECONDS"
        ].default
        == 30
    )
    assert Settings.model_fields["AI_CALL_OUTBOUND_EXECUTOR_ENABLED"].default is False


CAPABILITY_NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
ALLOWED_CALLEE = "19900001001"


def _linphone_settings(
    *,
    enabled: bool = True,
    allowed_callee: str = ALLOWED_CALLEE,
) -> SimpleNamespace:
    return SimpleNamespace(
        AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED=enabled,
        AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE=allowed_callee,
    )


class _PreflightFake:
    def __init__(self, *, ok: bool = True, message: str | None = None) -> None:
        self.ok = ok
        self.message = message
        self.calls: list[str] = []

    def __call__(self, callee_phone_number: str) -> SimpleNamespace:
        self.calls.append(callee_phone_number)
        return SimpleNamespace(ok=self.ok, message=self.message)


def _outbound_task(
    *,
    task_id: int = 1001,
    tenant_id: str = "000000",
    status: str = "SCHEDULED",
    task_mode: str = "single",
    scene_code: str = "intro_contract",
) -> AiCallOutboundTaskModel:
    return AiCallOutboundTaskModel(
        id=task_id,
        tenant_id=tenant_id,
        validation_id=2001,
        idempotency_key=f"task-{task_id}",
        request_fingerprint=f"fingerprint-{task_id}",
        task_name=f"task-{task_id}",
        task_mode=task_mode,
        status=status,
        total_targets=1,
        completed_targets=0,
        connected_targets=0,
        failed_targets=0,
        execution_mode="manual",
        prompt_profile_id=None,
        prompt_name="测试提示词",
        scene_code=scene_code,
        voice="Cherry",
        voice_name="测试音色",
        rule_id=3001,
        rule_name="测试规则",
        rule_summary="测试规则摘要",
        config_snapshot_json="{}",
        created_by=1,
        created_by_name="tester",
        created_at=CAPABILITY_NOW,
        updated_at=CAPABILITY_NOW,
    )


def _outbound_target(
    target_id: int = 4001,
    *,
    task_id: int = 1001,
    tenant_id: str = "000000",
    phone_number: str = ALLOWED_CALLEE,
    status: str = "PENDING",
) -> AiCallOutboundTargetModel:
    return AiCallOutboundTargetModel(
        id=target_id,
        tenant_id=tenant_id,
        task_id=task_id,
        validation_id=2001,
        source_validation_row_id=target_id + 10000,
        source_row_number=target_id,
        phone_number=phone_number,
        customer_name="测试客户",
        status=status,
        attempt_count=0,
        created_at=CAPABILITY_NOW,
        updated_at=CAPABILITY_NOW,
    )


def _active_linphone_attempt(
    attempt_id: int,
    *,
    task_id: int,
    tenant_id: str = "000000",
    status: str = "DIALING",
) -> AiCallOutboundAttemptModel:
    return AiCallOutboundAttemptModel(
        id=attempt_id,
        tenant_id=tenant_id,
        task_id=task_id,
        target_id=attempt_id + 10000,
        attempt_no=1,
        call_id=f"active-call-{attempt_id}",
        dialer_type="linphone_test",
        test_scenario="ai_only",
        command_idempotency_key=f"command-{attempt_id}",
        active_slot="linphone_test",
        status=status,
        started_at=CAPABILITY_NOW,
        created_at=CAPABILITY_NOW,
        updated_at=CAPABILITY_NOW,
    )


async def _seed_capability_task(
    database,
    *,
    task: AiCallOutboundTaskModel | None = None,
    targets: list[AiCallOutboundTargetModel] | None = None,
    attempts: list[AiCallOutboundAttemptModel] | None = None,
) -> None:
    async with database() as session, session.begin():
        if task is not None:
            session.add(task)
        session.add_all(targets or [])
        session.add_all(attempts or [])


async def _get_capability(
    database,
    *,
    tenant_id: str = "000000",
    task_id: int = 1001,
    settings_obj: SimpleNamespace | None = None,
    preflight: _PreflightFake | None = None,
):
    from app.api.v1.ai_call.outbound.linphone_test_service import LinphoneTestService

    fake = preflight or _PreflightFake()
    service = LinphoneTestService(
        session_factory=database,
        settings_obj=settings_obj or _linphone_settings(),
        now=lambda: CAPABILITY_NOW,
        sip_preflight=fake,
    )
    async with database() as session:
        result = await service.get_capability(session, tenant_id, task_id)
    return result, fake


@pytest.mark.anyio
async def test_capability_returns_early_when_feature_is_disabled(database) -> None:
    result, preflight = await _get_capability(
        database,
        settings_obj=_linphone_settings(enabled=False),
    )

    assert result.enabled is False
    assert result.eligible is False
    assert result.reasons == ["Linphone 测试功能未启用"]
    assert preflight.calls == []


@pytest.mark.anyio
async def test_capability_rejects_non_default_tenant_without_querying_sip(database) -> None:
    result, preflight = await _get_capability(database, tenant_id="tenant-a")

    assert result.eligible is False
    assert result.reasons == ["Linphone 测试仅支持默认租户 000000"]
    assert preflight.calls == []


@pytest.mark.anyio
async def test_capability_rejects_missing_task(database) -> None:
    result, preflight = await _get_capability(database)

    assert result.eligible is False
    assert result.reasons == ["外呼任务不存在"]
    assert preflight.calls == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("task", "targets", "attempts", "expected_reason"),
    [
        (
            _outbound_task(status="RUNNING"),
            [_outbound_target()],
            [],
            "仅支持状态为 SCHEDULED 的外呼任务",
        ),
        (
            _outbound_task(task_mode="batch"),
            [_outbound_target()],
            [],
            "仅支持 single 模式的外呼任务",
        ),
        (
            _outbound_task(),
            [_outbound_target(), _outbound_target(4002)],
            [],
            "任务必须且只能包含一条外呼对象",
        ),
        (
            _outbound_task(),
            [_outbound_target(status="DIALING")],
            [],
            "外呼对象必须处于 PENDING 状态",
        ),
        (
            _outbound_task(),
            [_outbound_target(phone_number="19900001002")],
            [],
            "外呼号码必须为允许的 Linphone 测试号码",
        ),
        (
            _outbound_task(),
            [_outbound_target()],
            [_active_linphone_attempt(5001, task_id=1002)],
            "当前租户已有其他任务进行中的 Linphone 测试通话",
        ),
    ],
)
async def test_capability_rejects_ineligible_task_states(
    database,
    task,
    targets,
    attempts,
    expected_reason,
) -> None:
    await _seed_capability_task(
        database,
        task=task,
        targets=targets,
        attempts=attempts,
    )

    result, _ = await _get_capability(database)

    assert result.eligible is False
    assert result.reasons == [expected_reason]
    assert result.can_end_active_call is False


@pytest.mark.anyio
async def test_capability_uses_preflight_failure_message(database) -> None:
    await _seed_capability_task(
        database,
        task=_outbound_task(),
        targets=[_outbound_target()],
    )
    preflight = _PreflightFake(ok=False, message="SIP trunk 配置缺失")

    result, preflight = await _get_capability(database, preflight=preflight)

    assert result.eligible is False
    assert result.reasons == ["SIP trunk 配置缺失"]
    assert preflight.calls == [ALLOWED_CALLEE]


@pytest.mark.anyio
async def test_capability_is_eligible_for_single_pending_allowed_target(database) -> None:
    await _seed_capability_task(
        database,
        task=_outbound_task(),
        targets=[_outbound_target()],
    )

    result, preflight = await _get_capability(database)

    assert result.enabled is True
    assert result.eligible is True
    assert result.reasons == []
    assert result.active_call_id is None
    assert result.can_end_active_call is False
    assert preflight.calls == [ALLOWED_CALLEE]


@pytest.mark.anyio
async def test_capability_exposes_same_task_active_call(database) -> None:
    await _seed_capability_task(
        database,
        task=_outbound_task(status="RUNNING"),
        targets=[_outbound_target(status="DIALING")],
        attempts=[_active_linphone_attempt(5001, task_id=1001, status="IN_CALL")],
    )

    result, preflight = await _get_capability(database)

    assert result.eligible is False
    assert result.reasons == ["该任务已有进行中的 Linphone 测试通话"]
    assert result.active_call_id == "active-call-5001"
    assert result.can_end_active_call is True
    assert preflight.calls == []


async def _seed_agent(
    database,
    *,
    row_id: int,
    agent_identity: str,
    tenant_id: str = "000000",
    enabled: bool = True,
    scene_codes: tuple[str, ...] = ("intro_contract",),
    status: str = "available",
    last_seen_at: datetime | None = CAPABILITY_NOW,
    active_handoff_id: str | None = None,
) -> None:
    async with database() as session, session.begin():
        session.add(
            AiCallAgentProfileModel(
                id=row_id,
                tenant_id=tenant_id,
                agent_identity=agent_identity,
                user_id=row_id,
                enabled=enabled,
                created_by=1,
                created_at=CAPABILITY_NOW,
                updated_by=1,
                updated_at=CAPABILITY_NOW,
            )
        )
        session.add_all(
            [
                AiCallAgentSceneScopeModel(
                    id=row_id * 100 + index,
                    tenant_id=tenant_id,
                    agent_identity=agent_identity,
                    scene_code=scene_code,
                    created_by=1,
                    created_at=CAPABILITY_NOW,
                )
                for index, scene_code in enumerate(scene_codes, start=1)
            ]
        )
        session.add(
            AiCallHandoffAgentModel(
                id=row_id,
                tenant_id=tenant_id,
                agent_identity=agent_identity,
                skill_group="default",
                status=status,
                active_handoff_id=active_handoff_id,
                active_call_id=None,
                console_session_id=f"session-{row_id}",
                last_seen_at=last_seen_at,
                status_updated_at=CAPABILITY_NOW,
            )
        )


@pytest.mark.anyio
async def test_capability_counts_only_distinct_available_agents_in_scene(database) -> None:
    await _seed_capability_task(
        database,
        task=_outbound_task(),
        targets=[_outbound_target()],
    )
    await _seed_agent(
        database,
        row_id=1,
        agent_identity="eligible",
        scene_codes=("intro_contract", "intro_document"),
    )
    await _seed_agent(
        database,
        row_id=2,
        agent_identity="other-tenant",
        tenant_id="tenant-a",
    )
    await _seed_agent(
        database,
        row_id=3,
        agent_identity="disabled",
        enabled=False,
    )
    await _seed_agent(
        database,
        row_id=4,
        agent_identity="wrong-scope",
        scene_codes=("intro_document",),
    )
    await _seed_agent(
        database,
        row_id=5,
        agent_identity="paused",
        status="paused",
    )
    await _seed_agent(
        database,
        row_id=6,
        agent_identity="offline",
        status="offline",
    )
    await _seed_agent(
        database,
        row_id=7,
        agent_identity="stale",
        last_seen_at=CAPABILITY_NOW - timedelta(seconds=31),
    )
    await _seed_agent(
        database,
        row_id=8,
        agent_identity="active-handoff",
        active_handoff_id="handoff-8",
    )

    result, _ = await _get_capability(database)

    assert result.available_agent_count == 1


def _attempt(
    attempt_id: int,
    *,
    tenant_id: str = "tenant-a",
    command_idempotency_key: str | None = None,
    active_slot: str | None = None,
) -> AiCallOutboundAttemptModel:
    now = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    return AiCallOutboundAttemptModel(
        id=attempt_id,
        tenant_id=tenant_id,
        task_id=attempt_id + 1000,
        target_id=attempt_id + 2000,
        attempt_no=1,
        call_id=f"linphone-test-{attempt_id}",
        dialer_type="linphone_local",
        test_scenario="local_outbound",
        command_idempotency_key=command_idempotency_key,
        active_slot=active_slot,
        status="PENDING",
        started_at=now,
        created_at=now,
        updated_at=now,
    )


def test_outbound_attempt_has_nullable_linphone_test_metadata() -> None:
    columns = AiCallOutboundAttemptModel.__table__.columns
    expected_lengths = {
        "dialer_type": 32,
        "test_scenario": 32,
        "command_idempotency_key": 128,
        "active_slot": 32,
    }

    for column_name, expected_length in expected_lengths.items():
        column = columns[column_name]
        assert column.nullable
        assert isinstance(column.type, String)
        assert column.type.length == expected_length


def test_outbound_attempt_has_linphone_test_unique_guards() -> None:
    unique_constraints = {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in AiCallOutboundAttemptModel.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert unique_constraints["uk_outbound_attempt_tenant_command"] == (
        "tenant_id",
        "command_idempotency_key",
    )
    assert unique_constraints["uk_outbound_attempt_tenant_active_slot"] == (
        "tenant_id",
        "active_slot",
    )


@pytest.mark.anyio
async def test_command_idempotency_key_uniqueness_is_tenant_scoped(database) -> None:
    async with database() as session:
        session.add(_attempt(1, command_idempotency_key="command-1"))
        await session.commit()

        session.add(_attempt(2, command_idempotency_key="command-1"))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        session.add(
            _attempt(
                3,
                tenant_id="tenant-b",
                command_idempotency_key="command-1",
            )
        )
        await session.commit()

        session.add_all([_attempt(4), _attempt(5)])
        await session.commit()


@pytest.mark.anyio
async def test_active_slot_uniqueness_is_tenant_scoped(database) -> None:
    async with database() as session:
        session.add(_attempt(11, active_slot="linphone-local-active"))
        await session.commit()

        session.add(_attempt(12, active_slot="linphone-local-active"))
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

        session.add(
            _attempt(
                13,
                tenant_id="tenant-b",
                active_slot="linphone-local-active",
            )
        )
        await session.commit()

        session.add_all([_attempt(14), _attempt(15)])
        await session.commit()


def test_linphone_test_postgres_migration_adds_columns_and_unique_indexes() -> None:
    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h4-outbound-linphone-test-postgres.sql"
    )
    migration = " ".join(
        migration_path.read_text(encoding="utf-8").lower().split()
    )

    assert "add column if not exists dialer_type varchar(32)" in migration
    assert "add column if not exists test_scenario varchar(32)" in migration
    assert "add column if not exists command_idempotency_key varchar(128)" in migration
    assert "add column if not exists active_slot varchar(32)" in migration
    assert (
        "create unique index concurrently if not exists "
        "uk_outbound_attempt_tenant_command "
        "on ai_call_outbound_attempt (tenant_id, command_idempotency_key)"
        in migration
    )
    assert (
        "create unique index concurrently if not exists "
        "uk_outbound_attempt_tenant_active_slot "
        "on ai_call_outbound_attempt (tenant_id, active_slot)"
        in migration
    )
    assert "must run with autocommit enabled and outside any transaction block" in migration
    assert (
        "postgresql does not allow create index concurrently inside a transaction block"
        in migration
    )
    assert "foreign key" not in migration
    assert "jsonb" not in migration


class _FakeSession:
    def __init__(self, owner: "_FakeSessionFactory", index: int) -> None:
        self.owner = owner
        self.index = index
        self.commit_count = 0
        self.rollback_count = 0
        self.scalar_calls: list[Any] = []
        self.added: list[Any] = []

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback

    async def commit(self) -> None:
        if self.index in self.owner.commit_failure_indexes:
            raise RuntimeError("commit failed")
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def scalar(self, statement):
        self.scalar_calls.append(statement)
        if not self.owner.records:
            return None
        return self.owner.records.pop(0)

    def add(self, value: Any) -> None:
        self.added.append(value)


class _FakeSessionFactory:
    def __init__(
        self,
        records: list[Any],
        *,
        commit_failure_indexes: set[int] | None = None,
    ) -> None:
        self.records = list(records)
        self.commit_failure_indexes = commit_failure_indexes or set()
        self.sessions: list[_FakeSession] = []

    def __call__(self) -> _FakeSession:
        session = _FakeSession(self, len(self.sessions))
        self.sessions.append(session)
        return session


class _FakeAiCallService:
    def __init__(self, exception: Exception | None = None) -> None:
        self.exception = exception
        self.create_calls: list[dict[str, Any]] = []

    async def create_sip_session(self, **kwargs):
        self.create_calls.append(kwargs)
        if self.exception is not None:
            raise self.exception
        return SimpleNamespace(call_id=kwargs["call_id"])


class _AnsweredThenFailingAiCallService:
    def __init__(self, session: _FakeSession, answered_record: Any) -> None:
        self.session = session
        self.answered_record = answered_record

    async def create_sip_session(self, **kwargs):
        del kwargs
        self.session.owner.records.append(self.answered_record)
        raise RuntimeError("opening failed after answer")


class _ControlledSleep:
    def __init__(self) -> None:
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.waiting.set()
        await self.release.wait()


def _dialer_class():
    from app.api.v1.ai_call.outbound.linphone_test_dialer import LinphoneTestDialer

    return LinphoneTestDialer


def _request() -> OutboundDialRequest:
    return OutboundDialRequest(
        tenant_id="tenant-a",
        task_id=1001,
        target_id=2001,
        attempt_no=1,
        phone_number="13800000000",
        customer_name="张三",
        scene_code="intro_geo",
        voice="Cherry",
        prompt_profile_id=None,
    )


def _record(
    *,
    status: str,
    end_reason: str | None = None,
    failure_message: str | None = None,
    answered_at: datetime | None = None,
    ended_at: datetime | None = None,
):
    return SimpleNamespace(
        status=status,
        end_reason=end_reason,
        failure_message=failure_message,
        answered_at=answered_at,
        ended_at=ended_at,
    )


async def _no_sleep(seconds: float) -> None:
    del seconds


def test_linphone_test_dialer_declares_real_record_ownership() -> None:
    dialer_class = _dialer_class()

    assert dialer_class.dialer_type == "linphone_test"
    assert dialer_class.manages_call_record is True


@pytest.mark.anyio
async def test_dial_starts_stable_sip_session_then_waits_for_terminal_record() -> None:
    answered_at = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    ended_at = datetime(2026, 7, 28, 8, 0, 4, tzinfo=timezone.utc)
    session_factory = _FakeSessionFactory(
        [
            _record(status="connected", answered_at=answered_at),
            _record(
                status="completed",
                end_reason="customer_end",
                answered_at=answered_at,
                ended_at=ended_at,
            ),
        ]
    )
    service = _FakeAiCallService()
    service_sessions: list[_FakeSession] = []
    controlled_sleep = _ControlledSleep()
    connected_count = 0

    def service_factory(session: _FakeSession) -> _FakeAiCallService:
        service_sessions.append(session)
        return service

    async def on_connected() -> None:
        nonlocal connected_count
        connected_count += 1

    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=service_factory,
        poll_seconds=0.25,
        sleep=controlled_sleep,
        now=lambda: ended_at,
    )

    task = asyncio.create_task(
        dialer.dial(
            _request(),
            call_id="stable-call-id",
            on_connected=on_connected,
        )
    )
    await controlled_sleep.waiting.wait()

    assert connected_count == 1
    assert not task.done()
    assert len(session_factory.sessions) == 2

    controlled_sleep.release.set()
    result = await task

    assert result.call_result == "connected"
    assert result.duration_ms == 4000
    assert result.error_message is None
    assert service_sessions == [session_factory.sessions[0]]
    assert session_factory.sessions[0].commit_count == 1
    assert service.create_calls == [
        {
            "callee_phone_number": "13800000000",
            "voice": "Cherry",
            "call_id": "stable-call-id",
            "business_type": "outbound_task",
            "business_id": "1001",
            "scene_code": "intro_geo",
            "business_params": {
                "customer_name": "张三",
                "target_id": "2001",
            },
        }
    ]
    assert len(session_factory.sessions) == 3
    assert [len(session.scalar_calls) for session in session_factory.sessions] == [0, 1, 1]
    assert all(session.added == [] for session in session_factory.sessions)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("end_reason", "failure_message", "expected_result", "expected_message"),
    [
        ("busy", "被叫正忙", "busy", "被叫正忙"),
        ("user_busy", None, "busy", "user_busy"),
        ("ringing_timeout", None, "no_answer", "ringing_timeout"),
        ("user_unavailable", None, "no_answer", "user_unavailable"),
        ("sip_connect_timeout", None, "no_answer", "sip_connect_timeout"),
        ("no_answer", None, "no_answer", "no_answer"),
        ("connect_timeout", None, "no_answer", "connect_timeout"),
        ("room_create_failed", "LiveKit Room 创建失败", "call_failed", "LiveKit Room 创建失败"),
        ("unknown_runtime_error", None, "call_failed", "unknown_runtime_error"),
    ],
)
async def test_unanswered_terminal_record_maps_call_result(
    end_reason: str,
    failure_message: str | None,
    expected_result: str,
    expected_message: str,
) -> None:
    session_factory = _FakeSessionFactory(
        [
            _record(
                status="failed",
                end_reason=end_reason,
                failure_message=failure_message,
            )
        ]
    )
    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=lambda session: _FakeAiCallService(),
        poll_seconds=0,
        sleep=_no_sleep,
    )

    result = await dialer.dial(
        _request(),
        call_id="mapping-call-id",
        on_connected=lambda: _no_sleep(0),
    )

    assert result.call_result == expected_result
    assert result.error_message == expected_message
    assert result.duration_ms == 0


@pytest.mark.anyio
async def test_answered_failed_record_stays_connected_and_clamps_duration() -> None:
    answered_at = datetime(2026, 7, 28, 8, 0, 5)
    now = datetime(2026, 7, 28, 8, 0, 4, tzinfo=timezone.utc)
    session_factory = _FakeSessionFactory(
        [
            _record(
                status="failed",
                end_reason="handoff_failed",
                failure_message="转人工失败",
                answered_at=answered_at,
            )
        ]
    )
    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=lambda session: _FakeAiCallService(),
        poll_seconds=0,
        sleep=_no_sleep,
        now=lambda: now,
    )

    result = await dialer.dial(
        _request(),
        call_id="answered-failed-call-id",
        on_connected=lambda: _no_sleep(0),
    )

    assert result.call_result == "connected"
    assert result.error_message is None
    assert result.duration_ms == 0


@pytest.mark.anyio
async def test_create_failure_uses_persisted_terminal_record_without_callback() -> None:
    session_factory = _FakeSessionFactory(
        [
            _record(
                status="failed",
                end_reason="sip_connect_timeout",
                failure_message="SIP 连接超时",
            )
        ]
    )
    service = _FakeAiCallService(RuntimeError("provider exploded"))
    connected_count = 0

    async def on_connected() -> None:
        nonlocal connected_count
        connected_count += 1

    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=lambda session: service,
        poll_seconds=0,
        sleep=_no_sleep,
    )

    result = await dialer.dial(
        _request(),
        call_id="failed-record-call-id",
        on_connected=on_connected,
    )

    assert result.call_result == "no_answer"
    assert result.error_message == "SIP 连接超时"
    assert connected_count == 0
    assert session_factory.sessions[0].commit_count == 1
    assert session_factory.sessions[0].rollback_count == 0
    assert len(session_factory.sessions) == 2


@pytest.mark.anyio
async def test_create_failure_after_answer_retains_ownership_until_terminal() -> None:
    answered_at = datetime(2026, 7, 28, 8, 0, tzinfo=timezone.utc)
    ended_at = datetime(2026, 7, 28, 8, 0, 3, tzinfo=timezone.utc)
    answered_record = _record(status="connected", answered_at=answered_at)
    terminal_record = _record(
        status="failed",
        end_reason="opening_failed",
        failure_message="开场白播放失败",
        answered_at=answered_at,
        ended_at=ended_at,
    )
    session_factory = _FakeSessionFactory([])
    controlled_sleep = _ControlledSleep()
    connected_count = 0

    async def on_connected() -> None:
        nonlocal connected_count
        connected_count += 1

    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=lambda session: _AnsweredThenFailingAiCallService(
            session,
            answered_record,
        ),
        poll_seconds=0.25,
        sleep=controlled_sleep,
        now=lambda: ended_at,
    )
    task = asyncio.create_task(
        dialer.dial(
            _request(),
            call_id="answered-then-failed-call-id",
            on_connected=on_connected,
        )
    )

    try:
        for _ in range(20):
            if controlled_sleep.waiting.is_set() or task.done():
                break
            await asyncio.sleep(0)

        assert connected_count == 1
        assert controlled_sleep.waiting.is_set()
        assert not task.done()

        session_factory.records.append(terminal_record)
        controlled_sleep.release.set()
        result = await task

        assert result.call_result == "connected"
        assert result.error_message is None
        assert result.duration_ms == 3000
        assert connected_count == 1
    finally:
        if not task.done():
            session_factory.records.append(terminal_record)
            controlled_sleep.release.set()
            await task


@pytest.mark.anyio
async def test_create_failure_without_record_rolls_back_failed_commit() -> None:
    session_factory = _FakeSessionFactory(
        [],
        commit_failure_indexes={0},
    )
    service = _FakeAiCallService(RuntimeError("provider exploded"))
    connected_count = 0

    async def on_connected() -> None:
        nonlocal connected_count
        connected_count += 1

    dialer = _dialer_class()(
        session_factory,
        ai_call_service_factory=lambda session: service,
        poll_seconds=0,
        sleep=_no_sleep,
    )

    result = await dialer.dial(
        _request(),
        call_id="missing-record-call-id",
        on_connected=on_connected,
    )

    assert result.call_result == "call_failed"
    assert "provider exploded" in (result.error_message or "")
    assert connected_count == 0
    assert session_factory.sessions[0].commit_count == 0
    assert session_factory.sessions[0].rollback_count == 1
    assert len(session_factory.sessions) == 2
