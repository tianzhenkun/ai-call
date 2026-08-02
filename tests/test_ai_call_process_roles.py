from __future__ import annotations

import importlib
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI

from app.common.enums import EnvironmentEnum
from app.config.setting import Settings
from app.plugin import init_app


def test_outbound_owner_runtime_backpressure_defaults() -> None:
    assert (
        Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TENANT"].default
        == 100
    )
    assert (
        Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TASK"].default
        == 20
    )
    assert (
        Settings.model_fields["AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_LINE"].default
        == 50
    )
    assert (
        Settings.model_fields["AI_CALL_OUTBOUND_ALLOCATION_TIMEOUT_SECONDS"].default
        == 30.0
    )


def _load_roles_module() -> ModuleType:
    try:
        return importlib.import_module("app.services.ai_call.runtime_control.roles")
    except ModuleNotFoundError:
        pytest.fail("runtime_control.roles 尚未实现")


def test_parse_process_roles_rejects_unknown_and_runtime_legacy_conflict() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.parse_process_roles("api,unknown")
    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.parse_process_roles("api,runtime,legacy_runtime")


def test_parse_process_roles_accepts_only_the_frozen_role_set() -> None:
    roles = _load_roles_module()

    parsed = roles.parse_process_roles(
        "api,runtime,dispatcher,outbound,jobs",
    )

    assert parsed == frozenset(
        {
            roles.ProcessRole.API,
            roles.ProcessRole.RUNTIME,
            roles.ProcessRole.DISPATCHER,
            roles.ProcessRole.OUTBOUND,
            roles.ProcessRole.JOBS,
        }
    )


def test_parse_owner_entries_rejects_non_runtime_entries_and_accepts_frozen_entries() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError, match="sip_inbound"):
        roles.parse_owner_command_entries("sip_inbound")
    with pytest.raises(roles.RuntimeRoleConfigurationError, match="preview"):
        roles.parse_owner_command_entries("preview")

    assert roles.parse_owner_command_entries("") == frozenset()
    assert roles.parse_owner_command_entries(
        "web,direct_sip,outbound",
    ) == frozenset(
        {
            roles.OwnerCommandEntry.WEB,
            roles.OwnerCommandEntry.DIRECT_SIP,
            roles.OwnerCommandEntry.OUTBOUND,
        }
    )


@pytest.mark.parametrize("database_type", ["mysql", "sqlite"])
def test_runtime_control_roles_require_postgres(database_type: str) -> None:
    roles = _load_roles_module()
    runtime_settings = SimpleNamespace(
        AI_CALL_PROCESS_ROLES="runtime,dispatcher",
        AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
        AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
        DATABASE_TYPE=database_type,
    )

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(runtime_settings)


def test_legacy_runtime_requires_api_but_can_coexist_with_outbound_and_jobs() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="legacy_runtime",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
                AI_CALL_RUNTIME_INSTANCE_ID="",
                DATABASE_TYPE="postgres",
            )
        )

    parsed = roles.validate_runtime_role_settings(
        SimpleNamespace(
            AI_CALL_PROCESS_ROLES="api,legacy_runtime,outbound,jobs",
            AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
            AI_CALL_RUNTIME_INSTANCE_ID="",
            DATABASE_TYPE="postgres",
        )
    )
    assert roles.ProcessRole.LEGACY_RUNTIME in parsed


def test_owner_command_entries_require_api_runtime_dispatcher_roles() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="api",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="web",
                AI_CALL_RUNTIME_INSTANCE_ID="",
                DATABASE_TYPE="postgres",
            )
        )


def test_owner_command_entries_reject_legacy_runtime_and_require_outbound_role() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="api,runtime,dispatcher,legacy_runtime",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="web",
                AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
                DATABASE_TYPE="postgres",
            )
        )

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="api,runtime,dispatcher",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="outbound",
                AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
                DATABASE_TYPE="postgres",
            )
        )


def test_owner_command_entries_are_allowed_only_in_non_production_isolated_roles() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="api,runtime,dispatcher",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="web,direct_sip",
                AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
                DATABASE_TYPE="postgres",
                ENVIRONMENT=EnvironmentEnum.PROD,
            )
        )

    parsed = roles.validate_runtime_role_settings(
        SimpleNamespace(
            AI_CALL_PROCESS_ROLES="api,runtime,dispatcher",
            AI_CALL_OWNER_COMMAND_V1_ENTRIES="web,direct_sip",
            AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
            DATABASE_TYPE="postgres",
            ENVIRONMENT=EnvironmentEnum.DEV,
        )
    )
    assert parsed == frozenset(
        {
            roles.ProcessRole.API,
            roles.ProcessRole.RUNTIME,
            roles.ProcessRole.DISPATCHER,
        }
    )


def test_real_provider_mode_is_rejected_in_production_even_without_entries() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError, match="Provider"):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="api",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
                AI_CALL_RUNTIME_INSTANCE_ID="",
                AI_CALL_RUNTIME_PROVIDER_MODE="livekit",
                DATABASE_TYPE="postgres",
                ENVIRONMENT=EnvironmentEnum.PROD,
            )
        )


def test_runtime_control_mode_is_selected_per_entry_without_fallback_guessing() -> None:
    roles = _load_roles_module()
    settings = SimpleNamespace(
        AI_CALL_PROCESS_ROLES="api,runtime,dispatcher",
        AI_CALL_OWNER_COMMAND_V1_ENTRIES="web",
        AI_CALL_RUNTIME_INSTANCE_ID="runtime-a",
        DATABASE_TYPE="postgres",
        ENVIRONMENT=EnvironmentEnum.DEV,
    )

    assert roles.runtime_control_mode_for_entry(settings, "web") == "owner_command_v1"
    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.runtime_control_mode_for_entry(settings, "sip_inbound")
    with pytest.raises(roles.RuntimeRoleConfigurationError, match="preview"):
        roles.runtime_control_mode_for_entry(settings, "preview")


def test_runtime_role_requires_instance_identity() -> None:
    roles = _load_roles_module()

    with pytest.raises(roles.RuntimeRoleConfigurationError):
        roles.validate_runtime_role_settings(
            SimpleNamespace(
                AI_CALL_PROCESS_ROLES="runtime",
                AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
                AI_CALL_RUNTIME_INSTANCE_ID="",
                DATABASE_TYPE="postgres",
            )
        )


def test_owner_command_role_defaults_are_frozen() -> None:
    assert (
        Settings.model_fields["AI_CALL_PROCESS_ROLES"].default
        == "api,legacy_runtime,outbound,jobs"
    )
    assert Settings.model_fields["AI_CALL_OWNER_COMMAND_V1_ENTRIES"].default == ""


def _patch_ai_call_workers(monkeypatch: pytest.MonkeyPatch):
    async_starters = {
        name: AsyncMock(return_value=None)
        for name in (
            "_start_ai_call_runtime_control",
            "_start_ai_call_dispatcher_control",
            "_start_ai_call_recovery_control",
            "_start_ai_call_event_worker",
            "_start_ai_call_dialogue_worker",
            "_start_ai_call_semantic_analysis_worker",
            "_start_ai_call_offline_asr_worker",
            "_start_ai_call_recording_reconcile_worker",
            "_start_ai_call_handoff_trigger_worker",
            "_start_ai_call_runtime_webhook_worker",
            "_start_ai_call_outbound_task_worker",
            "_start_ai_call_linphone_test_worker",
            "_start_ai_call_voice_worker",
            "_recover_ai_call_outbound_validations",
        )
    }
    async_stoppers = {
        name: AsyncMock(return_value=None)
        for name in (
            "_stop_ai_call_runtime_control",
            "_stop_ai_call_dispatcher_control",
            "_stop_ai_call_recovery_control",
            "_stop_ai_call_event_worker",
            "_stop_ai_call_dialogue_worker",
            "_stop_ai_call_semantic_analysis_worker",
            "_stop_ai_call_offline_asr_worker",
            "_stop_ai_call_recording_reconcile_worker",
            "_stop_ai_call_handoff_trigger_worker",
            "_stop_ai_call_runtime_webhook_worker",
            "_stop_ai_call_outbound_task_worker",
            "_stop_ai_call_linphone_test_worker",
            "_stop_ai_call_voice_worker",
            "_stop_ai_call_voice_preview_service",
        )
    }
    for name, mock in {**async_starters, **async_stoppers}.items():
        monkeypatch.setattr(init_app, name, mock)
    preview_start = Mock(return_value=None)
    monkeypatch.setattr(init_app, "_start_ai_call_voice_preview_service", preview_start)
    return async_starters, async_stoppers, preview_start


@pytest.mark.anyio
async def test_api_role_does_not_start_ai_call_workers_or_require_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.system.dict.service import DictDataService
    from app.api.v1.system.oss.service import OssService
    from app.core.ap_scheduler import SchedulerUtil

    app = FastAPI()
    starters, _stoppers, preview_start = _patch_ai_call_workers(monkeypatch)
    dict_init = AsyncMock(return_value=None)
    scheduler_init = AsyncMock(return_value=None)
    scheduler_shutdown = AsyncMock(return_value=None)

    monkeypatch.setattr(
        init_app,
        "settings",
        SimpleNamespace(
            AI_CALL_PROCESS_ROLES="api",
            AI_CALL_OWNER_COMMAND_V1_ENTRIES="",
            AI_CALL_RUNTIME_INSTANCE_ID="",
            AI_CALL_STANDALONE_ENABLE=False,
            REDIS_ENABLE=False,
            DATABASE_TYPE="postgres",
            EVENT_LIST=[],
            SERVER_HOST="127.0.0.1",
            SERVER_PORT=19010,
            ENVIRONMENT=EnvironmentEnum.DEV,
        ),
    )
    monkeypatch.setattr(init_app, "import_modules_async", AsyncMock(return_value=None))
    monkeypatch.setattr(DictDataService, "init_dict_service", dict_init)
    monkeypatch.setattr(OssService, "init_active_config", AsyncMock(return_value=None))
    monkeypatch.setattr(SchedulerUtil, "init_scheduler", scheduler_init)
    monkeypatch.setattr(SchedulerUtil, "shutdown", scheduler_shutdown)
    monkeypatch.setattr(SchedulerUtil, "is_running", lambda: False)
    monkeypatch.setattr(init_app, "console_run", Mock(return_value=None))
    monkeypatch.setattr(init_app, "console_close", Mock(return_value=None))

    async with init_app.lifespan(app):
        assert not hasattr(app.state, "redis")

    for starter in starters.values():
        starter.assert_not_awaited()
    preview_start.assert_not_called()
    dict_init.assert_not_awaited()
    scheduler_init.assert_not_awaited()
    scheduler_shutdown.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("role", "expected_async", "preview_expected"),
    [
        (
            "runtime",
            {"_start_ai_call_runtime_control"},
            False,
        ),
        (
            "dispatcher",
            {
                "_start_ai_call_dispatcher_control",
                "_start_ai_call_recovery_control",
            },
            False,
        ),
        (
            "legacy_runtime",
            {"_start_ai_call_event_worker", "_start_ai_call_dialogue_worker"},
            True,
        ),
        (
            "outbound",
            {
                "_recover_ai_call_outbound_validations",
                "_start_ai_call_outbound_task_worker",
                "_start_ai_call_linphone_test_worker",
            },
            False,
        ),
        (
            "jobs",
            {
                "_start_ai_call_semantic_analysis_worker",
                "_start_ai_call_offline_asr_worker",
                "_start_ai_call_recording_reconcile_worker",
                "_start_ai_call_handoff_trigger_worker",
                "_start_ai_call_runtime_webhook_worker",
                "_start_ai_call_voice_worker",
            },
            False,
        ),
    ],
)
async def test_ai_call_worker_start_matrix(
    monkeypatch: pytest.MonkeyPatch,
    role: str,
    expected_async: set[str],
    preview_expected: bool,
) -> None:
    roles = _load_roles_module()
    app = FastAPI()
    starters, _stoppers, preview_start = _patch_ai_call_workers(monkeypatch)

    handles = await init_app._start_ai_call_role_workers(
        app,
        frozenset({roles.ProcessRole(role)}),
    )
    await init_app._stop_ai_call_role_workers(app, handles)

    for name, starter in starters.items():
        if name in expected_async:
            starter.assert_awaited_once()
        else:
            starter.assert_not_awaited()
    if preview_expected:
        preview_start.assert_called_once_with(app)
    else:
        preview_start.assert_not_called()


@pytest.mark.anyio
async def test_outbound_owner_mode_starts_starter_and_reconciler_without_sip_dialer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.ai_call.outbound.attempt_reconciler import (
        OutboundAttemptReconcileWorker,
    )
    from app.api.v1.ai_call.outbound.owner_runtime_start import (
        OwnerRuntimeOutboundStart,
    )
    from app.api.v1.ai_call.outbound.task_executor import OutboundTaskWorker

    task_started = AsyncMock(return_value=None)
    reconcile_started = AsyncMock(return_value=None)
    monkeypatch.setattr(OutboundTaskWorker, "start", task_started)
    monkeypatch.setattr(
        OutboundAttemptReconcileWorker,
        "start",
        reconcile_started,
    )
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_EXECUTOR_ENABLED", True)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_OWNER_COMMAND_V1_ENTRIES",
        "outbound",
    )
    monkeypatch.setattr(init_app.settings, "AI_CALL_OUTBOUND_DIALER_MODE", "sip")
    monkeypatch.setattr(init_app.settings, "AI_CALL_SIP_OUTBOUND_ENABLED", False)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_RUNTIME_INSTANCE_ID",
        "runtime-owner-test",
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TENANT",
        11,
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TASK",
        7,
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_LINE",
        5,
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_OUTBOUND_ALLOCATION_TIMEOUT_SECONDS",
        17.5,
        raising=False,
    )

    worker = await init_app._start_ai_call_outbound_task_worker()

    assert worker is not None
    assert isinstance(worker.executor.owner_runtime_start, OwnerRuntimeOutboundStart)
    assert worker.executor.owner_runtime_start._allocation_timeout_seconds == 17.5
    assert worker.executor.owner_queue_limits.per_tenant == 11
    assert worker.executor.owner_queue_limits.per_task == 7
    assert worker.executor.owner_queue_limits.per_line == 5
    assert worker.executor.dialer.dialer_type == "mock"
    assert worker.reconcile_worker is not None
    task_started.assert_awaited_once()
    reconcile_started.assert_awaited_once()
