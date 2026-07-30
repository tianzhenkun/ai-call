from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI

from app.config.setting import Settings
from app.plugin import init_app
from app.services.ai_call import voice_enrollment_worker as worker_module
from app.services.ai_call.orchestrator import AiCallRuntimeConfig
from app.services.ai_call.providers.qwen_voice_enrollment import VoiceListItem
from app.services.ai_call.voice_enrollment_worker import (
    CleanupWorkItem,
    DeletionWorkItem,
    EnrollmentWorkItem,
    VoiceEnrollmentWorker,
)


class _FakeWorker:
    def __init__(self) -> None:
        self.running = False
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self.running = True

    async def stop(self) -> None:
        self.stop_calls += 1
        self.running = False


def _worker(
    run_once: Callable[[], Awaitable[None]],
    *,
    poll_interval_seconds: float = 3600,
    shutdown_grace_seconds: float | None = None,
) -> VoiceEnrollmentWorker:
    kwargs: dict[str, float] = {}
    if shutdown_grace_seconds is not None:
        kwargs["shutdown_grace_seconds"] = shutdown_grace_seconds
    worker = VoiceEnrollmentWorker(
        session_factory=lambda: None,
        provider=object(),
        storage=object(),
        worker_id="test-worker",
        batch_size=10,
        lease_seconds=60,
        poll_interval_seconds=poll_interval_seconds,
        **kwargs,
    )
    worker.run_once = run_once
    return worker


def test_voice_worker_settings_are_safe_by_default() -> None:
    config = Settings()

    assert config.AI_CALL_VOICE_WORKER_ENABLED is False
    assert config.AI_CALL_VOICE_WORKER_BATCH_SIZE == 10
    assert config.AI_CALL_VOICE_WORKER_POLL_INTERVAL_SECONDS == 2.0
    assert config.AI_CALL_VOICE_WORKER_LEASE_SECONDS == 60
    assert config.AI_CALL_VOICE_WORKER_SHUTDOWN_GRACE_SECONDS == 30.0
    assert config.AI_CALL_VOICE_SAMPLE_OBJECT_PREFIX == "ai-call/voice-samples"
    assert not hasattr(config, "AI_CALL_VOICE_TARGET_MODEL")
    assert config.AI_CALL_VOICE_ENROLLMENT_ENDPOINT.startswith("https://")


@pytest.mark.anyio
async def test_worker_start_is_idempotent_and_stop_wakes_poll() -> None:
    calls = 0
    completed = asyncio.Event()

    async def run_once() -> None:
        nonlocal calls
        calls += 1
        completed.set()

    worker = _worker(run_once)

    await worker.start()
    await worker.start()
    await asyncio.wait_for(completed.wait(), timeout=1)
    await asyncio.wait_for(worker.stop(), timeout=0.2)
    await worker.stop()

    assert calls == 1
    assert worker.running is False
    assert worker.last_success is not None
    assert worker.last_error_type is None


@pytest.mark.anyio
async def test_worker_stop_waits_for_current_run_once_without_leaving_task() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def run_once() -> None:
        entered.set()
        await release.wait()

    worker = _worker(run_once)
    await worker.start()
    await asyncio.wait_for(entered.wait(), timeout=1)

    stop_task = asyncio.create_task(worker.stop())
    await asyncio.sleep(0)
    assert stop_task.done() is False

    release.set()
    await asyncio.wait_for(stop_task, timeout=1)

    assert worker.running is False
    assert worker._runner_task is None


@pytest.mark.anyio
async def test_worker_stop_cancels_blocked_run_after_shutdown_grace() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def run_once() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    worker = _worker(run_once, shutdown_grace_seconds=0.01)
    await worker.start()
    await asyncio.wait_for(entered.wait(), timeout=1)

    await asyncio.wait_for(worker.stop(), timeout=1)

    assert cancelled.is_set()
    assert worker.running is False
    assert worker._runner_task is None


@pytest.mark.anyio
async def test_worker_survives_run_once_error_without_logging_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    recovered = asyncio.Event()
    error_log = Mock()

    async def run_once() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("server-secret-value")
        recovered.set()

    monkeypatch.setattr(worker_module.log, "error", error_log)
    worker = _worker(run_once, poll_interval_seconds=0.01)

    await worker.start()
    await asyncio.wait_for(recovered.wait(), timeout=1)
    await worker.stop()

    assert calls >= 2
    assert worker.last_success is not None
    assert worker.last_error_type == "RuntimeError"
    assert "server-secret-value" not in repr(error_log.call_args_list)


@pytest.mark.anyio
async def test_worker_loop_propagates_cancelled_error() -> None:
    async def run_once() -> None:
        raise asyncio.CancelledError

    worker = _worker(run_once)

    with pytest.raises(asyncio.CancelledError):
        await worker._run_loop()


@pytest.mark.anyio
async def test_run_once_reports_isolated_item_error_without_marking_round_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_processed = asyncio.Event()

    async def process(item: object) -> None:
        if item == "bad":
            raise RuntimeError("provider-secret")
        first_processed.set()

    worker = VoiceEnrollmentWorker(
        session_factory=lambda: None,
        provider=object(),
        storage=object(),
        worker_id="test-worker",
    )
    monkeypatch.setattr(worker, "_cleanup_terminal_samples", AsyncMock())
    monkeypatch.setattr(
        worker,
        "_claim_enrollments",
        AsyncMock(return_value=["bad", "good"]),
    )
    monkeypatch.setattr(worker, "_process_enrollment", process)
    monkeypatch.setattr(worker, "_claim_deletions", AsyncMock(return_value=[]))
    monkeypatch.setattr(worker, "_claim_cleanup_records", AsyncMock(return_value=[]))

    succeeded = await worker.run_once()

    assert succeeded is False
    assert first_processed.is_set()
    assert worker.last_success is None
    assert worker.last_error_type == "RuntimeError"


@pytest.mark.anyio
async def test_run_once_propagates_cancelled_item() -> None:
    async def process(_item: object) -> None:
        raise asyncio.CancelledError

    worker = VoiceEnrollmentWorker(
        session_factory=lambda: None,
        provider=object(),
        storage=object(),
        worker_id="test-worker",
    )
    worker._cleanup_terminal_samples = AsyncMock()
    worker._claim_enrollments = AsyncMock(return_value=[object()])
    worker._process_enrollment = process

    with pytest.raises(asyncio.CancelledError):
        await worker.run_once()


@pytest.mark.anyio
async def test_stop_signal_prevents_new_create_delete_and_cleanup_calls() -> None:
    provider = SimpleNamespace(
        create=AsyncMock(),
        delete=AsyncMock(),
        list=AsyncMock(),
    )
    storage = SimpleNamespace(get=AsyncMock(), delete=AsyncMock())
    worker = VoiceEnrollmentWorker(
        session_factory=lambda: None,
        provider=provider,
        storage=storage,
        worker_id="test-worker",
    )
    worker._stop_event.set()
    now = datetime.now(timezone.utc)

    await worker._process_enrollment(
        EnrollmentWorkItem(
            id=1,
            tenant_id="tenant-a",
            voice_profile_id=2,
            preferred_name="vcname",
            target_model="qwen-shared",
            sample_object_key="sample.wav",
            status="PROCESSING",
            attempt_count=1,
            lease_owner="test-worker",
            lease_expires_at=now + timedelta(seconds=60),
        )
    )
    await worker._process_deletion(
        DeletionWorkItem(
            id=3,
            tenant_id="tenant-a",
            voice_profile_id=2,
            voice="qwen-voice",
            target_model="qwen-shared",
            status="PROCESSING",
            attempt_count=1,
            reconcile_absent_count=0,
            lease_owner="test-worker",
            lease_expires_at=now + timedelta(seconds=60),
        )
    )
    await worker._process_cleanup_record(
        CleanupWorkItem(
            id=4,
            tenant_id="tenant-a",
            object_key="orphan.wav",
            attempt_count=1,
            lease_owner="test-worker",
        )
    )

    provider.create.assert_not_awaited()
    provider.delete.assert_not_awaited()
    provider.list.assert_not_awaited()
    storage.get.assert_not_awaited()
    storage.delete.assert_not_awaited()


@pytest.mark.anyio
async def test_stop_signal_prevents_next_reconciliation_page() -> None:
    provider = SimpleNamespace(list=AsyncMock())
    worker = VoiceEnrollmentWorker(
        session_factory=lambda: None,
        provider=provider,
        storage=object(),
        worker_id="test-worker",
    )
    item = DeletionWorkItem(
        id=3,
        tenant_id="tenant-a",
        voice_profile_id=2,
        voice="missing-voice",
        target_model="qwen-shared",
        status="RECONCILING",
        attempt_count=1,
        reconcile_absent_count=0,
        lease_owner="test-worker",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
    )

    async def first_page(**_kwargs):
        worker._stop_event.set()
        return [
            VoiceListItem(
                voice=f"other-{index}",
                target_model="qwen-shared",
                gmt_create=None,
            )
            for index in range(worker_module.DEFAULT_LIST_PAGE_SIZE)
        ]

    provider.list.side_effect = first_page

    state = await worker._provider_deletion_state(item)

    assert state == "INCOMPLETE"
    assert provider.list.await_count == 1


@pytest.mark.anyio
async def test_voice_runtime_uses_server_dependencies_and_unique_worker_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "server-dashscope-secret"

    def session_factory():
        return None

    oss_config = {
        "endpoint": "minio.internal:9000",
        "bucket_name": "private-ai-call",
        "access_key": "server-access",
        "secret_key": "server-storage-secret",
    }
    monkeypatch.setattr(init_app.settings, "DASHSCOPE_API_KEY", secret)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_ENROLLMENT_ENDPOINT",
        "https://dashscope.example.test/customization",
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "QWEN_REALTIME_MODEL",
        "qwen-shared-model",
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_SAMPLE_OBJECT_PREFIX",
        "private/voice-samples",
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_BATCH_SIZE",
        10,
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_POLL_INTERVAL_SECONDS",
        2.0,
        raising=False,
    )
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_LEASE_SECONDS",
        60,
        raising=False,
    )
    service, worker = init_app._build_ai_call_voice_runtime(
        session_factory=session_factory,
        oss_config=oss_config,
    )
    _, second_worker = init_app._build_ai_call_voice_runtime(
        session_factory=session_factory,
        oss_config=oss_config,
    )

    assert worker.session_factory is session_factory
    assert worker.provider.api_key == secret
    assert worker.provider.endpoint == "https://dashscope.example.test/customization"
    assert worker.provider.target_model == "qwen-shared-model"
    assert service.target_model == "qwen-shared-model"
    assert (
        AiCallRuntimeConfig.from_settings(init_app.settings).qwen_realtime_model
        == "qwen-shared-model"
    )
    assert worker.storage is service.storage
    assert service.sample_object_prefix == "private/voice-samples"
    assert worker.batch_size == 10
    assert worker.poll_interval_seconds == 2.0
    assert worker.lease_seconds == 60
    assert worker.shutdown_grace_seconds == 30.0
    assert worker.worker_id != second_worker.worker_id
    request = AsyncMock(
        return_value=({"output": {"voice": "qwen-voice"}}, "request-id")
    )
    monkeypatch.setattr(worker.provider, "_request", request)

    await worker.provider.create(
        preferred_name="vcname",
        audio_data_url="data:audio/wav;base64,AA==",
    )

    assert request.await_args.args[0]["target_model"] == "qwen-shared-model"


@pytest.mark.parametrize(
    ("api_key", "oss_config", "message"),
    [
        ("", {"endpoint": "minio", "bucket_name": "bucket"}, "DASHSCOPE_API_KEY"),
        (
            "server-key",
            None,
            "MinIO",
        ),
        (
            "server-key",
            {"endpoint": "minio", "bucket_name": "bucket"},
            "MinIO",
        ),
    ],
)
def test_voice_runtime_rejects_missing_server_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    api_key: str,
    oss_config: dict | None,
    message: str,
) -> None:
    monkeypatch.setattr(init_app.settings, "DASHSCOPE_API_KEY", api_key)

    with pytest.raises(RuntimeError, match=message):
        init_app._build_ai_call_voice_runtime(
            session_factory=lambda: None,
            oss_config=oss_config,
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql_enabled", "worker_enabled"),
    [(False, True), (True, False)],
)
async def test_voice_runtime_does_not_start_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    sql_enabled: bool,
    worker_enabled: bool,
) -> None:
    app = FastAPI()
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", sql_enabled)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_ENABLED",
        worker_enabled,
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_build_ai_call_voice_runtime",
        lambda **kwargs: pytest.fail("disabled runtime must not be built"),
        raising=False,
    )

    result = await init_app._start_ai_call_voice_worker(app)

    assert result is None
    assert not hasattr(app.state, "voice_enrollment_worker")
    assert not hasattr(app.state, "voice_enrollment_service")


@pytest.mark.anyio
async def test_voice_runtime_db_failure_aborts_startup_without_app_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_ENABLED",
        True,
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_verify_ai_call_voice_worker_database",
        AsyncMock(side_effect=RuntimeError("voice worker database unavailable")),
        raising=False,
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        await init_app._start_ai_call_voice_worker(app)

    assert not hasattr(app.state, "voice_enrollment_worker")
    assert not hasattr(app.state, "voice_enrollment_service")


@pytest.mark.anyio
async def test_database_probe_wraps_original_error_without_secret() -> None:
    class FailingFactory:
        def __call__(self):
            raise RuntimeError("postgres://user:database-secret@db/ai_call")

    with pytest.raises(RuntimeError) as exc_info:
        await init_app._verify_ai_call_voice_worker_database(FailingFactory())

    assert str(exc_info.value) == "AI Call 音色任务数据库依赖不可用"
    assert "database-secret" not in str(exc_info.value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "missing_dependency",
    [
        "ai_call_tenant_voice_profile.target_model",
        "ai_call_voice_enrollment.lease_owner",
        "ai_call_voice_deletion.reconcile_absent_count",
        "ai_call_voice_sample_cleanup.object_key",
        "ai_call_outbound_task.voice",
        "ai_call_outbound_task.status",
        "ai_call_outbound_task.tenant_id",
    ],
)
async def test_database_probe_checks_every_worker_dependency(
    missing_dependency: str,
) -> None:
    class ProbeSession:
        async def _probe(self, statement):
            sql = str(statement).lower()
            table, column = missing_dependency.split(".")
            if table in sql and column in sql:
                raise RuntimeError(f"missing secret column {missing_dependency}")

        async def execute(self, statement):
            return await self._probe(statement)

        async def scalar(self, statement):
            return await self._probe(statement)

        async def rollback(self) -> None:
            return None

    class ProbeContext(AbstractAsyncContextManager):
        async def __aenter__(self):
            return ProbeSession()

        async def __aexit__(self, exc_type, exc, traceback):
            return None

    with pytest.raises(
        RuntimeError,
        match="AI Call 音色任务数据库依赖不可用",
    ) as exc_info:
        await init_app._verify_ai_call_voice_worker_database(
            lambda: ProbeContext()
        )

    assert "missing secret" not in str(exc_info.value)


@pytest.mark.anyio
async def test_start_passes_real_server_session_factory_without_logging_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.core import database as database_module

    app = FastAPI()
    server_factory = object()
    service = object()
    worker = _FakeWorker()
    build_calls: list[dict] = []
    info_log = Mock()
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_VOICE_WORKER_ENABLED", True)
    monkeypatch.setattr(database_module, "async_db_session", server_factory)
    monkeypatch.setattr(
        init_app,
        "_verify_ai_call_voice_worker_database",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        init_app,
        "_active_ai_call_voice_oss_config",
        lambda: {"secret_key": "storage-secret"},
    )

    def build_runtime(**kwargs):
        build_calls.append(kwargs)
        return service, worker

    monkeypatch.setattr(init_app, "_build_ai_call_voice_runtime", build_runtime)
    monkeypatch.setattr(init_app.log, "info", info_log)

    await init_app._start_ai_call_voice_worker(app)
    await init_app._stop_ai_call_voice_worker(app)

    assert build_calls[0]["session_factory"] is server_factory
    assert "storage-secret" not in repr(info_log.call_args_list)


@pytest.mark.anyio
async def test_concurrent_voice_runtime_start_builds_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    worker = _FakeWorker()
    probe_entered = asyncio.Event()
    release_probe = asyncio.Event()
    build_runtime = Mock(return_value=(object(), worker))
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_VOICE_WORKER_ENABLED", True)

    async def probe(_session_factory) -> None:
        probe_entered.set()
        await release_probe.wait()

    monkeypatch.setattr(init_app, "_verify_ai_call_voice_worker_database", probe)
    monkeypatch.setattr(init_app, "_build_ai_call_voice_runtime", build_runtime)

    first = asyncio.create_task(init_app._start_ai_call_voice_worker(app))
    await asyncio.wait_for(probe_entered.wait(), timeout=1)
    second = asyncio.create_task(init_app._start_ai_call_voice_worker(app))
    release_probe.set()
    first_worker, second_worker = await asyncio.gather(first, second)

    assert first_worker is worker
    assert second_worker is worker
    assert build_runtime.call_count == 1
    assert worker.start_calls == 1

    await init_app._stop_ai_call_voice_worker(app)


@pytest.mark.anyio
async def test_stop_during_probe_waits_and_stops_final_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    worker = _FakeWorker()
    probe_entered = asyncio.Event()
    release_probe = asyncio.Event()
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_VOICE_WORKER_ENABLED", True)

    async def probe(_session_factory) -> None:
        probe_entered.set()
        await release_probe.wait()

    monkeypatch.setattr(init_app, "_verify_ai_call_voice_worker_database", probe)
    monkeypatch.setattr(
        init_app,
        "_build_ai_call_voice_runtime",
        Mock(return_value=(object(), worker)),
    )

    start_task = asyncio.create_task(init_app._start_ai_call_voice_worker(app))
    await asyncio.wait_for(probe_entered.wait(), timeout=1)
    stop_task = asyncio.create_task(init_app._stop_ai_call_voice_worker(app))
    await asyncio.sleep(0)
    assert stop_task.done() is False

    release_probe.set()
    await asyncio.gather(start_task, stop_task)

    assert worker.start_calls == 1
    assert worker.stop_calls == 1
    assert not hasattr(app.state, "voice_enrollment_worker")
    assert not hasattr(app.state, "voice_enrollment_service")


@pytest.mark.anyio
async def test_voice_runtime_start_failure_leaves_no_half_assembled_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    worker = _FakeWorker()
    worker.start = AsyncMock(side_effect=RuntimeError("start secret"))
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_VOICE_WORKER_ENABLED", True)
    monkeypatch.setattr(
        init_app,
        "_verify_ai_call_voice_worker_database",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        init_app,
        "_build_ai_call_voice_runtime",
        Mock(return_value=(object(), worker)),
    )

    with pytest.raises(RuntimeError, match="start secret"):
        await init_app._start_ai_call_voice_worker(app)

    assert not hasattr(app.state, "voice_enrollment_worker")
    assert not hasattr(app.state, "voice_enrollment_service")


@pytest.mark.anyio
async def test_voice_runtime_is_scoped_to_app_and_cleans_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_app = FastAPI()
    second_app = FastAPI()
    runtimes: list[tuple[object, _FakeWorker]] = [
        (object(), _FakeWorker()),
        (object(), _FakeWorker()),
    ]
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(
        init_app.settings,
        "AI_CALL_VOICE_WORKER_ENABLED",
        True,
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_verify_ai_call_voice_worker_database",
        AsyncMock(return_value=None),
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_active_ai_call_voice_oss_config",
        lambda: {"endpoint": "minio"},
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_build_ai_call_voice_runtime",
        lambda **kwargs: runtimes.pop(0),
        raising=False,
    )

    first = await init_app._start_ai_call_voice_worker(first_app)
    repeated = await init_app._start_ai_call_voice_worker(first_app)
    second = await init_app._start_ai_call_voice_worker(second_app)

    assert first is repeated
    assert first is not second
    assert first_app.state.voice_enrollment_service is not (
        second_app.state.voice_enrollment_service
    )

    await init_app._stop_ai_call_voice_worker(first_app)
    await init_app._stop_ai_call_voice_worker(second_app)

    assert not hasattr(first_app.state, "voice_enrollment_worker")
    assert not hasattr(first_app.state, "voice_enrollment_service")
    assert not hasattr(second_app.state, "voice_enrollment_worker")
    assert not hasattr(second_app.state, "voice_enrollment_service")


@pytest.mark.anyio
async def test_voice_runtime_can_start_again_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    first_worker = _FakeWorker()
    second_worker = _FakeWorker()
    runtimes = [(object(), first_worker), (object(), second_worker)]
    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_VOICE_WORKER_ENABLED", True)
    monkeypatch.setattr(
        init_app,
        "_verify_ai_call_voice_worker_database",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        init_app,
        "_build_ai_call_voice_runtime",
        Mock(side_effect=lambda **_kwargs: runtimes.pop(0)),
    )

    await init_app._start_ai_call_voice_worker(app)
    await init_app._stop_ai_call_voice_worker(app)
    await init_app._start_ai_call_voice_worker(app)
    await init_app._stop_ai_call_voice_worker(app)

    assert first_worker.start_calls == first_worker.stop_calls == 1
    assert second_worker.start_calls == second_worker.stop_calls == 1


def _patch_existing_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    start_names = [
        "_start_ai_call_event_worker",
        "_start_ai_call_dialogue_worker",
        "_start_ai_call_semantic_analysis_worker",
        "_start_ai_call_offline_asr_worker",
        "_start_ai_call_recording_reconcile_worker",
        "_start_ai_call_handoff_trigger_worker",
        "_start_ai_call_outbound_task_worker",
        "_start_ai_call_linphone_test_worker",
    ]
    stop_names = [
        "_stop_ai_call_event_worker",
        "_stop_ai_call_dialogue_worker",
        "_stop_ai_call_semantic_analysis_worker",
        "_stop_ai_call_offline_asr_worker",
        "_stop_ai_call_recording_reconcile_worker",
        "_stop_ai_call_handoff_trigger_worker",
        "_stop_ai_call_outbound_task_worker",
        "_stop_ai_call_linphone_test_worker",
    ]
    for name in start_names:
        monkeypatch.setattr(init_app, name, AsyncMock(return_value=None))
    for name in stop_names:
        monkeypatch.setattr(init_app, name, AsyncMock(return_value=None))
    monkeypatch.setattr(
        init_app,
        "_recover_ai_call_outbound_validations",
        AsyncMock(return_value=None),
    )


@pytest.mark.anyio
async def test_standalone_lifespan_starts_and_stops_voice_before_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    calls: list[str] = []
    worker = object()
    _patch_existing_workers(monkeypatch)
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", True)
    monkeypatch.setattr(
        init_app,
        "_init_ai_call_standalone_oss_config",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        init_app,
        "_start_ai_call_voice_worker",
        AsyncMock(side_effect=lambda _app: calls.append("voice-start") or worker),
        raising=False,
    )

    async def stop_voice(_app: FastAPI) -> None:
        calls.append("voice-stop")

    monkeypatch.setattr(
        init_app,
        "_stop_ai_call_voice_worker",
        stop_voice,
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_start_ai_call_voice_preview_service",
        lambda _app: calls.append("preview-start"),
    )

    async def stop_preview(_app: FastAPI) -> None:
        calls.append("preview-stop")

    monkeypatch.setattr(init_app, "_stop_ai_call_voice_preview_service", stop_preview)

    async with init_app.lifespan(app):
        assert calls == ["preview-start", "voice-start"]

    assert calls == [
        "preview-start",
        "voice-start",
        "voice-stop",
        "preview-stop",
    ]


@pytest.mark.anyio
async def test_normal_lifespan_stops_voice_on_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.system.dict.service import DictDataService
    from app.api.v1.system.oss.service import OssService
    from app.core.ap_scheduler import SchedulerUtil

    app = FastAPI()
    app.state.redis = object()
    calls: list[str] = []
    _patch_existing_workers(monkeypatch)
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", False)
    monkeypatch.setattr(init_app, "import_modules_async", AsyncMock(return_value=None))
    monkeypatch.setattr(DictDataService, "init_dict_service", AsyncMock(return_value=None))
    monkeypatch.setattr(OssService, "init_active_config", AsyncMock(return_value=None))
    monkeypatch.setattr(
        SchedulerUtil,
        "init_scheduler",
        AsyncMock(side_effect=RuntimeError("scheduler failed")),
    )
    monkeypatch.setattr(
        init_app,
        "_start_ai_call_voice_worker",
        AsyncMock(side_effect=lambda _app: calls.append("voice-start") or object()),
        raising=False,
    )

    async def stop_voice(_app: FastAPI) -> None:
        calls.append("voice-stop")

    monkeypatch.setattr(
        init_app,
        "_stop_ai_call_voice_worker",
        stop_voice,
        raising=False,
    )
    monkeypatch.setattr(init_app, "_start_ai_call_voice_preview_service", lambda _app: None)
    monkeypatch.setattr(
        init_app,
        "_stop_ai_call_voice_preview_service",
        AsyncMock(return_value=None),
    )

    with pytest.raises(SystemExit):
        async with init_app.lifespan(app):
            pytest.fail("initialization failure must not yield")

    assert calls == ["voice-start", "voice-stop"]


@pytest.mark.anyio
async def test_normal_lifespan_finally_stops_voice_before_preview_and_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.api.v1.system.dict.service import DictDataService
    from app.api.v1.system.oss.service import OssService
    from app.core.ap_scheduler import SchedulerUtil

    app = FastAPI()
    app.state.redis = object()
    calls: list[str] = []
    _patch_existing_workers(monkeypatch)
    monkeypatch.setattr(init_app.settings, "AI_CALL_STANDALONE_ENABLE", False)

    async def modules(*args, status: bool, **kwargs) -> None:
        del args, kwargs
        calls.append("dependencies-start" if status else "dependencies-stop")

    monkeypatch.setattr(init_app, "import_modules_async", modules)
    monkeypatch.setattr(DictDataService, "init_dict_service", AsyncMock(return_value=None))
    monkeypatch.setattr(OssService, "init_active_config", AsyncMock(return_value=None))
    monkeypatch.setattr(SchedulerUtil, "init_scheduler", AsyncMock(return_value=None))
    monkeypatch.setattr(SchedulerUtil, "is_running", lambda: True)
    monkeypatch.setattr(SchedulerUtil, "shutdown", AsyncMock(return_value=None))
    monkeypatch.setattr(init_app, "console_run", lambda **kwargs: None)
    monkeypatch.setattr(init_app, "console_close", lambda: None)
    monkeypatch.setattr(
        init_app,
        "_start_ai_call_voice_worker",
        AsyncMock(side_effect=lambda _app: calls.append("voice-start") or object()),
        raising=False,
    )

    async def stop_voice(_app: FastAPI) -> None:
        calls.append("voice-stop")

    monkeypatch.setattr(
        init_app,
        "_stop_ai_call_voice_worker",
        stop_voice,
        raising=False,
    )
    monkeypatch.setattr(
        init_app,
        "_start_ai_call_voice_preview_service",
        lambda _app: calls.append("preview-start"),
    )

    async def stop_preview(_app: FastAPI) -> None:
        calls.append("preview-stop")

    monkeypatch.setattr(init_app, "_stop_ai_call_voice_preview_service", stop_preview)

    async with init_app.lifespan(app):
        assert "voice-start" in calls

    assert calls.index("voice-stop") < calls.index("preview-stop")
    assert calls.index("voice-stop") < calls.index("dependencies-stop")
