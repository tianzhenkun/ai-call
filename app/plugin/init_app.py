import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from fastapi import FastAPI
from fastapi.concurrency import asynccontextmanager
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.config.setting import settings
from app.core.exceptions import handle_exception
from app.core.logger import log
from app.services.ai_call.runtime_control.roles import validate_runtime_role_settings
from app.services.ai_call.runtime_control.types import ProcessRole
from app.utils.common_util import import_module, import_modules_async
from app.utils.console import console_close, console_run


@dataclass(slots=True)
class AiCallRoleWorkerHandles:
    runtime_control: Any = None
    dispatcher_control: Any = None
    recovery_control: Any = None
    event_worker: Any = None
    dialogue_worker: Any = None
    semantic_analysis_worker: Any = None
    offline_asr_worker: Any = None
    recording_reconcile_worker: Any = None
    handoff_trigger_worker: Any = None
    runtime_webhook_worker: Any = None
    outbound_task_worker: Any = None
    linphone_test_worker: Any = None
    voice_worker: Any = None
    voice_preview_started: bool = False


@dataclass(slots=True)
class SystemServiceHandles:
    events_loaded: bool = False
    scheduler_started: bool = False
    console_started: bool = False


@dataclass(slots=True)
class _OutboundOwnerRuntimeWorker:
    task_worker: Any
    reconcile_worker: Any

    @property
    def executor(self) -> Any:
        return self.task_worker.executor

    async def stop(self) -> None:
        try:
            await self.reconcile_worker.stop()
        finally:
            await self.task_worker.stop()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Any, Any]:
    roles = validate_runtime_role_settings(settings)
    role_handles = await _start_ai_call_role_workers(app, roles, start_voice=False)
    if settings.AI_CALL_STANDALONE_ENABLE:
        try:
            if ProcessRole.JOBS in roles:
                await _init_ai_call_standalone_oss_config()
                role_handles.voice_worker = await _start_ai_call_voice_worker(app)
            log.info("✅ AI Call standalone 模式启动，跳过系统服务初始化")
            yield
            log.info("✅ AI Call standalone 模式关闭")
        finally:
            await _stop_ai_call_role_workers(app, role_handles)
        return

    system_handles = SystemServiceHandles()
    try:
        if ProcessRole.API in roles:
            system_handles = await _start_system_services(app, roles, role_handles)
        elif ProcessRole.JOBS in roles:
            await _init_ai_call_standalone_oss_config()
            role_handles.voice_worker = await _start_ai_call_voice_worker(app)
    except Exception as exc:
        await _stop_ai_call_role_workers(app, role_handles)
        log.error(f"❌ 应用初始化失败: {exc!s}")
        raise SystemExit(1)

    try:
        yield
    finally:
        await _stop_ai_call_role_workers(app, role_handles)
        await _stop_system_services(app, system_handles)


async def _start_ai_call_role_workers(
    app: FastAPI,
    roles: frozenset[ProcessRole],
    *,
    start_voice: bool = True,
) -> AiCallRoleWorkerHandles:
    handles = AiCallRoleWorkerHandles()
    try:
        if ProcessRole.RUNTIME in roles:
            handles.event_worker = await _start_ai_call_event_worker(
                project_terminal_records=False,
            )
            handles.runtime_control = await _start_ai_call_runtime_control()
        if ProcessRole.DISPATCHER in roles:
            handles.dispatcher_control = await _start_ai_call_dispatcher_control()
            handles.recovery_control = await _start_ai_call_recovery_control()
        if ProcessRole.LEGACY_RUNTIME in roles:
            handles.event_worker = await _start_ai_call_event_worker()
            handles.dialogue_worker = await _start_ai_call_dialogue_worker()
            _start_ai_call_voice_preview_service(app)
            handles.voice_preview_started = True
        if ProcessRole.OUTBOUND in roles:
            await _recover_ai_call_outbound_validations()
            handles.outbound_task_worker = await _start_ai_call_outbound_task_worker()
            handles.linphone_test_worker = await _start_ai_call_linphone_test_worker()
        if ProcessRole.JOBS in roles:
            handles.semantic_analysis_worker = (
                await _start_ai_call_semantic_analysis_worker()
            )
            handles.offline_asr_worker = await _start_ai_call_offline_asr_worker()
            handles.recording_reconcile_worker = (
                await _start_ai_call_recording_reconcile_worker()
            )
            handles.handoff_trigger_worker = (
                await _start_ai_call_handoff_trigger_worker()
            )
            handles.runtime_webhook_worker = (
                await _start_ai_call_runtime_webhook_worker()
            )
            if start_voice:
                handles.voice_worker = await _start_ai_call_voice_worker(app)
    except Exception:
        await _stop_ai_call_role_workers(app, handles)
        raise
    return handles


async def _stop_ai_call_role_workers(
    app: FastAPI,
    handles: AiCallRoleWorkerHandles,
) -> None:
    if handles.recovery_control is not None:
        await _stop_ai_call_recovery_control(handles.recovery_control)
    if handles.dispatcher_control is not None:
        await _stop_ai_call_dispatcher_control(handles.dispatcher_control)
    if handles.runtime_control is not None:
        await _stop_ai_call_runtime_control(handles.runtime_control)
    if handles.voice_worker is not None:
        await _stop_ai_call_voice_worker(app)
    if handles.voice_preview_started:
        await _stop_ai_call_voice_preview_service(app)
    if handles.linphone_test_worker is not None:
        await _stop_ai_call_linphone_test_worker(handles.linphone_test_worker)
    if handles.outbound_task_worker is not None:
        await _stop_ai_call_outbound_task_worker(handles.outbound_task_worker)
    if handles.handoff_trigger_worker is not None:
        await _stop_ai_call_handoff_trigger_worker(handles.handoff_trigger_worker)
    if handles.runtime_webhook_worker is not None:
        await _stop_ai_call_runtime_webhook_worker(handles.runtime_webhook_worker)
    if handles.event_worker is not None:
        await _stop_ai_call_event_worker(handles.event_worker)
    if handles.recording_reconcile_worker is not None:
        await _stop_ai_call_recording_reconcile_worker(
            handles.recording_reconcile_worker
        )
    if handles.offline_asr_worker is not None:
        await _stop_ai_call_offline_asr_worker(handles.offline_asr_worker)
    if handles.semantic_analysis_worker is not None:
        await _stop_ai_call_semantic_analysis_worker(
            handles.semantic_analysis_worker
        )
    if handles.dialogue_worker is not None:
        await _stop_ai_call_dialogue_worker(handles.dialogue_worker)


async def _start_system_services(
    app: FastAPI,
    roles: frozenset[ProcessRole],
    role_handles: AiCallRoleWorkerHandles,
) -> SystemServiceHandles:
    from app.api.v1.system.dict.service import DictDataService
    from app.api.v1.system.oss.service import OssService
    from app.common.enums import EnvironmentEnum
    from app.core.ap_scheduler import SchedulerUtil

    handles = SystemServiceHandles()
    try:
        await import_modules_async(
            modules=settings.EVENT_LIST,
            desc="全局事件",
            app=app,
            status=True,
        )
        handles.events_loaded = True
        log.info("✅ 全局事件模块加载完成")
        if settings.REDIS_ENABLE:
            redis = getattr(app.state, "redis", None)
            if redis is None:
                raise RuntimeError("Redis 已启用，但 app.state.redis 未初始化")
            await DictDataService().init_dict_service(redis=redis)
            log.info("✅ Redis数据字典初始化完成")
        await OssService.init_active_config()
        if ProcessRole.JOBS in roles:
            role_handles.voice_worker = await _start_ai_call_voice_worker(app)
        if settings.REDIS_ENABLE:
            await SchedulerUtil.init_scheduler(redis=app.state.redis)
            handles.scheduler_started = True
            log.info("✅ 定时任务调度器初始化完成")
        console_run(
            host=settings.SERVER_HOST,
            port=settings.SERVER_PORT,
            reload=settings.ENVIRONMENT
            in {EnvironmentEnum.LOCAL, EnvironmentEnum.DEV},
            database_ready=True,
            redis_ready=settings.REDIS_ENABLE,
            scheduler_ready=(
                SchedulerUtil.is_running() if handles.scheduler_started else False
            ),
        )
        handles.console_started = True
    except Exception:
        await _stop_system_services(app, handles)
        raise
    return handles


async def _stop_system_services(
    app: FastAPI,
    handles: SystemServiceHandles,
) -> None:
    from app.core.ap_scheduler import SchedulerUtil

    try:
        if handles.events_loaded:
            await import_modules_async(
                modules=settings.EVENT_LIST,
                desc="全局事件",
                app=app,
                status=False,
            )
            log.info("✅ 全局事件模块卸载完成")
        if handles.scheduler_started:
            await SchedulerUtil.shutdown(wait=False)
            log.info("✅ 定时任务调度器已关闭")
        if handles.console_started:
            console_close()
    except Exception as exc:
        log.error(f"❌ 应用关闭过程中发生错误: {exc!s}")


async def _init_ai_call_standalone_oss_config() -> None:
    if not settings.SQL_DB_ENABLE or not (
        settings.AI_CALL_RECORDING_ENABLED or settings.AI_CALL_VOICE_WORKER_ENABLED
    ):
        return
    from app.api.v1.system.oss.service import OssService

    await OssService.init_active_config()


async def _start_ai_call_runtime_control():
    from app.core.database import async_db_session
    from app.services.ai_call.runtime_control.lifecycle import (
        start_runtime_control_lifecycle,
    )

    service = await start_runtime_control_lifecycle(settings, async_db_session)
    log.info(f"✅ AI Call DB-only Runtime 已启动: {service.worker_id}")
    return service


async def _stop_ai_call_runtime_control(service) -> None:
    await service.stop()
    log.info("✅ AI Call DB-only Runtime 已关闭")


async def _start_ai_call_dispatcher_control():
    from app.core.database import async_db_session
    from app.services.ai_call.runtime_control.lifecycle import (
        start_dispatcher_control_lifecycle,
    )

    service = await start_dispatcher_control_lifecycle(settings, async_db_session)
    log.info("✅ AI Call DB-only Dispatcher 已启动")
    return service


async def _stop_ai_call_dispatcher_control(service) -> None:
    await service.stop()
    log.info("✅ AI Call DB-only Dispatcher 已关闭")


async def _start_ai_call_recovery_control():
    from app.core.database import async_db_session
    from app.services.ai_call.runtime_control.lifecycle import (
        start_recovery_control_lifecycle,
    )

    service = await start_recovery_control_lifecycle(settings, async_db_session)
    log.info("✅ AI Call DB-only Recovery 已启动")
    return service


async def _stop_ai_call_recovery_control(service) -> None:
    await service.stop()
    log.info("✅ AI Call DB-only Recovery 已关闭")


async def _start_ai_call_runtime_webhook_worker():
    from app.services.ai_call.runtime_control.roles import (
        parse_owner_command_entries,
    )

    if (
        not settings.SQL_DB_ENABLE
        or settings.DATABASE_TYPE != "postgres"
        or not parse_owner_command_entries(
            str(settings.AI_CALL_OWNER_COMMAND_V1_ENTRIES)
        )
    ):
        return None
    from app.core.database import async_db_session
    from app.services.ai_call.runtime_control.webhook_service import (
        RuntimeWebhookWorker,
    )

    worker = RuntimeWebhookWorker(
        async_db_session,
        worker_id=f"jobs:webhook:{uuid4().hex}",
    )
    await worker.start()
    log.info("✅ AI Call Runtime webhook worker 已启动")
    return worker


async def _stop_ai_call_runtime_webhook_worker(worker) -> None:
    await worker.stop()
    log.info("✅ AI Call Runtime webhook worker 已关闭")


def _start_ai_call_voice_preview_service(app: FastAPI) -> None:
    from app.api.v1.ai_call.voice.service import get_app_voice_preview_service

    get_app_voice_preview_service(app)


async def _stop_ai_call_voice_preview_service(app: FastAPI) -> None:
    service = getattr(app.state, "voice_preview_service", None)
    if service is None:
        return
    del app.state.voice_preview_service
    await service.shutdown()
    log.info("✅ AI Call 音色试听服务已关闭")


def _active_ai_call_voice_oss_config() -> dict | None:
    from app.api.v1.system.oss.service import OssService

    return OssService.active_config()


async def _verify_ai_call_voice_worker_database(session_factory) -> None:
    from sqlalchemy import select

    from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundTaskModel
    from app.api.v1.ai_call.voice.model import (
        AiCallTenantVoiceProfileModel,
        AiCallVoiceDeletionModel,
        AiCallVoiceEnrollmentModel,
        AiCallVoiceSampleCleanupModel,
    )

    try:
        async with session_factory() as database:
            probes = (
                select(
                    AiCallTenantVoiceProfileModel.id,
                    AiCallTenantVoiceProfileModel.tenant_id,
                    AiCallTenantVoiceProfileModel.voice,
                    AiCallTenantVoiceProfileModel.target_model,
                    AiCallTenantVoiceProfileModel.status,
                    AiCallTenantVoiceProfileModel.latest_enrollment_id,
                ).limit(1),
                select(
                    AiCallVoiceEnrollmentModel.id,
                    AiCallVoiceEnrollmentModel.tenant_id,
                    AiCallVoiceEnrollmentModel.voice_profile_id,
                    AiCallVoiceEnrollmentModel.sample_object_key,
                    AiCallVoiceEnrollmentModel.status,
                    AiCallVoiceEnrollmentModel.attempt_count,
                    AiCallVoiceEnrollmentModel.lease_owner,
                    AiCallVoiceEnrollmentModel.lease_expires_at,
                ).limit(1),
                select(
                    AiCallVoiceDeletionModel.id,
                    AiCallVoiceDeletionModel.tenant_id,
                    AiCallVoiceDeletionModel.voice_profile_id,
                    AiCallVoiceDeletionModel.status,
                    AiCallVoiceDeletionModel.attempt_count,
                    AiCallVoiceDeletionModel.lease_owner,
                    AiCallVoiceDeletionModel.lease_expires_at,
                    AiCallVoiceDeletionModel.reconcile_absent_count,
                ).limit(1),
                select(
                    AiCallVoiceSampleCleanupModel.id,
                    AiCallVoiceSampleCleanupModel.tenant_id,
                    AiCallVoiceSampleCleanupModel.object_key,
                    AiCallVoiceSampleCleanupModel.status,
                    AiCallVoiceSampleCleanupModel.attempt_count,
                    AiCallVoiceSampleCleanupModel.lease_owner,
                    AiCallVoiceSampleCleanupModel.lease_expires_at,
                ).limit(1),
                select(
                    AiCallOutboundTaskModel.id,
                    AiCallOutboundTaskModel.tenant_id,
                    AiCallOutboundTaskModel.voice,
                    AiCallOutboundTaskModel.status,
                ).limit(1),
            )
            for statement in probes:
                await database.execute(statement)
            await database.rollback()
    except Exception as exc:
        raise RuntimeError("AI Call 音色任务数据库依赖不可用") from exc


def _build_ai_call_voice_runtime(
    *,
    session_factory,
    oss_config: dict | None,
):
    from app.api.v1.ai_call.voice.service import VoiceEnrollmentService
    from app.services.ai_call.providers.qwen_voice_enrollment import (
        QwenVoiceEnrollmentProvider,
    )
    from app.services.ai_call.voice_enrollment_worker import VoiceEnrollmentWorker
    from app.services.ai_call.voice_sample import MinioVoiceSampleStorage

    api_key = settings.DASHSCOPE_API_KEY.strip()
    if not api_key:
        raise RuntimeError("AI Call 音色任务已启用，但服务端 DASHSCOPE_API_KEY 未配置")
    endpoint = settings.AI_CALL_VOICE_ENROLLMENT_ENDPOINT.strip()
    target_model = settings.QWEN_REALTIME_MODEL.strip()
    sample_prefix = settings.AI_CALL_VOICE_SAMPLE_OBJECT_PREFIX.strip().strip("/")
    if not endpoint or not target_model or not sample_prefix:
        raise RuntimeError("AI Call 音色任务配置不完整")

    required_minio_fields = ("endpoint", "bucket_name", "access_key", "secret_key")
    if not oss_config or any(
        not str(oss_config.get(field) or "").strip() for field in required_minio_fields
    ):
        raise RuntimeError("AI Call 音色任务所需 MinIO 私有存储配置不完整")

    storage = MinioVoiceSampleStorage(oss_config)
    provider = QwenVoiceEnrollmentProvider(
        api_key=api_key,
        endpoint=endpoint,
        target_model=target_model,
    )
    enrollment_service = VoiceEnrollmentService(
        storage=storage,
        cleanup_session_factory=session_factory,
        target_model=target_model,
        sample_object_prefix=sample_prefix,
    )
    worker = VoiceEnrollmentWorker(
        session_factory=session_factory,
        provider=provider,
        storage=storage,
        worker_id=f"voice-worker-{uuid4().hex}",
        batch_size=settings.AI_CALL_VOICE_WORKER_BATCH_SIZE,
        lease_seconds=settings.AI_CALL_VOICE_WORKER_LEASE_SECONDS,
        poll_interval_seconds=settings.AI_CALL_VOICE_WORKER_POLL_INTERVAL_SECONDS,
        shutdown_grace_seconds=settings.AI_CALL_VOICE_WORKER_SHUTDOWN_GRACE_SECONDS,
    )
    return enrollment_service, worker


class _AiCallVoiceWorkerLifecycle:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.phase = "STOPPED"


def _ai_call_voice_worker_lifecycle(app: FastAPI) -> _AiCallVoiceWorkerLifecycle:
    lifecycle = getattr(app.state, "_ai_call_voice_worker_lifecycle", None)
    if lifecycle is None:
        lifecycle = _AiCallVoiceWorkerLifecycle()
        app.state._ai_call_voice_worker_lifecycle = lifecycle
    return lifecycle


async def _start_ai_call_voice_worker(app: FastAPI):
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_VOICE_WORKER_ENABLED:
        return None

    lifecycle = _ai_call_voice_worker_lifecycle(app)
    async with lifecycle.lock:
        existing = getattr(app.state, "voice_enrollment_worker", None)
        if existing is not None and existing.running:
            lifecycle.phase = "RUNNING"
            return existing
        if existing is not None:
            lifecycle.phase = "STOPPING"
            try:
                await existing.stop()
            finally:
                if hasattr(app.state, "voice_enrollment_worker"):
                    del app.state.voice_enrollment_worker
                if hasattr(app.state, "voice_enrollment_service"):
                    del app.state.voice_enrollment_service

        from app.core.database import async_db_session

        lifecycle.phase = "STARTING"
        worker = None
        try:
            await _verify_ai_call_voice_worker_database(async_db_session)
            enrollment_service, worker = _build_ai_call_voice_runtime(
                session_factory=async_db_session,
                oss_config=_active_ai_call_voice_oss_config(),
            )
            await worker.start()
        except BaseException:
            if worker is not None:
                try:
                    await worker.stop()
                except Exception:
                    pass
            lifecycle.phase = "STOPPED"
            raise
        app.state.voice_enrollment_service = enrollment_service
        app.state.voice_enrollment_worker = worker
        lifecycle.phase = "RUNNING"
        log.info("✅ AI Call 音色任务 worker 已启动")
        return worker


async def _stop_ai_call_voice_worker(app: FastAPI) -> None:
    lifecycle = _ai_call_voice_worker_lifecycle(app)
    async with lifecycle.lock:
        lifecycle.phase = "STOPPING"
        worker = getattr(app.state, "voice_enrollment_worker", None)
        try:
            if worker is not None:
                await worker.stop()
        finally:
            if hasattr(app.state, "voice_enrollment_worker"):
                del app.state.voice_enrollment_worker
            if hasattr(app.state, "voice_enrollment_service"):
                del app.state.voice_enrollment_service
            lifecycle.phase = "STOPPED"
    if worker is not None:
        log.info("✅ AI Call 音色任务 worker 已关闭")


async def _start_ai_call_outbound_task_worker():
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_OUTBOUND_EXECUTOR_ENABLED:
        return None
    from app.api.v1.ai_call.outbound.task_executor import (
        MockOutboundDialer,
        OutboundTaskExecutor,
        OutboundTaskWorker,
    )
    from app.core.database import async_db_session
    from app.services.ai_call.runtime_control.roles import (
        runtime_control_mode_for_entry,
    )
    from app.services.ai_call.runtime_control.types import OwnerCommandEntry

    owner_mode = (
        runtime_control_mode_for_entry(settings, OwnerCommandEntry.OUTBOUND)
        == "owner_command_v1"
    )
    owner_runtime_start = None
    owner_executor_options = {}
    if owner_mode:
        from app.api.v1.ai_call.outbound.owner_runtime_start import (
            OwnerRuntimeOutboundStart,
        )
        from app.api.v1.ai_call.outbound.queue_control import OutboundQueueLimits

        dialer = MockOutboundDialer(settings.AI_CALL_OUTBOUND_MOCK_RESULT)
        owner_runtime_start = OwnerRuntimeOutboundStart(
            allocation_timeout_seconds=(
                settings.AI_CALL_OUTBOUND_ALLOCATION_TIMEOUT_SECONDS
            )
        )
        owner_executor_options["owner_queue_limits"] = OutboundQueueLimits(
            per_tenant=settings.AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TENANT,
            per_task=settings.AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_TASK,
            per_line=settings.AI_CALL_OUTBOUND_QUEUED_LIMIT_PER_LINE,
        )
    elif settings.AI_CALL_OUTBOUND_DIALER_MODE == "sip":
        if not settings.AI_CALL_SIP_OUTBOUND_ENABLED:
            raise RuntimeError(
                "AI Call 正式 SIP 拨号模式已启用，但 SIP 外呼总开关未启用"
            )
        from app.api.v1.ai_call.outbound.sip_outbound_dialer import (
            SipOutboundDialer,
        )

        dialer = SipOutboundDialer(async_db_session)
    else:
        dialer = MockOutboundDialer(settings.AI_CALL_OUTBOUND_MOCK_RESULT)

    executor = OutboundTaskExecutor(
        async_db_session,
        dialer,
        task_batch_size=settings.AI_CALL_OUTBOUND_EXECUTOR_TASK_BATCH_SIZE,
        target_batch_size=settings.AI_CALL_OUTBOUND_EXECUTOR_TARGET_BATCH_SIZE,
        business_timezone=settings.AI_CALL_OUTBOUND_TIMEZONE,
        dialing_timeout_seconds=settings.AI_CALL_OUTBOUND_DIALING_TIMEOUT_SECONDS,
        managed_attempt_timeout_seconds=(
            settings.AI_CALL_SIP_MAX_RINGING_TIMEOUT_SECONDS
            + settings.AI_CALL_SIP_MAX_CALL_DURATION_SECONDS
            + 60
        ),
        owner_runtime_start=owner_runtime_start,
        **owner_executor_options,
    )
    task_worker = OutboundTaskWorker(
        executor,
        poll_interval_seconds=settings.AI_CALL_OUTBOUND_EXECUTOR_POLL_INTERVAL_SECONDS,
    )
    if owner_mode:
        from app.api.v1.ai_call.outbound.attempt_reconciler import (
            OutboundAttemptReconcileWorker,
        )

        reconcile_worker = OutboundAttemptReconcileWorker(
            async_db_session,
            worker_id=(
                f"outbound-reconciler:{settings.AI_CALL_RUNTIME_INSTANCE_ID}:"
                f"{uuid4().hex}"
            ),
            batch_size=settings.AI_CALL_OUTBOUND_EXECUTOR_TARGET_BATCH_SIZE,
            poll_interval_seconds=(
                settings.AI_CALL_OUTBOUND_EXECUTOR_POLL_INTERVAL_SECONDS
            ),
        )
        worker = _OutboundOwnerRuntimeWorker(task_worker, reconcile_worker)
        await task_worker.start()
        try:
            await reconcile_worker.start()
        except BaseException:
            await task_worker.stop()
            raise
    else:
        worker = task_worker
        await worker.start()
    log.warning(
        "AI Call 通用外呼执行器已启动，"
        f"dialer_type={dialer.dialer_type}, owner_mode={owner_mode}"
    )
    return worker


async def _stop_ai_call_outbound_task_worker(worker) -> None:
    if worker is None:
        return
    await worker.stop()
    log.info("✅ AI Call 通用外呼执行器已关闭")


async def _start_ai_call_linphone_test_worker():
    if (
        not settings.SQL_DB_ENABLE
        or not settings.AI_CALL_OUTBOUND_LINPHONE_TEST_ENABLED
    ):
        return None
    from app.api.v1.ai_call.outbound.linphone_test_service import (
        LinphoneTestRecoveryWorker,
        LinphoneTestService,
    )
    from app.core.database import async_db_session

    service = LinphoneTestService(async_db_session)
    worker = LinphoneTestRecoveryWorker(
        service,
        poll_interval_seconds=settings.AI_CALL_OUTBOUND_LINPHONE_POLL_SECONDS,
    )
    await worker.start()
    log.warning(
        "AI Call 本机 Linphone 测试恢复 worker 已启动；"
        "普通任务自动执行器保持独立开关"
    )
    return worker


async def _stop_ai_call_linphone_test_worker(worker) -> None:
    if worker is None:
        return
    await worker.stop()
    log.info("✅ AI Call 本机 Linphone 测试恢复 worker 已关闭")


async def _start_ai_call_event_worker(*, project_terminal_records: bool = True):
    if not settings.SQL_DB_ENABLE:
        return None
    from app.api.v1.ai_call.service import configure_ai_call_event_persistence
    from app.core.database import async_db_session
    from app.services.ai_call.event_persistence import AiCallEventPersistenceWorker

    worker = AiCallEventPersistenceWorker(
        async_db_session,
        project_terminal_records=project_terminal_records,
    )
    await worker.start()
    configure_ai_call_event_persistence(worker)
    log.info("✅ AI Call 事件后台持久化 worker 已启动")
    return worker


async def _stop_ai_call_event_worker(worker) -> None:
    from app.api.v1.ai_call.service import configure_ai_call_event_persistence

    configure_ai_call_event_persistence(None)
    if worker is None:
        return
    worker.detach_all()
    await worker.stop()
    log.info("✅ AI Call 事件后台持久化 worker 已关闭")


async def _start_ai_call_dialogue_worker():
    if not settings.SQL_DB_ENABLE:
        return None
    from app.api.v1.ai_call.service import configure_ai_call_dialogue_persistence
    from app.core.database import async_db_session
    from app.services.ai_call.dialogue_service import AiCallDialoguePersistenceWorker

    worker = AiCallDialoguePersistenceWorker(async_db_session)
    await worker.start()
    configure_ai_call_dialogue_persistence(worker)
    log.info("✅ AI Call 对话文本后台持久化 worker 已启动")
    return worker


async def _stop_ai_call_dialogue_worker(worker) -> None:
    from app.api.v1.ai_call.service import configure_ai_call_dialogue_persistence

    configure_ai_call_dialogue_persistence(None)
    if worker is None:
        return
    worker.detach_all()
    await worker.stop()
    log.info("✅ AI Call 对话文本后台持久化 worker 已关闭")


async def _start_ai_call_handoff_trigger_worker():
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED:
        return None
    from app.api.v1.ai_call.service import (
        configure_ai_call_handoff_trigger,
        get_default_ai_call_service,
    )
    from app.core.database import async_db_session
    from app.services.ai_call.handoff_availability_service import (
        AiCallHandoffAvailabilityService,
    )
    from app.services.ai_call.handoff_trigger_service import (
        AiCallHandoffTriggerService,
        AiCallHandoffTriggerWorker,
        build_default_handoff_intent_classifier,
    )

    classifier = build_default_handoff_intent_classifier(
        base_url=settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL,
        api_key=settings.EFFECTIVE_LLM_API_KEY,
        model=settings.LLM_MODEL or settings.POST_ANALYSIS_MODEL or "qwen-plus",
        timeout_seconds=settings.AI_CALL_HANDOFF_INTENT_TIMEOUT_SECONDS,
    )
    trigger_service = AiCallHandoffTriggerService(
        async_db_session,
        get_default_ai_call_service,
        classifier,
        enabled=settings.AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED,
        customer_intent_enabled=settings.AI_CALL_HANDOFF_CUSTOMER_INTENT_ENABLED,
        threshold=settings.AI_CALL_HANDOFF_INTENT_THRESHOLD,
        timeout_seconds=settings.AI_CALL_HANDOFF_INTENT_TIMEOUT_SECONDS,
        availability_service_factory=AiCallHandoffAvailabilityService,
    )
    worker = AiCallHandoffTriggerWorker(trigger_service, transcript_trigger_enabled=True)
    await worker.start()
    configure_ai_call_handoff_trigger(worker)
    log.info("✅ AI Call 转人工自动触发 worker 已启动")
    return worker


async def _stop_ai_call_handoff_trigger_worker(worker) -> None:
    from app.api.v1.ai_call.service import configure_ai_call_handoff_trigger

    configure_ai_call_handoff_trigger(None)
    if worker is None:
        return
    worker.detach_all()
    await worker.stop()
    log.info("✅ AI Call 转人工自动触发 worker 已关闭")


async def _start_ai_call_semantic_analysis_worker():
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_SEMANTIC_ANALYSIS_ENABLED:
        return None
    from app.api.v1.ai_call.service import configure_ai_call_semantic_analysis
    from app.core.database import async_db_session
    from app.services.ai_call.semantic_analysis import (
        AiCallSemanticAnalysisWorker,
        build_default_semantic_analyzer,
    )

    analyzer = build_default_semantic_analyzer(
        base_url=settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL,
        api_key=settings.EFFECTIVE_POST_ANALYSIS_API_KEY or settings.EFFECTIVE_LLM_API_KEY,
        model=(
            settings.AI_CALL_SEMANTIC_ANALYSIS_MODEL
            or settings.POST_ANALYSIS_MODEL
            or settings.LLM_MODEL
            or "qwen-plus"
        ),
        timeout_seconds=settings.AI_CALL_SEMANTIC_ANALYSIS_TIMEOUT_SECONDS,
    )
    if analyzer is None:
        configure_ai_call_semantic_analysis(None)
        log.warning("AI Call 语义分析 worker 未启动：模型 base_url/api_key/model 未完整配置")
        return None

    worker = AiCallSemanticAnalysisWorker(
        async_db_session,
        analyzer=analyzer,
        enabled=settings.AI_CALL_SEMANTIC_ANALYSIS_ENABLED,
        queue_max_size=settings.AI_CALL_SEMANTIC_ANALYSIS_QUEUE_MAX_SIZE,
    )
    await worker.start()
    configure_ai_call_semantic_analysis(worker)
    log.info("✅ AI Call 语义分析 worker 已启动")
    return worker


async def _stop_ai_call_semantic_analysis_worker(worker) -> None:
    from app.api.v1.ai_call.service import configure_ai_call_semantic_analysis

    configure_ai_call_semantic_analysis(None)
    if worker is None:
        return
    await worker.stop()
    log.info("✅ AI Call 语义分析 worker 已关闭")


async def _start_ai_call_offline_asr_worker():
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_OFFLINE_ASR_ENABLED:
        return None
    from app.api.v1.ai_call.service import (
        configure_ai_call_offline_asr,
        enqueue_ai_call_semantic_analysis,
    )
    from app.core.database import async_db_session
    from app.services.ai_call.offline_asr_service import (
        AiCallOfflineAsrWorker,
        build_dashscope_offline_asr_provider,
        parse_language_hints,
    )

    provider = build_dashscope_offline_asr_provider(
        provider_name=settings.AI_CALL_OFFLINE_ASR_PROVIDER,
        api_key=settings.EFFECTIVE_ASR_API_KEY,
        model=settings.AI_CALL_OFFLINE_ASR_MODEL,
        language_hints=parse_language_hints(settings.AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS),
        timeout_seconds=settings.AI_CALL_OFFLINE_ASR_TIMEOUT_SECONDS,
        poll_interval_seconds=settings.AI_CALL_OFFLINE_ASR_POLL_INTERVAL_SECONDS,
    )
    worker = AiCallOfflineAsrWorker(
        async_db_session,
        provider=provider,
        enabled=settings.AI_CALL_OFFLINE_ASR_ENABLED,
        queue_max_size=settings.AI_CALL_OFFLINE_ASR_QUEUE_MAX_SIZE,
        on_call_ready_for_semantic_analysis=enqueue_ai_call_semantic_analysis,
    )
    await worker.start()
    configure_ai_call_offline_asr(worker)
    log.info("✅ AI Call 离线 ASR worker 已启动")
    return worker


async def _stop_ai_call_offline_asr_worker(worker) -> None:
    from app.api.v1.ai_call.service import configure_ai_call_offline_asr

    configure_ai_call_offline_asr(None)
    if worker is None:
        return
    await worker.stop()
    log.info("✅ AI Call 离线 ASR worker 已关闭")


async def _start_ai_call_recording_reconcile_worker():
    if (
        not settings.SQL_DB_ENABLE
        or not settings.AI_CALL_RECORDING_ENABLED
        or not settings.AI_CALL_RECORDING_RECONCILE_ENABLED
    ):
        return None
    from app.api.v1.ai_call.crud import AiCallRecordRepository
    from app.api.v1.ai_call.service import enqueue_ai_call_offline_asr
    from app.core.database import async_db_session
    from app.services.ai_call.recording_service import (
        AiCallRecordingReconcileWorker,
        AiCallRecordingService,
    )

    def service_factory(repository: AiCallRecordRepository) -> AiCallRecordingService:
        return AiCallRecordingService(
            repository,
            enabled=settings.AI_CALL_RECORDING_ENABLED,
            participant_recording_enabled=settings.AI_CALL_PARTICIPANT_RECORDING_ENABLED,
            verify_deadline_seconds=settings.AI_CALL_RECORDING_VERIFY_DEADLINE_SECONDS,
        )

    worker = AiCallRecordingReconcileWorker(
        async_db_session,
        service_factory,
        enabled=settings.AI_CALL_RECORDING_RECONCILE_ENABLED,
        interval_seconds=settings.AI_CALL_RECORDING_RECONCILE_INTERVAL_SECONDS,
        batch_size=settings.AI_CALL_RECORDING_RECONCILE_BATCH_SIZE,
        on_call_ready_for_asr=enqueue_ai_call_offline_asr,
    )
    await worker.start()
    log.info("✅ AI Call 录音对账 worker 已启动")
    return worker


async def _stop_ai_call_recording_reconcile_worker(worker) -> None:
    if worker is None:
        return
    await worker.stop()
    log.info("✅ AI Call 录音对账 worker 已关闭")


async def _recover_ai_call_outbound_validations() -> None:
    if not settings.SQL_DB_ENABLE:
        return
    from app.api.v1.ai_call.outbound.controller import get_outbound_validation_service

    await get_outbound_validation_service().recover_pending()
    log.info("✅ AI Call 通用外呼名单校验恢复扫描完成")


def register_middlewares(app: FastAPI) -> None:
    """
    注册全局中间件。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - None
    """
    for middleware in settings.MIDDLEWARE_LIST[::-1]:
        if not middleware:
            continue
        middleware = import_module(middleware, desc="中间件")
        app.add_middleware(middleware)


def register_exceptions(app: FastAPI) -> None:
    """
    统一注册异常处理器。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - None
    """
    handle_exception(app)


def register_routers(app: FastAPI) -> None:
    """
    注册根路由。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - None
    """
    from app.api.v1.ai_call import AiCallRouter

    app.include_router(AiCallRouter)
    if settings.AI_CALL_STANDALONE_ENABLE:
        return

    from app.api.v1.system import system_router

    app.include_router(system_router)

    from app.core.discover import get_dynamic_router

    app.include_router(router=get_dynamic_router())


def register_files(app: FastAPI) -> None:
    """
    注册静态资源挂载和文件相关配置。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - None
    """
    if settings.STATIC_ENABLE:
        settings.STATIC_ROOT.mkdir(parents=True, exist_ok=True)
        app.mount(
            path=settings.STATIC_URL,
            app=StaticFiles(directory=settings.STATIC_ROOT),
            name=settings.STATIC_DIR,
        )


def reset_api_docs(app: FastAPI) -> None:
    """
    使用本地静态资源自定义 API 文档页面（Swagger UI）。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - None
    """

    @app.get(settings.DOCS_URL, include_in_schema=False)
    async def custom_swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=str(app.root_path) + str(app.openapi_url),
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url=settings.SWAGGER_JS_URL,
            swagger_css_url=settings.SWAGGER_CSS_URL,
            swagger_favicon_url=settings.FAVICON_URL,
        )

    @app.get(str(app.swagger_ui_oauth2_redirect_url), include_in_schema=False)
    async def swagger_ui_redirect():
        return get_swagger_ui_oauth2_redirect_html()
