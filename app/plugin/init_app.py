from collections.abc import AsyncGenerator
from typing import Any

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
from app.utils.common_util import import_module, import_modules_async
from app.utils.console import console_close, console_run


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Any, Any]:
    """
    自定义 FastAPI 应用生命周期。

    参数:
    - app (FastAPI): FastAPI 应用实例。

    返回:
    - AsyncGenerator[Any, Any]: 生命周期上下文生成器。
    """
    ai_call_event_worker = await _start_ai_call_event_worker()
    ai_call_dialogue_worker = await _start_ai_call_dialogue_worker()
    ai_call_offline_asr_worker = await _start_ai_call_offline_asr_worker()
    ai_call_recording_reconcile_worker = await _start_ai_call_recording_reconcile_worker()
    ai_call_handoff_trigger_worker = await _start_ai_call_handoff_trigger_worker()
    if settings.AI_CALL_STANDALONE_ENABLE:
        try:
            log.info("✅ AI Call standalone 模式启动，跳过系统服务初始化")
            yield
            log.info("✅ AI Call standalone 模式关闭")
        finally:
            await _stop_ai_call_handoff_trigger_worker(ai_call_handoff_trigger_worker)
            await _stop_ai_call_event_worker(ai_call_event_worker)
            await _stop_ai_call_recording_reconcile_worker(ai_call_recording_reconcile_worker)
            await _stop_ai_call_offline_asr_worker(ai_call_offline_asr_worker)
            await _stop_ai_call_dialogue_worker(ai_call_dialogue_worker)
        return

    from app.api.v1.system.dict.service import DictDataService
    from app.api.v1.system.oss.service import OssService
    from app.core.ap_scheduler import SchedulerUtil

    try:
        await import_modules_async(
            modules=settings.EVENT_LIST, desc="全局事件", app=app, status=True
        )
        log.info("✅ 全局事件模块加载完成")
        await DictDataService().init_dict_service(redis=app.state.redis)
        log.info("✅ Redis数据字典初始化完成")
        await OssService.init_active_config()
        await SchedulerUtil.init_scheduler(redis=app.state.redis)
        log.info("✅ 定时任务调度器初始化完成")
        from app.common.enums import EnvironmentEnum

        console_run(
            host=settings.SERVER_HOST,
            port=settings.SERVER_PORT,
            reload=settings.ENVIRONMENT in {EnvironmentEnum.LOCAL, EnvironmentEnum.DEV},
            database_ready=True,
            redis_ready=True,
            scheduler_ready=SchedulerUtil.is_running(),
        )

    except Exception as e:
        await _stop_ai_call_handoff_trigger_worker(ai_call_handoff_trigger_worker)
        await _stop_ai_call_event_worker(ai_call_event_worker)
        await _stop_ai_call_recording_reconcile_worker(ai_call_recording_reconcile_worker)
        await _stop_ai_call_offline_asr_worker(ai_call_offline_asr_worker)
        await _stop_ai_call_dialogue_worker(ai_call_dialogue_worker)
        log.error(f"❌ 应用初始化失败: {e!s}")
        raise SystemExit(1)

    try:
        yield
    finally:
        await _stop_ai_call_handoff_trigger_worker(ai_call_handoff_trigger_worker)
        await _stop_ai_call_event_worker(ai_call_event_worker)
        await _stop_ai_call_recording_reconcile_worker(ai_call_recording_reconcile_worker)
        await _stop_ai_call_offline_asr_worker(ai_call_offline_asr_worker)
        await _stop_ai_call_dialogue_worker(ai_call_dialogue_worker)

    try:
        await import_modules_async(
            modules=settings.EVENT_LIST, desc="全局事件", app=app, status=False
        )
        log.info("✅ 全局事件模块卸载完成")
        await SchedulerUtil.shutdown(wait=False)
        log.info("✅ 定时任务调度器已关闭")
        console_close()

    except Exception as e:
        log.error(f"❌ 应用关闭过程中发生错误: {e!s}")


async def _start_ai_call_event_worker():
    if not settings.SQL_DB_ENABLE:
        return None
    from app.api.v1.ai_call.service import configure_ai_call_event_persistence
    from app.core.database import async_db_session
    from app.services.ai_call.event_persistence import AiCallEventPersistenceWorker

    worker = AiCallEventPersistenceWorker(async_db_session)
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
    )
    worker = AiCallHandoffTriggerWorker(trigger_service)
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


async def _start_ai_call_offline_asr_worker():
    if not settings.SQL_DB_ENABLE or not settings.AI_CALL_OFFLINE_ASR_ENABLED:
        return None
    from app.api.v1.ai_call.service import configure_ai_call_offline_asr
    from app.core.database import async_db_session
    from app.services.ai_call.offline_asr_service import (
        AiCallOfflineAsrWorker,
        DashScopeParaformerAsrProvider,
        parse_language_hints,
    )

    provider = DashScopeParaformerAsrProvider(
        api_key=settings.EFFECTIVE_ASR_API_KEY,
        model=settings.AI_CALL_OFFLINE_ASR_MODEL or settings.ASR_MODEL or "paraformer-v2",
        language_hints=parse_language_hints(settings.AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS),
        timeout_seconds=settings.AI_CALL_OFFLINE_ASR_TIMEOUT_SECONDS,
        poll_interval_seconds=settings.AI_CALL_OFFLINE_ASR_POLL_INTERVAL_SECONDS,
    )
    worker = AiCallOfflineAsrWorker(
        async_db_session,
        provider=provider,
        enabled=settings.AI_CALL_OFFLINE_ASR_ENABLED,
        queue_max_size=settings.AI_CALL_OFFLINE_ASR_QUEUE_MAX_SIZE,
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
