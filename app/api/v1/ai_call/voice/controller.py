from __future__ import annotations

from typing import Annotated, Any, Protocol

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    Header,
    Path,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, StringConstraints, ValidationError

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse, TableResponse
from app.core.database import async_db_session
from app.core.dependencies import get_current_user, get_voice_manager
from app.core.exceptions import CustomException
from app.services.ai_call.orchestrator import (
    BrowserEventReportResult,
    CreateSessionResult,
    EndSessionResult,
)
from app.services.ai_call.voice_profile import (
    QWEN_OMNI_REALTIME_TARGET_MODEL,
)

from ..outbound.controller import _identity
from ..schema import CreateSessionOut, EndSessionOut, EventOut
from .repository import VoiceRepository
from .schema import (
    VoiceEnrollmentAcceptedOut,
    VoiceEnrollmentRequest,
    VoiceStatus,
)
from .service import (
    VoiceDeletionService,
    VoiceEnrollmentService,
    VoicePreviewService,
    get_app_voice_preview_service,
)

VoiceRouter = APIRouter(tags=["租户音色管理"])


class VoiceLifecycleService(Protocol):
    async def get_enrollment(
        self,
        *,
        tenant_id: str,
        enrollment_id: int,
    ) -> Any: ...

    async def create_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        voice: str,
    ) -> Any: ...

    async def ready_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        call_id: str,
    ) -> Any: ...

    async def close_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        call_id: str,
    ) -> Any: ...

    async def deletion_check(
        self,
        *,
        tenant_id: str,
        profile_id: int,
    ) -> Any: ...

    async def request_deletion(
        self,
        *,
        tenant_id: str,
        user_id: int,
        profile_id: int,
        idempotency_key: str,
    ) -> Any: ...


class VoicePreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    voice: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=128),
    ]


class _UnavailableVoiceEnrollmentService:
    async def create(self, *args, **kwargs):
        self._raise_unavailable()

    async def reenroll(self, *args, **kwargs):
        self._raise_unavailable()

    @staticmethod
    def _raise_unavailable() -> None:
        raise CustomException(
            msg="音色复刻功能未启用",
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )


class _UnavailableVoiceLifecycleService:
    async def get_enrollment(self, **kwargs):
        self._raise_unavailable()

    async def create_preview_session(self, **kwargs):
        self._raise_unavailable()

    async def ready_preview_session(self, **kwargs):
        self._raise_unavailable()

    async def close_preview_session(self, **kwargs):
        self._raise_unavailable()

    async def deletion_check(self, **kwargs):
        self._raise_unavailable()

    async def request_deletion(self, **kwargs):
        self._raise_unavailable()

    @staticmethod
    def _raise_unavailable() -> None:
        raise CustomException(
            msg="音色生命周期服务尚未接入",
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
        )


class _DefaultVoiceLifecycleService(_UnavailableVoiceLifecycleService):
    def __init__(
        self,
        *,
        db: Any,
        preview_service: VoicePreviewService | None = None,
        deletion_service: VoiceDeletionService | None = None,
    ) -> None:
        self.db = db
        self.preview_service = preview_service
        self.deletion_service = deletion_service

    async def create_preview_session(self, **kwargs):
        if self.preview_service is None:
            self._raise_unavailable()
        return await self.preview_service.create_preview_session(
            self.db,
            **kwargs,
        )

    async def ready_preview_session(self, **kwargs):
        if self.preview_service is None:
            self._raise_unavailable()
        return await self.preview_service.ready_preview_session(**kwargs)

    async def close_preview_session(self, **kwargs):
        if self.preview_service is None:
            self._raise_unavailable()
        return await self.preview_service.close_preview_session(**kwargs)

    async def deletion_check(self, **kwargs):
        if self.deletion_service is None:
            self._raise_unavailable()
        return await self.deletion_service.deletion_check(self.db, **kwargs)

    async def request_deletion(self, **kwargs):
        if self.deletion_service is None:
            self._raise_unavailable()
        return await self.deletion_service.request_deletion(self.db, **kwargs)


_unavailable_enrollment_service = _UnavailableVoiceEnrollmentService()
_unavailable_lifecycle_service = _UnavailableVoiceLifecycleService()


def get_voice_enrollment_service(
    request: Request,
) -> VoiceEnrollmentService | _UnavailableVoiceEnrollmentService:
    return getattr(
        request.app.state,
        "voice_enrollment_service",
        _unavailable_enrollment_service,
    )


def get_voice_lifecycle_service(
    request: Request,
) -> VoiceLifecycleService:
    return getattr(
        request.app.state,
        "voice_lifecycle_service",
        _unavailable_lifecycle_service,
    )


def get_voice_preview_lifecycle_service(
    request: Request,
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    configured_service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_lifecycle_service),
    ],
) -> VoiceLifecycleService:
    if configured_service is not _unavailable_lifecycle_service:
        return configured_service
    return _DefaultVoiceLifecycleService(
        db=auth.db,
        preview_service=get_app_voice_preview_service(request.app),
    )


def get_voice_deletion_lifecycle_service(
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    configured_service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_lifecycle_service),
    ],
) -> VoiceLifecycleService:
    if configured_service is not _unavailable_lifecycle_service:
        return configured_service
    return _DefaultVoiceLifecycleService(
        db=auth.db,
        deletion_service=VoiceDeletionService(session_factory=async_db_session),
    )


def _parse_enrollment_request(value: str) -> VoiceEnrollmentRequest:
    try:
        return VoiceEnrollmentRequest.model_validate_json(value)
    except ValidationError as exc:
        first_error = exc.errors()[0].get("msg", "request JSON 不合法")
        raise CustomException(
            msg=f"音色复刻请求不合法：{first_error}",
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from exc


@VoiceRouter.get(
    "/voice-profiles",
    summary="查询租户可见音色列表",
)
async def list_voice_profiles_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    voice_type: Annotated[str | None, Query(alias="voiceType")] = None,
    gender: Annotated[str | None, Query()] = None,
    target_model: Annotated[
        str,
        Query(alias="targetModel"),
    ] = QWEN_OMNI_REALTIME_TARGET_MODEL,
    voice_status: Annotated[
        VoiceStatus | None,
        Query(alias="status"),
    ] = None,
    available_only: Annotated[
        bool,
        Query(alias="availableOnly"),
    ] = False,
    include_deleted: Annotated[
        bool,
        Query(alias="includeDeleted"),
    ] = False,
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[
        int,
        Query(alias="pageSize", ge=1, le=1000),
    ] = 20,
) -> JSONResponse:
    if not available_only:
        await get_voice_manager(auth)
    tenant_id, _user_id = _identity(auth)
    rows, total = await VoiceRepository(auth.db).list_profiles(
        tenant_id=tenant_id,
        target_model=target_model,
        voice_type=voice_type,
        gender=gender,
        status=voice_status,
        available_only=available_only,
        include_deleted=include_deleted,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@VoiceRouter.post(
    "/voice-enrollments",
    summary="创建自定义复刻音色",
    response_model=ResponseSchema[VoiceEnrollmentAcceptedOut],
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_voice_enrollment_controller(
    file: Annotated[UploadFile, File()],
    request_json: Annotated[str, Form(alias="request")],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceEnrollmentService | _UnavailableVoiceEnrollmentService,
        Depends(get_voice_enrollment_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    accepted = await service.create(
        auth.db,
        tenant_id=tenant_id,
        user_id=user_id,
        idempotency_key=idempotency_key,
        request=_parse_enrollment_request(request_json),
        sample=file,
    )
    return SuccessResponse(
        data=accepted,
        msg="音色复刻任务已受理",
        status_code=status.HTTP_202_ACCEPTED,
    )


@VoiceRouter.post(
    "/tenant-voice-profiles/{id}/enrollments",
    summary="重新上传失败音色样本",
    response_model=ResponseSchema[VoiceEnrollmentAcceptedOut],
    status_code=status.HTTP_202_ACCEPTED,
)
async def reenroll_voice_controller(
    profile_id: Annotated[int, Path(alias="id", gt=0)],
    file: Annotated[UploadFile, File()],
    request_json: Annotated[str, Form(alias="request")],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceEnrollmentService | _UnavailableVoiceEnrollmentService,
        Depends(get_voice_enrollment_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    accepted = await service.reenroll(
        auth.db,
        tenant_id=tenant_id,
        user_id=user_id,
        profile_id=profile_id,
        idempotency_key=idempotency_key,
        request=_parse_enrollment_request(request_json),
        sample=file,
    )
    return SuccessResponse(
        data=accepted,
        msg="音色复刻任务已受理",
        status_code=status.HTTP_202_ACCEPTED,
    )


@VoiceRouter.get(
    "/voice-enrollments/{id}",
    summary="查询音色复刻任务",
)
async def get_voice_enrollment_controller(
    enrollment_id: Annotated[int, Path(alias="id", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, _user_id = _identity(auth)
    result = await service.get_enrollment(
        tenant_id=tenant_id,
        enrollment_id=enrollment_id,
    )
    return SuccessResponse(data=result, msg="查询成功")


@VoiceRouter.post(
    "/voice-preview-sessions",
    summary="创建隔离音色试听会话",
)
async def create_voice_preview_controller(
    request: VoicePreviewRequest,
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_preview_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.create_preview_session(
        tenant_id=tenant_id,
        user_id=user_id,
        voice=request.voice,
    )
    response_data = (
        CreateSessionOut.model_validate(result)
        if isinstance(result, CreateSessionResult)
        else result
    )
    return SuccessResponse(data=response_data, msg="试听会话创建成功")


@VoiceRouter.post(
    "/voice-preview-sessions/{callId}/ready",
    summary="标记音色试听会话已就绪",
)
async def ready_voice_preview_controller(
    call_id: Annotated[str, Path(alias="callId", min_length=1)],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_preview_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.ready_preview_session(
        tenant_id=tenant_id,
        user_id=user_id,
        call_id=call_id,
    )
    response_data = (
        EventOut.model_validate(result) if isinstance(result, BrowserEventReportResult) else result
    )
    return SuccessResponse(data=response_data, msg="试听会话已就绪")


@VoiceRouter.delete(
    "/voice-preview-sessions/{callId}",
    summary="关闭音色试听会话",
)
async def close_voice_preview_controller(
    call_id: Annotated[str, Path(alias="callId", min_length=1)],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_preview_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.close_preview_session(
        tenant_id=tenant_id,
        user_id=user_id,
        call_id=call_id,
    )
    response_data = (
        EndSessionOut.model_validate(result) if isinstance(result, EndSessionResult) else result
    )
    return SuccessResponse(data=response_data, msg="试听会话已关闭")


@VoiceRouter.get(
    "/tenant-voice-profiles/{id}/deletion-check",
    summary="检查租户音色是否允许删除",
)
async def check_voice_deletion_controller(
    profile_id: Annotated[int, Path(alias="id", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_deletion_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, _user_id = _identity(auth)
    result = await service.deletion_check(
        tenant_id=tenant_id,
        profile_id=profile_id,
    )
    return SuccessResponse(data=result, msg="检查成功")


@VoiceRouter.delete(
    "/tenant-voice-profiles/{id}",
    summary="异步删除租户音色",
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_tenant_voice_controller(
    profile_id: Annotated[int, Path(alias="id", gt=0)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    auth: Annotated[AuthSchema, Depends(get_voice_manager)],
    service: Annotated[
        VoiceLifecycleService,
        Depends(get_voice_deletion_lifecycle_service),
    ],
) -> JSONResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.request_deletion(
        tenant_id=tenant_id,
        user_id=user_id,
        profile_id=profile_id,
        idempotency_key=idempotency_key,
    )
    return SuccessResponse(
        data=result,
        msg="音色删除任务已受理",
        status_code=status.HTTP_202_ACCEPTED,
    )
