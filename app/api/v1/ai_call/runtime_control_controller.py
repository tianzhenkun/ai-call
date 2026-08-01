from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.v1.ai_call.schema import (
    RuntimeBootstrapOut,
    RuntimeEndCallOut,
    RuntimeEndCallRequest,
    RuntimeStartCallOut,
    RuntimeStartCallRequest,
    TokenOut,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.constant import RET
from app.common.response import ResponseSchema, SuccessResponse
from app.config.setting import settings
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException
from app.services.ai_call.livekit_room import LiveKitRoomManager
from app.services.ai_call.runtime_control.bootstrap_service import (
    RuntimeBootstrapLegacyError,
    RuntimeBootstrapNotFoundError,
    RuntimeBootstrapService,
)
from app.services.ai_call.runtime_control.command_repository import (
    EndCallIntent,
    IdempotencyConflictError,
    RuntimeCommandRepository,
    RuntimeControlModeError,
    RuntimeRecordNotFoundError,
)
from app.services.ai_call.runtime_control.entry_start_service import (
    RuntimeEntryStartService,
    StartEntryRequest,
)
from app.services.ai_call.runtime_control.runtime_token_service import (
    RuntimeTokenGateError,
    RuntimeTokenGateRepository,
    RuntimeTokenNotFoundError,
    RuntimeTokenService,
)

RuntimeEntryRouter = APIRouter(tags=["智能外呼运行时"])


@RuntimeEntryRouter.post(
    "/runtime/start-call",
    summary="异步受理 AI Call START_CALL",
    response_model=ResponseSchema[RuntimeStartCallOut],
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_runtime_start_call_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    request: RuntimeStartCallRequest,
):
    tenant_id = str(getattr(getattr(auth, "user", None), "tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")

    snapshot = await RuntimeEntryStartService(
        settings=settings,
        repository=RuntimeCommandRepository(auth.db),
    ).submit(
        StartEntryRequest(
            tenant_id=tenant_id,
            entry_type=request.entry_type,
            idempotency_key=request.idempotency_key,
            payload=request.payload,
            business_type=request.business_type,
            business_id=request.business_id,
            scene_code=request.scene_code,
            prompt_source_key=request.prompt_source_key,
            allocation_deadline_at=request.allocation_deadline_at,
            sensitive_payload_ciphertext=request.sensitive_payload_ciphertext,
            payload_key_version=request.payload_key_version,
        )
    )
    if snapshot is None:
        raise CustomException(
            msg="该入口仍由 legacy_local 承载",
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
            data={"errorCode": "LEGACY_ENTRY_ACTIVE"},
        )

    return SuccessResponse(
        data=RuntimeStartCallOut(
            command_id=str(snapshot.command_id),
            call_id=snapshot.call_id,
            command_seq=str(snapshot.command_seq),
            command_type=snapshot.command_type,
            status=snapshot.status,
        ),
        msg="START_CALL 已受理",
        status_code=status.HTTP_202_ACCEPTED,
    )


@RuntimeEntryRouter.post(
    "/runtime/calls/{call_id}/end",
    summary="异步受理 AI Call END_CALL",
    response_model=ResponseSchema[RuntimeEndCallOut],
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_runtime_end_call_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    call_id: str,
    request: RuntimeEndCallRequest,
):
    tenant_id = str(getattr(getattr(auth, "user", None), "tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")

    try:
        decision = await RuntimeCommandRepository(auth.db).request_end(
            EndCallIntent(
                tenant_id=tenant_id,
                call_id=call_id,
                source="web_client",
                end_reason=request.end_reason,
                dedupe_key=request.dedupe_key,
            )
        )
    except RuntimeRecordNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeControlModeError, IdempotencyConflictError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return SuccessResponse(
        data=RuntimeEndCallOut(
            call_id=decision.call_id,
            command_id=str(decision.command_id),
            command_seq=str(decision.command_seq),
            command_status=decision.command_status,
        ),
        msg="END_CALL 已受理",
        status_code=status.HTTP_202_ACCEPTED,
    )


@RuntimeEntryRouter.get(
    "/runtime/calls/{call_id}/bootstrap",
    summary="读取 AI Call owner runtime 启动闸门",
    response_model=ResponseSchema[RuntimeBootstrapOut],
)
async def get_runtime_bootstrap_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    call_id: str,
):
    tenant_id = str(getattr(getattr(auth, "user", None), "tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")

    try:
        snapshot = await RuntimeBootstrapService(auth.db).get(
            tenant_id=tenant_id,
            call_id=call_id,
        )
    except RuntimeBootstrapNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeBootstrapLegacyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return SuccessResponse(
        data=RuntimeBootstrapOut.model_validate(snapshot),
        msg="runtime bootstrap 状态已读取",
    )


@RuntimeEntryRouter.post(
    "/runtime/calls/{call_id}/token",
    summary="签发 AI Call owner runtime 浏览器 Token",
    response_model=ResponseSchema[TokenOut],
)
async def create_runtime_token_controller(
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    call_id: str,
):
    tenant_id = str(getattr(getattr(auth, "user", None), "tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="租户上下文缺失")
    if not (
        settings.LIVEKIT_URL
        and settings.LIVEKIT_API_KEY
        and settings.LIVEKIT_API_SECRET
        and settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS > 0
    ):
        raise CustomException(
            msg="LiveKit Token 签名配置不可用",
            code=RET.ERROR.code,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            data={"errorCode": "TOKEN_SIGNER_UNAVAILABLE"},
        )

    try:
        token = await RuntimeTokenService(
            repository=RuntimeTokenGateRepository(auth.db),
            room_manager=LiveKitRoomManager(
                settings.LIVEKIT_URL,
                settings.LIVEKIT_API_KEY,
                settings.LIVEKIT_API_SECRET,
                settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS,
            ),
        ).issue_browser_token(tenant_id=tenant_id, call_id=call_id)
    except RuntimeTokenNotFoundError as exc:
        raise CustomException(
            msg=str(exc),
            code=RET.ERROR.code,
            status_code=status.HTTP_404_NOT_FOUND,
            data={"errorCode": exc.error_code},
        ) from exc
    except RuntimeTokenGateError as exc:
        raise CustomException(
            msg=str(exc),
            code=RET.ERROR.code,
            status_code=status.HTTP_409_CONFLICT,
            data={"errorCode": exc.error_code},
        ) from exc

    return SuccessResponse(
        data=TokenOut.model_validate(token),
        msg="runtime Token 签发成功",
    )
