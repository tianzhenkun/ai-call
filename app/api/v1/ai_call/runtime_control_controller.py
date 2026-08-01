from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.v1.ai_call.schema import (
    RuntimeStartCallOut,
    RuntimeStartCallRequest,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse
from app.config.setting import settings
from app.core.dependencies import get_current_user
from app.services.ai_call.runtime_control.command_repository import (
    RuntimeCommandRepository,
)
from app.services.ai_call.runtime_control.entry_start_service import (
    RuntimeEntryStartService,
    StartEntryRequest,
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
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="该入口仍由 legacy_local 承载",
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
