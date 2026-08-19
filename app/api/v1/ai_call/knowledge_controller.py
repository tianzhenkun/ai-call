from __future__ import annotations

from collections.abc import AsyncGenerator
from functools import lru_cache
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, Header, Path, Query, Request, UploadFile
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.outbound.controller import _identity
from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, StreamResponse, SuccessResponse, TableResponse
from app.config.setting import settings
from app.core.database import async_db_session
from app.core.dependencies import get_knowledge_manager, get_knowledge_viewer
from app.core.exceptions import CustomException

from .schema import ProductInfoExtractOut

KnowledgeRouter = APIRouter(prefix="/knowledge", tags=["AI Call 知识库"])
KnowledgePromptRouter = APIRouter(tags=["AI Call 知识库"])


class KnowledgeItemPatchRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    display_name: str | None = Field(default=None, alias="displayName", max_length=255)
    content_category: str | None = Field(
        default=None,
        alias="contentCategory",
        max_length=20,
    )
    note: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def require_change(self):
        if not self.model_fields_set:
            raise ValueError("至少提供一个待更新字段")
        return self


class KnowledgeSceneBindingsRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    prompt_profile_ids: list[int] = Field(
        alias="promptProfileIds",
        max_length=100,
    )


async def get_knowledge_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_db_session() as session:
        yield session


@lru_cache(maxsize=1)
def get_knowledge_service() -> Any:
    from app.services.ai_call.knowledge import (
        KnowledgeService,
        build_cos_knowledge_store,
    )

    try:
        return KnowledgeService(
            build_cos_knowledge_store(settings),
            binary_parser_enabled=bool(
                settings.AI_CALL_KNOWLEDGE_PARSER_SOCKET.strip()
            ),
        )
    except RuntimeError as exc:
        raise CustomException(msg="知识库对象存储尚未配置", status_code=503) from exc


@lru_cache(maxsize=1)
def get_knowledge_product_info_service() -> Any:
    from app.services.ai_call.knowledge import KnowledgeProductInfoService
    from app.services.ai_call.prompt_optimization import (
        build_knowledge_product_extractor,
    )

    return KnowledgeProductInfoService(
        build_knowledge_product_extractor(
            base_url=settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL,
            api_key=settings.EFFECTIVE_LLM_API_KEY,
            model=settings.AI_CALL_PROMPT_OPTIMIZE_MODEL,
            timeout_seconds=settings.AI_CALL_PROMPT_OPTIMIZE_TIMEOUT_SECONDS,
        )
    )


def _upload_response(result: Any) -> SuccessResponse:
    response = SuccessResponse(
        data={
            "itemId": str(result.item_id),
            "versionId": str(result.version_id),
            "status": result.status,
        },
        msg="知识文件已受理",
        status_code=202 if result.status in {"UPLOADING", "PROCESSING"} else 200,
    )
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return response


async def _accept_upload(
    *,
    http_request: Request,
    auth: AuthSchema,
    db: AsyncSession,
    service: Any,
    idempotency_key: str,
    file: UploadFile,
    file_sha256: str,
    content_category: str,
    note: str | None,
    item_id: int | None = None,
) -> SuccessResponse:
    form = await http_request.form()
    if len(form.getlist("file")) != 1:
        raise CustomException(msg="每次只能上传一个文件", status_code=400)
    tenant_id, user_id = _identity(auth)
    result = await service.accept_upload(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        idempotency_key=idempotency_key,
        file=file,
        file_sha256=file_sha256,
        content_category=content_category,
        note=note,
        item_id=item_id,
    )
    return _upload_response(result)


@KnowledgeRouter.post(
    "/items/upload",
    summary="上传新知识条目",
    response_model=ResponseSchema[dict],
)
async def upload_knowledge_item_controller(
    http_request: Request,
    file: Annotated[UploadFile, File()],
    file_sha256: Annotated[str, Form(alias="fileSha256")],
    content_category: Annotated[str, Form(alias="contentCategory")],
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    note: Annotated[str | None, Form()] = None,
) -> SuccessResponse:
    return await _accept_upload(
        http_request=http_request,
        auth=auth,
        db=db,
        service=service,
        idempotency_key=idempotency_key,
        file=file,
        file_sha256=file_sha256,
        content_category=content_category,
        note=note,
    )


@KnowledgeRouter.post(
    "/items/{itemId}/versions/upload",
    summary="上传知识条目的新版本",
    response_model=ResponseSchema[dict],
)
async def upload_knowledge_version_controller(
    http_request: Request,
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    file: Annotated[UploadFile, File()],
    file_sha256: Annotated[str, Form(alias="fileSha256")],
    content_category: Annotated[str, Form(alias="contentCategory")],
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    note: Annotated[str | None, Form()] = None,
) -> SuccessResponse:
    return await _accept_upload(
        http_request=http_request,
        auth=auth,
        db=db,
        service=service,
        idempotency_key=idempotency_key,
        file=file,
        file_sha256=file_sha256,
        content_category=content_category,
        note=note,
        item_id=item_id,
    )


@KnowledgeRouter.get(
    "/items",
    summary="查询知识条目列表",
)
async def list_knowledge_items_controller(
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
    page_num: Annotated[int, Query(alias="pageNum", ge=1)] = 1,
    page_size: Annotated[int, Query(alias="pageSize", ge=1, le=100)] = 20,
) -> TableResponse:
    tenant_id, _ = _identity(auth)
    rows, total = await service.list_items(
        db,
        tenant_id=tenant_id,
        page_num=page_num,
        page_size=page_size,
    )
    return TableResponse(rows=rows, total=total, msg="查询成功")


@KnowledgeRouter.get(
    "/items/{itemId}",
    summary="查询知识条目详情",
    response_model=ResponseSchema[dict],
)
async def get_knowledge_item_controller(
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_item(db, tenant_id=tenant_id, item_id=item_id)
    return SuccessResponse(data=result, msg="查询成功")


@KnowledgeRouter.patch(
    "/items/{itemId}",
    summary="更新知识条目",
    response_model=ResponseSchema[dict],
)
async def update_knowledge_item_controller(
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    request: KnowledgeItemPatchRequest,
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    result = await service.update_item(
        db,
        tenant_id=tenant_id,
        item_id=item_id,
        changes=request.model_dump(exclude_unset=True),
    )
    return SuccessResponse(data=result, msg="更新成功")


@KnowledgeRouter.put(
    "/items/{itemId}/scene-bindings",
    summary="整组替换知识条目场景绑定",
    response_model=ResponseSchema[dict],
)
async def replace_knowledge_scene_bindings_controller(
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    request: KnowledgeSceneBindingsRequest,
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, user_id = _identity(auth)
    bindings = await service.replace_scene_bindings(
        db,
        tenant_id=tenant_id,
        item_id=item_id,
        prompt_profile_ids=request.prompt_profile_ids,
        user_id=user_id,
    )
    return SuccessResponse(data={"sceneBindings": bindings}, msg="场景绑定已更新")


@KnowledgeRouter.delete(
    "/items/{itemId}",
    summary="删除知识条目",
    response_model=ResponseSchema[dict],
)
async def delete_knowledge_item_controller(
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    result = await service.delete_item(db, tenant_id=tenant_id, item_id=item_id)
    return SuccessResponse(data=result, msg="删除成功")


@KnowledgeRouter.get(
    "/items/{itemId}/versions",
    summary="查询知识条目历史版本",
    response_model=ResponseSchema[list[dict]],
)
async def list_knowledge_versions_controller(
    item_id: Annotated[int, Path(alias="itemId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    versions = await service.list_versions(db, tenant_id=tenant_id, item_id=item_id)
    return SuccessResponse(data=versions, msg="查询成功")


@KnowledgeRouter.get(
    "/versions/{versionId}/processing",
    summary="查询知识版本处理状态",
    response_model=ResponseSchema[dict],
)
async def get_knowledge_processing_controller(
    version_id: Annotated[int, Path(alias="versionId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    result = await service.get_processing(
        db,
        tenant_id=tenant_id,
        version_id=version_id,
    )
    return SuccessResponse(data=result, msg="查询成功")


@KnowledgeRouter.get(
    "/versions/{versionId}/download",
    summary="下载知识原文件",
)
async def download_knowledge_version_controller(
    version_id: Annotated[int, Path(alias="versionId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
    range_header: Annotated[str | None, Header(alias="Range")] = None,
) -> StreamResponse:
    tenant_id, _ = _identity(auth)
    download = await service.open_download(
        db,
        tenant_id=tenant_id,
        version_id=version_id,
        range_header=range_header,
    )
    return _knowledge_file_response(download, disposition="attachment")


@KnowledgeRouter.get(
    "/versions/{versionId}/preview",
    summary="预览知识原文件",
)
async def preview_knowledge_version_controller(
    version_id: Annotated[int, Path(alias="versionId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_viewer)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
    range_header: Annotated[str | None, Header(alias="Range")] = None,
) -> StreamResponse:
    tenant_id, _ = _identity(auth)
    preview = await service.open_download(
        db,
        tenant_id=tenant_id,
        version_id=version_id,
        range_header=range_header,
    )
    extension = preview.filename.rsplit(".", 1)[-1].lower()
    if extension not in {"txt", "md", "markdown"}:
        raise CustomException(msg="该文件类型不支持在线预览", status_code=415)
    return _knowledge_file_response(
        preview,
        disposition="inline",
        media_type="text/plain",
    )


def _knowledge_file_response(
    download: Any,
    *,
    disposition: str,
    media_type: str | None = None,
) -> StreamResponse:
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(download.content_length),
        "Content-Disposition": (
            f"{disposition}; filename*=UTF-8''{quote(download.filename, safe='')}"
        ),
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "sandbox",
        "Cache-Control": "private, no-store",
    }
    if download.content_range is not None:
        headers["Content-Range"] = download.content_range
    return StreamResponse(
        data=download.body,
        status_code=download.status_code,
        headers=headers,
        media_type=media_type or download.mime_type,
    )


@KnowledgeRouter.post(
    "/versions/{versionId}/retry",
    summary="重试可恢复的知识处理失败",
    response_model=ResponseSchema[dict],
)
async def retry_knowledge_version_controller(
    version_id: Annotated[int, Path(alias="versionId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_service)],
) -> SuccessResponse:
    tenant_id, _ = _identity(auth)
    result = await service.retry(
        db,
        tenant_id=tenant_id,
        version_id=version_id,
    )
    return SuccessResponse(data=result, msg="已重新进入处理队列")


@KnowledgePromptRouter.post(
    "/prompt-profiles/{profileId}/product-info:extract",
    summary="从已绑定知识生成产品与服务草稿",
    response_model=ResponseSchema[ProductInfoExtractOut],
)
async def extract_product_info_controller(
    profile_id: Annotated[int, Path(alias="profileId", gt=0)],
    auth: Annotated[AuthSchema, Depends(get_knowledge_manager)],
    db: Annotated[AsyncSession, Depends(get_knowledge_db)],
    service: Annotated[Any, Depends(get_knowledge_product_info_service)],
) -> SuccessResponse:
    tenant_id, user_id = _identity(auth)
    result = await service.extract(
        db,
        tenant_id=tenant_id,
        prompt_profile_id=profile_id,
        user_id=user_id,
    )
    return SuccessResponse(
        data=ProductInfoExtractOut.model_validate(result),
        msg="草稿生成成功，请确认后再保存",
    )
