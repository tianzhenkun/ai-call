from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ResponseSchema, SuccessResponse
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException

from .controller import ai_call_db_getter
from .outbound.controller import _identity
from .statistics_repository import OutboundStatisticsRepository
from .statistics_schema import OutboundStatisticsOut, StatisticsGranularity
from .statistics_service import OutboundStatisticsService

OutboundStatisticsRouter = APIRouter(tags=["AI Call 外呼统计"])


def get_outbound_statistics_service(
    db: Annotated[AsyncSession, Depends(ai_call_db_getter)],
) -> OutboundStatisticsService:
    return OutboundStatisticsService(OutboundStatisticsRepository(db))


@OutboundStatisticsRouter.get(
    "/outbound-statistics",
    summary="查询正式外呼统计",
    response_model=ResponseSchema[OutboundStatisticsOut],
)
async def get_outbound_statistics_controller(
    started_at_begin: Annotated[datetime, Query(alias="startedAtBegin")],
    started_at_end: Annotated[datetime, Query(alias="startedAtEnd")],
    time_zone: Annotated[str, Query(alias="timeZone", min_length=1)],
    granularity: Annotated[StatisticsGranularity, Query()],
    auth: Annotated[AuthSchema, Depends(get_current_user)],
    service: Annotated[
        OutboundStatisticsService,
        Depends(get_outbound_statistics_service),
    ],
) -> JSONResponse:
    tenant_id, _ = _identity(auth)
    try:
        result = await service.get_statistics(
            tenant_id=tenant_id,
            started_at_begin=started_at_begin,
            started_at_end=started_at_end,
            time_zone=time_zone,
            granularity=granularity,
        )
    except ValueError as exc:
        raise CustomException(msg=str(exc), status_code=400) from exc
    return SuccessResponse(data=result, msg="查询成功")
