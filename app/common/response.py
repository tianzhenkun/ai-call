from collections.abc import Mapping
from typing import Any, Generic

from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from pydantic.types import T
from starlette.background import BackgroundTask

from app.common.constant import RET


class ResponseSchema(BaseModel, Generic[T]):
    """响应模型"""

    code: int = Field(default=RET.OK.code, description="业务状态码")
    msg: str = Field(default=RET.OK.msg, description="响应消息")
    data: T | None = Field(default=None, description="响应数据")


class TableResponseSchema(BaseModel, Generic[T]):
    """分页表格响应模型"""

    total: int = Field(default=0, description="总记录数")
    rows: list[T] = Field(default_factory=list, description="数据列表")
    code: int = Field(default=RET.OK.code, description="业务状态码")
    msg: str = Field(default=RET.OK.msg, description="响应消息")


def _serialize_data(data: Any) -> Any:
    """
    递归序列化数据，确保 Pydantic 模型使用驼峰别名

    参数:
    - data: 要序列化的数据

    返回:
    - Any: 序列化后的数据
    """
    return jsonable_encoder(data, by_alias=True)


class TableResponse(JSONResponse):
    """分页表格响应类"""

    def __init__(
        self,
        rows: list[Any] | None = None,
        total: int = 0,
        msg: str = RET.OK.msg,
        code: int = RET.OK.code,
        status_code: int = status.HTTP_200_OK,
    ) -> None:
        """
        初始化分页表格响应类

        参数:
        - rows (list[Any] | None): 数据列表。
        - total (int): 总记录数。
        - msg (str): 响应消息。
        - code (int): 业务状态码。
        - status_code (int): HTTP 状态码。

        返回:
        - None
        """
        content = {
            "total": total,
            "rows": _serialize_data(rows) or [],
            "code": code,
            "msg": msg,
        }
        super().__init__(content=content, status_code=status_code)


class SuccessResponse(JSONResponse):
    """成功响应类"""

    def __init__(
        self,
        data: Any | None = None,
        msg: str = RET.OK.msg,
        code: int = RET.OK.code,
        status_code: int = status.HTTP_200_OK,
    ) -> None:
        """
        初始化成功响应类

        参数:
        - data (Any | None): 响应数据。
        - msg (str): 响应消息。
        - code (int): 业务状态码。
        - status_code (int): HTTP 状态码。

        返回:
        - None
        """
        content = {
            "code": code,
            "msg": msg,
            "data": _serialize_data(data),
        }
        super().__init__(content=content, status_code=status_code)


class ErrorResponse(JSONResponse):
    """错误响应类"""

    def __init__(
        self,
        data: Any = None,
        msg: str = RET.ERROR.msg,
        code: int = RET.ERROR.code,
        status_code: int = status.HTTP_400_BAD_REQUEST,
    ) -> None:
        """
        初始化错误响应类

        参数:
        - data (Any): 响应数据。
        - msg (str): 响应消息。
        - code (int): 业务状态码。
        - status_code (int): HTTP 状态码。

        返回:
        - None
        """
        content = {
            "code": code,
            "msg": msg,
            "data": _serialize_data(data),
        }
        super().__init__(content=content, status_code=status_code)


class StreamResponse(StreamingResponse):
    """流式响应类"""

    def __init__(
        self,
        data: Any = None,
        status_code: int = status.HTTP_200_OK,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        """
        初始化流式响应类

        参数:
        - data (Any): 响应数据。
        - status_code (int): HTTP 状态码。
        - headers (Mapping[str, str] | None): 响应头。
        - media_type (str | None): 媒体类型。
        - background (BackgroundTask | None): 后台任务。

        返回:
        - None
        """
        super().__init__(
            content=data,
            status_code=status_code,
            media_type=media_type,
            headers=headers,
            background=background,
        )


class UploadFileResponse(FileResponse):
    """文件响应"""

    def __init__(
        self,
        file_path: str,
        filename: str,
        media_type: str = "application/octet-stream",
        headers: Mapping[str, str] | None = None,
        background: BackgroundTask | None = None,
        status_code: int = 200,
    ) -> None:
        """
        初始化文件响应类

        参数:
        - file_path (str): 文件路径。
        - filename (str): 文件名。
        - media_type (str): 文件类型。
        - headers (Mapping[str, str] | None): 响应头。
        - background (BackgroundTask | None): 后台任务。
        - status_code (int): HTTP 状态码。

        返回:
        - None
        """
        super().__init__(
            path=file_path,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
            filename=filename,
            stat_result=None,
            method=None,
            content_disposition_type="attachment",
        )
