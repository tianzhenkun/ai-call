import time

from starlette.datastructures import MutableHeaders
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint,
)
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.common.response import ErrorResponse
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.core.logger import log


class ReachHttpConnectionMiddleware:
    """普通 JSON 响应结束后关闭 HTTP/1.1 连接，避免网关复用临近过期的连接。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("http_version") != "1.1":
            await self.app(scope, receive, send)
            return

        async def send_response(message: Message) -> None:
            if message["type"] == "http.response.start" and message["status"] != 101:
                headers = MutableHeaders(scope=message)
                media_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
                # 使用已知长度的普通 JSON；流、附件和升级连接保持原协议。
                if (
                    (media_type == "application/json" or media_type.endswith("+json"))
                    and "content-length" in headers
                    and "content-disposition" not in headers
                    and not any(name.lower() == b"upgrade" for name, _ in scope["headers"])
                ):
                    headers["connection"] = "close"
            await send(message)

        await self.app(scope, receive, send_response)


class CustomCORSMiddleware(CORSMiddleware):
    """CORS跨域中间件"""

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(
            app,
            allow_origins=settings.ALLOW_ORIGINS,
            allow_methods=settings.ALLOW_METHODS,
            allow_headers=settings.ALLOW_HEADERS,
            allow_credentials=settings.ALLOW_CREDENTIALS,
            expose_headers=settings.CORS_EXPOSE_HEADERS,
        )


class RequestLogMiddleware(BaseHTTPMiddleware):
    """
    记录请求日志中间件
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()

        log_fields = (
            f"请求来源: {request.client.host if request.client else '未知'},"
            f"请求方法: {request.method},"
            f"请求路径: {request.url.path}"
        )
        log.info(log_fields)

        try:
            response = await call_next(request)

            process_time = round(time.time() - start_time, 5)
            response.headers["X-Process-Time"] = str(process_time)

            content_length = response.headers.get("content-length", "0")
            response_info = f"响应状态: {response.status_code}, 响应内容长度: {content_length}, 处理时间: {round(process_time * 1000, 3)}ms"
            log.info(response_info)

            return response

        except CustomException as e:
            log.error(f"中间件处理异常: {e!s}")
            return ErrorResponse(msg="系统异常，请联系管理员", data=str(e))


class CustomGZipMiddleware(GZipMiddleware):
    """GZip压缩中间件"""

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(
            app,
            minimum_size=settings.GZIP_MIN_SIZE,
            compresslevel=settings.GZIP_COMPRESS_LEVEL,
        )
