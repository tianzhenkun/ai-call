import time
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Response
from fastapi.routing import APIRoute

from app.core.logger import log

"""
在 FastAPI 中，route_class 参数用于自定义路由的行为。
通过设置 route_class，你可以定义一个自定义的路由类，从而在每个路由处理之前或之后执行特定的操作。
这对于日志记录、权限验证、性能监控等场景非常有用。
"""


class OperationLogRoute(APIRoute):
    """操作日志路由装饰器 (已简化，仅记录日志到控制台)"""

    def get_route_handler(
        self,
    ) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """
        自定义路由处理程序,在每个路由处理之前或之后执行特定的操作。

        参数:
        - request (Request): FastAPI请求对象。

        返回:
        - Response: FastAPI响应对象。
        """
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            """
            自定义路由处理程序
            """
            start_time = time.time()
            # 请求前的处理
            response: Response = await original_route_handler(request)

            # 请求后的处理
            process_time = time.time() - start_time
            log.info(f"Request: {request.method} {request.url.path} - Status: {response.status_code} - Time: {process_time:.4f}s")

            return response

        return custom_route_handler
