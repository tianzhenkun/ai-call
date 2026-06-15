from fastapi import status


class AiCallError(Exception):
    """Phase A 内部错误，HTTP 层统一映射为响应壳。"""

    def __init__(
        self,
        error_id: str,
        msg: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ) -> None:
        super().__init__(msg)
        self.error_id = error_id
        self.msg = msg
        self.status_code = status_code
