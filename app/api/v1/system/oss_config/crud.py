from app.api.v1.system.auth.schema import AuthSchema
from app.core.base_crud import CRUDBase

from .model import OssConfigModel


class OssConfigCRUD(CRUDBase[OssConfigModel, None, None]):
    """OSS配置数据层（只读）"""

    def __init__(self, auth: AuthSchema) -> None:
        self.auth = auth
        super().__init__(model=OssConfigModel, auth=auth)

    async def get_active_config_crud(self) -> dict | None:
        """
        获取当前默认 OSS 配置（status='0'）

        返回:
        - dict | None: 配置信息，不存在返回 None
        """
        return await self.raw_one(
            "SELECT * FROM sys_oss_config WHERE status = '0' LIMIT 1",
        )
