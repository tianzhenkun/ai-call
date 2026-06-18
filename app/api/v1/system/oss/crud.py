from app.api.v1.system.auth.schema import AuthSchema
from app.core.base_crud import CRUDBase

from .model import OssModel


class OssCRUD(CRUDBase[OssModel, None, None]):
    """OSS对象存储数据层（只读查询）"""

    def __init__(self, auth: AuthSchema) -> None:
        self.auth = auth
        super().__init__(model=OssModel, auth=auth)

    async def get_by_oss_id_crud(self, oss_id: int) -> dict | None:
        """
        根据oss_id获取OSS对象信息

        参数:
        - oss_id (int): 对象存储主键

        返回:
        - dict | None: OSS对象信息
        """
        return await self.raw_one(
            "SELECT * FROM sys_oss WHERE oss_id = :oss_id", {"oss_id": oss_id}
        )

    async def get_url_by_oss_id_crud(self, oss_id: int) -> dict | None:
        """
        根据oss_id获取URL信息（简化版）

        参数:
        - oss_id (int): 对象存储主键

        返回:
        - dict | None: 包含url、original_name、file_suffix的字典
        """
        return await self.raw_one(
            "SELECT oss_id, url, original_name, file_suffix FROM sys_oss WHERE oss_id = :oss_id",
            {"oss_id": oss_id},
        )

    async def get_list_by_oss_ids_crud(self, oss_ids: list[int]) -> list[dict]:
        """
        根据oss_id列表批量获取OSS对象信息

        参数:
        - oss_ids (list[int]): 对象存储主键列表

        返回:
        - list[dict]: OSS对象信息列表
        """
        if not oss_ids:
            return []

        placeholders = ", ".join([f":id_{i}" for i in range(len(oss_ids))])
        params = {f"id_{i}": oss_id for i, oss_id in enumerate(oss_ids)}

        return await self.raw_dicts(
            f"SELECT * FROM sys_oss WHERE oss_id IN ({placeholders})", params
        )

    async def get_url_list_by_oss_ids_crud(self, oss_ids: list[int]) -> list[dict]:
        """
        根据oss_id列表批量获取URL信息（简化版）

        参数:
        - oss_ids (list[int]): 对象存储主键列表

        返回:
        - list[dict]: 包含oss_id、url、original_name、file_suffix的字典列表
        """
        if not oss_ids:
            return []

        placeholders = ", ".join([f":id_{i}" for i in range(len(oss_ids))])
        params = {f"id_{i}": oss_id for i, oss_id in enumerate(oss_ids)}

        return await self.raw_dicts(
            f"SELECT oss_id, url, original_name, file_suffix FROM sys_oss WHERE oss_id IN ({placeholders})",
            params,
        )
