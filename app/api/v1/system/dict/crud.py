from app.api.v1.system.auth.schema import AuthSchema
from app.core.base_crud import CRUDBase

from .model import DictDataModel


class DictDataCRUD(CRUDBase[DictDataModel, None, None]):
    """数据字典数据层（只读查询）"""

    def __init__(self, auth: AuthSchema) -> None:
        self.auth = auth
        super().__init__(model=DictDataModel, auth=auth)

    async def get_obj_list_crud(
        self,
        dict_type: str | None = None,
    ) -> list[dict]:
        """
        获取数据字典数据列表

        参数:
        - dict_type (str | None): 字典类型过滤

        返回:
        - list[dict]: 数据字典数据列表
        """
        sql = "SELECT * FROM sys_dict_data WHERE 1=1"
        params: dict = {}

        if dict_type:
            sql += " AND dict_type = :dict_type"
            params["dict_type"] = dict_type

        sql += " ORDER BY dict_sort ASC"

        return await self.raw_dicts(sql, params)

    async def get_obj_list_by_dict_type_crud(
        self, dict_type: str
    ) -> list[dict]:
        """
        根据字典类型获取字典数据列表

        参数:
        - dict_type (str): 字典类型

        返回:
        - list[dict]: 数据字典数据列表
        """
        sql = "SELECT * FROM sys_dict_data WHERE dict_type = :dict_type"
        params: dict = {"dict_type": dict_type}

        sql += " ORDER BY dict_sort ASC"

        return await self.raw_dicts(sql, params)

    async def get_dict_label_by_value(
        self, dict_type: str, dict_value: str
    ) -> str | None:
        """
        根据字典类型和字典值获取字典标签

        参数:
        - dict_type (str): 字典类型
        - dict_value (str): 字典值

        返回:
        - str | None: 字典标签
        """
        result = await self.raw_one(
            """
            SELECT dict_label FROM sys_dict_data 
            WHERE dict_type = :dict_type AND dict_value = :dict_value
            """,
            {"dict_type": dict_type, "dict_value": dict_value}
        )
        return result.get("dict_label") if result else None

    async def get_dict_value_by_label(
        self, dict_type: str, dict_label: str
    ) -> str | None:
        """
        根据字典类型和字典标签获取字典值

        参数:
        - dict_type (str): 字典类型
        - dict_label (str): 字典标签

        返回:
        - str | None: 字典值
        """
        result = await self.raw_one(
            """
            SELECT dict_value FROM sys_dict_data 
            WHERE dict_type = :dict_type AND dict_label = :dict_label
            """,
            {"dict_type": dict_type, "dict_label": dict_label}
        )
        return result.get("dict_value") if result else None

    async def batch_get_dict_labels_by_values(
        self, dict_type: str, dict_values: list[str]
    ) -> dict[str, str]:
        """
        批量根据字典值获取字典标签

        参数:
        - dict_type (str): 字典类型
        - dict_values (list[str]): 字典值列表

        返回:
        - dict[str, str]: {dict_value: dict_label} 映射
        """
        if not dict_values:
            return {}

        placeholders = ", ".join([f":val_{i}" for i in range(len(dict_values))])
        params = {"dict_type": dict_type}
        for i, val in enumerate(dict_values):
            params[f"val_{i}"] = val

        results = await self.raw_dicts(
            f"""
            SELECT dict_value, dict_label FROM sys_dict_data 
            WHERE dict_type = :dict_type AND dict_value IN ({placeholders})
            """,
            params
        )
        return {r["dict_value"]: r["dict_label"] for r in results}

    async def batch_get_dict_values_by_labels(
        self, dict_type: str, dict_labels: list[str]
    ) -> dict[str, str]:
        """
        批量根据字典标签获取字典值

        参数:
        - dict_type (str): 字典类型
        - dict_labels (list[str]): 字典标签列表

        返回:
        - dict[str, str]: {dict_label: dict_value} 映射
        """
        if not dict_labels:
            return {}

        placeholders = ", ".join([f":label_{i}" for i in range(len(dict_labels))])
        params = {"dict_type": dict_type}
        for i, label in enumerate(dict_labels):
            params[f"label_{i}"] = label

        results = await self.raw_dicts(
            f"""
            SELECT dict_label, dict_value FROM sys_dict_data 
            WHERE dict_type = :dict_type AND dict_label IN ({placeholders})
            """,
            params
        )
        return {r["dict_label"]: r["dict_value"] for r in results}
