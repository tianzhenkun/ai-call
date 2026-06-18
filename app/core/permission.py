from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.sql.elements import ColumnElement

from app.api.v1.system.auth.schema import AuthSchema
from app.common.enums import PermissionFilterStrategy


class Permission:
    """
    为业务模型提供数据权限过滤功能（简化版）

    使用策略模式，根据模型的 __permission_strategy__ 属性选择合适的过滤策略
    """

    DATA_SCOPE_SELF = 1
    DATA_SCOPE_DEPT = 2
    DATA_SCOPE_DEPT_AND_CHILD = 3
    DATA_SCOPE_ALL = 4
    DATA_SCOPE_CUSTOM = 5

    def __init__(self, model: Any, auth: AuthSchema) -> None:
        self.model = model
        self.auth = auth
        self.conditions: list[ColumnElement] = []

    def _get_pk_column(self) -> ColumnElement | None:
        """获取模型的主键列"""
        mapper = sa_inspect(self.model)
        pk_cols = list(getattr(mapper, "primary_key", []))
        return pk_cols[0] if pk_cols else None

    async def filter_query(self, query: Any) -> Any:
        condition = await self.__permission_condition()
        return query.where(condition) if condition is not None else query

    async def __permission_condition(self) -> ColumnElement | None:
        if not self.auth.user:
            return None

        if not self.auth.check_data_scope:
            return None

        if self.auth.user.is_superuser:
            return None

        strategy = getattr(
            self.model, "__permission_strategy__", PermissionFilterStrategy.DATA_SCOPE
        )

        if strategy == PermissionFilterStrategy.ROLE_BASED:
            return await self.__filter_by_role_based()
        elif strategy == PermissionFilterStrategy.SELF_ONLY:
            return await self.__filter_by_self_only()
        elif strategy == PermissionFilterStrategy.USER_ROLE:
            return await self.__filter_by_user_role()
        else:
            return await self.__filter_by_data_scope()

    async def __filter_by_role_based(self) -> ColumnElement | None:
        roles = getattr(self.auth.user, "roles", []) or []
        pk_col = self._get_pk_column()
        if not roles:
            if pk_col is not None:
                return pk_col == -1
            return None

        menu_ids = set()
        for role in roles:
            if hasattr(role, "menus") and role.menus:
                menu_ids.update(menu.id for menu in role.menus if menu.status == "0")

        if menu_ids:
            if pk_col is not None:
                return pk_col.in_(list(menu_ids))

        if pk_col is not None:
            return pk_col == -1
        return None

    async def __filter_by_user_role(self) -> ColumnElement | None:
        roles = getattr(self.auth.user, "roles", []) or []
        pk_col = self._get_pk_column()
        if not roles:
            if pk_col is not None:
                return pk_col == -1
            return None

        role_ids = [role.id for role in roles]
        if pk_col is not None:
            return pk_col.in_(role_ids)
        return None

    async def __filter_by_self_only(self) -> ColumnElement | None:
        mapper = sa_inspect(self.model)
        if "created_id" in mapper.columns:
            created_id_col = mapper.columns["created_id"]
            if self.auth.user:
                return created_id_col == self.auth.user.id
        return None

    async def __filter_by_data_scope(self) -> ColumnElement | None:
        mapper = sa_inspect(self.model)
        if "created_id" not in mapper.columns:
            return None

        created_id_col = mapper.columns["created_id"]
        roles = getattr(self.auth.user, "roles", []) or []
        if not roles:
            if self.auth.user:
                return created_id_col == self.auth.user.id
            return None

        data_scopes = set()
        for role in roles:
            data_scopes.add(role.data_scope)

        if self.DATA_SCOPE_ALL in data_scopes:
            return None

        if self.DATA_SCOPE_SELF in data_scopes:
            if self.auth.user:
                return created_id_col == self.auth.user.id
            return None

        if self.auth.user:
            return created_id_col == self.auth.user.id
        return None
