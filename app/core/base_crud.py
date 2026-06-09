from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel
from sqlalchemy import Select, asc, delete, desc, func, select, text, update
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Result, Row
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.elements import TextClause

from app.api.v1.system.auth.schema import AuthSchema
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.core.permission import Permission

ModelType = TypeVar("ModelType", bound=MappedBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    """
    CRUD 基类
    
    写操作使用 ORM（自动填充审计字段）
    查询操作使用原生 SQL（性能更高）
    """

    def __init__(self, model: type[ModelType], auth: AuthSchema) -> None:
        """
        初始化CRUDBase类

        参数:
        - model (Type[ModelType]): 数据模型类
        - auth (AuthSchema): 认证信息
        """
        self.model = model
        self.auth = auth

    # ==================== ORM 写操作 ====================

    async def create(self, data: CreateSchemaType | dict) -> ModelType:
        """
        创建新对象（自动填充审计字段）

        参数:
        - data (Union[CreateSchemaType, Dict]): 对象属性

        返回:
        - ModelType: 新创建的对象实例
        """
        try:
            obj_dict = data if isinstance(data, dict) else data.model_dump()
            obj = self.model(**obj_dict)

            # 填充审计字段
            self._fill_audit_fields(obj, is_create=True)

            self.auth.db.add(obj)
            await self.auth.db.flush()
            await self.auth.db.refresh(obj)
            return obj
        except Exception as e:
            raise CustomException(msg=f"创建失败: {e!s}")

    async def update(self, id: int, data: UpdateSchemaType | dict) -> ModelType:
        """
        更新对象（自动填充审计字段）

        参数:
        - id (int): 对象ID
        - data (Union[UpdateSchemaType, Dict]): 更新的属性及值

        返回:
        - ModelType: 更新后的对象实例
        """
        try:
            obj_dict = (
                data
                if isinstance(data, dict)
                else data.model_dump(exclude_unset=True, exclude={"id"})
            )
            obj = await self._get_by_id(id)
            if not obj:
                raise CustomException(msg="更新对象不存在")

            # 填充审计字段
            self._fill_audit_fields(obj, is_create=False)

            for key, value in obj_dict.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)

            await self.auth.db.flush()
            await self.auth.db.refresh(obj)
            return obj
        except Exception as e:
            raise CustomException(msg=f"更新失败: {e!s}")

    async def delete(self, ids: builtins.list[int]) -> None:
        """
        删除对象

        参数:
        - ids (List[int]): 对象ID列表
        """
        try:
            mapper = sa_inspect(self.model)
            pk_cols = list(getattr(mapper, "primary_key", []))
            if not pk_cols:
                raise CustomException(msg="模型缺少主键，无法删除")
            if len(pk_cols) > 1:
                raise CustomException(msg="暂不支持复合主键的批量删除")

            sql = delete(self.model).where(pk_cols[0].in_(ids))
            await self.auth.db.execute(sql)
            await self.auth.db.flush()
        except Exception as e:
            raise CustomException(msg=f"删除失败: {e!s}")

    async def _get_by_id(self, id: int) -> ModelType | None:
        """
        根据ID获取对象（内部方法，用于 update）

        参数:
        - id (int): 对象ID

        返回:
        - Optional[ModelType]: 对象实例
        """
        try:
            mapper = sa_inspect(self.model)
            pk_cols = list(getattr(mapper, "primary_key", []))
            if not pk_cols:
                return None
            
            sql = select(self.model).where(pk_cols[0] == id)
            sql = await self._filter_permissions(sql)
            result: Result = await self.auth.db.execute(sql)
            return result.scalars().first()
        except Exception:
            return None

    # ==================== 原生 SQL 查询 ====================

    async def raw_dicts(
        self,
        sql: str | TextClause,
        params: dict[str, Any] | None = None,
    ) -> list[dict]:
        """
        执行原生 SQL，返回字典列表

        参数:
        - sql: SQL 语句（字符串或 text() 对象）
        - params: SQL 参数

        返回:
        - list[dict]: 字典列表

        示例:
        >>> users = await crud.raw_dicts(
        ...     "SELECT user_id, user_name FROM sys_user WHERE status = :status",
        ...     {"status": "0"}
        ... )
        """
        try:
            stmt = text(sql) if isinstance(sql, str) else sql
            result = await self.auth.db.execute(stmt, params or {})
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]
        except Exception as e:
            raise CustomException(msg=f"原生SQL查询失败: {e!s}")

    async def raw_one(
        self,
        sql: str | TextClause,
        params: dict[str, Any] | None = None,
    ) -> dict | None:
        """
        执行原生 SQL，返回单个字典

        参数:
        - sql: SQL 语句
        - params: SQL 参数

        返回:
        - dict | None: 单个字典或 None

        示例:
        >>> user = await crud.raw_one(
        ...     "SELECT user_id, user_name FROM sys_user WHERE user_id = :id",
        ...     {"id": 1}
        ... )
        """
        try:
            stmt = text(sql) if isinstance(sql, str) else sql
            result = await self.auth.db.execute(stmt, params or {})
            row = result.fetchone()
            return dict(row._mapping) if row else None
        except Exception as e:
            raise CustomException(msg=f"原生SQL查询失败: {e!s}")

    async def raw_scalar(
        self,
        sql: str | TextClause,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        执行原生 SQL，返回单个值

        参数:
        - sql: SQL 语句
        - params: SQL 参数

        返回:
        - Any: 单个值

        示例:
        >>> count = await crud.raw_scalar(
        ...     "SELECT COUNT(*) FROM sys_user WHERE status = :status",
        ...     {"status": "0"}
        ... )
        """
        try:
            stmt = text(sql) if isinstance(sql, str) else sql
            result = await self.auth.db.execute(stmt, params or {})
            return result.scalar()
        except Exception as e:
            raise CustomException(msg=f"原生SQL查询失败: {e!s}")

    async def raw_page(
        self,
        sql: str | TextClause,
        params: dict[str, Any] | None = None,
        page_num: int = 1,
        page_size: int = 10,
    ) -> dict:
        """
        执行原生 SQL，返回分页数据

        参数:
        - sql: 查询 SQL（不含 LIMIT/OFFSET）
        - params: SQL 参数
        - page_num: 页码（从1开始）
        - page_size: 每页数量

        返回:
        - dict: {"total": int, "rows": list[dict]}

        示例:
        >>> result = await crud.raw_page(
        ...     "SELECT * FROM sys_user WHERE status = :status",
        ...     {"status": "0"},
        ...     page_num=1,
        ...     page_size=10
        ... )
        """
        try:
            stmt = text(sql) if isinstance(sql, str) else sql
            offset = (page_num - 1) * page_size

            count_sql = f"SELECT COUNT(*) as total FROM ({sql}) as _count_query"
            count_stmt = text(count_sql)
            count_result = await self.auth.db.execute(count_stmt, params or {})
            total = count_result.scalar() or 0

            page_sql = f"{sql} LIMIT :limit OFFSET :offset"
            page_stmt = text(page_sql)
            final_params = {**(params or {}), "limit": page_size, "offset": offset}
            result = await self.auth.db.execute(page_stmt, final_params)
            rows = result.fetchall()
            rows_data = [dict(row._mapping) for row in rows]

            return {"total": total, "rows": rows_data}
        except Exception as e:
            raise CustomException(msg=f"原生SQL分页查询失败: {e!s}")

    async def raw_execute(
        self,
        sql: str | TextClause,
        params: dict[str, Any] | None = None,
    ) -> Result:
        """
        执行原生 SQL（用于 UPDATE、DELETE、INSERT）

        参数:
        - sql: SQL 语句
        - params: SQL 参数

        返回:
        - Result: 执行结果，可通过 result.rowcount 获取影响行数

        示例:
        >>> result = await crud.raw_execute(
        ...     "UPDATE sys_user SET status = :status WHERE dept_id = :dept_id",
        ...     {"status": "1", "dept_id": 10}
        ... )
        >>> print(f"影响行数: {result.rowcount}")
        """
        try:
            stmt = text(sql) if isinstance(sql, str) else sql
            result = await self.auth.db.execute(stmt, params or {})
            await self.auth.db.flush()
            return result
        except Exception as e:
            raise CustomException(msg=f"原生SQL执行失败: {e!s}")

    # ==================== 内部方法 ====================

    def _fill_audit_fields(self, obj: ModelType, is_create: bool = True) -> None:
        """
        填充审计字段

        参数:
        - obj: 模型对象实例
        - is_create: 是否为创建操作
        """
        from datetime import datetime

        # 获取上下文信息
        user_id = -1
        tenant_id = "000000"
        
        if self.auth and self.auth.user:
            user_id = getattr(self.auth.user, "id", -1) or -1
            tenant_id = getattr(self.auth.user, "tenant_id", "000000") or "000000"

        now = datetime.now()

        # 创建操作填充的字段
        if is_create:
            if hasattr(obj, "create_by"):
                setattr(obj, "create_by", user_id)
            if hasattr(obj, "create_time"):
                setattr(obj, "create_time", now)
            if hasattr(obj, "tenant_id"):
                setattr(obj, "tenant_id", tenant_id)

        # 更新操作填充的字段（创建和更新都填充）
        if hasattr(obj, "update_by"):
            setattr(obj, "update_by", user_id)
        if hasattr(obj, "update_time"):
            setattr(obj, "update_time", now)

    async def _filter_permissions(self, sql: Select) -> Select:
        """过滤数据权限"""
        filter = Permission(model=self.model, auth=self.auth)
        return await filter.filter_query(sql)
