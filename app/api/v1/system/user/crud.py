from app.api.v1.system.auth.schema import AuthSchema
from app.core.base_crud import CRUDBase

from .model import UserModel


class UserCRUD(CRUDBase[UserModel, None, None]):
    """用户模块数据层"""

    def __init__(self, auth: AuthSchema) -> None:
        self.auth = auth
        super().__init__(model=UserModel, auth=auth)

    async def get_by_id_crud(self, user_id: int) -> dict | None:
        """
        根据ID获取用户信息

        参数:
        - user_id (int): 用户ID

        返回:
        - dict | None: 用户信息
        """
        return await self.raw_one(
            "SELECT * FROM sys_user WHERE user_id = :user_id",
            {"user_id": user_id}
        )

    async def get_by_username_crud(self, username: str) -> dict | None:
        """
        根据用户名获取用户信息

        参数:
        - username (str): 用户名

        返回:
        - dict | None: 用户信息
        """
        return await self.raw_one(
            "SELECT * FROM sys_user WHERE user_name = :username",
            {"username": username}
        )

    async def get_by_mobile_crud(self, mobile: str) -> dict | None:
        """
        根据手机号获取用户信息

        参数:
        - mobile (str): 手机号

        返回:
        - dict | None: 用户信息
        """
        return await self.raw_one(
            "SELECT * FROM sys_user WHERE phonenumber = :mobile",
            {"mobile": mobile}
        )

    async def get_list_crud(
        self,
        status: str | None = None,
        dept_id: int | None = None,
        keyword: str | None = None,
    ) -> list[dict]:
        """
        获取用户列表

        参数:
        - status (str | None): 状态过滤
        - dept_id (int | None): 部门ID过滤
        - keyword (str | None): 关键词搜索（用户名/昵称/手机号）

        返回:
        - list[dict]: 用户列表
        """
        sql = "SELECT * FROM sys_user WHERE 1=1"
        params: dict = {}

        if status is not None:
            sql += " AND status = :status"
            params["status"] = status

        if dept_id is not None:
            sql += " AND dept_id = :dept_id"
            params["dept_id"] = dept_id

        if keyword:
            sql += " AND (user_name LIKE :keyword OR nick_name LIKE :keyword OR phonenumber LIKE :keyword)"
            params["keyword"] = f"%{keyword}%"

        sql += " ORDER BY user_id ASC"

        return await self.raw_dicts(sql, params)

    async def get_page_crud(
        self,
        page_num: int,
        page_size: int,
        status: str | None = None,
        dept_id: int | None = None,
        keyword: str | None = None,
    ) -> dict:
        """
        分页获取用户列表

        参数:
        - page_num (int): 页码
        - page_size (int): 每页数量
        - status (str | None): 状态过滤
        - dept_id (int | None): 部门ID过滤
        - keyword (str | None): 关键词搜索

        返回:
        - dict: 分页数据
        """
        sql = "SELECT * FROM sys_user WHERE 1=1"
        params: dict = {}

        if status is not None:
            sql += " AND status = :status"
            params["status"] = status

        if dept_id is not None:
            sql += " AND dept_id = :dept_id"
            params["dept_id"] = dept_id

        if keyword:
            sql += " AND (user_name LIKE :keyword OR nick_name LIKE :keyword OR phonenumber LIKE :keyword)"
            params["keyword"] = f"%{keyword}%"

        sql += " ORDER BY user_id ASC"

        return await self.raw_page(sql, params, page_num=page_num, page_size=page_size)

    async def update_last_login_crud(self, user_id: int) -> dict | None:
        """
        更新用户最后登录时间

        参数:
        - user_id (int): 用户ID

        返回:
        - dict | None: 更新后的用户信息
        """
        from datetime import datetime

        await self.raw_execute(
            "UPDATE sys_user SET login_date = :login_date WHERE user_id = :user_id",
            {"login_date": datetime.now(), "user_id": user_id}
        )
        return await self.get_by_id_crud(user_id)
