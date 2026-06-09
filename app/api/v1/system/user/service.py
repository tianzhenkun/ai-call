from app.api.v1.system.auth.schema import AuthSchema
from app.core.exceptions import CustomException

from .crud import UserCRUD
from .schema import UserOutSchema


class UserService:
    """用户模块服务层（只读查询）"""

    @classmethod
    async def get_detail_by_id_service(cls, auth: AuthSchema, user_id: int) -> dict:
        """
        根据ID获取用户详情

        参数:
        - auth (AuthSchema): 认证信息模型
        - user_id (int): 用户ID

        返回:
        - dict: 用户详情字典
        """
        user = await UserCRUD(auth).get_by_id_crud(user_id=user_id)
        if not user:
            raise CustomException(msg="用户不存在")

        return UserOutSchema.model_validate(user).model_dump(by_alias=True)

    @classmethod
    async def get_user_list_service(
        cls,
        auth: AuthSchema,
        status: str | None = None,
        dept_id: int | None = None,
        keyword: str | None = None,
    ) -> list[dict]:
        """
        获取用户列表

        参数:
        - auth (AuthSchema): 认证信息模型
        - status (str | None): 状态过滤
        - dept_id (int | None): 部门ID过滤
        - keyword (str | None): 关键词搜索

        返回:
        - list[dict]: 用户详情字典列表
        """
        user_list = await UserCRUD(auth).get_list_crud(
            status=status,
            dept_id=dept_id,
            keyword=keyword,
        )
        return [UserOutSchema.model_validate(user).model_dump(by_alias=True) for user in user_list]

    @classmethod
    async def get_current_user_info_service(cls, auth: AuthSchema) -> dict:
        """
        获取当前用户信息

        参数:
        - auth (AuthSchema): 认证信息模型

        返回:
        - dict: 当前用户详情字典
        """
        if not auth.user:
            raise CustomException(msg="用户不存在")

        user = await UserCRUD(auth).get_by_id_crud(user_id=auth.user.id)
        if not user:
            raise CustomException(msg="用户不存在")

        return UserOutSchema.model_validate(user).model_dump(by_alias=True)
