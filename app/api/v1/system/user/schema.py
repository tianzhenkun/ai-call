from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from app.common.enums import QueueEnum
from app.core.base_schema import BaseSchema
from app.core.validator import DateTimeStr


class UserOutSchema(BaseModel):
    """用户响应模型"""

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    user_id: int = Field(..., description="用户ID")
    tenant_id: str = Field(default="000000", description="租户编号")
    dept_id: int | None = Field(default=None, description="部门ID")
    user_name: str = Field(..., description="用户账号")
    nick_name: str = Field(..., description="用户昵称")
    user_type: str = Field(default="sys_user", description="用户类型")
    email: str = Field(default="", description="用户邮箱")
    phonenumber: str = Field(default="", description="手机号码")
    sex: str = Field(default="0", description="用户性别")
    avatar: int | None = Field(default=None, description="头像地址")
    status: str = Field(default="0", description="账号状态")
    del_flag: str = Field(default="0", description="删除标志")
    login_ip: str = Field(default="", description="最后登陆IP")
    login_date: DateTimeStr | None = Field(default=None, description="最后登陆时间")
    remark: str | None = Field(default=None, description="备注")


class UserQueryParam:
    """用户查询参数"""

    def __init__(
        self,
        user_name: str | None = Query(None, description="用户账号"),
        nick_name: str | None = Query(None, description="用户昵称"),
        phonenumber: str | None = Query(None, description="手机号"),
        email: str | None = Query(None, description="邮箱"),
        status: str | None = Query(None, description="账号状态"),
        create_time: list[DateTimeStr] | None = Query(
            None,
            description="创建时间范围",
            examples=["2025-01-01 00:00:00", "2025-12-31 23:59:59"],
        ),
    ) -> None:
        self.user_name = (QueueEnum.like.value, user_name)
        self.nick_name = (QueueEnum.like.value, nick_name)
        self.phonenumber = (QueueEnum.like.value, phonenumber)
        self.email = (QueueEnum.like.value, email)
        self.status = (QueueEnum.eq.value, status)

        if create_time and len(create_time) == 2:
            self.create_time = (QueueEnum.between.value, (create_time[0], create_time[1]))
