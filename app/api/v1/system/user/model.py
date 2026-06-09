from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.base_model import MappedBase


class UserModel(MappedBase):
    """
    用户模型（适配实际数据库结构）
    """

    __tablename__: str = "sys_user"
    __table_args__: dict[str, str] = {"comment": "用户信息表"}
    __permission_strategy__ = None

    user_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="用户ID",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20), nullable=False, default="000000", comment="租户编号"
    )
    dept_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="部门ID"
    )
    user_name: Mapped[str] = mapped_column(
        String(30), nullable=False, comment="用户账号"
    )
    nick_name: Mapped[str] = mapped_column(
        String(30), nullable=False, comment="用户昵称"
    )
    user_type: Mapped[str] = mapped_column(
        String(10), nullable=False, default="sys_user", comment="用户类型"
    )
    email: Mapped[str] = mapped_column(
        String(50), nullable=False, default="", comment="用户邮箱"
    )
    phonenumber: Mapped[str] = mapped_column(
        String(11), nullable=False, default="", comment="手机号码"
    )
    sex: Mapped[str] = mapped_column(
        String(1), nullable=False, default="0", comment="用户性别（0男 1女 2未知）"
    )
    avatar: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="头像地址"
    )
    password: Mapped[str] = mapped_column(
        String(100), nullable=False, default="", comment="密码"
    )
    status: Mapped[str] = mapped_column(
        String(1), nullable=False, default="0", comment="账号状态（0正常 1停用）"
    )
    del_flag: Mapped[str] = mapped_column(
        String(1), nullable=False, default="0", comment="删除标志（0存在 1删除）"
    )
    login_ip: Mapped[str] = mapped_column(
        String(128), nullable=False, default="", comment="最后登陆IP"
    )
    login_date: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="最后登陆时间"
    )
    create_dept: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="创建部门"
    )
    create_by: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="创建者"
    )
    create_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="创建时间"
    )
    update_by: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="更新者"
    )
    update_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="更新时间"
    )
    remark: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="备注"
    )

    @property
    def id(self) -> int:
        return self.user_id

    @property
    def username(self) -> str:
        return self.user_name

    @property
    def name(self) -> str:
        return self.nick_name

    @property
    def mobile(self) -> str:
        return self.phonenumber

    @property
    def gender(self) -> str:
        return self.sex

    @property
    def last_login(self) -> datetime | None:
        return self.login_date

    @property
    def is_superuser(self) -> bool:
        return self.user_type == "admin" or self.user_id == 1
