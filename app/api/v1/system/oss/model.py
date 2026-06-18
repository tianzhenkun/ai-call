from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class OssModel(MappedBase):
    """OSS对象存储表"""

    __tablename__: str = "sys_oss"
    __table_args__: dict[str, str] = {"comment": "OSS对象存储表"}
    __permission_strategy__ = None

    oss_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="对象存储主键",
    )
    tenant_id: Mapped[str] = mapped_column(
        String(20), nullable=False, default="000000", comment="租户编码"
    )
    file_name: Mapped[str] = mapped_column(
        String(255), nullable=False, default="", comment="文件名"
    )
    original_name: Mapped[str] = mapped_column(
        String(255), nullable=False, default="", comment="原名"
    )
    file_suffix: Mapped[str] = mapped_column(
        String(10), nullable=False, default="", comment="文件后缀名"
    )
    url: Mapped[str] = mapped_column(String(500), nullable=False, default="", comment="URL地址")
    ext1: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="扩展字段")
    create_dept: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="创建部门")
    create_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="上传人")
    create_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="创建时间"
    )
    update_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="更新者")
    update_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="更新时间"
    )
    service: Mapped[str] = mapped_column(
        String(20), nullable=False, default="minio", comment="服务商"
    )
