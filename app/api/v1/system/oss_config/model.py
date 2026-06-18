from datetime import datetime

from sqlalchemy import CHAR, BigInteger, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class OssConfigModel(MappedBase):
    """OSS对象存储配置表"""

    __tablename__: str = "sys_oss_config"
    __table_args__: dict[str, str] = {"comment": "OSS对象存储配置表"}
    __permission_strategy__ = None

    oss_config_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="主键"
    )
    tenant_id: Mapped[str | None] = mapped_column(
        String(20), nullable=True, default="000000", comment="租户编码"
    )
    config_key: Mapped[str] = mapped_column(
        String(20), nullable=False, default="", comment="配置key"
    )
    access_key: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="accessKey"
    )
    secret_key: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="秘钥"
    )
    bucket_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="桶名称"
    )
    prefix: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="前缀"
    )
    endpoint: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="访问站点"
    )
    domain: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default="", comment="自定义域名"
    )
    is_https: Mapped[str | None] = mapped_column(
        CHAR(1), nullable=True, default="N", comment="是否https（Y=是,N=否）"
    )
    region: Mapped[str | None] = mapped_column(String(255), nullable=True, default="", comment="域")
    access_policy: Mapped[str] = mapped_column(
        CHAR(1), nullable=False, default="1", comment="桶权限类型(0=private 1=public 2=custom)"
    )
    status: Mapped[str | None] = mapped_column(
        CHAR(1), nullable=True, default="1", comment="是否默认（0=是,1=否）"
    )
    ext1: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="扩展字段")
    create_dept: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="创建部门")
    create_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="创建者")
    create_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="创建时间"
    )
    update_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="更新者")
    update_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="更新时间"
    )
    remark: Mapped[str | None] = mapped_column(
        String(500), nullable=True, default="", comment="备注"
    )
