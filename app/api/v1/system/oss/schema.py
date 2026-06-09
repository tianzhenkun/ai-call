from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class OssOutSchema(BaseModel):
    """OSS对象存储响应模型"""

    oss_id: int = Field(..., description="对象存储主键")
    tenant_id: str = Field(default="000000", description="租户编码")
    file_name: str = Field(..., description="文件名")
    original_name: str = Field(..., description="原名")
    file_suffix: str = Field(..., description="文件后缀名")
    url: str = Field(..., description="URL地址")
    ext1: str | None = Field(default=None, description="扩展字段")
    create_dept: int | None = Field(default=None, description="创建部门")
    create_by: int | None = Field(default=None, description="上传人")
    create_time: datetime | None = Field(default=None, description="创建时间")
    update_by: int | None = Field(default=None, description="更新者")
    update_time: datetime | None = Field(default=None, description="更新时间")
    service: str = Field(default="minio", description="服务商")

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )


class OssUrlOutSchema(BaseModel):
    """OSS URL响应模型（简化版，用于获取URL）"""

    oss_id: int = Field(..., description="对象存储主键")
    url: str = Field(..., description="URL地址")
    original_name: str = Field(..., description="原文件名")
    file_suffix: str = Field(..., description="文件后缀名")

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )
