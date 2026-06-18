from fastapi import Query
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from app.common.enums import QueueEnum
from app.core.validator import DateTimeStr


class DictDataOutSchema(BaseModel):
    """字典数据响应模型"""

    dict_code: int = Field(..., description="字典编码")
    dict_sort: int = Field(default=0, description="字典排序")
    dict_label: str = Field(..., description="字典标签")
    dict_value: str = Field(..., description="字典键值")
    dict_type: str = Field(..., description="字典类型")
    css_class: str | None = Field(default=None, description="样式属性")
    list_class: str | None = Field(default=None, description="表格回显样式")
    is_default: str = Field(default="N", description="是否默认（Y是 N否）")
    status: str = Field(default="0", description="状态（0正常 1停用）")
    remark: str | None = Field(default=None, description="备注")

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )


class DictDataQueryParam:
    """字典数据查询参数"""

    def __init__(
        self,
        dict_label: str | None = Query(default=None, description="字典标签", max_length=100),
        dict_type: str | None = Query(default=None, description="字典类型", max_length=100),
        dict_value: str | None = Query(default=None, description="字典键值", max_length=100),
        status: str | None = Query(default=None, description="状态（0正常 1停用）"),
        create_time: list[DateTimeStr] | None = Query(
            default=None,
            description="创建时间范围",
            examples=["2025-01-01 00:00:00", "2025-12-31 23:59:59"],
        ),
    ) -> None:
        self.dict_label = (QueueEnum.like.value, dict_label)
        self.dict_type = (QueueEnum.eq.value, dict_type)
        self.dict_value = (QueueEnum.eq.value, dict_value)
        self.status = (QueueEnum.eq.value, status)

        if create_time and len(create_time) == 2:
            self.create_time = (QueueEnum.between.value, (create_time[0], create_time[1]))


class DictLabelValueSchema(BaseModel):
    """字典标签-值响应模型（用于批量查询）"""

    dict_label: str = Field(..., description="字典标签")
    dict_value: str = Field(..., description="字典键值")


class DictTypeListResponse(BaseModel):
    """字典类型列表响应（按dict_type分组）"""

    dict_type: str = Field(..., description="字典类型")
    data: list[DictDataOutSchema] = Field(default_factory=list, description="字典数据列表")
