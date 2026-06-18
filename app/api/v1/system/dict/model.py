from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.base_model import MappedBase


class DictDataModel(MappedBase):
    """
    字典数据表（适配实际数据库结构）
    """

    __tablename__: str = "sys_dict_data"
    __table_args__: dict[str, str] = {"comment": "字典数据表"}
    __permission_strategy__ = None

    dict_code: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="字典编码",
    )
    dict_sort: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="字典排序")
    dict_label: Mapped[str] = mapped_column(
        String(100), nullable=False, default="", comment="字典标签"
    )
    dict_value: Mapped[str] = mapped_column(
        String(100), nullable=False, default="", comment="字典键值"
    )
    dict_type: Mapped[str] = mapped_column(
        String(100), nullable=False, default="", comment="字典类型", index=True
    )
    css_class: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="样式属性")
    list_class: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="表格回显样式"
    )
    is_default: Mapped[str] = mapped_column(
        String(1), nullable=False, default="N", comment="是否默认（Y是 N否）"
    )
    remark: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="备注")
