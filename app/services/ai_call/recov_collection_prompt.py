from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.services.ai_call.prompt_config import (
    PROMPT_SCENE_COLLECTION_PRODUCT_INTRO,
    BusinessPromptResult,
    PromptResolveContext,
)

DEBT_RECORD_SQL = """
select
  debtor_name,
  address,
  debt_amount,
  deadline_time,
  overdue_amount,
  debtor_gender,
  debtor_age,
  tenant_id,
  persona_id,
  organization
from debt_record
where id = $1
limit 1
"""

STRATEGY_SQL = """
select strategy_core, speaking_style, opening_template
from persona_call_strategy
where identity_name = $1 and persona_id = $2
limit 1
"""

DEFAULT_OPENING_TEMPLATE = "您好，请问是{{name}}吗？我是{{organization}}的{{identityName}}。"
TEMPLATE_PLACEHOLDER_RE = re.compile(r"{{\s*([^{}]+?)\s*}}")

ConnectFn = Callable[..., Awaitable[Any]]


class RecovCollectionPostgresPromptStore:
    def __init__(
        self,
        *,
        dsn: str,
        timeout_seconds: float,
        connect: ConnectFn | None = None,
    ) -> None:
        self.dsn = dsn
        self.timeout_seconds = max(0.1, timeout_seconds)
        self._connect = connect

    async def resolve_collection_prompt(
        self,
        *,
        debt_id: str,
        identity_name: str,
        context: PromptResolveContext | None,
    ) -> BusinessPromptResult | None:
        debt_id_value = _int_text(debt_id)
        if debt_id_value is None:
            return None

        conn = await self._open_connection()
        try:
            debt_row = await conn.fetchrow(DEBT_RECORD_SQL, int(debt_id_value))
            if debt_row is None:
                return None
            persona_id = _int_text(_row_value(debt_row, "persona_id"))
            if persona_id is None:
                return None
            strategy_row = await conn.fetchrow(STRATEGY_SQL, identity_name, int(persona_id))
            if strategy_row is None:
                return None
        finally:
            await conn.close()

        prompt = _render_collection_prompt(
            identity_name=identity_name,
            strategy=_row_value(strategy_row, "strategy_core"),
            speaking_style=_row_value(strategy_row, "speaking_style"),
            debtor_name=_row_value(debt_row, "debtor_name"),
            debtor_gender=_row_value(debt_row, "debtor_gender"),
            debtor_age=_row_value(debt_row, "debtor_age"),
            debt_amount=_row_value(debt_row, "debt_amount"),
            deadline_time=_row_value(debt_row, "deadline_time"),
            overdue_amount=_row_value(debt_row, "overdue_amount"),
            address=_row_value(debt_row, "address"),
            organization=_row_value(debt_row, "organization"),
        )
        opening = _render_collection_opening(
            identity_name=identity_name,
            template=_row_value(strategy_row, "opening_template"),
            debtor_name=_row_value(debt_row, "debtor_name"),
            debtor_gender=_row_value(debt_row, "debtor_gender"),
            organization=_row_value(debt_row, "organization"),
        )
        return BusinessPromptResult(
            prompt=prompt,
            opening_message=opening,
            source_key=PROMPT_SCENE_COLLECTION_PRODUCT_INTRO,
        )

    async def _open_connection(self) -> Any:
        if self._connect is not None:
            return await self._connect(self.dsn, timeout=self.timeout_seconds)
        import asyncpg

        return await asyncpg.connect(self.dsn, timeout=self.timeout_seconds)


def _render_collection_prompt(
    *,
    identity_name: object,
    strategy: object,
    speaking_style: object,
    debtor_name: object,
    debtor_gender: object,
    debtor_age: object,
    debt_amount: object,
    deadline_time: object,
    overdue_amount: object,
    address: object,
    organization: object,
) -> str:
    salutation = _debtor_salutation(debtor_name, debtor_gender)
    lines = [
        "# 角色",
        f"你是{_text(identity_name)}，负责通过电话进行合规的逾期费用提醒和费用处理沟通。",
        "",
        "# 催收策略",
        _text(strategy),
        "",
        "# 客服语气配置",
        _text(speaking_style) or "语气克制、表达清晰，避免压迫式沟通。",
        "",
        "# 身份核实与隐私边界",
        "1. 身份未确认前，不得主动披露地址、金额、欠费明细或费用原因。",
        f"2. 身份未确认时，下一句只能问：请问您是{salutation}本人，或者是这项费用事项的授权处理人吗？",
        "3. 用户追问具体事项但仍未确认身份时，只能说明“为保护信息安全，确认本人或授权处理人后才能说明具体内容”。",
        "",
        "# 本轮可核实业务信息",
        f"所属项目：{_fact(organization)}",
        f"地址：{_fact(address)}",
        f"缴费截止日期：{_fact(deadline_time)}",
        f"逾期金额：{_money(debt_amount)}",
        f"逾期滞纳金：{_money(overdue_amount)}",
        f"业主称呼：{salutation}",
        f"业主年龄：{_fact(debtor_age)}",
        "以上信息只允许在确认本人或授权处理人后按需回答；身份未确认前不得主动披露。",
        "",
        "# 沟通规范",
        "1. 不承诺减免、展期、销账或具体处理结果。",
        "2. 用户拒绝或不方便沟通时，不施压、不反复追问，礼貌收束。",
        "3. 系统未提供或无法确定的信息不要猜测，引导用户联系物业公司或人工进一步确认。",
    ]
    return "\n".join(lines)


def _render_collection_opening(
    *,
    identity_name: object,
    template: object,
    debtor_name: object,
    debtor_gender: object,
    organization: object,
) -> str:
    salutation = _debtor_salutation(debtor_name, debtor_gender)
    values = {
        "name": salutation,
        "identityName": _text(identity_name),
        "identity_name": _text(identity_name),
        "organization": _text(organization),
    }
    template_text = _text(template) or DEFAULT_OPENING_TEMPLATE

    def replace(match: re.Match[str]) -> str:
        return values.get(match.group(1).strip(), "")

    rendered = TEMPLATE_PLACEHOLDER_RE.sub(replace, template_text).strip()
    return rendered or DEFAULT_OPENING_TEMPLATE


def _debtor_salutation(name: object, gender: object) -> str:
    name_text = _text(name)
    title = _debtor_title(gender)
    if name_text and title:
        return f"{name_text[:1]}{title}"
    return name_text or "您"


def _debtor_title(gender: object) -> str:
    gender_text = _text(gender)
    if gender_text in {"男", "男性", "先生"}:
        return "先生"
    if gender_text in {"女", "女性", "女士"}:
        return "女士"
    return ""


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return None


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _fact(value: object) -> str:
    return _text(value) or "未提供"


def _money(value: object) -> str:
    text = _text(value)
    if not text:
        return "未提供"
    if text.endswith("元"):
        return text
    return f"{text}元"


def _int_text(value: object) -> str | None:
    text = _text(value)
    if not text:
        return None
    try:
        return str(int(text))
    except ValueError:
        return None
