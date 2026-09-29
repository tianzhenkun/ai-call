"""提示词编辑器的内容比较与模块边界。"""

from __future__ import annotations

import json
import re

SECTIONS = (
    "一、角色与任务",
    "二、业务信息",
    "三、沟通规则",
    "四、对话流程",
    "五、常见异议",
    "六、完成与结束条件",
)
MODULE_FIELDS = {
    "opening": {"opening_message", "opening_barge_in_enabled"},
    "productInfo": {"product_info", "knowledge_version_snapshot_hash"},
    "scenePrompt": {"prompt_text"},
}


def normalize_text(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def default_scene_prompt(common: str) -> str:
    return "\n\n".join(
        f"## {title}" + (f"\n{common.strip()}" if index == 2 else "")
        for index, title in enumerate(SECTIONS)
    )


def missing_fields(values: dict) -> list[str]:
    if values.get("provider_key", "static_profile") != "static_profile":
        return []
    missing = []
    if not normalize_text(values.get("opening_message")):
        missing.append("openingMessage")
    prompt = normalize_text(values.get("prompt_text"))
    # 新模板的角色与任务必须实际填写；历史自由格式不强制迁移章节。
    if prompt.startswith("## 一、角色与任务"):
        role = re.split(r"\n## ", prompt, maxsplit=1)[0].removeprefix("## 一、角色与任务").strip()
        if not role:
            missing.append("promptText")
    elif not prompt:
        missing.append("promptText")
    return missing


def normalize_variables(variables: list[dict]) -> list[dict]:
    return sorted(
        ({**item, "label": normalize_text(item["label"])} for item in variables),
        key=lambda item: item["key"],
    )


def canonical_content(snapshot: dict) -> dict:
    return {
        key: normalize_text(snapshot.get(key))
        for key in (
            "sceneCode",
            "name",
            "providerKey",
            "promptText",
            "openingMessage",
            "productInfo",
        )
    } | {
        "openingBargeInEnabled": snapshot.get("openingBargeInEnabled", True),
        "variables": normalize_variables(snapshot.get("variables") or []),
    }


def module_values(values: dict, saved_variables: str = "[]") -> dict:
    from app.api.v1.ai_call.schema import PromptProfileBaseRequest
    from app.core.exceptions import CustomException

    module = values.get("module")
    if module not in MODULE_FIELDS:
        raise CustomException(msg="不支持的保存模块", status_code=400)
    allowed = MODULE_FIELDS[module] | {
        "module",
        "expected_revision",
        "variables",
        "name",
        "creation_key",
    }
    if set(values) - allowed:
        raise CustomException(msg="不能保存其他模块的字段", status_code=400)
    if "opening_barge_in_enabled" in values and not isinstance(
        values["opening_barge_in_enabled"], bool
    ):
        raise CustomException(msg="允许打断必须是布尔值", status_code=400)
    result = {key: value for key, value in values.items() if key in MODULE_FIELDS[module]}
    variables = {item["key"]: item for item in normalize_variables(json.loads(saved_variables))}
    labels = {item["label"].strip(): item["key"] for item in variables.values()}
    for item in normalize_variables(values.get("variables") or []):
        if (
            item["key"] in variables
            and variables[item["key"]] != item
            or item["label"].strip() in labels
            and labels[item["label"].strip()] != item["key"]
        ):
            raise CustomException(msg="变量定义冲突，请检查变量名称和 key", status_code=400)
        variables[item["key"]] = item
        labels[item["label"].strip()] = item["key"]
    result["variables"] = sorted(variables.values(), key=lambda item: item["key"])
    # 合并后的集合也经过整份保存的边界校验，避免局部保存产生不可提交的配置。
    PromptProfileBaseRequest(name="模块校验", variables=result["variables"])
    text = "\n".join(
        str(result.get(key) or "") for key in ("opening_message", "product_info", "prompt_text")
    )
    undefined = set(re.findall(r"\{\{([A-Za-z][A-Za-z0-9_]*)\}\}", text)) - variables.keys()
    if undefined:
        raise CustomException(
            msg=f"提示词引用了未定义变量：{'、'.join(sorted(undefined))}", status_code=400
        )
    return result


async def require_available_profile(
    db, *, tenant_id, profile_id=None, scene_code=None, for_update=True
):
    from sqlalchemy import select

    from app.api.v1.ai_call.model import AiCallPromptProfileModel
    from app.core.exceptions import CustomException

    stmt = select(AiCallPromptProfileModel).where(AiCallPromptProfileModel.tenant_id == tenant_id)
    stmt = (
        stmt.where(AiCallPromptProfileModel.id == int(profile_id))
        if profile_id
        else stmt.where(
            AiCallPromptProfileModel.scene_code == scene_code,
        )
    )
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    profile = await db.scalar(stmt)
    if profile is None:
        raise CustomException(msg="业务场景提示词配置不存在", status_code=404)
    if profile.deleted_at is not None:
        raise CustomException(
            msg="该场景已删除", status_code=410, data={"errorCode": "PROMPT_PROFILE_DELETED"}
        )
    if profile.lifecycle_status != "READY":
        raise CustomException(msg="该场景仍为草稿，请先完成保存配置", status_code=409)
    return profile


async def deletion_check(db, profile):
    from sqlalchemy import and_, exists, func, or_, select

    from app.api.v1.ai_call.model import (
        AiCallFollowUpTaskModel,
        AiCallKnowledgeItemModel,
        AiCallPromptKnowledgeBindingModel,
        AiCallRecordModel,
    )
    from app.api.v1.ai_call.outbound.rule_task_model import (
        AiCallOutboundAttemptModel,
        AiCallOutboundExceptionBatchModel,
        AiCallOutboundTargetModel,
        AiCallOutboundTaskModel,
    )

    task, target, attempt, batch = (
        AiCallOutboundTaskModel,
        AiCallOutboundTargetModel,
        AiCallOutboundAttemptModel,
        AiCallOutboundExceptionBatchModel,
    )
    task_scope = and_(
        task.tenant_id == profile.tenant_id,
        or_(
            task.prompt_profile_id == str(profile.id),
            and_(task.prompt_profile_id.is_(None), task.scene_code == profile.scene_code),
        ),
    )
    active_target = exists(
        select(target.id).where(
            target.tenant_id == profile.tenant_id,
            target.task_id == task.id,
            target.status.in_(["PENDING", "RETRY_WAIT", "DIALING", "IN_CALL"]),
        )
    )
    active_attempt = exists(
        select(attempt.id).where(
            attempt.tenant_id == profile.tenant_id,
            attempt.task_id == task.id,
            attempt.status.in_(["QUEUED", "STARTING", "DIALING", "IN_CALL"]),
        )
    )
    active_batch = exists(
        select(target.id)
        .join(
            batch,
            and_(
                batch.id == target.exception_batch_id,
                batch.tenant_id == target.tenant_id,
            ),
        )
        .where(
            target.task_id == task.id,
            target.tenant_id == profile.tenant_id,
            batch.status == "RUNNING",
        )
    )
    tasks = (
        await db.execute(
            select(task.id, task.task_name).where(
                task_scope,
                or_(
                    task.status.in_(["SCHEDULED", "RUNNING", "PAUSING", "PAUSED", "STOPPING"]),
                    active_target,
                    active_attempt,
                    active_batch,
                ),
            )
        )
    ).all()
    blockers = [
        {"type": "task", "id": str(id), "label": name, "url": f"/reach/tasks/{id}"}
        for id, name in tasks
    ]
    records = (
        await db.scalars(
            select(AiCallRecordModel.call_id).where(
                AiCallRecordModel.tenant_id == profile.tenant_id,
                or_(
                    AiCallRecordModel.scene_code == profile.scene_code,
                    AiCallRecordModel.prompt_source_key == str(profile.id),
                ),
                AiCallRecordModel.status.not_in(["completed", "failed"]),
                AiCallRecordModel.ended_at.is_(None),
            )
        )
    ).all()
    blockers.extend({"type": "call", "id": id, "label": "进行中的通话"} for id in records)
    followups = (
        await db.scalars(
            select(AiCallFollowUpTaskModel.id).where(
                AiCallFollowUpTaskModel.tenant_id == profile.tenant_id,
                AiCallFollowUpTaskModel.scene_code == profile.scene_code,
                AiCallFollowUpTaskModel.status.in_(["pending", "processing"]),
            )
        )
    ).all()
    blockers.extend(
        {"type": "followUp", "id": str(id), "label": "未结束的跟进任务"} for id in followups
    )
    knowledge_count = await db.scalar(
        select(func.count())
        .select_from(AiCallPromptKnowledgeBindingModel)
        .join(
            AiCallKnowledgeItemModel,
            and_(
                AiCallKnowledgeItemModel.id == AiCallPromptKnowledgeBindingModel.knowledge_item_id,
                AiCallKnowledgeItemModel.tenant_id == AiCallPromptKnowledgeBindingModel.tenant_id,
            ),
        )
        .where(
            AiCallPromptKnowledgeBindingModel.tenant_id == profile.tenant_id,
            AiCallPromptKnowledgeBindingModel.prompt_profile_id == profile.id,
            AiCallKnowledgeItemModel.deleted_at.is_(None),
        )
    )
    return {
        "id": str(profile.id),
        "name": profile.name,
        "editRevision": profile.edit_revision,
        "deleted": profile.deleted_at is not None,
        "knowledgeCount": knowledge_count,
        "blockers": blockers if profile.deleted_at is None else [],
    }
