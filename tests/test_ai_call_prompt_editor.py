"""提示词编辑器持久化与版本合同，使用真实隔离数据库。"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from test_ai_call_phase_b4_prompt_config import TEST_TENANT_ID
from test_ai_call_phase_b4_prompt_config import b4_service as _b4_service

from app.api.v1.ai_call.model import AiCallRecordModel
from app.api.v1.ai_call.schema import PromptOptimizeRequest, PromptProfileRevisionRequest
from app.core.exceptions import CustomException
from app.services.ai_call.prompt_editor import module_values, require_available_profile
from app.services.ai_call.runtime_control.command_repository import (
    RuntimeCommandRepository,
    StartCallIntent,
)

pytestmark = pytest.mark.anyio
b4_service = _b4_service


def test_migration_cli_starts_without_application_import_order():
    completed = subprocess.run(
        [sys.executable, "tools/migrate_prompt_editor.py", "--help"],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "--check-rollback" in completed.stdout


async def test_module_draft_full_save_and_mutable_current_version(b4_service):
    service, repo, *_ = b4_service
    draft = await service.create_prompt_profile_draft(
        tenant_id=TEST_TENANT_ID,
        values={
            "name": "新场景",
            "creation_key": "editor-test-1",
            "module": "opening",
            "opening_message": "开场白 A",
            "opening_barge_in_enabled": True,
        },
    )
    assert draft["versionNo"] is None
    assert draft["lifecycleStatus"] == "DRAFT"
    filled = await service.save_prompt_profile_module(
        tenant_id=TEST_TENANT_ID,
        profile_id=int(draft["id"]),
        values={
            "module": "scenePrompt",
            "expected_revision": draft["editRevision"],
            "prompt_text": "你是产品顾问，了解客户需求。",
        },
    )
    assert filled["versionNo"] is None
    profile = await repo.get_prompt_profile(int(draft["id"]), tenant_id=TEST_TENANT_ID)
    values = service._profile_values_from_snapshot(service._prompt_profile_snapshot(profile))
    values["variables"] = json.loads(values.pop("variables_json"))
    values["expected_revision"] = filled["editRevision"]
    ready = await service.update_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        profile_id=profile.id,
        values=values,
    )
    assert ready["versionNo"] == 1 and ready["lifecycleStatus"] == "READY"
    updated = await service.save_prompt_profile_module(
        tenant_id=TEST_TENANT_ID,
        profile_id=profile.id,
        values={
            "module": "opening",
            "expected_revision": ready["editRevision"],
            "opening_message": "开场白 B",
            "opening_barge_in_enabled": False,
        },
    )
    assert updated["versionNo"] == 1
    detail = await service.get_prompt_profile_version(
        tenant_id=TEST_TENANT_ID,
        profile_id=profile.id,
        version_id=int(ready["currentVersionId"]),
    )
    assert detail["snapshot"]["openingMessage"] == "开场白 B"
    values.update(
        opening_message="开场白 B",
        opening_barge_in_enabled=False,
        expected_revision=updated["editRevision"],
    )
    unchanged = await service.update_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        profile_id=profile.id,
        values=values,
    )
    assert unchanged["versionNo"] == 1 and not unchanged["versionCreated"]
    values.update(prompt_text="新的产品介绍目标", expected_revision=unchanged["editRevision"])
    next_version = await service.update_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        profile_id=profile.id,
        values=values,
    )
    assert next_version["versionNo"] == 2
    assert next_version["versionCreated"]


async def test_draft_creation_is_idempotent_and_names_include_drafts(b4_service):
    service, *_ = b4_service
    values = {
        "name": " 测试草稿 ",
        "creation_key": "stable-new-id",
        "module": "opening",
        "opening_message": "你好",
    }
    first = await service.create_prompt_profile_draft(tenant_id=TEST_TENANT_ID, values=values)
    again = await service.create_prompt_profile_draft(tenant_id=TEST_TENANT_ID, values=values)
    assert first["id"] == again["id"]
    assert not (await service.check_prompt_profile_name(tenant_id=TEST_TENANT_ID, name="测试草稿"))[
        "available"
    ]
    with pytest.raises(CustomException, match="场景名称已存在"):
        await service.create_prompt_profile_draft(
            tenant_id=TEST_TENANT_ID,
            values={**values, "creation_key": "different-new-id"},
        )
    rows = await service.list_prompt_profiles(tenant_id=TEST_TENANT_ID)
    assert rows["total"] == 0
    rows = await service.list_prompt_profiles(tenant_id=TEST_TENANT_ID, include_drafts=True)
    assert rows["total"] == 1


async def test_stale_module_save_does_not_overwrite_and_does_not_accept_other_modules(b4_service):
    service, *_ = b4_service
    draft = await service.create_prompt_profile_draft(
        tenant_id=TEST_TENANT_ID,
        values={
            "name": "并发测试",
            "creation_key": "revision-test",
            "module": "opening",
            "opening_message": "A",
        },
    )
    common = {"tenant_id": TEST_TENANT_ID, "profile_id": int(draft["id"])}
    await service.save_prompt_profile_module(
        **common,
        values={
            "module": "opening",
            "expected_revision": draft["editRevision"],
            "opening_message": "B",
        },
    )
    with pytest.raises(CustomException, match="已更新"):
        await service.save_prompt_profile_module(
            **common,
            values={
                "module": "opening",
                "expected_revision": draft["editRevision"],
                "opening_message": "C",
            },
        )
    current = await service.get_prompt_profile(**common)
    assert current["openingMessage"] == "B"
    with pytest.raises(CustomException, match="模块"):
        await service.save_prompt_profile_module(
            **common,
            values={
                "module": "opening",
                "expected_revision": current["editRevision"],
                "opening_message": "D",
                "product_info": "不可夹带保存",
            },
        )


async def test_deleted_draft_name_reuse_does_not_resurrect_creation_key(b4_service):
    service, *_ = b4_service
    values = {
        "name": "可删除",
        "creation_key": "deleted-key",
        "module": "opening",
        "opening_message": "A",
    }
    draft = await service.create_prompt_profile_draft(tenant_id=TEST_TENANT_ID, values=values)
    await service.delete_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        profile_id=int(draft["id"]),
        expected_revision=draft["editRevision"],
    )
    with pytest.raises(CustomException, match="已删除"):
        await service.create_prompt_profile_draft(tenant_id=TEST_TENANT_ID, values=values)
    recreated = await service.create_prompt_profile_draft(
        tenant_id=TEST_TENANT_ID,
        values={**values, "creation_key": "new-key"},
    )
    assert recreated["id"] != draft["id"]
    assert recreated["sceneCode"] != draft["sceneCode"]


def test_module_variables_share_full_save_validation():
    with pytest.raises(CustomException, match="变量定义冲突"):
        module_values({
            "module": "opening",
            "variables": [
                {"key": "a", "label": "姓名"},
                {"key": "b", "label": " 姓名 "},
            ],
        })
    stored = json.dumps([{"key": f"k{index}", "label": f"变量{index}"} for index in range(100)])
    with pytest.raises(ValidationError):
        module_values(
            {"module": "opening", "variables": [{"key": "extra", "label": "额外"}]}, stored
        )
    with pytest.raises(ValidationError):
        PromptProfileRevisionRequest.model_validate({})


async def test_deleted_scene_history_remains_readable_but_not_usable(b4_service):
    service, repo, *_ = b4_service
    ready = await service.create_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        values={
            "name": "保留历史",
            "opening_message": "您好",
            "prompt_text": "介绍产品",
        },
    )
    scope = {"tenant_id": TEST_TENANT_ID, "profile_id": int(ready["id"])}
    await service.delete_prompt_profile(**scope, expected_revision=ready["editRevision"])
    assert (await service.list_prompt_profile_versions(**scope))["rows"][0]["sceneDeleted"]
    assert (
        await service.get_prompt_profile_version(**scope, version_id=int(ready["currentVersionId"]))
    )["sceneDeleted"]
    assert (await service.list_prompt_profile_version_applications(**scope))["total"] == 0
    with pytest.raises(CustomException, match="已删除"):
        await require_available_profile(repo.db, **scope)
    with pytest.raises(CustomException, match="已删除"):
        await service.optimize_prompt(tenant_id=TEST_TENANT_ID, values={"profile_id": ready["id"]})


async def test_active_record_blocks_deletion_and_failed_module_rolls_back(b4_service, monkeypatch):
    service, repo, *_ = b4_service
    ready = await service.create_prompt_profile(
        tenant_id=TEST_TENANT_ID,
        values={
            "name": "事务测试",
            "opening_message": "A",
            "prompt_text": "介绍产品",
        },
    )
    scope = {"tenant_id": TEST_TENANT_ID, "profile_id": int(ready["id"])}
    await repo.db.commit()
    monkeypatch.setattr(repo, "get_prompt_profile_version", AsyncMock(return_value=None))
    with pytest.raises(CustomException, match="当前版本不存在"):
        await service.save_prompt_profile_module(
            **scope,
            values={
                "module": "opening",
                "opening_message": "B",
                "expected_revision": ready["editRevision"],
            },
        )
    assert (await service.get_prompt_profile(**scope))["openingMessage"] == "A"
    repo.db.add(
        AiCallRecordModel(
            id=991,
            tenant_id=TEST_TENANT_ID,
            call_id="active-call",
            scene_code=ready["sceneCode"],
            entry_type="web",
            room_name="active-room",
            participant_identity="caller",
            status="preparing",
            started_at=datetime.now(timezone.utc),
        )
    )
    await repo.db.flush()
    with pytest.raises(CustomException) as error:
        await service.delete_prompt_profile(**scope, expected_revision=ready["editRevision"])
    assert error.value.data["errorCode"] == "PROMPT_DELETE_BLOCKED"
    assert error.value.data["blockers"][0]["type"] == "call"


async def test_public_start_cannot_impersonate_outbound_attempt(b4_service):
    service, repo, *_ = b4_service
    draft = await service.create_prompt_profile(
        tenant_id=TEST_TENANT_ID, values={"name": "不可通话"}
    )
    with pytest.raises(CustomException, match="草稿"):
        await RuntimeCommandRepository(repo.db).create_start_call(
            StartCallIntent(
                tenant_id=TEST_TENANT_ID,
                entry_type="web",
                idempotency_key="fake-task",
                payload={},
                business_type="outbound_attempt",
                scene_code=draft["sceneCode"],
            )
        )
    assert await repo.db.scalar(select(func.count()).select_from(AiCallRecordModel)) == 0


async def test_ai_explicit_operation_keeps_optimization_module_scoped(b4_service):
    service, *_ = b4_service
    service.prompt_optimizer = AsyncMock()
    request = PromptOptimizeRequest.model_validate({
        "targetType": "scenePrompt",
        "operation": "optimize",
        "currentContent": "正文",
        "sceneContext": {
            "sceneName": "其他名称",
            "productInfo": "其他模块",
            "commonPrompt": "隐藏规则",
        },
    })
    await service.optimize_prompt(tenant_id=TEST_TENANT_ID, values=request.model_dump())
    payload = service.prompt_optimizer.optimize.call_args.args[0]
    assert payload["currentContent"] == "正文"
    assert (
        "commonPrompt" not in payload
        and "productInfo" not in payload
        and "sceneName" not in payload
    )
