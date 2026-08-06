from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.model import AiCallPromptProfileModel, AiCallVoiceProfileModel
from app.api.v1.ai_call.outbound.model import (
    AiCallOutboundValidationModel,
    AiCallOutboundValidationRowModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundRuleModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.rule_task_schema import (
    CallRuleIn,
    CreateTaskRequest,
    SingleValidationRequest,
    UpdateTaskScheduleRequest,
)
from app.api.v1.ai_call.outbound.rule_task_service import OutboundRuleTaskService
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.api.v1.ai_call.outbound.sip_line_service import SipLineService
from app.api.v1.ai_call.voice.model import AiCallTenantVoiceProfileModel
from app.api.v1.ai_call.voice.repository import VoiceRepository
from app.api.v1.ai_call.voice.service import VoiceDeletionService
from app.config.setting import settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.utils.id_util import generate_snowflake_id

QWEN_OMNI_REALTIME_TARGET_MODEL = settings.QWEN_REALTIME_MODEL


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _rule_payload(name: str = "工作日规则") -> CallRuleIn:
    return CallRuleIn(
        rule_name=name,
        enabled=True,
        call_windows=[
            {"startTime": "09:00", "endTime": "12:00"},
            {"startTime": "14:00", "endTime": "18:00"},
        ],
        retry_count=2,
        retry_intervals_minutes=[30, 60],
        retryable_results=["no_answer", "busy", "call_failed"],
    )


def _validation_config(
    *,
    task_mode: str,
    rule_id: int,
    prompt_id: int,
    task_name: str = "合同审查客户回访",
    phone_number: str | None = None,
    customer_name: str | None = None,
    voice: str = "Tina",
    answer_mode: str = "linphone",
) -> dict:
    result = {
        "taskName": task_name,
        "taskMode": task_mode,
        "answerMode": answer_mode,
        "promptProfileId": str(prompt_id),
        "sceneCode": "intro_contract",
        "voice": voice,
        "ruleId": str(rule_id),
        "executionMode": "immediate",
        "scheduledAt": None,
    }
    if task_mode == "single":
        result["phoneNumber"] = phone_number
        result["customerName"] = customer_name
    return result


def _create_task_request(config: dict, validation_id: int) -> CreateTaskRequest:
    return CreateTaskRequest.model_validate({
        **config,
        "validationId": str(validation_id),
    })


def test_web_single_request_allows_empty_phone_and_excludes_it_from_config() -> None:
    request = SingleValidationRequest(
        task_name="Web 接听任务",
        task_mode="single",
        answer_mode="web",
        prompt_profile_id="1001",
        scene_code="intro_contract",
        voice="Tina",
        rule_id="1002",
        execution_mode="immediate",
        phone_number=None,
        customer_name="王先生",
    )

    assert request.answer_mode == "web"
    assert request.phone_number is None
    assert request.config_dict()["answerMode"] == "web"
    assert "phoneNumber" not in request.config_dict()


@pytest.mark.parametrize(
    "payload",
    [
        {"task_mode": "single", "answer_mode": "linphone", "phone_number": None},
        {"task_mode": "batch", "answer_mode": "web", "phone_number": None},
    ],
)
def test_answer_mode_rejects_invalid_task_combinations(payload: dict) -> None:
    with pytest.raises(ValidationError):
        CreateTaskRequest.model_validate({
            "taskName": "接听方式校验",
            "promptProfileId": "1001",
            "sceneCode": "intro_contract",
            "voice": "Tina",
            "ruleId": "1002",
            "executionMode": "immediate",
            "validationId": "1003",
            **payload,
        })


@pytest.mark.anyio
async def test_web_single_validation_and_task_creation_skip_sip_line(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()

    service.line_service.resolve_default = AsyncMock(
        side_effect=AssertionError("Web 接听不应解析 SIP 线路")
    )
    request = SingleValidationRequest(
        task_name="Web 单个客户任务",
        task_mode="single",
        answer_mode="web",
        prompt_profile_id=str(prompt_id),
        scene_code="intro_contract",
        voice="Tina",
        rule_id=str(rule.id),
        execution_mode="immediate",
        phone_number=None,
        customer_name="王先生",
    )

    async with database() as session:
        validation = await service.validate_single(session, "tenant-a", 10, request)
        await session.commit()
        validation_id = validation.id

    assert validation.line_id is None
    assert validation.line_snapshot_json is None
    service.line_service.resolve_default.assert_not_awaited()

    async with database() as session:
        task, created = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-web-single",
            _create_task_request(request.model_dump(mode="json", by_alias=True), validation_id),
        )
        await session.commit()
        targets, total = await service.list_targets(
            session,
            "tenant-a",
            task.id,
            page_num=1,
            page_size=20,
            phone_number=None,
            customer_name=None,
            target_status=None,
        )

    assert created is True
    assert task.answer_mode == "web"
    assert task.line_id is None
    assert "sipLine" not in json.loads(task.config_snapshot_json)
    assert total == 1
    assert targets[0].phone_number is None


@pytest.mark.anyio
async def test_task_write_query_uses_row_lock() -> None:
    marker = object()

    class CapturingSession:
        statement = None

        async def scalar(self, statement):
            self.statement = statement
            return marker

    session = CapturingSession()
    result = await OutboundRuleTaskService._get_task_for_update(
        session,
        "tenant-a",
        123,
    )

    assert result is marker
    assert session.statement is not None
    assert session.statement._for_update_arg is not None


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbound-task.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        now = _now()
        for tenant_id in ("tenant-a", "tenant-b", "tenant-c"):
            session.add(
                AiCallSipLineModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    line_code=f"default-{tenant_id}",
                    line_name=f"{tenant_id}默认线路",
                    enabled=True,
                    default_marker="OUTBOUND",
                    adapter_type="livekit_sip",
                    route_mode="inline_hostname",
                    trunk_id=None,
                    proxy_host="127.0.0.1",
                    proxy_port=5089,
                    auth_mode="ip_allowlist",
                    caller_number="10000",
                    destination_country="CN",
                    max_concurrency=1,
                    originate_timeout_seconds=45,
                    health_status="AVAILABLE",
                    health_message="测试线路",
                    last_checked_at=now,
                    deleted=False,
                    deleted_at=None,
                    created_by=10,
                    updated_by=10,
                    created_at=now,
                    updated_at=now,
                )
            )
        await session.commit()
    yield factory
    await engine.dispose()


async def _seed_references(database) -> tuple[int, int]:
    prompt_id = generate_snowflake_id()
    voice_id = generate_snowflake_id()
    async with database() as session:
        now = _now()
        session.add(
            AiCallPromptProfileModel(
                id=prompt_id,
                scene_code="intro_contract",
                name="合同审查产品介绍",
                provider_key="static_profile",
                prompt_text="测试提示词",
                opening_message="您好",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            AiCallVoiceProfileModel(
                id=voice_id,
                voice="Tina",
                display_name="甜甜 Tina",
                voice_type="内置",
                gender="女声",
                target_model="qwen3.5-omni-plus-realtime",
                description=None,
                sort_order=1,
                remark=None,
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
    return prompt_id, voice_id


async def _seed_tenant_voice(
    database,
    *,
    tenant_id: str = "tenant-a",
    voice: str = "qwen-omni-vc-tenant",
    status: str = "ENABLED",
    target_model: str = QWEN_OMNI_REALTIME_TARGET_MODEL,
) -> int:
    profile_id = generate_snowflake_id()
    async with database() as session:
        now = _now()
        session.add(
            AiCallTenantVoiceProfileModel(
                id=profile_id,
                tenant_id=tenant_id,
                display_name="租户客服音色",
                voice=voice,
                voice_type="自定义复刻",
                gender="女声",
                language="zh",
                target_model=target_model,
                provider="qwen",
                status=status,
                latest_enrollment_id=None,
                provider_created_at=now,
                error_message=None,
                created_by=10,
                deleted_by=None,
                deleted_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
    return profile_id


async def _seed_passed_validation(
    database,
    *,
    tenant_id: str,
    config: dict,
    rows: list[tuple[str, str | None]],
) -> int:
    validation_id = generate_snowflake_id()
    async with database() as session:
        now = _now()
        line = await SipLineService().resolve_default(session, tenant_id)
        session.add(
            AiCallOutboundValidationModel(
                id=validation_id,
                tenant_id=tenant_id,
                status="PASSED",
                processing_stage="COMPLETED",
                original_filename="test.xlsx",
                temp_file_path=None,
                file_size=1,
                task_config_json=json.dumps(config, ensure_ascii=False),
                line_id=line.id,
                line_snapshot_json=SipLineService().snapshot_json(line),
                valid_target_count=len(rows),
                issue_count=0,
                issue_stats_json="{}",
                error_message=None,
                retryable=False,
                retry_count=0,
                created_by=10,
                created_at=now,
                updated_at=now,
                finished_at=now,
            )
        )
        session.add_all([
            AiCallOutboundValidationRowModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                validation_id=validation_id,
                row_number=index + 2,
                phone_number=phone,
                customer_name=name,
                normalized_phone=phone,
                is_valid=True,
                reasons_json=None,
                duplicate_row_number=None,
                created_at=now,
            )
            for index, (phone, name) in enumerate(rows)
        ])
        await session.commit()
    return validation_id


@pytest.mark.anyio
async def test_formal_task_tenant_voice_lookup_uses_profile_row_lock() -> None:
    marker = AiCallTenantVoiceProfileModel(
        id=1,
        tenant_id="tenant-a",
        display_name="租户音色",
        voice="tenant-voice",
        voice_type="自定义复刻",
        gender="女声",
        language="zh",
        target_model=QWEN_OMNI_REALTIME_TARGET_MODEL,
        provider="qwen",
        status="ENABLED",
        latest_enrollment_id=None,
        provider_created_at=None,
        error_message=None,
        created_by=10,
        deleted_by=None,
        deleted_at=None,
        created_at=_now(),
        updated_at=_now(),
    )

    class CapturingSession:
        def __init__(self) -> None:
            self.statements = []

        async def scalar(self, statement):
            self.statements.append(statement)
            return None if len(self.statements) == 1 else marker

    session = CapturingSession()
    service = OutboundRuleTaskService(lambda: None)

    resolved = await service._resolve_voice(
        session,
        tenant_id="tenant-a",
        voice="tenant-voice",
        lock_tenant_voice=True,
    )

    assert resolved is marker
    assert session.statements[0].column_descriptions[0]["entity"] is AiCallVoiceProfileModel
    assert session.statements[1].column_descriptions[0]["entity"] is AiCallTenantVoiceProfileModel
    assert session.statements[1]._for_update_arg is not None
    assert session.statements[1]._for_update_arg.read is True
    assert "target_model" in str(session.statements[1])


@pytest.mark.anyio
async def test_formal_task_uses_enabled_tenant_voice_snapshot(database) -> None:
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-tenant",
        phone_number="19900001111",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001111", None)],
    )

    async with database() as session:
        task, created = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-tenant-voice",
            _create_task_request(config, validation_id),
        )
        await session.commit()

    assert created is True
    snapshot = json.loads(task.config_snapshot_json)
    assert snapshot["voice"] == {
        "scope": "TENANT",
        "profileId": str(profile_id),
        "voice": "qwen-omni-vc-tenant",
        "voiceName": "租户客服音色",
        "voiceType": "自定义复刻",
        "targetModel": QWEN_OMNI_REALTIME_TARGET_MODEL,
    }
    assert task.voice == "qwen-omni-vc-tenant"
    assert task.voice_name == "租户客服音色"
    assert task.voice_type == "自定义复刻"
    assert task.voice_target_model == QWEN_OMNI_REALTIME_TARGET_MODEL

    task_out = service.task_out(task)
    task_payload = task_out.model_dump(mode="json", by_alias=True)
    assert task_payload["taskId"] == str(task.id)
    assert task_payload["voice"] == "qwen-omni-vc-tenant"
    assert task_payload["voiceName"] == "租户客服音色"
    assert task_payload["voiceType"] == "自定义复刻"
    assert task_payload["voiceTargetModel"] == QWEN_OMNI_REALTIME_TARGET_MODEL


@pytest.mark.anyio
async def test_formal_task_voice_snapshot_uses_realtime_model_setting(
    database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared_model = "qwen-realtime-shared-test"
    monkeypatch.setattr(settings, "QWEN_REALTIME_MODEL", shared_model)
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(
        database,
        voice="qwen-omni-vc-shared",
        target_model=shared_model,
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-shared",
        phone_number="19900001111",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001111", None)],
    )

    async with database() as session:
        task, created = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-shared-realtime-model",
            _create_task_request(config, validation_id),
        )
        await session.commit()

    snapshot = json.loads(task.config_snapshot_json)
    assert created is True
    assert snapshot["voice"]["profileId"] == str(profile_id)
    assert snapshot["voice"]["targetModel"] == shared_model
    assert task.voice_target_model == shared_model


@pytest.mark.anyio
async def test_task_history_keeps_voice_snapshot_after_tenant_voice_deleted(
    database,
) -> None:
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(
        database,
        voice="qwen-omni-vc-history",
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-history",
        phone_number="19900001118",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001118", None)],
    )

    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-history-voice",
            _create_task_request(config, validation_id),
        )
        task.status = "COMPLETED"
        await session.commit()
        task_id = task.id

    async with database() as session:
        profile = await session.get(AiCallTenantVoiceProfileModel, profile_id)
        assert profile is not None
        profile.status = "DELETED"
        profile.voice = None
        profile.display_name = "删除后名称"
        profile.voice_type = "删除后类型"
        profile.target_model = "deleted-model"
        await session.commit()

    async with database() as session:
        task_out = await service.get_task(session, "tenant-a", task_id)
        persisted_task = await session.get(AiCallOutboundTaskModel, task_id)

    assert persisted_task is not None
    assert task_out.voice == "qwen-omni-vc-history"
    assert task_out.voice_name == "租户客服音色"
    assert task_out.voice_type == "自定义复刻"
    assert task_out.voice_target_model == QWEN_OMNI_REALTIME_TARGET_MODEL
    assert json.loads(persisted_task.config_snapshot_json)["voice"] == {
        "scope": "TENANT",
        "profileId": str(profile_id),
        "voice": "qwen-omni-vc-history",
        "voiceName": "租户客服音色",
        "voiceType": "自定义复刻",
        "targetModel": QWEN_OMNI_REALTIME_TARGET_MODEL,
    }


@pytest.mark.anyio
async def test_formal_task_rejects_non_enabled_tenant_voice(database) -> None:
    prompt_id, _ = await _seed_references(database)
    await _seed_tenant_voice(
        database,
        voice="qwen-omni-vc-deleting",
        status="DELETING",
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-deleting",
        phone_number="19900001112",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001112", None)],
    )

    async with database() as session:
        with pytest.raises(CustomException) as exc_info:
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-deleting-voice",
                _create_task_request(config, validation_id),
            )

    assert exc_info.value.status_code == 409
    assert "不可用" in exc_info.value.msg


@pytest.mark.anyio
async def test_formal_task_builtin_voice_is_scoped_by_target_model(database) -> None:
    prompt_id, builtin_voice_id = await _seed_references(database)
    async with database() as session:
        now = _now()
        session.add(
            AiCallVoiceProfileModel(
                id=generate_snowflake_id(),
                voice="Tina",
                display_name="错误模型 Tina",
                voice_type="内置",
                gender="女声",
                target_model="other-model",
                description=None,
                sort_order=0,
                remark=None,
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        phone_number="19900001113",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001113", None)],
    )

    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-built-in-model",
            _create_task_request(config, validation_id),
        )
        await session.commit()

    snapshot = json.loads(task.config_snapshot_json)
    assert snapshot["voice"] == {
        "scope": "BUILTIN",
        "profileId": str(builtin_voice_id),
        "voice": "Tina",
        "voiceName": "甜甜 Tina",
        "voiceType": "内置",
        "targetModel": QWEN_OMNI_REALTIME_TARGET_MODEL,
    }
    assert snapshot["voice"]["profileId"].isdigit()
    assert task.voice_type == "内置"
    assert task.voice_target_model == QWEN_OMNI_REALTIME_TARGET_MODEL


@pytest.mark.anyio
async def test_formal_task_rejects_tenant_voice_from_other_target_model(database) -> None:
    prompt_id, _ = await _seed_references(database)
    await _seed_tenant_voice(
        database,
        voice="tenant-wrong-model",
        target_model="other-model",
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="tenant-wrong-model",
        phone_number="19900001116",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001116", None)],
    )

    async with database() as session:
        with pytest.raises(CustomException) as exc_info:
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-wrong-model-voice",
                _create_task_request(config, validation_id),
            )

    assert exc_info.value.status_code == 400
    assert "音色不存在" in exc_info.value.msg


@pytest.mark.anyio
async def test_formal_task_rejects_tenant_voice_from_other_tenant(database) -> None:
    prompt_id, _ = await _seed_references(database)
    await _seed_tenant_voice(
        database,
        tenant_id="tenant-b",
        voice="tenant-b-private-voice",
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="tenant-b-private-voice",
        phone_number="19900001119",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001119", None)],
    )

    async with database() as session:
        with pytest.raises(CustomException) as exc_info:
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-cross-tenant-voice",
                _create_task_request(config, validation_id),
            )

    assert exc_info.value.status_code == 400
    assert "音色不存在" in exc_info.value.msg


@pytest.mark.anyio
@pytest.mark.parametrize("tenant_status", ["DELETING", "DELETE_FAILED"])
async def test_formal_task_builtin_voice_wins_over_same_name_unavailable_tenant_voice(
    database,
    tenant_status: str,
) -> None:
    prompt_id, builtin_voice_id = await _seed_references(database)
    await _seed_tenant_voice(
        database,
        voice="Tina",
        status=tenant_status,
    )
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        phone_number="19900001117",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001117", None)],
    )

    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            f"idem-builtin-over-{tenant_status.lower()}",
            _create_task_request(config, validation_id),
        )
        await session.commit()

    snapshot = json.loads(task.config_snapshot_json)
    assert snapshot["voice"]["scope"] == "BUILTIN"
    assert snapshot["voice"]["profileId"] == str(builtin_voice_id)
    assert snapshot["voice"]["voiceName"] == "甜甜 Tina"


@pytest.mark.anyio
@pytest.mark.parametrize("first_writer", ["deletion", "task"])
async def test_sqlite_serializes_task_creation_and_voice_deletion(
    database,
    monkeypatch: pytest.MonkeyPatch,
    first_writer: str,
) -> None:
    async with database() as session:
        await session.execute(text("PRAGMA journal_mode=WAL"))
        await session.commit()
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(database)
    task_service = OutboundRuleTaskService(database)
    deletion_service = VoiceDeletionService(session_factory=database)
    async with database() as session:
        rule = await task_service.create_rule(
            session,
            "tenant-a",
            10,
            _rule_payload(),
        )
        await session.commit()
    phone_number = "19900001118" if first_writer == "deletion" else "19900001119"
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-tenant",
        phone_number=phone_number,
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[(phone_number, None)],
    )
    first_read = asyncio.Event()
    release_first = asyncio.Event()
    second_sql_attempted = asyncio.Event()
    first_session = database()
    second_session = database()

    async def create_formal_task(session):
        task, _ = await task_service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            f"race-task-{first_writer}",
            _create_task_request(config, validation_id),
        )
        await session.commit()
        return task

    async def request_voice_deletion(session):
        return await deletion_service.request_deletion(
            session,
            tenant_id="tenant-a",
            user_id=10,
            profile_id=profile_id,
            idempotency_key=f"race-delete-{first_writer}",
        )

    original_second_execute = second_session.execute

    async def observed_second_execute(statement, *args, **kwargs):
        if str(statement).strip().upper() == "BEGIN IMMEDIATE":
            second_sql_attempted.set()
        return await original_second_execute(statement, *args, **kwargs)

    monkeypatch.setattr(second_session, "execute", observed_second_execute)
    try:
        if first_writer == "deletion":
            original_summary = VoiceRepository.task_reference_summary

            async def paused_summary(repository, **kwargs):
                result = await original_summary(repository, **kwargs)
                first_read.set()
                await release_first.wait()
                return result

            original_resolve_voice = task_service._resolve_voice

            async def observed_resolve_voice(*args, **kwargs):
                result = await original_resolve_voice(*args, **kwargs)
                second_sql_attempted.set()
                return result

            monkeypatch.setattr(
                VoiceRepository,
                "task_reference_summary",
                paused_summary,
            )
            monkeypatch.setattr(
                task_service,
                "_resolve_voice",
                observed_resolve_voice,
            )
            first = asyncio.create_task(request_voice_deletion(first_session))
            await asyncio.wait_for(first_read.wait(), timeout=2)
            second = asyncio.create_task(create_formal_task(second_session))
        else:
            original_resolve_voice = task_service._resolve_voice

            async def paused_resolve_voice(*args, **kwargs):
                result = await original_resolve_voice(*args, **kwargs)
                first_read.set()
                await release_first.wait()
                return result

            original_summary = VoiceRepository.task_reference_summary

            async def observed_summary(repository, **kwargs):
                result = await original_summary(repository, **kwargs)
                second_sql_attempted.set()
                return result

            monkeypatch.setattr(
                task_service,
                "_resolve_voice",
                paused_resolve_voice,
            )
            monkeypatch.setattr(
                VoiceRepository,
                "task_reference_summary",
                observed_summary,
            )
            first = asyncio.create_task(create_formal_task(first_session))
            await asyncio.wait_for(first_read.wait(), timeout=2)
            second = asyncio.create_task(request_voice_deletion(second_session))

        await asyncio.wait_for(second_sql_attempted.wait(), timeout=2)
        release_first.set()
        first_result, second_result = await asyncio.wait_for(
            asyncio.gather(first, second, return_exceptions=True),
            timeout=5,
        )
    finally:
        await first_session.close()
        await second_session.close()

    assert not isinstance(first_result, Exception)
    assert isinstance(second_result, CustomException)
    assert second_result.status_code == 409
    async with database() as session:
        stored_task = await session.scalar(
            select(AiCallOutboundTaskModel).where(
                AiCallOutboundTaskModel.idempotency_key == f"race-task-{first_writer}"
            )
        )
        profile = await session.get(AiCallTenantVoiceProfileModel, profile_id)
    if first_writer == "deletion":
        assert stored_task is None
        assert profile is not None and profile.status == "DELETING"
    else:
        assert stored_task is not None
        assert profile is not None and profile.status == "ENABLED"


@pytest.mark.anyio
async def test_delete_commits_deleting_before_task_create_rejects_insert(database) -> None:
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(database)
    task_service = OutboundRuleTaskService(database)
    deletion_service = VoiceDeletionService(session_factory=database)
    async with database() as session:
        rule = await task_service.create_rule(
            session,
            "tenant-a",
            10,
            _rule_payload(),
        )
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-tenant",
        phone_number="19900001114",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001114", None)],
    )
    async with database() as session:
        await deletion_service.request_deletion(
            session,
            tenant_id="tenant-a",
            user_id=10,
            profile_id=profile_id,
            idempotency_key="delete-before-task",
        )

    async with database() as session:
        with pytest.raises(CustomException) as exc_info:
            await task_service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "task-after-delete",
                _create_task_request(config, validation_id),
            )
        task = await session.scalar(
            select(AiCallOutboundTaskModel).where(
                AiCallOutboundTaskModel.idempotency_key == "task-after-delete"
            )
        )

    assert exc_info.value.status_code == 409
    assert task is None


@pytest.mark.anyio
async def test_task_commit_before_delete_recheck_returns_blocking_reference(database) -> None:
    prompt_id, _ = await _seed_references(database)
    profile_id = await _seed_tenant_voice(database)
    task_service = OutboundRuleTaskService(database)
    deletion_service = VoiceDeletionService(session_factory=database)
    async with database() as session:
        rule = await task_service.create_rule(
            session,
            "tenant-a",
            10,
            _rule_payload(),
        )
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        voice="qwen-omni-vc-tenant",
        phone_number="19900001115",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001115", None)],
    )
    async with database() as session:
        task, _ = await task_service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "task-before-delete",
            _create_task_request(config, validation_id),
        )
        await session.commit()

    async with database() as session:
        with pytest.raises(CustomException) as exc_info:
            await deletion_service.request_deletion(
                session,
                tenant_id="tenant-a",
                user_id=10,
                profile_id=profile_id,
                idempotency_key="delete-after-task",
            )

    assert exc_info.value.status_code == 409
    assert exc_info.value.data == {
        "blockingTaskCount": 1,
        "historicalTaskCount": 0,
        "blockingTaskIds": [str(task.id)],
    }


def test_models_are_tenant_scoped_without_foreign_keys_or_jsonb() -> None:
    assert {
        "tenant_id",
        "call_windows_json",
        "retry_intervals_json",
        "retryable_results_json",
        "deleted",
    } <= {column.name for column in AiCallOutboundRuleModel.__table__.columns}
    assert {
        "tenant_id",
        "validation_id",
        "config_snapshot_json",
        "answer_mode",
        "error_message",
        "voice",
        "voice_name",
        "voice_type",
        "voice_target_model",
    } <= {column.name for column in AiCallOutboundTaskModel.__table__.columns}
    assert {"tenant_id", "task_id", "validation_id"} <= {
        column.name for column in AiCallOutboundTargetModel.__table__.columns
    }
    assert AiCallOutboundTargetModel.__table__.columns.phone_number.nullable is True
    for model in (
        AiCallOutboundRuleModel,
        AiCallOutboundTaskModel,
        AiCallOutboundTargetModel,
    ):
        assert not model.__table__.foreign_keys
        assert not sa_inspect(model).relationships
        assert all(column.type.__class__.__name__ != "JSONB" for column in model.__table__.columns)

    migration = (
        (
            Path(__file__).parents[1]
            / "docs"
            / "livekit-ai-outbound"
            / "sql"
            / "phase-h2-outbound-rule-task-postgres.sql"
        )
        .read_text(encoding="utf-8")
        .lower()
    )
    assert "add column if not exists voice_type" in migration
    assert "add column if not exists voice_target_model" in migration

    web_answer_migration = (
        (
            Path(__file__).parents[1]
            / "docs"
            / "livekit-ai-outbound"
            / "sql"
            / "phase-h10-outbound-web-answer-mode-postgres.sql"
        )
        .read_text(encoding="utf-8")
        .lower()
    )
    assert "add column if not exists answer_mode varchar(16) not null default 'linphone'" in web_answer_migration
    assert "alter column phone_number drop not null" in web_answer_migration


def test_rule_task_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/outbound-rules",
        "/ai-call/outbound-rules/meta",
        "/ai-call/outbound-rules/{rule_id}",
        "/ai-call/outbound-validations/single",
        "/ai-call/outbound-tasks",
        "/ai-call/outbound-tasks/{task_id}",
        "/ai-call/outbound-tasks/{task_id}/schedule",
        "/ai-call/outbound-tasks/{task_id}/pause",
        "/ai-call/outbound-tasks/{task_id}/resume",
        "/ai-call/outbound-tasks/{task_id}/stop",
        "/ai-call/outbound-tasks/{task_id}/cancel",
        "/ai-call/outbound-tasks/{task_id}/targets",
    } <= paths


@pytest.mark.anyio
async def test_default_rule_initialization_is_idempotent_and_tenant_scoped(database) -> None:
    service = OutboundRuleTaskService(database)
    async with database() as session:
        first = await service.ensure_default_rule(session, "tenant-a", 10)
        second = await service.ensure_default_rule(session, "tenant-a", 10)
        other = await service.ensure_default_rule(session, "tenant-b", 20)
        await service.create_rule(session, "tenant-c", 30, _rule_payload("自定义规则"))
        tenant_c_default = await service.ensure_default_rule(session, "tenant-c", 30)
        await session.commit()

    assert first.id == second.id
    assert first.id != other.id
    assert tenant_c_default.rule_name == "工作日规则"
    async with database() as session:
        rules_a, total_a = await service.list_rules(
            session,
            "tenant-a",
            user_id=10,
            page_num=1,
            page_size=20,
            rule_name=None,
            enabled=True,
        )
        rules_b, total_b = await service.list_rules(
            session,
            "tenant-b",
            user_id=20,
            page_num=1,
            page_size=20,
            rule_name=None,
            enabled=True,
        )
    assert total_a == total_b == 1
    assert rules_a[0].rule_id.isdigit()
    assert rules_a[0].rule_name == "工作日规则"
    assert rules_a[0].call_windows[0].start_time == "09:00"
    assert rules_b[0].rule_id != rules_a[0].rule_id


@pytest.mark.anyio
async def test_rule_crud_soft_deletes_and_preserves_tenant_isolation(database) -> None:
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload("业务规则"))
        await session.commit()
        rule_id = rule.id

    async with database() as session:
        updated = await service.update_rule(
            session,
            "tenant-a",
            10,
            rule_id,
            _rule_payload("调整后规则"),
        )
        await session.commit()
    assert updated.rule_name == "调整后规则"

    async with database() as session:
        with pytest.raises(CustomException, match="不存在"):
            await service.update_rule(
                session,
                "tenant-b",
                20,
                rule_id,
                _rule_payload("越权规则"),
            )
        await session.rollback()
        await service.delete_rule(session, "tenant-a", 10, rule_id)
        await session.commit()

    async with database() as session:
        stored = await session.get(AiCallOutboundRuleModel, rule_id)
        rows, total = await service.list_rules(
            session,
            "tenant-a",
            user_id=10,
            page_num=1,
            page_size=20,
            rule_name="调整后",
            enabled=None,
        )
    assert stored is not None and stored.deleted is True
    assert rows == [] and total == 0


@pytest.mark.anyio
async def test_single_validation_checks_phone_and_all_references(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()

    request = SingleValidationRequest(
        task_name="单号测试",
        task_mode="single",
        prompt_profile_id=str(prompt_id),
        scene_code="intro_contract",
        voice="Tina",
        rule_id=str(rule.id),
        execution_mode="immediate",
        phone_number="19900001001",
        customer_name="王先生",
    )
    async with database() as session:
        validation = await service.validate_single(session, "tenant-a", 10, request)
        await session.commit()
    assert validation.status == "PASSED"
    assert validation.valid_target_count == 1
    assert validation.line_id is not None
    assert json.loads(validation.line_snapshot_json)["lineId"] == str(validation.line_id)

    async with database() as session:
        row = await session.scalar(
            select(AiCallOutboundValidationRowModel).where(
                AiCallOutboundValidationRowModel.validation_id == validation.id
            )
        )
        assert row is not None
        assert row.phone_number == "19900001001"
        assert row.customer_name == "王先生"

        for field, value, message in (
            ("phone_number", "123", "手机号格式错误"),
            ("rule_id", str(generate_snowflake_id()), "呼叫规则不存在"),
            ("prompt_profile_id", str(generate_snowflake_id()), "提示词不存在"),
            ("voice", "missing-voice", "音色不存在"),
        ):
            invalid = request.model_copy(update={field: value})
            with pytest.raises(CustomException, match=message):
                await service.validate_single(session, "tenant-a", 10, invalid)


@pytest.mark.anyio
async def test_batch_task_creation_copies_targets_in_batches_and_is_idempotent(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database, target_copy_batch_size=2)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="batch",
        rule_id=rule.id,
        prompt_id=prompt_id,
    )
    rows = [(f"13800138{index:03d}", f"客户{index}") for index in range(5)]
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=rows,
    )
    request = _create_task_request(config, validation_id)

    async with database() as session:
        task, created = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-create-batch",
            request,
        )
        await session.commit()
        task_id = task.id
    assert created is True
    assert task.status == "SCHEDULED"
    assert task.scheduled_at is None
    assert task.total_targets == 5
    assert task.line_id is not None
    assert task.line_name == "tenant-a默认线路"
    task_snapshot = json.loads(task.config_snapshot_json)
    assert task_snapshot["sipLine"]["lineId"] == str(task.line_id)
    assert "password" not in task.config_snapshot_json.lower()
    assert "secret" not in task.config_snapshot_json.lower()

    async with database() as session:
        same, created_again = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-create-batch",
            request,
        )
        await session.commit()
        targets, total = await service.list_targets(
            session,
            "tenant-a",
            task_id,
            page_num=1,
            page_size=20,
            phone_number=None,
            customer_name=None,
            target_status=None,
        )
    assert created_again is False
    assert same.id == task_id
    assert total == 5
    assert [target.phone_number for target in targets] == [
        f"{phone[:3]}****{phone[-4:]}" for phone, _ in rows
    ]
    assert all(target.target_id.isdigit() and target.task_id == str(task_id) for target in targets)

    other_request = request.model_copy(update={"task_name": "不同任务"})
    async with database() as session:
        with pytest.raises(CustomException, match="幂等键"):
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-create-batch",
                other_request,
            )


@pytest.mark.anyio
async def test_single_task_has_one_target_and_uses_snapshots_after_rule_delete(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    request = SingleValidationRequest(
        task_name="单号正式任务",
        task_mode="single",
        prompt_profile_id=str(prompt_id),
        scene_code="intro_contract",
        voice="Tina",
        rule_id=str(rule.id),
        execution_mode="immediate",
        phone_number="19900001001",
        customer_name="王先生",
    )
    async with database() as session:
        validation = await service.validate_single(session, "tenant-a", 10, request)
        await session.commit()
        validation_id = validation.id

    create_request = _create_task_request(
        request.model_dump(mode="json", by_alias=True),
        validation_id,
    )
    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-single",
            create_request,
        )
        line = await session.get(AiCallSipLineModel, task.line_id)
        assert line is not None
        line.line_code = "renamed-after-task-created"
        line.line_name = "任务创建后改名的线路"
        await service.delete_rule(session, "tenant-a", 10, rule.id)
        await session.commit()
        task_id = task.id

    async with database() as session:
        task_out = await service.get_task(session, "tenant-a", task_id)
        targets, total = await service.list_targets(
            session,
            "tenant-a",
            task_id,
            page_num=1,
            page_size=20,
            phone_number=None,
            customer_name=None,
            target_status=None,
        )
    assert total == 1
    assert targets[0].phone_number == "199****1001"
    assert task_out.rule_name == "工作日规则"
    assert "09:00" in task_out.rule_summary
    assert task_out.prompt_name == "合同审查产品介绍"
    assert task_out.voice_name == "甜甜 Tina"
    assert task_out.line_snapshot is not None
    assert task_out.line_snapshot.line_id == task_out.line_id
    assert task_out.line_snapshot.line_code == "default-tenant-a"
    assert task_out.line_snapshot.line_name == "tenant-a默认线路"
    assert task_out.line_snapshot.model_dump(mode="json", by_alias=True) == {
        "lineId": task_out.line_id,
        "lineCode": "default-tenant-a",
        "lineName": "tenant-a默认线路",
    }


@pytest.mark.anyio
async def test_task_and_target_outputs_expose_attempt_dialer_provenance(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="single",
        rule_id=rule.id,
        prompt_id=prompt_id,
        phone_number="19900001001",
        customer_name="王先生",
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("19900001001", "王先生")],
    )

    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-attempt-provenance",
            _create_task_request(config, validation_id),
        )
        target = await session.scalar(
            select(AiCallOutboundTargetModel).where(
                AiCallOutboundTargetModel.task_id == task.id
            )
        )
        assert target is not None
        now = _now()
        target.status = "COMPLETED"
        target.attempt_count = 1
        target.latest_result = "connected"
        task.status = "COMPLETED"
        task.completed_targets = 1
        task.connected_targets = 1
        session.add(
            AiCallOutboundAttemptModel(
                id=generate_snowflake_id(),
                tenant_id="tenant-a",
                task_id=task.id,
                target_id=target.id,
                attempt_no=1,
                call_id="attempt-provenance-call",
                dialer_type="mock",
                test_scenario=None,
                command_idempotency_key=None,
                active_slot=None,
                status="COMPLETED",
                call_result="connected",
                error_message=None,
                line_id=task.line_id,
                line_code="default-tenant-a",
                provider_status_code=None,
                provider_reason=None,
                hangup_cause=None,
                started_at=now,
                ended_at=now,
                created_at=now,
                updated_at=now,
            )
        )
        await session.commit()
        task_id = task.id

    async with database() as session:
        task_out = await service.get_task(session, "tenant-a", task_id)
        targets, total = await service.list_targets(
            session,
            "tenant-a",
            task_id,
            page_num=1,
            page_size=20,
            phone_number=None,
            customer_name=None,
            target_status=None,
        )

    assert task_out.attempt_dialer_types == ["mock"]
    assert total == 1
    assert targets[0].latest_dialer_type == "mock"


@pytest.mark.anyio
async def test_task_creation_rejects_invalid_validation_config_and_deleted_rule(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="batch",
        rule_id=rule.id,
        prompt_id=prompt_id,
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("13800138000", "客户")],
    )

    async with database() as session:
        mismatch = _create_task_request({**config, "voice": "Cindy"}, validation_id)
        with pytest.raises(CustomException, match="固化配置不一致"):
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-mismatch",
                mismatch,
            )
        await session.rollback()
        await service.delete_rule(session, "tenant-a", 10, rule.id)
        await session.commit()

    async with database() as session:
        with pytest.raises(CustomException, match="呼叫规则不存在"):
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-deleted-rule",
                _create_task_request(config, validation_id),
            )
        with pytest.raises(CustomException, match="校验结果不存在"):
            await service.create_task(
                session,
                "tenant-b",
                20,
                "其他租户",
                "idem-other-tenant",
                _create_task_request(config, validation_id),
            )


@pytest.mark.anyio
async def test_task_creation_rejects_disabled_or_invalid_snapshotted_line(database) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="batch",
        rule_id=rule.id,
        prompt_id=prompt_id,
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("13800138000", "客户")],
    )
    request = _create_task_request(config, validation_id)

    async with database() as session:
        line = await session.scalar(
            select(AiCallSipLineModel).where(
                AiCallSipLineModel.tenant_id == "tenant-a",
                AiCallSipLineModel.default_marker == "OUTBOUND",
            )
        )
        line.enabled = False
        await session.commit()

    async with database() as session:
        with pytest.raises(CustomException, match="线路已停用"):
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-disabled-line",
                request,
            )

    async with database() as session:
        line = await session.scalar(
            select(AiCallSipLineModel).where(
                AiCallSipLineModel.tenant_id == "tenant-a",
                AiCallSipLineModel.default_marker == "OUTBOUND",
            )
        )
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        line.enabled = True
        validation.line_snapshot_json = "{}"
        await session.commit()

    async with database() as session:
        with pytest.raises(CustomException, match="线路快照无效"):
            await service.create_task(
                session,
                "tenant-a",
                10,
                "管理员",
                "idem-invalid-line-snapshot",
                request,
            )


@pytest.mark.anyio
async def test_scheduled_task_can_be_rescheduled_and_cancelled_but_not_run_actions(
    database,
) -> None:
    prompt_id, _ = await _seed_references(database)
    service = OutboundRuleTaskService(database)
    async with database() as session:
        rule = await service.create_rule(session, "tenant-a", 10, _rule_payload())
        await session.commit()
    config = _validation_config(
        task_mode="batch",
        rule_id=rule.id,
        prompt_id=prompt_id,
    )
    validation_id = await _seed_passed_validation(
        database,
        tenant_id="tenant-a",
        config=config,
        rows=[("13800138000", "客户")],
    )
    async with database() as session:
        task, _ = await service.create_task(
            session,
            "tenant-a",
            10,
            "管理员",
            "idem-schedule-task",
            _create_task_request(config, validation_id),
        )
        await session.commit()
        task_id = task.id

    async with database() as session:
        await service.update_schedule(
            session,
            "tenant-a",
            task_id,
            UpdateTaskScheduleRequest(
                task_name="调整后的任务",
                scheduled_at="2026-07-29 10:00:00",
            ),
        )
        for action in ("pause", "resume", "stop"):
            with pytest.raises(CustomException, match="状态不允许"):
                await service.run_action(session, "tenant-a", task_id, action)
        await service.run_action(session, "tenant-a", task_id, "cancel")
        await service.run_action(session, "tenant-a", task_id, "cancel")
        await session.commit()

    async with database() as session:
        task_out = await service.get_task(session, "tenant-a", task_id)
        targets, _ = await service.list_targets(
            session,
            "tenant-a",
            task_id,
            page_num=1,
            page_size=20,
            phone_number=None,
            customer_name=None,
            target_status=None,
        )
    assert task_out.task_name == "调整后的任务"
    assert task_out.status == "CANCELLED"
    assert task_out.scheduled_at == "2026-07-29 10:00:00"
    assert targets[0].status == "CANCELLED"
