from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.model import AiCallPromptProfileModel, AiCallVoiceProfileModel
from app.api.v1.ai_call.outbound.model import (
    AiCallOutboundValidationModel,
    AiCallOutboundValidationRowModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import (
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
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.utils.id_util import generate_snowflake_id


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
) -> dict:
    result = {
        "taskName": task_name,
        "taskMode": task_mode,
        "promptProfileId": str(prompt_id),
        "sceneCode": "intro_contract",
        "voice": "Tina",
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


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbound-task.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
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
        "error_message",
    } <= {column.name for column in AiCallOutboundTaskModel.__table__.columns}
    assert {"tenant_id", "task_id", "validation_id"} <= {
        column.name for column in AiCallOutboundTargetModel.__table__.columns
    }
    for model in (
        AiCallOutboundRuleModel,
        AiCallOutboundTaskModel,
        AiCallOutboundTargetModel,
    ):
        assert not model.__table__.foreign_keys
        assert not sa_inspect(model).relationships
        assert all(column.type.__class__.__name__ != "JSONB" for column in model.__table__.columns)


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
    assert [target.phone_number for target in targets] == [phone for phone, _ in rows]
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
    assert targets[0].phone_number == "19900001001"
    assert task_out.rule_name == "工作日规则"
    assert "09:00" in task_out.rule_summary
    assert task_out.prompt_name == "合同审查产品介绍"
    assert task_out.voice_name == "甜甜 Tina"


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
