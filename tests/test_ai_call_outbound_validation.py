from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.datastructures import Headers, UploadFile

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.outbound.controller import get_outbound_validation_service
from app.api.v1.ai_call.outbound.model import (
    AiCallOutboundValidationModel,
    AiCallOutboundValidationRowModel,
)
from app.api.v1.ai_call.outbound.schema import BatchValidationRequest
from app.api.v1.ai_call.outbound.service import OutboundValidationService
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception


def _xlsx(rows: list[list[object | None]]) -> bytes:
    worksheet_rows = []
    for row_number, values in enumerate(rows, start=1):
        cells = []
        for column_number, value in enumerate(values, start=1):
            if value is None:
                continue
            column_name = ""
            current = column_number
            while current:
                current, remainder = divmod(current - 1, 26)
                column_name = chr(65 + remainder) + column_name
            cell_ref = f"{column_name}{row_number}"
            cells.append(
                f'<c r="{cell_ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'
            )
        worksheet_rows.append(f'<row r="{row_number}">{"".join(cells)}</row>')

    files = {
        "[Content_Types].xml": """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>""",
        "_rels/.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
        "xl/workbook.xml": """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="外呼名单" sheetId="1" r:id="rId1"/></sheets>
</workbook>""",
        "xl/_rels/workbook.xml.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>""",
        "xl/worksheets/sheet1.xml": f"""<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>{"".join(worksheet_rows)}</sheetData>
</worksheet>""",
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return output.getvalue()


def _request() -> BatchValidationRequest:
    return BatchValidationRequest(
        task_name="名单任务",
        task_mode="batch",
        prompt_profile_id="1001",
        scene_code="intro_geo",
        voice="Tina",
        rule_id="2001",
        execution_mode="scheduled",
        scheduled_at="2026-07-29 10:00:00",
    )


def _upload(content: bytes, filename: str = "外呼名单.xlsx") -> UploadFile:
    return UploadFile(
        io.BytesIO(content),
        filename=filename,
        headers=Headers({
            "content-type": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        }),
    )


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbound.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _accept(
    service: OutboundValidationService,
    database,
    content: bytes,
    *,
    tenant_id: str = "tenant-a",
    filename: str = "外呼名单.xlsx",
) -> tuple[int, str]:
    async with database() as session:
        validation = await service.accept_batch(
            db=session,
            tenant_id=tenant_id,
            user_id=20,
            file=_upload(content, filename),
            request=_request(),
        )
        await session.commit()
        return validation.id, validation.temp_file_path


def test_models_are_tenant_scoped_without_foreign_keys_or_jsonb() -> None:
    assert {
        "tenant_id",
        "task_config_json",
        "status",
        "processing_stage",
        "temp_file_path",
    } <= set(AiCallOutboundValidationModel.__table__.columns.keys())
    assert {
        "tenant_id",
        "validation_id",
        "row_number",
        "reasons_json",
    } <= set(AiCallOutboundValidationRowModel.__table__.columns.keys())
    for model in (AiCallOutboundValidationModel, AiCallOutboundValidationRowModel):
        assert not model.__table__.foreign_keys
        assert not sa_inspect(model).relationships
        assert all(column.type.__class__.__name__ != "JSONB" for column in model.__table__.columns)


def test_outbound_validation_routes_are_registered() -> None:
    paths = {route.path for route in AiCallRouter.routes}
    assert {
        "/ai-call/outbound-validations/batch",
        "/ai-call/outbound-validations/{validation_id}",
        "/ai-call/outbound-validations/{validation_id}/issues",
        "/ai-call/outbound-validations/{validation_id}/issues/export",
        "/ai-call/outbound-validations/{validation_id}/retry",
        "/ai-call/outbound-targets/import-template",
    } <= paths


@pytest.mark.anyio
async def test_valid_xlsx_is_streamed_to_rows_and_finishes_passed(database) -> None:
    service = OutboundValidationService(database, parse_batch_size=2)
    validation_id, temp_path = await _accept(
        service,
        database,
        _xlsx([
            ["手机号", "客户名称"],
            ["13800138000", "张先生"],
            ["13900139000", "李女士"],
            ["13700137000", None],
        ]),
    )

    await service.process_validation("tenant-a", validation_id)

    assert not Path(temp_path).exists()
    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        rows = (
            await session.scalars(
                select(AiCallOutboundValidationRowModel)
                .where(
                    AiCallOutboundValidationRowModel.tenant_id == "tenant-a",
                    AiCallOutboundValidationRowModel.validation_id == validation_id,
                )
                .order_by(AiCallOutboundValidationRowModel.row_number)
            )
        ).all()
    assert validation.status == "PASSED"
    assert validation.temp_file_path is None
    assert validation.valid_target_count == 3
    assert validation.issue_count == 0
    assert [row.row_number for row in rows] == [2, 3, 4]


@pytest.mark.anyio
async def test_business_issues_share_row_table_and_finish_failed(database) -> None:
    service = OutboundValidationService(database, parse_batch_size=2)
    validation_id, _ = await _accept(
        service,
        database,
        _xlsx([
            ["手机号", "客户名称"],
            ["123", "格式错误"],
            ["13800138000", "重复一"],
            ["13800138000", "重复二"],
            [None, None],
        ]),
    )

    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        rows = (
            await session.scalars(
                select(AiCallOutboundValidationRowModel)
                .where(
                    AiCallOutboundValidationRowModel.tenant_id == "tenant-a",
                    AiCallOutboundValidationRowModel.validation_id == validation_id,
                    AiCallOutboundValidationRowModel.is_valid.is_(False),
                )
                .order_by(AiCallOutboundValidationRowModel.row_number)
            )
        ).all()
    reasons = {row.row_number: json.loads(row.reasons_json or "[]") for row in rows}
    assert validation.status == "FAILED"
    assert validation.issue_count == 4
    assert "手机号格式错误" in reasons[2]
    assert reasons[3] == ["手机号重复"]
    assert reasons[4] == ["手机号重复"]
    assert "空行" in reasons[5]
    assert rows[1].duplicate_row_number == 4
    assert rows[2].duplicate_row_number == 3


@pytest.mark.anyio
async def test_parse_failure_deletes_temp_file_and_requires_reupload(database) -> None:
    service = OutboundValidationService(database)
    validation_id, temp_path = await _accept(service, database, b"not-an-xlsx")

    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.status == "SYSTEM_ERROR"
        assert validation.processing_stage == "PARSE_FAILED"
        assert validation.retryable is False
        assert "重新上传" in validation.error_message
        with pytest.raises(CustomException, match="重新上传"):
            await service.prepare_retry(session, "tenant-a", validation_id)
    assert not Path(temp_path).exists()


@pytest.mark.anyio
async def test_post_parse_system_error_can_retry_without_temp_file(
    database,
    monkeypatch,
) -> None:
    service = OutboundValidationService(database)
    validation_id, temp_path = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
    )
    original_finalize = service._finalize_validation

    async def fail_once(*_args, **_kwargs):
        raise RuntimeError("simulated final validation failure")

    monkeypatch.setattr(service, "_finalize_validation", fail_once)
    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.status == "SYSTEM_ERROR"
        assert validation.processing_stage == "PARSED"
        assert validation.retryable is True
        assert validation.temp_file_path is None
        await service.prepare_retry(session, "tenant-a", validation_id)
        await session.commit()
    assert not Path(temp_path).exists()

    monkeypatch.setattr(service, "_finalize_validation", original_finalize)
    await service.process_validation("tenant-a", validation_id)
    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.status == "PASSED"
        assert validation.retry_count == 1


@pytest.mark.anyio
async def test_recovery_resumes_existing_temp_and_marks_missing_temp(database, monkeypatch) -> None:
    service = OutboundValidationService(database)
    existing_id, _ = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
    )
    missing_id, missing_path = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["13900139000", "李女士"]]),
    )
    Path(missing_path).unlink()
    scheduled: list[tuple[str, int]] = []
    monkeypatch.setattr(
        service,
        "schedule_validation",
        lambda tenant_id, validation_id: scheduled.append((tenant_id, validation_id)),
    )

    await service.recover_pending()

    assert scheduled == [("tenant-a", existing_id)]
    async with database() as session:
        missing = await session.get(AiCallOutboundValidationModel, missing_id)
        assert missing.status == "SYSTEM_ERROR"
        assert missing.retryable is False
        assert "重新上传" in missing.error_message


@pytest.mark.anyio
async def test_recovery_retries_terminal_temp_file_cleanup(database, monkeypatch) -> None:
    service = OutboundValidationService(database)
    validation_id, temp_path = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
    )
    original_delete = service._delete_temp_file
    delete_attempts = 0

    def fail_once(path):
        nonlocal delete_attempts
        delete_attempts += 1
        if delete_attempts == 1:
            return False
        return original_delete(path)

    monkeypatch.setattr(service, "_delete_temp_file", fail_once)
    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.status == "PASSED"
        assert validation.temp_file_path == temp_path
        assert Path(temp_path).exists()

    await service.recover_pending()

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.temp_file_path is None
    assert not Path(temp_path).exists()


def _auth(db, tenant_id: str) -> AuthSchema:
    user = UserModel(
        user_id=20,
        tenant_id=tenant_id,
        user_name="operator",
        nick_name="运营",
        user_type="sys_user",
    )
    return AuthSchema(db=db, user=user, check_data_scope=False)


def _client(database, service: OutboundValidationService, tenant_id: str) -> TestClient:
    app = FastAPI()
    handle_exception(app)
    app.include_router(AiCallRouter)

    async def auth_override():
        async with database() as session:
            async with session.begin():
                yield _auth(session, tenant_id)

    app.dependency_overrides[get_current_user] = auth_override
    app.dependency_overrides[get_outbound_validation_service] = lambda: service
    return TestClient(app)


@pytest.mark.anyio
async def test_batch_api_requires_multipart_file_and_request_json(
    database,
    monkeypatch,
) -> None:
    service = OutboundValidationService(database)
    scheduled: list[tuple[str, int]] = []
    monkeypatch.setattr(
        service,
        "schedule_validation",
        lambda tenant_id, validation_id: scheduled.append((tenant_id, validation_id)),
    )
    request_json = _request().model_dump_json(by_alias=True)

    with _client(database, service, "tenant-a") as client:
        response = client.post(
            "/ai-call/outbound-validations/batch",
            files={
                "file": (
                    "外呼名单.xlsx",
                    _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
            data={"request": request_json},
        )

    assert response.status_code == 200
    assert response.json()["code"] == 200
    assert response.json()["data"] == {
        "validationId": response.json()["data"]["validationId"],
        "status": "VALIDATING",
        "validTargetCount": 0,
        "issueCount": 0,
        "accepted": True,
        "retryable": False,
        "errorMessage": None,
        "issueStats": {},
    }
    assert isinstance(response.json()["data"]["validationId"], str)
    assert scheduled == [("tenant-a", int(response.json()["data"]["validationId"]))]


@pytest.mark.anyio
async def test_batch_api_rejects_non_xlsx_and_files_over_10_mb(
    database,
    monkeypatch,
) -> None:
    service = OutboundValidationService(database)
    monkeypatch.setattr(
        service,
        "schedule_validation",
        lambda _tenant_id, _validation_id: None,
    )
    request_json = _request().model_dump_json(by_alias=True)

    with _client(database, service, "tenant-a") as client:
        wrong_type = client.post(
            "/ai-call/outbound-validations/batch",
            files={"file": ("名单.csv", b"phone\n13800138000", "text/csv")},
            data={"request": request_json},
        )
        too_large = client.post(
            "/ai-call/outbound-validations/batch",
            files={"file": ("名单.xlsx", b"x" * (10 * 1024 * 1024 + 1))},
            data={"request": request_json},
        )

    assert wrong_type.status_code == 400
    assert ".xlsx" in wrong_type.json()["msg"]
    assert too_large.status_code == 413
    assert "10 MB" in too_large.json()["msg"]


@pytest.mark.anyio
async def test_batch_api_rejects_multiple_files_and_oss_fields(
    database,
    monkeypatch,
) -> None:
    service = OutboundValidationService(database)
    monkeypatch.setattr(
        service,
        "schedule_validation",
        lambda _tenant_id, _validation_id: None,
    )
    content = _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]])
    request_data = _request().model_dump(mode="json", by_alias=True)

    with _client(database, service, "tenant-a") as client:
        multiple = client.post(
            "/ai-call/outbound-validations/batch",
            files=[
                ("file", ("名单一.xlsx", content)),
                ("file", ("名单二.xlsx", content)),
            ],
            data={"request": json.dumps(request_data, ensure_ascii=False)},
        )
        with_oss_id = client.post(
            "/ai-call/outbound-validations/batch",
            files={"file": ("名单.xlsx", content)},
            data={
                "request": json.dumps(
                    {**request_data, "ossId": "forbidden"},
                    ensure_ascii=False,
                )
            },
        )

    assert multiple.status_code == 400
    assert "单个" in multiple.json()["msg"]
    assert with_oss_id.status_code == 400
    assert "任务配置不合法" in with_oss_id.json()["msg"]


@pytest.mark.anyio
async def test_parser_persists_bounded_batches(database, monkeypatch) -> None:
    service = OutboundValidationService(database, parse_batch_size=200)
    validation_id, _ = await _accept(
        service,
        database,
        _xlsx(
            [["手机号", "客户名称"]]
            + [[f"13{index:09d}", f"客户{index}"] for index in range(1, 1_202)]
        ),
    )
    persisted_batch_sizes: list[int] = []
    original_persist = service._persist_batch

    async def record_batch(db, validation, batch):
        persisted_batch_sizes.append(len(batch))
        await original_persist(db, validation, batch)

    monkeypatch.setattr(service, "_persist_batch", record_batch)

    await service.process_validation("tenant-a", validation_id)

    assert len(persisted_batch_sizes) == 7
    assert max(persisted_batch_sizes) == 200
    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        assert validation.status == "PASSED"
        assert validation.valid_target_count == 1_201


@pytest.mark.anyio
async def test_duplicate_detection_remains_correct_across_many_batches(database) -> None:
    service = OutboundValidationService(database, parse_batch_size=100)
    validation_id, _ = await _accept(
        service,
        database,
        _xlsx(
            [["手机号", "客户名称"]] + [["13800138000", f"重复客户{index}"] for index in range(601)]
        ),
    )

    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        validation = await session.get(AiCallOutboundValidationModel, validation_id)
        rows = (
            await session.scalars(
                select(AiCallOutboundValidationRowModel)
                .where(
                    AiCallOutboundValidationRowModel.tenant_id == "tenant-a",
                    AiCallOutboundValidationRowModel.validation_id == validation_id,
                )
                .order_by(AiCallOutboundValidationRowModel.row_number)
            )
        ).all()
    assert validation.status == "FAILED"
    assert validation.valid_target_count == 0
    assert validation.issue_count == 601
    assert rows[0].duplicate_row_number == 3
    assert all(row.is_valid is False for row in rows)
    assert all("手机号重复" in json.loads(row.reasons_json) for row in rows)


@pytest.mark.anyio
async def test_status_and_issue_queries_are_tenant_isolated(database) -> None:
    service = OutboundValidationService(database)
    validation_id, _ = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["123", "错误号码"]]),
        tenant_id="tenant-a",
    )
    await service.process_validation("tenant-a", validation_id)

    with _client(database, service, "tenant-a") as client:
        status_response = client.get(f"/ai-call/outbound-validations/{validation_id}")
        issue_response = client.get(
            f"/ai-call/outbound-validations/{validation_id}/issues",
            params={"pageNum": 1, "pageSize": 20, "phoneNumber": "123"},
        )
        export_response = client.post(
            f"/ai-call/outbound-validations/{validation_id}/issues/export"
        )

    assert status_response.status_code == 200
    assert status_response.json()["data"]["validationId"] == str(validation_id)
    assert issue_response.json()["total"] == 1
    assert issue_response.json()["rows"][0]["issueId"].isdigit()
    assert issue_response.json()["rows"][0]["reasons"] == ["手机号格式错误"]
    assert export_response.status_code == 200
    assert export_response.content.startswith(b"PK")

    with _client(database, service, "tenant-b") as client:
        hidden_status = client.get(f"/ai-call/outbound-validations/{validation_id}")
        hidden_issues = client.get(f"/ai-call/outbound-validations/{validation_id}/issues")
    assert hidden_status.status_code == 404
    assert hidden_issues.status_code == 404


@pytest.mark.anyio
async def test_validation_does_not_create_formal_outbound_targets(database) -> None:
    service = OutboundValidationService(database)
    validation_id, _ = await _accept(
        service,
        database,
        _xlsx([["手机号", "客户名称"], ["13800138000", "张先生"]]),
    )
    await service.process_validation("tenant-a", validation_id)

    async with database() as session:
        table_names = await session.run_sync(
            lambda sync_session: sa_inspect(sync_session.bind).get_table_names()
        )
    assert "ai_call_outbound_validation" in table_names
    assert "ai_call_outbound_validation_row" in table_names
    assert "ai_call_outbound_target" not in table_names
