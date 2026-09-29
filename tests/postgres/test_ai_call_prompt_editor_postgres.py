import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_ai_call_runtime_control_postgres import _async_dsn, _psycopg_dsn

from app.api.v1.ai_call.crud import AiCallRecordRepository
from app.api.v1.ai_call.service import AiCallService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from tools.migrate_prompt_editor import check_rollback, migrate


def test_migration_preserves_content_and_reconciles_only_matching_versions():
    schema = f"prompt_migration_{uuid4().hex}"
    with psycopg.connect(_psycopg_dsn(), autocommit=True, row_factory=dict_row) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            db.execute("""CREATE TABLE ai_call_prompt_profile (
                id bigint primary key, tenant_id varchar(20), name varchar(100), scene_code varchar(64),
                provider_key varchar(64), prompt_text text, opening_message text,
                opening_barge_in_enabled boolean, product_info text, variables_json text,
                current_version_id bigint)""")
            db.execute("""CREATE TABLE ai_call_prompt_profile_version (
                id bigint primary key, tenant_id varchar(20), profile_id bigint, version_no integer,
                snapshot_json text, deleted_at timestamptz)""")
            db.execute(
                "INSERT INTO ai_call_prompt_profile VALUES (1, 'tenant', '场景', 'scene', 'static_profile', '业务内容', '您好', true, '', '[]', 11)"
            )
            content = {
                "sceneCode": "scene",
                "name": "场景",
                "providerKey": "static_profile",
                "promptText": "业务内容",
                "openingMessage": "您好",
                "openingBargeInEnabled": True,
                "productInfo": "",
                "variables": [],
            }
            db.execute(
                "INSERT INTO ai_call_prompt_profile_version VALUES (10, 'tenant', 1, 1, %s, NULL), (11, 'tenant', 1, 2, %s, NULL)",
                (json.dumps(content), json.dumps({**content, "openingMessage": "其他内容"})),
            )
            with db.transaction():
                report = migrate(db)
            assert report["repairedPointers"] == ["1"]
            row = db.execute("SELECT * FROM ai_call_prompt_profile").fetchone()
            assert row["current_version_id"] == 10 and row["opening_message"] == "您好"
            assert check_rollback(db)["allowed"]
            with db.transaction():
                assert migrate(db)["repairedPointers"] == []
            assert (
                db.execute("SELECT count(*) AS n FROM ai_call_prompt_profile_version").fetchone()[
                    "n"
                ]
                == 2
            )
            db.execute("UPDATE ai_call_prompt_profile SET opening_message = '没有历史匹配'")
            with db.transaction():
                assert migrate(db)["unmatchedPointers"] == ["1"]
            assert (
                db.execute("SELECT current_version_id FROM ai_call_prompt_profile").fetchone()[
                    "current_version_id"
                ]
                is None
            )
            db.execute("UPDATE ai_call_prompt_profile SET lifecycle_status = 'DRAFT'")
            assert not check_rollback(db)["allowed"]
            db.execute("INSERT INTO ai_call_prompt_profile (id, tenant_id, name, scene_code) VALUES (2, 'tenant', %s, 'other')", ("\t场景\n",))
            with pytest.raises(ValueError, match="1、2"):
                with db.transaction():
                    migrate(db)
            assert db.execute("SELECT name FROM ai_call_prompt_profile WHERE id = 2").fetchone()["name"] == "\t场景\n"
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
async def editor_postgres():
    schema = f"prompt_editor_{uuid4().hex}"
    engine = create_async_engine(
        _async_dsn(), connect_args={"server_settings": {"search_path": schema}}
    )
    async with engine.begin() as db:
        await db.execute(text(f'CREATE SCHEMA "{schema}"'))
        await db.run_sync(MappedBase.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as db:
            await db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.anyio
async def test_concurrent_module_save_has_one_winner(editor_postgres):
    async with editor_postgres() as db:
        service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
        saved = await service.create_prompt_profile(
            tenant_id="tenant",
            values={
                "name": "并发场景",
                "opening_message": "A",
                "prompt_text": "产品介绍",
                "creation_key": "create-1",
            },
        )
        await db.commit()

    async def save(opening):
        async with editor_postgres() as db:
            service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
            try:
                result = await service.save_prompt_profile_module(
                    tenant_id="tenant",
                    profile_id=int(saved["id"]),
                    values={
                        "module": "opening",
                        "opening_message": opening,
                        "expected_revision": saved["editRevision"],
                    },
                )
                await db.commit()
                return result
            except CustomException as exc:
                await db.rollback()
                return exc

    results = await asyncio.gather(save("B"), save("C"))
    assert sum(isinstance(result, CustomException) for result in results) == 1
    winner = next(result for result in results if isinstance(result, dict))
    assert winner["versionNo"] == 1 and winner["editRevision"] == saved["editRevision"] + 1


@pytest.mark.anyio
async def test_concurrent_names_have_one_winner(editor_postgres):
    async def create(key):
        async with editor_postgres() as db:
            service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
            try:
                result = await service.create_prompt_profile(
                    tenant_id="tenant", values={"name": "同名", "creation_key": key}
                )
                await db.commit()
                return result
            except CustomException as exc:
                await db.rollback()
                return exc

    results = await asyncio.gather(create("one"), create("two"))
    assert sum(isinstance(result, CustomException) for result in results) == 1
    assert "场景名称" in next(
        result.msg for result in results if isinstance(result, CustomException)
    )


@pytest.mark.anyio
async def test_concurrent_creation_key_returns_same_scene(editor_postgres):
    async def create():
        async with editor_postgres() as db:
            service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
            saved = await service.create_prompt_profile(
                tenant_id="tenant",
                values={
                    "name": "同一创建请求",
                    "creation_key": "same-key",
                },
            )
            await db.commit()
            return saved

    first, second = await asyncio.gather(create(), create())
    assert first["id"] == second["id"]


@pytest.mark.anyio
async def test_delete_waits_for_call_admission_then_reports_blocker(editor_postgres):
    from app.services.ai_call.runtime_control.command_repository import (
        RuntimeCommandRepository,
        StartCallIntent,
    )

    async with editor_postgres() as db:
        service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
        saved = await service.create_prompt_profile(
            tenant_id="tenant",
            values={
                "name": "并发删除",
                "opening_message": "您好",
                "prompt_text": "介绍产品",
            },
        )
        await db.commit()
    admitted = asyncio.Event()
    delete_started = asyncio.Event()

    async def admit():
        async with editor_postgres() as db:
            result = await RuntimeCommandRepository(db).create_start_call(
                StartCallIntent(
                    tenant_id="tenant",
                    entry_type="web",
                    idempotency_key="admit",
                    payload={},
                    scene_code=saved["sceneCode"],
                )
            )
            admitted.set()
            await delete_started.wait()
            await db.commit()
            return result

    async def delete():
        await admitted.wait()
        async with editor_postgres() as db:
            service = AiCallService(SimpleNamespace(), prompt_repository=AiCallRecordRepository(db))
            delete_started.set()
            with pytest.raises(CustomException) as error:
                await service.delete_prompt_profile(
                    tenant_id="tenant",
                    profile_id=int(saved["id"]),
                    expected_revision=saved["editRevision"],
                )
            assert error.value.data["errorCode"] == "PROMPT_DELETE_BLOCKED"

    await asyncio.wait_for(asyncio.gather(admit(), delete()), timeout=10)
