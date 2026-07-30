from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import AiCallVoiceProfileModel
from app.api.v1.ai_call.voice.model import AiCallTenantVoiceProfileModel
from app.api.v1.ai_call.voice.repository import VoiceRepository
from app.api.v1.ai_call.voice.schema import VoiceProfileOut
from app.core.base_model import MappedBase

TARGET_MODEL = "qwen3.5-omni-plus-realtime"
OTHER_MODEL = "qwen-omni-turbo-realtime"
NOW = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
async def voice_database():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    tables = [
        AiCallVoiceProfileModel.__table__,
        AiCallTenantVoiceProfileModel.__table__,
    ]
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: MappedBase.metadata.create_all(
                sync_connection,
                tables=tables,
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield engine, factory
    finally:
        await engine.dispose()


def _global_voice(
    *,
    profile_id: int,
    voice: str,
    display_name: str | None = None,
    voice_type: str = "内置",
    gender: str = "女声",
    target_model: str = TARGET_MODEL,
    created_at: datetime = NOW,
    updated_at: datetime = NOW,
) -> AiCallVoiceProfileModel:
    return AiCallVoiceProfileModel(
        id=profile_id,
        voice=voice,
        display_name=display_name or voice,
        voice_type=voice_type,
        gender=gender,
        target_model=target_model,
        description=None,
        sort_order=0,
        remark="",
        created_at=created_at,
        updated_at=updated_at,
    )


def _tenant_voice(
    *,
    profile_id: int,
    tenant_id: str = "tenant-a",
    voice: str | None,
    display_name: str | None = None,
    voice_type: str = "自定义复刻",
    gender: str = "女声",
    language: str = "zh",
    target_model: str = TARGET_MODEL,
    status: str = "ENABLED",
    error_message: str | None = None,
    created_at: datetime = NOW,
    updated_at: datetime = NOW,
) -> AiCallTenantVoiceProfileModel:
    return AiCallTenantVoiceProfileModel(
        id=profile_id,
        tenant_id=tenant_id,
        display_name=display_name or f"租户音色-{profile_id}",
        voice=voice,
        voice_type=voice_type,
        gender=gender,
        language=language,
        target_model=target_model,
        provider="QWEN",
        status=status,
        latest_enrollment_id=None,
        provider_created_at=None,
        error_message=error_message,
        created_by=10,
        deleted_by=None,
        deleted_at=NOW if status == "DELETED" else None,
        created_at=created_at,
        updated_at=updated_at,
    )


async def _seed(factory: async_sessionmaker, *profiles: Any) -> None:
    async with factory() as database:
        database.add_all(profiles)
        await database.commit()


@pytest.mark.anyio
async def test_list_merges_global_and_current_tenant_with_two_scoped_queries(
    voice_database,
) -> None:
    engine, factory = voice_database
    await _seed(
        factory,
        _global_voice(profile_id=1, voice="Tina"),
        _tenant_voice(profile_id=2, tenant_id="tenant-a", voice="vc-a"),
        _tenant_voice(profile_id=3, tenant_id="tenant-b", voice="vc-b"),
    )
    statements: list[tuple[str, object]] = []

    def capture_sql(
        _connection,
        _cursor,
        statement: str,
        parameters: object,
        _context,
        _executemany,
    ) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
    try:
        async with factory() as database:
            rows, total = await VoiceRepository(database).list_profiles(
                tenant_id="tenant-a",
                target_model=TARGET_MODEL,
                available_only=False,
                include_deleted=False,
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_sql)

    assert [row.voice for row in rows] == ["vc-a", "Tina"]
    assert total == 2
    global_queries = [
        (statement, parameters)
        for statement, parameters in statements
        if "ai_call_voice_profile" in statement and "ai_call_tenant_voice_profile" not in statement
    ]
    tenant_queries = [
        (statement, parameters)
        for statement, parameters in statements
        if "ai_call_tenant_voice_profile" in statement
    ]
    assert global_queries
    assert tenant_queries
    assert all("tenant_id" in statement for statement, _parameters in tenant_queries)
    assert all("tenant-a" in parameters for _statement, parameters in tenant_queries)
    assert all("tenant-b" not in parameters for _statement, parameters in tenant_queries)


@pytest.mark.anyio
async def test_list_applies_model_availability_type_gender_and_status_filters(
    voice_database,
) -> None:
    _engine, factory = voice_database
    await _seed(
        factory,
        _global_voice(profile_id=10, voice="Tina", gender="女声"),
        _global_voice(
            profile_id=12,
            voice="legacy-global-custom",
            voice_type="自定义复刻",
        ),
        _global_voice(profile_id=13, voice="", display_name="空音色"),
        _global_voice(
            profile_id=11,
            voice="Ryan",
            gender="男声",
            target_model=OTHER_MODEL,
        ),
        _tenant_voice(profile_id=20, voice="vc-enabled", status="ENABLED"),
        _tenant_voice(
            profile_id=21,
            voice=None,
            status="ENABLED",
            gender="男声",
        ),
        _tenant_voice(
            profile_id=22,
            voice=None,
            status="CREATING",
            gender="男声",
        ),
        _tenant_voice(
            profile_id=23,
            voice=None,
            status="CREATE_FAILED",
            gender="男声",
            error_message="样本不合格",
        ),
        _tenant_voice(profile_id=24, voice="vc-deleting", status="DELETING"),
        _tenant_voice(
            profile_id=25,
            voice="vc-delete-failed",
            status="DELETE_FAILED",
        ),
        _tenant_voice(profile_id=26, voice=None, status="DELETED"),
    )

    async with factory() as database:
        repository = VoiceRepository(database)
        available_rows, available_total = await repository.list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=True,
            include_deleted=True,
            page_num=1,
            page_size=100,
        )
        failed_rows, failed_total = await repository.list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            voice_type="自定义复刻",
            gender="男声",
            status="CREATE_FAILED",
            available_only=False,
            include_deleted=False,
            page_num=1,
            page_size=100,
        )
        enabled_rows, enabled_total = await repository.list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            status="ENABLED",
            available_only=False,
            include_deleted=False,
            page_num=1,
            page_size=100,
        )
        hidden_deleted, hidden_total = await repository.list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            status="DELETED",
            available_only=False,
            include_deleted=False,
            page_num=1,
            page_size=100,
        )
        deleted_rows, deleted_total = await repository.list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            status="DELETED",
            available_only=False,
            include_deleted=True,
            page_num=1,
            page_size=100,
        )

    assert {row.voice for row in available_rows} == {"Tina", "vc-enabled"}
    assert available_total == 2
    assert [row.status for row in failed_rows] == ["CREATE_FAILED"]
    assert failed_rows[0].error_message == "样本不合格"
    assert failed_total == 1
    assert {row.voice for row in enabled_rows} == {
        "Tina",
        "legacy-global-custom",
        "",
        "vc-enabled",
        None,
    }
    assert enabled_total == 5
    assert hidden_deleted == []
    assert hidden_total == 0
    assert [row.status for row in deleted_rows] == ["DELETED"]
    assert deleted_total == 1


@pytest.mark.anyio
async def test_list_sorts_tenant_first_and_paginates_after_merging(
    voice_database,
) -> None:
    _engine, factory = voice_database
    await _seed(
        factory,
        _global_voice(
            profile_id=101,
            voice="global-old",
            updated_at=NOW - timedelta(days=2),
        ),
        _global_voice(
            profile_id=102,
            voice="global-new",
            updated_at=NOW - timedelta(days=1),
        ),
        _tenant_voice(
            profile_id=201,
            voice="tenant-old",
            updated_at=NOW - timedelta(hours=2),
        ),
        _tenant_voice(
            profile_id=202,
            voice="tenant-new-low-id",
            updated_at=NOW - timedelta(hours=1),
        ),
        _tenant_voice(
            profile_id=203,
            voice="tenant-new-high-id",
            updated_at=NOW - timedelta(hours=1),
        ),
    )

    async with factory() as database:
        first_page, first_total = await VoiceRepository(database).list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=False,
            include_deleted=False,
            page_num=1,
            page_size=2,
        )
        second_page, second_total = await VoiceRepository(database).list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=False,
            include_deleted=False,
            page_num=2,
            page_size=2,
        )
        last_page, last_total = await VoiceRepository(database).list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=False,
            include_deleted=False,
            page_num=3,
            page_size=2,
        )

    assert [row.voice for row in first_page] == [
        "tenant-new-high-id",
        "tenant-new-low-id",
    ]
    assert [row.voice for row in second_page] == ["tenant-old", "global-new"]
    assert [row.voice for row in last_page] == ["global-old"]
    assert first_total == second_total == last_total == 5
    assert all(isinstance(row.id, str) for row in first_page + second_page + last_page)


@pytest.mark.anyio
async def test_later_page_reads_only_bounded_rows_across_source_boundary(
    voice_database,
) -> None:
    engine, factory = voice_database
    await _seed(
        factory,
        *[
            _tenant_voice(profile_id=2_000 + index, voice=f"tenant-{index}")
            for index in range(1, 38)
        ],
        *[
            _global_voice(profile_id=1_000 + index, voice=f"global-{index}")
            for index in range(1, 38)
        ],
    )
    statements: list[tuple[str, object]] = []

    def capture_sql(
        _connection,
        _cursor,
        statement: str,
        parameters: object,
        _context,
        _executemany,
    ) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
    try:
        async with factory() as database:
            rows, total = await VoiceRepository(database).list_profiles(
                tenant_id="tenant-a",
                target_model=TARGET_MODEL,
                available_only=False,
                include_deleted=False,
                page_num=4,
                page_size=10,
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_sql)

    assert [row.voice for row in rows] == [
        "tenant-7",
        "tenant-6",
        "tenant-5",
        "tenant-4",
        "tenant-3",
        "tenant-2",
        "tenant-1",
        "global-37",
        "global-36",
        "global-35",
    ]
    assert total == 74
    tenant_queries = [
        (statement, parameters)
        for statement, parameters in statements
        if "ai_call_tenant_voice_profile" in statement
    ]
    global_queries = [
        (statement, parameters)
        for statement, parameters in statements
        if "ai_call_voice_profile" in statement and "ai_call_tenant_voice_profile" not in statement
    ]
    assert tenant_queries
    assert global_queries
    assert all("tenant_id" in statement for statement, _parameters in tenant_queries)
    assert all("tenant-a" in parameters for _statement, parameters in tenant_queries)
    row_queries = [
        (statement, parameters)
        for statement, parameters in statements
        if "count(" not in statement.lower()
    ]
    assert row_queries
    assert all("limit" in statement.lower() for statement, _parameters in row_queries)
    tenant_row_parameters = next(
        parameters
        for statement, parameters in row_queries
        if "ai_call_tenant_voice_profile" in statement
    )
    global_row_parameters = next(
        parameters
        for statement, parameters in row_queries
        if "ai_call_voice_profile" in statement and "ai_call_tenant_voice_profile" not in statement
    )
    assert tuple(tenant_row_parameters)[-2:] == (7, 30)
    assert tuple(global_row_parameters)[-2:] == (3, 0)


def _profile_payload(profile_id: object) -> dict[str, object]:
    return {
        "id": profile_id,
        "scope": "GLOBAL",
        "voice": "Tina",
        "display_name": "甜甜 Tina",
        "voice_type": "内置",
        "gender": "女声",
        "language": None,
        "target_model": TARGET_MODEL,
        "status": "ENABLED",
        "error_message": None,
        "can_preview": True,
        "can_delete": False,
        "created_at": NOW,
        "updated_at": NOW,
    }


@pytest.mark.parametrize(
    ("profile_id", "expected"),
    [
        (1, "1"),
        ("1", "1"),
        ("0001", "1"),
        (2**63 - 1, str(2**63 - 1)),
        (str(2**63 - 1), str(2**63 - 1)),
    ],
)
def test_voice_profile_id_accepts_positive_signed_bigint_and_canonicalizes(
    profile_id: object,
    expected: str,
) -> None:
    assert VoiceProfileOut.model_validate(_profile_payload(profile_id)).id == expected


@pytest.mark.parametrize(
    "profile_id",
    [
        True,
        False,
        None,
        1.0,
        {},
        "",
        " ",
        "abc",
        "+1",
        "-1",
        0,
        "0",
        -1,
        2**63,
        str(2**63),
    ],
)
def test_voice_profile_id_rejects_values_outside_positive_signed_bigint(
    profile_id: object,
) -> None:
    with pytest.raises(ValidationError):
        VoiceProfileOut.model_validate(_profile_payload(profile_id))


@pytest.mark.anyio
async def test_list_derives_actions_and_normalizes_invalid_page_values(
    voice_database,
) -> None:
    _engine, factory = voice_database
    await _seed(
        factory,
        _global_voice(profile_id=301, voice="Tina"),
        _global_voice(profile_id=308, voice="", display_name="空音色"),
        _tenant_voice(profile_id=302, voice="ready", status="ENABLED"),
        _tenant_voice(profile_id=303, voice=None, status="ENABLED"),
        _tenant_voice(profile_id=304, voice=None, status="CREATING"),
        _tenant_voice(profile_id=305, voice=None, status="CREATE_FAILED"),
        _tenant_voice(profile_id=306, voice="deleting", status="DELETING"),
        _tenant_voice(
            profile_id=307,
            voice="delete-failed",
            status="DELETE_FAILED",
        ),
    )

    async with factory() as database:
        rows, total = await VoiceRepository(database).list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=False,
            include_deleted=False,
            page_num=0,
            page_size=0,
        )

    assert total == 8
    assert len(rows) == 1
    all_profiles = await _all_profiles(factory)
    by_id = {row.id: row for row in all_profiles}
    by_status = {row.status: row for row in all_profiles if row.scope == "TENANT"}
    assert by_id["303"].status == "ENABLED"
    assert by_id["303"].can_preview is False
    assert by_id["303"].can_delete is True
    assert by_status["ENABLED"].can_preview is True
    assert by_status["ENABLED"].can_delete is True
    assert by_status["CREATING"].can_preview is False
    assert by_status["CREATING"].can_delete is False
    assert by_status["CREATE_FAILED"].can_preview is False
    assert by_status["CREATE_FAILED"].can_delete is False
    assert by_status["DELETING"].can_preview is False
    assert by_status["DELETING"].can_delete is False
    assert by_status["DELETE_FAILED"].can_preview is False
    assert by_status["DELETE_FAILED"].can_delete is True

    global_row = next(row for row in all_profiles if row.voice == "Tina")
    assert global_row.status == "ENABLED"
    assert global_row.language is None
    assert global_row.can_preview is True
    assert global_row.can_delete is False
    assert by_id["308"].scope == "GLOBAL"
    assert by_id["308"].can_preview is False
    assert by_id["308"].can_delete is False
    assert global_row.model_dump(by_alias=True)["displayName"] == "Tina"
    assert (
        VoiceProfileOut.model_validate({
            **global_row.model_dump(),
            "id": 9_007_199_254_740_993,
        }).id
        == "9007199254740993"
    )


async def _all_profiles(factory: async_sessionmaker) -> list[VoiceProfileOut]:
    async with factory() as database:
        rows, _total = await VoiceRepository(database).list_profiles(
            tenant_id="tenant-a",
            target_model=TARGET_MODEL,
            available_only=False,
            include_deleted=True,
            page_num=1,
            page_size=100,
        )
        return rows
