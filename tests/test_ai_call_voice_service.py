from __future__ import annotations

import io
import wave
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi import UploadFile
from pydantic import ValidationError
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.datastructures import Headers

from app.api.v1.ai_call.model import AiCallVoiceProfileModel
from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceEnrollmentModel,
)
from app.api.v1.ai_call.voice.repository import VoiceRepository
from app.api.v1.ai_call.voice.schema import (
    VoiceEnrollmentAcceptedOut,
    VoiceEnrollmentRequest,
    VoiceProfileOut,
)
from app.api.v1.ai_call.voice.service import VoiceEnrollmentService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException

TARGET_MODEL = "qwen3.5-omni-plus-realtime"
OTHER_MODEL = "qwen-omni-turbo-realtime"
NOW = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)


class FakeVoiceSampleStorage:
    def __init__(
        self,
        *,
        put_error: Exception | None = None,
        delete_error: Exception | None = None,
    ) -> None:
        self.put_error = put_error
        self.delete_error = delete_error
        self.put_calls: list[dict[str, object]] = []
        self.delete_calls: list[str] = []

    async def put(self, *, data: bytes, filename: str, content_type: str) -> str:
        self.put_calls.append({
            "data": data,
            "filename": filename,
            "content_type": content_type,
        })
        if self.put_error is not None:
            raise self.put_error
        return f"private/sample-{len(self.put_calls)}"

    async def get(self, object_key: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        self.delete_calls.append(object_key)
        if self.delete_error is not None:
            raise self.delete_error


class SequenceIds:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


def _valid_wav(*, seconds: int = 3) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(24_000)
        wav_file.writeframes(b"\x00\x00" * 24_000 * seconds)
    return output.getvalue()


def _upload(
    data: bytes | None = None,
    *,
    filename: str = "sample.wav",
    content_type: str = "audio/wav",
) -> UploadFile:
    return UploadFile(
        io.BytesIO(data if data is not None else _valid_wav()),
        filename=filename,
        headers=Headers({"content-type": content_type}),
    )


def _enrollment_request(**overrides: object) -> VoiceEnrollmentRequest:
    values: dict[str, object] = {
        "display_name": " 客服小林 ",
        "gender": "女声",
        "language": "zh",
        "transcript": " 您好 ",
        "consent_confirmed": True,
    }
    values.update(overrides)
    return VoiceEnrollmentRequest(**values)


@pytest.fixture
async def enrollment_database(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'voice-enrollment.db'}",
    )
    tables = [
        AiCallTenantVoiceProfileModel.__table__,
        AiCallVoiceEnrollmentModel.__table__,
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


def _voice_service(
    storage: FakeVoiceSampleStorage,
    *,
    ids: tuple[int, ...] = (101, 102),
) -> VoiceEnrollmentService:
    return VoiceEnrollmentService(
        storage=storage,
        target_model=TARGET_MODEL,
        now=lambda: NOW,
        id_generator=SequenceIds(*ids),
    )


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


def test_enrollment_request_normalizes_fields_and_forbids_server_owned_values() -> None:
    request = _enrollment_request(transcript="   ")

    assert request.display_name == "客服小林"
    assert request.transcript is None
    assert request.model_dump(by_alias=True) == {
        "displayName": "客服小林",
        "gender": "女声",
        "language": "zh",
        "transcript": None,
        "consentConfirmed": True,
    }

    with pytest.raises(ValidationError):
        VoiceEnrollmentRequest.model_validate({
            **request.model_dump(by_alias=True),
            "tenantId": "tenant-from-browser",
        })
    with pytest.raises(ValidationError):
        VoiceEnrollmentRequest.model_validate({
            **request.model_dump(by_alias=True),
            "targetModel": "browser-model",
        })


@pytest.mark.parametrize(
    "payload",
    [
        {"display_name": "  "},
        {"display_name": "a" * 101},
        {"gender": "其他"},
        {"language": "en"},
        {"transcript": "a" * 2001},
        {"consent_confirmed": "true"},
    ],
)
def test_enrollment_request_rejects_invalid_business_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _enrollment_request(**payload)


def test_enrollment_accepted_output_stringifies_ids() -> None:
    output = VoiceEnrollmentAcceptedOut(
        voice_profile_id=9_007_199_254_740_993,
        enrollment_id=9_007_199_254_740_994,
        status="CREATING",
        display_name="客服小林",
    )

    assert output.voice_profile_id == "9007199254740993"
    assert output.enrollment_id == "9007199254740994"
    assert output.model_dump(by_alias=True)["voiceProfileId"] == "9007199254740993"


@pytest.mark.anyio
async def test_create_persists_normalized_profile_and_enrollment(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(123456789012345678, 223456789012345678))
    sample = _upload()
    await sample.read(7)

    async with factory() as database:
        accepted = await service.create(
            database,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key=" key-1 ",
            request=_enrollment_request(),
            sample=sample,
        )

    async with factory() as database:
        profile = await database.scalar(select(AiCallTenantVoiceProfileModel))
        enrollment = await database.scalar(select(AiCallVoiceEnrollmentModel))

    assert accepted.model_dump(by_alias=True) == {
        "voiceProfileId": "123456789012345678",
        "enrollmentId": "223456789012345678",
        "status": "CREATING",
        "displayName": "客服小林",
    }
    assert profile is not None
    assert enrollment is not None
    assert profile.tenant_id == "tenant-a"
    assert profile.display_name == "客服小林"
    assert profile.voice is None
    assert profile.voice_type == "自定义复刻"
    assert profile.provider == "aliyun_qwen"
    assert profile.target_model == TARGET_MODEL
    assert profile.status == "CREATING"
    assert profile.latest_enrollment_id == enrollment.id
    assert profile.created_by == 7
    assert profile.created_at.replace(tzinfo=timezone.utc) == NOW
    assert enrollment.idempotency_key == "key-1"
    assert enrollment.preferred_name == "3456789012345678"
    assert enrollment.status == "PENDING"
    assert enrollment.provider_voice is None
    assert enrollment.attempt_count == 0
    assert enrollment.consent_user_id == 7
    assert enrollment.consent_at.replace(tzinfo=timezone.utc) == NOW
    assert enrollment.created_at.replace(tzinfo=timezone.utc) == NOW
    assert enrollment.request_hash
    assert enrollment.sample_sha256
    assert enrollment.sample_object_key == "private/sample-1"
    assert storage.put_calls[0]["content_type"] == "audio/wav"


@pytest.mark.anyio
async def test_create_reuses_same_tenant_key_and_canonical_hash_without_upload(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(101, 102))

    async with factory() as database:
        first = await service.create(
            database,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key=" key-1 ",
            request=_enrollment_request(),
            sample=_upload(),
        )
        second = await service.create(
            database,
            tenant_id="tenant-a",
            user_id=8,
            idempotency_key="key-1",
            request=_enrollment_request(
                display_name="客服小林",
                transcript="您好",
            ),
            sample=_upload(),
        )

    assert second == first
    assert len(storage.put_calls) == 1


@pytest.mark.anyio
async def test_create_rejects_same_tenant_key_with_different_payload(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(101, 102))

    async with factory() as database:
        await service.create(
            database,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="key-1",
            request=_enrollment_request(),
            sample=_upload(),
        )
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(display_name="另一个音色"),
                sample=_upload(),
            )

    assert caught.value.status_code == 409
    assert len(storage.put_calls) == 1


@pytest.mark.anyio
async def test_create_scopes_same_idempotency_key_by_tenant(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(101, 102, 201, 202))
    max_length_key = "k" * 128

    async with factory() as database:
        first = await service.create(
            database,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key=max_length_key,
            request=_enrollment_request(),
            sample=_upload(),
        )
        second = await service.create(
            database,
            tenant_id="tenant-b",
            user_id=8,
            idempotency_key=max_length_key,
            request=_enrollment_request(),
            sample=_upload(),
        )

    assert second.voice_profile_id != first.voice_profile_id
    assert len(storage.put_calls) == 2


@pytest.mark.anyio
async def test_create_rejects_missing_consent_before_reading_or_uploading(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage)

    class UnreadableSample:
        filename = "secret.wav"
        content_type = "audio/wav"

        async def seek(self, _offset: int) -> None:
            raise AssertionError("consent failure must happen before seek")

        async def read(self, _size: int) -> bytes:
            raise AssertionError("consent failure must happen before read")

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(consent_confirmed=False),
                sample=UnreadableSample(),  # type: ignore[arg-type]
            )

    assert caught.value.status_code == 422
    assert storage.put_calls == []


@pytest.mark.parametrize("key", ["", " ", "x" * 129])
@pytest.mark.anyio
async def test_create_rejects_invalid_idempotency_key(
    enrollment_database,
    key: str,
) -> None:
    _engine, factory = enrollment_database
    service = _voice_service(FakeVoiceSampleStorage())

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key=key,
                request=_enrollment_request(),
                sample=_upload(),
            )

    assert caught.value.status_code == 400


@pytest.mark.anyio
async def test_create_reads_from_start_with_hard_size_limit(enrollment_database) -> None:
    _engine, factory = enrollment_database
    service = _voice_service(FakeVoiceSampleStorage())

    class OversizedSample:
        filename = "sample.wav"
        content_type = "audio/wav"

        def __init__(self) -> None:
            self.seek_calls: list[int] = []
            self.read_sizes: list[int] = []

        async def seek(self, offset: int) -> None:
            self.seek_calls.append(offset)

        async def read(self, size: int) -> bytes:
            self.read_sizes.append(size)
            return b"x" * size

    sample = OversizedSample()
    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(),
                sample=sample,  # type: ignore[arg-type]
            )

    assert caught.value.status_code == 413
    assert sample.seek_calls == [0]
    assert sample.read_sizes == [10 * 1024 * 1024 + 1]


@pytest.mark.anyio
async def test_create_upload_failure_leaves_database_empty(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(put_error=RuntimeError("secret storage credential"))
    service = _voice_service(storage)

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(),
                sample=_upload(),
            )

    async with factory() as database:
        assert await database.scalar(select(AiCallTenantVoiceProfileModel)) is None
        assert await database.scalar(select(AiCallVoiceEnrollmentModel)) is None
    assert caught.value.status_code == 502
    assert "secret" not in str(caught.value).lower()


@pytest.mark.anyio
async def test_create_commit_failure_rolls_back_and_deletes_uploaded_sample(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage)

    async with factory() as database:

        async def fail_commit() -> None:
            raise RuntimeError("database secret")

        monkeypatch.setattr(database, "commit", fail_commit)
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(),
                sample=_upload(),
            )

    async with factory() as database:
        assert await database.scalar(select(AiCallTenantVoiceProfileModel)) is None
        assert await database.scalar(select(AiCallVoiceEnrollmentModel)) is None
    assert caught.value.status_code == 500
    assert storage.delete_calls == ["private/sample-1"]
    assert "secret" not in str(caught.value).lower()


@pytest.mark.anyio
async def test_create_post_upload_setup_failure_deletes_uploaded_sample(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()

    def fail_id_generation() -> int:
        raise RuntimeError("database setup secret")

    service = VoiceEnrollmentService(
        storage=storage,
        target_model=TARGET_MODEL,
        now=lambda: NOW,
        id_generator=fail_id_generation,
    )

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="key-1",
                request=_enrollment_request(),
                sample=_upload(),
            )

    assert caught.value.status_code == 500
    assert storage.delete_calls == ["private/sample-1"]
    assert "secret" not in str(caught.value).lower()


@pytest.mark.anyio
async def test_create_cleanup_failure_is_redacted(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(delete_error=RuntimeError("secret sample and key"))
    service = _voice_service(storage)
    warnings: list[str] = []
    monkeypatch.setattr(
        "app.api.v1.ai_call.voice.service.log.warning",
        lambda message: warnings.append(str(message)),
    )

    async with factory() as database:

        async def fail_commit() -> None:
            raise RuntimeError("database secret")

        monkeypatch.setattr(database, "commit", fail_commit)
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="sensitive-idempotency-key",
                request=_enrollment_request(),
                sample=_upload(filename="sensitive-sample.wav"),
            )

    rendered = " ".join(warnings + [str(caught.value)])
    assert "secret" not in rendered.lower()
    assert "sensitive" not in rendered.lower()
    assert "private/sample-1" not in rendered


@pytest.mark.parametrize("winner_hash_matches", [True, False])
@pytest.mark.anyio
async def test_create_recovers_unique_race_from_persisted_winner(
    enrollment_database,
    monkeypatch,
    winner_hash_matches: bool,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(101, 102))

    async with factory() as database:
        real_flush = database.flush
        raced = False

        async def race_flush(*args, **kwargs) -> None:
            nonlocal raced
            if raced:
                await real_flush(*args, **kwargs)
                return
            raced = True
            pending_profile = next(
                row for row in database.new if isinstance(row, AiCallTenantVoiceProfileModel)
            )
            pending_enrollment = next(
                row for row in database.new if isinstance(row, AiCallVoiceEnrollmentModel)
            )
            winner_profile = _tenant_voice(
                profile_id=901,
                tenant_id=pending_profile.tenant_id,
                voice=None,
                display_name=pending_profile.display_name,
                status="CREATING",
            )
            winner_profile.latest_enrollment_id = 902
            winner_enrollment = AiCallVoiceEnrollmentModel(
                id=902,
                tenant_id=pending_enrollment.tenant_id,
                voice_profile_id=901,
                idempotency_key=pending_enrollment.idempotency_key,
                request_hash=(pending_enrollment.request_hash if winner_hash_matches else "0" * 64),
                preferred_name="vc901",
                language=pending_enrollment.language,
                transcript=pending_enrollment.transcript,
                sample_object_key="winner/sample",
                sample_sha256=pending_enrollment.sample_sha256,
                status="PENDING",
                provider_voice=None,
                provider_request_id=None,
                attempt_count=0,
                next_retry_at=None,
                lease_owner=None,
                lease_expires_at=None,
                error_message=None,
                cleanup_error_message=None,
                consent_user_id=7,
                consent_at=NOW,
                started_at=None,
                finished_at=None,
                created_at=NOW,
                updated_at=NOW,
            )
            await database.rollback()
            async with factory() as winner_db:
                winner_db.add_all([winner_profile, winner_enrollment])
                await winner_db.commit()
            raise IntegrityError("insert", {}, RuntimeError("unique"))

        monkeypatch.setattr(database, "flush", race_flush)
        if winner_hash_matches:
            accepted = await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="race-key",
                request=_enrollment_request(),
                sample=_upload(),
            )
            assert accepted.voice_profile_id == "901"
            assert accepted.enrollment_id == "902"
        else:
            with pytest.raises(CustomException) as caught:
                await service.create(
                    database,
                    tenant_id="tenant-a",
                    user_id=7,
                    idempotency_key="race-key",
                    request=_enrollment_request(),
                    sample=_upload(),
                )
            assert caught.value.status_code == 409

    assert storage.delete_calls == ["private/sample-1"]


@pytest.mark.anyio
async def test_reenroll_requires_current_tenant_failed_profile(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(201, 202))
    async with factory() as database:
        database.add_all([
            _tenant_voice(
                profile_id=101,
                tenant_id="tenant-a",
                voice=None,
                status="ENABLED",
            ),
            _tenant_voice(
                profile_id=102,
                tenant_id="tenant-b",
                voice=None,
                status="CREATE_FAILED",
            ),
        ])
        await database.commit()

        with pytest.raises(CustomException) as wrong_state:
            await service.reenroll(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=101,
                idempotency_key="retry-1",
                request=_enrollment_request(),
                sample=_upload(),
            )
        with pytest.raises(CustomException) as other_tenant:
            await service.reenroll(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=102,
                idempotency_key="retry-2",
                request=_enrollment_request(),
                sample=_upload(),
            )

    assert wrong_state.value.status_code == 409
    assert other_tenant.value.status_code == 404
    assert storage.put_calls == []


@pytest.mark.anyio
async def test_reenroll_updates_failed_profile_and_is_idempotent(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, ids=(202,))
    async with factory() as database:
        profile = _tenant_voice(
            profile_id=101,
            tenant_id="tenant-a",
            voice=None,
            display_name="旧名称",
            status="CREATE_FAILED",
            error_message="旧错误",
        )
        database.add(profile)
        await database.commit()

        first = await service.reenroll(
            database,
            tenant_id="tenant-a",
            user_id=9,
            profile_id=101,
            idempotency_key="retry-key",
            request=_enrollment_request(display_name=" 新名称 ", gender="男声"),
            sample=_upload(),
        )
        second = await service.reenroll(
            database,
            tenant_id="tenant-a",
            user_id=9,
            profile_id=101,
            idempotency_key="retry-key",
            request=_enrollment_request(display_name="新名称", gender="男声"),
            sample=_upload(),
        )

    async with factory() as database:
        saved_profile = await database.get(AiCallTenantVoiceProfileModel, 101)
        enrollments = (await database.execute(select(AiCallVoiceEnrollmentModel))).scalars().all()

    assert first == second
    assert first.voice_profile_id == "101"
    assert first.enrollment_id == "202"
    assert first.display_name == "新名称"
    assert saved_profile is not None
    assert saved_profile.status == "CREATING"
    assert saved_profile.latest_enrollment_id == 202
    assert saved_profile.error_message is None
    assert saved_profile.display_name == "新名称"
    assert saved_profile.gender == "男声"
    assert len(enrollments) == 1
    assert len(storage.put_calls) == 1
