from __future__ import annotations

import asyncio
import hashlib
import io
import wave
from contextlib import asynccontextmanager
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
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundTaskModel
from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceDeletionModel,
    AiCallVoiceEnrollmentModel,
    AiCallVoiceSampleCleanupModel,
)
from app.api.v1.ai_call.voice.repository import VoiceRepository
from app.api.v1.ai_call.voice.schema import (
    VoiceEnrollmentAcceptedOut,
    VoiceEnrollmentRequest,
    VoiceProfileOut,
)
from app.api.v1.ai_call.voice.service import VoiceDeletionService, VoiceEnrollmentService
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException

TARGET_MODEL = "qwen3.5-omni-plus-realtime"
OTHER_MODEL = "qwen-omni-turbo-realtime"
NOW = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)
TEST_SAMPLE_NONCE = "a" * 32


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

    async def put(self, *, object_key: str, data: bytes, content_type: str) -> None:
        self.put_calls.append({
            "object_key": object_key,
            "data": data,
            "content_type": content_type,
        })
        if self.put_error is not None:
            raise self.put_error

    async def get(self, object_key: str) -> bytes:
        raise NotImplementedError

    async def delete(self, object_key: str) -> None:
        self.delete_calls.append(object_key)
        if self.delete_error is not None:
            raise self.delete_error


class BlockingVoiceSampleStorage(FakeVoiceSampleStorage):
    def __init__(self) -> None:
        super().__init__()
        self.first_put_started = asyncio.Event()
        self.release_put = asyncio.Event()

    async def put(self, *, object_key: str, data: bytes, content_type: str) -> None:
        self.put_calls.append({
            "object_key": object_key,
            "data": data,
            "content_type": content_type,
        })
        self.first_put_started.set()
        await self.release_put.wait()


class StatefulVoiceSampleStorage(FakeVoiceSampleStorage):
    def __init__(self, objects: dict[str, bytes]) -> None:
        super().__init__()
        self.objects = dict(objects)

    async def put(self, *, object_key: str, data: bytes, content_type: str) -> None:
        await super().put(
            object_key=object_key,
            data=data,
            content_type=content_type,
        )
        self.objects[object_key] = data

    async def get(self, object_key: str) -> bytes:
        return self.objects[object_key]

    async def delete(self, object_key: str) -> None:
        await super().delete(object_key)
        self.objects.pop(object_key, None)


class SequenceIds:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


class SequenceNonces:
    def __init__(self, *values: str) -> None:
        self._values = iter(values)

    def __call__(self) -> bytes:
        return bytes.fromhex(next(self._values))


def _exception_chain(root: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    pending = [root]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        chain.append(current)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return chain


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
        AiCallVoiceSampleCleanupModel.__table__,
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
    session_factory: async_sessionmaker,
    *,
    ids: tuple[int, ...] = (101, 102),
    cleanup_ids: tuple[int, ...] = (901, 902),
    nonces: tuple[str, ...] = (TEST_SAMPLE_NONCE,) * 4,
) -> VoiceEnrollmentService:
    return VoiceEnrollmentService(
        storage=storage,
        cleanup_session_factory=session_factory,
        target_model=TARGET_MODEL,
        now=lambda: NOW,
        id_generator=SequenceIds(*ids),
        cleanup_id_generator=SequenceIds(*cleanup_ids),
        sample_nonce_generator=SequenceNonces(*nonces),
    )


def test_voice_service_uses_configured_private_sample_prefix() -> None:
    service = VoiceEnrollmentService(
        storage=FakeVoiceSampleStorage(),
        cleanup_session_factory=lambda: None,
        target_model=TARGET_MODEL,
        sample_object_prefix="private/tenant-voice-samples/",
    )

    object_key = service._sample_object_key(
        tenant_id="tenant-a",
        enrollment_id=123,
        sample_nonce=TEST_SAMPLE_NONCE,
        content_type="audio/wav",
    )

    assert object_key.startswith("private/tenant-voice-samples/")
    assert "//" not in object_key


def _sample_key(
    tenant_id: str,
    enrollment_id: int,
    extension: str = ".wav",
    *,
    nonce: str | None = TEST_SAMPLE_NONCE,
) -> str:
    tenant_digest = hashlib.sha256(tenant_id.encode()).hexdigest()[:12]
    object_name = f"{enrollment_id}-{nonce}" if nonce is not None else str(enrollment_id)
    return f"ai-call/voice-samples/{tenant_digest}/{object_name}{extension}"


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


@pytest.fixture
async def deletion_database(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'voice-deletion.db'}",
    )
    tables = [
        AiCallTenantVoiceProfileModel.__table__,
        AiCallVoiceDeletionModel.__table__,
        AiCallOutboundTaskModel.__table__,
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


def _outbound_task(
    *,
    task_id: int,
    tenant_id: str = "tenant-a",
    voice: str = "vc-enabled",
    status: str = "SCHEDULED",
) -> AiCallOutboundTaskModel:
    return AiCallOutboundTaskModel(
        id=task_id,
        tenant_id=tenant_id,
        validation_id=10_000 + task_id,
        idempotency_key=f"task-key-{task_id}",
        request_fingerprint=f"{task_id:064d}"[-64:],
        task_name=f"任务-{task_id}",
        task_mode="batch",
        status=status,
        total_targets=1,
        completed_targets=0,
        connected_targets=0,
        failed_targets=0,
        execution_mode="immediate",
        scheduled_at=None,
        next_dispatch_at=None,
        last_dispatched_at=None,
        started_at=None,
        ended_at=None,
        prompt_profile_id="prompt-1",
        prompt_name="默认提示词",
        scene_code="default",
        voice=voice,
        voice_name="客服音色",
        rule_id=20_000 + task_id,
        rule_name="默认规则",
        rule_summary="默认规则摘要",
        line_id=None,
        line_name=None,
        config_snapshot_json="{}",
        error_message=None,
        created_by=7,
        created_by_name="测试用户",
        created_at=NOW,
        updated_at=NOW,
    )


def _deletion_service(
    factory: async_sessionmaker,
    *,
    ids: tuple[int, ...] = (801, 802, 803),
) -> VoiceDeletionService:
    return VoiceDeletionService(
        session_factory=factory,
        now=lambda: NOW,
        id_generator=SequenceIds(*ids),
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
        _tenant_voice(profile_id=27, voice="vc-disabled", status="DISABLED"),
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


@pytest.mark.anyio
async def test_deletion_check_classifies_task_states_and_isolates_tenant(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    blocking_statuses = ["SCHEDULED", "RUNNING", "PAUSING", "PAUSED", "STOPPING"]
    historical_statuses = ["STOPPED", "COMPLETED", "FAILED", "CANCELLED"]
    await _seed(
        factory,
        _tenant_voice(profile_id=701, voice="vc-enabled"),
        *[
            _outbound_task(task_id=index, status=task_status)
            for index, task_status in enumerate(blocking_statuses, start=1)
        ],
        *[
            _outbound_task(task_id=index, status=task_status)
            for index, task_status in enumerate(historical_statuses, start=101)
        ],
        _outbound_task(task_id=201, tenant_id="tenant-b", status="RUNNING"),
        _outbound_task(task_id=202, voice="other-voice", status="RUNNING"),
    )
    service = _deletion_service(factory)

    async with factory() as database:
        result = await service.deletion_check(
            database,
            tenant_id="tenant-a",
            profile_id=701,
        )

    assert result == {
        "voiceProfileId": "701",
        "deletable": False,
        "blockingTaskCount": 5,
        "historicalTaskCount": 4,
        "blockingTaskIds": ["1", "2", "3", "4", "5"],
    }


@pytest.mark.anyio
async def test_deletion_check_bounds_blocking_task_id_page(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(
        factory,
        _tenant_voice(profile_id=702, voice="vc-enabled"),
        *[_outbound_task(task_id=1_000 + index, status="SCHEDULED") for index in range(105)],
    )

    async with factory() as database:
        result = await _deletion_service(factory).deletion_check(
            database,
            tenant_id="tenant-a",
            profile_id=702,
        )

    assert result["blockingTaskCount"] == 105
    assert len(result["blockingTaskIds"]) == 100
    assert result["blockingTaskIds"][0] == "1000"
    assert result["blockingTaskIds"][-1] == "1099"


@pytest.mark.anyio
async def test_tenant_voice_availability_can_be_disabled_and_reenabled(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(factory, _tenant_voice(profile_id=7021, voice="vc-enabled"))
    service = _deletion_service(factory)

    async with factory() as database:
        disabled = await service.set_availability(
            database,
            tenant_id="tenant-a",
            profile_id=7021,
            availability_status="DISABLED",
        )
    async with factory() as database:
        enabled = await service.set_availability(
            database,
            tenant_id="tenant-a",
            profile_id=7021,
            availability_status="ENABLED",
        )

    assert disabled.status == "DISABLED"
    assert disabled.can_preview is False
    assert disabled.can_delete is True
    assert enabled.status == "ENABLED"
    assert enabled.can_preview is True


@pytest.mark.anyio
async def test_tenant_voice_availability_rejects_transient_status(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(
        factory,
        _tenant_voice(profile_id=7023, voice="vc-creating", status="CREATING"),
    )
    service = _deletion_service(factory)

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.set_availability(
                database,
                tenant_id="tenant-a",
                profile_id=7023,
                availability_status="DISABLED",
            )

    assert caught.value.status_code == 409


@pytest.mark.anyio
async def test_disabled_tenant_voice_can_be_deleted(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(
        factory,
        _tenant_voice(
            profile_id=7022,
            voice="vc-disabled",
            status="DISABLED",
        ),
    )
    service = _deletion_service(factory, ids=(822,))

    async with factory() as database:
        preflight = await service.deletion_check(
            database,
            tenant_id="tenant-a",
            profile_id=7022,
        )
    async with factory() as database:
        accepted = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=7022,
            idempotency_key="delete-disabled-voice",
        )

    assert preflight["deletable"] is True
    assert accepted["status"] == "DELETING"


@pytest.mark.anyio
async def test_delete_rechecks_references_after_preflight_and_rejects_cross_tenant(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(
        factory,
        _tenant_voice(profile_id=703, voice="vc-enabled"),
        _tenant_voice(
            profile_id=704,
            tenant_id="tenant-b",
            voice="tenant-b-voice",
        ),
    )
    service = _deletion_service(factory)
    async with factory() as database:
        preflight = await service.deletion_check(
            database,
            tenant_id="tenant-a",
            profile_id=703,
        )
    assert preflight["deletable"] is True

    await _seed(
        factory,
        _outbound_task(task_id=301, voice="vc-enabled", status="RUNNING"),
    )
    async with factory() as database:
        with pytest.raises(CustomException) as blocked:
            await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=703,
                idempotency_key="delete-key-1",
            )
        with pytest.raises(CustomException) as cross_tenant:
            await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=704,
                idempotency_key="delete-key-2",
            )

    assert blocked.value.status_code == 409
    assert blocked.value.data == {
        "blockingTaskCount": 1,
        "historicalTaskCount": 0,
        "blockingTaskIds": ["301"],
    }
    assert cross_tenant.value.status_code == 404
    async with factory() as database:
        profile = await database.get(AiCallTenantVoiceProfileModel, 703)
        assert profile is not None
        assert profile.status == "ENABLED"
        assert await database.scalar(select(AiCallVoiceDeletionModel)) is None


@pytest.mark.anyio
async def test_delete_is_idempotent_and_failed_delete_requires_new_key(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(
        factory,
        _tenant_voice(profile_id=705, voice="vc-enabled"),
        _tenant_voice(profile_id=706, voice="vc-other"),
    )
    service = _deletion_service(factory, ids=(811, 812))

    async with factory() as database:
        accepted = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=705,
            idempotency_key="delete-key-1",
        )
    async with factory() as database:
        repeated = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=705,
            idempotency_key="delete-key-1",
        )
        with pytest.raises(CustomException) as reused_for_other_profile:
            await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=706,
                idempotency_key="delete-key-1",
            )
        with pytest.raises(CustomException) as new_key_while_deleting:
            await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=705,
                idempotency_key="delete-key-2",
            )

    assert accepted == repeated
    assert accepted == {
        "voiceProfileId": "705",
        "deletionId": "811",
        "status": "DELETING",
    }
    assert reused_for_other_profile.value.status_code == 409
    assert new_key_while_deleting.value.status_code == 409

    async with factory() as database:
        deletion = await database.get(AiCallVoiceDeletionModel, 811)
        profile = await database.get(AiCallTenantVoiceProfileModel, 705)
        assert deletion is not None
        assert profile is not None
        deletion.status = "FAILED"
        profile.status = "DELETE_FAILED"
        await database.commit()

    async with factory() as database:
        old_key = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=705,
            idempotency_key="delete-key-1",
        )
        retried = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=705,
            idempotency_key="delete-key-3",
        )

    assert old_key["status"] == "DELETE_FAILED"
    assert retried == {
        "voiceProfileId": "705",
        "deletionId": "812",
        "status": "DELETING",
    }


@pytest.mark.anyio
async def test_concurrent_same_key_delete_returns_single_task(
    deletion_database,
) -> None:
    _engine, factory = deletion_database
    await _seed(factory, _tenant_voice(profile_id=707, voice="vc-enabled"))
    service = _deletion_service(factory, ids=(821, 822))

    async def submit() -> dict[str, object]:
        async with factory() as database:
            return await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=707,
                idempotency_key="same-delete-key",
            )

    first, second = await asyncio.gather(submit(), submit())

    assert first == second
    async with factory() as database:
        rows = (await database.scalars(select(AiCallVoiceDeletionModel))).all()
    assert len(rows) == 1
    assert rows[0].status == "PENDING"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("winner_profile_id", "expected_status"),
    [(709, None), (710, 409)],
)
async def test_delete_rechecks_idempotency_after_profile_lock_wait(
    winner_profile_id: int,
    expected_status: int | None,
) -> None:
    profile = _tenant_voice(
        profile_id=709,
        voice="vc-lock-wait",
        status="DELETING",
    )
    winner = AiCallVoiceDeletionModel(
        id=841,
        tenant_id="tenant-a",
        voice_profile_id=winner_profile_id,
        idempotency_key="same-key-after-lock",
        status="PENDING",
        provider_request_id=None,
        attempt_count=0,
        next_retry_at=None,
        lease_owner=None,
        lease_expires_at=None,
        historical_task_count=0,
        reconcile_absent_count=0,
        error_message=None,
        requested_by=7,
        started_at=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )

    class LockWaitSession:
        def __init__(self) -> None:
            self.scalar_calls = 0
            self.profile_statement = None
            self.rollback_calls = 0

        async def scalar(self, statement):
            self.scalar_calls += 1
            if self.scalar_calls == 1:
                return None
            if self.scalar_calls == 2:
                self.profile_statement = statement
                return profile
            return winner

        async def rollback(self) -> None:
            self.rollback_calls += 1

    database = LockWaitSession()
    service = VoiceDeletionService(session_factory=lambda: None)

    if expected_status is None:
        result = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=709,
            idempotency_key="same-key-after-lock",
        )
        assert result["deletionId"] == "841"
    else:
        with pytest.raises(CustomException) as exc_info:
            await service.request_deletion(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=709,
                idempotency_key="same-key-after-lock",
            )
        assert exc_info.value.status_code == expected_status

    assert database.scalar_calls == 3
    assert database.profile_statement._for_update_arg is not None
    assert database.rollback_calls >= 2


@pytest.mark.anyio
async def test_delete_commit_then_raise_reconciles_persisted_task(
    deletion_database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _engine, factory = deletion_database
    await _seed(factory, _tenant_voice(profile_id=708, voice="vc-enabled"))
    service = _deletion_service(factory, ids=(831,))

    async with factory() as database:
        real_commit = database.commit

        async def commit_then_raise() -> None:
            await real_commit()
            raise RuntimeError("secret connection lost after commit")

        monkeypatch.setattr(database, "commit", commit_then_raise)
        accepted = await service.request_deletion(
            database,
            tenant_id="tenant-a",
            user_id=7,
            profile_id=708,
            idempotency_key="unknown-commit-key",
        )

    assert accepted["deletionId"] == "831"
    assert "secret" not in str(accepted)


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
    service = _voice_service(
        storage,
        factory,
        ids=(123456789012345678, 223456789012345678),
    )
    sample = _upload(filename="../../tenant-a-secret.wav")
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
    assert enrollment.sample_object_key == _sample_key(
        "tenant-a",
        223456789012345678,
    )
    assert "tenant-a" not in enrollment.sample_object_key
    assert "secret" not in enrollment.sample_object_key
    assert ".." not in enrollment.sample_object_key
    assert storage.put_calls[0]["object_key"] == enrollment.sample_object_key
    assert storage.put_calls[0]["content_type"] == "audio/wav"


@pytest.mark.anyio
async def test_create_reuses_same_tenant_key_and_canonical_hash_without_upload(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory, ids=(101, 102))

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
    service = _voice_service(storage, factory, ids=(101, 102))

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
    service = _voice_service(storage, factory, ids=(101, 102, 201, 202))
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
    service = _voice_service(storage, factory)

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
    service = _voice_service(FakeVoiceSampleStorage(), factory)

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
    service = _voice_service(FakeVoiceSampleStorage(), factory)

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
async def test_create_write_then_raise_upload_deletes_known_key(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(put_error=RuntimeError("secret storage credential"))
    service = _voice_service(storage, factory)

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
    assert len(storage.put_calls) == 1
    assert storage.delete_calls == [_sample_key("tenant-a", 102)]
    assert "secret" not in str(caught.value).lower()
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_create_initial_database_failure_has_no_sensitive_exception_chain(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory)

    async with factory() as database:

        async def fail_scalar(*_args, **_kwargs):
            raise RuntimeError("secret sample key from database")

        monkeypatch.setattr(database, "scalar", fail_scalar)
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="sensitive-key",
                request=_enrollment_request(),
                sample=_upload(filename="sensitive.wav"),
            )

    assert caught.value.status_code == 500
    assert storage.put_calls == []
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_create_commit_failure_rolls_back_and_deletes_uploaded_sample(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory)

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
    assert storage.delete_calls == [_sample_key("tenant-a", 102)]
    assert "secret" not in str(caught.value).lower()
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_create_flush_failure_rolls_back_and_deletes_uploaded_sample(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory)

    async with factory() as database:

        async def fail_flush(*_args, **_kwargs) -> None:
            raise RuntimeError("database secret")

        monkeypatch.setattr(database, "flush", fail_flush)
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
    assert storage.delete_calls == [_sample_key("tenant-a", 102)]
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_create_commit_then_raise_reconciles_persisted_winner_without_delete(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory)

    async with factory() as database:
        real_commit = database.commit

        async def commit_then_raise() -> None:
            await real_commit()
            raise RuntimeError("connection lost after commit")

        monkeypatch.setattr(database, "commit", commit_then_raise)
        accepted = await service.create(
            database,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="key-1",
            request=_enrollment_request(),
            sample=_upload(),
        )

    async with factory() as database:
        enrollment = await database.scalar(select(AiCallVoiceEnrollmentModel))
    assert enrollment is not None
    assert accepted.enrollment_id == str(enrollment.id)
    assert storage.delete_calls == []


@pytest.mark.anyio
async def test_create_commit_reconciliation_failure_keeps_sample_and_writes_cleanup(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    factory_calls = 0

    @asynccontextmanager
    async def fail_reconcile_once():
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise RuntimeError("reconciliation unavailable")
        async with factory() as cleanup_db:
            yield cleanup_db

    service = VoiceEnrollmentService(
        storage=storage,
        cleanup_session_factory=fail_reconcile_once,
        target_model=TARGET_MODEL,
        now=lambda: NOW,
        id_generator=SequenceIds(101, 102),
        cleanup_id_generator=SequenceIds(901),
        sample_nonce_generator=SequenceNonces(TEST_SAMPLE_NONCE),
    )

    async with factory() as database:

        async def fail_commit() -> None:
            raise RuntimeError("commit result unknown")

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
        cleanup = await database.scalar(select(AiCallVoiceSampleCleanupModel))
    assert caught.value.status_code == 500
    assert storage.delete_calls == []
    assert cleanup is not None
    assert cleanup.object_key == _sample_key("tenant-a", 102)


@pytest.mark.anyio
async def test_create_id_generation_failure_happens_before_upload(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()

    def fail_id_generation() -> int:
        raise RuntimeError("database setup secret")

    service = VoiceEnrollmentService(
        storage=storage,
        cleanup_session_factory=factory,
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
    assert storage.put_calls == []
    assert storage.delete_calls == []
    assert "secret" not in str(caught.value).lower()


@pytest.mark.anyio
async def test_create_cleanup_failure_is_redacted(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(delete_error=RuntimeError("secret sample and key"))
    service = _voice_service(storage, factory, ids=(101, 102, 103))
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

    async with factory() as database:
        cleanup = await database.scalar(select(AiCallVoiceSampleCleanupModel))

    assert cleanup is not None
    assert cleanup.tenant_id == "tenant-a"
    assert cleanup.object_key == _sample_key("tenant-a", 102)
    assert cleanup.status == "PENDING"
    assert cleanup.attempt_count == 0
    assert cleanup.next_retry_at is None
    assert cleanup.error_message == "即时删除声音样本失败，等待后台重试"
    assert cleanup.created_at.replace(tzinfo=timezone.utc) == NOW
    rendered = " ".join(warnings + [str(caught.value), cleanup.error_message])
    assert "secret" not in rendered.lower()
    assert "sensitive" not in rendered.lower()
    assert _sample_key("tenant-a", 102) not in rendered
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_cleanup_compensation_persistence_failure_only_logs_safe_constant(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(delete_error=RuntimeError("secret object private/sample-1"))
    warnings: list[str] = []
    monkeypatch.setattr(
        "app.api.v1.ai_call.voice.service.log.warning",
        lambda message: warnings.append(str(message)),
    )

    @asynccontextmanager
    async def failing_cleanup_factory():
        async with factory() as cleanup_db:

            async def fail_cleanup_commit() -> None:
                raise RuntimeError("secret cleanup database")

            monkeypatch.setattr(cleanup_db, "commit", fail_cleanup_commit)
            yield cleanup_db

    service = VoiceEnrollmentService(
        storage=storage,
        cleanup_session_factory=failing_cleanup_factory,
        target_model=TARGET_MODEL,
        now=lambda: NOW,
        id_generator=SequenceIds(101, 102, 103),
        sample_nonce_generator=SequenceNonces(TEST_SAMPLE_NONCE),
    )

    async with factory() as database:

        async def fail_main_commit() -> None:
            raise RuntimeError("secret main database")

        monkeypatch.setattr(database, "commit", fail_main_commit)
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="sensitive-key",
                request=_enrollment_request(),
                sample=_upload(filename="sensitive.wav"),
            )

    async with factory() as database:
        assert await database.scalar(select(AiCallVoiceSampleCleanupModel)) is None
    assert warnings == ["音色样本清理补偿持久化失败，需人工检查后台回收"]
    rendered = " ".join(warnings + [str(caught.value)])
    assert "secret" not in rendered.lower()
    assert "sensitive" not in rendered.lower()
    assert _sample_key("tenant-a", 102) not in rendered
    assert _exception_chain(caught.value) == [caught.value]


@pytest.mark.anyio
async def test_cleanup_primary_key_collision_retries_with_new_id(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    existing_key = "ai-call/voice-samples/existing/1.wav"
    target_key = "ai-call/voice-samples/target/2.wav"
    async with factory() as database:
        database.add(
            AiCallVoiceSampleCleanupModel(
                id=901,
                tenant_id="tenant-a",
                object_key=existing_key,
                status="PENDING",
                attempt_count=0,
                next_retry_at=None,
                lease_owner=None,
                lease_expires_at=None,
                error_message="existing",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        await database.commit()

    service = _voice_service(
        FakeVoiceSampleStorage(),
        factory,
        cleanup_ids=(901, 902),
    )
    await service._persist_cleanup_compensation(
        tenant_id="tenant-a",
        object_key=target_key,
    )

    async with factory() as database:
        rows = (
            (
                await database.execute(
                    select(AiCallVoiceSampleCleanupModel).order_by(AiCallVoiceSampleCleanupModel.id)
                )
            )
            .scalars()
            .all()
        )
    assert [(row.id, row.object_key) for row in rows] == [
        (901, existing_key),
        (902, target_key),
    ]


@pytest.mark.anyio
async def test_cleanup_object_key_duplicate_is_treated_as_existing_compensation(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    object_key = "ai-call/voice-samples/target/2.wav"
    async with factory() as database:
        database.add(
            AiCallVoiceSampleCleanupModel(
                id=901,
                tenant_id="tenant-a",
                object_key=object_key,
                status="PENDING",
                attempt_count=0,
                next_retry_at=None,
                lease_owner=None,
                lease_expires_at=None,
                error_message="existing",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        await database.commit()

    service = _voice_service(
        FakeVoiceSampleStorage(),
        factory,
        cleanup_ids=(902,),
    )
    await service._persist_cleanup_compensation(
        tenant_id="tenant-a",
        object_key=object_key,
    )

    async with factory() as database:
        rows = (await database.execute(select(AiCallVoiceSampleCleanupModel))).scalars().all()
    assert [(row.id, row.object_key) for row in rows] == [(901, object_key)]


@pytest.mark.anyio
async def test_create_integrity_failure_without_idempotent_winner_returns_500(
    enrollment_database,
    monkeypatch,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory)

    async with factory() as database:

        async def fail_flush(*_args, **_kwargs) -> None:
            raise IntegrityError("insert", {}, RuntimeError("primary-key collision"))

        monkeypatch.setattr(database, "flush", fail_flush)
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
    assert storage.delete_calls == [_sample_key("tenant-a", 102)]


@pytest.mark.anyio
async def test_enrollment_id_collision_does_not_overwrite_or_delete_old_sample(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    old_object_key = _sample_key("tenant-a", 102, nonce=None)
    old_data = b"old-enrollment-sample"
    storage = StatefulVoiceSampleStorage({old_object_key: old_data})
    service = _voice_service(storage, factory, ids=(101, 102))
    old_profile = _tenant_voice(
        profile_id=900,
        tenant_id="tenant-a",
        voice=None,
        status="CREATING",
    )
    old_profile.latest_enrollment_id = 102
    old_enrollment = AiCallVoiceEnrollmentModel(
        id=102,
        tenant_id="tenant-a",
        voice_profile_id=900,
        idempotency_key="old-key",
        request_hash="0" * 64,
        preferred_name="vc900",
        language="zh",
        transcript=None,
        sample_object_key=old_object_key,
        sample_sha256="1" * 64,
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
    async with factory() as database:
        database.add_all([old_profile, old_enrollment])
        await database.commit()

    async with factory() as database:
        with pytest.raises(CustomException) as caught:
            await service.create(
                database,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="new-key",
                request=_enrollment_request(),
                sample=_upload(),
            )

    assert caught.value.status_code == 500
    assert storage.objects[old_object_key] == old_data
    assert storage.delete_calls == [_sample_key("tenant-a", 102)]
    async with factory() as database:
        persisted = await database.get(AiCallVoiceEnrollmentModel, 102)
    assert persisted is not None
    assert persisted.sample_object_key == old_object_key


@pytest.mark.parametrize("winner_hash_matches", [True, False])
@pytest.mark.anyio
async def test_create_recovers_unique_race_from_persisted_winner(
    enrollment_database,
    monkeypatch,
    winner_hash_matches: bool,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory, ids=(101, 102))

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

    assert storage.delete_calls == [_sample_key("tenant-a", 102)]


@pytest.mark.anyio
async def test_reenroll_concurrent_different_keys_only_one_is_accepted(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = BlockingVoiceSampleStorage()
    async with factory() as database:
        database.add(
            _tenant_voice(
                profile_id=101,
                tenant_id="tenant-a",
                voice=None,
                status="CREATE_FAILED",
            )
        )
        await database.commit()

    first_service = _voice_service(storage, factory, ids=(201,))
    second_service = _voice_service(storage, factory, ids=(301,))

    async def submit(service: VoiceEnrollmentService, key: str):
        async with factory() as database:
            return await service.reenroll(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=101,
                idempotency_key=key,
                request=_enrollment_request(),
                sample=_upload(),
            )

    first_task = asyncio.create_task(submit(first_service, "retry-a"))
    await asyncio.wait_for(storage.first_put_started.wait(), timeout=1)
    second_task = asyncio.create_task(submit(second_service, "retry-b"))
    await asyncio.sleep(0.05)
    storage.release_put.set()
    results = await asyncio.gather(first_task, second_task, return_exceptions=True)

    accepted = [result for result in results if isinstance(result, VoiceEnrollmentAcceptedOut)]
    rejected = [result for result in results if isinstance(result, CustomException)]
    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0].status_code == 409
    assert len(storage.put_calls) == 2
    assert len(storage.delete_calls) == 1
    async with factory() as database:
        enrollments = (await database.execute(select(AiCallVoiceEnrollmentModel))).scalars().all()
    assert len(enrollments) == 1


@pytest.mark.anyio
async def test_reenroll_concurrent_same_key_returns_persisted_winner_to_both(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = BlockingVoiceSampleStorage()
    async with factory() as database:
        database.add(
            _tenant_voice(
                profile_id=101,
                tenant_id="tenant-a",
                voice=None,
                status="CREATE_FAILED",
            )
        )
        await database.commit()

    first_service = _voice_service(storage, factory, ids=(201,))
    second_service = _voice_service(storage, factory, ids=(301,))

    async def submit(service: VoiceEnrollmentService):
        async with factory() as database:
            return await service.reenroll(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=101,
                idempotency_key="same-key",
                request=_enrollment_request(),
                sample=_upload(),
            )

    first_task = asyncio.create_task(submit(first_service))
    await asyncio.wait_for(storage.first_put_started.wait(), timeout=1)
    second_task = asyncio.create_task(submit(second_service))
    await asyncio.sleep(0.05)
    storage.release_put.set()
    first, second = await asyncio.gather(first_task, second_task)

    assert first == second
    assert len(storage.put_calls) == 2
    assert len(storage.delete_calls) == 1
    async with factory() as database:
        enrollments = (await database.execute(select(AiCallVoiceEnrollmentModel))).scalars().all()
    assert len(enrollments) == 1


@pytest.mark.anyio
async def test_reenroll_upload_failure_rolls_back_atomic_state_reservation(
    enrollment_database,
) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage(put_error=RuntimeError("secret upload"))
    service = _voice_service(storage, factory, ids=(201,))
    async with factory() as database:
        database.add(
            _tenant_voice(
                profile_id=101,
                tenant_id="tenant-a",
                voice=None,
                status="CREATE_FAILED",
            )
        )
        await database.commit()

        with pytest.raises(CustomException) as caught:
            await service.reenroll(
                database,
                tenant_id="tenant-a",
                user_id=7,
                profile_id=101,
                idempotency_key="retry-a",
                request=_enrollment_request(),
                sample=_upload(),
            )

    async with factory() as database:
        profile = await database.get(AiCallTenantVoiceProfileModel, 101)
        enrollment = await database.scalar(select(AiCallVoiceEnrollmentModel))
    assert caught.value.status_code == 502
    assert _exception_chain(caught.value) == [caught.value]
    assert profile is not None
    assert profile.status == "CREATE_FAILED"
    assert enrollment is None


@pytest.mark.anyio
async def test_reenroll_requires_current_tenant_failed_profile(enrollment_database) -> None:
    _engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory, ids=(201, 202))
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
    engine, factory = enrollment_database
    storage = FakeVoiceSampleStorage()
    service = _voice_service(storage, factory, ids=(202,))
    update_statements: list[str] = []

    def capture_sql(
        _connection,
        _cursor,
        statement: str,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        if statement.lstrip().upper().startswith("UPDATE"):
            update_statements.append(statement.lower())

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

        event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
        try:
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
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture_sql)

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
    assert any(
        all(
            column in statement.partition(" where ")[2]
            for column in ("tenant_id", ".id", ".status")
        )
        for statement in update_statements
    )
