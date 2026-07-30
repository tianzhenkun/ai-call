from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundTaskModel
from app.api.v1.ai_call.voice.model import (
    AiCallTenantVoiceProfileModel,
    AiCallVoiceDeletionModel,
    AiCallVoiceEnrollmentModel,
    AiCallVoiceSampleCleanupModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call import voice_enrollment_worker as worker_module
from app.services.ai_call.providers.qwen_voice_enrollment import (
    VoiceCreateResult,
    VoiceListItem,
    VoiceProviderRejectedError,
    VoiceProviderResultUnknownError,
    VoiceProviderRetryableError,
)
from app.services.ai_call.voice_enrollment_worker import (
    DELETION_BLOCKED_MESSAGE,
    DELETION_FAILURE_MESSAGE,
    DELETION_RETRY_MESSAGE,
    ENROLLMENT_FAILURE_MESSAGE,
    RETRY_DELAYS,
    SAMPLE_CLEANUP_ERROR_MESSAGE,
    VoiceEnrollmentWorker,
    _matching_voice,
)

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
TARGET_MODEL = "qwen3.5-omni-plus-realtime"


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class FakeProvider:
    def __init__(self) -> None:
        self.create_result = VoiceCreateResult(
            voice="qwen-omni-vc-default",
            request_id="req-default",
        )
        self.create_error: Exception | None = None
        self.list_result: list[VoiceListItem] = []
        self.list_results_by_page: dict[int, list[VoiceListItem]] = {}
        self.list_error: Exception | None = None
        self.delete_result: str | None = "req-delete"
        self.delete_error: Exception | None = None
        self.create_calls: list[dict[str, str]] = []
        self.list_calls: list[dict[str, int]] = []
        self.delete_calls: list[str] = []
        self.on_create: Callable[[], Awaitable[None]] | None = None
        self.on_delete: Callable[[], Awaitable[None]] | None = None

    async def create(
        self,
        *,
        preferred_name: str,
        audio_data_url: str,
    ) -> VoiceCreateResult:
        self.create_calls.append({
            "preferred_name": preferred_name,
            "audio_data_url": audio_data_url,
        })
        if self.on_create is not None:
            await self.on_create()
        if self.create_error is not None:
            raise self.create_error
        return self.create_result

    async def list(
        self,
        *,
        page_index: int = 0,
        page_size: int = 1000,
    ) -> list[VoiceListItem]:
        self.list_calls.append({
            "page_index": page_index,
            "page_size": page_size,
        })
        if self.list_error is not None:
            raise self.list_error
        if page_index in self.list_results_by_page:
            return list(self.list_results_by_page[page_index])
        return list(self.list_result)

    async def delete(self, *, voice: str) -> str | None:
        self.delete_calls.append(voice)
        if self.on_delete is not None:
            await self.on_delete()
        if self.delete_error is not None:
            raise self.delete_error
        return self.delete_result


class FakeStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.get_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.deleted: list[str] = []

    async def put(
        self,
        *,
        object_key: str,
        data: bytes,
        content_type: str,
    ) -> None:
        self.objects[object_key] = data

    async def get(self, object_key: str) -> bytes:
        if self.get_error is not None:
            raise self.get_error
        return self.objects[object_key]

    async def delete(self, object_key: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(object_key)
        self.objects.pop(object_key, None)


class WorkerHarness:
    def __init__(
        self,
        factory: async_sessionmaker,
        *,
        batch_size: int = 10,
        worker_id: str = "worker-a",
    ) -> None:
        self.factory = factory
        self.clock = MutableClock()
        self.provider = FakeProvider()
        self.storage = FakeStorage()
        self.worker = VoiceEnrollmentWorker(
            session_factory=factory,
            provider=self.provider,
            storage=self.storage,
            worker_id=worker_id,
            now=self.clock,
            batch_size=batch_size,
            lease_seconds=60,
        )
        self.next_id = 1

    async def seed_enrollment(
        self,
        *,
        status: str = "PENDING",
        next_retry_at: datetime | None = None,
        lease_owner: str | None = None,
        lease_expires_at: datetime | None = None,
        attempt_count: int = 0,
        sample_object_key: str | None = None,
    ) -> AiCallVoiceEnrollmentModel:
        enrollment_id = self.next_id
        self.next_id += 1
        profile_id = 1000 + enrollment_id
        object_key = sample_object_key or f"voice-samples/{enrollment_id}.wav"
        profile = _profile(
            profile_id=profile_id,
            latest_enrollment_id=enrollment_id,
        )
        enrollment = _enrollment(
            enrollment_id=enrollment_id,
            profile_id=profile_id,
            status=status,
            next_retry_at=next_retry_at,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            attempt_count=attempt_count,
            sample_object_key=object_key,
        )
        async with self.factory() as database:
            database.add_all([profile, enrollment])
            await database.commit()
        self.storage.objects[object_key] = b"sample"
        return enrollment

    async def seed_cleanup(
        self,
        object_key: str,
        *,
        status: str = "PENDING",
        attempt_count: int = 0,
        next_retry_at: datetime | None = None,
        lease_owner: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> AiCallVoiceSampleCleanupModel:
        cleanup = AiCallVoiceSampleCleanupModel(
            id=9000 + self.next_id,
            tenant_id="tenant-a",
            object_key=object_key,
            status=status,
            attempt_count=attempt_count,
            next_retry_at=next_retry_at,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            error_message=None,
            created_at=NOW,
            updated_at=NOW,
        )
        self.next_id += 1
        async with self.factory() as database:
            database.add(cleanup)
            await database.commit()
        self.storage.objects[object_key] = b"orphan"
        return cleanup

    async def seed_deletion(
        self,
        *,
        status: str = "PENDING",
        profile_status: str = "DELETING",
        voice: str = "qwen-omni-vc-delete",
        next_retry_at: datetime | None = None,
        lease_owner: str | None = None,
        lease_expires_at: datetime | None = None,
        attempt_count: int = 0,
        reconcile_absent_count: int = 0,
        tenant_id: str = "tenant-a",
    ) -> AiCallVoiceDeletionModel:
        deletion_id = 20_000 + self.next_id
        profile_id = 30_000 + self.next_id
        self.next_id += 1
        profile = _profile(
            profile_id=profile_id,
            latest_enrollment_id=1,
            tenant_id=tenant_id,
            voice=voice,
            status=profile_status,
        )
        deletion = AiCallVoiceDeletionModel(
            id=deletion_id,
            tenant_id=tenant_id,
            voice_profile_id=profile_id,
            idempotency_key=f"delete-key-{deletion_id}",
            status=status,
            provider_request_id=None,
            attempt_count=attempt_count,
            next_retry_at=next_retry_at,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            historical_task_count=0,
            reconcile_absent_count=reconcile_absent_count,
            error_message=None,
            requested_by=7,
            started_at=None,
            finished_at=None,
            created_at=NOW,
            updated_at=NOW,
        )
        async with self.factory() as database:
            database.add_all([profile, deletion])
            await database.commit()
        return deletion

    async def seed_outbound_task(
        self,
        *,
        task_id: int,
        voice: str,
        status: str,
        tenant_id: str = "tenant-a",
    ) -> None:
        async with self.factory() as database:
            database.add(
                _outbound_task(
                    task_id=task_id,
                    tenant_id=tenant_id,
                    voice=voice,
                    status=status,
                )
            )
            await database.commit()

    async def enrollment(self, enrollment_id: int) -> AiCallVoiceEnrollmentModel:
        async with self.factory() as database:
            row = await database.get(AiCallVoiceEnrollmentModel, enrollment_id)
            assert row is not None
            return row

    async def profile(self, profile_id: int) -> AiCallTenantVoiceProfileModel:
        async with self.factory() as database:
            row = await database.get(AiCallTenantVoiceProfileModel, profile_id)
            assert row is not None
            return row

    async def cleanup(self, cleanup_id: int) -> AiCallVoiceSampleCleanupModel:
        async with self.factory() as database:
            row = await database.get(AiCallVoiceSampleCleanupModel, cleanup_id)
            assert row is not None
            return row

    async def deletion(self, deletion_id: int) -> AiCallVoiceDeletionModel:
        async with self.factory() as database:
            row = await database.get(AiCallVoiceDeletionModel, deletion_id)
            assert row is not None
            return row


@pytest.fixture
async def worker_database(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'voice-worker.db'}",
    )
    tables = [
        AiCallTenantVoiceProfileModel.__table__,
        AiCallVoiceEnrollmentModel.__table__,
        AiCallVoiceDeletionModel.__table__,
        AiCallVoiceSampleCleanupModel.__table__,
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


@pytest.fixture
def harness(worker_database) -> WorkerHarness:
    _engine, factory = worker_database
    return WorkerHarness(factory)


def _profile(
    *,
    profile_id: int,
    latest_enrollment_id: int,
    tenant_id: str = "tenant-a",
    voice: str | None = None,
    status: str = "CREATING",
) -> AiCallTenantVoiceProfileModel:
    return AiCallTenantVoiceProfileModel(
        id=profile_id,
        tenant_id=tenant_id,
        display_name=f"音色-{profile_id}",
        voice=voice,
        voice_type="自定义复刻",
        gender="女声",
        language="zh",
        target_model=TARGET_MODEL,
        provider="aliyun_qwen",
        status=status,
        latest_enrollment_id=latest_enrollment_id,
        provider_created_at=None,
        error_message=None,
        created_by=7,
        deleted_by=None,
        deleted_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _outbound_task(
    *,
    task_id: int,
    tenant_id: str,
    voice: str,
    status: str,
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


def _enrollment(
    *,
    enrollment_id: int,
    profile_id: int,
    status: str,
    sample_object_key: str | None,
    next_retry_at: datetime | None = None,
    lease_owner: str | None = None,
    lease_expires_at: datetime | None = None,
    attempt_count: int = 0,
) -> AiCallVoiceEnrollmentModel:
    return AiCallVoiceEnrollmentModel(
        id=enrollment_id,
        tenant_id="tenant-a",
        voice_profile_id=profile_id,
        idempotency_key=f"key-{enrollment_id}",
        request_hash=f"{enrollment_id:064x}",
        preferred_name=f"vc{enrollment_id}",
        language="zh",
        transcript=None,
        sample_object_key=sample_object_key,
        sample_sha256=f"{enrollment_id + 1:064x}",
        status=status,
        provider_voice=None,
        provider_request_id=None,
        attempt_count=attempt_count,
        next_retry_at=next_retry_at,
        lease_owner=lease_owner,
        lease_expires_at=lease_expires_at,
        error_message=None,
        cleanup_error_message=None,
        consent_user_id=7,
        consent_at=NOW,
        started_at=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


@pytest.mark.anyio
async def test_claims_due_and_expired_states_with_lease_and_attempt(harness) -> None:
    pending = await harness.seed_enrollment(status="PENDING")
    due_retry = await harness.seed_enrollment(
        status="RETRY_WAIT",
        next_retry_at=NOW,
        attempt_count=1,
    )
    await harness.seed_enrollment(
        status="RETRY_WAIT",
        next_retry_at=NOW + timedelta(seconds=1),
    )
    expired_processing = await harness.seed_enrollment(
        status="PROCESSING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=2,
    )
    await harness.seed_enrollment(
        status="PROCESSING",
        lease_owner="active-worker",
        lease_expires_at=NOW + timedelta(seconds=1),
    )
    expired_reconciling = await harness.seed_enrollment(
        status="RECONCILING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=1,
    )

    claimed = await harness.worker._claim_enrollments()

    assert [item.id for item in claimed] == [
        pending.id,
        due_retry.id,
        expired_processing.id,
        expired_reconciling.id,
    ]
    assert [item.status for item in claimed] == [
        "PROCESSING",
        "PROCESSING",
        "RECONCILING",
        "RECONCILING",
    ]
    assert [item.attempt_count for item in claimed] == [1, 2, 3, 2]
    for item in claimed:
        assert item.lease_owner == "worker-a"
        assert item.lease_expires_at == NOW + timedelta(seconds=60)


@pytest.mark.anyio
async def test_claim_batch_is_bounded(harness) -> None:
    harness.worker.batch_size = 2
    await harness.seed_enrollment()
    await harness.seed_enrollment()
    await harness.seed_enrollment()

    claimed = await harness.worker._claim_enrollments()

    assert len(claimed) == 2


def test_postgres_claim_uses_for_update_skip_locked() -> None:
    statement = VoiceEnrollmentWorker._enrollment_claim_select(
        now=NOW,
        batch_size=10,
    )

    sql = str(statement.compile(dialect=postgresql.dialect())).lower()

    assert "for update skip locked" in sql
    assert "limit" in sql


@pytest.mark.parametrize(
    "voice",
    [
        "vc1",
        "qwen-omni-vc-vc1",
        "qwen-omni-vc-vc1-voice-20260730",
    ],
)
def test_matching_voice_accepts_only_supported_qwen_formats(voice: str) -> None:
    voices = [
        VoiceListItem(
            voice=voice,
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    assert (
        _matching_voice(
            voices,
            preferred_name="vc1",
            target_model=TARGET_MODEL,
        )
        == voice
    )


@pytest.mark.parametrize(
    "voice",
    [
        "qwen-omni-vc-vc10-voice-20260730",
        "qwen-omni-vc-xvc1-voice-20260730",
        "other-vc1-voice-20260730",
        "qwen-omni-vc-vc1-voice-",
        "not-qwen-omni-vc-vc1",
    ],
)
def test_matching_voice_rejects_similar_or_malformed_names(voice: str) -> None:
    voices = [
        VoiceListItem(
            voice=voice,
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    assert (
        _matching_voice(
            voices,
            preferred_name="vc1",
            target_model=TARGET_MODEL,
        )
        is None
    )


@pytest.mark.anyio
async def test_sqlite_workers_do_not_claim_same_enrollment(worker_database) -> None:
    _engine, factory = worker_database
    first = WorkerHarness(factory, worker_id="worker-a")
    second = WorkerHarness(factory, worker_id="worker-b")
    await first.seed_enrollment()

    first_claimed, second_claimed = await asyncio.gather(
        first.worker._claim_enrollments(),
        second.worker._claim_enrollments(),
    )

    claimed_ids = [item.id for item in first_claimed + second_claimed]
    assert claimed_ids == [1]


@pytest.mark.anyio
async def test_claims_due_and_expired_deletions_with_fencing(harness) -> None:
    pending = await harness.seed_deletion(status="PENDING")
    due_retry = await harness.seed_deletion(
        status="RETRY_WAIT",
        voice="qwen-omni-vc-delete-2",
        next_retry_at=NOW,
        attempt_count=1,
    )
    await harness.seed_deletion(
        status="RETRY_WAIT",
        voice="qwen-omni-vc-delete-3",
        next_retry_at=NOW + timedelta(seconds=1),
    )
    expired_processing = await harness.seed_deletion(
        status="PROCESSING",
        voice="qwen-omni-vc-delete-4",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=2,
    )
    expired_reconciling = await harness.seed_deletion(
        status="RECONCILING",
        voice="qwen-omni-vc-delete-5",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=1,
    )

    claimed = await harness.worker._claim_deletions()

    assert [item.id for item in claimed] == [
        pending.id,
        due_retry.id,
        expired_processing.id,
        expired_reconciling.id,
    ]
    assert [item.status for item in claimed] == [
        "PROCESSING",
        "PROCESSING",
        "RECONCILING",
        "RECONCILING",
    ]
    assert [item.attempt_count for item in claimed] == [1, 2, 3, 2]
    assert all(item.lease_owner == "worker-a" for item in claimed)


def test_postgres_deletion_claim_uses_for_update_skip_locked() -> None:
    statement = VoiceEnrollmentWorker._deletion_claim_select(
        now=NOW,
        batch_size=10,
    )

    sql = str(statement.compile(dialect=postgresql.dialect())).lower()

    assert "for update skip locked" in sql
    assert "limit" in sql


@pytest.mark.anyio
async def test_worker_deletes_provider_voice_and_marks_profile_deleted(harness) -> None:
    task = await harness.seed_deletion()

    await harness.worker.run_once()

    deletion = await harness.deletion(task.id)
    profile = await harness.profile(task.voice_profile_id)
    assert harness.provider.delete_calls == ["qwen-omni-vc-delete"]
    assert deletion.status == "SUCCEEDED"
    assert deletion.provider_request_id == "req-delete"
    assert deletion.lease_owner is None
    assert profile.status == "DELETED"
    assert profile.deleted_at is not None


@pytest.mark.anyio
async def test_delete_provider_call_happens_after_claim_commit(harness) -> None:
    task = await harness.seed_deletion()

    async def assert_committed_claim() -> None:
        current = await harness.deletion(task.id)
        assert current.status == "PROCESSING"
        assert current.lease_owner == "worker-a"

    harness.provider.on_delete = assert_committed_claim

    await harness.worker.run_once()


@pytest.mark.anyio
async def test_sqlite_workers_do_not_delete_same_voice_twice(worker_database) -> None:
    _engine, factory = worker_database
    first = WorkerHarness(factory, worker_id="worker-a")
    task = await first.seed_deletion()
    second = VoiceEnrollmentWorker(
        session_factory=factory,
        provider=first.provider,
        storage=first.storage,
        worker_id="worker-b",
        now=first.clock,
        batch_size=10,
        lease_seconds=60,
    )

    await asyncio.gather(first.worker.run_once(), second.run_once())

    assert first.provider.delete_calls == ["qwen-omni-vc-delete"]
    assert (await first.deletion(task.id)).status == "SUCCEEDED"


@pytest.mark.anyio
async def test_worker_rechecks_blocking_reference_without_calling_provider(
    harness,
) -> None:
    task = await harness.seed_deletion()
    await harness.seed_outbound_task(
        task_id=91,
        voice="qwen-omni-vc-delete",
        status="RUNNING",
    )
    await harness.seed_outbound_task(
        task_id=92,
        voice="qwen-omni-vc-delete",
        status="COMPLETED",
    )
    await harness.seed_outbound_task(
        task_id=93,
        tenant_id="tenant-b",
        voice="qwen-omni-vc-delete",
        status="RUNNING",
    )

    await harness.worker.run_once()

    deletion = await harness.deletion(task.id)
    profile = await harness.profile(task.voice_profile_id)
    assert harness.provider.delete_calls == []
    assert deletion.status == "FAILED"
    assert deletion.error_message == DELETION_BLOCKED_MESSAGE
    assert deletion.historical_task_count == 1
    assert profile.status == "ENABLED"
    assert profile.error_message is None


@pytest.mark.anyio
async def test_worker_does_not_load_profile_from_another_tenant(harness) -> None:
    task = await harness.seed_deletion()
    async with harness.factory() as database:
        stored = await database.get(AiCallVoiceDeletionModel, task.id)
        assert stored is not None
        stored.tenant_id = "tenant-b"
        await database.commit()

    await harness.worker.run_once()

    assert harness.provider.delete_calls == []
    assert (await harness.deletion(task.id)).status == "FAILED"
    assert (await harness.profile(task.voice_profile_id)).status == "DELETING"


@pytest.mark.anyio
async def test_retryable_delete_uses_backoff_and_eventually_fails_safely(
    harness,
) -> None:
    task = await harness.seed_deletion()
    harness.provider.delete_error = VoiceProviderRetryableError(
        "secret provider failure qwen-omni-vc-delete"
    )

    for expected_delay in RETRY_DELAYS:
        await harness.worker.run_once()
        deletion = await harness.deletion(task.id)
        assert deletion.status == "RETRY_WAIT"
        assert (_aware(deletion.next_retry_at) - harness.clock.value).total_seconds() == (
            expected_delay
        )
        assert deletion.error_message == DELETION_RETRY_MESSAGE
        harness.clock.advance(expected_delay)

    await harness.worker.run_once()

    deletion = await harness.deletion(task.id)
    profile = await harness.profile(task.voice_profile_id)
    assert deletion.status == "FAILED"
    assert deletion.error_message == DELETION_FAILURE_MESSAGE
    assert profile.status == "DELETE_FAILED"
    assert "secret" not in (deletion.error_message or "")


@pytest.mark.anyio
async def test_unknown_delete_result_reconciles_absence_without_second_delete(
    harness,
) -> None:
    task = await harness.seed_deletion()
    harness.provider.delete_error = VoiceProviderResultUnknownError("secret timeout")

    await harness.worker.run_once()

    awaiting_reconciliation = await harness.deletion(task.id)
    assert awaiting_reconciliation.status == "RECONCILING"
    assert len(harness.provider.delete_calls) == 1

    harness.clock.advance(RETRY_DELAYS[0] + 1)
    harness.provider.delete_error = None
    harness.provider.list_result = []
    await harness.worker.run_once()

    first_absence = await harness.deletion(task.id)
    assert first_absence.status == "RECONCILING"
    assert first_absence.reconcile_absent_count == 1
    harness.clock.advance(RETRY_DELAYS[1] + 1)
    await harness.worker.run_once()

    completed = await harness.deletion(task.id)
    assert completed.status == "SUCCEEDED"
    assert completed.reconcile_absent_count == 2
    assert (await harness.profile(task.voice_profile_id)).status == "DELETED"
    assert len(harness.provider.delete_calls) == 1
    assert len(harness.provider.list_calls) == 2


@pytest.mark.anyio
async def test_unknown_delete_reconciles_presence_without_retrying_delete(
    harness,
) -> None:
    task = await harness.seed_deletion()
    harness.provider.delete_error = VoiceProviderResultUnknownError("secret timeout")

    await harness.worker.run_once()
    harness.clock.advance(RETRY_DELAYS[0] + 1)
    harness.provider.delete_error = None
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-delete",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await harness.worker.run_once()

    reconciled = await harness.deletion(task.id)
    assert reconciled.status == "RECONCILING"
    assert reconciled.reconcile_absent_count == 0
    assert len(harness.provider.delete_calls) == 1
    assert len(harness.provider.list_calls) == 1

    harness.clock.advance(RETRY_DELAYS[1] + 1)
    harness.provider.list_result = []
    await harness.worker.run_once()
    assert (await harness.deletion(task.id)).status == "RECONCILING"
    harness.clock.advance(RETRY_DELAYS[2] + 1)
    await harness.worker.run_once()

    assert (await harness.deletion(task.id)).status == "SUCCEEDED"
    assert len(harness.provider.delete_calls) == 1


@pytest.mark.anyio
async def test_reconciliation_absence_wins_over_new_blocking_reference(harness) -> None:
    task = await harness.seed_deletion()
    harness.provider.delete_error = VoiceProviderResultUnknownError("secret timeout")
    await harness.worker.run_once()
    await harness.seed_outbound_task(
        task_id=94,
        voice="qwen-omni-vc-delete",
        status="RUNNING",
    )
    harness.provider.delete_error = None
    harness.provider.list_result = []

    harness.clock.advance(RETRY_DELAYS[0] + 1)
    await harness.worker.run_once()
    harness.clock.advance(RETRY_DELAYS[1] + 1)
    await harness.worker.run_once()

    assert (await harness.deletion(task.id)).status == "SUCCEEDED"
    assert (await harness.profile(task.voice_profile_id)).status == "DELETED"
    assert harness.provider.delete_calls == ["qwen-omni-vc-delete"]


@pytest.mark.anyio
async def test_reconciliation_found_then_blocking_reference_restores_profile(harness) -> None:
    task = await harness.seed_deletion()
    harness.provider.delete_error = VoiceProviderResultUnknownError("secret timeout")
    await harness.worker.run_once()
    await harness.seed_outbound_task(
        task_id=95,
        voice="qwen-omni-vc-delete",
        status="RUNNING",
    )
    harness.provider.delete_error = None
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-delete",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    harness.clock.advance(RETRY_DELAYS[0] + 1)
    await harness.worker.run_once()

    assert (await harness.deletion(task.id)).status == "FAILED"
    assert (await harness.profile(task.voice_profile_id)).status == "ENABLED"
    assert harness.provider.delete_calls == ["qwen-omni-vc-delete"]


@pytest.mark.anyio
async def test_reconciliation_scans_pages_without_accumulating_full_list(
    harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "DEFAULT_LIST_PAGE_SIZE", 2)
    task = await harness.seed_deletion(
        status="RECONCILING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
    )
    harness.provider.list_results_by_page = {
        0: [
            VoiceListItem(voice="other-1", target_model=TARGET_MODEL, gmt_create=None),
            VoiceListItem(voice="other-2", target_model=TARGET_MODEL, gmt_create=None),
        ],
        1: [
            VoiceListItem(
                voice="qwen-omni-vc-delete",
                target_model=TARGET_MODEL,
                gmt_create=None,
            )
        ],
    }

    await harness.worker.run_once()

    assert [call["page_index"] for call in harness.provider.list_calls] == [0, 1]
    assert (await harness.deletion(task.id)).status == "RECONCILING"
    assert (await harness.deletion(task.id)).reconcile_absent_count == 0
    assert harness.provider.delete_calls == []


@pytest.mark.anyio
async def test_full_page_cap_never_proves_voice_absent(
    harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, "DEFAULT_LIST_PAGE_SIZE", 2)
    monkeypatch.setattr(worker_module, "MAX_RECONCILIATION_PAGES", 2)
    task = await harness.seed_deletion(
        status="RECONCILING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
    )
    full_page = [
        VoiceListItem(voice="other-1", target_model=TARGET_MODEL, gmt_create=None),
        VoiceListItem(voice="other-2", target_model=TARGET_MODEL, gmt_create=None),
    ]
    harness.provider.list_results_by_page = {0: full_page, 1: full_page}

    await harness.worker.run_once()

    deletion = await harness.deletion(task.id)
    assert [call["page_index"] for call in harness.provider.list_calls] == [0, 1]
    assert deletion.status == "RECONCILING"
    assert deletion.reconcile_absent_count == 0


@pytest.mark.anyio
async def test_same_owner_stale_delete_attempt_cannot_publish_after_reclaim(
    harness,
) -> None:
    task = await harness.seed_deletion(
        status="RECONCILING",
        lease_owner="worker-a",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=1,
    )
    first_claim = (await harness.worker._claim_deletions())[0]
    harness.clock.advance(61)
    second_claim = (await harness.worker._claim_deletions())[0]

    await harness.worker._complete_deletion(first_claim, request_id=None)
    assert (await harness.deletion(task.id)).status == "RECONCILING"

    await harness.worker._complete_deletion(second_claim, request_id=None)
    assert (await harness.deletion(task.id)).status == "SUCCEEDED"


@pytest.mark.anyio
async def test_delete_publish_failure_recovers_by_list_without_second_delete(
    harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = await harness.seed_deletion()

    async def fail_publish(*_args, **_kwargs) -> None:
        raise RuntimeError("database publish failed")

    monkeypatch.setattr(harness.worker, "_complete_deletion", fail_publish)
    await harness.worker.run_once()

    assert (await harness.deletion(task.id)).status == "PROCESSING"
    assert len(harness.provider.delete_calls) == 1

    second_worker = VoiceEnrollmentWorker(
        session_factory=harness.factory,
        provider=harness.provider,
        storage=harness.storage,
        worker_id="worker-a",
        now=harness.clock,
        batch_size=10,
        lease_seconds=60,
    )
    harness.clock.advance(61)
    harness.provider.list_result = []
    await second_worker.run_once()
    harness.clock.advance(RETRY_DELAYS[1] + 1)
    await second_worker.run_once()

    assert (await harness.deletion(task.id)).status == "SUCCEEDED"
    assert len(harness.provider.delete_calls) == 1
    assert len(harness.provider.list_calls) == 2


@pytest.mark.anyio
async def test_provider_call_happens_after_claim_commit(harness) -> None:
    task = await harness.seed_enrollment()

    async def assert_committed_claim() -> None:
        current = await harness.enrollment(task.id)
        assert current.status == "PROCESSING"
        assert current.lease_owner == "worker-a"

    harness.provider.on_create = assert_committed_claim

    await harness.worker.run_once()


@pytest.mark.anyio
async def test_claimed_batch_runs_concurrently_and_isolates_task_failure(
    harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed_task = await harness.seed_enrollment()
    successful_task = await harness.seed_enrollment()
    entered: list[int] = []
    all_entered = asyncio.Event()
    original_process = harness.worker._process_enrollment

    async def isolated_process(item) -> None:
        entered.append(item.id)
        if len(entered) == 2:
            all_entered.set()
        await asyncio.wait_for(all_entered.wait(), timeout=1)
        if item.id == failed_task.id:
            raise RuntimeError("isolated task failure")
        await original_process(item)

    monkeypatch.setattr(harness.worker, "_process_enrollment", isolated_process)

    await harness.worker.run_once()

    assert sorted(entered) == [failed_task.id, successful_task.id]
    assert (await harness.enrollment(failed_task.id)).status == "PROCESSING"
    assert (await harness.enrollment(successful_task.id)).status == "SUCCEEDED"


@pytest.mark.anyio
async def test_worker_publishes_voice_and_deletes_sample(harness) -> None:
    task = await harness.seed_enrollment()
    harness.provider.create_result = VoiceCreateResult(
        voice="qwen-omni-vc-a",
        request_id="req-1",
    )

    await harness.worker.run_once()

    profile = await harness.profile(task.voice_profile_id)
    enrollment = await harness.enrollment(task.id)
    assert profile.status == "ENABLED"
    assert profile.voice == "qwen-omni-vc-a"
    assert profile.provider_created_at is not None
    assert enrollment.status == "SUCCEEDED"
    assert enrollment.provider_voice == "qwen-omni-vc-a"
    assert enrollment.provider_request_id == "req-1"
    assert enrollment.sample_object_key is None
    assert enrollment.cleanup_error_message is None
    assert harness.storage.deleted == ["voice-samples/1.wav"]
    assert harness.provider.create_calls[0]["audio_data_url"].startswith("data:audio/wav;base64,")


@pytest.mark.anyio
async def test_rejected_request_fails_profile_and_cleans_sample(harness) -> None:
    task = await harness.seed_enrollment()
    harness.provider.create_error = VoiceProviderRejectedError(
        "secret provider response for voice-samples/1.wav"
    )

    await harness.worker.run_once()

    profile = await harness.profile(task.voice_profile_id)
    enrollment = await harness.enrollment(task.id)
    assert profile.status == "CREATE_FAILED"
    assert profile.error_message == ENROLLMENT_FAILURE_MESSAGE
    assert enrollment.status == "FAILED"
    assert enrollment.error_message == ENROLLMENT_FAILURE_MESSAGE
    assert enrollment.sample_object_key is None
    rendered = f"{profile.error_message} {enrollment.error_message}"
    assert "secret" not in rendered
    assert "voice-samples" not in rendered


@pytest.mark.parametrize("failure_kind", ["429", "500", "connection"])
@pytest.mark.anyio
async def test_retryable_provider_failures_use_fixed_backoff_and_eventually_fail(
    harness,
    failure_kind: str,
) -> None:
    task = await harness.seed_enrollment()
    harness.provider.create_error = VoiceProviderRetryableError(
        f"secret {failure_kind} voice-samples/1.wav"
    )

    for expected_delay in RETRY_DELAYS:
        await harness.worker.run_once()
        enrollment = await harness.enrollment(task.id)
        assert enrollment.status == "RETRY_WAIT"
        assert (
            _aware(enrollment.next_retry_at) - harness.clock.value
        ).total_seconds() == expected_delay
        assert enrollment.sample_object_key == "voice-samples/1.wav"
        harness.clock.advance(expected_delay)

    await harness.worker.run_once()

    enrollment = await harness.enrollment(task.id)
    profile = await harness.profile(task.voice_profile_id)
    assert enrollment.status == "FAILED"
    assert enrollment.attempt_count == 4
    assert enrollment.sample_object_key is None
    assert profile.status == "CREATE_FAILED"
    assert "secret" not in (enrollment.error_message or "")


@pytest.mark.anyio
async def test_unknown_create_result_reconciles_without_second_create(harness) -> None:
    task = await harness.seed_enrollment()
    harness.provider.create_error = VoiceProviderResultUnknownError("secret timeout")

    await harness.worker.run_once()

    first = await harness.enrollment(task.id)
    assert first.status == "RECONCILING"
    assert first.sample_object_key == "voice-samples/1.wav"
    assert len(harness.provider.create_calls) == 1
    # PROCESSING / RECONCILING leases are reclaimable only after, not at, expiry.
    harness.clock.advance(RETRY_DELAYS[0] + 1)
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await harness.worker.run_once()

    enrollment = await harness.enrollment(task.id)
    profile = await harness.profile(task.voice_profile_id)
    assert enrollment.status == "SUCCEEDED"
    assert profile.voice == "qwen-omni-vc-vc1"
    assert len(harness.provider.create_calls) == 1
    assert len(harness.provider.list_calls) == 1


@pytest.mark.anyio
async def test_last_create_attempt_with_unknown_result_reconciles_before_failure(
    harness,
) -> None:
    task = await harness.seed_enrollment(
        status="RETRY_WAIT",
        next_retry_at=NOW,
        attempt_count=3,
    )
    harness.provider.create_error = VoiceProviderResultUnknownError("secret timeout")

    await harness.worker.run_once()

    awaiting_reconciliation = await harness.enrollment(task.id)
    assert awaiting_reconciliation.status == "RECONCILING"
    assert awaiting_reconciliation.attempt_count == 4
    assert len(harness.provider.create_calls) == 1

    harness.clock.advance(RETRY_DELAYS[-1] + 1)
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1-voice-final",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await harness.worker.run_once()

    assert (await harness.enrollment(task.id)).status == "SUCCEEDED"
    assert len(harness.provider.create_calls) == 1
    assert len(harness.provider.list_calls) >= 1


@pytest.mark.anyio
async def test_expired_reconciling_uses_list_and_never_create(harness) -> None:
    task = await harness.seed_enrollment(
        status="RECONCILING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=1,
    )
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model="qwen-other-model",
            gmt_create=None,
        ),
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model=None,
            gmt_create=None,
        ),
    ]

    await harness.worker.run_once()

    awaiting_match = await harness.enrollment(task.id)
    assert awaiting_match.status == "RECONCILING"
    assert harness.provider.create_calls == []
    assert len(harness.provider.list_calls) == 1

    harness.clock.advance(RETRY_DELAYS[1] + 1)
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await harness.worker.run_once()

    assert (await harness.enrollment(task.id)).status == "SUCCEEDED"
    assert harness.provider.create_calls == []
    assert len(harness.provider.list_calls) == 2


@pytest.mark.anyio
async def test_expired_processing_reconciles_and_never_creates_again(harness) -> None:
    task = await harness.seed_enrollment(
        status="PROCESSING",
        lease_owner="dead-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        attempt_count=1,
    )
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await harness.worker.run_once()

    assert (await harness.enrollment(task.id)).status == "SUCCEEDED"
    assert harness.provider.create_calls == []
    assert len(harness.provider.list_calls) == 1


@pytest.mark.anyio
async def test_create_publish_failure_recovers_via_list_without_second_create(
    harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = await harness.seed_enrollment()

    async def fail_publish(*args, **kwargs) -> None:
        raise RuntimeError("database publish failed")

    monkeypatch.setattr(harness.worker, "_publish_success", fail_publish)

    await harness.worker.run_once()

    assert (await harness.enrollment(task.id)).status == "PROCESSING"
    assert len(harness.provider.create_calls) == 1

    second_worker_with_same_owner = VoiceEnrollmentWorker(
        session_factory=harness.factory,
        provider=harness.provider,
        storage=harness.storage,
        worker_id="worker-a",
        now=harness.clock,
        batch_size=10,
        lease_seconds=60,
    )
    harness.clock.advance(61)
    harness.provider.list_result = [
        VoiceListItem(
            voice="qwen-omni-vc-vc1",
            target_model=TARGET_MODEL,
            gmt_create=None,
        )
    ]

    await second_worker_with_same_owner.run_once()

    assert (await harness.enrollment(task.id)).status == "SUCCEEDED"
    assert len(harness.provider.create_calls) == 1
    assert len(harness.provider.list_calls) == 1


@pytest.mark.anyio
async def test_same_owner_stale_attempt_cannot_publish_after_reclaim(harness) -> None:
    task = await harness.seed_enrollment(
        status="RECONCILING",
        lease_owner="worker-a",
        lease_expires_at=NOW - timedelta(seconds=1),
    )
    first_claim = (await harness.worker._claim_enrollments())[0]
    harness.clock.advance(61)
    second_claim = (await harness.worker._claim_enrollments())[0]

    await harness.worker._publish_success(
        first_claim,
        voice="qwen-omni-vc-stale",
        request_id=None,
    )

    after_stale_publish = await harness.enrollment(task.id)
    assert after_stale_publish.status == "RECONCILING"
    assert after_stale_publish.provider_voice is None

    await harness.worker._publish_success(
        second_claim,
        voice="qwen-omni-vc-current",
        request_id=None,
    )

    assert (await harness.enrollment(task.id)).status == "SUCCEEDED"


@pytest.mark.anyio
async def test_terminal_sample_cleanup_retries_without_exposing_failure(
    harness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    task = await harness.seed_enrollment()
    harness.storage.delete_error = RuntimeError("secret storage failure voice-samples/1.wav")
    caplog.set_level("WARNING")
    caplog.clear()

    await harness.worker.run_once()

    first = await harness.enrollment(task.id)
    assert first.status == "SUCCEEDED"
    assert first.sample_object_key == "voice-samples/1.wav"
    assert first.cleanup_error_message == SAMPLE_CLEANUP_ERROR_MESSAGE
    assert "secret" not in caplog.text
    assert "voice-samples/1.wav" not in caplog.text

    harness.storage.delete_error = None
    await harness.worker.run_once()

    cleaned = await harness.enrollment(task.id)
    assert cleaned.sample_object_key is None
    assert cleaned.cleanup_error_message is None
    assert harness.storage.deleted == ["voice-samples/1.wav"]


@pytest.mark.anyio
async def test_cleanup_record_with_enrollment_reference_never_deletes_object(
    harness,
) -> None:
    enrollment = await harness.seed_enrollment(
        status="PROCESSING",
        lease_owner="active-worker",
        lease_expires_at=NOW + timedelta(minutes=5),
        sample_object_key="voice-samples/referenced.wav",
    )
    cleanup = await harness.seed_cleanup("voice-samples/referenced.wav")

    await harness.worker.run_once()

    saved = await harness.cleanup(cleanup.id)
    assert saved.status == "SUCCEEDED"
    assert saved.error_message is None
    assert harness.storage.deleted == []
    assert (
        await harness.enrollment(enrollment.id)
    ).sample_object_key == "voice-samples/referenced.wav"


@pytest.mark.anyio
async def test_same_owner_stale_cleanup_attempt_cannot_finish_after_reclaim(
    harness,
) -> None:
    cleanup = await harness.seed_cleanup(
        "voice-samples/orphan.wav",
        status="PROCESSING",
        lease_owner="worker-a",
        lease_expires_at=NOW - timedelta(seconds=1),
    )
    first_claim = (await harness.worker._claim_cleanup_records())[0]
    harness.clock.advance(61)
    second_claim = (await harness.worker._claim_cleanup_records())[0]

    await harness.worker._finish_cleanup(first_claim)

    after_stale_finish = await harness.cleanup(cleanup.id)
    assert after_stale_finish.status == "PROCESSING"
    assert after_stale_finish.attempt_count == second_claim.attempt_count

    await harness.worker._finish_cleanup(second_claim)

    assert (await harness.cleanup(cleanup.id)).status == "SUCCEEDED"


@pytest.mark.anyio
async def test_cleanup_failure_uses_safe_fixed_backoff(
    harness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cleanup = await harness.seed_cleanup("voice-samples/orphan-secret.wav")
    harness.storage.delete_error = RuntimeError(
        "secret storage failure voice-samples/orphan-secret.wav"
    )
    caplog.set_level("WARNING")
    caplog.clear()

    for expected_delay in RETRY_DELAYS:
        await harness.worker.run_once()
        saved = await harness.cleanup(cleanup.id)
        assert saved.status == "RETRY_WAIT"
        assert (_aware(saved.next_retry_at) - harness.clock.value).total_seconds() == expected_delay
        assert saved.error_message == SAMPLE_CLEANUP_ERROR_MESSAGE
        assert "secret" not in saved.error_message
        assert "voice-samples" not in saved.error_message
        harness.clock.advance(expected_delay)

    assert "secret" not in caplog.text
    assert "voice-samples/orphan-secret.wav" not in caplog.text
