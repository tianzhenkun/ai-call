from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.voice import controller as voice_controller
from app.api.v1.ai_call.voice.controller import (
    get_voice_enrollment_service,
    get_voice_lifecycle_service,
)
from app.api.v1.ai_call.voice.schema import (
    VoiceEnrollmentAcceptedOut,
    VoiceProfileOut,
)
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.config.setting import settings
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception

TARGET_MODEL = "qwen3.5-omni-plus-realtime"
NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
BIG_PROFILE_ID = 9_007_199_254_740_993
BIG_ENROLLMENT_ID = 9_007_199_254_740_995


class FakeVoiceRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def list_profiles(self, **filters):
        self.calls.append(filters)
        return (
            [
                VoiceProfileOut(
                    id=BIG_PROFILE_ID,
                    scope="TENANT",
                    voice="qwen-omni-vc-vc1-voice-test",
                    display_name="客服小林",
                    voice_type="自定义复刻",
                    gender="女声",
                    language="zh",
                    target_model=TARGET_MODEL,
                    status="ENABLED",
                    error_message=None,
                    can_preview=True,
                    can_delete=True,
                    created_at=NOW,
                    updated_at=NOW,
                )
            ],
            1,
        )


class FakeEnrollmentService:
    def __init__(self) -> None:
        self.create_dbs: list[object] = []
        self.create_calls: list[dict[str, object]] = []
        self.reenroll_dbs: list[object] = []
        self.reenroll_calls: list[dict[str, object]] = []

    async def create(self, db, **values):
        self.create_dbs.append(db)
        self.create_calls.append(values)
        return VoiceEnrollmentAcceptedOut(
            voice_profile_id=BIG_PROFILE_ID,
            enrollment_id=BIG_ENROLLMENT_ID,
            status="CREATING",
            display_name=values["request"].display_name,
        )

    async def reenroll(self, db, **values):
        self.reenroll_dbs.append(db)
        self.reenroll_calls.append(values)
        if values["profile_id"] == 9002:
            raise CustomException(
                msg="音色资产不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return VoiceEnrollmentAcceptedOut(
            voice_profile_id=values["profile_id"],
            enrollment_id=BIG_ENROLLMENT_ID,
            status="CREATING",
            display_name=values["request"].display_name,
        )


class FakeLifecycleService:
    async def get_enrollment(self, *, tenant_id: str, enrollment_id: int):
        if enrollment_id == 9002:
            raise CustomException(
                msg="复刻任务不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return {
            "id": str(enrollment_id),
            "voiceProfileId": str(BIG_PROFILE_ID),
            "status": "SUCCEEDED",
        }

    async def create_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        voice: str,
    ):
        if voice == "tenant-b-voice":
            raise CustomException(
                msg="音色资产不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return {"callId": "preview-a", "voice": voice}

    async def ready_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        call_id: str,
    ):
        return {"callId": call_id, "status": "READY"}

    async def close_preview_session(
        self,
        *,
        tenant_id: str,
        user_id: int,
        call_id: str,
    ):
        return {"callId": call_id, "status": "CLOSED"}

    async def deletion_check(self, *, tenant_id: str, profile_id: int):
        if profile_id == 9002:
            raise CustomException(
                msg="音色资产不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return {
            "voiceProfileId": str(profile_id),
            "deletable": True,
            "blockingTaskCount": 0,
            "historicalTaskCount": 0,
            "blockingTaskIds": [],
        }

    async def request_deletion(
        self,
        *,
        tenant_id: str,
        user_id: int,
        profile_id: int,
        idempotency_key: str,
    ):
        if profile_id == 9002:
            raise CustomException(
                msg="音色资产不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if profile_id == 9003:
            raise CustomException(
                msg="音色正在被外呼任务使用，暂不可删除",
                status_code=status.HTTP_409_CONFLICT,
                data={
                    "blockingTaskCount": 2,
                    "historicalTaskCount": 3,
                    "blockingTaskIds": ["101", "102"],
                },
            )
        return {
            "voiceProfileId": str(profile_id),
            "deletionId": str(BIG_ENROLLMENT_ID),
            "status": "DELETING",
        }


class FakeEnrollmentDbFactory:
    def __init__(self) -> None:
        self.sessions: list[object] = []

    def __call__(self):
        return self

    async def __aenter__(self):
        session = object()
        self.sessions.append(session)
        return session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeDefaultDeletionService:
    def __init__(self) -> None:
        self.check_calls: list[tuple[AsyncSession, dict[str, object]]] = []
        self.delete_calls: list[tuple[AsyncSession, dict[str, object]]] = []

    async def deletion_check(self, db: AsyncSession, **values):
        self.check_calls.append((db, values))
        return {
            "voiceProfileId": str(values["profile_id"]),
            "deletable": True,
            "blockingTaskCount": 0,
            "historicalTaskCount": 0,
            "blockingTaskIds": [],
        }

    async def request_deletion(self, db: AsyncSession, **values):
        self.delete_calls.append((db, values))
        return {
            "voiceProfileId": str(values["profile_id"]),
            "deletionId": str(BIG_ENROLLMENT_ID),
            "status": "DELETING",
        }


def _auth(*, permissions: frozenset[str]) -> AuthSchema:
    user = UserModel(
        user_id=7,
        tenant_id="tenant-a",
        user_name="tenant-user",
        nick_name="租户用户",
        user_type="sys_user",
    )
    return AuthSchema(
        db=AsyncSession(),
        user=user,
        check_data_scope=False,
        permissions=permissions,
    )


def _client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    permissions: frozenset[str],
) -> tuple[
    TestClient,
    FakeVoiceRepository,
    FakeEnrollmentService,
    FakeLifecycleService,
]:
    repository = FakeVoiceRepository()
    enrollment_service = FakeEnrollmentService()
    lifecycle_service = FakeLifecycleService()
    auth = _auth(permissions=permissions)

    app = FastAPI()
    handle_exception(app)
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_current_user] = lambda: auth
    app.dependency_overrides[get_voice_enrollment_service] = lambda: enrollment_service
    app.dependency_overrides[get_voice_lifecycle_service] = lambda: lifecycle_service
    monkeypatch.setattr(
        voice_controller,
        "VoiceRepository",
        lambda _db: repository,
    )
    return TestClient(app), repository, enrollment_service, lifecycle_service


def _enrollment_files() -> dict[str, tuple]:
    return {
        "file": ("voice.wav", b"fake-wav", "audio/wav"),
        "request": (
            None,
            ('{"displayName":"客服小林","gender":"女声","language":"zh","consentConfirmed":true}'),
            "application/json",
        ),
    }


def test_voice_routes_replace_old_direct_registration_entry() -> None:
    routes = [(route.path, frozenset(route.methods or set())) for route in AiCallRouter.routes]

    expected = {
        ("/ai-call/voice-profiles", "GET"),
        ("/ai-call/voice-enrollments", "POST"),
        ("/ai-call/tenant-voice-profiles/{id}/enrollments", "POST"),
        ("/ai-call/voice-enrollments/{id}", "GET"),
        ("/ai-call/voice-preview-audio", "POST"),
        ("/ai-call/voice-preview-sessions", "POST"),
        ("/ai-call/voice-preview-sessions/{callId}/ready", "POST"),
        ("/ai-call/voice-preview-sessions/{callId}", "DELETE"),
        ("/ai-call/tenant-voice-profiles/{id}/deletion-check", "GET"),
        ("/ai-call/tenant-voice-profiles/{id}", "DELETE"),
    }
    registered = {
        (path, method)
        for path, methods in routes
        for method in methods
        if (path, method) in expected
    }

    assert registered == expected
    assert routes.count(("/ai-call/voice-profiles", frozenset({"GET"}))) == 1
    assert (
        "/ai-call/voice-profiles",
        frozenset({"POST"}),
    ) not in routes


def test_create_voice_accepts_multipart_and_returns_202(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, _repository, service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    response = client.post(
        "/ai-call/voice-enrollments",
        headers={"Idempotency-Key": "key-1"},
        files=_enrollment_files(),
    )

    assert response.status_code == 202
    assert response.json()["data"] == {
        "voiceProfileId": str(BIG_PROFILE_ID),
        "enrollmentId": str(BIG_ENROLLMENT_ID),
        "status": "CREATING",
        "displayName": "客服小林",
    }
    assert service.create_calls[0]["tenant_id"] == "tenant-a"
    assert service.create_calls[0]["user_id"] == 7
    assert service.create_calls[0]["idempotency_key"] == "key-1"


def test_voice_enrollment_mutations_use_independent_db_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    db_factory = FakeEnrollmentDbFactory()
    monkeypatch.setattr(voice_controller, "async_db_session", db_factory)
    client, _repository, service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    create_response = client.post(
        "/ai-call/voice-enrollments",
        headers={"Idempotency-Key": "key-1"},
        files=_enrollment_files(),
    )
    reenroll_response = client.post(
        "/ai-call/tenant-voice-profiles/9001/enrollments",
        headers={"Idempotency-Key": "key-2"},
        files=_enrollment_files(),
    )

    assert create_response.status_code == 202
    assert reenroll_response.status_code == 202
    assert service.create_dbs == [db_factory.sessions[0]]
    assert service.reenroll_dbs == [db_factory.sessions[1]]


def test_disabled_voice_worker_keeps_read_available_but_create_returns_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, repository, _service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )
    client.app.dependency_overrides.pop(get_voice_enrollment_service)

    read_response = client.get("/ai-call/voice-profiles")
    create_response = client.post(
        "/ai-call/voice-enrollments",
        headers={"Idempotency-Key": "disabled-worker-key"},
        files=_enrollment_files(),
    )

    assert read_response.status_code == 200
    assert repository.calls
    assert create_response.status_code == 503
    assert "未启用" in create_response.json()["msg"]


def test_create_voice_requires_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, _repository, service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    response = client.post(
        "/ai-call/voice-enrollments",
        files=_enrollment_files(),
    )

    assert response.status_code in {400, 422}
    assert service.create_calls == []


def test_permissions_default_deny_management_but_allow_available_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, repository, service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset(),
    )

    management = client.get("/ai-call/voice-profiles")
    write = client.post(
        "/ai-call/voice-enrollments",
        headers={"Idempotency-Key": "key-1"},
        files=_enrollment_files(),
    )
    available = client.get(
        "/ai-call/voice-profiles",
        params={"availableOnly": "true"},
    )

    assert management.status_code == 403
    assert write.status_code == 403
    assert available.status_code == 200
    assert repository.calls[-1]["available_only"] is True
    assert service.create_calls == []


def test_list_accepts_camel_case_query_and_returns_string_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, repository, _service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    response = client.get(
        "/ai-call/voice-profiles",
        params={
            "voiceType": "自定义复刻",
            "gender": "女声",
            "targetModel": TARGET_MODEL,
            "status": "CREATE_FAILED",
            "includeDeleted": "true",
            "pageNum": 2,
            "pageSize": 30,
        },
    )

    assert response.status_code == 200
    assert response.json()["rows"][0]["id"] == str(BIG_PROFILE_ID)
    assert repository.calls == [
        {
            "tenant_id": "tenant-a",
            "target_model": TARGET_MODEL,
            "voice_type": "自定义复刻",
            "gender": "女声",
            "status": "CREATE_FAILED",
            "available_only": False,
            "include_deleted": True,
            "page_num": 2,
            "page_size": 30,
        }
    ]


def test_cross_tenant_resource_operations_do_not_expose_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, _repository, _service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    query = client.get("/ai-call/voice-enrollments/9002")
    reenroll = client.post(
        "/ai-call/tenant-voice-profiles/9002/enrollments",
        headers={"Idempotency-Key": "key-2"},
        files=_enrollment_files(),
    )
    preview = client.post(
        "/ai-call/voice-preview-sessions",
        json={"voice": "tenant-b-voice"},
    )
    deletion_check = client.get("/ai-call/tenant-voice-profiles/9002/deletion-check")
    delete = client.delete(
        "/ai-call/tenant-voice-profiles/9002",
        headers={"Idempotency-Key": "delete-key-1"},
    )

    assert query.status_code in {403, 404}
    assert reenroll.status_code in {403, 404}
    assert preview.status_code in {403, 404}
    assert deletion_check.status_code in {403, 404}
    assert delete.status_code in {403, 404}


def test_delete_conflict_returns_structured_task_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    client, _repository, _service, _lifecycle = _client(
        monkeypatch,
        permissions=frozenset({"ai_call:voice:manage"}),
    )

    response = client.delete(
        "/ai-call/tenant-voice-profiles/9003",
        headers={"Idempotency-Key": "delete-key-blocked"},
    )

    assert response.status_code == 409
    assert response.json()["data"] == {
        "blockingTaskCount": 2,
        "historicalTaskCount": 3,
        "blockingTaskIds": ["101", "102"],
    }


def test_default_deletion_routes_delegate_to_real_service_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "JWT_ENABLE", True)
    deletion_service = FakeDefaultDeletionService()
    monkeypatch.setattr(
        voice_controller,
        "VoiceDeletionService",
        lambda *, session_factory: deletion_service,
    )
    auth = _auth(permissions=frozenset({"ai_call:voice:manage"}))
    app = FastAPI()
    handle_exception(app)
    app.include_router(AiCallRouter)
    app.dependency_overrides[get_current_user] = lambda: auth
    client = TestClient(app)

    check = client.get(
        f"/ai-call/tenant-voice-profiles/{BIG_PROFILE_ID}/deletion-check",
    )
    delete = client.delete(
        f"/ai-call/tenant-voice-profiles/{BIG_PROFILE_ID}",
        headers={"Idempotency-Key": "delete-key-default"},
    )

    assert check.status_code == 200
    assert delete.status_code == 202
    assert deletion_service.check_calls == [
        (
            auth.db,
            {
                "tenant_id": "tenant-a",
                "profile_id": BIG_PROFILE_ID,
            },
        )
    ]
    assert deletion_service.delete_calls == [
        (
            auth.db,
            {
                "tenant_id": "tenant-a",
                "user_id": 7,
                "profile_id": BIG_PROFILE_ID,
                "idempotency_key": "delete-key-default",
            },
        )
    ]
