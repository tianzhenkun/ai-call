"""账号隔离回归：真实路由与临时数据库，对象存储和模型只使用测试替身。"""

import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_ai_call_knowledge import _FakeCosClient, _upload

from app.api.v1.ai_call.knowledge_controller import (
    KnowledgePromptRouter,
    KnowledgeRouter,
    get_knowledge_db,
    get_knowledge_product_info_service,
    get_knowledge_service,
)
from app.api.v1.ai_call.model import (
    AiCallKnowledgeChunkModel,
    AiCallKnowledgeItemModel,
    AiCallKnowledgeUsageModel,
    AiCallKnowledgeVersionModel,
    AiCallPromptKnowledgeBindingModel,
    AiCallPromptProfileModel,
)
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundTaskModel
from app.api.v1.ai_call.outbound.rule_task_service import OutboundRuleTaskService
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException, handle_exception
from app.services.ai_call.knowledge import (
    CosKnowledgeStore,
    KnowledgeProductInfoService,
    KnowledgeRealtimeSearchService,
    KnowledgeSearchService,
    KnowledgeService,
    knowledge_version_snapshot_hash,
)
from app.services.ai_call.session_registry import KnowledgeRuntimeContext

NOW = datetime(2026, 10, 24, tzinfo=timezone.utc)


class Extractor:
    model_name = "isolated-test-model"

    def __init__(self):
        self.inputs = []

    async def extract(self, payload):
        self.inputs.append(payload)
        chunk = payload["chunks"][0]
        return {
            "draftText": chunk["content"],
            "sources": [{"claim": chunk["content"], "chunkId": chunk["chunkId"]}],
            "conflicts": [],
        }


@pytest.fixture
async def knowledge_scope():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    models = (
        AiCallPromptProfileModel, AiCallKnowledgeItemModel,
        AiCallKnowledgeVersionModel, AiCallKnowledgeChunkModel,
        AiCallPromptKnowledgeBindingModel, AiCallKnowledgeUsageModel,
        AiCallOutboundTaskModel,
    )
    async with engine.begin() as connection:
        for model in models:
            await connection.run_sync(model.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    objects = _FakeCosClient()
    service = KnowledgeService(CosKnowledgeStore(client=objects, bucket="test"))
    extractor = Extractor()
    async with sessions() as db:
        for profile_id, tenant in ((101, "tenant-a"), (201, "tenant-b")):
            db.add(AiCallPromptProfileModel(
                id=profile_id, tenant_id=tenant, scene_code="intro", name="共享场景",
                provider_key="static_profile", product_info="", variables_json="[]",
                created_at=NOW, updated_at=NOW,
            ))
        for item_id, tenant, owner in (
            (1, "tenant-a", 7), (2, "tenant-a", 8),
            (3, "tenant-b", 7), (4, "tenant-a", None),
        ):
            version_id = item_id * 10 + 1
            text = f"账号 {owner} 的私有资料 {item_id}"
            payload = text.encode()
            key = f"test/{item_id}.md"
            objects.objects[service.store.physical_key(key)] = (payload, "text/markdown")
            db.add_all([
                AiCallKnowledgeItemModel(
                    id=item_id, tenant_id=tenant, display_name=f"{item_id}.md",
                    content_category="FAQ", current_ready_version_id=version_id,
                    created_by=owner, created_at=NOW, updated_at=NOW,
                ),
                AiCallKnowledgeVersionModel(
                    id=version_id, tenant_id=tenant, knowledge_item_id=item_id,
                    version_no=1, status="READY", source_object_key=key,
                    source_filename=f"{item_id}.md", extension="md",
                    mime_type="text/markdown", byte_size=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(), chunk_count=1,
                    chunk_set_sha256="a" * 64, created_by=owner,
                    created_at=NOW, ready_at=NOW,
                ),
                AiCallKnowledgeChunkModel(
                    id=version_id * 10, tenant_id=tenant, knowledge_version_id=version_id,
                    chunk_index=0, content=text, content_checksum="b" * 64,
                    content_type="TEXT", source_type="MARKDOWN", created_at=NOW,
                ),
                AiCallPromptKnowledgeBindingModel(
                    id=item_id, tenant_id=tenant,
                    prompt_profile_id=101 if tenant == "tenant-a" else 201,
                    knowledge_item_id=item_id, created_by=owner, created_at=NOW,
                ),
            ])
        db.add(AiCallOutboundTaskModel(
            id=701, tenant_id="tenant-a", validation_id=1, idempotency_key="task-701",
            request_fingerprint="a" * 64, task_name="隔离验证", task_mode="ai",
            status="COMPLETED", execution_mode="immediate", prompt_name="共享场景",
            scene_code="intro", voice="test", rule_id=1, rule_name="test",
            rule_summary="test", config_snapshot_json="{}", created_by=7,
            created_at=NOW, updated_at=NOW,
        ))
        await db.commit()
        yield SimpleNamespace(
            db=db, sessions=sessions, service=service, objects=objects,
            extractor=extractor, product=KnowledgeProductInfoService(extractor),
        )
    await engine.dispose()


def app_for(scope, user_id, *, superuser=False):
    app = FastAPI()
    app.include_router(KnowledgeRouter)
    app.include_router(KnowledgePromptRouter)
    handle_exception(app)
    auth = SimpleNamespace(
        db=scope.db, permissions={"*:*:*", "ai_call:agent:manage"},
        user=SimpleNamespace(tenant_id="tenant-a", user_id=user_id, is_superuser=superuser),
    )
    app.dependency_overrides[get_current_user] = lambda: auth
    app.dependency_overrides[get_knowledge_db] = lambda: scope.db
    app.dependency_overrides[get_knowledge_service] = lambda: scope.service
    app.dependency_overrides[get_knowledge_product_info_service] = lambda: scope.product
    return app


@pytest.mark.anyio
@pytest.mark.parametrize("user_id,item_id", [(7, 1), (8, 2)])
@pytest.mark.parametrize("superuser", [False, True])
async def test_routes_keep_knowledge_private_even_with_management_permission(
    knowledge_scope, user_id, item_id, superuser,
):
    scope = knowledge_scope
    async with AsyncClient(
        transport=ASGITransport(app_for(scope, user_id, superuser=superuser)),
        base_url="http://test",
    ) as client:
        response = await client.get("/knowledge/items", params={"userId": 15 - user_id})
        assert response.status_code == 200
        assert response.json()["total"] == 1
        assert [row["id"] for row in response.json()["rows"]] == [str(item_id)]
        for suffix in ("", "/versions"):
            response = await client.get(f"/knowledge/items/{item_id}{suffix}")
            assert response.status_code == 200
        for suffix in ("processing", "download", "preview"):
            response = await client.get(f"/knowledge/versions/{item_id * 10 + 1}/{suffix}")
            assert response.status_code == 200
        extracted = await client.post("/prompt-profiles/101/product-info:extract")
        assert extracted.status_code == 200
        assert extracted.json()["data"]["sourceVersionIds"] == [str(item_id * 10 + 1)]
        assert len(scope.extractor.inputs) == 1
        assert [chunk["versionId"] for chunk in scope.extractor.inputs[0]["chunks"]] == [
            str(item_id * 10 + 1)
        ]


@pytest.mark.anyio
@pytest.mark.parametrize("foreign_item", [2, 3, 4])
async def test_foreign_and_unowned_ids_fail_before_reading_files_or_changing_data(
    knowledge_scope, foreign_item,
):
    scope = knowledge_scope
    version_id = foreign_item * 10 + 1
    payload = b"replacement"
    async with AsyncClient(
        transport=ASGITransport(app_for(scope, 7)), base_url="http://test",
    ) as client:
        requests = [
            ("GET", f"/knowledge/items/{foreign_item}", {}),
            ("GET", f"/knowledge/items/{foreign_item}/versions", {}),
            ("PATCH", f"/knowledge/items/{foreign_item}", {"json": {"note": "changed"}}),
            ("PUT", f"/knowledge/items/{foreign_item}/scene-bindings", {
                "json": {"promptProfileIds": [101]},
            }),
            ("DELETE", f"/knowledge/items/{foreign_item}", {}),
            ("POST", f"/knowledge/items/{foreign_item}/versions/upload", {
                "headers": {"Idempotency-Key": "foreign-version"},
                "files": {"file": ("replacement.md", payload, "text/markdown")},
                "data": {"fileSha256": hashlib.sha256(payload).hexdigest(), "contentCategory": "FAQ"},
            }),
        ]
        requests.extend(
            ("GET", f"/knowledge/versions/{version_id}/{suffix}", {})
            for suffix in ("processing", "download", "preview")
        )
        requests.append(("POST", f"/knowledge/versions/{version_id}/retry", {}))
        for method, path, kwargs in requests:
            response = await client.request(method, path, **kwargs)
            assert response.status_code == 404, (method, path, response.text)
    await scope.db.refresh(await scope.db.get(AiCallKnowledgeItemModel, foreign_item))
    item = await scope.db.get(AiCallKnowledgeItemModel, foreign_item)
    assert item.note is None and item.deleted_at is None
    assert len(scope.objects.objects) == 4
    assert len((await scope.db.scalars(select(AiCallKnowledgeVersionModel))).all()) == 4


@pytest.mark.anyio
async def test_upload_replay_is_per_account_and_keeps_legacy_own_keys(knowledge_scope):
    scope = knowledge_scope
    payload = b"private upload"

    async def upload(user_id):
        return await scope.service.accept_upload(
            scope.db, tenant_id="tenant-a", user_id=user_id, idempotency_key="same-key",
            file=_upload(payload), file_sha256=hashlib.sha256(payload).hexdigest(),
            content_category="FAQ", note=None,
        )

    own = await upload(7)
    legacy = await scope.db.get(AiCallKnowledgeVersionModel, own.version_id)
    legacy.upload_idempotency_key = "same-key"
    await scope.db.commit()
    replay = await upload(7)
    other = await upload(8)
    other_replay = await upload(8)
    assert replay.replayed and replay.version_id == own.version_id
    assert other.version_id != own.version_id and other.item_id != own.item_id
    assert other_replay.replayed and other_replay.version_id == other.version_id


@pytest.mark.anyio
async def test_task_freeze_and_runtime_refuse_foreign_or_unowned_history(knowledge_scope, monkeypatch):
    scope = knowledge_scope
    for user_id, version_ids in ((7, ["11"]), (8, ["21"]), (9, [])):
        frozen = await OutboundRuleTaskService._freeze_knowledge(
            scope.db, tenant_id="tenant-a", user_id=user_id,
            prompt_profile_id=101, frozen_at=NOW,
        )
        assert frozen["versionIds"] == version_ids

    calls = []

    async def search(_db, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(KnowledgeSearchService, "search", search)
    service = KnowledgeRealtimeSearchService(scope.sessions)
    for version_ids in ((11,), (21,), (31,), (41,), (11, 21), (11, 41)):
        versions = [await scope.db.get(AiCallKnowledgeVersionModel, value) for value in version_ids]
        context = KnowledgeRuntimeContext(
            tenant_id="tenant-a", task_id=701, prompt_profile_id=101,
            version_ids=version_ids, version_snapshot_hash=knowledge_version_snapshot_hash(versions),
            retriever_version="postgres-ngram-tsvector-v1",
        )
        if version_ids == (11,):
            assert await service._search(context, "资料") == []
        else:
            with pytest.raises(ValueError, match="冻结知识快照"):
                await service._search(context, "资料")
    assert len(calls) == 1 and calls[0]["user_id"] == 7

    own_item = await scope.db.get(AiCallKnowledgeItemModel, 1)
    own_item.deleted_at = NOW
    await scope.db.commit()
    own_version = await scope.db.get(AiCallKnowledgeVersionModel, 11)
    context = KnowledgeRuntimeContext(
        tenant_id="tenant-a", task_id=701, prompt_profile_id=101, version_ids=(11,),
        version_snapshot_hash=knowledge_version_snapshot_hash([own_version]),
        retriever_version="postgres-ngram-tsvector-v1",
    )
    assert await service._search(context, "历史资料") == []
    frozen = await OutboundRuleTaskService._freeze_knowledge(
        scope.db, tenant_id="tenant-a", user_id=7, prompt_profile_id=101, frozen_at=NOW,
    )
    assert frozen["versionIds"] == []


@pytest.mark.anyio
async def test_missing_account_and_empty_account_fail_closed(knowledge_scope):
    scope = knowledge_scope
    for user_id in (None, 0):
        with pytest.raises(CustomException) as error:
            await scope.service.list_items(
                scope.db, tenant_id="tenant-a", user_id=user_id, page_num=1, page_size=20,
            )
        assert error.value.status_code == 401
    with pytest.raises(CustomException) as error:
        await scope.product.extract(scope.db, tenant_id="tenant-a", user_id=9, prompt_profile_id=101)
    assert error.value.status_code == 409
    assert scope.extractor.inputs == []
