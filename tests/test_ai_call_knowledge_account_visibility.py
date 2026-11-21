"""账号隔离回归：真实路由与临时数据库，对象存储和模型只使用测试替身。"""

import hashlib
import json
from datetime import UTC, datetime
from tempfile import TemporaryFile
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, UploadFile
from httpx import ASGITransport, AsyncClient
from lingchen_knowledge.content import content_chars
from lingchen_knowledge.formats import policy as public_upload_policy
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_ai_call_knowledge import (
    _UNNAMED_PRODUCT,
    _AcceptProductReview,
    _FakeCosClient,
    _product_claim,
    _upload,
)

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
    KnowledgeWorker,
    _validate_upload,
    knowledge_version_snapshot_hash,
)
from app.services.ai_call.session_registry import KnowledgeRuntimeContext

NOW = datetime(2026, 10, 24, tzinfo=UTC)


class Extractor(_AcceptProductReview):
    model_name = "isolated-test-model"

    def __init__(self):
        self.inputs = []

    async def extract(self, payload):
        self.inputs.append(payload)
        chunk = payload["chunks"][0]
        return {
            "scope": _UNNAMED_PRODUCT,
            "sections": {"核心能力": [_product_claim(chunk)]},
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


def app_for(scope, user_id, *, superuser=False, permissions=None):
    app = FastAPI()
    app.include_router(KnowledgeRouter)
    app.include_router(KnowledgePromptRouter)
    handle_exception(app)
    auth = SimpleNamespace(
        db=scope.db, permissions={"*:*:*", "ai_call:agent:manage"} if permissions is None else permissions,
        user=SimpleNamespace(tenant_id="tenant-a", user_id=user_id, is_superuser=superuser),
    )
    app.dependency_overrides[get_current_user] = lambda: auth
    app.dependency_overrides[get_knowledge_db] = lambda: scope.db
    app.dependency_overrides[get_knowledge_service] = lambda: scope.service
    app.dependency_overrides[get_knowledge_product_info_service] = lambda: scope.product
    return app


@pytest.mark.parametrize(("filename", "header"), [
    ("guide.pdf", b"%PDF-1.7"), ("image.png", b"\x89PNG\r\n\x1a\n"),
    ("voice.wav", b"RIFF0000WAVE"), ("video.mp4", b"\x00\x00\x00\x18ftypisom"),
])
def test_upload_validates_actual_size_at_public_limit(filename, header):
    fmt = next(f for f in public_upload_policy()["formats"] if filename.endswith(f["suffix"]))
    with TemporaryFile() as stream:
        stream.write(header)
        stream.truncate(fmt["maxBytes"])
        upload = UploadFile(stream, filename=filename)
        assert _validate_upload(upload, "0" * 64, "FAQ", None).byte_size == fmt["maxBytes"]
        stream.truncate(fmt["maxBytes"] + 1)
        with pytest.raises(CustomException) as too_large:
            _validate_upload(upload, "0" * 64, "FAQ", None)
        assert too_large.value.status_code == 413
        assert stream.tell() == 0


@pytest.mark.anyio
@pytest.mark.parametrize("legacy_extension", ["pptx", "pdf"])
async def test_public_upload_policy_excludes_ineligible_history_and_rejects_pptx_uploads(
    knowledge_scope, legacy_extension,
):
    scope = knowledge_scope
    item = await scope.db.get(AiCallKnowledgeItemModel, 1)
    item.display_name = "曾经关联的产品资料"
    old = await scope.db.get(AiCallKnowledgeVersionModel, 11)
    old.extension = legacy_extension
    old.source_filename = f"产品介绍.{legacy_extension}"
    if legacy_extension == "pdf":
        old.byte_size = next(f["maxBytes"] for f in public_upload_policy()["formats"] if f["suffix"] == ".pdf") + 1
    await scope.db.commit()
    scenes = "/prompt-profiles/101/knowledge-items"
    async with AsyncClient(transport=ASGITransport(app_for(scope, 7)), base_url="http://test") as client:
        assert (await client.get("/knowledge/upload-policy")).json()["data"] == public_upload_policy()
        assert (await client.get("/knowledge/items")).json()["total"] == 0
        for suffix in ("", "/versions"):
            assert (await client.get(f"/knowledge/items/1{suffix}")).status_code == 404
        for action in ("preview", "download", "processing"):
            assert (await client.get(f"/knowledge/versions/11/{action}")).status_code == 404
        assert (await client.get(scenes)).json()["data"] == []
        assert (await client.put(scenes, json={"itemIds": ["1"], "expectedItemIds": []})).status_code == 409
        assert (await client.post("/prompt-profiles/101/product-info:extract")).status_code == 409
        for path in ("/knowledge/items/upload", "/knowledge/items/1/versions/upload"):
            data = b"PK\x03\x04old-presentation"
            response = await client.post(
                path, headers={"Idempotency-Key": "pptx"},
                files={"file": ("资料.PPTX", data, "application/octet-stream")},
                data={"contentCategory": "FAQ", "fileSha256": hashlib.sha256(data).hexdigest()},
            )
            assert response.status_code == 400
        frozen = await OutboundRuleTaskService._freeze_knowledge(
            scope.db, tenant_id="tenant-a", user_id=7, prompt_profile_id=101, frozen_at=NOW,
        )
        assert frozen["versionIds"] == []
        assert scope.extractor.inputs == []


@pytest.mark.anyio
async def test_media_filter_uses_latest_public_version_and_preserves_notes(knowledge_scope):
    scope = knowledge_scope
    item = await scope.db.get(AiCallKnowledgeItemModel, 1)
    item.note = "保存的备注"
    await scope.db.commit()
    async with AsyncClient(transport=ASGITransport(app_for(scope, 7)), base_url="http://test") as client:
        assert (await client.get("/knowledge/items", params={"carrierType": "image"})).json()["rows"] == []
        data = b"\x89PNG\r\n\x1a\n" + b"sample"
        result = await client.post(
            "/knowledge/items/1/versions/upload", headers={"Idempotency-Key": "image-update"},
            files={"file": ("说明.png", data, "image/png")},
            data={"contentCategory": "FAQ", "fileSha256": hashlib.sha256(data).hexdigest()},
        )
        assert result.status_code == 202, result.text
        page = (await client.get("/knowledge/items", params={"carrierType": "image"})).json()
        assert page["total"] == 1
        assert page["rows"][0]["carrierType"] == "image"
        assert page["rows"][0]["note"] == "保存的备注"
        conflict = await client.post(
            "/knowledge/items/1/versions/upload", headers={"Idempotency-Key": "image-update"},
            files={"file": ("说明.png", data, "image/png")},
            data={"contentCategory": "FAQ", "fileSha256": hashlib.sha256(data).hexdigest(), "note": ""},
        )
        assert conflict.status_code == 409
        assert page["rows"][0]["currentReadyVersionId"] == "11"
        assert (await client.get("/knowledge/items", params={"carrierType": "document"})).json()["total"] == 0
        # 有支持格式的新版本在处理时，旧 PPTX 也不能作为就绪版本参与选取或提取。
        old = await scope.db.get(AiCallKnowledgeVersionModel, 11)
        old.extension = "pptx"
        await scope.db.commit()
        assert (await client.get("/knowledge/items/1")).json()["data"]["currentReadyVersionId"] is None
        assert (await client.get("/prompt-profiles/101/knowledge-items")).json()["data"] == []
        assert (await client.post("/prompt-profiles/101/product-info:extract")).status_code == 409
        result = await client.post(
            "/knowledge/items/1/versions/upload", headers={"Idempotency-Key": "clear-note"},
            files={"file": ("说明.png", data, "image/png")},
            data={"contentCategory": "FAQ", "fileSha256": hashlib.sha256(data).hexdigest(), "note": ""},
        )
        assert result.status_code == 202
        assert (await client.get("/knowledge/items/1")).json()["data"]["note"] is None


@pytest.mark.anyio
async def test_scene_picker_reads_legacy_links_and_extracts_only_the_saved_selection(knowledge_scope):
    scope = knowledge_scope
    path = "/prompt-profiles/101/knowledge-items"
    extract = "/prompt-profiles/101/product-info:extract"
    scope.db.add(AiCallPromptProfileModel(
        id=102, tenant_id="tenant-a", scene_code="other", name="其他场景",
        provider_key="static_profile", product_info="", variables_json="[]",
        created_at=NOW, updated_at=NOW,
    ))
    await scope.db.commit()
    await scope.service.replace_scene_bindings(
        scope.db, tenant_id="tenant-a", user_id=7, item_id=1, prompt_profile_ids=[101, 102],
    )
    payload = "新产品支持按月结算".encode()
    upload = await scope.service.accept_upload(
        scope.db, tenant_id="tenant-a", user_id=7, idempotency_key="scene-picker",
        file=_upload(payload), file_sha256=hashlib.sha256(payload).hexdigest(),
        content_category="FAQ", note=None,
    )
    selected = str(upload.item_id)
    async with AsyncClient(transport=ASGITransport(app_for(scope, 7)), base_url="http://test") as client:
        original = [{"id": "1", "displayName": "1.md"}]
        assert (await client.get(path)).json()["data"] == original
        assert (await client.get("/prompt-profiles/201/knowledge-items")).status_code == 404
        for invalid_id in ("2", "3", "4", selected):
            rejected = await client.put(path, json={"expectedItemIds": ["1"], "itemIds": [invalid_id]})
            assert rejected.status_code == 409
            assert (await client.get(path)).json()["data"] == original
        worker = KnowledgeWorker(scope.sessions, scope.service.store, worker_id="picker-test")
        assert await worker.run_once()
        body = {"expectedItemIds": ["1"], "itemIds": [selected]}
        saved = await client.put(path, json=body)
        assert saved.status_code == 200, saved.text
        assert saved.json()["data"] == [{"id": selected, "displayName": "faq.md"}]
        assert (await client.put(path, json=body)).json()["data"] == saved.json()["data"]
        assert (await client.get(path)).json()["data"] == saved.json()["data"]
        assert (await client.get("/prompt-profiles/102/knowledge-items")).json()["data"] == original
        assert (await client.put(path, json={"expectedItemIds": ["1"], "itemIds": []})).status_code == 409
        draft = (await client.post(extract)).json()["data"]
        assert draft["sourceVersionIds"] == [str(upload.version_id)]
        assert "按月结算" in draft["draftText"]
        assert (await client.put(path, json={"expectedItemIds": [selected], "itemIds": []})).json()["data"] == []
        assert (await client.post(extract)).status_code == 409
    async with AsyncClient(transport=ASGITransport(app_for(scope, 8)), base_url="http://test") as other:
        assert (await other.get(path)).json()["data"] == [{"id": "2", "displayName": "2.md"}]


@pytest.mark.anyio
async def test_existing_unready_binding_remains_visible_and_removable(knowledge_scope):
    item = await knowledge_scope.db.get(AiCallKnowledgeItemModel, 1)
    item.current_ready_version_id = None
    await knowledge_scope.db.commit()
    path = "/prompt-profiles/101/knowledge-items"
    async with AsyncClient(transport=ASGITransport(app_for(knowledge_scope, 7)), base_url="http://test") as client:
        original = (await client.get(path)).json()["data"]
        assert original == [{"id": "1", "displayName": "1.md"}]
        assert (await client.put(path, json={"expectedItemIds": ["1"], "itemIds": ["1"]})).json()["data"] == original
        assert (await client.put(path, json={"expectedItemIds": ["1"], "itemIds": []})).json()["data"] == []
        assert (await client.put(path, json={"expectedItemIds": [], "itemIds": ["1"]})).status_code == 409


@pytest.mark.anyio
async def test_item_metrics_use_visible_version_and_only_its_actual_citations(knowledge_scope):
    db = knowledge_scope.db
    version = await db.get(AiCallKnowledgeVersionModel, 11)
    version.parser_version = "txt-markdown-utf8-v1"
    version.chunk_count = 2
    chunk = await db.scalar(
        select(AiCallKnowledgeChunkModel).where(
            AiCallKnowledgeChunkModel.knowledge_version_id == 11
        )
    )
    chunk.section_path, chunk.content = "同一节", "同一节\n真实 A\u00a0文"
    db.add(
        AiCallKnowledgeChunkModel(
            id=502,
            tenant_id="tenant-a",
            knowledge_version_id=11,
            chunk_index=1,
            content="同一节\n又\u3000一段",
            content_checksum="c" * 64,
            source_type="MARKDOWN",
            section_path="同一节",
            content_type="TEXT",
            created_at=NOW,
        )
    )
    for audit_id, (tenant, purpose, status, evidence) in enumerate(
        (
            (
                "tenant-a",
                "PRODUCT_SUMMARY",
                "OK",
                {"sources": [{"versionId": "11"}, {"versionId": "11"}]},
            ),
            ("tenant-a", "REALTIME_ANSWER", "OK", [{"versionId": 11}, {"versionId": "11"}]),
            ("tenant-a", "REALTIME_ANSWER", "OK", [{"versionId": "21"}]),
            ("tenant-b", "REALTIME_ANSWER", "OK", [{"versionId": "11"}]),
            ("tenant-a", "PRODUCT_SUMMARY", "FAILED", {"sources": [{"versionId": "11"}]}),
            (
                "tenant-a",
                "PRODUCT_SUMMARY",
                "OK",
                {"sourceDocuments": [{"versionId": "11"}], "sources": []},
            ),
            ("tenant-a", "REALTIME_ANSWER", "NO_HIT", []),
        ),
        start=1000,
    ):
        db.add(
            AiCallKnowledgeUsageModel(
                id=audit_id,
                tenant_id=tenant,
                prompt_profile_id=101,
                purpose=purpose,
                status=status,
                knowledge_version_ids='["11","21"]',
                version_snapshot_hash="a" * 64,
                retriever_version="test",
                evidence_json=json.dumps(evidence),
                created_at=NOW,
            )
        )
    await db.commit()
    async with AsyncClient(
        transport=ASGITransport(app_for(knowledge_scope, 7)), base_url="http://test"
    ) as client:

        async def assert_metrics(chars, calls):
            page = (await client.get("/knowledge/items")).json()["rows"]
            detail = (await client.get("/knowledge/items/1")).json()["data"]
            assert [(row["contentCharCount"], row["callCount"]) for row in (page[0], detail)] == [
                (chars, calls)
            ] * 2

        await assert_metrics(content_chars("真实 A 文又一段"), 2)
        next_version = AiCallKnowledgeVersionModel(
            id=12,
            tenant_id="tenant-a",
            knowledge_item_id=1,
            version_no=2,
            status="PROCESSING",
            source_object_key="new",
            source_filename="版本二.md",
            extension="md",
            mime_type="text/markdown",
            byte_size=10,
            sha256="d" * 64,
            created_by=7,
            created_at=NOW,
        )
        db.add(next_version)
        await db.commit()
        await assert_metrics(None, 2)
        next_version.status = "READY"
        next_version.ready_at, next_version.chunk_count, next_version.chunk_set_sha256 = (
            NOW,
            1,
            "d" * 64,
        )
        db.add(
            AiCallKnowledgeChunkModel(
                id=503,
                tenant_id="tenant-a",
                knowledge_version_id=12,
                chunk_index=0,
                content="原文标题\n新 文！",
                content_checksum="e" * 64,
                source_type="MD",
                content_type="TEXT",
                created_at=NOW,
            )
        )
        audit = await db.get(AiCallKnowledgeUsageModel, 1000)
        audit.evidence_json = json.dumps({"sources": [{"versionId": "11"}, {"versionId": "12"}]})
        await db.commit()
        await assert_metrics(content_chars("原文标题新 文！"), 2)
    async with AsyncClient(
        transport=ASGITransport(app_for(knowledge_scope, 8)), base_url="http://test"
    ) as client:
        rows = (await client.get("/knowledge/items")).json()["rows"]
        assert [(row["id"], row["callCount"]) for row in rows] == [("2", 1)]
        assert (await client.get("/knowledge/items/1")).status_code == 404


@pytest.mark.anyio
async def test_viewer_edits_only_own_notes_and_category(knowledge_scope):
    transport = ASGITransport(app_for(knowledge_scope, 7, permissions={"ai_call:knowledge:view"}))
    path = "/knowledge/items/1"
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.patch(path, json={"note": "回访前阅读"})).status_code == 200
        assert (await client.get("/knowledge/items")).json()["rows"][0]["note"] == "回访前阅读"
        assert (await client.patch(path, json={"contentCategory": "OTHER"})).status_code == 200
        assert (await client.patch(path, json={"note": "一起改", "contentCategory": "INDUSTRY"})).status_code == 200
        assert (await client.patch(path, json={"note": "改名时的备注", "displayName": "另一个名字"})).status_code == 403
        item = (await client.get(path)).json()["data"]
        assert (item["note"], item["contentCategory"], item["displayName"]) == ("一起改", "INDUSTRY", "1.md")
        for foreign in (2, 3, 4):
            assert (await client.patch(f"/knowledge/items/{foreign}", json={"note": "其他资料", "contentCategory": "OTHER"})).status_code == 404
        assert (await client.patch(path, json={"note": ""})).status_code == 200
        assert (await client.get(path)).json()["data"]["note"] is None

    transport = ASGITransport(app_for(knowledge_scope, 7, permissions={"ai_call:knowledge:manage"}))
    async with AsyncClient(transport=transport, base_url="http://test") as manager:
        response = await manager.patch(path, json={"note": "已核对", "contentCategory": "OTHER"})
        assert response.status_code == 200
        assert response.json()["data"]["contentCategory"] == "OTHER"


@pytest.mark.anyio
async def test_viewer_retries_only_own_recoverable_failed_versions(knowledge_scope):
    for version_id in (11, 21, 31, 41):
        version = await knowledge_scope.db.get(AiCallKnowledgeVersionModel, version_id)
        version.status, version.failure_retryable = "FAILED", True
    await knowledge_scope.db.commit()

    transport = ASGITransport(app_for(knowledge_scope, 7, permissions=set()))
    async with AsyncClient(transport=transport, base_url="http://test") as outsider:
        assert (await outsider.patch("/knowledge/items/1", json={"note": "备注"})).status_code == 403
        assert (await outsider.post("/knowledge/versions/11/retry")).status_code == 403

    path = "/knowledge/versions/11/retry"
    transport = ASGITransport(app_for(knowledge_scope, 7, permissions={"ai_call:knowledge:view"}))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        for foreign in (21, 31, 41):
            assert (await client.post(f"/knowledge/versions/{foreign}/retry")).status_code == 404
        response = await client.post(path)
        assert response.status_code == 200
        assert response.json()["data"]["status"] == "PROCESSING"
        assert (await client.post(path)).status_code == 409
        version = await knowledge_scope.db.get(AiCallKnowledgeVersionModel, 11)
        for status, retryable in (("FAILED", False), ("READY", True)):
            version.status, version.failure_retryable = status, retryable
            await knowledge_scope.db.commit()
            assert (await client.post(path)).status_code == 409


@pytest.mark.anyio
@pytest.mark.parametrize("permissions,read_status", [
    ({"ai_call:knowledge:manage"}, 403),
    ({"ai_call:prompt:manage"}, 403),
    ({"ai_call:prompt:manage", "ai_call:knowledge:view"}, 200),
])
async def test_scene_knowledge_requires_both_permissions(knowledge_scope, permissions, read_status):
    app = app_for(knowledge_scope, 7, permissions=permissions)
    path = "/prompt-profiles/101/knowledge-items"
    async with AsyncClient(transport=ASGITransport(app), base_url="http://test") as client:
        assert (await client.get(path)).status_code == read_status
        assert (await client.put(path, json={"itemIds": [], "expectedItemIds": ["1"]})).status_code == 403


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
