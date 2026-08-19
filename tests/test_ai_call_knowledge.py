from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from multiprocessing import get_context
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest
from fastapi import UploadFile
from sqlalchemy import UniqueConstraint, inspect, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.datastructures import Headers

from app.api.v1.ai_call import AiCallRouter
from app.api.v1.ai_call.knowledge_controller import (
    get_knowledge_db,
    preview_knowledge_version_controller,
)
from app.api.v1.ai_call.model import (
    AiCallKnowledgeChunkModel,
    AiCallKnowledgeItemModel,
    AiCallKnowledgeUsageModel,
    AiCallKnowledgeVersionModel,
    AiCallPromptKnowledgeBindingModel,
    AiCallPromptProfileModel,
)
from app.core.dependencies import get_knowledge_manager, get_knowledge_viewer
from app.core.exceptions import CustomException
from app.services.ai_call.knowledge import (
    CosKnowledgeStore,
    KnowledgeDownload,
    KnowledgeService,
    KnowledgeTextParseError,
    KnowledgeWorker,
    parse_byte_range,
    parse_text_knowledge,
)
from app.services.ai_call.knowledge_binary_parser import (
    KnowledgeBinaryParserClient,
)
from app.services.ai_call.knowledge_binary_parser import (
    serve as serve_binary_parser,
)
from app.services.ai_call.prompt_optimization import (
    OpenAICompatibleKnowledgeProductExtractor,
)


def _unique_column_sets(model: type) -> set[tuple[str, ...]]:
    return {
        tuple(constraint.columns.keys())
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def test_knowledge_module_imports_before_ai_call_router() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from app.services.ai_call.knowledge import RETRIEVER_VERSION",
        ],
        cwd=Path(__file__).parents[1],
        check=True,
    )


def test_knowledge_models_freeze_tenant_version_and_chunk_contract() -> None:
    item_columns = inspect(AiCallKnowledgeItemModel).columns
    version_columns = inspect(AiCallKnowledgeVersionModel).columns
    chunk_columns = inspect(AiCallKnowledgeChunkModel).columns
    usage_columns = inspect(AiCallKnowledgeUsageModel).columns

    assert item_columns.tenant_id.type.length == 20
    assert item_columns.current_ready_version_id.nullable is True
    assert {
        "status",
        "attempt_count",
        "lease_owner",
        "lease_expires_at",
        "chunk_count",
        "chunk_set_sha256",
    } <= set(version_columns.keys())
    assert "attempt_id" not in version_columns
    assert ("tenant_id", "knowledge_item_id", "version_no") in _unique_column_sets(
        AiCallKnowledgeVersionModel
    )
    assert ("tenant_id", "knowledge_version_id", "chunk_index") in _unique_column_sets(
        AiCallKnowledgeChunkModel
    )
    assert "normalized_content" not in chunk_columns
    assert "ngram_tsv" not in chunk_columns
    assert {
        "purpose",
        "prompt_profile_id",
        "knowledge_version_ids",
        "version_snapshot_hash",
        "status",
        "evidence_json",
        "latency_ms",
    } <= set(usage_columns.keys())
    assert (
        "tenant_id",
        "prompt_profile_id",
        "knowledge_item_id",
    ) in _unique_column_sets(AiCallPromptKnowledgeBindingModel)


def test_knowledge_routes_use_explicit_view_and_manage_permissions() -> None:
    routes = [route for route in AiCallRouter.routes if hasattr(route, "dependant")]

    def dependency_calls(method: str, path: str) -> set:
        route = next(route for route in routes if route.path == path and method in route.methods)
        return {dependency.call for dependency in route.dependant.dependencies}

    for method, path in (
        ("GET", "/ai-call/knowledge/items"),
        ("GET", "/ai-call/knowledge/items/{itemId}"),
        ("GET", "/ai-call/knowledge/items/{itemId}/versions"),
        ("GET", "/ai-call/knowledge/versions/{versionId}/processing"),
        ("GET", "/ai-call/knowledge/versions/{versionId}/download"),
        ("GET", "/ai-call/knowledge/versions/{versionId}/preview"),
    ):
        calls = dependency_calls(method, path)
        assert get_knowledge_viewer in calls
        assert get_knowledge_db in calls

    for method, path in (
        ("POST", "/ai-call/knowledge/items/upload"),
        ("POST", "/ai-call/knowledge/items/{itemId}/versions/upload"),
        ("PATCH", "/ai-call/knowledge/items/{itemId}"),
        ("PUT", "/ai-call/knowledge/items/{itemId}/scene-bindings"),
        ("DELETE", "/ai-call/knowledge/items/{itemId}"),
        ("POST", "/ai-call/knowledge/versions/{versionId}/retry"),
    ):
        calls = dependency_calls(method, path)
        assert get_knowledge_manager in calls
        assert get_knowledge_db in calls


def test_txt_and_markdown_parser_is_deterministic_and_bounded() -> None:
    source = (
        "# 售后政策\r\n\r\n退款将在审核通过后原路退回。\r\n\r\n"
        "## 补充说明\r\n\r\n" + "这是用于验证长段落切分的说明。" * 100
    ).encode()

    first = parse_text_knowledge(source, extension=".md")
    second = parse_text_knowledge(source, extension="md")

    assert first == second
    assert first.parser_name == "text"
    assert first.parser_version == "txt-markdown-utf8-v1"
    assert first.chunk_strategy_version == "paragraph-900-1200-v1"
    assert len(first.chunks) >= 2
    assert all(0 < len(chunk.content) <= 1200 for chunk in first.chunks)
    assert all(len(chunk.content_checksum) == 64 for chunk in first.chunks)
    assert len(first.chunk_set_sha256) == 64
    assert first.chunks[0].section_path == "售后政策"
    assert first.chunks[-1].section_path == "售后政策 / 补充说明"


@pytest.mark.anyio
async def test_text_preview_is_inline_and_cannot_be_sniffed_as_active_content() -> None:
    async def body():
        yield b"# safe text"

    class _PreviewService:
        async def open_download(self, *args, **kwargs):
            return KnowledgeDownload(
                filename="faq.md",
                mime_type="text/markdown",
                status_code=200,
                content_length=11,
                content_range=None,
                body=body(),
            )

    response = await preview_knowledge_version_controller(
        version_id=1,
        auth=SimpleNamespace(user=SimpleNamespace(tenant_id="tenant-a", user_id=7)),
        db=object(),
        service=_PreviewService(),
        range_header=None,
    )

    assert response.media_type == "text/plain"
    assert response.headers["content-disposition"].startswith("inline;")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-security-policy"] == "sandbox"


def test_text_parser_rejects_unsupported_or_unsafe_input() -> None:
    unsafe_inputs = (
        (b"hello", ".pdf"),
        (b"\xff", ".txt"),
        (b"a\x00b", ".md"),
        (("# " + "x" * 1001 + "\nbody").encode(), ".md"),
    )
    for payload, extension in unsafe_inputs:
        try:
            parse_text_knowledge(payload, extension=extension)
        except KnowledgeTextParseError:
            continue
        raise AssertionError(f"expected parse failure for {extension}")


class _Body:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def get_raw_stream(self) -> BytesIO:
        return BytesIO(self.payload)


class _FakeCosClient:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    def put_object(self, *, Bucket, Body, Key, **kwargs):
        assert isinstance(kwargs["ContentLength"], str)
        payload = bytearray()
        while chunk := Body.read(7):
            payload.extend(chunk)
        self.objects[Key] = (bytes(payload), kwargs["ContentType"])
        return {"ETag": "fake"}

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise _FakeCosServiceError(404)
        payload, content_type = self.objects[Key]
        return {"Content-Length": str(len(payload)), "Content-Type": content_type}

    def get_object(self, *, Bucket, Key, **kwargs):
        payload, content_type = self.objects[Key]
        range_header = kwargs.get("Range")
        if range_header:
            start, end = (int(value) for value in range_header[6:].split("-"))
            payload = payload[start : end + 1]
        return {
            "Body": _Body(payload),
            "Content-Length": str(len(payload)),
            "Content-Type": content_type,
        }

    def delete_object(self, *, Bucket, Key):
        self.objects.pop(Key, None)
        return {}


class _FakeCosServiceError(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def get_status_code(self) -> int:
        return self.status_code


def _upload(
    payload: bytes,
    filename: str = "faq.md",
    content_type: str = "text/markdown",
) -> UploadFile:
    return UploadFile(
        file=BytesIO(payload),
        filename=filename,
        size=len(payload),
        headers=Headers({"content-type": content_type}),
    )


def _pptx_payload(*slides: str) -> bytes:
    payload = BytesIO()
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        )
        for index, text in enumerate(slides, start=1):
            archive.writestr(
                f"ppt/slides/slide{index}.xml",
                (
                    '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                    'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                    f"<p:cSld><a:p><a:r><a:t>{text}</a:t></a:r></a:p></p:cSld></p:sld>"
                ),
            )
    return payload.getvalue()


@pytest.mark.anyio
async def test_pptx_upload_stays_closed_without_isolated_parser() -> None:
    payload = _pptx_payload("产品能力")
    service = KnowledgeService(
        CosKnowledgeStore(client=_FakeCosClient(), bucket="bucket-1", prefix="ai-call")
    )

    with pytest.raises(CustomException) as error:
        await service.accept_upload(
            object(),
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="upload-pptx-disabled",
            file=_upload(
                payload,
                filename="产品资料.pptx",
                content_type=(
                    "application/vnd.openxmlformats-officedocument."
                    "presentationml.presentation"
                ),
            ),
            file_sha256=hashlib.sha256(payload).hexdigest(),
            content_category="PRODUCT_SERVICE",
            note=None,
        )

    assert error.value.status_code == 400


@pytest.mark.anyio
async def test_cos_upload_replay_worker_and_range_download_form_one_closed_loop() -> None:
    payload = "# 售后政策\n\n退款将在审核通过后原路退回。".encode()
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    client = _FakeCosClient()
    store = CosKnowledgeStore(client=client, bucket="bucket-1", prefix="ai-call")
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(AiCallPromptProfileModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeItemModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeVersionModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeChunkModel.__table__.create)
        await connection.run_sync(AiCallPromptKnowledgeBindingModel.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = KnowledgeService(store)

    async with sessions() as db:
        accepted = await service.accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="upload-1",
            file=_upload(payload),
            file_sha256=payload_sha256,
            content_category="FAQ",
            note="退款说明",
        )
        replayed = await service.accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="upload-1",
            file=_upload(payload),
            file_sha256=payload_sha256,
            content_category="FAQ",
            note="退款说明",
        )
        assert accepted.status == "PROCESSING"
        assert replayed.replayed is True
        assert replayed.item_id == accepted.item_id
        assert replayed.version_id == accepted.version_id
        with pytest.raises(CustomException) as conflict:
            await service.accept_upload(
                db,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="upload-1",
                file=_upload(payload + b"x"),
                file_sha256=hashlib.sha256(payload + b"x").hexdigest(),
                content_category="FAQ",
                note="退款说明",
            )
        assert conflict.value.status_code == 409

    worker = KnowledgeWorker(
        sessions,
        store,
        worker_id="knowledge-test-worker",
        poll_interval_seconds=0.01,
    )
    assert await worker.run_once() is True

    async with sessions() as db:
        version = await db.get(AiCallKnowledgeVersionModel, accepted.version_id)
        item = await db.get(AiCallKnowledgeItemModel, accepted.item_id)
        chunks = (
            await db.scalars(
                select(AiCallKnowledgeChunkModel).where(
                    AiCallKnowledgeChunkModel.knowledge_version_id == accepted.version_id
                )
            )
        ).all()
        processing = await service.get_processing(
            db,
            tenant_id="tenant-a",
            version_id=accepted.version_id,
        )
        assert version is not None and version.status == "READY"
        assert item is not None and item.current_ready_version_id == version.id
        assert len(chunks) == version.chunk_count == 1
        assert processing["status"] == "READY"

        download = await service.open_download(
            db,
            tenant_id="tenant-a",
            version_id=accepted.version_id,
            range_header="bytes=0-5",
        )
        downloaded = b"".join([chunk async for chunk in download.body])
        assert download.status_code == 206
        assert download.content_range == f"bytes 0-5/{len(payload)}"
        assert downloaded == payload[:6]

        await service.update_item(
            db,
            tenant_id="tenant-a",
            item_id=accepted.item_id,
            changes={"display_name": "售后知识"},
        )
        await service.accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="upload-2",
            file=_upload(payload, filename="faq-v2.md"),
            file_sha256=payload_sha256,
            content_category="FAQ",
            note="退款说明",
            item_id=accepted.item_id,
        )
        await db.refresh(item)
        assert item.display_name == "售后知识"

    assert next(iter(client.objects)) == (
        f"ai-call/knowledge/tenant-a/{accepted.item_id}/{accepted.version_id}/source.md"
    )
    assert parse_byte_range("bytes=-4", len(payload)) == (len(payload) - 4, len(payload) - 1)


@pytest.mark.anyio
async def test_pptx_upload_worker_persists_parser_and_page_citations() -> None:
    payload = _pptx_payload("第一页产品能力", "第二页交付周期")
    client = _FakeCosClient()
    store = CosKnowledgeStore(client=client, bucket="bucket-1", prefix="ai-call")
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(AiCallPromptProfileModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeItemModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeVersionModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeChunkModel.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    with TemporaryDirectory() as directory:
        socket_path = Path(directory) / "parser.sock"
        parser = get_context("spawn").Process(
            target=serve_binary_parser,
            kwargs={"socket_path": socket_path, "max_requests": 1},
        )
        parser.start()
        deadline = monotonic() + 2
        while not socket_path.exists() and monotonic() < deadline:
            sleep(0.01)

        service = KnowledgeService(store, binary_parser_enabled=True)
        async with sessions() as db:
            accepted = await service.accept_upload(
                db,
                tenant_id="tenant-a",
                user_id=7,
                idempotency_key="upload-pptx-1",
                file=_upload(
                    payload,
                    filename="产品资料.pptx",
                    content_type=(
                        "application/vnd.openxmlformats-officedocument."
                        "presentationml.presentation"
                    ),
                ),
                file_sha256=hashlib.sha256(payload).hexdigest(),
                content_category="PRODUCT_SERVICE",
                note=None,
            )

        worker = KnowledgeWorker(
            sessions,
            store,
            worker_id="knowledge-pptx-worker",
            binary_parser=KnowledgeBinaryParserClient(socket_path, timeout_seconds=2),
        )
        assert await worker.run_once() is True
        parser.join(timeout=2)

    async with sessions() as db:
        version = await db.get(AiCallKnowledgeVersionModel, accepted.version_id)
        chunks = (
            await db.scalars(
                select(AiCallKnowledgeChunkModel)
                .where(AiCallKnowledgeChunkModel.knowledge_version_id == accepted.version_id)
                .order_by(AiCallKnowledgeChunkModel.chunk_index)
            )
        ).all()
    await engine.dispose()

    assert not parser.is_alive()
    assert version is not None and version.status == "READY"
    assert version.parser_version == "pptx-ooxml-stdlib-v1"
    assert [chunk.page_no for chunk in chunks] == [1, 2]
    assert [chunk.source_path for chunk in chunks] == ["slides/1", "slides/2"]
    await engine.dispose()


@pytest.mark.anyio
async def test_worker_reconciles_stale_uploads_from_cos() -> None:
    payload = "# 售后政策\n\n退款将在审核通过后原路退回。".encode()
    now = datetime.now(timezone.utc)
    client = _FakeCosClient()
    client.objects["ai-call/knowledge/tenant-a/1/11/source.md"] = (
        payload,
        "text/markdown",
    )
    store = CosKnowledgeStore(client=client, bucket="bucket-1", prefix="ai-call")
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in (
            AiCallKnowledgeItemModel.__table__,
            AiCallKnowledgeVersionModel.__table__,
            AiCallKnowledgeChunkModel.__table__,
        ):
            await connection.run_sync(table.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db:
        db.add_all([
            AiCallKnowledgeItemModel(
                id=1,
                tenant_id="tenant-a",
                display_name="完整.md",
                content_category="FAQ",
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeItemModel(
                id=2,
                tenant_id="tenant-a",
                display_name="中断.md",
                content_category="FAQ",
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeVersionModel(
                id=11,
                tenant_id="tenant-a",
                knowledge_item_id=1,
                version_no=1,
                status="UPLOADING",
                source_object_key="knowledge/tenant-a/1/11/source.md",
                source_filename="完整.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                created_at=now - timedelta(hours=2),
            ),
            AiCallKnowledgeVersionModel(
                id=21,
                tenant_id="tenant-a",
                knowledge_item_id=2,
                version_no=1,
                status="UPLOADING",
                source_object_key="knowledge/tenant-a/2/21/source.md",
                source_filename="中断.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=10,
                sha256="2" * 64,
                created_at=now - timedelta(hours=1),
            ),
        ])
        await db.commit()

    worker = KnowledgeWorker(
        sessions,
        store,
        worker_id="knowledge-reconcile-test-worker",
        upload_reconcile_after_seconds=0,
    )
    assert await worker.run_once() is True
    assert await worker.run_once() is True
    assert await worker.run_once() is False

    async with sessions() as db:
        recovered = await db.get(AiCallKnowledgeVersionModel, 11)
        interrupted = await db.get(AiCallKnowledgeVersionModel, 21)
        assert recovered is not None and recovered.status == "READY"
        assert interrupted is not None and interrupted.status == "FAILED"
        assert interrupted.failure_code == "UPLOAD_INCOMPLETE"

    await engine.dispose()


@pytest.mark.anyio
async def test_knowledge_management_is_tenant_scoped_and_preserves_history() -> None:
    now = datetime.now(timezone.utc)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in (
            AiCallPromptProfileModel.__table__,
            AiCallKnowledgeItemModel.__table__,
            AiCallKnowledgeVersionModel.__table__,
            AiCallPromptKnowledgeBindingModel.__table__,
        ):
            await connection.run_sync(table.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = KnowledgeService(
        CosKnowledgeStore(client=_FakeCosClient(), bucket="bucket-1", prefix="ai-call")
    )

    async with sessions() as db:
        db.add_all([
            AiCallKnowledgeItemModel(
                id=1,
                tenant_id="tenant-a",
                display_name="合同资料.md",
                content_category="PRODUCT_SERVICE",
                current_ready_version_id=11,
                created_by=7,
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeItemModel(
                id=2,
                tenant_id="tenant-a",
                display_name="未完成.md",
                content_category="FAQ",
                created_by=7,
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeItemModel(
                id=3,
                tenant_id="tenant-b",
                display_name="其他租户.md",
                content_category="FAQ",
                current_ready_version_id=31,
                created_by=8,
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeVersionModel(
                id=11,
                tenant_id="tenant-a",
                knowledge_item_id=1,
                version_no=1,
                status="READY",
                source_object_key="a/11",
                source_filename="合同资料.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=10,
                sha256="1" * 64,
                chunk_count=1,
                chunk_set_sha256="a" * 64,
                created_by=7,
                created_at=now,
                ready_at=now,
            ),
            AiCallKnowledgeVersionModel(
                id=12,
                tenant_id="tenant-a",
                knowledge_item_id=1,
                version_no=2,
                status="FAILED",
                source_object_key="a/12",
                source_filename="合同资料-v2.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=11,
                sha256="2" * 64,
                failure_code="PARSE_FAILED",
                failure_message="解析失败",
                created_by=7,
                created_at=now,
            ),
            AiCallKnowledgeVersionModel(
                id=21,
                tenant_id="tenant-a",
                knowledge_item_id=2,
                version_no=1,
                status="UPLOADING",
                source_object_key="a/21",
                source_filename="未完成.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=5,
                sha256="3" * 64,
                created_by=7,
                created_at=now,
            ),
            AiCallKnowledgeVersionModel(
                id=31,
                tenant_id="tenant-b",
                knowledge_item_id=3,
                version_no=1,
                status="READY",
                source_object_key="b/31",
                source_filename="其他租户.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=8,
                sha256="4" * 64,
                chunk_count=1,
                chunk_set_sha256="b" * 64,
                created_by=8,
                created_at=now,
                ready_at=now,
            ),
        ])
        await db.commit()

        rows, total = await service.list_items(
            db,
            tenant_id="tenant-a",
            page_num=1,
            page_size=20,
        )
        assert total == 1
        assert rows[0]["id"] == "1"
        assert rows[0]["latestVersion"]["id"] == "12"
        assert rows[0]["currentReadyVersionId"] == "11"

        updated = await service.update_item(
            db,
            tenant_id="tenant-a",
            item_id=1,
            changes={"display_name": "正式合同知识", "note": "已审核"},
        )
        assert updated["displayName"] == "正式合同知识"
        assert updated["note"] == "已审核"

        with pytest.raises(CustomException) as unavailable_binding:
            await service.replace_scene_bindings(
                db,
                tenant_id="tenant-a",
                item_id=1,
                prompt_profile_ids=[101],
                user_id=7,
            )
        assert unavailable_binding.value.status_code == 503

        versions = await service.list_versions(db, tenant_id="tenant-a", item_id=1)
        assert [version["id"] for version in versions] == ["12", "11"]
        with pytest.raises(CustomException) as hidden_tenant:
            await service.get_item(db, tenant_id="tenant-a", item_id=3)
        assert hidden_tenant.value.status_code == 404

        await service.delete_item(db, tenant_id="tenant-a", item_id=1)
        rows, total = await service.list_items(
            db,
            tenant_id="tenant-a",
            page_num=1,
            page_size=20,
        )
        assert rows == [] and total == 0
        assert await db.get(AiCallKnowledgeVersionModel, 11) is not None
        assert await db.scalar(select(AiCallPromptKnowledgeBindingModel)) is None

    await engine.dispose()


@pytest.mark.anyio
async def test_product_info_extractor_marks_knowledge_as_untrusted_json_input() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["model"] == "qwen3.7-plus"
        assert "不可信资料" in payload["messages"][0]["content"]
        assert json.loads(payload["messages"][1]["content"])["mode"] == "extract"
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "draftText": "核心产品：合同审查。",
                                    "sources": [
                                        {"claim": "提供合同审查", "chunkId": "101"}
                                    ],
                                    "conflicts": [],
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ]
            },
        )

    extractor = OpenAICompatibleKnowledgeProductExtractor(
        base_url="https://dashscope.test/v1",
        api_key="test-key",
        model="qwen3.7-plus",
        transport=httpx.MockTransport(handler),
    )

    result = await extractor.extract({"mode": "extract", "chunks": []})

    assert result["draftText"] == "核心产品：合同审查。"
