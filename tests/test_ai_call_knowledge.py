from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
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
from lingchen_knowledge.models import Block, KnowledgeError, Parsed
from sqlalchemy import UniqueConstraint, func, inspect, select
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
    KnowledgeProductInfoService,
    KnowledgeService,
    KnowledgeWorker,
    PlatformOssKnowledgeStore,
    build_knowledge_store,
    parse_byte_range,
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


def test_knowledge_store_prefers_the_platform_active_oss_config(monkeypatch) -> None:
    from app.api.v1.system.oss.service import OssService

    monkeypatch.setattr(
        OssService,
        "_active_config",
        {
            "endpoint": "minio.internal:9000",
            "bucket_name": "lingchen",
            "access_key": "test-access",
            "secret_key": "test-secret",
            "prefix": "products",
        },
    )
    config = type("Config", (), {"AI_CALL_KNOWLEDGE_OSS_PREFIX": "reach"})()

    store = build_knowledge_store(config)

    assert isinstance(store, PlatformOssKnowledgeStore)
    assert store.physical_key("knowledge/tenant-a/source.md") == (
        "products/reach/knowledge/tenant-a/source.md"
    )


def test_knowledge_store_rejects_legacy_direct_secret_fallback(monkeypatch) -> None:
    from app.api.v1.system.oss.service import OssService

    monkeypatch.setattr(OssService, "_active_config", None)
    config = type("Config", (), {"AI_CALL_KNOWLEDGE_OSS_PREFIX": "reach"})()

    with pytest.raises(RuntimeError, match="sys_oss_config"):
        build_knowledge_store(config)


@pytest.mark.anyio
async def test_platform_oss_knowledge_store_keeps_upload_and_range_contract(
    monkeypatch,
) -> None:
    from app.utils.minio_util import MinioUtil

    payload = b"platform knowledge"
    calls: list[tuple[str, str, tuple[int, int] | None]] = []

    async def put_object(config, object_name, data, content_type):
        assert data == payload
        assert content_type == "text/plain"
        calls.append(("put", object_name, None))

    async def head_object_size(config, object_name):
        calls.append(("stat", object_name, None))
        return len(payload)

    async def get_object(config, object_name, *, byte_range=None):
        calls.append(("get", object_name, byte_range))
        if byte_range is None:
            return payload
        start, end = byte_range
        return payload[start : end + 1]

    async def delete_object(config, object_name):
        calls.append(("delete", object_name, None))

    monkeypatch.setattr(MinioUtil, "put_object", put_object)
    monkeypatch.setattr(MinioUtil, "head_object_size", head_object_size)
    monkeypatch.setattr(MinioUtil, "get_object", get_object)
    monkeypatch.setattr(MinioUtil, "delete_object", delete_object)
    store = PlatformOssKnowledgeStore(
        {
            "endpoint": "minio.internal:9000",
            "bucket_name": "lingchen",
            "access_key": "test-access",
            "secret_key": "test-secret",
            "prefix": "products",
        },
        prefix="reach",
    )

    stored = await store.put(
        "knowledge/tenant-a/source.txt",
        BytesIO(payload),
        content_type="text/plain",
        expected_size=len(payload),
    )
    remote = await store.stat("knowledge/tenant-a/source.txt")
    opened = await store.open("knowledge/tenant-a/source.txt", byte_range=(2, 7))
    ranged = b"".join([chunk async for chunk in opened.body])
    await store.delete("knowledge/tenant-a/source.txt")

    assert stored.byte_size == remote.byte_size == len(payload)
    assert stored.sha256 == hashlib.sha256(payload).hexdigest()
    assert ranged == payload[2:8]
    assert calls == [
        ("put", "products/reach/knowledge/tenant-a/source.txt", None),
        ("stat", "products/reach/knowledge/tenant-a/source.txt", None),
        ("get", "products/reach/knowledge/tenant-a/source.txt", (2, 7)),
        ("delete", "products/reach/knowledge/tenant-a/source.txt", None),
    ]


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
        ("POST", "/ai-call/prompt-profiles/{profileId}/product-info:extract"),
    ):
        calls = dependency_calls(method, path)
        assert get_knowledge_manager in calls
        assert get_knowledge_db in calls


@pytest.mark.parametrize(
    ("filename", "mime_type", "expected_media_type"),
    (
        ("faq.md", "text/markdown", "text/plain"),
        ("note.html", "text/html", "text/plain"),
        ("guide.pdf", "application/pdf", "application/pdf"),
        ("guide.docx", "application/octet-stream",
         "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ("voice.mp3", "audio/mpeg", "audio/mpeg"),
        ("clip.mp4", "video/mp4", "video/mp4"),
    ),
)
@pytest.mark.anyio
async def test_safe_preview_is_inline_and_cannot_be_sniffed_as_active_content(
    filename: str,
    mime_type: str,
    expected_media_type: str,
) -> None:
    async def body():
        yield b"# safe text"

    class _PreviewService:
        async def open_download(self, *args, **kwargs):
            return KnowledgeDownload(
                filename=filename,
                mime_type=mime_type,
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

    assert response.media_type == expected_media_type
    assert response.headers["content-disposition"].startswith("inline;")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-security-policy"] == "sandbox"


def test_product_draft_sections_render_as_text_and_keep_their_sources() -> None:
    source = {"claim": "登记到店人数", "chunkId": "12"}
    result = {"draftText": {
        "核心能力": ["登记到店人数", "导出确认单"], "关键边界": "营业时段受理", "产品定位": "预约登记工具",
    }, "sources": [source], "conflicts": []}
    draft = KnowledgeProductInfoService._normalize_model_result(result, {"12"})
    assert draft["draftText"] == "产品定位\n预约登记工具\n\n核心能力\n- 登记到店人数\n- 导出确认单\n\n关键边界\n营业时段受理"
    assert draft["sources"] == [source]
    with pytest.raises(ValueError):
        KnowledgeProductInfoService._normalize_model_result({**result, "draftText": {"未知": {}}}, {"12"})


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


def _docx_payload(*paragraphs: str) -> bytes:
    payload = BytesIO()
    body = "".join(f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>" for text in paragraphs)
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        )
        archive.writestr(
            "word/document.xml",
            (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/'
                f'wordprocessingml/2006/main"><w:body>{body}</w:body></w:document>'
            ),
        )
    return payload.getvalue()


def _pdf_payload(*pages: str) -> bytes:
    page_ids = [4 + index * 2 for index in range(len(pages))]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{' '.join(f'{page_id} 0 R' for page_id in page_ids)}] /Count {len(pages)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for page_id, text in zip(page_ids, pages, strict=True):
        content_id = page_id + 1
        escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        stream = f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode()
        objects.extend([
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
            ).encode(),
            f"<< /Length {len(stream)} >>\nstream\n".encode()
            + stream
            + b"\nendstream",
        ])

    payload = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for number, body in enumerate(objects, start=1):
        offsets.append(len(payload))
        payload.extend(f"{number} 0 obj\n".encode() + body + b"\nendobj\n")
    xref_offset = len(payload)
    payload.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    payload.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        payload.extend(f"{offset:010d} 00000 n \n".encode())
    payload.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n".encode()
    )
    return bytes(payload)


@pytest.mark.anyio
async def test_pptx_upload_is_outside_the_shared_format_catalog() -> None:
    payload = _pptx_payload("产品能力")
    content_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    service = KnowledgeService(
        CosKnowledgeStore(client=_FakeCosClient(), bucket="bucket-1", prefix="ai-call")
    )

    with pytest.raises(CustomException) as error:
        await service.accept_upload(
            object(),
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key="upload-binary-disabled",
            file=_upload(
                payload,
                filename="产品资料.pptx",
                content_type=content_type,
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
            user_id=7,
            version_id=accepted.version_id,
        )
        assert version is not None and version.status == "READY"
        assert item is not None and item.current_ready_version_id == version.id
        assert len(chunks) == version.chunk_count == 2
        assert version.parser_version == "markdown-v1"
        assert {chunk.source_path for chunk in chunks} == {"paragraphs/1", "paragraphs/3"}
        assert processing["status"] == "READY"

        download = await service.open_download(
            db,
            tenant_id="tenant-a",
            user_id=7,
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
            user_id=7,
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
    await engine.dispose()


@pytest.mark.parametrize(
    (
        "payload",
        "filename",
        "content_type",
        "parser_version",
        "page_numbers",
        "source_paths",
    ),
    [
        (
            _docx_payload("产品能力", "交付周期"),
            "产品资料.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "docx-xml-v1",
            [None, None],
            ["paragraphs/1", "paragraphs/2"],
        ),
        (
            _pdf_payload("Product capability includes delivery scheduling.", "Delivery timeline is five business days."),
            "产品资料.pdf",
            "application/pdf",
            "pypdf-v1",
            [1, 2],
            ["pages/1", "pages/2"],
        ),
    ],
)
@pytest.mark.anyio
async def test_binary_upload_worker_persists_parser_and_source_citations(
    payload: bytes,
    filename: str,
    content_type: str,
    parser_version: str,
    page_numbers: list[int | None],
    source_paths: list[str],
    isolated_parser,
) -> None:
    client = _FakeCosClient()
    store = CosKnowledgeStore(client=client, bucket="bucket-1", prefix="ai-call")
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(AiCallPromptProfileModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeItemModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeVersionModel.__table__.create)
        await connection.run_sync(AiCallKnowledgeChunkModel.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    service = KnowledgeService(store)
    async with sessions() as db:
        accepted = await service.accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key=f"upload-{filename}",
            file=_upload(payload, filename=filename, content_type=content_type),
            file_sha256=hashlib.sha256(payload).hexdigest(),
            content_category="PRODUCT_SERVICE",
            note=None,
        )
    worker = KnowledgeWorker(
        sessions, store, worker_id="knowledge-binary-worker", binary_parser=isolated_parser
    )
    assert await worker.run_once()

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

    assert version is not None and version.status == "READY"
    assert version.parser_version == parser_version
    assert [chunk.page_no for chunk in chunks] == page_numbers
    assert [chunk.source_path for chunk in chunks] == source_paths


@pytest.fixture
def isolated_parser():
    with TemporaryDirectory() as directory:
        path = Path(directory) / "parser.sock"
        server = get_context("spawn").Process(
            target=serve_binary_parser,
            kwargs={"socket_path": path, "max_requests": 1},
        )
        server.start()
        try:
            deadline = monotonic() + 5
            while not path.exists() and monotonic() < deadline:
                sleep(0.01)
            assert path.exists()
            yield KnowledgeBinaryParserClient(path, timeout_seconds=3)
            server.join(timeout=3)
            assert server.exitcode == 0
        finally:
            if server.is_alive():
                server.terminate()
                server.join(timeout=3)
            server.close()


@pytest.mark.parametrize(
    ("environment", "configured", "expected"),
    [
        ("prod", "", "/run/ai-call-parser/parser.sock"),
        ("prod", "/custom/parser.sock", "/custom/parser.sock"),
        ("dev", "", None),
    ],
)
@pytest.mark.anyio
async def test_knowledge_worker_document_parser_for_environment(
    monkeypatch, environment, configured, expected
):
    from app.common.enums import EnvironmentEnum
    from app.plugin import init_app
    from app.services.ai_call import knowledge

    async def idle(_worker):
        pass

    monkeypatch.setattr(init_app.settings, "SQL_DB_ENABLE", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_KNOWLEDGE_WORKER_ENABLED", True)
    monkeypatch.setattr(init_app.settings, "AI_CALL_KNOWLEDGE_PARSER_SOCKET", configured)
    monkeypatch.setattr(init_app.settings, "ENVIRONMENT", EnvironmentEnum(environment))
    monkeypatch.setattr(knowledge, "build_knowledge_store", lambda _settings: object())
    monkeypatch.setattr(KnowledgeWorker, "start", idle)
    handle = await init_app._start_ai_call_knowledge_worker()
    worker, client = handle
    try:
        parser = worker.processor.binary_parser
        assert (str(parser.socket_path) if parser else None) == expected
    finally:
        await init_app._stop_ai_call_knowledge_worker(handle)
    assert client.is_closed


@pytest.fixture
async def shared_processing():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for model in (
            AiCallPromptProfileModel,
            AiCallKnowledgeItemModel,
            AiCallKnowledgeVersionModel,
            AiCallKnowledgeChunkModel,
            AiCallPromptKnowledgeBindingModel,
            AiCallKnowledgeUsageModel,
        ):
            await connection.run_sync(model.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = CosKnowledgeStore(client=_FakeCosClient(), bucket="test", prefix="ai-call")
    try:
        yield sessions, store
    finally:
        await engine.dispose()


async def _accept_media(sessions, store, filename, mime_type, payload):
    async with sessions() as db:
        return await KnowledgeService(store).accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            idempotency_key=filename,
            file=_upload(payload, filename, mime_type),
            file_sha256=hashlib.sha256(payload).hexdigest(),
            content_category="FAQ",
            note=None,
        )


@pytest.mark.parametrize(
    ("filename", "mime_type", "payload", "carrier", "block"),
    [
        (
            "介绍.wav",
            "audio/wav",
            b"RIFF\x10\x00\x00\x00WAVEfmt ",
            "audio",
            Block("支持配送", start_ms=120, end_ms=890, speaker="A"),
        ),
        (
            "介绍.mp4",
            "video/mp4",
            b"\x00\x00\x00\x14ftypisom",
            "video",
            Block("开场介绍", start_ms=230, end_ms=1510),
        ),
        ("产品.png", "image/png", b"\x89PNG\r\n\x1a\n", "image", Block("保修一年", page=1)),
        ("扫描.pdf", "application/pdf", _pdf_payload(""), "document", Block("保修两年", page=1)),
    ],
    ids=["audio", "video", "image", "scanned-pdf"],
)
@pytest.mark.anyio
async def test_media_worker_resumes_and_extraction_cites_the_source(
    shared_processing,
    request,
    filename,
    mime_type,
    payload,
    carrier,
    block,
) -> None:
    class Provider:
        name = "test-provider"
        submissions = []

        async def prepare(self, claim, content, request_key):
            assert content == payload and claim.item.media_type == carrier
            return content

        async def submit(self, source, request_key):
            self.submissions.append(request_key)
            return "job-7"

        async def poll(self, job_id):
            assert job_id == "job-7"
            return Parsed("test-parser-v1", (block,))

    class Extractor:
        model_name = "test-extractor"

        async def extract(self, request):
            chunk = request["chunks"][0]
            assert chunk["content"] == block.text
            return {
                "draftText": chunk["content"],
                "sources": [
                    {"claim": chunk["content"], "chunkId": chunk["chunkId"]},
                ],
                "conflicts": [],
            }

    sessions, store = shared_processing
    version = await _accept_media(sessions, store, filename, mime_type, payload)
    provider = Provider()
    parser = request.getfixturevalue("isolated_parser") if carrier == "document" else None
    worker = KnowledgeWorker(
        sessions,
        store,
        worker_id="before-restart",
        providers={carrier: provider},
        binary_parser=parser,
    )
    assert await worker.run_once()
    async with sessions() as db:
        waiting = await db.get(AiCallKnowledgeVersionModel, version.version_id)
        assert waiting.status == "PROCESSING" and waiting.provider_job_id == "job-7"
        assert waiting.provider_request_key == provider.submissions[0]
        waiting.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()

    restarted = KnowledgeWorker(
        sessions, store, worker_id="after-restart", providers={carrier: provider}
    )
    assert await restarted.run_once()
    async with sessions() as db:
        ready = await db.get(AiCallKnowledgeVersionModel, version.version_id)
        item = await db.get(AiCallKnowledgeItemModel, version.item_id)
        chunk = (await db.scalars(select(AiCallKnowledgeChunkModel))).one()
        assert ready.status == "READY" and item.current_ready_version_id == ready.id
        assert (chunk.content, chunk.start_ms, chunk.end_ms, chunk.page_no, chunk.speaker_id) == (
            block.text,
            block.start_ms,
            block.end_ms,
            block.page,
            block.speaker,
        )
        now = datetime.now(UTC)
        db.add(
            AiCallPromptProfileModel(
                id=101,
                tenant_id="tenant-a",
                scene_code="intro",
                name="产品介绍",
                provider_key="static_profile",
                product_info="",
                variables_json="[]",
                created_at=now,
                updated_at=now,
            )
        )
        await db.commit()
        await KnowledgeService(store).replace_scene_bindings(
            db,
            tenant_id="tenant-a",
            item_id=item.id,
            prompt_profile_ids=[101],
            user_id=7,
        )
        extracted = await KnowledgeProductInfoService(Extractor()).extract(
            db,
            tenant_id="tenant-a",
            prompt_profile_id=101,
            user_id=7,
        )
        source = extracted["sources"][0]
        assert source["sourceFilename"] == filename and source["excerpt"] == block.text
        assert (source["startMs"], source["endMs"], source["pageNo"]) == (
            block.start_ms,
            block.end_ms,
            block.page,
        )
        assert len(provider.submissions) == 1


@pytest.mark.anyio
async def test_unknown_provider_submission_is_held_without_resubmitting(shared_processing) -> None:
    class Provider:
        name = "unknown-ocr"
        submissions = []
        recoveries = []

        async def prepare(self, claim, content, request_key):
            return content

        async def submit(self, source, request_key):
            self.submissions.append(request_key)
            raise TimeoutError

        async def recover(self, request_key):
            self.recoveries.append(request_key)
            return None

    sessions, store = shared_processing
    version = await _accept_media(sessions, store, "扫描件.png", "image/png", b"\x89PNG\r\n\x1a\n")
    provider = Provider()
    worker = KnowledgeWorker(sessions, store, worker_id="unknown", providers={"image": provider})
    for attempt in range(1, 4):
        assert await worker.run_once()
        async with sessions() as db:
            row = await db.get(AiCallKnowledgeVersionModel, version.version_id)
            assert row.failure_code == "SUBMISSION_UNKNOWN" and row.attempt_count == attempt
            if attempt < 3:
                assert row.next_attempt_at is not None
                row.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
                await db.commit()
            else:
                assert row.next_attempt_at is None and row.status == "PROCESSING"
    assert not await worker.run_once()
    assert provider.recoveries == provider.submissions * 2
    assert len(provider.submissions) == 1


@pytest.mark.anyio
async def test_retry_resumes_legacy_ocr_result_with_the_existing_job(shared_processing):
    sessions, store = shared_processing
    service = KnowledgeService(store)
    accepted = await _accept_media(sessions, store, "资料.png", "image/png", b"\x89PNG\r\n\x1a\n")
    args = {"tenant_id": "tenant-a", "user_id": 7, "version_id": accepted.version_id}
    async with sessions() as db:
        row = await db.get(AiCallKnowledgeVersionModel, accepted.version_id)
        row.status, row.failure_code, row.failure_retryable = "FAILED", "PROVIDER_URL_INVALID", False
        row.provider_name = "paddleocr"
        await db.commit()
        assert not (await service.get_processing(db, **args))["failureRetryable"]
        with pytest.raises(CustomException):
            await service.retry(db, **args)
        row.provider_job_id = "stored-job"
        await db.commit()
        assert service._version_to_dict(row)["failureRetryable"]
        assert (await service.get_processing(db, **args))["failureRetryable"]
        assert (await service.retry(db, **args))["status"] == "PROCESSING"

    class ExistingJob:
        name = "paddleocr"

        async def poll(self, job_id):
            assert job_id == "stored-job"
            return Parsed("paddleocr:test", (Block("已取回的文字", page=1),))

    worker = KnowledgeWorker(sessions, store, worker_id="ocr-retry", providers={"image": ExistingJob()})
    assert await worker.run_once()
    async with sessions() as db:
        row = await db.get(AiCallKnowledgeVersionModel, accepted.version_id)
        assert (row.status, row.provider_job_id, row.chunk_count) == ("READY", "stored-job", 1)


@pytest.mark.anyio
async def test_retry_continues_known_job_and_expired_job_creates_a_new_run(
    shared_processing,
) -> None:
    sessions, store = shared_processing
    payload = b"RIFF\x10\x00\x00\x00WAVEfmt "
    first = await _accept_media(sessions, store, "录音.wav", "audio/wav", payload)
    staged = []
    submissions = []

    class Provider:
        name = "test-asr"

        async def prepare(self, claim, content, request_key):
            key = f"knowledge-asr/{claim.item.tenant_id}/{claim.item.id}-{claim.run_no}.mp3"
            await store.put(key, BytesIO(b"mp3"), content_type="audio/mpeg", expected_size=3)
            staged.append(key)
            return content

        async def submit(self, source, request_key):
            submissions.append(request_key)
            return f"job-{len(submissions)}"

        async def poll(self, job_id):
            if job_id == "job-1":
                raise KnowledgeError("PROVIDER_JOB_EXPIRED", "任务已失效", retryable=True)
            return Parsed("test-asr-v1", (Block("历史录音", start_ms=1, end_ms=200),))

    service = KnowledgeService(store)
    worker = KnowledgeWorker(sessions, store, worker_id="retries", providers={"audio": Provider()})
    assert await worker.run_once()
    async with sessions() as db:
        waiting = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        assert waiting.provider_job_id == "job-1" and await store.stat_or_none(staged[0])
        waiting.provider_started_at = datetime.now(UTC) - timedelta(hours=1)
        waiting.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
    assert await worker.run_once()
    async with sessions() as db:
        failed = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        assert failed.failure_code == "PROVIDER_TIMEOUT" and failed.provider_job_id == "job-1"
        assert await store.stat_or_none(staged[0])
        await service.retry(db, tenant_id="tenant-a", user_id=7, version_id=first.version_id)
        assert failed.attempt_count == 0 and failed.provider_request_key == submissions[0]

    assert await worker.run_once()
    async with sessions() as db:
        expired = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        assert expired.failure_code == "PROVIDER_JOB_EXPIRED" and expired.provider_job_id is None
        assert await store.stat_or_none(staged[0]) is None
        await service.retry(db, tenant_id="tenant-a", user_id=7, version_id=first.version_id)
    assert await worker.run_once()
    assert len(submissions) == 2 and submissions[0] != submissions[1] and staged[0] != staged[1]

    async with sessions() as db:
        replacement = b"updated policy"
        current = await service.accept_upload(
            db,
            tenant_id="tenant-a",
            user_id=7,
            item_id=first.item_id,
            idempotency_key="newer",
            file=_upload(replacement, "版本二.txt", "text/plain"),
            file_sha256=hashlib.sha256(replacement).hexdigest(),
            content_category="FAQ",
            note=None,
        )
        pending = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        pending.next_attempt_at = datetime.now(UTC) + timedelta(hours=1)
        await db.commit()
    assert await worker.run_once()
    async with sessions() as db:
        pending = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        pending.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
    assert await worker.run_once()
    async with sessions() as db:
        old = await db.get(AiCallKnowledgeVersionModel, first.version_id)
        item = await db.get(AiCallKnowledgeItemModel, first.item_id)
        assert old.status == "READY" and old.sha256 == hashlib.sha256(payload).hexdigest()
        assert item.current_ready_version_id == current.version_id
        assert await store.stat_or_none(staged[1]) is None


@pytest.mark.anyio
async def test_expired_lease_cannot_publish_and_mismatched_original_is_rejected(
    shared_processing,
) -> None:
    sessions, store = shared_processing
    version = await _accept_media(sessions, store, "手册.txt", "text/plain", b"manual")
    worker = KnowledgeWorker(sessions, store, worker_id="lease")
    first = await worker.processor.claim(lease_seconds=300)
    assert first is not None
    async with sessions() as db:
        row = await db.get(AiCallKnowledgeVersionModel, version.version_id)
        row.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
    successor = await worker.processor.claim(lease_seconds=300)
    assert successor is not None and successor.token != first.token
    assert not await worker.processor.remember_provider(
        first, name="stale", request_key="stale", job_id="stale"
    )
    assert not await worker.processor.release(first, status="failed", delay=None)
    assert await worker.processor.release(successor, status="pending", delay=0)

    key = store.physical_key(first.item.original.ref)
    store.client.objects[key] = (b"broken", "text/plain")
    assert await worker.run_once()
    async with sessions() as db:
        failed = await db.get(AiCallKnowledgeVersionModel, version.version_id)
        item = await db.get(AiCallKnowledgeItemModel, version.item_id)
        assert failed.status == "FAILED" and failed.failure_code == "ORIGINAL_CHECKSUM_MISMATCH"
        assert item.current_ready_version_id is None and failed.chunk_count == 0


@pytest.mark.anyio
async def test_worker_reconciles_stale_uploads_from_cos() -> None:
    payload = "# 售后政策\n\n退款将在审核通过后原路退回。".encode()
    now = datetime.now(UTC)
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
    now = datetime.now(UTC)
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
            AiCallPromptProfileModel(
                id=101,
                tenant_id="tenant-a",
                scene_code="intro_contract",
                name="合同介绍",
                provider_key="static_profile",
                product_info="",
                variables_json="[]",
                created_at=now,
                updated_at=now,
            ),
            AiCallPromptProfileModel(
                id=201,
                tenant_id="tenant-b",
                scene_code="other",
                name="其他租户场景",
                provider_key="static_profile",
                product_info="",
                variables_json="[]",
                created_at=now,
                updated_at=now,
            ),
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
            user_id=7,
            page_num=1,
            page_size=20,
            content_category="FAQ",
        )
        assert rows == [] and total == 0
        with pytest.raises(CustomException, match="内容分类不合法"):
            await service.list_items(
                db,
                tenant_id="tenant-a",
                user_id=7,
                page_num=1,
                page_size=20,
                content_category="UNKNOWN",
            )

        rows, total = await service.list_items(
            db,
            tenant_id="tenant-a",
            user_id=7,
            page_num=1,
            page_size=20,
            content_category="PRODUCT_SERVICE",
        )
        assert total == 1
        assert rows[0]["id"] == "1"
        assert rows[0]["latestVersion"]["id"] == "12"
        assert rows[0]["currentReadyVersionId"] == "11"
        assert rows[0]["sceneBindings"] == []

        updated = await service.update_item(
            db,
            tenant_id="tenant-a",
            user_id=7,
            item_id=1,
            changes={"display_name": "正式合同知识", "note": "已审核"},
        )
        assert updated["displayName"] == "正式合同知识"
        assert updated["note"] == "已审核"

        bindings = await service.replace_scene_bindings(
            db,
            tenant_id="tenant-a",
            item_id=1,
            prompt_profile_ids=[101, 101],
            user_id=7,
        )
        assert bindings == [
            {
                "promptProfileId": "101",
                "sceneCode": "intro_contract",
                "name": "合同介绍",
            }
        ]
        rows, _ = await service.list_items(
            db,
            tenant_id="tenant-a",
            user_id=7,
            page_num=1,
            page_size=20,
        )
        assert rows[0]["sceneBindings"] == bindings
        with pytest.raises(CustomException) as cross_tenant:
            await service.replace_scene_bindings(
                db,
                tenant_id="tenant-a",
                item_id=1,
                prompt_profile_ids=[201],
                user_id=7,
            )
        assert cross_tenant.value.status_code == 400

        versions = await service.list_versions(db, tenant_id="tenant-a", user_id=7, item_id=1)
        assert [version["id"] for version in versions] == ["12", "11"]
        with pytest.raises(CustomException) as hidden_tenant:
            await service.get_item(db, tenant_id="tenant-a", user_id=7, item_id=3)
        assert hidden_tenant.value.status_code == 404

        # 删除场景保留资料与绑定原记录，但从正常关联展示中移除。
        profile = await db.get(AiCallPromptProfileModel, 101)
        profile.deleted_at = now
        await db.commit()
        remaining = await service.get_item(db, tenant_id="tenant-a", user_id=7, item_id=1)
        assert remaining["sceneBindings"] == []
        assert await db.scalar(select(func.count()).select_from(AiCallPromptKnowledgeBindingModel)) == 1
        with pytest.raises(CustomException, match="不存在或无权管理"):
            await service.replace_scene_bindings(db, tenant_id="tenant-a", item_id=1,
                                                prompt_profile_ids=[101], user_id=7)

        await service.delete_item(db, tenant_id="tenant-a", user_id=7, item_id=1)
        rows, total = await service.list_items(
            db,
            tenant_id="tenant-a",
            user_id=7,
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
        instructions = payload["messages"][0]["content"]
        assert "不可信资料" in instructions
        assert all(section in instructions for section in ("产品定位", "核心能力", "关键边界"))
        assert "300～600" in instructions
        assert "价格、政策、案例、周期和具体参数" in instructions
        assert "合并时保留" in instructions
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


@pytest.mark.anyio
async def test_product_info_extraction_uses_all_current_chunks_and_keeps_audit() -> None:
    now = datetime.now(UTC)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in (
            AiCallPromptProfileModel.__table__,
            AiCallKnowledgeItemModel.__table__,
            AiCallKnowledgeVersionModel.__table__,
            AiCallKnowledgeChunkModel.__table__,
            AiCallPromptKnowledgeBindingModel.__table__,
            AiCallKnowledgeUsageModel.__table__,
        ):
            await connection.run_sync(table.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db:
        db.add_all([
            AiCallPromptProfileModel(
                id=101,
                tenant_id="tenant-a",
                scene_code="intro_contract",
                name="合同介绍",
                provider_key="static_profile",
                product_info="原产品信息",
                variables_json="[]",
                created_at=now,
                updated_at=now,
            ),
            AiCallKnowledgeItemModel(
                id=1,
                tenant_id="tenant-a",
                display_name="合同产品.md",
                content_category="PRODUCT_SERVICE",
                current_ready_version_id=11,
                created_by=7,
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
                source_filename="合同产品.md",
                extension="md",
                mime_type="text/markdown",
                byte_size=100,
                sha256="1" * 64,
                chunk_count=2,
                chunk_set_sha256="a" * 64,
                created_by=7,
                created_at=now,
                ready_at=now,
            ),
            AiCallKnowledgeChunkModel(
                id=101,
                tenant_id="tenant-a",
                knowledge_version_id=11,
                chunk_index=0,
                content="合同审查覆盖风险识别。",
                content_checksum="b" * 64,
                content_type="TEXT",
                source_type="MARKDOWN",
                section_path="核心能力",
                source_path="lines:1-1",
                created_at=now,
            ),
            AiCallKnowledgeChunkModel(
                id=102,
                tenant_id="tenant-a",
                knowledge_version_id=11,
                chunk_index=1,
                content="具体效果取决于客户材料完整度。",
                content_checksum="c" * 64,
                content_type="TEXT",
                source_type="MARKDOWN",
                section_path="限制条件",
                source_path="lines:2-2",
                created_at=now,
            ),
            AiCallPromptKnowledgeBindingModel(
                id=201,
                tenant_id="tenant-a",
                prompt_profile_id=101,
                knowledge_item_id=1,
                created_by=7,
                created_at=now,
            ),
        ])
        await db.commit()

    class _Extractor:
        model_name = "test-summary-model"

        def __init__(self) -> None:
            self.modes: list[str] = []

        async def extract(self, payload):
            self.modes.append(payload["mode"])
            if payload["mode"] == "extract":
                chunk = payload["chunks"][0]
                return {
                    "draftText": chunk["content"],
                    "sources": [
                        {"claim": chunk["content"], "chunkId": chunk["chunkId"]}
                    ],
                    "conflicts": [],
                }
            return {
                "draftText": "\n".join(
                    partial["draftText"] for partial in payload["partials"]
                ),
                "sources": [
                    source
                    for partial in payload["partials"]
                    for source in partial["sources"]
                ],
                "conflicts": [],
            }

    extractor = _Extractor()
    service = KnowledgeProductInfoService(extractor, max_batch_chars=10)
    async with sessions() as db:
        result = await service.extract(
            db,
            tenant_id="tenant-a",
            prompt_profile_id=101,
            user_id=7,
        )
        profile = await db.get(AiCallPromptProfileModel, 101)
        usage = await db.scalar(select(AiCallKnowledgeUsageModel))

    assert extractor.modes == ["extract", "extract", "merge"]
    assert result["draftText"] == (
        "合同审查覆盖风险识别。\n具体效果取决于客户材料完整度。"
    )
    assert result["sourceVersionIds"] == ["11"]
    assert result["sourceDocuments"] == [
        {
            "versionId": "11",
            "versionNo": 1,
            "sourceFilename": "合同产品.md",
        }
    ]
    assert [source["chunkId"] for source in result["sources"]] == ["101", "102"]
    assert result["sources"][0]["sourceFilename"] == "合同产品.md"
    assert result["sources"][0]["excerpt"] == "合同审查覆盖风险识别。"
    assert len(result["versionSnapshotHash"]) == 64
    assert profile is not None and profile.product_info == "原产品信息"
    assert usage is not None
    assert usage.status == "OK"
    assert usage.purpose == "PRODUCT_SUMMARY"
    assert usage.knowledge_version_ids == '["11"]'
    assert usage.version_snapshot_hash == result["versionSnapshotHash"]
    assert usage.model_name == "test-summary-model"
    evidence = json.loads(usage.evidence_json)
    assert [source["chunkId"] for source in evidence["sources"]] == ["101", "102"]

    await engine.dispose()
