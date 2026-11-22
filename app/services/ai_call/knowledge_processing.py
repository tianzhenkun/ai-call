from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from uuid import uuid4

from lingchen_knowledge.content import content_chars
from lingchen_knowledge.documents import PdfNeedsOcr
from lingchen_knowledge.formats import classify, validate_size
from lingchen_knowledge.models import (
    Chunk,
    Claim,
    Content,
    Item,
    KnowledgeError,
    MediaProvider,
    MediaType,
    Original,
    Status,
)
from lingchen_knowledge.processing import KnowledgeProcessor
from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import AiCallKnowledgeChunkModel as VersionChunk
from app.api.v1.ai_call.model import AiCallKnowledgeItemModel as KnowledgeItem
from app.api.v1.ai_call.model import AiCallKnowledgeVersionModel as Version
from app.services.ai_call.knowledge_binary_parser import KnowledgeBinaryParserClient
from app.services.ai_call.knowledge_media import discard_knowledge_audio
from app.utils.id_util import generate_snowflake_id

if TYPE_CHECKING:
    from app.services.ai_call.knowledge import KnowledgeObjectStore


def chunk_set_hash(content: Content) -> str:
    snapshot = [(chunk.ordinal, chunk.sha256, chunk.locator) for chunk in content.chunks]
    return hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def chunks_with_heading(content: Content) -> Iterator[tuple[Chunk, str | None]]:
    heading = None
    for chunk in content.chunks:
        if chunk.locator.get("role") == "heading" and not chunk.locator.get("offset"):
            heading = chunk.text
        yield chunk, heading


def version_chunks(
    version_id: int, tenant_id: str, extension: str, content: Content, now: datetime
) -> Iterator[VersionChunk]:
    for chunk, heading in chunks_with_heading(content):
        locator = chunk.locator
        role = locator.get("role")
        if "page" in locator:
            source_path = f"pages/{locator['page']}"
        elif "paragraph" in locator:
            source_path = f"paragraphs/{locator['paragraph']}"
        elif "startMs" in locator:
            source_path = f"time/{locator['startMs']}-{locator.get('endMs', '')}"
        else:
            source_path = f"chunks/{chunk.ordinal}"
        yield VersionChunk(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            knowledge_version_id=version_id,
            chunk_index=chunk.ordinal - 1,
            content=chunk.text,
            content_checksum=chunk.sha256,
            content_type={"body": "DOCX_BODY", "heading": "DOCX_HEADING"}.get(role, "TEXT"),
            source_type=extension.upper(),
            source_path=source_path,
            section_path=heading if role == "body" else None,
            page_no=locator.get("page"),
            start_ms=locator.get("startMs"),
            end_ms=locator.get("endMs"),
            speaker_id=locator.get("speaker"),
            created_at=now,
        )


def version_original(version: Version) -> Original:
    return Original(version.source_object_key, version.source_filename, version.mime_type,
                    version.byte_size, version.sha256)


async def read_original(store: KnowledgeObjectStore, original: Original) -> bytes:
    validate_size(original.size_bytes, maximum=classify(original.filename).max_bytes)
    try:
        opened = await store.open(original.ref)
        with BytesIO() as result:
            async for part in opened.body:
                if result.tell() + len(part) > original.size_bytes:
                    raise KnowledgeError("ORIGINAL_CHECKSUM_MISMATCH", "原件校验失败")
                result.write(part)
            return result.getvalue()
    except KnowledgeError:
        raise
    except Exception as exc:
        raise KnowledgeError("ORIGINAL_UNAVAILABLE", "暂时无法读取原件", retryable=True) from exc


async def parse_stored_document(
    store: KnowledgeObjectStore, original: Original, parser: KnowledgeBinaryParserClient,
) -> Content:
    raw = await read_original(store, original)
    if len(raw) != original.size_bytes or hashlib.sha256(raw).hexdigest() != original.sha256:
        raise KnowledgeError("ORIGINAL_CHECKSUM_MISMATCH", "原件校验失败")
    with TemporaryDirectory(prefix="ai-call-knowledge-") as directory:
        path = Path(directory) / "source"
        await asyncio.to_thread(path.write_bytes, raw)
        return await asyncio.to_thread(parser.parse, path, extension=Path(original.filename).suffix)


class KnowledgeVersionProcessor:
    """将公共组件的原件读取、租约、供应商任务和结果写入接到 Reach 的不可变版本。"""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        store: KnowledgeObjectStore,
        *,
        providers: Mapping[MediaType, MediaProvider] | None,
        binary_parser: KnowledgeBinaryParserClient | None,
        lease_seconds: int,
        poll_interval: float,
        public_extensions: tuple[str, ...],
    ) -> None:
        self.sessions = sessions
        self.store = store
        self.binary_parser = binary_parser
        self.providers = providers or {}
        self.extensions = public_extensions
        self.processor = KnowledgeProcessor(
            self,
            self,
            self.providers,
            step_timeout=min(60, (lease_seconds - 5) * 0.9),
            lease_seconds=lease_seconds,
            poll_interval=poll_interval,
        )

    async def run_once(self) -> bool:
        isolated = None
        if self.binary_parser is not None:
            isolated = await self._claim(lease_seconds=self.processor.lease_seconds, isolated=True)
            if isolated is not None:
                await self._parse_isolated(isolated, self.binary_parser)
        return await self.processor.run_once() or isolated is not None

    async def claim(self, *, lease_seconds: float) -> Claim | None:
        return await self._claim(lease_seconds=lease_seconds, isolated=False)

    async def _claim(self, *, lease_seconds: float, isolated: bool) -> Claim | None:
        now = datetime.now(UTC)
        query = (
            select(Version.id)
            .join(
                KnowledgeItem,
                and_(
                    KnowledgeItem.id == Version.knowledge_item_id,
                    KnowledgeItem.tenant_id == Version.tenant_id,
                ),
            )
            .where(
                Version.status == "PROCESSING",
                Version.extension.in_(self.extensions),
                KnowledgeItem.deleted_at.is_(None),
                or_(Version.next_attempt_at.is_(None), Version.next_attempt_at <= now),
                or_(Version.lease_expires_at.is_(None), Version.lease_expires_at <= now),
                # 未知提交到达公共组件的核对上限后停在这里，不能重新提交。
                or_(
                    func.coalesce(Version.failure_code, "") != "SUBMISSION_UNKNOWN",
                    Version.next_attempt_at.is_not(None),
                ),
            )
        )
        if self.binary_parser is not None:
            local_document = and_(
                Version.extension.in_(("pdf", "docx")), Version.provider_name.is_(None)
            )
            query = query.where(local_document if isolated else ~local_document)
        candidate = (
            query
            .order_by(Version.created_at, Version.id)
            .limit(1)
            .with_for_update(
                of=Version,
                skip_locked=True,
            )
            .correlate(None)
            .scalar_subquery()
        )
        async with self.sessions() as db, db.begin():
            version = await db.scalar(
                update(Version)
                .where(Version.id == candidate)
                .values(
                    attempt_count=Version.attempt_count + 1,
                    lease_owner=uuid4().hex,
                    lease_expires_at=now + timedelta(seconds=lease_seconds),
                )
                .returning(Version)
            )
            if version is None:
                return None
            item = (
                await db.scalars(
                    select(KnowledgeItem).where(
                        KnowledgeItem.id == version.knowledge_item_id,
                        KnowledgeItem.tenant_id == version.tenant_id,
                    )
                )
            ).one()
            started = version.provider_started_at
            return Claim(
                item=Item(
                    id=str(version.id),
                    tenant_id=version.tenant_id,
                    scope="user",
                    owner_user_id=str(item.created_by) if item.created_by is not None else None,
                    workspace_id=None,
                    name=item.display_name,
                    category=item.content_category,
                    note=item.note,
                    media_type=classify(version.source_filename).media_type,
                    status="processing",
                    created_at=version.created_at,
                    updated_at=item.updated_at,
                    original=version_original(version),
                ),
                run_no=version.provider_run_no,
                token=version.lease_owner,
                attempt=version.attempt_count,
                provider_name=version.provider_name,
                provider_state=(
                    "submitted"
                    if version.provider_job_id
                    else "unknown"
                    if version.provider_request_key
                    else None
                ),
                request_key=version.provider_request_key,
                job_id=version.provider_job_id,
                provider_started_at=started.replace(tzinfo=UTC)
                if started and started.tzinfo is None
                else started,
            )

    @staticmethod
    def _owned(claim: Claim):
        return and_(
            Version.tenant_id == claim.item.tenant_id,
            Version.id == int(claim.item.id),
            Version.status == "PROCESSING",
            Version.lease_owner == claim.token,
            Version.lease_expires_at > datetime.now(UTC),
        )

    async def read(self, tenant_id: str, original: Original) -> bytes:
        return await read_original(self.store, original)

    async def _parse_isolated(self, claim: Claim, parser: KnowledgeBinaryParserClient) -> None:
        try:
            if claim.attempt > self.processor.max_attempts:
                raise KnowledgeError(
                    "PARSE_INTERRUPTED", "文档解析多次中断，可重新解析", retryable=True
                )
            async with asyncio.timeout(self.processor.step_timeout):
                content = await parse_stored_document(self.store, claim.item.original, parser)
        except PdfNeedsOcr:
            ocr = self.providers.get("document")
            if ocr is not None:
                # 使用下一次租约进入公共 OCR 流程，重启后也能继续。
                async with self.sessions() as db, db.begin():
                    await db.execute(
                        update(Version)
                        .where(self._owned(claim))
                        .values(
                            provider_name=ocr.name,
                            lease_owner=None,
                            lease_expires_at=None,
                            attempt_count=0,
                        )
                    )
                return
            error = KnowledgeError("OCR_UNAVAILABLE", "扫描版 PDF 需要文字识别，当前未配置识别服务")
        except KnowledgeError as exc:
            error = exc
        except TimeoutError:
            error = KnowledgeError("PARSE_TIMEOUT", "文档解析超时", retryable=True)
        except OSError:
            error = KnowledgeError(
                "DOCUMENT_WORKER_UNAVAILABLE", "暂时无法连接文档解析服务", retryable=True
            )
        else:
            await self.complete(claim, content)
            return
        again = error.retryable and claim.attempt < self.processor.max_attempts
        await self.release(
            claim,
            status="pending" if again else "failed",
            delay=2**claim.attempt if again else None,
            error=error,
        )

    async def remember_provider(
        self, claim: Claim, *, name: str, request_key: str, job_id: str | None
    ) -> bool:
        async with self.sessions() as db, db.begin():
            result = await db.execute(
                update(Version)
                .where(self._owned(claim))
                .values(
                    provider_name=name,
                    provider_request_key=request_key,
                    provider_job_id=job_id,
                    provider_started_at=func.coalesce(
                        Version.provider_started_at, datetime.now(UTC)
                    ),
                )
            )
            return result.rowcount == 1

    async def release(
        self,
        claim: Claim,
        *,
        status: Status,
        delay: float | None,
        error: KnowledgeError | None = None,
        clear_provider: bool = False,
        reset_attempts: bool = False,
    ) -> bool:
        fields = {
            "status": "FAILED" if status == "failed" else "PROCESSING",
            "next_attempt_at": datetime.now(UTC) + timedelta(seconds=delay)
            if delay is not None
            else None,
            "lease_owner": None,
            "lease_expires_at": None,
            "failure_code": error.code if error else None,
            "failure_message": str(error)[:1000] if error else None,
            "failure_retryable": error.retryable if error else False,
        }
        if reset_attempts:
            fields["attempt_count"] = 0
        if clear_provider:
            fields.update(
                provider_name=None,
                provider_request_key=None,
                provider_job_id=None,
                provider_started_at=None,
                provider_run_no=Version.provider_run_no + 1,
            )
        async with self.sessions() as db, db.begin():
            result = await db.execute(update(Version).where(self._owned(claim)).values(**fields))
        # 等待超时或查询暂时失败后仍可续查同一任务，供应商可能还要读取暂存音频。
        can_resume_job = claim.job_id is not None and error is not None and error.retryable
        if result.rowcount and (clear_provider or (delay is None and not can_resume_job)):
            await discard_knowledge_audio(self.store, claim)
        return result.rowcount == 1

    async def complete(self, claim: Claim, content: Content) -> bool:
        now = datetime.now(UTC)
        async with self.sessions() as db, db.begin():
            version = await db.scalar(select(Version).where(self._owned(claim)).with_for_update())
            if version is None:
                return False
            await db.execute(
                delete(VersionChunk).where(
                    VersionChunk.tenant_id == version.tenant_id,
                    VersionChunk.knowledge_version_id == version.id,
                )
            )
            db.add_all(
                version_chunks(version.id, version.tenant_id, version.extension, content, now)
            )
            version.status = "READY"
            version.parser_name = "lingchen-knowledge"
            version.parser_version = content.parser_version
            version.chunk_strategy_version = content.chunker_version
            version.chunk_count = len(content.chunks)
            version.content_char_count = content_chars(content.text)
            version.chunk_set_sha256 = chunk_set_hash(content)
            version.retrieval_version = version.retrieval_metadata = version.retrieval_vectors = None
            version.retrieval_retry_at = version.retrieval_lease = None
            version.ready_at = now
            version.next_attempt_at = version.lease_owner = version.lease_expires_at = None
            version.failure_code = version.failure_message = None
            version.failure_retryable = False
            item = (
                await db.scalars(
                    select(KnowledgeItem)
                    .where(
                        KnowledgeItem.id == version.knowledge_item_id,
                        KnowledgeItem.tenant_id == version.tenant_id,
                    )
                    .with_for_update()
                )
            ).one()
            current_no = (
                (
                    await db.scalar(
                        select(Version.version_no).where(
                            Version.id == item.current_ready_version_id,
                            Version.tenant_id == item.tenant_id,
                        )
                    )
                )
                if item.current_ready_version_id
                else 0
            )
            if version.version_no > (current_no or 0):
                item.current_ready_version_id = version.id
                item.updated_at = now
        await discard_knowledge_audio(self.store, claim)
        return True
