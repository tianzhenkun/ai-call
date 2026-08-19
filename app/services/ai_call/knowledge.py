from __future__ import annotations

import asyncio
import codecs
import hashlib
import json
import re
import unicodedata
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any, BinaryIO

from fastapi import UploadFile
from sqlalchemy import BigInteger, and_, bindparam, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import (
    AiCallKnowledgeChunkModel,
    AiCallKnowledgeItemModel,
    AiCallKnowledgeUsageModel,
    AiCallKnowledgeVersionModel,
    AiCallPromptKnowledgeBindingModel,
    AiCallPromptProfileModel,
)
from app.core.exceptions import CustomException
from app.core.logger import log
from app.services.ai_call.knowledge_binary_parser import (
    KnowledgeBinaryParseError,
    KnowledgeBinaryParserClient,
)
from app.services.ai_call.prompt_optimization import KnowledgeProductExtractorProtocol
from app.services.ai_call.session_registry import KnowledgeRuntimeContext
from app.utils.id_util import generate_snowflake_id

PARSER_NAME = "text"
PARSER_VERSION = "txt-markdown-utf8-v1"
CHUNK_STRATEGY_VERSION = "paragraph-900-1200-v1"
RETRIEVER_VERSION = "postgres-ngram-tsvector-v1"
MAX_UPLOAD_BYTES = 100 * 1024 * 1024

_TARGET_CHARS = 900
_MAX_CHARS = 1200
_MAX_SECTION_PATH_CHARS = 1000
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_SENTENCE_ENDINGS = "。！？；.!?;\n"
_STREAM_CHUNK_BYTES = 64 * 1024
_CONTENT_CATEGORIES = {
    "PRODUCT_SERVICE",
    "FAQ",
    "PROFESSIONAL",
    "INDUSTRY",
    "OTHER",
}
_SUPPORTED_MIME_TYPES = {
    "text/plain",
    "text/markdown",
    "text/x-markdown",
    "application/octet-stream",
}
_PPTX_MIME_TYPE = (
    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
)
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class KnowledgeTextParseError(ValueError):
    """TXT/Markdown 内容不满足安全解析合同。"""


@dataclass(frozen=True)
class KnowledgeChunkDraft:
    chunk_index: int
    content: str
    content_checksum: str
    content_type: str
    source_type: str
    section_path: str | None
    source_path: str
    page_no: int | None = None
    start_ms: int | None = None
    end_ms: int | None = None


@dataclass(frozen=True)
class ParsedKnowledge:
    parser_name: str
    parser_version: str
    chunk_strategy_version: str
    chunk_set_sha256: str
    chunks: tuple[KnowledgeChunkDraft, ...]


@dataclass(frozen=True)
class KnowledgeSearchHit:
    chunk_id: int
    version_id: int
    chunk_index: int
    content: str
    content_checksum: str
    source_filename: str
    page_no: int | None
    section_path: str | None
    source_path: str | None
    start_ms: int | None
    end_ms: int | None
    score: float


@dataclass(frozen=True)
class KnowledgeRealtimeSearchResult:
    audit_id: int | None
    status: str
    output: dict[str, Any]


@dataclass(frozen=True)
class _Block:
    body: str
    section_path: str | None
    start_line: int
    end_line: int


def parse_text_knowledge(payload: bytes, *, extension: str) -> ParsedKnowledge:
    normalized_extension = extension.strip().lower().lstrip(".")
    if normalized_extension not in {"txt", "md", "markdown"}:
        raise KnowledgeTextParseError("只支持 TXT 和 Markdown")
    try:
        source = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise KnowledgeTextParseError("文件必须是 UTF-8 编码") from exc
    if "\x00" in source:
        raise KnowledgeTextParseError("文件包含 NUL 字节")

    source = source.replace("\r\n", "\n").replace("\r", "\n")
    blocks = _parse_blocks(source, markdown=normalized_extension in {"md", "markdown"})
    if not blocks:
        raise KnowledgeTextParseError("文件没有可用正文")

    chunks = _build_chunks(
        blocks, source_type="MARKDOWN" if normalized_extension != "txt" else "TXT"
    )
    chunk_set_sha256 = hashlib.sha256(
        "".join(f"{chunk.chunk_index}:{chunk.content_checksum}\n" for chunk in chunks).encode()
    ).hexdigest()
    return ParsedKnowledge(
        parser_name=PARSER_NAME,
        parser_version=PARSER_VERSION,
        chunk_strategy_version=CHUNK_STRATEGY_VERSION,
        chunk_set_sha256=chunk_set_sha256,
        chunks=chunks,
    )


def _parse_binary_result(payload: dict[str, Any]) -> ParsedKnowledge:
    if (
        payload.get("parserName") != "pptx"
        or payload.get("parserVersion") != "pptx-ooxml-stdlib-v1"
        or payload.get("chunkStrategyVersion") != "pptx-slide-semantic-900-1200-v1"
    ):
        raise KnowledgeBinaryParseError("解析器版本不受信任")
    raw_chunks = payload.get("chunks")
    if not isinstance(raw_chunks, list) or not 0 < len(raw_chunks) <= 100_000:
        raise KnowledgeBinaryParseError("解析器切片数量不合法")

    chunks: list[KnowledgeChunkDraft] = []
    for index, raw in enumerate(raw_chunks):
        if not isinstance(raw, dict) or raw.get("chunkIndex") != index:
            raise KnowledgeBinaryParseError("解析器切片顺序不合法")
        content = raw.get("content")
        page_no = raw.get("pageNo")
        source_path = raw.get("sourcePath")
        if (
            not isinstance(content, str)
            or not 0 < len(content) <= _MAX_CHARS
            or hashlib.sha256(content.encode()).hexdigest() != raw.get("contentChecksum")
            or raw.get("contentType") != "TEXT"
            or raw.get("sourceType") != "PPTX"
            or not isinstance(page_no, int)
            or page_no <= 0
            or source_path != f"slides/{page_no}"
            or raw.get("sectionPath") is not None
            or raw.get("startMs") is not None
            or raw.get("endMs") is not None
        ):
            raise KnowledgeBinaryParseError("解析器切片内容不合法")
        chunks.append(
            KnowledgeChunkDraft(
                chunk_index=index,
                content=content,
                content_checksum=raw["contentChecksum"],
                content_type="TEXT",
                source_type="PPTX",
                section_path=None,
                source_path=source_path,
                page_no=page_no,
            )
        )

    chunk_set_sha256 = hashlib.sha256(
        "".join(f"{chunk.chunk_index}:{chunk.content_checksum}\n" for chunk in chunks).encode()
    ).hexdigest()
    if chunk_set_sha256 != payload.get("chunkSetSha256"):
        raise KnowledgeBinaryParseError("解析器切片校验值不合法")
    return ParsedKnowledge(
        parser_name="pptx",
        parser_version="pptx-ooxml-stdlib-v1",
        chunk_strategy_version="pptx-slide-semantic-900-1200-v1",
        chunk_set_sha256=chunk_set_sha256,
        chunks=tuple(chunks),
    )


def _parse_blocks(source: str, *, markdown: bool) -> list[_Block]:
    blocks: list[_Block] = []
    section_stack: list[str] = []
    paragraph: list[str] = []
    paragraph_start = 0

    def flush(end_line: int) -> None:
        nonlocal paragraph, paragraph_start
        body = " ".join(part.strip() for part in paragraph if part.strip()).strip()
        if body:
            blocks.append(
                _Block(
                    body=body,
                    section_path=" / ".join(section_stack) or None,
                    start_line=paragraph_start,
                    end_line=end_line,
                )
            )
        paragraph = []
        paragraph_start = 0

    lines = source.split("\n")
    for line_no, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        heading = _HEADING_RE.match(stripped) if markdown else None
        if heading:
            flush(line_no - 1)
            level = len(heading.group(1))
            title = heading.group(2).strip()
            section_stack[level - 1 :] = [title]
            if len(" / ".join(section_stack)) > _MAX_SECTION_PATH_CHARS:
                raise KnowledgeTextParseError("Markdown 标题路径不能超过 1000 个字符")
            continue
        if not stripped:
            flush(line_no - 1)
            continue
        if not paragraph:
            paragraph_start = line_no
        paragraph.append(stripped)
    flush(len(lines))
    return blocks


def _build_chunks(blocks: list[_Block], *, source_type: str) -> tuple[KnowledgeChunkDraft, ...]:
    drafts: list[KnowledgeChunkDraft] = []
    current: list[_Block] = []

    def render(parts: list[_Block]) -> str:
        section_path = parts[0].section_path
        prefix = f"{section_path}\n" if section_path else ""
        return prefix + "\n\n".join(part.body for part in parts)

    def emit(parts: list[_Block]) -> None:
        content = render(parts)
        checksum = hashlib.sha256(content.encode()).hexdigest()
        drafts.append(
            KnowledgeChunkDraft(
                chunk_index=len(drafts),
                content=content,
                content_checksum=checksum,
                content_type="TEXT",
                source_type=source_type,
                section_path=parts[0].section_path,
                source_path=f"lines:{parts[0].start_line}-{parts[-1].end_line}",
            )
        )

    pieces: list[_Block] = []
    for block in blocks:
        prefix_length = len(block.section_path) + 1 if block.section_path else 0
        available = _MAX_CHARS - prefix_length
        for body in _split_long_text(block.body, available):
            pieces.append(
                _Block(
                    body=body,
                    section_path=block.section_path,
                    start_line=block.start_line,
                    end_line=block.end_line,
                )
            )

    for piece in pieces:
        if current and (
            current[0].section_path != piece.section_path
            or len(render([*current, piece])) > _TARGET_CHARS
        ):
            emit(current)
            current = []
        current.append(piece)
    if current:
        emit(current)

    return tuple(drafts)


def _split_long_text(value: str, limit: int) -> list[str]:
    parts: list[str] = []
    remaining = value
    while len(remaining) > limit:
        boundary = max(remaining.rfind(char, 0, limit + 1) + 1 for char in _SENTENCE_ENDINGS)
        if boundary < min(_TARGET_CHARS, limit):
            boundary = limit
        parts.append(remaining[:boundary].strip())
        remaining = remaining[boundary:].strip()
    if remaining:
        parts.append(remaining)
    return parts


class KnowledgeSearchService:
    retriever_version = RETRIEVER_VERSION
    _SEARCH_SQL = text(
        """
        with query_features as (
            select ai_call_knowledge_ngram_tsquery(:query) as query_value
        )
        select
            chunk.id as chunk_id,
            chunk.knowledge_version_id as version_id,
            chunk.chunk_index,
            chunk.content,
            chunk.content_checksum,
            version.source_filename,
            chunk.page_no,
            chunk.section_path,
            chunk.source_path,
            chunk.start_ms,
            chunk.end_ms,
            ts_rank_cd(
                array[0.05, 0.20, 0.50, 1.00]::real[],
                chunk.ngram_tsv,
                query_features.query_value,
                32
            )::double precision as score
        from ai_call_knowledge_chunk as chunk
        join ai_call_knowledge_version as version
          on version.id = chunk.knowledge_version_id
         and version.tenant_id = chunk.tenant_id
        cross join query_features
        where chunk.tenant_id = :tenant_id
          and chunk.knowledge_version_id = any(:frozen_version_ids)
          and version.status = 'READY'
          and chunk.ngram_tsv @@ query_features.query_value
        order by score desc, chunk.knowledge_version_id, chunk.chunk_index, chunk.id
        limit :result_limit
        """
    ).bindparams(bindparam("frozen_version_ids", type_=ARRAY(BigInteger)))

    @classmethod
    async def search(
        cls,
        session: AsyncSession,
        *,
        tenant_id: str,
        frozen_version_ids: list[int] | tuple[int, ...],
        query: str,
        limit: int = 5,
    ) -> list[KnowledgeSearchHit]:
        if not tenant_id.strip():
            raise ValueError("tenant_id 不能为空")
        if not query.strip():
            return []
        if len(query) > 500:
            raise ValueError("query 不能超过 500 个字符")
        if not 1 <= limit <= 5:
            raise ValueError("limit 必须在 1 到 5 之间")
        version_ids = sorted({int(version_id) for version_id in frozen_version_ids})
        if not version_ids:
            return []

        rows = (
            await session.execute(
                cls._SEARCH_SQL,
                {
                    "tenant_id": tenant_id,
                    "frozen_version_ids": version_ids,
                    "query": query,
                    "result_limit": limit,
                },
            )
        ).mappings()
        return [
            KnowledgeSearchHit(
                chunk_id=row["chunk_id"],
                version_id=row["version_id"],
                chunk_index=row["chunk_index"],
                content=row["content"],
                content_checksum=row["content_checksum"],
                source_filename=row["source_filename"],
                page_no=row["page_no"],
                section_path=row["section_path"],
                source_path=row["source_path"],
                start_ms=row["start_ms"],
                end_ms=row["end_ms"],
                score=float(row["score"]),
            )
            for row in rows
        ]


def parse_knowledge_runtime_context(
    snapshot_json: str | dict[str, Any] | None,
    *,
    tenant_id: str,
    task_id: int,
) -> KnowledgeRuntimeContext | None:
    if not tenant_id.strip() or task_id <= 0:
        return None
    if isinstance(snapshot_json, str):
        try:
            snapshot = json.loads(snapshot_json)
        except (TypeError, ValueError):
            return None
    else:
        snapshot = snapshot_json
    if not isinstance(snapshot, dict):
        return None
    prompt = snapshot.get("prompt")
    knowledge = snapshot.get("knowledge")
    if not isinstance(prompt, dict) or not isinstance(knowledge, dict):
        return None
    prompt_profile_id = _canonical_positive_id(knowledge.get("promptProfileId"))
    if (
        prompt_profile_id is None
        or _canonical_positive_id(prompt.get("id")) != prompt_profile_id
    ):
        return None
    raw_version_ids = knowledge.get("versionIds")
    if not isinstance(raw_version_ids, list):
        return None
    parsed_version_ids = tuple(
        version_id
        for value in raw_version_ids
        if (version_id := _canonical_positive_id(value)) is not None
    )
    if (
        not parsed_version_ids
        or len(parsed_version_ids) != len(raw_version_ids)
        or parsed_version_ids != tuple(sorted(set(parsed_version_ids)))
    ):
        return None
    snapshot_hash = knowledge.get("versionSnapshotHash")
    if (
        not isinstance(snapshot_hash, str)
        or snapshot_hash != snapshot_hash.lower()
        or _SHA256_RE.fullmatch(snapshot_hash) is None
        or knowledge.get("retrieverVersion") != RETRIEVER_VERSION
    ):
        return None
    return KnowledgeRuntimeContext(
        tenant_id=tenant_id,
        task_id=task_id,
        prompt_profile_id=prompt_profile_id,
        version_ids=parsed_version_ids,
        version_snapshot_hash=snapshot_hash,
        retriever_version=RETRIEVER_VERSION,
    )


def _canonical_positive_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if not isinstance(value, str) or not value.isdecimal():
        return None
    parsed = int(value)
    return parsed if parsed > 0 and str(parsed) == value else None


def _normalize_knowledge_query(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKC", value).lower()
        if not char.isspace() and not unicodedata.category(char).startswith("P")
    )


class KnowledgeRealtimeSearchService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        timeout_seconds: float = 1.0,
        model_name: str | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.timeout_seconds = max(0.01, timeout_seconds)
        self.model_name = model_name

    async def search(
        self,
        *,
        context: KnowledgeRuntimeContext,
        call_id: str,
        customer_transcript_event_id: str | None,
        tool_call_id: str,
        query: Any,
    ) -> KnowledgeRealtimeSearchResult:
        started = perf_counter()
        hits: list[KnowledgeSearchHit] = []
        status = "FAILED"
        if isinstance(query, str):
            try:
                if len(query) > 500:
                    raise ValueError("query 不能超过 500 个字符")
                hits = await asyncio.wait_for(
                    self._search(context, query),
                    timeout=self.timeout_seconds,
                )
                status = "OK" if hits else "NO_HIT"
            except TimeoutError:
                status = "TIMEOUT"
            except Exception as exc:
                log.warning(
                    "AI Call 实时知识检索失败: tenantId={}, taskId={}, "
                    "callId={}, errorType={}",
                    context.tenant_id,
                    context.task_id,
                    call_id,
                    type(exc).__name__,
                )

        latency_ms = int((perf_counter() - started) * 1000)
        try:
            audit_id = await self._record_usage(
                context=context,
                call_id=call_id,
                customer_transcript_event_id=customer_transcript_event_id,
                tool_call_id=tool_call_id,
                query=query if isinstance(query, str) else "",
                status=status,
                hits=hits,
                latency_ms=latency_ms,
            )
        except Exception as exc:
            log.warning(
                "AI Call 实时知识审计写入失败: tenantId={}, taskId={}, "
                "callId={}, errorType={}",
                context.tenant_id,
                context.task_id,
                call_id,
                type(exc).__name__,
            )
            return KnowledgeRealtimeSearchResult(
                audit_id=None,
                status="failed",
                output=self._tool_output("FAILED", []),
            )
        return KnowledgeRealtimeSearchResult(
            audit_id=audit_id,
            status=status.lower(),
            output=self._tool_output(status, hits),
        )

    async def _search(
        self,
        context: KnowledgeRuntimeContext,
        query: str,
    ) -> list[KnowledgeSearchHit]:
        if context.retriever_version != RETRIEVER_VERSION:
            raise ValueError("不支持的知识检索器版本")
        async with self.session_factory() as db:
            versions = list(
                (
                    await db.scalars(
                        select(AiCallKnowledgeVersionModel)
                        .where(
                            AiCallKnowledgeVersionModel.tenant_id == context.tenant_id,
                            AiCallKnowledgeVersionModel.id.in_(context.version_ids),
                            AiCallKnowledgeVersionModel.status == "READY",
                        )
                        .order_by(AiCallKnowledgeVersionModel.id)
                    )
                ).all()
            )
            if (
                tuple(version.id for version in versions) != context.version_ids
                or knowledge_version_snapshot_hash(versions)
                != context.version_snapshot_hash
            ):
                raise ValueError("任务冻结知识快照与数据库不一致")
            return await KnowledgeSearchService.search(
                db,
                tenant_id=context.tenant_id,
                frozen_version_ids=context.version_ids,
                query=query,
            )

    async def _record_usage(
        self,
        *,
        context: KnowledgeRuntimeContext,
        call_id: str,
        customer_transcript_event_id: str | None,
        tool_call_id: str,
        query: str,
        status: str,
        hits: list[KnowledgeSearchHit],
        latency_ms: int,
    ) -> int:
        audit_id = generate_snowflake_id()
        evidence = [self._audit_evidence(hit) for hit in hits]
        async with self.session_factory() as db:
            db.add(
                AiCallKnowledgeUsageModel(
                    id=audit_id,
                    tenant_id=context.tenant_id,
                    purpose="REALTIME_ANSWER",
                    prompt_profile_id=context.prompt_profile_id,
                    task_id=context.task_id,
                    call_id=call_id,
                    customer_transcript_event_id=customer_transcript_event_id,
                    tool_call_id=tool_call_id,
                    tool_result_event_id=None,
                    answer_event_id=None,
                    qwen_response_id=None,
                    query_hash=hashlib.sha256(
                        _normalize_knowledge_query(query).encode()
                    ).hexdigest(),
                    query_excerpt_redacted=None,
                    knowledge_version_ids=json.dumps(
                        [str(version_id) for version_id in context.version_ids],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    version_snapshot_hash=context.version_snapshot_hash,
                    status=status,
                    retriever_version=context.retriever_version,
                    model_name=self.model_name,
                    evidence_json=json.dumps(
                        evidence,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    latency_ms=latency_ms,
                    created_at=datetime.now(timezone.utc),
                )
            )
            await db.commit()
        return audit_id

    async def link_tool_result(self, audit_id: int, event_id: str) -> None:
        async with self.session_factory() as db:
            result = await db.execute(
                update(AiCallKnowledgeUsageModel)
                .where(AiCallKnowledgeUsageModel.id == audit_id)
                .values(tool_result_event_id=event_id)
            )
            if result.rowcount != 1:
                raise LookupError("知识检索审计不存在")
            await db.commit()

    async def link_answer(
        self,
        audit_ids: list[int],
        *,
        event_id: str,
        response_id: str | None,
    ) -> None:
        if not audit_ids:
            return
        async with self.session_factory() as db:
            await db.execute(
                update(AiCallKnowledgeUsageModel)
                .where(AiCallKnowledgeUsageModel.id.in_(audit_ids))
                .values(answer_event_id=event_id, qwen_response_id=response_id)
            )
            await db.commit()

    @staticmethod
    def _audit_evidence(hit: KnowledgeSearchHit) -> dict[str, Any]:
        return {
            "chunkId": str(hit.chunk_id),
            "versionId": str(hit.version_id),
            "score": hit.score,
            "contentChecksum": hit.content_checksum,
            "sourceFilename": hit.source_filename,
            "pageNo": hit.page_no,
            "sectionPath": hit.section_path,
            "startMs": hit.start_ms,
            "endMs": hit.end_ms,
            "excerpt": hit.content[:200],
        }

    @classmethod
    def _tool_output(
        cls,
        status: str,
        hits: list[KnowledgeSearchHit],
    ) -> dict[str, Any]:
        messages = {
            "OK": "仅依据 evidence 中有明确来源的内容回答；证据不足或矛盾时转业务顾问。",
            "NO_HIT": "资料中暂未找到可靠依据，请说明需要业务顾问进一步确认。",
            "TIMEOUT": "知识查询超时，请说明暂时无法确认并建议业务顾问进一步确认。",
            "FAILED": "知识查询失败，请说明暂时无法确认并建议业务顾问进一步确认。",
        }
        return {
            "status": status.lower(),
            "evidenceType": "untrusted_business_data",
            "message": messages[status],
            "evidence": [
                {
                    **cls._audit_evidence(hit),
                    "content": hit.content,
                }
                for hit in hits
            ],
        }


async def load_current_ready_knowledge_versions(
    db: AsyncSession,
    *,
    tenant_id: str,
    prompt_profile_id: int,
) -> list[AiCallKnowledgeVersionModel]:
    return list(
        (
            await db.scalars(
                select(AiCallKnowledgeVersionModel)
                .join(
                    AiCallKnowledgeItemModel,
                    and_(
                        AiCallKnowledgeItemModel.id
                        == AiCallKnowledgeVersionModel.knowledge_item_id,
                        AiCallKnowledgeItemModel.tenant_id
                        == AiCallKnowledgeVersionModel.tenant_id,
                    ),
                )
                .join(
                    AiCallPromptKnowledgeBindingModel,
                    and_(
                        AiCallPromptKnowledgeBindingModel.knowledge_item_id
                        == AiCallKnowledgeItemModel.id,
                        AiCallPromptKnowledgeBindingModel.tenant_id
                        == AiCallKnowledgeItemModel.tenant_id,
                    ),
                )
                .where(
                    AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                    AiCallKnowledgeVersionModel.status == "READY",
                    AiCallKnowledgeItemModel.tenant_id == tenant_id,
                    AiCallKnowledgeItemModel.deleted_at.is_(None),
                    AiCallKnowledgeItemModel.current_ready_version_id
                    == AiCallKnowledgeVersionModel.id,
                    AiCallPromptKnowledgeBindingModel.tenant_id == tenant_id,
                    AiCallPromptKnowledgeBindingModel.prompt_profile_id
                    == prompt_profile_id,
                )
                .order_by(AiCallKnowledgeVersionModel.id)
            )
        ).all()
    )


def knowledge_version_snapshot_hash(
    versions: list[AiCallKnowledgeVersionModel],
) -> str:
    payload = [
        {
            "id": str(version.id),
            "sha256": version.sha256,
            "chunkSetSha256": version.chunk_set_sha256,
        }
        for version in versions
    ]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class KnowledgeProductInfoService:
    def __init__(
        self,
        extractor: KnowledgeProductExtractorProtocol | None,
        *,
        max_batch_chars: int = 30_000,
    ) -> None:
        self.extractor = extractor
        self.max_batch_chars = max(1, max_batch_chars)

    async def extract(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        prompt_profile_id: int,
        user_id: int,
    ) -> dict[str, Any]:
        del user_id
        if self.extractor is None:
            raise CustomException(msg="产品信息提取服务未配置", status_code=503)
        profile = await db.scalar(
            select(AiCallPromptProfileModel).where(
                AiCallPromptProfileModel.id == prompt_profile_id,
                AiCallPromptProfileModel.tenant_id == tenant_id,
            )
        )
        if profile is None:
            raise CustomException(msg="提示词配置不存在", status_code=404)
        versions = await load_current_ready_knowledge_versions(
            db,
            tenant_id=tenant_id,
            prompt_profile_id=prompt_profile_id,
        )
        if not versions:
            raise CustomException(msg="请先关联至少一份已就绪知识", status_code=409)

        version_ids = [version.id for version in versions]
        chunks = list(
            (
                await db.scalars(
                    select(AiCallKnowledgeChunkModel)
                    .where(
                        AiCallKnowledgeChunkModel.tenant_id == tenant_id,
                        AiCallKnowledgeChunkModel.knowledge_version_id.in_(version_ids),
                    )
                    .order_by(
                        AiCallKnowledgeChunkModel.knowledge_version_id,
                        AiCallKnowledgeChunkModel.chunk_index,
                        AiCallKnowledgeChunkModel.id,
                    )
                )
            ).all()
        )
        if len(chunks) != sum(version.chunk_count for version in versions):
            raise CustomException(msg="知识切片状态不一致，请稍后重试", status_code=409)

        version_by_id = {version.id: version for version in versions}
        chunk_payloads = [
            {
                "chunkId": str(chunk.id),
                "versionId": str(chunk.knowledge_version_id),
                "versionNo": version_by_id[chunk.knowledge_version_id].version_no,
                "sourceFilename": version_by_id[
                    chunk.knowledge_version_id
                ].source_filename,
                "pageNo": chunk.page_no,
                "sectionPath": chunk.section_path,
                "startMs": chunk.start_ms,
                "endMs": chunk.end_ms,
                "content": chunk.content,
            }
            for chunk in chunks
        ]
        snapshot_hash = knowledge_version_snapshot_hash(versions)
        started = perf_counter()
        try:
            raw_result = await self._extract_batches(
                scene_code=profile.scene_code,
                scene_name=profile.name,
                chunks=chunk_payloads,
            )
            result = self._enrich_result(
                raw_result,
                chunks=chunks,
                versions=version_by_id,
            )
        except Exception as exc:
            latency_ms = int((perf_counter() - started) * 1000)
            db.add(
                self._usage(
                    tenant_id=tenant_id,
                    prompt_profile_id=prompt_profile_id,
                    version_ids=version_ids,
                    snapshot_hash=snapshot_hash,
                    status="FAILED",
                    evidence={"errorType": type(exc).__name__},
                    latency_ms=latency_ms,
                )
            )
            await db.commit()
            log.warning(
                "AI Call 产品信息提取失败: "
                f"tenantId={tenant_id}, profileId={prompt_profile_id}, "
                f"errorType={type(exc).__name__}"
            )
            raise CustomException(msg="产品信息提取失败，请稍后重试", status_code=502) from exc

        latency_ms = int((perf_counter() - started) * 1000)
        source_version_ids = [str(version_id) for version_id in version_ids]
        response = {
            **result,
            "sourceVersionIds": source_version_ids,
            "versionSnapshotHash": snapshot_hash,
        }
        db.add(
            self._usage(
                tenant_id=tenant_id,
                prompt_profile_id=prompt_profile_id,
                version_ids=version_ids,
                snapshot_hash=snapshot_hash,
                status="OK",
                evidence={
                    "sources": result["sources"],
                    "conflicts": result["conflicts"],
                },
                latency_ms=latency_ms,
            )
        )
        await db.commit()
        return response

    async def _extract_batches(
        self,
        *,
        scene_code: str,
        scene_name: str,
        chunks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        assert self.extractor is not None
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_chars = 0
        for chunk in chunks:
            content_chars = len(chunk["content"])
            if current and current_chars + content_chars > self.max_batch_chars:
                batches.append(current)
                current = []
                current_chars = 0
            current.append(chunk)
            current_chars += content_chars
        if current:
            batches.append(current)

        allowed_chunk_ids = {chunk["chunkId"] for chunk in chunks}
        partials = [
            self._normalize_model_result(
                await self.extractor.extract({
                    "mode": "extract",
                    "sceneCode": scene_code,
                    "sceneName": scene_name,
                    "chunks": batch,
                }),
                allowed_chunk_ids,
            )
            for batch in batches
        ]
        while len(partials) > 1:
            merged: list[dict[str, Any]] = []
            for offset in range(0, len(partials), 4):
                group = partials[offset : offset + 4]
                if len(group) == 1:
                    merged.append(group[0])
                    continue
                merged.append(
                    self._normalize_model_result(
                        await self.extractor.extract({
                            "mode": "merge",
                            "sceneCode": scene_code,
                            "sceneName": scene_name,
                            "partials": group,
                        }),
                        allowed_chunk_ids,
                    )
                )
            partials = merged
        if not partials:
            raise ValueError("没有可提取的知识切片")
        return partials[0]

    @staticmethod
    def _normalize_model_result(
        result: dict[str, Any],
        allowed_chunk_ids: set[str],
    ) -> dict[str, Any]:
        if not isinstance(result, dict):
            raise ValueError("提取响应不是 JSON 对象")
        draft_text = str(result.get("draftText") or "").strip()
        if not draft_text or len(draft_text) > 20_000:
            raise ValueError("产品信息草稿为空或超过 20000 字符")

        raw_sources = result.get("sources")
        if not isinstance(raw_sources, list) or not raw_sources or len(raw_sources) > 200:
            raise ValueError("产品信息草稿缺少有效来源")
        sources: list[dict[str, str]] = []
        seen_sources: set[tuple[str, str]] = set()
        for source in raw_sources:
            if not isinstance(source, dict):
                raise ValueError("来源格式错误")
            claim = str(source.get("claim") or "").strip()
            chunk_id = str(source.get("chunkId") or "").strip()
            if not claim or len(claim) > 1000 or chunk_id not in allowed_chunk_ids:
                raise ValueError("来源引用了不存在的知识切片")
            key = (claim, chunk_id)
            if key not in seen_sources:
                sources.append({"claim": claim, "chunkId": chunk_id})
                seen_sources.add(key)

        raw_conflicts = result.get("conflicts") or []
        if not isinstance(raw_conflicts, list) or len(raw_conflicts) > 100:
            raise ValueError("冲突列表格式错误")
        conflicts: list[dict[str, Any]] = []
        for conflict in raw_conflicts:
            if not isinstance(conflict, dict):
                raise ValueError("冲突格式错误")
            topic = str(conflict.get("topic") or "").strip()
            description = str(conflict.get("description") or "").strip()
            source_chunk_ids = [
                str(chunk_id) for chunk_id in conflict.get("sourceChunkIds") or []
            ]
            if (
                not topic
                or len(topic) > 500
                or not description
                or len(description) > 2000
                or not source_chunk_ids
                or any(chunk_id not in allowed_chunk_ids for chunk_id in source_chunk_ids)
            ):
                raise ValueError("冲突引用了不存在的知识切片")
            conflicts.append({
                "topic": topic,
                "description": description,
                "sourceChunkIds": source_chunk_ids,
            })
        return {
            "draftText": draft_text,
            "sources": sources,
            "conflicts": conflicts,
        }

    @staticmethod
    def _enrich_result(
        result: dict[str, Any],
        *,
        chunks: list[AiCallKnowledgeChunkModel],
        versions: dict[int, AiCallKnowledgeVersionModel],
    ) -> dict[str, Any]:
        chunk_by_id = {str(chunk.id): chunk for chunk in chunks}
        sources = []
        for source in result["sources"]:
            chunk = chunk_by_id[source["chunkId"]]
            version = versions[chunk.knowledge_version_id]
            sources.append({
                **source,
                "versionId": str(version.id),
                "versionNo": version.version_no,
                "sourceFilename": version.source_filename,
                "pageNo": chunk.page_no,
                "sectionPath": chunk.section_path,
                "startMs": chunk.start_ms,
                "endMs": chunk.end_ms,
                "excerpt": chunk.content[:200],
            })
        return {
            "draftText": result["draftText"],
            "sources": sources,
            "conflicts": result["conflicts"],
        }

    def _usage(
        self,
        *,
        tenant_id: str,
        prompt_profile_id: int,
        version_ids: list[int],
        snapshot_hash: str,
        status: str,
        evidence: dict[str, Any],
        latency_ms: int,
    ) -> AiCallKnowledgeUsageModel:
        model_name = self.extractor.model_name if self.extractor is not None else None
        return AiCallKnowledgeUsageModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            purpose="PRODUCT_SUMMARY",
            prompt_profile_id=prompt_profile_id,
            task_id=None,
            call_id=None,
            customer_transcript_event_id=None,
            tool_call_id=None,
            tool_result_event_id=None,
            answer_event_id=None,
            qwen_response_id=None,
            query_hash=None,
            query_excerpt_redacted=None,
            knowledge_version_ids=json.dumps(
                [str(version_id) for version_id in version_ids],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            version_snapshot_hash=snapshot_hash,
            status=status,
            retriever_version=RETRIEVER_VERSION,
            model_name=model_name,
            evidence_json=json.dumps(
                evidence,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            latency_ms=latency_ms,
            created_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class StoredObject:
    byte_size: int
    content_type: str


@dataclass(frozen=True)
class OpenedObject:
    body: AsyncIterator[bytes]


@dataclass(frozen=True)
class StoredUpload:
    byte_size: int
    sha256: str


@dataclass(frozen=True)
class KnowledgeUploadResult:
    item_id: int
    version_id: int
    status: str
    replayed: bool = False


@dataclass(frozen=True)
class KnowledgeDownload:
    filename: str
    mime_type: str
    status_code: int
    content_length: int
    content_range: str | None
    body: AsyncIterator[bytes]


class _HashingReader:
    def __init__(self, source: BinaryIO, max_bytes: int) -> None:
        self.source = source
        self.max_bytes = max_bytes
        self.byte_size = 0
        self.digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        if size < 0 or size > _STREAM_CHUNK_BYTES:
            size = _STREAM_CHUNK_BYTES
        chunk = self.source.read(size)
        if not isinstance(chunk, bytes):
            raise TypeError("COS 上传流必须返回 bytes")
        self.byte_size += len(chunk)
        if self.byte_size > self.max_bytes:
            raise ValueError("文件正文不能超过 100 MB")
        self.digest.update(chunk)
        return chunk

    @property
    def sha256(self) -> str:
        return self.digest.hexdigest()


class CosKnowledgeStore:
    """只在这一层拼接 AI Call 前缀并调用腾讯 COS SDK。"""

    def __init__(self, *, client: Any, bucket: str, prefix: str = "ai-call") -> None:
        self.client = client
        self.bucket = bucket.strip()
        self.prefix = prefix.strip().strip("/")
        if not self.bucket or not self.prefix:
            raise ValueError("COS Bucket 和前缀不能为空")

    def physical_key(self, logical_key: str) -> str:
        normalized = logical_key.strip().strip("/")
        segments = normalized.split("/")
        if (
            not normalized
            or "\\" in normalized
            or "//" in normalized
            or any(segment in {"", ".", ".."} for segment in segments)
            or any(ord(char) < 32 for char in normalized)
        ):
            raise ValueError("COS 对象 Key 不合法")
        return f"{self.prefix}/{normalized}"

    async def put(
        self,
        logical_key: str,
        source: BinaryIO,
        *,
        content_type: str,
        expected_size: int,
    ) -> StoredUpload:
        reader = _HashingReader(source, MAX_UPLOAD_BYTES)
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=self.physical_key(logical_key),
            Body=reader,
            ContentType=content_type,
            ContentLength=str(expected_size),
        )
        if reader.byte_size != expected_size:
            raise ValueError("上传文件大小与声明不一致")
        return StoredUpload(byte_size=reader.byte_size, sha256=reader.sha256)

    async def stat(self, logical_key: str) -> StoredObject:
        response = await asyncio.to_thread(
            self.client.head_object,
            Bucket=self.bucket,
            Key=self.physical_key(logical_key),
        )
        return StoredObject(
            byte_size=int(_response_header(response, "Content-Length")),
            content_type=str(
                _response_header(response, "Content-Type", "application/octet-stream")
            ),
        )

    async def stat_or_none(self, logical_key: str) -> StoredObject | None:
        try:
            return await self.stat(logical_key)
        except Exception as exc:
            if _cos_status_code(exc) == 404:
                return None
            raise

    async def open(
        self,
        logical_key: str,
        *,
        byte_range: tuple[int, int] | None = None,
    ) -> OpenedObject:
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": self.physical_key(logical_key),
        }
        if byte_range is not None:
            kwargs["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
        response = await asyncio.to_thread(self.client.get_object, **kwargs)
        raw_stream = response["Body"].get_raw_stream()
        return OpenedObject(body=_iterate_blocking_stream(raw_stream))

    async def delete(self, logical_key: str) -> None:
        await asyncio.to_thread(
            self.client.delete_object,
            Bucket=self.bucket,
            Key=self.physical_key(logical_key),
        )


def build_cos_knowledge_store(config: Any) -> CosKnowledgeStore:
    fields = {
        "AI_CALL_KNOWLEDGE_COS_SECRET_ID": config.AI_CALL_KNOWLEDGE_COS_SECRET_ID,
        "AI_CALL_KNOWLEDGE_COS_SECRET_KEY": config.AI_CALL_KNOWLEDGE_COS_SECRET_KEY,
        "AI_CALL_KNOWLEDGE_COS_BUCKET": config.AI_CALL_KNOWLEDGE_COS_BUCKET,
        "AI_CALL_KNOWLEDGE_COS_REGION": config.AI_CALL_KNOWLEDGE_COS_REGION,
        "AI_CALL_KNOWLEDGE_COS_PREFIX": config.AI_CALL_KNOWLEDGE_COS_PREFIX,
    }
    missing = [name for name, value in fields.items() if not str(value).strip()]
    if missing:
        raise RuntimeError(f"知识库 COS 配置缺失：{', '.join(missing)}")

    from qcloud_cos import CosConfig, CosS3Client

    client = CosS3Client(
        CosConfig(
            Region=config.AI_CALL_KNOWLEDGE_COS_REGION,
            SecretId=config.AI_CALL_KNOWLEDGE_COS_SECRET_ID,
            SecretKey=config.AI_CALL_KNOWLEDGE_COS_SECRET_KEY,
            Scheme="https",
            AllowRedirects=False,
        )
    )
    return CosKnowledgeStore(
        client=client,
        bucket=config.AI_CALL_KNOWLEDGE_COS_BUCKET,
        prefix=config.AI_CALL_KNOWLEDGE_COS_PREFIX,
    )


def _response_header(response: dict[str, Any], name: str, default: Any = None) -> Any:
    lowered_name = name.lower()
    for key, value in response.items():
        if key.lower() == lowered_name:
            return value
    if default is not None:
        return default
    raise ValueError(f"COS 响应缺少 {name}")


def _cos_status_code(exc: Exception) -> int | None:
    getter = getattr(exc, "get_status_code", None)
    value = getter() if callable(getter) else getattr(exc, "status_code", None)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _iterate_blocking_stream(stream: Any) -> AsyncIterator[bytes]:
    try:
        while chunk := await asyncio.to_thread(stream.read, _STREAM_CHUNK_BYTES):
            yield chunk
    finally:
        close = getattr(stream, "close", None)
        if close is not None:
            await asyncio.to_thread(close)


def parse_byte_range(value: str | None, total_size: int) -> tuple[int, int] | None:
    if value is None:
        return None
    if total_size <= 0 or not value.startswith("bytes=") or "," in value:
        raise ValueError("Range 不合法")
    spec = value[6:].strip()
    if "-" not in spec:
        raise ValueError("Range 不合法")
    start_text, end_text = spec.split("-", 1)
    if not start_text:
        if not end_text.isdigit() or int(end_text) <= 0:
            raise ValueError("Range 不合法")
        length = min(int(end_text), total_size)
        return total_size - length, total_size - 1
    if not start_text.isdigit() or (end_text and not end_text.isdigit()):
        raise ValueError("Range 不合法")
    start = int(start_text)
    end = int(end_text) if end_text else total_size - 1
    if start >= total_size or end < start:
        raise ValueError("Range 超出文件范围")
    return start, min(end, total_size - 1)


@dataclass(frozen=True)
class _ValidatedUpload:
    filename: str
    extension: str
    mime_type: str
    byte_size: int
    sha256: str
    content_category: str
    note: str | None


class KnowledgeService:
    def __init__(
        self,
        store: CosKnowledgeStore,
        *,
        binary_parser_enabled: bool = False,
    ) -> None:
        self.store = store
        self.binary_parser_enabled = binary_parser_enabled

    async def accept_upload(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        user_id: int,
        idempotency_key: str,
        file: UploadFile,
        file_sha256: str,
        content_category: str,
        note: str | None,
        item_id: int | None = None,
    ) -> KnowledgeUploadResult:
        tenant_id = tenant_id.strip()
        key = idempotency_key.strip()
        if not tenant_id:
            raise CustomException(msg="租户上下文缺失", status_code=401)
        if not key or len(key) > 128:
            raise CustomException(msg="缺少或无效的 Idempotency-Key", status_code=400)
        upload = _validate_upload(
            file,
            file_sha256,
            content_category,
            note,
            binary_parser_enabled=self.binary_parser_enabled,
        )
        operation = "CREATE_VERSION" if item_id is not None else "CREATE_ITEM"
        fingerprint = _upload_fingerprint(operation, item_id, upload)

        replay = await self._find_replay(db, tenant_id, operation, key, fingerprint)
        if replay is not None:
            return replay

        now = datetime.now(timezone.utc)
        if item_id is None:
            item_id = generate_snowflake_id()
            version_no = 1
            item = AiCallKnowledgeItemModel(
                id=item_id,
                tenant_id=tenant_id,
                display_name=upload.filename,
                content_category=upload.content_category,
                note=upload.note,
                created_by=user_id,
                created_at=now,
                updated_at=now,
            )
            db.add(item)
        else:
            item = await db.scalar(
                select(AiCallKnowledgeItemModel)
                .where(
                    AiCallKnowledgeItemModel.id == item_id,
                    AiCallKnowledgeItemModel.tenant_id == tenant_id,
                    AiCallKnowledgeItemModel.deleted_at.is_(None),
                )
                .with_for_update()
            )
            if item is None:
                raise CustomException(msg="知识条目不存在", status_code=404)
            version_no = (
                await db.scalar(
                    select(func.max(AiCallKnowledgeVersionModel.version_no)).where(
                        AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                        AiCallKnowledgeVersionModel.knowledge_item_id == item_id,
                    )
                )
                or 0
            ) + 1
            item.content_category = upload.content_category
            item.note = upload.note
            item.updated_at = now

        version_id = generate_snowflake_id()
        object_key = f"knowledge/{tenant_id}/{item_id}/{version_id}/source.{upload.extension}"
        version = AiCallKnowledgeVersionModel(
            id=version_id,
            tenant_id=tenant_id,
            knowledge_item_id=item_id,
            version_no=version_no,
            status="UPLOADING",
            source_object_key=object_key,
            source_filename=upload.filename,
            extension=upload.extension,
            mime_type=upload.mime_type,
            byte_size=upload.byte_size,
            sha256=upload.sha256,
            upload_operation=operation,
            upload_idempotency_key=key,
            upload_request_fingerprint=fingerprint,
            created_by=user_id,
            created_at=now,
        )
        db.add(version)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            replay = await self._find_replay(
                db,
                tenant_id,
                operation,
                key,
                fingerprint,
            )
            if replay is not None:
                return replay
            raise

        try:
            file.file.seek(0)
            stored = await self.store.put(
                object_key,
                file.file,
                content_type=upload.mime_type,
                expected_size=upload.byte_size,
            )
            remote = await self.store.stat(object_key)
            if stored.byte_size != upload.byte_size or remote.byte_size != upload.byte_size:
                raise ValueError("COS 文件大小校验失败")
            if stored.sha256 != upload.sha256:
                raise _UploadChecksumMismatch
        except Exception as exc:
            try:
                await self.store.delete(object_key)
            except Exception:
                pass
            checksum_mismatch = isinstance(exc, _UploadChecksumMismatch)
            await self._mark_upload_failed(
                db,
                tenant_id=tenant_id,
                version_id=version_id,
                code="SHA256_MISMATCH" if checksum_mismatch else "COS_UPLOAD_FAILED",
                message=(
                    "文件 SHA-256 与声明不一致" if checksum_mismatch else "对象存储上传或校验失败"
                ),
            )
            raise CustomException(
                msg=(
                    "文件 SHA-256 与声明不一致" if checksum_mismatch else "文件上传失败，请稍后重试"
                ),
                status_code=400 if checksum_mismatch else 502,
            ) from exc

        try:
            changed = await db.execute(
                update(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.id == version_id,
                    AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                    AiCallKnowledgeVersionModel.status == "UPLOADING",
                )
                .values(
                    status="PROCESSING",
                    lease_owner=None,
                    lease_expires_at=None,
                )
            )
            if changed.rowcount != 1:
                raise RuntimeError("知识版本上传状态已变化")
            await db.commit()
        except Exception as exc:
            await db.rollback()
            raise CustomException(
                msg="文件已上传，版本状态待系统对账",
                status_code=503,
            ) from exc
        return KnowledgeUploadResult(item_id, version_id, "PROCESSING")

    async def _find_replay(
        self,
        db: AsyncSession,
        tenant_id: str,
        operation: str,
        key: str,
        fingerprint: str,
    ) -> KnowledgeUploadResult | None:
        version = await db.scalar(
            select(AiCallKnowledgeVersionModel).where(
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                AiCallKnowledgeVersionModel.upload_operation == operation,
                AiCallKnowledgeVersionModel.upload_idempotency_key == key,
            )
        )
        if version is None:
            return None
        if version.upload_request_fingerprint != fingerprint:
            raise CustomException(
                msg="Idempotency-Key 已用于不同的上传请求",
                status_code=409,
            )
        return KnowledgeUploadResult(
            item_id=version.knowledge_item_id,
            version_id=version.id,
            status=version.status,
            replayed=True,
        )

    async def _mark_upload_failed(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        version_id: int,
        code: str,
        message: str,
    ) -> None:
        await db.rollback()
        await db.execute(
            update(AiCallKnowledgeVersionModel)
            .where(
                AiCallKnowledgeVersionModel.id == version_id,
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                AiCallKnowledgeVersionModel.status == "UPLOADING",
            )
            .values(
                status="FAILED",
                lease_owner=None,
                lease_expires_at=None,
                failure_code=code,
                failure_message=message,
                failure_retryable=False,
            )
        )
        await db.commit()

    async def list_items(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        page_num: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        tenant_id = tenant_id.strip()
        if not tenant_id:
            raise CustomException(msg="租户上下文缺失", status_code=401)
        if page_num < 1 or not 1 <= page_size <= 100:
            raise CustomException(msg="分页参数不合法", status_code=400)

        visible_version = (
            select(AiCallKnowledgeVersionModel.id)
            .where(
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                AiCallKnowledgeVersionModel.knowledge_item_id == AiCallKnowledgeItemModel.id,
                AiCallKnowledgeVersionModel.status != "UPLOADING",
            )
            .exists()
        )
        conditions = (
            AiCallKnowledgeItemModel.tenant_id == tenant_id,
            AiCallKnowledgeItemModel.deleted_at.is_(None),
            visible_version,
        )
        total = int(
            await db.scalar(
                select(func.count()).select_from(AiCallKnowledgeItemModel).where(*conditions)
            )
            or 0
        )
        items = list(
            (
                await db.scalars(
                    select(AiCallKnowledgeItemModel)
                    .where(*conditions)
                    .order_by(
                        AiCallKnowledgeItemModel.updated_at.desc(),
                        AiCallKnowledgeItemModel.id.desc(),
                    )
                    .offset((page_num - 1) * page_size)
                    .limit(page_size)
                )
            ).all()
        )
        if not items:
            return [], total

        item_ids = [item.id for item in items]
        versions = list(
            (
                await db.scalars(
                    select(AiCallKnowledgeVersionModel)
                    .where(
                        AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                        AiCallKnowledgeVersionModel.knowledge_item_id.in_(item_ids),
                        AiCallKnowledgeVersionModel.status != "UPLOADING",
                    )
                    .order_by(
                        AiCallKnowledgeVersionModel.knowledge_item_id,
                        AiCallKnowledgeVersionModel.version_no.desc(),
                    )
                )
            ).all()
        )
        latest_versions: dict[int, AiCallKnowledgeVersionModel] = {}
        version_counts: dict[int, int] = {}
        for version in versions:
            version_counts[version.knowledge_item_id] = (
                version_counts.get(version.knowledge_item_id, 0) + 1
            )
            latest_versions.setdefault(version.knowledge_item_id, version)
        binding_counts = dict(
            (
                await db.execute(
                    select(
                        AiCallPromptKnowledgeBindingModel.knowledge_item_id,
                        func.count(),
                    )
                    .where(
                        AiCallPromptKnowledgeBindingModel.tenant_id == tenant_id,
                        AiCallPromptKnowledgeBindingModel.knowledge_item_id.in_(item_ids),
                    )
                    .group_by(AiCallPromptKnowledgeBindingModel.knowledge_item_id)
                )
            ).all()
        )
        return [
            self._item_to_dict(
                item,
                latest_version=latest_versions.get(item.id),
                version_count=version_counts.get(item.id, 0),
                binding_count=int(binding_counts.get(item.id, 0)),
            )
            for item in items
        ], total

    async def get_item(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        item_id: int,
    ) -> dict[str, Any]:
        item = await self._get_item_record(db, tenant_id, item_id)
        versions = await self._list_version_models(db, tenant_id, item_id)
        bindings = await self._scene_bindings(db, tenant_id, item_id)
        return self._item_to_dict(
            item,
            latest_version=versions[0],
            version_count=len(versions),
            binding_count=len(bindings),
            scene_bindings=bindings,
        )

    async def list_versions(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        item_id: int,
    ) -> list[dict[str, Any]]:
        await self._get_item_record(db, tenant_id, item_id)
        return [
            self._version_to_dict(version)
            for version in await self._list_version_models(db, tenant_id, item_id)
        ]

    async def update_item(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        item_id: int,
        changes: dict[str, Any],
    ) -> dict[str, Any]:
        values = _normalize_item_changes(changes)
        item = await self._get_item_record(db, tenant_id, item_id, for_update=True)
        for field, value in values.items():
            setattr(item, field, value)
        item.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return await self.get_item(db, tenant_id=tenant_id, item_id=item_id)

    async def replace_scene_bindings(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        item_id: int,
        prompt_profile_ids: list[int],
        user_id: int,
    ) -> list[dict[str, str]]:
        await self._get_item_record(db, tenant_id, item_id, for_update=True)
        profile_ids = sorted({int(profile_id) for profile_id in prompt_profile_ids})
        if len(profile_ids) > 100 or any(profile_id <= 0 for profile_id in profile_ids):
            raise CustomException(msg="提示词配置 ID 不合法", status_code=400)
        profiles = (
            list(
                (
                    await db.scalars(
                        select(AiCallPromptProfileModel).where(
                            AiCallPromptProfileModel.tenant_id == tenant_id,
                            AiCallPromptProfileModel.id.in_(profile_ids),
                        )
                    )
                ).all()
            )
            if profile_ids
            else []
        )
        if {profile.id for profile in profiles} != set(profile_ids):
            raise CustomException(msg="提示词配置不存在或无权管理", status_code=400)

        await db.execute(
            delete(AiCallPromptKnowledgeBindingModel).where(
                AiCallPromptKnowledgeBindingModel.tenant_id == tenant_id,
                AiCallPromptKnowledgeBindingModel.knowledge_item_id == item_id,
            )
        )
        now = datetime.now(timezone.utc)
        db.add_all([
            AiCallPromptKnowledgeBindingModel(
                id=generate_snowflake_id(),
                tenant_id=tenant_id,
                prompt_profile_id=profile.id,
                knowledge_item_id=item_id,
                created_by=user_id,
                created_at=now,
            )
            for profile in profiles
        ])
        await db.commit()
        return await self._scene_bindings(db, tenant_id, item_id)

    async def delete_item(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        item_id: int,
    ) -> dict[str, Any]:
        item = await self._get_item_record(db, tenant_id, item_id, for_update=True)
        now = datetime.now(timezone.utc)
        item.deleted_at = now
        item.updated_at = now
        await db.execute(
            delete(AiCallPromptKnowledgeBindingModel).where(
                AiCallPromptKnowledgeBindingModel.tenant_id == tenant_id,
                AiCallPromptKnowledgeBindingModel.knowledge_item_id == item_id,
            )
        )
        await db.commit()
        return {"itemId": str(item.id), "deletedAt": now}

    @staticmethod
    async def _get_item_record(
        db: AsyncSession,
        tenant_id: str,
        item_id: int,
        *,
        for_update: bool = False,
    ) -> AiCallKnowledgeItemModel:
        tenant_id = tenant_id.strip()
        visible_version = (
            select(AiCallKnowledgeVersionModel.id)
            .where(
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                AiCallKnowledgeVersionModel.knowledge_item_id == AiCallKnowledgeItemModel.id,
                AiCallKnowledgeVersionModel.status != "UPLOADING",
            )
            .exists()
        )
        statement = select(AiCallKnowledgeItemModel).where(
            AiCallKnowledgeItemModel.id == item_id,
            AiCallKnowledgeItemModel.tenant_id == tenant_id,
            AiCallKnowledgeItemModel.deleted_at.is_(None),
            visible_version,
        )
        if for_update:
            statement = statement.with_for_update()
        item = await db.scalar(statement)
        if item is None:
            raise CustomException(msg="知识条目不存在", status_code=404)
        return item

    @staticmethod
    async def _list_version_models(
        db: AsyncSession,
        tenant_id: str,
        item_id: int,
    ) -> list[AiCallKnowledgeVersionModel]:
        return list(
            (
                await db.scalars(
                    select(AiCallKnowledgeVersionModel)
                    .where(
                        AiCallKnowledgeVersionModel.tenant_id == tenant_id,
                        AiCallKnowledgeVersionModel.knowledge_item_id == item_id,
                        AiCallKnowledgeVersionModel.status != "UPLOADING",
                    )
                    .order_by(AiCallKnowledgeVersionModel.version_no.desc())
                )
            ).all()
        )

    @staticmethod
    async def _scene_bindings(
        db: AsyncSession,
        tenant_id: str,
        item_id: int,
    ) -> list[dict[str, str]]:
        rows = (
            await db.execute(
                select(
                    AiCallPromptKnowledgeBindingModel.prompt_profile_id,
                    AiCallPromptProfileModel.scene_code,
                    AiCallPromptProfileModel.name,
                )
                .join(
                    AiCallPromptProfileModel,
                    (
                        AiCallPromptProfileModel.id
                        == AiCallPromptKnowledgeBindingModel.prompt_profile_id
                    )
                    & (
                        AiCallPromptProfileModel.tenant_id
                        == AiCallPromptKnowledgeBindingModel.tenant_id
                    ),
                )
                .where(
                    AiCallPromptKnowledgeBindingModel.tenant_id == tenant_id,
                    AiCallPromptKnowledgeBindingModel.knowledge_item_id == item_id,
                )
                .order_by(AiCallPromptProfileModel.scene_code, AiCallPromptProfileModel.id)
            )
        ).all()
        return [
            {
                "promptProfileId": str(prompt_profile_id),
                "sceneCode": scene_code,
                "name": name,
            }
            for prompt_profile_id, scene_code, name in rows
        ]

    @staticmethod
    def _item_to_dict(
        item: AiCallKnowledgeItemModel,
        *,
        latest_version: AiCallKnowledgeVersionModel,
        version_count: int,
        binding_count: int,
        scene_bindings: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        result = {
            "id": str(item.id),
            "displayName": item.display_name,
            "contentCategory": item.content_category,
            "note": item.note,
            "currentReadyVersionId": (
                str(item.current_ready_version_id)
                if item.current_ready_version_id is not None
                else None
            ),
            "latestVersion": KnowledgeService._version_to_dict(latest_version),
            "versionCount": version_count,
            "bindingCount": binding_count,
            "createdBy": str(item.created_by) if item.created_by is not None else None,
            "createdAt": item.created_at,
            "updatedAt": item.updated_at,
        }
        if scene_bindings is not None:
            result["sceneBindings"] = scene_bindings
        return result

    @staticmethod
    def _version_to_dict(version: AiCallKnowledgeVersionModel) -> dict[str, Any]:
        return {
            "id": str(version.id),
            "itemId": str(version.knowledge_item_id),
            "versionNo": version.version_no,
            "status": version.status,
            "sourceFilename": version.source_filename,
            "extension": version.extension,
            "mimeType": version.mime_type,
            "byteSize": version.byte_size,
            "sha256": version.sha256,
            "parserName": version.parser_name,
            "parserVersion": version.parser_version,
            "chunkStrategyVersion": version.chunk_strategy_version,
            "chunkCount": version.chunk_count,
            "attemptCount": version.attempt_count,
            "failureCode": version.failure_code,
            "failureMessage": version.failure_message,
            "failureRetryable": version.failure_retryable,
            "createdBy": (str(version.created_by) if version.created_by is not None else None),
            "createdAt": version.created_at,
            "readyAt": version.ready_at,
        }

    async def get_processing(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        version_id: int,
    ) -> dict[str, Any]:
        version = await self._get_version(db, tenant_id, version_id)
        return {
            "itemId": str(version.knowledge_item_id),
            "versionId": str(version.id),
            "status": version.status,
            "attemptCount": version.attempt_count,
            "chunkCount": version.chunk_count,
            "warning": json.loads(version.processing_warning_json)
            if version.processing_warning_json
            else None,
            "failureCode": version.failure_code,
            "failureMessage": version.failure_message,
            "failureRetryable": version.failure_retryable,
        }

    async def open_download(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        version_id: int,
        range_header: str | None,
    ) -> KnowledgeDownload:
        version = await self._get_version(db, tenant_id, version_id)
        if version.status == "UPLOADING":
            raise CustomException(msg="文件仍在上传中", status_code=409)
        try:
            remote = await self.store.stat(version.source_object_key)
            byte_range = parse_byte_range(range_header, remote.byte_size)
            opened = await self.store.open(
                version.source_object_key,
                byte_range=byte_range,
            )
        except ValueError as exc:
            raise CustomException(msg=str(exc), status_code=416) from exc
        except Exception as exc:
            raise CustomException(msg="原文件暂时无法读取", status_code=502) from exc
        if byte_range is None:
            return KnowledgeDownload(
                version.source_filename,
                version.mime_type,
                200,
                remote.byte_size,
                None,
                opened.body,
            )
        start, end = byte_range
        return KnowledgeDownload(
            version.source_filename,
            version.mime_type,
            206,
            end - start + 1,
            f"bytes {start}-{end}/{remote.byte_size}",
            opened.body,
        )

    async def retry(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        version_id: int,
    ) -> dict[str, Any]:
        version = await db.scalar(
            select(AiCallKnowledgeVersionModel)
            .where(
                AiCallKnowledgeVersionModel.id == version_id,
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
            )
            .with_for_update()
        )
        if version is None:
            raise CustomException(msg="知识版本不存在", status_code=404)
        if version.status != "FAILED" or not version.failure_retryable:
            raise CustomException(msg="当前失败不可重试，请上传新版本", status_code=409)
        version.status = "PROCESSING"
        version.next_attempt_at = None
        version.lease_owner = None
        version.lease_expires_at = None
        version.failure_code = None
        version.failure_message = None
        await db.commit()
        return await self.get_processing(db, tenant_id=tenant_id, version_id=version_id)

    @staticmethod
    async def _get_version(
        db: AsyncSession,
        tenant_id: str,
        version_id: int,
    ) -> AiCallKnowledgeVersionModel:
        version = await db.scalar(
            select(AiCallKnowledgeVersionModel).where(
                AiCallKnowledgeVersionModel.id == version_id,
                AiCallKnowledgeVersionModel.tenant_id == tenant_id,
            )
        )
        if version is None:
            raise CustomException(msg="知识版本不存在", status_code=404)
        return version


def _normalize_item_changes(changes: dict[str, Any]) -> dict[str, Any]:
    allowed = {"display_name", "content_category", "note"}
    if not changes or set(changes) - allowed:
        raise CustomException(msg="没有可更新的知识条目字段", status_code=400)

    normalized: dict[str, Any] = {}
    if "display_name" in changes:
        value = unicodedata.normalize("NFKC", str(changes["display_name"] or "").strip())
        if not value or len(value) > 255 or any(ord(char) < 32 for char in value):
            raise CustomException(msg="知识名称不合法", status_code=400)
        normalized["display_name"] = value
    if "content_category" in changes:
        value = str(changes["content_category"] or "").strip().upper()
        if value not in _CONTENT_CATEGORIES:
            raise CustomException(msg="内容分类不合法", status_code=400)
        normalized["content_category"] = value
    if "note" in changes:
        value = str(changes["note"]).strip() if changes["note"] is not None else ""
        if len(value) > 1000:
            raise CustomException(msg="备注不能超过 1000 个字符", status_code=400)
        normalized["note"] = value or None
    return normalized


class _UploadChecksumMismatch(ValueError):
    pass


def _validate_upload(
    file: UploadFile,
    file_sha256: str,
    content_category: str,
    note: str | None,
    *,
    binary_parser_enabled: bool = False,
) -> _ValidatedUpload:
    filename = unicodedata.normalize("NFKC", (file.filename or "").strip())
    if (
        not filename
        or len(filename) > 255
        or "/" in filename
        or "\\" in filename
        or any(ord(char) < 32 for char in filename)
        or "." not in filename
    ):
        raise CustomException(msg="文件名不合法", status_code=400)
    extension = filename.rsplit(".", 1)[1].lower()
    text_extension = extension in {"txt", "md", "markdown"}
    if not text_extension and not (extension == "pptx" and binary_parser_enabled):
        raise CustomException(msg="当前只支持 TXT、Markdown 和已启用的 PPTX", status_code=400)
    mime_type = (file.content_type or "application/octet-stream").split(";", 1)[0].lower()
    supported_mime_types = (
        _SUPPORTED_MIME_TYPES
        if text_extension
        else {_PPTX_MIME_TYPE, "application/octet-stream"}
    )
    if mime_type not in supported_mime_types:
        raise CustomException(msg="文件 MIME 类型不支持", status_code=400)
    sha256 = file_sha256.strip().lower()
    if not _SHA256_RE.fullmatch(sha256):
        raise CustomException(msg="fileSha256 必须是 64 位十六进制", status_code=400)
    category = content_category.strip().upper()
    if category not in _CONTENT_CATEGORIES:
        raise CustomException(msg="内容分类不合法", status_code=400)
    normalized_note = (note or "").strip() or None
    if normalized_note is not None and len(normalized_note) > 1000:
        raise CustomException(msg="备注不能超过 1000 个字符", status_code=400)

    source = file.file
    source.seek(0, 2)
    byte_size = source.tell()
    source.seek(0)
    if byte_size <= 0:
        raise CustomException(msg="文件不能为空", status_code=400)
    if byte_size > MAX_UPLOAD_BYTES:
        raise CustomException(msg="文件正文不能超过 100 MB", status_code=413)
    prefix = source.read(min(512, byte_size))
    source.seek(0)
    if text_extension:
        if b"\x00" in prefix:
            raise CustomException(msg="文本文件包含 NUL 字节", status_code=400)
        try:
            codecs.getincrementaldecoder("utf-8-sig")().decode(prefix, final=False)
        except UnicodeDecodeError as exc:
            raise CustomException(msg="文本文件必须是 UTF-8 编码", status_code=400) from exc
    elif not prefix.startswith(b"PK\x03\x04"):
        raise CustomException(msg="PPTX 文件头不合法", status_code=400)
    return _ValidatedUpload(
        filename,
        extension,
        mime_type,
        byte_size,
        sha256,
        category,
        normalized_note,
    )


def _upload_fingerprint(
    operation: str,
    item_id: int | None,
    upload: _ValidatedUpload,
) -> str:
    payload = json.dumps(
        {
            "operation": operation,
            "itemId": item_id,
            "filename": upload.filename,
            "byteSize": upload.byte_size,
            "sha256": upload.sha256,
            "contentCategory": upload.content_category,
            "note": upload.note,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class _ClaimedVersion:
    id: int
    tenant_id: str
    item_id: int
    version_no: int
    object_key: str
    extension: str
    byte_size: int
    sha256: str
    attempt_count: int


@dataclass(frozen=True)
class _ClaimedUpload:
    id: int
    object_key: str
    byte_size: int


class KnowledgeWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        store: CosKnowledgeStore,
        *,
        worker_id: str,
        poll_interval_seconds: float = 2.0,
        lease_seconds: int = 300,
        upload_reconcile_after_seconds: int = 3600,
        binary_parser: KnowledgeBinaryParserClient | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.store = store
        self.worker_id = worker_id
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self.upload_reconcile_after_seconds = max(0, upload_reconcile_after_seconds)
        self.binary_parser = binary_parser
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="ai-call-knowledge-worker")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            worked = await self.run_once()
            if worked:
                continue
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                pass

    async def run_once(self) -> bool:
        reconciled = await self._reconcile_stale_upload()
        claimed = await self._claim()
        if claimed is None:
            return reconciled
        try:
            parsed = await self._download_and_parse(claimed)
            await self._complete(claimed, parsed)
        except KnowledgeTextParseError:
            await self._fail(
                claimed,
                code="TEXT_PARSE_FAILED",
                message="文件不是可用的 UTF-8 TXT/Markdown",
                retryable=False,
            )
        except KnowledgeBinaryParseError:
            await self._fail(
                claimed,
                code="BINARY_PARSE_FAILED",
                message="PPTX 文件不满足安全解析合同或没有可用正文",
                retryable=False,
            )
        except _SourceChecksumMismatch:
            await self._fail(
                claimed,
                code="SOURCE_CHECKSUM_MISMATCH",
                message="COS 原文件校验失败",
                retryable=False,
            )
        except Exception as exc:
            log.warning(
                "AI Call 知识处理失败：version_id={}, error_type={}",
                claimed.id,
                type(exc).__name__,
            )
            await self._fail(
                claimed,
                code="SOURCE_READ_FAILED",
                message="读取或处理原文件失败",
                retryable=True,
            )
        return True

    async def _reconcile_stale_upload(self) -> bool:
        claimed = await self._claim_stale_upload()
        if claimed is None:
            return False
        try:
            remote = await self.store.stat_or_none(claimed.object_key)
            if remote is not None and remote.byte_size != claimed.byte_size:
                await self.store.delete(claimed.object_key)
                remote = None
            await self._finish_upload_reconciliation(
                claimed,
                complete=remote is not None,
            )
        except Exception as exc:
            log.warning(
                "AI Call 知识上传对账失败：version_id={}, error_type={}",
                claimed.id,
                type(exc).__name__,
            )
        return True

    async def _claim_stale_upload(self) -> _ClaimedUpload | None:
        async with self.session_factory() as db, db.begin():
            now = datetime.now(timezone.utc)
            version = await db.scalar(
                select(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.status == "UPLOADING",
                    AiCallKnowledgeVersionModel.created_at
                    <= now - timedelta(seconds=self.upload_reconcile_after_seconds),
                    or_(
                        AiCallKnowledgeVersionModel.lease_expires_at.is_(None),
                        AiCallKnowledgeVersionModel.lease_expires_at <= now,
                    ),
                )
                .order_by(AiCallKnowledgeVersionModel.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if version is None:
                return None
            version.lease_owner = self.worker_id
            version.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            return _ClaimedUpload(
                id=version.id,
                object_key=version.source_object_key,
                byte_size=version.byte_size,
            )

    async def _finish_upload_reconciliation(
        self,
        claimed: _ClaimedUpload,
        *,
        complete: bool,
    ) -> None:
        async with self.session_factory() as db, db.begin():
            version = await db.scalar(
                select(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.id == claimed.id,
                    AiCallKnowledgeVersionModel.status == "UPLOADING",
                    AiCallKnowledgeVersionModel.lease_owner == self.worker_id,
                )
                .with_for_update()
            )
            if version is None:
                return
            version.status = "PROCESSING" if complete else "FAILED"
            version.lease_owner = None
            version.lease_expires_at = None
            version.next_attempt_at = None
            if complete:
                version.failure_code = None
                version.failure_message = None
                version.failure_retryable = False
            else:
                version.failure_code = "UPLOAD_INCOMPLETE"
                version.failure_message = "上传未完成，请重新上传文件"
                version.failure_retryable = False

    async def _claim(self) -> _ClaimedVersion | None:
        async with self.session_factory() as db, db.begin():
            now = datetime.now(timezone.utc)
            version = await db.scalar(
                select(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.status == "PROCESSING",
                    or_(
                        AiCallKnowledgeVersionModel.next_attempt_at.is_(None),
                        AiCallKnowledgeVersionModel.next_attempt_at <= now,
                    ),
                    or_(
                        AiCallKnowledgeVersionModel.lease_expires_at.is_(None),
                        AiCallKnowledgeVersionModel.lease_expires_at <= now,
                    ),
                )
                .order_by(AiCallKnowledgeVersionModel.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if version is None:
                return None
            version.attempt_count += 1
            version.lease_owner = self.worker_id
            version.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            return _ClaimedVersion(
                version.id,
                version.tenant_id,
                version.knowledge_item_id,
                version.version_no,
                version.source_object_key,
                version.extension,
                version.byte_size,
                version.sha256,
                version.attempt_count,
            )

    async def _download_and_parse(self, claimed: _ClaimedVersion) -> ParsedKnowledge:
        remote = await self.store.stat(claimed.object_key)
        if remote.byte_size != claimed.byte_size:
            raise _SourceChecksumMismatch
        opened = await self.store.open(claimed.object_key)
        with TemporaryDirectory(prefix="ai-call-knowledge-") as directory:
            path = Path(directory) / f"source.{claimed.extension}"
            digest = hashlib.sha256()
            byte_size = 0
            with path.open("wb") as destination:
                async for chunk in opened.body:
                    byte_size += len(chunk)
                    if byte_size > MAX_UPLOAD_BYTES:
                        raise _SourceChecksumMismatch
                    digest.update(chunk)
                    destination.write(chunk)
            if byte_size != claimed.byte_size or digest.hexdigest() != claimed.sha256:
                raise _SourceChecksumMismatch
            if claimed.extension in {"txt", "md", "markdown"}:
                return parse_text_knowledge(path.read_bytes(), extension=claimed.extension)
            if claimed.extension == "pptx" and self.binary_parser is not None:
                payload = await asyncio.to_thread(
                    self.binary_parser.parse,
                    path,
                    extension=claimed.extension,
                )
                return _parse_binary_result(payload)
            raise KnowledgeBinaryParseError("二进制解析器未配置")

    async def _complete(
        self,
        claimed: _ClaimedVersion,
        parsed: ParsedKnowledge,
    ) -> None:
        async with self.session_factory() as db, db.begin():
            version = await db.scalar(
                select(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.id == claimed.id,
                    AiCallKnowledgeVersionModel.status == "PROCESSING",
                    AiCallKnowledgeVersionModel.lease_owner == self.worker_id,
                    AiCallKnowledgeVersionModel.attempt_count == claimed.attempt_count,
                )
                .with_for_update()
            )
            if version is None:
                return
            await db.execute(
                delete(AiCallKnowledgeChunkModel).where(
                    AiCallKnowledgeChunkModel.tenant_id == claimed.tenant_id,
                    AiCallKnowledgeChunkModel.knowledge_version_id == claimed.id,
                )
            )
            now = datetime.now(timezone.utc)
            db.add_all([
                AiCallKnowledgeChunkModel(
                    id=generate_snowflake_id(),
                    tenant_id=claimed.tenant_id,
                    knowledge_version_id=claimed.id,
                    chunk_index=chunk.chunk_index,
                    content=chunk.content,
                    content_checksum=chunk.content_checksum,
                    content_type=chunk.content_type,
                    source_type=chunk.source_type,
                    page_no=chunk.page_no,
                    section_path=chunk.section_path,
                    source_path=chunk.source_path,
                    start_ms=chunk.start_ms,
                    end_ms=chunk.end_ms,
                    created_at=now,
                )
                for chunk in parsed.chunks
            ])
            version.status = "READY"
            version.parser_name = parsed.parser_name
            version.parser_version = parsed.parser_version
            version.chunk_strategy_version = parsed.chunk_strategy_version
            version.chunk_count = len(parsed.chunks)
            version.chunk_set_sha256 = parsed.chunk_set_sha256
            version.ready_at = now
            version.next_attempt_at = None
            version.lease_owner = None
            version.lease_expires_at = None
            version.failure_code = None
            version.failure_message = None
            version.failure_retryable = False

            item = await db.scalar(
                select(AiCallKnowledgeItemModel)
                .where(
                    AiCallKnowledgeItemModel.id == claimed.item_id,
                    AiCallKnowledgeItemModel.tenant_id == claimed.tenant_id,
                )
                .with_for_update()
            )
            if item is None:
                raise RuntimeError("知识条目不存在")
            current_version_no = 0
            if item.current_ready_version_id is not None:
                current_version_no = (
                    await db.scalar(
                        select(AiCallKnowledgeVersionModel.version_no).where(
                            AiCallKnowledgeVersionModel.id == item.current_ready_version_id,
                            AiCallKnowledgeVersionModel.tenant_id == claimed.tenant_id,
                        )
                    )
                    or 0
                )
            if claimed.version_no > current_version_no:
                item.current_ready_version_id = claimed.id
                item.updated_at = now

    async def _fail(
        self,
        claimed: _ClaimedVersion,
        *,
        code: str,
        message: str,
        retryable: bool,
    ) -> None:
        async with self.session_factory() as db, db.begin():
            version = await db.scalar(
                select(AiCallKnowledgeVersionModel)
                .where(
                    AiCallKnowledgeVersionModel.id == claimed.id,
                    AiCallKnowledgeVersionModel.status == "PROCESSING",
                    AiCallKnowledgeVersionModel.lease_owner == self.worker_id,
                    AiCallKnowledgeVersionModel.attempt_count == claimed.attempt_count,
                )
                .with_for_update()
            )
            if version is None:
                return
            should_retry = retryable and claimed.attempt_count < 3
            version.status = "PROCESSING" if should_retry else "FAILED"
            version.next_attempt_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=5 * (2 ** (claimed.attempt_count - 1)))
                if should_retry
                else None
            )
            version.lease_owner = None
            version.lease_expires_at = None
            version.failure_code = code
            version.failure_message = message
            version.failure_retryable = retryable


class _SourceChecksumMismatch(ValueError):
    pass
