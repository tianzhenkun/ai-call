from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from lingchen_knowledge.content import build_content
from lingchen_knowledge.documents import material_document_policy, parse_document_bytes
from psycopg import ClientCursor
from sqlalchemy import create_engine, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException
from app.services.ai_call.knowledge import (
    CosKnowledgeStore,
    KnowledgeRealtimeSearchService,
    KnowledgeSearchService,
    KnowledgeService,
)
from app.services.ai_call.knowledge_processing import chunk_set_hash, version_chunks
from app.services.ai_call.session_registry import KnowledgeRuntimeContext

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT / "docs/livekit-ai-outbound/sql/phase-j1-knowledge-lexical-postgres.sql"
)


def _dsn() -> str:
    value = os.getenv("AI_CALL_TEST_POSTGRES_DSN", "").strip()
    if not value:
        pytest.fail("AI_CALL_TEST_POSTGRES_DSN 未配置，必须通过隔离 PostgreSQL 脚本运行")
    return value


def _psycopg_dsn() -> str:
    return _dsn().replace("postgresql+asyncpg://", "postgresql://", 1)


def _sqlalchemy_dsn() -> str:
    return _dsn().replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)


def _execute(sql: str) -> None:
    with psycopg.connect(
        _psycopg_dsn(),
        autocommit=True,
        cursor_factory=ClientCursor,
    ) as connection:
        connection.execute(sql)


def _reset_schema() -> None:
    _execute(
        """
        drop table if exists ai_call_knowledge_usage cascade;
        drop table if exists ai_call_prompt_knowledge_binding cascade;
        drop table if exists ai_call_knowledge_chunk cascade;
        drop table if exists ai_call_knowledge_version cascade;
        drop table if exists ai_call_knowledge_item cascade;
        drop table if exists ai_call_prompt_profile cascade;
        drop table if exists ai_call_outbound_task cascade;
        drop function if exists ai_call_knowledge_ngram_tsquery(text);
        drop function if exists ai_call_knowledge_ngram_tsvector(text);
        drop function if exists ai_call_knowledge_ngrams(text, integer);
        drop function if exists ai_call_knowledge_normalize(text);
        """
    )


def _create_portable_schema() -> None:
    engine = create_engine(_sqlalchemy_dsn())
    try:
        MappedBase.metadata.create_all(
            engine,
            tables=[
                AiCallPromptProfileModel.__table__,
                AiCallKnowledgeItemModel.__table__,
                AiCallKnowledgeVersionModel.__table__,
                AiCallKnowledgeChunkModel.__table__,
                AiCallPromptKnowledgeBindingModel.__table__,
                AiCallKnowledgeUsageModel.__table__,
                AiCallOutboundTaskModel.__table__,
            ],
        )
    finally:
        engine.dispose()


def _insert_parsed_version_content() -> str:
    parsed = build_content(
        parse_document_bytes(
            "售后.md",
            "# 售后政策\n\n退款将在审核通过后五个工作日内原路退回。".encode(),
            policy=material_document_policy(),
        )
    )
    chunks = version_chunks(11, "tenant-a", "md", parsed, datetime.now(UTC))
    with psycopg.connect(_psycopg_dsn(), cursor_factory=ClientCursor) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                insert into ai_call_knowledge_chunk (
                    id, tenant_id, knowledge_version_id, chunk_index, content,
                    content_checksum, content_type, source_type, section_path,
                    source_path, created_at
                ) values (
                    %s, 'tenant-a', 11, %s, %s, %s, %s, %s, %s, %s, now()
                )
                """,
                [
                    (
                        100 + chunk.chunk_index,
                        chunk.chunk_index,
                        chunk.content,
                        chunk.content_checksum,
                        chunk.content_type,
                        chunk.source_type,
                        chunk.section_path,
                        chunk.source_path,
                    )
                    for chunk in chunks
                ],
            )
        connection.execute(
            """
            update ai_call_knowledge_version
            set status = 'READY',
                parser_name = %s,
                parser_version = %s,
                chunk_strategy_version = %s,
                chunk_count = %s,
                chunk_set_sha256 = %s,
                ready_at = now()
            where id = 11 and status = 'PROCESSING'
            """,
            (
                "lingchen-knowledge",
                parsed.parser_version,
                parsed.chunker_version,
                len(parsed.chunks),
                chunk_set_hash(parsed),
            ),
        )
    return chunk_set_hash(parsed)


async def test_knowledge_migration_and_search_enforce_all_frozen_scope() -> None:
    _reset_schema()
    _create_portable_schema()
    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    _execute(
        """
        alter table ai_call_knowledge_version
            drop column provider_run_no, drop column provider_name, drop column provider_request_key,
            drop column provider_job_id, drop column provider_started_at;

        insert into ai_call_knowledge_item (
            id, tenant_id, display_name, content_category, created_by, created_at, updated_at
        ) values
            (1, 'tenant-a', 'A资料', 'PRODUCT_SERVICE', 7, now(), now()),
            (2, 'tenant-b', 'B资料', 'PRODUCT_SERVICE', 7, now(), now()),
            (3, 'tenant-a', '其他账号资料', 'PRODUCT_SERVICE', 8, now(), now()),
            (4, 'tenant-a', '无主资料', 'PRODUCT_SERVICE', null, now(), now());

        insert into ai_call_prompt_profile (
            id, tenant_id, scene_code, name, provider_key, product_info,
            variables_json, created_at, updated_at
        ) values
            (101, 'tenant-a', 'intro_contract', '合同介绍', 'static_profile',
             '', '[]', now(), now()),
            (201, 'tenant-b', 'other', '其他租户场景', 'static_profile',
             '', '[]', now(), now());

        insert into ai_call_knowledge_version (
            id, tenant_id, knowledge_item_id, version_no, status,
            source_object_key, source_filename, extension, mime_type,
            byte_size, sha256, parser_name, parser_version,
            chunk_strategy_version, chunk_count, chunk_set_sha256,
            created_at, ready_at
        ) values
            (11, 'tenant-a', 1, 1, 'PROCESSING', 'a/11', 'a.md', 'md',
             'text/markdown', 10, repeat('1', 64), null, null, null, 0,
             null, now(), null),
            (12, 'tenant-a', 1, 2, 'READY', 'a/12', 'a2.md', 'md',
             'text/markdown', 10, repeat('2', 64), 'text', 'v1', 'c1', 1,
             repeat('b', 64), now(), now()),
            (13, 'tenant-a', 1, 3, 'FAILED', 'a/13', 'a3.md', 'md',
             'text/markdown', 10, repeat('3', 64), 'text', 'v1', 'c1', 1,
             repeat('c', 64), now(), null),
            (21, 'tenant-b', 2, 1, 'READY', 'b/21', 'b.md', 'md',
             'text/markdown', 10, repeat('4', 64), 'text', 'v1', 'c1', 1,
             repeat('d', 64), now(), now()),
            (31, 'tenant-a', 3, 1, 'READY', 'a/31', 'other.md', 'md',
             'text/markdown', 10, repeat('5', 64), 'text', 'v1', 'c1', 1,
             repeat('d', 64), now(), now()),
            (41, 'tenant-a', 4, 1, 'READY', 'a/41', 'unowned.md', 'md',
             'text/markdown', 10, repeat('6', 64), 'text', 'v1', 'c1', 1,
             repeat('d', 64), now(), now());

        insert into ai_call_knowledge_chunk (
            id, tenant_id, knowledge_version_id, chunk_index, content,
            content_checksum, content_type, source_type, source_path, created_at
        ) values
            (102, 'tenant-a', 12, 0, '退款政策已经更新为十个工作日。',
             repeat('2', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now()),
            (103, 'tenant-a', 13, 0, '退款将在失败版本中立即到账。',
             repeat('3', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now()),
            (201, 'tenant-b', 21, 0, '退款将在另一个租户中当天到账。',
             repeat('4', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now()),
            (301, 'tenant-a', 31, 0, '退款将在其他账号中一个工作日内原路退回。',
             repeat('5', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now()),
            (401, 'tenant-a', 41, 0, '退款将在无主资料中两个工作日内原路退回。',
             repeat('6', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now());
        """
    )
    _execute(migration)
    _execute(migration)
    with psycopg.connect(_psycopg_dsn()) as connection:
        assert connection.execute(
            "select status, provider_run_no, provider_name, provider_job_id, provider_request_key, "
            "provider_started_at from ai_call_knowledge_version where id = 12"
        ).fetchone() == ("READY", 1, None, None, None, None)
    parsed_chunk_set_sha256 = _insert_parsed_version_content()

    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            hits = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                user_id=7,
                frozen_version_ids=[11, 13, 21, 31, 41],
                query="退款审核后多久原路退回",
            )
            other_hits = await KnowledgeSearchService.search(
                session, tenant_id="tenant-a", user_id=8,
                frozen_version_ids=[11, 31, 41], query="退款原路退回",
            )
            assert [hit.version_id for hit in other_hits] == [31]
            assert await KnowledgeSearchService.search(
                session, tenant_id="tenant-a", user_id=9,
                frozen_version_ids=[11, 31, 41], query="退款原路退回",
            ) == []
            no_hit = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                user_id=7,
                frozen_version_ids=[11],
                query="完全不相关的天气预报",
            )
            short_query = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                user_id=7,
                frozen_version_ids=[11],
                query="退",
            )
            with pytest.raises(ValueError, match="500"):
                await KnowledgeSearchService.search(
                    session,
                    tenant_id="tenant-a",
                    user_id=7,
                    frozen_version_ids=[11],
                    query="x" * 501,
                )
            with pytest.raises(ValueError, match="1 到 5"):
                await KnowledgeSearchService.search(
                    session,
                    tenant_id="tenant-a",
                    user_id=7,
                    frozen_version_ids=[11],
                    query="退款",
                    limit=6,
                )

            service = KnowledgeService(
                CosKnowledgeStore(client=object(), bucket="test", prefix="ai-call")
            )
            items, total = await service.list_items(
                session,
                tenant_id="tenant-a",
                user_id=7,
                page_num=1,
                page_size=20,
            )
            assert total == 1
            assert items[0]["id"] == "1"
            bindings = await service.replace_scene_bindings(
                session,
                tenant_id="tenant-a",
                item_id=1,
                prompt_profile_ids=[101],
                user_id=7,
            )
            assert bindings[0]["promptProfileId"] == "101"
            frozen_at = datetime.now(UTC)
            session.add(
                AiCallPromptKnowledgeBindingModel(
                    id=301,
                    tenant_id="tenant-b",
                    prompt_profile_id=101,
                    knowledge_item_id=2,
                    created_by=7,
                    created_at=frozen_at,
                )
            )
            await session.execute(
                update(AiCallKnowledgeItemModel)
                .where(AiCallKnowledgeItemModel.id == 1)
                .values(current_ready_version_id=11)
            )
            await session.execute(
                update(AiCallKnowledgeItemModel)
                .where(AiCallKnowledgeItemModel.id == 2)
                .values(current_ready_version_id=21)
            )
            await session.commit()
            frozen = await OutboundRuleTaskService._freeze_knowledge(
                session,
                tenant_id="tenant-a",
                user_id=7,
                prompt_profile_id=101,
                frozen_at=frozen_at,
            )
            hash_payload = [
                {
                    "id": "11",
                    "sha256": "1" * 64,
                    "chunkSetSha256": parsed_chunk_set_sha256,
                }
            ]
            assert frozen == {
                "promptProfileId": "101",
                "versionIds": ["11"],
                "versionSnapshotHash": hashlib.sha256(
                    json.dumps(
                        hash_payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode()
                ).hexdigest(),
                "retrieverVersion": "postgres-ngram-tsvector-v1",
                "frozenAt": frozen_at.isoformat(),
            }
            runtime_service = KnowledgeRealtimeSearchService(
                sessions,
                timeout_seconds=1,
                model_name="qwen-realtime-test",
            )
            session.add(AiCallOutboundTaskModel(
                id=701, tenant_id="tenant-a", validation_id=1,
                idempotency_key="knowledge-task", request_fingerprint="a" * 64,
                task_name="知识检索验证", task_mode="ai", status="COMPLETED",
                execution_mode="immediate", prompt_name="场景", scene_code="intro_contract",
                voice="test", rule_id=1, rule_name="test", rule_summary="test",
                config_snapshot_json="{}", created_by=7,
                created_at=frozen_at, updated_at=frozen_at,
            ))
            await session.commit()
            runtime_result = await runtime_service.search(
                context=KnowledgeRuntimeContext(
                    tenant_id="tenant-a",
                    task_id=701,
                    prompt_profile_id=101,
                    version_ids=(11,),
                    version_snapshot_hash=str(frozen["versionSnapshotHash"]),
                    retriever_version="postgres-ngram-tsvector-v1",
                ),
                call_id="call-runtime-1",
                customer_transcript_event_id="evt-customer-1",
                tool_call_id="tool-1",
                query="退款审核后多久原路退回",
            )
            assert runtime_result.status == "ok"
            assert runtime_result.audit_id is not None
            assert runtime_result.output["evidenceType"] == "untrusted_business_data"
            assert runtime_result.output["evidence"][0]["versionId"] == "11"
            await runtime_service.link_tool_result(
                runtime_result.audit_id,
                "evt-tool-result-1",
            )
            await runtime_service.link_answer(
                [runtime_result.audit_id],
                event_id="evt-answer-1",
                response_id="resp-1",
            )
            invalid_runtime_result = await runtime_service.search(
                context=KnowledgeRuntimeContext(
                    tenant_id="tenant-a",
                    task_id=702,
                    prompt_profile_id=101,
                    version_ids=(11,),
                    version_snapshot_hash="f" * 64,
                    retriever_version="postgres-ngram-tsvector-v1",
                ),
                call_id="call-runtime-invalid",
                customer_transcript_event_id="evt-customer-2",
                tool_call_id="tool-2",
                query="退款审核后多久原路退回",
            )
            assert invalid_runtime_result.status == "failed"
            assert invalid_runtime_result.output["evidence"] == []
            with pytest.raises(CustomException) as cross_tenant:
                await service.replace_scene_bindings(
                    session,
                    tenant_id="tenant-a",
                    item_id=1,
                    prompt_profile_ids=[201],
                    user_id=7,
                )
            assert cross_tenant.value.status_code == 400
            await session.execute(update(AiCallKnowledgeVersionModel).where(
                AiCallKnowledgeVersionModel.id == 11,
            ).values(extension="pptx"))
            assert await KnowledgeSearchService.search(
                session, tenant_id="tenant-a", user_id=7, frozen_version_ids=[11],
                query="退款审核后多久原路退回",
            ) == []
    finally:
        await engine.dispose()

    assert [(hit.chunk_id, hit.version_id) for hit in hits] == [(101, 11)]
    assert "五个工作日" in hits[0].content
    assert len(parsed_chunk_set_sha256) == 64
    assert no_hit == []
    assert short_query == []

    with psycopg.connect(_psycopg_dsn(), cursor_factory=ClientCursor) as connection:
        indexes = {
            row[0]
            for row in connection.execute(
                """
                select indexname
                from pg_indexes
                where tablename = 'ai_call_knowledge_chunk'
                """
            ).fetchall()
        }
        generated = dict(
            connection.execute(
                """
                select column_name, is_generated
                from information_schema.columns
                where table_name = 'ai_call_knowledge_chunk'
                  and column_name in ('normalized_content', 'ngram_tsv')
                """
            ).fetchall()
        )
        binding_constraints = {
            row[0]
            for row in connection.execute(
                """
                select conname
                from pg_constraint
                where conrelid = 'ai_call_prompt_knowledge_binding'::regclass
                """
            ).fetchall()
        }
        usage_indexes = {
            row[0]
            for row in connection.execute(
                """
                select indexname
                from pg_indexes
                where tablename = 'ai_call_knowledge_usage'
                """
            ).fetchall()
        }
        usage_constraints = {
            row[0]
            for row in connection.execute(
                """
                select conname
                from pg_constraint
                where conrelid = 'ai_call_knowledge_usage'::regclass
                """
            ).fetchall()
        }
        runtime_usage = connection.execute(
            """
            select purpose, task_id, call_id, customer_transcript_event_id,
                   tool_call_id, tool_result_event_id, answer_event_id,
                   qwen_response_id, status, query_excerpt_redacted,
                   knowledge_version_ids, retriever_version, model_name
            from ai_call_knowledge_usage
            where call_id = 'call-runtime-1'
            """
        ).fetchone()

    assert "idx_ai_call_knowledge_chunk_ngram_tsv" in indexes
    assert generated == {"ngram_tsv": "ALWAYS"}
    assert "uk_ai_call_prompt_knowledge_binding" in binding_constraints
    assert "idx_ai_call_knowledge_usage_tenant_created" in usage_indexes
    assert "idx_ai_call_knowledge_usage_call_created" in usage_indexes
    assert "ck_ai_call_knowledge_usage_purpose" in usage_constraints
    assert "ck_ai_call_knowledge_usage_status" in usage_constraints
    assert runtime_usage == (
        "REALTIME_ANSWER",
        701,
        "call-runtime-1",
        "evt-customer-1",
        "tool-1",
        "evt-tool-result-1",
        "evt-answer-1",
        "resp-1",
        "OK",
        None,
        '["11"]',
        "postgres-ngram-tsvector-v1",
        "qwen-realtime-test",
    )


async def test_word_search_uses_the_heading_to_locate_only_its_body(monkeypatch) -> None:
    from lingchen_knowledge.models import Block, Parsed

    from app.services.ai_call import knowledge

    _reset_schema()
    _create_portable_schema()
    _execute(MIGRATION_PATH.read_text(encoding="utf-8"))
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    heading = "蓝桥接待名额"
    body = "每场上限八人。"
    second_heading = "交通路线"
    second_body = "请在东门集合。"
    originals = {
        11: build_content(Parsed("docx-xml-v2", (Block(heading, role="heading"),))),
        12: build_content(Parsed("docx-xml-v2", (
            Block(heading, role="heading"), Block(body, role="body"),
            Block(second_heading, role="heading"), Block(second_body, role="body"),
        ))),
    }
    originals[13] = originals[12]
    originals[14] = originals[11]

    reads = []

    async def read_old(version):
        assert version.id in {13, 14}
        reads.append(version.id)
        return originals[version.id]

    try:
        async with sessions.begin() as db:
            for vid, content in originals.items():
                db.add(AiCallKnowledgeItemModel(
                    id=vid, tenant_id="local", display_name=f"{vid}.docx", content_category="FAQ",
                    created_by=7, created_at=now, updated_at=now,
                ))
                chunks = list(version_chunks(vid, "local", "docx", content, now))
                if vid in {13, 14}:
                    # 老版本可能把标题和多段正文存为同一个不可变切片。
                    chunks = chunks[:1]
                    chunks[0].content_type = "TEXT"
                    chunks[0].content = content.text
                    chunks[0].content_checksum = hashlib.sha256(content.text.encode()).hexdigest()
                db.add(AiCallKnowledgeVersionModel(
                    id=vid, tenant_id="local", knowledge_item_id=vid, version_no=1, status="READY",
                    source_object_key=f"word/{vid}", source_filename=f"{vid}.docx", extension="docx",
                    mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    byte_size=100, sha256="a" * 64, chunk_count=len(chunks), chunk_set_sha256="b" * 64,
                    created_at=now, ready_at=now,
                ))
                db.add_all(chunks)
        async with sessions() as db:
            async def search(vid, question, limit=5):
                return await KnowledgeSearchService.search(
                    db, tenant_id="local", user_id=7, frozen_version_ids=[vid], query=question, limit=limit,
                )

            assert await search(11, heading) == []
            monkeypatch.setattr(knowledge, "read_legacy_word", read_old)
            assert await search(14, heading) == []
            for vid in (12, 13):
                hits = await search(vid, heading)
                assert [(h.section_path, h.content) for h in hits] == [(heading, body)]
                assert hits[0].content_checksum == hashlib.sha256(body.encode()).hexdigest()
                assert [h.content for h in await search(vid, second_heading)] == [second_body]
                assert [h.content for h in await search(vid, "八人", limit=1)] == [body]
                assert [h.content for h in await search(vid, heading + "东门", limit=1)] == [second_body]
                before = len(reads)
                assert await search(vid, "秋天落叶") == []
                assert len(reads) == before
            before = len(reads)
            assert await KnowledgeSearchService.search(
                db, tenant_id="local", user_id=8, frozen_version_ids=[13], query=heading,
            ) == []
            assert len(reads) == before
    finally:
        await engine.dispose()
