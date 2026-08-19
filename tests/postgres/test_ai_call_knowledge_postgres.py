from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest
from psycopg import ClientCursor
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.model import (
    AiCallKnowledgeChunkModel,
    AiCallKnowledgeItemModel,
    AiCallKnowledgeUsageModel,
    AiCallKnowledgeVersionModel,
    AiCallPromptKnowledgeBindingModel,
)
from app.core.base_model import MappedBase
from app.services.ai_call.knowledge import (
    CosKnowledgeStore,
    KnowledgeSearchService,
    KnowledgeService,
    parse_text_knowledge,
)

pytestmark = pytest.mark.anyio

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = (
    PROJECT_ROOT / "docs/livekit-ai-outbound/sql/phase-j1-knowledge-lexical-postgres.sql"
)
PROMPT_TENANT_MIGRATION_PATH = (
    PROJECT_ROOT
    / "docs/livekit-ai-outbound/sql/phase-j2-prompt-profile-tenant-postgres.sql"
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
                AiCallKnowledgeItemModel.__table__,
                AiCallKnowledgeVersionModel.__table__,
                AiCallKnowledgeChunkModel.__table__,
                AiCallPromptKnowledgeBindingModel.__table__,
                AiCallKnowledgeUsageModel.__table__,
            ],
        )
    finally:
        engine.dispose()


def _insert_parsed_version_content() -> str:
    parsed = parse_text_knowledge(
        "# 售后政策\n\n退款将在审核通过后五个工作日内原路退回。".encode(),
        extension=".md",
    )
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
                        101 + chunk.chunk_index,
                        chunk.chunk_index,
                        chunk.content,
                        chunk.content_checksum,
                        chunk.content_type,
                        chunk.source_type,
                        chunk.section_path,
                        chunk.source_path,
                    )
                    for chunk in parsed.chunks
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
                parsed.parser_name,
                parsed.parser_version,
                parsed.chunk_strategy_version,
                len(parsed.chunks),
                parsed.chunk_set_sha256,
            ),
        )
    return parsed.chunk_set_sha256


async def test_knowledge_migration_and_search_enforce_all_frozen_scope() -> None:
    _reset_schema()
    _create_portable_schema()
    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    _execute(migration)
    _execute(migration)
    _execute(
        """
        insert into ai_call_knowledge_item (
            id, tenant_id, display_name, content_category, created_at, updated_at
        ) values
            (1, 'tenant-a', 'A资料', 'PRODUCT_SERVICE', now(), now()),
            (2, 'tenant-b', 'B资料', 'PRODUCT_SERVICE', now(), now());

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
             repeat('4', 64), 'TEXT', 'MARKDOWN', 'lines:1-1', now());
        """
    )
    parsed_chunk_set_sha256 = _insert_parsed_version_content()

    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session:
            hits = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                frozen_version_ids=[11, 13, 21],
                query="退款审核后多久原路退回",
            )
            no_hit = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                frozen_version_ids=[11],
                query="完全不相关的天气预报",
            )
            short_query = await KnowledgeSearchService.search(
                session,
                tenant_id="tenant-a",
                frozen_version_ids=[11],
                query="退",
            )
            with pytest.raises(ValueError, match="500"):
                await KnowledgeSearchService.search(
                    session,
                    tenant_id="tenant-a",
                    frozen_version_ids=[11],
                    query="x" * 501,
                )
            with pytest.raises(ValueError, match="1 到 5"):
                await KnowledgeSearchService.search(
                    session,
                    tenant_id="tenant-a",
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
                page_num=1,
                page_size=20,
            )
            assert total == 1
            assert items[0]["id"] == "1"
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

    assert "idx_ai_call_knowledge_chunk_ngram_tsv" in indexes
    assert generated == {"ngram_tsv": "ALWAYS"}
    assert "uk_ai_call_prompt_knowledge_binding" in binding_constraints
    assert "idx_ai_call_knowledge_usage_tenant_created" in usage_indexes
    assert "idx_ai_call_knowledge_usage_call_created" in usage_indexes
    assert "ck_ai_call_knowledge_usage_purpose" in usage_constraints
    assert "ck_ai_call_knowledge_usage_status" in usage_constraints


async def test_prompt_profile_tenant_migration_is_fail_closed_and_idempotent() -> None:
    _execute(
        """
        drop table if exists ai_call_prompt_knowledge_binding cascade;
        drop table if exists ai_call_prompt_profile cascade;
        create table ai_call_prompt_profile (
            id bigint primary key,
            scene_code varchar(64) not null,
            name varchar(128) not null,
            provider_key varchar(64) not null,
            prompt_text text,
            opening_message varchar(1000),
            created_at timestamptz not null,
            updated_at timestamptz not null,
            constraint uk_ai_call_prompt_profile_scene unique (scene_code)
        );
        insert into ai_call_prompt_profile (
            id, scene_code, name, provider_key, created_at, updated_at
        ) values (1, 'default', '默认场景', 'aliyun-qwen-realtime', now(), now());
        """
    )
    migration = PROMPT_TENANT_MIGRATION_PATH.read_text(encoding="utf-8")

    with pytest.raises(psycopg.errors.RaiseException, match="尚未指定租户"):
        _execute(f"begin; {migration} commit;")

    _execute(
        f"""
        begin;
        set local ai_call.prompt_tenant_id = 'tenant-a';
        {migration}
        {migration}
        commit;
        """
    )
    with psycopg.connect(_psycopg_dsn(), cursor_factory=ClientCursor) as connection:
        assert connection.execute(
            "select tenant_id from ai_call_prompt_profile where id = 1"
        ).fetchone() == ("tenant-a",)
        assert connection.execute(
            """
            select is_nullable
            from information_schema.columns
            where table_name = 'ai_call_prompt_profile' and column_name = 'tenant_id'
            """
        ).fetchone() == ("NO",)
        connection.execute(
            """
            insert into ai_call_prompt_profile (
                id, tenant_id, scene_code, name, provider_key, created_at, updated_at
            ) values (2, 'tenant-b', 'default', '其他租户默认场景',
                      'aliyun-qwen-realtime', now(), now())
            """
        )
