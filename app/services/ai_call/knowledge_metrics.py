"""资料列表和详情共用的正文与实际引用统计。"""

from collections.abc import Sequence

from lingchen_knowledge.content import content_chars
from sqlalchemy import bindparam, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallKnowledgeChunkModel as Chunk
from app.api.v1.ai_call.model import AiCallKnowledgeVersionModel as Version


async def item_metrics(
    db: AsyncSession,
    tenant_id: str,
    latest_versions: Sequence[Version],
) -> dict[int, tuple[int | None, int]]:
    chars = {
        v.knowledge_item_id: v.content_char_count if v.status == "READY" else None
        for v in latest_versions
    }
    missing = {
        v.id: v for v in latest_versions if v.status == "READY" and v.content_char_count is None
    }
    if missing:
        # 未回填的历史版本按当前页一次读取。缺失切片不能解释为 0 字。
        rows = await db.execute(
            select(
                Chunk.knowledge_version_id,
                Chunk.content,
                Chunk.section_path,
            ).where(Chunk.tenant_id == tenant_id, Chunk.knowledge_version_id.in_(missing))
        )
        totals: dict[int, int] = {}
        counts: dict[int, int] = {}
        for version_id, body, section in rows:
            if section and missing[version_id].parser_version == "txt-markdown-utf8-v1":
                body = body.removeprefix(section + "\n")
            totals[version_id] = totals.get(version_id, 0) + content_chars(body)
            counts[version_id] = counts.get(version_id, 0) + 1
        for version in missing.values():
            if counts.get(version.id) == version.chunk_count:
                chars[version.knowledge_item_id] = totals[version.id]

    dialect = db.get_bind().dialect.name
    if dialect == "postgresql":
        # 与迁移中的部分 GIN 索引匹配，按本页资料的版本查找确实引用它的审计。
        match_evidence = """
            and ai_call_knowledge_cited_versions(u.purpose, u.evidence_json)
                @> array[cast(v.id as text)]
        """
    elif dialect == "mysql":
        match_evidence = """
            join json_table(
                json_extract(if(json_valid(u.evidence_json), u.evidence_json, null),
                    if(u.purpose = 'PRODUCT_SUMMARY', '$.sources', '$')),
                '$[*]' columns(version_id varchar(24) path '$.versionId' null on error)
            ) as e on e.version_id = cast(v.id as char)
        """
    else:
        # SQLite 开发和测试库的 JSON1 表函数使用同一份审计证据。
        match_evidence = """
            join json_each(
                case when json_valid(u.evidence_json) then u.evidence_json end,
                case when u.purpose = 'PRODUCT_SUMMARY' then '$.sources' else '$' end
            ) as e on typeof(e.key) = 'integer'
                and cast(json_extract(case when e.type = 'object' then e.value end, '$.versionId') as text)
                = cast(v.id as text)
        """
    query = text(f"""
        select v.knowledge_item_id, count(distinct u.id)
        from ai_call_knowledge_version v
        join ai_call_knowledge_usage u on u.tenant_id = v.tenant_id and u.status = 'OK'
        {match_evidence}
        where v.tenant_id = :tenant_id and v.knowledge_item_id in :item_ids
        group by v.knowledge_item_id
    """).bindparams(bindparam("item_ids", expanding=True))
    calls = dict((await db.execute(query, {"tenant_id": tenant_id, "item_ids": list(chars)})).all())
    return {item_id: (count, calls.get(item_id, 0)) for item_id, count in chars.items()}
