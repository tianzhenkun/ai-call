-- 在 phase-j1-knowledge-lexical-postgres.sql 之后执行，可对既有资料重复运行。
alter table ai_call_knowledge_version add column if not exists content_char_count integer;

-- 公共组件 content_chars 按 Unicode str.isspace() 排除空白。
-- 历史 Markdown 解析器为检索附在每一片之前的章节路径不属于该片正文。
with counts as (
    select v.id, sum(length(translate(
        case when v.parser_version = 'txt-markdown-utf8-v1' and c.section_path is not null
                   and left(c.content, length(c.section_path) + 1) = c.section_path || chr(10)
             then substr(c.content, length(c.section_path) + 2)
             else c.content end,
        U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000', ''
    ))) as chars
    from ai_call_knowledge_version v
    join ai_call_knowledge_chunk c on c.tenant_id = v.tenant_id and c.knowledge_version_id = v.id
    where v.status = 'READY' and v.content_char_count is null
    group by v.id, v.chunk_count
    having count(*) = v.chunk_count
)
update ai_call_knowledge_version as version
set content_char_count = counts.chars
from counts where version.id = counts.id and version.content_char_count is null;

-- 审计的 knowledge_version_ids 是冻结快照，实际引用仅来自成功记录里的来源。
create or replace function ai_call_knowledge_cited_versions(kind text, evidence text)
returns text[] language plpgsql immutable strict parallel safe as $$
declare
    sources jsonb;
begin
    sources := evidence::jsonb;
    if kind = 'PRODUCT_SUMMARY' then
        sources := sources -> 'sources';
    elsif kind <> 'REALTIME_ANSWER' then
        return '{}';
    end if;
    if jsonb_typeof(sources) is distinct from 'array' then
        return '{}';
    end if;
    return array(
        select distinct entry ->> 'versionId'
        from jsonb_array_elements(sources) as entry
        where entry ->> 'versionId' is not null
    );
exception when invalid_text_representation or untranslatable_character then
    -- 无法解读的历史记录没有可归属到任何资料的引用，不妨碍其他证据计数。
    return '{}';
end
$$;

create index if not exists idx_ai_call_knowledge_usage_cited_versions
    on ai_call_knowledge_usage using gin (ai_call_knowledge_cited_versions(purpose, evidence_json))
    where status = 'OK';
