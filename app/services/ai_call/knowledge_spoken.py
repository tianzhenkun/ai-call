"""入库时整理产品专章和口语问法；通话中只读取授权原文和本地向量。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

import numpy as np
import onnxruntime as ort
from sqlalchemy import or_, select, tuple_, update
from tokenizers import Tokenizer

from app.api.v1.ai_call.model import AiCallKnowledgeChunkModel as Chunk
from app.api.v1.ai_call.model import AiCallKnowledgeItemModel as Item
from app.api.v1.ai_call.model import AiCallKnowledgeVersionModel as Version
from app.config.setting import settings
from app.core.logger import log
from app.services.ai_call.prompt_optimization import (
    build_knowledge_product_extractor,
    scene_editor_deepseek_key,
)
from app.services.ai_call.session_registry import KNOWLEDGE_FOCUS, SPOKEN_VERSION

_COMMERCIAL_TERMS = re.compile(
    r"报价|价格|价钱|定价|计价|收费|费用|多少钱|免费|试用|试订|订阅|付费|合同金额|元[/／]|[¥￥]"
)
_RESPONSIBILITY = re.compile(
    r"负责|分工|接管|接手|主导|经办|转交|审批人|复核人|由.{1,14}(?:处理|执行|审核|批准|确认)|"
    r"人工.{0,6}(?:复核|确认|审核)|responsib(?:le|ility)|take[s]? over|assigned to|in charge",
    re.I,
)
_MODEL_FILES = {
    "model_optimized.onnx": "1294ea4b6331115a353d81f96b85e8c8d7fdcc284453d5b2fab5b016230aad38",
    "tokenizer.json": "48cea5d44424912a6fd1ea647bf4fe50b55ab8b1e5879c3275f80e339e8fae26",
}


@lru_cache(maxsize=1)
def encoder() -> tuple[ort.InferenceSession, Tokenizer]:
    directory = settings.AI_CALL_KNOWLEDGE_EMBEDDING_DIR
    if not directory:
        raise ValueError("未配置知识库中文向量模型目录")
    options = ort.SessionOptions()
    options.intra_op_num_threads = 2
    directory = Path(directory)
    for filename, expected in _MODEL_FILES.items():
        with (directory / filename).open("rb") as file:
            if hashlib.file_digest(file, "sha256").hexdigest() != expected:
                raise ValueError(f"知识向量模型与已入库向量版本不一致：{filename}")
    session = ort.InferenceSession(
        str(directory / "model_optimized.onnx"), options, providers=["CPUExecutionProvider"]
    )
    tokenizer = Tokenizer.from_file(str(directory / "tokenizer.json"))
    tokenizer.enable_truncation(512)
    tokenizer.enable_padding()
    return session, tokenizer


def _embed(texts: list[str]) -> np.ndarray:
    session, tokenizer = encoder()
    encoded = tokenizer.encode_batch(texts)
    inputs = {
        name: np.asarray([getattr(e, attr) for e in encoded], dtype=np.int64)
        for name, attr in (
            ("input_ids", "ids"),
            ("attention_mask", "attention_mask"),
            ("token_type_ids", "type_ids"),
        )
    }
    # BGE-small-zh-v1.5 使用归一化的 CLS 向量。
    cls = session.run(["last_hidden_state"], inputs)[0][:, 0]
    return cls / np.linalg.norm(cls, axis=1, keepdims=True)


def _key(value: str) -> str:
    return "".join(unicodedata.normalize("NFKC", value).casefold().split())


def _terms(value: str) -> set[str]:
    words = re.findall(r"[a-z0-9]+|[\u3400-\u9fff]+", _key(value))
    return {
        term
        for word in words
        for term in (
            [word]
            if word.isascii() or len(word) == 1
            else [word[i : i + 2] for i in range(len(word) - 1)]
        )
    }


def _lines(body: str) -> list[tuple[int, int]]:
    # 保留原文偏移；无换行的长段也可以精确引用其中一句。
    return [
        (m.start(), m.end())
        for m in re.finditer(r"[^\r\n。！？!?]{1,320}[。！？!?]?|[。！？!?]+", body)
        if m.group().strip()
    ]


def _validate(response, batch):
    pages = response.get("pages")
    if not isinstance(pages, list) or len(pages) != len(batch):
        raise ValueError("DeepSeek 返回的切片数量与输入不一致")
    by_ref = {r["ref"]: r for r in batch}
    facts = []
    for page in pages:
        ref = page["ref"]
        if type(ref) is not int or ref not in by_ref:
            raise ValueError("DeepSeek 返回了重复或未知切片")
        row = by_ref.pop(ref)
        section = page["section"]
        if not isinstance(section, str) or not isinstance(page["facts"], list):
            raise ValueError("DeepSeek 返回了无效章节")
        section = section.strip()[:160]
        for item in page["facts"]:
            first, last = item["first"], item["last"]
            lines = row["lines"]
            if (
                type(first) is not int
                or type(last) is not int
                or not 1 <= first <= last <= len(lines)
            ):
                raise ValueError("DeepSeek 引用了不存在的行")
            questions = item["questions"]
            if (
                not isinstance(questions, list)
                or not questions
                or any(not isinstance(q, str) or not q.strip() or len(q) > 160 for q in questions)
            ):
                raise ValueError("DeepSeek 返回了无效问法")
            quote = row["body"][lines[first - 1][0] : lines[last - 1][1]].strip()
            if item.get("focus") not in KNOWLEDGE_FOCUS:
                raise ValueError("原文问题类型不在允许的范围")
            facts.append({
                "chunk": row["chunk"],
                "product": row["product"],
                "section": section,
                "quote": quote,
                "focus": item["focus"],
                "questions": list(dict.fromkeys(q.strip() for q in questions)),
            })
    return facts


def _chapter_owners(plan, rows, filename):
    titles = {r["ref"]: (r["heading"] + "\n" + r["body"][:180]).strip() for r in rows}
    kind, product = plan["kind"], plan["product"].strip()
    if kind == "general" or kind == "single":
        # 明确用“品牌 + 产品介绍”命名的文件保留其业务名称；封面可能使用另一套市场描述。
        brand = re.match(
            r"^([A-Za-z][A-Za-z0-9 .+-]*?)\s*(?:产品介绍|AI智能体产品介绍|智能体产品介绍)", filename
        )
        if kind == "single" and brand:
            product = brand[1].strip()
        if kind == "single" and (
            not product
            or _key(product) not in _key(filename + " " + next(iter(titles.values()), ""))
        ):
            raise ValueError("单产品名称与文件名或封面不符")
        return dict.fromkeys(titles, product if kind == "single" else "")
    if kind != "portfolio" or not isinstance(plan["chapters"], list):
        raise ValueError("未能辨认文档类型")
    owners = {}
    for chapter in plan["chapters"]:
        first, last, name = chapter["first"], chapter["last"], chapter["product"].strip()
        if (
            type(first) is not int
            or type(last) is not int
            or first not in titles
            or last not in titles
            or last < first
            or not name
        ):
            raise ValueError("产品专章范围无效")
        prefix = re.compile("^" + re.escape(_key(name)) + r"(?:[·•:：|/—–-]|\d|$)")

        def signed(ref, pattern=prefix):
            # 品牌必须出现在独立标题开头；矩阵正文里提到的品牌不构成专章。
            return any(pattern.match(_key(line)) for line in titles[ref].splitlines()[:3])

        if not signed(first):
            continue
        # 同一栏目的连续编号可用于识别续页，封面或产品矩阵均不能充当栏目起点。
        series = re.fullmatch(r"(.{3,}?)[·•|—–-]?\d+", _key(titles[first].splitlines()[0]))
        for ref in range(first, last + 1):
            marker = _key(titles[ref].splitlines()[0]) if ref in titles else ""
            if ref in titles and (
                signed(ref)
                or (series and re.fullmatch(re.escape(series[1]) + r"[·•|—–-]?\d+", marker))
            ):
                if ref in owners:
                    raise ValueError("同一章节归属多种产品")
                owners[ref] = name
    return owners


def _index_sections(rows):
    results = []
    for row in rows:
        body = row["body"]
        marks = list(re.finditer(r"(?im)^[ \t]*(?:L\s*\d+|STEP\s*\d+|阶段\s*\d+)[ \t]*\r?$", body))
        # 独立编号的能力分别生成问法，避免把同页的四个边界压成一个笼统问题。
        starts = sorted({0, *(m.start() for m in marks)}) if len(marks) >= 2 else [0]
        for start, stop in zip(starts, starts[1:] + [len(body)], strict=True):
            part = body[start:stop]
            if part.strip():
                results.append(dict(row, ref=len(results), body=part, lines=_lines(part)))
    return results


async def build_index(version, chunks, extractor):
    from app.services.ai_call.knowledge import _legacy_docx_body, read_legacy_word

    if version.extension == "docx" and any(
        c.content_type not in {"DOCX_BODY", "DOCX_HEADING"} for c in chunks
    ):
        originals = list(_legacy_docx_body(chunks, await read_legacy_word(version)))
    else:
        originals = [
            (c, c.content, c.section_path) for c in chunks if c.content_type != "DOCX_HEADING"
        ]
    headings = [version.source_filename]
    rows = []
    for ref, (c, body, heading) in enumerate(originals):
        lines = _lines(body)
        if not lines:
            continue
        headings.append((heading or "") + " " + body[:130].replace("\n", " "))
        rows.append({
            "ref": ref,
            "chunk": c.chunk_index,
            "body": body,
            "lines": lines,
            "heading": heading or "",
            "page": c.page_no,
        })
    # 同一批需要文档级目录以辨认总览和专章。仅向模型发送当前批的正文。
    directory = "\n".join(headings)
    if rows:
        titles = [{"ref": r["ref"], "title": r["heading"] + " " + r["body"][:180]} for r in rows]
        for attempt in range(2):
            try:
                plan = await extractor.index_chapters(version.source_filename, titles)
                owners = _chapter_owners(plan, rows, version.source_filename)
                break
            except (KeyError, TypeError, ValueError):
                if attempt:
                    raise
        rows = _index_sections([
            dict(r, product=owners[r["ref"]]) for r in rows if r["ref"] in owners
        ])
    facts = []
    for offset in range(0, len(rows), 4):
        batch = rows[offset : offset + 4]
        payload = {
            "filename": version.source_filename,
            "headings": directory[:18000],
            "chunks": [
                {
                    "ref": r["ref"],
                    "page": r["page"],
                    "product": r["product"],
                    "lines": [f"{i} {r['body'][a:b]}" for i, (a, b) in enumerate(r["lines"], 1)],
                }
                for r in batch
            ],
        }
        pending = {r["ref"]: r for r in batch}
        for attempt in range(3):
            payload["requiredRefs"] = list(pending)
            payload["chunks"] = [c for c in payload["chunks"] if c["ref"] in pending]
            try:
                reply = await extractor.index_passages(payload)
                covered = [pending[page["ref"]] for page in reply["pages"]]
                extracted = _validate(reply, covered)
                facts.extend(extracted)
                for row in covered:
                    del pending[row["ref"]]
                if not pending:
                    break
                raise ValueError(
                    f"尚缺切片 {list(pending)}，封底等无事实时也要返回该 ref 和空 facts"
                )
            except (KeyError, TypeError, ValueError) as exc:
                if attempt == 2:
                    raise
                payload["repair"] = f"请逐项输出 requiredRefs，只处理本次列出的切片。{exc}"
    texts = []
    for fact in facts:
        texts.extend([fact["section"] + " " + fact["quote"], *fact["questions"]])
    vectors = await asyncio.to_thread(
        lambda: b"".join(
            _embed(texts[i : i + 32]).astype("<f2").tobytes() for i in range(0, len(texts), 32)
        )
    )
    return json.dumps(facts, ensure_ascii=False, separators=(",", ":")), vectors


async def index_next_version(sessions):
    from app.services.ai_call.knowledge import _public_version

    now, token = datetime.now(UTC), str(uuid4())
    async with sessions() as db, db.begin():
        version = await db.scalar(
            select(Version)
            .join(
                Item,
                (Item.tenant_id == Version.tenant_id)
                & (Item.current_ready_version_id == Version.id),
            )
            .where(
                Version.status == "READY",
                Item.id == Version.knowledge_item_id,
                Item.deleted_at.is_(None),
                Item.created_by.is_not(None),
                _public_version(),
                Version.retrieval_version.is_distinct_from(SPOKEN_VERSION),
                or_(Version.retrieval_retry_at.is_(None), Version.retrieval_retry_at <= now),
            )
            .order_by(Version.ready_at, Version.id)
            .limit(1)
            .with_for_update(of=Version, skip_locked=True)
        )
        if version is None:
            return False
        vid, tenant, digest = version.id, version.tenant_id, version.chunk_set_sha256
        version.retrieval_lease, version.retrieval_retry_at = token, now + timedelta(minutes=5)

    owned = (
        Version.id == vid,
        Version.tenant_id == tenant,
        Version.status == "READY",
        Version.chunk_set_sha256 == digest,
        Version.retrieval_lease == token,
    )

    async def renew():
        while True:
            await asyncio.sleep(40)
            async with sessions() as db, db.begin():
                changed = await db.execute(
                    update(Version)
                    .where(*owned)
                    .values(retrieval_retry_at=datetime.now(UTC) + timedelta(minutes=5))
                )
                if not changed.rowcount:
                    return

    heartbeat = asyncio.create_task(renew())
    try:
        async with sessions() as db:
            version = await db.get(Version, vid)
            chunks = list(
                await db.scalars(
                    select(Chunk)
                    .where(Chunk.tenant_id == tenant, Chunk.knowledge_version_id == vid)
                    .order_by(Chunk.chunk_index)
                )
            )
            if len(chunks) != version.chunk_count:
                raise ValueError("版本切片与入库记录不一致")
            # 相同账号、相同原文和名称的重复上传可以复用索引；元数据只保存版本内切片序号。
            previous = (
                await db.execute(
                    select(Version.retrieval_metadata, Version.retrieval_vectors)
                    .where(
                        Version.tenant_id == tenant,
                        Version.created_by == version.created_by,
                        Version.sha256 == version.sha256,
                        Version.chunk_set_sha256 == digest,
                        Version.source_filename == version.source_filename,
                        Version.retrieval_version == SPOKEN_VERSION,
                    )
                    .limit(1)
                )
            ).first()
        if previous is None:
            extractor = build_knowledge_product_extractor(
                api_key=scene_editor_deepseek_key(), timeout_seconds=90
            )
            if extractor is None:
                raise ValueError("入库整理缺少 DeepSeek 凭证")
            metadata, vectors = await build_index(version, chunks, extractor)
        else:
            metadata, vectors = previous
        async with sessions() as db, db.begin():
            await db.execute(
                update(Version)
                .where(*owned)
                .values(
                    retrieval_metadata=metadata,
                    retrieval_vectors=vectors,
                    retrieval_version=SPOKEN_VERSION,
                    retrieval_lease=None,
                    retrieval_retry_at=None,
                )
            )
    except Exception as exc:
        reason = str(exc)[:180] if isinstance(exc, ValueError) else type(exc).__name__
        log.warning("知识口语索引暂未完成 version={} reason={}", vid, reason)
        async with sessions() as db, db.begin():
            await db.execute(
                update(Version)
                .where(*owned)
                .values(
                    retrieval_lease=None,
                    retrieval_retry_at=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
    return True


class ProductClarification(ValueError):
    pass


def _mentions(text, products):
    normalized = _key(text)
    exact = {p for p in products if p and p in normalized}
    if exact:
        return exact
    # 独有品牌词可以作为简称，例如场景名只写 GEO。
    tokens = set(re.findall(r"[a-zA-Z0-9]+", text.casefold()))
    return {
        p
        for t in tokens
        if len(t) >= 3
        for p in products
        if t in p and sum(t in other for other in products) == 1
    }


async def indexed_products(db, tenant, versions):
    rows = await db.scalars(select(Version.retrieval_metadata).where(
        Version.tenant_id == tenant, Version.id.in_([v.id for v in versions])))
    names = {f["product"] for row in rows for f in json.loads(row) if f["product"]}
    return sorted(names, key=_key)


def named_products(text, choices):
    matches = _mentions(text, {_key(p) for p in choices})
    return [p for p in choices if _key(p) in matches]


def _rank(indexes, query, scene, positioning, topic="", focus=""):
    records = [(vid, json.loads(metadata), packed) for vid, metadata, packed in indexes]
    products = {_key(f["product"]) for _, facts, _ in records for f in facts}
    named = _mentions(scene, products) or _mentions(positioning, products)
    asked = _mentions(query, products)
    if named and asked and not named.intersection(asked):
        return []
    targets = (named & asked if named and asked else named or asked) or products
    if len(targets) > 1 or (targets == {""} and len(records) > 1):
        raise ProductClarification("请先询问客户想了解哪个产品，再查询该产品。")
    if not targets:
        return []
    queries = [query, topic] if topic and topic != query else [query]
    vectors = _embed(queries)
    terms, ranked = [_terms(q) for q in queries], []
    commercial = _COMMERCIAL_TERMS.search(query)
    other_best = 0.0
    for vid, facts, packed in records:
        if not facts:
            continue
        matrix = np.frombuffer(packed, dtype="<f2").reshape(-1, vectors.shape[1])
        scores = np.einsum("ij,kj->ik", matrix, vectors, dtype=np.float32)
        start = 0
        for fact in facts:
            stop = start + 1 + len(fact["questions"])
            if not commercial or _COMMERCIAL_TERMS.search(fact["quote"]):
                vocab = _terms(" ".join([fact["quote"], *fact["questions"]]))
                score = max(
                    float(s) + 0.13 * len(t & vocab) / max(1, len(t))
                    for s, t in zip(scores[start:stop].max(axis=0), terms, strict=True)
                )
                # “适合销售/主管”等岗位清单可能被模型误标，分工加权还需要原文明确交代谁来做。
                if (
                    score >= 0.53
                    and focus in KNOWLEDGE_FOCUS
                    and focus != "其他"
                    and fact["focus"] == focus
                    and (focus != "职责分工" or _RESPONSIBILITY.search(fact["quote"]))
                ):
                    score += 0.14
                if _key(fact["product"]) in targets:
                    ranked.append((score, vid, fact))
                elif fact["product"]:
                    other_best = max(other_best, score)
            start = stop
    ranked.sort(key=lambda hit: -hit[0])
    # 只有其他产品给出明显更强的解释时，目标产品的弱匹配不能据以作答。
    if ranked and other_best >= 0.66 and other_best > ranked[0][0] + 0.045:
        return []
    selected, seen = [], set()
    for score, vid, fact in ranked:
        margin = 0.15 if focus == "职责分工" and _RESPONSIBILITY.search(fact["quote"]) else 0.095
        if score < 0.59:
            break
        if score < ranked[0][0] - margin:
            continue
        if fact["quote"] in seen:
            continue
        seen.add(fact["quote"])
        selected.append((score, vid, fact))
        if len(selected) == 5:
            break
    return selected


async def search_spoken(db, versions, context, query, topic="", focus=""):
    from app.services.ai_call.knowledge import KnowledgeSearchHit, _public_version

    if not query.strip():
        return []
    indexes = (
        await db.execute(
            select(Version.id, Version.retrieval_metadata, Version.retrieval_vectors)
            .where(
                Version.id.in_(context.version_ids),
                Version.tenant_id == context.tenant_id,
                Version.retrieval_version == SPOKEN_VERSION,
                _public_version(),
            )
            .order_by(Version.id)
        )
    ).all()
    if len(indexes) != len(versions):
        raise ValueError("冻结资料的口语索引尚未就绪")
    seen = set()
    unique = []
    for row, version in zip(indexes, versions, strict=True):
        identity = (version.sha256, version.chunk_set_sha256, version.source_filename)
        if identity not in seen:
            unique.append(row)
            seen.add(identity)
    ranked = await asyncio.to_thread(
        _rank, unique, query, context.selected_product or context.scene_name, context.positioning, topic, focus
    )
    if not ranked:
        return []
    positions = {(vid, f["chunk"]) for _, vid, f in ranked}
    chunks = {
        (c.knowledge_version_id, c.chunk_index): c
        for c in await db.scalars(
            select(Chunk).where(
                Chunk.tenant_id == context.tenant_id,
                tuple_(Chunk.knowledge_version_id, Chunk.chunk_index).in_(positions),
            )
        )
    }
    names = {v.id: v.source_filename for v in versions}
    hits = []
    for score, vid, fact in ranked:
        c, quote = chunks[vid, fact["chunk"]], fact["quote"]
        hits.append(
            KnowledgeSearchHit(
                c.id,
                vid,
                c.chunk_index,
                quote,
                hashlib.sha256(quote.encode()).hexdigest(),
                names[vid],
                c.page_no,
                c.section_path or fact["section"],
                c.source_path,
                c.start_ms,
                c.end_ms,
                score,
            )
        )
    return hits
