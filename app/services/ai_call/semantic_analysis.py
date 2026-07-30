from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import (
    DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE,
    SEMANTIC_ANALYSIS_STATUS_FAILED,
    SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT,
    SEMANTIC_ANALYSIS_STATUS_PENDING,
    SEMANTIC_ANALYSIS_STATUS_RUNNING,
    SEMANTIC_ANALYSIS_STATUS_SUCCEEDED,
    AiCallRecordRepository,
)
from app.api.v1.ai_call.model import (
    AiCallAsrJobModel,
    AiCallDialogueSegmentModel,
    AiCallHandoffModel,
    AiCallSemanticAnalysisModel,
)
from app.core.logger import log
from app.services.ai_call.dialogue_merge import (
    is_cross_source_customer_transcript_conflict,
)
from app.services.ai_call.post_call_follow_up_service import (
    AiCallPostCallFollowUpService,
)

ANALYSIS_SCENE_CODE = DEFAULT_SEMANTIC_ANALYSIS_SCENE_CODE
ANALYSIS_STATUS_PENDING = SEMANTIC_ANALYSIS_STATUS_PENDING
ANALYSIS_STATUS_RUNNING = SEMANTIC_ANALYSIS_STATUS_RUNNING
ANALYSIS_STATUS_SUCCEEDED = SEMANTIC_ANALYSIS_STATUS_SUCCEEDED
ANALYSIS_STATUS_FAILED = SEMANTIC_ANALYSIS_STATUS_FAILED
ANALYSIS_STATUS_NO_USER_INPUT = SEMANTIC_ANALYSIS_STATUS_NO_USER_INPUT
NO_EFFECTIVE_USER_INPUT_ERROR = "未获取到用户有效话术，无需进行语义分析"

OFFLINE_ASR_SOURCE = "offline_asr"
QWEN_REALTIME_SOURCE = "qwen_realtime"
SPEAKER_CUSTOMER = "customer"
SPEAKER_AI = "ai"
SPEAKER_HUMAN_AGENT = "human_agent"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
VALID_FEEDBACK_TYPES = {"正向", "负向", "中性"}
CUSTOMER_FINAL_STATUSES = {"final"}
AI_FINAL_STATUSES = {"final", "interrupted"}
NOISE_USER_TEXTS = {"嗯", "呃", "啊", "喂", "哦", "好", "行"}
TEXT_NORMALIZE_PATTERN = re.compile(r"[\W_]+", re.UNICODE)
CJK_PATTERN = re.compile(r"[\u3400-\u9fff]")
LATIN_PATTERN = re.compile(r"[A-Za-z]")
TRANSCRIPT_LISTING_PREFIX_PATTERN = re.compile(
    r"^[’”\"'\s]*(?:[‘“\"'][^’”\"']{1,30}[’”\"']\s*){2,}"
    r"等表达参与对话[。！？!?；;]?"
)
SUMMARY_LEADING_JUNK_PATTERN = re.compile(r"^[，,。；;、’”\"'\s]+")
ASR_CORRECTION_PARENTHESES_PATTERN = re.compile(
    r"[（(][^（）()]{0,40}应为[^（）()]{0,80}ASR错误[^（）()]{0,20}[）)]"
)
INTERNAL_EVIDENCE_TOKEN_PATTERN = re.compile(
    r"(role=|semantic_evidence|supports_strong_fact|"
    r"supported_strong_fact_types|unsupported_strong_fact_types|analysis_usage|"
    r"transcript_quality|human_agent_track_crosstalk_count|"
    r"new_question_or_intent|conversation_control_intent|weak_feedback|"
    r"reason_codes|key_point_candidate|handoff request|handoff_id|"
    r"supports_[a-z_]+_fact|unsupported_[a-z_]+_fact)"
)
INTERNAL_EVIDENCE_PARENTHESES_PATTERN = re.compile(
    r"[（(][^（）()]{0,240}"
    r"(?:"
    + INTERNAL_EVIDENCE_TOKEN_PATTERN.pattern
    + r")"
    r"[^（）()]{0,240}[）)]"
)
SUMMARY_NEEDS_CUSTOMER_SUBJECT_PATTERN = re.compile(r"^(表现出|反映|显示)")
USER_QUESTION_PATTERN = re.compile(
    r"[？?]|(吗|么|怎么|如何|为什么|多少|有没有|能不能|是否|是不是|什么|啥|哪|支持|能做)",
    re.IGNORECASE,
)
ASSISTANT_QUESTION_PATTERN = re.compile(r"[？?]|(请问|吗|么|怎么|如何|是否|是不是|哪)")
IDENTITY_QUESTION_PATTERN = re.compile(r"(怎么称呼|如何称呼|贵姓|姓名|名字|哪位|怎么叫)")
AVAILABILITY_QUESTION_PATTERN = re.compile(r"(方便|可以|有空|能聊|能沟通)")
IDENTITY_SELF_STATEMENT_PATTERN = re.compile(
    r"(我叫|我是|本人是|我的名字是|我名字叫|姓名是|名字叫)"
)
BUSINESS_DETAIL_PATTERN = re.compile(
    r"(我们|我司|公司|业务|合同|单证|单据|信用证|提单|发票|贸易|外贸|境外|跨境|国际结算|审查|修改建议|律师|法务|复核|费时|费力|效率|风险|全面|强大|覆盖|品牌|曝光|推荐|需求|主要|比较多|想了解|关注|感兴趣|市场|增长|产品)"
)
CONVERSATION_CONTROL_PATTERN = re.compile(
    r"(先到这|到这儿|先挂|挂了|再见|不方便|稍后再联系|再继续|继续吧|聊一会)"
)
RECORD_ONLY_SHORT_BLOCK_PATTERN = re.compile(r"(不帮我|别打|别联系|你管我|不聊|别管)")
REQUIREMENT_CONCLUSION_PATTERN = re.compile(
    r"(主要|需要|想|关注|感兴趣|比较多|业务|合同|审查|费时|费力|效率|风险|全面|强大|覆盖|品牌|曝光|推荐|需求)"
)
TIME_HINT_PATTERN = re.compile(
    r"(今天|明天|后天|上午|中午|下午|晚上|周|星期|月|号|"
    r"(?:[0-9二两三四五六七八九十]{1,2}|十一|十二)点|"
    r"一点(?:半|钟|整|左右|之前|以后|后|前|再|联系|沟通))"
)
METADATA_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
METADATA_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COMMITMENT_PATTERN = re.compile(r"(会|可以|方便|再联系|回头|稍后|安排|确认)")
FOLLOW_UP_CONTACT_PATTERN = re.compile(
    r"(回访|回拨|(?:再|稍后|之后|明天|后天|改天).{0,4}联系|联系我|"
    r"给我.{0,3}(?:打电话|来电)|打(?:电话)?给我|电话联系|"
    r"(?:到时候|届时).{0,6}打(?:个|一个)?电话|"
    r"顾问.{0,6}联系|安排.{0,6}(?:顾问|专人|人员).{0,6}联系)"
)
FOLLOW_UP_CONTACT_OFFER_PATTERN = re.compile(
    r"(回访|回拨|再联系|稍后联系|之后联系|联系您|联系你|"
    r"给您.{0,3}(?:打电话|来电)|顾问.{0,6}联系|"
    r"安排.{0,6}(?:顾问|专人|人员))"
)
FOLLOW_UP_REFUSAL_PATTERN = re.compile(
    r"(?:不用|不要|别|无需|不需要|拒绝).{0,6}(?:联系|回访|回拨|打电话)|"
    r"(?:联系|回访|回拨).{0,6}(?:不用|不要|别)"
)
IDENTITY_RESULT_CLAIM_PATTERN = re.compile(
    r"(客户|用户|对方|来电人|联系人).{0,10}(自报|叫|姓名|名字|称呼)"
)
WEAK_FEEDBACK_TEXTS = {"嗯", "嗯嗯", "哦", "好", "好的"}
SHORT_ANSWER_TEXTS = {"方便", "可以", "有", "有的", "行", "好的", "对", "是"}
RECORD_ONLY_BACKGROUND_TEXTS = (
    NOISE_USER_TEXTS
    | WEAK_FEEDBACK_TEXTS
    | SHORT_ANSWER_TEXTS
    | {"你好", "知道了", "是的"}
)
OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH = "offline_asr_low_quality_language_mismatch"
OFFLINE_SHADOWED_BY_RICHER_REALTIME = "offline_asr_shadowed_by_richer_realtime"
OFFLINE_SPAN_REALTIME_DIVERGENCE = "offline_asr_span_realtime_divergence"
OFFLINE_SHORT_QUESTION_REALTIME_DIVERGENCE = (
    "offline_asr_short_question_realtime_divergence"
)
OFFLINE_NEARBY_TRANSCRIPT_CONFLICT = "nearby_transcript_conflict"
HUMAN_AGENT_TRACK_CROSSTALK_SIGNAL = "human_agent_track_crosstalk"
HUMAN_AGENT_TRACK_CUSTOMER_OVERLAP = "human_agent_track_customer_overlap"
TRANSCRIPT_UNCERTAINTY_ANALYSIS_GUIDANCE = (
    "存在转写来源冲突、降级或补充时，不得把低置信片段当作强业务结论；"
    "应优先依据多轮一致、上下文连续的客户表达，并在摘要或标签中提示转写噪声风险。"
)
SEMANTIC_ANALYSIS_SYSTEM_PROMPT = """你是 AI Call 通话后语义分析器。
只根据 transcript_json 中的真实对话文本分析客户侧表达，不使用提示词配置或外部知识补全。
只把 role=user 的轮次当作客户表达；role=assistant 或 speaker_type=ai 的轮次只能作为上下文，不能作为客户意向、诉求、异议或时间线证据。
即使 assistant 轮次使用客户口吻、第一人称或疑问句，也必须识别为 AI 话术异常，不得把它归因给客户。
必须查看 transcript_json.metadata.transcript_quality；当 has_uncertain_transcript 为 true 时，将孤立、冲突或单路 ASR 片段视为低置信证据。
不得基于孤立、冲突或单路 ASR 片段输出强业务结论；如果转写噪声会影响判断，应在 summary 或 tags 中说明结论置信度较低。
必须读取每个 role=user 轮次的 semantic_evidence，并优先依据 semantic_evidence.analysis_usage、key_point_candidate 与 supports_strong_fact 做总结。
semantic_evidence.analysis_usage=record_only 且 key_point_candidate=false 的片段不得写入 summary 或 key_points 的确定事实，只能作为转写噪声或弱反馈背景。
姓名、公司、电话、时间、明确承诺、明确需求结论等强事实，只有在对应用户轮次 semantic_evidence.supports_strong_fact=true 且 supported_strong_fact_types 包含相应类型时，才能写入 summary 或 key_points 的确定事实。
semantic_evidence.low_confidence_source、source_conflict 或 unsupported_strong_fact_types 命中的片段，只能作为低置信背景；不得把 unsupported_strong_fact 片段改写成“客户叫某某”“客户自报姓名”“客户公司是某某”等确定事实。
semantic_evidence.new_question_or_intent=true 的客户跳话题问题、conversation_control_intent=true 的继续/结束/稍后联系意图应保留为客户问题或关注点；semantic_evidence.weak_feedback=true 且 key_point_candidate=false 的弱反馈只能记录为弱反馈，不能扩写成强意向或业务事实。
speaker_type=human_agent 且 transcript_quality.low_confidence_source=true 的人工坐席轮次只可作为低置信坐席上下文或音频污染风险，不能反推为客户事实，也不能作为客户诉求、异议或承诺。
必须只返回六字段 JSON 对象，不输出 Markdown、解释或额外字段。
JSON 字段固定为：
- summary: 字符串，本通电话摘要，重点描述客户侧表达。
- feedback_type: 字符串，只能是 正向、负向、中性。
- key_points: 字符串数组，客户表达的事实、诉求、异议、承诺、风险点或约束条件。
- time_hint: 对象，包含 time_text、time_value、original_texts。
- tags: 字符串数组，开放中文标签。
- follow_up: 对象，包含 required、consent、reason、preferred_time、confidence。required 仅表示客户侧存在后续联系需求；consent 只能是 explicit、missing、refused；confidence 只能是 high、medium、low；assistant 自己提出或承诺联系不能作为客户同意证据。"""

FOLLOW_UP_CONSENTS = {"explicit", "missing", "refused"}
FOLLOW_UP_CONFIDENCES = {"high", "medium", "low"}
CUSTOMER_INTENT_BY_FEEDBACK = {
    "正向": "positive",
    "中性": "neutral",
    "负向": "negative",
}


class SemanticAnalyzerProtocol(Protocol):
    async def analyze(
        self,
        *,
        transcript_snapshot: dict[str, Any],
        reference_date: str | None = None,
    ) -> dict[str, Any]:
        """Return raw structured semantic analysis and post-call result."""


class OpenAICompatibleSemanticAnalyzer:
    """OpenAI-compatible chat completion adapter for post-call semantic analysis."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.model = model.strip()
        self.timeout_seconds = max(1.0, timeout_seconds)
        self.transport = transport

    async def analyze(
        self,
        *,
        transcript_snapshot: dict[str, Any],
        reference_date: str | None = None,
    ) -> dict[str, Any]:
        if not self.base_url:
            raise ValueError("语义分析 base_url 未配置")
        if not self.api_key:
            raise ValueError("语义分析 API key 未配置")
        if not self.model:
            raise ValueError("语义分析模型未配置")

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SEMANTIC_ANALYSIS_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "transcript_json": transcript_snapshot,
                            "reference_date": reference_date,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds,
            transport=self.transport,
        ) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        response.raise_for_status()
        return self._parse_json_object(self._message_content(response.json()))

    @staticmethod
    def _message_content(data: Any) -> str:
        if not isinstance(data, dict):
            raise ValueError("语义分析响应不是 JSON 对象")
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("语义分析响应缺少 choices")
        first = choices[0]
        if not isinstance(first, dict):
            raise ValueError("语义分析响应 choices 格式异常")
        message = first.get("message")
        if not isinstance(message, dict):
            raise ValueError("语义分析响应缺少 message")
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("语义分析响应 content 为空")
        return content

    @staticmethod
    def _parse_json_object(content: str) -> dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("语义分析响应不是 JSON 对象")
        return data


def build_default_semantic_analyzer(
    *,
    base_url: str | None,
    api_key: str | None,
    model: str | None,
    timeout_seconds: float = 30.0,
) -> OpenAICompatibleSemanticAnalyzer | None:
    if not _is_configured(base_url) or not _is_configured(api_key) or not _is_configured(model):
        return None
    return OpenAICompatibleSemanticAnalyzer(
        base_url=str(base_url),
        api_key=str(api_key),
        model=str(model),
        timeout_seconds=timeout_seconds,
    )


@dataclass(frozen=True, slots=True)
class _SemanticAnalysisJob:
    call_id: str
    scene_code: str | None = None


class AiCallSemanticAnalysisWorker:
    """Queue worker that runs post-call semantic analysis after transcript readiness."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        analyzer: SemanticAnalyzerProtocol | None,
        enabled: bool = True,
        queue_max_size: int = 1000,
        reference_date_factory: Callable[[], str | None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.analyzer = analyzer
        self.enabled = enabled
        self.reference_date_factory = reference_date_factory or self._today
        self.queue: asyncio.Queue[_SemanticAnalysisJob] = asyncio.Queue(
            maxsize=max(1, queue_max_size)
        )
        self.dropped_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._queued_or_running: set[tuple[str, str | None]] = set()

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="ai-call-semantic-analysis-worker")

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 语义分析队列关闭超时，仍有任务未完成")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def enqueue(self, call_id: str, scene_code: str | None = None) -> None:
        if not self.enabled or not call_id:
            return
        key = (call_id, scene_code)
        if key in self._queued_or_running:
            return
        try:
            self.queue.put_nowait(_SemanticAnalysisJob(call_id=call_id, scene_code=scene_code))
            self._queued_or_running.add(key)
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning("AI Call 语义分析队列已满，丢弃任务: callId={}", call_id)

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def process_one(self) -> bool:
        if not self.enabled:
            return False
        try:
            job = self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return False
        try:
            await self._process_job(job)
            self.processed_count += 1
            return True
        except Exception as exc:
            self.failed_count += 1
            log.warning(
                "AI Call 语义分析任务处理失败: callId={}, errorType={}, message={}",
                job.call_id,
                type(exc).__name__,
                str(exc),
            )
            return False
        finally:
            self._queued_or_running.discard((job.call_id, job.scene_code))
            self.queue.task_done()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            try:
                job = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            try:
                await self._process_job(job)
                self.processed_count += 1
            except Exception as exc:
                self.failed_count += 1
                log.warning(
                    "AI Call 语义分析任务处理失败: callId={}, errorType={}, message={}",
                    job.call_id,
                    type(exc).__name__,
                    str(exc),
                )
            finally:
                self._queued_or_running.discard((job.call_id, job.scene_code))
                self.queue.task_done()

    async def _process_job(self, job: _SemanticAnalysisJob) -> None:
        async with self.session_factory() as db:
            repository = AiCallRecordRepository(db)
            service = AiCallSemanticAnalysisService(repository, analyzer=self.analyzer)
            await service.analyze_call_once(
                call_id=job.call_id,
                scene_code=job.scene_code,
                reference_date=self.reference_date_factory(),
                now=datetime.now(timezone.utc),
            )
            await db.commit()

    @staticmethod
    def _today() -> str:
        return date.today().isoformat()


class SemanticTranscriptBuilder:
    """Build the audit snapshot used by post-call semantic analysis."""

    def build(
        self,
        *,
        call_id: str,
        scene_code: str | None,
        rows: list[AiCallDialogueSegmentModel],
        asr_jobs: list[AiCallAsrJobModel] | None = None,
        handoffs: list[AiCallHandoffModel] | None = None,
    ) -> dict[str, Any]:
        handoff_rows = sorted(handoffs or [], key=self._handoff_sort_key)
        offline_customer_rows = [
            row
            for row in rows
            if row.speaker_type == SPEAKER_CUSTOMER and row.source == OFFLINE_ASR_SOURCE
        ]
        customer_asr_jobs = [
            job for job in (asr_jobs or []) if job.track_role == SPEAKER_CUSTOMER
        ]
        usable_offline_customer_rows = [
            row for row in offline_customer_rows if self._is_usable_customer_row(row)
        ]
        realtime_customer_rows = [
            row
            for row in rows
            if row.speaker_type == SPEAKER_CUSTOMER
            and row.source == QWEN_REALTIME_SOURCE
            and self._is_usable_customer_row(row)
        ]
        source_decisions = self._customer_source_decisions(
            offline_rows=usable_offline_customer_rows,
            realtime_rows=realtime_customer_rows,
        )
        excluded_offline_keys = {
            candidate["row_key"]
            for decision in source_decisions
            for candidate in decision["candidate_sources"]
            if (
                candidate["source"] == OFFLINE_ASR_SOURCE
                and decision["selected_source"] == QWEN_REALTIME_SOURCE
            )
        }
        effective_offline_customer_rows = [
            row
            for row in usable_offline_customer_rows
            if self._row_key(row) not in excluded_offline_keys
        ]
        source_decision_by_row_key = {
            str(decision["selected_row_key"]): self._public_source_decision(decision)
            for decision in source_decisions
        }
        quality_fallback_reason = next(
            (
                str(decision["fallback_reason"])
                for decision in source_decisions
                if decision.get("fallback_reason")
            ),
            None,
        )

        if effective_offline_customer_rows:
            supplemental_realtime_rows = self._realtime_customer_supplement_rows(
                realtime_customer_rows,
                effective_offline_customer_rows,
            )
            customer_rows = [*effective_offline_customer_rows, *supplemental_realtime_rows]
            fallback_to_realtime = quality_fallback_reason is not None
            fallback_reason = quality_fallback_reason
        else:
            supplemental_realtime_rows = []
            customer_rows = realtime_customer_rows
            fallback_to_realtime = bool(realtime_customer_rows)
            fallback_reason = (
                quality_fallback_reason
                or self._offline_fallback_reason(offline_customer_rows, customer_asr_jobs)
                if fallback_to_realtime
                else None
            )

        ai_rows = [
            row
            for row in rows
            if row.speaker_type == SPEAKER_AI
            and row.source == QWEN_REALTIME_SOURCE
            and self._text(row)
            and row.segment_status in AI_FINAL_STATUSES
        ]
        human_agent_rows = [
            row
            for row in rows
            if row.speaker_type == SPEAKER_HUMAN_AGENT
            and self._text(row)
            and row.segment_status in CUSTOMER_FINAL_STATUSES
        ]
        human_agent_quality_by_row_key = self._human_agent_quality_decisions(
            human_agent_rows=human_agent_rows,
            customer_rows=[
                row
                for row in rows
                if row.speaker_type == SPEAKER_CUSTOMER
                and self._is_usable_customer_row(row)
            ],
        )
        selected_rows = sorted(
            [*ai_rows, *customer_rows, *human_agent_rows],
            key=self._sort_key,
        )
        turns = [
            self._turn(
                seq=index + 1,
                row=row,
                source_decision=source_decision_by_row_key.get(self._row_key(row)),
                quality_decision=human_agent_quality_by_row_key.get(self._row_key(row)),
                handoff_id=self._handoff_id_for_row(row, handoff_rows),
            )
            for index, row in enumerate(selected_rows)
        ]
        turns = self._annotate_semantic_evidence(turns)
        metadata = {
            "customer_text_source_policy": "quality_aware_offline_asr_preferred",
            "fallback_to_realtime": fallback_to_realtime,
            "fallback_reason": fallback_reason,
            "realtime_supplemented_count": len(supplemental_realtime_rows),
            "offline_asr_quality_rejected_count": len(excluded_offline_keys),
            "transcript_quality": self._transcript_quality_summary(
                fallback_to_realtime=fallback_to_realtime,
                fallback_reason=fallback_reason,
                realtime_supplemented_count=len(supplemental_realtime_rows),
                offline_asr_quality_rejected_count=len(excluded_offline_keys),
                source_decisions=source_decisions,
                human_agent_quality_decisions=list(
                    human_agent_quality_by_row_key.values()
                ),
                asr_jobs=customer_asr_jobs,
            ),
            "customer_source_decisions": [
                self._public_source_decision(decision)
                for decision in source_decisions
            ],
            "human_agent_track_crosstalk_count": len(human_agent_quality_by_row_key),
            "offline_asr_jobs": [
                self._asr_job_summary(job)
                for job in customer_asr_jobs
            ],
            "has_interrupted_ai": any(
                row.speaker_type == SPEAKER_AI and row.segment_status == "interrupted"
                for row in ai_rows
            ),
            "handoff_summary": self._handoff_summary(
                handoffs=handoff_rows,
                turns=turns,
            ),
        }
        return {
            "call_id": call_id,
            "scene_code": scene_code,
            "turns": turns,
            "handoffs": [self._handoff_to_dict(handoff) for handoff in handoff_rows],
            "metadata": metadata,
        }

    def has_effective_user_input(self, snapshot: dict[str, Any]) -> bool:
        turns = snapshot.get("turns")
        if not isinstance(turns, list):
            return False
        for turn in turns:
            if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
                continue
            text = str(turn.get("text") or "").strip()
            if text and text not in NOISE_USER_TEXTS:
                return True
        return False

    @staticmethod
    def _is_usable_customer_row(row: AiCallDialogueSegmentModel) -> bool:
        return bool(
            row.segment_status in CUSTOMER_FINAL_STATUSES
            and (row.segment_text or "").strip()
        )

    @classmethod
    def _customer_source_decisions(
        cls,
        *,
        offline_rows: list[AiCallDialogueSegmentModel],
        realtime_rows: list[AiCallDialogueSegmentModel],
    ) -> list[dict[str, Any]]:
        decisions: list[dict[str, Any]] = []
        reliable_realtime_rows = [
            realtime_row
            for realtime_row in realtime_rows
            if cls._is_reliable_realtime_customer_row(realtime_row)
        ]
        realtime_rows_by_offline_key = {
            cls._row_key(offline_row): [
                realtime_row
                for realtime_row in reliable_realtime_rows
                if cls._same_cross_source_customer_utterance(
                    realtime_row,
                    offline_row,
                )
            ]
            for offline_row in offline_rows
        }
        for realtime_row in realtime_rows:
            if not cls._is_reliable_realtime_customer_row(realtime_row):
                continue
            low_quality_overlaps: list[AiCallDialogueSegmentModel] = []
            fallback_reason: str | None = None
            for offline_row in offline_rows:
                if not cls._same_cross_source_customer_utterance(
                    realtime_row,
                    offline_row,
                ):
                    continue
                issue = cls._offline_realtime_quality_issue(
                    offline_row=offline_row,
                    realtime_row=realtime_row,
                    overlapping_realtime_rows=realtime_rows_by_offline_key.get(
                        cls._row_key(offline_row),
                        [],
                    ),
                )
                if issue is None:
                    continue
                low_quality_overlaps.append(offline_row)
                fallback_reason = fallback_reason or issue
            if not low_quality_overlaps:
                continue
            decisions.append({
                "selected_row_key": cls._row_key(realtime_row),
                "selected_source": QWEN_REALTIME_SOURCE,
                "fallback_reason": fallback_reason,
                "candidate_sources": [
                    *[
                        cls._candidate_source_summary(offline_row)
                        for offline_row in low_quality_overlaps
                    ],
                    cls._candidate_source_summary(realtime_row),
                ],
            })
        return decisions

    @classmethod
    def _offline_realtime_quality_issue(
        cls,
        *,
        offline_row: AiCallDialogueSegmentModel,
        realtime_row: AiCallDialogueSegmentModel,
        overlapping_realtime_rows: list[AiCallDialogueSegmentModel],
    ) -> str | None:
        has_nearby_conflict = is_cross_source_customer_transcript_conflict(
            source=offline_row.source,
            speaker_type=offline_row.speaker_type,
            text=cls._text(offline_row),
            started_at=offline_row.started_at,
            ended_at=offline_row.ended_at,
            candidate_source=realtime_row.source,
            candidate_speaker_type=realtime_row.speaker_type,
            candidate_text=cls._text(realtime_row),
            candidate_started_at=realtime_row.started_at,
            candidate_ended_at=realtime_row.ended_at,
        )
        if has_nearby_conflict and not cls._time_ranges_overlap(
            offline_row,
            realtime_row,
        ):
            return OFFLINE_NEARBY_TRANSCRIPT_CONFLICT
        direct_issue = cls._offline_customer_quality_issue(offline_row)
        if direct_issue is not None:
            return direct_issue
        if cls._offline_span_has_mixed_language_divergence(
            offline_row,
            overlapping_realtime_rows,
        ):
            return OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH
        if cls._offline_span_diverges_from_realtime_rows(
            offline_row,
            overlapping_realtime_rows,
        ):
            return OFFLINE_SPAN_REALTIME_DIVERGENCE
        if cls._offline_short_question_diverges_from_realtime_rows(
            offline_row,
            overlapping_realtime_rows,
        ):
            return OFFLINE_SHORT_QUESTION_REALTIME_DIVERGENCE
        if cls._offline_mixed_language_noise_shadowed_by_business_realtime(
            offline_row,
            realtime_row,
        ):
            return OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH
        if cls._offline_short_fragment_shadowed_by_richer_realtime(
            offline_row,
            realtime_row,
        ):
            return OFFLINE_SHADOWED_BY_RICHER_REALTIME
        return None

    @classmethod
    def _same_cross_source_customer_utterance(
        cls,
        left: AiCallDialogueSegmentModel,
        right: AiCallDialogueSegmentModel,
    ) -> bool:
        return cls._time_ranges_overlap(left, right) or (
            is_cross_source_customer_transcript_conflict(
                source=left.source,
                speaker_type=left.speaker_type,
                text=cls._text(left),
                started_at=left.started_at,
                ended_at=left.ended_at,
                candidate_source=right.source,
                candidate_speaker_type=right.speaker_type,
                candidate_text=cls._text(right),
                candidate_started_at=right.started_at,
                candidate_ended_at=right.ended_at,
            )
        )

    @classmethod
    def _offline_customer_quality_issue(
        cls,
        row: AiCallDialogueSegmentModel,
    ) -> str | None:
        text = cls._text(row)
        if not text:
            return None
        cjk_count = len(CJK_PATTERN.findall(text))
        latin_count = len(LATIN_PATTERN.findall(text))
        if latin_count >= 2 and cjk_count == 0:
            return OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH
        if latin_count >= 4 and cjk_count <= 1 and latin_count > cjk_count * 2:
            return OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH
        return None

    @classmethod
    def _offline_span_has_mixed_language_divergence(
        cls,
        offline_row: AiCallDialogueSegmentModel,
        overlapping_realtime_rows: list[AiCallDialogueSegmentModel],
    ) -> bool:
        if len(overlapping_realtime_rows) < 2:
            return False
        text = cls._text(offline_row)
        if len(LATIN_PATTERN.findall(text)) < 2 or len(CJK_PATTERN.findall(text)) < 2:
            return False
        offline_text = cls._normalized_text(offline_row)
        if not offline_text:
            return False
        return any(
            realtime_text and realtime_text not in offline_text
            for realtime_text in (
                cls._normalized_text(realtime_row)
                for realtime_row in overlapping_realtime_rows
            )
        )

    @classmethod
    def _offline_span_diverges_from_realtime_rows(
        cls,
        offline_row: AiCallDialogueSegmentModel,
        overlapping_realtime_rows: list[AiCallDialogueSegmentModel],
    ) -> bool:
        realtime_texts = [
            cls._normalized_text(realtime_row)
            for realtime_row in overlapping_realtime_rows
            if cls._normalized_text(realtime_row)
        ]
        if len(realtime_texts) < 2:
            return False
        offline_text = cls._normalized_text(offline_row)
        if len(offline_text) < 8:
            return False
        missing_count = sum(
            1
            for realtime_text in realtime_texts
            if realtime_text not in offline_text and offline_text not in realtime_text
        )
        return missing_count * 2 >= len(realtime_texts)

    @classmethod
    def _offline_short_question_diverges_from_realtime_rows(
        cls,
        offline_row: AiCallDialogueSegmentModel,
        overlapping_realtime_rows: list[AiCallDialogueSegmentModel],
    ) -> bool:
        if not overlapping_realtime_rows:
            return False
        offline_text = cls._text(offline_row)
        offline_normalized = cls._normalized_text(offline_row)
        if not offline_normalized or len(offline_normalized) > 8:
            return False
        if not cls._is_user_question_or_intent(offline_text):
            return False
        realtime_texts = [
            cls._text(realtime_row)
            for realtime_row in overlapping_realtime_rows
            if cls._normalized_text(realtime_row)
        ]
        if not realtime_texts:
            return False
        if any(cls._is_user_question_or_intent(text) for text in realtime_texts):
            return False
        realtime_normalized_values = [
            cls._normalized_text_value(text)
            for text in realtime_texts
            if cls._normalized_text_value(text)
        ]
        return all(
            offline_normalized not in realtime_text
            and realtime_text not in offline_normalized
            for realtime_text in realtime_normalized_values
        )

    @classmethod
    def _offline_short_fragment_shadowed_by_richer_realtime(
        cls,
        offline_row: AiCallDialogueSegmentModel,
        realtime_row: AiCallDialogueSegmentModel,
    ) -> bool:
        offline_text = cls._normalized_text(offline_row)
        realtime_text = cls._normalized_text(realtime_row)
        if not offline_text or not realtime_text:
            return False
        return len(offline_text) <= 1 and len(realtime_text) >= 3

    @classmethod
    def _offline_mixed_language_noise_shadowed_by_business_realtime(
        cls,
        offline_row: AiCallDialogueSegmentModel,
        realtime_row: AiCallDialogueSegmentModel,
    ) -> bool:
        offline_text = cls._text(offline_row)
        realtime_text = cls._text(realtime_row)
        latin_count = len(LATIN_PATTERN.findall(offline_text))
        cjk_count = len(CJK_PATTERN.findall(offline_text))
        if latin_count < 4 or cjk_count == 0 or cjk_count > 2:
            return False
        if latin_count <= cjk_count * 2:
            return False
        offline_normalized = cls._normalized_text(offline_row)
        realtime_normalized = cls._normalized_text(realtime_row)
        if not realtime_normalized or realtime_normalized in offline_normalized:
            return False
        return cls._has_business_detail(realtime_text)

    @classmethod
    def _human_agent_quality_decisions(
        cls,
        *,
        human_agent_rows: list[AiCallDialogueSegmentModel],
        customer_rows: list[AiCallDialogueSegmentModel],
    ) -> dict[str, dict[str, Any]]:
        decisions: dict[str, dict[str, Any]] = {}
        for human_agent_row in human_agent_rows:
            decision = cls._human_agent_track_quality_issue(
                human_agent_row=human_agent_row,
                customer_rows=customer_rows,
            )
            if decision is not None:
                decisions[cls._row_key(human_agent_row)] = decision
        return decisions

    @classmethod
    def _human_agent_track_quality_issue(
        cls,
        *,
        human_agent_row: AiCallDialogueSegmentModel,
        customer_rows: list[AiCallDialogueSegmentModel],
    ) -> dict[str, Any] | None:
        if human_agent_row.source != OFFLINE_ASR_SOURCE:
            return None
        overlaps = [
            (cls._time_overlap_ms(human_agent_row, customer_row), customer_row)
            for customer_row in customer_rows
            if customer_row is not human_agent_row
        ]
        overlaps = [
            (overlap_ms, customer_row)
            for overlap_ms, customer_row in overlaps
            if overlap_ms > 0 and cls._text(customer_row)
        ]
        if not overlaps:
            return None

        _, overlap_row = max(overlaps, key=lambda item: item[0])
        has_language_mismatch = (
            cls._offline_customer_quality_issue(human_agent_row)
            == OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH
        )
        if not has_language_mismatch:
            return None

        return {
            "low_confidence_source": True,
            "reason_codes": [HUMAN_AGENT_TRACK_CUSTOMER_OVERLAP],
            "overlap_speaker_type": overlap_row.speaker_type,
            "overlap_source": overlap_row.source,
            "overlap_text": cls._text(overlap_row),
        }

    @classmethod
    def _is_reliable_realtime_customer_row(cls, row: AiCallDialogueSegmentModel) -> bool:
        if cls._offline_customer_quality_issue(row) == OFFLINE_LOW_QUALITY_LANGUAGE_MISMATCH:
            return False
        return bool(cls._text(row))

    @classmethod
    def _candidate_source_summary(cls, row: AiCallDialogueSegmentModel) -> dict[str, Any]:
        return {
            "row_key": cls._row_key(row),
            "source": row.source,
            "source_segment_id": row.source_segment_id,
            "segment_no": row.segment_no,
            "segment_status": row.segment_status,
            "text": cls._text(row),
            "started_at": cls._datetime_to_text(row.started_at),
            "ended_at": cls._datetime_to_text(row.ended_at),
        }

    @staticmethod
    def _public_source_decision(decision: dict[str, Any]) -> dict[str, Any]:
        return {
            "selected_source": decision["selected_source"],
            "fallback_reason": decision["fallback_reason"],
            "candidate_sources": [
                {
                    key: value
                    for key, value in candidate.items()
                    if key != "row_key"
                }
                for candidate in decision["candidate_sources"]
            ],
        }

    @staticmethod
    def _row_key(row: AiCallDialogueSegmentModel) -> str:
        return f"{row.source or ''}:{row.source_segment_id or ''}:{row.segment_no}"

    @classmethod
    def _realtime_customer_supplement_rows(
        cls,
        realtime_rows: list[AiCallDialogueSegmentModel],
        offline_rows: list[AiCallDialogueSegmentModel],
    ) -> list[AiCallDialogueSegmentModel]:
        return [
            row
            for row in realtime_rows
            if not cls._is_realtime_customer_row_covered_by_offline(row, offline_rows)
        ]

    @classmethod
    def _is_realtime_customer_row_covered_by_offline(
        cls,
        realtime_row: AiCallDialogueSegmentModel,
        offline_rows: list[AiCallDialogueSegmentModel],
    ) -> bool:
        realtime_text = cls._normalized_text(realtime_row)
        if not realtime_text:
            return True
        for offline_row in offline_rows:
            offline_text = cls._normalized_text(offline_row)
            if offline_text and (
                realtime_text in offline_text or offline_text in realtime_text
            ):
                return True
            if cls._time_ranges_overlap(realtime_row, offline_row):
                return True
        return False

    @classmethod
    def _normalized_text(cls, row: AiCallDialogueSegmentModel) -> str:
        return TEXT_NORMALIZE_PATTERN.sub("", cls._text(row)).lower()

    @staticmethod
    def _time_ranges_overlap(
        left: AiCallDialogueSegmentModel,
        right: AiCallDialogueSegmentModel,
    ) -> bool:
        if left.started_at is None or right.started_at is None:
            return False
        left_end = left.ended_at or left.started_at
        right_end = right.ended_at or right.started_at
        return left.started_at <= right_end and right.started_at <= left_end

    @staticmethod
    def _time_overlap_ms(
        left: AiCallDialogueSegmentModel,
        right: AiCallDialogueSegmentModel,
    ) -> int:
        if left.started_at is None or right.started_at is None:
            return 0
        left_end = left.ended_at or left.started_at
        right_end = right.ended_at or right.started_at
        overlap_start = max(left.started_at, right.started_at)
        overlap_end = min(left_end, right_end)
        if overlap_end <= overlap_start:
            return 0
        return int((overlap_end - overlap_start).total_seconds() * 1000)

    @staticmethod
    def _offline_fallback_reason(
        rows: list[AiCallDialogueSegmentModel],
        asr_jobs: list[AiCallAsrJobModel],
    ) -> str:
        job_reason = SemanticTranscriptBuilder._offline_job_fallback_reason(asr_jobs)
        if job_reason is not None:
            return job_reason
        if not rows:
            return "offline_asr_unavailable"
        for row in rows:
            values = [
                str(row.segment_status or "").lower(),
                str(row.failure_stage or "").lower(),
                str(row.failure_message or "").lower(),
            ]
            if any("timeout" in value or "超时" in value for value in values):
                return "offline_asr_timeout"
        if any(row.failure_stage or row.failure_message for row in rows):
            return "offline_asr_failed"
        return "offline_asr_unavailable"

    @staticmethod
    def _offline_job_fallback_reason(asr_jobs: list[AiCallAsrJobModel]) -> str | None:
        for job in asr_jobs:
            values = [
                str(job.status or "").lower(),
                str(job.failure_stage or "").lower(),
                str(job.failure_message or "").lower(),
            ]
            if any("timeout" in value or "超时" in value for value in values):
                return "offline_asr_timeout"
        for job in asr_jobs:
            values = [
                str(job.status or "").lower(),
                str(job.failure_stage or "").lower(),
                str(job.failure_message or "").lower(),
            ]
            if any(value in {"failed", "canceled", "cancelled"} for value in values):
                return "offline_asr_failed"
            if job.failure_stage or job.failure_message:
                return "offline_asr_failed"
        return None

    @staticmethod
    def _asr_job_summary(job: AiCallAsrJobModel) -> dict[str, Any]:
        return {
            "provider": job.provider,
            "model": job.model,
            "status": job.status,
            "failure_stage": job.failure_stage,
            "failure_message": job.failure_message,
        }

    @classmethod
    def _transcript_quality_summary(
        cls,
        *,
        fallback_to_realtime: bool,
        fallback_reason: str | None,
        realtime_supplemented_count: int,
        offline_asr_quality_rejected_count: int,
        source_decisions: list[dict[str, Any]],
        human_agent_quality_decisions: list[dict[str, Any]],
        asr_jobs: list[AiCallAsrJobModel],
    ) -> dict[str, Any]:
        signals: list[str] = []
        reasons: list[str] = []

        if source_decisions:
            _append_unique(signals, "source_conflict")
        if fallback_to_realtime:
            _append_unique(signals, "source_fallback")
        if realtime_supplemented_count > 0:
            _append_unique(signals, "realtime_supplement")
        if offline_asr_quality_rejected_count > 0:
            _append_unique(signals, "offline_asr_quality_rejected")
        if human_agent_quality_decisions:
            _append_unique(signals, HUMAN_AGENT_TRACK_CROSSTALK_SIGNAL)

        if fallback_reason:
            _append_unique(reasons, fallback_reason)
        for decision in source_decisions:
            reason = decision.get("fallback_reason")
            if reason:
                _append_unique(reasons, str(reason))
        for decision in human_agent_quality_decisions:
            reason_codes = decision.get("reason_codes")
            if not isinstance(reason_codes, list):
                continue
            for reason in reason_codes:
                if isinstance(reason, str) and reason:
                    _append_unique(reasons, reason)
        job_reason = cls._offline_job_fallback_reason(asr_jobs)
        if job_reason:
            _append_unique(signals, "offline_asr_job_issue")
            _append_unique(reasons, job_reason)

        has_uncertain_transcript = bool(signals)
        return {
            "has_uncertain_transcript": has_uncertain_transcript,
            "signals": signals,
            "reasons": reasons,
            "analysis_guidance": TRANSCRIPT_UNCERTAINTY_ANALYSIS_GUIDANCE
            if has_uncertain_transcript
            else "",
        }

    @classmethod
    def _turn(
        cls,
        *,
        seq: int,
        row: AiCallDialogueSegmentModel,
        source_decision: dict[str, Any] | None = None,
        quality_decision: dict[str, Any] | None = None,
        handoff_id: str | None = None,
    ) -> dict[str, Any]:
        turn = {
            "seq": seq,
            "role": ROLE_USER if row.speaker_type == SPEAKER_CUSTOMER else ROLE_ASSISTANT,
            "speaker_type": row.speaker_type,
            "speaker_identity": row.speaker_identity,
            "text": cls._text(row),
            "source": row.source,
            "segment_status": row.segment_status,
            "started_at": cls._datetime_to_text(row.started_at),
            "ended_at": cls._datetime_to_text(row.ended_at),
        }
        if handoff_id is not None:
            turn["handoff_id"] = handoff_id
        if source_decision is not None:
            turn["source_decision"] = source_decision
        if quality_decision is not None:
            turn["transcript_quality"] = quality_decision
        if row.speaker_type == SPEAKER_AI and row.segment_status == "interrupted":
            turn["note"] = "AI 话术被用户打断，不代表用户完整听到"
        return turn

    @classmethod
    def _annotate_semantic_evidence(
        cls,
        turns: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        annotated: list[dict[str, Any]] = []
        previous_assistant_turn: dict[str, Any] | None = None
        for turn in turns:
            if turn.get("role") == ROLE_ASSISTANT:
                previous_assistant_turn = turn
                annotated.append(turn)
                continue
            if turn.get("role") != ROLE_USER:
                annotated.append(turn)
                continue
            enriched = dict(turn)
            enriched["semantic_evidence"] = cls._semantic_evidence_for_user_turn(
                enriched,
                previous_assistant_turn,
            )
            annotated.append(enriched)
        return annotated

    @classmethod
    def _semantic_evidence_for_user_turn(
        cls,
        turn: dict[str, Any],
        previous_assistant_turn: dict[str, Any] | None,
    ) -> dict[str, Any]:
        text = str(turn.get("text") or "").strip()
        normalized = cls._normalized_text_value(text)
        previous_ai_text = str((previous_assistant_turn or {}).get("text") or "")
        source_decision = turn.get("source_decision")
        source_conflict = cls._has_source_conflict(source_decision)
        low_confidence_source = source_conflict
        new_question_or_intent = cls._is_user_question_or_intent(text)
        previous_ai_is_question = cls._assistant_asks_question(previous_ai_text)
        previous_ai_asks_identity = cls._assistant_asks_identity(previous_ai_text)
        previous_ai_asks_availability = cls._assistant_asks_availability(previous_ai_text)
        business_detail = cls._has_business_detail(text)
        conversation_control_intent = cls._has_conversation_control_intent(text)
        responds_to_previous_ai = cls._responds_to_previous_ai(
            text=text,
            normalized=normalized,
            business_detail=business_detail,
            new_question_or_intent=new_question_or_intent,
            previous_ai_is_question=previous_ai_is_question,
            previous_ai_asks_identity=previous_ai_asks_identity,
            previous_ai_asks_availability=previous_ai_asks_availability,
        )
        weak_feedback = bool(
            normalized in WEAK_FEEDBACK_TEXTS
            and not responds_to_previous_ai
            and not new_question_or_intent
        )

        supported_fact_types: list[str] = []
        unsupported_fact_types: list[str] = []
        identity_candidate = cls._is_unsupported_identity_candidate(
            text=text,
            previous_ai_asks_identity=previous_ai_asks_identity,
            low_confidence_source=low_confidence_source,
            responds_to_previous_ai=responds_to_previous_ai,
            new_question_or_intent=new_question_or_intent,
            business_detail=business_detail,
            weak_feedback=weak_feedback,
        )
        if cls._supports_identity_fact(
            text=text,
            previous_ai_asks_identity=previous_ai_asks_identity,
            low_confidence_source=low_confidence_source,
        ):
            supported_fact_types.append("identity")
        elif identity_candidate:
            unsupported_fact_types.append("identity")

        if cls._supports_time_fact(text, previous_ai_text):
            supported_fact_types.append("time")
        if cls._supports_commitment_fact(text, previous_ai_text):
            supported_fact_types.append("commitment")
        follow_up_consent = cls._supports_follow_up_consent_fact(
            text,
            previous_ai_text,
        )
        if follow_up_consent:
            supported_fact_types.append("follow_up_consent")
        if business_detail and cls._has_requirement_conclusion(text):
            supported_fact_types.append("requirement_conclusion")

        key_point_candidate = bool(
            new_question_or_intent
            or business_detail
            or conversation_control_intent
            or follow_up_consent
            or (responds_to_previous_ai and normalized in SHORT_ANSWER_TEXTS)
        )
        analysis_usage = (
            "use_as_customer_signal"
            if key_point_candidate and not (low_confidence_source and unsupported_fact_types)
            else "record_only"
        )

        return {
            "responds_to_previous_ai": responds_to_previous_ai,
            "new_question_or_intent": new_question_or_intent,
            "business_detail": business_detail,
            "conversation_control_intent": conversation_control_intent,
            "weak_feedback": weak_feedback,
            "source_conflict": source_conflict,
            "low_confidence_source": low_confidence_source,
            "key_point_candidate": key_point_candidate,
            "supports_strong_fact": bool(supported_fact_types),
            "supported_strong_fact_types": supported_fact_types,
            "unsupported_strong_fact_types": unsupported_fact_types,
            "analysis_usage": analysis_usage,
            "reason_codes": cls._semantic_reason_codes(
                responds_to_previous_ai=responds_to_previous_ai,
                new_question_or_intent=new_question_or_intent,
                business_detail=business_detail,
                conversation_control_intent=conversation_control_intent,
                weak_feedback=weak_feedback,
                source_conflict=source_conflict,
                supported_fact_types=supported_fact_types,
                unsupported_fact_types=unsupported_fact_types,
            ),
        }

    @staticmethod
    def _normalized_text_value(text: str) -> str:
        return TEXT_NORMALIZE_PATTERN.sub("", text).lower()

    @staticmethod
    def _has_source_conflict(source_decision: Any) -> bool:
        if not isinstance(source_decision, dict):
            return False
        candidates = source_decision.get("candidate_sources")
        return bool(source_decision.get("fallback_reason")) or (
            isinstance(candidates, list) and len(candidates) > 1
        )

    @classmethod
    def _assistant_asks_question(cls, text: str) -> bool:
        return bool(text and ASSISTANT_QUESTION_PATTERN.search(text))

    @classmethod
    def _assistant_asks_identity(cls, text: str) -> bool:
        return bool(text and IDENTITY_QUESTION_PATTERN.search(text))

    @classmethod
    def _assistant_asks_availability(cls, text: str) -> bool:
        return bool(text and AVAILABILITY_QUESTION_PATTERN.search(text))

    @classmethod
    def _is_user_question_or_intent(cls, text: str) -> bool:
        return bool(text and USER_QUESTION_PATTERN.search(text))

    @classmethod
    def _has_business_detail(cls, text: str) -> bool:
        return bool(text and BUSINESS_DETAIL_PATTERN.search(text))

    @classmethod
    def _has_conversation_control_intent(cls, text: str) -> bool:
        return bool(text and CONVERSATION_CONTROL_PATTERN.search(text))

    @classmethod
    def _has_requirement_conclusion(cls, text: str) -> bool:
        return bool(
            text
            and REQUIREMENT_CONCLUSION_PATTERN.search(text)
            and not cls._is_user_question_or_intent(text)
        )

    @classmethod
    def _is_identity_candidate(cls, text: str) -> bool:
        normalized = cls._normalized_text_value(text)
        return bool(
            cls._is_identity_self_statement(text)
            or (2 <= len(normalized) <= 4 and CJK_PATTERN.search(normalized))
        )

    @classmethod
    def _is_identity_self_statement(cls, text: str) -> bool:
        return bool(text and IDENTITY_SELF_STATEMENT_PATTERN.search(text))

    @classmethod
    def _is_unsupported_identity_candidate(
        cls,
        *,
        text: str,
        previous_ai_asks_identity: bool,
        low_confidence_source: bool,
        responds_to_previous_ai: bool,
        new_question_or_intent: bool,
        business_detail: bool,
        weak_feedback: bool,
    ) -> bool:
        if cls._is_identity_self_statement(text):
            return True
        normalized = cls._normalized_text_value(text)
        if not (2 <= len(normalized) <= 4 and CJK_PATTERN.search(normalized)):
            return False
        if previous_ai_asks_identity:
            return True
        if not low_confidence_source:
            return False
        if (
            responds_to_previous_ai
            or new_question_or_intent
            or business_detail
            or weak_feedback
            or normalized in SHORT_ANSWER_TEXTS
        ):
            return False
        return True

    @classmethod
    def _responds_to_previous_ai(
        cls,
        *,
        text: str,
        normalized: str,
        business_detail: bool,
        new_question_or_intent: bool,
        previous_ai_is_question: bool,
        previous_ai_asks_identity: bool,
        previous_ai_asks_availability: bool,
    ) -> bool:
        if not previous_ai_is_question:
            return False
        if previous_ai_asks_identity:
            return cls._is_identity_candidate(text)
        if new_question_or_intent:
            return False
        if previous_ai_asks_availability:
            return normalized in SHORT_ANSWER_TEXTS
        if normalized in SHORT_ANSWER_TEXTS:
            return True
        return business_detail

    @classmethod
    def _supports_identity_fact(
        cls,
        *,
        text: str,
        previous_ai_asks_identity: bool,
        low_confidence_source: bool,
    ) -> bool:
        if low_confidence_source:
            return False
        if cls._is_identity_self_statement(text):
            return True
        normalized = cls._normalized_text_value(text)
        return bool(previous_ai_asks_identity and 2 <= len(normalized) <= 4)

    @classmethod
    def _supports_time_fact(cls, text: str, previous_ai_text: str) -> bool:
        if not text or not TIME_HINT_PATTERN.search(text):
            return False
        return bool(cls._assistant_asks_question(previous_ai_text) or COMMITMENT_PATTERN.search(text))

    @classmethod
    def _supports_commitment_fact(cls, text: str, previous_ai_text: str) -> bool:
        return bool(
            text
            and not cls._is_user_question_or_intent(text)
            and COMMITMENT_PATTERN.search(text)
            and (TIME_HINT_PATTERN.search(text) or cls._assistant_asks_question(previous_ai_text))
        )

    @classmethod
    def _supports_follow_up_consent_fact(
        cls,
        text: str,
        previous_ai_text: str,
    ) -> bool:
        if not text or FOLLOW_UP_REFUSAL_PATTERN.search(text):
            return False
        if FOLLOW_UP_CONTACT_PATTERN.search(text):
            return True
        return bool(
            cls._normalized_text_value(text) in SHORT_ANSWER_TEXTS
            and FOLLOW_UP_CONTACT_OFFER_PATTERN.search(previous_ai_text)
        )

    @staticmethod
    def _semantic_reason_codes(
        *,
        responds_to_previous_ai: bool,
        new_question_or_intent: bool,
        business_detail: bool,
        conversation_control_intent: bool,
        weak_feedback: bool,
        source_conflict: bool,
        supported_fact_types: list[str],
        unsupported_fact_types: list[str],
    ) -> list[str]:
        reasons: list[str] = []
        if responds_to_previous_ai:
            _append_unique(reasons, "answers_previous_ai_question")
        if new_question_or_intent:
            _append_unique(reasons, "new_question_or_intent")
        if business_detail:
            _append_unique(reasons, "business_detail")
        if conversation_control_intent:
            _append_unique(reasons, "conversation_control_intent")
        if weak_feedback:
            _append_unique(reasons, "weak_feedback_without_business_content")
        if source_conflict:
            _append_unique(reasons, "source_conflict_or_fallback")
        for fact_type in supported_fact_types:
            _append_unique(reasons, f"supports_{fact_type}_fact")
        for fact_type in unsupported_fact_types:
            _append_unique(reasons, f"unsupported_{fact_type}_fact")
        return reasons

    @staticmethod
    def _text(row: AiCallDialogueSegmentModel) -> str:
        return str(row.segment_text or "").strip()

    @staticmethod
    def _sort_key(row: AiCallDialogueSegmentModel) -> tuple[int, datetime | None, int]:
        if row.started_at is None:
            return (1, None, row.segment_no)
        return (0, row.started_at, row.segment_no)

    @classmethod
    def _handoff_sort_key(
        cls,
        handoff: AiCallHandoffModel,
    ) -> tuple[int, datetime | None, int]:
        requested_at = handoff.requested_at
        row_id = int(getattr(handoff, "id", 0) or 0)
        if requested_at is None:
            return (1, None, row_id)
        return (0, requested_at, row_id)

    @classmethod
    def _handoff_to_dict(cls, handoff: AiCallHandoffModel) -> dict[str, Any]:
        return {
            "handoff_id": handoff.handoff_id,
            "status": handoff.status,
            "request_source": handoff.request_source,
            "request_reason": handoff.request_reason,
            "request_message": handoff.request_message,
            "human_agent_identity": handoff.human_agent_identity,
            "requested_at": cls._datetime_to_text(handoff.requested_at),
            "accepted_at": cls._datetime_to_text(handoff.accepted_at),
            "connected_at": cls._datetime_to_text(handoff.connected_at),
            "ended_at": cls._datetime_to_text(handoff.ended_at),
            "expires_at": cls._datetime_to_text(handoff.expires_at),
            "end_reason": handoff.end_reason,
            "failure_stage": handoff.failure_stage,
            "failure_message": handoff.failure_message,
        }

    @classmethod
    def _handoff_id_for_row(
        cls,
        row: AiCallDialogueSegmentModel,
        handoffs: list[AiCallHandoffModel],
    ) -> str | None:
        if row.speaker_type not in {SPEAKER_CUSTOMER, SPEAKER_HUMAN_AGENT}:
            return None
        for handoff in reversed(handoffs):
            if cls._row_in_handoff_connected_window(row, handoff):
                return handoff.handoff_id
        return None

    @classmethod
    def _row_in_handoff_connected_window(
        cls,
        row: AiCallDialogueSegmentModel,
        handoff: AiCallHandoffModel,
    ) -> bool:
        if handoff.connected_at is None:
            return False
        row_start = row.started_at or row.ended_at
        row_end = row.ended_at or row.started_at
        if row_start is None and row_end is None:
            return False
        if row_end is not None and row_end < handoff.connected_at:
            return False
        if handoff.ended_at is not None and row_start is not None:
            return row_start <= handoff.ended_at
        return True

    @classmethod
    def _handoff_summary(
        cls,
        *,
        handoffs: list[AiCallHandoffModel],
        turns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        has_connected_handoff = any(handoff.connected_at is not None for handoff in handoffs)
        human_turns = [turn for turn in turns if turn.get("handoff_id")]
        human_agent_turns = [
            turn
            for turn in human_turns
            if turn.get("speaker_type") == SPEAKER_HUMAN_AGENT
        ]
        if not has_connected_handoff:
            human_transcript_status = "not_applicable"
        elif human_turns:
            human_transcript_status = "available"
        else:
            human_transcript_status = "missing"
        return {
            "has_handoff": bool(handoffs),
            "has_connected_handoff": has_connected_handoff,
            "human_turn_count": len(human_turns),
            "human_agent_turn_count": len(human_agent_turns),
            "human_transcript_status": human_transcript_status,
        }

    @staticmethod
    def _datetime_to_text(value: datetime | None) -> str | None:
        return value.isoformat() if value else None


class AiCallSemanticAnalysisService:
    def __init__(
        self,
        repository: AiCallRecordRepository,
        *,
        analyzer: SemanticAnalyzerProtocol | None = None,
        transcript_builder: SemanticTranscriptBuilder | None = None,
    ) -> None:
        self.repository = repository
        self.analyzer = analyzer
        self.transcript_builder = transcript_builder or SemanticTranscriptBuilder()

    async def analyze_call_once(
        self,
        *,
        call_id: str,
        scene_code: str | None = None,
        reference_date: str | None = None,
        now: datetime | None = None,
        force: bool = False,
    ) -> AiCallSemanticAnalysisModel:
        if scene_code is None:
            record = await self.repository.get_record(call_id)
            if record is not None:
                scene_code = record.scene_code
        analysis = await self.repository.ensure_semantic_analysis_record(
            call_id=call_id,
            scene_code=scene_code,
        )
        claimed = await self.repository.claim_semantic_analysis(
            call_id=call_id,
            now=now,
            force=force,
        )
        if claimed is None:
            return analysis

        rows = await self.repository.list_dialogue_segments(call_id)
        asr_jobs = await self.repository.list_asr_jobs(call_id)
        handoffs = await self.repository.list_handoffs(call_id)
        snapshot = self.transcript_builder.build(
            call_id=call_id,
            scene_code=scene_code,
            rows=rows,
            asr_jobs=asr_jobs,
            handoffs=handoffs,
        )
        snapshot_json = snapshot_to_json(snapshot)
        snapshot_hash = transcript_snapshot_hash(snapshot)
        if not self.transcript_builder.has_effective_user_input(snapshot):
            no_user = await self.repository.update_semantic_analysis_no_user_input(
                call_id=call_id,
                analysis_error=NO_EFFECTIVE_USER_INPUT_ERROR,
                transcript_snapshot_json=snapshot_json,
                transcript_hash=snapshot_hash,
                now=now,
            )
            if no_user is not None:
                return no_user
            return claimed

        if self.analyzer is None:
            failed = await self.repository.update_semantic_analysis_failed(
                call_id=call_id,
                analysis_error="语义分析模型未配置",
                transcript_snapshot_json=snapshot_json,
                transcript_hash=snapshot_hash,
                now=now,
            )
            if failed is not None:
                return failed
            return claimed

        try:
            raw_result = await self.analyzer.analyze(
                transcript_snapshot=snapshot,
                reference_date=reference_date,
            )
            result = enforce_semantic_evidence_on_result(
                normalize_analysis_result(raw_result),
                snapshot,
            )
        except Exception as exc:
            failed = await self.repository.update_semantic_analysis_failed(
                call_id=call_id,
                analysis_error=str(exc) or type(exc).__name__,
                transcript_snapshot_json=snapshot_json,
                transcript_hash=snapshot_hash,
                now=now,
            )
            if failed is not None:
                return failed
            return claimed

        succeeded = await self.repository.update_semantic_analysis_success(
            call_id=call_id,
            analysis_result=result,
            transcript_snapshot_json=snapshot_json,
            transcript_hash=snapshot_hash,
            **post_call_materialized_fields(result),
            now=now,
        )
        if succeeded is not None:
            await AiCallPostCallFollowUpService(self.repository).apply(succeeded)
            return succeeded
        return claimed

    async def reanalyze_call_once(
        self,
        *,
        call_id: str,
        scene_code: str | None = None,
        reference_date: str | None = None,
        now: datetime | None = None,
    ) -> AiCallSemanticAnalysisModel:
        return await self.analyze_call_once(
            call_id=call_id,
            scene_code=scene_code,
            reference_date=reference_date,
            now=now,
            force=True,
        )

    @staticmethod
    def analysis_to_dict(analysis: AiCallSemanticAnalysisModel) -> dict[str, Any]:
        return {
            "id": str(analysis.id),
            "callId": analysis.call_id,
            "sceneCode": analysis.scene_code,
            "analysisSceneCode": analysis.analysis_scene_code,
            "analysisStatus": analysis.analysis_status,
            "analysisResult": analysis.analysis_result_dict,
            "analysisError": analysis.analysis_error,
            "analysisRetryCount": analysis.analysis_retry_count,
            "analysisStartedAt": analysis.analysis_started_at,
            "analysisFinishedAt": analysis.analysis_finished_at,
            "transcriptHash": analysis.transcript_hash,
            "transcriptSnapshot": analysis.transcript_snapshot_dict,
            "createdAt": analysis.created_at,
            "updatedAt": analysis.updated_at,
        }


def normalize_analysis_result(value: dict[str, Any] | None) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    feedback_type = _string_value(raw.get("feedback_type"))
    if feedback_type not in VALID_FEEDBACK_TYPES:
        feedback_type = "中性"
    return {
        "summary": _string_value(raw.get("summary")),
        "feedback_type": feedback_type,
        "key_points": _string_list(raw.get("key_points")),
        "time_hint": _normalize_time_hint(raw.get("time_hint")),
        "tags": _string_list(raw.get("tags")),
        "follow_up": _normalize_follow_up(raw.get("follow_up")),
    }


def enforce_semantic_evidence_on_result(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    normalized = normalize_analysis_result(result)
    normalized = _remove_internal_evidence_annotations(normalized)
    normalized = _remove_transcript_listing_summary(normalized)
    normalized = _remove_record_only_claims(
        normalized,
        _snapshot_record_only_texts(snapshot),
    )
    normalized = _remove_rejected_candidate_source_claims(
        normalized,
        _snapshot_rejected_candidate_source_texts(snapshot),
    )
    normalized = _remove_assistant_only_claims(normalized, snapshot)
    normalized = _remove_metadata_time_hints(normalized)
    normalized = _remove_record_only_time_hints(normalized, snapshot)
    normalized = _remove_transcript_listing_summary(normalized)
    normalized = _append_transcript_quality_risk_tags(normalized, snapshot)
    normalized = _enforce_follow_up_evidence(normalized, snapshot)
    if _snapshot_supports_strong_fact(snapshot, "identity"):
        return normalized
    unsupported_identity_texts = _snapshot_unsupported_strong_fact_texts(snapshot, "identity")
    if not _analysis_result_contains_identity_claim(normalized, unsupported_identity_texts):
        return normalized

    cleaned_summary = _remove_identity_claim_sentences(
        normalized["summary"],
        unsupported_identity_texts,
    )
    cleaned_key_points = [
        point
        for point in normalized["key_points"]
        if not _contains_identity_claim(point, unsupported_identity_texts)
    ]
    cleaned_tags = list(normalized["tags"])
    _append_unique(cleaned_tags, "转写噪声风险")
    return _remove_transcript_listing_summary({
        **normalized,
        "summary": cleaned_summary
        or "客户身份信息未被可靠确认，存在转写噪声风险。",
        "key_points": cleaned_key_points,
        "tags": cleaned_tags,
    })


def _remove_internal_evidence_annotations(result: dict[str, Any]) -> dict[str, Any]:
    cleaned_summary = _clean_summary_text(
        _strip_internal_evidence_annotations(result["summary"])
    )
    cleaned_key_points = [
        cleaned
        for cleaned in (
            _strip_internal_evidence_annotations(point).strip(" ，,；;、")
            for point in result["key_points"]
        )
        if cleaned
    ]
    if cleaned_summary == result["summary"] and cleaned_key_points == result["key_points"]:
        return result
    return {
        **result,
        "summary": cleaned_summary,
        "key_points": cleaned_key_points,
    }


def _strip_internal_evidence_annotations(text: str) -> str:
    without_annotations = INTERNAL_EVIDENCE_PARENTHESES_PATTERN.sub("", text)
    without_internal_sentences = _remove_internal_evidence_sentences(without_annotations)
    return re.sub(r"\s+", " ", without_internal_sentences).strip()


def _remove_internal_evidence_sentences(text: str) -> str:
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？!?；;])\s*", text)
        if sentence.strip()
    ]
    if not sentences:
        return "" if INTERNAL_EVIDENCE_TOKEN_PATTERN.search(text) else text
    kept = [
        sentence
        for sentence in sentences
        if not INTERNAL_EVIDENCE_TOKEN_PATTERN.search(sentence)
    ]
    return "".join(kept).strip()


def _append_transcript_quality_risk_tags(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    signals = _snapshot_transcript_quality_signals(snapshot)
    if HUMAN_AGENT_TRACK_CROSSTALK_SIGNAL not in signals:
        return result
    tags = list(result["tags"])
    _append_unique(tags, "转写噪声风险")
    return {
        **result,
        "tags": tags,
    }


def snapshot_to_json(snapshot: dict[str, Any]) -> str:
    return json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))


def transcript_snapshot_hash(snapshot: dict[str, Any]) -> str:
    encoded = json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _remove_record_only_claims(
    result: dict[str, Any],
    record_only_texts: list[str],
) -> dict[str, Any]:
    if not record_only_texts:
        return result
    cleaned_summary = _remove_record_only_sentences(result["summary"], record_only_texts)
    cleaned_key_points = [
        point
        for point in result["key_points"]
        if not _contains_record_only_text(point, record_only_texts)
    ]
    if cleaned_summary == result["summary"] and cleaned_key_points == result["key_points"]:
        return result
    cleaned_tags = list(result["tags"])
    _append_unique(cleaned_tags, "转写噪声风险")
    return {
        **result,
        "summary": cleaned_summary or "客户有效语义信息不足，存在转写噪声风险。",
        "key_points": cleaned_key_points,
        "tags": cleaned_tags,
    }


def _remove_assistant_only_claims(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    assistant_only_texts = _snapshot_assistant_only_texts(snapshot)
    if not assistant_only_texts:
        return result
    cleaned_summary = _remove_text_sentences(result["summary"], assistant_only_texts)
    cleaned_key_points = [
        point
        for point in result["key_points"]
        if not _contains_normalized_text(point, assistant_only_texts)
    ]
    cleaned_tags = [
        tag
        for tag in result["tags"]
        if not _contains_normalized_text(tag, assistant_only_texts)
    ]
    if (
        cleaned_summary == result["summary"]
        and cleaned_key_points == result["key_points"]
        and cleaned_tags == result["tags"]
    ):
        return result
    _append_unique(cleaned_tags, "转写噪声风险")
    return {
        **result,
        "summary": cleaned_summary or "客户有效语义信息不足，存在转写噪声风险。",
        "key_points": cleaned_key_points,
        "tags": cleaned_tags,
    }


def _remove_metadata_time_hints(result: dict[str, Any]) -> dict[str, Any]:
    time_hint = _normalize_time_hint(result.get("time_hint"))
    if not time_hint["time_text"] and not time_hint["original_texts"]:
        return result
    kept_original_texts = [
        text
        for text in time_hint["original_texts"]
        if not _looks_like_metadata_time_value(text)
    ]
    removed_original_texts = len(kept_original_texts) != len(time_hint["original_texts"])
    if kept_original_texts:
        if not removed_original_texts:
            return result
        return {
            **result,
            "time_hint": {
                **time_hint,
                "original_texts": kept_original_texts,
            },
        }
    if not removed_original_texts and not (
        _looks_like_metadata_time_value(time_hint["time_text"])
        or _looks_like_metadata_time_value(time_hint["time_value"])
    ):
        return result
    cleaned_tags = list(result["tags"])
    _append_unique(cleaned_tags, "转写噪声风险")
    return {
        **result,
        "time_hint": _normalize_time_hint({}),
        "tags": cleaned_tags,
    }


def _remove_record_only_time_hints(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    time_hint = _normalize_time_hint(result.get("time_hint"))
    if not time_hint["time_text"] and not time_hint["original_texts"]:
        return result
    record_only_texts = _snapshot_record_only_turn_texts(snapshot)
    if not record_only_texts:
        return result

    kept_original_texts = [
        original
        for original in time_hint["original_texts"]
        if not _contains_normalized_text(original, record_only_texts)
    ]
    removed_original_texts = len(kept_original_texts) != len(time_hint["original_texts"])
    if kept_original_texts:
        if not removed_original_texts:
            return result
        return {
            **result,
            "time_hint": {
                **time_hint,
                "original_texts": kept_original_texts,
            },
        }

    if not removed_original_texts and not _time_text_only_appears_in_record_only_turn(
        time_hint["time_text"],
        record_only_texts,
    ):
        return result

    cleaned_result = _remove_removed_time_hint_claims(result, time_hint["time_text"])
    cleaned_tags = list(result["tags"])
    _append_unique(cleaned_tags, "转写噪声风险")
    return {
        **cleaned_result,
        "time_hint": _normalize_time_hint({}),
        "tags": cleaned_tags,
    }


def _remove_removed_time_hint_claims(result: dict[str, Any], time_text: str) -> dict[str, Any]:
    if not time_text:
        return result
    cleaned_summary = _remove_time_hint_claim_sentences(result["summary"], time_text)
    cleaned_key_points = [
        point
        for point in result["key_points"]
        if not _contains_time_hint_claim(point, time_text)
    ]
    return {
        **result,
        "summary": cleaned_summary
        or "客户有效语义信息不足，存在转写噪声风险。",
        "key_points": cleaned_key_points,
    }


def _remove_rejected_candidate_source_claims(
    result: dict[str, Any],
    rejected_texts: list[str],
) -> dict[str, Any]:
    if not rejected_texts:
        return result
    cleaned_summary = _remove_text_sentences(result["summary"], rejected_texts)
    cleaned_key_points = [
        point
        for point in result["key_points"]
        if not _contains_normalized_text(point, rejected_texts)
    ]
    if cleaned_summary == result["summary"] and cleaned_key_points == result["key_points"]:
        return result
    cleaned_tags = list(result["tags"])
    _append_unique(cleaned_tags, "转写噪声风险")
    return {
        **result,
        "summary": cleaned_summary or "客户有效语义信息不足，存在转写噪声风险。",
        "key_points": cleaned_key_points,
        "tags": cleaned_tags,
    }


def _remove_transcript_listing_summary(result: dict[str, Any]) -> dict[str, Any]:
    cleaned_summary = _clean_summary_text(
        _remove_transcript_listing_sentences(result["summary"])
    )
    if cleaned_summary == result["summary"]:
        return result
    return {
        **result,
        "summary": cleaned_summary or "客户有效语义信息不足，需复核。",
    }


def _remove_transcript_listing_sentences(text: str) -> str:
    cleaned_prefix = TRANSCRIPT_LISTING_PREFIX_PATTERN.sub("", text).strip()
    if cleaned_prefix != text:
        return cleaned_prefix
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？!?；;])\s*", text)
        if sentence.strip()
    ]
    if not sentences:
        return text
    kept = [
        sentence
        for sentence in sentences
        if not _is_transcript_listing_sentence(sentence)
    ]
    return "".join(kept).strip()


def _is_transcript_listing_sentence(sentence: str) -> bool:
    if "等表达参与对话" not in sentence:
        return False
    quoted_count = len(re.findall(r"[‘“\"']([^’”\"']{1,30})[’”\"']", sentence))
    return quoted_count >= 2


def _clean_summary_prefix(text: str) -> str:
    return SUMMARY_LEADING_JUNK_PATTERN.sub("", text).strip()


def _clean_summary_text(text: str) -> str:
    without_asr_correction = ASR_CORRECTION_PARENTHESES_PATTERN.sub("", text)
    without_boundary_junk = _remove_dangling_boundary_junk(without_asr_correction)
    without_trailing_junk = _clean_summary_prefix(without_boundary_junk).strip(" ，,；;、")
    return _ensure_customer_summary_subject(without_trailing_junk)


def _ensure_customer_summary_subject(text: str) -> str:
    if SUMMARY_NEEDS_CUSTOMER_SUBJECT_PATTERN.search(text):
        return f"客户{text}"
    return text


def _remove_dangling_boundary_junk(text: str) -> str:
    pattern = re.compile(r"([。！？!?；;])([’”\"'])([，,、]\s*)")

    def replace(match: re.Match[str]) -> str:
        quote = match.group(2)
        prefix = text[: match.start(2)]
        if quote == "'" and prefix.count("'") % 2 == 1:
            return match.group(0)
        if quote == '"' and prefix.count('"') % 2 == 1:
            return match.group(0)
        if quote == "’" and prefix.count("‘") > prefix.count("’"):
            return match.group(0)
        if quote == "”" and prefix.count("“") > prefix.count("”"):
            return match.group(0)
        return match.group(1)

    return pattern.sub(replace, text)


def _normalize_time_hint(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    return {
        "time_text": _string_value(raw.get("time_text")),
        "time_value": _string_value(raw.get("time_value")),
        "original_texts": _string_list(raw.get("original_texts")),
    }


def _normalize_follow_up(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    consent = _string_value(raw.get("consent"))
    confidence = _string_value(raw.get("confidence"))
    return {
        "required": raw.get("required") is True,
        "consent": consent if consent in FOLLOW_UP_CONSENTS else "missing",
        "reason": _string_value(raw.get("reason")),
        "preferred_time": _string_value(raw.get("preferred_time")) or None,
        "confidence": (
            confidence if confidence in FOLLOW_UP_CONFIDENCES else "low"
        ),
    }


def post_call_materialized_fields(result: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_analysis_result(result)
    follow_up = normalized["follow_up"]
    return {
        "customer_intent": CUSTOMER_INTENT_BY_FEEDBACK[normalized["feedback_type"]],
        "follow_up_suggested": (
            follow_up["required"] is True
            and follow_up["consent"] != "refused"
        ),
        "follow_up_consent": follow_up["consent"],
        "follow_up_reason": follow_up["reason"] or None,
        "follow_up_preferred_at": _parse_rfc3339_or_none(
            follow_up["preferred_time"]
        ),
        "follow_up_confidence": follow_up["confidence"],
    }


def _parse_rfc3339_or_none(value: Any) -> datetime | None:
    text = _string_value(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _enforce_follow_up_evidence(
    result: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    follow_up = result["follow_up"]
    supports_consent = _snapshot_supports_follow_up_consent(snapshot)
    if (
        follow_up["required"] is True
        and follow_up["consent"] == "missing"
        and supports_consent
    ):
        return {
            **result,
            "follow_up": {
                **follow_up,
                "consent": "explicit",
                "reason": "客户明确同意后续电话联系",
                "confidence": "high",
            },
        }
    if follow_up["consent"] != "explicit" and follow_up["confidence"] != "high":
        return result
    if supports_consent:
        return result
    return {
        **result,
        "follow_up": {
            **follow_up,
            "consent": "missing",
            "confidence": "low",
        },
    }


def _snapshot_supports_follow_up_consent(snapshot: dict[str, Any]) -> bool:
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return False
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict):
            continue
        if (
            evidence.get("analysis_usage") == "record_only"
            or evidence.get("low_confidence_source")
            or evidence.get("source_conflict")
            or not evidence.get("supports_strong_fact")
        ):
            continue
        supported_types = evidence.get("supported_strong_fact_types")
        if not isinstance(supported_types, list):
            continue
        if "follow_up_consent" in supported_types:
            return True
    return False


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        source = value
    elif value is None:
        source = []
    else:
        source = [value]
    return [item for item in (_string_value(item) for item in source) if item]


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _snapshot_supports_strong_fact(snapshot: dict[str, Any], fact_type: str) -> bool:
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return False
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict) or not evidence.get("supports_strong_fact"):
            continue
        supported_types = evidence.get("supported_strong_fact_types")
        if isinstance(supported_types, list) and fact_type in supported_types:
            return True
    return False


def _snapshot_transcript_quality_signals(snapshot: dict[str, Any]) -> list[str]:
    metadata = snapshot.get("metadata")
    if not isinstance(metadata, dict):
        return []
    quality = metadata.get("transcript_quality")
    if not isinstance(quality, dict):
        return []
    signals = quality.get("signals")
    if not isinstance(signals, list):
        return []
    return [signal for signal in (_string_value(item) for item in signals) if signal]


def _snapshot_unsupported_strong_fact_texts(
    snapshot: dict[str, Any],
    fact_type: str,
) -> list[str]:
    values: list[str] = []
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return values
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict):
            continue
        unsupported_types = evidence.get("unsupported_strong_fact_types")
        if not isinstance(unsupported_types, list) or fact_type not in unsupported_types:
            continue
        text = SemanticTranscriptBuilder._normalized_text_value(
            _string_value(turn.get("text")),
        )
        if text:
            _append_unique(values, text)
    return values


def _snapshot_record_only_texts(snapshot: dict[str, Any]) -> list[str]:
    values: list[str] = []
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return values
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict):
            continue
        if evidence.get("analysis_usage") != "record_only":
            continue
        _append_record_only_blocked_texts(values, _string_value(turn.get("text")))
    return values


def _snapshot_record_only_turn_texts(snapshot: dict[str, Any]) -> list[str]:
    values: list[str] = []
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return values
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        evidence = turn.get("semantic_evidence")
        if not isinstance(evidence, dict):
            continue
        if evidence.get("analysis_usage") != "record_only":
            continue
        _append_record_only_blocked_texts(values, _string_value(turn.get("text")))
    return values


def _append_record_only_blocked_texts(values: list[str], raw_text: str) -> None:
    normalized_text = SemanticTranscriptBuilder._normalized_text_value(raw_text)
    _append_record_only_blocked_text(values, normalized_text)
    for fragment in re.split(r"[，,。！？!?；;、\s]+", raw_text):
        normalized_fragment = SemanticTranscriptBuilder._normalized_text_value(fragment)
        _append_record_only_blocked_text(values, normalized_fragment)


def _append_record_only_blocked_text(values: list[str], text: str) -> None:
    if not text or text in RECORD_ONLY_BACKGROUND_TEXTS:
        return
    should_block_short_text = bool(RECORD_ONLY_SHORT_BLOCK_PATTERN.search(text))
    if len(text) >= 6 or should_block_short_text:
        _append_unique(values, text)


def _snapshot_rejected_candidate_source_texts(snapshot: dict[str, Any]) -> list[str]:
    values: list[str] = []
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return values
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_USER:
            continue
        source_decision = turn.get("source_decision")
        if not isinstance(source_decision, dict):
            continue
        selected_source = _string_value(source_decision.get("selected_source"))
        candidate_sources = source_decision.get("candidate_sources")
        if not isinstance(candidate_sources, list):
            continue
        for candidate in candidate_sources:
            if not isinstance(candidate, dict):
                continue
            if _string_value(candidate.get("source")) == selected_source:
                continue
            text = SemanticTranscriptBuilder._normalized_text_value(
                _string_value(candidate.get("text")),
            )
            if len(text) >= 2:
                _append_unique(values, text)
    return values


def _snapshot_assistant_only_texts(snapshot: dict[str, Any]) -> list[str]:
    values: list[str] = []
    turns = snapshot.get("turns")
    if not isinstance(turns, list):
        return values
    user_texts = [
        SemanticTranscriptBuilder._normalized_text_value(_string_value(turn.get("text")))
        for turn in turns
        if isinstance(turn, dict) and turn.get("role") == ROLE_USER
    ]
    for turn in turns:
        if not isinstance(turn, dict) or turn.get("role") != ROLE_ASSISTANT:
            continue
        for phrase in _salient_text_phrases(_string_value(turn.get("text"))):
            if any(phrase in user_text for user_text in user_texts):
                continue
            _append_unique(values, phrase)
    return values


def _salient_text_phrases(text: str) -> list[str]:
    values: list[str] = []
    for raw_part in re.split(r"[，。！？；,.!?;\s]+", text):
        normalized = SemanticTranscriptBuilder._normalized_text_value(raw_part)
        if len(normalized) >= 6:
            _append_unique(values, normalized)
            _append_assistant_phrase_variants(values, normalized)
        for marker in ("主要是", "比如", "例如", "包括"):
            if marker not in raw_part:
                continue
            suffix = raw_part.split(marker, 1)[1]
            normalized_suffix = SemanticTranscriptBuilder._normalized_text_value(suffix)
            if len(normalized_suffix) >= 4:
                _append_unique(values, normalized_suffix)
                _append_assistant_phrase_variants(values, normalized_suffix)
    normalized_full = SemanticTranscriptBuilder._normalized_text_value(text)
    if len(normalized_full) >= 8:
        _append_unique(values, normalized_full)
    return values


def _append_assistant_phrase_variants(values: list[str], phrase: str) -> None:
    for suffix in ("协议", "合同", "场景", "类型"):
        if not phrase.endswith(suffix):
            continue
        shortened = phrase[: -len(suffix)]
        if len(shortened) >= 4:
            _append_unique(values, shortened)


def _analysis_result_contains_identity_claim(
    result: dict[str, Any],
    unsupported_identity_texts: list[str],
) -> bool:
    if _contains_identity_claim(_string_value(result.get("summary")), unsupported_identity_texts):
        return True
    return any(
        _contains_identity_claim(point, unsupported_identity_texts)
        for point in _string_list(result.get("key_points"))
    )


def _remove_identity_claim_sentences(
    text: str,
    unsupported_identity_texts: list[str],
) -> str:
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？!?；;])\s*", text)
        if sentence.strip()
    ]
    if not sentences:
        return ""
    kept = [
        sentence
        for sentence in sentences
        if not _contains_identity_claim(sentence, unsupported_identity_texts)
    ]
    return "".join(kept).strip()


def _remove_record_only_sentences(text: str, record_only_texts: list[str]) -> str:
    return _remove_text_sentences(text, record_only_texts)


def _remove_text_sentences(text: str, blocked_texts: list[str]) -> str:
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？!?；;])\s*", text)
        if sentence.strip()
    ]
    if not sentences:
        return text
    kept = [
        sentence
        for sentence in sentences
        if not _contains_normalized_text(sentence, blocked_texts)
    ]
    return "".join(kept).strip()


def _contains_identity_claim(text: str, unsupported_identity_texts: list[str]) -> bool:
    if not text:
        return False
    normalized = SemanticTranscriptBuilder._normalized_text_value(text)
    if IDENTITY_RESULT_CLAIM_PATTERN.search(text):
        return True
    return any(
        _contains_unsupported_identity_text_with_claim_context(normalized, value)
        for value in unsupported_identity_texts
    )


def _contains_record_only_text(text: str, record_only_texts: list[str]) -> bool:
    return _contains_normalized_text(text, record_only_texts)


def _contains_normalized_text(text: str, blocked_texts: list[str]) -> bool:
    if not text:
        return False
    normalized = SemanticTranscriptBuilder._normalized_text_value(text)
    return any(
        value and (value in normalized or _has_long_text_overlap(normalized, value))
        for value in blocked_texts
    )


def _looks_like_metadata_time_value(text: str) -> bool:
    value = _string_value(text)
    if not value:
        return False
    return bool(
        METADATA_TIMESTAMP_PATTERN.fullmatch(value)
        or METADATA_DATE_PATTERN.fullmatch(value)
    )


def _remove_time_hint_claim_sentences(text: str, time_text: str) -> str:
    sentences = [
        sentence.strip()
        for sentence in re.split(r"(?<=[。！？!?；;])\s*", text)
        if sentence.strip()
    ]
    if not sentences:
        return text
    kept = [
        sentence
        for sentence in sentences
        if not _contains_time_hint_claim(sentence, time_text)
    ]
    return "".join(kept).strip()


def _contains_time_hint_claim(text: str, time_text: str) -> bool:
    normalized_text = SemanticTranscriptBuilder._normalized_text_value(text)
    normalized_time_text = SemanticTranscriptBuilder._normalized_text_value(time_text)
    if not normalized_time_text or normalized_time_text not in normalized_text:
        return False
    return bool(
        COMMITMENT_PATTERN.search(text)
        or re.search(r"(联系|沟通|跟进|继续|安排|回访|回电)", text)
    )


def _time_text_only_appears_in_record_only_turn(
    time_text: str,
    record_only_texts: list[str],
) -> bool:
    normalized_time_text = SemanticTranscriptBuilder._normalized_text_value(time_text)
    if not normalized_time_text:
        return False
    return any(normalized_time_text in text for text in record_only_texts)


def _has_long_text_overlap(text: str, candidate: str) -> bool:
    if len(candidate) < 6:
        return False
    window_size = min(len(candidate), 10)
    for index in range(0, len(candidate) - window_size + 1):
        if candidate[index : index + window_size] in text:
            return True
    return False


def _contains_unsupported_identity_text_with_claim_context(
    normalized_text: str,
    unsupported_identity_text: str,
) -> bool:
    if not unsupported_identity_text or unsupported_identity_text not in normalized_text:
        return False
    escaped = re.escape(unsupported_identity_text)
    return bool(
        re.search(rf"(客户|用户|对方|来电人|联系人){escaped}", normalized_text)
        or re.search(rf"(姓名|名字|称呼|叫|自报){escaped}", normalized_text)
        or re.search(rf"{escaped}(是)?(客户|用户|对方|来电人|联系人)", normalized_text)
    )


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _is_configured(value: str | None) -> bool:
    text = str(value or "").strip()
    return bool(text) and "CHANGE_ME" not in text and "your_" not in text.lower()
