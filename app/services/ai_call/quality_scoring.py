from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Protocol

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.crud import (
    DEFAULT_QUALITY_SCORE_MODEL_VERSION,
    QUALITY_SCORE_RETRY_COOLDOWN_MINUTES,
    QUALITY_SCORE_STATUS_FAILED,
    AiCallRecordRepository,
)
from app.api.v1.ai_call.model import AiCallQualityScoreModel
from app.core.logger import log
from app.services.ai_call.semantic_analysis import SemanticTranscriptBuilder

QUALITY_SCORING_SYSTEM_PROMPT = (
    '你是 AI 外呼质检评分员。只输出 JSON：{"score":0-100,'
    '"reason":"不超过200字的评分理由"}。评分依据包括话术完整度、客户问题回应、'
    "转人工时机和通话结束是否规范。"
)
QUALITY_SCORING_RECOVERY_INTERVAL_SECONDS = 60


class QualityScorerProtocol(Protocol):
    async def score(self, *, transcript_snapshot: dict[str, Any]) -> dict[str, Any]:
        """Return AI quality score payload."""


class OpenAICompatibleQualityScorer:
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

    async def score(self, *, transcript_snapshot: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url:
            raise ValueError("AI 评分 base_url 未配置")
        if not self.api_key:
            raise ValueError("AI 评分 API key 未配置")
        if not self.model:
            raise ValueError("AI 评分模型未配置")
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": QUALITY_SCORING_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"transcript_json": transcript_snapshot},
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
        return self._parse_response(response.json())

    @staticmethod
    def _parse_response(data: Any) -> dict[str, Any]:
        choices = data.get("choices") if isinstance(data, dict) else None
        if not isinstance(choices, list) or not choices:
            raise ValueError("AI 评分响应缺少 choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise ValueError("AI 评分响应 content 为空")
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError("AI 评分响应不是 JSON 对象")
        return result


def build_default_quality_scorer(
    *,
    base_url: str | None,
    api_key: str | None,
    model: str | None,
    timeout_seconds: float = 30.0,
) -> OpenAICompatibleQualityScorer | None:
    if not _is_configured(base_url) or not _is_configured(api_key) or not _is_configured(model):
        return None
    return OpenAICompatibleQualityScorer(
        base_url=str(base_url),
        api_key=str(api_key),
        model=str(model),
        timeout_seconds=timeout_seconds,
    )


class AiCallQualityScoringService:
    def __init__(
        self,
        repository: AiCallRecordRepository,
        *,
        scorer: QualityScorerProtocol | None,
    ) -> None:
        self.repository = repository
        self.scorer = scorer
        self.transcript_builder = SemanticTranscriptBuilder()

    async def score_call_once(
        self,
        *,
        tenant_id: str,
        call_id: str,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
    ) -> AiCallQualityScoreModel:
        score = await self.repository.ensure_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )
        if not await self._has_ready_evidence(tenant_id=tenant_id, call_id=call_id):
            return score
        claimed = await self.repository.claim_quality_score(
            tenant_id=tenant_id,
            call_id=call_id,
            model_version=model_version,
        )
        if claimed is None:
            return score
        rows = await self.repository.list_dialogue_segments(call_id)
        snapshot = self.transcript_builder.build(
            call_id=call_id,
            scene_code=None,
            rows=rows,
            handoffs=await self.repository.list_handoffs(call_id),
        )
        if self.scorer is None:
            return await self.repository.update_quality_score_failed(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
                error_message="AI 评分模型未配置",
            )
        try:
            result = await self.scorer.score(transcript_snapshot=snapshot)
            return await self.repository.update_quality_score_success(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
                score=_normalize_score(result.get("score")),
                reason=str(result.get("reason") or "")[:1000],
            )
        except Exception as exc:
            return await self.repository.update_quality_score_failed(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=model_version,
                error_message=str(exc) or type(exc).__name__,
            )

    async def _has_ready_evidence(self, *, tenant_id: str, call_id: str) -> bool:
        record = await self.repository.get_record_for_tenant(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if record is None:
            return False
        if record.entry_type == "web" and not await self.repository.has_task_owned_outbound_attempt(
            tenant_id=tenant_id,
            call_id=call_id,
        ):
            return False
        if record.entry_type not in {"outbound", "sip_outbound", "web"}:
            return False
        if record.status != "completed":
            return False
        recording = await self.repository.get_recording(
            tenant_id=tenant_id,
            call_id=call_id,
        )
        if recording is None or recording.status != "completed":
            return False
        if not recording.object_name and not recording.oss_id:
            return False
        return bool(await self.repository.list_dialogue_segments(call_id, limit=1))


class AiCallQualityScoringWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        scorer: QualityScorerProtocol | None,
        enabled: bool = True,
        queue_max_size: int = 1000,
        model_version: str = DEFAULT_QUALITY_SCORE_MODEL_VERSION,
    ) -> None:
        self.session_factory = session_factory
        self.scorer = scorer
        self.enabled = enabled
        self.model_version = model_version
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max(1, queue_max_size))
        self.dropped_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._queued_or_running: set[str] = set()
        self._last_recovery_at = 0.0

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="ai-call-quality-scoring-worker")
        await self.recover_pending()

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 评分队列关闭超时，仍有任务未完成")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def enqueue(self, call_id: str) -> None:
        if not self.enabled or not call_id:
            return
        if call_id in self._queued_or_running:
            return
        try:
            self.queue.put_nowait(call_id)
            self._queued_or_running.add(call_id)
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning("AI Call 评分队列已满，丢弃任务: callId={}", call_id)

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def recover_pending(self) -> None:
        async with self.session_factory() as db:
            repository = AiCallRecordRepository(db)
            call_ids = await repository.list_recoverable_quality_score_call_ids(
                limit=self.queue.maxsize
            )
        for call_id in call_ids:
            self.enqueue(call_id)
        self._last_recovery_at = time.monotonic()

    async def process_one(self) -> bool:
        if not self.enabled:
            return False
        try:
            call_id = self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return False
        try:
            await self._process_call(call_id)
            self.processed_count += 1
            return True
        except Exception as exc:
            self.failed_count += 1
            log.warning(
                "AI Call 评分任务处理失败: callId={}, errorType={}, message={}",
                call_id,
                type(exc).__name__,
                str(exc),
            )
            return False
        finally:
            self._queued_or_running.discard(call_id)
            self.queue.task_done()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            try:
                call_id = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                await self._recover_if_due()
                continue
            try:
                await self._process_call(call_id)
                self.processed_count += 1
            except Exception as exc:
                self.failed_count += 1
                log.warning(
                    "AI Call 评分任务处理失败: callId={}, errorType={}, message={}",
                    call_id,
                    type(exc).__name__,
                    str(exc),
                )
            finally:
                self._queued_or_running.discard(call_id)
                self.queue.task_done()

    async def _process_call(self, call_id: str) -> None:
        async with self.session_factory() as db:
            repository = AiCallRecordRepository(db)
            record = await repository.get_record(call_id)
            tenant_id = str(record.tenant_id or "").strip() if record else ""
            if not tenant_id:
                return
            service = AiCallQualityScoringService(repository, scorer=self.scorer)
            result = await service.score_call_once(
                tenant_id=tenant_id,
                call_id=call_id,
                model_version=self.model_version,
            )
            if result.status == QUALITY_SCORE_STATUS_FAILED:
                self._schedule_retry(call_id)
            await db.commit()

    async def _recover_if_due(self) -> None:
        if (
            time.monotonic() - self._last_recovery_at
            < QUALITY_SCORING_RECOVERY_INTERVAL_SECONDS
        ):
            return
        try:
            await self.recover_pending()
        except Exception as exc:
            log.warning(
                "AI Call 评分补偿扫描失败: errorType={}, message={}",
                type(exc).__name__,
                str(exc),
            )

    def _schedule_retry(self, call_id: str) -> None:
        if not self.enabled:
            return
        asyncio.create_task(
            self._enqueue_after_cooldown(call_id),
            name=f"ai-call-quality-scoring-retry-{call_id}",
        )

    async def _enqueue_after_cooldown(self, call_id: str) -> None:
        await asyncio.sleep(QUALITY_SCORE_RETRY_COOLDOWN_MINUTES * 60)
        self.enqueue(call_id)


def _normalize_score(value: Any) -> int:
    score = int(value)
    if score < 0 or score > 100:
        raise ValueError("AI 评分必须在 0 到 100 之间")
    return score


def _is_configured(value: str | None) -> bool:
    return bool(str(value or "").strip())
