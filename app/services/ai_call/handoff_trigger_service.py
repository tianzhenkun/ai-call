from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logger import log
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore


@dataclass(frozen=True, slots=True)
class HandoffIntentResult:
    matched: bool
    confidence: float
    reason: str
    summary: str
    source: str = "classifier"


class HandoffIntentClassifierProtocol(Protocol):
    async def classify(self, *, transcript: str) -> HandoffIntentResult: ...


class RuleBasedHandoffIntentClassifier:
    """只做高置信兜底，避免散落在通话链路里的关键词判断。"""

    NEGATIVE_HINTS = (
        "人工智能",
        "人工审核",
        "人工处理",
        "人工标注",
        "人工成本",
        "你是人工",
        "你是真人",
    )
    STRONG_PATTERNS = (
        "转人工",
        "转接人工",
        "接人工",
        "找人工",
        "换人工",
        "人工客服",
        "人工坐席",
        "人工服务",
        "真人客服",
        "真人坐席",
        "找真人",
        "接真人",
        "换真人",
        "转真人",
        "接客服",
        "找客服",
        "换客服",
        "别让ai",
        "不要ai",
        "不想和ai",
        "不跟ai",
        "别让机器",
        "不要机器",
        "真人聊",
        "人来接",
    )

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        normalized = self._normalize(transcript)
        if not normalized:
            return HandoffIntentResult(
                matched=False,
                confidence=0.0,
                reason="empty_transcript",
                summary="用户文本为空",
                source="rule_fallback",
            )
        if any(hint in normalized for hint in self.NEGATIVE_HINTS):
            return HandoffIntentResult(
                matched=False,
                confidence=0.95,
                reason="not_handoff",
                summary="用户只是提到人工相关概念",
                source="rule_fallback",
            )
        if any(pattern in normalized for pattern in self.STRONG_PATTERNS):
            return HandoffIntentResult(
                matched=True,
                confidence=0.95,
                reason="customer_request",
                summary="用户明确要求转人工",
                source="rule_fallback",
            )
        return HandoffIntentResult(
            matched=False,
            confidence=0.4,
            reason="not_handoff",
            summary="未识别到明确转人工意图",
            source="rule_fallback",
        )

    @staticmethod
    def _normalize(text: str) -> str:
        return "".join(
            char.lower() for char in text.strip() if char not in " \t\r\n，。！？,.!?；;：:、"
        )


class OpenAICompatibleHandoffIntentClassifier:
    """通过 OpenAI-compatible chat completions 做轻量语义分类。"""

    SYSTEM_PROMPT = (
        "你是通话转人工意图分类器，只判断用户是否明确希望转人工、找真人、找客服或不想继续和AI沟通。"
        "不要受业务话术影响。"
        "如果用户只是提到人工智能、人工审核、人工处理、询问你是否真人，不算转人工。"
        "只返回JSON，字段为 matched(boolean), confidence(number 0-1), reason(string), summary(string)。"
    )

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 1.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = max(0.2, timeout_seconds)

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"transcript": transcript},
                        ensure_ascii=False,
                    ),
                },
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
        data = response.json()
        content = self._message_content(data)
        parsed = json.loads(content)
        return HandoffIntentResult(
            matched=bool(parsed.get("matched")),
            confidence=self._safe_confidence(parsed.get("confidence")),
            reason=str(parsed.get("reason") or "customer_request"),
            summary=str(parsed.get("summary") or "用户转人工意图判断"),
            source="llm_classifier",
        )

    @staticmethod
    def _message_content(data: dict[str, Any]) -> str:
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("意图分类响应缺少 choices")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise ValueError("意图分类响应缺少 message")
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("意图分类响应内容为空")
        return content

    @staticmethod
    def _safe_confidence(value: Any) -> float:
        try:
            return max(0.0, min(float(value), 1.0))
        except (TypeError, ValueError):
            return 0.0


class CompositeHandoffIntentClassifier:
    def __init__(
        self,
        primary: HandoffIntentClassifierProtocol | None,
        fallback: HandoffIntentClassifierProtocol,
    ) -> None:
        self.primary = primary
        self.fallback = fallback

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        if self.primary is not None:
            try:
                return await self.primary.classify(transcript=transcript)
            except Exception as exc:
                log.warning(
                    "转人工语义分类失败，使用兜底分类: errorType={}, message={}",
                    type(exc).__name__,
                    str(exc),
                )
        return await self.fallback.classify(transcript=transcript)


def build_default_handoff_intent_classifier(
    *,
    base_url: str,
    api_key: str,
    model: str,
    timeout_seconds: float,
) -> HandoffIntentClassifierProtocol:
    fallback = RuleBasedHandoffIntentClassifier()
    if _looks_configured(base_url) and _looks_configured(api_key) and _looks_configured(model):
        return CompositeHandoffIntentClassifier(
            OpenAICompatibleHandoffIntentClassifier(
                base_url=base_url,
                api_key=api_key,
                model=model,
                timeout_seconds=timeout_seconds,
            ),
            fallback,
        )
    return fallback


def _looks_configured(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip()
    return bool(normalized) and not normalized.upper().startswith("CHANGE_ME")


@dataclass(slots=True)
class QueuedHandoffTriggerEvent:
    event: AiCallEvent
    event_store: InMemoryEventStore
    attempts: int = 0


class AiCallHandoffTriggerService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        service_factory: Callable[[AsyncSession], Any],
        classifier: HandoffIntentClassifierProtocol,
        *,
        enabled: bool = True,
        customer_intent_enabled: bool = True,
        threshold: float = 0.8,
        timeout_seconds: float = 1.0,
    ) -> None:
        self.session_factory = session_factory
        self.service_factory = service_factory
        self.classifier = classifier
        self.enabled = enabled
        self.customer_intent_enabled = customer_intent_enabled
        self.threshold = max(0.0, min(threshold, 1.0))
        self.timeout_seconds = max(0.2, timeout_seconds)

    async def handle_event(
        self,
        *,
        event: AiCallEvent,
        event_store: InMemoryEventStore,
    ) -> None:
        if event.type == "handoff_tool_requested":
            await self._handle_tool_request(event=event, event_store=event_store)
            return
        if event.type != "user_transcript_done":
            return
        if not self.enabled or not self.customer_intent_enabled:
            self._append_ignored(
                event_store,
                event.call_id,
                reason="disabled",
                transcript=self._transcript_text(event),
            )
            return
        transcript = self._transcript_text(event)
        if not transcript:
            return
        if await self._has_active_handoff(event.call_id):
            self._append_ignored(
                event_store,
                event.call_id,
                reason="active_handoff_exists",
                transcript=transcript,
            )
            return
        try:
            result = await asyncio.wait_for(
                self.classifier.classify(transcript=transcript),
                timeout=self.timeout_seconds,
            )
        except TimeoutError:
            self._append_ignored(
                event_store,
                event.call_id,
                reason="classifier_timeout",
                transcript=transcript,
            )
            return
        except Exception as exc:
            self._append_failed(
                event_store,
                event.call_id,
                stage="classify",
                message=str(exc),
                error_type=type(exc).__name__,
                transcript=transcript,
            )
            return

        if not result.matched or result.confidence < self.threshold:
            self._append_ignored(
                event_store,
                event.call_id,
                reason=result.reason or "low_confidence",
                transcript=transcript,
                confidence=result.confidence,
                classifier_source=result.source,
            )
            return

        event_store.append(
            event.call_id,
            "handoff_intent_detected",
            "handoff",
            {
                "source": "customer",
                "reason": result.reason or "customer_request",
                "confidence": result.confidence,
                "classifierSource": result.source,
                "summary": self._truncate(result.summary, 200),
                "transcriptPreview": self._truncate(transcript, 120),
            },
        )
        try:
            handoff = await self._create_handoff(
                call_id=event.call_id,
                reason=result.reason or "customer_request",
                request_message=self._truncate(result.summary or transcript, 500),
            )
        except Exception as exc:
            self._append_failed(
                event_store,
                event.call_id,
                stage="create_handoff",
                message=str(exc),
                error_type=type(exc).__name__,
                transcript=transcript,
            )
            return

        event_store.append(
            event.call_id,
            "handoff_auto_triggered",
            "handoff",
            {
                "handoffId": handoff.get("handoffId"),
                "status": handoff.get("status"),
                "source": "customer",
                "reason": result.reason or "customer_request",
                "confidence": result.confidence,
                "transcriptPreview": self._truncate(transcript, 120),
            },
        )

    async def _handle_tool_request(
        self,
        *,
        event: AiCallEvent,
        event_store: InMemoryEventStore,
    ) -> None:
        reason = self._tool_request_reason(event)
        request_message = self._tool_request_message(event)
        if not self.enabled or not self.customer_intent_enabled:
            self._append_ignored(
                event_store,
                event.call_id,
                reason="disabled",
                transcript=request_message,
                classifier_source="realtime_tool",
            )
            return
        if await self._has_active_handoff(event.call_id):
            self._append_ignored(
                event_store,
                event.call_id,
                reason="active_handoff_exists",
                transcript=request_message,
                classifier_source="realtime_tool",
            )
            return

        event_store.append(
            event.call_id,
            "handoff_intent_detected",
            "handoff",
            {
                "source": "customer",
                "reason": reason,
                "confidence": 1.0,
                "classifierSource": "realtime_tool",
                "summary": self._truncate(request_message, 200),
                "toolCallId": event.payload.get("toolCallId"),
            },
        )
        try:
            handoff = await self._create_handoff(
                call_id=event.call_id,
                reason=reason,
                request_message=self._truncate(request_message, 500),
            )
        except Exception as exc:
            self._append_failed(
                event_store,
                event.call_id,
                stage="create_handoff",
                message=str(exc),
                error_type=type(exc).__name__,
                transcript=request_message,
            )
            return

        event_store.append(
            event.call_id,
            "handoff_auto_triggered",
            "handoff",
            {
                "handoffId": handoff.get("handoffId"),
                "status": handoff.get("status"),
                "source": "customer",
                "reason": reason,
                "confidence": 1.0,
                "classifierSource": "realtime_tool",
                "toolCallId": event.payload.get("toolCallId"),
            },
        )

    async def _has_active_handoff(self, call_id: str) -> bool:
        async with self.session_factory() as db:
            async with db.begin():
                service = self.service_factory(db)
                return await service.get_current_handoff(call_id) is not None

    async def _create_handoff(
        self,
        *,
        call_id: str,
        reason: str,
        request_message: str,
    ) -> dict:
        async with self.session_factory() as db:
            async with db.begin():
                service = self.service_factory(db)
                return await service.create_handoff(
                    call_id=call_id,
                    source="customer",
                    reason=reason,
                    request_message=request_message,
                )

    def _append_ignored(
        self,
        event_store: InMemoryEventStore,
        call_id: str,
        *,
        reason: str,
        transcript: str,
        confidence: float | None = None,
        classifier_source: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "reason": reason,
            "transcriptPreview": self._truncate(transcript, 120),
        }
        if confidence is not None:
            payload["confidence"] = confidence
        if classifier_source:
            payload["classifierSource"] = classifier_source
        event_store.append(call_id, "handoff_intent_ignored", "handoff", payload)

    def _append_failed(
        self,
        event_store: InMemoryEventStore,
        call_id: str,
        *,
        stage: str,
        message: str,
        error_type: str,
        transcript: str,
    ) -> None:
        event_store.append(
            call_id,
            "handoff_auto_trigger_failed",
            "handoff",
            {
                "stage": stage,
                "errorType": error_type,
                "message": self._truncate(message, 300),
                "transcriptPreview": self._truncate(transcript, 120),
            },
        )

    @staticmethod
    def _transcript_text(event: AiCallEvent) -> str:
        value = (
            event.payload.get("transcript")
            or event.payload.get("text")
            or event.payload.get("delta")
        )
        return value.strip() if isinstance(value, str) else ""

    @staticmethod
    def _tool_request_reason(event: AiCallEvent) -> str:
        reason = event.payload.get("reason")
        if isinstance(reason, str) and reason in {"customer_request", "business_escalation"}:
            return reason
        return "customer_request"

    @staticmethod
    def _tool_request_message(event: AiCallEvent) -> str:
        reason = AiCallHandoffTriggerService._tool_request_reason(event)
        return {
            "customer_request": "模型判断用户需要转人工",
            "business_escalation": "模型判断当前问题需要人工继续处理",
        }.get(reason, "模型发起转人工请求")

    @staticmethod
    def _truncate(value: str, limit: int) -> str:
        text = value.strip()
        return text if len(text) <= limit else f"{text[:limit]}..."


class AiCallHandoffTriggerWorker:
    """后台处理转人工自动触发，避免分类和数据库操作进入实时音频路径。"""

    def __init__(
        self,
        trigger_service: AiCallHandoffTriggerService,
        *,
        queue_max_size: int = 1000,
        max_retries: int = 1,
        transcript_trigger_enabled: bool = False,
    ) -> None:
        self.trigger_service = trigger_service
        self.max_retries = max(0, max_retries)
        self.transcript_trigger_enabled = transcript_trigger_enabled
        self.queue: asyncio.Queue[QueuedHandoffTriggerEvent] = asyncio.Queue(
            maxsize=max(1, queue_max_size)
        )
        self.dropped_count = 0
        self.failed_count = 0
        self.processed_count = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._attached_stores: dict[
            int, tuple[InMemoryEventStore, Callable[[AiCallEvent], None]]
        ] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="ai-call-handoff-trigger-worker",
        )

    async def stop(self, timeout_seconds: float = 3.0) -> None:
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            log.warning("AI Call 转人工触发队列关闭超时，仍有事件未处理")
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    def attach_event_store(self, event_store: InMemoryEventStore) -> None:
        store_id = id(event_store)
        if store_id in self._attached_stores:
            return

        def listener(event: AiCallEvent, store: InMemoryEventStore = event_store) -> None:
            self.enqueue(event, store)

        event_store.add_listener(listener)
        self._attached_stores[store_id] = (event_store, listener)

    def detach_all(self) -> None:
        for event_store, listener in self._attached_stores.values():
            event_store.remove_listener(listener)
        self._attached_stores.clear()

    def enqueue(self, event: AiCallEvent, event_store: InMemoryEventStore) -> None:
        should_enqueue = event.type == "handoff_tool_requested" or (
            self.transcript_trigger_enabled and event.type == "user_transcript_done"
        )
        if not should_enqueue:
            return
        try:
            self.queue.put_nowait(QueuedHandoffTriggerEvent(event=event, event_store=event_store))
        except asyncio.QueueFull:
            self.dropped_count += 1
            log.warning(
                "AI Call 转人工触发队列已满，丢弃事件: callId={}, eventId={}",
                event.call_id,
                event.event_id,
            )

    async def flush_pending(self) -> None:
        await self.queue.join()

    async def _run(self) -> None:
        while not self._stopping.is_set() or not self.queue.empty():
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            try:
                await self.trigger_service.handle_event(
                    event=item.event,
                    event_store=item.event_store,
                )
                self.processed_count += 1
            except Exception as exc:
                self.failed_count += 1
                log.error(
                    "AI Call 转人工触发处理失败: callId={}, eventId={}, errorType={}, message={}",
                    item.event.call_id,
                    item.event.event_id,
                    type(exc).__name__,
                    str(exc),
                )
                await self._retry(item)
            finally:
                self.queue.task_done()

    async def _retry(self, item: QueuedHandoffTriggerEvent) -> None:
        if item.attempts >= self.max_retries:
            return
        try:
            self.queue.put_nowait(
                QueuedHandoffTriggerEvent(
                    event=item.event,
                    event_store=item.event_store,
                    attempts=item.attempts + 1,
                )
            )
        except asyncio.QueueFull:
            self.dropped_count += 1
