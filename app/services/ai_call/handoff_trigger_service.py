from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logger import log
from app.services.ai_call.event_store import AiCallEvent, InMemoryEventStore
from app.services.ai_call.transcript_trust import (
    is_realtime_transcript_semantically_rejected,
)


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
    ROLE_QUESTION_HINTS = (
        "客户经理是做什么",
        "客户经理是什么",
        "什么是客户经理",
        "客户经理职责",
        "客户经理负责什么",
        "客户经理能做什么",
        "客户经理叫什么",
        "客户顾问叫什么",
        "销售顾问叫什么",
        "业务经理叫什么",
        "负责人是做什么",
        "负责人是什么",
        "什么是负责人",
        "负责人叫什么",
        "专人叫什么",
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
    PRODUCT_FOLLOW_UP_PATTERNS = (
        "怎么联系",
        "如何联系",
        "联系方式",
        "联系谁",
        "谁联系",
        "有没有人接",
        "有人接吗",
        "怎么联系我",
        "怎么联系你们",
        "你们怎么联系",
        "联系我",
        "给我联系",
        "给我回电",
        "回头联系",
        "后续联系",
        "到时候联系",
    )
    PRODUCT_FOLLOW_UP_ROLE_TERMS = (
        "客服",
        "顾问",
        "产品顾问",
        "销售",
    )
    PRODUCT_FOLLOW_UP_ACTION_TERMS = (
        "联系",
        "沟通",
        "一起聊",
        "聊吧",
    )
    PRODUCT_CONSULTATION_PATTERNS = (
        "有demo",
        "demo吗",
        "安排demo",
        "产品演示",
        "安排演示",
        "演示一下",
        "试用",
        "怎么合作",
        "合作方式",
        "想合作",
        "报价",
        "多少钱",
        "价格",
        "费用",
        "收费",
    )
    HUMAN_ROLE_TERMS = (
        "客户经理",
        "客户顾问",
        "人工顾问",
        "销售顾问",
        "业务经理",
        "负责人",
        "专人",
    )
    ROLE_REQUEST_PREFIXES = (
        "找",
        "接",
        "转",
        "换",
        "联系",
        "叫",
        "安排",
    )
    ROLE_REQUEST_CONNECTORS = ("", "你们", "你", "给", "一下", "下", "个", "一个", "一位")
    ROLE_REQUEST_SUFFIXES = (
        "联系我",
        "回电",
        "给我回电",
        "来接",
        "接一下",
        "跟我聊",
        "过来",
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
        if any(hint in normalized for hint in self.ROLE_QUESTION_HINTS):
            return HandoffIntentResult(
                matched=False,
                confidence=0.95,
                reason="not_handoff",
                summary="用户只是询问人工角色概念",
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
        if self._matches_human_role_request(normalized):
            return HandoffIntentResult(
                matched=True,
                confidence=0.95,
                reason="customer_request",
                summary="用户明确要求联系人工角色",
                source="rule_fallback",
            )
        if self._matches_product_follow_up_intent(normalized):
            return HandoffIntentResult(
                matched=True,
                confidence=0.95,
                reason="business_escalation",
                summary="用户表达产品合作推进或顾问沟通意向",
                source="rule_fallback",
            )
        if self._matches_product_consultation(normalized):
            return HandoffIntentResult(
                matched=False,
                confidence=0.95,
                reason="not_handoff",
                summary="用户只是咨询产品信息或演示安排",
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

    @classmethod
    def _matches_human_role_request(cls, normalized: str) -> bool:
        for role in cls.HUMAN_ROLE_TERMS:
            if role not in normalized:
                continue
            for prefix in cls.ROLE_REQUEST_PREFIXES:
                for connector in cls.ROLE_REQUEST_CONNECTORS:
                    if f"{prefix}{connector}{role}" in normalized:
                        return True
            if any(f"{role}{suffix}" in normalized for suffix in cls.ROLE_REQUEST_SUFFIXES):
                return True
        return False

    @classmethod
    def _matches_product_follow_up_intent(cls, normalized: str) -> bool:
        if any(pattern in normalized for pattern in cls.PRODUCT_FOLLOW_UP_PATTERNS):
            return True
        return any(role in normalized for role in cls.PRODUCT_FOLLOW_UP_ROLE_TERMS) and any(
            action in normalized for action in cls.PRODUCT_FOLLOW_UP_ACTION_TERMS
        )

    @classmethod
    def _matches_product_consultation(cls, normalized: str) -> bool:
        return any(pattern in normalized for pattern in cls.PRODUCT_CONSULTATION_PATTERNS)


class OpenAICompatibleHandoffIntentClassifier:
    """通过 OpenAI-compatible chat completions 做轻量语义分类。"""

    SYSTEM_PROMPT = (
        "你是通话转人工意图分类器，只判断用户是否明确希望转人工、找真人、找客服、找客户经理、"
        "找负责人、找销售顾问、找专人或不想继续和AI沟通。"
        "不要受业务话术影响。"
        "如果用户只是提到人工智能、人工审核、人工处理、询问你是否真人、询问客户经理职责，不算转人工。"
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
        primary_timeout_seconds: float | None = None,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.primary_timeout_seconds = (
            max(0.05, primary_timeout_seconds)
            if primary_timeout_seconds is not None
            else None
        )

    async def classify(self, *, transcript: str) -> HandoffIntentResult:
        fallback_result = await self.fallback.classify(transcript=transcript)
        if fallback_result.confidence >= 0.9:
            return fallback_result
        if self.primary is not None:
            try:
                if self.primary_timeout_seconds is None:
                    return await self.primary.classify(transcript=transcript)
                return await asyncio.wait_for(
                    self.primary.classify(transcript=transcript),
                    timeout=self.primary_timeout_seconds,
                )
            except Exception as exc:
                log.warning(
                    "转人工语义分类失败，使用兜底分类: errorType={}, message={}",
                    type(exc).__name__,
                    str(exc),
                )
        return fallback_result


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
            primary_timeout_seconds=timeout_seconds * 0.8,
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


@dataclass(frozen=True, slots=True)
class PendingHandoffConfirmation:
    reason: str
    request_message: str
    tool_call_id: str | None = None


@dataclass(frozen=True, slots=True)
class RecentHandoffConfirmationCandidate:
    transcript: str
    timestamp: datetime


class AiCallHandoffTriggerService:
    RECENT_CONFIRMATION_WINDOW_SECONDS = 2.0
    EXPLICIT_TRANSFER_DELTA_COMMANDS = frozenset(
        {
            "转人工",
            "转人工吧",
            "请转人工",
            "帮我转人工",
            "请帮我转人工",
            "给我转人工",
            "我要转人工",
            "我想转人工",
            "找人工客服",
            "我要找人工客服",
            "找真人客服",
            "我要找真人客服",
        }
    )
    CONFIRMATION_ACCEPT_PATTERNS = (
        "可以",
        "行",
        "好的",
        "好",
        "嗯",
        "是",
        "是的",
        "确认",
        "转吧",
        "帮我转",
        "给我转",
        "那转",
        "可以转",
        "转人工吧",
    )
    CONFIRMATION_TRANSFER_URGE_PATTERNS = (
        "转啊",
        "快转",
        "赶紧转",
        "马上转",
        "现在转",
        "直接转",
        "那就转",
        "你转",
        "帮我转",
        "给我转",
        "怎么还不转",
        "怎么不转",
        "还没转",
        "你不转",
        "你怎么不转",
        "为什么不转",
    )
    CONFIRMATION_DECLINE_PATTERNS = (
        "不可以",
        "不行",
        "不好",
        "不用",
        "不用了",
        "不要",
        "不了",
        "不需要",
        "算了",
        "别转",
        "不用转",
        "不转了",
        "不要转",
        "先不用",
        "继续说",
    )

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
        self._pending_confirmations: dict[str, PendingHandoffConfirmation] = {}
        self._recent_confirmation_candidates: dict[
            str, RecentHandoffConfirmationCandidate
        ] = {}

    async def handle_event(
        self,
        *,
        event: AiCallEvent,
        event_store: InMemoryEventStore,
    ) -> None:
        if event.type == "handoff_tool_requested":
            await self._handle_tool_request(event=event, event_store=event_store)
            return
        is_explicit_delta = self._is_explicit_transfer_delta(event)
        if event.type != "user_transcript_done" and not is_explicit_delta:
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
        if is_realtime_transcript_semantically_rejected(event.payload):
            self._append_ignored(
                event_store,
                event.call_id,
                reason="low_confidence_transcript",
                transcript=transcript,
                confidence=0.0,
                classifier_source="transcript_trust",
            )
            return
        if await self._has_active_handoff(event.call_id):
            self._pending_confirmations.pop(event.call_id, None)
            self._recent_confirmation_candidates.pop(event.call_id, None)
            self._append_ignored(
                event_store,
                event.call_id,
                reason="active_handoff_exists",
                transcript=transcript,
            )
            return
        pending_confirmation = self._pending_confirmations.get(event.call_id)
        if pending_confirmation is not None:
            await self._handle_confirmation_transcript(
                event=event,
                event_store=event_store,
                transcript=transcript,
                confirmation=pending_confirmation,
            )
            return
        self._remember_confirmation_candidate(event, transcript)
        if is_explicit_delta:
            result = HandoffIntentResult(
                matched=True,
                confidence=0.95,
                reason="customer_request",
                summary="用户明确要求转人工",
                source="realtime_delta_guard",
            )
        else:
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
            self._pending_confirmations.pop(event.call_id, None)
            self._recent_confirmation_candidates.pop(event.call_id, None)
            self._append_ignored(
                event_store,
                event.call_id,
                reason="active_handoff_exists",
                transcript=request_message,
                classifier_source="realtime_tool",
            )
            return

        confirmation_required = (
            reason == "business_escalation"
            or event.payload.get("confirmationRequired") is True
        )
        if confirmation_required:
            tool_call_id = event.payload.get("toolCallId")
            confirmation = PendingHandoffConfirmation(
                reason=reason,
                request_message=(
                    "客户确认转人工"
                    if reason == "customer_request"
                    else request_message
                ),
                tool_call_id=tool_call_id if isinstance(tool_call_id, str) else None,
            )
            event_store.append(
                event.call_id,
                "handoff_confirmation_requested",
                "handoff",
                {
                    "source": "customer",
                    "reason": reason,
                    "confidence": 1.0,
                    "classifierSource": "realtime_tool",
                    "summary": self._truncate(request_message, 200),
                    "toolCallId": tool_call_id,
                },
            )
            recent_confirmation = self._pop_recent_confirmation_candidate(event)
            if recent_confirmation is not None:
                await self._confirm_handoff(
                    event=event,
                    event_store=event_store,
                    transcript=recent_confirmation.transcript,
                    confirmation=confirmation,
                    classifier_source="recent_confirmation",
                )
                return
            self._pending_confirmations[event.call_id] = confirmation
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

    async def _handle_confirmation_transcript(
        self,
        *,
        event: AiCallEvent,
        event_store: InMemoryEventStore,
        transcript: str,
        confirmation: PendingHandoffConfirmation,
    ) -> None:
        normalized = self._normalize(transcript)
        if self._is_confirmation_declined(normalized):
            self._pending_confirmations.pop(event.call_id, None)
            self._recent_confirmation_candidates.pop(event.call_id, None)
            event_store.append(
                event.call_id,
                "handoff_confirmation_declined",
                "handoff",
                {
                    "source": "customer",
                    "reason": confirmation.reason,
                    "toolCallId": confirmation.tool_call_id,
                    "transcriptPreview": self._truncate(transcript, 120),
                },
            )
            return

        if not self._is_confirmation_accepted(normalized):
            self._pending_confirmations.pop(event.call_id, None)
            self._recent_confirmation_candidates.pop(event.call_id, None)
            self._append_ignored(
                event_store,
                event.call_id,
                reason="handoff_confirmation_unresolved",
                transcript=transcript,
                confidence=1.0,
                classifier_source="confirmation_gate",
            )
            return

        self._pending_confirmations.pop(event.call_id, None)
        await self._confirm_handoff(
            event=event,
            event_store=event_store,
            transcript=transcript,
            confirmation=confirmation,
            classifier_source="confirmation_gate",
        )

    async def _confirm_handoff(
        self,
        *,
        event: AiCallEvent,
        event_store: InMemoryEventStore,
        transcript: str,
        confirmation: PendingHandoffConfirmation,
        classifier_source: str,
    ) -> None:
        self._recent_confirmation_candidates.pop(event.call_id, None)
        event_store.append(
            event.call_id,
            "handoff_confirmation_confirmed",
            "handoff",
            {
                "source": "customer",
                "reason": confirmation.reason,
                "toolCallId": confirmation.tool_call_id,
                "transcriptPreview": self._truncate(transcript, 120),
            },
        )
        event_store.append(
            event.call_id,
            "handoff_intent_detected",
            "handoff",
            {
                "source": "customer",
                "reason": confirmation.reason,
                "confidence": 1.0,
                "classifierSource": classifier_source,
                "summary": self._truncate(confirmation.request_message, 200),
                "toolCallId": confirmation.tool_call_id,
                "transcriptPreview": self._truncate(transcript, 120),
            },
        )
        try:
            handoff = await self._create_handoff(
                call_id=event.call_id,
                reason=confirmation.reason,
                request_message=self._truncate(confirmation.request_message, 500),
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
                "reason": confirmation.reason,
                "confidence": 1.0,
                "classifierSource": classifier_source,
                "toolCallId": confirmation.tool_call_id,
                "transcriptPreview": self._truncate(transcript, 120),
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
        text = event.payload.get("text")
        stash = event.payload.get("stash")
        if isinstance(text, str) or isinstance(stash, str):
            return f"{text or ''}{stash or ''}".strip()
        value = event.payload.get("transcript") or event.payload.get("delta")
        return value.strip() if isinstance(value, str) else ""

    @classmethod
    def _is_explicit_transfer_delta(cls, event: AiCallEvent) -> bool:
        if event.type != "user_transcript_delta":
            return False
        transcript = cls._transcript_text(event)
        normalized = cls._normalize(transcript)
        return normalized in cls.EXPLICIT_TRANSFER_DELTA_COMMANDS

    def _remember_confirmation_candidate(
        self,
        event: AiCallEvent,
        transcript: str,
    ) -> None:
        normalized = self._normalize(transcript)
        if self._is_confirmation_declined(normalized):
            self._recent_confirmation_candidates.pop(event.call_id, None)
            return
        if self._is_confirmation_accepted(normalized):
            self._recent_confirmation_candidates[event.call_id] = (
                RecentHandoffConfirmationCandidate(
                    transcript=transcript,
                    timestamp=event.timestamp,
                )
            )
            return
        self._recent_confirmation_candidates.pop(event.call_id, None)

    def _pop_recent_confirmation_candidate(
        self,
        event: AiCallEvent,
    ) -> RecentHandoffConfirmationCandidate | None:
        candidate = self._recent_confirmation_candidates.pop(event.call_id, None)
        if candidate is None:
            return None
        age_seconds = (event.timestamp - candidate.timestamp).total_seconds()
        if 0 <= age_seconds <= self.RECENT_CONFIRMATION_WINDOW_SECONDS:
            return candidate
        return None

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
    def _normalize(text: str) -> str:
        return "".join(
            char.lower() for char in text.strip() if char not in " \t\r\n，。！？,.!?；;：:、"
        )

    @classmethod
    def _is_confirmation_accepted(cls, normalized: str) -> bool:
        return any(
            pattern in normalized
            for pattern in (
                cls.CONFIRMATION_ACCEPT_PATTERNS
                + cls.CONFIRMATION_TRANSFER_URGE_PATTERNS
            )
        )

    @classmethod
    def _is_confirmation_declined(cls, normalized: str) -> bool:
        if any(pattern in normalized for pattern in cls.CONFIRMATION_TRANSFER_URGE_PATTERNS):
            return False
        return any(pattern in normalized for pattern in cls.CONFIRMATION_DECLINE_PATTERNS)

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
            self.transcript_trigger_enabled
            and (
                event.type == "user_transcript_done"
                or self.trigger_service._is_explicit_transfer_delta(event)
            )
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
