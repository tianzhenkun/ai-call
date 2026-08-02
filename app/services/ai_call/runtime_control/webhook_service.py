from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import log
from app.services.ai_call.runtime_control.webhook_repository import (
    RuntimeWebhookRepository,
    WebhookReceiveDecision,
    WebhookReceiveIntent,
    sanitize_webhook_payload,
)


def livekit_provider_namespace(settings: Any) -> str:
    endpoint = str(getattr(settings, "LIVEKIT_URL", "") or "").strip()
    api_key = str(getattr(settings, "LIVEKIT_API_KEY", "") or "").strip()
    digest = hashlib.sha256(f"{endpoint}|{api_key}".encode()).hexdigest()
    return f"livekit:{digest[:32]}"


def livekit_event_dedupe_key(
    event_type: str,
    payload: Mapping[str, object] | None,
) -> str:
    provider_event_id = (payload or {}).get("id")
    if isinstance(provider_event_id, str) and provider_event_id:
        if len(provider_event_id) <= 160:
            return provider_event_id
        return hashlib.sha256(provider_event_id.encode()).hexdigest()
    sanitized = sanitize_webhook_payload(event_type=event_type, payload=payload)
    encoded = json.dumps(
        sanitized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


class RuntimeWebhookIngressService:
    def __init__(self, session: AsyncSession, settings: Any) -> None:
        self._repository = RuntimeWebhookRepository(session)
        self._provider_namespace = livekit_provider_namespace(settings)

    async def receive_livekit(
        self,
        *,
        event_type: str,
        room_name: str | None,
        participant_identity: str | None,
        payload: Mapping[str, object] | None,
    ) -> WebhookReceiveDecision:
        return await self._repository.receive(
            WebhookReceiveIntent(
                provider="livekit",
                provider_namespace=self._provider_namespace,
                dedupe_key=livekit_event_dedupe_key(event_type, payload),
                event_type=event_type,
                room_name=room_name,
                participant_identity=participant_identity,
                payload=payload,
            )
        )


class RuntimeWebhookWorker:
    """DB-only Inbox/Quarantine worker; it never invokes LiveKit or SIP."""

    def __init__(
        self,
        session_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        *,
        worker_id: str,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self.worker_id = worker_id
        self._poll_interval_seconds = max(0.01, poll_interval_seconds)
        self._stop_event = asyncio.Event()
        self._runner_task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._runner_task is not None and not self._runner_task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stop_event = asyncio.Event()
        self._runner_task = asyncio.create_task(
            self._run_loop(),
            name=f"ai-call-runtime-webhook-{self.worker_id}",
        )

    async def stop(self) -> None:
        task = self._runner_task
        if task is None:
            return
        self._stop_event.set()
        try:
            await task
        except asyncio.CancelledError:
            raise
        finally:
            if self._runner_task is task:
                self._runner_task = None

    async def run_once(self) -> bool:
        inbox_claim = None
        async with self._session_factory() as session, session.begin():
            inbox_claim = await RuntimeWebhookRepository(session).claim_inbox(
                self.worker_id
            )
        if inbox_claim is not None:
            async with self._session_factory() as session, session.begin():
                await RuntimeWebhookRepository(session).apply_inbox_media(inbox_claim)
            return True

        quarantine_claim = None
        async with self._session_factory() as session, session.begin():
            quarantine_claim = await RuntimeWebhookRepository(
                session
            ).claim_quarantine(self.worker_id)
        if quarantine_claim is None:
            return False
        async with self._session_factory() as session, session.begin():
            await RuntimeWebhookRepository(session).resolve_quarantine(
                quarantine_claim
            )
        return True

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(
                    "AI Call Runtime webhook worker 单轮失败，errorType={}",
                    type(exc).__name__,
                )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._poll_interval_seconds,
                )
            except TimeoutError:
                pass
