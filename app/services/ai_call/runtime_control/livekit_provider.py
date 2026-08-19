from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from sqlalchemy import select

from app.services.ai_call.livekit_egress import (
    LiveKitEgressAlreadyCompleteError,
    LiveKitEgressNotFoundError,
)
from app.services.ai_call.prompt_config import (
    PromptEffectiveConfig,
    PromptResolveContext,
    resolve_static_prompt_snapshot,
)
from app.services.ai_call.runtime_control.customer_track import customer_track_keys
from app.services.ai_call.runtime_control.dialogue_bridge import (
    OwnerDialogueFinalizeResult,
    OwnerRuntimeDialogueBridge,
)
from app.services.ai_call.runtime_control.dialogue_repository import OwnerDialogueFence
from app.services.ai_call.runtime_control.effect_repository import (
    EffectClaim,
    ProviderObservation,
    ProviderObservationKind,
)
from app.services.ai_call.runtime_control.handoff_handlers import (
    AgentMediaObservation,
)
from app.services.ai_call.runtime_control.owner_repository import RuntimeOwnerRepository
from app.services.ai_call.session_registry import (
    CallSession,
    CallSessionStatus,
    KnowledgeRuntimeContext,
)


@dataclass(frozen=True, slots=True)
class RuntimeProviderResource:
    call_id: str
    room_name: str
    customer_participant_identity: str
    agent_participant_identity: str
    callee_phone_number: str | None = None
    egress_id: str | None = None
    voice: str | None = None
    tenant_id: str | None = None
    runtime_owner_id: str | None = None
    runtime_fencing_token: int | None = None
    egress_scope: str | None = None
    prompt_effective_config: PromptEffectiveConfig | None = None
    knowledge_context: KnowledgeRuntimeContext | None = None


class RuntimeProviderResourceResolver(Protocol):
    async def resolve(self, effect: EffectClaim) -> RuntimeProviderResource: ...


class DatabaseRuntimeProviderResourceResolver:
    """Loads immutable provider inputs, then closes the DB session before I/O."""

    def __init__(
        self,
        session_factory: Any,
        *,
        orchestrator: Any | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._orchestrator = orchestrator

    async def resolve(self, claim: EffectClaim) -> RuntimeProviderResource:
        from app.api.v1.ai_call.model import AiCallRecordModel
        from app.services.ai_call.runtime_control.models import (
            AiCallRuntimeCommandModel,
            AiCallRuntimeEffectModel,
        )

        async with self._session_factory() as session:
            record = await session.scalar(
                select(AiCallRecordModel).where(
                    AiCallRecordModel.tenant_id == claim.tenant_id,
                    AiCallRecordModel.call_id == claim.call_id,
                )
            )
            current_effect = await session.scalar(
                select(AiCallRuntimeEffectModel).where(
                    AiCallRuntimeEffectModel.id == claim.effect_id,
                    AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                    AiCallRuntimeEffectModel.call_id == claim.call_id,
                    AiCallRuntimeEffectModel.provider_namespace
                    == claim.provider_namespace,
                )
            )
            if record is None or current_effect is None:
                raise LookupError("runtime provider resource snapshot is missing")
            command = await session.scalar(
                select(AiCallRuntimeCommandModel).where(
                    AiCallRuntimeCommandModel.id == current_effect.command_id,
                    AiCallRuntimeCommandModel.tenant_id == claim.tenant_id,
                    AiCallRuntimeCommandModel.call_id == claim.call_id,
                )
            )
            source_effect = None
            if claim.source_create_effect_id is not None:
                source_effect = await session.scalar(
                    select(AiCallRuntimeEffectModel).where(
                        AiCallRuntimeEffectModel.id
                        == claim.source_create_effect_id,
                        AiCallRuntimeEffectModel.tenant_id == claim.tenant_id,
                        AiCallRuntimeEffectModel.call_id == claim.call_id,
                        AiCallRuntimeEffectModel.provider_namespace
                        == claim.provider_namespace,
                    )
                )
                if source_effect is None:
                    raise LookupError("runtime provider source effect is missing")

            voice = self._payload_voice(
                getattr(command, "payload_json", None) if command is not None else None
            )
            prompt_effective_config = None
            knowledge_context = None
            if claim.effect_type == "ATTACH_AGENT_PARTICIPANT":
                prompt_effective_config = await self._resolve_prompt_effective_config(
                    session,
                    record,
                    command,
                )
                knowledge_context = await self._resolve_knowledge_context(
                    session,
                    record,
                )
            egress_scope = "main"
            track_effect = current_effect
            if claim.effect_type in {"START_TRACK_EGRESS", "STOP_TRACK_EGRESS"}:
                egress_scope = "participant"
                track_effect = source_effect or current_effect
                identity = str(record.participant_identity or "")
                _, provider_key, resource_key = customer_track_keys(
                    str(record.call_id), identity
                )
                if (
                    track_effect.effect_type != "START_TRACK_EGRESS"
                    or track_effect.provider_idempotency_key != provider_key
                    or track_effect.resource_key != resource_key
                    or track_effect.resource_generation != 1
                    or current_effect.resource_key != resource_key
                    or current_effect.resource_generation != 1
                ):
                    raise ProviderPreconditionError(
                        "customer_track_resource_mismatch"
                    )
            resource_generation = int(current_effect.resource_generation)
            reference_effect = (
                track_effect
                if egress_scope == "participant"
                else source_effect or current_effect
            )
            egress_id = str(
                getattr(reference_effect, "provider_reference", "")
                or ""
            ) or None
            return RuntimeProviderResource(
                call_id=str(record.call_id),
                room_name=str(record.room_name),
                customer_participant_identity=str(record.participant_identity),
                agent_participant_identity=(
                    f"agent-{record.call_id}-g{resource_generation}"
                ),
                callee_phone_number=(
                    str(record.callee_phone_number)
                    if record.callee_phone_number is not None
                    else None
                ),
                egress_id=egress_id,
                voice=voice,
                tenant_id=claim.tenant_id,
                runtime_owner_id=claim.processing_owner_id,
                runtime_fencing_token=claim.processing_fencing_token,
                egress_scope=egress_scope,
                prompt_effective_config=prompt_effective_config,
                knowledge_context=knowledge_context,
            )

    async def _resolve_prompt_effective_config(self, session, record, command):
        if self._orchestrator is None:
            return None
        from app.api.v1.ai_call.crud import AiCallRecordRepository
        from app.api.v1.ai_call.service import (
            _build_prompt_composer,
            _build_prompt_resolver,
        )

        payload = self._payload(getattr(command, "payload_json", None))
        business_params = payload.get("business_params")
        if not isinstance(business_params, dict):
            business_params = {}
        context = PromptResolveContext(
            call_id=str(record.call_id),
            tenant_id=str(record.tenant_id),
            business_id=(
                str(record.business_id) if record.business_id is not None else None
            ),
            scene_code=(
                str(record.scene_code) if record.scene_code is not None else None
            ),
            business_params=business_params,
        )
        prompt_snapshot = payload.get("prompt_snapshot")
        result = resolve_static_prompt_snapshot(
            prompt_snapshot if isinstance(prompt_snapshot, dict) else None,
            context,
        )
        if result is None:
            result = await _build_prompt_resolver(
                AiCallRecordRepository(session),
                self._orchestrator,
            ).resolve(context)
        return _build_prompt_composer(self._orchestrator).compose(result)

    @staticmethod
    async def _resolve_knowledge_context(
        session: Any,
        record: Any,
    ) -> KnowledgeRuntimeContext | None:
        if getattr(record, "business_type", None) != "outbound_attempt":
            return None
        business_id = str(getattr(record, "business_id", "") or "")
        if (
            not business_id.isdecimal()
            or str(int(business_id)) != business_id
        ):
            return None
        from app.api.v1.ai_call.crud import AiCallRecordRepository
        from app.services.ai_call.knowledge import parse_knowledge_runtime_context

        tenant_id = str(record.tenant_id)
        task_snapshot = await AiCallRecordRepository(
            session
        ).get_outbound_attempt_task_snapshot(
            int(business_id),
            tenant_id=tenant_id,
        )
        if task_snapshot is None:
            return None
        task_id, snapshot_json = task_snapshot
        return parse_knowledge_runtime_context(
            snapshot_json,
            tenant_id=tenant_id,
            task_id=task_id,
        )

    @staticmethod
    def _payload_voice(payload_json: str | None) -> str | None:
        payload = DatabaseRuntimeProviderResourceResolver._payload(payload_json)
        voice = payload.get("voice")
        return str(voice).strip() or None if isinstance(voice, str) else None

    @staticmethod
    def _payload(payload_json: str | None) -> dict[str, Any]:
        if not payload_json:
            return {}
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload


class RuntimeRoomManager(Protocol):
    async def create_room(self, room_name: str) -> None: ...

    async def room_exists(self, room_name: str) -> bool: ...

    async def delete_room(self, room_name: str) -> None: ...

    async def remove_participant(self, room_name: str, identity: str) -> None: ...

    async def participant_exists(self, room_name: str, identity: str) -> bool: ...

    async def get_participant_media(self, room_name: str, identity: str): ...


class RuntimeAgentManager(Protocol):
    async def start(self, resource: RuntimeProviderResource) -> str: ...

    async def exists(self, call_id: str) -> bool: ...

    async def stop(self, call_id: str) -> None: ...

    async def finalize_dialogue(
        self,
        call_id: str,
        *,
        ended_at: datetime,
    ) -> OwnerDialogueFinalizeResult: ...

    async def shutdown(self) -> None: ...


class RuntimeSipClient(Protocol):
    async def create_participant(self, **kwargs): ...


class RuntimeEgressManager(Protocol):
    def build_object_name(self, call_id: str) -> str: ...

    def build_participant_object_name(
        self,
        *,
        call_id: str,
        track_role: str,
        participant_identity: str,
    ) -> str: ...

    async def start_room_audio_recording(self, **kwargs): ...

    async def start_participant_audio_recording(self, **kwargs): ...

    async def stop_egress(self, egress_id: str): ...

    async def get_egress(self, egress_id: str): ...

    async def find_room_audio_recording(self, room_name: str, object_name: str): ...

    async def get_egress_status(self, egress_id: str) -> str | None: ...


class ProviderResourceNotFoundError(LookupError):
    pass


class ProviderReferenceMissingError(RuntimeError):
    pass


class ProviderPolicyDeniedError(RuntimeError):
    pass


class ProviderPreconditionError(ValueError):
    pass


class _OwnerAgentLocalHandle:
    def __init__(self, manager: OwnerRuntimeAgentManager, call_id: str) -> None:
        self._manager = manager
        self._call_id = call_id

    async def fail_closed(self) -> None:
        await self._manager.fail_closed(self._call_id)

    async def start_opening(self) -> None:
        await self._manager.start_opening(self._call_id)

    async def shutdown(self) -> None:
        await self._manager.shutdown_call(self._call_id)


class OwnerRuntimeAgentManager:
    """Owns the process-local realtime runner for the fenced Runtime Owner."""

    def __init__(
        self,
        *,
        orchestrator: Any,
        runtime_registry: Any,
        session_factory: Any | None = None,
        dialogue_bridge: OwnerRuntimeDialogueBridge | None = None,
    ) -> None:
        self._orchestrator = orchestrator
        self._runtime_registry = runtime_registry
        self._session_factory = session_factory
        self._dialogue_bridge = dialogue_bridge
        self._active_identities: dict[str, str] = {}
        self._dialogue_fences: dict[str, OwnerDialogueFence] = {}

    async def start(self, resource: RuntimeProviderResource) -> str:
        current_identity = self._active_identities.get(resource.call_id)
        if current_identity == resource.agent_participant_identity:
            self._bind_sip_connected_observer(resource)
            return current_identity
        if current_identity is not None:
            await self.fail_closed(resource.call_id)

        dialogue_fence = self._dialogue_fence(resource)
        if self._dialogue_bridge is not None:
            if dialogue_fence is None:
                raise RuntimeError("runtime dialogue fence is missing")
            if not await self._dialogue_bridge.bind_call(dialogue_fence):
                raise RuntimeError("runtime dialogue fence was rejected")
            self._dialogue_fences[resource.call_id] = dialogue_fence

        effective_config = (
            self._orchestrator._build_effective_config(
                resource.voice,
                None,
                resource.prompt_effective_config,
            )
            if resource.prompt_effective_config is not None
            else self._orchestrator._build_effective_config(resource.voice, None)
        )
        session = CallSession(
            call_id=resource.call_id,
            room_name=resource.room_name,
            participant_identity=resource.customer_participant_identity,
            status=CallSessionStatus.READY,
            effective_config=effective_config,
            knowledge_context=resource.knowledge_context,
            local_participant_identity=resource.agent_participant_identity,
        )
        self._orchestrator.registry.add(session)
        handle = _OwnerAgentLocalHandle(self, resource.call_id)
        self._active_identities[resource.call_id] = (
            resource.agent_participant_identity
        )
        self._runtime_registry.local_handles[resource.call_id] = handle
        try:
            self._bind_sip_connected_observer(resource)
            await self._orchestrator.agent_runner.start(session)
        except BaseException:
            if dialogue_fence is not None and self._dialogue_bridge is not None:
                self._dialogue_bridge.mark_failed(
                    dialogue_fence,
                    error="agent_start_failed",
                )
            self._active_identities.pop(resource.call_id, None)
            if self._runtime_registry.local_handles.get(resource.call_id) is handle:
                self._runtime_registry.local_handles.pop(resource.call_id, None)
            self._orchestrator.registry.discard(resource.call_id)
            self._unbind_sip_connected_observer(resource.call_id)
            with suppress(Exception):
                await self._orchestrator.agent_runner.stop(resource.call_id)
            raise
        return resource.agent_participant_identity

    async def exists(self, call_id: str) -> bool:
        return call_id in self._active_identities

    async def start_opening(self, call_id: str) -> None:
        await self._orchestrator.start_opening(call_id)

    async def stop(self, call_id: str) -> None:
        await self._stop_agent(call_id)

    async def finalize_dialogue(
        self,
        call_id: str,
        *,
        ended_at: datetime,
    ) -> OwnerDialogueFinalizeResult:
        if call_id in self._active_identities:
            return OwnerDialogueFinalizeResult("pending", 0, 0)
        fence = self._dialogue_fences.pop(call_id, None)
        if fence is None or self._dialogue_bridge is None:
            return OwnerDialogueFinalizeResult("pending", 0, 0)
        return await self._dialogue_bridge.finalize_call(
            fence,
            ended_at=ended_at,
        )

    async def fail_closed(self, call_id: str) -> None:
        await self._stop_agent(call_id)
        fence = self._dialogue_fences.pop(call_id, None)
        if fence is not None and self._dialogue_bridge is not None:
            self._dialogue_bridge.abandon_call(fence)

    async def shutdown_call(self, call_id: str) -> OwnerDialogueFinalizeResult:
        await self.stop(call_id)
        return await self.finalize_dialogue(
            call_id,
            ended_at=datetime.now(timezone.utc),
        )

    async def shutdown(self) -> None:
        call_ids = sorted(
            set(self._active_identities) | set(self._dialogue_fences)
        )
        for call_id in call_ids:
            await self.fail_closed(call_id)

    async def _stop_agent(self, call_id: str) -> None:
        try:
            await self._orchestrator.agent_runner.stop(call_id)
        except Exception:
            if self._dialogue_bridge is not None:
                fence = self._dialogue_fences.get(call_id)
                if fence is not None:
                    self._dialogue_bridge.mark_failed(
                        fence,
                        error="agent_stop_failed",
                    )
            raise
        self._active_identities.pop(call_id, None)
        self._runtime_registry.local_handles.pop(call_id, None)
        self._unbind_sip_connected_observer(call_id)
        self._orchestrator.registry.discard(call_id)

    @staticmethod
    def _dialogue_fence(
        resource: RuntimeProviderResource,
    ) -> OwnerDialogueFence | None:
        if (
            resource.tenant_id is None
            or resource.runtime_owner_id is None
            or resource.runtime_fencing_token is None
        ):
            return None
        return OwnerDialogueFence(
            tenant_id=resource.tenant_id,
            call_id=resource.call_id,
            owner_id=resource.runtime_owner_id,
            fencing_token=resource.runtime_fencing_token,
        )

    def _bind_sip_connected_observer(self, resource: RuntimeProviderResource) -> None:
        if (
            resource.tenant_id is None
            or resource.runtime_owner_id is None
            or resource.runtime_fencing_token is None
        ):
            return
        if self._session_factory is None:
            raise RuntimeError("runtime connected fact session factory is missing")
        audio_transport = getattr(self._orchestrator.agent_runner, "audio_transport", None)
        bind = getattr(audio_transport, "bind_sip_connected_observer", None)
        if bind is None:
            raise RuntimeError("runtime audio transport cannot observe SIP connected facts")

        async def persist(sip_call_status: str) -> bool:
            async with self._session_factory.begin() as session:
                accepted = await RuntimeOwnerRepository(session).record_sip_connected(
                    tenant_id=resource.tenant_id,
                    call_id=resource.call_id,
                    owner_id=resource.runtime_owner_id,
                    fencing_token=resource.runtime_fencing_token,
                    sip_call_status=sip_call_status,
                )
            if accepted:
                await self._orchestrator.start_opening(resource.call_id)
            return accepted

        bind(resource.call_id, persist)

    def _unbind_sip_connected_observer(self, call_id: str) -> None:
        audio_transport = getattr(self._orchestrator.agent_runner, "audio_transport", None)
        unbind = getattr(audio_transport, "unbind_sip_connected_observer", None)
        if unbind is not None:
            unbind(call_id)


_TERMINAL_EGRESS_STATUSES = frozenset(
    {
        "EGRESS_COMPLETE",
        "EGRESS_FAILED",
        "EGRESS_ABORTED",
        "COMPLETE",
        "FAILED",
        "ABORTED",
    }
)


class LiveKitRuntimeProvider:
    """Owner-only adapter; provider I/O starts after resource snapshot loading."""

    def __init__(
        self,
        *,
        resolver: RuntimeProviderResourceResolver,
        room_manager: RuntimeRoomManager,
        agent_manager: RuntimeAgentManager,
        sip_client: RuntimeSipClient,
        egress_manager: RuntimeEgressManager,
        dialogue_bridge: OwnerRuntimeDialogueBridge | None = None,
        handoff_trigger_worker: Any | None = None,
        provider_namespace: str = "livekit:unconfigured",
        allowed_callee_phone_number: str | None = None,
        main_recording_enabled: bool = False,
        oss_config_provider: Callable[[], dict | None] | None = None,
    ) -> None:
        self._resolver = resolver
        self._room_manager = room_manager
        self._agent_manager = agent_manager
        self._sip_client = sip_client
        self._egress_manager = egress_manager
        self._dialogue_bridge = dialogue_bridge
        self._handoff_trigger_worker = handoff_trigger_worker
        self.provider_namespace = provider_namespace
        self.main_recording_enabled = main_recording_enabled
        self._oss_config_provider = oss_config_provider or (lambda: None)
        self._allowed_callee_phone_number = (
            str(allowed_callee_phone_number).strip()
            if allowed_callee_phone_number is not None
            else None
        )

    async def start(self) -> None:
        if self._handoff_trigger_worker is not None:
            await self._handoff_trigger_worker.start()
        if self._dialogue_bridge is not None:
            await self._dialogue_bridge.start()

    async def stop(self) -> None:
        if self._handoff_trigger_worker is not None:
            self._handoff_trigger_worker.detach_all()
            await self._handoff_trigger_worker.stop()
        await self._agent_manager.shutdown()
        if self._dialogue_bridge is not None:
            await self._dialogue_bridge.stop()

    async def finalize_dialogue(
        self,
        owner_lease: Any,
        *,
        ended_at: datetime,
    ) -> OwnerDialogueFinalizeResult:
        return await self._agent_manager.finalize_dialogue(
            owner_lease.call_id,
            ended_at=ended_at,
        )

    async def apply(self, effect: EffectClaim) -> ProviderObservation:
        try:
            resource = await self._resolver.resolve(effect)
            if effect.effect_type == "START_EGRESS":
                return await self._apply_start_egress(effect, resource)
            if effect.effect_type == "STOP_EGRESS":
                return await self._apply_stop_egress(effect, resource)
            if effect.effect_type == "START_TRACK_EGRESS":
                return await self._apply_start_track_egress(effect, resource)
            if effect.effect_type == "STOP_TRACK_EGRESS":
                return await self._apply_stop_track_egress(effect, resource)
            if not effect.reconcile_only:
                provider_reference = await self._mutate(effect, resource)
            else:
                provider_reference = None
            return await self._observe(effect, resource, provider_reference)
        except (ProviderResourceNotFoundError, LiveKitEgressNotFoundError):
            if effect.effect_type in {
                "HANGUP_SIP",
                "DISCONNECT_AGENT_PARTICIPANT",
                "STOP_EGRESS",
                "STOP_TRACK_EGRESS",
                "DELETE_ROOM",
            }:
                return ProviderObservation(
                    kind=ProviderObservationKind.TERMINAL_CONFIRMED
                )
            return ProviderObservation(kind=ProviderObservationKind.RESOURCE_ABSENT)
        except TimeoutError as exc:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                error_message=f"{type(exc).__name__}: {exc!s}",
            )
        except ProviderReferenceMissingError:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                error_message="provider_reference_missing",
            )
        except ProviderPolicyDeniedError:
            return ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                error_message="callee_not_allowed",
            )
        except ProviderPreconditionError as exc:
            return ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                failure_code=str(exc),
                error_message=str(exc),
            )

    async def query_agent_media(
        self,
        room_name: str,
        participant_identity: str,
    ) -> AgentMediaObservation:
        fact = await self._room_manager.get_participant_media(
            room_name,
            participant_identity,
        )
        if fact is None:
            return AgentMediaObservation(ready=False)
        return AgentMediaObservation(
            ready=bool(fact.microphone_ready),
            participant_identity=str(fact.participant_identity),
            participant_sid=str(fact.participant_sid or "") or None,
            track_sid=str(fact.track_sid or "") or None,
        )

    async def _mutate(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
    ) -> str | None:
        if effect.effect_type == "CREATE_ROOM":
            await self._room_manager.create_room(resource.room_name)
            return resource.room_name
        if effect.effect_type == "ATTACH_AGENT_PARTICIPANT":
            return await self._agent_manager.start(resource)
        if effect.effect_type == "CREATE_SIP_PARTICIPANT":
            if not resource.callee_phone_number:
                raise ProviderPreconditionError("callee_phone_number_missing")
            if (
                self._allowed_callee_phone_number is not None
                and resource.callee_phone_number
                != self._allowed_callee_phone_number
            ):
                raise ProviderPolicyDeniedError("callee does not match allowlist")
            result = await self._sip_client.create_participant(
                room_name=resource.room_name,
                participant_identity=resource.customer_participant_identity,
                callee_phone_number=resource.callee_phone_number,
                wait_until_answered=False,
            )
            return str(getattr(result, "sip_call_id", "") or "") or None
        if effect.effect_type == "HANGUP_SIP":
            await self._room_manager.remove_participant(
                resource.room_name,
                resource.customer_participant_identity,
            )
            return None
        if effect.effect_type == "DISCONNECT_AGENT_PARTICIPANT":
            await self._agent_manager.stop(resource.call_id)
            await self._room_manager.remove_participant(
                resource.room_name,
                resource.agent_participant_identity,
            )
            return None
        if effect.effect_type == "STOP_EGRESS":
            if not resource.egress_id:
                raise ProviderReferenceMissingError("egress id is missing")
            result = await self._egress_manager.stop_egress(resource.egress_id)
            return str(getattr(result, "egress_id", "") or "") or None
        if effect.effect_type == "DELETE_ROOM":
            await self._room_manager.delete_room(resource.room_name)
            return None
        raise LookupError(f"unsupported LiveKit effect type {effect.effect_type}")

    async def _apply_start_egress(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
    ) -> ProviderObservation:
        object_name = self._egress_manager.build_object_name(resource.call_id)
        if effect.reconcile_only:
            fact = (
                await self._egress_manager.get_egress(resource.egress_id)
                if resource.egress_id
                else await self._egress_manager.find_room_audio_recording(
                    resource.room_name,
                    object_name,
                )
            )
            if fact is None:
                return ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_ABSENT,
                    object_name=object_name,
                )
            return self._egress_fact_observation(
                fact,
                kind=ProviderObservationKind.RESOURCE_PRESENT,
            )

        oss_config = self._oss_config_provider()
        if not oss_config:
            return ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                object_name=object_name,
                failure_code="oss_config_missing",
                error_message="oss_config_missing",
            )
        try:
            result = await self._egress_manager.start_room_audio_recording(
                room_name=resource.room_name,
                call_id=resource.call_id,
                oss_config=oss_config,
            )
        except TimeoutError:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=object_name,
                failure_code="egress_start_timeout",
                error_message="egress_start_timeout",
            )
        egress_id = str(getattr(result, "egress_id", "") or "")
        if not egress_id:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=object_name,
                failure_code="provider_reference_missing",
                error_message="provider_reference_missing",
            )
        fact = await self._egress_manager.get_egress(egress_id)
        if fact is None:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                provider_reference=egress_id,
                provider_status=str(getattr(result, "status", "") or "") or None,
                object_name=str(getattr(result, "object_name", "") or object_name),
                started_at=getattr(result, "started_at", None),
                failure_code="egress_start_unconfirmed",
                error_message="egress_start_unconfirmed",
            )
        return self._egress_fact_observation(
            fact,
            kind=ProviderObservationKind.RESOURCE_PRESENT,
        )

    async def _apply_stop_egress(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
    ) -> ProviderObservation:
        object_name = self._egress_manager.build_object_name(resource.call_id)
        egress_id = resource.egress_id
        if not egress_id:
            fact = await self._egress_manager.find_room_audio_recording(
                resource.room_name,
                object_name,
            )
            if fact is None:
                return ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_ABSENT,
                    object_name=object_name,
                )
            egress_id = str(getattr(fact, "egress_id", "") or "") or None
        if not egress_id:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=object_name,
                failure_code="provider_reference_missing",
                error_message="provider_reference_missing",
            )
        stop_fact = None
        if not effect.reconcile_only:
            try:
                stop_fact = await self._egress_manager.stop_egress(egress_id)
            except LiveKitEgressAlreadyCompleteError:
                pass
            except TimeoutError:
                return ProviderObservation(
                    kind=ProviderObservationKind.UNCERTAIN,
                    provider_reference=egress_id,
                    object_name=object_name,
                    failure_code="egress_stop_timeout",
                    error_message="egress_stop_timeout",
                )
        fact = await self._egress_manager.get_egress(egress_id)
        if fact is None:
            stop_status = str(getattr(stop_fact, "status", "") or "").upper()
            if stop_fact is not None:
                return self._egress_fact_observation(
                    stop_fact,
                    kind=(
                        ProviderObservationKind.TERMINAL_CONFIRMED
                        if stop_status in _TERMINAL_EGRESS_STATUSES
                        else ProviderObservationKind.ACCEPTED
                    ),
                )
            return ProviderObservation(
                kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                provider_reference=egress_id,
                object_name=object_name,
            )
        status = str(getattr(fact, "status", "") or "").upper()
        return self._egress_fact_observation(
            fact,
            kind=(
                ProviderObservationKind.TERMINAL_CONFIRMED
                if status in _TERMINAL_EGRESS_STATUSES
                else ProviderObservationKind.ACCEPTED
            ),
        )

    async def _apply_start_track_egress(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
    ) -> ProviderObservation:
        object_name = self._egress_manager.build_participant_object_name(
            call_id=resource.call_id,
            track_role="customer",
            participant_identity=resource.customer_participant_identity,
        )
        if effect.reconcile_only:
            fact = (
                await self._egress_manager.get_egress(resource.egress_id)
                if resource.egress_id
                else await self._egress_manager.find_room_audio_recording(
                    resource.room_name,
                    object_name,
                )
            )
            if fact is None:
                return ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_ABSENT,
                    object_name=object_name,
                )
            return self._egress_fact_observation(
                fact,
                kind=ProviderObservationKind.RESOURCE_PRESENT,
                fallback_object_name=object_name,
            )

        oss_config = self._oss_config_provider()
        if not oss_config:
            return ProviderObservation(
                kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                object_name=object_name,
                failure_code="oss_config_missing",
                error_message="oss_config_missing",
            )
        try:
            result = await self._egress_manager.start_participant_audio_recording(
                room_name=resource.room_name,
                call_id=resource.call_id,
                track_role="customer",
                participant_identity=resource.customer_participant_identity,
                oss_config=oss_config,
            )
        except TimeoutError:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=object_name,
                failure_code="egress_start_timeout",
                error_message="egress_start_timeout",
            )
        egress_id = str(getattr(result, "egress_id", "") or "")
        result_object_name = str(getattr(result, "object_name", "") or object_name)
        if not egress_id:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=result_object_name,
                failure_code="provider_reference_missing",
                error_message="provider_reference_missing",
            )
        fact = await self._egress_manager.get_egress(egress_id)
        if fact is None:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                provider_reference=egress_id,
                provider_status=str(getattr(result, "status", "") or "") or None,
                object_name=result_object_name,
                started_at=getattr(result, "started_at", None),
                failure_code="egress_start_unconfirmed",
                error_message="egress_start_unconfirmed",
            )
        return self._egress_fact_observation(
            fact,
            kind=ProviderObservationKind.RESOURCE_PRESENT,
            fallback_object_name=result_object_name,
        )

    async def _apply_stop_track_egress(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
    ) -> ProviderObservation:
        object_name = self._egress_manager.build_participant_object_name(
            call_id=resource.call_id,
            track_role="customer",
            participant_identity=resource.customer_participant_identity,
        )
        egress_id = resource.egress_id
        if not egress_id:
            fact = await self._egress_manager.find_room_audio_recording(
                resource.room_name,
                object_name,
            )
            if fact is None:
                return ProviderObservation(
                    kind=ProviderObservationKind.RESOURCE_ABSENT,
                    object_name=object_name,
                )
            egress_id = str(getattr(fact, "egress_id", "") or "") or None
        if not egress_id:
            return ProviderObservation(
                kind=ProviderObservationKind.UNCERTAIN,
                object_name=object_name,
                failure_code="provider_reference_missing",
                error_message="provider_reference_missing",
            )

        stop_fact = None
        if not effect.reconcile_only:
            try:
                stop_fact = await self._egress_manager.stop_egress(egress_id)
            except LiveKitEgressAlreadyCompleteError:
                pass
            except TimeoutError:
                return ProviderObservation(
                    kind=ProviderObservationKind.UNCERTAIN,
                    provider_reference=egress_id,
                    object_name=object_name,
                    failure_code="egress_stop_timeout",
                    error_message="egress_stop_timeout",
                )
        fact = await self._egress_manager.get_egress(egress_id)
        if fact is None:
            stop_status = str(getattr(stop_fact, "status", "") or "").upper()
            if stop_fact is not None:
                return self._egress_fact_observation(
                    stop_fact,
                    kind=(
                        ProviderObservationKind.TERMINAL_CONFIRMED
                        if stop_status in _TERMINAL_EGRESS_STATUSES
                        else ProviderObservationKind.ACCEPTED
                    ),
                    fallback_object_name=object_name,
                )
            return ProviderObservation(
                kind=ProviderObservationKind.TERMINAL_CONFIRMED,
                provider_reference=egress_id,
                object_name=object_name,
            )
        status = str(getattr(fact, "status", "") or "").upper()
        return self._egress_fact_observation(
            fact,
            kind=(
                ProviderObservationKind.TERMINAL_CONFIRMED
                if status in _TERMINAL_EGRESS_STATUSES
                else ProviderObservationKind.ACCEPTED
            ),
            fallback_object_name=object_name,
        )

    @staticmethod
    def _egress_fact_observation(
        fact: object,
        *,
        kind: ProviderObservationKind,
        fallback_object_name: str | None = None,
    ) -> ProviderObservation:
        return ProviderObservation(
            kind=kind,
            provider_reference=str(getattr(fact, "egress_id", "") or "") or None,
            provider_status=str(getattr(fact, "status", "") or "") or None,
            object_name=(
                str(getattr(fact, "object_name", "") or "")
                or fallback_object_name
            ),
            started_at=getattr(fact, "started_at", None),
            ended_at=getattr(fact, "ended_at", None),
            duration_ms=getattr(fact, "duration_ms", None),
            file_size=getattr(fact, "file_size", None),
        )

    async def _observe(
        self,
        effect: EffectClaim,
        resource: RuntimeProviderResource,
        provider_reference: str | None,
    ) -> ProviderObservation:
        if effect.effect_type == "CREATE_ROOM":
            present = await self._room_manager.room_exists(resource.room_name)
            return ProviderObservation(
                kind=(
                    ProviderObservationKind.RESOURCE_PRESENT
                    if present
                    else (
                        ProviderObservationKind.RESOURCE_ABSENT
                        if effect.reconcile_only
                        else ProviderObservationKind.UNCERTAIN
                    )
                ),
                provider_reference=provider_reference or resource.room_name,
            )
        if effect.effect_type == "ATTACH_AGENT_PARTICIPANT":
            local_exists = await self._agent_manager.exists(resource.call_id)
            participant_exists = await self._room_manager.participant_exists(
                resource.room_name,
                resource.agent_participant_identity,
            )
            return ProviderObservation(
                kind=(
                    ProviderObservationKind.RESOURCE_PRESENT
                    if local_exists and participant_exists
                    else (
                        ProviderObservationKind.RESOURCE_ABSENT
                        if effect.reconcile_only
                        else ProviderObservationKind.UNCERTAIN
                    )
                ),
                provider_reference=provider_reference,
            )
        if effect.effect_type == "CREATE_SIP_PARTICIPANT":
            present = await self._room_manager.participant_exists(
                resource.room_name,
                resource.customer_participant_identity,
            )
            if (
                not present
                and effect.reconcile_only
                and not resource.callee_phone_number
            ):
                return ProviderObservation(
                    kind=ProviderObservationKind.PERMANENT_NO_RESOURCE,
                    error_message="callee_phone_number_missing",
                )
            return ProviderObservation(
                kind=(
                    ProviderObservationKind.RESOURCE_PRESENT
                    if present
                    else (
                        ProviderObservationKind.RESOURCE_ABSENT
                        if effect.reconcile_only
                        else ProviderObservationKind.UNCERTAIN
                    )
                ),
                provider_reference=provider_reference,
            )
        if effect.effect_type == "HANGUP_SIP":
            present = await self._room_manager.participant_exists(
                resource.room_name,
                resource.customer_participant_identity,
            )
            return self._destroy_observation(present)
        if effect.effect_type == "DISCONNECT_AGENT_PARTICIPANT":
            local_exists = await self._agent_manager.exists(resource.call_id)
            participant_exists = await self._room_manager.participant_exists(
                resource.room_name,
                resource.agent_participant_identity,
            )
            return self._destroy_observation(local_exists or participant_exists)
        if effect.effect_type == "STOP_EGRESS":
            if not resource.egress_id:
                return ProviderObservation(
                    kind=ProviderObservationKind.UNCERTAIN,
                    error_message="provider_reference_missing",
                )
            status = await self._egress_manager.get_egress_status(resource.egress_id)
            return ProviderObservation(
                kind=(
                    ProviderObservationKind.TERMINAL_CONFIRMED
                    if status is None
                    or str(status).upper() in _TERMINAL_EGRESS_STATUSES
                    else ProviderObservationKind.ACCEPTED
                ),
                provider_reference=resource.egress_id,
            )
        if effect.effect_type == "DELETE_ROOM":
            present = await self._room_manager.room_exists(resource.room_name)
            return self._destroy_observation(present)
        raise LookupError(f"unsupported LiveKit effect type {effect.effect_type}")

    @staticmethod
    def _destroy_observation(present: bool) -> ProviderObservation:
        return ProviderObservation(
            kind=(
                ProviderObservationKind.ACCEPTED
                if present
                else ProviderObservationKind.TERMINAL_CONFIRMED
            )
        )


def build_livekit_runtime_provider(
    *,
    settings: Any,
    session_factory: Any,
    registry: Any,
) -> LiveKitRuntimeProvider:
    from app.api.v1.ai_call.crud import AiCallRecordRepository
    from app.api.v1.ai_call.service import (
        AiCallService,
        attach_ai_call_event_persistence,
        build_ai_call_handoff_exception_manager,
    )
    from app.api.v1.system.oss.service import OssService
    from app.services.ai_call.handoff_availability_service import (
        AiCallHandoffAvailabilityService,
    )
    from app.services.ai_call.handoff_service import AiCallHandoffService
    from app.services.ai_call.handoff_trigger_service import (
        AiCallHandoffTriggerService,
        AiCallHandoffTriggerWorker,
        build_default_handoff_intent_classifier,
    )
    from app.services.ai_call.livekit_egress import LiveKitEgressManager
    from app.services.ai_call.livekit_room import LiveKitRoomManager
    from app.services.ai_call.livekit_sip import LiveKitSipClient, SipOutboundConfig
    from app.services.ai_call.orchestrator import AiCallOrchestrator
    from app.services.ai_call.runtime_control.webhook_service import (
        livekit_provider_namespace,
    )

    room_manager = LiveKitRoomManager(
        livekit_url=settings.LIVEKIT_URL,
        browser_livekit_url=settings.LIVEKIT_BROWSER_URL,
        api_key=settings.LIVEKIT_API_KEY,
        api_secret=settings.LIVEKIT_API_SECRET,
        browser_token_ttl_seconds=settings.LIVEKIT_BROWSER_TOKEN_TTL_SECONDS,
    )
    orchestrator = AiCallOrchestrator.from_settings(settings)
    attach_ai_call_event_persistence(orchestrator.event_store)
    dialogue_bridge = OwnerRuntimeDialogueBridge(session_factory)
    dialogue_bridge.attach_event_store(orchestrator.event_store)
    handoff_trigger_worker = None
    if settings.AI_CALL_HANDOFF_AUTO_TRIGGER_ENABLED:
        exception_manager = build_ai_call_handoff_exception_manager(
            orchestrator,
            session_factory,
        )

        def service_factory(db):
            repository = AiCallRecordRepository(db)
            return AiCallService(
                orchestrator,
                handoff_service=AiCallHandoffService(
                    repository,
                    request_timeout_seconds=settings.AI_CALL_HANDOFF_TOTAL_WAIT_SECONDS,
                ),
                handoff_exception_manager=exception_manager,
            )

        trigger_service = AiCallHandoffTriggerService(
            session_factory,
            service_factory,
            build_default_handoff_intent_classifier(
                base_url=settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL,
                api_key=settings.EFFECTIVE_LLM_API_KEY,
                model=settings.LLM_MODEL or settings.POST_ANALYSIS_MODEL or "qwen-plus",
                timeout_seconds=settings.AI_CALL_HANDOFF_INTENT_TIMEOUT_SECONDS,
            ),
            enabled=True,
            customer_intent_enabled=settings.AI_CALL_HANDOFF_CUSTOMER_INTENT_ENABLED,
            threshold=settings.AI_CALL_HANDOFF_INTENT_THRESHOLD,
            timeout_seconds=settings.AI_CALL_HANDOFF_INTENT_TIMEOUT_SECONDS,
            availability_service_factory=AiCallHandoffAvailabilityService,
        )
        handoff_trigger_worker = AiCallHandoffTriggerWorker(
            trigger_service,
            transcript_trigger_enabled=True,
        )
        handoff_trigger_worker.attach_event_store(orchestrator.event_store)
    return LiveKitRuntimeProvider(
        resolver=DatabaseRuntimeProviderResourceResolver(
            session_factory,
            orchestrator=orchestrator,
        ),
        room_manager=room_manager,
        agent_manager=OwnerRuntimeAgentManager(
            orchestrator=orchestrator,
            runtime_registry=registry,
            session_factory=session_factory,
            dialogue_bridge=dialogue_bridge,
        ),
        sip_client=LiveKitSipClient(
            config=SipOutboundConfig.from_settings(settings),
            livekit_url=settings.LIVEKIT_URL,
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
        ),
        egress_manager=LiveKitEgressManager(
            livekit_url=settings.LIVEKIT_URL,
            api_key=settings.LIVEKIT_API_KEY,
            api_secret=settings.LIVEKIT_API_SECRET,
            timeout_seconds=settings.AI_CALL_EGRESS_TIMEOUT_SECONDS,
            stop_timeout_seconds=settings.AI_CALL_EGRESS_STOP_TIMEOUT_SECONDS,
            object_prefix=settings.AI_CALL_RECORDING_OBJECT_PREFIX,
            file_type=settings.AI_CALL_RECORDING_FORMAT,
            participant_file_type=settings.AI_CALL_PARTICIPANT_RECORDING_FORMAT,
        ),
        dialogue_bridge=dialogue_bridge,
        handoff_trigger_worker=handoff_trigger_worker,
        provider_namespace=livekit_provider_namespace(settings),
        allowed_callee_phone_number=(
            settings.AI_CALL_OUTBOUND_LINPHONE_ALLOWED_CALLEE
        ),
        main_recording_enabled=bool(settings.AI_CALL_RECORDING_ENABLED),
        oss_config_provider=OssService.active_config,
    )
