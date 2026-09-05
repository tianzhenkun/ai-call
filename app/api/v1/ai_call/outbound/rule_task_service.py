from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import status
from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.v1.ai_call.model import (
    AiCallPromptProfileModel,
    AiCallPromptProfileVersionModel,
    AiCallRecordModel,
    AiCallSemanticAnalysisModel,
    AiCallVoiceProfileModel,
)
from app.api.v1.ai_call.voice.model import AiCallTenantVoiceProfileModel
from app.config.setting import settings
from app.core.exceptions import CustomException
from app.services.ai_call.call_outcome import detect_answer_type
from app.services.ai_call.credit_metering import (
    CreditMeteringClient,
    require_credit_eligible_for_request,
)
from app.services.ai_call.livekit_sip import (
    SipOutboundConfig,
    validate_sip_outbound_preflight,
)
from app.services.ai_call.sqlite_serialization import begin_sqlite_immediate_write
from app.utils.id_util import generate_snowflake_id

from .attempt_projection import complete_exception_batch_if_done, refresh_task_counters
from .model import AiCallOutboundValidationModel, AiCallOutboundValidationRowModel
from .rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundExceptionBatchModel,
    AiCallOutboundRuleModel,
    AiCallOutboundTargetModel,
    AiCallOutboundTaskModel,
)
from .rule_task_schema import (
    MAX_RETRY_COUNT,
    AcceptedCommandOut,
    CallRuleIn,
    CallRuleMetadataOut,
    CallRuleOut,
    CreateTaskRequest,
    OutboundTargetOut,
    OutboundTaskLineSnapshotOut,
    OutboundTaskOut,
    RetryableResultMeta,
    SingleValidationRequest,
    UpdateTaskScheduleRequest,
)
from .service import PHONE_PATTERN
from .sip_line_model import AiCallSipLineModel
from .sip_line_schema import SipLineSnapshot
from .sip_line_service import SipLineService

DEFAULT_TARGET_COPY_BATCH_SIZE = 500
MAX_FROZEN_KNOWLEDGE_CHUNKS = 500


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _mask_phone_number(phone_number: str | None) -> str | None:
    if phone_number is None:
        return None
    digits = "".join(character for character in phone_number if character.isdigit())
    if len(digits) <= 7:
        return "***"
    return f"{digits[:3]}****{digits[-4:]}"


class OutboundRuleTaskService:
    """呼叫规则、单号校验和正式任务持久化。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        target_copy_batch_size: int = DEFAULT_TARGET_COPY_BATCH_SIZE,
        line_service: SipLineService | None = None,
        credit_metering_client: CreditMeteringClient | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.target_copy_batch_size = max(1, target_copy_batch_size)
        self.line_service = line_service or SipLineService()
        self.credit_metering_client = credit_metering_client

    @staticmethod
    def metadata_out() -> CallRuleMetadataOut:
        return CallRuleMetadataOut(
            max_retry_count=MAX_RETRY_COUNT,
            retryable_results=[
                RetryableResultMeta(value="no_answer", label="无人接听"),
                RetryableResultMeta(value="busy", label="忙线"),
                RetryableResultMeta(value="rejected", label="拒接"),
            ],
        )

    async def ensure_default_rule(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
    ) -> AiCallOutboundRuleModel:
        existing = await db.scalar(
            select(AiCallOutboundRuleModel)
            .where(
                AiCallOutboundRuleModel.tenant_id == tenant_id,
                AiCallOutboundRuleModel.rule_name == "工作日规则",
            )
            .order_by(AiCallOutboundRuleModel.created_at, AiCallOutboundRuleModel.id)
            .limit(1)
        )
        if existing is not None:
            return existing
        return await self.create_rule(
            db,
            tenant_id,
            user_id,
            CallRuleIn(
                rule_name="工作日规则",
                enabled=True,
                call_windows=[
                    {"startTime": "09:00", "endTime": "12:00"},
                    {"startTime": "14:00", "endTime": "18:00"},
                ],
                retry_count=2,
                retry_intervals_minutes=[30, 60],
                retryable_results=["no_answer", "busy"],
            ),
        )

    async def list_rules(
        self,
        db: AsyncSession,
        tenant_id: str,
        *,
        user_id: int,
        page_num: int,
        page_size: int,
        rule_name: str | None,
        enabled: bool | None,
    ) -> tuple[list[CallRuleOut], int]:
        await self.ensure_default_rule(db, tenant_id, user_id)
        conditions = [
            AiCallOutboundRuleModel.tenant_id == tenant_id,
            AiCallOutboundRuleModel.deleted.is_(False),
        ]
        if rule_name and rule_name.strip():
            conditions.append(AiCallOutboundRuleModel.rule_name.contains(rule_name.strip()))
        if enabled is not None:
            conditions.append(AiCallOutboundRuleModel.enabled.is_(enabled))
        total = int(
            await db.scalar(select(func.count(AiCallOutboundRuleModel.id)).where(*conditions)) or 0
        )
        rules = (
            await db.scalars(
                select(AiCallOutboundRuleModel)
                .where(*conditions)
                .order_by(AiCallOutboundRuleModel.updated_at.desc())
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        return [self.rule_out(rule) for rule in rules], total

    async def create_rule(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        request: CallRuleIn,
    ) -> AiCallOutboundRuleModel:
        duplicate = await db.scalar(
            select(AiCallOutboundRuleModel.id).where(
                AiCallOutboundRuleModel.tenant_id == tenant_id,
                AiCallOutboundRuleModel.rule_name == request.rule_name,
            )
        )
        if duplicate is not None:
            raise CustomException(
                msg="同名呼叫规则已存在",
                status_code=status.HTTP_409_CONFLICT,
            )
        now = _now()
        rule = AiCallOutboundRuleModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            rule_name=request.rule_name,
            enabled=request.enabled,
            call_windows_json=_json([
                window.model_dump(mode="json", by_alias=True) for window in request.call_windows
            ]),
            retry_count=request.retry_count,
            retry_intervals_json=_json(request.retry_intervals_minutes),
            retryable_results_json=_json(request.retryable_results),
            deleted=False,
            deleted_at=None,
            created_by=user_id,
            updated_by=user_id,
            created_at=now,
            updated_at=now,
        )
        db.add(rule)
        await db.flush()
        return rule

    async def update_rule(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        rule_id: int,
        request: CallRuleIn,
    ) -> AiCallOutboundRuleModel:
        rule = await self._get_rule(db, tenant_id, rule_id)
        duplicate = await db.scalar(
            select(AiCallOutboundRuleModel.id).where(
                AiCallOutboundRuleModel.tenant_id == tenant_id,
                AiCallOutboundRuleModel.rule_name == request.rule_name,
                AiCallOutboundRuleModel.id != rule_id,
            )
        )
        if duplicate is not None:
            raise CustomException(
                msg="同名呼叫规则已存在",
                status_code=status.HTTP_409_CONFLICT,
            )
        rule.rule_name = request.rule_name
        rule.enabled = request.enabled
        rule.call_windows_json = _json([
            window.model_dump(mode="json", by_alias=True) for window in request.call_windows
        ])
        rule.retry_count = request.retry_count
        rule.retry_intervals_json = _json(request.retry_intervals_minutes)
        rule.retryable_results_json = _json(request.retryable_results)
        rule.updated_by = user_id
        rule.updated_at = _now()
        await db.flush()
        return rule

    async def delete_rule(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        rule_id: int,
    ) -> None:
        rule = await self._get_rule(db, tenant_id, rule_id)
        now = _now()
        rule.deleted = True
        rule.deleted_at = now
        rule.updated_by = user_id
        rule.updated_at = now
        await db.flush()

    async def validate_single(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        request: SingleValidationRequest,
    ) -> AiCallOutboundValidationModel:
        if request.answer_mode == "linphone" and (
            not request.phone_number or not PHONE_PATTERN.fullmatch(request.phone_number)
        ):
            raise CustomException(
                msg="手机号格式错误",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        await self._resolve_references(db, tenant_id, request)
        line = (
            await self.line_service.resolve_default(db, tenant_id)
            if request.answer_mode == "linphone"
            else None
        )
        if line is not None:
            self._ensure_sip_target_allowed(
                self.line_service.to_sip_config(line),
                request.phone_number or "",
            )
        now = _now()
        validation = AiCallOutboundValidationModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            status="PASSED",
            processing_stage="COMPLETED",
            original_filename="single",
            temp_file_path=None,
            file_size=0,
            task_config_json=_json(request.config_dict()),
            line_id=line.id if line is not None else None,
            line_snapshot_json=(
                self.line_service.snapshot_json(line) if line is not None else None
            ),
            valid_target_count=1,
            issue_count=0,
            issue_stats_json="{}",
            error_message=None,
            retryable=False,
            retry_count=0,
            created_by=user_id,
            created_at=now,
            updated_at=now,
            finished_at=now,
        )
        row = AiCallOutboundValidationRowModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            validation_id=validation.id,
            row_number=1,
            phone_number=request.phone_number,
            customer_name=request.customer_name,
            business_params_json=_json(
                {"customerName": request.customer_name} if request.customer_name else {}
            ),
            normalized_phone=request.phone_number,
            is_valid=True,
            reasons_json=None,
            duplicate_row_number=None,
            created_at=now,
        )
        db.add_all([validation, row])
        await db.flush()
        return validation

    async def create_task(
        self,
        db: AsyncSession,
        tenant_id: str,
        user_id: int,
        user_name: str | None,
        idempotency_key: str,
        request: CreateTaskRequest,
    ) -> tuple[AiCallOutboundTaskModel, bool]:
        key = idempotency_key.strip()
        if not key or len(key) > 128:
            raise CustomException(
                msg="Idempotency-Key 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        await begin_sqlite_immediate_write(db)
        validation_id = self._business_id(request.validation_id, "validationId")
        fingerprint = self._fingerprint(request)
        existing = await db.scalar(
            select(AiCallOutboundTaskModel).where(
                AiCallOutboundTaskModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.idempotency_key == key,
            )
        )
        if existing is not None:
            if (
                existing.validation_id != validation_id
                or existing.request_fingerprint != fingerprint
            ):
                raise CustomException(
                    msg="幂等键已用于其他任务请求",
                    status_code=status.HTTP_409_CONFLICT,
                )
            return existing, False

        if request.answer_mode == "linphone" and self.credit_metering_client is not None:
            await require_credit_eligible_for_request(
                self.credit_metering_client,
                tenant_id=tenant_id,
                owner_id=str(user_id),
            )

        validation = await db.scalar(
            select(AiCallOutboundValidationModel).where(
                AiCallOutboundValidationModel.tenant_id == tenant_id,
                AiCallOutboundValidationModel.id == validation_id,
            )
        )
        if validation is None:
            raise CustomException(
                msg="校验结果不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if validation.status != "PASSED":
            raise CustomException(
                msg="只有 PASSED 校验结果可以创建任务",
                status_code=status.HTTP_409_CONFLICT,
            )
        line = None
        line_snapshot = None
        sip_config = None
        if request.answer_mode == "linphone":
            line, line_snapshot = await self._resolve_validation_line(
                db,
                tenant_id,
                validation,
            )
            sip_config = self.line_service.to_sip_config(line_snapshot)
        validation_config = self._load_object(validation.task_config_json)
        request_config = request.config_dict()
        if validation_config != request_config:
            raise CustomException(
                msg="任务参数与校验固化配置不一致",
                status_code=status.HTTP_409_CONFLICT,
            )
        rule, prompt, voice = await self._resolve_references(
            db,
            tenant_id,
            request,
            lock_tenant_voice=True,
        )
        now = _now()
        scheduled_at = (
            self._parse_datetime(request.scheduled_at)
            if request.execution_mode == "scheduled"
            else None
        )
        prompt_version = None
        if prompt.current_version_id is not None:
            prompt_version = await db.scalar(
                select(AiCallPromptProfileVersionModel).where(
                    AiCallPromptProfileVersionModel.id == prompt.current_version_id,
                    AiCallPromptProfileVersionModel.tenant_id == tenant_id,
                    AiCallPromptProfileVersionModel.profile_id == prompt.id,
                    AiCallPromptProfileVersionModel.deleted_at.is_(None),
                )
            )
        if prompt_version is None:
            prompt_version = await db.scalar(
                select(AiCallPromptProfileVersionModel)
                .where(
                    AiCallPromptProfileVersionModel.tenant_id == tenant_id,
                    AiCallPromptProfileVersionModel.profile_id == prompt.id,
                    AiCallPromptProfileVersionModel.deleted_at.is_(None),
                )
                .order_by(AiCallPromptProfileVersionModel.version_no.desc())
                .limit(1)
            )
        knowledge_snapshot = await self._freeze_knowledge(
            db,
            tenant_id=tenant_id,
            prompt_profile_id=prompt.id,
            frozen_at=now,
        )
        snapshot = {
            "request": request_config,
            "prompt": {
                "id": str(prompt.id),
                "sceneCode": prompt.scene_code,
                "name": prompt.name,
                "providerKey": prompt.provider_key,
                "promptText": prompt.prompt_text,
                "openingMessage": prompt.opening_message,
                "productInfo": prompt.product_info,
                "variables": self._load_list(prompt.variables_json),
                "versionId": str(prompt_version.id) if prompt_version is not None else None,
                "versionNo": prompt_version.version_no if prompt_version is not None else 1,
            },
            "voice": {
                "scope": (
                    "TENANT" if isinstance(voice, AiCallTenantVoiceProfileModel) else "BUILTIN"
                ),
                "profileId": str(voice.id),
                "voice": voice.voice,
                "voiceName": voice.display_name,
                "voiceType": voice.voice_type,
                "targetModel": voice.target_model,
            },
            "rule": self.rule_out(rule).model_dump(mode="json", by_alias=True),
            "knowledge": knowledge_snapshot,
        }
        if line_snapshot is not None:
            snapshot["sipLine"] = line_snapshot.model_dump(mode="json", by_alias=True)
        task = AiCallOutboundTaskModel(
            id=generate_snowflake_id(),
            tenant_id=tenant_id,
            validation_id=validation_id,
            idempotency_key=key,
            request_fingerprint=fingerprint,
            task_name=request.task_name,
            task_mode=request.task_mode,
            answer_mode=request.answer_mode,
            status="SCHEDULED",
            total_targets=0,
            completed_targets=0,
            connected_targets=0,
            failed_targets=0,
            execution_mode=request.execution_mode,
            scheduled_at=scheduled_at,
            started_at=None,
            ended_at=None,
            prompt_profile_id=str(prompt.id),
            prompt_name=prompt.name,
            scene_code=prompt.scene_code,
            voice=voice.voice,
            voice_name=voice.display_name,
            voice_type=voice.voice_type,
            voice_target_model=voice.target_model,
            rule_id=rule.id,
            rule_name=rule.rule_name,
            rule_summary=self._rule_summary(rule),
            line_id=line.id if line is not None else None,
            line_name=line.line_name if line is not None else None,
            config_snapshot_json=_json(snapshot),
            error_message=None,
            created_by=user_id,
            created_by_name=user_name,
            created_at=now,
            updated_at=now,
        )
        db.add(task)
        await db.flush()
        task.total_targets = await self._copy_targets(
            db,
            tenant_id=tenant_id,
            validation_id=validation_id,
            task_id=task.id,
            sip_config=sip_config,
        )
        if task.total_targets != validation.valid_target_count:
            raise CustomException(
                msg="有效名单数量发生变化，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            )
        await db.flush()
        return task, True

    @staticmethod
    async def _freeze_knowledge(
        db: AsyncSession,
        *,
        tenant_id: str,
        prompt_profile_id: int,
        frozen_at: datetime,
    ) -> dict[str, object]:
        from app.services.ai_call.knowledge import (
            RETRIEVER_VERSION,
            knowledge_version_snapshot_hash,
            load_current_ready_knowledge_versions,
        )

        versions = await load_current_ready_knowledge_versions(
            db,
            tenant_id=tenant_id,
            prompt_profile_id=prompt_profile_id,
        )
        chunk_count = sum(version.chunk_count for version in versions)
        if chunk_count > MAX_FROZEN_KNOWLEDGE_CHUNKS:
            raise CustomException(
                msg=(
                    f"当前场景关联知识共 {chunk_count} 个切片，超过单任务 "
                    f"{MAX_FROZEN_KNOWLEDGE_CHUNKS} 个上限，请减少资料后重试"
                ),
                status_code=status.HTTP_409_CONFLICT,
            )
        return {
            "promptProfileId": str(prompt_profile_id),
            "versionIds": [str(version.id) for version in versions],
            "versionSnapshotHash": knowledge_version_snapshot_hash(versions),
            "retrieverVersion": RETRIEVER_VERSION,
            "frozenAt": frozen_at.isoformat(),
        }

    async def _resolve_validation_line(
        self,
        db: AsyncSession,
        tenant_id: str,
        validation: AiCallOutboundValidationModel,
    ) -> tuple[AiCallSipLineModel, SipLineSnapshot]:
        if validation.line_id is None or not validation.line_snapshot_json:
            raise CustomException(
                msg="校验结果缺少 SIP 外呼线路，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            )
        try:
            snapshot = SipLineSnapshot.model_validate_json(validation.line_snapshot_json)
        except Exception as exc:
            raise CustomException(
                msg="校验结果中的 SIP 线路快照无效，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            ) from exc
        if snapshot.line_id != str(validation.line_id):
            raise CustomException(
                msg="校验结果中的 SIP 线路快照不一致，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            )
        try:
            line = await self.line_service.get_line(
                db,
                tenant_id,
                validation.line_id,
            )
        except CustomException as exc:
            raise CustomException(
                msg="校验绑定的 SIP 外呼线路已失效，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            ) from exc
        if not line.enabled:
            raise CustomException(
                msg="校验绑定的 SIP 外呼线路已停用，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            )
        return line, snapshot

    async def _copy_targets(
        self,
        db: AsyncSession,
        *,
        tenant_id: str,
        validation_id: int,
        task_id: int,
        sip_config: SipOutboundConfig | None,
    ) -> int:
        last_id = 0
        copied = 0
        while True:
            rows = (
                await db.scalars(
                    select(AiCallOutboundValidationRowModel)
                    .where(
                        AiCallOutboundValidationRowModel.tenant_id == tenant_id,
                        AiCallOutboundValidationRowModel.validation_id == validation_id,
                        AiCallOutboundValidationRowModel.is_valid.is_(True),
                        AiCallOutboundValidationRowModel.id > last_id,
                    )
                    .order_by(AiCallOutboundValidationRowModel.id)
                    .limit(self.target_copy_batch_size)
                )
            ).all()
            if not rows:
                return copied
            if sip_config is not None:
                for row in rows:
                    self._ensure_sip_target_allowed(
                        sip_config,
                        row.normalized_phone or row.phone_number or "",
                    )
            now = _now()
            db.add_all([
                AiCallOutboundTargetModel(
                    id=generate_snowflake_id(),
                    tenant_id=tenant_id,
                    task_id=task_id,
                    validation_id=validation_id,
                    source_validation_row_id=row.id,
                    source_row_number=row.row_number,
                    phone_number=row.normalized_phone or row.phone_number,
                    customer_name=row.customer_name,
                    business_params_json=row.business_params_json,
                    status="PENDING",
                    attempt_count=0,
                    latest_result=None,
                    created_at=now,
                    updated_at=now,
                )
                for row in rows
            ])
            await db.flush()
            copied += len(rows)
            last_id = rows[-1].id

    @staticmethod
    def _ensure_sip_target_allowed(
        config: SipOutboundConfig,
        phone_number: str,
    ) -> None:
        preflight = validate_sip_outbound_preflight(
            config,
            callee_phone_number=phone_number,
        )
        if not preflight.ok:
            raise CustomException(
                msg=preflight.message or "SIP 外呼配置不合法",
                status_code=status.HTTP_409_CONFLICT,
            )

    async def list_tasks(
        self,
        db: AsyncSession,
        tenant_id: str,
        *,
        page_num: int,
        page_size: int,
        task_name: str | None,
        task_status: str | None,
        begin_time: str | None,
        end_time: str | None,
        scene_code: str | None = None,
    ) -> tuple[list[OutboundTaskOut], int]:
        conditions = [AiCallOutboundTaskModel.tenant_id == tenant_id]
        if task_name and task_name.strip():
            conditions.append(AiCallOutboundTaskModel.task_name.contains(task_name.strip()))
        if task_status and task_status.strip():
            conditions.append(AiCallOutboundTaskModel.status == task_status.strip())
        if begin_time:
            conditions.append(
                AiCallOutboundTaskModel.created_at >= self._parse_datetime(begin_time)
            )
        if end_time:
            conditions.append(AiCallOutboundTaskModel.created_at <= self._parse_datetime(end_time))
        if scene_code and scene_code.strip():
            conditions.append(AiCallOutboundTaskModel.scene_code == scene_code.strip())
        total = int(
            await db.scalar(select(func.count(AiCallOutboundTaskModel.id)).where(*conditions)) or 0
        )
        tasks = (
            await db.scalars(
                select(AiCallOutboundTaskModel)
                .where(*conditions)
                .order_by(AiCallOutboundTaskModel.created_at.desc())
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        dialer_types_by_task = await self._attempt_dialer_types_by_task(
            db,
            tenant_id,
            [task.id for task in tasks],
        )
        failed_attempts_by_task = await self._failed_attempt_counts_by_task(
            db,
            tenant_id,
            [task.id for task in tasks],
        )
        return [
            self.task_out(
                task,
                attempt_dialer_types=dialer_types_by_task.get(task.id, []),
                failed_attempts=failed_attempts_by_task.get(task.id, 0),
            )
            for task in tasks
        ], total

    async def get_task(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> OutboundTaskOut:
        task = await self._get_task(db, tenant_id, task_id)
        dialer_types_by_task = await self._attempt_dialer_types_by_task(
            db,
            tenant_id,
            [task.id],
        )
        failed_attempts_by_task = await self._failed_attempt_counts_by_task(
            db,
            tenant_id,
            [task.id],
        )
        return self.task_out(
            task,
            attempt_dialer_types=dialer_types_by_task.get(task.id, []),
            failed_attempts=failed_attempts_by_task.get(task.id, 0),
        )

    async def update_schedule(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
        request: UpdateTaskScheduleRequest,
    ) -> AcceptedCommandOut:
        task = await self._get_task_for_update(db, tenant_id, task_id)
        if task.status != "SCHEDULED":
            raise CustomException(
                msg="仅待执行任务可以修改名称和计划执行时间",
                status_code=status.HTTP_409_CONFLICT,
            )
        task.task_name = request.task_name
        task.execution_mode = "scheduled"
        task.scheduled_at = self._parse_datetime(request.scheduled_at)
        task.next_dispatch_at = None
        task.updated_at = _now()
        await db.flush()
        return AcceptedCommandOut()

    async def run_action(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
        action: str,
    ) -> AcceptedCommandOut:
        task = await self._get_task_for_update(db, tenant_id, task_id)
        allowed = {
            "pause": {"RUNNING"},
            "resume": {"PAUSED"},
            "stop": {"RUNNING", "PAUSED"},
            "cancel": {"SCHEDULED"},
        }
        idempotent_statuses = {
            "pause": {"PAUSING", "PAUSED"},
            "resume": {"RUNNING"},
            "stop": {"STOPPING", "STOPPED"},
            "cancel": {"CANCELLED"},
        }.get(action, set())
        if task.status in idempotent_statuses:
            return AcceptedCommandOut()
        if action not in allowed or task.status not in allowed[action]:
            raise CustomException(
                msg=f"任务当前状态不允许{self._action_name(action)}",
                status_code=status.HTTP_409_CONFLICT,
            )
        now = _now()
        if action == "pause":
            task.status = (
                "PAUSING" if await self._active_target_count(db, tenant_id, task_id) else "PAUSED"
            )
        elif action == "resume":
            task.status = "RUNNING"
            task.next_dispatch_at = None
        else:
            await db.execute(
                update(AiCallOutboundTargetModel)
                .where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.task_id == task_id,
                    AiCallOutboundTargetModel.status.in_(["PENDING", "RETRY_WAIT"]),
                )
                .values(
                    status="CANCELLED",
                    next_attempt_at=None,
                    updated_at=now,
                )
            )
            await db.flush()
            batch_ids = (
                await db.scalars(
                    select(AiCallOutboundTargetModel.exception_batch_id)
                    .where(
                        AiCallOutboundTargetModel.tenant_id == tenant_id,
                        AiCallOutboundTargetModel.task_id == task_id,
                        AiCallOutboundTargetModel.exception_batch_id.is_not(None),
                    )
                    .distinct()
                )
            ).all()
            for batch_id in batch_ids:
                batch = await db.scalar(
                    select(AiCallOutboundExceptionBatchModel)
                    .where(
                        AiCallOutboundExceptionBatchModel.tenant_id == tenant_id,
                        AiCallOutboundExceptionBatchModel.id == batch_id,
                        AiCallOutboundExceptionBatchModel.status == "RUNNING",
                    )
                    .with_for_update()
                )
                if batch is not None:
                    await complete_exception_batch_if_done(db, batch, now)
            if action == "stop":
                has_active_target = bool(await self._active_target_count(db, tenant_id, task_id))
                task.status = "STOPPING" if has_active_target else "STOPPED"
                task.ended_at = None if has_active_target else now
            else:
                task.status = "CANCELLED"
                task.ended_at = now
            await self._sync_task_counts(db, task)
        task.updated_at = now
        await db.flush()
        return AcceptedCommandOut()

    @staticmethod
    async def _active_target_count(
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> int:
        return int(
            await db.scalar(
                select(func.count(AiCallOutboundTargetModel.id)).where(
                    AiCallOutboundTargetModel.tenant_id == tenant_id,
                    AiCallOutboundTargetModel.task_id == task_id,
                    AiCallOutboundTargetModel.status.in_(["DIALING", "IN_CALL"]),
                )
            )
            or 0
        )

    @staticmethod
    async def _sync_task_counts(
        db: AsyncSession,
        task: AiCallOutboundTaskModel,
    ) -> None:
        await refresh_task_counters(db, task, _now())

    async def list_targets(
        self,
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
        *,
        page_num: int,
        page_size: int,
        phone_number: str | None,
        customer_name: str | None,
        target_status: str | None,
    ) -> tuple[list[OutboundTargetOut], int]:
        await self._get_task(db, tenant_id, task_id)
        conditions = [
            AiCallOutboundTargetModel.tenant_id == tenant_id,
            AiCallOutboundTargetModel.task_id == task_id,
        ]
        if phone_number and phone_number.strip():
            conditions.append(AiCallOutboundTargetModel.phone_number.contains(phone_number.strip()))
        if customer_name and customer_name.strip():
            conditions.append(
                AiCallOutboundTargetModel.customer_name.contains(customer_name.strip())
            )
        if target_status and target_status.strip():
            conditions.append(AiCallOutboundTargetModel.status == target_status.strip())
        total = int(
            await db.scalar(select(func.count(AiCallOutboundTargetModel.id)).where(*conditions))
            or 0
        )
        targets = (
            await db.scalars(
                select(AiCallOutboundTargetModel)
                .where(*conditions)
                .order_by(
                    AiCallOutboundTargetModel.source_row_number,
                    AiCallOutboundTargetModel.id,
                )
                .offset((page_num - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        latest_attempts = await self._latest_attempts_by_target(
            db,
            tenant_id,
            [target.id for target in targets],
        )
        active_call_ids = await self._active_call_ids_by_target(
            db,
            tenant_id,
            [target.id for target in targets],
        )
        outputs: list[OutboundTargetOut] = []
        for target in targets:
            dialer_type, status_code, provider_reason, hangup_cause, answer_type = (
                latest_attempts.get(target.id) or (None, None, None, None, None)
            )
            active_call_id, active_call_status = active_call_ids.get(
                target.id,
                (None, None),
            )
            outputs.append(
                self.target_out(
                    target,
                    answer_type=answer_type,
                    latest_dialer_type=dialer_type,
                    provider_status_code=status_code,
                    provider_reason=provider_reason,
                    hangup_cause=hangup_cause,
                    active_call_id=active_call_id,
                    active_call_status=active_call_status,
                )
            )
        return outputs, total

    async def _resolve_references(
        self,
        db: AsyncSession,
        tenant_id: str,
        request: SingleValidationRequest | CreateTaskRequest,
        *,
        lock_tenant_voice: bool = False,
    ) -> tuple[
        AiCallOutboundRuleModel,
        AiCallPromptProfileModel,
        AiCallVoiceProfileModel | AiCallTenantVoiceProfileModel,
    ]:
        rule_id = self._business_id(request.rule_id, "ruleId")
        rule = await db.scalar(
            select(AiCallOutboundRuleModel).where(
                AiCallOutboundRuleModel.tenant_id == tenant_id,
                AiCallOutboundRuleModel.id == rule_id,
                AiCallOutboundRuleModel.deleted.is_(False),
            )
        )
        if rule is None:
            raise CustomException(
                msg="呼叫规则不存在",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if not rule.enabled:
            raise CustomException(
                msg="呼叫规则不可用",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        prompt_conditions = [
            AiCallPromptProfileModel.tenant_id == tenant_id,
            AiCallPromptProfileModel.scene_code == request.scene_code,
        ]
        if request.prompt_profile_id:
            prompt_conditions.append(
                AiCallPromptProfileModel.id
                == self._business_id(request.prompt_profile_id, "promptProfileId")
            )
        prompt = await db.scalar(select(AiCallPromptProfileModel).where(*prompt_conditions))
        if prompt is None:
            raise CustomException(
                msg="提示词不存在或与场景不匹配",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        voice = await self._resolve_voice(
            db,
            tenant_id=tenant_id,
            voice=request.voice,
            lock_tenant_voice=lock_tenant_voice,
        )
        return rule, prompt, voice

    @staticmethod
    async def _resolve_voice(
        db: AsyncSession,
        *,
        tenant_id: str,
        voice: str,
        lock_tenant_voice: bool,
    ) -> AiCallVoiceProfileModel | AiCallTenantVoiceProfileModel:
        builtin_voice = await db.scalar(
            select(AiCallVoiceProfileModel)
            .where(
                AiCallVoiceProfileModel.target_model == settings.QWEN_REALTIME_MODEL,
                AiCallVoiceProfileModel.voice == voice,
            )
            .order_by(AiCallVoiceProfileModel.sort_order, AiCallVoiceProfileModel.id)
            .limit(1)
        )
        if builtin_voice is not None:
            return builtin_voice

        tenant_statement = select(AiCallTenantVoiceProfileModel).where(
            AiCallTenantVoiceProfileModel.tenant_id == tenant_id,
            AiCallTenantVoiceProfileModel.target_model == settings.QWEN_REALTIME_MODEL,
            AiCallTenantVoiceProfileModel.voice == voice,
        )
        if lock_tenant_voice:
            tenant_statement = tenant_statement.with_for_update(read=True)
        tenant_voice = await db.scalar(tenant_statement)
        if tenant_voice is not None:
            if tenant_voice.status != "ENABLED":
                raise CustomException(
                    msg="租户音色当前不可用",
                    status_code=status.HTTP_409_CONFLICT,
                )
            return tenant_voice

        raise CustomException(
            msg="音色不存在",
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    @staticmethod
    async def _get_rule(
        db: AsyncSession,
        tenant_id: str,
        rule_id: int,
    ) -> AiCallOutboundRuleModel:
        rule = await db.scalar(
            select(AiCallOutboundRuleModel).where(
                AiCallOutboundRuleModel.tenant_id == tenant_id,
                AiCallOutboundRuleModel.id == rule_id,
                AiCallOutboundRuleModel.deleted.is_(False),
            )
        )
        if rule is None:
            raise CustomException(
                msg="呼叫规则不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return rule

    @staticmethod
    async def _get_task(
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> AiCallOutboundTaskModel:
        task = await db.scalar(
            select(AiCallOutboundTaskModel).where(
                AiCallOutboundTaskModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.id == task_id,
            )
        )
        if task is None:
            raise CustomException(
                msg="外呼任务不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return task

    @staticmethod
    async def _get_task_for_update(
        db: AsyncSession,
        tenant_id: str,
        task_id: int,
    ) -> AiCallOutboundTaskModel:
        task = await db.scalar(
            select(AiCallOutboundTaskModel)
            .where(
                AiCallOutboundTaskModel.tenant_id == tenant_id,
                AiCallOutboundTaskModel.id == task_id,
            )
            .with_for_update()
        )
        if task is None:
            raise CustomException(
                msg="外呼任务不存在",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return task

    @staticmethod
    def rule_out(rule: AiCallOutboundRuleModel) -> CallRuleOut:
        return CallRuleOut(
            rule_id=str(rule.id),
            rule_name=rule.rule_name,
            enabled=rule.enabled,
            call_windows=OutboundRuleTaskService._load_list(rule.call_windows_json),
            retry_count=rule.retry_count,
            retry_intervals_minutes=OutboundRuleTaskService._load_list(rule.retry_intervals_json),
            retryable_results=OutboundRuleTaskService._load_list(rule.retryable_results_json),
            updated_at=OutboundRuleTaskService._format_datetime(rule.updated_at) or "",
        )

    @staticmethod
    def task_out(
        task: AiCallOutboundTaskModel,
        *,
        attempt_dialer_types: list[str] | None = None,
        failed_attempts: int = 0,
    ) -> OutboundTaskOut:
        return OutboundTaskOut(
            task_id=str(task.id),
            task_name=task.task_name,
            task_mode=task.task_mode,
            answer_mode=task.answer_mode,
            status=task.status,
            total_targets=task.total_targets,
            completed_targets=task.completed_targets,
            connected_targets=task.connected_targets,
            failed_targets=task.failed_targets,
            failed_attempts=failed_attempts,
            attempt_dialer_types=attempt_dialer_types or [],
            execution_mode=task.execution_mode,
            scheduled_at=OutboundRuleTaskService._format_datetime(task.scheduled_at),
            next_dispatch_at=OutboundRuleTaskService._format_business_datetime(
                task.next_dispatch_at
            ),
            started_at=OutboundRuleTaskService._format_datetime(task.started_at),
            ended_at=OutboundRuleTaskService._format_datetime(task.ended_at),
            prompt_profile_id=task.prompt_profile_id,
            prompt_name=task.prompt_name,
            scene_code=task.scene_code,
            voice=task.voice,
            voice_name=task.voice_name,
            voice_type=task.voice_type,
            voice_target_model=task.voice_target_model,
            rule_id=str(task.rule_id),
            rule_name=task.rule_name,
            rule_summary=task.rule_summary,
            line_id=str(task.line_id) if task.line_id is not None else None,
            line_name=task.line_name,
            line_snapshot=OutboundRuleTaskService._task_line_snapshot_out(task),
            created_by_name=task.created_by_name,
            created_at=OutboundRuleTaskService._format_datetime(task.created_at) or "",
            updated_at=OutboundRuleTaskService._format_datetime(task.updated_at) or "",
            error_message=task.error_message,
        )

    @staticmethod
    def _task_line_snapshot_out(
        task: AiCallOutboundTaskModel,
    ) -> OutboundTaskLineSnapshotOut | None:
        try:
            config_snapshot = json.loads(task.config_snapshot_json)
            line_snapshot = SipLineSnapshot.model_validate(config_snapshot.get("sipLine"))
        except (AttributeError, TypeError, ValueError):
            return None
        if task.line_id is not None and line_snapshot.line_id != str(task.line_id):
            return None
        return OutboundTaskLineSnapshotOut(
            line_id=line_snapshot.line_id,
            line_code=line_snapshot.line_code,
            line_name=line_snapshot.line_name,
        )

    @staticmethod
    def target_out(
        target: AiCallOutboundTargetModel,
        *,
        answer_type: str | None = None,
        latest_dialer_type: str | None = None,
        provider_status_code: str | None = None,
        provider_reason: str | None = None,
        hangup_cause: str | None = None,
        active_call_id: str | None = None,
        active_call_status: str | None = None,
    ) -> OutboundTargetOut:
        return OutboundTargetOut(
            target_id=str(target.id),
            task_id=str(target.task_id),
            customer_name=target.customer_name,
            phone_number=_mask_phone_number(target.phone_number),
            status=target.status,
            attempt_count=target.attempt_count,
            latest_result=target.latest_result,
            answer_type=answer_type,
            latest_dialer_type=latest_dialer_type,
            provider_status_code=provider_status_code,
            provider_reason=provider_reason,
            hangup_cause=hangup_cause,
            active_call_id=active_call_id,
            active_call_status=active_call_status,
            updated_at=OutboundRuleTaskService._format_datetime(target.updated_at) or "",
        )

    @staticmethod
    async def _attempt_dialer_types_by_task(
        db: AsyncSession,
        tenant_id: str,
        task_ids: list[int],
    ) -> dict[int, list[str]]:
        if not task_ids:
            return {}
        rows = (
            await db.execute(
                select(
                    AiCallOutboundAttemptModel.task_id,
                    AiCallOutboundAttemptModel.dialer_type,
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.task_id.in_(task_ids),
                    AiCallOutboundAttemptModel.dialer_type.is_not(None),
                )
                .distinct()
            )
        ).all()
        result: dict[int, set[str]] = {}
        for task_id, dialer_type in rows:
            if dialer_type:
                result.setdefault(task_id, set()).add(dialer_type)
        return {task_id: sorted(dialer_types) for task_id, dialer_types in result.items()}

    @staticmethod
    async def _failed_attempt_counts_by_task(
        db: AsyncSession,
        tenant_id: str,
        task_ids: list[int],
    ) -> dict[int, int]:
        if not task_ids:
            return {}
        rows = (
            await db.execute(
                select(
                    AiCallOutboundAttemptModel.task_id,
                    func.count(AiCallOutboundAttemptModel.id),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.task_id.in_(task_ids),
                    AiCallOutboundAttemptModel.status == "FAILED",
                )
                .group_by(AiCallOutboundAttemptModel.task_id)
            )
        ).all()
        return {task_id: int(count) for task_id, count in rows}

    @staticmethod
    async def _latest_attempts_by_target(
        db: AsyncSession,
        tenant_id: str,
        target_ids: list[int],
    ) -> dict[
        int,
        tuple[str | None, str | None, str | None, str | None, str | None],
    ]:
        if not target_ids:
            return {}
        rows = (
            await db.execute(
                select(
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.dialer_type,
                    AiCallOutboundAttemptModel.provider_status_code,
                    AiCallOutboundAttemptModel.provider_reason,
                    AiCallOutboundAttemptModel.hangup_cause,
                    AiCallOutboundAttemptModel.call_result,
                    AiCallSemanticAnalysisModel.analysis_status,
                    AiCallSemanticAnalysisModel.analysis_result,
                )
                .outerjoin(
                    AiCallSemanticAnalysisModel,
                    and_(
                        AiCallSemanticAnalysisModel.call_id == AiCallOutboundAttemptModel.call_id,
                        AiCallSemanticAnalysisModel.analysis_scene_code
                        == "ai_call_semantic_analysis",
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.target_id.in_(target_ids),
                )
                .order_by(
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.attempt_no.desc(),
                )
            )
        ).all()
        result: dict[
            int,
            tuple[str | None, str | None, str | None, str | None, str | None],
        ] = {}
        for (
            target_id,
            dialer_type,
            status_code,
            provider_reason,
            hangup_cause,
            call_result,
            analysis_status,
            analysis_result_raw,
        ) in rows:
            try:
                analysis_result = json.loads(analysis_result_raw or "{}")
            except (TypeError, ValueError):
                analysis_result = {}
            result.setdefault(
                target_id,
                (
                    dialer_type,
                    status_code,
                    provider_reason,
                    hangup_cause,
                    detect_answer_type(
                        call_result=call_result,
                        analysis_status=analysis_status,
                        analysis_result=analysis_result,
                    ),
                ),
            )
        return result

    @staticmethod
    async def _active_call_ids_by_target(
        db: AsyncSession,
        tenant_id: str,
        target_ids: list[int],
    ) -> dict[int, tuple[str, str | None]]:
        if not target_ids:
            return {}
        rows = (
            await db.execute(
                select(
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.call_id,
                    AiCallRecordModel.status,
                )
                .outerjoin(
                    AiCallRecordModel,
                    and_(
                        AiCallRecordModel.tenant_id == AiCallOutboundAttemptModel.tenant_id,
                        AiCallRecordModel.call_id == AiCallOutboundAttemptModel.call_id,
                    ),
                )
                .where(
                    AiCallOutboundAttemptModel.tenant_id == tenant_id,
                    AiCallOutboundAttemptModel.target_id.in_(target_ids),
                    AiCallOutboundAttemptModel.status.in_({
                        "QUEUED",
                        "STARTING",
                        "DIALING",
                        "IN_CALL",
                    }),
                )
                .order_by(
                    AiCallOutboundAttemptModel.target_id,
                    AiCallOutboundAttemptModel.attempt_no.desc(),
                )
            )
        ).all()
        result: dict[int, tuple[str, str | None]] = {}
        for target_id, call_id, call_status in rows:
            result.setdefault(target_id, (call_id, call_status))
        return result

    @staticmethod
    def _rule_summary(rule: AiCallOutboundRuleModel) -> str:
        windows = OutboundRuleTaskService._load_list(rule.call_windows_json)
        window_text = "、".join(
            f"{item.get('startTime', '')}–{item.get('endTime', '')}"
            for item in windows
            if isinstance(item, dict)
        )
        return f"{window_text}，最多重试 {rule.retry_count} 次"

    @staticmethod
    def _fingerprint(request: CreateTaskRequest) -> str:
        payload = {
            "validationId": request.validation_id,
            "config": request.config_dict(),
        }
        return hashlib.sha256(_json(payload).encode()).hexdigest()

    @staticmethod
    def _business_id(value: str, field: str) -> int:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise CustomException(
                msg=f"{field} 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            ) from exc
        if result <= 0:
            raise CustomException(
                msg=f"{field} 不合法",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return result

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime:
        if not value:
            raise CustomException(
                msg="计划执行时间不能为空",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        try:
            return (
                datetime
                .strptime(value, "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=ZoneInfo(settings.AI_CALL_OUTBOUND_TIMEZONE))
                .astimezone(timezone.utc)
            )
        except ValueError as exc:
            raise CustomException(
                msg="时间格式必须为 YYYY-MM-DD HH:mm:ss",
                status_code=status.HTTP_400_BAD_REQUEST,
            ) from exc

    @staticmethod
    def _format_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        aware_value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return aware_value.astimezone(ZoneInfo(settings.AI_CALL_OUTBOUND_TIMEZONE)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @staticmethod
    def _format_business_datetime(value: datetime | None) -> str | None:
        return OutboundRuleTaskService._format_datetime(value)

    @staticmethod
    def _load_list(value: str) -> list:
        try:
            result = json.loads(value)
        except json.JSONDecodeError:
            return []
        return result if isinstance(result, list) else []

    @staticmethod
    def _load_object(value: str) -> dict:
        try:
            result = json.loads(value)
        except json.JSONDecodeError as exc:
            raise CustomException(
                msg="校验固化配置损坏，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            ) from exc
        if not isinstance(result, dict):
            raise CustomException(
                msg="校验固化配置损坏，请重新校验",
                status_code=status.HTTP_409_CONFLICT,
            )
        return result

    @staticmethod
    def _action_name(action: str) -> str:
        return {
            "pause": "暂停",
            "resume": "恢复",
            "stop": "停止",
            "cancel": "取消",
        }.get(action, "执行该操作")
