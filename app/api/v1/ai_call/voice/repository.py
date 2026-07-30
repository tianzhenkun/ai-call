from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import false, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.ai_call.model import AiCallVoiceProfileModel
from app.api.v1.ai_call.outbound.rule_task_model import AiCallOutboundTaskModel
from app.api.v1.ai_call.voice.model import AiCallTenantVoiceProfileModel
from app.api.v1.ai_call.voice.schema import VoiceProfileOut, VoiceStatus

BLOCKING_TASK_STATUSES = ("SCHEDULED", "RUNNING", "PAUSING", "PAUSED", "STOPPING")
HISTORICAL_TASK_STATUSES = ("STOPPED", "COMPLETED", "FAILED", "CANCELLED")
MAX_BLOCKING_TASK_IDS = 100


@dataclass(frozen=True, slots=True)
class VoiceTaskReferenceSummary:
    blocking_task_count: int
    historical_task_count: int
    blocking_task_ids: tuple[int, ...]


class VoiceRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list_profiles(
        self,
        *,
        tenant_id: str,
        target_model: str,
        voice_type: str | None = None,
        gender: str | None = None,
        status: VoiceStatus | None = None,
        available_only: bool = False,
        include_deleted: bool = False,
        page_num: int = 1,
        page_size: int = 20,
    ) -> tuple[list[VoiceProfileOut], int]:
        global_statement = select(AiCallVoiceProfileModel).where(
            AiCallVoiceProfileModel.target_model == target_model
        )
        if voice_type is not None:
            global_statement = global_statement.where(
                AiCallVoiceProfileModel.voice_type == voice_type
            )
        if gender is not None:
            global_statement = global_statement.where(AiCallVoiceProfileModel.gender == gender)
        if available_only:
            global_statement = global_statement.where(
                AiCallVoiceProfileModel.voice_type == "内置",
                AiCallVoiceProfileModel.voice != "",
            )
        if status is not None and status != "ENABLED":
            global_statement = global_statement.where(false())

        tenant_statement = select(AiCallTenantVoiceProfileModel).where(
            AiCallTenantVoiceProfileModel.tenant_id == tenant_id,
            AiCallTenantVoiceProfileModel.target_model == target_model,
        )
        if voice_type is not None:
            tenant_statement = tenant_statement.where(
                AiCallTenantVoiceProfileModel.voice_type == voice_type
            )
        if gender is not None:
            tenant_statement = tenant_statement.where(
                AiCallTenantVoiceProfileModel.gender == gender
            )
        if status is not None:
            tenant_statement = tenant_statement.where(
                AiCallTenantVoiceProfileModel.status == status
            )
        if not include_deleted:
            tenant_statement = tenant_statement.where(
                AiCallTenantVoiceProfileModel.status != "DELETED"
            )
        if available_only:
            tenant_statement = tenant_statement.where(
                AiCallTenantVoiceProfileModel.status == "ENABLED",
                AiCallTenantVoiceProfileModel.voice.is_not(None),
                AiCallTenantVoiceProfileModel.voice != "",
            )

        safe_page_num = max(1, page_num)
        safe_page_size = max(1, min(page_size, 1000))
        combined_offset = (safe_page_num - 1) * safe_page_size

        tenant_total = int(
            (
                await self.db.execute(select(func.count()).select_from(tenant_statement.subquery()))
            ).scalar_one()
        )
        global_total = int(
            (
                await self.db.execute(select(func.count()).select_from(global_statement.subquery()))
            ).scalar_one()
        )

        rows: list[VoiceProfileOut] = []
        remaining = safe_page_size
        global_offset = max(0, combined_offset - tenant_total)

        if combined_offset < tenant_total:
            tenant_limit = min(remaining, tenant_total - combined_offset)
            tenant_profiles = (
                (
                    await self.db.execute(
                        tenant_statement
                        .order_by(
                            AiCallTenantVoiceProfileModel.updated_at.desc(),
                            AiCallTenantVoiceProfileModel.created_at.desc(),
                            AiCallTenantVoiceProfileModel.id.desc(),
                        )
                        .limit(tenant_limit)
                        .offset(combined_offset)
                    )
                )
                .scalars()
                .all()
            )
            rows.extend(self._tenant_profile(profile) for profile in tenant_profiles)
            remaining -= len(tenant_profiles)
            global_offset = 0

        if remaining > 0 and global_offset < global_total:
            global_profiles = (
                (
                    await self.db.execute(
                        global_statement
                        .order_by(
                            AiCallVoiceProfileModel.updated_at.desc(),
                            AiCallVoiceProfileModel.created_at.desc(),
                            AiCallVoiceProfileModel.id.desc(),
                        )
                        .limit(remaining)
                        .offset(global_offset)
                    )
                )
                .scalars()
                .all()
            )
            rows.extend(self._global_profile(profile) for profile in global_profiles)

        return rows, tenant_total + global_total

    async def get_tenant_profile(
        self,
        *,
        tenant_id: str,
        profile_id: int,
        for_update: bool = False,
    ) -> AiCallTenantVoiceProfileModel | None:
        statement = select(AiCallTenantVoiceProfileModel).where(
            AiCallTenantVoiceProfileModel.tenant_id == tenant_id,
            AiCallTenantVoiceProfileModel.id == profile_id,
        )
        if for_update:
            statement = statement.with_for_update()
        return await self.db.scalar(statement)

    async def task_reference_summary(
        self,
        *,
        tenant_id: str,
        voice: str,
        blocking_id_limit: int = MAX_BLOCKING_TASK_IDS,
    ) -> VoiceTaskReferenceSummary:
        base_filters = (
            AiCallOutboundTaskModel.tenant_id == tenant_id,
            AiCallOutboundTaskModel.voice == voice,
        )
        blocking_count = int(
            await self.db.scalar(
                select(func.count(AiCallOutboundTaskModel.id)).where(
                    *base_filters,
                    AiCallOutboundTaskModel.status.in_(BLOCKING_TASK_STATUSES),
                )
            )
            or 0
        )
        historical_count = int(
            await self.db.scalar(
                select(func.count(AiCallOutboundTaskModel.id)).where(
                    *base_filters,
                    AiCallOutboundTaskModel.status.in_(HISTORICAL_TASK_STATUSES),
                )
            )
            or 0
        )
        safe_limit = max(1, min(blocking_id_limit, MAX_BLOCKING_TASK_IDS))
        blocking_ids = tuple(
            (
                await self.db.scalars(
                    select(AiCallOutboundTaskModel.id)
                    .where(
                        *base_filters,
                        AiCallOutboundTaskModel.status.in_(BLOCKING_TASK_STATUSES),
                    )
                    .order_by(AiCallOutboundTaskModel.id)
                    .limit(safe_limit)
                )
            ).all()
        )
        return VoiceTaskReferenceSummary(
            blocking_task_count=blocking_count,
            historical_task_count=historical_count,
            blocking_task_ids=blocking_ids,
        )

    @staticmethod
    def _global_profile(profile: AiCallVoiceProfileModel) -> VoiceProfileOut:
        return VoiceProfileOut(
            id=profile.id,
            scope="GLOBAL",
            voice=profile.voice,
            display_name=profile.display_name,
            voice_type=profile.voice_type,
            gender=profile.gender,
            language=None,
            target_model=profile.target_model,
            status="ENABLED",
            error_message=None,
            can_preview=bool(profile.voice),
            can_delete=False,
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )

    @staticmethod
    def _tenant_profile(
        profile: AiCallTenantVoiceProfileModel,
    ) -> VoiceProfileOut:
        return VoiceProfileOut(
            id=profile.id,
            scope="TENANT",
            voice=profile.voice,
            display_name=profile.display_name,
            voice_type=profile.voice_type,
            gender=profile.gender,
            language=profile.language,
            target_model=profile.target_model,
            status=profile.status,
            error_message=profile.error_message,
            can_preview=profile.status == "ENABLED" and bool(profile.voice),
            can_delete=profile.status in {"ENABLED", "DELETE_FAILED"},
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )
