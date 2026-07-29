from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.outbound.model import AiCallOutboundValidationModel
from app.api.v1.ai_call.outbound.rule_task_model import (
    AiCallOutboundAttemptModel,
    AiCallOutboundTaskModel,
)
from app.api.v1.ai_call.outbound.sip_line_model import AiCallSipLineModel
from app.api.v1.ai_call.outbound.sip_line_schema import SipLineIn
from app.api.v1.ai_call.outbound.sip_line_service import SipLineService
from app.config.setting import Settings
from app.core.base_model import MappedBase
from app.core.exceptions import CustomException


@pytest.fixture
async def database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'sip-line.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _line_request(
    line_code: str,
    *,
    route_mode: str = "inline_hostname",
) -> SipLineIn:
    payload = {
        "lineCode": line_code,
        "lineName": f"线路 {line_code}",
        "enabled": True,
        "adapterType": "livekit_sip",
        "routeMode": route_mode,
        "authMode": "ip_allowlist",
        "callerNumber": "10000",
        "destinationCountry": "CN",
        "maxConcurrency": 1,
        "originateTimeoutSeconds": 45,
    }
    if route_mode == "inline_hostname":
        payload.update({
            "proxyHost": "127.0.0.1",
            "proxyPort": 5089,
        })
    else:
        payload.update({
            "trunkId": "ST_test",
            "authMode": "managed_trunk",
        })
    return SipLineIn.model_validate(payload)


class PassingPreflightChecker:
    def __init__(self) -> None:
        self.configs = []

    async def check(self, config) -> None:
        self.configs.append(config)


class FailingPreflightChecker:
    async def check(self, config) -> None:
        del config
        raise RuntimeError("LiveKit unavailable")


def test_sip_line_model_is_tenant_scoped_without_secrets_or_foreign_keys() -> None:
    columns = set(AiCallSipLineModel.__table__.columns.keys())
    assert {
        "tenant_id",
        "line_code",
        "line_name",
        "enabled",
        "default_marker",
        "adapter_type",
        "route_mode",
        "trunk_id",
        "proxy_host",
        "proxy_port",
        "auth_mode",
        "caller_number",
        "destination_country",
        "max_concurrency",
        "originate_timeout_seconds",
        "health_status",
        "health_message",
        "last_checked_at",
        "deleted",
        "deleted_at",
        "created_by",
        "updated_by",
        "created_at",
        "updated_at",
    } <= columns
    assert "password" not in columns
    assert "secret" not in columns
    assert not AiCallSipLineModel.__table__.foreign_keys

    unique_names = {
        constraint.name
        for constraint in AiCallSipLineModel.__table__.constraints
        if constraint.name
    }
    assert "uk_ai_call_sip_line_tenant_code" in unique_names
    assert "uk_ai_call_sip_line_tenant_default" in unique_names


def test_outbound_models_store_line_and_provider_diagnostics() -> None:
    assert {"line_id", "line_snapshot_json"} <= set(
        AiCallOutboundValidationModel.__table__.columns.keys()
    )
    assert {"line_id", "line_name"} <= set(
        AiCallOutboundTaskModel.__table__.columns.keys()
    )
    assert {
        "line_id",
        "line_code",
        "provider_status_code",
        "provider_reason",
        "hangup_cause",
    } <= set(AiCallOutboundAttemptModel.__table__.columns.keys())

    assert not AiCallOutboundValidationModel.__table__.foreign_keys
    assert not AiCallOutboundTaskModel.__table__.foreign_keys
    assert not AiCallOutboundAttemptModel.__table__.foreign_keys


def test_sip_line_migration_adds_line_and_attempt_diagnostics() -> None:
    migration_path = (
        Path(__file__).parents[1]
        / "docs"
        / "livekit-ai-outbound"
        / "sql"
        / "phase-h5-outbound-sip-line-postgres.sql"
    )
    migration = migration_path.read_text(encoding="utf-8").lower()
    assert "create table if not exists ai_call_sip_line" in migration
    assert "add column if not exists line_id" in migration
    assert "add column if not exists provider_status_code" in migration
    assert "uk_ai_call_sip_line_tenant_default" in migration
    assert "jsonb" not in migration


def test_sip_line_schema_validates_route_and_exposes_no_credentials() -> None:
    inline = _line_request("inline-a")
    assert inline.trunk_id is None
    assert inline.proxy_host == "127.0.0.1"

    managed = _line_request("managed-a", route_mode="managed_trunk_id")
    assert managed.trunk_id == "ST_test"
    assert managed.proxy_host is None

    invalid_payload = inline.model_dump(mode="json", by_alias=True)
    invalid_payload["trunkId"] = "ST_conflict"
    with pytest.raises(ValidationError, match="内联线路"):
        SipLineIn.model_validate(invalid_payload)

    assert "password" not in SipLineIn.model_fields
    assert "secret" not in SipLineIn.model_fields


@pytest.mark.anyio
async def test_default_line_switch_is_tenant_scoped_and_atomic(database) -> None:
    service = SipLineService(settings=Settings())
    async with database() as db:
        first = await service.create_line(db, "tenant-a", 1, _line_request("line-a"))
        second = await service.create_line(db, "tenant-a", 1, _line_request("line-b"))
        foreign = await service.create_line(db, "tenant-b", 2, _line_request("line-c"))
        await service.set_default(db, "tenant-a", 1, first.id)
        await service.set_default(db, "tenant-a", 1, second.id)
        await service.set_default(db, "tenant-b", 2, foreign.id)
        await db.commit()

    async with database() as db:
        tenant_a_defaults = (
            await db.scalars(
                select(AiCallSipLineModel).where(
                    AiCallSipLineModel.tenant_id == "tenant-a",
                    AiCallSipLineModel.default_marker == "OUTBOUND",
                )
            )
        ).all()
        tenant_b_default = await service.resolve_default(
            db,
            "tenant-b",
            require_available=False,
        )
        with pytest.raises(CustomException) as exc_info:
            await service.get_line(db, "tenant-b", second.id)

    assert [row.id for row in tenant_a_defaults] == [second.id]
    assert tenant_b_default.id == foreign.id
    assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_default_line_cannot_be_disabled_or_deleted(database) -> None:
    service = SipLineService(settings=Settings())
    async with database() as db:
        line = await service.create_line(db, "tenant-a", 1, _line_request("line-a"))
        await service.set_default(db, "tenant-a", 1, line.id)
        with pytest.raises(CustomException, match="先指定其他默认线路") as disable_error:
            await service.disable(db, "tenant-a", 1, line.id)
        with pytest.raises(CustomException, match="先指定其他默认线路") as delete_error:
            await service.delete(db, "tenant-a", 1, line.id)

    assert disable_error.value.status_code == 409
    assert delete_error.value.status_code == 409


@pytest.mark.anyio
async def test_preflight_updates_health_without_creating_sip_participant(database) -> None:
    checker = PassingPreflightChecker()
    settings = Settings(
        AI_CALL_SIP_OUTBOUND_ENABLED=True,
        SIP_PUBLIC_IP="127.0.0.1",
        SIP_RTP_RANGE="16384-16484",
    )
    service = SipLineService(settings=settings, preflight_checker=checker)
    async with database() as db:
        line = await service.create_line(db, "tenant-a", 1, _line_request("line-a"))
        result = await service.preflight(db, "tenant-a", 1, line.id)
        await db.commit()

    assert result.health_status == "AVAILABLE"
    assert result.last_checked_at is not None
    assert len(checker.configs) == 1
    assert checker.configs[0].trunk_hostname == "127.0.0.1:5089"


@pytest.mark.anyio
async def test_preflight_distinguishes_bad_config_from_unreachable_livekit(database) -> None:
    misconfigured_service = SipLineService(
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=False,
            SIP_PUBLIC_IP="",
        ),
        preflight_checker=PassingPreflightChecker(),
    )
    async with database() as db:
        line = await misconfigured_service.create_line(
            db,
            "tenant-a",
            1,
            _line_request("line-a"),
        )
        misconfigured = await misconfigured_service.preflight(
            db,
            "tenant-a",
            1,
            line.id,
        )
        await db.commit()
    assert misconfigured.health_status == "MISCONFIGURED"

    unavailable_service = SipLineService(
        settings=Settings(
            AI_CALL_SIP_OUTBOUND_ENABLED=True,
            SIP_PUBLIC_IP="127.0.0.1",
            SIP_RTP_RANGE="16384-16484",
        ),
        preflight_checker=FailingPreflightChecker(),
    )
    async with database() as db:
        line = await unavailable_service.create_line(
            db,
            "tenant-b",
            2,
            _line_request("line-b"),
        )
        unavailable = await unavailable_service.preflight(
            db,
            "tenant-b",
            2,
            line.id,
        )
        await db.commit()
    assert unavailable.health_status == "UNAVAILABLE"
    assert unavailable.health_message == "LiveKit unavailable"
