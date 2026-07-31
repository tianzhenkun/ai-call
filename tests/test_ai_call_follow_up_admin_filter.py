from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.ai_call.agent_console_controller import (
    list_admin_follow_ups_controller,
)
from app.api.v1.ai_call.model import AiCallFollowUpTaskModel, AiCallRecordModel
from app.api.v1.system.auth.schema import AuthSchema
from app.api.v1.system.user.model import UserModel
from app.core.base_model import MappedBase
from app.services.ai_call.agent_console_reconciler import AiCallAgentConsoleReconciler


def _auth(db, tenant_id: str = "tenant-a") -> AuthSchema:
    return AuthSchema(
        db=db,
        user=UserModel(
            user_id=1,
            tenant_id=tenant_id,
            user_name="admin",
            nick_name="管理员",
            user_type="sys_user",
        ),
        check_data_scope=False,
    )


def _follow_up(
    *,
    row_id: int,
    tenant_id: str,
    status: str,
    created_at: datetime,
) -> AiCallFollowUpTaskModel:
    return AiCallFollowUpTaskModel(
        id=row_id,
        tenant_id=tenant_id,
        source_type="ai_suggested",
        source_key=f"call:call-{row_id}",
        source_call_id=f"call-{row_id}",
        scene_code="product_intro",
        contact_ref=f"contact-{row_id}",
        masked_contact="138****0000",
        status=status,
        follow_up_reason="客户要求后续联系",
        created_at=created_at,
        updated_at=created_at,
    )


def _record(
    *,
    row_id: int,
    started_at: datetime,
) -> AiCallRecordModel:
    return AiCallRecordModel(
        id=10_000 + row_id,
        call_id=f"call-{row_id}",
        follow_up_id=row_id,
        entry_type="sip_outbound",
        room_name=f"room-{row_id}",
        participant_identity=f"sip-{row_id}",
        status="completed",
        started_at=started_at,
    )


@pytest.mark.anyio
async def test_admin_follow_ups_filter_by_status_source_period_and_page() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(MappedBase.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    begin = datetime(2026, 7, 25, tzinfo=timezone.utc)
    end = datetime(2026, 8, 1, tzinfo=timezone.utc)

    async with session_maker() as session:
        for row_id in range(1, 8):
            session.add_all([
                _follow_up(
                    row_id=row_id,
                    tenant_id="tenant-a",
                    status="pending",
                    created_at=begin + timedelta(hours=row_id),
                ),
                _record(
                    row_id=row_id,
                    started_at=begin + timedelta(hours=row_id),
                ),
            ])
        for row_id in range(8, 10):
            session.add_all([
                _follow_up(
                    row_id=row_id,
                    tenant_id="tenant-a",
                    status="pending",
                    created_at=begin - timedelta(days=row_id),
                ),
                _record(
                    row_id=row_id,
                    started_at=begin - timedelta(days=row_id),
                ),
            ])
        for row_id in range(10, 12):
            session.add_all([
                _follow_up(
                    row_id=row_id,
                    tenant_id="tenant-a",
                    status="processing",
                    created_at=begin + timedelta(hours=row_id),
                ),
                _record(
                    row_id=row_id,
                    started_at=begin + timedelta(hours=row_id),
                ),
            ])
        session.add_all([
            _follow_up(
                row_id=12,
                tenant_id="tenant-a",
                status="completed",
                created_at=begin,
            ),
            _record(row_id=12, started_at=begin),
            _follow_up(
                row_id=13,
                tenant_id="tenant-b",
                status="pending",
                created_at=begin,
            ),
            _record(row_id=13, started_at=begin),
        ])
        await session.commit()

        service = AiCallAgentConsoleReconciler(
            session,
            room_exists=lambda _room: False,
        )
        first_page = await service.list_follow_ups(
            _auth(session),
            status="pending",
            source_started_at_begin=begin,
            source_started_at_end=end,
            page_num=1,
            page_size=5,
        )
        second_page = await service.list_follow_ups(
            _auth(session),
            status="pending",
            source_started_at_begin=begin,
            source_started_at_end=end,
            page_num=2,
            page_size=5,
        )

    await engine.dispose()

    assert first_page["total"] == 7
    assert len(first_page["rows"]) == 5
    assert len(second_page["rows"]) == 2
    assert {row["id"] for row in first_page["rows"] + second_page["rows"]} == {
        str(row_id) for row_id in range(1, 8)
    }
    assert first_page["metrics"]["pending"] == 9
    assert first_page["metrics"]["completed"] == 1


@pytest.mark.anyio
async def test_admin_follow_up_controller_forwards_deep_link_filters() -> None:
    begin = datetime(2026, 7, 25, tzinfo=timezone.utc)
    end = datetime(2026, 8, 1, tzinfo=timezone.utc)
    auth = SimpleNamespace()
    service = SimpleNamespace(
        list_follow_ups=AsyncMock(return_value={"rows": [], "total": 0, "metrics": {}})
    )

    await list_admin_follow_ups_controller(
        auth,
        service,
        status="pending",
        source_started_at_begin=begin,
        source_started_at_end=end,
        page_num=2,
        page_size=20,
    )

    service.list_follow_ups.assert_awaited_once_with(
        auth,
        status="pending",
        source_started_at_begin=begin,
        source_started_at_end=end,
        page_num=2,
        page_size=20,
    )
