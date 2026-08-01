from __future__ import annotations

import asyncio
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.core.logger import log

CONTROL_WAKEUP_CHANNEL = "ai_call_runtime_control_wakeup"


class WakeupListener(Protocol):
    async def start(self) -> bool: ...

    async def wait(
        self,
        *,
        timeout_seconds: float,
        stop_event: asyncio.Event,
    ) -> bool: ...

    async def stop(self) -> None: ...


async def publish_control_wakeup(session: AsyncSession) -> None:
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    await session.execute(
        text("select pg_notify(:channel, '')"),
        {"channel": CONTROL_WAKEUP_CHANNEL},
    )


class PostgresWakeupListener:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._connection: Any | None = None
        self._driver_connection: Any | None = None
        self._wakeup_event = asyncio.Event()
        self._connection_lost = True
        self._stopped = False
        self._ever_connected = False
        self._failure_logged = False
        self.notification_count = 0
        self.timeout_count = 0
        self.reconnect_count = 0

    async def start(self) -> bool:
        self._stopped = False
        return await self._ensure_connected()

    async def wait(
        self,
        *,
        timeout_seconds: float,
        stop_event: asyncio.Event,
    ) -> bool:
        if self._stopped or stop_event.is_set():
            return False

        if self._connection_lost and not self._wakeup_event.is_set():
            await self._ensure_connected()

        wakeup_waiter = asyncio.create_task(self._wakeup_event.wait())
        stop_waiter = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {wakeup_waiter, stop_waiter},
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for waiter in pending:
            waiter.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if self._stopped or stop_waiter in done:
            return False
        if wakeup_waiter in done:
            self._wakeup_event.clear()
            return True

        self.timeout_count += 1
        return False

    async def stop(self) -> None:
        self._stopped = True
        self._wakeup_event.set()
        await self._close_connection()

    async def _ensure_connected(self) -> bool:
        if self._stopped:
            return False
        if not self._connection_lost and self._connection is not None:
            return True

        was_connected = self._ever_connected
        await self._close_connection()
        try:
            connection = await self._engine.connect()
            raw_connection = await connection.get_raw_connection()
            driver_connection = raw_connection.driver_connection
            await driver_connection.add_listener(
                CONTROL_WAKEUP_CHANNEL,
                self._on_notification,
            )
            driver_connection.add_termination_listener(self._on_termination)
        except Exception:
            if "connection" in locals():
                await connection.close()
            self._connection_lost = True
            if not self._failure_logged:
                log.warning("AI Call PostgreSQL 唤醒监听不可用，退回周期扫描")
                self._failure_logged = True
            return False

        self._connection = connection
        self._driver_connection = driver_connection
        self._connection_lost = False
        self._ever_connected = True
        self._failure_logged = False
        if was_connected:
            self.reconnect_count += 1
        return True

    async def _close_connection(self) -> None:
        driver_connection = self._driver_connection
        connection = self._connection
        self._driver_connection = None
        self._connection = None
        self._connection_lost = True

        if driver_connection is not None and not driver_connection.is_closed():
            await driver_connection.remove_listener(
                CONTROL_WAKEUP_CHANNEL,
                self._on_notification,
            )
            driver_connection.remove_termination_listener(self._on_termination)
        if connection is not None:
            await connection.close()

    def _on_notification(
        self,
        _connection: object,
        _process_id: int,
        _channel: str,
        _payload: str,
    ) -> None:
        self.notification_count += 1
        self._wakeup_event.set()

    def _on_termination(self, _connection: object) -> None:
        self._connection_lost = True
        self._wakeup_event.set()
