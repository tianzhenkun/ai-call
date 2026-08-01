from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.services.ai_call.runtime_control.dispatcher_service import (
    DispatcherControlService,
)
from app.services.ai_call.runtime_control.postgres_wakeup import (
    CONTROL_WAKEUP_CHANNEL,
    PostgresWakeupListener,
    publish_control_wakeup,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
)


class _RecordingSession:
    def __init__(self, dialect_name: str) -> None:
        self._bind = SimpleNamespace(dialect=SimpleNamespace(name=dialect_name))
        self.executions: list[tuple[object, dict[str, str] | None]] = []

    def get_bind(self) -> object:
        return self._bind

    async def execute(
        self,
        statement: object,
        params: dict[str, str] | None = None,
    ) -> None:
        self.executions.append((statement, params))


class _FakeDriverConnection:
    def __init__(self) -> None:
        self.listeners: dict[str, object] = {}
        self.termination_listeners: list[object] = []
        self.closed = False

    async def add_listener(self, channel: str, callback: object) -> None:
        self.listeners[channel] = callback

    async def remove_listener(self, channel: str, callback: object) -> None:
        if self.listeners.get(channel) == callback:
            del self.listeners[channel]

    def add_termination_listener(self, callback: object) -> None:
        self.termination_listeners.append(callback)

    def remove_termination_listener(self, callback: object) -> None:
        if callback in self.termination_listeners:
            self.termination_listeners.remove(callback)

    def is_closed(self) -> bool:
        return self.closed

    def emit(self, payload: str) -> None:
        callback = self.listeners[CONTROL_WAKEUP_CHANNEL]
        callback(self, 123, CONTROL_WAKEUP_CHANNEL, payload)

    def terminate(self) -> None:
        self.closed = True
        for callback in list(self.termination_listeners):
            callback(self)


class _FakeAsyncConnection:
    def __init__(self, driver: _FakeDriverConnection) -> None:
        self.driver = driver
        self.closed = False

    async def get_raw_connection(self) -> object:
        return SimpleNamespace(driver_connection=self.driver)

    async def close(self) -> None:
        self.closed = True


class _FakeEngine:
    def __init__(self) -> None:
        self.connections: list[_FakeAsyncConnection] = []

    async def connect(self) -> _FakeAsyncConnection:
        connection = _FakeAsyncConnection(_FakeDriverConnection())
        self.connections.append(connection)
        return connection


class _FailingEngine:
    def __init__(self) -> None:
        self.connect_count = 0

    async def connect(self) -> None:
        self.connect_count += 1
        raise OSError("database unavailable")


class _FakeWakeupListener:
    def __init__(self) -> None:
        self.event = asyncio.Event()
        self.start_count = 0
        self.wait_count = 0
        self.stop_count = 0

    async def start(self) -> bool:
        self.start_count += 1
        return True

    async def wait(
        self,
        *,
        timeout_seconds: float,
        stop_event: asyncio.Event,
    ) -> bool:
        self.wait_count += 1
        notification = asyncio.create_task(self.event.wait())
        stopped = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {notification, stopped},
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if stopped in done:
            return False
        if notification in done:
            self.event.clear()
            return True
        return False

    async def stop(self) -> None:
        self.stop_count += 1

    def signal(self) -> None:
        self.event.set()


class _RuntimeSession:
    async def execute(self, _statement: object) -> None:
        return None


class _RuntimeTransaction:
    async def __aenter__(self) -> _RuntimeSession:
        return _RuntimeSession()

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        return None


class _RuntimeSessionFactory:
    def begin(self) -> _RuntimeTransaction:
        return _RuntimeTransaction()


@pytest.mark.anyio
async def test_publish_control_wakeup_uses_fixed_channel_and_empty_payload() -> None:
    session = _RecordingSession("postgresql")

    await publish_control_wakeup(session)

    assert len(session.executions) == 1
    statement, params = session.executions[0]
    assert str(statement) == "select pg_notify(:channel, '')"
    assert params == {"channel": CONTROL_WAKEUP_CHANNEL}


@pytest.mark.anyio
async def test_publish_control_wakeup_is_noop_outside_postgresql() -> None:
    session = _RecordingSession("sqlite")

    await publish_control_wakeup(session)

    assert session.executions == []


@pytest.mark.anyio
async def test_listener_treats_payload_as_opaque_and_coalesces_notifications() -> None:
    engine = _FakeEngine()
    listener = PostgresWakeupListener(engine)
    stop_event = asyncio.Event()

    assert await listener.start() is True
    driver = engine.connections[0].driver
    driver.emit("tenant=should-not-be-parsed")
    driver.emit("call=should-not-be-parsed")

    assert await listener.wait(timeout_seconds=0.1, stop_event=stop_event) is True
    assert await listener.wait(timeout_seconds=0.001, stop_event=stop_event) is False
    assert listener.notification_count == 2
    assert listener.timeout_count == 1
    assert "dsn" not in vars(listener)

    await listener.stop()
    assert driver.listeners == {}
    assert driver.termination_listeners == []
    assert engine.connections[0].closed is True


@pytest.mark.anyio
async def test_listener_stop_event_ends_wait_without_timeout() -> None:
    engine = _FakeEngine()
    listener = PostgresWakeupListener(engine)
    stop_event = asyncio.Event()
    await listener.start()

    waiter = asyncio.create_task(
        listener.wait(timeout_seconds=30, stop_event=stop_event)
    )
    await asyncio.sleep(0)
    stop_event.set()

    assert await asyncio.wait_for(waiter, timeout=0.1) is False
    assert listener.timeout_count == 0
    await listener.stop()


@pytest.mark.anyio
async def test_listener_reconnects_after_driver_termination() -> None:
    engine = _FakeEngine()
    listener = PostgresWakeupListener(engine)
    stop_event = asyncio.Event()
    await listener.start()

    engine.connections[0].driver.terminate()

    assert await listener.wait(timeout_seconds=0.1, stop_event=stop_event) is True
    assert await listener.wait(timeout_seconds=0.1, stop_event=stop_event) is False
    assert len(engine.connections) == 2
    assert engine.connections[0].closed is True
    assert listener.reconnect_count == 1
    await listener.stop()


@pytest.mark.anyio
async def test_listener_connection_failure_falls_back_without_busy_loop() -> None:
    engine = _FailingEngine()
    listener = PostgresWakeupListener(engine)
    stop_event = asyncio.Event()

    assert await listener.start() is False
    assert await listener.wait(timeout_seconds=0.001, stop_event=stop_event) is False
    assert engine.connect_count == 2
    assert listener.timeout_count == 1
    await listener.stop()


@pytest.mark.anyio
async def test_dispatcher_notification_only_triggers_existing_database_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = _FakeWakeupListener()
    service = DispatcherControlService(
        object(),
        scan_interval_seconds=30,
        wakeup_listener=listener,
    )
    scans: list[tuple[object, ...]] = []
    first_scan = asyncio.Event()
    second_scan = asyncio.Event()

    async def _run_once(*args: object) -> int:
        scans.append(args)
        if len(scans) == 1:
            first_scan.set()
        if len(scans) == 2:
            second_scan.set()
        return 0

    monkeypatch.setattr(service, "run_once", _run_once)
    await service.start()
    await asyncio.wait_for(first_scan.wait(), timeout=0.1)
    listener.signal()
    await asyncio.wait_for(second_scan.wait(), timeout=0.1)
    await service.stop()

    assert scans == [(), ()]
    assert listener.start_count == 1
    assert listener.wait_count >= 1
    assert listener.stop_count == 1


@pytest.mark.anyio
async def test_dispatcher_without_listener_keeps_periodic_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = DispatcherControlService(
        object(),
        scan_interval_seconds=0.001,
    )
    scan_count = 0
    second_scan = asyncio.Event()

    async def _run_once() -> int:
        nonlocal scan_count
        scan_count += 1
        if scan_count == 2:
            second_scan.set()
        return 0

    monkeypatch.setattr(service, "run_once", _run_once)
    await service.start()
    await asyncio.wait_for(second_scan.wait(), timeout=0.1)
    await service.stop()

    assert scan_count >= 2


@pytest.mark.anyio
async def test_runtime_notification_only_triggers_existing_database_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app.services.ai_call.runtime_control import runtime_service

    listener = _FakeWakeupListener()
    worker_id = "runtime-a:12345678-1234-5678-1234-567812345678"

    class _WorkerRegistryRepository:
        def __init__(self, _session: object, **_kwargs: object) -> None:
            pass

        async def register(self, _registration: object) -> object:
            return SimpleNamespace(worker_id=worker_id)

    monkeypatch.setattr(
        runtime_service,
        "WorkerRegistryRepository",
        _WorkerRegistryRepository,
    )
    service = RuntimeControlService(
        worker_id=worker_id,
        registry=RuntimeRegistry(),
        session_factory=_RuntimeSessionFactory(),
        provider=object(),
        scan_interval_seconds=30,
        wakeup_listener=listener,
    )
    scans: list[tuple[object, ...]] = []
    first_scan = asyncio.Event()
    second_scan = asyncio.Event()

    async def _run_once(*args: object) -> int:
        scans.append(args)
        if len(scans) == 1:
            first_scan.set()
        if len(scans) == 2:
            second_scan.set()
        return 0

    monkeypatch.setattr(service, "run_once", _run_once)
    await service.start()
    await asyncio.wait_for(first_scan.wait(), timeout=0.1)
    listener.signal()
    await asyncio.wait_for(second_scan.wait(), timeout=0.1)
    await service.stop()

    assert scans == [(), ()]
    assert listener.start_count == 1
    assert listener.stop_count == 1
