from types import SimpleNamespace

from app.core import database


def test_async_postgres_engine_bounds_database_commands(monkeypatch) -> None:
    captured: dict[str, object] = {}
    engine = object()

    monkeypatch.setattr(
        database,
        "settings",
        SimpleNamespace(
            SQL_DB_ENABLE=True,
            DATABASE_TYPE="postgres",
            DATABASE_ECHO=False,
            ECHO_POOL=False,
            POOL_PRE_PING=True,
            FUTURE=True,
            POOL_RECYCLE=1800,
            POOL_SIZE=10,
            MAX_OVERFLOW=20,
            POOL_TIMEOUT=30,
            POOL_USE_LIFO=True,
            DATABASE_COMMAND_TIMEOUT=17,
            AUTOCOMMIT=False,
            AUTOFETCH=False,
            EXPIRE_ON_COMMIT=False,
        ),
    )
    monkeypatch.setattr(
        database,
        "create_async_engine",
        lambda **kwargs: captured.update(kwargs) or engine,
    )
    monkeypatch.setattr(database, "async_sessionmaker", lambda **_kwargs: object())

    database.create_async_engine_and_session("postgresql+asyncpg://local/test")

    assert captured["connect_args"] == {"command_timeout": 17}
