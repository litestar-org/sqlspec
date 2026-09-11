"""Config-built service lifetime and borrowing contracts."""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec import sql
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError, SQLSpecError
from sqlspec.loader import SQLFileLoader
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

pytestmark = pytest.mark.anyio


@pytest.fixture
def sync_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[SqliteConfig, list[tuple[str, SqliteDriver]]]]:
    config = SqliteConfig(connection_config={"database": str(tmp_path / "service.sqlite")})
    events: list[tuple[str, SqliteDriver]] = []
    original = SqliteConfig.provide_session
    with original(config) as session:
        session.execute("CREATE TABLE service_values (value INTEGER)")
        session.execute("INSERT INTO service_values VALUES (1)")
        session.commit()

    @contextmanager
    def observed(self: SqliteConfig, *args: Any, **kwargs: Any) -> Iterator[SqliteDriver]:
        driver: SqliteDriver | None = None
        try:
            with original(self, *args, **kwargs) as driver:
                events.append(("enter", driver))
                yield driver
        finally:
            if driver is not None:
                events.append(("exit", driver))

    monkeypatch.setattr(SqliteConfig, "provide_session", observed)
    yield config, events
    config.close_pool()


@pytest.fixture
async def async_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]]]:
    config = AiosqliteConfig(connection_config={"database": str(tmp_path / "service.sqlite")})
    events: list[tuple[str, AiosqliteDriver]] = []
    original = AiosqliteConfig.provide_session
    async with original(config) as session:
        await session.execute("CREATE TABLE service_values (value INTEGER)")
        await session.execute("INSERT INTO service_values VALUES (1)")
        await session.commit()

    @asynccontextmanager
    async def observed(self: AiosqliteConfig, *args: Any, **kwargs: Any) -> AsyncIterator[AiosqliteDriver]:
        driver: AiosqliteDriver | None = None
        try:
            async with original(self, *args, **kwargs) as driver:
                events.append(("enter", driver))
                yield driver
        finally:
            if driver is not None:
                events.append(("exit", driver))

    monkeypatch.setattr(AiosqliteConfig, "provide_session", observed)
    yield config, events
    await config.close_pool()


@pytest.mark.parametrize("method", ["paginate", "get_one", "exists"])
def test_sync_service_opens_short_sessions(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], method: str
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    for _ in range(2):
        result = getattr(service, method)(sql.select("value").from_("service_values"))
        assert result
        assert events[-1][0] == "exit"
        with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
            _ = service.session
    assert [event for event, _ in events] == ["enter", "exit", "enter", "exit"]
    assert events[0][1] is not events[2][1]


@pytest.mark.parametrize("method", ["paginate", "get_one", "exists"])
async def test_async_service_opens_short_sessions(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], method: str
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    for _ in range(2):
        result = await getattr(service, method)(sql.select("value").from_("service_values"))
        assert result
        assert events[-1][0] == "exit"
        with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
            _ = service.driver
    assert [event for event, _ in events] == ["enter", "exit", "enter", "exit"]
    assert events[0][1] is not events[2][1]


@pytest.mark.parametrize("method", ["paginate", "get_one", "exists"])
def test_sync_service_releases_failed_query(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], method: str
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    with pytest.raises(SQLSpecError):
        getattr(service, method)(sql.select("value").from_("missing_service_table"))
    assert [event for event, _ in events] == ["enter", "exit"]
    with pytest.raises(NotFoundError):
        service.get_one(sql.select("value").from_("service_values").where("1 = 0"))
    assert events[-1][0] == "exit"
    assert service.exists(sql.select("value").from_("service_values"))


@pytest.mark.parametrize("method", ["paginate", "get_one", "exists"])
async def test_async_service_releases_failed_query(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], method: str
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    with pytest.raises(SQLSpecError):
        await getattr(service, method)(sql.select("value").from_("missing_service_table"))
    assert [event for event, _ in events] == ["enter", "exit"]
    with pytest.raises(NotFoundError):
        await service.get_one(sql.select("value").from_("service_values").where("1 = 0"))
    assert events[-1][0] == "exit"
    assert await service.exists(sql.select("value").from_("service_values"))


def test_sync_service_borrows_explicit_session(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]],
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    with service.provide_session() as session:
        assert service.get_one(sql.select("value").from_("service_values"), session=session) == {"value": 1}
        assert service.exists(sql.select("value").from_("service_values"), session=session)
        assert service.paginate(sql.select("value").from_("service_values"), session=session).total == 1
        assert [event for event, _ in events] == ["enter"]
    assert [event for event, _ in events] == ["enter", "exit"]


async def test_async_service_borrows_explicit_session(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]],
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    async with service.provide_session() as session:
        assert await service.get_one(sql.select("value").from_("service_values"), session=session) == {"value": 1}
        assert await service.exists(sql.select("value").from_("service_values"), session=session)
        assert (await service.paginate(sql.select("value").from_("service_values"), session=session)).total == 1
        assert [event for event, _ in events] == ["enter"]
    assert [event for event, _ in events] == ["enter", "exit"]


@pytest.mark.parametrize("config", [SqliteConfig(), AdbcConfig()])
def test_sync_service_constructor_and_loader(config: Any) -> None:
    loader = SQLFileLoader()
    service = SQLSpecSyncService(config=config, loader=loader)
    assert service.loader is loader
    assert SQLSpecSyncService(MagicMock()).loader is None
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecSyncService()
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecSyncService(MagicMock(), config=config)
    with pytest.raises(ImproperConfigurationError, match="sync"):
        SQLSpecSyncService(config=AiosqliteConfig())  # type: ignore[arg-type]


def test_service_uses_local_no_pool_config() -> None:
    service = SQLSpecSyncService(config=AdbcConfig())
    assert service.get_one(sql.select("1 AS value")) == {"value": 1}
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.driver


def test_session_built_service_borrows_and_forwards_kwargs() -> None:
    original = MagicMock()
    explicit = MagicMock()
    explicit.select_one_or_none.return_value = {"value": 1}
    service: SQLSpecSyncService = SQLSpecSyncService(original)
    statement = sql.select("1 AS value")
    with service.provide_session() as borrowed:
        assert borrowed is original
    assert service.session is service.driver is original
    assert service.get_one(statement, session=explicit, value=1) == {"value": 1}
    explicit.select_one_or_none.assert_called_once_with(statement, schema_type=None, value=1)
    original.select_one_or_none.assert_not_called()
    original.close.assert_not_called()


async def test_async_session_built_service_borrows_and_forwards_kwargs() -> None:
    original = AsyncMock()
    explicit = AsyncMock()
    explicit.select_one_or_none.return_value = {"value": 1}
    service: SQLSpecAsyncService = SQLSpecAsyncService(original)
    statement = sql.select("1 AS value")
    async with service.provide_session() as borrowed:
        assert borrowed is original
    assert service.session is service.driver is original
    assert await service.get_one(statement, session=explicit, value=1) == {"value": 1}
    explicit.select_one_or_none.assert_awaited_once_with(statement, schema_type=None, value=1)
    original.select_one_or_none.assert_not_awaited()
    original.close.assert_not_awaited()


@pytest.mark.parametrize("config", [AiosqliteConfig(), MysqlConnectorAsyncConfig()])
def test_async_service_constructor_and_loader(config: Any) -> None:
    loader = SQLFileLoader()
    service = SQLSpecAsyncService(config=config, loader=loader)
    assert service.loader is loader
    assert SQLSpecAsyncService(AsyncMock()).loader is None
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecAsyncService()
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecAsyncService(AsyncMock(), config=config)
    with pytest.raises(ImproperConfigurationError, match="async"):
        SQLSpecAsyncService(config=SqliteConfig())  # type: ignore[arg-type]


@pytest.mark.parametrize("method", ["begin", "commit", "rollback"])
def test_sync_service_manual_control_requires_session(method: str) -> None:
    service = SQLSpecSyncService(config=SqliteConfig())
    with pytest.raises(ImproperConfigurationError, match=r"begin_transaction\(\).*session="):
        getattr(service, method)()
    session = MagicMock()
    getattr(service, method)(session=session)
    getattr(session, method).assert_called_once_with()


@pytest.mark.parametrize("method", ["begin", "commit", "rollback"])
async def test_async_service_manual_control_requires_session(method: str) -> None:
    service = SQLSpecAsyncService(config=AiosqliteConfig())
    with pytest.raises(ImproperConfigurationError, match=r"begin_transaction\(\).*session="):
        await getattr(service, method)()
    session = AsyncMock()
    await getattr(service, method)(session=session)
    getattr(session, method).assert_awaited_once_with()
