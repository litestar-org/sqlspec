"""Config-built service lifetime and borrowing contracts."""

import asyncio
import sqlite3
import threading
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, closing, contextmanager
from contextvars import copy_context
from pathlib import Path
from typing import Any

import pytest

from sqlspec import sql
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError, SQLSpecError
from sqlspec.loader import SQLFileLoader
from sqlspec.service import _TRANSACTIONS, SQLSpecAsyncService, SQLSpecSyncService

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
def test_sync_service_constructor_and_loader(
    config: Any, sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]]
) -> None:
    loader = SQLFileLoader()
    service = SQLSpecSyncService(config=config, loader=loader)
    assert service.loader is loader
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecSyncService()
    with sync_config[0].provide_session() as session:
        assert SQLSpecSyncService(session).loader is None
        with pytest.raises(ImproperConfigurationError, match="exactly one"):
            SQLSpecSyncService(session, config=config)
    with pytest.raises(ImproperConfigurationError, match="sync"):
        SQLSpecSyncService(config=AiosqliteConfig())  # type: ignore[arg-type]


def test_service_uses_local_no_pool_config() -> None:
    service = SQLSpecSyncService(config=AdbcConfig())
    assert service.get_one(sql.select("1 AS value")) == {"value": 1}
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.driver


def test_session_built_service_borrows_and_forwards_kwargs(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, _ = sync_config
    statement = sql.select("1 AS value")
    calls: list[tuple[SqliteDriver, object, dict[str, Any]]] = []

    def select(self: SqliteDriver, query: object, **kwargs: Any) -> dict[str, int]:
        calls.append((self, query, kwargs))
        return {"value": 1}

    monkeypatch.setattr(SqliteDriver, "select_one_or_none", select)
    with config.provide_session() as original, config.provide_session() as explicit:
        service = SQLSpecSyncService(original)
        with service.provide_session() as borrowed:
            assert borrowed is original
        assert service.session is service.driver is original
        assert service.get_one(statement, session=explicit, value=1) == {"value": 1}
        assert calls == [(explicit, statement, {"schema_type": None, "value": 1})]
        original.execute("SELECT 1")


async def test_async_session_built_service_borrows_and_forwards_kwargs(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, _ = async_config
    statement = sql.select("1 AS value")
    calls: list[tuple[AiosqliteDriver, object, dict[str, Any]]] = []

    async def select(self: AiosqliteDriver, query: object, **kwargs: Any) -> dict[str, int]:
        calls.append((self, query, kwargs))
        return {"value": 1}

    monkeypatch.setattr(AiosqliteDriver, "select_one_or_none", select)
    async with config.provide_session() as original, config.provide_session() as explicit:
        service = SQLSpecAsyncService(original)
        async with service.provide_session() as borrowed:
            assert borrowed is original
        assert service.session is service.driver is original
        assert await service.get_one(statement, session=explicit, value=1) == {"value": 1}
        assert calls == [(explicit, statement, {"schema_type": None, "value": 1})]
        await original.execute("SELECT 1")


@pytest.mark.parametrize("config", [AiosqliteConfig(), MysqlConnectorAsyncConfig()])
async def test_async_service_constructor_and_loader(
    config: Any, async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]]
) -> None:
    loader = SQLFileLoader()
    service = SQLSpecAsyncService(config=config, loader=loader)
    assert service.loader is loader
    with pytest.raises(ImproperConfigurationError, match="exactly one"):
        SQLSpecAsyncService()
    async with async_config[0].provide_session() as session:
        assert SQLSpecAsyncService(session).loader is None
        with pytest.raises(ImproperConfigurationError, match="exactly one"):
            SQLSpecAsyncService(session, config=config)
    with pytest.raises(ImproperConfigurationError, match="async"):
        SQLSpecAsyncService(config=SqliteConfig())  # type: ignore[arg-type]


@pytest.mark.parametrize("method", ["begin", "commit", "rollback"])
def test_sync_service_manual_control_requires_session(
    method: str, sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, _ = sync_config
    service = SQLSpecSyncService(config=config)
    with pytest.raises(ImproperConfigurationError, match=r"begin_transaction\(\).*session="):
        getattr(service, method)()
    calls: list[SqliteDriver] = []

    def control(self: SqliteDriver) -> None:
        calls.append(self)

    monkeypatch.setattr(SqliteDriver, method, control)
    with config.provide_session() as session:
        getattr(service, method)(session=session)
    assert calls == [session]


@pytest.mark.parametrize("method", ["begin", "commit", "rollback"])
async def test_async_service_manual_control_requires_session(
    method: str,
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = async_config
    service = SQLSpecAsyncService(config=config)
    with pytest.raises(ImproperConfigurationError, match=r"begin_transaction\(\).*session="):
        await getattr(service, method)()
    calls: list[AiosqliteDriver] = []

    async def control(self: AiosqliteDriver) -> None:
        calls.append(self)

    monkeypatch.setattr(AiosqliteDriver, method, control)
    async with config.provide_session() as session:
        await getattr(service, method)(session=session)
    assert calls == [session]


@pytest.mark.parametrize("fail", [False, True])
def test_sync_config_transaction_spans_helpers(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], fail: bool
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    try:
        with service.begin_transaction() as session:
            assert service.session is service.driver is session
            session.execute("INSERT INTO service_values VALUES (2)")
            assert service.get_one(sql.select("value").from_("service_values").where("value = 2")) == {"value": 2}
            assert service.paginate(sql.select("value").from_("service_values")).total == 2
            assert [event for event, _ in events] == ["enter"]
            with closing(sqlite3.connect(config.connection_config["database"])) as outside:
                assert outside.execute("SELECT COUNT(*) FROM service_values WHERE value = 2").fetchone() == (0,)
            if fail:
                raise ValueError("rollback")
    except ValueError:
        assert fail
    assert events[-1] == ("exit", events[0][1])
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.session
    assert service.exists(sql.select("value").from_("service_values").where("value = 2")) is not fail


@pytest.mark.parametrize("fail", [False, True])
async def test_async_config_transaction_spans_helpers(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], fail: bool
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    try:
        async with service.begin_transaction() as session:
            assert service.session is service.driver is session
            await session.execute("INSERT INTO service_values VALUES (2)")
            assert await service.get_one(sql.select("value").from_("service_values").where("value = 2")) == {"value": 2}
            assert (await service.paginate(sql.select("value").from_("service_values"))).total == 2
            assert [event for event, _ in events] == ["enter"]
            async with config.provide_session() as outside:
                assert not await service.exists(
                    sql.select("value").from_("service_values").where("value = 2"), session=outside
                )
            if fail:
                raise ValueError("rollback")
    except ValueError:
        assert fail
    assert events[-1] == ("exit", events[0][1])
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.driver
    assert await service.exists(sql.select("value").from_("service_values").where("value = 2")) is not fail


@pytest.mark.parametrize("stage", ["begin", "commit", "rollback", "provider"])
def test_sync_transaction_failure_releases_and_resets(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    error = RuntimeError(stage)
    received: list[BaseException] = []
    original_provider = SqliteConfig.provide_session

    @contextmanager
    def provider(self: SqliteConfig) -> Iterator[SqliteDriver]:
        try:
            with original_provider(self) as driver:
                yield driver
        except BaseException as exc:
            received.append(exc)
            raise
        if stage == "provider":
            raise error

    def fail(self: SqliteDriver) -> None:
        raise error

    with monkeypatch.context() as patch:
        patch.setattr(SqliteConfig, "provide_session", provider)
        if stage != "provider":
            patch.setattr(SqliteDriver, stage, fail)
        with pytest.raises(RuntimeError) as raised:
            with service.begin_transaction():
                if stage == "rollback":
                    raise ValueError("body")
        assert raised.value is error
    assert [event for event, _ in events] == ["enter", "exit"]
    if stage != "provider":
        assert received == [error]
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.session
    assert service.exists(sql.select("value").from_("service_values"))


@pytest.mark.parametrize("stage", ["begin", "commit", "rollback", "provider"])
async def test_async_transaction_failure_releases_and_resets(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    error = RuntimeError(stage)
    received: list[BaseException] = []
    original_provider = AiosqliteConfig.provide_session

    @asynccontextmanager
    async def provider(self: AiosqliteConfig) -> AsyncIterator[AiosqliteDriver]:
        try:
            async with original_provider(self) as driver:
                yield driver
        except BaseException as exc:
            received.append(exc)
            raise
        if stage == "provider":
            raise error

    async def fail(self: AiosqliteDriver) -> None:
        raise error

    with monkeypatch.context() as patch:
        patch.setattr(AiosqliteConfig, "provide_session", provider)
        if stage != "provider":
            patch.setattr(AiosqliteDriver, stage, fail)
        with pytest.raises(RuntimeError) as raised:
            async with service.begin_transaction():
                if stage == "rollback":
                    raise ValueError("body")
        assert raised.value is error
    assert [event for event, _ in events] == ["enter", "exit"]
    if stage != "provider":
        assert received == [error]
    with pytest.raises(ImproperConfigurationError, match="begin_transaction"):
        _ = service.session
    assert await service.exists(sql.select("value").from_("service_values"))


def test_sync_nested_transaction_does_not_commit(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]],
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    with pytest.raises(ValueError, match="outer"):
        with service.begin_transaction() as session:
            session.execute("INSERT INTO service_values VALUES (2)")
            with pytest.raises(ImproperConfigurationError, match="Nested"):
                with service.begin_transaction():
                    pytest.fail("nested entry")
            assert service.session is session
            assert [event for event, _ in events] == ["enter"]
            raise ValueError("outer")
    assert not service.exists(sql.select("value").from_("service_values").where("value = 2"))


async def test_async_nested_transaction_does_not_commit(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]],
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    with pytest.raises(ValueError, match="outer"):
        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO service_values VALUES (2)")
            with pytest.raises(ImproperConfigurationError, match="Nested"):
                async with service.begin_transaction():
                    pytest.fail("nested entry")
            assert service.session is session
            assert [event for event, _ in events] == ["enter"]
            raise ValueError("outer")
    assert not await service.exists(sql.select("value").from_("service_values").where("value = 2"))


async def test_async_independent_transactions_do_not_share_sessions(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    ready = [asyncio.Event(), asyncio.Event()]

    async def begin_read_transaction(self: AiosqliteDriver) -> None:
        """Start a deferred read transaction so two transactions can overlap."""
        await self.connection.execute("BEGIN")

    monkeypatch.setattr(AiosqliteDriver, "begin", begin_read_transaction)

    async def work(index: int) -> AiosqliteDriver:
        async with service.begin_transaction() as session:
            ready[index].set()
            await ready[1 - index].wait()
            assert service.session is session
            assert await service.exists(sql.select("value").from_("service_values"))
            return session

    first, second = await asyncio.gather(work(0), work(1))
    assert first is not second
    assert sorted(event for event, _ in events) == ["enter", "enter", "exit", "exit"]


async def test_async_inherited_transaction_refuses_implicit_reuse(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]],
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    released = asyncio.Event()

    async def child() -> None:
        with pytest.raises(ImproperConfigurationError, match="another task"):
            await service.exists(sql.select("value").from_("service_values"))
        async with config.provide_session() as explicit:
            assert await service.exists(sql.select("value").from_("service_values"), session=explicit)
        released.set()
        await finished.wait()
        with pytest.raises(ImproperConfigurationError, match="no longer active"):
            _ = service.session

    finished = asyncio.Event()
    async with service.begin_transaction():
        task = asyncio.create_task(child())
        await released.wait()
        assert [event for event, _ in events] == ["enter", "enter", "exit"]
    finished.set()
    await task
    assert await service.exists(sql.select("value").from_("service_values"))


async def test_async_transaction_cancellation_rolls_back_and_releases(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]],
) -> None:
    config, events = async_config
    service = SQLSpecAsyncService(config=config)
    ready = asyncio.Event()

    async def work() -> None:
        async with service.begin_transaction() as session:
            await session.execute("INSERT INTO service_values VALUES (2)")
            ready.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(work())
    await ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [event for event, _ in events] == ["enter", "exit"]
    assert not await service.exists(sql.select("value").from_("service_values").where("value = 2"))


def test_sync_transactions_are_independent_between_threads(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]],
) -> None:
    config, _ = sync_config
    service = SQLSpecSyncService(config=config)
    barrier = threading.Barrier(2)

    def work() -> SqliteDriver:
        with service.begin_transaction() as session:
            barrier.wait(timeout=10)
            assert service.session is session
            assert service.exists(sql.select("value").from_("service_values"))
        session.connection.close()
        return session

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(work) for _ in range(2)]
        first, second = (future.result(timeout=15) for future in futures)
    assert first is not second


def test_service_transaction_context_releases_retained_driver(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]],
) -> None:
    config, _ = sync_config
    initial = _TRANSACTIONS.get()
    for _ in range(10):
        service = SQLSpecSyncService(config=config)
        with service.begin_transaction():
            inherited = copy_context()
        assert _TRANSACTIONS.get() is initial
        assert all(state.driver is None for state in (inherited.run(_TRANSACTIONS.get) or {}).values())
        with pytest.raises(ImproperConfigurationError, match="no longer active"):
            inherited.run(lambda: service.session)


def test_sync_inherited_transaction_rejects_another_thread(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]],
) -> None:
    config, events = sync_config
    service = SQLSpecSyncService(config=config)
    with service.begin_transaction():
        inherited = copy_context()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(inherited.run, lambda: service.exists(sql.select("1")))
            with pytest.raises(ImproperConfigurationError, match="another task or thread"):
                future.result(timeout=10)
        assert [event for event, _ in events] == ["enter"]


@pytest.mark.parametrize("fail", [False, True])
def test_sync_session_service_preserves_subclass_transaction_hooks(
    sync_config: tuple[SqliteConfig, list[tuple[str, SqliteDriver]]], fail: bool
) -> None:
    config, _ = sync_config
    calls: list[str] = []

    class CustomService(SQLSpecSyncService[SqliteDriver]):
        """Subclass whose transaction hooks predate the session keyword."""

        def begin(self) -> None:  # type: ignore[override]
            calls.append("begin")
            super().begin()

        def commit(self) -> None:  # type: ignore[override]
            calls.append("commit")
            super().commit()

        def rollback(self) -> None:  # type: ignore[override]
            calls.append("rollback")
            super().rollback()

    with config.provide_session() as session:
        service = CustomService(session)
        try:
            with service.begin_transaction() as bound:
                assert bound is session
                assert service.exists(sql.select("value").from_("service_values"))
                if fail:
                    raise ValueError("body")
        except ValueError:
            assert fail
        assert service.session is session
        assert calls == ["begin", "rollback" if fail else "commit"]


@pytest.mark.parametrize("fail", [False, True])
async def test_async_session_service_preserves_subclass_transaction_hooks(
    async_config: tuple[AiosqliteConfig, list[tuple[str, AiosqliteDriver]]], fail: bool
) -> None:
    config, _ = async_config
    calls: list[str] = []

    class CustomService(SQLSpecAsyncService[AiosqliteDriver]):
        """Subclass whose transaction hooks predate the session keyword."""

        async def begin(self) -> None:  # type: ignore[override]
            calls.append("begin")
            await super().begin()

        async def commit(self) -> None:  # type: ignore[override]
            calls.append("commit")
            await super().commit()

        async def rollback(self) -> None:  # type: ignore[override]
            calls.append("rollback")
            await super().rollback()

    async with config.provide_session() as session:
        service = CustomService(session)
        try:
            async with service.begin_transaction() as bound:
                assert bound is session
                assert await service.exists(sql.select("value").from_("service_values"))
                if fail:
                    raise ValueError("body")
        except ValueError:
            assert fail
        assert service.session is session
        assert calls == ["begin", "rollback" if fail else "commit"]
