"""Session contexts close open streams before rollback, release, and session close."""

import asyncio
import importlib
import inspect
import sqlite3
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.psycopg.core import TRANSACTION_STATUS_INTRANS
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig
from sqlspec.driver._common import _CLOSED_CONNECTION
from sqlspec.driver._stream import AsyncRowStream, SyncRowStream
from sqlspec.exceptions import ImproperConfigurationError

_EMBEDDED_CONFIGS = {
    "sqlite": (SqliteConfig, "database"),
    "aiosqlite": (AiosqliteConfig, "database"),
    "duckdb": (DuckDBConfig, "database"),
    "adbc": (AdbcConfig, "uri"),
}


class RecordingSource:
    """Stream source that logs its close and can fail while closing."""

    def __init__(
        self,
        events: "list[str]",
        *,
        is_async: bool,
        fail_on_close: bool = False,
        close_error: "BaseException | None" = None,
    ) -> None:
        self._events = events
        self._fail_on_close = fail_on_close
        self._close_error = close_error
        self._is_async = is_async
        self.closed = False

    def start(self) -> Any:
        return self._noop()

    def fetch_chunk(self) -> Any:
        return self._rows()

    def close(self, error: bool = False) -> Any:
        return self._close()

    async def _noop(self) -> None:
        return None

    async def _rows(self) -> "list[dict[str, Any]]":
        return []

    async def _close(self) -> None:
        self._record_close()

    def _record_close(self) -> None:
        self.closed = True
        self._events.append("stream-close")
        if self._close_error is not None:
            raise self._close_error
        if self._fail_on_close:
            msg = "close failed"
            raise RuntimeError(msg)


class SyncRecordingSource(RecordingSource):
    def start(self) -> None:
        return None

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        return []

    def close(self, error: bool = False) -> None:
        self._record_close()


def make_stream(
    events: "list[str]", *, is_async: bool, fail_on_close: bool = False, close_error: "BaseException | None" = None
) -> Any:
    if is_async:
        return AsyncRowStream(
            RecordingSource(events, is_async=True, fail_on_close=fail_on_close, close_error=close_error)
        )
    return SyncRowStream(
        SyncRecordingSource(events, is_async=False, fail_on_close=fail_on_close, close_error=close_error)
    )


class StreamAbort(BaseException):
    """Non-``Exception`` interrupt raised while a stream closes, like ``KeyboardInterrupt``."""


ABORTS = (asyncio.CancelledError, StreamAbort)


async def resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.fixture(params=sorted(_EMBEDDED_CONFIGS))
def embedded(request: pytest.FixtureRequest, tmp_path: Path) -> "tuple[str, Any, str]":
    name = str(request.param)
    path = str(tmp_path / f"{name}.db")
    if name == "sqlite" or name == "aiosqlite":
        with sqlite3.connect(path) as connection:
            connection.execute("create table t (x text)")
            connection.executemany("insert into t values (?)", [("A0",), ("A1",), ("A2",)])
    config_cls, key = _EMBEDDED_CONFIGS[name]
    connection_config: dict[str, Any] = {key: path}
    if name == "aiosqlite":
        connection_config.update(pool_size=1, connect_timeout=2.0)
    if name == "adbc":
        connection_config["driver_name"] = "adbc_driver_sqlite"
        with sqlite3.connect(path) as connection:
            connection.execute("create table t (x text)")
            connection.executemany("insert into t values (?)", [("A0",), ("A1",), ("A2",)])
    return name, config_cls(connection_config=connection_config), path


@asynccontextmanager
async def session_scope(config: Any) -> "AsyncGenerator[Any, None]":
    if config.is_async:
        async with config.provide_session() as session:
            yield session
    else:
        with config.provide_session() as session:
            yield session


def spy_on_release(monkeypatch: pytest.MonkeyPatch, config: Any, events: "list[str]") -> None:
    factory = config._session_factory_class
    original = factory.release_connection

    if config.is_async:

        async def async_release(self: Any, *args: Any, **kwargs: Any) -> Any:
            events.append("release")
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(factory, "release_connection", async_release)
    else:

        def release(self: Any, *args: Any, **kwargs: Any) -> Any:
            events.append("release")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(factory, "release_connection", release)


@pytest.mark.anyio
async def test_stream_closed_before_connection_release(
    embedded: "tuple[str, Any, str]", monkeypatch: pytest.MonkeyPatch
) -> None:
    _, config, _ = embedded
    events: list[str] = []
    spy_on_release(monkeypatch, config, events)
    try:
        async with session_scope(config) as session:
            stream = make_stream(events, is_async=config.is_async)
            session._register_open_stream(stream)
        assert events == ["stream-close", "release"]
        assert session.connection is _CLOSED_CONNECTION
    finally:
        await resolve(config.close_pool())


@pytest.mark.anyio
async def test_stream_close_error_does_not_skip_release(
    embedded: "tuple[str, Any, str]", monkeypatch: pytest.MonkeyPatch
) -> None:
    _, config, _ = embedded
    events: list[str] = []
    spy_on_release(monkeypatch, config, events)
    try:
        async with session_scope(config) as session:
            session._register_open_stream(make_stream(events, is_async=config.is_async, fail_on_close=True))
        assert events == ["stream-close", "release"]
    finally:
        await resolve(config.close_pool())


@pytest.mark.anyio
@pytest.mark.parametrize("abort", ABORTS, ids=lambda abort: abort.__name__)
async def test_stream_close_interrupt_still_releases_connection(
    embedded: "tuple[str, Any, str]", monkeypatch: pytest.MonkeyPatch, abort: "type[BaseException]"
) -> None:
    _, config, _ = embedded
    events: list[str] = []
    spy_on_release(monkeypatch, config, events)
    sessions: list[Any] = []
    try:
        with pytest.raises(abort):
            async with session_scope(config) as session:
                sessions.append(session)
                session._register_open_stream(make_stream(events, is_async=config.is_async, close_error=abort()))
        assert events == ["stream-close", "release"]
        assert sessions[0].connection is _CLOSED_CONNECTION
        async with session_scope(config) as second:
            assert await resolve(second.select_value("select 1")) == 1
    finally:
        await resolve(config.close_pool())


@pytest.mark.anyio
async def test_started_native_stream_does_not_lock_reused_connection(embedded: "tuple[str, Any, str]") -> None:
    name, config, _ = embedded
    if name in {"duckdb", "adbc"}:
        pytest.skip("only sqlite file locks make an open cursor observable")
    try:
        async with session_scope(config) as first:
            stream = first.select_stream("select x from t order by x", chunk_size=1)
            if config.is_async:
                await stream.__anext__()
            else:
                next(stream)
        async with session_scope(config) as second:
            await resolve(second.begin())
            await resolve(second.execute("drop table t"))
            await resolve(second.commit())
    finally:
        await resolve(config.close_pool())


def open_transaction_connection(adapter: str, is_async: bool, events: "list[str]") -> MagicMock:
    """Return a fake server connection that reports an open transaction and logs its rollback.

    Args:
        adapter: Adapter package the connection stands in for.
        is_async: Whether the adapter driver awaits connection calls.
        events: Event log receiving ``"rollback"``.

    Returns:
        The configured connection mock.
    """
    connection = MagicMock()

    def record_rollback(*_args: Any, **_kwargs: Any) -> None:
        events.append("rollback")

    if adapter == "psycopg":
        connection.info.transaction_status = TRANSACTION_STATUS_INTRANS
    elif adapter == "oracledb":
        connection.transaction_in_progress = True
    if adapter == "asyncpg":
        connection.is_in_transaction.return_value = True

        async def execute(sql: str, *_args: Any, **_kwargs: Any) -> None:
            if sql == "ROLLBACK":
                record_rollback()

        connection.execute = execute
    elif is_async:
        connection.rollback = AsyncMock(side_effect=record_rollback)
    else:
        connection.rollback.side_effect = record_rollback
    return connection


CONTEXT_ROLLBACK_ADAPTERS = frozenset({"psycopg", "mssql_python"})
FAKE_FAMILIES = (
    ("psycopg", "PsycopgSyncSessionContext", "PsycopgSyncConfig", False),
    ("psycopg", "PsycopgAsyncSessionContext", "PsycopgAsyncConfig", True),
    ("asyncpg", "AsyncpgSessionContext", "AsyncpgConfig", True),
    ("oracledb", "OracleSyncSessionContext", "OracleSyncConfig", False),
    ("oracledb", "OracleAsyncSessionContext", "OracleAsyncConfig", True),
    ("mssql_python", "MssqlPythonSessionContext", "MssqlPythonConfig", False),
)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter", "context_name", "config_name", "is_async"),
    FAKE_FAMILIES,
    ids=[f"{adapter}-{context}" for adapter, context, _, _ in FAKE_FAMILIES],
)
async def test_server_context_orders_stream_close_rollback_release_close(
    monkeypatch: pytest.MonkeyPatch, adapter: str, context_name: str, config_name: str, is_async: bool
) -> None:
    typing_module = importlib.import_module(f"sqlspec.adapters.{adapter}._typing")
    config_module = importlib.import_module(f"sqlspec.adapters.{adapter}")
    context_cls = getattr(typing_module, context_name)
    config = getattr(config_module, config_name)()
    events: list[str] = []
    connection = open_transaction_connection(adapter, is_async, events)

    def acquire() -> Any:
        return connection

    async def acquire_async() -> Any:
        return connection

    def release(*_args: Any, **_kwargs: Any) -> None:
        events.append("release")

    async def release_async(*_args: Any, **_kwargs: Any) -> None:
        events.append("release")

    def prepare(driver: Any) -> Any:
        if not is_async:
            driver.begin()
        driver._register_open_stream(make_stream(events, is_async=is_async))
        return driver

    context = context_cls(
        acquire_connection=acquire_async if is_async else acquire,
        release_connection=release_async if is_async else release,
        statement_config=config.statement_config,
        driver_features={},
        prepare_driver=prepare,
    )
    if is_async:
        driver = await context.__aenter__()
        await context.__aexit__(None, None, None)
    else:
        driver = context.__enter__()
        context.__exit__(None, None, None)
    expected_rollback = ["rollback"] if adapter in CONTEXT_ROLLBACK_ADAPTERS else []
    assert events == ["stream-close", *expected_rollback, "release"]
    assert driver.connection is _CLOSED_CONNECTION
    with pytest.raises(ImproperConfigurationError):
        driver.connection.cursor()


SESSION_CONTEXTS = (
    ("adbc._typing", "AdbcSessionContext", False),
    ("aiomysql._typing", "AiomysqlSessionContext", True),
    ("aiosqlite._typing", "AiosqliteSessionContext", True),
    ("arrow_odbc._typing", "ArrowOdbcSessionContext", False),
    ("asyncmy.config", "AsyncmySessionContext", True),
    ("asyncpg._typing", "AsyncpgSessionContext", True),
    ("bigquery._typing", "BigQuerySessionContext", False),
    ("cockroach_asyncpg._typing", "CockroachAsyncpgSessionContext", True),
    ("cockroach_psycopg._typing", "CockroachPsycopgSyncSessionContext", False),
    ("cockroach_psycopg._typing", "CockroachPsycopgAsyncSessionContext", True),
    ("db2._typing", "Db2SyncSessionContext", False),
    ("db2._typing", "Db2AsyncSessionContext", True),
    ("duckdb._typing", "DuckDBSessionContext", False),
    ("mssql_python._typing", "MssqlPythonSessionContext", False),
    ("mysqlconnector._typing", "MysqlConnectorSyncSessionContext", False),
    ("mysqlconnector._typing", "MysqlConnectorAsyncSessionContext", True),
    ("oracledb._typing", "OracleSyncSessionContext", False),
    ("oracledb._typing", "OracleAsyncSessionContext", True),
    ("psqlpy._typing", "PsqlpySessionContext", True),
    ("psycopg._typing", "PsycopgSyncSessionContext", False),
    ("psycopg._typing", "PsycopgAsyncSessionContext", True),
    ("pymssql._typing", "PymssqlSessionContext", False),
    ("pymysql._typing", "PyMysqlSessionContext", False),
    ("snowflake._typing", "SnowflakeSessionContext", False),
    ("spanner._typing", "SpannerSyncSessionContext", False),
    ("spanner._typing", "SpannerAsyncSessionContext", True),
    ("sqlanywhere._typing", "SqlanywhereSessionContext", False),
    ("sqlite._typing", "SqliteSessionContext", False),
    ("sybase._typing", "SybaseSessionContext", False),
    ("teradata._typing", "TeradataSessionContext", False),
)


@pytest.mark.anyio
@pytest.mark.parametrize("abort", ABORTS, ids=lambda abort: abort.__name__)
@pytest.mark.parametrize(
    ("module_name", "context_name", "is_async"), SESSION_CONTEXTS, ids=[context for _, context, _ in SESSION_CONTEXTS]
)
async def test_session_context_releases_when_stream_close_is_interrupted(
    monkeypatch: pytest.MonkeyPatch, module_name: str, context_name: str, is_async: bool, abort: "type[BaseException]"
) -> None:
    try:
        typing_module = importlib.import_module(f"sqlspec.adapters.{module_name}")
    except ImportError as exc:
        pytest.skip(f"optional driver dependency is not installed: {exc}")
    context_cls = getattr(typing_module, context_name)
    events: list[str] = []
    released: list[dict[str, Any]] = []
    connection = MagicMock()

    def acquire() -> Any:
        return connection

    async def acquire_async() -> Any:
        return connection

    def release(*_args: Any, **kwargs: Any) -> None:
        events.append("release")
        released.append(kwargs)

    async def release_async(*_args: Any, **kwargs: Any) -> None:
        events.append("release")
        released.append(kwargs)

    def prepare(driver: Any) -> Any:
        monkeypatch.setattr(type(driver), "_connection_in_transaction", lambda self: False)
        driver._register_open_stream(make_stream(events, is_async=is_async, close_error=abort()))
        return driver

    context = context_cls(
        acquire_async if is_async else acquire, release_async if is_async else release, StatementConfig(), {}, prepare
    )
    drivers: list[Any] = []
    with pytest.raises(abort):
        if is_async:
            async with context as driver:
                drivers.append(driver)
        else:
            with context as driver:
                drivers.append(driver)
    assert events == ["stream-close", "release"]
    assert released[0]["exc_type"] is abort
    assert drivers[0].connection is _CLOSED_CONNECTION
