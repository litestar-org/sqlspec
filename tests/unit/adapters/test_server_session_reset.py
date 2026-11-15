"""Opt-in session reset on release for the Postgres and MySQL families, verified with fakes."""

import importlib
import inspect
from typing import Any, Self

import pytest

from sqlspec.adapters.aiomysql import AiomysqlConfig
from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool

pytestmark = pytest.mark.anyio

ASYNCPG_FAMILY = [
    (AsyncpgConfig, "sqlspec.adapters.asyncpg.config"),
    (CockroachAsyncpgConfig, "sqlspec.adapters.cockroach_asyncpg.config"),
]
PSYCOPG_FAMILY = [
    (PsycopgSyncConfig, "sqlspec.adapters.psycopg.config", "ConnectionPool", False),
    (PsycopgAsyncConfig, "sqlspec.adapters.psycopg.config", "AsyncConnectionPool", True),
    (CockroachPsycopgSyncConfig, "sqlspec.adapters.cockroach_psycopg.config", "ConnectionPool", False),
    (CockroachPsycopgAsyncConfig, "sqlspec.adapters.cockroach_psycopg.config", "AsyncConnectionPool", True),
]


class RecordingConnection:
    """Postgres-style connection double recording executed statements and autocommit changes."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.autocommit = False
        self.closed = False

    def execute(self, statement: str, *args: Any) -> Any:
        self.statements.append(statement)

    async def set_autocommit(self, value: bool) -> None:
        self.autocommit = value


class AsyncRecordingConnection(RecordingConnection):
    async def execute(self, statement: str, *args: Any) -> Any:
        self.statements.append(statement)


class CapturedPool:
    """Pool double capturing the keyword arguments a config passes to the native pool."""

    captured: dict[str, Any] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        type(self).captured = kwargs

    async def open(self) -> None:
        return None


async def resolve(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.mark.parametrize(("config_cls", "module_name", "pool_name", "is_async"), PSYCOPG_FAMILY)
async def test_psycopg_reset_runs_discard_all_then_user_callback(
    config_cls: Any, module_name: str, pool_name: str, is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, pool_name, CapturedPool)
    events: list[str] = []
    connection = AsyncRecordingConnection() if is_async else RecordingConnection()

    def user_reset(conn: Any) -> Any:
        events.append(f"user:{conn.statements[-1]}")

    async def async_user_reset(conn: Any) -> None:
        events.append(f"user:{conn.statements[-1]}")

    config = config_cls(
        connection_config={
            "conninfo": "postgresql://localhost/db",
            "reset": async_user_reset if is_async else user_reset,
        },
        driver_features={"reset_session_on_release": True},
    )
    await resolve(config._create_pool())
    await resolve(CapturedPool.captured["reset"](connection))
    assert connection.statements == ["DISCARD ALL"]
    assert connection.autocommit is False
    assert events == ["user:DISCARD ALL"]


@pytest.mark.parametrize(("config_cls", "module_name", "pool_name", "is_async"), PSYCOPG_FAMILY)
async def test_psycopg_reset_disabled_by_default(
    config_cls: Any, module_name: str, pool_name: str, is_async: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, pool_name, CapturedPool)
    config = config_cls(connection_config={"conninfo": "postgresql://localhost/db"})
    await resolve(config._create_pool())
    assert "reset" not in CapturedPool.captured


@pytest.mark.parametrize(("config_cls", "module_name"), ASYNCPG_FAMILY)
async def test_asyncpg_reset_keeps_statement_cache(
    config_cls: Any, module_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module(module_name)
    captured: dict[str, Any] = {}

    async def fake_create_pool(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(module, "asyncpg_create_pool", fake_create_pool)
    config = config_cls(
        connection_config={"dsn": "postgresql://localhost/db"}, driver_features={"reset_session_on_release": True}
    )
    await config._create_pool()
    connection = AsyncRecordingConnection()
    await captured["reset"](connection)
    joined = " ".join(connection.statements).upper()
    assert "DEALLOCATE" not in joined
    assert "RESET ALL" in joined
    assert "CLOSE ALL" in joined


async def test_asyncpg_reset_runs_user_callback_after_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("sqlspec.adapters.asyncpg.config")
    captured: dict[str, Any] = {}
    seen: list[int] = []

    async def fake_create_pool(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    async def user_reset(conn: Any) -> None:
        seen.append(len(conn.statements))

    monkeypatch.setattr(module, "asyncpg_create_pool", fake_create_pool)
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db", "reset": user_reset},
        driver_features={"reset_session_on_release": True},
    )
    await config._create_pool()
    connection = AsyncRecordingConnection()
    await captured["reset"](connection)
    assert seen == [1]


@pytest.mark.parametrize(("config_cls", "module_name"), ASYNCPG_FAMILY)
async def test_asyncpg_reset_disabled_by_default(
    config_cls: Any, module_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module(module_name)
    captured: dict[str, Any] = {}

    async def fake_create_pool(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(module, "asyncpg_create_pool", fake_create_pool)
    config = config_cls(connection_config={"dsn": "postgresql://localhost/db"})
    await config._create_pool()
    assert "reset" not in captured


class FakeMysqlConnection:
    """MySQL connection double; the command path is optional."""

    def __init__(self, *, command_path: bool = True, fail_reset: bool = False) -> None:
        self.events: list[str] = []
        self.closed = False
        self.autocommit_mode = True
        if command_path:
            self._execute_command = self._record_command
            self._read_ok_packet = self._record_ok
        self.fail_reset = fail_reset

    def _record_command(self, command: int, payload: bytes) -> None:
        if self.fail_reset:
            msg = "reset failed"
            raise RuntimeError(msg)
        self.events.append(f"command:{command}")

    def _record_ok(self) -> None:
        self.events.append("ok")

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        self.events.append("rollback")

    def ping(self, reconnect: bool = False) -> None:
        return None

    def get_autocommit(self) -> bool:
        return True

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, statement: str) -> None:
        self.events.append(statement)


def make_pymysql_pool(
    connections: "list[FakeMysqlConnection]", hook_calls: "list[Any]", **fake_options: Any
) -> PyMysqlConnectionPool:
    def factory() -> FakeMysqlConnection:
        connection = FakeMysqlConnection(**fake_options)
        connections.append(connection)
        return connection

    return PyMysqlConnectionPool(
        {},
        connection_factory=factory,
        read_only=True,
        on_connection_create=hook_calls.append,
        reset_session_on_release=True,
    )


def test_pymysql_reset_then_reinitializes() -> None:
    connections: list[FakeMysqlConnection] = []
    hook_calls: list[Any] = []
    pool = make_pymysql_pool(connections, hook_calls)
    connection = pool.acquire()
    pool.release(connection)
    read_only = "SET SESSION TRANSACTION READ ONLY"
    assert connection.events == [read_only, "command:31", "ok", read_only]
    assert len(hook_calls) == 2
    assert pool.acquire() is connection
    assert len(connections) == 1
    pool.close()


def test_pymysql_reset_falls_back_to_reconnect() -> None:
    connections: list[FakeMysqlConnection] = []
    pool = make_pymysql_pool(connections, [], command_path=False)
    first = pool.acquire()
    pool.release(first)
    assert first.closed
    second = pool.acquire()
    assert second is not first
    assert len(connections) == 2
    pool.close()


def test_pymysql_reset_failure_retires_connection() -> None:
    connections: list[FakeMysqlConnection] = []
    pool = make_pymysql_pool(connections, [], fail_reset=True)
    first = pool.acquire()
    pool.release(first)
    assert first.closed
    assert pool.acquire() is not first
    pool.close()


class AsyncFakeMysqlConnection(FakeMysqlConnection):
    def __init__(self, *, command_path: bool = True, fail_reset: bool = False) -> None:
        super().__init__(command_path=False, fail_reset=fail_reset)
        if command_path:
            self._execute_command = self._record_async_command
            self._read_ok_packet = self._record_async_ok

    async def _record_async_command(self, command: int, payload: bytes) -> None:
        self._record_command(command, payload)

    async def _record_async_ok(self) -> None:
        self._record_ok()


@pytest.mark.parametrize(
    ("config_cls", "module_name"),
    [(AiomysqlConfig, "sqlspec.adapters.aiomysql.config"), (AsyncmyConfig, "sqlspec.adapters.asyncmy.config")],
)
async def test_async_mysql_reset_then_reinitializes(config_cls: Any, module_name: str) -> None:
    module = importlib.import_module(module_name)
    hook_calls: list[Any] = []

    async def hook(connection: Any) -> None:
        hook_calls.append(connection)

    config = config_cls(driver_features={"on_connection_create": hook, "reset_session_on_release": True})
    connection = AsyncFakeMysqlConnection()
    await config._ensure_connection(connection)
    await module._reset_session(config, connection)
    assert connection.events == ["command:31", "ok"]
    assert not connection.closed
    assert hook_calls == [connection, connection]


@pytest.mark.parametrize(
    ("config_cls", "module_name"),
    [(AiomysqlConfig, "sqlspec.adapters.aiomysql.config"), (AsyncmyConfig, "sqlspec.adapters.asyncmy.config")],
)
@pytest.mark.parametrize("failure", ["no_command_path", "reset_error"])
async def test_async_mysql_reset_failure_closes_connection(config_cls: Any, module_name: str, failure: str) -> None:
    module = importlib.import_module(module_name)
    config = config_cls(driver_features={"reset_session_on_release": True})
    connection = AsyncFakeMysqlConnection(command_path=failure == "reset_error", fail_reset=failure == "reset_error")
    await module._reset_session(config, connection)
    assert connection.closed


class PsqlpyRecordingConnection:
    def __init__(self, fail_on: str | None = None) -> None:
        self.statements: list[str] = []
        self.fail_on = fail_on

    async def execute(self, statement: str, *args: Any) -> Any:
        self.statements.append(statement)
        if statement == self.fail_on:
            msg = "reset failed"
            raise RuntimeError(msg)


async def test_psqlpy_reset_runs_each_statement() -> None:
    from sqlspec.adapters.psqlpy.config import _reset_or_terminate

    connection = PsqlpyRecordingConnection()
    await _reset_or_terminate(connection)
    assert connection.statements[0] == "CLOSE ALL"
    assert not any("DEALLOCATE" in statement.upper() for statement in connection.statements)
    assert "DISCARD TEMP" in connection.statements


async def test_psqlpy_reset_failure_terminates_backend() -> None:
    from sqlspec.adapters.psqlpy.config import _reset_or_terminate

    connection = PsqlpyRecordingConnection(fail_on="RESET ALL")
    with pytest.raises(RuntimeError, match="reset failed"):
        await _reset_or_terminate(connection)
    assert connection.statements[-1] == "SELECT pg_terminate_backend(pg_backend_pid())"
