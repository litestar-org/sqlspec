"""Unit tests for the mssql_python async wrapper."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig, MssqlPythonAsyncDriver


class DummyCursor:
    """Minimal cursor for async driver dispatch tests."""

    description = (("value",),)
    rowcount = 1

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.executed.append((sql, parameters))

    def fetchall(self) -> list[tuple[int]]:
        return [(1,)]

    def close(self) -> None:
        self.closed = True


class DummyConnection:
    """Minimal connection for async config and driver tests."""

    def __init__(self) -> None:
        self.cursor_obj = DummyCursor()
        self.closed = False

    def cursor(self) -> DummyCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


@pytest.mark.anyio
async def test_async_config_provide_session_yields_async_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    """MssqlPythonAsyncConfig should provide an async driver and release connections."""
    connection = DummyConnection()

    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: connection
    )

    config = MssqlPythonAsyncConfig(connection_config={"server": "localhost"})

    async with config.provide_session() as session:
        assert isinstance(session, MssqlPythonAsyncDriver)

    assert connection.closed is True


@pytest.mark.anyio
async def test_async_config_connection_hook_runs_for_session_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async session acquisition should preserve the on_connection_create hook."""
    connection = DummyConnection()
    seen: list[DummyConnection] = []

    monkeypatch.setattr("sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.pooling", lambda **_: None)
    monkeypatch.setattr(
        "sqlspec.adapters.mssql_python.config.MSSQL_PYTHON_MODULE.connect", lambda *_args, **_kwargs: connection
    )

    config = MssqlPythonAsyncConfig(
        connection_config={"server": "localhost"}, driver_features={"on_connection_create": seen.append}
    )

    async with config.provide_session():
        pass

    assert seen == [connection]


@pytest.mark.anyio
async def test_async_driver_uses_to_thread_for_cursor_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Async driver dispatch should offload blocking cursor work through asyncio.to_thread."""
    from sqlspec.adapters.mssql_python import driver as driver_module

    calls: list[str] = []

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    monkeypatch.setattr(driver_module.asyncio, "to_thread", fake_to_thread)

    connection = DummyConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))

    result = await driver.select_value("SELECT 1")

    assert result == 1
    assert "_execute_cursor" in calls
    assert "fetchall" in calls
