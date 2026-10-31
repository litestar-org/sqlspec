"""Unit tests for the asyncmy driver transaction-state contract."""

from typing import Any, cast

import pytest

from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

pytest.importorskip("asyncmy", reason="asyncmy adapter requires the asyncmy package")


class _FakeConnection:
    def __init__(self, server_status: int) -> None:
        self.server_status = server_status


class _StatelessConnection:
    pass


@pytest.mark.parametrize(("server_status", "expected"), [(0, False), (1, True), (2, False), (3, True)])
def test_connection_in_transaction_reflects_driver_state(server_status: int, expected: bool) -> None:
    """_connection_in_transaction() must reflect the SERVER_STATUS_IN_TRANS bit on server_status."""
    driver = AsyncmyDriver(connection=cast("Any", _FakeConnection(server_status)))
    assert driver._connection_in_transaction() is expected


def test_connection_in_transaction_defaults_false_without_server_status() -> None:
    """_connection_in_transaction() returns False when connection has no server_status attribute."""
    driver = AsyncmyDriver(connection=cast("Any", _StatelessConnection()))
    assert driver._connection_in_transaction() is False


def test_mysql_connection_errno_takes_precedence_over_sqlstate_08() -> None:
    """MySQL errno 2006/2013 maps to ConnectionTimeoutError even when SQLSTATE starts with 08."""
    from sqlspec.adapters.asyncmy.core import create_mapped_exception
    from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError

    class _FakeExc(Exception):
        pass

    lost_exc = _FakeExc(2013, "08S01", "Lost connection to MySQL server during query")
    assert isinstance(create_mapped_exception(lost_exc), ConnectionTimeoutError)

    refused_exc = _FakeExc(2003, "08001", "Can't connect to MySQL server")
    assert isinstance(create_mapped_exception(refused_exc), DatabaseConnectionError)


def test_can_use_local_infile_records_rejects_binary_and_timedelta_values() -> None:
    """can_use_local_infile_records returns False when records contain binary or timedelta values."""
    from datetime import timedelta

    from sqlspec.adapters.asyncmy.core import can_use_local_infile_records

    assert can_use_local_infile_records(None, [(1, "clean"), (2, "ok")]) is True
    assert can_use_local_infile_records(None, [(1, b"raw_bytes")]) is False
    assert can_use_local_infile_records(None, [(1, timedelta(seconds=5))]) is False


async def test_provide_connection_rolls_back_dirty_transaction_on_release() -> None:
    """AsyncmyConfig.provide_connection rolls back dirty transaction state before releasing connection."""
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

    class _DirtyConn:
        def __init__(self) -> None:
            self.server_status = 1
            self.rolled_back = False

        async def rollback(self) -> None:
            self.rolled_back = True
            self.server_status = 0

    class _AcquireCtx:
        def __init__(self, conn: _DirtyConn, released: list[Any]) -> None:
            self._conn = conn
            self._released = released

        async def __aenter__(self) -> _DirtyConn:
            return self._conn

        async def __aexit__(self, *_: object) -> None:
            self._released.append(self._conn)

    class _FakePool:
        def __init__(self, conn: _DirtyConn) -> None:
            self._conn = conn
            self.released: list[Any] = []

        def acquire(self) -> _AcquireCtx:
            return _AcquireCtx(self._conn, self.released)

    conn = _DirtyConn()
    pool = _FakePool(conn)
    config = AsyncmyConfig(
        connection_config={"host": "localhost", "database": "test"}, connection_instance=cast("Any", pool)
    )
    async with config.provide_connection() as acquired:
        assert cast("object", acquired) is conn
    assert conn.rolled_back is True
    assert pool.released == [conn]


async def test_asyncmy_execute_cache_hit_invokes_collect_rows_on_clean_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cached query without unescaped literal percent delegates to fast path and invokes collect_rows."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.core import OperationProfile, ParameterProfile, ProcessedState
    from sqlspec.core.cache import CachedQuery

    cursor = MagicMock()
    cursor.description = [("id", 3, None, None, None, None, None)]
    cursor.fetchall = AsyncMock(return_value=[(1,)])
    cursor.execute = AsyncMock(return_value=None)

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)

    driver = AsyncmyDriver(connection=cast("Any", conn))
    collect_called: list[bool] = []
    original_collect_rows = AsyncmyDriver.collect_rows

    def _spy_collect_rows(*args: Any, **kwargs: Any) -> Any:
        collect_called.append(True)
        return original_collect_rows(*args, **kwargs)

    monkeypatch.setattr(AsyncmyDriver, "collect_rows", _spy_collect_rows)

    cached = CachedQuery(
        compiled_sql="SELECT 1",
        parameter_profile=ParameterProfile(),
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=0,
        processed_state=ProcessedState(compiled_sql="SELECT 1", execution_parameters=[], operation_type="SELECT"),
    )

    result = await driver._execute_cache_hit("SELECT 1", (), cached)
    assert bool(collect_called) is True
    assert result.data == [(1,)]


def test_asyncmy_mapped_exceptions_without_sqlstate() -> None:
    """Explicit MySQL errnos map correctly even when exception object lacks sqlstate."""
    from sqlspec.adapters.asyncmy.core import create_mapped_exception
    from sqlspec.exceptions import DatabaseConnectionError, PermissionDeniedError, SQLParsingError

    class _FakeErr(Exception):
        pass

    assert isinstance(create_mapped_exception(_FakeErr(1054, "Unknown column")), SQLParsingError)
    assert isinstance(create_mapped_exception(_FakeErr(1146, "Table doesn't exist")), SQLParsingError)
    assert isinstance(create_mapped_exception(_FakeErr(1040, "Too many connections")), DatabaseConnectionError)
    assert isinstance(
        create_mapped_exception(_FakeErr(1290, "The MySQL server is running with the --read-only option")),
        PermissionDeniedError,
    )
    assert isinstance(create_mapped_exception(_FakeErr(1836, "Running in read-only mode")), PermissionDeniedError)


async def test_asyncmy_dispatch_execute_uses_statement_statement_config() -> None:
    """dispatch_execute compiles using statement.statement_config."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec import SQL
    from sqlspec.core import StatementConfig

    custom_config = StatementConfig(enable_caching=False)
    statement = SQL("SELECT 1", statement_config=custom_config)
    cursor = MagicMock()
    cursor.description = [("1", 3, None, None, None, None, None)]
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[(1,)])
    cursor.rowcount = 1
    cursor.lastrowid = None

    driver = AsyncmyDriver(connection=MagicMock())
    result = await driver.dispatch_execute(cursor, statement)
    assert cursor.execute.called
    assert result.data_row_count == 1


async def test_asyncmy_dispatch_select_stream_uses_statement_config() -> None:
    """dispatch_select_stream compiles using statement.statement_config."""
    from unittest.mock import MagicMock

    from sqlspec import SQL
    from sqlspec.core import StatementConfig

    custom_config = StatementConfig(enable_caching=False)
    statement = SQL("SELECT 1", statement_config=custom_config)
    driver = AsyncmyDriver(connection=MagicMock())
    stream = driver.dispatch_select_stream(statement, chunk_size=10)
    assert stream is not None


async def test_asyncmy_dispatch_execute_script_delegates_to_super() -> None:
    """dispatch_execute_script emits script statement events via super()."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec import SQL

    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.description = None
    cursor.rowcount = 0
    cursor.lastrowid = None

    driver = AsyncmyDriver(connection=MagicMock())
    statement = SQL("SELECT 1; SELECT 2").as_script()
    result = await driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2


async def test_asyncmy_stream_source_close_closes_connection_on_cursor_close_error() -> None:
    """AsyncmyStreamSource.close closes the connection if cursor.close raises."""
    from unittest.mock import AsyncMock, MagicMock

    from sqlspec.adapters.asyncmy.core import AsyncmyStreamSource

    cursor = MagicMock()
    cursor.close = AsyncMock(side_effect=RuntimeError("close boom"))
    connection = MagicMock()
    driver = MagicMock()
    driver.connection = connection

    source = AsyncmyStreamSource(driver, "SELECT 1", (), 10, set())
    source._cursor = cursor

    await source.close()
    assert connection.close.called
