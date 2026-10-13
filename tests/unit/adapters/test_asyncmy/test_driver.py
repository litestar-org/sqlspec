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
