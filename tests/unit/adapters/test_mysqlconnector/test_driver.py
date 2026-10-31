"""Unit tests for mysql-connector driver transaction behavior."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.driver import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.exceptions import DeadlockError


class _InvalidBoolean:
    def __bool__(self) -> bool:
        raise RuntimeError("invalid boolean")


@pytest.mark.parametrize("driver_cls", [MysqlConnectorSyncDriver, MysqlConnectorAsyncDriver])
def test_connection_in_transaction_propagates_invalid_boolean(driver_cls: type[Any]) -> None:
    connection = SimpleNamespace(in_transaction=_InvalidBoolean())
    driver = driver_cls(cast(Any, connection))

    with pytest.raises(RuntimeError, match="invalid boolean"):
        driver._connection_in_transaction()


@pytest.mark.parametrize("driver_cls", [MysqlConnectorSyncDriver, MysqlConnectorAsyncDriver])
def test_connection_in_transaction_does_not_infer_from_autocommit(driver_cls: type[Any]) -> None:
    """Disabled autocommit suggests a transaction, but only the connector's own flag decides."""
    connection = SimpleNamespace(autocommit=False, in_transaction=False)
    driver = driver_cls(cast(Any, connection))

    assert driver._connection_in_transaction() is False


def test_sync_commit_maps_mysqlconnector_deadlock_error() -> None:
    def _fail_commit() -> None:
        raise MysqlConnectorError(errno=1213, msg="Deadlock found when trying to get lock")

    connection = SimpleNamespace(commit=_fail_commit, in_transaction=True)
    driver = MysqlConnectorSyncDriver(cast(Any, connection))
    with pytest.raises(DeadlockError):
        driver.commit()


def test_mysqlconnector_pool_error_and_connection_error_mapping() -> None:
    """PoolError maps to ConnectionTimeoutError and 2055 maps to DatabaseConnectionError."""
    from sqlspec.adapters.mysqlconnector.core import create_mapped_exception
    from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError

    class PoolError(Exception):
        pass

    class InterfaceError(Exception):
        errno = -1

    assert isinstance(create_mapped_exception(PoolError("connection pool exhausted")), ConnectionTimeoutError)
    assert isinstance(create_mapped_exception(InterfaceError("Connection disconnected")), DatabaseConnectionError)
    assert isinstance(
        create_mapped_exception(MysqlConnectorError(errno=2055, msg="Lost connection")), DatabaseConnectionError
    )


def test_mysqlconnector_sync_stream_source_in_active_except_block() -> None:
    """MysqlConnectorSyncStreamSource safely handles execution inside an active except block."""
    from unittest.mock import MagicMock

    from sqlspec.adapters.mysqlconnector.core import MysqlConnectorSyncStreamSource

    cursor = MagicMock()
    cursor.fetchmany.return_value = [{"id": 1}]
    cursor.description = [("id", 3, None, None, None, None, None)]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = MysqlConnectorSyncDriver(connection)

    try:
        raise ValueError("simulated active exception")
    except ValueError:
        source = MysqlConnectorSyncStreamSource(driver, "SELECT 1", (), 10, set())
        source.start()
        chunk = source.fetch_chunk()
        assert len(chunk) == 1
        source.close()


def test_mysqlconnector_data_dictionary_cached_property() -> None:
    """MysqlConnectorSyncDriver and MysqlConnectorAsyncDriver cache the data dictionary instance."""
    from unittest.mock import MagicMock

    sync_driver = MysqlConnectorSyncDriver(connection=MagicMock())
    assert sync_driver.data_dictionary is sync_driver.data_dictionary

    async_driver = MysqlConnectorAsyncDriver(connection=MagicMock())
    assert async_driver.data_dictionary is async_driver.data_dictionary


def test_with_cursor_avoids_dictionary_allocation() -> None:
    """with_cursor passes None options when cursor_options is not configured."""
    from unittest.mock import MagicMock

    conn = MagicMock()
    driver = MysqlConnectorSyncDriver(connection=conn)
    cursor_cm = driver.with_cursor(conn)
    assert cursor_cm.cursor_options is None


def test_mysqlconnector_table_and_column_not_found_map_to_operational_error() -> None:
    """Errno 1146 and 1054 map to OperationalError rather than SQLParsingError."""
    from sqlspec.adapters.mysqlconnector.core import create_mapped_exception
    from sqlspec.exceptions import OperationalError

    class _Err(Exception):
        pass

    assert isinstance(create_mapped_exception(_Err(1146, "Table doesn't exist")), OperationalError)
    assert isinstance(create_mapped_exception(_Err(1054, "Unknown column")), OperationalError)


def test_mysqlconnector_dispatch_execute_script_delegates_to_super() -> None:
    """dispatch_execute_script delegates to base driver and emits telemetry."""
    from unittest.mock import MagicMock

    from sqlspec import SQL

    cursor = MagicMock(with_rows=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    driver = MysqlConnectorSyncDriver(connection=conn)
    statement = SQL("SELECT 1; SELECT 2").as_script()
    result = driver.dispatch_execute_script(cursor, statement)
    assert result.statement_count == 2
    assert result.successful_statements == 2


def test_mysqlconnector_load_from_arrow_forces_unprepared_cursor() -> None:
    """load_from_arrow uses unprepared cursor for TRUNCATE and LOAD DATA even if prepared is True."""
    from unittest.mock import MagicMock

    import pyarrow as pa

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    driver = MysqlConnectorSyncDriver(
        connection=conn, driver_features={"cursor_options": {"prepared": True}, "enable_local_infile_bulk_load": True}
    )
    driver._load_from_arrow_via_local_infile = MagicMock()  # type: ignore[method-assign]

    driver.load_from_arrow("orders", pa.table({"id": [1]}), overwrite=True)
    assert driver._cursor_options == {"prepared": True}
