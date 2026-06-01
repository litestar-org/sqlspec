"""Unit tests for OracleSyncDriver.dispatch_execute force-select parity."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("oracledb")

from sqlspec.adapters.oracledb.driver import OracleSyncDriver


def _make_sync_driver() -> OracleSyncDriver:
    connection = MagicMock()
    connection.in_transaction = False
    return OracleSyncDriver(connection=connection)


def test_sync_dispatch_execute_force_select_fallback() -> None:
    """The sync driver must fetch rows when cursor metadata proves rows exist."""
    driver = _make_sync_driver()
    statement = MagicMock()
    statement.returns_rows.return_value = False
    statement.operation_type = "COMMAND"

    cursor = MagicMock()
    cursor.description = [("id", None, None, None, None, None, None)]
    cursor.fetchall.return_value = [(42,)]
    del cursor.statement_type

    with (
        patch.object(OracleSyncDriver, "_get_compiled_sql", return_value=("SELECT 1 FROM DUAL", {})),
        patch("sqlspec.adapters.oracledb.driver.coerce_large_parameters_sync", return_value={}),
        patch.object(OracleSyncDriver, "_resolve_row_metadata", return_value=(["id"], False)),
        patch("sqlspec.adapters.oracledb.driver.collect_sync_rows", return_value=([(42,)], ["id"])),
    ):
        result = driver.dispatch_execute(cursor, statement)

    assert result.is_select_result is True
    assert result.data_row_count == 1
    cursor.fetchall.assert_called_once()


def test_sync_dispatch_execute_no_force_select_when_returns_rows_false_and_no_description() -> None:
    """The sync driver must keep DML behavior when no row metadata exists."""
    driver = _make_sync_driver()
    statement = MagicMock()
    statement.returns_rows.return_value = False
    statement.operation_type = "COMMAND"

    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 3
    del cursor.statement_type

    with (
        patch.object(OracleSyncDriver, "_get_compiled_sql", return_value=("DELETE FROM t", {})),
        patch("sqlspec.adapters.oracledb.driver.coerce_large_parameters_sync", return_value={}),
        patch("sqlspec.adapters.oracledb.driver.resolve_rowcount", return_value=3),
    ):
        result = driver.dispatch_execute(cursor, statement)

    assert result.is_select_result is False
    assert result.rowcount_override == 3
    cursor.fetchall.assert_not_called()


def test_sync_and_async_dispatch_execute_both_call_should_force_select() -> None:
    """Both Oracle dispatch implementations should use the force-select fallback."""
    import inspect

    from sqlspec.adapters.oracledb.driver import OracleAsyncDriver

    sync_src = inspect.getsource(OracleSyncDriver.dispatch_execute)
    async_src = inspect.getsource(OracleAsyncDriver.dispatch_execute)

    assert "_should_force_select" in sync_src
    assert "_should_force_select" in async_src
    assert "is_select_like" in sync_src
