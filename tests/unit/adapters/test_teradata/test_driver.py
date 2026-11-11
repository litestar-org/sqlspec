from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.teradata import TeradataDriver
from sqlspec.exceptions import TransactionError, UniqueViolationError


@pytest.mark.parametrize("finish", ["commit", "rollback"])
@pytest.mark.parametrize("baseline", [True, False])
def test_teradata_transaction_restores_autocommit(finish: str, baseline: bool) -> None:
    connection = MagicMock()
    connection.autocommit = baseline
    driver = TeradataDriver(connection)
    driver.begin()
    assert connection.autocommit is False
    with pytest.raises(TransactionError, match="already active"):
        driver.begin()
    getattr(driver, finish)()
    assert connection.autocommit is baseline
    getattr(connection, finish).assert_called_once_with()


@pytest.mark.parametrize("operation", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_teradata_savepoint_refusal(operation: str) -> None:
    connection = MagicMock()
    driver = TeradataDriver(connection)
    with pytest.raises(NotImplementedError, match="savepoints"):
        getattr(driver, operation)("point")
    connection.cursor.assert_not_called()


@pytest.mark.parametrize("rows", [[(1, Decimal("12.50"))], []])
def test_teradata_execute_preserves_metadata_and_closes_cursor(rows: list[tuple[int, Decimal]]) -> None:
    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.description = [("ID", int, None, None, None, None, False), ("total", Decimal, None, None, 12, 2, True)]
    cursor.fetchmany.side_effect = [rows, []] if rows else [[]]
    result = TeradataDriver(connection).execute("SELECT id, total FROM orders WHERE id = :id", {"id": 1})
    assert result.column_names == ["id", "total"]
    assert result.get_data() == ([{"id": 1, "total": Decimal("12.50")}] if rows else [])
    assert cursor.execute.call_args.args == ("SELECT id, total FROM orders WHERE id = ?", (1,))
    cursor.close.assert_called_once_with()


def test_teradata_execution_error_mapping_closes_cursor() -> None:
    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("[Teradata Database] [Error 2801] Duplicate key")
    with pytest.raises(UniqueViolationError, match="2801"):
        TeradataDriver(connection).execute("INSERT INTO orders VALUES (:id)", {"id": 1})
    cursor.close.assert_called_once_with()


def test_teradata_ddl_commits_ansi_boundary() -> None:
    connection = MagicMock()
    connection.autocommit = False
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 0
    TeradataDriver(connection).execute("CREATE MULTISET TABLE orders (id INTEGER)")
    connection.commit.assert_called_once_with()
    assert connection.autocommit is False


def test_teradata_transaction_band_requires_active_transaction() -> None:
    connection = MagicMock()
    connection.autocommit = True
    driver = TeradataDriver(connection)
    with pytest.raises(TransactionError, match="active transaction"):
        driver.set_query_band({"app": "sqlspec"}, transaction=True)
    connection.cursor.assert_not_called()
