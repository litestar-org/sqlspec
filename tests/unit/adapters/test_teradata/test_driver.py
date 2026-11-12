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


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_teradata_transaction_failure_remains_recoverable(finish: str) -> None:
    connection = MagicMock()
    connection.autocommit = True
    driver = TeradataDriver(connection)
    driver.begin()
    error = RuntimeError("temporary failure")
    getattr(connection, finish).side_effect = [error, None]
    with pytest.raises(TransactionError, match=finish) as raised:
        getattr(driver, finish)()
    assert raised.value.__cause__ is error
    getattr(driver, finish)()
    assert connection.autocommit is True


def test_teradata_begin_failure_is_reported_as_transaction_error() -> None:
    class Connection:
        @property
        def autocommit(self) -> bool:
            return True

        @autocommit.setter
        def autocommit(self, value: bool) -> None:
            msg = "unsupported setting"
            raise RuntimeError(msg)

    error_driver = TeradataDriver(Connection())
    with pytest.raises(TransactionError, match="begin"):
        error_driver.begin()


@pytest.mark.parametrize("found", [True, False])
def test_teradata_has_schema_uses_catalog_probe(found: bool) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = (1,) if found else None
    assert TeradataDriver(connection).has_schema("analytics") is found
    assert cursor.execute.call_args.args == (
        "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.DatabasesV WHERE UPPER(TRIM(DatabaseName)) = UPPER(TRIM(?))",
        ("analytics",),
    )
    cursor.close.assert_called_once_with()


def test_teradata_schema_probe_maps_errors_during_exception_handling() -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("[Error 2801] duplicate")
    try:
        raise ValueError("outer failure")
    except ValueError:
        with pytest.raises(UniqueViolationError):
            TeradataDriver(connection).has_schema("analytics")
    cursor.close.assert_called_once_with()


def test_teradata_query_band_session_and_transaction_boundaries() -> None:
    connection = MagicMock()
    connection.autocommit = False
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 0
    driver = TeradataDriver(connection)
    driver.set_query_band({"app": "etl"})
    driver.set_query_band({"request": "R1"}, transaction=True)
    assert cursor.execute.call_args_list[0].args == ("SET QUERY_BAND = 'app=etl;' FOR SESSION", ())
    assert cursor.execute.call_args_list[1].args == ("SET QUERY_BAND = 'request=R1;' FOR TRANSACTION", ())
    connection.commit.assert_called_once_with()


def test_teradata_session_release_closes_stream_and_rolls_back_open_work() -> None:
    from sqlspec.adapters.teradata import TeradataSessionContext
    from sqlspec.adapters.teradata.core import default_statement_config

    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.description = [("ID", int, None, None, None, None, False)]
    cursor.fetchmany.return_value = [(1,)]
    released = []
    with TeradataSessionContext(
        lambda: connection,
        lambda conn, **kwargs: released.append(conn),
        default_statement_config,
        {},
        lambda session: session,
    ) as driver:
        driver.begin()
        stream = driver.select_stream("SELECT id FROM orders")
        assert next(iter(stream)) == {"id": 1}
    cursor.close.assert_called_once_with()
    connection.rollback.assert_called_once_with()
    assert released == [connection]
    assert connection.autocommit is True


def test_teradata_cached_select_preserves_fastexport_casing_and_row_coercion() -> None:
    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.description = [("ID", int, None, None, None, None, False), ("PAYLOAD", bytes, None, None, None, None, True)]
    cursor.fetchmany.side_effect = [[(1, bytearray(b"first"))], [], [(2, memoryview(b"second"))], []]
    driver = TeradataDriver(
        connection, driver_features={"enable_fastexport": True, "enable_lowercase_column_names": False}
    )
    first = driver.execute("SELECT id, payload FROM orders")
    cursor.description = [("ID", int, None, None, None, None, False), ("PAYLOAD", bytes, None, None, None, None, True)]
    second = driver.execute("SELECT id, payload FROM orders")
    assert first.column_names == ["ID", "PAYLOAD"]
    assert second.column_names == ["ID", "PAYLOAD"]
    assert first.get_data() == [{"ID": 1, "PAYLOAD": b"first"}]
    assert second.get_data() == [{"ID": 2, "PAYLOAD": b"second"}]
    assert [c.args for c in cursor.execute.call_args_list] == [
        ("{fn teradata_try_fastexport} SELECT id, payload FROM orders", ()),
        ("{fn teradata_try_fastexport} SELECT id, payload FROM orders", ()),
    ]


def test_teradata_cached_ddl_and_dml_commit_rules_and_bytearray_params() -> None:
    connection = MagicMock()
    connection.autocommit = False
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 1
    driver = TeradataDriver(connection)
    driver.execute("CREATE MULTISET TABLE orders (id INTEGER)")
    driver.execute("CREATE MULTISET TABLE orders (id INTEGER)")
    assert connection.commit.call_count == 2

    driver.execute("INSERT INTO blobs VALUES (?)", (bytearray(b"raw"),))
    driver.execute("INSERT INTO blobs VALUES (?)", (memoryview(b"raw2"),))
    assert cursor.execute.call_args_list[-2].args == ("INSERT INTO blobs VALUES (?)", (b"raw",))
    assert cursor.execute.call_args_list[-1].args == ("INSERT INTO blobs VALUES (?)", (b"raw2",))
    assert connection.commit.call_count == 2


def test_teradata_collect_rows_stream_casing_and_nested_exception_paths() -> None:
    from sqlspec.adapters.teradata.driver import TeradataExceptionHandler, TeradataStreamSource

    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.description = [
        ("ORDER_ID", int, None, None, None, None, False),
        ("RAW", bytes, None, None, None, None, True),
    ]
    driver = TeradataDriver(connection, driver_features={"enable_lowercase_column_names": False})

    rows, cols, count = driver.collect_rows(cursor, [(7, bytearray(b"bin"))])
    assert (rows, cols, count) == ([(7, b"bin")], ["ORDER_ID", "RAW"], 1)
    assert driver.resolve_rowcount(cursor) == 0
    assert driver.data_dictionary is driver.data_dictionary

    from sqlspec.core import SQL

    assert driver.dispatch_select_stream(SQL("DELETE FROM orders"), 10) is None

    cursor.fetchmany.side_effect = [[(9, bytearray(b"chunk"))], []]
    source = TeradataStreamSource(driver, "SELECT order_id, raw FROM orders", (1,), 10)
    assert source.fetch_chunk() == []
    try:
        raise ValueError("active outer exception")
    except ValueError:
        source.start()
        source._column_names = None
        assert source.fetch_chunk() == [{"ORDER_ID": 9, "RAW": b"chunk"}]
        assert source.fetch_chunk() == []
    source.close()

    handler = TeradataExceptionHandler()
    assert handler._handle_exception(None, RuntimeError("ignored")) is False
    assert handler._handle_exception(UniqueViolationError, UniqueViolationError("already mapped")) is False
