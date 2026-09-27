"""Unit tests for mssql_python Arrow and BulkCopy driver methods."""

from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

import pytest

from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.exceptions import UniqueViolationError

mssql_python = pytest.importorskip("mssql_python")

if TYPE_CHECKING:
    from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection


class ArrowCursor:
    """Cursor stub for Arrow and BulkCopy tests."""

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, object]] = []
        self.bulkcopy_calls: list[tuple[str, list[object], dict[str, object]]] = []
        self.description = (("id",), ("name",))
        self.fetchmany_sizes: list[int] = []
        self.rowcount = 0
        self.rows = [(1, "Ada"), (2, "Grace"), (3, "Linus")]

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))

    def fetchmany(self, size: int) -> list[tuple[int, str]]:
        self.fetchmany_sizes.append(size)
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def arrow(self, batch_size: int = 8192) -> object:
        import pyarrow as pa

        return pa.table({"x": [1, 2, 3]})

    def arrow_reader(self, batch_size: int = 8192) -> object:
        import pyarrow as pa

        table = pa.table({"x": [1, 2, 3]})
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=batch_size))

    def bulkcopy(self, target_table: str, rows: Iterable[object], **kwargs: object) -> dict[str, object]:
        copied_rows = list(rows)
        self.bulkcopy_calls.append((target_table, copied_rows, kwargs))
        self.rowcount = len(copied_rows)
        return {"rows_copied": len(copied_rows), "batch_count": 1, "elapsed_time": 0.01}

    def close(self) -> None:
        self.closed = True


class ArrowConnection:
    """Connection stub returning the same cursor instance."""

    def __init__(self) -> None:
        self.cursor_obj = ArrowCursor()

    def cursor(self) -> ArrowCursor:
        return self.cursor_obj

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class ErrorArrowCursor(ArrowCursor):
    """Cursor stub that raises a mssql-python error from native methods."""

    def arrow(self, batch_size: int = 8192) -> object:
        raise mssql_python.Error("driver", "(2627)")

    def bulkcopy(self, target_table: str, rows: Iterable[object], **kwargs: object) -> dict[str, object]:
        raise mssql_python.Error("driver", "(2627)")


class ErrorArrowConnection(ArrowConnection):
    """Connection stub returning an erroring cursor."""

    def __init__(self) -> None:
        self.cursor_obj = ErrorArrowCursor()


def test_select_to_arrow_returns_arrow_result_from_native_cursor() -> None:
    """The sync driver should wrap cursor.arrow() in an ArrowResult."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    result = driver.select_to_arrow("SELECT 1 AS x")

    table = result.get_data()
    assert table.column_names == ["x"]
    assert table.num_rows == 3
    assert connection.cursor_obj.executed[0][0] == "SELECT 1 AS x"
    assert connection.cursor_obj.closed is True


def test_select_to_arrow_raises_mapped_driver_exception() -> None:
    """Native Arrow failures should use SQLSpec's deferred exception mapping."""
    connection = ErrorArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    with pytest.raises(UniqueViolationError):
        driver.select_to_arrow("SELECT 1 AS x")

    assert connection.cursor_obj.closed is True


def test_select_to_arrow_batches_returns_batches() -> None:
    """The sync driver should support SQLSpec's canonical batches return format."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert connection.cursor_obj.closed is True


def test_select_to_arrow_reader_uses_arrow_reader_and_defers_cursor_close() -> None:
    """The sync driver should keep the cursor alive for lazy RecordBatchReader results."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="reader", batch_size=2)

    assert result.rows_affected == -1
    assert connection.cursor_obj.closed is False
    assert result.get_data().read_all().to_pydict() == {"x": [1, 2, 3]}
    assert connection.cursor_obj.closed is True


def test_select_stream_uses_fetchmany_chunks() -> None:
    """The sync driver should stream rows with cursor.fetchmany()."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    with driver.select_stream("SELECT id, name FROM dbo.users", native_only=True, chunk_size=2) as stream:
        rows = list(stream)

    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}, {"id": 3, "name": "Linus"}]
    assert connection.cursor_obj.executed == [("SELECT id, name FROM dbo.users", [])]
    assert connection.cursor_obj.fetchmany_sizes == [2, 2, 2]
    assert connection.cursor_obj.closed is True


def test_bulk_copy_forwards_options_to_cursor_bulkcopy() -> None:
    """BulkCopy options should be forwarded without SQLSpec-side rewriting."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    result = driver.bulk_copy(
        "dbo.target", [(1, "a"), (2, "b")], batch_size=1000, timeout=30, table_lock=True, keep_nulls=True
    )

    target_table, rows, options = connection.cursor_obj.bulkcopy_calls[0]
    assert result == {"rows_copied": 2, "batch_count": 1, "elapsed_time": 0.01}
    assert target_table == "dbo.target"
    assert rows == [(1, "a"), (2, "b")]
    assert options["batch_size"] == 1000
    assert options["timeout"] == 30
    assert options["table_lock"] is True
    assert options["keep_nulls"] is True
    assert connection.cursor_obj.closed is True


def test_bulk_copy_defaults_match_mssql_python_runtime() -> None:
    """BulkCopy defaults should match mssql-python 1.8 runtime defaults."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    result = driver.bulk_copy("dbo.target", [(1,)])

    _, _, options = connection.cursor_obj.bulkcopy_calls[0]
    assert result["rows_copied"] == 1
    assert options["batch_size"] == 0
    assert options["timeout"] == 30
    assert options["check_constraints"] is False


def test_bulk_copy_raises_mapped_driver_exception() -> None:
    """BulkCopy failures should not be swallowed by the deferred exception handler."""
    connection = ErrorArrowConnection()
    driver = MssqlPythonDriver(cast("MssqlPythonConnection", connection))

    with pytest.raises(UniqueViolationError):
        driver.bulk_copy("dbo.target", [(1,)])

    assert connection.cursor_obj.closed is True
