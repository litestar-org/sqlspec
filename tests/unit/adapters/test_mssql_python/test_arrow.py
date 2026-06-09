"""Unit tests for mssql_python Arrow and BulkCopy driver methods."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python import driver as driver_module
from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver, MssqlPythonDriver
from sqlspec.exceptions import UniqueViolationError

mssql_python = pytest.importorskip("mssql_python")


class ArrowCursor:
    """Cursor stub for Arrow and BulkCopy tests."""

    description = (("x",),)

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[tuple[str, Any]] = []
        self.bulkcopy_calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.rowcount = 0

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.executed.append((sql, parameters))

    def arrow(self, batch_size: int = 8192) -> Any:
        import pyarrow as pa

        return pa.table({"x": [1, 2, 3]})

    def arrow_reader(self, batch_size: int = 8192) -> Any:
        import pyarrow as pa

        table = pa.table({"x": [1, 2, 3]})
        return iter(table.to_batches(max_chunksize=batch_size))

    def bulkcopy(self, target_table: str, rows: Any, **kwargs: Any) -> dict[str, Any]:
        rows = list(rows)
        self.bulkcopy_calls.append((target_table, rows, kwargs))
        self.rowcount = len(rows)
        return {"rows_copied": len(rows), "batch_count": 1, "elapsed_time": 0.01}

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

    def arrow(self, batch_size: int = 8192) -> Any:
        raise mssql_python.Error("driver", "(2627)")

    def bulkcopy(self, target_table: str, rows: Any, **kwargs: Any) -> dict[str, Any]:
        raise mssql_python.Error("driver", "(2627)")


class ErrorArrowConnection(ArrowConnection):
    """Connection stub returning an erroring cursor."""

    def __init__(self) -> None:
        self.cursor_obj = ErrorArrowCursor()


def test_select_to_arrow_returns_arrow_result_from_native_cursor() -> None:
    """The sync driver should wrap cursor.arrow() in an ArrowResult."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))

    result = driver.select_to_arrow("SELECT 1 AS x")

    table = result.get_data()
    assert table.column_names == ["x"]
    assert table.num_rows == 3
    assert connection.cursor_obj.executed[0][0] == "SELECT 1 AS x"
    assert connection.cursor_obj.closed is True


def test_select_to_arrow_precompiles_prepared_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))
    original = MssqlPythonDriver._get_compiled_sql
    captured: list[bool] = []

    def get_compiled_sql(self: MssqlPythonDriver, statement: Any, config: Any) -> Any:
        captured.append(statement.is_processed)
        return original(self, statement, config)

    monkeypatch.setattr(MssqlPythonDriver, "_get_compiled_sql", get_compiled_sql)

    driver.select_to_arrow("SELECT 1 AS x")

    assert captured == [True]


def test_select_to_arrow_raises_mapped_driver_exception() -> None:
    """Native Arrow failures should use SQLSpec's deferred exception mapping."""
    connection = ErrorArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))

    with pytest.raises(UniqueViolationError):
        driver.select_to_arrow("SELECT 1 AS x")

    assert connection.cursor_obj.closed is True


def test_select_to_arrow_batches_returns_batches() -> None:
    """The sync driver should support SQLSpec's canonical batches return format."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))

    result = driver.select_to_arrow("SELECT 1 AS x", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert connection.cursor_obj.closed is True


def test_bulk_copy_forwards_options_to_cursor_bulkcopy() -> None:
    """BulkCopy options should be forwarded without SQLSpec-side rewriting."""
    connection = ArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))

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
    driver = MssqlPythonDriver(cast("Any", connection))

    result = driver.bulk_copy("dbo.target", [(1,)])

    _, _, options = connection.cursor_obj.bulkcopy_calls[0]
    assert result["rows_copied"] == 1
    assert options["batch_size"] == 0
    assert options["timeout"] == 30
    assert options["check_constraints"] is False


def test_bulk_copy_raises_mapped_driver_exception() -> None:
    """BulkCopy failures should not be swallowed by the deferred exception handler."""
    connection = ErrorArrowConnection()
    driver = MssqlPythonDriver(cast("Any", connection))

    with pytest.raises(UniqueViolationError):
        driver.bulk_copy("dbo.target", [(1,)])

    assert connection.cursor_obj.closed is True


@pytest.mark.anyio
async def test_async_select_to_arrow_offloads_cursor_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """The async driver should offload blocking Arrow cursor work."""
    calls: list[str] = []

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    monkeypatch.setattr(driver_module.asyncio, "to_thread", fake_to_thread)

    connection = ArrowConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))

    result = await driver.select_to_arrow("SELECT 1 AS x")

    assert result.get_data().num_rows == 3
    assert "_execute_cursor" in calls
    assert "arrow" in calls
    assert "close" in calls


@pytest.mark.anyio
async def test_async_select_to_arrow_precompiles_prepared_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = ArrowConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))
    original = MssqlPythonAsyncDriver._get_compiled_sql
    captured: list[bool] = []

    def get_compiled_sql(self: MssqlPythonAsyncDriver, statement: Any, config: Any) -> Any:
        captured.append(statement.is_processed)
        return original(self, statement, config)

    monkeypatch.setattr(MssqlPythonAsyncDriver, "_get_compiled_sql", get_compiled_sql)

    await driver.select_to_arrow("SELECT 1 AS x")

    assert captured == [True]


@pytest.mark.anyio
async def test_async_select_to_arrow_raises_mapped_driver_exception() -> None:
    """Async native Arrow failures should use SQLSpec's deferred exception mapping."""
    connection = ErrorArrowConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))

    with pytest.raises(UniqueViolationError):
        await driver.select_to_arrow("SELECT 1 AS x")

    assert connection.cursor_obj.closed is True


@pytest.mark.anyio
async def test_async_select_to_arrow_batches_offloads_table_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """The async driver should offload native Arrow reads for SQLSpec's batches format."""
    calls: list[str] = []

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    monkeypatch.setattr(driver_module.asyncio, "to_thread", fake_to_thread)

    connection = ArrowConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))

    result = await driver.select_to_arrow("SELECT 1 AS x", return_format="batches", batch_size=2)
    batches = result.get_data()

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert "_execute_cursor" in calls
    assert "arrow" in calls
    assert "close" in calls


@pytest.mark.anyio
async def test_async_bulk_copy_offloads_bulkcopy(monkeypatch: pytest.MonkeyPatch) -> None:
    """The async driver should offload cursor.bulkcopy()."""
    calls: list[str] = []

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    monkeypatch.setattr(driver_module.asyncio, "to_thread", fake_to_thread)

    connection = ArrowConnection()
    driver = MssqlPythonAsyncDriver(cast("Any", connection))

    result = await driver.bulk_copy("dbo.target", [(1,), (2,)], batch_size=500)

    assert result["rows_copied"] == 2
    assert "bulkcopy" in calls
    assert "close" in calls
