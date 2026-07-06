# pyright: reportArgumentType=false, reportIncompatibleMethodOverride=false
"""Unit tests for DuckDB Arrow paths."""

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from sqlspec.adapters.duckdb import default_statement_config
from sqlspec.adapters.duckdb.driver import DuckDBDriver

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._typing import DuckDBConnection


_STORAGE_CAPABILITIES = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "requires_staging_for_load": False,
    "staging_protocols": [],
    "partition_strategies": [],
}


class _ArrowCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []
        self.reader_calls: list[int | None] = []

    def execute(self, sql: str, parameters: object) -> None:
        self.executed.append((sql, parameters))

    def arrow(self) -> object:
        msg = "select_to_arrow should use DuckDB cursor.to_arrow_table()"
        raise AssertionError(msg)

    def to_arrow_table(self) -> pa.Table:
        return pa.table({"id": [1]})

    def to_arrow_reader(self, batch_size: int | None = None) -> pa.RecordBatchReader:
        self.reader_calls.append(batch_size)
        table = pa.table({"id": [1, 2, 3]})
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=batch_size))


def _connection() -> "DuckDBConnection":
    return cast("DuckDBConnection", object())


class _ArrowDriver(DuckDBDriver):
    def __init__(self, cursor: _ArrowCursor) -> None:
        self.cursor = cursor
        super().__init__(connection=_connection(), statement_config=default_statement_config)

    def _compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return ("SELECT 1 AS id", [])

    def with_cursor(self, _connection: Any) -> Any:

        @contextmanager
        def manager() -> Any:
            yield self.cursor

        return manager()


def test_arrow_select_to_arrow_uses_to_arrow_table() -> None:
    cursor = _ArrowCursor()
    driver = _ArrowDriver(cursor)
    result = driver.select_to_arrow("SELECT 1 AS id")
    assert result.get_data().to_pydict() == {"id": [1]}
    assert cursor.executed == [("SELECT 1 AS id", ())]


def test_arrow_select_to_arrow_reader_uses_to_arrow_reader() -> None:
    cursor = _ArrowCursor()
    driver = _ArrowDriver(cursor)

    result = driver.select_to_arrow("SELECT 1 AS id", return_format="reader", batch_size=2)

    assert result.rows_affected == -1
    assert result.get_data().read_all().to_pydict() == {"id": [1, 2, 3]}
    assert cursor.reader_calls == [2]


def test_load_from_arrow_registers_record_batch_reader_directly() -> None:
    import duckdb

    connection = duckdb.connect(":memory:")
    driver = DuckDBDriver(connection=connection, driver_features={"storage_capabilities": _STORAGE_CAPABILITIES})
    connection.execute("CREATE TABLE target (id INTEGER)")
    table = pa.table({"id": [1, 2, 3]})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    job = driver.load_from_arrow("target", reader)

    assert connection.execute("SELECT id FROM target ORDER BY id").fetchall() == [(1,), (2,), (3,)]
    assert job.telemetry["rows_processed"] == 3
