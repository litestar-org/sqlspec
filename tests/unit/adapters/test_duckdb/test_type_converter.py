# pyright: reportArgumentType=false, reportIncompatibleMethodOverride=false
"""Unit tests for DuckDB type converter UUID conversion control."""

import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from sqlspec.adapters.duckdb import default_statement_config
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.core import BaseTypeConverter

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


def test_uuid_conversion_enabled_by_default() -> None:
    """Test that UUID conversion is enabled by default."""
    converter = DuckDBOutputConverter()
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = converter.convert_if_detected(uuid_str)
    assert isinstance(result, uuid.UUID)
    assert str(result) == uuid_str


def test_uuid_conversion_can_be_disabled() -> None:
    """Test that UUID conversion can be disabled."""
    converter = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = converter.convert_if_detected(uuid_str)
    assert isinstance(result, str)
    assert result == uuid_str


def test_uuid_objects_pass_through_regardless_of_flag() -> None:
    """Test that UUID objects pass through unchanged regardless of conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_obj = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    result_enabled = converter_enabled.convert_if_detected(uuid_obj)
    result_disabled = converter_disabled.convert_if_detected(uuid_obj)
    assert result_enabled is uuid_obj
    assert result_disabled is uuid_obj


def test_duckdb_output_converter_inherits_base_type_converter_directly() -> None:
    """DuckDB output conversion should use the shared base converter directly."""
    assert DuckDBOutputConverter.__mro__[1] is BaseTypeConverter


def test_legacy_duckdb_output_converter_helpers_are_gone() -> None:
    """DuckDB output conversion should no longer expose cached-path helpers."""
    assert not hasattr(DuckDBOutputConverter, "convert")
    assert not hasattr(DuckDBOutputConverter, "handle_uuid")
    assert not hasattr(DuckDBOutputConverter, "format_datetime")


def test_convert_if_detected_respects_uuid_flag() -> None:
    """Test that convert_if_detected respects the UUID conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result_enabled = converter_enabled.convert_if_detected(uuid_str)
    result_disabled = converter_disabled.convert_if_detected(uuid_str)
    assert isinstance(result_enabled, uuid.UUID)
    assert isinstance(result_disabled, str)
    assert result_disabled == uuid_str


def test_non_uuid_strings_unaffected_by_flag() -> None:
    """Test that non-UUID strings are unaffected by the conversion flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    regular_str = "just a regular string"
    result_enabled = converter_enabled.convert_if_detected(regular_str)
    result_disabled = converter_disabled.convert_if_detected(regular_str)
    assert result_enabled == regular_str
    assert result_disabled == regular_str


def test_datetime_conversion_unaffected_by_uuid_flag() -> None:
    """Test that datetime conversion works regardless of UUID flag."""
    converter_enabled = DuckDBOutputConverter(enable_uuid_conversion=True)
    converter_disabled = DuckDBOutputConverter(enable_uuid_conversion=False)
    datetime_str = "2024-01-15T10:30:00"
    result_enabled = converter_enabled.convert_if_detected(datetime_str)
    result_disabled = converter_disabled.convert_if_detected(datetime_str)
    from datetime import datetime

    assert isinstance(result_enabled, datetime)
    assert isinstance(result_disabled, datetime)


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
