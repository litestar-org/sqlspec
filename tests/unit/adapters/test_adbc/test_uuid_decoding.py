"""Unit tests for PostgreSQL-family ADBC UUID result decoding."""

from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pyarrow as pa

from sqlspec.adapters.adbc.core import get_statement_config
from sqlspec.adapters.adbc.driver import AdbcDriver

if TYPE_CHECKING:
    from sqlspec.adapters.adbc._typing import AdbcConnection


UUID_VALUE = UUID("550e8400-e29b-41d4-a716-446655440000")
OTHER_UUID_VALUE = UUID("550e8400-e29b-41d4-a716-446655440001")


def test_opaque_uuid_columns_decode_scalar_list_and_null_values() -> None:
    table = _build_uuid_table()
    driver = _make_driver(table)

    rows = driver.select("SELECT identifier, identifiers, label FROM uuid_values")

    assert rows == [
        {"identifier": UUID_VALUE, "identifiers": [UUID_VALUE, None, OTHER_UUID_VALUE], "label": "first"},
        {"identifier": None, "identifiers": [], "label": "second"},
    ]


def test_disabled_arrow_extension_types_preserve_opaque_storage_bytes() -> None:
    table = _build_uuid_table()
    driver = _make_driver(table, enable_arrow_extension_types=False)

    rows = driver.select("SELECT identifier, identifiers, label FROM uuid_values")

    assert rows == table.to_pylist()


def test_plain_arrow_table_uses_whole_table_pylist_path() -> None:
    table = _TrackingTable(pa.table({"identifier": [1, 2], "label": ["first", "second"]}))
    driver = _make_driver(cast("pa.Table", table))

    rows = driver.select("SELECT identifier, label FROM plain_values")

    assert rows == [{"identifier": 1, "label": "first"}, {"identifier": 2, "label": "second"}]
    assert table.to_pylist_calls == 1


class _AdbcUuidResultCursor:
    def __init__(self, table: pa.Table) -> None:
        self.closed = False
        self.executed: list[tuple[str, object]] = []
        self._table = table

    def execute(self, sql: str, parameters: object = None) -> None:
        self.executed.append((sql, parameters))

    def fetch_arrow_table(self) -> pa.Table:
        return self._table

    def close(self) -> None:
        self.closed = True


class _AdbcUuidResultConnection:
    def __init__(self, table: pa.Table) -> None:
        self.cursor_obj = _AdbcUuidResultCursor(table)

    def adbc_get_info(self) -> dict[str, str]:
        return {"vendor_name": "postgres", "driver_name": "postgres"}

    def cursor(self) -> _AdbcUuidResultCursor:
        return self.cursor_obj


class _TrackingTable:
    def __init__(self, table: pa.Table) -> None:
        self._table = table
        self.to_pylist_calls = 0

    @property
    def column_names(self) -> list[str]:
        return self._table.column_names

    @property
    def schema(self) -> pa.Schema:
        return self._table.schema

    def column(self, name: str) -> pa.ChunkedArray:
        return self._table.column(name)

    def to_pylist(self) -> list[dict[str, Any]]:
        self.to_pylist_calls += 1
        return self._table.to_pylist()


def _make_driver(table: pa.Table, *, enable_arrow_extension_types: bool = True) -> AdbcDriver:
    connection = _AdbcUuidResultConnection(table)
    return AdbcDriver(
        cast("AdbcConnection", connection),
        statement_config=get_statement_config("postgres"),
        driver_features={"enable_arrow_extension_types": enable_arrow_extension_types},
        dialect="postgres",
    )


def _build_uuid_table() -> pa.Table:
    opaque_type = pa.opaque(pa.binary(), "uuid", "PostgreSQL")
    scalar_values = pa.ExtensionArray.from_storage(
        opaque_type,
        pa.array([UUID_VALUE.bytes, None], type=pa.binary()),
    )
    list_values = pa.ExtensionArray.from_storage(
        opaque_type,
        pa.array([UUID_VALUE.bytes, None, OTHER_UUID_VALUE.bytes], type=pa.binary()),
    )
    uuid_lists = pa.ListArray.from_arrays(pa.array([0, 3, 3], type=pa.int32()), list_values)
    return pa.Table.from_arrays(
        [scalar_values, uuid_lists, pa.array(["first", "second"])],
        names=["identifier", "identifiers", "label"],
    )
