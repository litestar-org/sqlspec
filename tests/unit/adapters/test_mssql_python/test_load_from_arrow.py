"""mssql_python load_from_arrow wiring over the native Arrow bulk copy."""

from typing import Any, cast

import pyarrow as pa

from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver

_CAPS: dict[str, Any] = {
    "arrow_export_enabled": True,
    "arrow_import_enabled": True,
    "parquet_export_enabled": False,
    "parquet_import_enabled": False,
    "partition_strategies": [],
}


class _FakeCursor:
    def __init__(self) -> None:
        self.bulkcopy_calls: list[tuple[str, list[Any], dict[str, Any]]] = []
        self.arrow_calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.execute_calls: list[str] = []
        self.rowcount = 0

    def bulkcopy(self, target_table: str, rows: Any, **kwargs: Any) -> dict[str, Any]:
        materialized = list(rows)
        self.bulkcopy_calls.append((target_table, materialized, kwargs))
        return {"rows_copied": len(materialized)}

    def bulkcopy_arrow(self, table_name: str, source: Any, **kwargs: Any) -> dict[str, Any]:
        self.arrow_calls.append((table_name, source, kwargs))
        return {"rows_copied": source.num_rows}

    def execute(self, sql: str, *_args: Any) -> None:
        self.execute_calls.append(sql)

    def close(self) -> None:
        pass


class _FakeConnection:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def test_sync_load_from_arrow_uses_the_native_arrow_bulk_copy() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    job = driver.load_from_arrow("orders", table)

    assert job.telemetry["rows_processed"] == 2
    target, source, kwargs = conn._cursor.arrow_calls[0]
    assert target == "orders"
    assert source is table
    assert kwargs["column_mappings"] == ["id", "name"]
    assert conn._cursor.bulkcopy_calls == []
    assert conn._cursor.execute_calls == []


def test_sync_load_from_arrow_skips_an_empty_table() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow("orders", pa.table({"id": pa.array([], type=pa.int64())}))

    assert conn._cursor.arrow_calls == []
    assert conn._cursor.bulkcopy_calls == []


def test_sync_load_from_arrow_overwrite_truncates_first() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow("dbo.orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls == ["TRUNCATE TABLE [dbo].[orders]"]
    assert conn._cursor.arrow_calls


def test_sync_load_from_arrow_overwrite_preserves_quoted_dots() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    driver.load_from_arrow('"dbo.schema"."orders.table"', pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls == ["TRUNCATE TABLE [dbo.schema].[orders.table]"]
    assert conn._cursor.arrow_calls


def test_sync_load_from_arrow_overwrite_falls_back_to_delete_on_fk_reference() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})

    class FkError(Exception):
        number = 4712

    original_execute = conn._cursor.execute

    def execute_with_fk(sql: str, *args: Any) -> None:
        original_execute(sql, *args)
        if sql.startswith("TRUNCATE"):
            raise FkError("Cannot truncate table referenced by foreign key")

    conn._cursor.execute = cast("Any", execute_with_fk)
    driver.load_from_arrow("dbo.orders", pa.table({"id": [1]}), overwrite=True)

    assert conn._cursor.execute_calls == ["TRUNCATE TABLE [dbo].[orders]", "DELETE FROM [dbo].[orders]"]
    assert conn._cursor.arrow_calls


def test_sync_load_from_arrow_forwards_bulk_copy_options() -> None:
    conn = _FakeConnection()
    driver = MssqlPythonDriver(cast("Any", conn), driver_features={"storage_capabilities": _CAPS})
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    job = driver.load_from_arrow(
        "orders", table, batch_size=500, check_constraints=True, fire_triggers=True, keep_nulls=True, table_lock=True
    )

    assert job.telemetry["rows_processed"] == 2
    _, _, kwargs = conn._cursor.arrow_calls[0]
    assert kwargs["batch_size"] == 500
    assert kwargs["check_constraints"] is True
    assert kwargs["fire_triggers"] is True
    assert kwargs["keep_nulls"] is True
    assert kwargs["table_lock"] is True
