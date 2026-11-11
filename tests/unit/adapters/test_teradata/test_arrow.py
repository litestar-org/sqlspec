"""Native Teradata streaming and Arrow behavior at the vendor connection seam."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, call

import pyarrow as pa
import pytest

from sqlspec.adapters.teradata import TeradataDriver
from sqlspec.exceptions import ImproperConfigurationError, UniqueViolationError


def _connection(chunks: list[list[tuple[Any, ...]]]) -> MagicMock:
    connection = MagicMock()
    connection.autocommit = True
    cursor = connection.cursor.return_value
    cursor.description = [("ID", int, None, None, None, None, False), ("TOTAL", Decimal, None, None, 12, 2, True)]
    cursor.fetchmany.side_effect = chunks
    cursor.rowcount = -1
    return connection


def test_select_stream_chunking_and_early_close() -> None:
    connection = _connection([[(1, Decimal("12.50")), (2, None)], [(3, Decimal("2.00"))], []])
    driver = TeradataDriver(connection)
    with driver.select_stream("SELECT id, total FROM orders WHERE id > :id", {"id": 0}, chunk_size=2) as stream:
        assert next(iter(stream)) == {"id": 1, "total": Decimal("12.50")}
    connection.cursor.return_value.close.assert_called_once_with()
    connection.cursor.return_value.fetchmany.assert_called_once_with(2)
    assert connection.cursor.return_value.execute.call_args.args == ("SELECT id, total FROM orders WHERE id > ?", (0,))


def test_select_stream_exhaustion_preserves_values() -> None:
    connection = _connection([[(1, Decimal("12.50")), (2, None)], [(3, Decimal("2.00"))], []])
    with TeradataDriver(connection).select_stream("SELECT id, total FROM orders", chunk_size=2) as stream:
        rows = list(stream)
    assert rows == [{"id": 1, "total": Decimal("12.50")}, {"id": 2, "total": None}, {"id": 3, "total": Decimal("2.00")}]
    assert connection.cursor.return_value.fetchmany.call_args_list == [call(2), call(2), call(2)]
    connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("phase", ["execute", "fetchmany"])
def test_stream_error_maps_and_closes_cursor(phase: str) -> None:
    connection = _connection([])
    getattr(connection.cursor.return_value, phase).side_effect = RuntimeError("[Teradata] [Error 2801] duplicate")
    with pytest.raises(UniqueViolationError, match="2801"):
        with TeradataDriver(connection).select_stream("SELECT id, total FROM orders", chunk_size=2) as stream:
            list(stream)
    connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("empty", [True, False])
def test_select_to_arrow_preserves_decimal_schema_and_nulls(empty: bool) -> None:
    chunks = [[]] if empty else [[(1, Decimal("12.50")), (2, None)], []]
    connection = _connection(chunks)
    result = TeradataDriver(connection).select_to_arrow("SELECT id, total FROM orders", batch_size=2)
    table = result.get_data()
    assert table.schema == pa.schema([("id", pa.int64()), ("total", pa.decimal128(12, 2))])
    assert table.to_pydict() == (
        {"id": [], "total": []} if empty else {"id": [1, 2], "total": [Decimal("12.50"), None]}
    )
    connection.cursor.return_value.close.assert_called_once_with()


def test_select_to_arrow_reader_owns_cursor_until_consumed() -> None:
    connection = _connection([[(1, Decimal("1.25"))], [(2, None)], []])
    result = TeradataDriver(connection).select_to_arrow(
        "SELECT id, total FROM orders", batch_size=1, return_format="reader"
    )
    connection.cursor.return_value.close.assert_not_called()
    reader = result.get_data()
    assert reader.read_all().to_pydict() == {"id": [1, 2], "total": [Decimal("1.25"), None]}
    connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("options", [{"native_only": True}, {"batch_size": 0}, {"batch_size": True}])
def test_arrow_export_rejects_invalid_options_before_cursor(options: dict[str, Any]) -> None:
    connection = _connection([])
    with pytest.raises(ImproperConfigurationError):
        TeradataDriver(connection).select_to_arrow("SELECT id FROM orders", **options)
    connection.cursor.assert_not_called()


@pytest.mark.parametrize("active", [True, False])
def test_fastexport_is_opt_in_and_disabled_in_transaction(active: bool) -> None:
    connection = _connection([[(1, Decimal("1.25"))], []])
    driver = TeradataDriver(connection, driver_features={"enable_fastexport": True})
    if active:
        driver.begin()
    driver.select_to_arrow("SELECT id, total FROM orders", batch_size=2)
    executed = connection.cursor.return_value.execute.call_args.args[0]
    assert executed == (
        "SELECT id, total FROM orders" if active else "{fn teradata_try_fastexport} SELECT id, total FROM orders"
    )
    connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("empty", [True, False])
def test_load_from_arrow_uses_positional_rows_and_closes_cursor(empty: bool) -> None:
    connection = _connection([])
    table = (
        pa.table({"id": pa.array([], type=pa.int64()), "enabled": pa.array([], type=pa.bool_())})
        if empty
        else pa.table({"id": [1, 2], "enabled": [True, False]})
    )
    job = TeradataDriver(
        connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
    ).load_from_arrow("orders", table)
    assert job.status == "completed"
    assert job.telemetry["rows_processed"] == (0 if empty else 2)
    if empty:
        connection.cursor.assert_not_called()
    else:
        assert connection.cursor.return_value.executemany.call_args.args == (
            'INSERT INTO "orders" ("id", "enabled") VALUES (?, ?)',
            [(1, 1), (2, 0)],
        )
        connection.cursor.return_value.close.assert_called_once_with()


def test_load_from_arrow_maps_error_and_closes_cursor() -> None:
    connection = _connection([])
    connection.cursor.return_value.executemany.side_effect = RuntimeError("[Teradata] [Error 2801] duplicate")
    with pytest.raises(UniqueViolationError, match="2801"):
        TeradataDriver(
            connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
        ).load_from_arrow("orders", pa.table({"id": [1]}))
    connection.cursor.return_value.close.assert_called_once_with()


@pytest.mark.parametrize("failure", [True, False])
def test_fastload_restores_autocommit_and_finishes_owned_transaction(failure: bool) -> None:
    connection = _connection([])
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 2
    if failure:
        cursor.executemany.side_effect = RuntimeError("[Teradata] [Error 2801] duplicate")
    driver = TeradataDriver(connection, driver_features={"enable_fastload": True})
    if failure:
        with pytest.raises(UniqueViolationError):
            driver.execute_many("INSERT INTO orders (id) VALUES (:id)", [{"id": 1}, {"id": 2}])
        connection.rollback.assert_called_once_with()
    else:
        result = driver.execute_many("INSERT INTO orders (id) VALUES (:id)", [{"id": 1}, {"id": 2}])
        assert result.rows_affected == 2
        connection.commit.assert_called_once_with()
    assert cursor.executemany.call_args.args[0] == "{fn teradata_try_fastload} INSERT INTO orders (id) VALUES (?)"
    assert connection.autocommit is True
    cursor.close.assert_called_once_with()


def test_arrow_reader_close_releases_cursor_before_exhaustion() -> None:
    connection = _connection([[(1, Decimal("1.25"))], [(2, None)], []])
    result = TeradataDriver(connection).select_to_arrow(
        "SELECT id, total FROM orders", batch_size=1, return_format="reader"
    )
    reader = result.get_data()
    assert reader.read_next_batch().to_pydict() == {"id": [1], "total": [Decimal("1.25")]}
    reader.close()
    connection.cursor.return_value.close.assert_called_once_with()


def test_arrow_export_explicit_schema_is_preserved() -> None:
    connection = _connection([[(1, Decimal("1.25"))], []])
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("total", pa.decimal128(14, 2))])
    table = TeradataDriver(connection).select_to_arrow("SELECT id, total FROM orders", arrow_schema=schema).get_data()
    assert table.schema == schema
    assert table.to_pydict() == {"id": [1], "total": [Decimal("1.25")]}
    connection.cursor.return_value.close.assert_called_once_with()


def test_fastexport_disabled_by_default() -> None:
    connection = _connection([[]])
    TeradataDriver(connection).select_to_arrow("SELECT id, total FROM orders")
    assert connection.cursor.return_value.execute.call_args.args == ("SELECT id, total FROM orders",)


def test_fastload_keeps_caller_transaction_open() -> None:
    connection = _connection([])
    connection.autocommit = False
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 1
    driver = TeradataDriver(connection, driver_features={"enable_fastload": True})
    driver.execute_many("INSERT INTO orders (id) VALUES (:id)", [{"id": 1}])
    assert connection.autocommit is False
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


def test_fastload_does_not_prefix_updates() -> None:
    connection = _connection([])
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 1
    TeradataDriver(connection, driver_features={"enable_fastload": True}).execute_many(
        "UPDATE orders SET total = :total WHERE id = :id", [{"total": 1, "id": 2}]
    )
    assert cursor.executemany.call_args.args == ("UPDATE orders SET total = ? WHERE id = ?", [(1, 2)])
    connection.commit.assert_not_called()
    cursor.close.assert_called_once_with()


def test_arrow_storage_export_and_import_roundtrip(tmp_path: Any) -> None:
    import pyarrow.parquet as pq

    destination = str(tmp_path / "orders.parquet")
    connection = _connection([[(1, Decimal("1.25")), (2, None)], []])
    features = {"storage_capabilities": {"arrow_export_enabled": True, "arrow_import_enabled": True}}
    driver = TeradataDriver(connection, driver_features=features)
    exported = driver.select_to_storage("SELECT id, total FROM orders", destination, format_hint="parquet")
    assert exported.status == "completed"
    assert pq.read_table(destination).to_pydict() == {"id": [1, 2], "total": [Decimal("1.25"), None]}
    loaded = driver.load_from_storage("orders", destination, file_format="parquet", overwrite=True)
    assert loaded.status == "completed"
    assert loaded.telemetry["rows_processed"] == 2
    assert connection.cursor.return_value.executemany.call_args.args == (
        'INSERT INTO "orders" ("id", "total") VALUES (?, ?)',
        [(1, Decimal("1.25")), (2, None)],
    )
    assert connection.cursor.return_value.execute.call_args.args == ('DELETE FROM "orders"',)


def test_arrow_staged_load_emits_teradata_update_insert_and_cleans_stage() -> None:
    connection = _connection([])
    cursor = connection.cursor.return_value
    cursor.description = None
    cursor.rowcount = 1
    driver = TeradataDriver(connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}})
    job = driver.load_from_arrow(
        "orders", pa.table({"id": [1], "total": [2]}), stage_table="stage_orders", conflict_keys=["id"]
    )
    assert job.status == "completed"
    sql_statements = [item.args[0] for item in cursor.execute.call_args_list]
    assert sql_statements == [
        "CREATE MULTISET TABLE stage_orders AS orders WITH NO DATA",
        "UPDATE orders FROM stage_orders stg SET total = stg.total WHERE orders.id = stg.id",
        "INSERT INTO orders (id, total) SELECT stg.id, stg.total FROM stage_orders stg WHERE NOT EXISTS (SELECT 1 FROM orders tgt WHERE tgt.id = stg.id)",
        "DROP TABLE stage_orders",
    ]
    assert cursor.executemany.call_args.args == ('INSERT INTO "stage_orders" ("id", "total") VALUES (?, ?)', [(1, 2)])


def test_arrow_export_fetch_failure_closes_cursor() -> None:
    connection = _connection([])
    connection.cursor.return_value.fetchmany.side_effect = RuntimeError("[Error 2801] duplicate")
    with pytest.raises(UniqueViolationError):
        TeradataDriver(connection).select_to_arrow("SELECT id, total FROM orders")
    connection.cursor.return_value.close.assert_called_once_with()
