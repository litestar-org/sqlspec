"""Snowflake Arrow ingestion, stage cleanup, and fixture SQL contracts."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from sqlspec.adapters.snowflake import SnowflakeDriver
from sqlspec.exceptions import SQLParsingError


def test_arrow_small_load_batches_values_and_reports_rows(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    job = driver.load_from_arrow(
        "sales.orders", pa.table({"ID": [1, 2, 3], "NAME": ["a", "b", "c"]}), batch_size=2, if_not_exists=True
    )
    assert job.telemetry["rows_processed"] == 3
    inserts = [call for call in snowflake_connection.calls if call[0].startswith("INSERT")]
    assert [call[1] for call in inserts] == [[(1, "a"), (2, "b")], [(3, "c")]]
    assert inserts[0][0] == 'INSERT INTO "SALES"."ORDERS" ("ID", "NAME") VALUES (?, ?)'
    assert snowflake_connection.calls[0][0].startswith('CREATE TABLE IF NOT EXISTS "SALES"."ORDERS"')
    assert all(cursor.closed for cursor in snowflake_connection.cursors)


def test_arrow_nested_values_use_parse_json_select(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    job = driver.load_from_arrow('"Orders"', pa.table({"id": [1, 2], "payload": [{"x": 1}, None]}))
    assert job.telemetry["rows_processed"] == 2
    sql = snowflake_connection.calls[-1][0]
    assert sql == 'INSERT INTO "Orders" ("ID", "PAYLOAD") SELECT ?, PARSE_JSON(?)'
    assert [call[1] for call in snowflake_connection.calls if call[0].startswith("INSERT")] == [
        (1, '{"x":1}'),
        (2, None),
    ]


def test_arrow_large_load_cleans_remote_and_local_stage(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection, driver_features={"stage_parquet_threshold_rows": 2})
    job = driver.load_from_arrow("orders", pa.table({"id": [1, 2]}))
    assert job.telemetry["rows_processed"] == 2
    statements = [call[0] for call in snowflake_connection.calls]
    assert statements[0].startswith("PUT 'file://")
    assert statements[1].startswith('COPY INTO "ORDERS" FROM @~/sqlspec_stage/')
    assert "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE PURGE=TRUE" in statements[1]
    assert statements[2].startswith("REMOVE @~/sqlspec_stage/")
    local_uri = statements[0].split("'", 2)[1]
    local_path = Path(local_uri.removeprefix("file://"))
    assert list(local_path.parent.parent.glob(local_path.parent.name)) == []


def test_stage_copy_error_still_removes_staged_files(snowflake_connection: Any) -> None:
    from snowflake.connector.errors import ProgrammingError

    snowflake_connection.fail_on = "COPY INTO"
    snowflake_connection.failure = ProgrammingError(msg="COPY rejected", errno=1003, sqlstate="42000")
    driver = SnowflakeDriver(snowflake_connection, driver_features={"stage_parquet_threshold_rows": 1})
    with pytest.raises(SQLParsingError, match="COPY rejected"):
        driver.load_from_arrow("orders", pa.table({"id": [1]}))
    assert snowflake_connection.calls[-1][0].startswith("REMOVE @~/sqlspec_stage/")
    assert snowflake_connection.cursors[-1].closed


def test_staged_merge_creates_target_then_stage_and_skips_index_ddl(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    job = driver.load_from_arrow(
        "orders",
        pa.table({"id": [1], "name": ["updated"]}),
        if_not_exists=True,
        conflict_keys=["id"],
        stage_table="orders_stage",
        drop_indexes=True,
    )
    assert job.telemetry["rows_processed"] == 1
    statements = [call[0] for call in snowflake_connection.calls]
    assert statements[0].startswith('CREATE TABLE IF NOT EXISTS "ORDERS"')
    assert any(statement.startswith("MERGE INTO") for statement in statements)
    assert any(statement.startswith("DROP TABLE") for statement in statements)


def test_arrow_load_rejects_invalid_batch_size_before_sql(snowflake_connection: Any) -> None:
    with pytest.raises(ValueError, match="batch_size must be positive"):
        SnowflakeDriver(snowflake_connection).load_from_arrow("orders", pa.table({"id": [1]}), batch_size=0)
    assert snowflake_connection.calls == []


def test_arrow_ingest_folds_unquoted_columns_and_preserves_quoted_case(snowflake_connection: Any) -> None:
    source = pa.table({"id": [1], '"MixedValue"': ["kept"]})
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    driver.load_from_arrow("orders", source, stage_table="orders_stage", conflict_keys=["id"])
    inserts = [call[0] for call in snowflake_connection.calls if call[0].startswith("INSERT")]
    assert inserts == ['INSERT INTO "ORDERS_STAGE" ("ID", "MixedValue") VALUES (?, ?)']
    merge = next(call[0] for call in snowflake_connection.calls if call[0].startswith("MERGE"))
    assert 'tgt."ID" = stg."ID"' in merge
    assert 'tgt."MixedValue" = stg."MixedValue"' in merge


def test_storage_export_import_roundtrip_uses_arrow_pipeline(tmp_path: Path, snowflake_connection: Any) -> None:
    import pyarrow.parquet as pq

    snowflake_connection.default_result = {
        "description": [("ID", 0, None, None, 18, 0, False), ("NAME", 2, None, None, None, None, True)],
        "tables": [pa.table({"ID": [1, 2], "NAME": ["one", "two"]})],
    }
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    destination = tmp_path / "export.parquet"
    exported = driver.select_to_storage("SELECT ID, NAME FROM data", destination, format_hint="parquet")
    assert exported.telemetry["rows_processed"] == 2
    assert pq.read_table(destination).to_pylist() == [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]
    loaded = driver.load_from_storage("copied", destination, file_format="parquet")
    assert loaded.telemetry["rows_processed"] == 2
    assert snowflake_connection.calls[-1][0] == 'INSERT INTO "COPIED" ("ID", "NAME") VALUES (?, ?)'
    assert snowflake_connection.calls[-1][1] == [(1, "one"), (2, "two")]


@pytest.mark.parametrize("paramstyle", ["qmark", "numeric", "pyformat", "format"])
@pytest.mark.parametrize("nested", [False, True])
def test_arrow_load_uses_configured_binding_mode(snowflake_connection: Any, paramstyle: str, nested: bool) -> None:
    from sqlspec.adapters.snowflake.core import build_statement_config

    snowflake_connection.paramstyle = paramstyle
    driver = SnowflakeDriver(
        snowflake_connection,
        statement_config=build_statement_config(paramstyle=paramstyle),
        driver_features={"enable_stage_parquet_load": False},
    )
    source = pa.table({"id": [1, 2], "payload": [{"x": 1}, {"x": 2}]}) if nested else pa.table({"id": [1, 2]})
    driver.load_from_arrow("orders", source)
    statements = [call[0] for call in snowflake_connection.calls if call[0].startswith("INSERT")]
    placeholder = "%s" if paramstyle in {"format", "pyformat"} else ":1" if paramstyle == "numeric" else "?"
    assert all(placeholder in statement for statement in statements)
    assert len(snowflake_connection.committed_rows) == 2


@pytest.mark.parametrize("autocommit", [True, False])
def test_arrow_load_rolls_back_partial_batches_when_it_owns_transaction(
    snowflake_connection: Any, autocommit: bool
) -> None:
    from snowflake.connector.errors import ProgrammingError

    snowflake_connection.autocommit_enabled = autocommit
    snowflake_connection.fail_on_batch = 2
    snowflake_connection.failure = ProgrammingError(msg="partial batch failure", errno=1003, sqlstate="42000")
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    with pytest.raises(SQLParsingError, match="partial batch failure"):
        driver.load_from_arrow("orders", pa.table({"id": [1, 2, 3]}), batch_size=2)
    assert snowflake_connection.pending_rows == []
    assert snowflake_connection.committed_rows == []
    assert snowflake_connection.rollbacks == 1
    assert snowflake_connection.commits == 0


def test_arrow_load_preserves_existing_caller_transaction(snowflake_connection: Any) -> None:
    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    driver.execute("BEGIN")
    driver.load_from_arrow("orders", pa.table({"id": [1, 2]}), batch_size=1)
    assert snowflake_connection.commits == 0
    assert len(snowflake_connection.pending_rows) == 2
    driver.rollback()
    assert snowflake_connection.committed_rows == []


def test_arrow_batch_failure_keeps_caller_transaction_under_caller_control(snowflake_connection: Any) -> None:
    from snowflake.connector.errors import ProgrammingError

    driver = SnowflakeDriver(snowflake_connection, driver_features={"enable_stage_parquet_load": False})
    driver.execute("BEGIN")
    snowflake_connection.fail_on_batch = 2
    snowflake_connection.failure = ProgrammingError(msg="caller batch failure", errno=1003, sqlstate="42000")
    with pytest.raises(SQLParsingError, match="caller batch failure"):
        driver.load_from_arrow("orders", pa.table({"id": [1, 2, 3]}), batch_size=2)
    assert snowflake_connection.commits == 0
    assert snowflake_connection.rollbacks == 0
    assert len(snowflake_connection.pending_rows) == 2
    driver.rollback()
    assert snowflake_connection.committed_rows == []
