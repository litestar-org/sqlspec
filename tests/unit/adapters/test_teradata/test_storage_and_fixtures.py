import pytest

from sqlspec.data_dictionary._index_ddl import build_create_index_sql, build_drop_index_sql
from sqlspec.storage._staging_sql import (
    build_checkpoint_insert_sql,
    build_checkpoint_table_ddl,
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
)


def test_teradata_checkpoint_sql() -> None:
    ddl = build_checkpoint_table_ddl("teradata", "checkpoint")
    assert ddl.startswith("CREATE MULTISET TABLE checkpoint (")
    assert "TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6)" in ddl
    assert ddl.endswith(") UNIQUE PRIMARY INDEX (phase, shard_uri)")
    merge = build_checkpoint_insert_sql("teradata", "checkpoint")
    assert merge.startswith("MERGE INTO checkpoint AS tgt")
    assert "CAST(:phase AS VARCHAR(128))" in merge


def test_teradata_stage_sql() -> None:
    assert build_stage_table_ddl("teradata", "orders", "stage") == [
        "CREATE MULTISET TABLE stage AS (SELECT * FROM orders) WITH NO DATA"
    ]
    assert build_drop_stage_table_sql("teradata", "stage") == "DROP TABLE stage"
    sql = build_stage_merge_sql("teradata", "orders", "stage", ["id", "total"], conflict_keys=["id"])
    assert sql == (
        "UPDATE tgt FROM orders tgt, stage src SET total = src.total WHERE tgt.id = src.id; "
        "INSERT INTO orders (id, total) SELECT stg.id, stg.total FROM stage stg "
        "WHERE NOT EXISTS (SELECT 1 FROM orders tgt WHERE tgt.id = stg.id)"
    )
    qualified = build_stage_merge_sql(
        "teradata",
        '"analytics"."orders"',
        '"analytics"."stage"',
        ["id", "total"],
        conflict_keys=["id"],
        type_casts={"total": "DECIMAL(12, 2)"},
    )
    assert qualified.startswith(
        'UPDATE tgt FROM "analytics"."orders" tgt, "analytics"."stage" src '
        "SET total = CAST(src.total AS DECIMAL(12, 2)) WHERE tgt.id = src.id; "
    )
    assert build_stage_merge_sql("teradata", "orders", "stage", ["id"], conflict_keys=["id"]) == (
        "INSERT INTO orders (id) SELECT stg.id FROM stage stg "
        "WHERE NOT EXISTS (SELECT 1 FROM orders tgt WHERE tgt.id = stg.id)"
    )


def test_teradata_secondary_indexes() -> None:
    metadata = {"name": 'idx"name', "columns": ["created_at DESC", "status ASC"], "is_unique": True}
    assert build_create_index_sql("teradata", metadata, "orders", schema="analytics") == (
        'CREATE UNIQUE INDEX "idx""name" (created_at, status) ON "analytics"."orders"'
    )
    assert build_drop_index_sql("teradata", metadata, "orders", schema="analytics") == (
        'DROP INDEX "idx""name" ON "analytics"."orders"'
    )


def test_teradata_checkpoint_existing_table_and_permission_error() -> None:
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from sqlspec.exceptions import OperationalError
    from sqlspec.storage._ingest import execute_load_from_storage_sync

    driver = MagicMock()
    driver.dialect = "teradata"
    driver.execute.side_effect = [SimpleNamespace(data=[(1,)]), SimpleNamespace(data=[])]
    job = execute_load_from_storage_sync(
        driver, "orders", [], file_format="parquet", checkpoint_table="mart.checkpoint"
    )
    assert job.status == "completed"
    assert "DBC.TablesV" in driver.execute.call_args_list[0].args[0]
    assert driver.execute.call_args_list[0].args[1] == ("MART", "CHECKPOINT")
    assert driver.execute.call_args_list[1].args[0] == "SELECT shard_uri FROM mart.checkpoint WHERE phase = :phase"

    driver.execute.reset_mock()
    driver.execute.side_effect = [
        SimpleNamespace(data=[]),
        OperationalError("[Error 3803] Table already exists"),
        SimpleNamespace(data=[]),
    ]
    job = execute_load_from_storage_sync(driver, "orders", [], file_format="parquet", checkpoint_table="checkpoint")
    assert job.status == "completed"
    assert driver.execute.call_args.args[0] == "SELECT shard_uri FROM checkpoint WHERE phase = :phase"

    driver.execute.side_effect = [SimpleNamespace(data=[]), OperationalError("[Error 3523] Permission denied")]
    with pytest.raises(OperationalError, match="3523"):
        execute_load_from_storage_sync(driver, "orders", [], file_format="parquet", checkpoint_table="checkpoint")


@pytest.mark.anyio
async def test_teradata_checkpoint_probe_async_skips_ddl_when_existing() -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from sqlspec.storage._ingest import _ensure_checkpoint_table_async

    driver = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(data=[(1,)])))
    await _ensure_checkpoint_table_async(driver, "teradata", "mart.checkpoint")
    driver.execute.assert_awaited_once()
    assert "DBC.TablesV" in driver.execute.await_args.args[0]
    assert driver.execute.await_args.args[1] == ("MART", "CHECKPOINT")
