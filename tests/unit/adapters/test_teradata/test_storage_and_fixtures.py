from sqlspec.driver._storage_helpers import (
    build_checkpoint_insert_sql,
    build_checkpoint_table_ddl,
    build_create_index_sql,
    build_drop_index_sql,
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
)


def test_teradata_checkpoint_sql() -> None:
    ddl = build_checkpoint_table_ddl("teradata", "checkpoint")
    assert ddl.startswith("CREATE MULTISET TABLE checkpoint (")
    assert "TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6)" in ddl
    merge = build_checkpoint_insert_sql("teradata", "checkpoint")
    assert merge.startswith("MERGE INTO checkpoint AS tgt")
    assert "CAST(:phase AS VARCHAR(128))" in merge


def test_teradata_stage_sql() -> None:
    assert build_stage_table_ddl("teradata", "orders", "stage") == [
        "CREATE MULTISET TABLE stage AS orders WITH NO DATA"
    ]
    assert build_drop_stage_table_sql("teradata", "stage") == "DROP TABLE stage"
    sql = build_stage_merge_sql("teradata", "orders", "stage", ["id", "total"], conflict_keys=["id"])
    assert sql == (
        "UPDATE orders FROM stage stg SET total = stg.total WHERE orders.id = stg.id; "
        "INSERT INTO orders (id, total) SELECT stg.id, stg.total FROM stage stg "
        "WHERE NOT EXISTS (SELECT 1 FROM orders tgt WHERE tgt.id = stg.id)"
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

    import pytest

    from sqlspec.driver._storage_helpers import execute_load_from_storage_sync
    from sqlspec.exceptions import OperationalError

    driver = MagicMock()
    driver.dialect = "teradata"
    driver.execute.side_effect = [OperationalError("[Error 3803] Table already exists"), SimpleNamespace(data=[])]
    job = execute_load_from_storage_sync(driver, "orders", [], file_format="parquet", checkpoint_table="checkpoint")
    assert job.status == "completed"
    assert driver.execute.call_args.args[0] == "SELECT shard_uri FROM checkpoint WHERE phase = :phase"
    driver.execute.side_effect = OperationalError("[Error 3523] Permission denied")
    with pytest.raises(OperationalError, match="3523"):
        execute_load_from_storage_sync(driver, "orders", [], file_format="parquet", checkpoint_table="checkpoint")
