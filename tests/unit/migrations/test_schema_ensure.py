"""Tests for additive schema ensure and diff behavior."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec import sql
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_async, ensure_schema_sync


def test_ensure_diff_engine_reproduces_tracker_self_heal() -> None:
    target = SchemaTarget(
        "ddl_migrations",
        sql.create_table("ddl_migrations").column("version_num", "VARCHAR(32)").column("checksum", "VARCHAR(64)"),
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "DDL_MIGRATIONS"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "VERSION_NUM"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.added_columns == {"ddl_migrations": ["checksum"]}
    driver.execute.assert_called_once()
    assert "ALTER TABLE" in str(driver.execute.call_args.args[0]).upper()
    assert "ADD COLUMN" in str(driver.execute.call_args.args[0]).upper()


def test_ensure_diff_creates_missing_table_idempotently() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    driver.data_dictionary.get_tables.side_effect = [[], [{"table_name": "widgets"}]]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}]

    first = ensure_schema_sync(driver, [target], manage_schema=True)
    second = ensure_schema_sync(driver, [target], manage_schema=True)

    assert first.created_tables == ["widgets"]
    assert second.created_tables == []
    driver.execute.assert_called_once_with(target.create_table)


def test_ensure_diff_additive_only_ignores_rename() -> None:
    target = SchemaTarget(
        "widgets", sql.create_table("widgets").column("id", "INTEGER").column("new_name", "VARCHAR(50)")
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}, {"column_name": "old_name"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.added_columns == {}
    assert result.deferred_tables == ["widgets"]
    driver.execute.assert_not_called()


def test_ensure_diff_checksum_ledger_untouched() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.created_tables == []
    assert result.added_columns == {}
    assert all("DDL_MIGRATIONS" not in str(call.args[0]).upper() for call in driver.execute.call_args_list)


def test_manage_schema_false_is_noop() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()

    result = ensure_schema_sync(driver, [target], manage_schema=False)

    assert result.created_tables == []
    assert result.added_columns == {}
    driver.data_dictionary.get_tables.assert_not_called()
    driver.data_dictionary.get_columns.assert_not_called()
    driver.execute.assert_not_called()


def test_run_migrations_flag_invokes_explicit_migration_path() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    migration_runner = MagicMock()

    result = ensure_schema_sync(
        driver,
        [target],
        manage_schema=False,
        run_migrations=True,
        migration_runner=migration_runner,
    )

    assert result.migrations_run is True
    migration_runner.assert_called_once_with(driver)
    driver.data_dictionary.get_tables.assert_not_called()


@pytest.mark.anyio
async def test_async_ensure_diff_adds_missing_column() -> None:
    target = SchemaTarget(
        "widgets", sql.create_table("widgets").column("id", "INTEGER").column("label", "VARCHAR(50)")
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "widgets"}])
    driver.data_dictionary.get_columns = AsyncMock(return_value=[{"column_name": "id"}])
    driver.execute = AsyncMock()
    driver.commit = AsyncMock()

    result = await ensure_schema_async(driver, [target], manage_schema=True)

    assert result.added_columns == {"widgets": ["label"]}
    driver.execute.assert_awaited_once()
    driver.commit.assert_awaited_once()
