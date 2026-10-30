"""Unit tests for Sybase ASE and SAP SQL Anywhere fixture loading, decoding, and identity resync."""

import json
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from sqlspec.utils.fixtures import export_table_fixtures_sync, load_table_fixtures_async, load_table_fixtures_sync


def _mock_sync_driver(dialect: str, columns: list[dict[str, Any]], max_identity: int = 42) -> MagicMock:
    driver = MagicMock()
    driver.statement_config.dialect = dialect
    driver.data_dictionary.get_columns.return_value = columns
    driver.select_value_or_none.return_value = max_identity
    return driver


def _mock_async_driver(dialect: str, columns: list[dict[str, Any]], max_identity: int = 42) -> MagicMock:
    driver = MagicMock()
    driver.statement_config.dialect = dialect
    driver.data_dictionary.get_columns = AsyncMock(return_value=columns)
    driver.execute_many = AsyncMock()
    driver.execute = AsyncMock()
    driver.select_value_or_none = AsyncMock(return_value=max_identity)
    return driver


SYBASE_IDENTITY_COLUMNS: list[dict[str, Any]] = [
    {"column_name": "id", "data_type": "int", "is_primary": True, "identity_generation": "YES", "is_generated": 0},
    {
        "column_name": "name",
        "data_type": "varchar",
        "is_primary": False,
        "identity_generation": None,
        "is_generated": 0,
    },
]

SQLANYWHERE_IDENTITY_COLUMNS: list[dict[str, Any]] = [
    {
        "column_name": "id",
        "data_type": "integer",
        "is_primary": True,
        "identity_generation": "AUTOINCREMENT",
        "is_generated": 0,
    },
    {
        "column_name": "name",
        "data_type": "varchar",
        "is_primary": False,
        "identity_generation": None,
        "is_generated": 0,
    },
]


def test_sybase_identity_resync_executes_sp_chgattribute_sync(tmp_path: Path) -> None:
    """Sybase fixture load with resync_sequences=True should execute sp_chgattribute when max > 0."""
    rows = [{"id": 42, "name": "Alice"}]
    (tmp_path / "users.json").write_text(json.dumps(rows), encoding="utf-8")
    driver = _mock_sync_driver("sybase", SYBASE_IDENTITY_COLUMNS, max_identity=42)

    counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

    assert counts == {"users": 1}
    assert driver.execute_many.call_args.args[1] == [{"id": 42, "name": "Alice"}]
    driver.select_value_or_none.assert_called_once_with('SELECT coalesce(max("id"), 0) FROM "users"')
    driver.execute.assert_called_once_with("EXEC sp_chgattribute 'users', 'identity_burn_max', 0, '42'")


def test_sybase_identity_resync_skips_when_max_is_zero(tmp_path: Path) -> None:
    """Sybase fixture load should skip sp_chgattribute when table max is 0."""
    (tmp_path / "users.json").write_text("[]", encoding="utf-8")
    driver = _mock_sync_driver("sybase", SYBASE_IDENTITY_COLUMNS, max_identity=0)

    counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

    assert counts == {"users": 0}
    driver.select_value_or_none.assert_called_once_with('SELECT coalesce(max("id"), 0) FROM "users"')
    driver.execute.assert_not_called()


async def test_sqlanywhere_identity_resync_executes_sa_reset_identity_async(tmp_path: Path) -> None:
    """SQL Anywhere async fixture load should execute CALL sa_reset_identity with table, schema, and max."""
    rows = [{"id": 15, "name": "Bob"}]
    (tmp_path / "DBA.users.json").write_text(json.dumps(rows), encoding="utf-8")
    driver = _mock_async_driver("sqlanywhere", SQLANYWHERE_IDENTITY_COLUMNS, max_identity=15)

    counts = await load_table_fixtures_async(driver, tmp_path, resync_sequences=True)

    assert counts == {"DBA.users": 1}
    assert driver.execute_many.call_args.args[1] == [{"id": 15, "name": "Bob"}]
    driver.select_value_or_none.assert_awaited_once_with('SELECT coalesce(max("id"), 0) FROM "DBA"."users"')
    driver.execute.assert_awaited_once_with("CALL sa_reset_identity('users', 'DBA', 15)")


def test_sybase_and_sqlanywhere_lob_types_excluded_from_export_order_by(tmp_path: Path) -> None:
    """Unorderable LOB types in Sybase and SQL Anywhere should be excluded from export ORDER BY."""
    driver = MagicMock()
    driver.statement_config.dialect = "sybase"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "code", "data_type": "varchar", "is_primary": False},
        {"column_name": "notes_text", "data_type": "text", "is_primary": False},
        {"column_name": "notes_unitext", "data_type": "unitext", "is_primary": False},
        {"column_name": "photo", "data_type": "image", "is_primary": False},
        {"column_name": "sa_clob", "data_type": "long varchar", "is_primary": False},
        {"column_name": "sa_nclob", "data_type": "long nvarchar", "is_primary": False},
        {"column_name": "sa_blob", "data_type": "long binary", "is_primary": False},
    ]
    driver.select.return_value = []

    export_table_fixtures_sync(driver, tmp_path, ["docs"], compress=False)

    select_query = driver.select.call_args.args[0]
    order_by_clause = select_query.to_statement().sql.partition("ORDER BY")[2]
    assert "[code]" in order_by_clause or '"code"' in order_by_clause
    for lob_col in ("notes_text", "notes_unitext", "photo", "sa_clob", "sa_nclob", "sa_blob"):
        assert lob_col not in order_by_clause


def test_sybase_and_sqlanywhere_value_decoders(tmp_path: Path) -> None:
    """Sybase and SQL Anywhere temporal, monetary, UUID, and binary column types should decode on load."""
    rows = [
        {
            "id": 1,
            "big_dt": "2026-10-30T12:34:56.123456+00:00",
            "small_dt": "2026-10-30T12:34:00+00:00",
            "dt_tz": "2026-10-30T12:34:56+00:00",
            "big_t": "12:34:56.123456",
            "price": "19.95",
            "small_price": "5.50",
            "ext_id": "7f3c1d2e-9a4b-4c5d-8e6f-0a1b2c3d4e5f",
            "raw_image": "AP8=",
            "raw_long_bin": "AP8=",
        }
    ]
    (tmp_path / "records.json").write_text(json.dumps(rows), encoding="utf-8")
    columns = [
        {"column_name": "id", "data_type": "int", "is_primary": True},
        {"column_name": "big_dt", "data_type": "bigdatetime", "is_primary": False},
        {"column_name": "small_dt", "data_type": "smalldatetime", "is_primary": False},
        {"column_name": "dt_tz", "data_type": "datetimetz", "is_primary": False},
        {"column_name": "big_t", "data_type": "bigtime", "is_primary": False},
        {"column_name": "price", "data_type": "money", "is_primary": False},
        {"column_name": "small_price", "data_type": "smallmoney", "is_primary": False},
        {"column_name": "ext_id", "data_type": "uniqueidentifier", "is_primary": False},
        {"column_name": "raw_image", "data_type": "image", "is_primary": False},
        {"column_name": "raw_long_bin", "data_type": "long binary", "is_primary": False},
    ]
    driver = _mock_sync_driver("sybase", columns)

    load_table_fixtures_sync(driver, tmp_path)

    loaded_row = driver.execute_many.call_args.args[1][0]
    assert loaded_row["big_dt"] == datetime(2026, 10, 30, 12, 34, 56, 123456, tzinfo=UTC)
    assert loaded_row["small_dt"] == datetime(2026, 10, 30, 12, 34, 0, tzinfo=UTC)
    assert loaded_row["dt_tz"] == datetime(2026, 10, 30, 12, 34, 56, tzinfo=UTC)
    assert loaded_row["big_t"] == time(12, 34, 56, 123456)
    assert loaded_row["price"] == Decimal("19.95")
    assert loaded_row["small_price"] == Decimal("5.50")
    assert loaded_row["ext_id"] == UUID("7f3c1d2e-9a4b-4c5d-8e6f-0a1b2c3d4e5f")
    assert loaded_row["raw_image"] == b"\x00\xff"
    assert loaded_row["raw_long_bin"] == b"\x00\xff"
