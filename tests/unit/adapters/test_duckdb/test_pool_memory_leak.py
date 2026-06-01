"""Regression tests for DuckDB pool write-only attribute removal."""

import pytest

pytest.importorskip("duckdb", reason="DuckDB adapter requires duckdb package")

from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool


def test_pool_has_no_connection_times_attribute() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})

    assert not hasattr(pool, "_connection_times")


def test_pool_has_no_created_connections_attribute() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})

    assert not hasattr(pool, "_created_connections")


def test_pool_slots_do_not_contain_removed_attrs() -> None:
    slots = DuckDBConnectionPool.__slots__

    assert "_connection_times" not in slots
    assert "_created_connections" not in slots


def test_pool_creates_connection_after_attribute_removal() -> None:
    pool = DuckDBConnectionPool({"database": ":memory:"})
    try:
        conn = pool.acquire()
        row = conn.execute("SELECT 42").fetchone()
    finally:
        pool.close()

    assert row == (42,)
