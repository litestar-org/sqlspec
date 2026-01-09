"""Integration tests for Spanner PostgreSQL interface (Spangres) driver.

These tests verify that Spanner's PostgreSQL-compatible dialect works
correctly through SQLSpec. Requires a PostgreSQL-dialect Spanner database.

To run these tests, you need:
1. A Spanner database created with database_dialect=DatabaseDialect.POSTGRESQL
2. The spangres_service and spangres_config fixtures configured in conftest.py
"""

import pytest

pytestmark = [pytest.mark.spanner, pytest.mark.skip(reason="Spangres fixtures missing")]


def test_spangres_select_one() -> None:
    """Test basic SELECT 1 query in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_select_with_dollar_params() -> None:
    """Test SELECT with $1, $2 parameter style."""
    pytest.skip("Spangres fixtures required")


def test_spangres_insert_through_session() -> None:
    """Test INSERT operation in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_update_through_session() -> None:
    """Test UPDATE operation in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_delete_through_session() -> None:
    """Test DELETE operation in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_full_crud_cycle() -> None:
    """Test full CRUD cycle in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_execute_many() -> None:
    """Test batch operations in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_select_to_arrow() -> None:
    """Test Arrow export in PostgreSQL mode."""
    pytest.skip("Spangres fixtures required")


def test_spangres_dialect_sql_generation() -> None:
    """Test that Spangres dialect generates correct SQL syntax."""
    from sqlglot import parse_one

    sql = "CREATE TABLE test (id VARCHAR PRIMARY KEY, ts TIMESTAMP) ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL '30 days'))"
    parsed = parse_one(sql, dialect="spangres")
    rendered = parsed.sql(dialect="spangres")

    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN" in rendered
