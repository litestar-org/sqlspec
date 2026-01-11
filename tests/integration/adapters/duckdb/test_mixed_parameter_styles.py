"""Test mixed parameter styles for DuckDB driver."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core import SQL
from tests.integration.adapters.duckdb.utils import get_unique_table_name

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_test_setup() -> Generator[tuple[DuckDBDriver, str], None, None]:
    """Create a DuckDB session and unique table for testing.

    Returns:
        A tuple of (session, table_name)
    """
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db"})

    table_name = get_unique_table_name("test_table")

    with config.provide_session() as session:
        session.execute_script(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)

        session.execute_many(
            f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)",
            [(1, "test1", 100), (2, "test2", 200), (3, "test3", 300)],
        )
        yield session, table_name


def test_mixed_qmark_and_numeric_styles(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test mixing ? and $1 parameter styles in the same query."""
    session, table_name = duckdb_test_setup

    sql = SQL(f"SELECT * FROM {table_name} WHERE name = ? AND value > $1", "test2", 150)

    result = session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["name"] == "test2"
    assert result.data[0]["value"] == 200


def test_numeric_style_extraction(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test that numeric style parameters are correctly extracted and compiled."""
    session, table_name = duckdb_test_setup

    sql = SQL(f"SELECT * FROM {table_name} WHERE id = $1 AND value >= $2", 2, 100)

    result = session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["id"] == 2
    assert result.data[0]["value"] == 200


def test_qmark_style_extraction(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test that qmark style parameters are correctly extracted and compiled."""
    session, table_name = duckdb_test_setup

    sql = SQL(f"SELECT * FROM {table_name} WHERE name = ? AND value < ?", "test1", 150)

    result = session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100


def test_complex_mixed_styles(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test complex query with multiple mixed parameter styles."""
    session, table_name = duckdb_test_setup

    session.execute_many(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)",
        [(4, "test4", 400), (5, "test5", 500), (6, "test6", 600)],
    )

    sql = SQL(
        f"""
        SELECT * FROM {table_name}
        WHERE (name LIKE ? OR name = $1)
        AND value BETWEEN $2 AND ?
        ORDER BY value
        """,
        "test%",
        "special",
        250,
        550,
    )

    result = session.execute(sql)

    assert len(result.data) == 3
    assert result.data[0]["value"] == 300
    assert result.data[1]["value"] == 400
    assert result.data[2]["value"] == 500


def test_parameter_info_detection(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test that parameter_info correctly identifies mixed styles."""

    session, table_name = duckdb_test_setup

    sql = SQL(f"SELECT * FROM {table_name} WHERE id = ? AND name = $1", 1, "test1")

    result = session.execute(sql)
    assert len(result.data) == 1


def test_unsupported_style_fallback(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test that unsupported parameter styles fall back to default."""
    session, table_name = duckdb_test_setup

    sql = SQL(f"SELECT * FROM {table_name} WHERE name = :name", {"name": "test1"})

    result = session.execute(sql)
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_execute_many_with_numeric_style(duckdb_test_setup: tuple[DuckDBDriver, str]) -> None:
    """Test execute_many with numeric parameter style."""
    session, _table_name = duckdb_test_setup

    session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_many (
            id INTEGER PRIMARY KEY,
            data VARCHAR
        )
    """)

    sql = SQL(
        "INSERT INTO test_many (id, data) VALUES ($1, $2)", [(7, "seven"), (8, "eight"), (9, "nine")], is_many=True
    )

    result = session.execute(sql)
    assert result.rows_affected == 3

    verify_result = session.execute("SELECT COUNT(*) as count FROM test_many")
    assert verify_result.data[0]["count"] == 3
