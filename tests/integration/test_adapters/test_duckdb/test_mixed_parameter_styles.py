"""Test mixed parameter styles for DuckDB driver."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.statement.sql import SQL, SQLConfig


@pytest.fixture
def duckdb_session() -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session for testing."""
    config = DuckDBConfig(database=":memory:", statement_config=SQLConfig())

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)
        # Insert test data
        session.execute_many(
            "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
            [(1, "test1", 100), (2, "test2", 200), (3, "test3", 300)],
        )
        yield session


def test_mixed_qmark_and_numeric_styles(duckdb_session: DuckDBDriver) -> None:
    """Test mixing ? and $1 parameter styles in the same query."""
    # Create SQL with mixed parameter styles
    sql = SQL("SELECT * FROM test_table WHERE name = ? AND value > $1", parameters=["test2", 150])

    result = duckdb_session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["name"] == "test2"
    assert result.data[0]["value"] == 200


def test_numeric_style_extraction(duckdb_session: DuckDBDriver) -> None:
    """Test that numeric style parameters are correctly extracted and compiled."""
    # Use only numeric style
    sql = SQL("SELECT * FROM test_table WHERE id = $1 AND value >= $2", parameters=[2, 100])

    result = duckdb_session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["id"] == 2
    assert result.data[0]["value"] == 200


def test_qmark_style_extraction(duckdb_session: DuckDBDriver) -> None:
    """Test that qmark style parameters are correctly extracted and compiled."""
    # Use only qmark style
    sql = SQL("SELECT * FROM test_table WHERE name = ? AND value < ?", parameters=["test1", 150])

    result = duckdb_session.execute(sql)

    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100


def test_complex_mixed_styles(duckdb_session: DuckDBDriver) -> None:
    """Test complex query with multiple mixed parameter styles."""
    # Insert more test data
    duckdb_session.execute_many(
        "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
        [(4, "test4", 400), (5, "test5", 500), (6, "test6", 600)],
    )

    # Complex query with mixed styles
    sql = SQL(
        """
        SELECT * FROM test_table
        WHERE (name LIKE ? OR name = $1)
        AND value BETWEEN $2 AND ?
        ORDER BY value
        """,
        parameters=["test%", "special", 250, 550],
    )

    result = duckdb_session.execute(sql)

    # Should find test3 (300), test4 (400), test5 (500)
    assert len(result.data) == 3
    assert result.data[0]["value"] == 300
    assert result.data[1]["value"] == 400
    assert result.data[2]["value"] == 500


def test_parameter_info_detection(duckdb_session: DuckDBDriver) -> None:
    """Test that parameter_info correctly identifies mixed styles."""
    from sqlspec.statement.parameters import ParameterStyle

    # Create SQL with mixed styles
    sql = SQL("SELECT * FROM test_table WHERE id = ? AND name = $1", parameters=[1, "test1"])

    # Check parameter_info
    param_styles = {p.style for p in sql.parameter_info}
    assert ParameterStyle.QMARK in param_styles
    assert ParameterStyle.NUMERIC in param_styles

    # Execute to ensure it works
    result = duckdb_session.execute(sql)
    assert len(result.data) == 1


def test_unsupported_style_fallback(duckdb_session: DuckDBDriver) -> None:
    """Test that unsupported parameter styles fall back to default."""
    # Create SQL with named style (not supported by DuckDB)
    sql = SQL("SELECT * FROM test_table WHERE name = :name", parameters={"name": "test1"})

    # This should still work because it should be converted to supported style
    result = duckdb_session.execute(sql)
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_execute_many_with_numeric_style(duckdb_session: DuckDBDriver) -> None:
    """Test execute_many with numeric parameter style."""
    # Create a new table for this test
    duckdb_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_many (
            id INTEGER PRIMARY KEY,
            data VARCHAR
        )
    """)

    # Use numeric style for execute_many
    sql = SQL("INSERT INTO test_many (id, data) VALUES ($1, $2)").as_many(
        parameters=[(7, "seven"), (8, "eight"), (9, "nine")]
    )

    result = duckdb_session.execute(sql)
    assert result.rows_affected == 3

    # Verify the data was inserted
    verify_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_many")
    assert verify_result.data[0]["count"] == 3
