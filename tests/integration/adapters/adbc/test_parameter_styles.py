# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Test different parameter styles and None handling for ADBC drivers.

This file tests comprehensive None parameter handling for ADBC,
which has specific AST transformation logic for NULL values.
"""

import math
from collections.abc import Generator
from datetime import date

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import SQLResult
from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core import replace_null_parameters_with_literals
from sqlspec.exceptions import SQLSpecError
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def adbc_postgresql_session(postgres_service: "PostgresService") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC PostgreSQL session for parameter testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        # Create test tables for parameter testing
        session.execute_script("""
            DROP TABLE IF EXISTS test_parameters CASCADE;
            CREATE TABLE test_parameters (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                description TEXT,
                flag BOOLEAN,
                created_date DATE,
                score REAL,
                metadata JSONB
            );

            DROP TABLE IF EXISTS test_none_handling CASCADE;
            CREATE TABLE test_none_handling (
                id INTEGER PRIMARY KEY,
                text_col TEXT,
                int_col INTEGER,
                bool_col BOOLEAN,
                date_col DATE,
                timestamp_col TIMESTAMP,
                real_col REAL,
                json_col JSONB
            );
        """)
        yield session


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_numeric_parameter_style_basic(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic PostgreSQL numeric parameter style with ADBC."""
    # Insert test data
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES ($1, $2, $3)",
        ("basic_test", 100, "Basic description"),
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Query with numeric parameters
    select_result = adbc_postgresql_session.execute("SELECT * FROM test_parameters WHERE name = $1", ("basic_test",))

    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "basic_test"
    assert select_result.data[0]["value"] == 100


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_parameters_single_none(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC handling of single None parameter.

    This tests the AST transformer's ability to convert None to NULL literals.
    """
    # Insert with single None parameter
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)", (1, None, 42)
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify the None was properly stored as NULL
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (1,))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["id"] == 1
    assert row["text_col"] is None
    assert row["int_col"] == 42


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_parameters_multiple_none(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC handling of multiple None parameters.

    This tests the AST transformer with multiple NULL replacements.
    """
    # Insert with multiple None parameters
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col, bool_col, real_col) VALUES ($1, $2, $3, $4, $5)",
        (2, None, None, True, None),
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify all None values were properly handled
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (2,))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["id"] == 2
    assert row["text_col"] is None
    assert row["int_col"] is None
    assert row["bool_col"] is True
    assert row["real_col"] is None


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_all_none_parameters(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC when all parameters are None except the required ones.

    This is a critical edge case that often reveals parameter handling bugs.
    """
    # Insert with all optional parameters as None
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col, bool_col, date_col, real_col, json_col) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        (3, None, None, None, None, None, None),
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify the row exists and all values are NULL
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (3,))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["id"] == 3
    assert row["text_col"] is None
    assert row["int_col"] is None
    assert row["bool_col"] is None
    assert row["date_col"] is None
    assert row["real_col"] is None
    assert row["json_col"] is None


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_in_where_clause(adbc_postgresql_session: AdbcDriver) -> None:
    """Test None parameters in WHERE clauses.

    This tests whether the AST transformer correctly handles None in queries.
    """
    # First, insert test data with and without NULL values
    test_data = [
        (10, "has_value", 100, True),
        (11, None, 200, False),
        (12, "another_value", None, True),
        (13, None, None, None),
    ]

    for row_data in test_data:
        adbc_postgresql_session.execute(
            "INSERT INTO test_none_handling (id, text_col, int_col, bool_col) VALUES ($1, $2, $3, $4)", row_data
        )

    # Query using None parameter with IS NULL comparison
    result = adbc_postgresql_session.execute(
        "SELECT * FROM test_none_handling WHERE text_col IS NULL AND id >= $1", (10,)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2  # IDs 11 and 13

    found_ids = {row["id"] for row in result.data}
    assert found_ids == {11, 13}


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_with_execute_many(adbc_postgresql_session: AdbcDriver) -> None:
    """Test None parameters work correctly with execute_many.

    This tests the ADBC execute_many path with None handling.
    """
    # Test data with various None combinations
    many_data = [
        (20, "batch1", 10, True, date(2025, 1, 21)),
        (21, None, 20, False, date(2025, 1, 22)),
        (22, "batch3", None, True, None),
        (23, None, None, None, None),
        (24, "batch5", 50, False, date(2025, 1, 25)),
    ]

    result = adbc_postgresql_session.execute_many(
        "INSERT INTO test_none_handling (id, text_col, int_col, bool_col, date_col) VALUES ($1, $2, $3, $4, $5)",
        many_data,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 5)

    # Verify all rows were inserted correctly
    verify_result = adbc_postgresql_session.execute(
        "SELECT * FROM test_none_handling WHERE id >= $1 ORDER BY id", (20,)
    )

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 5

    # Check specific None handling in the results
    rows = verify_result.data
    assert rows[1]["text_col"] is None and rows[1]["int_col"] == 20  # ID 21
    assert rows[2]["int_col"] is None and rows[2]["date_col"] is None  # ID 22
    assert rows[3]["text_col"] is None and rows[3]["int_col"] is None  # ID 23


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_parameter_reuse(adbc_postgresql_session: AdbcDriver) -> None:
    """Test None parameters when the same parameter is used multiple times.

    This tests the AST transformer with parameter reuse scenarios.
    """
    # Insert test data first
    adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)", (30, "reuse_test", 100)
    )

    # Query with parameter reuse where one instance might be None
    result = adbc_postgresql_session.execute(
        """
        SELECT * FROM test_none_handling
        WHERE (text_col = $1 OR $1 IS NULL)
        AND (int_col = $2 OR $2 IS NULL)
        AND id = $3
        """,
        (None, None, 30),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    # Should return the row because both conditions evaluate to TRUE
    # (NULL IS NULL is TRUE)
    assert len(result.data) == 1
    assert result.data[0]["id"] == 30


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_mixed_none_and_complex_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test None mixed with various data types.

    This tests ADBC's type coercion and None handling with different column types.
    """
    result = adbc_postgresql_session.execute(
        """
        INSERT INTO test_none_handling
        (id, text_col, int_col, bool_col, date_col, timestamp_col, real_col)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        (
            40,
            "complex_test",
            None,  # int_col as None
            True,
            date(2025, 1, 21),
            None,  # timestamp_col as None
            math.pi,
        ),
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify the complex insert worked
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (40,))

    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]

    assert row["text_col"] == "complex_test"
    assert row["int_col"] is None
    assert row["bool_col"] is True
    assert row["timestamp_col"] is None
    assert abs(row["real_col"] - math.pi) < 0.00001


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_parameter_count_validation_with_none(adbc_postgresql_session: AdbcDriver) -> None:
    """Test parameter count validation with None values.

    This tests the fix for the ADBC parameter validation bug where extra parameters
    were silently ignored when None values were present.
    """
    # Test: Too many parameters should raise an error
    with pytest.raises(Exception) as exc_info:
        adbc_postgresql_session.execute(
            "INSERT INTO test_none_handling (id, text_col) VALUES ($1, $2)",  # 2 placeholders
            (50, None, "extra_param"),  # 3 parameters
        )

    # Should be a parameter count mismatch error
    error_msg = str(exc_info.value).lower()
    assert "parameter" in error_msg and ("mismatch" in error_msg or "count" in error_msg)

    # Test: Too few parameters should raise an error
    with pytest.raises(Exception) as exc_info:
        adbc_postgresql_session.execute(
            "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)",  # 3 placeholders
            (51, None),  # Only 2 parameters
        )

    # Should be a parameter count mismatch error
    error_msg = str(exc_info.value).lower()
    assert "parameter" in error_msg and ("mismatch" in error_msg or "count" in error_msg)

    # Test: Correct count with None should work fine
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)", (52, None, None)
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify this one worked
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (52,))
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    assert verify_result.data[0]["id"] == 52


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_edge_cases(adbc_postgresql_session: AdbcDriver) -> None:
    """Test edge cases with None parameters that might reveal bugs.

    These are scenarios that commonly cause issues in ADBC implementations.
    """
    # Edge case 1: None as first parameter
    result1 = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)", (60, None, 100)
    )
    assert isinstance(result1, SQLResult)
    assert result1.rows_affected in (-1, 0, 1)

    # Edge case 2: None as last parameter
    result2 = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col) VALUES ($1, $2, $3)", (61, "test", None)
    )
    assert isinstance(result2, SQLResult)
    assert result2.rows_affected in (-1, 0, 1)

    # Edge case 3: Multiple consecutive None parameters
    result3 = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col, int_col, bool_col, real_col) VALUES ($1, $2, $3, $4, $5)",
        (62, None, None, None, 1.5),
    )
    assert isinstance(result3, SQLResult)
    assert result3.rows_affected in (-1, 0, 1)

    # Edge case 4: Single None parameter
    result4 = adbc_postgresql_session.execute(
        "INSERT INTO test_none_handling (id, text_col) VALUES ($1, $2)", (63, None)
    )
    assert isinstance(result4, SQLResult)
    assert result4.rows_affected in (-1, 0, 1)

    # Verify all edge cases worked
    count_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_none_handling WHERE id >= $1", (60,)
    )
    assert count_result.data[0]["count"] == 4


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_none_with_returning_clause(adbc_postgresql_session: AdbcDriver) -> None:
    """Test None parameters with PostgreSQL RETURNING clause.

    This tests a PostgreSQL-specific feature with None handling.
    """
    result = adbc_postgresql_session.execute(
        """
        INSERT INTO test_none_handling (id, text_col, int_col, bool_col)
        VALUES ($1, $2, $3, $4)
        RETURNING id, text_col, int_col, bool_col
        """,
        (70, None, 200, None),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    returned_row = result.data[0]
    assert returned_row["id"] == 70
    assert returned_row["text_col"] is None
    assert returned_row["int_col"] == 200
    assert returned_row["bool_col"] is None


@pytest.mark.xdist_group("sqlite")
@pytest.mark.adbc
@xfail_if_driver_missing
def test_adbc_sqlite_none_parameters() -> None:
    """Test None parameter handling with SQLite ADBC driver.

    SQLite has different parameter handling than PostgreSQL.
    """
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "adbc_driver_sqlite"})

    with config.provide_session() as session:
        # Create test table
        session.execute("""
            CREATE TABLE test_sqlite_none (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                flag BOOLEAN
            )
        """)

        # Test None parameters with SQLite (uses QMARK style)
        result = session.execute(
            "INSERT INTO test_sqlite_none (id, name, value, flag) VALUES (?, ?, ?, ?)", (1, None, None, True)
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected in (-1, 0, 1)  # ADBC SQLite may return -1

        # Verify the None values were handled correctly
        verify_result = session.execute("SELECT * FROM test_sqlite_none WHERE id = ?", (1,))

        assert isinstance(verify_result, SQLResult)
        assert verify_result.data is not None
        assert len(verify_result.data) == 1
        row = verify_result.data[0]
        assert row["name"] is None
        assert row["value"] is None
        assert row["flag"] in (True, 1)  # SQLite may return 1 for boolean True


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_parameter_style_consistency(adbc_postgresql_session: AdbcDriver) -> None:
    """Test that parameter handling is consistent across different query types.

    This ensures None handling works the same in SELECT, INSERT, UPDATE, DELETE.
    """
    # Setup: Insert test data
    setup_data = [(80, "original", 100, True), (81, "update_me", 200, False), (82, "delete_me", 300, True)]

    for row in setup_data:
        adbc_postgresql_session.execute(
            "INSERT INTO test_none_handling (id, text_col, int_col, bool_col) VALUES ($1, $2, $3, $4)", row
        )

    # Test SELECT with None parameter
    select_result = adbc_postgresql_session.execute(
        "SELECT * FROM test_none_handling WHERE bool_col = $1 OR $1 IS NULL", (None,)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    # Should return all rows since None IS NULL is TRUE
    assert len(select_result.data) >= 3

    # Test UPDATE with None parameters
    update_result = adbc_postgresql_session.execute(
        "UPDATE test_none_handling SET text_col = $1, int_col = $2 WHERE id = $3", (None, None, 81)
    )
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (-1, 0, 1)

    # Verify the update worked
    verify_update = adbc_postgresql_session.execute("SELECT * FROM test_none_handling WHERE id = $1", (81,))
    assert verify_update.data[0]["text_col"] is None
    assert verify_update.data[0]["int_col"] is None

    # Test DELETE with None parameter in WHERE clause
    delete_result = adbc_postgresql_session.execute(
        "DELETE FROM test_none_handling WHERE id = $1 AND ($2 IS NULL OR text_col = $2)", (82, None)
    )
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (-1, 0, 1)

    # Verify the delete worked
    verify_delete = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_none_handling WHERE id = $1", (82,)
    )
    assert verify_delete.data[0]["count"] == 0


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_parameter_count_validation_fixed(adbc_postgresql_session: AdbcDriver) -> None:
    """FIXED: ADBC AST transformer now properly validates parameter count.

    This test verifies the fix that prevents:
    1. Queries succeeding when they should fail due to parameter count mismatch
    2. Data corruption or silent data loss from ignored parameters
    3. Violations of parameter count validation principle
    4. Hard-to-debug issues from silently ignored parameters
    """
    # Setup test table
    adbc_postgresql_session.execute("""
        CREATE TEMP TABLE bug_test (
            id INTEGER PRIMARY KEY,
            col1 TEXT,
            col2 TEXT
        )
    """)

    # FIXED BEHAVIOR: This now correctly fails with parameter count mismatch
    with pytest.raises(SQLSpecError) as exc_info:
        adbc_postgresql_session.execute(
            "INSERT INTO bug_test (id, col1) VALUES ($1, $2)",  # 2 placeholders
            (1, None, "this_extra_param_should_cause_error"),  # 3 parameters - SHOULD FAIL!
        )

    # Verify we get the correct error message
    error_msg = str(exc_info.value).lower()
    assert "parameter count mismatch" in error_msg

    # Verify that correct parameter count works fine
    result = adbc_postgresql_session.execute(
        "INSERT INTO bug_test (id, col1) VALUES ($1, $2)",  # 2 placeholders
        (1, None),  # 2 parameters - SHOULD SUCCEED!
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected in (-1, 0, 1)

    # Verify the data was inserted correctly
    verify_result = adbc_postgresql_session.execute("SELECT * FROM bug_test WHERE id = $1", (1,))
    assert verify_result.data is not None
    assert len(verify_result.data) == 1
    row = verify_result.data[0]
    assert row["id"] == 1
    assert row["col1"] is None  # None was properly converted to NULL
    assert row["col2"] is None  # This column wasn't specified


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_ast_transformer_validation_fixed(adbc_postgresql_session: AdbcDriver) -> None:
    """FIXED: AST transformer now properly validates parameter count before transformation.

    This test verifies that the AST transformer now correctly rejects parameter count mismatches.
    """
    from sqlglot import parse_one

    # Create a test case with parameter count mismatch
    original_sql = "INSERT INTO bug_test (id, col1) VALUES ($1, $2)"
    original_params = (200, None, "extra_param")  # 3 params for 2 placeholders

    # Parse the SQL
    parsed = parse_one(original_sql, dialect="postgres")

    # FIXED: AST transformer now validates parameter count and rejects mismatches
    with pytest.raises(SQLSpecError) as exc_info:
        replace_null_parameters_with_literals(parsed, original_params, dialect="postgres")

    # Verify we get the correct error message
    error_msg = str(exc_info.value).lower()
    assert "parameter count mismatch" in error_msg
    assert "3 parameters provided but 2 placeholders" in error_msg

    # Verify that correct parameter count works fine
    correct_params = (200, None)  # 2 params for 2 placeholders
    modified_ast, cleaned_params = replace_null_parameters_with_literals(parsed, correct_params, dialect="postgres")

    # Convert back to SQL to see the transformation
    transformed_sql = modified_ast.sql(dialect="postgres")

    # Verify the transformation works correctly with proper parameter count
    assert len(correct_params) == 2  # We started with 2 params
    assert isinstance(cleaned_params, tuple)
    assert len(cleaned_params) == 1  # AST transformer reduced to 1 param (None converted to NULL)

    # The transformed SQL shows the None became NULL
    assert "NULL" in transformed_sql
    assert "$2" not in transformed_sql  # The $2 placeholder was replaced with NULL


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_adbc_repeated_parameter_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test that repeated parameters ($1 appearing multiple times) work correctly.

    This verifies the fix for parameter counting that now correctly handles
    PostgreSQL-style repeated parameter references.
    """
    # Setup test table
    adbc_postgresql_session.execute("""
        CREATE TEMP TABLE param_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    """)

    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO param_test (id, name, value) VALUES ($1, $2, $3)", (1, "test", 100))
    adbc_postgresql_session.execute("INSERT INTO param_test (id, name, value) VALUES ($1, $2, $3)", (2, None, 200))

    # Test query with repeated parameter - this should work correctly now
    result = adbc_postgresql_session.execute(
        """
        SELECT * FROM param_test
        WHERE (name = $1 OR ($1 IS NULL AND name IS NULL))
        """,
        (None,),  # 1 parameter used in 2 places
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1  # Should find the row with NULL name
    assert result.data[0]["id"] == 2
    assert result.data[0]["name"] is None
