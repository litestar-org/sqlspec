"""Test for parameter duplication bug fix in ParameterizeLiterals."""

from sqlspec.statement import SQL
from sqlspec.statement.sql import SQLConfig


def test_no_duplication_with_reordering() -> None:
    """Test that parameters are not duplicated when reordering is needed.

    This tests the fix for the issue where parameters were being added to both
    extracted_parameters_from_pipeline and merged_parameters when
    input_sql_had_placeholders was True, causing duplication during merge.
    """
    # Create config with input_sql_had_placeholders=True to trigger reordering
    config = SQLConfig(
        enable_transformations=True,
        default_parameter_style="?",
        input_sql_had_placeholders=True,  # This triggers the reordering logic
    )

    # SQL with literals that will be parameterized
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('script_test1', 'script_test2') ORDER BY name"

    # Create and compile the SQL
    stmt = SQL(sql, _config=config)
    compiled_sql, params = stmt.compile()

    # Verify no duplication
    assert isinstance(params, list)
    assert len(params) == 3, f"Expected 3 parameters, got {len(params)}: {params}"
    assert params == ["table", "script_test1", "script_test2"]

    # Verify SQL is correct
    assert compiled_sql == "SELECT name FROM sqlite_master WHERE type = ? AND name IN (?, ?) ORDER BY name"


def test_normal_operation_without_reordering() -> None:
    """Test that normal operation (without reordering) still works correctly."""
    # Create config without input_sql_had_placeholders
    config = SQLConfig(
        enable_transformations=True,
        default_parameter_style="?",
        # input_sql_had_placeholders defaults to False
    )

    # SQL with literals that will be parameterized
    sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'"

    # Create and compile the SQL
    stmt = SQL(sql, _config=config)
    compiled_sql, params = stmt.compile()

    # Verify parameters are extracted correctly
    assert isinstance(params, list)
    assert len(params) == 2
    assert params == [18, "active"]

    # Verify SQL is correct
    assert compiled_sql == "SELECT * FROM users WHERE age > ? AND status = ?"


def test_reordering_with_existing_parameters() -> None:
    """Test reordering when SQL already has user-provided parameters."""
    # First process a SQL with placeholders to set input_sql_had_placeholders
    config = SQLConfig(enable_transformations=True, default_parameter_style="?")

    # Process SQL with existing placeholders
    sql1 = "SELECT * FROM users WHERE id = ?"
    stmt1 = SQL(sql1, 123, _config=config)
    stmt1.compile()

    # Config should now have input_sql_had_placeholders=True
    assert config.input_sql_had_placeholders is True

    # Now process SQL with literals using the same config
    sql2 = "SELECT * FROM products WHERE price > 100 AND category = 'electronics'"
    stmt2 = SQL(sql2, _config=config)
    compiled_sql2, params2 = stmt2.compile()

    # Verify no duplication
    assert isinstance(params2, list)
    assert len(params2) == 2
    assert params2 == [100, "electronics"]

    # Verify SQL is correct
    assert compiled_sql2 == "SELECT * FROM products WHERE price > ? AND category = ?"


def test_mixed_parameters_and_literals() -> None:
    """Test SQL with both user parameters and literals to be parameterized."""
    config = SQLConfig(enable_transformations=True, default_parameter_style="?")

    # SQL with both placeholders and literals
    sql = "SELECT * FROM orders WHERE user_id = ? AND status IN ('pending', 'processing') AND amount > 50.0"
    stmt = SQL(sql, 456, _config=config)
    compiled_sql, params = stmt.compile()

    # Verify all parameters are included correctly
    assert isinstance(params, list)
    assert len(params) == 4
    assert params == [456, "pending", "processing", 50.0]

    # Verify SQL is correct
    assert compiled_sql == "SELECT * FROM orders WHERE user_id = ? AND status IN (?, ?) AND amount > ?"
