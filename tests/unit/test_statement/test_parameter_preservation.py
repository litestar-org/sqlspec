"""Tests for parameter name preservation in SQL builders."""

import pytest

from sqlspec.statement.builder.ddl import CreateTableAsSelectBuilder
from sqlspec.statement.builder.select import SelectBuilder
from sqlspec.statement.sql import SQL, SQLConfig


def test_ctas_preserves_parameter_names() -> None:
    """Test that CTAS preserves original parameter names without _1 suffix."""
    # Create a SELECT query with parameters
    select_builder = SelectBuilder()
    select_builder.from_("users").where("active = :active AND status = :status")
    select_builder.add_parameter(True, name="active")
    select_builder.add_parameter("enabled", name="status")

    # Create CTAS from the SELECT
    ctas_builder = CreateTableAsSelectBuilder()
    ctas_builder.name("new_table").as_select(select_builder)

    # Build and check
    safe_query = ctas_builder.build()
    # Disable validation for DDL operations
    config = SQLConfig(enable_validation=False, strict_mode=False)
    sql_statement = SQL(safe_query.sql, parameters=safe_query.parameters, _config=config)

    # Parameters should preserve original names
    assert "active" in safe_query.parameters
    assert "status" in safe_query.parameters
    assert safe_query.parameters["active"] is True
    assert safe_query.parameters["status"] == "enabled"

    # SQL should contain original parameter names, not active_1
    sql_str = sql_statement.to_sql()
    assert ":active" in sql_str
    assert ":status" in sql_str
    assert ":active_1" not in sql_str
    assert ":status_1" not in sql_str


def test_ctas_handles_parameter_collision() -> None:
    """Test that CTAS handles parameter name collisions by using the later value."""
    # Create a CTAS with a parameter
    ctas_builder = CreateTableAsSelectBuilder()
    ctas_builder.name("new_table")
    ctas_builder.add_parameter("initial_value", name="test_param")

    # Add SELECT with same parameter name - this should override the previous value
    select_builder = SelectBuilder()
    select_builder.from_("users").where("name = :test_param")
    select_builder.add_parameter("select_value", name="test_param")

    # The select parameter should override the CTAS parameter
    ctas_builder.as_select(select_builder)

    safe_query = ctas_builder.build()

    # Should have the parameter with the select value (override behavior)
    assert "test_param" in safe_query.parameters
    assert safe_query.parameters["test_param"] == "select_value"
    # No collision suffix should be created
    assert "test_param_1" not in safe_query.parameters


def test_mixed_parameter_style_normalization() -> None:
    """Test mixed parameter style handling without unnecessary renaming."""
    # Create SQL with mixed styles - pass both positional and named params
    sql = SQL(
        "SELECT * FROM users WHERE id = ? AND status = :active", parameters=[123], active="enabled"
    )  # Use kwargs directly

    # Parameters should be properly handled
    params = sql.parameters
    assert isinstance(params, dict)

    # Check both the positional parameter (as arg_0) and named parameter
    assert "arg_0" in params
    assert params["arg_0"] == 123
    assert "active" in params
    assert params["active"] == "enabled"


@pytest.mark.xfail(reason="SQLglot modifies parameter names in complex queries - see SQLGLOT_PARAMETER_BUG.md")
def test_complex_ctas_with_ctes() -> None:
    """Test CTAS with CTEs preserves all parameter names."""
    # Create CTE with parameters
    cte_builder = SelectBuilder()
    cte_builder.from_("orders").where("created_at > :start_date")
    cte_builder.add_parameter("2024-01-01", name="start_date")

    # Create main query with different parameters
    main_builder = SelectBuilder()
    main_builder.with_cte("recent_orders", cte_builder)
    main_builder.from_("recent_orders").where("amount > :min_amount")
    main_builder.add_parameter(100, name="min_amount")

    # Create CTAS
    ctas_builder = CreateTableAsSelectBuilder()
    ctas_builder.name("summary_table").as_select(main_builder)

    safe_query = ctas_builder.build()

    # All parameters should be preserved
    assert "start_date" in safe_query.parameters
    assert "min_amount" in safe_query.parameters
    assert safe_query.parameters["start_date"] == "2024-01-01"
    assert safe_query.parameters["min_amount"] == 100

    # No unnecessary suffixes
    sql_str = safe_query.sql
    assert ":start_date" in sql_str
    assert ":min_amount" in sql_str
    assert ":start_date_1" not in sql_str
    assert ":min_amount_1" not in sql_str


def test_get_unique_parameter_name_with_namespace() -> None:
    """Test the enhanced get_unique_parameter_name method."""
    sql = SQL("SELECT 1")

    # Test without namespace - should preserve original
    name1 = sql.get_unique_parameter_name("test", preserve_original=True)
    assert name1 == "test"

    # Add the parameter
    sql = sql.add_named_parameter("test", "value1")

    # Test with namespace - should add namespace prefix
    name2 = sql.get_unique_parameter_name("test", namespace="cte", preserve_original=True)
    assert name2 == "cte_test"

    # Test collision - should add suffix
    sql = sql.add_named_parameter("cte_test", "value2")
    name3 = sql.get_unique_parameter_name("test", namespace="cte", preserve_original=True)
    assert name3 == "cte_test_1"


def test_builder_parameter_collision_resolution() -> None:
    """Test that builders handle parameter collisions gracefully."""
    builder = SelectBuilder()

    # Add first parameter
    builder.add_parameter("value1", name="param")

    # Try to add same name - should raise error
    with pytest.raises(Exception) as exc_info:
        builder.add_parameter("value2", name="param")
    assert "already exists" in str(exc_info.value)

    # Using _generate_unique_parameter_name should work
    unique_name = builder._generate_unique_parameter_name("param")
    assert unique_name == "param_1"
    builder.add_parameter("value2", name=unique_name)

    # Verify both exist
    assert builder._parameters["param"] == "value1"
    assert builder._parameters["param_1"] == "value2"
