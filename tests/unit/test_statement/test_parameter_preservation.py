"""Tests for parameter name preservation in SQL builders."""

import pytest

from sqlspec.statement.builder._ddl import CreateTableAsSelect
from sqlspec.statement.builder._select import Select
from sqlspec.statement.sql import SQL, SQLConfig


def test_ctas_preserves_parameter_names() -> None:
    """Test that CTAS preserves original parameter names without _1 suffix."""
    # Create a SELECT query with parameters
    select_builder = Select()
    select_builder.from_("users").where("active = :active AND status = :status")
    select_builder.add_parameter(True, name="active")
    select_builder.add_parameter("enabled", name="status")

    # Create CTAS from the SELECT
    ctas_builder = CreateTableAsSelect()
    ctas_builder.name("new_table").as_select(select_builder)

    # Build and check
    safe_query = ctas_builder.build()
    # Disable validation for DDL operations
    config = SQLConfig(enable_validation=False)
    sql_statement = SQL(safe_query.sql, parameters=safe_query.parameters, config=config)

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
    ctas_builder = CreateTableAsSelect()
    ctas_builder.name("new_table")
    ctas_builder.add_parameter("initial_value", name="test_param")

    # Add SELECT with same parameter name - this should override the previous value
    select_builder = Select()
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
    # When both positional and named parameters are present,
    # the SQL class merges them into a single dictionary
    sql = SQL("SELECT * FROM users WHERE id = ? AND status = :active", 123, active="enabled")

    # Parameters returns merged dict when both positional and named are present
    params = sql.parameters
    assert isinstance(params, dict)
    assert params["active"] == "enabled"
    assert params["arg_0"] == 123  # Positional param gets assigned a name

    # Test just positional - returns tuple
    sql2 = SQL("SELECT * FROM users WHERE id = ?", 123)
    assert sql2.parameters == (123,)

    # Test just named - returns dict
    sql3 = SQL("SELECT * FROM users WHERE status = :active", active="enabled")
    assert sql3.parameters == {"active": "enabled"}


def test_complex_ctas_with_ctes() -> None:
    """Test CTAS with CTEs preserves all parameter names."""
    # Create CTE with parameters
    cte_builder = Select()
    cte_builder.select("*").from_("orders").where("created_at > :start_date")
    cte_builder.add_parameter("2024-01-01", name="start_date")

    # Create main query with different parameters
    main_builder = Select()
    main_builder.with_cte("recent_orders", cte_builder)
    main_builder.select("*").from_("recent_orders").where("amount > :min_amount")
    main_builder.add_parameter(100, name="min_amount")

    # Create CTAS
    ctas_builder = CreateTableAsSelect()
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


def test_builder_parameter_collision_resolution() -> None:
    """Test that builders handle parameter collisions gracefully."""
    builder = Select()

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
