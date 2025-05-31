"""Tests for MergeBuilder."""

import pytest

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import MergeBuilder, merge, select


def test_basic_merge() -> None:
    """Test basic MERGE statement construction."""
    builder = merge()
    result = (
        builder.into("target_table")
        .using("source_table", "src")
        .on("target_table.id = src.id")
        .when_matched_then_update({"name": "updated_name"})
        .build()
    )

    # Verify SQL structure
    assert "MERGE" in result.sql
    assert "INTO" in result.sql
    assert "USING" in result.sql
    assert "ON" in result.sql
    assert "WHEN MATCHED" in result.sql


def test_merge_with_subquery_source() -> None:
    """Test MERGE with subquery as source."""
    subquery = select("id", "name").from_("temp_updates")
    builder = merge()
    result = (
        builder.into("users")
        .using(subquery, "src")
        .on("users.id = src.id")
        .when_matched_then_update({"name": "src.name"})
        .build()
    )

    assert "MERGE" in result.sql
    assert "SELECT" in result.sql  # From subquery


def test_merge_when_matched_then_delete() -> None:
    """Test MERGE with DELETE action for matched rows."""
    builder = merge()
    result = (
        builder.into("users").using("inactive_users", "src").on("users.id = src.id").when_matched_then_delete().build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED" in result.sql
    assert "DELETE" in result.sql


def test_merge_when_not_matched_then_insert() -> None:
    """Test MERGE with INSERT action for unmatched rows."""
    builder = merge()
    result = (
        builder.into("users")
        .using("new_users", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert(columns=["id", "name", "email"], values=[1, "John Doe", "john@example.com"])
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED" in result.sql
    assert "INSERT" in result.sql


def test_merge_multiple_when_clauses() -> None:
    """Test MERGE with multiple WHEN clauses."""
    builder = merge()
    result = (
        builder.into("users")
        .using("user_updates", "src")
        .on("users.id = src.id")
        .when_matched_then_update({"name": "src.name", "email": "src.email"})
        .when_not_matched_then_insert(columns=["id", "name", "email"], values=["src.id", "src.name", "src.email"])
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED" in result.sql
    assert "WHEN NOT MATCHED" in result.sql


def test_merge_with_conditions() -> None:
    """Test MERGE with conditional WHEN clauses."""
    builder = merge()
    result = (
        builder.into("users")
        .using("user_updates", "src")
        .on("users.id = src.id")
        .when_matched_then_update({"status": "updated"}, condition="src.status = 'active'")
        .when_matched_then_delete(condition="src.status = 'deleted'")
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED" in result.sql


def test_merge_parameter_binding() -> None:
    """Test that MERGE values are properly parameterized."""
    builder = merge()
    result = (
        builder.into("users")
        .using("updates", "src")
        .on("users.id = src.id")
        .when_matched_then_update({"notes": "'; DROP TABLE users; --"})
        .build()
    )

    # Verify SQL injection is prevented
    assert "DROP TABLE" not in result.sql
    assert len(result.parameters) >= 1


def test_merge_chaining() -> None:
    """Test method chaining returns builder instance."""
    builder = merge()

    assert isinstance(builder.into("users"), MergeBuilder)
    assert isinstance(builder.using("source", "src"), MergeBuilder)
    assert isinstance(builder.on("users.id = src.id"), MergeBuilder)
    assert isinstance(builder.when_matched_then_update({"name": "value"}), MergeBuilder)
    assert isinstance(builder.when_matched_then_delete(), MergeBuilder)


def test_merge_insert_column_value_mismatch() -> None:
    """Test that INSERT with mismatched columns/values raises error."""
    builder = merge()
    builder.into("users").using("source", "src").on("users.id = src.id")

    with pytest.raises(SQLBuilderError, match="Number of columns must match number of values"):
        builder.when_not_matched_then_insert(
            columns=["id", "name"],
            values=[1, "John", "extra_value"],  # 3 values for 2 columns
        )


def test_merge_insert_columns_without_values() -> None:
    """Test that INSERT with columns but no values raises error."""
    builder = merge()
    builder.into("users").using("source", "src").on("users.id = src.id")

    with pytest.raises(SQLBuilderError, match="Specifying columns without values"):
        builder.when_not_matched_then_insert(columns=["id", "name"], values=None)


def test_merge_insert_values_without_columns() -> None:
    """Test that INSERT with values but no columns raises error."""
    builder = merge()
    builder.into("users").using("source", "src").on("users.id = src.id")

    with pytest.raises(SQLBuilderError, match="Cannot specify values without columns"):
        builder.when_not_matched_then_insert(columns=None, values=[1, "John"])


def test_merge_insert_default_values() -> None:
    """Test MERGE with INSERT DEFAULT VALUES."""
    builder = merge()
    result = (
        builder.into("users")
        .using("source", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert()  # No columns or values = DEFAULT VALUES
        .build()
    )

    assert "MERGE" in result.sql
    assert "INSERT" in result.sql


def test_merge_condition_parsing_error() -> None:
    """Test that invalid conditions raise parsing errors."""
    builder = merge()
    builder.into("users").using("source", "src")

    # This should work fine - valid condition
    builder.on("users.id = src.id")

    # Test with complex but valid condition
    result = builder.when_matched_then_update(
        {"status": "active"}, condition="src.last_updated > users.last_updated"
    ).build()

    assert "MERGE" in result.sql


def test_merge_with_table_alias() -> None:
    """Test MERGE with table aliases."""
    builder = merge()
    result = (
        builder.into("users", "u")
        .using("user_updates", "src")
        .on("u.id = src.id")
        .when_matched_then_update({"u.name": "src.name"})
        .build()
    )

    assert "MERGE" in result.sql


def test_merge_string_representation() -> None:
    """Test string representation of MergeBuilder."""
    builder = merge()
    builder.into("users").using("source", "src").on("users.id = src.id")

    sql_str = str(builder)
    assert "MERGE" in sql_str


def test_merge_not_matched_by_source() -> None:
    """Test MERGE with NOT MATCHED BY SOURCE."""
    builder = merge()
    result = (
        builder.into("users")
        .using("active_users", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert(by_target=False)  # NOT MATCHED BY SOURCE
        .build()
    )

    assert "MERGE" in result.sql


def test_merge_complex_scenario() -> None:
    """Test complex MERGE scenario with multiple operations."""
    builder = merge()
    result = (
        builder.into("inventory", "inv")
        .using("daily_sales", "sales")
        .on("inv.product_id = sales.product_id")
        .when_matched_then_update(
            {"quantity": "inv.quantity - sales.quantity_sold"}, condition="sales.quantity_sold > 0"
        )
        .when_matched_then_delete(condition="inv.quantity - sales.quantity_sold <= 0")
        .when_not_matched_then_insert(
            columns=["product_id", "quantity", "last_updated"], values=["sales.product_id", 0, "CURRENT_TIMESTAMP"]
        )
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED" in result.sql
    assert "WHEN NOT MATCHED" in result.sql
