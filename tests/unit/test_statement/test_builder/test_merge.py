"""Tests for MergeBuilder."""

import pytest

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import MergeBuilder, SelectBuilder


def test_basic_MergeBuilder() -> None:
    """Test basic MERGE statement construction."""
    builder = MergeBuilder()
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
    assert "WHEN MATCHED THEN UPDATE" in result.sql
    assert "SET" in result.sql

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)
    assert "updated_name" in result.parameters.values()


def test_merge_with_subquery_source() -> None:
    """Test MERGE with subquery as source."""
    subquery = SelectBuilder().select("id", "name").from_("temp_updates")
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using(subquery, "src")
        .on("users.id = src.id")
        .when_matched_then_update({"name": "src.name"})
        .build()
    )

    assert "MERGE" in result.sql
    assert "SELECT" in result.sql  # From subquery
    assert "WHEN MATCHED THEN UPDATE" in result.sql


def test_merge_when_matched_then_delete() -> None:
    """Test MERGE with DELETE action for matched rows."""
    builder = MergeBuilder()
    result = (
        builder.into("users").using("inactive_users", "src").on("users.id = src.id").when_matched_then_delete().build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED THEN DELETE" in result.sql


def test_merge_when_not_matched_then_insert() -> None:
    """Test MERGE with INSERT action for unmatched rows."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("new_users", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert(columns=["id", "name", "email"], values=[1, "John Doe", "john@example.com"])
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED THEN INSERT" in result.sql
    assert "VALUES" in result.sql

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)
    assert 1 in result.parameters.values()
    assert "John Doe" in result.parameters.values()
    assert "john@example.com" in result.parameters.values()


def test_merge_multiple_when_clauses() -> None:
    """Test MERGE with multiple WHEN clauses."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("user_updates", "src")
        .on("users.id = src.id")
        .when_matched_then_update({"name": "src.name", "email": "src.email"})
        .when_not_matched_then_insert(columns=["id", "name", "email"], values=["src.id", "src.name", "src.email"])
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED THEN UPDATE" in result.sql
    assert "WHEN NOT MATCHED THEN INSERT" in result.sql

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)


def test_merge_with_conditions() -> None:
    """Test MERGE with conditional WHEN clauses."""
    builder = MergeBuilder()
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

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)
    assert "updated" in result.parameters.values()


def test_merge_chaining() -> None:
    """Test method chaining returns builder instance."""
    builder = MergeBuilder()

    assert isinstance(builder.into("users"), MergeBuilder)
    assert isinstance(builder.using("source", "src"), MergeBuilder)
    assert isinstance(builder.on("users.id = src.id"), MergeBuilder)
    assert isinstance(builder.when_matched_then_update({"name": "value"}), MergeBuilder)
    assert isinstance(builder.when_matched_then_delete(), MergeBuilder)
    assert isinstance(builder.when_not_matched_then_insert(columns=["id", "name"], values=[1, "test"]), MergeBuilder)


def test_merge_insert_columns_values_mismatch() -> None:
    """Test that mismatched columns and values raise an error."""
    builder = MergeBuilder()
    with pytest.raises(SQLBuilderError, match="Number of columns must match number of values"):
        builder.into("users").using("source", "src").on("users.id = src.id").when_not_matched_then_insert(
            columns=["id", "name"],
            values=[1],  # Mismatch: 2 columns, 1 value
        )


def test_merge_insert_columns_without_values() -> None:
    """Test that specifying columns without values raises an error."""
    builder = MergeBuilder()
    with pytest.raises(SQLBuilderError, match="Specifying columns without values"):
        builder.into("users").using("source", "src").on("users.id = src.id").when_not_matched_then_insert(
            columns=["id", "name"]  # No values provided
        )


def test_merge_insert_values_without_columns() -> None:
    """Test that specifying values without columns raises an error."""
    builder = MergeBuilder()
    with pytest.raises(SQLBuilderError, match="Cannot specify values without columns"):
        builder.into("users").using("source", "src").on("users.id = src.id").when_not_matched_then_insert(
            values=[1, "test"]  # No columns provided
        )


def test_merge_insert_default_values() -> None:
    """Test MERGE with INSERT DEFAULT VALUES."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("source", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert()  # No columns or values = DEFAULT VALUES
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED THEN INSERT" in result.sql


def test_merge_condition_parsing_error() -> None:
    """Test that invalid conditions raise parsing errors."""
    builder = MergeBuilder()
    builder.into("users").using("source", "src")

    # This should work fine - valid condition
    builder.on("users.id = src.id")

    # Test with complex but valid condition
    result = builder.when_matched_then_update(
        {"status": "active"}, condition="src.last_updated > users.last_updated"
    ).build()

    assert "MERGE" in result.sql
    assert "WHEN MATCHED" in result.sql


def test_merge_with_table_alias() -> None:
    """Test MERGE with table aliases."""
    builder = MergeBuilder()
    result = (
        builder.into("users", "u")
        .using("user_updates", "src")
        .on("u.id = src.id")
        .when_matched_then_update({"u.name": "src.name"})
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN MATCHED THEN UPDATE" in result.sql


def test_merge_not_matched_by_source() -> None:
    """Test MERGE with NOT MATCHED BY SOURCE."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("active_users", "src")
        .on("users.id = src.id")
        .when_not_matched_then_insert(by_target=False)  # NOT MATCHED BY SOURCE
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED" in result.sql


def test_merge_complex_scenario() -> None:
    """Test complex MERGE scenario with multiple operations."""
    builder = MergeBuilder()
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
    assert "WHEN MATCHED THEN UPDATE" in result.sql
    assert "WHEN MATCHED THEN DELETE" in result.sql
    assert "WHEN NOT MATCHED THEN INSERT" in result.sql

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)


def test_merge_not_matched_by_source_update() -> None:
    """Test MERGE with UPDATE action for rows not matched by source."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("active_users", "src")
        .on("users.id = src.id")
        .when_not_matched_by_source_then_update({"status": "inactive"})
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED" in result.sql
    assert "UPDATE" in result.sql

    # Verify parameters are captured
    assert isinstance(result.parameters, dict)
    assert "inactive" in result.parameters.values()


def test_merge_not_matched_by_source_delete() -> None:
    """Test MERGE with DELETE action for rows not matched by source."""
    builder = MergeBuilder()
    result = (
        builder.into("users")
        .using("active_users", "src")
        .on("users.id = src.id")
        .when_not_matched_by_source_then_delete()
        .build()
    )

    assert "MERGE" in result.sql
    assert "WHEN NOT MATCHED" in result.sql
    assert "DELETE" in result.sql
