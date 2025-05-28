"""Tests for DeleteBuilder."""

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder import DeleteBuilder, delete


class TestDeleteBuilder:
    """Test cases for DeleteBuilder."""

    def test_basic_delete(self) -> None:
        """Test basic DELETE statement construction."""
        builder = delete()
        result = builder.from_("users").build()

        # Verify SQL structure
        assert "DELETE" in result.sql
        assert "users" in result.sql

    def test_delete_with_where(self) -> None:
        """Test DELETE with WHERE clause."""
        builder = delete()
        result = builder.from_("users").where("age < 18").build()

        assert "DELETE" in result.sql
        assert "WHERE" in result.sql

    def test_delete_with_where_eq(self) -> None:
        """Test DELETE with parameterized WHERE equality."""
        builder = delete()
        result = builder.from_("users").where_eq("id", 123).build()

        assert "DELETE" in result.sql
        assert "WHERE" in result.sql
        assert len(result.parameters) == 1
        assert 123 in result.parameters.values()

    def test_delete_with_where_in(self) -> None:
        """Test DELETE with WHERE IN clause."""
        builder = delete()
        result = builder.from_("users").where_in("status", ["inactive", "banned"]).build()

        assert "DELETE" in result.sql
        assert "WHERE" in result.sql
        assert "IN" in result.sql
        assert len(result.parameters) == 2

    def test_delete_with_joins(self) -> None:
        """Test DELETE with JOIN clauses."""
        builder = delete()
        result = (
            builder.from_("users")
            .inner_join("orders", "users.id = orders.user_id")
            .where("orders.status = 'cancelled'")
            .build()
        )

        assert "DELETE" in result.sql
        assert "JOIN" in result.sql

    def test_delete_parameter_binding(self) -> None:
        """Test that values are properly parameterized."""
        builder = delete()
        result = builder.from_("users").where_eq("name", "'; DROP TABLE users; --").build()

        # Verify SQL injection is prevented
        assert "DROP TABLE" not in result.sql
        assert len(result.parameters) == 1

    def test_delete_with_expression_column(self) -> None:
        """Test DELETE with sqlglot expression as column."""
        builder = delete()
        col_expr = exp.column("status")
        result = builder.from_("users").where_eq(col_expr, "deleted").build()

        assert "DELETE" in result.sql
        assert len(result.parameters) == 1

    def test_delete_join_types(self) -> None:
        """Test different JOIN types in DELETE."""
        builder = delete()

        # Test LEFT JOIN
        result1 = (
            builder.from_("users")
            .left_join("profiles", "users.id = profiles.user_id")
            .where("profiles.status = 'inactive'")
            .build()
        )
        assert "LEFT JOIN" in result1.sql

        # Test RIGHT JOIN
        builder2 = delete()
        result2 = (
            builder2.from_("users")
            .right_join("profiles", "users.id = profiles.user_id")
            .where("profiles.status = 'inactive'")
            .build()
        )
        assert "RIGHT JOIN" in result2.sql

    def test_delete_chaining(self) -> None:
        """Test method chaining returns builder instance."""
        builder = delete()

        assert isinstance(builder.from_("users"), DeleteBuilder)
        assert isinstance(builder.where("id = 1"), DeleteBuilder)
        assert isinstance(builder.where_eq("status", "active"), DeleteBuilder)
        assert isinstance(builder.where_in("id", [1, 2, 3]), DeleteBuilder)
        assert isinstance(builder.inner_join("orders", "users.id = orders.user_id"), DeleteBuilder)

    def test_delete_multiple_where_conditions(self) -> None:
        """Test DELETE with multiple WHERE conditions."""
        builder = delete()
        result = (
            builder.from_("users")
            .where("age < 18")
            .where_eq("status", "inactive")
            .where_in("role", ["guest", "temp"])
            .build()
        )

        assert "DELETE" in result.sql
        assert "WHERE" in result.sql
        # Should have parameters for the parameterized conditions
        assert len(result.parameters) >= 3

    def test_delete_without_from_raises_error_on_join(self) -> None:
        """Test that JOIN without FROM raises error."""
        builder = delete()

        with pytest.raises(SQLBuilderError, match="Cannot add JOIN to DELETE without a FROM clause"):
            builder.inner_join("orders", "users.id = orders.user_id")

    def test_delete_error_on_non_delete_expression(self) -> None:
        """Test that methods raise errors on non-DELETE expressions."""
        builder = DeleteBuilder()
        builder._expression = exp.Select()  # Set wrong expression type

        with pytest.raises(SQLBuilderError, match="Cannot add WHERE clause to non-DELETE expression"):
            builder.where("id = 1")

        with pytest.raises(SQLBuilderError, match="Cannot add JOIN clause to non-DELETE expression"):
            builder.inner_join("orders", "users.id = orders.user_id")

    def test_delete_string_representation(self) -> None:
        """Test string representation of DeleteBuilder."""
        builder = delete()
        builder.from_("users")

        sql_str = str(builder)
        assert "DELETE" in sql_str
        assert "users" in sql_str

    def test_delete_with_complex_conditions(self) -> None:
        """Test DELETE with complex WHERE conditions."""
        builder = delete()
        result = builder.from_("users").where("created_at < '2020-01-01' AND last_login IS NULL").build()

        assert "DELETE" in result.sql
        assert "WHERE" in result.sql
        assert "AND" in result.sql

    def test_delete_table_storage(self) -> None:
        """Test that table name is stored internally."""
        builder = delete()
        builder.from_("users")

        assert builder._table == "users"

    def test_delete_condition_chaining(self) -> None:
        """Test that WHERE conditions are properly chained."""
        builder = delete()
        result = builder.from_("users").where_eq("status", "inactive").where("age > 65").build()

        # Both conditions should be present
        assert "WHERE" in result.sql
        # Should have at least one parameter from where_eq
        assert len(result.parameters) >= 1

    def test_delete_where_in_with_tuples(self) -> None:
        """Test DELETE with WHERE IN using tuples."""
        builder = delete()
        result = builder.from_("users").where_in("id", (1, 2, 3, 4, 5)).build()

        assert "DELETE" in result.sql
        assert "IN" in result.sql
        assert len(result.parameters) == 5

    def test_delete_empty_where_in_list(self) -> None:
        """Test DELETE with empty WHERE IN list."""
        builder = delete()
        result = builder.from_("users").where_in("id", []).build()

        assert "DELETE" in result.sql
        # Empty IN clause should still be valid SQL
        assert len(result.parameters) == 0
