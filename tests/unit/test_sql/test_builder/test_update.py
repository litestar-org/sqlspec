"""Tests for UpdateBuilder."""

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder import UpdateBuilder, update


class TestUpdateBuilder:
    """Test cases for UpdateBuilder."""

    def test_basic_update(self) -> None:
        """Test basic UPDATE statement construction."""
        builder = update("users")
        result = builder.set("name", "John").set("age", 25).build()

        # Verify SQL structure
        assert "UPDATE" in result.sql
        assert "users" in result.sql
        assert "SET" in result.sql

        # Verify parameters
        assert len(result.parameters) == 2
        assert "John" in result.parameters.values()
        assert 25 in result.parameters.values()

    def test_update_with_where(self) -> None:
        """Test UPDATE with WHERE clause."""
        builder = update("users")
        result = builder.set("name", "Jane").where("id = 1").build()

        assert "UPDATE" in result.sql
        assert "SET" in result.sql
        assert "WHERE" in result.sql
        assert len(result.parameters) == 1

    def test_update_with_joins(self) -> None:
        """Test UPDATE with JOIN clauses."""
        builder = update("users")
        result = (
            builder.set("name", "Updated")
            .inner_join("orders", "users.id = orders.user_id")
            .where("orders.status = 'pending'")
            .build()
        )

        assert "UPDATE" in result.sql
        assert "JOIN" in result.sql

    def test_update_multiple_sets(self) -> None:
        """Test UPDATE with multiple SET clauses."""
        builder = update("users")
        result = builder.set("name", "John").set("email", "john@example.com").set("age", 30).where("id = 1").build()

        assert "SET" in result.sql
        assert len(result.parameters) == 3

    def test_update_with_from_clause(self) -> None:
        """Test UPDATE with FROM clause (PostgreSQL style)."""
        builder = update("users")
        result = (
            builder.set("status", "active").from_("user_profiles").where("users.id = user_profiles.user_id").build()
        )

        assert "UPDATE" in result.sql
        assert "FROM" in result.sql

    def test_update_without_table_raises_error(self) -> None:
        """Test that UPDATE without table raises error on build."""
        builder = UpdateBuilder()

        with pytest.raises(SQLBuilderError):
            builder.set("name", "John").build()

    def test_update_parameter_binding(self) -> None:
        """Test that values are properly parameterized."""
        builder = update("users")
        result = builder.set("data", "'; DROP TABLE users; --").build()

        # Verify SQL injection is prevented
        assert "DROP TABLE" not in result.sql
        assert len(result.parameters) == 1

    def test_update_with_expression_column(self) -> None:
        """Test UPDATE with sqlglot expression as column."""
        builder = update("users")
        col_expr = exp.column("name")
        result = builder.set(col_expr, "John").build()

        assert "UPDATE" in result.sql
        assert len(result.parameters) == 1

    def test_update_join_types(self) -> None:
        """Test different JOIN types in UPDATE."""
        builder = update("users")

        # Test LEFT JOIN
        result1 = builder.set("status", "inactive").left_join("profiles", "users.id = profiles.user_id").build()
        assert "LEFT JOIN" in result1.sql

        # Test RIGHT JOIN
        builder2 = update("users")
        result2 = builder2.set("status", "inactive").right_join("profiles", "users.id = profiles.user_id").build()
        assert "RIGHT JOIN" in result2.sql

    def test_update_table_method(self) -> None:
        """Test setting table via table() method."""
        builder = UpdateBuilder()
        result = builder.table("users").set("name", "John").build()

        assert "UPDATE" in result.sql
        assert "users" in result.sql

    def test_update_chaining(self) -> None:
        """Test method chaining returns builder instance."""
        builder = update("users")

        assert isinstance(builder.set("name", "John"), UpdateBuilder)
        assert isinstance(builder.where("id = 1"), UpdateBuilder)
        assert isinstance(builder.inner_join("orders", "users.id = orders.user_id"), UpdateBuilder)

    def test_update_with_complex_where(self) -> None:
        """Test UPDATE with complex WHERE conditions."""
        builder = update("users")
        result = builder.set("status", "updated").where("age > 18 AND created_at < '2023-01-01'").build()

        assert "WHERE" in result.sql
        assert "AND" in result.sql

    def test_update_error_on_non_update_expression(self) -> None:
        """Test that methods raise errors on non-UPDATE expressions."""
        builder = UpdateBuilder()
        builder._expression = exp.Select()  # Set wrong expression type

        with pytest.raises(SQLBuilderError, match="Cannot set table for a non-UPDATE expression"):
            builder.table("users")

        with pytest.raises(SQLBuilderError, match="Cannot add SET clause to non-UPDATE expression"):
            builder.set("name", "John")

        with pytest.raises(SQLBuilderError, match="Cannot add WHERE clause to non-UPDATE expression"):
            builder.where("id = 1")

        with pytest.raises(SQLBuilderError, match="Cannot add JOIN clause to non-UPDATE expression"):
            builder.inner_join("orders", "users.id = orders.user_id")

    def test_update_string_representation(self) -> None:
        """Test string representation of UpdateBuilder."""
        builder = update("users")
        builder.set("name", "John")

        sql_str = str(builder)
        assert "UPDATE" in sql_str
        assert "users" in sql_str
