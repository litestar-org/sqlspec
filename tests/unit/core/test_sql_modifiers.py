"""Unit tests for SQL class query modification methods.

This module tests the new parameterized WHERE methods, pagination methods,
and select_only functionality added to the SQL class.
"""

import pytest

from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("core")


# =============================================================================
# WHERE Method Tests
# =============================================================================


class TestSQLWhereEq:
    """Tests for SQL.where_eq method."""

    def test_where_eq_creates_equality_condition(self) -> None:
        """Test that where_eq creates WHERE column = :param."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_eq("status", "active")

        assert "WHERE" in modified.raw_sql
        assert "status" in modified.raw_sql
        assert "_sqlspec_status" in modified.named_parameters
        assert modified.named_parameters["_sqlspec_status"] == "active"

    def test_where_eq_preserves_original(self) -> None:
        """Test that where_eq returns new instance without modifying original."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_eq("status", "active")

        assert "WHERE" not in stmt.raw_sql
        assert len(stmt.named_parameters) == 0
        assert "WHERE" in modified.raw_sql

    def test_where_eq_chains_with_and(self) -> None:
        """Test that chained where_eq uses AND."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_eq("status", "active").where_eq("role", "admin")

        assert "AND" in modified.raw_sql
        assert "_sqlspec_status" in modified.named_parameters
        assert "_sqlspec_role" in modified.named_parameters


class TestSQLWhereNeq:
    """Tests for SQL.where_neq method."""

    def test_where_neq_creates_not_equal_condition(self) -> None:
        """Test that where_neq creates WHERE column != :param."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_neq("status", "deleted")

        assert "WHERE" in modified.raw_sql
        assert "<>" in modified.raw_sql or "!=" in modified.raw_sql
        assert "_sqlspec_status" in modified.named_parameters
        assert modified.named_parameters["_sqlspec_status"] == "deleted"


class TestSQLWhereComparisons:
    """Tests for SQL comparison WHERE methods."""

    def test_where_lt(self) -> None:
        """Test where_lt creates less-than condition."""
        stmt = SQL("SELECT * FROM products")

        modified = stmt.where_lt("price", 100)

        assert "WHERE" in modified.raw_sql
        assert "<" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_price"] == 100

    def test_where_lte(self) -> None:
        """Test where_lte creates less-than-or-equal condition."""
        stmt = SQL("SELECT * FROM products")

        modified = stmt.where_lte("price", 100)

        assert "WHERE" in modified.raw_sql
        assert "<=" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_price"] == 100

    def test_where_gt(self) -> None:
        """Test where_gt creates greater-than condition."""
        stmt = SQL("SELECT * FROM products")

        modified = stmt.where_gt("price", 50)

        assert "WHERE" in modified.raw_sql
        assert ">" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_price"] == 50

    def test_where_gte(self) -> None:
        """Test where_gte creates greater-than-or-equal condition."""
        stmt = SQL("SELECT * FROM products")

        modified = stmt.where_gte("price", 50)

        assert "WHERE" in modified.raw_sql
        assert ">=" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_price"] == 50


class TestSQLWhereLike:
    """Tests for SQL LIKE WHERE methods."""

    def test_where_like(self) -> None:
        """Test where_like creates LIKE condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_like("name", "%john%")

        assert "WHERE" in modified.raw_sql
        assert "LIKE" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_name"] == "%john%"

    def test_where_ilike(self) -> None:
        """Test where_ilike creates ILIKE condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_ilike("name", "%john%")

        assert "WHERE" in modified.raw_sql
        assert "ILIKE" in modified.raw_sql
        assert modified.named_parameters["_sqlspec_name"] == "%john%"


class TestSQLWhereNull:
    """Tests for SQL NULL check WHERE methods."""

    def test_where_is_null(self) -> None:
        """Test where_is_null creates IS NULL condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_is_null("deleted_at")

        assert "WHERE" in modified.raw_sql
        assert "IS NULL" in modified.raw_sql
        # IS NULL doesn't add a parameter
        assert len(modified.named_parameters) == 0

    def test_where_is_not_null(self) -> None:
        """Test where_is_not_null creates IS NOT NULL condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_is_not_null("email")

        assert "WHERE" in modified.raw_sql
        assert "NOT" in modified.raw_sql
        assert "NULL" in modified.raw_sql
        # IS NOT NULL doesn't add a parameter
        assert len(modified.named_parameters) == 0


class TestSQLWhereIn:
    """Tests for SQL IN WHERE methods."""

    def test_where_in_creates_in_clause(self) -> None:
        """Test where_in creates IN condition with multiple placeholders."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_in("status", ["active", "pending", "review"])

        assert "WHERE" in modified.raw_sql
        assert "IN" in modified.raw_sql
        # Check parameters were created for each value
        assert len(modified.named_parameters) == 3
        values = list(modified.named_parameters.values())
        assert "active" in values
        assert "pending" in values
        assert "review" in values

    def test_where_in_empty_returns_false_condition(self) -> None:
        """Test where_in with empty list returns false condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_in("status", [])

        assert "WHERE" in modified.raw_sql
        assert "1 = 0" in modified.raw_sql
        assert len(modified.named_parameters) == 0


class TestSQLWhereNotIn:
    """Tests for SQL NOT IN WHERE methods."""

    def test_where_not_in_creates_not_in_clause(self) -> None:
        """Test where_not_in creates NOT IN condition."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_not_in("status", ["deleted", "banned"])

        assert "WHERE" in modified.raw_sql
        assert "NOT" in modified.raw_sql
        assert "IN" in modified.raw_sql
        assert len(modified.named_parameters) == 2

    def test_where_not_in_empty_returns_unchanged(self) -> None:
        """Test where_not_in with empty list returns original statement."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_not_in("status", [])

        # Empty NOT IN is always true, so we return unchanged
        assert modified is stmt


class TestSQLWhereBetween:
    """Tests for SQL BETWEEN WHERE method."""

    def test_where_between_creates_between_condition(self) -> None:
        """Test where_between creates BETWEEN condition."""
        stmt = SQL("SELECT * FROM orders")

        modified = stmt.where_between("total", 100, 500)

        assert "WHERE" in modified.raw_sql
        assert "BETWEEN" in modified.raw_sql
        assert "AND" in modified.raw_sql
        # Check both bounds are parameterized
        assert "_sqlspec_total_low" in modified.named_parameters
        assert "_sqlspec_total_high" in modified.named_parameters
        assert modified.named_parameters["_sqlspec_total_low"] == 100
        assert modified.named_parameters["_sqlspec_total_high"] == 500


# =============================================================================
# Pagination Method Tests
# =============================================================================


class TestSQLLimit:
    """Tests for SQL.limit method."""

    def test_limit_adds_limit_clause(self) -> None:
        """Test that limit adds LIMIT clause."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.limit(10)

        assert "LIMIT 10" in modified.raw_sql

    def test_limit_preserves_where(self) -> None:
        """Test that limit preserves existing WHERE clause."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_eq("status", "active").limit(10)

        assert "WHERE" in modified.raw_sql
        assert "LIMIT 10" in modified.raw_sql


class TestSQLOffset:
    """Tests for SQL.offset method."""

    def test_offset_adds_offset_clause(self) -> None:
        """Test that offset adds OFFSET clause."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.offset(20)

        assert "OFFSET 20" in modified.raw_sql


class TestSQLPaginate:
    """Tests for SQL.paginate method."""

    def test_paginate_adds_limit_and_offset(self) -> None:
        """Test that paginate adds both LIMIT and OFFSET."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.paginate(page=3, page_size=20)

        assert "LIMIT 20" in modified.raw_sql
        assert "OFFSET 40" in modified.raw_sql

    def test_paginate_first_page(self) -> None:
        """Test paginate with page 1 has zero offset."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.paginate(page=1, page_size=10)

        assert "LIMIT 10" in modified.raw_sql
        assert "OFFSET 0" in modified.raw_sql

    def test_paginate_rejects_zero_or_negative_page(self) -> None:
        """Test paginate rejects page values less than 1."""
        stmt = SQL("SELECT * FROM users")

        with pytest.raises(SQLSpecError):
            stmt.paginate(page=0, page_size=10)

        with pytest.raises(SQLSpecError):
            stmt.paginate(page=-1, page_size=10)

    def test_paginate_rejects_non_positive_page_size(self) -> None:
        """Test paginate rejects page_size values less than 1."""
        stmt = SQL("SELECT * FROM users")

        with pytest.raises(SQLSpecError):
            stmt.paginate(page=1, page_size=0)

        with pytest.raises(SQLSpecError):
            stmt.paginate(page=1, page_size=-5)


# =============================================================================
# Column Projection Tests
# =============================================================================


class TestSQLSelectOnly:
    """Tests for SQL.select_only method."""

    def test_select_only_replaces_columns(self) -> None:
        """Test that select_only replaces SELECT columns."""
        stmt = SQL("SELECT * FROM users WHERE active = 1")

        modified = stmt.select_only("id", "name", "email")

        assert "id" in modified.raw_sql
        assert "name" in modified.raw_sql
        assert "email" in modified.raw_sql
        # Original * should be gone, WHERE preserved
        assert "*" not in modified.raw_sql
        assert "WHERE" in modified.raw_sql

    def test_select_only_empty_returns_unchanged(self) -> None:
        """Test that select_only with no columns returns unchanged."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.select_only()

        assert modified is stmt

    def test_select_only_preserves_conditions(self) -> None:
        """Test select_only preserves WHERE and other clauses."""
        stmt = SQL("SELECT * FROM users")

        modified = stmt.where_eq("status", "active").order_by("name").select_only("id", "name")

        assert "WHERE" in modified.raw_sql
        assert "ORDER BY" in modified.raw_sql
        assert "id" in modified.raw_sql
        assert "name" in modified.raw_sql


# =============================================================================
# Parameter Generation Tests
# =============================================================================


class TestParameterGeneration:
    """Tests for unique parameter name generation."""

    def test_same_column_generates_unique_params(self) -> None:
        """Test that using same column twice generates unique parameter names."""
        stmt = SQL("SELECT * FROM users")

        # Two where_eq on same column
        modified = stmt.where_eq("status", "active").where_eq("status", "pending")

        # Should have two different parameter names
        assert len(modified.named_parameters) == 2
        params = list(modified.named_parameters.keys())
        assert params[0] != params[1]

    def test_params_dont_collide_with_user_params(self) -> None:
        """Test auto-generated params don't collide with user-provided params."""
        stmt = SQL("SELECT * FROM users WHERE id = :status", {"status": 1})

        # where_eq should not overwrite user's :status parameter
        modified = stmt.where_eq("status", "active")

        # Original user param should be preserved
        assert "status" in modified.named_parameters
        assert modified.named_parameters["status"] == 1
        # Auto-generated param should have _sqlspec_ prefix
        assert "_sqlspec_status" in modified.named_parameters


# =============================================================================
# CTE Preservation Tests
# =============================================================================


class TestCTEPreservation:
    """Tests for CTE preservation during modifications."""

    def test_where_eq_preserves_cte(self) -> None:
        """Test that where_eq preserves CTE in query."""
        # Query with CTE
        stmt = SQL(
            """
            WITH active_users AS (
                SELECT * FROM users WHERE active = 1
            )
            SELECT * FROM active_users
            """
        )

        modified = stmt.where_eq("name", "John")

        # CTE should still be present
        assert "WITH" in modified.raw_sql
        assert "active_users" in modified.raw_sql
        assert "WHERE" in modified.raw_sql

    def test_limit_preserves_cte(self) -> None:
        """Test that limit preserves CTE in query."""
        stmt = SQL(
            """
            WITH top_orders AS (
                SELECT * FROM orders ORDER BY total DESC
            )
            SELECT * FROM top_orders
            """
        )

        modified = stmt.limit(10)

        assert "WITH" in modified.raw_sql
        assert "top_orders" in modified.raw_sql
        assert "LIMIT 10" in modified.raw_sql


# =============================================================================
# Method Chaining Tests
# =============================================================================


class TestMethodChaining:
    """Tests for fluent method chaining."""

    def test_complex_chain(self) -> None:
        """Test complex method chain produces correct SQL."""
        stmt = SQL("SELECT * FROM orders")

        modified = (
            stmt
            .where_eq("customer_id", 123)
            .where_gte("total", 100)
            .where_lt("total", 1000)
            .where_in("status", ["pending", "processing"])
            .limit(50)
            .offset(100)
        )

        # Check all clauses present
        assert "WHERE" in modified.raw_sql
        assert "customer_id" in modified.raw_sql
        assert "total" in modified.raw_sql
        assert "IN" in modified.raw_sql
        assert "LIMIT 50" in modified.raw_sql
        assert "OFFSET 100" in modified.raw_sql

        # Check parameter count (1 + 1 + 1 + 2 = 5 parameters)
        assert len(modified.named_parameters) == 5

    def test_immutability_in_chain(self) -> None:
        """Test that chaining doesn't modify intermediate results."""
        stmt = SQL("SELECT * FROM users")
        step1 = stmt.where_eq("status", "active")
        step2 = step1.limit(10)
        step3 = step2.select_only("id", "name")

        # Each step should be independent
        assert "LIMIT" not in step1.raw_sql
        assert "id" not in step1.raw_sql
        assert "id" not in step2.raw_sql
        assert "LIMIT" in step2.raw_sql
        assert "id" in step3.raw_sql
        assert "LIMIT" in step3.raw_sql
