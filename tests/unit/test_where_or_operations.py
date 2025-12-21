"""Unit tests for WHERE OR clause operations.

This module tests the new OR clause functionality in the builder API,
including or_where(), where_or(), and or_where_* helper methods.
"""

import pytest

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError

pytestmark = pytest.mark.xdist_group("builder")


def test_or_where_basic_chaining() -> None:
    """Test basic OR chaining with or_where method."""
    query = sql.select("*").from_("users").where_eq("role", "admin").or_where_eq("role", "moderator")

    stmt = query.build()

    # Should contain OR in the WHERE clause
    assert "OR" in stmt.sql
    assert "WHERE" in stmt.sql

    # Should have two role parameters with different names
    role_params = [name for name in stmt.parameters.keys() if "role" in name]
    assert len(role_params) == 2

    # Verify parameter values
    param_values = list(stmt.parameters.values())
    assert "admin" in param_values
    assert "moderator" in param_values


def test_or_where_requires_existing_where() -> None:
    """Test that or_where requires an existing WHERE clause."""
    query = sql.select("*").from_("users")

    # Should raise error when no existing WHERE clause
    with pytest.raises(SQLBuilderError, match="no existing WHERE clause found"):
        query.or_where_eq("role", "admin")


def test_where_or_grouping() -> None:
    """Test where_or method for grouping multiple OR conditions."""
    query = (
        sql
        .select("*")
        .from_("users")
        .where_or(("role", "admin"), ("role", "moderator"), ("permissions", "LIKE", "%admin%"))
    )

    stmt = query.build()

    # Should contain OR conditions
    assert "OR" in stmt.sql
    assert "WHERE" in stmt.sql
    assert "LIKE" in stmt.sql

    # Should have parameters for all conditions
    assert len(stmt.parameters) == 3

    # Verify parameter values
    param_values = list(stmt.parameters.values())
    assert "admin" in param_values
    assert "moderator" in param_values
    assert "%admin%" in param_values


def test_where_or_empty_conditions() -> None:
    """Test that where_or requires at least one condition."""
    query = sql.select("*").from_("users")

    with pytest.raises(SQLBuilderError, match="requires at least one condition"):
        query.where_or()


def test_mixed_and_or_conditions() -> None:
    """Test mixing AND and OR conditions properly."""
    query = (
        sql
        .select("*")
        .from_("products")
        .where_eq("category", "electronics")  # AND condition
        .where_or(("price", "<", 100), ("on_sale", True))  # OR group
        .where_is_not_null("inventory_count")
    )  # Another AND condition

    stmt = query.build()

    # Should have proper AND/OR structure
    assert "AND" in stmt.sql
    assert "OR" in stmt.sql
    assert "IS NOT NULL" in stmt.sql or ("NOT" in stmt.sql and "IS NULL" in stmt.sql)  # SQLGlot may format differently

    # Should have parameters for all conditions
    param_values = list(stmt.parameters.values())
    assert "electronics" in param_values
    assert 100 in param_values
    assert True in param_values


def test_or_where_with_string_conditions() -> None:
    """Test or_where with raw string conditions."""
    query = (
        sql.select("*").from_("events").where_eq("status", "active").or_where("priority > 5").or_where("urgent = true")
    )

    stmt = query.build()

    # Should contain OR conditions
    assert "OR" in stmt.sql
    assert "priority" in stmt.sql and "> 5" in stmt.sql  # SQLGlot may qualify column names
    assert "urgent" in stmt.sql and "TRUE" in stmt.sql

    # Should have parameter for the where_eq condition
    assert "active" in stmt.parameters.values()


def test_or_where_with_parameterized_strings() -> None:
    """Test or_where with parameterized string conditions."""
    query = (
        sql
        .select("*")
        .from_("users")
        .where_eq("status", "active")
        .or_where("age > ?", 65)
        .or_where("department = :dept", dept="admin")
    )

    stmt = query.build()

    # Should handle different parameter styles
    assert "OR" in stmt.sql

    # Should have parameters for all conditions
    param_values = list(stmt.parameters.values())
    assert "active" in param_values
    assert 65 in param_values
    assert "admin" in param_values


def test_or_where_helper_methods() -> None:
    """Test all or_where_* helper methods work correctly."""
    query = (
        sql
        .select("*")
        .from_("posts")
        .where_eq("status", "draft")
        .or_where_eq("status", "review")
        .or_where_neq("author_id", 1)
        .or_where_in("category", ["tech", "news"])
        .or_where_like("title", "%urgent%")
        .or_where_is_null("deleted_at")
    )

    stmt = query.build()

    # Should contain all OR conditions
    assert stmt.sql.count("OR") >= 5  # At least 5 OR clauses

    # Should have parameters for parameterized conditions
    param_values = list(stmt.parameters.values())
    assert "draft" in param_values
    assert "review" in param_values
    assert 1 in param_values
    assert "tech" in param_values
    assert "news" in param_values
    assert "%urgent%" in param_values


def test_or_where_in_with_subquery() -> None:
    """Test or_where_in with subquery."""
    subquery = sql.select("user_id").from_("banned_users").where_eq("status", "active")

    query = sql.select("*").from_("posts").where_eq("published", True).or_where_in("author_id", subquery)

    stmt = query.build()

    # Should contain OR with IN subquery
    assert "OR" in stmt.sql
    assert "IN" in stmt.sql

    # Should have parameters from both main query and subquery
    param_values = list(stmt.parameters.values())
    assert True in param_values
    assert "active" in param_values


def test_complex_nested_or_conditions() -> None:
    """Test complex nested OR conditions with parentheses."""
    query = (
        sql
        .select("*")
        .from_("orders")
        .where_eq("customer_id", 123)
        .where_or(("status", "pending"), ("status", "processing"), ("priority", ">", 5))
        .where_between("created_at", "2023-01-01", "2023-12-31")
    )

    stmt = query.build()

    # Should have proper grouping and multiple conditions
    assert "AND" in stmt.sql
    assert "OR" in stmt.sql
    assert "BETWEEN" in stmt.sql or ("created_at" in stmt.sql and "<=" in stmt.sql)  # SQLGlot may expand BETWEEN

    # Verify all parameter values
    param_values = list(stmt.parameters.values())
    assert 123 in param_values
    assert "pending" in param_values
    assert "processing" in param_values
    assert 5 in param_values
    assert "2023-01-01" in param_values
    assert "2023-12-31" in param_values


def test_or_where_with_tuples() -> None:
    """Test or_where with tuple conditions."""
    query = (
        sql
        .select("*")
        .from_("products")
        .where("price > 100")
        .or_where(("category", "electronics"))  # 2-tuple equality
        .or_where(("rating", ">=", 4.5))
    )  # 3-tuple with operator

    stmt = query.build()

    # Should handle tuple conditions properly
    assert "OR" in stmt.sql
    assert "category" in stmt.sql
    assert "rating" in stmt.sql
    assert ">=" in stmt.sql

    # Should have parameters for tuple conditions
    param_values = list(stmt.parameters.values())
    assert "electronics" in param_values
    assert 4.5 in param_values


def test_where_or_with_mixed_condition_types() -> None:
    """Test where_or with mixed condition types."""
    query = (
        sql
        .select("*")
        .from_("users")
        .where_or(
            ("role", "admin"),  # 2-tuple
            ("age", ">", 65),  # 3-tuple
            "department = 'IT'",  # string
            ("permissions", "LIKE", "%write%"),  # 3-tuple with LIKE
        )
    )

    stmt = query.build()

    # Should handle all condition types
    assert "OR" in stmt.sql
    assert "role" in stmt.sql
    assert "age" in stmt.sql
    assert "department" in stmt.sql and "IT" in stmt.sql  # SQLGlot qualifies columns
    assert "LIKE" in stmt.sql

    # Should have parameters where appropriate
    param_values = list(stmt.parameters.values())
    assert "admin" in param_values
    assert 65 in param_values
    assert "%write%" in param_values


def test_or_where_preserves_parameter_naming() -> None:
    """Test that OR conditions preserve descriptive parameter naming."""
    query = (
        sql
        .select("*")
        .from_("events")
        .where_eq("event_type", "click")
        .or_where_eq("event_type", "view")
        .or_where_in("user_category", ["premium", "trial"])
        .or_where_between("timestamp", "2023-01-01", "2023-02-01")
    )

    stmt = query.build()

    # Should not have generic parameter names
    param_names = list(stmt.parameters.keys())
    generic_params = [name for name in param_names if name.startswith("param_")]
    assert len(generic_params) == 0, f"Found generic parameters: {generic_params}"

    # Should have descriptive parameter names
    assert any("event_type" in name for name in param_names)
    assert any("user_category" in name for name in param_names)
    assert any("timestamp" in name for name in param_names)


def test_or_where_with_update_statements() -> None:
    """Test OR clauses work with UPDATE statements."""
    query = (
        sql
        .update("users")
        .set("last_active", "2023-12-01")
        .where_eq("status", "inactive")
        .or_where_lt("last_login", "2023-01-01")
    )

    stmt = query.build()

    # Should work with UPDATE statements
    assert "UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "WHERE" in stmt.sql
    assert "OR" in stmt.sql

    # Should have parameters for both SET and WHERE clauses
    param_values = list(stmt.parameters.values())
    assert "2023-12-01" in param_values
    assert "inactive" in param_values
    assert "2023-01-01" in param_values


def test_or_where_with_delete_statements() -> None:
    """Test OR clauses work with DELETE statements."""
    query = (
        sql
        .delete()
        .from_("logs")
        .where_lt("created_at", "2023-01-01")
        .or_where_eq("level", "DEBUG")
        .or_where_is_null("user_id")
    )

    stmt = query.build()

    # Should work with DELETE statements
    assert "DELETE" in stmt.sql
    assert "FROM" in stmt.sql
    assert "WHERE" in stmt.sql
    assert "OR" in stmt.sql
    assert "IS NULL" in stmt.sql

    # Should have appropriate parameters
    param_values = list(stmt.parameters.values())
    assert "2023-01-01" in param_values
    assert "DEBUG" in param_values


def test_chained_or_operations() -> None:
    """Test multiple chained OR operations work correctly."""
    query = (
        sql
        .select("*")
        .from_("products")
        .where_eq("category", "books")
        .or_where_eq("category", "music")
        .or_where_eq("category", "movies")
        .or_where(("price", "<", 20))
        .or_where_is_null("discontinued")
    )

    stmt = query.build()

    # Should have multiple OR clauses
    assert stmt.sql.count("OR") >= 4

    # Should have unique parameter names despite repetition
    param_names = list(stmt.parameters.keys())
    category_params = [name for name in param_names if "category" in name]
    assert len(category_params) == 3  # Three different category parameters

    # All should be unique
    assert len(set(param_names)) == len(param_names)


def test_or_where_error_handling() -> None:
    """Test proper error handling for invalid OR operations."""
    query = sql.select("*").from_("users")

    # Should fail when trying to use or_where without existing WHERE
    with pytest.raises(SQLBuilderError, match="no existing WHERE clause found"):
        query.or_where_eq("status", "active")

    # Insert doesn't have WhereClauseMixin, so these methods don't exist
    insert_query = sql.insert("users").columns("name").values("John")
    assert not hasattr(insert_query, "or_where_eq")
    assert not hasattr(insert_query, "where_or")


def test_performance_with_many_or_conditions() -> None:
    """Test performance and correctness with many OR conditions."""
    query = sql.select("*").from_("items").where_eq("active", True)

    # Add many OR conditions
    categories = [f"category_{i}" for i in range(20)]
    for category in categories:
        query = query.or_where_eq("category", category)

    stmt = query.build()

    # Should handle many conditions without issue
    assert stmt.sql.count("OR") >= 19  # Should have at least 19 OR clauses

    # Should have unique parameter names
    param_names = list(stmt.parameters.keys())
    assert len(set(param_names)) == len(param_names)

    # Should have all category values
    param_values = list(stmt.parameters.values())
    for category in categories:
        assert category in param_values


def test_or_where_complete_method_parity() -> None:
    """Test that all where_* methods have corresponding or_where_* methods."""
    # Test basic OR methods - no complex methods to avoid interference
    query = (
        sql
        .select("*")
        .from_("test_table")
        .where_eq("base", "condition")
        .or_where_eq("col1", "value1")
        .or_where_neq("col2", "value2")
        .or_where_lt("col3", 10)
        .or_where_lte("col4", 20)
        .or_where_gt("col5", 30)
        .or_where_gte("col6", 40)
        .or_where_between("col7", 1, 100)
        .or_where_like("col8", "%pattern%")
        .or_where_not_like("col9", "%bad%")
        .or_where_ilike("col10", "%CASE%")
        .or_where_is_null("col11")
        .or_where_is_not_null("col12")
        .or_where_null("col13")  # Alias
        .or_where_not_null("col14")  # Alias
        .or_where_in("col15", ["a", "b", "c"])
        .or_where_not_in("col16", ["x", "y", "z"])
    )

    basic_stmt = query.build()
    assert "OR" in basic_stmt.sql, f"Expected OR in basic methods SQL: {basic_stmt.sql}"
    basic_or_count = basic_stmt.sql.count("OR")
    assert basic_or_count >= 15, f"Expected at least 15 OR conditions, got {basic_or_count}"

    # Should have descriptive parameter names (no generic param_N)
    param_names = list(basic_stmt.parameters.keys())
    generic_params = [name for name in param_names if name.startswith("param_")]
    assert len(generic_params) == 0, f"Found generic parameters: {generic_params}"


def test_or_where_complex_methods() -> None:
    """Test OR methods that work with subqueries and complex operations."""
    query = sql.select("*").from_("test_table").where_eq("base", "condition")

    # Test EXISTS/NOT EXISTS
    subquery = sql.select("id").from_("related").where_eq("status", "active")
    query = query.or_where_exists(subquery)
    query = query.or_where_not_exists(subquery)

    # Test ANY/NOT ANY
    query = query.or_where_any("col17", ["val1", "val2"])
    query = query.or_where_not_any("col18", ["val3", "val4"])

    final_stmt = query.build()

    # Should generate valid SQL with OR clauses
    assert "OR" in final_stmt.sql, f"Expected OR in complex methods SQL: {final_stmt.sql}"
    or_count = final_stmt.sql.count("OR")
    assert or_count >= 4, f"Expected at least 4 OR conditions, got {or_count}"

    # Should have EXISTS and ANY in SQL
    assert "EXISTS" in final_stmt.sql
    assert "NOT EXISTS" in final_stmt.sql
    assert "ANY" in final_stmt.sql


def test_or_where_subquery_methods() -> None:
    """Test OR methods that work with subqueries."""
    base_query = sql.select("*").from_("users").where_eq("active", True)

    # Test EXISTS with subquery
    exists_subquery = sql.select("1").from_("orders").where_eq("status", "pending")

    # Test ANY with subquery
    any_subquery = sql.select("user_id").from_("premium_users").where_eq("tier", "gold")

    # Test IN with subquery (already tested but adding for completeness)
    in_subquery = sql.select("user_id").from_("banned_users").where_eq("banned", True)

    query = (
        base_query
        .or_where_exists(exists_subquery)
        .or_where_not_exists(exists_subquery)
        .or_where_any("user_id", any_subquery)
        .or_where_not_any("user_id", any_subquery)
        .or_where_in("user_id", in_subquery)
        .or_where_not_in("user_id", in_subquery)
    )

    stmt = query.build()

    # Should contain subquery constructs
    assert "EXISTS" in stmt.sql
    assert "NOT EXISTS" in stmt.sql
    assert "ANY" in stmt.sql
    assert "IN" in stmt.sql

    # Should handle parameters from all subqueries
    param_values = list(stmt.parameters.values())
    assert True in param_values  # From base query
    assert "pending" in param_values  # From exists subquery
    assert "gold" in param_values  # From any subquery
    assert True in param_values  # From in subquery (banned=True)


def test_or_where_aliases_work() -> None:
    """Test that or_where_null and or_where_not_null aliases work correctly."""
    query = (
        sql
        .select("*")
        .from_("products")
        .where_eq("active", True)
        .or_where_null("deleted_at")  # Should be alias for or_where_is_null
        .or_where_not_null("created_at")
    )  # Should be alias for or_where_is_not_null

    stmt = query.build()

    # Should contain NULL checks
    assert "IS NULL" in stmt.sql
    assert "IS NOT NULL" in stmt.sql or ("NOT" in stmt.sql and "IS NULL" in stmt.sql)
    assert "OR" in stmt.sql

    # Should have parameter for active condition
    assert True in stmt.parameters.values()


def test_or_where_ilike_case_insensitive() -> None:
    """Test that or_where_ilike works for case-insensitive pattern matching."""
    query = (
        sql
        .select("*")
        .from_("articles")
        .where_eq("published", True)
        .or_where_ilike("title", "%PYTHON%")  # Should match regardless of case
        .or_where_like("content", "%exactly%")
    )  # Regular LIKE for comparison

    stmt = query.build()

    # Should contain both ILIKE and LIKE
    assert "ILIKE" in stmt.sql
    assert "LIKE" in stmt.sql
    assert "OR" in stmt.sql

    # Should have parameters for both patterns
    param_values = list(stmt.parameters.values())
    assert "%PYTHON%" in param_values
    assert "%exactly%" in param_values
    assert True in param_values
