"""Unit tests for CTE parameter collision resolution.

This module tests that CTEs with duplicate parameter names are properly
handled with unique parameter naming to prevent collisions.
"""

import pytest

from sqlspec import sql

pytestmark = pytest.mark.xdist_group("builder")


def test_cte_parameter_collision_resolution() -> None:
    """Test that CTEs with same parameter names don't collide."""
    # Create two CTEs that would normally have parameter name collision
    cte1 = sql.select("user_id").from_("events").where_in("status", ["active", "pending"])
    cte2 = sql.select("order_id").from_("orders").where_in("status", ["completed", "shipped"])

    # This should not raise an error about parameter collision
    query = sql.select("*").with_cte("recent_events", cte1).with_cte("recent_orders", cte2).from_("analytics")

    stmt = query.build()

    # Should have unique parameter names for both CTEs
    assert len(stmt.parameters) == 4  # 2 parameters per CTE

    # Check that parameter names are unique and prefixed with CTE names
    param_names = list(stmt.parameters.keys())
    recent_events_params = [name for name in param_names if "recent_events" in name]
    recent_orders_params = [name for name in param_names if "recent_orders" in name]

    assert len(recent_events_params) == 2
    assert len(recent_orders_params) == 2

    # Verify parameter values are preserved
    recent_events_values = [stmt.parameters[name] for name in recent_events_params]
    recent_orders_values = [stmt.parameters[name] for name in recent_orders_params]

    assert "active" in recent_events_values
    assert "pending" in recent_events_values
    assert "completed" in recent_orders_values
    assert "shipped" in recent_orders_values


def test_cte_parameter_uniqueness_with_same_column_names() -> None:
    """Test parameter names are unique across multiple CTEs with same column names."""
    # Create CTEs that use the same column name "name"
    cte1 = sql.select("id", "name").from_("users").where_in("name", ["Alice", "Bob"])
    cte2 = sql.select("id", "name").from_("products").where_in("name", ["Widget", "Gadget"])
    cte3 = sql.select("id", "name").from_("categories").where_eq("name", "Electronics")

    query = (
        sql
        .select("*")
        .with_cte("active_users", cte1)
        .with_cte("popular_products", cte2)
        .with_cte("main_category", cte3)
        .from_("dashboard")
    )

    stmt = query.build()

    # Should have unique parameter names
    assert len(stmt.parameters) == 5  # 2 + 2 + 1 parameters

    # Check parameter name prefixing
    param_names = list(stmt.parameters.keys())
    active_users_params = [name for name in param_names if "active_users" in name]
    popular_products_params = [name for name in param_names if "popular_products" in name]
    main_category_params = [name for name in param_names if "main_category" in name]

    assert len(active_users_params) == 2
    assert len(popular_products_params) == 2
    assert len(main_category_params) == 1

    # Verify all parameter names are unique
    assert len(set(param_names)) == len(param_names)


def test_cte_parameter_preservation() -> None:
    """Test parameter values are preserved despite name changes."""
    original_values = ["status1", "status2", "status3"]

    cte1 = sql.select("id").from_("table1").where_in("status", original_values[:2])
    cte2 = sql.select("id").from_("table2").where_eq("status", original_values[2])

    query = sql.select("*").with_cte("first_cte", cte1).with_cte("second_cte", cte2).from_("main")

    stmt = query.build()

    # All original values should be preserved in parameters
    param_values = list(stmt.parameters.values())
    for value in original_values:
        assert value in param_values


def test_nested_cte_parameter_handling() -> None:
    """Test parameter handling with nested CTEs and complex conditions."""
    # Create a complex nested scenario
    inner_cte = sql.select("user_id").from_("sessions").where_between("created_at", "2023-01-01", "2023-12-31")

    outer_cte = (
        sql
        .select("user_id", "COUNT(*) as visit_count")
        .from_("visits")
        .where_in("user_id", inner_cte)
        .where_gt("duration", 300)
        .group_by("user_id")
    )

    final_query = (
        sql.select("*").with_cte("active_sessions", inner_cte).with_cte("frequent_visitors", outer_cte).from_("users")
    )

    stmt = final_query.build()

    # Should handle all parameters without collision
    assert len(stmt.parameters) >= 3  # At least created_at bounds + duration

    # Check that parameter values are preserved
    param_values = list(stmt.parameters.values())
    assert "2023-01-01" in param_values
    assert "2023-12-31" in param_values
    assert 300 in param_values


def test_cte_with_multiple_where_conditions() -> None:
    """Test CTE parameter collision with multiple WHERE conditions."""
    # Create CTEs with multiple conditions on same column names
    cte1 = (
        sql
        .select("*")
        .from_("orders")
        .where_eq("status", "pending")
        .where_gt("amount", 100)
        .where_like("customer_name", "%Smith%")
    )

    cte2 = (
        sql
        .select("*")
        .from_("invoices")
        .where_eq("status", "paid")
        .where_lt("amount", 50)
        .where_like("customer_name", "%Johnson%")
    )

    query = sql.select("*").with_cte("pending_orders", cte1).with_cte("small_invoices", cte2).from_("financials")

    stmt = query.build()

    # Should have parameters for all conditions without collision
    param_names = list(stmt.parameters.keys())

    # Check parameter prefixing
    pending_params = [name for name in param_names if "pending_orders" in name]
    invoice_params = [name for name in param_names if "small_invoices" in name]

    assert len(pending_params) == 3  # status, amount, customer_name
    assert len(invoice_params) == 3  # status, amount, customer_name

    # Verify values are correct
    assert stmt.parameters[next(name for name in pending_params if "status" in name)] == "pending"
    assert stmt.parameters[next(name for name in invoice_params if "status" in name)] == "paid"


def test_cte_parameter_collision_with_main_query_params() -> None:
    """Test that CTE parameters don't collide with main query parameters."""
    cte = sql.select("id").from_("products").where_eq("category", "electronics")

    query = (
        sql
        .select("*")
        .with_cte("electronics", cte)
        .from_("orders")
        .where_eq("category", "books")  # Same parameter name as CTE
        .where_in("product_id", cte)
    )

    stmt = query.build()

    # Should have unique parameters for both CTE and main query
    param_names = list(stmt.parameters.keys())

    # Should have at least 2 parameters for category (one for CTE, one for main query)
    category_params = [name for name in param_names if "category" in name]
    assert len(category_params) >= 2

    # Verify different values
    param_values = [stmt.parameters[name] for name in category_params]
    assert "electronics" in param_values
    assert "books" in param_values


def test_cte_with_empty_parameters() -> None:
    """Test CTE handling when no parameters are involved."""
    cte = sql.select("id", "name").from_("users").limit(10)

    query = sql.select("*").with_cte("limited_users", cte).from_("orders")

    stmt = query.build()

    # Should work without issues even with no parameters
    assert isinstance(stmt.parameters, dict)
    assert "WITH" in stmt.sql
    assert "limited_users" in stmt.sql


def test_multiple_cte_levels_parameter_isolation() -> None:
    """Test parameter isolation across multiple levels of CTE nesting."""
    # Level 1 CTE
    level1 = sql.select("user_id").from_("events").where_eq("type", "login")

    # Level 2 CTE that references Level 1
    level2 = (
        sql
        .select("user_id", "COUNT(*) as login_count")
        .from_("daily_stats")
        .where_in("user_id", level1)
        .where_eq("type", "summary")
    )  # Same parameter name "type"

    # Main query
    query = (
        sql
        .select("*")
        .with_cte("login_events", level1)
        .with_cte("login_summary", level2)
        .from_("reports")
        .where_eq("type", "monthly")
    )  # Another "type" parameter

    stmt = query.build()

    # Should have unique parameters for each "type" usage (may have more due to subquery handling)
    type_params = [name for name in stmt.parameters.keys() if "type" in name.lower()]
    assert len(type_params) >= 3  # At least 3, may have more due to complex nesting

    # Verify values are preserved correctly
    param_values = [stmt.parameters[name] for name in type_params]
    assert "login" in param_values
    assert "summary" in param_values
    assert "monthly" in param_values
