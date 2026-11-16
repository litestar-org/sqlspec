"""Unit tests for CTE parameter collision resolution.

This module tests that CTEs with duplicate parameter names are properly
handled with unique parameter naming to prevent collisions.
"""

from collections.abc import Callable
from typing import Any, cast

import pytest
from sqlglot import expressions as exp

from sqlspec import sql


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
    query.enable_optimization = False

    stmt = query.build()

    # Should work without issues even with no parameters
    assert isinstance(stmt.parameters, dict)
    assert stmt.sql.startswith("WITH")
    assert '"limited_users" AS (' in stmt.sql


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


def test_final_expression_copy_detaches_nested_ctes() -> None:
    from sqlglot import exp

    inner = sql.select("id").from_("items")
    owner = sql.select("id").with_cte("source", inner).from_("source")
    owner.enable_optimization = False
    before = owner.build().sql
    detached = owner._build_final_expression(copy=True)
    cte = next(detached.find_all(exp.CTE))
    cte.this.set("where", exp.Where(this=exp.false()))

    assert owner.build().sql == before
    assert "FALSE" in detached.sql()


def test_from_subquery_keeps_nested_cte_owner_isolated() -> None:
    from sqlglot import exp

    source = sql.select("id").with_cte("nested", sql.select("id").from_("items")).from_("nested")
    source.enable_optimization = False
    before = source.build().sql
    target = sql.select("*").from_(source, alias="derived")
    target.enable_optimization = False
    cte = next(cast("exp.Expr", target.get_expression()).find_all(exp.CTE))
    cte.this.set("where", exp.Where(this=exp.false()))

    assert source.build().sql == before
    assert "FALSE" in target.build().sql


@pytest.mark.parametrize(
    "build_query",
    [
        pytest.param(
            lambda owned: (
                sql.select("*").from_("tasks").where_in("project_id", owned).where(sql.column("assignee_id") == 200)
            ),
            id="where_in",
        ),
        pytest.param(
            lambda owned: sql.select("*").from_("tasks").where_exists(owned).where(sql.column("assignee_id") == 200),
            id="where_exists",
        ),
        pytest.param(
            lambda owned: (
                sql
                .select("*")
                .from_("tasks")
                .where(sql.column("project_id") == sql.any_(owned))
                .where(sql.column("assignee_id") == 200)
            ),
            id="sql_any",
        ),
        pytest.param(
            lambda owned: (
                sql.select("*").from_("tasks").where_any("project_id", owned).where(sql.column("assignee_id") == 200)
            ),
            id="where_any",
        ),
        pytest.param(
            lambda owned: sql.select("*").from_("tasks").with_("owned", owned).where(sql.column("assignee_id") == 200),
            id="with_",
        ),
        pytest.param(
            lambda owned: (
                sql
                .update("tasks")
                .set(status="done")
                .where_in("project_id", owned)
                .where(sql.column("assignee_id") == 200)
            ),
            id="update",
        ),
        pytest.param(
            lambda owned: sql.delete("tasks").where_in("project_id", owned).where(sql.column("assignee_id") == 200),
            id="delete",
        ),
        pytest.param(
            lambda owned: sql.select(owned).from_("tasks").where(sql.column("assignee_id") == 200), id="scalar_select"
        ),
        pytest.param(
            lambda owned: (
                sql.select(sql.exists_(owned).as_("has_proj")).from_("tasks").where(sql.column("assignee_id") == 200)
            ),
            id="select_exists_fragment",
        ),
        pytest.param(
            lambda owned: (
                sql
                .select("*")
                .from_("tasks")
                .join(owned, on="tasks.project_id = owned.id", alias="owned")
                .where(sql.column("assignee_id") == 200)
            ),
            id="join_subquery",
        ),
        pytest.param(
            lambda owned: (
                sql
                .select("*")
                .from_("tasks")
                .where_eq("active", True)
                .or_where_exists(sql.exists_(owned))
                .where(sql.column("assignee_id") == 200)
            ),
            id="or_where_exists_fragment",
        ),
    ],
)
def test_embedded_subquery_preserves_distinct_where_param_bindings(build_query: Callable[[Any], Any]) -> None:
    """Embedding a parameterized subquery keeps inner and outer where_param_N bindings distinct."""
    owned = sql.select("id").from_("projects").where(sql.column("owner_id") == 100)
    built = build_query(owned).build()

    owner_keys = [name for name, val in built.parameters.items() if val == 100]
    assignee_keys = [name for name, val in built.parameters.items() if val == 200]
    assert len(owner_keys) == 1
    assert len(assignee_keys) == 1
    assert owner_keys[0] != assignee_keys[0]
    assert f":{owner_keys[0]}" in built.sql
    assert f":{assignee_keys[0]}" in built.sql


@pytest.mark.parametrize(
    "build_query",
    [
        pytest.param(
            lambda owned: (
                sql.select("*").from_("tasks").where(sql.column("assignee_id") == 200).where_in("project_id", owned)
            ),
            id="where_in",
        ),
        pytest.param(
            lambda owned: sql.select("*").from_("tasks").where(sql.column("assignee_id") == 200).where_exists(owned),
            id="where_exists",
        ),
        pytest.param(
            lambda owned: (
                sql
                .select("*")
                .from_("tasks")
                .where(sql.column("assignee_id") == 200)
                .where(sql.column("project_id") == sql.any_(owned))
            ),
            id="sql_any",
        ),
        pytest.param(
            lambda owned: (
                sql
                .update("tasks")
                .set(status="done")
                .where(sql.column("assignee_id") == 200)
                .where_in("project_id", owned)
            ),
            id="update",
        ),
        pytest.param(
            lambda owned: sql.delete("tasks").where(sql.column("assignee_id") == 200).where_in("project_id", owned),
            id="delete",
        ),
    ],
)
def test_outer_where_before_embedded_subquery_avoids_parameter_overwrite(build_query: Callable[[Any], Any]) -> None:
    """Binding outer where_param_1 before embedding a subquery renames colliding inner placeholders."""
    owned = sql.select("id").from_("projects").where(sql.column("owner_id") == 100)
    built = build_query(owned).build()

    owner_keys = [name for name, val in built.parameters.items() if val == 100]
    assignee_keys = [name for name, val in built.parameters.items() if val == 200]
    assert len(owner_keys) == 1
    assert len(assignee_keys) == 1
    assert owner_keys[0] != assignee_keys[0]
    assert f":{owner_keys[0]}" in built.sql
    assert f":{assignee_keys[0]}" in built.sql


def test_multi_parameter_subquery_advances_outer_counter_beyond_all_inner_indices() -> None:
    """Outer parameters after a multi-parameter subquery never collide with its higher-numbered keys."""
    owned = sql.select("id").from_("projects").where(sql.column("owner_id") == 100).where(sql.column("org_id") == 101)
    built = (
        sql
        .select("*")
        .from_("tasks")
        .where(sql.column("assignee_id") == 200)
        .where_in("project_id", owned)
        .where(sql.column("reviewer_id") == 300)
        .build()
    )

    assert sorted(built.parameters.values()) == [100, 101, 200, 300]
    assert len(built.parameters) == 4


def test_merge_with_subquery_and_condition_fragment_preserves_distinct_parameters() -> None:
    """Merge using and WHEN condition fragment preserve distinct parameter bindings."""
    owned = sql.select("id").from_("projects").where(sql.column("owner_id") == 100)
    guard = sql.select("id").from_("projects").where(sql.column("owner_id") == 200)
    built = (
        sql
        .merge("tasks", dialect="postgres")
        .using(owned, alias="owned")
        .on("tasks.project_id = owned.id")
        .when_matched_then_update(status="done", condition=sql.exists_(guard))
        .build()
    )

    values = list(built.parameters.values())
    assert 100 in values
    assert 200 in values
    assert "done" in values


def test_reusable_subquery_fragment_stays_isolated_with_outer_where_params() -> None:
    """Reusing one subquery fragment across queries keeps each query's parameter names distinct."""
    owned = sql.select("id").from_("projects").where(sql.column("owner_id") == 100)
    fragment = sql.exists_(owned)

    first = sql.select("*").from_("tasks").where(fragment).where(sql.column("assignee_id") == 200).build()
    second = (
        sql
        .select("*")
        .from_("tasks")
        .where(sql.column("reviewer_id") == 300)
        .where(fragment)
        .where(sql.column("assignee_id") == 400)
        .build()
    )

    assert sorted(first.parameters.values()) == [100, 200]
    assert sorted(second.parameters.values()) == [100, 300, 400]


def test_merge_sanitizes_non_identifier_child_names() -> None:
    """Merged child parameters with non-identifier names are renamed and their placeholders rewritten."""
    outer = sql.select("*").from_("t")
    mapping = outer._merge_parameter_dict({"a-b": 5})  # pyright: ignore[reportPrivateUsage]

    assert mapping == {"a-b": "a_b"}
    assert outer.parameters == {"a_b": 5}
    rewritten = outer._update_placeholders(  # pyright: ignore[reportPrivateUsage]
        exp.EQ(this=exp.column("v"), expression=exp.Placeholder(this="a-b")), mapping
    )
    assert rewritten.sql() == "v = :a_b"
