"""Unit tests for SQL factory functionality including parameter binding fixes and new features."""

import math

import pytest
from sqlglot import exp, parse_one

from sqlspec import sql
from sqlspec.builder import (
    AggregateExpression,
    Case,
    Column,
    Delete,
    Insert,
    JoinBuilder,
    Select,
    SQLFactory,
    SubqueryBuilder,
    Update,
    WindowFunctionBuilder,
    build_copy_from_statement,
    build_copy_to_statement,
)
from sqlspec.core import SQL
from sqlspec.exceptions import SQLBuilderError

pytestmark = pytest.mark.xdist_group("builder")


def test_sql_factory_instance() -> None:
    """Test that sql is an instance of SQLFactory."""
    assert isinstance(sql, SQLFactory)


def test_sql_factory_default_dialect() -> None:
    """Test SQL factory default dialect behavior."""
    factory = SQLFactory()
    assert factory.dialect is None

    factory_with_dialect = SQLFactory(dialect="postgres")
    assert factory_with_dialect.dialect == "postgres"


def test_where_eq_uses_placeholder_not_var() -> None:
    """Test that where_eq uses Placeholder instead of var for parameters."""
    query = sql.select("*").from_("users").where_eq("name", "John")
    stmt = query.build()

    assert ":name" in stmt.sql
    assert "name" in stmt.parameters
    assert stmt.parameters["name"] == "John"


def test_where_neq_uses_placeholder() -> None:
    """Test that where_neq uses proper parameter binding."""
    query = sql.select("*").from_("users").where_neq("status", "inactive")
    stmt = query.build()

    assert ":status" in stmt.sql
    assert stmt.parameters["status"] == "inactive"


def test_where_comparison_operators_use_placeholders() -> None:
    """Test all comparison WHERE methods use proper parameter binding."""
    from collections.abc import Callable
    from typing import Any

    test_cases: list[tuple[str, Callable[[Any], Any], str]] = [
        ("where_lt", lambda q: q.where_lt("age", 18), "age"),
        ("where_lte", lambda q: q.where_lte("score", 100), "score"),
        ("where_gt", lambda q: q.where_gt("price", 50.0), "price"),
        ("where_gte", lambda q: q.where_gte("rating", 3.5), "rating"),
    ]

    for method_name, query_builder, column_name in test_cases:
        query = query_builder(sql.select("*").from_("test_table"))
        stmt = query.build()

        assert f":{column_name}" in stmt.sql, f"{method_name} should use :{column_name} placeholder"
        assert column_name in stmt.parameters, f"{method_name} should have {column_name} in parameters"

        sql_upper = stmt.sql.upper()
        bare_param_exists = column_name.upper() in sql_upper and f":{column_name.upper()}" not in sql_upper
        assert not bare_param_exists, f"{method_name} should not have bare {column_name} reference"


def test_where_between_uses_placeholders() -> None:
    """Test that where_between uses proper parameter binding for both values."""
    query = sql.select("*").from_("products").where_between("price", 10, 100)
    stmt = query.build()

    assert "price_low" in stmt.parameters
    assert "price_high" in stmt.parameters
    assert stmt.parameters["price_low"] == 10
    assert stmt.parameters["price_high"] == 100

    assert ":price_low" in stmt.sql
    assert ":price_high" in stmt.sql


def test_where_like_uses_placeholder() -> None:
    """Test that where_like uses proper parameter binding."""
    query = sql.select("*").from_("users").where_like("name", "%John%")
    stmt = query.build()

    assert ":name" in stmt.sql
    assert stmt.parameters["name"] == "%John%"


def test_where_not_like_uses_placeholder() -> None:
    """Test that where_not_like uses proper parameter binding."""
    query = sql.select("*").from_("users").where_not_like("name", "%spam%")
    stmt = query.build()

    assert ":name" in stmt.sql
    assert stmt.parameters["name"] == "%spam%"


def test_where_ilike_uses_placeholder() -> None:
    """Test that where_ilike uses proper parameter binding."""
    query = sql.select("*").from_("users").where_ilike("email", "%@example.com")
    stmt = query.build()

    assert ":email" in stmt.sql
    assert stmt.parameters["email"] == "%@example.com"


def test_where_in_uses_placeholders() -> None:
    """Test that where_in uses proper parameter binding for multiple values."""
    query = sql.select("*").from_("users").where_in("status", ["active", "pending"])
    stmt = query.build()

    assert "status_1" in stmt.parameters
    assert "status_2" in stmt.parameters
    assert stmt.parameters["status_1"] == "active"
    assert stmt.parameters["status_2"] == "pending"

    assert ":status_1" in stmt.sql
    assert ":status_2" in stmt.sql


def test_where_not_in_uses_placeholders() -> None:
    """Test that where_not_in uses proper parameter binding."""
    query = sql.select("*").from_("users").where_not_in("role", ["admin", "superuser"])
    stmt = query.build()

    assert "role_1" in stmt.parameters
    assert "role_2" in stmt.parameters
    assert stmt.parameters["role_1"] == "admin"
    assert stmt.parameters["role_2"] == "superuser"


def test_where_any_with_values_uses_placeholders() -> None:
    """Test that where_any with value list uses proper parameter binding."""
    query = sql.select("*").from_("users").where_any("status", ["active", "verified"])
    stmt = query.build()

    assert "status_any_1" in stmt.parameters
    assert "status_any_2" in stmt.parameters
    assert stmt.parameters["status_any_1"] == "active"
    assert stmt.parameters["status_any_2"] == "verified"


def test_where_not_any_with_values_uses_placeholders() -> None:
    """Test that where_not_any with value list uses proper parameter binding."""
    query = sql.select("*").from_("users").where_not_any("status", ["banned", "suspended"])
    stmt = query.build()

    assert "status_not_any_1" in stmt.parameters
    assert "status_not_any_2" in stmt.parameters
    assert stmt.parameters["status_not_any_1"] == "banned"
    assert stmt.parameters["status_not_any_2"] == "suspended"


def test_multiple_where_conditions_sequential_parameters() -> None:
    """Test that multiple WHERE conditions create descriptive parameters."""
    query = (
        sql.select("*").from_("users").where_eq("name", "John").where_gt("age", 21).where_like("email", "%@gmail.com")
    )
    stmt = query.build()

    assert len(stmt.parameters) == 3
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["age"] == 21
    assert stmt.parameters["email"] == "%@gmail.com"

    assert ":name" in stmt.sql
    assert ":age" in stmt.sql
    assert ":email" in stmt.sql


def test_user_reproducible_example_fixed() -> None:
    """Test the exact user example that was failing before the fix."""
    query = sql.select("id", "name", "slug").from_("test_table").where_eq("slug", "test-item")

    stmt = query.build()

    assert "WHERE" in stmt.sql
    assert ":slug" in stmt.sql
    assert stmt.parameters["slug"] == "test-item"

    sql_upper = stmt.sql.upper()
    bare_param_exists = "SLUG" in sql_upper and ":SLUG" not in sql_upper
    assert not bare_param_exists, "Should not contain bare slug reference"


def test_raw_without_parameters_backward_compatibility() -> None:
    """Test that raw() without parameters maintains backward compatibility."""
    expr = sql.raw("COALESCE(name, 'Unknown')")

    assert isinstance(expr, exp.Expression)
    assert not isinstance(expr, SQL)


def test_raw_expression_in_insert_values() -> None:
    """Test that raw expressions work properly in insert values."""
    query = sql.insert("logs").values(message="Test", created_at=sql.raw("NOW()"))
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "logs" in stmt.sql
    assert "message" in stmt.parameters
    assert stmt.parameters["message"] == "Test"

    assert "NOW()" in stmt.sql


def test_raw_with_named_parameters_returns_sql_object() -> None:
    """Test that raw() with parameters returns SQL statement object."""
    stmt = sql.raw("name = :name_param", name_param="John")

    assert isinstance(stmt, SQL)
    assert stmt.sql == "name = :name_param"
    assert stmt.parameters["name_param"] == "John"


def test_raw_with_multiple_named_parameters() -> None:
    """Test raw SQL with multiple named parameters."""
    stmt = sql.raw("price BETWEEN :min_price AND :max_price", min_price=100, max_price=500)

    assert isinstance(stmt, SQL)
    assert stmt.sql == "price BETWEEN :min_price AND :max_price"
    assert stmt.parameters["min_price"] == 100
    assert stmt.parameters["max_price"] == 500


def test_raw_with_complex_sql_and_parameters() -> None:
    """Test raw SQL with complex query and named parameters."""
    stmt = sql.raw("LOWER(name) LIKE LOWER(:pattern) AND status = :status", pattern="%test%", status="active")

    assert isinstance(stmt, SQL)
    assert "LOWER(name) LIKE LOWER(:pattern)" in stmt.sql
    assert "status = :status" in stmt.sql
    assert stmt.parameters["pattern"] == "%test%"
    assert stmt.parameters["status"] == "active"


def test_raw_with_various_parameter_types() -> None:
    """Test raw SQL with different parameter value types."""
    stmt = sql.raw(
        "id = :user_id AND active = :is_active AND score >= :min_score", user_id=123, is_active=True, min_score=4.5
    )

    assert isinstance(stmt, SQL)
    assert stmt.parameters["user_id"] == 123
    assert stmt.parameters["is_active"] is True
    assert stmt.parameters["min_score"] == 4.5


def test_raw_empty_parameters_returns_expression() -> None:
    """Test that raw() with empty kwargs returns expression."""
    expr = sql.raw("SELECT 1")

    assert isinstance(expr, exp.Expression)
    assert not isinstance(expr, SQL)


def test_raw_none_values_in_parameters() -> None:
    """Test raw SQL with None values in parameters."""
    stmt = sql.raw("description = :desc", desc=None)

    assert isinstance(stmt, SQL)
    assert stmt.parameters["desc"] is None


def test_raw_parameter_overwrite_behavior() -> None:
    """Test behavior when same parameter name used multiple times."""
    stmt = sql.raw("field1 = :value AND field2 = :value", value="test")

    assert isinstance(stmt, SQL)
    assert stmt.sql.count(":value") == 2
    assert len(stmt.parameters) == 1
    assert stmt.parameters["value"] == "test"


def test_select_method() -> None:
    """Test sql.select() method."""
    query = sql.select("name", "email").from_("users")
    stmt = query.build()

    assert "SELECT" in stmt.sql
    assert "name" in stmt.sql
    assert "email" in stmt.sql
    assert "FROM" in stmt.sql
    assert "users" in stmt.sql


def test_insert_method() -> None:
    """Test sql.insert() method."""
    query = sql.insert("users").values_from_dict({"name": "John", "email": "john@test.com"})
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "users" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters


def test_insert_values_with_kwargs() -> None:
    """Test Insert.values() method with keyword arguments."""
    query = (
        sql
        .insert("team_member")
        .values(team_id=1, user_id=2, role="admin", joined_at=sql.raw("NOW()"))
        .returning("id", "team_id", "user_id", "role", "is_owner", "joined_at")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "team_member" in stmt.sql
    assert "RETURNING" in stmt.sql
    assert "team_id" in stmt.parameters
    assert "user_id" in stmt.parameters
    assert "role" in stmt.parameters
    assert stmt.parameters["team_id"] == 1
    assert stmt.parameters["user_id"] == 2
    assert stmt.parameters["role"] == "admin"


def test_insert_values_mixed_args_error() -> None:
    """Test Insert.values() raises error when mixing positional and keyword arguments."""
    with pytest.raises(SQLBuilderError, match="Cannot mix positional values with keyword values"):
        sql.insert("users").values("John", email="john@test.com")


def test_insert_values_with_mapping() -> None:
    """Test Insert.values() method with a mapping argument."""
    data = {"name": "John", "email": "john@test.com"}
    query = sql.insert("users").values(data)
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "users" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["email"] == "john@test.com"


def test_update_method() -> None:
    """Test sql.update() method."""
    query = sql.update("users").set({"name": "Jane"}).where_eq("id", 1)
    stmt = query.build()

    assert "UPDATE" in stmt.sql
    assert "users" in stmt.sql
    assert "SET" in stmt.sql
    assert "WHERE" in stmt.sql


def test_delete_method() -> None:
    """Test sql.delete() method."""
    query = sql.delete().from_("users").where_eq("inactive", True)
    stmt = query.build()

    assert "DELETE FROM" in stmt.sql
    assert "users" in stmt.sql
    assert "WHERE" in stmt.sql


def test_create_table_method_exists() -> None:
    """Test that create_table method exists and works."""
    builder = sql.create_table("test_table")

    assert builder is not None
    assert hasattr(builder, "column")


def test_create_index_method_exists() -> None:
    """Test that create_index method exists and works."""
    builder = sql.create_index("idx_test")

    assert builder is not None
    assert hasattr(builder, "on_table")


def test_drop_table_method_exists() -> None:
    """Test that drop_table method exists and works."""
    builder = sql.drop_table("test_table")

    assert builder is not None
    assert hasattr(builder, "if_exists")


def test_alter_table_method_exists() -> None:
    """Test that alter_table method exists and works."""
    builder = sql.alter_table("test_table")

    assert builder is not None
    assert hasattr(builder, "add_column")


def test_all_ddl_methods_exist() -> None:
    """Test that all expected DDL methods exist on the sql factory."""
    ddl_methods = [
        "create_table",
        "create_view",
        "create_index",
        "create_schema",
        "create_materialized_view",
        "create_table_as_select",
        "drop_table",
        "drop_view",
        "drop_index",
        "drop_schema",
        "alter_table",
        "rename_table",
        "comment_on",
    ]

    for method_name in ddl_methods:
        assert hasattr(sql, method_name), f"sql.{method_name}() should exist"
        method = getattr(sql, method_name)
        assert callable(method), f"sql.{method_name} should be callable"


def test_count_function() -> None:
    """Test sql.count() function."""

    expr = sql.count()
    assert isinstance(expr, AggregateExpression)
    assert hasattr(expr, "as_")
    assert hasattr(expr, "expression")
    assert isinstance(expr.expression, exp.Expression)

    count_column = sql.count("user_id")
    assert isinstance(count_column, AggregateExpression)
    assert hasattr(count_column, "as_")
    assert hasattr(count_column, "expression")
    assert isinstance(count_column.expression, exp.Expression)


def test_sum_function() -> None:
    """Test sql.sum() function."""

    expr = sql.sum("amount")
    assert isinstance(expr, AggregateExpression)
    assert hasattr(expr, "as_")
    assert hasattr(expr, "expression")
    assert isinstance(expr.expression, exp.Expression)


def test_avg_function() -> None:
    """Test sql.avg() function."""

    expr = sql.avg("score")
    assert isinstance(expr, AggregateExpression)
    assert hasattr(expr, "as_")
    assert hasattr(expr, "expression")
    assert isinstance(expr.expression, exp.Expression)


def test_max_function() -> None:
    """Test sql.max() function."""

    expr = sql.max("created_at")
    assert isinstance(expr, AggregateExpression)
    assert hasattr(expr, "as_")
    assert hasattr(expr, "expression")
    assert isinstance(expr.expression, exp.Expression)


def test_min_function() -> None:
    """Test sql.min() function."""

    expr = sql.min("price")
    assert isinstance(expr, AggregateExpression)
    assert hasattr(expr, "as_")
    assert hasattr(expr, "expression")
    assert isinstance(expr.expression, exp.Expression)


def test_column_method() -> None:
    """Test sql.column() method."""
    col = sql.column("name")
    assert col is not None
    assert hasattr(col, "like")
    assert hasattr(col, "in_")

    col_with_table = sql.column("name", "users")
    assert col_with_table is not None


def test_dynamic_column_access() -> None:
    """Test dynamic column access via __getattr__."""
    col = sql.name
    assert col is not None
    assert hasattr(col, "like")
    assert hasattr(col, "in_")

    test_col = sql.some_column_name
    assert test_col is not None


def test_raw_sql_parsing_error() -> None:
    """Test that raw SQL parsing errors raise appropriate exceptions."""
    with pytest.raises(SQLBuilderError) as exc_info:
        sql.raw("INVALID SQL SYNTAX ((())")

    assert "Failed to parse raw SQL fragment" in str(exc_info.value)


def test_empty_raw_sql() -> None:
    """Test raw SQL with empty string raises error."""
    with pytest.raises(SQLBuilderError) as exc_info:
        sql.raw("")

    assert "Failed to parse raw SQL fragment" in str(exc_info.value)


def test_parameter_names_use_column_names() -> None:
    """Test that parameters use column names when possible."""
    query = sql.select("*").from_("users").where_eq("name", "John").where_eq("status", "active")
    stmt = query.build()

    assert "name" in stmt.parameters
    assert "status" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["status"] == "active"


def test_parameter_values_preserved_correctly() -> None:
    """Test that parameter values are preserved exactly."""
    test_values = [("string_val", "test"), ("int_val", 42), ("float_val", math.pi), ("bool_val", True)]

    query = sql.select("*").from_("test")
    for column_name, value in test_values:
        query = query.where_eq(column_name, value)

    stmt = query.build()

    for column_name, expected_value in test_values:
        assert column_name in stmt.parameters
        assert stmt.parameters[column_name] == expected_value

    none_query = sql.select("*").from_("test").where_eq("none_col", None)
    none_stmt = none_query.build()
    assert "none_col" in none_stmt.parameters
    assert none_stmt.parameters["none_col"] is None


def test_case_expression_basic_syntax() -> None:
    """Test basic CASE expression syntax using sql.case_."""
    case_expr = sql.case_.when("status = 'active'", "Active").else_("Inactive").end()

    query = sql.select("id", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "WHEN" in stmt.sql
    assert "ELSE" in stmt.sql
    assert "END" in stmt.sql
    assert "Active" in stmt.sql
    assert "Inactive" in stmt.sql


def test_case_expression_with_alias() -> None:
    """Test CASE expression with alias using as_() method."""
    case_expr = sql.case_.when("status = 'active'", "Active").else_("Inactive").as_("status_display")

    query = sql.select("id", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "status_display" in stmt.sql
    assert "Active" in stmt.sql
    assert "Inactive" in stmt.sql


def test_case_property_syntax() -> None:
    """Test new sql.case_ property syntax."""
    case_expr = sql.case_.when("status = 'active'", "Active").else_("Inactive").end()

    query = sql.select("id", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "WHEN" in stmt.sql
    assert "ELSE" in stmt.sql
    assert "END" in stmt.sql
    assert "Active" in stmt.sql
    assert "Inactive" in stmt.sql


def test_case_property_with_alias() -> None:
    """Test new sql.case_ property syntax with alias."""
    case_expr = sql.case_.when("status = 'active'", "Active").else_("Inactive").as_("status_display")

    query = sql.select("id", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "status_display" in stmt.sql
    assert "Active" in stmt.sql
    assert "Inactive" in stmt.sql


def test_case_multiple_when_clauses() -> None:
    """Test CASE expression with multiple WHEN clauses."""
    case_expr = sql.case_.when("age < 18", "Minor").when("age < 65", "Adult").else_("Senior").end()

    query = sql.select("name", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "Minor" in stmt.sql
    assert "Adult" in stmt.sql
    assert "Senior" in stmt.sql


def test_case_expression_type_compatibility() -> None:
    """Test that all CASE expression variants are compatible with select()."""
    old_case = sql.case().when("x = 1", "one").end()
    new_case = sql.case_.when("x = 2", "two").end()
    aliased_case = sql.case_.when("x = 3", "three").as_("x_desc")

    query = sql.select("id", old_case, new_case, aliased_case).from_("test")
    stmt = query.build()

    assert "SELECT" in stmt.sql
    assert "CASE" in stmt.sql
    assert "one" in stmt.sql
    assert "two" in stmt.sql
    assert "three" in stmt.sql
    assert "x_desc" in stmt.sql


def test_case_property_returns_case_builder() -> None:
    """Test that sql.case_ returns a Case builder instance."""

    case_builder = sql.case_
    assert isinstance(case_builder, Case)
    assert hasattr(case_builder, "when")
    assert hasattr(case_builder, "else_")
    assert hasattr(case_builder, "end")
    assert hasattr(case_builder, "as_")


def test_window_function_shortcuts() -> None:
    """Test window function shortcuts like sql.row_number_."""

    assert isinstance(sql.row_number_, WindowFunctionBuilder)
    assert isinstance(sql.rank_, WindowFunctionBuilder)
    assert isinstance(sql.dense_rank_, WindowFunctionBuilder)


def test_window_function_with_alias() -> None:
    """Test window function with alias and partition/order."""
    window_func = sql.row_number_.partition_by("department").order_by("salary").as_("row_num")

    query = sql.select("name", window_func).from_("employees")
    stmt = query.build()

    assert "ROW_NUMBER()" in stmt.sql
    assert "OVER" in stmt.sql
    assert "PARTITION BY" in stmt.sql
    assert "ORDER BY" in stmt.sql
    assert "row_num" in stmt.sql


def test_window_function_without_alias() -> None:
    """Test window function without alias."""
    window_func = sql.rank_.partition_by("department").order_by("salary").build()

    query = sql.select("name", window_func).from_("employees")
    stmt = query.build()

    assert "RANK()" in stmt.sql
    assert "OVER" in stmt.sql
    assert "PARTITION BY" in stmt.sql
    assert "ORDER BY" in stmt.sql


def test_multiple_window_functions() -> None:
    """Test multiple window functions in same query."""
    row_num = sql.row_number_.partition_by("department").order_by("salary").as_("row_num")
    rank_val = sql.rank_.partition_by("department").order_by("salary").as_("rank_val")

    query = sql.select("name", row_num, rank_val).from_("employees")
    stmt = query.build()

    assert "ROW_NUMBER()" in stmt.sql
    assert "RANK()" in stmt.sql
    assert "row_num" in stmt.sql
    assert "rank_val" in stmt.sql


def test_window_function_multiple_partition_columns() -> None:
    """Test window function with multiple partition and order columns."""
    window_func = sql.dense_rank_.partition_by("department", "team").order_by("salary", "hire_date").build()

    query = sql.select("name", window_func).from_("employees")
    stmt = query.build()

    assert "DENSE_RANK()" in stmt.sql
    assert "PARTITION BY" in stmt.sql
    assert "department" in stmt.sql
    assert "team" in stmt.sql
    assert "salary" in stmt.sql
    assert "hire_date" in stmt.sql


def test_normal_column_access_preserved() -> None:
    """Test that normal column access still works after adding window functions."""

    assert isinstance(sql.department, Column)
    assert isinstance(sql.some_normal_column, Column)

    assert isinstance(sql.row_number_, WindowFunctionBuilder)
    assert isinstance(sql.rank_, WindowFunctionBuilder)


def test_subquery_builders() -> None:
    """Test subquery builder shortcuts."""

    assert isinstance(sql.exists_, SubqueryBuilder)
    assert isinstance(sql.in_, SubqueryBuilder)
    assert isinstance(sql.any_, SubqueryBuilder)
    assert isinstance(sql.all_, SubqueryBuilder)


def test_exists_subquery() -> None:
    """Test EXISTS subquery functionality."""
    subquery = sql.select("1").from_("orders").where_eq("user_id", "123")
    exists_expr = sql.exists_(subquery)

    query = sql.select("*").from_("users").where(exists_expr)
    stmt = query.build()

    assert "EXISTS" in stmt.sql
    assert "SELECT" in stmt.sql
    assert "orders" in stmt.sql


def test_in_subquery() -> None:
    """Test IN subquery functionality."""
    subquery = sql.select("category_id").from_("categories").where_eq("active", True)
    in_expr = sql.in_(subquery)

    from sqlglot.expressions import In

    assert isinstance(in_expr, In)


def test_any_subquery() -> None:
    """Test ANY subquery functionality."""
    subquery = sql.select("salary").from_("employees").where_eq("department", "Engineering")
    any_expr = sql.any_(subquery)

    from sqlglot.expressions import Any

    assert isinstance(any_expr, Any)


def test_all_subquery() -> None:
    """Test ALL subquery functionality."""
    subquery = sql.select("salary").from_("employees").where_eq("department", "Sales")
    all_expr = sql.all_(subquery)

    from sqlglot.expressions import All

    assert isinstance(all_expr, All)


def test_join_builders() -> None:
    """Test join builder shortcuts."""

    assert isinstance(sql.left_join_, JoinBuilder)
    assert isinstance(sql.inner_join_, JoinBuilder)
    assert isinstance(sql.right_join_, JoinBuilder)
    assert isinstance(sql.full_join_, JoinBuilder)
    assert isinstance(sql.cross_join_, JoinBuilder)


def test_left_join_builder() -> None:
    """Test LEFT JOIN builder functionality."""
    join_expr = sql.left_join_("posts").on("users.id = posts.user_id")

    from sqlglot.expressions import Join

    assert isinstance(join_expr, Join)

    query = sql.select("users.name", "posts.title").from_("users").join(join_expr)
    stmt = query.build()

    assert "LEFT JOIN" in stmt.sql
    assert "posts" in stmt.sql
    assert '"users"."id"' in stmt.sql or '"posts"."user_id"' in stmt.sql


def test_inner_join_builder_with_alias() -> None:
    """Test INNER JOIN builder with table alias."""
    join_expr = sql.inner_join_("profiles", "p").on("users.id = p.user_id")

    query = sql.select("users.name", "p.bio").from_("users").join(join_expr)
    stmt = query.build()

    assert "JOIN" in stmt.sql
    assert "profiles" in stmt.sql or "p" in stmt.sql


def test_right_join_builder() -> None:
    """Test RIGHT JOIN builder functionality."""
    join_expr = sql.right_join_("comments").on("posts.id = comments.post_id")

    query = sql.select("posts.title", "comments.content").from_("posts").join(join_expr)
    stmt = query.build()

    assert "RIGHT JOIN" in stmt.sql
    assert "comments" in stmt.sql


def test_full_join_builder() -> None:
    """Test FULL JOIN builder functionality."""
    join_expr = sql.full_join_("archive").on("users.id = archive.user_id")

    query = sql.select("users.name", "archive.data").from_("users").join(join_expr)
    stmt = query.build()

    assert "FULL" in stmt.sql
    assert "JOIN" in stmt.sql
    assert "archive" in stmt.sql


def test_cross_join_builder() -> None:
    """Test CROSS JOIN builder functionality."""
    join_expr = sql.cross_join_("settings").on("1=1")

    query = sql.select("users.name", "settings.value").from_("users").join(join_expr)
    stmt = query.build()

    assert "CROSS" in stmt.sql or "JOIN" in stmt.sql
    assert "settings" in stmt.sql


def test_multiple_join_builders() -> None:
    """Test multiple join builders in same query."""
    left_join = sql.left_join_("posts").on("users.id = posts.user_id")
    inner_join = sql.inner_join_("categories").on("posts.category_id = categories.id")

    query = sql.select("users.name", "posts.title", "categories.name").from_("users").join(left_join).join(inner_join)
    stmt = query.build()

    assert "LEFT JOIN" in stmt.sql
    assert "JOIN" in stmt.sql
    assert "posts" in stmt.sql
    assert "categories" in stmt.sql


def test_backward_compatibility_preserved() -> None:
    """Test that all existing functionality still works with new builders."""

    query1 = sql.select("u.name", "p.title").from_("users u").left_join("posts p", "u.id = p.user_id")
    stmt1 = query1.build()
    assert "LEFT JOIN" in stmt1.sql

    case_expr = sql.case().when("status = 'active'", "Active").else_("Inactive").end()
    query2 = sql.select("name", case_expr).from_("users")
    stmt2 = query2.build()
    assert "CASE" in stmt2.sql

    window_func = sql.row_number_.partition_by("department").order_by("salary").build()
    query3 = sql.select("name", window_func).from_("employees")
    stmt3 = query3.build()
    assert "ROW_NUMBER" in stmt3.sql

    assert isinstance(sql.users, Column)
    assert isinstance(sql.posts, Column)


def test_case_as_method_type_annotation_fix() -> None:
    """Test that sql.case().as_() method returns proper type without 'partially unknown' errors."""

    case_expr = sql.case().when("status = 'active'", "Active").else_("Inactive").as_("status_display")

    query = sql.select("id", "name", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "status_display" in stmt.sql
    assert "Active" in stmt.sql
    assert "Inactive" in stmt.sql

    assert " AS " in stmt.sql or "status_display" in stmt.sql


def test_window_function_as_method_type_annotation_fix() -> None:
    """Test that window function as_() method also has proper type annotations."""
    window_func = sql.row_number_.partition_by("department").order_by("salary").as_("row_num")

    query = sql.select("name", window_func).from_("employees")
    stmt = query.build()

    assert "ROW_NUMBER()" in stmt.sql
    assert "row_num" in stmt.sql
    assert "OVER" in stmt.sql


def test_sql_raw_object_in_select_clause() -> None:
    """Test that SQL objects from sql.raw work in SELECT clauses with parameter merging."""
    raw_expr = sql.raw("COALESCE(name, :default_name)", default_name="Unknown")

    query = sql.select("id", raw_expr).from_("users")
    stmt = query.build()

    assert "COALESCE" in stmt.sql
    assert "default_name" in stmt.parameters
    assert stmt.parameters["default_name"] == "Unknown"
    assert ":default_name" in stmt.sql


def test_sql_raw_object_in_join_conditions() -> None:
    """Test that SQL objects from sql.raw work in JOIN conditions with parameter merging."""
    join_condition = sql.raw("users.id = posts.user_id AND posts.status = :status", status="published")

    query = sql.select("users.name", "posts.title").from_("users").left_join("posts", join_condition)
    stmt = query.build()

    assert "LEFT JOIN" in stmt.sql
    assert "status" in stmt.parameters
    assert stmt.parameters["status"] == "published"
    assert ":status" in stmt.sql


def test_sql_raw_object_in_where_clauses() -> None:
    """Test that SQL objects from sql.raw work in WHERE clauses with parameter merging."""
    where_condition = sql.raw("LENGTH(name) > :min_length", min_length=5)

    query = sql.select("*").from_("users").where(where_condition)
    stmt = query.build()

    assert "LENGTH" in stmt.sql
    assert "min_length" in stmt.parameters
    assert stmt.parameters["min_length"] == 5
    assert ":min_length" in stmt.sql


def test_sql_raw_object_in_distinct_clause() -> None:
    """Test that SQL objects work in DISTINCT clauses with parameter merging."""
    raw_expr = sql.raw("UPPER(category)")

    query = sql.select("*").from_("products").distinct(raw_expr)
    stmt = query.build()

    assert "DISTINCT" in stmt.sql
    assert "UPPER" in stmt.sql


def test_multiple_sql_raw_objects_parameter_merging() -> None:
    """Test that multiple SQL objects properly merge their parameters."""
    select_expr = sql.raw("COALESCE(name, :default_name)", default_name="Unknown")
    join_condition = sql.raw("users.id = posts.user_id AND posts.status = :status", status="published")
    where_condition = sql.raw("users.created_at > :min_date", min_date="2023-01-01")

    query = sql.select("id", select_expr).from_("users").left_join("posts", join_condition).where(where_condition)
    stmt = query.build()

    assert len(stmt.parameters) == 3
    assert stmt.parameters["default_name"] == "Unknown"
    assert stmt.parameters["status"] == "published"
    assert stmt.parameters["min_date"] == "2023-01-01"

    assert ":default_name" in stmt.sql
    assert ":status" in stmt.sql
    assert ":min_date" in stmt.sql


def test_sql_raw_without_parameters_still_works() -> None:
    """Test that SQL objects without parameters still work correctly."""
    raw_expr = sql.raw("NOW()")

    query = sql.select("id", raw_expr).from_("logs")
    stmt = query.build()

    assert "NOW()" in stmt.sql
    assert len(stmt.parameters) == 0


def test_mixed_sql_objects_and_regular_parameters() -> None:
    """Test mixing SQL objects with regular builder parameters."""
    raw_expr = sql.raw("UPPER(name)")

    query = (
        sql
        .select("id", raw_expr)
        .from_("users")
        .where_eq("status", "active")
        .where(sql.raw("created_at > :min_date", min_date="2023-01-01"))
    )
    stmt = query.build()

    assert "status" in stmt.parameters
    assert "min_date" in stmt.parameters
    assert stmt.parameters["status"] == "active"
    assert stmt.parameters["min_date"] == "2023-01-01"

    assert "UPPER" in stmt.sql
    assert ":status" in stmt.sql
    assert ":min_date" in stmt.sql


def test_sql_raw_parameter_name_conflicts_handled() -> None:
    """Test that parameter name conflicts are detected when merging SQL objects."""

    raw_expr1 = sql.raw("COALESCE(name, :value)", value="default1")
    raw_expr2 = sql.raw("COALESCE(email, :other_value)", other_value="default2")

    query = sql.select("id", raw_expr1, raw_expr2).from_("users")
    stmt = query.build()

    assert "value" in stmt.parameters
    assert "other_value" in stmt.parameters
    assert stmt.parameters["value"] == "default1"
    assert stmt.parameters["other_value"] == "default2"

    raw_conflict1 = sql.raw("COALESCE(name, :conflict)", conflict="first")
    raw_conflict2 = sql.raw("COALESCE(email, :conflict)", conflict="second")

    with pytest.raises(SQLBuilderError, match="Parameter name 'conflict' already exists"):
        sql.select("id", raw_conflict1, raw_conflict2).from_("users").build()


def test_original_user_case_example_regression_test() -> None:
    """Regression test for the exact user example that was failing."""

    case_expr = sql.case().when("password IS NOT NULL", True).else_(False).as_("has_password")

    query = sql.select("id", "name", case_expr).from_("users")
    stmt = query.build()

    assert "CASE" in stmt.sql
    assert "has_password" in stmt.sql
    assert "password" in stmt.sql and ("NULL" in stmt.sql or "IS" in stmt.sql)

    update_query = sql.update("users").set({"last_check": sql.raw("NOW()")}).where(case_expr)
    update_stmt = update_query.build()

    assert "UPDATE" in update_stmt.sql
    assert "CASE" in update_stmt.sql


def test_type_compatibility_across_all_operations() -> None:
    """Test that SQL objects work across all major SQL operations."""

    raw_condition = sql.raw("LENGTH(name) > :min_len", min_len=3)
    raw_value = sql.raw("UPPER(:new_name)", new_name="test")
    raw_select = sql.raw("COUNT(*) as total")

    select_query = sql.select("id", raw_select).from_("users").where(raw_condition)
    select_stmt = select_query.build()
    assert "COUNT(*)" in select_stmt.sql
    assert "min_len" in select_stmt.parameters

    update_query = sql.update("users").set(name=raw_value, status="updated").where(raw_condition)
    update_stmt = update_query.build()
    assert "UPDATE" in update_stmt.sql
    assert "min_len" in update_stmt.parameters
    assert "new_name" in update_stmt.parameters
    assert "status" in update_stmt.parameters

    delete_query = sql.delete().from_("users").where(raw_condition)
    delete_stmt = delete_query.build()
    assert "DELETE" in delete_stmt.sql
    assert "min_len" in delete_stmt.parameters


def test_update_set_method_with_sql_objects() -> None:
    """Test that UPDATE.set_() method properly handles SQL objects with kwargs."""
    raw_timestamp = sql.raw("NOW()")
    raw_computed = sql.raw("UPPER(:value)", value="test")

    query = (
        sql.update("users").set(name="John", last_updated=raw_timestamp, computed_field=raw_computed).where_eq("id", 1)
    )

    stmt = query.build()

    assert "UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "UPPER" in stmt.sql
    assert "name" in stmt.parameters
    assert "value" in stmt.parameters
    assert "id" in stmt.parameters
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["value"] == "test"
    assert stmt.parameters["id"] == 1


def test_update_set_method_backward_compatibility() -> None:
    """Test that UPDATE.set_() method maintains backward compatibility with dict."""
    raw_timestamp = sql.raw("NOW()")

    query1 = sql.update("users").set({"name": "John", "updated_at": raw_timestamp})
    stmt1 = query1.build()

    assert "UPDATE" in stmt1.sql
    assert "NOW()" in stmt1.sql
    assert "name" in stmt1.parameters
    assert stmt1.parameters["name"] == "John"

    query2 = sql.update("users").set("status", "active")
    stmt2 = query2.build()

    assert "UPDATE" in stmt2.sql
    assert "status" in stmt2.parameters
    assert stmt2.parameters["status"] == "active"


def test_on_conflict_do_nothing_basic() -> None:
    """Test basic ON CONFLICT DO NOTHING functionality."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict("id").do_nothing()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    assert '"id"' in stmt.sql or "id" in stmt.sql
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert stmt.parameters["id"] == 1
    assert stmt.parameters["name"] == "John"


def test_on_conflict_do_nothing_multiple_columns() -> None:
    """Test ON CONFLICT DO NOTHING with multiple conflict columns."""
    query = (
        sql
        .insert("users")
        .columns("email", "username", "name")
        .values("john@test.com", "john", "John")
        .on_conflict("email", "username")
        .do_nothing()
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    assert "email" in stmt.sql and "username" in stmt.sql
    assert "email" in stmt.parameters
    assert "username" in stmt.parameters
    assert "name" in stmt.parameters


def test_on_conflict_do_nothing_no_columns() -> None:
    """Test ON CONFLICT DO NOTHING without specific columns (catches all conflicts)."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict().do_nothing()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql

    assert "ON CONFLICT(" not in stmt.sql


def test_on_conflict_do_update_basic() -> None:
    """Test basic ON CONFLICT DO UPDATE functionality."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict("id").do_update(name="Updated John")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert stmt.parameters["id"] == 1
    assert stmt.parameters["name"] == "John"
    assert stmt.parameters["name_1"] == "Updated John"


def test_on_conflict_do_update_multiple_values() -> None:
    """Test ON CONFLICT DO UPDATE with multiple update values."""
    query = (
        sql
        .insert("users")
        .columns("id", "name", "email")
        .values(1, "John", "john@test.com")
        .on_conflict("id")
        .do_update(name="Updated John", email="updated@test.com")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "name_1" in stmt.parameters
    assert "email_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated John"
    assert stmt.parameters["email_1"] == "updated@test.com"


def test_on_conflict_do_update_with_sql_raw() -> None:
    """Test ON CONFLICT DO UPDATE with sql.raw expressions."""
    query = (
        sql
        .insert("users")
        .columns("id", "name")
        .values(1, "John")
        .on_conflict("id")
        .do_update(updated_at=sql.raw("NOW()"), name="Updated")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"


def test_on_conflict_do_update_with_sql_raw_parameters() -> None:
    """Test ON CONFLICT DO UPDATE with sql.raw that has parameters."""
    query = (
        sql
        .insert("users")
        .columns("id", "name")
        .values(1, "John")
        .on_conflict("id")
        .do_update(
            updated_at=sql.raw("NOW()"), status=sql.raw("COALESCE(:new_status, 'active')", new_status="verified")
        )
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "COALESCE" in stmt.sql
    assert "new_status" in stmt.parameters
    assert stmt.parameters["new_status"] == "verified"


def test_on_conflict_convenience_method() -> None:
    """Test the convenience method on_conflict_do_nothing."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict_do_nothing("id")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql
    assert "id" in stmt.parameters
    assert stmt.parameters["id"] == 1


def test_legacy_on_duplicate_key_update_method() -> None:
    """Test that the legacy on_duplicate_key_update method generates MySQL syntax."""
    query = (
        sql
        .insert("users")
        .columns("id", "name")
        .values(1, "John")
        .on_duplicate_key_update(name="Updated", updated_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON DUPLICATE KEY UPDATE" in stmt.sql
    assert "SET" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated"


def test_on_conflict_with_insert_from_dict() -> None:
    """Test ON CONFLICT with insert using from_dict methods."""
    data = {"id": 1, "name": "John", "email": "john@test.com"}
    query = sql.insert("users").values_from_dict(data).on_conflict("id").do_update(name="Updated John")
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "name_1" in stmt.parameters
    assert stmt.parameters["name_1"] == "Updated John"


def test_on_conflict_with_multiple_rows() -> None:
    """Test ON CONFLICT with multiple value rows."""
    query = sql.insert("users").columns("id", "name").values(1, "John").values(2, "Jane").on_conflict("id").do_nothing()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO NOTHING" in stmt.sql

    assert "id" in stmt.parameters
    assert "name" in stmt.parameters
    assert "id_1" in stmt.parameters
    assert "name_1" in stmt.parameters


def test_on_conflict_chaining_with_returning() -> None:
    """Test ON CONFLICT chaining with RETURNING clause."""
    query = (
        sql
        .insert("users")
        .columns("id", "name")
        .values(1, "John")
        .on_conflict("id")
        .do_update(name="Updated John")
        .returning("id", "name", "updated_at")
    )
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql
    assert "RETURNING" in stmt.sql
    assert "id" in stmt.sql and "name" in stmt.sql and "updated_at" in stmt.sql


def test_on_conflict_empty_do_update() -> None:
    """Test ON CONFLICT DO UPDATE with no arguments (should work but do nothing)."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict("id").do_update()
    stmt = query.build()

    assert "INSERT INTO" in stmt.sql
    assert "ON CONFLICT" in stmt.sql
    assert "DO UPDATE" in stmt.sql


def test_on_conflict_sql_generation_postgres_style() -> None:
    """Test that ON CONFLICT generates PostgreSQL-style syntax that SQLGlot can transpile."""
    query = sql.insert("users").columns("id", "name").values(1, "John").on_conflict("id").do_update(name="Updated")
    stmt = query.build()

    assert "ON CONFLICT(" in stmt.sql or "ON CONFLICT (" in stmt.sql
    assert "DO UPDATE SET" in stmt.sql
    assert '"id"' in stmt.sql or "id" in stmt.sql


def test_on_conflict_type_safety() -> None:
    """Test that ON CONFLICT methods return proper types for method chaining."""

    query_builder = sql.insert("users").columns("id", "name").values(1, "John")

    conflict_builder = query_builder.on_conflict("id")
    assert hasattr(conflict_builder, "do_nothing")
    assert hasattr(conflict_builder, "do_update")

    final_builder = conflict_builder.do_nothing()
    assert hasattr(final_builder, "returning")
    assert hasattr(final_builder, "build")

    final_query = final_builder.returning("id")
    stmt = final_query.build()
    assert "RETURNING" in stmt.sql


def test_merge_when_matched_then_update_with_kwargs() -> None:
    """Test MERGE when_matched_then_update with kwargs support."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_matched_then_update(name="Updated John", email="updated@test.com")
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN MATCHED THEN UPDATE" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert stmt.parameters["name"] == "Updated John"
    assert stmt.parameters["email"] == "updated@test.com"


def test_merge_when_matched_then_update_mixed_dict_kwargs() -> None:
    """Test MERGE when_matched_then_update with mixed dict and kwargs."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_matched_then_update({"name": "Dict Name"}, email="Kwargs Email", status="active")
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN MATCHED THEN UPDATE" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "status" in stmt.parameters
    assert stmt.parameters["name"] == "Dict Name"
    assert stmt.parameters["email"] == "Kwargs Email"
    assert stmt.parameters["status"] == "active"


def test_merge_when_matched_then_update_with_sql_raw() -> None:
    """Test MERGE when_matched_then_update with sql.raw expressions."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_matched_then_update(
            name="Updated John",
            updated_at=sql.raw("NOW()"),
            status=sql.raw("COALESCE(:new_status, 'active')", new_status="verified"),
        )
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN MATCHED THEN UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "COALESCE" in stmt.sql
    assert "name" in stmt.parameters
    assert "new_status" in stmt.parameters
    assert stmt.parameters["name"] == "Updated John"
    assert stmt.parameters["new_status"] == "verified"


def test_merge_when_not_matched_by_source_then_update_with_kwargs() -> None:
    """Test MERGE when_not_matched_by_source_then_update with kwargs support."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_not_matched_by_source_then_update(status="inactive", last_seen=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "status" in stmt.parameters
    assert stmt.parameters["status"] == "inactive"


def test_merge_when_not_matched_by_source_then_update_mixed() -> None:
    """Test MERGE when_not_matched_by_source_then_update with mixed dict and kwargs."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_not_matched_by_source_then_update(
            {"status": "Dict Status"}, last_seen=sql.raw("NOW()"), notes="Kwargs Notes"
        )
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "status" in stmt.parameters
    assert "notes" in stmt.parameters
    assert stmt.parameters["status"] == "Dict Status"
    assert stmt.parameters["notes"] == "Kwargs Notes"


def test_merge_empty_update_values_error() -> None:
    """Test that MERGE update methods raise error when no values provided."""
    merge_builder = sql.merge("users").using("new_users").on("users.id = new_users.id")

    with pytest.raises(SQLBuilderError, match="No update values provided"):
        merge_builder.when_matched_then_update()

    with pytest.raises(SQLBuilderError, match="No update values provided"):
        merge_builder.when_not_matched_by_source_then_update()


def test_merge_backward_compatibility() -> None:
    """Test that MERGE methods maintain backward compatibility with dict-only usage."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_matched_then_update({"name": "Updated", "email": "updated@test.com"})
        .when_not_matched_by_source_then_update({"status": "inactive"})
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN MATCHED THEN UPDATE" in stmt.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN UPDATE" in stmt.sql
    assert "name" in stmt.parameters
    assert "email" in stmt.parameters
    assert "status" in stmt.parameters


def test_merge_complete_example() -> None:
    """Test MERGE example with all features."""
    query = (
        sql
        .merge("users")
        .using("new_users")
        .on("users.id = new_users.id")
        .when_matched_then_update(name="new_users.name", email="new_users.email", updated_at=sql.raw("NOW()"))
        .when_not_matched_then_insert(
            ["id", "name", "email", "created_at"],
            ["new_users.id", "new_users.name", "new_users.email", sql.raw("NOW()")],
        )
        .when_not_matched_by_source_then_update(status="archived", archived_at=sql.raw("NOW()"))
    )
    stmt = query.build()

    assert "MERGE INTO" in stmt.sql
    assert "WHEN MATCHED THEN UPDATE" in stmt.sql
    assert "WHEN NOT MATCHED THEN INSERT" in stmt.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN UPDATE" in stmt.sql
    assert "NOW()" in stmt.sql
    assert "status" in stmt.parameters
    assert stmt.parameters["status"] == "archived"
    assert len(stmt.parameters) == 1


def test_querybuilder_parameter_style_handling_regression() -> None:
    """Regression test for QueryBuilder parameter style handling fix."""

    query = sql.select("id", "name", "price").from_("products").where("category = $1", "Electronics")
    stmt = query.build()

    assert "WHERE" in stmt.sql
    assert "category" in stmt.sql
    assert len(stmt.parameters) == 1
    assert "Electronics" in stmt.parameters.values()


def test_querybuilder_handles_all_parameter_styles() -> None:
    """Test that QueryBuilder handles all parameter styles correctly."""

    query1 = sql.select("*").from_("test_table").where("category = $1", "Electronics")
    stmt1 = query1.build()
    assert "WHERE" in stmt1.sql
    assert len(stmt1.parameters) == 1
    assert "Electronics" in stmt1.parameters.values()

    query2 = sql.select("*").from_("test_table").where("status = :status", status="active")
    stmt2 = query2.build()
    assert "WHERE" in stmt2.sql
    assert len(stmt2.parameters) == 1
    assert "active" in stmt2.parameters.values()

    query3 = sql.select("*").from_("test_table").where("name = ?", "John")
    stmt3 = query3.build()
    assert "WHERE" in stmt3.sql
    assert len(stmt3.parameters) == 1
    assert "John" in stmt3.parameters.values()


def test_querybuilder_parameter_conversion_preserves_functionality() -> None:
    """Test that parameter conversion in QueryBuilder preserves all functionality."""

    query = sql.select("*").from_("orders").where("total > $1 AND status = $2", 100.0, "pending")
    stmt = query.build()

    assert "WHERE" in stmt.sql
    assert len(stmt.parameters) == 2
    assert 100.0 in stmt.parameters.values()
    assert "pending" in stmt.parameters.values()

    complex_query = sql.select("*").from_("events").where("created_at > $1 AND type = $2", "2023-01-01", "click")
    complex_stmt = complex_query.build()

    assert "WHERE" in complex_stmt.sql
    assert len(complex_stmt.parameters) == 2
    assert "2023-01-01" in complex_stmt.parameters.values()
    assert "click" in complex_stmt.parameters.values()


def test_sql_call_with_update_returning() -> None:
    """Test that sql() accepts UPDATE statements with RETURNING clause."""
    update_sql = "UPDATE books SET title = :title, pages = :pages WHERE id = :id RETURNING *"
    query = sql(update_sql)

    assert isinstance(query, SQL)
    assert query.returns_rows()
    assert "UPDATE" in query.sql.upper()
    assert "RETURNING" in query.sql.upper()


def test_sql_call_with_insert_returning() -> None:
    """Test that sql() accepts INSERT statements with RETURNING clause."""
    insert_sql = "INSERT INTO books (title, pages) VALUES (:title, :pages) RETURNING id, title"
    query = sql(insert_sql)

    assert isinstance(query, SQL)
    assert query.returns_rows()
    assert "INSERT" in query.sql.upper()
    assert "RETURNING" in query.sql.upper()


def test_sql_call_with_delete_returning() -> None:
    """Test that sql() accepts DELETE statements with RETURNING clause."""
    delete_sql = "DELETE FROM books WHERE id = :id RETURNING id, title"
    query = sql(delete_sql)

    assert isinstance(query, SQL)
    assert query.returns_rows()
    assert "DELETE" in query.sql.upper()
    assert "RETURNING" in query.sql.upper()


def test_sql_call_rejects_update_without_returning() -> None:
    """Test that sql() rejects UPDATE statements without RETURNING clause."""
    update_sql = "UPDATE books SET title = :title WHERE id = :id"

    with pytest.raises(SQLBuilderError) as exc_info:
        sql(update_sql)

    assert "only supports SELECT statements or DML statements with RETURNING clause" in str(exc_info.value)
    assert "UPDATE" in str(exc_info.value)


def test_sql_call_rejects_insert_without_returning() -> None:
    """Test that sql() rejects INSERT statements without RETURNING clause."""
    insert_sql = "INSERT INTO books (title, pages) VALUES (:title, :pages)"

    with pytest.raises(SQLBuilderError) as exc_info:
        sql(insert_sql)

    assert "only supports SELECT statements or DML statements with RETURNING clause" in str(exc_info.value)
    assert "INSERT" in str(exc_info.value)


def test_sql_call_rejects_delete_without_returning() -> None:
    """Test that sql() rejects DELETE statements without RETURNING clause."""
    delete_sql = "DELETE FROM books WHERE id = :id"

    with pytest.raises(SQLBuilderError) as exc_info:
        sql(delete_sql)

    assert "only supports SELECT statements or DML statements with RETURNING clause" in str(exc_info.value)
    assert "DELETE" in str(exc_info.value)


def test_sql_update_method_with_returning() -> None:
    """Test that sql.update() returns Update builder for statements with RETURNING (use sql() for SQL object)."""

    update_sql = "UPDATE books SET title = :title WHERE id = :id RETURNING *"
    query = sql.update(update_sql)

    assert isinstance(query, Update)
    # For RETURNING statements, use sql() instead to get SQL object
    sql_query = sql(update_sql)
    assert isinstance(sql_query, SQL)
    assert sql_query.returns_rows()


def test_sql_insert_method_with_returning() -> None:
    """Test that sql.insert() returns Insert builder for statements with RETURNING (use sql() for SQL object)."""

    insert_sql = "INSERT INTO books (title) VALUES (:title) RETURNING id, title"
    query = sql.insert(insert_sql)

    assert isinstance(query, Insert)
    # For RETURNING statements, use sql() instead to get SQL object
    sql_query = sql(insert_sql)
    assert isinstance(sql_query, SQL)
    assert sql_query.returns_rows()


def test_sql_delete_method_with_returning() -> None:
    """Test that sql.delete() returns Delete builder for statements with RETURNING (use sql() for SQL object)."""

    delete_sql = "DELETE FROM books WHERE id = :id RETURNING *"
    query = sql.delete(delete_sql)

    assert isinstance(query, Delete)
    # For RETURNING statements, use sql() instead to get SQL object
    sql_query = sql(delete_sql)
    assert isinstance(sql_query, SQL)
    assert sql_query.returns_rows()


def test_sql_update_method_without_returning_returns_builder() -> None:
    """Test that sql.update() returns Update builder for statements without RETURNING."""

    update_sql = "UPDATE books SET title = :title WHERE id = :id"
    query = sql.update(update_sql)

    assert isinstance(query, Update)
    assert not isinstance(query, SQL)


def test_sql_insert_method_without_returning_returns_builder() -> None:
    """Test that sql.insert() returns Insert builder for statements without RETURNING."""

    insert_sql = "INSERT INTO books (title) VALUES (:title)"
    query = sql.insert(insert_sql)

    assert isinstance(query, Insert)
    assert not isinstance(query, SQL)


def test_sql_delete_method_without_returning_returns_builder() -> None:
    """Test that sql.delete() returns Delete builder for statements without RETURNING."""

    delete_sql = "DELETE FROM books WHERE id = :id"
    query = sql.delete(delete_sql)

    assert isinstance(query, Delete)
    assert not isinstance(query, SQL)


def test_select_statements_still_work_with_sql_call() -> None:
    """Test that SELECT statements continue to work with sql()."""

    select_sql = "SELECT * FROM books WHERE id = :id"
    query = sql(select_sql)

    assert isinstance(query, Select)
    assert not isinstance(query, SQL)


def test_with_statements_still_work_with_sql_call() -> None:
    """Test that WITH statements continue to work with sql()."""

    with_sql = "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) as rn FROM books) SELECT * FROM ranked"
    query = sql(with_sql)

    assert isinstance(query, Select)
    assert not isinstance(query, SQL)


def test_for_update_with_sql_factory() -> None:
    """Test sql.select().for_update() combinations."""
    query = sql.select("id", "status", dialect="postgres").from_("job").for_update()
    stmt = query.build()

    assert "FOR UPDATE" in stmt.sql
    assert "SELECT" in stmt.sql
    assert "job" in stmt.sql


def test_for_update_chaining_with_where() -> None:
    """Test for_update chaining with other operations."""
    query = (
        sql
        .select("id", "status", dialect="postgres")
        .from_("job")
        .where_eq("user_id", 123)
        .for_update(skip_locked=True)
    )
    stmt = query.build()

    assert "FOR UPDATE SKIP LOCKED" in stmt.sql
    # PostgreSQL uses %(param)s format, not :param format
    assert "%(user_id)s" in stmt.sql or ":user_id" in stmt.sql
    assert stmt.parameters["user_id"] == 123


def test_for_share_with_sql_factory() -> None:
    """Test sql.select().for_share() functionality."""
    query = sql.select("*", dialect="postgres").from_("job").for_share(nowait=True)
    stmt = query.build()

    assert "FOR SHARE NOWAIT" in stmt.sql


def test_for_update_of_with_parameters() -> None:
    """Test FOR UPDATE OF with parameter binding."""
    query = (
        sql
        .select("j.id", "u.name", dialect="postgres")
        .from_("job j")
        .join("users u ON j.user_id = u.id")
        .where_eq("j.status", "pending")
        .for_update(of=["j"])
    )
    stmt = query.build()

    assert "FOR UPDATE OF j" in stmt.sql
    # PostgreSQL uses %(param)s format, not :param format
    assert "%(status)s" in stmt.sql or ":status" in stmt.sql
    assert stmt.parameters["status"] == "pending"


def test_build_copy_from_statement_generates_expected_sql() -> None:
    statement = build_copy_from_statement(
        "public.users", "s3://bucket/data.parquet", columns=("id", "name"), options={"format": "parquet"}
    )

    assert isinstance(statement, SQL)
    rendered = statement.sql
    assert rendered == "COPY \"public.users\" (id, name) FROM 's3://bucket/data.parquet' WITH (FORMAT 'parquet')"

    expression = parse_one(rendered, read="postgres")
    assert expression.args["kind"] is True


def test_build_copy_to_statement_generates_expected_sql() -> None:
    statement = build_copy_to_statement(
        "public.users", "s3://bucket/output.parquet", options={"format": "parquet", "compression": "gzip"}
    )

    assert isinstance(statement, SQL)
    rendered = statement.sql
    assert rendered == (
        "COPY \"public.users\" TO 's3://bucket/output.parquet' WITH (FORMAT 'parquet', COMPRESSION 'gzip')"
    )

    expression = parse_one(rendered, read="postgres")
    assert expression.args["kind"] is False


def test_sql_factory_copy_helpers() -> None:
    statement = sql.copy_from("users", "s3://bucket/in.csv", columns=("id", "name"), options={"format": "csv"})
    assert isinstance(statement, SQL)
    assert statement.sql.startswith("COPY users")

    to_statement = sql.copy("users", target="s3://bucket/out.csv", options={"format": "csv", "header": True})
    assert isinstance(to_statement, SQL)
    parsed = parse_one(to_statement.sql, read="postgres")
    assert parsed.args["kind"] is False
