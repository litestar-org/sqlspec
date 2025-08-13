"""Unit tests for SQL factory functionality including parameter binding fixes and new features."""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec._sql import SQLFactory
from sqlspec.core.statement import SQL
from sqlspec.exceptions import SQLBuilderError


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
    test_cases = [
        ("where_lt", lambda q: q.where_lt("age", 18), "age"),
        ("where_lte", lambda q: q.where_lte("score", 100), "score"),
        ("where_gt", lambda q: q.where_gt("price", 50.0), "price"),
        ("where_gte", lambda q: q.where_gte("rating", 3.5), "rating"),
    ]

    for method_name, query_builder, column_name in test_cases:
        query = query_builder(sql.select("*").from_("test_table"))  # type: ignore[no-untyped-call]
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
    # The raw expression should be included directly, not as a parameter
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
    assert stmt.parameters["value"] == "test"
    assert stmt.sql.count(":value") == 2
    assert len(stmt.parameters) == 1


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
        sql.insert("team_member")
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
    assert isinstance(expr, exp.Expression)

    count_column = sql.count("user_id")
    assert isinstance(count_column, exp.Expression)


def test_sum_function() -> None:
    """Test sql.sum() function."""
    expr = sql.sum("amount")
    assert isinstance(expr, exp.Expression)


def test_avg_function() -> None:
    """Test sql.avg() function."""
    expr = sql.avg("score")
    assert isinstance(expr, exp.Expression)


def test_max_function() -> None:
    """Test sql.max() function."""
    expr = sql.max("created_at")
    assert isinstance(expr, exp.Expression)


def test_min_function() -> None:
    """Test sql.min() function."""
    expr = sql.min("price")
    assert isinstance(expr, exp.Expression)


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
    test_values = [("string_val", "test"), ("int_val", 42), ("float_val", 3.14159), ("bool_val", True)]

    query = sql.select("*").from_("test")
    for column_name, value in test_values:
        query = query.where_eq(column_name, value)

    stmt = query.build()

    for column_name, expected_value in test_values:
        assert column_name in stmt.parameters
        assert stmt.parameters[column_name] == expected_value

    # Test None value separately - it creates a parameter with None value
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
    from sqlspec._sql import Case

    case_builder = sql.case_
    assert isinstance(case_builder, Case)
    assert hasattr(case_builder, "when")
    assert hasattr(case_builder, "else_")
    assert hasattr(case_builder, "end")
    assert hasattr(case_builder, "as_")


def test_window_function_shortcuts() -> None:
    """Test window function shortcuts like sql.row_number_."""
    from sqlspec._sql import WindowFunctionBuilder

    # Test that shortcuts return WindowFunctionBuilder instances
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
    # This should still return a Column, not a WindowFunctionBuilder
    from sqlspec.builder._column import Column

    assert isinstance(sql.department, Column)
    assert isinstance(sql.some_normal_column, Column)

    # But these should return WindowFunctionBuilder
    from sqlspec._sql import WindowFunctionBuilder

    assert isinstance(sql.row_number_, WindowFunctionBuilder)
    assert isinstance(sql.rank_, WindowFunctionBuilder)


def test_subquery_builders() -> None:
    """Test subquery builder shortcuts."""
    from sqlspec._sql import SubqueryBuilder

    # Test that shortcuts return SubqueryBuilder instances
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
    # Note: The subquery parameters are embedded in the SQL structure


def test_in_subquery() -> None:
    """Test IN subquery functionality."""
    subquery = sql.select("category_id").from_("categories").where_eq("active", True)
    in_expr = sql.in_(subquery)

    # Test that the expression is created correctly
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
    from sqlspec._sql import JoinBuilder

    # Test that shortcuts return JoinBuilder instances
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

    # Test in a complete query
    query = sql.select("users.name", "posts.title").from_("users").join(join_expr)
    stmt = query.build()

    assert "LEFT JOIN" in stmt.sql
    assert "posts" in stmt.sql
    assert "users.id" in stmt.sql or "posts.user_id" in stmt.sql


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
    join_expr = sql.cross_join_("settings").on("1=1")  # ON condition ignored for CROSS JOIN

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
    # Original join methods should still work
    query1 = sql.select("u.name", "p.title").from_("users u").left_join("posts p", "u.id = p.user_id")
    stmt1 = query1.build()
    assert "LEFT JOIN" in stmt1.sql

    # Original case syntax should still work
    case_expr = sql.case().when("status = 'active'", "Active").else_("Inactive").end()
    query2 = sql.select("name", case_expr).from_("users")
    stmt2 = query2.build()
    assert "CASE" in stmt2.sql

    # New window functions work
    window_func = sql.row_number_.partition_by("department").order_by("salary").build()
    query3 = sql.select("name", window_func).from_("employees")
    stmt3 = query3.build()
    assert "ROW_NUMBER" in stmt3.sql

    # Column access should still work
    from sqlspec.builder._column import Column

    assert isinstance(sql.users, Column)
    assert isinstance(sql.posts, Column)
