"""Unit tests for SelectBuilder functionality.

This module tests the SelectBuilder including:
- Basic SELECT operations
- DISTINCT support
- JOIN operations (INNER, LEFT, RIGHT, CROSS)
- WHERE conditions and helpers (LIKE, BETWEEN, IN, EXISTS, NULL, ANY)
- Aggregate functions (COUNT, SUM, AVG, MAX, MIN)
- Window functions with OVER clauses
- CASE expressions
- Set operations (UNION, INTERSECT, EXCEPT)
- GROUP BY with ROLLUP
- PIVOT/UNPIVOT operations
- Query hints
- Parameter handling and SQL injection prevention
- Schema type setting with as_schema
"""

from typing import TYPE_CHECKING, Any, Optional

import pytest
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import SelectBuilder
from sqlspec.statement.builder.mixins._case_builder import CaseBuilder

if TYPE_CHECKING:
    pass

# Optional dependencies
try:
    import pydantic
except ImportError:
    pydantic = None


# Test schema type setting with as_schema
@pytest.mark.skipif(pydantic is None, reason="pydantic not installed")
def test_as_schema_with_pydantic_model() -> None:
    """Test as_schema with Pydantic model."""
    BaseModel = getattr(pydantic, "BaseModel", object)

    class UserModel(BaseModel):  # type: ignore[misc,valid-type]
        id: int
        name: str

    builder = SelectBuilder().select("id", "name").from_("users")
    new_builder = builder.as_schema(UserModel)

    assert getattr(new_builder, "_schema", None) is UserModel
    assert new_builder is not builder
    assert new_builder.build().sql == builder.build().sql


def test_as_schema_with_dataclass() -> None:
    """Test as_schema with dataclass."""
    from dataclasses import dataclass

    @dataclass
    class User:
        id: int
        name: str

    builder = SelectBuilder().select("id", "name").from_("users")
    new_builder = builder.as_schema(User)  # type: ignore[arg-type]

    assert getattr(new_builder, "_schema", None) is User
    assert new_builder is not builder
    assert new_builder.build().sql == builder.build().sql


def test_as_schema_with_dict_type() -> None:
    """Test as_schema with dict type."""
    builder = SelectBuilder().select("id", "name").from_("users")
    new_builder = builder.as_schema(dict)

    assert getattr(new_builder, "_schema", None) is dict
    assert new_builder is not builder


def test_as_schema_preserves_parameters() -> None:
    """Test that as_schema preserves query parameters."""
    builder = SelectBuilder().select("id").from_("users").where(("name", "John"))
    new_builder = builder.as_schema(dict)

    assert new_builder.build().parameters == builder.build().parameters


# Test basic SELECT operations
@pytest.mark.parametrize(
    "columns,expected_count",
    [
        (["id"], 1),
        (["id", "name"], 2),
        (["id", "name", "email"], 3),
        (["*"], 1),
        ([], 0),  # No columns
    ],
    ids=["single_column", "two_columns", "three_columns", "star", "no_columns"],
)
def test_select_columns(columns: list[str], expected_count: int) -> None:
    """Test selecting various column combinations."""
    builder = SelectBuilder()
    if columns:
        builder = builder.select(*columns)
    builder = builder.from_("users")

    query = builder.build()
    assert 'FROM "users"' in query.sql or "FROM users" in query.sql
    if expected_count > 0:
        assert "SELECT" in query.sql


# Test DISTINCT functionality
@pytest.mark.parametrize(
    "distinct_cols,all_cols",
    [
        ([], ["id", "name"]),  # DISTINCT without specific columns
        (["id"], ["id", "name"]),  # DISTINCT on one column
        (["id", "name"], ["id", "name", "email"]),  # DISTINCT on multiple columns
    ],
    ids=["distinct_all", "distinct_single", "distinct_multiple"],
)
def test_distinct_variations(distinct_cols: list[str], all_cols: list[str]) -> None:
    """Test DISTINCT with various column specifications."""
    builder = SelectBuilder().select(*all_cols)
    if distinct_cols:
        builder = builder.distinct(*distinct_cols)
    else:
        builder = builder.distinct()
    builder = builder.from_("users")

    query = builder.build()
    assert "DISTINCT" in query.sql
    for col in all_cols:
        assert col in query.sql


# Test JOIN operations
@pytest.mark.parametrize(
    "join_type,method_name",
    [("INNER", "join"), ("LEFT", "left_join"), ("RIGHT", "right_join"), ("CROSS", "cross_join")],
    ids=["inner_join", "left_join", "right_join", "cross_join"],
)
def test_join_types(join_type: str, method_name: str) -> None:
    """Test different JOIN types."""
    builder = SelectBuilder().select("*").from_("users")

    # Get the join method dynamically
    join_method = getattr(builder, method_name)
    if join_type == "CROSS":
        builder = join_method("orders")
    else:
        builder = join_method("orders", on="users.id = orders.user_id")

    query = builder.build()
    # For INNER JOIN, sqlglot might generate just "JOIN" which is equivalent
    if join_type == "INNER":
        assert "JOIN" in query.sql or "INNER JOIN" in query.sql
    else:
        assert f"{join_type} JOIN" in query.sql
    assert "users" in query.sql
    assert "orders" in query.sql


# Test WHERE condition helpers
@pytest.mark.parametrize(
    "method,args,expected_sql_parts",
    [
        ("where_like", ("name", "%John%"), ["LIKE"]),
        ("where_like", ("name", "John\\_%", "\\"), ["LIKE"]),  # With escape
        ("where_between", ("age", 18, 65), ["BETWEEN"]),
        ("where_in", ("id", [1, 2, 3]), ["IN"]),
        ("where_in", ("status", ("active", "pending")), ["IN"]),
        ("where_not_in", ("status", ["banned", "deleted"]), ["NOT IN", "NOT", "IN"]),
        ("where_null", ("deleted_at",), ["IS NULL"]),
        ("where_not_null", ("email",), ["IS NOT NULL", "NOT", "IS NULL"]),
        ("where_any", ("id", [1, 2, 3]), ["= ANY"]),
        ("where_not_any", ("id", [1, 2, 3]), ["<> ANY"]),
    ],
    ids=[
        "like_basic",
        "like_with_escape",
        "between",
        "in_list",
        "in_tuple",
        "not_in",
        "is_null",
        "is_not_null",
        "any_list",
        "not_any_list",
    ],
)
def test_where_condition_helpers(method: str, args: tuple, expected_sql_parts: list[str]) -> None:
    """Test various WHERE condition helper methods."""
    # Disable optimization for BETWEEN to preserve the clause
    builder = SelectBuilder(enable_optimization=False).select("*").from_("users")
    where_method = getattr(builder, method)
    builder = where_method(*args)

    query = builder.build()
    # Check that at least one of the expected SQL parts is present
    assert any(part in query.sql for part in expected_sql_parts)


def test_where_exists_with_subquery() -> None:
    """Test WHERE EXISTS with subquery."""
    subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id"))
    builder = SelectBuilder(enable_optimization=False).select("*").from_("users").where_exists(subquery)

    query = builder.build()
    assert "EXISTS" in query.sql
    assert "orders" in query.sql


def test_where_not_exists() -> None:
    """Test WHERE NOT EXISTS."""
    subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id"))
    builder = SelectBuilder(enable_optimization=False).select("*").from_("users").where_not_exists(subquery)

    query = builder.build()
    assert "NOT EXISTS" in query.sql or ("NOT" in query.sql and "EXISTS" in query.sql)


# Test aggregate functions
@pytest.mark.parametrize(
    "method,args,expected_sql",
    [
        ("count_", (), "COUNT(*)"),
        ("count_", ("id",), "COUNT"),
        ("count_", ("id", "total_users"), "COUNT"),
        ("sum_", ("amount", "total_amount"), "SUM"),
        ("avg_", ("price", "avg_price"), "AVG"),
        ("max_", ("created_at", "latest"), "MAX"),
        ("min_", ("price", "lowest_price"), "MIN"),
    ],
    ids=["count_star", "count_column", "count_with_alias", "sum", "avg", "max", "min"],
)
def test_aggregate_functions(method: str, args: tuple, expected_sql: str) -> None:
    """Test aggregate function helpers."""
    builder = SelectBuilder()
    agg_method = getattr(builder, method)
    builder = agg_method(*args).from_("test_table")

    query = builder.build()
    assert expected_sql in query.sql or expected_sql.replace("(*)", "") in query.sql


# Test window functions
@pytest.mark.parametrize(
    "window_func,partition_by,order_by,frame,expected_parts",
    [
        ("ROW_NUMBER()", None, None, None, ["ROW_NUMBER()", "OVER"]),
        ("ROW_NUMBER()", "department", None, None, ["ROW_NUMBER()", "PARTITION BY", "department"]),
        ("ROW_NUMBER()", None, "salary", None, ["ROW_NUMBER()", "ORDER BY", "salary"]),
        ("RANK()", "department", "salary", None, ["RANK()", "PARTITION BY", "ORDER BY"]),
        ("COUNT(*)", ["department", "location"], None, None, ["COUNT", "PARTITION BY"]),
        ("SUM(amount)", None, "date", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", ["SUM", "OVER"]),
    ],
    ids=["basic", "with_partition", "with_order", "partition_and_order", "multiple_partitions", "with_frame"],
)
def test_window_functions(
    window_func: str,
    partition_by: Optional[Any],
    order_by: Optional[str],
    frame: Optional[str],
    expected_parts: list[str],
) -> None:
    """Test window function variations."""
    builder = (
        SelectBuilder()
        .window(window_func, partition_by=partition_by, order_by=order_by, frame=frame, alias="window_result")
        .from_("test_table")
    )

    query = builder.build()
    for part in expected_parts:
        assert part in query.sql


# Test CASE expressions
def test_case_when_else_basic() -> None:
    """Test basic CASE WHEN ELSE expression."""
    builder = (
        SelectBuilder()
        .case_("status_text")
        .when("status = 1", "Active")
        .when("status = 2", "Inactive")
        .else_("Unknown")
        .end()
        .from_("users")  # type: ignore[attr-defined]
    )

    query = builder.build()
    assert "CASE" in query.sql
    assert "WHEN" in query.sql
    assert "ELSE" in query.sql
    assert "END" in query.sql
    assert isinstance(query.parameters, dict)
    assert "Active" in query.parameters.values()
    assert "Inactive" in query.parameters.values()
    assert "Unknown" in query.parameters.values()


def test_case_without_else() -> None:
    """Test CASE expression without ELSE clause."""
    builder = (
        SelectBuilder()
        .case_("priority_text")
        .when("priority = 1", "High")
        .when("priority = 2", "Medium")
        .end()
        .from_("tasks")  # type: ignore[attr-defined]
    )

    query = builder.build()
    assert "CASE" in query.sql
    assert "WHEN" in query.sql
    assert "END" in query.sql
    assert "ELSE" not in query.sql


def test_case_with_expression_conditions() -> None:
    """Test CASE with sqlglot expression conditions."""
    condition_expr = exp.GT(this=exp.column("age"), expression=exp.Literal.number(18))
    builder = SelectBuilder().case_("age_group").when(condition_expr, "Adult").else_("Minor").end().from_("people")  # type: ignore[attr-defined]

    query = builder.build()
    assert "CASE" in query.sql
    assert isinstance(query.parameters, dict)
    assert "Adult" in query.parameters.values()
    assert "Minor" in query.parameters.values()


# Test set operations
@pytest.mark.parametrize(
    "method,all_flag,expected_sql",
    [
        ("union", False, "UNION"),
        ("union", True, "UNION ALL"),
        ("intersect", False, "INTERSECT"),
        ("except_", False, "EXCEPT"),
    ],
    ids=["union", "union_all", "intersect", "except"],
)
def test_set_operations(method: str, all_flag: bool, expected_sql: str) -> None:
    """Test set operations between queries."""
    builder1 = SelectBuilder().select("id").from_("users")
    builder2 = SelectBuilder().select("id").from_("customers")

    set_method = getattr(builder1, method)
    if method == "union" and all_flag:
        result_builder = set_method(builder2, all_=True)
    else:
        result_builder = set_method(builder2)

    query = result_builder.build()
    assert expected_sql in query.sql
    assert "users" in query.sql
    assert "customers" in query.sql


# Test PIVOT/UNPIVOT operations
def test_pivot_basic() -> None:
    """Test basic PIVOT functionality."""
    builder = (
        SelectBuilder()
        .select("product", "Q1", "Q2", "Q3", "Q4")
        .from_("sales_data")
        .pivot("SUM", "sales", "quarter", ["Q1", "Q2", "Q3", "Q4"])
    )
    query = builder.build()
    assert "PIVOT" in query.sql
    assert "SUM" in query.sql


def test_unpivot_basic() -> None:
    """Test basic UNPIVOT functionality."""
    builder = (
        SelectBuilder()
        .select("product", "quarter", "sales")
        .from_("pivot_data")
        .unpivot("sales", "quarter", ["Q1", "Q2", "Q3", "Q4"])
    )
    query = builder.build()
    assert "UNPIVOT" in query.sql or "PIVOT" in query.sql


# Test GROUP BY ROLLUP
def test_group_by_rollup() -> None:
    """Test GROUP BY ROLLUP functionality."""
    builder = (
        SelectBuilder()
        .select("product", "region", "SUM(sales)")
        .from_("sales_data")
        .group_by_rollup("product", "region")
    )

    query = builder.build()
    assert "ROLLUP" in query.sql
    assert "GROUP BY" in query.sql


# Test query hints
@pytest.mark.parametrize(
    "hint,location,table,dialect,expected_in_sql",
    [
        ("INDEX(users idx_users_name)", None, None, None, True),  # Statement level
        ("NOLOCK", "table", "users", None, False),  # Table level (stored, not in statement SQL)
        ("INDEX(users idx_users_name)", None, None, "oracle", True),  # With dialect
    ],
    ids=["statement_hint", "table_hint", "dialect_hint"],
)
def test_query_hints(
    hint: str, location: Optional[str], table: Optional[str], dialect: Optional[str], expected_in_sql: bool
) -> None:
    """Test query hint functionality."""
    builder = SelectBuilder().select("id").from_("users")
    if location and table:
        builder = builder.with_hint(hint, location=location, table=table, dialect=dialect)
    else:
        builder = builder.with_hint(hint, dialect=dialect)

    query = builder.build()
    if expected_in_sql:
        assert "/*+" in query.sql
        assert hint.split("(")[0] in query.sql.upper()  # Check hint keyword
    else:
        # Table hints are stored but not in statement SQL
        assert any(h["hint"] == hint for h in builder._hints)


# Test parameter handling and SQL injection prevention
@pytest.mark.parametrize(
    "malicious_input,context",
    [
        ("'; DROP TABLE users; --", "where"),
        ("%'; DELETE FROM users WHERE '1'='1", "like"),
        (["1'; DROP TABLE users; --"], "in_list"),
        ("'; DROP TABLE users; SELECT '", "case_value"),
    ],
    ids=["where_injection", "like_injection", "in_list_injection", "case_injection"],
)
def test_sql_injection_prevention(malicious_input: Any, context: str) -> None:
    """Test that malicious inputs are properly parameterized."""
    builder = SelectBuilder().select("*").from_("users")

    if context == "where":
        builder = builder.where(("name", malicious_input))
    elif context == "like":
        builder = builder.where_like("name", malicious_input)
    elif context == "in_list":
        builder = builder.where_in("id", malicious_input)
    elif context == "case_value":
        builder = builder.case_("result").when("status = 1", malicious_input).else_("Safe").end()

    query = builder.build()
    assert isinstance(query.parameters, dict)

    # Check that malicious input is parameterized, not in SQL
    if isinstance(malicious_input, list):
        assert malicious_input[0] in query.parameters.values()
    else:
        assert malicious_input in query.parameters.values()
    assert "DROP TABLE" not in query.sql
    assert "DELETE FROM" not in query.sql


# Test error conditions
@pytest.mark.parametrize(
    "error_scenario,expected_exception",
    [
        ("invalid_where_in_type", SQLBuilderError),
        ("invalid_where_any_type", SQLBuilderError),
        ("invalid_subquery_string", (SQLBuilderError, ParseError)),
        ("invalid_window_function", (SQLBuilderError, ParseError)),
    ],
    ids=["where_in_type", "where_any_type", "invalid_subquery", "invalid_window"],
)
def test_error_conditions(error_scenario: str, expected_exception: Any) -> None:
    """Test various error conditions."""
    builder = SelectBuilder().select("*").from_("users")

    with pytest.raises(expected_exception):
        if error_scenario == "invalid_where_in_type":
            builder.where_in("id", 42)  # type: ignore[arg-type]
        elif error_scenario == "invalid_where_any_type":
            builder.where_any("id", 42)  # type: ignore[arg-type]
        elif error_scenario == "invalid_subquery_string":
            builder.where_exists("INVALID SQL SYNTAX")
        elif error_scenario == "invalid_window_function":
            builder.window("INVALID SYNTAX WITH SPACES AND NO PARENS").build()


# Test complex queries
def test_complex_analytics_query() -> None:
    """Test complex analytics query with multiple features."""
    subquery = (
        SelectBuilder().select("department").avg_("salary", "dept_avg_salary").from_("employees").group_by("department")
    )

    builder = (
        SelectBuilder()
        .select("e.id", "e.name", "e.department", "e.salary")
        .case_("performance_tier")
        .when("e.salary > dept_avg.dept_avg_salary * 1.2", "High Performer")
        .when("e.salary > dept_avg.dept_avg_salary * 0.8", "Average Performer")
        .else_("Needs Improvement")
        .end()
        .window("RANK()", partition_by="e.department", order_by="e.salary", alias="salary_rank")  # type: ignore[attr-defined]
        .from_("employees e")
        .left_join(f"({subquery.build().sql}) dept_avg", "e.department = dept_avg.department")
        .where_not_null("e.salary")
        .where_in("e.status", ["active", "on_leave"])
        .order_by("e.department", "salary_rank")
    )

    query = builder.build()

    # Verify all features are present
    assert "SELECT" in query.sql
    assert "CASE" in query.sql
    assert "RANK()" in query.sql
    assert "PARTITION BY" in query.sql
    assert "LEFT JOIN" in query.sql
    assert "ORDER BY" in query.sql

    # Verify parameters
    assert isinstance(query.parameters, dict)
    param_values = list(query.parameters.values())
    assert "High Performer" in param_values
    assert "Average Performer" in param_values
    assert "Needs Improvement" in param_values


def test_complex_set_operations_query() -> None:
    """Test complex query with multiple UNION operations."""
    current = (
        SelectBuilder()
        .select("'current'", "customer_id", "total")
        .from_("orders")
        .where_between("order_date", "2024-01-01", "2024-12-31")
    )

    archived = (
        SelectBuilder()
        .select("'archived'", "customer_id", "total")
        .from_("archived_orders")
        .where_between("order_date", "2024-01-01", "2024-12-31")
    )

    pending = (
        SelectBuilder()
        .select("'pending'", "customer_id", "estimated_total")
        .from_("pending_orders")
        .where_not_null("estimated_date")
    )

    combined = current.union(archived).union(pending, all_=True)
    query = combined.build()

    assert "UNION" in query.sql
    assert "orders" in query.sql
    assert "archived_orders" in query.sql
    assert "pending_orders" in query.sql


# Test edge cases
def test_empty_where_in_list() -> None:
    """Test WHERE IN with empty list."""
    builder = SelectBuilder().select("*").from_("users").where_in("id", [])
    query = builder.build()
    assert "SELECT" in query.sql
    assert 'FROM "users"' in query.sql or "FROM users" in query.sql


def test_parameter_naming_consistency() -> None:
    """Test that parameter naming is consistent across multiple conditions."""
    builder = (
        SelectBuilder()
        .select("*")
        .from_("users")
        .where_like("name", "%test%")
        .where_between("age", 18, 65)
        .where_in("status", ["active", "pending"])
    )

    query = builder.build()
    assert isinstance(query.parameters, dict)

    # All parameter names should be unique
    param_names = list(query.parameters.keys())
    assert len(param_names) == len(set(param_names))

    # All values should be preserved
    param_values = list(query.parameters.values())
    assert "%test%" in param_values
    assert 18 in param_values
    assert 65 in param_values
    assert "active" in param_values
    assert "pending" in param_values


def test_large_in_clause() -> None:
    """Test handling of large IN clause with many parameters."""
    large_list = list(range(1000))
    builder = SelectBuilder().select("*").from_("users").where_in("id", large_list)

    query = builder.build()
    assert "IN" in query.sql
    assert isinstance(query.parameters, dict)
    assert len(query.parameters) == 1000


# Test dialect-specific behavior
@pytest.mark.parametrize(
    "dialect,expected_behavior",
    [("postgres", "postgres"), ("mysql", "mysql"), ("sqlite", "sqlite")],
    ids=["postgres", "mysql", "sqlite"],
)
def test_dialect_specific_queries(dialect: str, expected_behavior: str) -> None:
    """Test dialect-specific SQL generation."""
    builder = SelectBuilder(dialect=dialect).select("id", "name").from_("users").where(("active", True)).limit(10)

    query = builder.build()
    assert "SELECT" in query.sql
    assert "LIMIT" in query.sql
    assert isinstance(query.parameters, dict)


# Test fluent interface chaining
def test_fluent_interface_chaining() -> None:
    """Test that all methods return builder for fluent chaining."""
    builder = (
        SelectBuilder()
        .select("id", "name")
        .distinct()
        .from_("users")
        .join("orders", on="users.id = orders.user_id")
        .where(("active", True))
        .where_like("name", "%John%")
        .where_between("age", 18, 65)
        .group_by("department")
        .having("COUNT(*) > 5")
        .order_by("name")
        .limit(10)
        .offset(20)
    )

    query = builder.build()
    # Verify the query has all components
    assert all(
        keyword in query.sql
        for keyword in ["SELECT", "DISTINCT", "FROM", "JOIN", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]
    )


# Test CaseBuilder integration
def test_case_builder_initialization() -> None:
    """Test CaseBuilder basic initialization."""
    select_builder = SelectBuilder()
    case_builder = CaseBuilder(select_builder, "test_alias")

    assert case_builder._alias == "test_alias"
    assert isinstance(case_builder._case_expr, exp.Case)


# Test pivot/unpivot without FROM clause
def test_pivot_without_from() -> None:
    """Test PIVOT without FROM clause should not include PIVOT in SQL."""
    builder = SelectBuilder().select("*").pivot("SUM", "sales", "quarter", ["Q1", "Q2"])
    query = builder.build()
    assert "PIVOT" not in query.sql


def test_unpivot_without_from() -> None:
    """Test UNPIVOT without FROM clause should not include UNPIVOT in SQL."""
    builder = SelectBuilder().select("*").unpivot("sales", "quarter", ["Q1", "Q2"])
    query = builder.build()
    assert "UNPIVOT" not in query.sql and "PIVOT" not in query.sql
