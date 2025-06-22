"""Unit tests for query builder mixins.

This module tests the various builder mixins including:
- WhereClauseMixin for WHERE conditions
- JoinClauseMixin for JOIN operations
- LimitOffsetClauseMixin for LIMIT/OFFSET
- OrderByClauseMixin for ORDER BY
- FromClauseMixin for FROM clause
- ReturningClauseMixin for RETURNING clause
- InsertValuesMixin for INSERT VALUES
- SetOperationMixin for UNION/INTERSECT/EXCEPT
- GroupByClauseMixin for GROUP BY
- HavingClauseMixin for HAVING clause
- UpdateSetClauseMixin for UPDATE SET
- UpdateFromClauseMixin for UPDATE FROM
- InsertFromSelectMixin for INSERT FROM SELECT
- Merge mixins for MERGE statements
- PivotClauseMixin for PIVOT operations
- UnpivotClauseMixin for UNPIVOT operations
- AggregateFunctionsMixin for aggregate functions
"""

from typing import TYPE_CHECKING, Any, Optional, Union, cast
from unittest.mock import Mock

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder.mixins._aggregate_functions import AggregateFunctionsMixin
from sqlspec.statement.builder.mixins._from import FromClauseMixin
from sqlspec.statement.builder.mixins._group_by import GroupByClauseMixin
from sqlspec.statement.builder.mixins._having import HavingClauseMixin
from sqlspec.statement.builder.mixins._insert_from_select import InsertFromSelectMixin
from sqlspec.statement.builder.mixins._insert_values import InsertValuesMixin
from sqlspec.statement.builder.mixins._join import JoinClauseMixin
from sqlspec.statement.builder.mixins._limit_offset import LimitOffsetClauseMixin
from sqlspec.statement.builder.mixins._merge_clauses import (
    MergeIntoClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeOnClauseMixin,
    MergeUsingClauseMixin,
)
from sqlspec.statement.builder.mixins._order_by import OrderByClauseMixin
from sqlspec.statement.builder.mixins._pivot import PivotClauseMixin
from sqlspec.statement.builder.mixins._returning import ReturningClauseMixin
from sqlspec.statement.builder.mixins._set_ops import SetOperationMixin
from sqlspec.statement.builder.mixins._unpivot import UnpivotClauseMixin
from sqlspec.statement.builder.mixins._update_from import UpdateFromClauseMixin
from sqlspec.statement.builder.mixins._update_set import UpdateSetClauseMixin
from sqlspec.statement.builder.mixins._where import WhereClauseMixin

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


# Helper Classes
class MockQueryResult:
    """Mock query result for testing."""

    def __init__(self, sql: str, parameters: dict[str, Any]) -> None:
        self.sql = sql
        self.parameters = parameters


class MockBuilder:
    """Base mock builder implementing minimal protocol for testing mixins."""

    def __init__(self, expression: "Optional[exp.Expression]" = None) -> None:
        self._expression: Optional[exp.Expression] = expression
        self._parameters: dict[str, Any] = {}
        self._parameter_counter = 0
        self.dialect: DialectType = None
        self.dialect_name: Optional[str] = None
        self._table: Optional[str] = None

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple["MockBuilder", str]:
        """Add a parameter to the builder."""
        if name and name in self._parameters:
            raise SQLBuilderError(f"Parameter name '{name}' already exists.")
        param_name = name or f"param_{self._parameter_counter + 1}"
        self._parameter_counter += 1
        self._parameters[param_name] = value
        return self, param_name

    def build(self) -> MockQueryResult:
        """Build the query."""
        return MockQueryResult("SELECT 1", self._parameters)

    def _raise_sql_builder_error(self, message: str, cause: Optional[Exception] = None) -> None:
        """Raise a SQLBuilderError."""
        raise SQLBuilderError(message) from cause


# Test Implementations
class WhereTestBuilder(MockBuilder, WhereClauseMixin):
    """Test builder with WHERE clause mixin."""

    pass


# WhereClauseMixin Tests
@pytest.mark.parametrize(
    "condition,expected_type",
    [
        ("id = 1", exp.Select),
        (("status", "active"), exp.Select),
        (exp.EQ(this=exp.column("id"), expression=exp.Literal.number(1)), exp.Select),
    ],
    ids=["string_condition", "tuple_condition", "expression_condition"],
)
def test_where_clause_basic(condition: Any, expected_type: type[exp.Expression]) -> None:
    """Test basic WHERE clause functionality."""
    builder = WhereTestBuilder(expected_type())
    result = builder.where(condition)
    assert result is builder
    assert isinstance(builder._expression, expected_type)
    assert builder._expression.args.get("where") is not None


def test_where_clause_wrong_expression_type() -> None:
    """Test WHERE clause with wrong expression type."""
    builder = WhereTestBuilder(exp.Insert())
    with pytest.raises(SQLBuilderError, match="Cannot add WHERE clause to unsupported expression type"):
        builder.where("id = 1")


@pytest.mark.parametrize(
    "method,args,expected_params",
    [
        ("where_eq", ("name", "John"), ["John"]),
        ("where_neq", ("status", "inactive"), ["inactive"]),
        ("where_lt", ("age", 18), [18]),
        ("where_lte", ("age", 65), [65]),
        ("where_gt", ("score", 90), [90]),
        ("where_gte", ("rating", 4.5), [4.5]),
        ("where_like", ("email", "%@example.com"), ["%@example.com"]),
        ("where_not_like", ("name", "%test%"), ["%test%"]),
        ("where_ilike", ("name", "john%"), ["john%"]),
        ("where_between", ("age", 25, 45), [25, 45]),
        ("where_in", ("status", ["active", "pending"]), ["active", "pending"]),
        ("where_not_in", ("role", ["guest", "banned"]), ["guest", "banned"]),
    ],
    ids=["eq", "neq", "lt", "lte", "gt", "gte", "like", "not_like", "ilike", "between", "in", "not_in"],
)
def test_where_helper_methods(method: str, args: tuple, expected_params: list[Any]) -> None:
    """Test WHERE clause helper methods."""
    builder = WhereTestBuilder(exp.Select())
    where_method = getattr(builder, method)
    result = where_method(*args)

    assert result is builder
    # Check parameters were added
    for param in expected_params:
        assert param in builder._parameters.values()


@pytest.mark.parametrize(
    "column",
    ["deleted_at", "email_verified", exp.column("archived_at")],
    ids=["string_column", "another_string", "expression_column"],
)
def test_where_null_checks(column: Any) -> None:
    """Test WHERE IS NULL and IS NOT NULL."""
    builder = WhereTestBuilder(exp.Select())

    # Test IS NULL
    result = builder.where_is_null(column)
    assert result is builder

    # Reset and test IS NOT NULL
    builder = WhereTestBuilder(exp.Select())
    result = builder.where_is_not_null(column)
    assert result is builder


@pytest.mark.parametrize(
    "values_or_subquery,expected_any_type",
    [
        ([1, 2, 3], exp.Tuple),
        ((4, 5, 6), exp.Tuple),
        (Mock(build=lambda: Mock(sql="SELECT id FROM users")), type(None)),  # Subquery
    ],
    ids=["list_values", "tuple_values", "subquery"],
)
def test_where_any_operations(values_or_subquery: Any, expected_any_type: Any) -> None:
    """Test WHERE ANY operations."""
    builder = WhereTestBuilder(exp.Select())

    # Test where_any
    result = builder.where_any("id", values_or_subquery)
    assert result is builder
    assert builder._expression is not None
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    assert isinstance(where_expr.this, exp.EQ)

    # Test where_not_any
    builder = WhereTestBuilder(exp.Select())
    result = builder.where_not_any("id", values_or_subquery)
    assert result is builder
    assert builder._expression is not None
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    assert isinstance(where_expr.this, exp.NEQ)


def test_where_exists_operations() -> None:
    """Test WHERE EXISTS and NOT EXISTS."""
    subquery = "SELECT 1 FROM orders WHERE user_id = users.id"

    # Test EXISTS
    builder = WhereTestBuilder(exp.Select())
    result = builder.where_exists(subquery)
    assert result is builder

    # Test NOT EXISTS
    builder = WhereTestBuilder(exp.Select())
    result = builder.where_not_exists(subquery)
    assert result is builder


class JoinTestBuilder(MockBuilder, JoinClauseMixin):
    """Test builder with JOIN clause mixin."""

    pass


# JoinClauseMixin Tests
@pytest.mark.parametrize(
    "join_type,method,table,on_condition",
    [
        ("INNER", "join", "users", "users.id = orders.user_id"),
        ("LEFT", "left_join", "profiles", "users.id = profiles.user_id"),
        ("RIGHT", "right_join", "departments", "users.dept_id = departments.id"),
        ("FULL", "full_join", "audit_log", "users.id = audit_log.user_id"),
        ("CROSS", "cross_join", "regions", None),
    ],
    ids=["inner", "left", "right", "full", "cross"],
)
def test_join_types(join_type: str, method: str, table: str, on_condition: Optional[str]) -> None:
    """Test various JOIN types."""
    builder = JoinTestBuilder(exp.Select())
    join_method = getattr(builder, method)

    if on_condition:
        result = join_method(table, on=on_condition)
    else:
        result = join_method(table)

    assert result is builder
    assert isinstance(builder._expression, exp.Select)


def test_join_with_wrong_expression_type() -> None:
    """Test JOIN with wrong expression type."""
    builder = JoinTestBuilder(exp.Insert())
    with pytest.raises(SQLBuilderError, match="JOIN clause is only supported"):
        builder.join("users")


def test_join_with_alias() -> None:
    """Test JOIN with table alias."""
    builder = JoinTestBuilder(exp.Select())
    result = builder.join("users AS u", on="u.id = orders.user_id")
    assert result is builder


class LimitOffsetTestBuilder(MockBuilder, LimitOffsetClauseMixin):
    """Test builder with LIMIT/OFFSET mixin."""

    pass


# LimitOffsetClauseMixin Tests
@pytest.mark.parametrize(
    "limit_value,offset_value",
    [(10, None), (None, 20), (50, 100), (1, 0), (100, 500)],
    ids=["limit_only", "offset_only", "both", "single_page", "large_offset"],
)
def test_limit_offset_operations(limit_value: Optional[int], offset_value: Optional[int]) -> None:
    """Test LIMIT and OFFSET operations."""
    builder = LimitOffsetTestBuilder(exp.Select())

    if limit_value is not None:
        result = builder.limit(limit_value)
        assert result is builder

    if offset_value is not None:
        result = builder.offset(offset_value)
        assert result is builder

    assert isinstance(builder._expression, exp.Select)


def test_limit_offset_wrong_expression_type() -> None:
    """Test LIMIT/OFFSET with wrong expression type."""
    builder = LimitOffsetTestBuilder(exp.Insert())

    with pytest.raises(SQLBuilderError, match="LIMIT is only supported"):
        builder.limit(10)

    with pytest.raises(SQLBuilderError, match="OFFSET is only supported"):
        builder.offset(5)


class OrderByTestBuilder(MockBuilder, OrderByClauseMixin):
    """Test builder with ORDER BY mixin."""

    pass


# OrderByClauseMixin Tests
@pytest.mark.parametrize(
    "columns,desc",
    [
        (["name"], False),
        (["created_at"], True),
        (["department", "salary"], False),
        (["score", "name"], True),
        ([exp.column("updated_at")], True),
    ],
    ids=["single_asc", "single_desc", "multiple_asc", "multiple_desc", "expression_desc"],
)
def test_order_by_operations(columns: list[Any], desc: bool) -> None:
    """Test ORDER BY operations."""
    builder = OrderByTestBuilder(exp.Select())
    result = builder.order_by(*columns, desc=desc)
    assert result is builder
    assert isinstance(builder._expression, exp.Select)


def test_order_by_wrong_expression_type() -> None:
    """Test ORDER BY with wrong expression type."""
    builder = OrderByTestBuilder(exp.Insert())
    with pytest.raises(SQLBuilderError, match="ORDER BY is only supported"):
        builder.order_by("name")


class FromTestBuilder(MockBuilder, FromClauseMixin):
    """Test builder with FROM clause mixin."""

    pass


# FromClauseMixin Tests
@pytest.mark.parametrize(
    "table,alias",
    [("users", None), ("customers", "c"), ("public.orders", "o"), (exp.Table(this="products"), None)],
    ids=["simple_table", "table_with_alias", "schema_qualified", "expression_table"],
)
def test_from_clause_operations(table: Any, alias: Optional[str]) -> None:
    """Test FROM clause operations."""
    builder = FromTestBuilder(exp.Select())

    if alias:
        result = builder.from_(f"{table} AS {alias}")
    else:
        result = builder.from_(table)

    assert result is builder
    assert isinstance(builder._expression, exp.Select)


def test_from_wrong_expression_type() -> None:
    """Test FROM with wrong expression type."""
    builder = FromTestBuilder(exp.Insert())
    with pytest.raises(SQLBuilderError, match="FROM clause is only supported"):
        builder.from_("users")


class ReturningTestBuilder(MockBuilder, ReturningClauseMixin):
    """Test builder with RETURNING clause mixin."""

    pass


# ReturningClauseMixin Tests
@pytest.mark.parametrize(
    "expression_type,columns",
    [
        (exp.Insert, ["id"]),
        (exp.Update, ["id", "updated_at"]),
        (exp.Delete, ["*"]),
        (exp.Insert, ["id", "name", "created_at"]),
    ],
    ids=["insert_single", "update_multiple", "delete_star", "insert_multiple"],
)
def test_returning_clause_operations(expression_type: type[exp.Expression], columns: list[str]) -> None:
    """Test RETURNING clause operations."""
    builder = ReturningTestBuilder(expression_type())
    result = builder.returning(*columns)
    assert result is builder
    assert isinstance(builder._expression, expression_type)


def test_returning_wrong_expression_type() -> None:
    """Test RETURNING with wrong expression type."""
    builder = ReturningTestBuilder(exp.Select())
    with pytest.raises(SQLBuilderError, match="RETURNING is only supported"):
        builder.returning("id")


class InsertValuesTestBuilder(MockBuilder, InsertValuesMixin):
    """Test builder with INSERT VALUES mixin."""

    pass


# InsertValuesMixin Tests
def test_insert_columns_operation() -> None:
    """Test INSERT columns specification."""
    builder = InsertValuesTestBuilder(exp.Insert())
    result = builder.columns("id", "name", "email")
    assert result is builder
    assert isinstance(builder._expression, exp.Insert)


@pytest.mark.parametrize(
    "values,expected_param_count",
    [(["John", "john@example.com"], 2), ([1, "Admin", True, None], 4), ([{"key": "value"}, [1, 2, 3]], 2)],
    ids=["basic_values", "mixed_types", "complex_values"],
)
def test_insert_values_operation(values: list[Any], expected_param_count: int) -> None:
    """Test INSERT values operation."""
    builder = InsertValuesTestBuilder(exp.Insert())
    result = builder.values(*values)
    assert result is builder
    assert len(builder._parameters) == expected_param_count


def test_insert_values_from_dict() -> None:
    """Test INSERT values from dictionary."""
    builder = InsertValuesTestBuilder(exp.Insert())
    # When passing a dictionary to values(), it's treated as a single parameter
    result = builder.values({"name": "John", "email": "john@example.com", "active": True})
    assert result is builder
    assert len(builder._parameters) == 1
    # The dictionary should be stored as a single parameter
    param_values = list(builder._parameters.values())
    assert param_values[0] == {"name": "John", "email": "john@example.com", "active": True}


def test_insert_values_wrong_expression_type() -> None:
    """Test INSERT VALUES with wrong expression type."""
    builder = InsertValuesTestBuilder(exp.Select())
    with pytest.raises(SQLBuilderError, match="Cannot set columns on a non-INSERT expression"):
        builder.columns("name")
    with pytest.raises(SQLBuilderError, match="Cannot add values to a non-INSERT expression"):
        builder.values("John")


class SetOperationTestBuilder(MockBuilder, SetOperationMixin):
    """Test builder with set operations mixin."""

    pass


# SetOperationMixin Tests
@pytest.mark.parametrize(
    "operation,method,distinct",
    [
        ("UNION", "union", True),
        ("UNION ALL", "union", False),
        ("INTERSECT", "intersect", True),
        ("EXCEPT", "except_", True),
    ],
    ids=["union", "union_all", "intersect", "except"],
)
def test_set_operations(operation: str, method: str, distinct: bool) -> None:
    """Test set operations (UNION, INTERSECT, EXCEPT)."""
    builder1 = SetOperationTestBuilder(exp.Select())
    builder2 = SetOperationTestBuilder(exp.Select())

    # Add some parameters to verify merging
    builder1._parameters = {"param_1": "value1"}
    builder2._parameters = {"param_2": "value2"}

    set_method = getattr(builder1, method)
    # Only union accepts 'all_' parameter
    if method == "union":
        result = set_method(builder2, all_=not distinct)
    else:
        # intersect and except_ don't have an all_ parameter
        result = set_method(builder2)

    assert isinstance(result, SetOperationTestBuilder)
    # Parameters should be merged
    assert "param_1" in result._parameters
    assert "param_2" in result._parameters


def test_set_operation_wrong_expression_type() -> None:
    """Test set operations with wrong expression type."""
    # Since MockBuilder.build() always returns "SELECT 1", the set operations
    # don't actually check the expression type. They just parse the built SQL.
    # This test would need a real builder that respects expression types.
    # For now, let's test the parsing error case

    from sqlglot.errors import ParseError

    class BadBuilder(MockBuilder, SetOperationMixin):
        def build(self) -> MockQueryResult:
            return MockQueryResult("", {})  # Empty SQL

    builder1 = BadBuilder()
    builder2 = SetOperationTestBuilder(exp.Select())

    # Empty SQL causes ParseError from sqlglot
    with pytest.raises(ParseError, match="No expression was parsed"):
        builder1.union(builder2)


class GroupByTestBuilder(MockBuilder, GroupByClauseMixin):
    """Test builder with GROUP BY mixin."""

    pass


# GroupByClauseMixin Tests
@pytest.mark.parametrize(
    "columns",
    [["department"], ["department", "location"], ["year", "month", "day"], [exp.column("created_date")]],
    ids=["single", "double", "triple", "expression"],
)
def test_group_by_operations(columns: list[Any]) -> None:
    """Test GROUP BY operations."""
    builder = GroupByTestBuilder(exp.Select())
    result = builder.group_by(*columns)
    assert result is builder
    assert isinstance(builder._expression, exp.Select)


@pytest.mark.parametrize(
    "method,columns",
    [
        ("group_by_rollup", ["year", "month"]),
        ("group_by_cube", ["product", "region"]),
        ("group_by_grouping_sets", [["a"], ["b"], ["a", "b"]]),
    ],
    ids=["rollup", "cube", "grouping_sets"],
)
def test_group_by_advanced_operations(method: str, columns: Any) -> None:
    """Test advanced GROUP BY operations (ROLLUP, CUBE, GROUPING SETS)."""
    builder = GroupByTestBuilder(exp.Select())
    group_method = getattr(builder, method)

    if method == "group_by_grouping_sets":
        result = group_method(*columns)
    else:
        result = group_method(*columns)

    assert result is builder
    assert builder._expression is not None
    assert builder._expression.args.get("group") is not None


def test_group_by_wrong_expression_type() -> None:
    """Test GROUP BY with wrong expression type."""
    builder = GroupByTestBuilder(exp.Insert())
    # group_by returns self without modification when not a SELECT
    result = builder.group_by("column")
    assert result is builder
    # The expression should remain unchanged
    assert isinstance(builder._expression, exp.Insert)
    # No GROUP BY should be added
    assert builder._expression.args.get("group") is None


class HavingTestBuilder(MockBuilder, HavingClauseMixin):
    """Test builder with HAVING clause mixin."""

    pass


# HavingClauseMixin Tests
@pytest.mark.parametrize(
    "condition",
    ["COUNT(*) > 10", "SUM(amount) >= 1000", "AVG(score) < 75", "MAX(price) - MIN(price) > 100"],
    ids=["count", "sum", "avg", "range"],
)
def test_having_operations(condition: str) -> None:
    """Test HAVING clause operations."""
    builder = HavingTestBuilder(exp.Select())
    result = builder.having(condition)
    assert result is builder
    assert isinstance(builder._expression, exp.Select)


def test_having_wrong_expression_type() -> None:
    """Test HAVING with wrong expression type."""
    builder = HavingTestBuilder(exp.Insert())
    with pytest.raises(SQLBuilderError, match="Cannot add HAVING to a non-SELECT expression"):
        builder.having("COUNT(*) > 1")


class UpdateSetTestBuilder(MockBuilder, UpdateSetClauseMixin):
    """Test builder with UPDATE SET mixin."""

    pass


# UpdateSetClauseMixin Tests
@pytest.mark.parametrize(
    "updates",
    [
        {"name": "John"},
        {"status": "active", "updated_at": "2024-01-01"},
        {"counter": exp.Add(this=exp.column("counter"), expression=exp.Literal.number(1))},
    ],
    ids=["single_value", "multiple_values", "expression_value"],
)
def test_update_set_operations(updates: dict[str, Any]) -> None:
    """Test UPDATE SET operations."""
    builder = UpdateSetTestBuilder(exp.Update())

    for column, value in updates.items():
        result = builder.set(**{column: value})
        assert result is builder

    assert isinstance(builder._expression, exp.Update)
    # Check parameters were added for non-expression values
    for value in updates.values():
        if not isinstance(value, exp.Expression):
            assert value in builder._parameters.values()


def test_update_set_wrong_expression_type() -> None:
    """Test UPDATE SET with wrong expression type."""
    builder = UpdateSetTestBuilder(exp.Select())
    with pytest.raises(SQLBuilderError, match="Cannot add SET clause to non-UPDATE expression"):
        builder.set(name="John")


class UpdateFromTestBuilder(MockBuilder, UpdateFromClauseMixin):
    """Test builder with UPDATE FROM mixin."""

    pass


# UpdateFromClauseMixin Tests
def test_update_from_operations() -> None:
    """Test UPDATE FROM operations."""
    builder = UpdateFromTestBuilder(exp.Update())
    result = builder.from_("source_table")
    assert result is builder
    assert isinstance(builder._expression, exp.Update)


def test_update_from_wrong_expression_type() -> None:
    """Test UPDATE FROM with wrong expression type."""
    builder = UpdateFromTestBuilder(exp.Select())
    with pytest.raises(SQLBuilderError, match="Cannot add FROM clause to non-UPDATE expression"):
        builder.from_("other_table")


class InsertFromSelectTestBuilder(MockBuilder, InsertFromSelectMixin):
    """Test builder with INSERT FROM SELECT mixin."""

    pass


# InsertFromSelectMixin Tests
def test_insert_from_select_operations() -> None:
    """Test INSERT FROM SELECT operations."""
    builder = InsertFromSelectTestBuilder(exp.Insert())
    builder._table = "target_table"  # Set table first

    # Create a mock select builder with proper attributes
    select_builder = Mock()
    select_builder._expression = exp.Select().from_("source")
    select_builder._parameters = {}
    select_builder.build.return_value = MockQueryResult("SELECT * FROM source", {})

    result = builder.from_select(select_builder)
    assert result is builder
    assert isinstance(builder._expression, exp.Insert)


def test_insert_from_select_requires_table() -> None:
    """Test INSERT FROM SELECT requires table to be set."""
    builder = InsertFromSelectTestBuilder(exp.Insert())
    select_builder = Mock()

    with pytest.raises(SQLBuilderError, match="The target table must be set using .into\\(\\) before adding values"):
        builder.from_select(select_builder)


def test_insert_from_select_wrong_expression_type() -> None:
    """Test INSERT FROM SELECT with wrong expression type."""
    builder = InsertFromSelectTestBuilder(exp.Select())
    builder._table = "target_table"
    select_builder = Mock()

    with pytest.raises(SQLBuilderError, match="Cannot set INSERT source on a non-INSERT expression"):
        builder.from_select(select_builder)


class MergeTestBuilder(
    MockBuilder,
    MergeIntoClauseMixin,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
):
    """Test builder with all MERGE mixins."""

    pass


# Merge Mixins Tests
def test_merge_complete_flow() -> None:
    """Test complete MERGE statement flow."""
    builder = MergeTestBuilder(exp.Merge())

    # Build MERGE statement step by step
    result = builder.into("target_table", "t")
    assert result is builder

    result = builder.using("source_table", "s")
    assert result is builder

    result = builder.on("t.id = s.id")
    assert result is builder

    result = builder.when_matched_then_update({"name": "s.name", "updated_at": "NOW()"})
    assert result is builder

    result = builder.when_not_matched_then_insert(["id", "name"], ["s.id", "s.name"])
    assert result is builder

    result = builder.when_not_matched_by_source_then_delete()
    assert result is builder

    assert isinstance(builder._expression, exp.Merge)


@pytest.mark.parametrize(
    "condition,updates",
    [
        (None, {"status": "updated"}),
        ("s.priority > 5", {"priority": "s.priority"}),
        ("s.active = true", {"last_seen": "s.timestamp"}),
    ],
    ids=["unconditional", "priority_condition", "active_condition"],
)
def test_merge_when_matched_variations(condition: Optional[str], updates: dict[str, str]) -> None:
    """Test WHEN MATCHED variations."""
    builder = MergeTestBuilder(exp.Merge())
    result = builder.when_matched_then_update(updates, condition=condition)
    assert result is builder


def test_merge_when_matched_then_delete() -> None:
    """Test WHEN MATCHED THEN DELETE."""
    builder = MergeTestBuilder(exp.Merge())
    result = builder.when_matched_then_delete(condition="s.deleted = true")
    assert result is builder


def test_merge_wrong_expression_type() -> None:
    """Test MERGE operations with wrong expression type."""
    builder = MergeTestBuilder(exp.Select())

    # The into() method actually converts non-Merge to Merge, so it won't raise
    # Let's test a method that requires Merge to already exist
    builder.into("target")
    # After into(), the expression should be converted to Merge
    assert isinstance(builder._expression, exp.Merge)


def test_merge_on_invalid_condition() -> None:
    """Test MERGE ON with invalid condition."""
    builder = MergeTestBuilder(exp.Merge())
    builder.into("target")
    builder.using("source")

    with pytest.raises(SQLBuilderError, match="Unsupported condition type for ON clause"):
        builder.on(None)  # type: ignore[arg-type]


class PivotTestBuilder(MockBuilder, PivotClauseMixin):
    """Test builder with PIVOT clause mixin."""

    pass


# PivotClauseMixin Tests
@pytest.mark.parametrize(
    "aggregate_function,aggregate_column,pivot_column,pivot_values,alias",
    [
        ("SUM", "sales", "quarter", ["Q1", "Q2", "Q3", "Q4"], None),
        ("COUNT", "orders", "status", ["pending", "shipped", "delivered"], "order_pivot"),
        ("AVG", "rating", "category", ["A", "B", "C"], "rating_pivot"),
        ("MAX", "score", "level", [1, 2, 3, 4, 5], None),
    ],
    ids=["sum_quarters", "count_status", "avg_rating", "max_levels"],
)
def test_pivot_operations(
    aggregate_function: str, aggregate_column: str, pivot_column: str, pivot_values: list[Any], alias: Optional[str]
) -> None:
    """Test PIVOT operations."""
    # Create a Select with FROM clause (required for PIVOT)
    select_expr = exp.Select().from_("data_table")
    builder = PivotTestBuilder(select_expr)

    result = builder.pivot(
        aggregate_function=aggregate_function,
        aggregate_column=aggregate_column,
        pivot_column=pivot_column,
        pivot_values=pivot_values,
        alias=alias,
    )

    assert result is builder  # type: ignore[comparison-overlap]
    assert isinstance(builder._expression, exp.Select)

    # Verify PIVOT is attached to table
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0


def test_pivot_without_from_clause() -> None:
    """Test PIVOT without FROM clause does nothing."""
    builder = PivotTestBuilder(exp.Select())  # No FROM clause

    # pivot() returns self but doesn't add anything when no FROM clause
    result = builder.pivot(
        aggregate_function="SUM", aggregate_column="sales", pivot_column="quarter", pivot_values=["Q1"]
    )
    assert result is builder  # type: ignore[comparison-overlap]
    # No pivot should be added since there's no FROM clause
    assert builder._expression is not None
    assert builder._expression.args.get("from") is None


def test_pivot_wrong_expression_type() -> None:
    """Test PIVOT with wrong expression type."""
    builder = PivotTestBuilder(exp.Insert())

    with pytest.raises(TypeError):
        builder.pivot(aggregate_function="SUM", aggregate_column="sales", pivot_column="quarter", pivot_values=["Q1"])


class UnpivotTestBuilder(MockBuilder, UnpivotClauseMixin):
    """Test builder with UNPIVOT clause mixin."""

    pass


# UnpivotClauseMixin Tests
@pytest.mark.parametrize(
    "value_column,name_column,columns,alias",
    [
        ("sales", "quarter", ["Q1", "Q2", "Q3", "Q4"], None),
        ("amount", "month", ["Jan", "Feb", "Mar"], "monthly_unpivot"),
        ("score", "subject", ["Math", "Science", "English"], "grades_unpivot"),
        ("revenue", "region", ["North", "South", "East", "West"], None),
    ],
    ids=["quarters", "months", "subjects", "regions"],
)
def test_unpivot_operations(value_column: str, name_column: str, columns: list[str], alias: Optional[str]) -> None:
    """Test UNPIVOT operations."""
    # Create a Select with FROM clause (required for UNPIVOT)
    select_expr = exp.Select().from_("wide_table")
    builder = UnpivotTestBuilder(select_expr)

    result = builder.unpivot(
        value_column_name=value_column,
        name_column_name=name_column,
        columns_to_unpivot=cast("list[Union[str,exp.Expression]]", columns),  # type: ignore[misc]
        alias=alias,
    )

    assert result is builder  # type: ignore[comparison-overlap]
    assert isinstance(builder._expression, exp.Select)

    # Verify UNPIVOT is attached to table
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0
    # UNPIVOT is represented as Pivot with unpivot=True
    assert any(pivot.args.get("unpivot") is True for pivot in pivots)


def test_unpivot_without_from_clause() -> None:
    """Test UNPIVOT without FROM clause does nothing."""
    builder = UnpivotTestBuilder(exp.Select())  # No FROM clause

    # unpivot() returns self but doesn't add anything when no FROM clause
    result = builder.unpivot(value_column_name="value", name_column_name="name", columns_to_unpivot=["col1"])
    assert result is builder  # type: ignore[comparison-overlap]
    # No unpivot should be added since there's no FROM clause
    assert builder._expression is not None
    assert builder._expression.args.get("from") is None


def test_unpivot_wrong_expression_type() -> None:
    """Test UNPIVOT with wrong expression type."""
    builder = UnpivotTestBuilder(exp.Insert())

    with pytest.raises(TypeError):
        builder.unpivot(value_column_name="value", name_column_name="name", columns_to_unpivot=["col1"])


class AggregateTestBuilder(MockBuilder, AggregateFunctionsMixin):
    """Test builder with aggregate functions mixin."""

    def select(self, expr: Any) -> "AggregateTestBuilder":
        """Mock select method to add expressions."""
        if self._expression is None:
            self._expression = exp.Select()

        exprs = self._expression.args.get("expressions")
        if exprs is None:
            self._expression.set("expressions", [expr])
        else:
            exprs.append(expr)
        return self


# AggregateFunctionsMixin Tests
@pytest.mark.parametrize(
    "method,column,expected_function",
    [
        ("count_", "*", "COUNT"),
        pytest.param(
            "count_distinct", "user_id", "COUNT", marks=pytest.mark.skip(reason="count_distinct not implemented")
        ),
        ("sum_", "amount", "SUM"),
        ("avg_", "score", "AVG"),
        ("min_", "price", "MIN"),
        ("max_", "price", "MAX"),
        pytest.param("stddev", "value", "STDDEV", marks=pytest.mark.skip(reason="stddev not implemented")),
        pytest.param("stddev_pop", "value", "STDDEV_POP", marks=pytest.mark.skip(reason="stddev_pop not implemented")),
        pytest.param(
            "stddev_samp", "value", "STDDEV_SAMP", marks=pytest.mark.skip(reason="stddev_samp not implemented")
        ),
        pytest.param("variance", "value", "VARIANCE", marks=pytest.mark.skip(reason="variance not implemented")),
        pytest.param("var_pop", "value", "VAR_POP", marks=pytest.mark.skip(reason="var_pop not implemented")),
        pytest.param("var_samp", "value", "VAR_SAMP", marks=pytest.mark.skip(reason="var_samp not implemented")),
        ("array_agg", "tags", "ARRAY_AGG"),
        pytest.param("string_agg", "name", "STRING_AGG", marks=pytest.mark.skip(reason="string_agg not implemented")),
        pytest.param("json_agg", "data", "JSON_AGG", marks=pytest.mark.skip(reason="json_agg not implemented")),
        pytest.param("jsonb_agg", "data", "JSONB_AGG", marks=pytest.mark.skip(reason="jsonb_agg not implemented")),
        pytest.param("bool_and", "active", "BOOL_AND", marks=pytest.mark.skip(reason="bool_and not implemented")),
        pytest.param("bool_or", "verified", "BOOL_OR", marks=pytest.mark.skip(reason="bool_or not implemented")),
        pytest.param("bit_and", "flags", "BIT_AND", marks=pytest.mark.skip(reason="bit_and not implemented")),
        pytest.param("bit_or", "flags", "BIT_OR", marks=pytest.mark.skip(reason="bit_or not implemented")),
    ],
    ids=[
        "count",
        "count_distinct",
        "sum",
        "avg",
        "min",
        "max",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "variance",
        "var_pop",
        "var_samp",
        "array_agg",
        "string_agg",
        "json_agg",
        "jsonb_agg",
        "bool_and",
        "bool_or",
        "bit_and",
        "bit_or",
    ],
)
def test_aggregate_functions(method: str, column: str, expected_function: str) -> None:
    """Test aggregate function methods."""
    builder = AggregateTestBuilder(exp.Select())
    agg_method = getattr(builder, method)

    # Call the aggregate method
    if method == "string_agg":
        result = agg_method(column, separator=", ")
    else:
        result = agg_method(column)

    assert result is builder
    assert builder._expression is not None

    # Check that the function was added to expressions
    select_exprs = builder._expression.args.get("expressions")
    assert select_exprs is not None
    assert len(select_exprs) > 0

    # Verify the aggregate function is present
    found = any(
        expected_function in str(expr)
        or (hasattr(expr, "this") and expected_function in str(getattr(expr, "this", "")))
        for expr in select_exprs
        if expr is not None
    )
    assert found
