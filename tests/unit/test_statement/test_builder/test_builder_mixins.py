from typing import Optional

import pytest
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

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


# --- Minimal Protocol for Mixins ---
class DummyBuilder:
    def __init__(self, expression: "Optional[exp.Expression]" = None) -> None:
        self._expression: Optional[exp.Expression] = expression
        self._parameters: dict[str, object] = {}
        self._parameter_counter: int = 0
        self.dialect: DialectType = None
        self.dialect_name: Optional[str] = None
        self._table: Optional[str] = None

    def add_parameter(self, value: object, name: Optional[str] = None) -> tuple["DummyBuilder", str]:
        if name and name in self._parameters:
            raise SQLBuilderError(f"Parameter name '{name}' already exists.")
        param_name = name or f"param_{self._parameter_counter + 1}"
        self._parameter_counter += 1
        self._parameters[param_name] = value
        return self, param_name

    def build(self) -> object:
        # Dummy build for set ops
        class DummyQuery:
            def __init__(self, sql: str, parameters: dict[str, object]) -> None:
                self.sql: str = sql
                self.parameters: dict[str, object] = parameters

        return DummyQuery("SELECT 1", self._parameters)


# --- Import Mixins ---


# --- WhereClauseMixin ---
class _TestWhereMixin(DummyBuilder, WhereClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_where_clause_basic() -> None:
    builder = _TestWhereMixin(exp.Select())
    assert builder._expression is not None
    builder.where("id = 1")
    assert isinstance(builder._expression, exp.Select)


def test_where_clause_wrong_type() -> None:
    builder = _TestWhereMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.where("id = 1")


def test_where_eq_adds_param() -> None:
    builder = _TestWhereMixin(exp.Select())
    builder.where_eq("name", "John")
    assert "John" in builder._parameters.values()


def test_where_any_with_values() -> None:
    builder = _TestWhereMixin(exp.Select())
    assert builder._expression is not None
    builder.where_any("id", [1, 2, 3])
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    eq_expr = where_expr.this
    assert isinstance(eq_expr, exp.EQ)
    expr_any = eq_expr.args.get("expression")
    assert expr_any is not None
    assert isinstance(expr_any, exp.Any)
    any_arg = expr_any.args.get("this")
    assert any_arg is not None
    assert isinstance(any_arg, exp.Tuple)


def test_where_any_with_subquery() -> None:
    class SubqueryBuilder:
        def build(self) -> object:
            class Dummy:
                sql = "SELECT id FROM users"

            return Dummy()

    builder = _TestWhereMixin(exp.Select())
    assert builder._expression is not None
    builder.where_any("id", SubqueryBuilder())
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    eq_expr = where_expr.this
    assert isinstance(eq_expr, exp.EQ)
    expr_any = eq_expr.args.get("expression")
    assert expr_any is not None
    assert isinstance(expr_any, exp.Any)


def test_where_not_any_with_values() -> None:
    builder = _TestWhereMixin(exp.Select())
    assert builder._expression is not None
    builder.where_not_any("id", [1, 2, 3])
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    neq_expr = where_expr.this
    assert isinstance(neq_expr, exp.NEQ)
    expr_any = neq_expr.args.get("expression")
    assert expr_any is not None
    assert isinstance(expr_any, exp.Any)
    any_arg = expr_any.args.get("this")
    assert any_arg is not None
    assert isinstance(any_arg, exp.Tuple)


def test_where_not_any_with_subquery() -> None:
    class SubqueryBuilder:
        def build(self) -> object:
            class Dummy:
                sql = "SELECT id FROM users"

            return Dummy()

    builder = _TestWhereMixin(exp.Select())
    assert builder._expression is not None
    builder.where_not_any("id", SubqueryBuilder())
    where_expr = builder._expression.args.get("where")
    assert where_expr is not None
    neq_expr = where_expr.this
    assert isinstance(neq_expr, exp.NEQ)
    expr_any = neq_expr.args.get("expression")
    assert expr_any is not None
    assert isinstance(expr_any, exp.Any)


# --- JoinClauseMixin ---
class _TestJoinMixin(DummyBuilder, JoinClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_join_inner() -> None:
    builder = _TestJoinMixin(exp.Select())
    builder.join("users", on="users.id = orders.user_id", join_type="INNER")
    assert isinstance(builder._expression, exp.Select)


def test_join_invalid_type() -> None:
    builder = _TestJoinMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.join("users")


# --- LimitOffsetClauseMixin ---
class _TestLimitOffsetMixin(DummyBuilder, LimitOffsetClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_limit_clause() -> None:
    builder = _TestLimitOffsetMixin(exp.Select())
    builder.limit(10)
    assert isinstance(builder._expression, exp.Select)


def test_offset_clause() -> None:
    builder = _TestLimitOffsetMixin(exp.Select())
    builder.offset(5)
    assert isinstance(builder._expression, exp.Select)


def test_limit_offset_wrong_type() -> None:
    builder = _TestLimitOffsetMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.limit(1)
    with pytest.raises(SQLBuilderError):
        builder.offset(1)


# --- OrderByClauseMixin ---
class _TestOrderByMixin(DummyBuilder, OrderByClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_order_by_clause() -> None:
    builder = _TestOrderByMixin(exp.Select())
    builder.order_by("name")
    assert isinstance(builder._expression, exp.Select)


def test_order_by_wrong_type() -> None:
    builder = _TestOrderByMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.order_by("name")


# --- FromClauseMixin ---
class _TestFromMixin(DummyBuilder, FromClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_from_clause() -> None:
    builder = _TestFromMixin(exp.Select())
    builder.from_("users")
    assert isinstance(builder._expression, exp.Select)


def test_from_wrong_type() -> None:
    builder = _TestFromMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.from_("users")


# --- ReturningClauseMixin ---
class _TestReturningMixin(DummyBuilder, ReturningClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_returning_insert() -> None:
    builder = _TestReturningMixin(exp.Insert())
    builder.returning("id")
    assert isinstance(builder._expression, exp.Insert)


def test_returning_wrong_type() -> None:
    builder = _TestReturningMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.returning("id")


# --- InsertValuesMixin ---
class _TestInsertValuesMixin(DummyBuilder, InsertValuesMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_insert_columns_and_values() -> None:
    builder = _TestInsertValuesMixin(exp.Insert())
    builder.columns("name", "email")
    builder.values("John", "john@example.com")
    assert isinstance(builder._expression, exp.Insert)


def test_insert_values_wrong_type() -> None:
    builder = _TestInsertValuesMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.values("John")


# --- SetOperationMixin ---
class _TestSetOpMixin(DummyBuilder, SetOperationMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    def build(self) -> object:
        class DummyQuery:
            def __init__(self, sql: str, parameters: dict[str, object]) -> None:
                self.sql: str = sql
                self.parameters: dict[str, object] = parameters

        return DummyQuery("SELECT 1", self._parameters)


def test_union_operation() -> None:
    builder1 = _TestSetOpMixin(exp.Select())
    builder2 = _TestSetOpMixin(exp.Select())
    builder1._parameters = {"param_1": 1}
    builder2._parameters = {"param_2": 2}
    result = builder1.union(builder2)
    assert isinstance(result, _TestSetOpMixin)
    assert "param_1" in result._parameters and "param_2" in result._parameters


# --- GroupByClauseMixin ---
class _TestGroupByMixin(DummyBuilder, GroupByClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_group_by_clause() -> None:
    builder = _TestGroupByMixin(exp.Select())
    builder.group_by("name")
    assert isinstance(builder._expression, exp.Select)


def test_group_by_rollup() -> None:
    builder = _TestGroupByMixin(exp.Select())
    assert builder._expression is not None
    builder.group_by_rollup("a", "b")
    group = builder._expression.args.get("group")
    assert group is not None
    # Should be a Group node containing a Rollup in its expressions
    assert isinstance(group, exp.Group)
    exprs = group.args.get("expressions")
    assert exprs is not None
    found = any(isinstance(e, exp.Rollup) for e in exprs if e is not None)
    assert found


# --- HavingClauseMixin ---
class _TestHavingMixin(DummyBuilder, HavingClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_having_clause() -> None:
    builder = _TestHavingMixin(exp.Select())
    builder.having("COUNT(*) > 1")
    assert isinstance(builder._expression, exp.Select)


def test_having_wrong_type() -> None:
    builder = _TestHavingMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.having("COUNT(*) > 1")


# --- UpdateSetClauseMixin ---
class _TestUpdateSetMixin(DummyBuilder, UpdateSetClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_update_set_clause() -> None:
    builder = _TestUpdateSetMixin(exp.Update())
    builder.set(name="John")
    assert isinstance(builder._expression, exp.Update)


def test_update_set_wrong_type() -> None:
    builder = _TestUpdateSetMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.set(name="John")


# --- UpdateFromClauseMixin ---
class _TestUpdateFromMixin(DummyBuilder, UpdateFromClauseMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_update_from_clause() -> None:
    builder = _TestUpdateFromMixin(exp.Update())
    builder.from_("other_table")
    assert isinstance(builder._expression, exp.Update)


def test_update_from_wrong_type() -> None:
    builder = _TestUpdateFromMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.from_("other_table")


# --- InsertFromSelectMixin ---
class _TestInsertFromSelectMixin(DummyBuilder, InsertFromSelectMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_insert_from_select_requires_table() -> None:
    builder = _TestInsertFromSelectMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.from_select(DummyBuilder(exp.Select()))


# --- Merge*ClauseMixins ---
class _TestMergeMixin(
    DummyBuilder,
    MergeIntoClauseMixin,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    pass


def test_merge_into_and_using() -> None:
    builder = _TestMergeMixin(exp.Merge())
    builder.into("target")
    builder.using("source")
    builder.on("target.id = source.id")
    builder.when_matched_then_update({"name": "new"})
    builder.when_not_matched_then_insert(["id"], [1])
    assert isinstance(builder._expression, exp.Merge)


def test_merge_on_invalid_condition() -> None:
    builder = _TestMergeMixin(exp.Merge())
    builder.into("target")
    builder.using("source")
    with pytest.raises(SQLBuilderError):
        builder.on(None)  # type: ignore[arg-type]


# --- PivotClauseMixin ---
class _TestPivotMixin(DummyBuilder, PivotClauseMixin):
    _expression: "Optional[exp.Expression]"
    dialect: "DialectType"

    def __init__(self, expression: "Optional[exp.Expression]" = None) -> None:
        super().__init__(expression)

    pass


def test_pivot_clause_basic() -> None:
    # Create a Select with a FROM clause (required for PIVOT)
    select_expr = exp.Select().from_("sales_data")
    builder = _TestPivotMixin(select_expr)
    builder.pivot(
        aggregate_function="SUM",
        aggregate_column="sales",
        pivot_column="quarter",
        pivot_values=["Q1", "Q2", "Q3", "Q4"],
        alias=None,
    )
    assert isinstance(builder._expression, exp.Select)
    # PIVOT should be attached to the table in FROM clause
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0
    assert any(pivot.args.get("unpivot") is False for pivot in pivots)


def test_pivot_clause_with_alias() -> None:
    # Create a Select with a FROM clause (required for PIVOT)
    select_expr = exp.Select().from_("sales_data")
    builder = _TestPivotMixin(select_expr)
    builder.pivot(
        aggregate_function="SUM",
        aggregate_column="sales",
        pivot_column="quarter",
        pivot_values=["Q1", "Q2"],
        alias="pivot_table",
    )
    assert builder._expression is not None
    # PIVOT should be attached to the table in FROM clause
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0
    pivot_node = pivots[0]
    alias_val = pivot_node.args.get("alias")
    assert alias_val is not None
    assert "pivot_table" in str(alias_val)


def test_pivot_clause_wrong_type() -> None:
    builder = _TestPivotMixin(exp.Insert())
    with pytest.raises(TypeError):
        builder.pivot(
            aggregate_function="SUM",
            aggregate_column="sales",
            pivot_column="quarter",
            pivot_values=["Q1"],
        )


# --- UnpivotClauseMixin ---
class _TestUnpivotMixin(DummyBuilder, UnpivotClauseMixin):
    _expression: "Optional[exp.Expression]"
    dialect: "DialectType"

    def __init__(self, expression: "Optional[exp.Expression]" = None) -> None:
        super().__init__(expression)

    pass


def test_unpivot_clause_basic() -> None:
    # Create a Select with a FROM clause (required for UNPIVOT)
    select_expr = exp.Select().from_("quarterly_sales")
    builder = _TestUnpivotMixin(select_expr)
    builder.unpivot(
        value_column_name="sales",
        name_column_name="quarter",
        columns_to_unpivot=["Q1", "Q2", "Q3", "Q4"],
        alias=None,
    )
    assert isinstance(builder._expression, exp.Select)
    # UNPIVOT should be attached to the table in FROM clause as Pivot with unpivot=True
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0
    assert any(pivot.args.get("unpivot") is True for pivot in pivots)


def test_unpivot_clause_with_alias() -> None:
    # Create a Select with a FROM clause (required for UNPIVOT)
    select_expr = exp.Select().from_("monthly_sales")
    builder = _TestUnpivotMixin(select_expr)
    builder.unpivot(
        value_column_name="amount",
        name_column_name="month",
        columns_to_unpivot=["Jan", "Feb"],
        alias="unpivot_table",
    )
    assert builder._expression is not None
    # UNPIVOT should be attached to the table in FROM clause as Pivot with unpivot=True
    from_clause = builder._expression.args.get("from")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    pivots = table.args.get("pivots", [])
    assert len(pivots) > 0
    pivot_node = pivots[0]
    assert pivot_node.args.get("unpivot") is True
    alias_val = pivot_node.args.get("alias")
    assert alias_val is not None
    assert "unpivot_table" in str(alias_val)


def test_unpivot_clause_wrong_type() -> None:
    builder = _TestUnpivotMixin(exp.Insert())
    with pytest.raises(TypeError):
        builder.unpivot(
            value_column_name="sales",
            name_column_name="quarter",
            columns_to_unpivot=["Q1"],
        )


# --- AggregateFunctionsMixin ---
class MockAggregateFunctionsMixin(DummyBuilder, AggregateFunctionsMixin):
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        super().__init__(expression)

    def select(self, expr: object) -> "MockAggregateFunctionsMixin":
        assert self._expression is not None
        exprs = self._expression.args.get("expressions")
        if exprs is None:
            self._expression.set("expressions", [expr])
        else:
            exprs.append(expr)
        return self


def test_array_agg() -> None:
    builder = MockAggregateFunctionsMixin(exp.Select())
    assert builder._expression is not None
    builder.array_agg("foo")
    assert builder._expression is not None
    select_exprs = builder._expression.args.get("expressions")
    assert select_exprs is not None
    found = any(
        isinstance(expr, exp.ArrayAgg)
        or (isinstance(expr, exp.Anonymous) and getattr(expr, "this", None) == "ARRAY_AGG")
        for expr in select_exprs
        if expr is not None
    )
    assert found


def test_bool_and() -> None:
    builder = MockAggregateFunctionsMixin(exp.Select())
    assert builder._expression is not None
    builder.bool_and("bar")
    assert builder._expression is not None
    select_exprs = builder._expression.args.get("expressions")
    assert select_exprs is not None
    found = any(
        (isinstance(expr, exp.Anonymous) and getattr(expr, "this", None) == "BOOL_AND")
        for expr in select_exprs
        if expr is not None
    )
    assert found


def test_bool_or() -> None:
    builder = MockAggregateFunctionsMixin(exp.Select())
    assert builder._expression is not None
    builder.bool_or("baz")
    assert builder._expression is not None
    select_exprs = builder._expression.args.get("expressions")
    assert select_exprs is not None
    found = any(
        (isinstance(expr, exp.Anonymous) and getattr(expr, "this", None) == "BOOL_OR")
        for expr in select_exprs
        if expr is not None
    )
    assert found
