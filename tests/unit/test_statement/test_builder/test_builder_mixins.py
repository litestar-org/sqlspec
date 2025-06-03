from typing import Dict, Optional, Tuple

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError


# --- Minimal Protocol for Mixins ---
class DummyBuilder:
    def __init__(self, expression: Optional[exp.Expression] = None) -> None:
        self._expression: Optional[exp.Expression] = expression
        self._parameters: Dict[str, object] = {}
        self._parameter_counter: int = 0
        self.dialect: Optional[str] = None
        self.dialect_name: Optional[str] = None
        self._table: Optional[str] = None

    def add_parameter(self, value: object, name: Optional[str] = None) -> Tuple["DummyBuilder", str]:
        if name and name in self._parameters:
            raise SQLBuilderError(f"Parameter name '{name}' already exists.")
        param_name = name or f"param_{self._parameter_counter + 1}"
        self._parameter_counter += 1
        self._parameters[param_name] = value
        return self, param_name

    def build(self) -> object:
        # Dummy build for set ops
        class DummyQuery:
            def __init__(self, sql: str, parameters: Dict[str, object]) -> None:
                self.sql: str = sql
                self.parameters: Dict[str, object] = parameters

        return DummyQuery("SELECT 1", self._parameters)


# --- Import Mixins ---
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
from sqlspec.statement.builder.mixins._returning import ReturningClauseMixin
from sqlspec.statement.builder.mixins._set_ops import SetOperationMixin
from sqlspec.statement.builder.mixins._update_from import UpdateFromClauseMixin
from sqlspec.statement.builder.mixins._update_set import UpdateSetClauseMixin
from sqlspec.statement.builder.mixins._where import WhereClauseMixin


# --- WhereClauseMixin ---
class TestWhereMixin(DummyBuilder, WhereClauseMixin):
    pass


def test_where_clause_basic() -> None:
    builder = TestWhereMixin(exp.Select())
    builder.where("id = 1")
    assert isinstance(builder._expression, exp.Select)


def test_where_clause_wrong_type() -> None:
    builder = TestWhereMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.where("id = 1")


def test_where_eq_adds_param() -> None:
    builder = TestWhereMixin(exp.Select())
    builder.where_eq("name", "John")
    assert "John" in builder._parameters.values()


# --- JoinClauseMixin ---
class TestJoinMixin(DummyBuilder, JoinClauseMixin):
    pass


def test_join_inner() -> None:
    builder = TestJoinMixin(exp.Select())
    builder.join("users", on="users.id = orders.user_id", join_type="INNER")
    assert isinstance(builder._expression, exp.Select)


def test_join_invalid_type() -> None:
    builder = TestJoinMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.join("users")


# --- LimitOffsetClauseMixin ---
class TestLimitOffsetMixin(DummyBuilder, LimitOffsetClauseMixin):
    pass


def test_limit_clause() -> None:
    builder = TestLimitOffsetMixin(exp.Select())
    builder.limit(10)
    assert isinstance(builder._expression, exp.Select)


def test_offset_clause() -> None:
    builder = TestLimitOffsetMixin(exp.Select())
    builder.offset(5)
    assert isinstance(builder._expression, exp.Select)


def test_limit_offset_wrong_type() -> None:
    builder = TestLimitOffsetMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.limit(1)
    with pytest.raises(SQLBuilderError):
        builder.offset(1)


# --- OrderByClauseMixin ---
class TestOrderByMixin(DummyBuilder, OrderByClauseMixin):
    pass


def test_order_by_clause() -> None:
    builder = TestOrderByMixin(exp.Select())
    builder.order_by("name")
    assert isinstance(builder._expression, exp.Select)


def test_order_by_wrong_type() -> None:
    builder = TestOrderByMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.order_by("name")


# --- FromClauseMixin ---
class TestFromMixin(DummyBuilder, FromClauseMixin):
    pass


def test_from_clause() -> None:
    builder = TestFromMixin(exp.Select())
    builder.from_("users")
    assert isinstance(builder._expression, exp.Select)


def test_from_wrong_type() -> None:
    builder = TestFromMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.from_("users")


# --- ReturningClauseMixin ---
class TestReturningMixin(DummyBuilder, ReturningClauseMixin):
    pass


def test_returning_insert() -> None:
    builder = TestReturningMixin(exp.Insert())
    builder.returning("id")
    assert isinstance(builder._expression, exp.Insert)


def test_returning_wrong_type() -> None:
    builder = TestReturningMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.returning("id")


# --- InsertValuesMixin ---
class TestInsertValuesMixin(DummyBuilder, InsertValuesMixin):
    pass


def test_insert_columns_and_values() -> None:
    builder = TestInsertValuesMixin(exp.Insert())
    builder.columns("name", "email")
    builder.values("John", "john@example.com")
    assert isinstance(builder._expression, exp.Insert)


def test_insert_values_wrong_type() -> None:
    builder = TestInsertValuesMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.values("John")


# --- SetOperationMixin ---
class TestSetOpMixin(DummyBuilder, SetOperationMixin):
    def build(self) -> object:
        class DummyQuery:
            def __init__(self, sql: str, parameters: Dict[str, object]) -> None:
                self.sql: str = sql
                self.parameters: Dict[str, object] = parameters

        return DummyQuery("SELECT 1", self._parameters)


def test_union_operation() -> None:
    builder1 = TestSetOpMixin(exp.Select())
    builder2 = TestSetOpMixin(exp.Select())
    builder1._parameters = {"param_1": 1}
    builder2._parameters = {"param_2": 2}
    result = builder1.union(builder2)
    assert isinstance(result, TestSetOpMixin)
    assert "param_1" in result._parameters and "param_2" in result._parameters


# --- GroupByClauseMixin ---
class TestGroupByMixin(DummyBuilder, GroupByClauseMixin):
    pass


def test_group_by_clause() -> None:
    builder = TestGroupByMixin(exp.Select())
    builder.group_by("name")
    assert isinstance(builder._expression, exp.Select)


# --- HavingClauseMixin ---
class TestHavingMixin(DummyBuilder, HavingClauseMixin):
    pass


def test_having_clause() -> None:
    builder = TestHavingMixin(exp.Select())
    builder.having("COUNT(*) > 1")
    assert isinstance(builder._expression, exp.Select)


def test_having_wrong_type() -> None:
    builder = TestHavingMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.having("COUNT(*) > 1")


# --- UpdateSetClauseMixin ---
class TestUpdateSetMixin(DummyBuilder, UpdateSetClauseMixin):
    pass


def test_update_set_clause() -> None:
    builder = TestUpdateSetMixin(exp.Update())
    builder.set(name="John")
    assert isinstance(builder._expression, exp.Update)


def test_update_set_wrong_type() -> None:
    builder = TestUpdateSetMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.set(name="John")


# --- UpdateFromClauseMixin ---
class TestUpdateFromMixin(DummyBuilder, UpdateFromClauseMixin):
    pass


def test_update_from_clause() -> None:
    builder = TestUpdateFromMixin(exp.Update())
    builder.from_("other_table")
    assert isinstance(builder._expression, exp.Update)


def test_update_from_wrong_type() -> None:
    builder = TestUpdateFromMixin(exp.Select())
    with pytest.raises(SQLBuilderError):
        builder.from_("other_table")


# --- InsertFromSelectMixin ---
class TestInsertFromSelectMixin(DummyBuilder, InsertFromSelectMixin):
    pass


def test_insert_from_select_requires_table() -> None:
    builder = TestInsertFromSelectMixin(exp.Insert())
    with pytest.raises(SQLBuilderError):
        builder.from_select(DummyBuilder(exp.Select()))


# --- Merge*ClauseMixins ---
class TestMergeMixin(
    DummyBuilder,
    MergeIntoClauseMixin,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
):
    pass


def test_merge_into_and_using() -> None:
    builder = TestMergeMixin()
    builder.into("target")
    builder.using("source")
    builder.on("target.id = source.id")
    builder.when_matched_then_update({"name": "new"})
    builder.when_not_matched_then_insert(["id"], [1])
    assert isinstance(builder._expression, exp.Merge)


def test_merge_on_invalid_condition() -> None:
    builder = TestMergeMixin()
    builder.into("target")
    builder.using("source")
    with pytest.raises(SQLBuilderError):
        builder.on(None)  # type: ignore[arg-type]
