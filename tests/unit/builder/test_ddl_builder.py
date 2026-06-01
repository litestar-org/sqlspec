"""Regression tests for DDL builder Wave 1 fixes."""

import inspect

import pytest

from sqlspec import sql
from sqlspec.builder._ddl import (
    CONSTRAINT_TYPE_CHECK,
    CONSTRAINT_TYPE_FOREIGN_KEY,
    VALID_FOREIGN_KEY_ACTIONS,
    ColumnDefinition,
    ConstraintDefinition,
    CreateIndex,
    CreateTable,
    build_column_expression,
    build_constraint_expression,
)
from sqlspec.exceptions import SQLBuilderError


def test_auto_increment_column_renders_auto_increment() -> None:
    expr = build_column_expression(ColumnDefinition("id", "INT", auto_increment=True))
    assert "AUTO_INCREMENT" in expr.sql()


def test_create_index_using_renders_using_clause() -> None:
    result = CreateIndex("idx").on_table("t").columns("a").using("BTREE").build()
    assert "USING" in result.sql
    assert "BTREE" in result.sql


def test_create_index_using_without_columns_renders_using_clause() -> None:
    result = CreateIndex("idx").on_table("t").using("HASH").build()
    assert "USING" in result.sql
    assert "HASH" in result.sql


def test_foreign_key_deferrable_initially_deferred_renders_clause() -> None:
    result = (
        CreateTable("t")
        .column("user_id", "INT")
        .foreign_key_constraint("user_id", "users", "id", deferrable=True, initially_deferred=True)
        .build()
    )
    assert "DEFERRABLE" in result.sql
    assert "INITIALLY DEFERRED" in result.sql


def test_foreign_key_deferrable_initially_immediate_renders_clause() -> None:
    constraint = ConstraintDefinition(
        constraint_type=CONSTRAINT_TYPE_FOREIGN_KEY,
        columns=["user_id"],
        references_table="users",
        references_columns=["id"],
        deferrable=True,
        initially_deferred=False,
    )
    expr = build_constraint_expression(constraint)
    assert expr is not None
    sql = expr.sql()
    assert "DEFERRABLE" in sql
    assert "INITIALLY IMMEDIATE" in sql


def test_valid_foreign_key_actions_excludes_none() -> None:
    assert None not in VALID_FOREIGN_KEY_ACTIONS


def test_foreign_key_action_none_still_short_circuits_validation() -> None:
    table = CreateTable("orders")
    table.foreign_key_constraint("user_id", "users", "id", on_delete=None, on_update=None)


def test_foreign_key_action_invalid_value_still_raises() -> None:
    with pytest.raises(SQLBuilderError):
        CreateTable("orders").foreign_key_constraint("user_id", "users", "id", on_delete="EXPLODE")


def test_ddl_module_uses_pep604_annotations() -> None:
    import sqlspec.builder._ddl as ddl_module

    assert "Union" not in inspect.getsource(ddl_module)


def test_ddl_check_constraint_check_constraint_column_expression_stores_condition_expr() -> None:
    condition = sql.column("age") > 0
    table = CreateTable("users").column("age", "INT").check_constraint(condition)
    constraint = table._constraints[-1]
    assert constraint.condition is None
    assert constraint.condition_expr is condition.sqlglot_expression


def test_ddl_check_constraint_build_constraint_expression_uses_condition_expr_without_reparse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    condition = sql.column("age") > 0
    constraint = ConstraintDefinition(
        constraint_type=CONSTRAINT_TYPE_CHECK, name="ck_age", condition_expr=condition.sqlglot_expression
    )

    def fail_maybe_parse(*args: object, **kwargs: object) -> object:
        msg = "condition_expr should bypass exp.maybe_parse"
        raise AssertionError(msg)

    monkeypatch.setattr("sqlspec.builder._ddl.exp.maybe_parse", fail_maybe_parse)
    expr = build_constraint_expression(constraint)
    assert expr is not None
    assert "CK_AGE" in expr.sql().upper()
