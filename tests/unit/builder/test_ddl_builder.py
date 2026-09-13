"""Regression tests for DDL builder Wave 1 fixes."""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder._ddl import (
    CONSTRAINT_TYPE_CHECK,
    CONSTRAINT_TYPE_FOREIGN_KEY,
    VALID_FOREIGN_KEY_ACTIONS,
    AlterTable,
    ColumnDefinition,
    ConstraintDefinition,
    CreateIndex,
    CreateTable,
    build_column_expression,
    build_constraint_expression,
)
from sqlspec.core import StatementConfig
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


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "mysql", "oracle", "tsql"])
def test_create_index_where_renders_predicate_per_dialect(dialect: str) -> None:
    result = CreateIndex("ix_t_k").unique().on_table("t").columns("k").where("k IS NOT NULL").build(dialect=dialect)
    assert "WHERE" in result.sql.upper()
    assert "NULL" in result.sql.upper()


def test_create_index_where_with_tsql_if_not_exists_guard() -> None:
    result = (
        CreateIndex("ix_queue_tasks_task_key")
        .if_not_exists()
        .unique()
        .on_table("queue_tasks")
        .columns("task_key")
        .where("task_key IS NOT NULL")
        .build(dialect="tsql")
    )
    exec_payload = result.sql.split("EXEC")[-1]
    assert "WHERE" in exec_payload.upper()
    assert "NULL" in exec_payload.upper()


def test_create_index_where_accepts_expression() -> None:
    result = (
        CreateIndex("ix_t_k")
        .unique()
        .on_table("t")
        .columns("k")
        .where(exp.condition("k IS NOT NULL"))
        .build(dialect="postgres")
    )
    assert "WHERE" in result.sql.upper()
    assert "NULL" in result.sql.upper()


def test_create_table_parses_types_with_target_dialect() -> None:
    """Target dialect should allow dialect-specific column types to parse."""
    result = sql.create_table("t").column("a", "DATETIME2(6)").build(dialect="tsql")
    assert "DATETIME2(6)" in result.sql


def test_unparsable_type_raises_builder_error() -> None:
    """Unparsable column types raise SQLBuilderError naming the column."""
    with pytest.raises(SQLBuilderError) as exc_info:
        sql.create_table("t").column("a", "DATETIME2(6)").build()
    assert "'a'" in str(exc_info.value)


def test_rebuild_for_other_dialect() -> None:
    """Switching dialect on the same builder re-parses and raises if unparsable."""
    builder = sql.create_table("t").column("a", "TIMESTAMPTZ")
    pg_result = builder.build(dialect="postgres")
    assert "TIMESTAMPTZ" in pg_result.sql.upper() or "TIMESTAMP" in pg_result.sql.upper()

    tsql_result = builder.build(dialect="tsql")
    assert "DATETIMEOFFSET" in tsql_result.sql.upper() or "TIMESTAMP" in tsql_result.sql.upper()

    raising_builder = sql.create_table("t").column("a", "DATETIME2(6)")
    assert "DATETIME2(6)" in raising_builder.build(dialect="tsql").sql
    with pytest.raises(SQLBuilderError) as exc_info:
        raising_builder.build()
    assert "'a'" in str(exc_info.value)


def test_alter_table_add_column_with_target_dialect() -> None:
    """AlterTable add_column parses column types with target dialect."""
    result = sql.alter_table("t").add_column("b", "NVARCHAR(MAX)").build(dialect="tsql")
    assert "NVARCHAR(MAX)" in result.sql

    dt2_result = sql.alter_table("t").add_column("b", "DATETIME2(6)").build(dialect="tsql")
    assert "DATETIME2(6)" in dt2_result.sql


@pytest.mark.parametrize("enable_caching", [True, False])
@pytest.mark.parametrize("operation", ["create", "add", "alter"])
def test_ddl_to_statement_uses_configured_dialect(operation: str, enable_caching: bool) -> None:
    if operation == "create":
        builder: CreateTable | AlterTable = sql.create_table("t").column("a", "DATETIME2(6)")
    elif operation == "add":
        builder = sql.alter_table("t").add_column("a", "DATETIME2(6)")
    else:
        builder = sql.alter_table("t").alter_column_type("a", "DATETIME2(6)")

    result = builder.to_statement(StatementConfig(dialect="tsql", enable_caching=enable_caching))
    assert "DATETIME2(6)" in result.sql
    with pytest.raises(SQLBuilderError, match="Column 'a'"):
        builder.to_statement(StatementConfig(enable_caching=enable_caching))
    assert "DATETIME2(6)" in builder.build(dialect="tsql").sql


def test_alter_column_type_uses_target_dialect() -> None:
    builder = sql.alter_table("t").alter_column_type("a", "DATETIME2(6)")
    assert "DATETIME2(6)" in builder.build(dialect="tsql").sql
    with pytest.raises(SQLBuilderError, match="Column 'a'"):
        builder.build()
