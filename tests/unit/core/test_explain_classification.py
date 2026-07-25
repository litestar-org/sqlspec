"""Unit tests for row-returning classification of command-shaped statements."""

from typing import get_args

import pytest
import sqlglot

from sqlspec.core import SQL, StatementConfig
from sqlspec.core.compiler import OperationType, SQLProcessor
from sqlspec.core.statement import RETURNS_ROWS_OPERATIONS

ROW_RETURNING_EXPLAIN_DIALECTS = ("postgres", "mysql", "sqlite", "duckdb", "bigquery", "tsql", "spanner")


def test_returns_rows_operations_are_reachable_operation_types() -> None:
    """Every entry must be an operation type the compiler can actually emit."""
    unreachable = RETURNS_ROWS_OPERATIONS - set(get_args(OperationType))

    assert not unreachable, f"unreachable operation types in RETURNS_ROWS_OPERATIONS: {sorted(unreachable)}"


def test_pragma_row_returning_comes_from_the_operation_type_set() -> None:
    """PRAGMA is classified by operation type because the profile does not cover it."""
    expression = sqlglot.parse_one("PRAGMA table_info(t)", dialect="sqlite")
    operation_type = SQLProcessor._operation_type(expression)

    assert operation_type == "PRAGMA"
    assert SQLProcessor._operation_profile(expression, operation_type).returns_rows is False
    assert "PRAGMA" in RETURNS_ROWS_OPERATIONS


@pytest.mark.parametrize("dialect", ROW_RETURNING_EXPLAIN_DIALECTS)
def test_explain_profile_returns_rows(dialect: str) -> None:
    """EXPLAIN reports row-returning on every dialect whose EXPLAIN emits a plan."""
    expression = sqlglot.parse_one(f"{'DESCRIBE' if dialect == 'mysql' else 'EXPLAIN'} SELECT 1", dialect=dialect)
    operation_type = SQLProcessor._operation_type(expression)
    profile = SQLProcessor._operation_profile(expression, operation_type)

    assert profile.returns_rows is True
    assert profile.modifies_rows is False


def test_oracle_explain_plan_for_does_not_return_rows() -> None:
    """Oracle EXPLAIN PLAN FOR writes PLAN_TABLE and yields no result rows."""
    expression = sqlglot.parse_one("EXPLAIN PLAN FOR SELECT 1", dialect="oracle")
    profile = SQLProcessor._operation_profile(expression, SQLProcessor._operation_type(expression))

    assert profile.returns_rows is False


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        ("EXPLAIN SELECT 1", "mysql"),
        ("DESCRIBE some_table", "mysql"),
        ("SHOW TABLES", "mysql"),
        ("SHOW ALL", "postgres"),
    ],
)
def test_describe_and_show_return_rows(sql: str, dialect: str) -> None:
    """DESCRIBE and SHOW are row-returning even though they classify as COMMAND."""
    expression = sqlglot.parse_one(sql, dialect=dialect)
    profile = SQLProcessor._operation_profile(expression, SQLProcessor._operation_type(expression))

    assert profile.returns_rows is True


def test_explain_keeps_command_operation_type() -> None:
    """Classification changes the profile only, so the driver COMMAND safety net still applies."""
    expression = sqlglot.parse_one("EXPLAIN SELECT 1", dialect="postgres")

    assert SQLProcessor._operation_type(expression) == "COMMAND"


def test_unrelated_commands_stay_non_row_returning() -> None:
    """Commands with no result set keep returns_rows False."""
    for sql, dialect in (("VACUUM", "postgres"), ("COMMIT", "postgres"), ("SET search_path = public", "postgres")):
        expression = sqlglot.parse_one(sql, dialect=dialect)
        profile = SQLProcessor._operation_profile(expression, SQLProcessor._operation_type(expression))

        assert profile.returns_rows is False, sql


@pytest.mark.parametrize("dialect", ROW_RETURNING_EXPLAIN_DIALECTS)
def test_sql_explain_returns_rows(dialect: str) -> None:
    """SQL.explain() produces a row-returning statement so drivers fetch the plan."""
    statement = SQL("SELECT 1", statement_config=StatementConfig(dialect=dialect)).explain(analyze=True)

    assert statement.returns_rows() is True


def test_sql_explain_oracle_stays_non_row_returning() -> None:
    """Oracle keeps its two-step EXPLAIN PLAN FOR then DBMS_XPLAN flow."""
    statement = SQL("SELECT 1", statement_config=StatementConfig(dialect="oracle")).explain()

    assert statement.sql == "EXPLAIN PLAN FOR SELECT 1"
    assert statement.returns_rows() is False
