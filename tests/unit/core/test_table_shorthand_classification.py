"""Unit tests for row-returning classification of the ``TABLE t`` shorthand."""

import pytest
import sqlglot

from sqlspec.core import SQL, StatementConfig
from sqlspec.core.compiler import SQLProcessor

TABLE_SHORTHAND_DIALECTS = ("postgres", "duckdb", "mysql")


@pytest.mark.parametrize("dialect", TABLE_SHORTHAND_DIALECTS)
def test_table_shorthand_returns_rows(dialect: str) -> None:
    """``TABLE t`` is shorthand for ``SELECT * FROM t`` and yields rows."""
    statement = SQL("TABLE t", statement_config=StatementConfig(dialect=dialect))

    assert statement.returns_rows() is True


@pytest.mark.parametrize("dialect", TABLE_SHORTHAND_DIALECTS)
def test_qualified_table_shorthand_returns_rows(dialect: str) -> None:
    """A schema-qualified operand keeps the shorthand row-returning."""
    statement = SQL("TABLE public.t", statement_config=StatementConfig(dialect=dialect))

    assert statement.returns_rows() is True


def test_table_shorthand_with_trailing_clauses_returns_rows() -> None:
    """PostgreSQL accepts ordering and limit clauses that sqlglot cannot parse."""
    statement = SQL("TABLE t ORDER BY a LIMIT 5", statement_config=StatementConfig(dialect="postgres"))

    assert statement.returns_rows() is True


def test_leading_whitespace_does_not_hide_the_shorthand() -> None:
    """Statement text is matched after leading whitespace."""
    statement = SQL("\n   TABLE t\n", statement_config=StatementConfig(dialect="postgres"))

    assert statement.returns_rows() is True


def test_table_shorthand_keeps_raw_sql_intact() -> None:
    """Classification must not rewrite the statement, which sqlglot renders as ``TABLE AS t``."""
    statement = SQL("TABLE t", statement_config=StatementConfig(dialect="postgres"))
    compiled, _ = statement.compile()

    assert compiled == "TABLE t"


def test_table_shorthand_keeps_command_operation_type() -> None:
    """Only the profile changes, so the driver COMMAND safety net still applies."""
    statement = SQL("TABLE t", statement_config=StatementConfig(dialect="postgres"))

    assert statement.operation_type == "COMMAND"


def test_bare_table_keyword_is_not_a_shorthand() -> None:
    """The shorthand requires an operand after the keyword."""
    statement = SQL("TABLE", statement_config=StatementConfig(dialect="postgres"))

    assert statement.returns_rows() is False


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        ("TRUNCATE TABLE t", "postgres"),
        ("CREATE TABLE t (a INT)", "postgres"),
        ("DROP TABLE t", "postgres"),
        ("ALTER TABLE t ADD COLUMN b INT", "postgres"),
    ],
)
def test_statements_that_merely_mention_table_do_not_return_rows(sql: str, dialect: str) -> None:
    """Only a leading TABLE keyword marks the shorthand."""
    statement = SQL(sql, statement_config=StatementConfig(dialect=dialect))

    assert statement.returns_rows() is False


def test_upstream_table_expression_already_returns_rows() -> None:
    """A parsed exp.Table keeps working, so an upstream sqlglot fix needs no change here."""
    expression = sqlglot.exp.Table(this=sqlglot.exp.to_identifier("t"))
    profile = SQLProcessor._operation_profile(expression, SQLProcessor._operation_type(expression))

    assert profile.returns_rows is True
