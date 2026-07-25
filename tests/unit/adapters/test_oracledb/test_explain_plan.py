"""Unit tests for the Oracle EXPLAIN PLAN retrieval flow."""

from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.oracledb.driver import (
    PLAN_STATEMENT_ID_MAX_LENGTH,
    OracleSyncDriver,
    _managed_explain_statement,
    _new_plan_statement_id,
    _tagged_explain_plan_sql,
)
from sqlspec.builder import Explain
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import SQLSpecError


def test_managed_explain_plan_yields_the_explained_statement() -> None:
    """SQLSpec ownership metadata selects the two-step path without reparsing."""
    statement = Explain("SELECT * FROM t WHERE a = :1", dialect="oracle").build()

    assert _managed_explain_statement(statement, statement.compile()[0]) == "SELECT * FROM t WHERE a = ?"


def test_raw_explain_plan_is_not_managed() -> None:
    """Identical caller SQL remains a normal one-statement execution."""
    statement = SQL("EXPLAIN PLAN FOR SELECT 1 FROM dual", statement_config=StatementConfig(dialect="oracle"))

    assert _managed_explain_statement(statement, statement.compile()[0]) is None


def test_managed_marker_survives_oracle_parameter_config_rebuild() -> None:
    """Driver preparation preserves the marked SQLGlot expression while changing bind style."""
    driver = OracleSyncDriver(MagicMock())
    statement = Explain("SELECT 1 FROM dual", dialect="oracle").build()

    prepared = driver.prepare_statement(statement, (), statement_config=driver.statement_config)
    compiled_sql, _ = prepared.compile()

    assert prepared is not statement
    assert _managed_explain_statement(prepared, compiled_sql) == "SELECT 1 FROM dual"


def test_managed_explain_requires_the_generated_prefix() -> None:
    """Marked statements fail closed when an output transformer breaks the invariant."""
    statement = Explain("SELECT 1 FROM dual", dialect="oracle").build()

    with pytest.raises(SQLSpecError, match="lost its generated prefix"):
        _managed_explain_statement(statement, "SELECT 1 FROM dual")


def test_generated_statement_id_fits_the_plan_table_column() -> None:
    """Oracle stores STATEMENT_ID in a VARCHAR2(30) column."""
    statement_id = _new_plan_statement_id()

    assert len(statement_id) <= PLAN_STATEMENT_ID_MAX_LENGTH


def test_generated_statement_ids_are_unique() -> None:
    """Concurrent sessions and repeated calls must not collide."""
    assert len({_new_plan_statement_id() for _ in range(100)}) == 100


def test_generated_statement_id_cannot_break_out_of_the_literal() -> None:
    """The identifier is interpolated into a quoted literal, so it stays alphanumeric."""
    statement_id = _new_plan_statement_id()

    assert statement_id.replace("_", "").isalnum()


def test_tagged_sql_preserves_the_explained_statement() -> None:
    """Tagging adds the statement id without disturbing binds in the explained SQL."""
    tagged = _tagged_explain_plan_sql("sqlspec_abc123", "SELECT * FROM t WHERE a = :1")

    assert tagged == "EXPLAIN PLAN SET STATEMENT_ID = 'sqlspec_abc123' FOR SELECT * FROM t WHERE a = :1"
