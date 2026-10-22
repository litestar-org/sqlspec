"""Unit tests for the Oracle EXPLAIN PLAN retrieval flow."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb.driver import (
    PLAN_STATEMENT_ID_MAX_LENGTH,
    OracleAsyncDriver,
    OracleSyncDriver,
    _managed_explain_options,
    _managed_explain_statement,
    _new_plan_statement_id,
    _tagged_explain_plan_sql,
)
from sqlspec.builder import Explain, build_oracle_display_cursor
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import SQLBuilderError, SQLSpecError


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


def test_oracle_explain_builder_display_cursor_and_xplan_format() -> None:
    stmt = (
        Explain("SELECT * FROM t WHERE a = :1", dialect="oracle")
        .display_cursor()
        .xplan_format("ALLSTATS LAST +PEEKED_BINDS")
        .build()
    )
    display_cursor, xplan_format = _managed_explain_options(stmt)
    assert display_cursor is True
    assert xplan_format == "ALLSTATS LAST +PEEKED_BINDS"

    sql_cursor = build_oracle_display_cursor(format="TYPICAL +PREDICATE +NOTE")
    assert (
        sql_cursor
        == "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'TYPICAL +PREDICATE +NOTE'))"
    )

    with pytest.raises(SQLBuilderError):
        Explain("SELECT 1 FROM dual", dialect="oracle").xplan_format("BAD'FMT")


def test_oracle_explain_plan_custom_xplan_format_and_display_cursor_sync() -> None:
    driver = OracleSyncDriver(MagicMock())
    cursor = MagicMock()
    cursor.description = [("PLAN_TABLE_OUTPUT", None, None, None, None, None, None)]
    cursor.fetchall.return_value = [("Plan hash value: 123",)]

    driver._dispatch_explain_plan(
        cursor, "SELECT 1 FROM dual", {}, display_cursor=True, xplan_format="ALLSTATS LAST +PEEKED_BINDS"
    )
    executed_sqls = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed_sqls[0] == "SELECT 1 FROM dual"
    assert "DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST +PEEKED_BINDS')" in executed_sqls[1]


@pytest.mark.anyio
async def test_oracle_explain_plan_custom_xplan_format_and_display_cursor_async() -> None:
    driver = OracleAsyncDriver(MagicMock())
    cursor = MagicMock()
    cursor.description = [("PLAN_TABLE_OUTPUT", None, None, None, None, None, None)]
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[("Plan hash value: 123",)])

    await driver._dispatch_explain_plan(
        cursor, "SELECT 1 FROM dual", {}, display_cursor=True, xplan_format="ALLSTATS LAST +PEEKED_BINDS"
    )
    executed_sqls = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed_sqls[0] == "SELECT 1 FROM dual"
    assert "DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST +PEEKED_BINDS')" in executed_sqls[1]
