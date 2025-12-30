"""Tests for the _should_force_select safety net."""

import sqlite3
from typing import Any, cast

from sqlspec import SQL, ProcessedState
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import get_default_config


class _CursorWithStatementType:
    """Cursor exposing a statement_type attribute."""

    def __init__(self, statement_type: str | None) -> None:
        self.statement_type = statement_type
        self.description = None


class _CursorWithDescription:
    """Cursor exposing a description attribute."""

    def __init__(self, has_description: bool) -> None:
        self.description = [("col",)] if has_description else None
        self.statement_type = None


def _make_unknown_statement(sql_text: str = "select 1") -> "SQL":
    stmt = SQL(sql_text)
    cast("Any", stmt)._processed_state = ProcessedState(
        compiled_sql=sql_text, execution_parameters={}, operation_type="UNKNOWN"
    )
    return stmt


def _make_select_statement(sql_text: str = "select 1") -> "SQL":
    stmt = SQL(sql_text)
    cast("Any", stmt)._processed_state = ProcessedState(
        compiled_sql=sql_text, execution_parameters={}, operation_type="SELECT"
    )
    return stmt


def _get_test_driver() -> tuple[SqliteDriver, Any]:
    """Create a test driver with SQLite in-memory connection."""
    connection = sqlite3.connect(":memory:")
    statement_config = get_default_config()
    driver = SqliteDriver(connection, statement_config)
    return driver, connection


def test_force_select_uses_statement_type_select() -> None:
    driver, connection = _get_test_driver()
    try:
        stmt = _make_unknown_statement()
        cursor = _CursorWithStatementType("SELECT")

        assert cast("Any", driver)._should_force_select(stmt, cursor) is True
    finally:
        connection.close()


def test_force_select_uses_description_when_unknown() -> None:
    driver, connection = _get_test_driver()
    try:
        stmt = _make_unknown_statement()
        cursor = _CursorWithDescription(True)

        assert cast("Any", driver)._should_force_select(stmt, cursor) is True
    finally:
        connection.close()


def test_force_select_false_when_no_metadata() -> None:
    driver, connection = _get_test_driver()
    try:
        stmt = _make_unknown_statement()
        cursor = _CursorWithDescription(False)

        assert cast("Any", driver)._should_force_select(stmt, cursor) is False
    finally:
        connection.close()


def test_force_select_ignored_when_operation_known() -> None:
    driver, connection = _get_test_driver()
    try:
        stmt = _make_select_statement()
        cursor = _CursorWithDescription(True)

        assert cast("Any", driver)._should_force_select(stmt, cursor) is False
    finally:
        connection.close()
