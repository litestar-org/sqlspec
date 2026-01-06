"""Tests for _create_count_query parsing behavior."""

import sqlite3
from typing import Any, cast

from sqlspec import SQL
from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import get_default_config


def test_create_count_query_compiles_missing_expression() -> None:
    """Ensure count query generation parses SQL lacking prebuilt expression."""
    connection = sqlite3.connect(":memory:")
    statement_config = get_default_config()
    driver = SqliteDriver(connection, statement_config)

    try:
        sql_statement = SQL("SELECT id FROM users WHERE active = true")

        assert sql_statement.expression is None

        count_sql = cast("Any", driver)._create_count_query(sql_statement)

        assert sql_statement.expression is not None

        compiled_sql, _ = count_sql.compile()

        assert count_sql.expression is not None
        assert "count" in compiled_sql.lower()
    finally:
        connection.close()
