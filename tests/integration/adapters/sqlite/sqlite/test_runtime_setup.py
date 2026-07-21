"""Integration tests for SQLite runtime setup behavior."""

import sqlite3
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("sqlite")

TRACE_STATEMENTS: list[str] = []
PROGRESS_CALLS: list[int] = []


def _double(value: int) -> int:
    return value * 2


def _reverse_collation(left: str, right: str) -> int:
    if left < right:
        return 1
    if left > right:
        return -1
    return 0


class _SumAggregate:
    def __init__(self) -> None:
        self.total = 0

    def step(self, value: int | None) -> None:
        if value is not None:
            self.total += int(value)

    def finalize(self) -> int:
        return self.total


def _deny_secrets_read(
    action: int, arg1: str | None, arg2: str | None, db_name: str | None, trigger_name: str | None
) -> int:
    _ = arg2, db_name, trigger_name
    if action == sqlite3.SQLITE_READ and arg1 == "secrets":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _progress_handler() -> int:
    PROGRESS_CALLS.append(1)
    return 0


def _trace_callback(statement: str) -> None:
    TRACE_STATEMENTS.append(statement)


def test_custom_function_visible_in_sql() -> None:
    """Custom SQLite functions should be callable from SQL."""
    config = SqliteConfig(
        driver_features={
            "custom_functions": [{"name": "double_value", "narg": 1, "func": _double, "deterministic": True}]
        }
    )

    try:
        with config.provide_session() as session:
            assert session.select_value("SELECT double_value(21)") == 42
    finally:
        config.close_pool()


def test_custom_aggregate_visible_in_sql(tmp_path: Path) -> None:
    """Custom SQLite aggregates should be callable from SQL."""
    db_path = tmp_path / "aggregate.db"
    config = SqliteConfig(
        connection_config={"database": db_path},
        driver_features={"custom_aggregates": [{"name": "sum_values", "narg": 1, "aggregate_class": _SumAggregate}]},
    )

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE numbers (value INTEGER);
                INSERT INTO numbers (value) VALUES (1);
                INSERT INTO numbers (value) VALUES (2);
                INSERT INTO numbers (value) VALUES (3);
                INSERT INTO numbers (value) VALUES (4);
            """)
            session.commit()

            assert session.select_value("SELECT sum_values(value) FROM numbers") == 10
    finally:
        config.close_pool()


def test_custom_collation_orders_results(tmp_path: Path) -> None:
    """Custom SQLite collations should change ORDER BY ordering."""
    db_path = tmp_path / "collation.db"
    config = SqliteConfig(
        connection_config={"database": db_path},
        driver_features={"custom_collations": [{"name": "reverse_order", "func": _reverse_collation}]},
    )

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE names (name TEXT);
                INSERT INTO names (name) VALUES ('alice');
                INSERT INTO names (name) VALUES ('bob');
                INSERT INTO names (name) VALUES ('carol');
            """)
            session.commit()

            rows = session.select("SELECT name FROM names ORDER BY name COLLATE reverse_order")
            assert [row["name"] for row in rows] == ["carol", "bob", "alice"]
    finally:
        config.close_pool()


def test_authorizer_blocks_table_read(tmp_path: Path) -> None:
    """Authorizer callbacks should be able to deny reads from a table."""
    db_path = tmp_path / "authorizer.db"

    config = SqliteConfig(connection_config={"database": db_path})
    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE secrets (id INTEGER PRIMARY KEY, secret TEXT);
                INSERT INTO secrets (secret) VALUES ('hidden');
            """)
            session.commit()
    finally:
        config.close_pool()

    config = SqliteConfig(
        connection_config={"database": db_path}, driver_features={"authorizer_callback": _deny_secrets_read}
    )
    try:
        with config.provide_session() as session:
            with pytest.raises(SQLSpecError):
                session.select("SELECT * FROM secrets")
    finally:
        config.close_pool()


def test_progress_handler_fires() -> None:
    """Progress handlers should be invoked during long-running queries."""
    PROGRESS_CALLS.clear()
    config = SqliteConfig(driver_features={"progress_handler": _progress_handler, "progress_handler_interval": 10})

    try:
        with config.provide_session() as session:
            assert (
                session.select_value(
                    """
                    SELECT count(*)
                    FROM (
                        WITH RECURSIVE c(x) AS (
                            SELECT 1
                            UNION ALL
                            SELECT x + 1 FROM c WHERE x < 5000
                        )
                        SELECT x FROM c
                    )
                    """
                )
                == 5000
            )
            assert PROGRESS_CALLS
    finally:
        config.close_pool()


def test_trace_callback_records_statements() -> None:
    """Trace callbacks should record executed SQL statements."""
    TRACE_STATEMENTS.clear()
    config = SqliteConfig(driver_features={"trace_callback": _trace_callback})

    try:
        with config.provide_session() as session:
            assert session.select_value("SELECT 1") == 1
    finally:
        config.close_pool()

    assert any("SELECT 1" in statement for statement in TRACE_STATEMENTS)


def test_row_factory_row_literal_applies() -> None:
    """The row_factory literal should apply to raw connections without breaking SQLSpec rows."""
    config = SqliteConfig(driver_features={"row_factory": "row"})

    try:
        with config.provide_connection() as conn:
            row = conn.execute("SELECT 1 AS v").fetchone()
            assert row["v"] == 1

        with config.provide_session() as session:
            assert session.select_value("SELECT 1") == 1
    finally:
        config.close_pool()


def test_text_factory_applies() -> None:
    """The text_factory setting should affect raw connection rows."""
    config = SqliteConfig(driver_features={"text_factory": bytes})

    try:
        with config.provide_connection() as conn:
            row = conn.execute("SELECT 'hello'").fetchone()
            assert row[0] == b"hello"
    finally:
        config.close_pool()


def test_user_pragmas_override_optimizations(tmp_path: Path) -> None:
    """User PRAGMAs should win over built-in optimization PRAGMAs."""
    db_path = tmp_path / "pragma.db"
    config = SqliteConfig(connection_config={"database": db_path}, driver_features={"pragmas": {"synchronous": "FULL"}})

    try:
        with config.provide_connection() as conn:
            row = conn.execute("PRAGMA synchronous").fetchone()
            assert row[0] == 2
    finally:
        config.close_pool()


def test_extension_loading_attempts_paths(tmp_path: Path) -> None:
    """Configured extension paths should be applied when opening a connection."""
    config = SqliteConfig(driver_features={"extensions": [str(tmp_path / "missing_extension.so")]})

    with pytest.raises(sqlite3.OperationalError):
        with config.provide_session():
            pass

    config.close_pool()
