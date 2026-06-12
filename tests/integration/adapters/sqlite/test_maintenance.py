"""Integration tests for SQLite runtime features and maintenance behavior."""

import sqlite3
import sys
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SQLSpecError

pytestmark = pytest.mark.xdist_group("sqlite")

TRACE_STATEMENTS: list[str] = []
PROGRESS_CALLS: list[int] = []
BACKUP_PROGRESS_CALLS: list[tuple[int, int, int]] = []


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


def _deny_secrets_read(action: int, arg1: str | None, arg2: str | None, db_name: str | None, trigger_name: str | None) -> int:
    _ = arg2, db_name, trigger_name
    if action == sqlite3.SQLITE_READ and arg1 == "secrets":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _progress_handler() -> int:
    PROGRESS_CALLS.append(1)
    return 0


def _trace_callback(statement: str) -> None:
    TRACE_STATEMENTS.append(statement)


def _backup_progress(status: int, remaining: int, total: int) -> None:
    BACKUP_PROGRESS_CALLS.append((status, remaining, total))


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

    config = SqliteConfig(connection_config={"database": db_path}, driver_features={"authorizer_callback": _deny_secrets_read})
    try:
        with config.provide_session() as session:
            with pytest.raises(SQLSpecError):
                session.select("SELECT * FROM secrets")
    finally:
        config.close_pool()


def test_progress_handler_fires() -> None:
    """Progress handlers should be invoked during long-running queries."""
    PROGRESS_CALLS.clear()
    config = SqliteConfig(
        driver_features={"progress_handler": _progress_handler, "progress_handler_interval": 10}
    )

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


def test_backup_to_file_path(tmp_path: Path) -> None:
    """The driver should back up a populated database to a file path."""
    source_path = tmp_path / "source.db"
    backup_path = tmp_path / "backup.db"
    config = SqliteConfig(connection_config={"database": source_path})

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
                INSERT INTO items (name) VALUES ('beta');
            """)
            session.commit()
            session.backup(backup_path)
    finally:
        config.close_pool()

    with sqlite3.connect(backup_path) as conn:
        rows = conn.execute("SELECT name FROM items ORDER BY id").fetchall()
    assert rows == [("alpha",), ("beta",)]


def test_backup_to_connection_with_progress(tmp_path: Path) -> None:
    """The driver should back up into another connection and call progress callbacks."""
    BACKUP_PROGRESS_CALLS.clear()
    source_path = tmp_path / "progress-source.db"
    config = SqliteConfig(connection_config={"database": source_path})
    target = sqlite3.connect(":memory:")

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
            """)
            session.commit()
            session.backup(target, progress=_backup_progress)

        assert target.execute("SELECT name FROM items").fetchone() == ("alpha",)
        assert BACKUP_PROGRESS_CALLS
    finally:
        target.close()
        config.close_pool()


def test_iterdump_roundtrip() -> None:
    """Database dumps should recreate schema and data on a fresh connection."""
    config = SqliteConfig()

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
                INSERT INTO items (name) VALUES ('beta');
            """)
            session.commit()
            dump_sql = "\n".join(session.iterdump())
    finally:
        config.close_pool()

    assert "CREATE TABLE items" in dump_sql
    with sqlite3.connect(":memory:") as conn:
        conn.executescript(dump_sql)
        assert conn.execute("SELECT count(*) FROM items").fetchone() == (2,)


@pytest.mark.skipif(sys.version_info < (3, 11), reason="sqlite3 serialize requires Python 3.11+")
def test_serialize_deserialize_roundtrip() -> None:
    """Serialized bytes should load into a second SQLite session."""
    source_config = SqliteConfig()
    target_config = SqliteConfig()

    try:
        with source_config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
                INSERT INTO items (name) VALUES ('beta');
            """)
            session.commit()
            payload = session.serialize()

        with target_config.provide_session() as session:
            session.deserialize(payload)
            rows = session.select("SELECT name FROM items ORDER BY id")
            assert [row["name"] for row in rows] == ["alpha", "beta"]
    finally:
        source_config.close_pool()
        target_config.close_pool()


@pytest.mark.skipif(sys.version_info < (3, 11), reason="sqlite3 blobopen requires Python 3.11+")
def test_blob_open_read_write() -> None:
    """Incremental blob I/O should read and write an existing blob cell."""
    config = SqliteConfig()

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE blobs (data BLOB);
                INSERT INTO blobs (data) VALUES (zeroblob(8));
            """)
            rowid = session.select_value("SELECT last_insert_rowid()")
            with session.blob_open("blobs", "data", int(rowid)) as blob:
                blob.write(b"abcd1234")

            assert session.select_value("SELECT data FROM blobs WHERE rowid = :rowid", rowid=rowid) == b"abcd1234"
    finally:
        config.close_pool()


def test_optimize_executes() -> None:
    """PRAGMA optimize should execute on a populated database."""
    config = SqliteConfig()

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
            """)
            session.commit()
            session.optimize()
    finally:
        config.close_pool()


def test_wal_checkpoint_truncate_on_file_database(tmp_path: Path) -> None:
    """WAL checkpoint TRUNCATE should run against a file-backed database."""
    db_path = tmp_path / "checkpoint.db"
    config = SqliteConfig(connection_config={"database": db_path})

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO items (name) VALUES ('alpha');
                INSERT INTO items (name) VALUES ('beta');
            """)
            session.commit()
            busy, _log_pages, _checkpointed_pages = session.wal_checkpoint("TRUNCATE")
            assert busy == 0
    finally:
        config.close_pool()


def test_integrity_check_ok() -> None:
    """Clean databases should report ok from PRAGMA integrity_check."""
    config = SqliteConfig()

    try:
        with config.provide_session() as session:
            session.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            session.commit()
            assert session.integrity_check() == ["ok"]
    finally:
        config.close_pool()
