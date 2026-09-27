import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.adapters.sqlite import SqliteDriver
from sqlspec.adapters.sqlite.core import SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT, default_statement_config


def test_rowid_eligibility_falls_back_when_table_list_is_unavailable() -> None:
    from sqlspec.adapters.sqlite.core import _target_supports_rowid

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE rowid_target (id INTEGER PRIMARY KEY)")
    connection.execute("CREATE TABLE without_rowid_target (id TEXT PRIMARY KEY) WITHOUT ROWID")
    connection.execute("CREATE TABLE shadowed_without_rowid (rowid TEXT PRIMARY KEY) WITHOUT ROWID")

    class LegacyConnection:
        def execute(self, sql: str, parameters: object = ()) -> sqlite3.Cursor:
            if sql == "PRAGMA table_list":
                raise sqlite3.OperationalError
            return connection.execute(sql, cast("Any", parameters))

    legacy_connection = LegacyConnection()
    try:
        assert _target_supports_rowid(legacy_connection, (None, "rowid_target"))
        assert not _target_supports_rowid(legacy_connection, (None, "without_rowid_target"))
        assert not _target_supports_rowid(legacy_connection, (None, "shadowed_without_rowid"))
    finally:
        connection.close()


def test_pool_no_duplicate_typedef_sqlite_connection_params_not_exported_from_pool() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert not hasattr(pool_mod, "SqliteConnectionParams")


def test_pool_no_duplicate_typedef_sqlite_connection_pool_still_importable() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    assert SqliteConnectionPool is not None


def test_pool_no_duplicate_typedef_pool_module_all_unchanged() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert pool_mod.__all__ == ("SqliteConnectionPool",)


def test_pool_no_duplicate_typedef_canonical_typedef_still_importable_from_config() -> None:
    from sqlspec.adapters.sqlite.config import SqliteConnectionParams

    assert hasattr(SqliteConnectionParams, "__annotations__") or hasattr(SqliteConnectionParams, "__required_keys__")


def test_pool_no_duplicate_typedef_canonical_typedef_importable_from_package() -> None:
    from sqlspec.adapters.sqlite import SqliteConnectionParams

    assert SqliteConnectionParams is not None


def test_pool_no_duplicate_typedef_pool_creates_connection_after_cleanup() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"})
    conn = pool.acquire()
    cursor = conn.execute("SELECT 1 AS n")
    row = cursor.fetchone()
    pool.close()
    assert row is not None
    assert row[0] == 1


@pytest.mark.skipif(not SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT, reason="sqlite3 autocommit requires Python 3.12+")
def test_commit_is_durable_under_sqlite_autocommit_mode(tmp_path: Path) -> None:
    """A transaction opened in autocommit mode must be committable."""
    database = str(tmp_path / "autocommit.sqlite")
    connection = sqlite3.connect(database, **cast("Any", {"autocommit": True}))
    driver = SqliteDriver(connection=cast("Any", connection))
    try:
        connection.execute("CREATE TABLE items (id INTEGER)")
        driver.begin()
        connection.execute("INSERT INTO items (id) VALUES (1)")
        driver.commit()

        observer = sqlite3.connect(database)
        try:
            assert observer.execute("SELECT id FROM items").fetchall() == [(1,)]
        finally:
            observer.close()
    finally:
        connection.close()


@pytest.mark.skipif(not SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT, reason="sqlite3 autocommit requires Python 3.12+")
def test_rollback_discards_work_under_sqlite_autocommit_mode(tmp_path: Path) -> None:
    """A rollback in autocommit mode must actually discard the transaction."""
    database = str(tmp_path / "autocommit_rollback.sqlite")
    connection = sqlite3.connect(database, **cast("Any", {"autocommit": True}))
    driver = SqliteDriver(connection=cast("Any", connection))
    try:
        connection.execute("CREATE TABLE items (id INTEGER)")
        driver.begin()
        connection.execute("INSERT INTO items (id) VALUES (1)")
        driver.rollback()

        assert connection.execute("SELECT id FROM items").fetchall() == []
    finally:
        connection.close()


class _AutocommitConnection:
    """Mimics a Python 3.12+ sqlite3 connection in autocommit mode."""

    def __init__(self, in_transaction: bool = True) -> None:
        self.autocommit = True
        self.in_transaction = in_transaction
        self.statements: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def execute(self, sql: str, parameters: object = ()) -> None:
        _ = parameters
        self.statements.append(sql)

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


@pytest.mark.parametrize(
    ("method", "statement"), [("commit", "COMMIT"), ("rollback", "ROLLBACK")], ids=["commit", "rollback"]
)
def test_autocommit_mode_ends_transactions_with_an_explicit_statement(
    method: str, statement: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In autocommit mode the no-op driver methods must be replaced by real SQL."""
    monkeypatch.setattr("sqlspec.adapters.sqlite.driver.SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT", True)
    connection = _AutocommitConnection()
    driver = SqliteDriver(connection=cast("Any", connection))

    getattr(driver, method)()

    assert connection.statements == [statement]
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 0


@pytest.mark.parametrize("method", ["commit", "rollback"])
def test_autocommit_mode_skips_the_statement_without_an_open_transaction(
    method: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ending a transaction that was never opened must not raise."""
    monkeypatch.setattr("sqlspec.adapters.sqlite.driver.SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT", True)
    connection = _AutocommitConnection(in_transaction=False)
    driver = SqliteDriver(connection=cast("Any", connection))

    getattr(driver, method)()

    assert connection.statements == []


@pytest.mark.parametrize(
    ("method", "attribute"), [("commit", "commit_calls"), ("rollback", "rollback_calls")], ids=["commit", "rollback"]
)
def test_legacy_mode_still_uses_the_connection_methods(
    method: str, attribute: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Outside autocommit mode the driver must keep using the DB-API methods."""
    monkeypatch.setattr("sqlspec.adapters.sqlite.driver.SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT", False)
    connection = _AutocommitConnection()
    driver = SqliteDriver(connection=cast("Any", connection))

    getattr(driver, method)()

    assert connection.statements == []
    assert getattr(connection, attribute) == 1


class _TrackingCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []
        self.description = [("id",)] if rows is not None else None
        self.rowcount = len(self._rows) if self._rows else 2
        self.lastrowid = 1
        self.closed = False

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        self.closed = True


class _TrackingConnection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.cursors: list[_TrackingCursor] = []

    def execute(self, sql: str, parameters: object = ()) -> _TrackingCursor:
        _ = (sql, parameters)
        cursor = _TrackingCursor(rows=[(1,)])
        self.cursors.append(cursor)
        return cursor

    def executemany(self, sql: str, parameters: object) -> _TrackingCursor:
        _ = (sql, parameters)
        cursor = _TrackingCursor()
        self.cursors.append(cursor)
        return cursor


def test_execute_many_thin_path_closes_cursor() -> None:
    """SqliteDriver.execute_many thin path must close its cursor in finally."""
    connection = _TrackingConnection()
    driver = SqliteDriver(connection=cast("Any", connection))

    result = driver.execute_many("INSERT INTO items (name) VALUES (?)", [("a",), ("b",)])

    assert result.rows_affected == 2
    assert len(connection.cursors) == 1
    assert connection.cursors[0].closed is True


def test_execute_cache_hit_closes_cursor() -> None:
    """SqliteDriver._execute_cache_hit must close its cursor on cached execution."""
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    connection.execute("INSERT INTO items (name) VALUES ('alpha')")
    driver = SqliteDriver(connection=connection)
    try:
        first = driver.execute("SELECT id, name FROM items WHERE id = ?", (1,))
        second = driver.execute("SELECT id, name FROM items WHERE id = ?", (1,))
        assert first.get_data() == [{"id": 1, "name": "alpha"}]
        assert second.get_data() == [{"id": 1, "name": "alpha"}]
    finally:
        connection.close()


def test_execute_many_thin_path_checks_all_rows_beyond_sample_threshold() -> None:
    """_thin_path_parameters_are_eligible must inspect every row even in large batches."""
    rows: list[tuple[Any, ...]] = [(i,) for i in range(120)]
    rows[57] = (defaultdict(int, a=1),)

    assert (
        SqliteDriver._thin_path_parameters_are_eligible(
            rows, default_statement_config.parameter_config.type_coercion_map
        )
        is False
    )


def test_arrow_ingest_rolls_back_when_a_later_batch_conversion_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    import pyarrow as pa

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE target (value INTEGER)")
    driver = SqliteDriver(
        connection=connection, driver_features={"storage_capabilities": {"arrow_import_enabled": True}}
    )
    batches_prepared = 0

    def prepare(parameters: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal batches_prepared
        batches_prepared += 1
        if batches_prepared == 2:
            raise ValueError("conversion failed")
        return parameters

    monkeypatch.setattr(SqliteDriver, "_arrow_rows_need_preparation", lambda *args: True)
    monkeypatch.setattr(
        SqliteDriver, "prepare_driver_parameters", lambda self, *args, **kwargs: prepare(*args, **kwargs)
    )
    try:
        with pytest.raises(ValueError, match="conversion failed"):
            driver.load_from_arrow("target", pa.table({"value": list(range(10001))}))
        assert connection.in_transaction is False
        assert connection.execute("SELECT COUNT(*) FROM target").fetchone() == (0,)
    finally:
        connection.close()
