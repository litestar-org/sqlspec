import inspect
import sqlite3
from typing import Any, cast

import pytest

from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.core import SQL


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


def test_driver_cache_execute_cache_hit_has_no_unreachable_returns_rows_guard() -> None:
    source = inspect.getsource(SqliteDriver._execute_cache_hit)
    assert "if returns_rows:" not in source


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


def test_dispatch_execute_detects_record_row_format(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Row:
        def __init__(self, data: dict[str, object]) -> None:
            self._data = data

        def keys(self) -> object:
            return self._data.keys()

        def __getitem__(self, key: str) -> object:
            return self._data[key]

    class _Cursor:
        def __init__(self) -> None:
            self.description = [("id",), ("name",)]
            self.executed: list[tuple[str, object]] = []

        def execute(self, sql: str, parameters: object) -> None:
            self.executed.append((sql, parameters))

        def fetchall(self) -> list[_Row]:
            return [_Row({"id": 1, "name": "alice"})]

    monkeypatch.setattr(SqliteDriver, "_compiled_sql", lambda *_args, **_kwargs: ("SELECT id, name FROM users", []))

    driver = SqliteDriver(connection=cast("Any", object()))
    statement = SQL("SELECT id, name FROM users")
    cursor = _Cursor()

    result = driver.dispatch_execute(cursor, statement)

    assert result.row_format == "record"
    selected_data = result.selected_data
    assert selected_data is not None
    assert dict(selected_data[0]) == {"id": 1, "name": "alice"}
