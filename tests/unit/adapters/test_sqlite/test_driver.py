import sqlite3
from typing import Any, cast


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
