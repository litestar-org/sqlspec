"""Regression tests for the sqlite pool module public surface."""


def test_sqlite_connection_params_not_exported_from_pool() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert not hasattr(pool_mod, "SqliteConnectionParams")


def test_sqlite_connection_pool_still_importable() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    assert SqliteConnectionPool is not None


def test_pool_module_all_unchanged() -> None:
    import sqlspec.adapters.sqlite.pool as pool_mod

    assert pool_mod.__all__ == ("SqliteConnectionPool",)


def test_canonical_typedef_still_importable_from_config() -> None:
    from sqlspec.adapters.sqlite.config import SqliteConnectionParams

    assert hasattr(SqliteConnectionParams, "__annotations__") or hasattr(SqliteConnectionParams, "__required_keys__")


def test_canonical_typedef_importable_from_package() -> None:
    from sqlspec.adapters.sqlite import SqliteConnectionParams

    assert SqliteConnectionParams is not None


def test_pool_creates_connection_after_cleanup() -> None:
    from sqlspec.adapters.sqlite.pool import SqliteConnectionPool

    pool = SqliteConnectionPool(connection_parameters={"database": ":memory:"})
    conn = pool.acquire()
    cursor = conn.execute("SELECT 1 AS n")
    row = cursor.fetchone()
    pool.close()

    assert row is not None
    assert row[0] == 1
