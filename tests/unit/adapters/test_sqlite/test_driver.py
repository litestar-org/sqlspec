import inspect

from sqlspec.adapters.sqlite.driver import SqliteDriver


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
