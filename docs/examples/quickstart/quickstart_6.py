__all__ = ("test_quickstart_6",)


def test_quickstart_6() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": "app.db"}))
    duckdb_db = db_manager.add_config(DuckDBConfig(pool_config={"database": "analytics.duckdb"}))

    with db_manager.provide_session(sqlite_db) as sqlite_session:
        users = sqlite_session.select("SELECT 1")

    with db_manager.provide_session(duckdb_db) as duckdb_session:
        analytics = duckdb_session.select("SELECT 1")
    # end-example

    assert isinstance(users, list)
    assert isinstance(analytics, list)
