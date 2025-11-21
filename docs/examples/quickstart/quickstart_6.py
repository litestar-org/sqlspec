__all__ = ("test_quickstart_6",)


from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("duckdb")


def test_quickstart_6(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.duckdb import DuckDBConfig
    from sqlspec.adapters.sqlite import SqliteConfig

    app_db = tmp_path / "app.db"
    analytics_db = tmp_path / "analytics.duckdb"

    db_manager = SQLSpec()
    sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": app_db.name}))
    duckdb_db = db_manager.add_config(DuckDBConfig(pool_config={"database": analytics_db.name}))

    with db_manager.provide_session(sqlite_db) as sqlite_session:
        users = sqlite_session.select("SELECT 1")

    with db_manager.provide_session(duckdb_db) as duckdb_session:
        analytics = duckdb_session.select("SELECT 1")
    # end-example

    assert isinstance(users, list)
    assert isinstance(analytics, list)
