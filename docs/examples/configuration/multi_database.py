from __future__ import annotations

from pathlib import Path

__all__ = ("test_multi_database",)


def test_multi_database(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.observability import ObservabilityConfig, SamplingConfig

    spec = SQLSpec(observability_config=ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.1), print_sql=True))

    # Primary database
    primary = spec.add_config(SqliteConfig(connection_config={"database": str(tmp_path / "primary.db")}))

    # Analytics database
    analytics = spec.add_config(SqliteConfig(connection_config={"database": str(tmp_path / "analytics.db")}))

    # Load SQL files from multiple directories
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    (sql_dir / "queries.sql").write_text("-- name: list_users\nselect id, name from users order by id;\n")
    spec.load_sql_files(sql_dir)

    # Use each config independently
    with spec.provide_session(primary) as session:
        session.execute("create table users (id integer primary key, name text)")
        session.execute("insert into users (name) values ('Alice')")
        result = session.execute(spec.get_sql("list_users"))
        print(result.all())

    with spec.provide_session(analytics) as session:
        session.execute("create table events (id integer primary key, event text)")
        session.execute("insert into events (event) values ('page_view')")
        result = session.execute("select * from events")
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "event": "page_view"}]
