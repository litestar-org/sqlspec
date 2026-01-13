from __future__ import annotations

__all__ = ("test_named_queries",)


def test_named_queries() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))
    spec.add_named_sql("find_team", "select id, name from teams where name = :name")

    with spec.provide_session(config) as session:
        session.execute("create table teams (id integer primary key, name text)")
        session.execute("insert into teams (name) values ('Litestar'), ('SQLSpec')")
        result = session.execute(spec.get_sql("find_team"), {"name": "SQLSpec"})
        row = result.one()
    # end-example

    assert row["name"] == "SQLSpec"
