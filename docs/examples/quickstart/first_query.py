__all__ = ("test_first_query",)


def test_first_query() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text)")
        session.execute("insert into users (name) values (?)", "Ada")
        result = session.execute("select id, name from users where name = ?", "Ada")
        print(result.one())
    # end-example

    assert result.one() == {"id": 1, "name": "Ada"}
