__all__ = ("test_basic_connection",)


def test_basic_connection() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists notes (id integer primary key, body text)")
        session.execute("insert into notes (body) values (?)", "Hello, SQLSpec!")
        result = session.execute("select id, body from notes")
        print(result.all())
    # end-example

    assert result.all() == [{"id": 1, "body": "Hello, SQLSpec!"}]
