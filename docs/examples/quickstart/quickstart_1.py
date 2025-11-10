__all__ = ("test_quickstart_1",)


def test_quickstart_1() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        result = session.execute("SELECT 'Hello, SQLSpec!' as message")
        print(result.get_first())
    # end-example

    assert result.get_first() == {"message": "Hello, SQLSpec!"}
