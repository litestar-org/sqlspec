__all__ = ("test_quickstart_8",)


def test_quickstart_8() -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    query = sql.select("id", "name", "email").from_("qs8_users").where("age > ?").order_by("name")

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        session.execute(
            """
            CREATE TABLE qs8_users (id INTEGER, name TEXT, email TEXT, age INTEGER)
            """
        )
        session.execute("INSERT INTO qs8_users VALUES (?, ?, ?, ?)", 1, "Alice", "alice@example.com", 30)
        results = session.select(query, 25)
        print(results)
    # end-example

    assert len(results) == 1
    assert results[0] == {"id": 1, "name": "Alice", "email": "alice@example.com"}
