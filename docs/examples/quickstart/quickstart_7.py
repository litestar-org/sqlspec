__all__ = ("test_quickstart_7",)


def test_quickstart_7() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        session.begin()
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS qs7_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS qs7_orders (
                id INTEGER PRIMARY KEY,
                user_name TEXT NOT NULL
            )
            """
        )
        session.execute("DELETE FROM qs7_users")
        session.execute("DELETE FROM qs7_orders")
        session.execute("INSERT INTO qs7_users (name) VALUES (?)", "Alice")
        session.execute("INSERT INTO qs7_orders (user_name) VALUES (?)", "Alice")
        session.commit()

    with db_manager.provide_session(db) as session:
        session.begin()
        session.execute("INSERT INTO qs7_users (name) VALUES (?)", "Bob")
        session.rollback()

    with db_manager.provide_session(db) as session:
        alice = session.select_one_or_none("SELECT * FROM qs7_users WHERE name = ?", "Alice")
        bob = session.select_one_or_none("SELECT * FROM qs7_users WHERE name = ?", "Bob")
    # end-example

    assert alice is not None
    assert alice["name"] == "Alice"
    assert bob is None
