__all__ = ("test_quickstart_2",)


def test_quickstart_2() -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        _ = session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
            """
        )

        _ = session.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")

        _ = session.execute_many(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            [("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")],
        )

        users = session.select("SELECT * FROM users")
        print(f"All users: {users}")

        alice = session.select_one_or_none("SELECT * FROM users WHERE name = ?", "Alice")
        print(f"Alice: {alice}")

        count = session.select_value("SELECT COUNT(*) FROM users")
        print(f"Total users: {count}")
    # end-example

    assert len(users) == 3
    assert alice == {"id": 1, "name": "Alice", "email": "alice@example.com"}
    assert count == 3
