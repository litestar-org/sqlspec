__all__ = ("test_quickstart_3",)


def test_quickstart_3() -> None:
    # start-example
    from pydantic import BaseModel

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    class User(BaseModel):
        id: int
        name: str
        email: str

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        _ = session.execute(
            """
            CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
            """
        )
        _ = session.execute("INSERT INTO users VALUES (?, ?, ?)", 1, "Alice", "alice@example.com")

        user = session.select_one("SELECT * FROM users WHERE id = ?", 1, schema_type=User)
        print(f"User: {user.name} ({user.email})")

        all_users = session.select("SELECT * FROM users", schema_type=User)
        for typed_user in all_users:
            print(f"User: {typed_user.name}")
    # end-example

    assert user == User(id=1, name="Alice", email="alice@example.com")
    assert len(all_users) == 1
    assert isinstance(all_users[0], User)
