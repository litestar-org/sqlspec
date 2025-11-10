from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig


def test_index_1() -> None:
    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig())

    with db_manager.provide_session(db) as session:
        _ = session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
        _ = session.execute("INSERT INTO users VALUES (?, ?)", 1, "alice")

    with db_manager.provide_session(db) as session:
        user = session.select_one("SELECT * FROM users WHERE id = ?", 1)

    assert user == {"id": 1, "name": "alice"}
