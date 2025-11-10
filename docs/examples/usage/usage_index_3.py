from pathlib import Path

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.loader import SQLFileLoader

__all__ = ("test_index_3",)


QUERIES_PATH = Path(__file__).parent.parent / "queries" / "users.sql"


def test_index_3() -> None:
    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig())
    loader = SQLFileLoader()
    loader.load_sql(QUERIES_PATH)
    get_user_by_id = loader.get_sql("get_user_by_id")

    with db_manager.provide_session(db) as session:
        _ = session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ = session.execute(
            """
            INSERT INTO users(id, username, email)
            VALUES (1, 'alice', 'alice@example.com'),
                   (2, 'bob', 'bob@example.com')
            """
        )
        user = session.select_one(get_user_by_id, user_id=2)

    assert user["username"] == "bob"
    assert user["email"] == "bob@example.com"
