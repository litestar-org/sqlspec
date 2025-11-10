__all__ = ("test_index_3",)


def test_index_3() -> None:
    # start-example
    from pathlib import Path

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.loader import SQLFileLoader

    queries_path = Path(__file__).parent.parent / "queries" / "users.sql"

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig())
    loader = SQLFileLoader()
    loader.load_sql(queries_path)
    get_user_by_id = loader.get_sql("get_user_by_id")

    with db_manager.provide_session(db) as session:
        session.execute(
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
        session.execute(
            """
            INSERT INTO users(id, username, email)
            VALUES (1, 'alice', 'alice@example.com'),
                   (2, 'bob', 'bob@example.com')
            """
        )
        user = session.select_one(get_user_by_id, user_id=2)
    # end-example

    assert user["username"] == "bob"
    assert user["email"] == "bob@example.com"
