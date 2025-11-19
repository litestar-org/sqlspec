"""Script execution example."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_example_20_script_execution",)


def test_example_20_script_execution() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        # start-example
        session.execute_script(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                title TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE INDEX idx_posts_user_id ON posts(user_id);
            """
        )
        # end-example

        indices = session.execute("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'posts'")
        assert indices.data
        assert indices.data[0]["name"] == "idx_posts_user_id"
