from pathlib import Path

__all__ = ("test_example_30",)


def test_example_30(tmp_path: Path) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example30.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, region text, created_at timestamp)"""
        )
        # start-example
        # This is easier to read as raw SQL:
        session.execute("""
            WITH ranked_users AS (
                SELECT id, name,
                       ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at DESC) as rn
                FROM users
            )
            SELECT * FROM ranked_users WHERE rn <= 5
        """)
        # end-example
