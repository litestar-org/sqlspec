from pathlib import Path

__all__ = ("test_example_10",)


def test_example_10(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example10.db"  # Database file path
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,  # Lock timeout in seconds
            "check_same_thread": False,  # Allow multi-thread access
            "cached_statements": 100,  # Statement cache size
            "uri": False,  # Enable URI mode
        }
    )

    with db.provide_session(config) as session:
        # start-example
        # Multiple value sets
        query = sql.insert("users").columns("name", "email").values("?", "?").values("?", "?").values("?", "?")

        session.execute(query, "Alice", "alice@example.com", "Bob", "bob@example.com", "Charlie", "charlie@example.com")
        # end-example
    # end-example
