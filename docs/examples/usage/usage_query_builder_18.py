from pathlib import Path

__all__ = ("test_example_18", )


def test_example_18(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example18.db"
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
        # start-example
        # Drop table
        query = sql.drop_table("users")

        # Drop if exists
        query = sql.drop_table("users").if_exists()

        session.execute(query)
        # end-example
