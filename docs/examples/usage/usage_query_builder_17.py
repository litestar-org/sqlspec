from pathlib import Path

def test_example_17(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example17.db"
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
        # Create table
        query = (
            sql.create_table("users")
            .column("id", "INTEGER PRIMARY KEY")
            .column("name", "TEXT NOT NULL")
            .column("email", "TEXT UNIQUE NOT NULL")
            .column("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        )

        session.execute(query)
        # end-example

