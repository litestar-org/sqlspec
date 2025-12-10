from pathlib import Path

__all__ = ("test_example_1",)


def test_example_1(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example1.db"  # Database file path
    config = SqliteConfig(
        connection_config={
            "database": database.name,
            "timeout": 5.0,  # Lock timeout in seconds
            "check_same_thread": False,  # Allow multi-thread access
            "cached_statements": 100,  # Statement cache size
            "uri": False,  # Enable URI mode
        }
    )
    with db.provide_session(config) as session:
        create_table_query = """CREATE TABLE if not exists users(id int primary key,name text,email text, status text, created_at timestamp)"""
        _ = session.execute(create_table_query)
        # start-example
        # Build SELECT query
        query = (
            sql.select("id", "name", "email").from_("users").where("status = ?").order_by("created_at DESC").limit(10)
        )
        # Execute with session
        session.execute(query, "active")
        # end-example
