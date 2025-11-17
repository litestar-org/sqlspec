from pathlib import Path

__all__ = ("test_example_27",)


def test_example_27(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example27.db"
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
        session.execute("""CREATE TABLE if not exists users(id integer primary key autoincrement, name text)""")
        # start-example
        # Always use placeholders for user input
        search_term = "Alice"  # Example user input
        query = sql.select("*").from_("users").where("name LIKE ?")
        session.execute(query, f"%{search_term}%")
        # end-example
