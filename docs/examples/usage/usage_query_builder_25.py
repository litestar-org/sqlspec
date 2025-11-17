from pathlib import Path

def test_example_25(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example25.db"
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
        # Prefer this for simple, static queries:
        result = session.execute("SELECT * FROM users WHERE id = ?", 1)

        # Over this:
        query = sql.select("*").from_("users").where("id = ?")
        result = session.execute(query, 1)
        # end-example

