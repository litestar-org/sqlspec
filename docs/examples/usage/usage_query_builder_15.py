from pathlib import Path

def test_example_15(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example15.db"
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
        session.execute("""CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text)""")
        # start-example
        # Delete with WHERE
        query = sql.delete().from_("users").where("id = ?")
        # SQL: DELETE FROM users WHERE id = ?

        result = session.execute(query, 1)
        # print(f"Deleted {result.rows_affected} rows")
        # end-example

