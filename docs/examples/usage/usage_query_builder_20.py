from pathlib import Path

def test_example_20(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example20.db"
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
        session.execute("""CREATE TABLE if not exists employees(id integer primary key autoincrement, name text, salary real, department text)""")
        # start-example
        query = sql.select(
            "id",
            "name",
            "salary",
            "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank"
        ).from_("employees")
        result = session.execute(query)
        # end-example

