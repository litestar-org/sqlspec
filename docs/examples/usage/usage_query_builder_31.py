from pathlib import Path

def test_example_31(tmp_path: Path) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example31.db"
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
        session.execute("""CREATE TABLE if not exists events(id integer primary key autoincrement, data text)""")
        # start-example
        # PostgreSQL JSON operators (use raw SQL)
        json_filter = '{"name": "Alice"}'
        session.execute("SELECT data->>'name' FROM events WHERE data @> ?", json_filter)
        # end-example

