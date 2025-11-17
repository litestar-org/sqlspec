from pathlib import Path

def test_example_19(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example19.db"
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
        session.execute("""CREATE TABLE if not exists users(id integer primary key autoincrement, email text)""")
        # start-example
        # Create index
        query = (
            sql.create_index("idx_users_email")
            .table("users")
            .columns("email")
        )

        # Unique index
        query = (
            sql.create_index("idx_users_email")
            .table("users")
            .columns("email")
            .unique()
        )

        session.execute(query)
        # end-example

