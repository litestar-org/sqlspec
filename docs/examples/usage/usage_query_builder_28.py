from pathlib import Path

__all__ = ("test_example_28", )


def test_example_28(tmp_path: Path) -> None:
    from pydantic import BaseModel

    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example28.db"
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
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text)"""
        )

        # start-example
        class User(BaseModel):
            id: int
            name: str
            email: str

        query = sql.select("id", "name", "email").from_("users")
        result = session.execute(query)
        result.all(schema_type=User)
        # end-example
