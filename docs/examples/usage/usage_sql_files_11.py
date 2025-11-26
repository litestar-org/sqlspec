from pathlib import Path

from docs.examples.usage.usage_sql_files_1 import create_loader
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_type_safe_query_execution",)


def test_type_safe_query_execution(tmp_path: Path) -> None:
    loader, _queries = create_loader(tmp_path)
    # start-example

    from pydantic import BaseModel

    class User(BaseModel):
        id: int
        username: str
        email: str

    # Load and execute with type safety
    query = loader.get_sql("get_user_by_id")

    spec = SQLSpec(loader=loader)
    config = SqliteConfig(pool_config={"database": ":memory:"})

    with spec.provide_session(config) as session:
        session.execute("""CREATE TABLE users ( id INTEGER PRIMARY KEY, username TEXT, email TEXT)""")
        session.execute(
            """ INSERT INTO users (id, username, email) VALUES (1, 'alice', 'alice@example.com'), (2, 'bob', 'bob@example.com');"""
        )
        user: User = session.select_one(query, user_id=1, schema_type=User)
    # end-example
    # Dummy asserts for doc example
    assert user.id == 1
    assert user.username == "alice"
    assert user.email == "alice@example.com"
    assert query is not None
