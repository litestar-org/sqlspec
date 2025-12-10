"""Schema mapping example with dataclasses."""

from dataclasses import dataclass

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("User", "test_example_28_schema_mapping")


@dataclass
class User:
    id: int
    name: str


def test_example_28_schema_mapping() -> None:
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(db) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute_many("INSERT INTO users (id, name) VALUES (?, ?)", [(1, "River"), (2, "Kay")])

        # start-example
        result = session.execute("SELECT id, name FROM users ORDER BY id")
        users = result.all(schema_type=User)
        assert users == [User(id=1, name="River"), User(id=2, name="Kay")]
        # end-example
