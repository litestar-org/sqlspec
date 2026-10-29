"""Decode query rows directly into typed msgspec.Struct instances."""

import msgspec

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("User", "main")


class User(msgspec.Struct, frozen=True, gc=False):
    """Typed user row returned from SQLite."""

    id: int
    name: str
    points: int


def main() -> User:
    """Insert a user record and decode it into a frozen msgspec.Struct."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, points INTEGER)")
        session.execute("INSERT INTO users (name, points) VALUES (:name, :points)", name="Ada", points=42)
        user = session.select_one("SELECT id, name, points FROM users WHERE name = :name", name="Ada", schema_type=User)

    assert isinstance(user, User)
    assert user.name == "Ada"
    assert user.points == 42
    return user


if __name__ == "__main__":
    loaded = main()
    print(f"Loaded {loaded.name} with {loaded.points} points")
