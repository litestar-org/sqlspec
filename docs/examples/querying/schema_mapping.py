"""Map query results automatically into typed msgspec.Struct models with schema_type."""

import msgspec

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("User", "main")


class User(msgspec.Struct, frozen=True, gc=False):
    """Typed user row projection."""

    id: int
    name: str


def main() -> tuple[list[User], User, User | None]:
    """Fetch dictionary rows and typed User structs via select, select_one, and select_one_or_none."""
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        session.execute("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")

        raw_rows = session.select("SELECT id, name FROM users ORDER BY id")
        assert raw_rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        users = session.select("SELECT id, name FROM users ORDER BY id", schema_type=User)
        alice = session.select_one("SELECT id, name FROM users WHERE name = ?", "Alice", schema_type=User)
        nobody = session.select_one_or_none("SELECT id, name FROM users WHERE name = ?", "Nobody", schema_type=User)

    assert isinstance(users[0], User)
    assert alice.name == "Alice"
    assert nobody is None
    return users, alice, nobody


if __name__ == "__main__":
    print(main())
