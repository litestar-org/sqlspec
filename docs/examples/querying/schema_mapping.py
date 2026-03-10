from __future__ import annotations

from pathlib import Path

__all__ = ("test_schema_mapping",)


def test_schema_mapping(tmp_path: Path) -> None:
    # start-example
    from dataclasses import dataclass

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @dataclass
    class User:
        id: int
        name: str

    db_path = tmp_path / "schema.db"
    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": str(db_path)}))

    with spec.provide_session(config) as session:
        session.execute("create table users (id integer primary key, name text)")
        session.execute("insert into users (name) values ('Alice'), ('Bob')")

        # select returns list of dicts by default
        rows = session.select("select id, name from users order by id")
        print(rows)  # [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        # Use schema_type to map rows to dataclass instances
        users = session.select("select id, name from users order by id", schema_type=User)
        print(users)  # [User(id=1, name='Alice'), User(id=2, name='Bob')]

        # select_one with schema_type
        user = session.select_one(
            "select id, name from users where name = ?", "Alice", schema_type=User
        )
        print(user)  # User(id=1, name='Alice')

        # select_one_or_none returns None if no match
        maybe_user = session.select_one_or_none(
            "select id, name from users where name = ?", "Nobody", schema_type=User
        )
        print(maybe_user)  # None
    # end-example

    assert isinstance(users[0], User)
    assert user.name == "Alice"
    assert maybe_user is None
