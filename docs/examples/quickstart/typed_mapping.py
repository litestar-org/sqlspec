from dataclasses import dataclass

__all__ = ("test_typed_mapping",)


def test_typed_mapping() -> None:
    # start-example

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @dataclass
    class User:
        id: int
        name: str
        points: int

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        session.execute("create table if not exists users (id integer primary key, name text, points integer)")
        session.execute("insert into users (name, points) values (:name, :points)", name="Ada", points=42)

        result = session.execute("select id, name, points from users where name = :name", name="Ada")
        user = result.one(schema_type=User)
        print(f"Loaded {user.name} with {user.points} points")
    # end-example

    assert isinstance(user, User)
    assert user.name == "Ada"
    assert user.points == 42
