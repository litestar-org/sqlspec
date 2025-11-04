"""Example 13: Schema Mapping."""


def test_schema_mapping() -> None:
    """Test mapping results to typed objects."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    # start-example
    from pydantic import BaseModel
    from typing import Optional

    class User(BaseModel):
        id: int
        name: str
        email: str
        is_active: Optional[bool] = True

    spec = SQLSpec()
    config = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with spec.provide_session(config) as session:
        # Create test table
        session.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT, is_active INTEGER)")
        session.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 1)")

        # Execute query
        result = session.execute("SELECT id, name, email, is_active FROM users")

        # Map results to typed User instances
        users: list[User] = result.all(schema_type=User)

        # Or get single typed user
        single_result = session.execute("SELECT id, name, email, is_active FROM users WHERE id = ?", 1)
        user: User = single_result.one(schema_type=User)  # Type-safe!
    # end-example

    # Verify typed results
    assert len(users) == 1
    assert isinstance(user, User)
    assert user.id == 1

