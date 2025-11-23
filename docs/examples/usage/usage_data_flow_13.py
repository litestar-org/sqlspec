"""Example 13: Schema Mapping."""

__all__ = ("test_schema_mapping",)


def test_schema_mapping() -> None:
    """Test mapping results to typed objects."""
    # start-example
    from pydantic import BaseModel
    from rich import print

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    class User(BaseModel):
        id: int
        name: str
        email: str
        is_active: bool | None = True

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        # Create test table
        session.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT, is_active INTEGER)")
        session.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 1)")

        # Execute query
        result = session.select("SELECT id, name, email, is_active FROM users", schema_type=User)
        print(len(result))

        # Or get single typed user
        single_result = session.select_one(
            "SELECT id, name, email, is_active FROM users WHERE id = ?", 1, schema_type=User
        )
        print(single_result)
    # end-example

    # Verify typed results
    assert len(result) == 1
    assert isinstance(single_result, User)
    assert single_result.id == 1
