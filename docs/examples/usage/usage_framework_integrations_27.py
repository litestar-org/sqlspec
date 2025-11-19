# start-example
import pytest

from sqlspec.adapters.sqlite import SqliteConfig

__all__ = ("test_create_user", "test_db" )


@pytest.fixture
async def test_db():
    spec = SQLSpec()
    db = spec.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    async with spec.provide_session(db) as session:
        # Set up test schema
        await session.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        yield session


async def test_create_user(test_db) -> None:
    result = await test_db.execute("INSERT INTO users (name) VALUES ($1) RETURNING id", "Test User")
    assert result.scalar() == 1


# end-example
