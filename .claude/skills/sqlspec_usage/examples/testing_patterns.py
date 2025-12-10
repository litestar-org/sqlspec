"""SQLSpec testing patterns with pytest."""

import tempfile
from collections.abc import AsyncGenerator, Generator
from contextlib import suppress

import pytest
from pydantic import BaseModel

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import NotFoundError

EXPECTED_BATCH_SIZE = 3


# Models
class User(BaseModel):
    id: int
    name: str
    email: str


# Session-scoped database fixtures
@pytest.fixture(scope="session")
def asyncpg_config() -> AsyncpgConfig:
    """PostgreSQL config using pytest-databases."""
    # pytest-databases automatically provides postgres_service fixture

    # This is managed by pytest-databases
    return AsyncpgConfig(connection_config={"dsn": "postgresql://postgres:password@localhost:5432/test"})


@pytest.fixture(scope="session")
async def asyncpg_db(asyncpg_config: AsyncpgConfig) -> AsyncGenerator[type[AsyncpgConfig], None]:
    spec = SQLSpec()
    db = spec.add_config(asyncpg_config)

    # Apply schema once per test session
    async with spec.provide_session(db) as session:
        await session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
        """)

    yield db

    # Cleanup
    await spec.close_all_pools()


@pytest.fixture
async def asyncpg_session(asyncpg_db: type[AsyncpgConfig]) -> AsyncGenerator[AsyncpgDriver, None]:
    spec = SQLSpec()

    async with spec.provide_session(asyncpg_db) as session:
        # Clean data before each test
        await session.execute("DELETE FROM users")
        yield session


# SQLite fixtures with proper test isolation
@pytest.fixture
def sqlite_config() -> Generator[type[SqliteConfig], None, None]:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        # IMPORTANT: Use temp file for isolation in parallel tests
        config = SqliteConfig(connection_config={"database": tmp.name})

        # Apply schema
        spec = SQLSpec()
        db = spec.add_config(config)

        with spec.provide_session(db) as session:
            session.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            """)

        yield db


# Test patterns


@pytest.mark.asyncio
async def test_create_user(asyncpg_session: AsyncpgDriver) -> None:
    """Test creating a user."""
    result = await asyncpg_session.execute(
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email", "Alice", "alice@example.com"
    )

    user = result.one(schema_type=User)
    assert user.name == "Alice"
    assert user.email == "alice@example.com"
    assert user.id > 0


@pytest.mark.asyncio
async def test_get_user_not_found(asyncpg_session: AsyncpgDriver) -> None:
    """Test getting non-existent user raises NotFoundError."""
    with pytest.raises(NotFoundError):
        await asyncpg_session.select_one("SELECT * FROM users WHERE id = $1", 999)


@pytest.mark.asyncio
async def test_get_user_or_none(asyncpg_session: AsyncpgDriver) -> None:
    """Test getting non-existent user returns None."""
    user = await asyncpg_session.select_one_or_none("SELECT * FROM users WHERE id = $1", 999)
    assert user is None


@pytest.mark.asyncio
async def test_list_users_empty(asyncpg_session: AsyncpgDriver) -> None:
    """Test listing users when table is empty."""
    result = await asyncpg_session.execute("SELECT * FROM users")
    users = result.all()
    assert users == []


@pytest.mark.asyncio
async def test_transaction_rollback(asyncpg_session: AsyncpgDriver) -> None:
    """Test transaction rollback on error."""
    # Start transaction
    await asyncpg_session.begin()
    await asyncpg_session.execute("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")

    # Simulate error - transaction will auto-rollback
    with suppress(Exception):
        await asyncpg_session.execute("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")

    # Verify nothing was inserted
    count = await asyncpg_session.select_value("SELECT COUNT(*) FROM users")
    assert count == 0


@pytest.mark.asyncio
async def test_batch_insert(asyncpg_session: AsyncpgDriver) -> None:
    """Test batch insert with execute_many."""
    users = [("Alice", "alice@example.com"), ("Bob", "bob@example.com"), ("Charlie", "charlie@example.com")]

    result = await asyncpg_session.execute_many("INSERT INTO users (name, email) VALUES ($1, $2)", users)

    assert result.rows_affected == EXPECTED_BATCH_SIZE

    # Verify all inserted
    count = await asyncpg_session.select_value("SELECT COUNT(*) FROM users")
    assert count == EXPECTED_BATCH_SIZE


@pytest.mark.asyncio
async def test_type_safe_mapping(asyncpg_session: AsyncpgDriver) -> None:
    """Test type-safe schema mapping with Pydantic."""
    await asyncpg_session.execute("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")

    users = await asyncpg_session.execute("SELECT id, name, email FROM users", schema_type=User).all()

    assert len(users) == 1
    assert isinstance(users[0], User)
    assert users[0].name == "Alice"


# SQLite-specific tests (test isolation with temp files)


def test_sqlite_isolation() -> None:
    """Test SQLite test isolation with temp files."""
    # Each test gets own temp file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        spec = SQLSpec()
        db = spec.add_config(config)

        with spec.provide_session(db) as session:
            session.execute("""
                CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)
            """)
            session.execute("INSERT INTO test (value) VALUES (?)", "test")

            result = session.select_value("SELECT COUNT(*) FROM test")
            assert result == 1


# Parametrized tests for multiple adapters


@pytest.mark.parametrize(
    ("adapter_name", "config"),
    [
        ("asyncpg", AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}))
        # Add more adapters as needed
    ],
)
@pytest.mark.asyncio
async def test_basic_query(adapter_name: str, config: AsyncpgConfig) -> None:
    """Test basic query execution across adapters."""
    spec = SQLSpec()
    db = spec.add_config(config)

    async with spec.provide_session(db) as session:
        result = await session.select_value("SELECT 1")
        assert result == 1

    await spec.close_all_pools()


# Fixture for cleaning up between tests


@pytest.fixture(autouse=True)
async def cleanup_between_tests(asyncpg_session: AsyncpgDriver) -> AsyncGenerator[None, None]:
    """Auto-cleanup between tests."""
    yield
    await asyncpg_session.execute("DELETE FROM users")


# Mark for parallel execution

pytestmark = pytest.mark.asyncio  # All tests in this module are async


if __name__ == "__main__":
    # Run tests with parallel execution
    # pytest -n auto --dist=loadgroup tests/
    pytest.main([__file__, "-v", "-n", "auto", "--dist=loadgroup"])
