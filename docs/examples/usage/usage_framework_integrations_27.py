# start-example
from collections.abc import Generator

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver

__all__ = ("test_create_user", "test_db")


@pytest.fixture
def test_db() -> Generator[SqliteDriver]:
    spec = SQLSpec()
    config = SqliteConfig(pool_config={"database": ":memory:"})
    spec.add_config(config)

    with spec.provide_session(config) as session:
        # Set up test schema
        session.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        yield session


def test_create_user(test_db: SqliteDriver) -> None:
    result = test_db.execute("INSERT INTO users (name) VALUES ($1) RETURNING id", "Test User")
    assert result.scalar() == 1


# end-example
