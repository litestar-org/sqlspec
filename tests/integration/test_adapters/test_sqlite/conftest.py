"""SQLite-specific fixtures for integration tests."""

from collections.abc import Generator
from typing import Any, cast

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver


@pytest.fixture
def sqlite_session() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite session with test table for integration tests.

    This fixture creates an in-memory SQLite database with a test table
    and ensures proper cleanup after test completion.
    """
    config = SqliteConfig(pool_config={"database": ":memory:"})

    try:
        with config.provide_session() as session:
            # Create test table
            session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Commit DDL to prevent table locking issues in subsequent operations
            session.commit()

            try:
                yield session
            finally:
                # Ensure any pending transactions are committed before test ends
                try:
                    session.commit()
                except Exception:
                    # If commit fails, try rollback to clean up transaction state
                    try:
                        session.rollback()
                    except Exception:
                        pass
            # Cleanup is automatic with in-memory database
    finally:
        # Ensure pool is closed properly to avoid threading issues during test shutdown
        config.close_pool()


@pytest.fixture
def sqlite_driver() -> Generator[SqliteDriver, None, None]:
    """Create a SQLite driver with a test table for direct driver testing.

    This fixture creates a raw driver instance for testing driver-specific
    functionality like query mixins.
    """
    import sqlite3

    # Create in-memory database
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    driver = SqliteDriver(conn)

    # Create test table
    driver.execute_script("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        );

        INSERT INTO users (name, email, age) VALUES
            ('John Doe', 'john@example.com', 30),
            ('Jane Smith', 'jane@example.com', 25),
            ('Bob Johnson', 'bob@example.com', 35),
            ('Alice Brown', 'alice@example.com', 28),
            ('Charlie Davis', 'charlie@example.com', 32);
    """)

    yield driver

    conn.close()


@pytest.fixture
def sqlite_config_shared_memory() -> SqliteConfig:
    """Create SQLite config with shared memory for pooling tests."""
    return SqliteConfig(
        pool_config=cast(
            "Any", {"database": "file::memory:?cache=shared", "uri": True, "pool_min_size": 2, "pool_max_size": 5}
        )
    )


@pytest.fixture
def sqlite_config_regular_memory() -> SqliteConfig:
    """Create SQLite config with regular memory for auto-conversion tests."""
    return SqliteConfig(pool_config=cast("Any", {"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10}))


@pytest.fixture
def sqlite_temp_file_config() -> Generator[SqliteConfig, None, None]:
    """Create SQLite config with temporary file for file-based pooling tests."""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        config = SqliteConfig(pool_config=cast("Any", {"database": db_path, "pool_min_size": 3, "pool_max_size": 8}))
        yield config
    finally:
        try:
            os.unlink(db_path)
        except Exception:
            pass
