"""Fixtures and configuration for AIOSQLite integration tests."""

import os
import tempfile
from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver

pytestmark = pytest.mark.xdist_group("sqlite")


@pytest.fixture(scope="function")
async def aiosqlite_session() -> "AsyncGenerator[AiosqliteDriver, None]":
    """Create an aiosqlite session with test table.

    Ensures proper pool cleanup to prevent event loop issues
    when running with pytest-xdist and function-scoped event loops.
    """
    config = AiosqliteConfig()

    try:
        async with config.provide_session() as session:
            await session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await session.commit()

            try:
                yield session
            finally:
                try:
                    await session.commit()
                except Exception:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture(scope="function")
async def aiosqlite_config() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Provide AiosqliteConfig for connection tests.

    Ensures proper pool cleanup to prevent event loop issues
    when running with pytest-xdist and function-scoped event loops.
    """
    config = AiosqliteConfig()

    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture(scope="function")
async def aiosqlite_config_file() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Provide AiosqliteConfig with temporary file database for concurrent access tests.

    Ensures proper pool cleanup to prevent event loop issues
    when running with pytest-xdist and function-scoped event loops.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    config = AiosqliteConfig(connection_config={"database": db_path, "pool_size": 5})

    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None
        try:
            os.unlink(db_path)
        except Exception:
            pass
