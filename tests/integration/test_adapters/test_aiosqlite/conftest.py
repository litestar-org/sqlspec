"""Fixtures and configuration for AIOSQLite integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver


@pytest.fixture
async def aiosqlite_session() -> AsyncGenerator[AiosqliteDriver, None]:
    """Create an aiosqlite session with test table."""
    # Use unique shared memory database name to ensure test isolation
    # Format: file:memdb<unique_id>?mode=memory&cache=shared
    unique_db = f"file:memdb{uuid4().hex}?mode=memory&cache=shared"
    config = AiosqliteConfig(pool_config={"database": unique_db})

    try:
        async with config.provide_session() as session:
            # Create test table
            await session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Commit DDL to prevent table locking issues in subsequent operations
            await session.commit()

            try:
                yield session
            finally:
                # Ensure any pending transactions are committed before test ends
                try:
                    await session.commit()
                except Exception:
                    # If commit fails, try rollback to clean up transaction state
                    try:
                        await session.rollback()
                    except Exception:
                        pass
    finally:
        # Ensure pool is closed properly to avoid threading issues during test shutdown
        await config.close_pool()


@pytest.fixture
async def aiosqlite_config() -> AsyncGenerator[AiosqliteConfig, None]:
    """Provide AiosqliteConfig for connection tests."""
    unique_db = f"file:memdb{uuid4().hex}?mode=memory&cache=shared"
    config = AiosqliteConfig(pool_config={"database": unique_db})

    try:
        yield config
    finally:
        await config.close_pool()
