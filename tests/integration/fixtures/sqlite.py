"""Shared SQLite-family integration fixtures."""

import sqlite3
import tempfile
from collections.abc import AsyncGenerator, Generator
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import pytest
from pytest_databases.helpers import get_xdist_worker_num

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver

__all__ = (
    "aiosqlite_config",
    "aiosqlite_config_file",
    "aiosqlite_session",
    "aiosqlite_session_config",
    "sqlite_basic_session",
    "sqlite_config_regular_memory",
    "sqlite_config_shared_memory",
    "sqlite_driver",
    "sqlite_session",
    "sqlite_temp_file_config",
)


def _temporary_database_path(prefix: str) -> Path:
    worker_num = get_xdist_worker_num()
    worker_suffix = f"-worker-{worker_num}" if worker_num is not None else ""
    with tempfile.NamedTemporaryFile(prefix=f"{prefix}{worker_suffix}-", suffix=".db", delete=False) as tmp:
        return Path(tmp.name)


@pytest.fixture
def sqlite_session() -> "Generator[SqliteDriver, None, None]":
    """Create a SQLite session with a test table for integration tests."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            session.commit()
            try:
                yield session
            finally:
                try:
                    session.commit()
                except Exception:
                    with suppress(Exception):
                        session.rollback()
    finally:
        config.close_pool()


@pytest.fixture
def sqlite_basic_session() -> "Generator[SqliteDriver, None, None]":
    """Yield a bare SQLite session for tests needing a clean database."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as session:
            session.execute("PRAGMA foreign_keys = ON")
            yield session
    finally:
        config.close_pool()


@pytest.fixture
def sqlite_driver() -> "Generator[SqliteDriver, None, None]":
    """Create a SQLite driver with a populated users table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    driver = SqliteDriver(conn)
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
    try:
        yield driver
    finally:
        conn.close()


@pytest.fixture
def sqlite_config_shared_memory() -> "Generator[SqliteConfig, None, None]":
    """Create SQLite config with shared memory for pooling tests."""
    config = SqliteConfig(
        connection_config=cast(
            "Any", {"database": "file::memory:?cache=shared", "uri": True, "pool_min_size": 2, "pool_max_size": 5}
        )
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def sqlite_config_regular_memory() -> "Generator[SqliteConfig, None, None]":
    """Create SQLite config with regular memory for auto-conversion tests."""
    config = SqliteConfig(
        connection_config=cast("Any", {"database": ":memory:", "pool_min_size": 5, "pool_max_size": 10})
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def sqlite_temp_file_config() -> "Generator[SqliteConfig, None, None]":
    """Create SQLite config with an xdist-isolated temporary database."""
    db_path = _temporary_database_path("sqlspec-sqlite")
    config: SqliteConfig | None = None
    try:
        config = SqliteConfig(
            connection_config=cast("Any", {"database": str(db_path), "pool_min_size": 3, "pool_max_size": 8})
        )
        yield config
    finally:
        if config is not None:
            config.close_pool()
        db_path.unlink(missing_ok=True)


@pytest.fixture
async def aiosqlite_session_config() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Provide a per-test config for sessions that create mutable tables."""
    config = AiosqliteConfig()
    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture
async def aiosqlite_session(aiosqlite_session_config: "AiosqliteConfig") -> "AsyncGenerator[AiosqliteDriver, None]":
    """Create an aiosqlite session with a fresh test table."""
    async with aiosqlite_session_config.provide_session() as session:
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
                with suppress(Exception):
                    await session.rollback()


@pytest.fixture(scope="session")
async def aiosqlite_config() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Provide a reusable config for tests that own their session cleanup."""
    config = AiosqliteConfig()
    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture
async def aiosqlite_config_file() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Provide an xdist-isolated file-backed config for concurrent access tests."""
    db_path = _temporary_database_path("sqlspec-aiosqlite")
    config = AiosqliteConfig(connection_config={"database": str(db_path), "pool_size": 5})
    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None
        db_path.unlink(missing_ok=True)
