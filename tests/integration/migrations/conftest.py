"""Shared fixtures for migration integration tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver


@pytest.fixture
def sqlite_config() -> Generator[SqliteConfig, None, None]:
    """Create SQLite config for migration testing."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    yield config
    config.close_pool()


@pytest.fixture
def sqlite_session(sqlite_config: SqliteConfig) -> Generator[SqliteDriver, None, None]:
    """Create SQLite session for migration testing."""
    with sqlite_config.provide_session() as session:
        yield session


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Create temporary migrations directory."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    return migrations_dir
