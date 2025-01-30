"""Tests for SQLite configuration."""

from __future__ import annotations

from sqlite3 import Connection
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def mock_sqlite_connection() -> Generator[MagicMock, None, None]:
    """Create a mock SQLite connection."""
    with patch("sqlite3.connect") as mock_connect:
        connection = MagicMock(spec=Connection)
        mock_connect.return_value = connection
        yield connection


class TestSqliteConfig:
    """Test SqliteConfig class."""

    def test_default_values(self) -> None:
        """Test default values for SqliteConfig."""
        config = SqliteConfig()
        assert config.database == ":memory:"
        assert config.timeout is Empty
        assert config.detect_types is Empty
        assert config.isolation_level is Empty
        assert config.check_same_thread is Empty
        assert config.factory is Empty
        assert config.cached_statements is Empty
        assert config.uri is Empty

    def test_with_all_values(self) -> None:
        """Test SqliteConfig with all values set."""
        config = SqliteConfig(
            database="test.db",
            timeout=30.0,
            detect_types=1,
            isolation_level="IMMEDIATE",
            check_same_thread=False,
            factory=Connection,
            cached_statements=100,
            uri=True,
        )
        assert config.database == "test.db"
        assert config.timeout == 30.0
        assert config.detect_types == 1
        assert config.isolation_level == "IMMEDIATE"
        assert config.check_same_thread is False
        assert config.factory == Connection
        assert config.cached_statements == 100
        assert config.uri is True

    def test_connection_config_dict(self) -> None:
        """Test connection_config_dict property."""
        config = SqliteConfig(database="test.db", timeout=30.0)
        config_dict = config.connection_config_dict
        assert config_dict == {"database": "test.db", "timeout": 30.0}

    def test_create_connection(self, mock_sqlite_connection: MagicMock) -> None:
        """Test create_connection method."""
        config = SqliteConfig(database="test.db")
        connection = config.create_connection()
        assert connection is mock_sqlite_connection

    def test_create_connection_error(self) -> None:
        """Test create_connection raises error on failure."""
        with patch("sqlite3.connect", side_effect=Exception("Test error")):
            config = SqliteConfig(database="test.db")
            with pytest.raises(ImproperConfigurationError, match="Could not configure the SQLite connection"):
                config.create_connection()

    def test_provide_connection(self, mock_sqlite_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        config = SqliteConfig(database="test.db")
        with config.provide_connection() as connection:
            assert connection is mock_sqlite_connection
