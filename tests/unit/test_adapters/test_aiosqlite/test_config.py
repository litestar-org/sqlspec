"""Tests for Aiosqlite configuration."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiosqlite import Connection

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def mock_aiosqlite_connection() -> Generator[MagicMock, None, None]:
    """Create a mock Aiosqlite connection."""
    connection = MagicMock(spec=Connection)
    connection.close = AsyncMock()
    yield connection


class TestAiosqliteConfig:
    """Test AiosqliteConfig class."""

    def test_minimal_config(self) -> None:
        """Test minimal configuration with only required values."""
        config = AiosqliteConfig()
        assert config.database == ":memory:"
        assert config.timeout is Empty
        assert config.detect_types is Empty
        assert config.isolation_level is Empty
        assert config.check_same_thread is Empty
        assert config.factory is Empty
        assert config.cached_statements is Empty
        assert config.uri is Empty

    def test_full_config(self) -> None:
        """Test configuration with all values set."""
        config = AiosqliteConfig(
            database=":memory:",
            timeout=5.0,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            isolation_level="IMMEDIATE",
            check_same_thread=False,
            factory=sqlite3.Connection,
            cached_statements=256,
            uri=True,
        )

        assert config.database == ":memory:"
        assert config.timeout == 5.0
        assert config.detect_types == sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        assert config.isolation_level == "IMMEDIATE"
        assert config.check_same_thread is False
        assert config.factory == sqlite3.Connection
        assert config.cached_statements == 256
        assert config.uri is True

    def test_connection_config_dict(self) -> None:
        """Test connection_config_dict property."""
        config = AiosqliteConfig(
            database=":memory:",
            timeout=5.0,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level="IMMEDIATE",
        )
        config_dict = config.connection_config_dict
        assert config_dict == {
            "database": ":memory:",
            "timeout": 5.0,
            "detect_types": sqlite3.PARSE_DECLTYPES,
            "isolation_level": "IMMEDIATE",
        }

    @pytest.mark.asyncio
    async def test_create_connection_success(self, mock_aiosqlite_connection: MagicMock) -> None:
        """Test successful connection creation."""
        with patch("aiosqlite.connect", AsyncMock(return_value=mock_aiosqlite_connection)) as mock_connect:
            config = AiosqliteConfig(database=":memory:")
            connection = await config.create_connection()

            assert connection is mock_aiosqlite_connection
            mock_connect.assert_called_once_with(database=":memory:")

    @pytest.mark.asyncio
    async def test_create_connection_failure(self) -> None:
        """Test connection creation failure."""
        with patch("aiosqlite.connect", AsyncMock(side_effect=Exception("Connection failed"))):
            config = AiosqliteConfig(database=":memory:")
            with pytest.raises(ImproperConfigurationError, match="Could not configure the Aiosqlite connection"):
                await config.create_connection()

    @pytest.mark.asyncio
    async def test_provide_connection(self, mock_aiosqlite_connection: MagicMock) -> None:
        """Test provide_connection context manager."""
        with patch("aiosqlite.connect", AsyncMock(return_value=mock_aiosqlite_connection)):
            config = AiosqliteConfig(database=":memory:")
            async with config.provide_connection() as conn:
                assert conn is mock_aiosqlite_connection

            # Verify connection was closed
            mock_aiosqlite_connection.close.assert_awaited_once()
