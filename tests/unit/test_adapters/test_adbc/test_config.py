"""Tests for ADBC configuration."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from adbc_driver_manager.dbapi import Connection

from sqlspec.adapters.adbc import AdbcConfig

if TYPE_CHECKING:
    from collections.abc import Generator


class MockAdbc(AdbcConfig):
    """Mock implementation of ADBC for testing."""

    def __init__(self, mock_connection: MagicMock | None = None, **kwargs: Any) -> None:
        """Initialize with optional mock connection."""
        super().__init__(**kwargs)  # pyright: ignore
        self._mock_connection = mock_connection

    def create_connection(*args: Any, **kwargs: Any) -> Connection:
        """Mock create_connection method."""
        return MagicMock(spec=Connection)  # pyright: ignore

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        _ = super().connection_config_dict  # pyright: ignore
        return {"driver": "test_driver"}

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
        """Mock provide_connection context manager."""
        if self._mock_connection is not None:
            yield self._mock_connection
        else:
            yield MagicMock(spec=Connection)  # pyright: ignore


@pytest.fixture(scope="session")
def mock_adbc_connection() -> Generator[MagicMock, None, None]:
    """Create a mock ADBC connection."""
    return MagicMock(spec=Connection)  # pyright: ignore


def test_default_values() -> None:
    """Test default values for ADBC."""
    config = AdbcConfig()
    assert config.connection_config_dict == {}  # pyright: ignore


def test_with_all_values() -> None:
    """Test ADBC with all values set."""
    config = AdbcConfig(
        uri="localhost",
        driver_name="test_driver",
        db_kwargs={"user": "test_user", "password": "test_pass", "database": "test_db"},
    )

    assert config.connection_config_dict == {
        "uri": "localhost",
        "user": "test_user",
        "password": "test_pass",
        "database": "test_db",
    }


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    config = AdbcConfig(
        uri="localhost",
        driver_name="test_driver",
        db_kwargs={"user": "test_user", "password": "test_pass", "database": "test_db"},
    )
    config_dict = config.connection_config_dict
    assert config_dict["uri"] == "localhost"
    assert config_dict["user"] == "test_user"
    assert config_dict["password"] == "test_pass"
    assert config_dict["database"] == "test_db"


def test_provide_connection(mock_adbc_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockAdbc(mock_connection=mock_adbc_connection)  # pyright: ignore
    with config.provide_connection() as connection:  # pyright: ignore
        assert connection is mock_adbc_connection
