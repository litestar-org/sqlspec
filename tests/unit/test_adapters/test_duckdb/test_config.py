"""Tests for DuckDB configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import duckdb
import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig, ExtensionConfig, SecretConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


class MockDuckDB(DuckDBConfig):
    """Mock implementation of DuckDB for testing."""

    def __init__(self, *args: Any, connection: MagicMock | None = None, **kwargs: Any) -> None:
        """Initialize with optional connection."""
        super().__init__(*args, **kwargs)
        self._connection = connection

    def create_connection(*args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        """Mock create_connection method."""
        # If a connection was provided, use it, otherwise create a new mock
        if hasattr(args[0], "_connection") and args[0]._connection is not None:  # noqa: SLF001
            return args[0]._connection  # type: ignore[no-any-return]  # noqa: SLF001
        return MagicMock(spec=duckdb.DuckDBPyConnection)

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Mock connection_config_dict property."""
        return {}


@pytest.fixture(scope="session")
def mock_duckdb_connection() -> Generator[MagicMock, None, None]:
    """Create a mock DuckDB connection."""
    return MagicMock(spec=duckdb.DuckDBPyConnection)


def test_default_values() -> None:
    """Test default values for DuckDB."""
    config = DuckDBConfig()
    assert config.database == ":memory:"
    assert config.read_only is Empty
    assert config.config == {}
    assert isinstance(config.extensions, list)
    assert len(config.extensions) == 0
    assert isinstance(config.secrets, list)
    assert len(config.secrets) == 0
    assert not config.auto_update_extensions
    assert config.on_connection_create is None


def test_with_all_values() -> None:
    """Test DuckDB with all values set."""

    def on_connection_create(conn: duckdb.DuckDBPyConnection) -> None:
        pass

    extensions: list[ExtensionConfig] = [{"name": "test_ext"}]
    secrets: list[SecretConfig] = [{"name": "test_secret", "secret_type": "s3", "value": {"key": "value"}}]

    config = DuckDBConfig(
        database="test.db",
        read_only=True,
        config={"setting": "value"},
        extensions=extensions,
        secrets=secrets,
        auto_update_extensions=True,
        on_connection_create=on_connection_create,
    )

    assert config.database == "test.db"
    assert config.read_only is True
    assert config.config == {"setting": "value"}
    assert isinstance(config.extensions, list)
    assert len(config.extensions) == 1
    assert config.extensions[0]["name"] == "test_ext"
    assert isinstance(config.secrets, list)
    assert len(config.secrets) == 1
    assert config.secrets[0]["name"] == "test_secret"
    assert config.auto_update_extensions is True
    assert config.on_connection_create == on_connection_create


def test_connection_config_dict() -> None:
    """Test connection_config_dict property."""
    config = DuckDBConfig(
        database="test.db",
        read_only=True,
        config={"setting": "value"},
    )
    config_dict = config.connection_config_dict
    assert config_dict["database"] == "test.db"
    assert config_dict["read_only"] is True
    assert config_dict["config"] == {"setting": "value"}


def test_create_connection() -> None:
    """Test create_connection method."""
    config = MockDuckDB(
        database="test.db",
        read_only=True,
        config={"setting": "value"},
    )
    connection = config.create_connection()
    assert isinstance(connection, MagicMock)
    assert connection._spec_class == duckdb.DuckDBPyConnection  # noqa: SLF001


def test_create_connection_error() -> None:
    """Test create_connection method with error."""
    config = DuckDBConfig(
        database="test.db",
        read_only=True,
        config={"setting": "value"},
    )
    with pytest.raises(ImproperConfigurationError):
        config.create_connection()


def test_provide_connection(mock_duckdb_connection: MagicMock) -> None:
    """Test provide_connection context manager."""
    config = MockDuckDB(
        database="test.db",
        read_only=True,
        config={"setting": "value"},
        connection=mock_duckdb_connection,
    )
    with config.provide_connection() as connection:
        assert connection is mock_duckdb_connection
