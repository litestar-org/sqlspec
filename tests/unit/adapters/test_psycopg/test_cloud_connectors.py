"""Unit tests for Google AlloyDB connector integration in Psycopg."""

import sys
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from psycopg import Connection

import sqlspec.adapters.psycopg.config as psycopg_config
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError

# pyright: reportPrivateUsage=false


class _CapturedSyncPool:
    """Capture psycopg sync pool constructor arguments without opening a connection."""

    calls: list[tuple[str, dict[str, Any]]] = []

    def __init__(self, conninfo: str = "", **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.calls.append((conninfo, kwargs))

    def close(self) -> None:
        return None


class _CapturedAsyncPool:
    """Capture psycopg async pool constructor arguments without opening a connection."""

    calls: list[tuple[str, dict[str, Any]]] = []
    open_calls: int = 0

    def __init__(self, conninfo: str = "", **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.calls.append((conninfo, kwargs))

    async def open(self) -> None:
        type(self).open_calls += 1


@pytest.fixture(autouse=True)
def disable_alloydb_by_default() -> Generator[None, None, None]:
    """Disable the AlloyDB connector by default for stable tests."""
    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", False):
        yield


@pytest.fixture
def mock_alloydb_module() -> Generator[MagicMock, None, None]:
    """Create and register a mock google.cloud.alloydb module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.Connector = mock_connector_class

    sys.modules["google.cloud.alloydb"] = mock_module
    sys.modules["google.cloud.alloydb.connector"] = mock_module.connector

    yield mock_connector_class

    sys.modules.pop("google.cloud.alloydb", None)
    sys.modules.pop("google.cloud.alloydb.connector", None)


def test_alloydb_defaults_to_false() -> None:
    """AlloyDB connector support should be explicit opt-in for sync psycopg."""
    config = PsycopgSyncConfig(connection_config={"dbname": "app"})

    assert config.driver_features["enable_alloydb"] is False
    assert config.driver_features["enable_alloydb_iam_auth"] is False
    assert config.driver_features["alloydb_ip_type"] == "PRIVATE"
    assert "alloydb_instance_uri" not in config.driver_features


def test_alloydb_missing_package_raises_error() -> None:
    """Enabling AlloyDB without the connector package should fail during config creation."""
    with pytest.raises(MissingDependencyError, match="google-cloud-alloydb-connector"):
        PsycopgSyncConfig(
            connection_config={"dbname": "app"},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
            },
        )


def test_alloydb_missing_instance_uri_raises_error() -> None:
    """AlloyDB requires an instance URI when enabled."""
    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        with pytest.raises(
            ImproperConfigurationError, match="alloydb_instance_uri required when enable_alloydb is True"
        ):
            PsycopgSyncConfig(connection_config={"dbname": "app"}, driver_features={"enable_alloydb": True})


def test_alloydb_invalid_instance_uri_format_raises_error() -> None:
    """AlloyDB instance URIs must use the projects/... resource path form."""
    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid AlloyDB instance URI format"):
            PsycopgSyncConfig(
                connection_config={"dbname": "app"},
                driver_features={"enable_alloydb": True, "alloydb_instance_uri": "invalid-format"},
            )


def test_user_connection_class_is_preserved_when_alloydb_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """A user-supplied psycopg connection_class should pass through when AlloyDB is disabled."""
    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    PsycopgSyncConfig(connection_config={"dbname": "app", "connection_class": Connection})._create_pool()

    _conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    assert pool_kwargs["connection_class"] is Connection
    assert pool_kwargs["kwargs"] == {"dbname": "app"}


def test_alloydb_pool_uses_connector_backed_connection_class(
    monkeypatch: pytest.MonkeyPatch, mock_alloydb_module: MagicMock
) -> None:
    """AlloyDB should strip direct connection fields and install a connector connection class."""
    mock_connector = MagicMock()
    mock_alloydb_module.return_value = mock_connector
    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = PsycopgSyncConfig(
            connection_config={
                "conninfo": "postgresql://user:pass@localhost/app",
                "host": "127.0.0.1",
                "port": 5432,
                "user": "testuser",
                "password": "testpass",
                "dbname": "app",
                "application_name": "sqlspec",
            },
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
            },
        )
        config._create_pool()

    conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    connection_class = pool_kwargs["connection_class"]

    assert conninfo == ""
    assert issubclass(connection_class, Connection)
    assert connection_class is not Connection
    assert pool_kwargs["kwargs"] == {"application_name": "sqlspec"}
    assert config._alloydb_connector is mock_connector
    assert "host" not in pool_kwargs["kwargs"]
    assert "port" not in pool_kwargs["kwargs"]
    assert "user" not in pool_kwargs["kwargs"]
    assert "password" not in pool_kwargs["kwargs"]
    assert "dbname" not in pool_kwargs["kwargs"]


def test_alloydb_connection_class_calls_connector(
    monkeypatch: pytest.MonkeyPatch, mock_alloydb_module: MagicMock
) -> None:
    """The injected connection class should call the AlloyDB connector with psycopg settings."""
    mock_connector = MagicMock()
    expected_connection = MagicMock()
    mock_connector.connect.return_value = expected_connection
    mock_alloydb_module.return_value = mock_connector
    _CapturedSyncPool.calls.clear()
    monkeypatch.setattr(psycopg_config, "ConnectionPool", _CapturedSyncPool)

    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = PsycopgSyncConfig(
            connection_config={"user": "testuser", "password": "testpass", "dbname": "app"},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                "enable_alloydb_iam_auth": True,
                "alloydb_ip_type": "PSC",
            },
        )
        config._create_pool()

    _conninfo, pool_kwargs = _CapturedSyncPool.calls[-1]
    connection_class = pool_kwargs["connection_class"]

    assert connection_class.connect("ignored") is expected_connection
    mock_connector.connect.assert_called_once_with(
        "projects/p/locations/r/clusters/c/instances/i",
        "psycopg",
        enable_iam_auth=True,
        ip_type="PSC",
        user="testuser",
        password="testpass",
        db="app",
    )


def test_alloydb_connector_cleanup(mock_alloydb_module: MagicMock) -> None:
    """Closing a sync psycopg config should close the AlloyDB connector."""
    mock_connector = MagicMock()
    mock_alloydb_module.return_value = mock_connector
    with patch("sqlspec.adapters.psycopg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = PsycopgSyncConfig(
            connection_config={"user": "testuser", "password": "testpass", "dbname": "app"},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
            },
        )
        config._setup_alloydb_connector({})
        config._close_pool()

    mock_connector.close.assert_called_once()
    assert config._alloydb_connector is None


@pytest.mark.anyio
async def test_async_config_does_not_validate_or_route_alloydb(monkeypatch: pytest.MonkeyPatch) -> None:
    """PsycopgAsyncConfig should remain unaffected by sync-only AlloyDB support."""
    _CapturedAsyncPool.calls.clear()
    _CapturedAsyncPool.open_calls = 0
    monkeypatch.setattr(psycopg_config, "AsyncConnectionPool", _CapturedAsyncPool)

    config = PsycopgAsyncConfig(
        connection_config={"dbname": "app"},
        driver_features={"enable_alloydb": True, "alloydb_instance_uri": "invalid-format"},
    )

    await config._create_pool()

    conninfo, pool_kwargs = _CapturedAsyncPool.calls[-1]
    assert conninfo == ""
    assert pool_kwargs["kwargs"] == {"dbname": "app"}
    assert "connection_class" not in pool_kwargs
    assert config.driver_features["enable_alloydb"] is True
