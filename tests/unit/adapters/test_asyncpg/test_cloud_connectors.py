"""Unit tests for Google Cloud SQL and AlloyDB connector integration in AsyncPG."""

import asyncio
import math
import subprocess
import sys
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from sqlspec.adapters.asyncpg.config import AsyncpgConfig, _AsyncpgAlloydbConnector, _AsyncpgCloudSqlConnector
from sqlspec.adapters.asyncpg.core import calculate_connector_backoff_seconds, is_transient_connector_error
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.utils.module_loader import reset_dependency_cache

try:
    import aiohttp as _aiohttp_mod
except ImportError:
    _aiohttp_mod = None

try:
    from google.api_core import exceptions as _api_exceptions_mod
except ImportError:
    _api_exceptions_mod = None

try:
    from google.auth import exceptions as _auth_exceptions_mod
except ImportError:
    _auth_exceptions_mod = None

aiohttp_mod: Any = _aiohttp_mod
api_exceptions: Any = _api_exceptions_mod
auth_exceptions: Any = _auth_exceptions_mod

# pyright: reportPrivateUsage=false


@pytest.fixture(autouse=True)
def disable_connectors_by_default():
    """Disable both connectors by default for clean test state."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", False):
        with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", False):
            yield


@pytest.fixture
def mock_cloud_sql_module():
    """Create and register mock google.cloud.sql module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.Connector = mock_connector_class

    sys.modules["google.cloud.sql"] = mock_module
    sys.modules["google.cloud.sql.connector"] = mock_module.connector
    reset_dependency_cache("google.cloud.sql.connector")

    yield mock_connector_class

    sys.modules.pop("google.cloud.sql", None)
    sys.modules.pop("google.cloud.sql.connector", None)
    reset_dependency_cache("google.cloud.sql.connector")


@pytest.fixture
def mock_alloydb_module():
    """Create and register mock google.cloud.alloydb module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.AsyncConnector = mock_connector_class

    sys.modules["google.cloud.alloydb"] = mock_module
    sys.modules["google.cloud.alloydb.connector"] = mock_module.connector
    reset_dependency_cache("google.cloud.alloydb.connector")

    yield mock_connector_class

    sys.modules.pop("google.cloud.alloydb", None)
    sys.modules.pop("google.cloud.alloydb.connector", None)
    reset_dependency_cache("google.cloud.alloydb.connector")


def test_cloud_sql_defaults_to_false() -> None:
    """Cloud SQL connector should always default to False (explicit opt-in required)."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    assert config.driver_features["enable_cloud_sql"] is False


def test_alloydb_defaults_to_false() -> None:
    """AlloyDB connector should always default to False (explicit opt-in required)."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    assert config.driver_features["enable_alloydb"] is False


def test_mutual_exclusion_both_enabled_raises_error() -> None:
    """Enabling both Cloud SQL and AlloyDB connectors should raise error."""
    with pytest.raises(
        ImproperConfigurationError, match="Cannot enable both Cloud SQL and AlloyDB connectors simultaneously"
    ):
        AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/test"},
            driver_features={
                "enable_cloud_sql": True,
                "cloud_sql_instance": "project:region:instance",
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
            },
        )


def test_cloud_sql_missing_package_raises_error() -> None:
    """Enabling Cloud SQL without package installed should raise error."""
    with pytest.raises(MissingDependencyError, match="cloud-sql-python-connector"):
        AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/test"},
            driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
        )


def test_alloydb_missing_package_raises_error() -> None:
    """Enabling AlloyDB without package installed should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", False):
        with pytest.raises(MissingDependencyError, match="google-cloud-alloydb-connector"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                },
            )


def test_cloud_sql_missing_instance_raises_error() -> None:
    """Enabling Cloud SQL without instance string should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        with pytest.raises(
            ImproperConfigurationError, match="cloud_sql_instance required when enable_cloud_sql is True"
        ):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"}, driver_features={"enable_cloud_sql": True}
            )


def test_alloydb_missing_instance_uri_raises_error() -> None:
    """Enabling AlloyDB without instance URI should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        with pytest.raises(
            ImproperConfigurationError, match="alloydb_instance_uri required when enable_alloydb is True"
        ):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"}, driver_features={"enable_alloydb": True}
            )


def test_cloud_sql_invalid_instance_format_raises_error() -> None:
    """Cloud SQL instance with invalid format should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid Cloud SQL instance format"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "invalid-format"},
            )


def test_cloud_sql_instance_format_too_many_colons() -> None:
    """Cloud SQL instance with too many colons should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid Cloud SQL instance format"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance:extra"},
            )


def test_alloydb_invalid_instance_uri_format_raises_error() -> None:
    """AlloyDB instance URI with invalid format should raise error."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid AlloyDB instance URI format"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={"enable_alloydb": True, "alloydb_instance_uri": "invalid-format"},
            )


def test_cloud_sql_explicit_disable() -> None:
    """Explicitly disabling Cloud SQL should work even when package installed."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/test"}, driver_features={"enable_cloud_sql": False}
        )
        assert config.driver_features["enable_cloud_sql"] is False


def test_alloydb_explicit_disable() -> None:
    """Explicitly disabling AlloyDB should work even when package installed."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/test"}, driver_features={"enable_alloydb": False}
        )
        assert config.driver_features["enable_alloydb"] is False


def test_normal_config_without_connectors() -> None:
    """Normal config without connectors should work."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    assert config is not None
    assert config.driver_features.get("enable_cloud_sql", False) is not True
    assert config.driver_features.get("enable_alloydb", False) is not True


async def test_cloud_sql_connector_initialization(mock_cloud_sql_module) -> None:
    """Cloud SQL connector should be initialized correctly in create_pool."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect_async = AsyncMock(return_value=MagicMock())
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
            )

            pool = await config._create_pool()

            assert pool is mock_pool
            mock_cloud_sql_module.assert_called_once()
            assert config._cloud_sql_connector is mock_connector  # pyright: ignore
            mock_create_pool.assert_called_once()
            call_kwargs = mock_create_pool.call_args.kwargs
            assert "connect" in call_kwargs
            assert "dsn" not in call_kwargs
            assert "host" not in call_kwargs
            assert "user" not in call_kwargs


async def test_cloud_sql_iam_auth_enabled(mock_cloud_sql_module) -> None:
    """Cloud SQL IAM authentication should configure enable_iam_auth=True."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["enable_iam_auth"] is True
            return MagicMock()

        mock_connector.connect_async = AsyncMock(side_effect=mock_connect)
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "database": "testdb"},
                driver_features={
                    "enable_cloud_sql": True,
                    "cloud_sql_instance": "project:region:instance",
                    "cloud_sql_enable_iam_auth": True,
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_cloud_sql_iam_auth_disabled(mock_cloud_sql_module) -> None:
    """Cloud SQL with IAM disabled should configure enable_iam_auth=False."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["enable_iam_auth"] is False
            return MagicMock()

        mock_connector.connect_async = AsyncMock(side_effect=mock_connect)
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_cloud_sql": True,
                    "cloud_sql_instance": "project:region:instance",
                    "cloud_sql_enable_iam_auth": False,
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_cloud_sql_ip_type_configuration(mock_cloud_sql_module) -> None:
    """Cloud SQL IP type should be passed to connector."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["ip_type"] == "PUBLIC"
            return MagicMock()

        mock_connector.connect_async = AsyncMock(side_effect=mock_connect)
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_cloud_sql": True,
                    "cloud_sql_instance": "project:region:instance",
                    "cloud_sql_ip_type": "PUBLIC",
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_cloud_sql_default_ip_type(mock_cloud_sql_module) -> None:
    """Cloud SQL should default to PRIVATE IP type."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["ip_type"] == "PRIVATE"
            return MagicMock()

        mock_connector.connect_async = AsyncMock(side_effect=mock_connect)
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_alloydb_connector_initialization(mock_alloydb_module) -> None:
    """AlloyDB connector should be initialized correctly in create_pool."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect = AsyncMock(return_value=MagicMock())
        mock_alloydb_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                },
            )

            pool = await config._create_pool()

            assert pool is mock_pool
            mock_alloydb_module.assert_called_once()
            assert config._alloydb_connector is mock_connector
            mock_create_pool.assert_called_once()
            call_kwargs = mock_create_pool.call_args.kwargs
            assert "connect" in call_kwargs
            assert "dsn" not in call_kwargs
            assert "host" not in call_kwargs
            assert "user" not in call_kwargs


async def test_alloydb_iam_auth_enabled(mock_alloydb_module) -> None:
    """AlloyDB IAM authentication should configure enable_iam_auth=True."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["enable_iam_auth"] is True
            return MagicMock()

        mock_connector.connect = AsyncMock(side_effect=mock_connect)
        mock_alloydb_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "database": "testdb"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                    "enable_alloydb_iam_auth": True,
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_alloydb_ip_type_configuration(mock_alloydb_module) -> None:
    """AlloyDB IP type should be passed to connector."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()

        async def mock_connect(**kwargs):
            assert kwargs["ip_type"] == "PSC"
            return MagicMock()

        mock_connector.connect = AsyncMock(side_effect=mock_connect)
        mock_alloydb_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                    "alloydb_ip_type": "PSC",
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


async def test_cloud_sql_connector_cleanup(mock_cloud_sql_module) -> None:
    """Cloud SQL connector should be closed on pool close."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect_async = AsyncMock(return_value=MagicMock())
        mock_connector.close_async = AsyncMock()
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
            )

            await config._create_pool()
            await config._close_pool()

            mock_connector.close_async.assert_called_once()
            assert config._cloud_sql_connector is None


async def test_alloydb_connector_cleanup(mock_alloydb_module) -> None:
    """AlloyDB connector should be closed on pool close."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect = AsyncMock(return_value=MagicMock())
        mock_connector.close = AsyncMock()
        mock_alloydb_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_pool.close = AsyncMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                },
            )

            await config._create_pool()
            await config._close_pool()

            mock_connector.close.assert_called_once()
            assert config._alloydb_connector is None


async def test_connection_factory_pattern_cloud_sql(mock_cloud_sql_module) -> None:
    """Cloud SQL should use connection factory pattern with connect parameter."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect_async = AsyncMock(return_value=MagicMock())
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
            )

            await config._create_pool()

            call_kwargs = mock_create_pool.call_args.kwargs
            assert "connect" in call_kwargs
            assert callable(call_kwargs["connect"])


async def test_connection_factory_pattern_alloydb(mock_alloydb_module) -> None:
    """AlloyDB should use connection factory pattern with connect parameter."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.connect = AsyncMock(return_value=MagicMock())
        mock_alloydb_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_pool = MagicMock()
            mock_create_pool.return_value = mock_pool

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
                },
            )

            await config._create_pool()

            call_kwargs = mock_create_pool.call_args.kwargs
            assert "connect" in call_kwargs
            assert callable(call_kwargs["connect"])


async def test_pool_close_without_connectors() -> None:
    """Closing pool without connectors should not raise errors."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    mock_pool = MagicMock()
    mock_pool.close = AsyncMock()
    config.connection_instance = mock_pool

    await config._close_pool()

    mock_pool.close.assert_called_once()
    assert config._cloud_sql_connector is None
    assert config._alloydb_connector is None


def test_cloud_sql_empty_segments_rejected() -> None:
    """Cloud SQL instance with empty segments like '::' should raise ImproperConfigurationError."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid Cloud SQL instance format"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "::"},
            )


def test_alloydb_incomplete_uri_rejected() -> None:
    """AlloyDB URI missing required resource segments should raise ImproperConfigurationError."""
    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        with pytest.raises(ImproperConfigurationError, match="Invalid AlloyDB instance URI format"):
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/test"},
                driver_features={"enable_alloydb": True, "alloydb_instance_uri": "projects/my-proj"},
            )


async def test_cloud_sql_connector_cleaned_up_when_create_pool_fails(mock_cloud_sql_module) -> None:
    """If asyncpg_create_pool raises, the initialized Cloud SQL connector should be closed and reset."""
    with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
        mock_connector = MagicMock()
        mock_connector.close_async = AsyncMock()
        mock_cloud_sql_module.return_value = mock_connector

        with patch("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.side_effect = RuntimeError("pool init failed")

            config = AsyncpgConfig(
                connection_config={"user": "testuser", "password": "testpass", "database": "testdb"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "project:region:instance"},
            )

            with pytest.raises(RuntimeError, match="pool init failed"):
                await config._create_pool()

            mock_connector.close_async.assert_awaited_once()
            assert config._cloud_sql_connector is None


class _FakePoolConnection(asyncpg.Connection):
    """Minimal asyncpg.Connection subclass for pool lifecycle tests."""

    def __init__(self) -> None:
        self._closed = False
        self._protocol = None
        self._pool_holder = None
        self._proxy = None

    def is_closed(self) -> bool:
        return self._closed

    def is_in_transaction(self) -> bool:
        return False

    async def set_type_codec(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def fetch(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def reset(self, *, timeout: float | None = None) -> None:
        _ = timeout

    async def close(self, *, timeout: float | None = None) -> None:
        _ = timeout
        self._closed = True

    def terminate(self) -> None:
        self._closed = True

    def _set_proxy(self, _proxy: Any) -> None:
        return None


class _FakeGrpcStatusError(Exception):
    def __init__(self, status_name: str, *, use_enum_obj: bool = True) -> None:
        super().__init__(status_name)
        self._status = SimpleNamespace(name=status_name) if use_enum_obj else status_name

    def code(self) -> Any:
        return self._status


def _make_aiohttp_response_error(status: int) -> BaseException:
    if aiohttp_mod is None:
        pytest.skip("aiohttp is not installed")
    return cast(
        "BaseException",
        aiohttp_mod.ClientResponseError(request_info=MagicMock(), history=(), status=status, message=f"HTTP {status}"),
    )


def _make_aiohttp_ssl_error() -> BaseException:
    if aiohttp_mod is None:
        pytest.skip("aiohttp is not installed")
    conn_key = SimpleNamespace(host="db.example", port=443, is_ssl=True)
    return cast("BaseException", aiohttp_mod.ClientSSLError(conn_key, OSError("certificate verify failed")))


def _build_api_error(factory: Callable[[Any], BaseException]) -> BaseException:
    if api_exceptions is None:
        pytest.skip("google-api-core is not installed")
    return factory(api_exceptions)


def _build_auth_error(factory: Callable[[Any], BaseException]) -> BaseException:
    if auth_exceptions is None:
        pytest.skip("google-auth is not installed")
    return factory(auth_exceptions)


def _build_aiohttp_error(factory: Callable[[Any], BaseException]) -> BaseException:
    if aiohttp_mod is None:
        pytest.skip("aiohttp is not installed")
    return factory(aiohttp_mod)


def test_connector_retry_defaults() -> None:
    """Retry defaults are populated on AsyncpgConfig.driver_features."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})
    assert config.driver_features["connector_max_retries"] == 3
    assert config.driver_features["connector_retry_backoff_seconds"] == 0.5
    assert config.driver_features["connector_retry_max_backoff_seconds"] == 5.0


@pytest.mark.parametrize(
    ("features", "message"),
    [
        ({"connector_max_retries": -1}, "connector_max_retries"),
        ({"connector_max_retries": True}, "connector_max_retries"),
        ({"connector_max_retries": "3"}, "connector_max_retries"),
        ({"connector_retry_backoff_seconds": -0.1}, "connector_retry_backoff_seconds"),
        ({"connector_retry_backoff_seconds": True}, "connector_retry_backoff_seconds"),
        ({"connector_retry_backoff_seconds": math.nan}, "connector_retry_backoff_seconds"),
        ({"connector_retry_backoff_seconds": math.inf}, "connector_retry_backoff_seconds"),
        ({"connector_retry_max_backoff_seconds": -1.0}, "connector_retry_max_backoff_seconds"),
        ({"connector_retry_max_backoff_seconds": False}, "connector_retry_max_backoff_seconds"),
        ({"connector_retry_max_backoff_seconds": math.nan}, "connector_retry_max_backoff_seconds"),
        ({"connector_retry_backoff_seconds": 4.0, "connector_retry_max_backoff_seconds": 2.0}, "greater than or equal"),
    ],
)
def test_connector_retry_config_validation_runs_even_when_disabled(features: dict[str, Any], message: str) -> None:
    """Invalid retry knobs fail eagerly even when both cloud connectors are disabled."""
    with pytest.raises(ImproperConfigurationError, match=message):
        AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"}, driver_features=features)


@pytest.mark.parametrize("connector_kind", ["cloud_sql", "alloydb"])
async def test_real_asyncpg_create_pool_with_connector_wrapper(connector_kind: str) -> None:
    """Real asyncpg.create_pool invokes managed connector wrappers with pool kwargs without TypeError."""
    captured: list[dict[str, Any]] = []
    fake_conn = _FakePoolConnection()
    mock_connector = MagicMock()

    async def fake_connect(**kwargs: Any) -> _FakePoolConnection:
        captured.append(kwargs)
        return fake_conn

    if connector_kind == "cloud_sql":
        mock_connector.connect_async = AsyncMock(side_effect=fake_connect)
        patch_target = "sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED"
        features: dict[str, Any] = {"enable_cloud_sql": True, "cloud_sql_instance": "p:r:i"}
        expected_key = ("instance_connection_string", "p:r:i")
    else:
        mock_connector.connect = AsyncMock(side_effect=fake_connect)
        patch_target = "sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED"
        features = {"enable_alloydb": True, "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i"}
        expected_key = ("instance_uri", "projects/p/locations/l/clusters/c/instances/i")

    with patch(patch_target, True):
        config = AsyncpgConfig(
            connection_config={"user": "u", "password": "p", "database": "d"}, driver_features=features
        )
        if connector_kind == "cloud_sql":
            config._cloud_sql_connector = mock_connector
            wrapper: Any = _AsyncpgCloudSqlConnector(config, "u", "p", "d")
        else:
            config._alloydb_connector = mock_connector
            wrapper = _AsyncpgAlloydbConnector(config, "u", "p", "d")

        pool = await asyncpg.create_pool(
            connect=cast("Any", wrapper),
            min_size=1,
            max_size=1,
            connection_class=_FakePoolConnection,
            timeout=12,
            statement_cache_size=0,
            command_timeout=15,
            server_settings={"application_name": "sqlspec-test"},
        )
        try:
            assert pool is not None
            assert len(captured) == 1
            call_kwargs = captured[0]
            assert call_kwargs[expected_key[0]] == expected_key[1]
            assert call_kwargs["driver"] == "asyncpg"
            assert call_kwargs["user"] == "u"
            assert call_kwargs["password"] == "p"
            assert call_kwargs["db"] == "d"
            assert call_kwargs["connection_class"] is _FakePoolConnection
            assert call_kwargs["loop"] is asyncio.get_running_loop()
            assert call_kwargs["record_class"] is asyncpg.Record
            assert call_kwargs["timeout"] == 12
            assert call_kwargs["statement_cache_size"] == 0
            assert call_kwargs["command_timeout"] == 15
            assert call_kwargs["server_settings"] == {"application_name": "sqlspec-test"}
        finally:
            await pool.close()


@pytest.mark.parametrize("connector_kind", ["cloud_sql", "alloydb"])
async def test_connector_wrappers_strip_reserved_and_none_kwargs(connector_kind: str) -> None:
    """Connector-owned args like ssl/direct_tls/host/port/dsn and None values cannot clobber connector calls."""
    captured: list[dict[str, Any]] = []

    async def fake_call(**kwargs: Any) -> MagicMock:
        captured.append(kwargs)
        return MagicMock()

    mock_connector = MagicMock()
    mock_connector.connect = AsyncMock(side_effect=fake_call)
    mock_connector.connect_async = AsyncMock(side_effect=fake_call)

    if connector_kind == "cloud_sql":
        with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
            config = AsyncpgConfig(
                connection_config={"user": "u", "password": "p", "database": "d"},
                driver_features={"enable_cloud_sql": True, "cloud_sql_instance": "p:r:i"},
            )
            config._cloud_sql_connector = mock_connector
            wrapper: Any = _AsyncpgCloudSqlConnector(config, "u", "p", "d")
    else:
        with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
            config = AsyncpgConfig(
                connection_config={"user": "u", "password": "p", "database": "d"},
                driver_features={
                    "enable_alloydb": True,
                    "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i",
                },
            )
            config._alloydb_connector = mock_connector
            wrapper = _AsyncpgAlloydbConnector(config, "u", "p", "d")

    await wrapper(
        "postgresql://ignored",
        dsn="postgresql://ignored",
        host="bad-host",
        port=9999,
        ssl="require",
        direct_tls=False,
        driver="psycopg",
        user="override",
        password="override",
        db="override",
        database="override",
        instance_uri="bad",
        instance_connection_string="bad",
        enable_iam_auth=True,
        ip_type="PUBLIC",
        timeout=None,
        command_timeout=30,
        statement_cache_size=0,
    )

    assert len(captured) == 1
    kwargs = captured[0]
    for forbidden in ("dsn", "host", "port", "ssl", "direct_tls", "database", "timeout"):
        assert forbidden not in kwargs
    assert kwargs["driver"] == "asyncpg"
    assert kwargs["user"] == "u"
    assert kwargs["password"] == "p"
    assert kwargs["db"] == "d"
    assert kwargs["enable_iam_auth"] is False
    assert kwargs["ip_type"] == "PRIVATE"
    assert kwargs["command_timeout"] == 30
    assert kwargs["statement_cache_size"] == 0


async def test_create_connection_forwards_options_to_alloydb_and_keeps_custom_connect_zero_arg(
    mock_alloydb_module: MagicMock,
) -> None:
    """create_connection forwards safe options to managed connector wrappers but calls custom connect() with no args."""
    fake_conn = _FakePoolConnection()
    captured: list[dict[str, Any]] = []

    async def fake_connect(**kwargs: Any) -> _FakePoolConnection:
        captured.append(kwargs)
        return fake_conn

    mock_connector = MagicMock()
    mock_connector.connect = AsyncMock(side_effect=fake_connect)
    mock_alloydb_module.return_value = mock_connector

    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = AsyncpgConfig(
            connection_config={"user": "u", "password": "p", "database": "d", "command_timeout": 25},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i",
                "enable_json_codecs": False,
                "enable_pgvector": False,
                "enable_paradedb": False,
                "enable_pg_textsearch": False,
                "pooler_mode": "transaction",
            },
        )
        conn = await config.create_connection()
        assert conn is fake_conn
        assert captured[0]["command_timeout"] == 25
        assert captured[0]["statement_cache_size"] == 0

    custom_calls = 0

    async def custom_zero_arg_connect() -> _FakePoolConnection:
        nonlocal custom_calls
        custom_calls += 1
        return fake_conn

    plain_config = AsyncpgConfig(
        connection_config={"connect": custom_zero_arg_connect, "command_timeout": 25},
        driver_features={
            "enable_json_codecs": False,
            "enable_pgvector": False,
            "enable_paradedb": False,
            "enable_pg_textsearch": False,
        },
    )
    plain_conn = await plain_config.create_connection()
    assert plain_conn is fake_conn
    assert custom_calls == 1


@pytest.mark.parametrize(
    "make_error",
    [
        lambda: _build_api_error(lambda m: m.DeadlineExceeded("504 gateway timeout")),
        lambda: _build_api_error(lambda m: m.ServiceUnavailable("503 unavailable")),
        lambda: _build_api_error(lambda m: m.TooManyRequests("429 too many requests")),
        lambda: _build_api_error(lambda m: m.ResourceExhausted("429 quota exceeded")),
        lambda: _build_api_error(lambda m: m.InternalServerError("500 internal")),
        lambda: _build_api_error(lambda m: m.Aborted("aborted")),
        lambda: _build_api_error(lambda m: m.RetryError("deadline", cause=m.DeadlineExceeded("504"))),
        lambda: _build_api_error(lambda m: m.RetryError("deadline", cause=None)),
        lambda: _build_api_error(lambda m: m.RetryError("opaque", cause=RuntimeError("opaque"))),
        lambda: _FakeGrpcStatusError("DEADLINE_EXCEEDED"),
        lambda: _FakeGrpcStatusError("UNAVAILABLE"),
        lambda: _FakeGrpcStatusError("RESOURCE_EXHAUSTED"),
        lambda: _FakeGrpcStatusError("ABORTED"),
        lambda: _FakeGrpcStatusError("StatusCode.DEADLINE_EXCEEDED", use_enum_obj=False),
        lambda: TimeoutError("timed out"),
        lambda: ConnectionResetError("reset by peer"),
        lambda: ConnectionRefusedError("refused"),
        lambda: BrokenPipeError("broken pipe"),
        lambda: _build_auth_error(lambda m: m.TransportError("metadata timeout")),
        lambda: _make_aiohttp_response_error(429),
        lambda: _make_aiohttp_response_error(500),
        lambda: _make_aiohttp_response_error(502),
        lambda: _make_aiohttp_response_error(503),
        lambda: _make_aiohttp_response_error(504),
        lambda: _build_aiohttp_error(lambda m: m.ServerTimeoutError("timeout")),
        lambda: _build_aiohttp_error(lambda m: m.ServerDisconnectedError("disconnected")),
    ],
    ids=[
        "api-deadline",
        "api-unavailable",
        "api-429",
        "api-exhausted",
        "api-500",
        "api-aborted",
        "api-retry-deadline",
        "api-retry-bare",
        "api-retry-opaque",
        "grpc-deadline",
        "grpc-unavailable",
        "grpc-exhausted",
        "grpc-aborted",
        "grpc-str-deadline",
        "timeout",
        "conn-reset",
        "conn-refused",
        "broken-pipe",
        "auth-transport",
        "aiohttp-429",
        "aiohttp-500",
        "aiohttp-502",
        "aiohttp-503",
        "aiohttp-504",
        "aiohttp-timeout",
        "aiohttp-disconnected",
    ],
)
def test_is_transient_connector_error_classifies_transient(make_error: Callable[[], BaseException]) -> None:
    """Transient control-plane and transport failures are classified as retryable."""
    assert is_transient_connector_error(make_error()) is True


@pytest.mark.parametrize(
    "make_error",
    [
        lambda: _build_api_error(lambda m: m.PermissionDenied("denied")),
        lambda: _build_api_error(lambda m: m.Unauthenticated("unauthenticated")),
        lambda: _build_api_error(lambda m: m.NotFound("missing instance")),
        lambda: _build_api_error(lambda m: m.InvalidArgument("bad arg")),
        lambda: _build_api_error(lambda m: m.FailedPrecondition("failed precondition")),
        lambda: _build_api_error(lambda m: m.Conflict("conflict")),
        lambda: _build_api_error(lambda m: m.AlreadyExists("already exists")),
        lambda: _build_api_error(lambda m: m.MethodNotImplemented("unimplemented")),
        lambda: _build_api_error(lambda m: m.PreconditionFailed("precondition failed")),
        lambda: _build_api_error(lambda m: m.DataLoss("data loss")),
        lambda: _build_api_error(lambda m: m.DuplicateCredentialArgs("duplicate creds")),
        lambda: _build_api_error(lambda m: m.RetryError("retry failed", cause=m.PermissionDenied("denied"))),
        lambda: _FakeGrpcStatusError("PERMISSION_DENIED"),
        lambda: _FakeGrpcStatusError("UNAUTHENTICATED"),
        lambda: _FakeGrpcStatusError("NOT_FOUND"),
        lambda: _FakeGrpcStatusError("INVALID_ARGUMENT"),
        lambda: _make_aiohttp_response_error(400),
        lambda: _make_aiohttp_response_error(401),
        lambda: _make_aiohttp_response_error(403),
        lambda: _make_aiohttp_response_error(404),
        _make_aiohttp_ssl_error,
        lambda: _build_aiohttp_error(lambda m: m.ServerFingerprintMismatch(b"a", b"b", "host", 443)),
        lambda: asyncpg.exceptions.InvalidPasswordError("bad password"),
        lambda: asyncpg.exceptions.InvalidCatalogNameError("unknown db"),
        lambda: asyncpg.InterfaceError("bad interface"),
        lambda: ImproperConfigurationError("bad config"),
        lambda: MissingDependencyError(package="pkg"),
        lambda: ValueError("bad value"),
        lambda: TypeError("bad type"),
        lambda: KeyError("missing"),
        lambda: _build_auth_error(lambda m: m.DefaultCredentialsError("missing creds")),
        lambda: _build_auth_error(lambda m: m.RefreshError("invalid_grant")),
        asyncio.CancelledError,
    ],
    ids=[
        "api-permission-denied",
        "api-unauthenticated",
        "api-not-found",
        "api-invalid-arg",
        "api-precondition",
        "api-conflict",
        "api-already-exists",
        "api-method-not-implemented",
        "api-precondition-failed",
        "api-data-loss",
        "api-duplicate-creds",
        "api-retry-perm-cause",
        "grpc-permission-denied",
        "grpc-unauthenticated",
        "grpc-not-found",
        "grpc-invalid-arg",
        "aiohttp-400",
        "aiohttp-401",
        "aiohttp-403",
        "aiohttp-404",
        "aiohttp-ssl",
        "aiohttp-fingerprint-mismatch",
        "asyncpg-invalid-password",
        "asyncpg-invalid-catalog",
        "asyncpg-interface",
        "improper-config",
        "missing-dep",
        "value-error",
        "type-error",
        "key-error",
        "auth-default-creds",
        "auth-refresh-non-retryable",
        "cancelled",
    ],
)
def test_is_transient_connector_error_rejects_permanent(make_error: Callable[[], BaseException]) -> None:
    """Permanent auth, config, database, TLS, and cancellation failures fail fast without retry."""
    assert is_transient_connector_error(make_error()) is False


def test_is_transient_connector_error_handles_cause_chains_and_module_qualified_exceptions() -> None:
    """Wrapped permanent exceptions fail fast while wrappers around transient causes are retried."""
    api_exc = pytest.importorskip("google.api_core.exceptions")
    auth_exc = pytest.importorskip("google.auth.exceptions")

    wrapped_perm = api_exc.PermissionDenied("denied")
    wrapped_perm.__cause__ = TimeoutError("inner timeout")
    assert is_transient_connector_error(wrapped_perm) is False

    retryable_auth = auth_exc.RefreshError("transient token issue", retryable=True)
    assert is_transient_connector_error(retryable_auth) is True

    wrapped_transient = RuntimeError("refresh failed")
    wrapped_transient.__cause__ = api_exc.DeadlineExceeded("504")
    assert is_transient_connector_error(wrapped_transient) is True

    cyclic_retry = api_exc.RetryError("cyclic", cause=None)
    cyclic_retry._cause = cyclic_retry
    assert is_transient_connector_error(cyclic_retry) is True

    cloud_sql_exc = pytest.importorskip("google.cloud.sql.connector.exceptions")

    assert is_transient_connector_error(cloud_sql_exc.ClosedConnectorError("closed")) is False
    assert is_transient_connector_error(cloud_sql_exc.CloudSQLIPTypeError("bad ip")) is False
    assert is_transient_connector_error(cloud_sql_exc.TLSVersionError("bad tls")) is False
    assert is_transient_connector_error(cloud_sql_exc.CacheClosedError("closed")) is False
    assert is_transient_connector_error(cloud_sql_exc.DnsLoopError("loop")) is False
    assert is_transient_connector_error(cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", None)) is True
    assert (
        is_transient_connector_error(
            cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", RuntimeError("opaque cooldown"))
        )
        is True
    )
    assert (
        is_transient_connector_error(
            cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", api_exc.PermissionDenied("denied"))
        )
        is False
    )
    nested_perm = RuntimeError("outer wrapper")
    nested_perm.__cause__ = api_exc.PermissionDenied("denied")
    assert is_transient_connector_error(cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", nested_perm)) is False
    retry_perm = api_exc.RetryError("retry failed", cause=api_exc.PermissionDenied("denied"))
    assert is_transient_connector_error(cloud_sql_exc.ResourceExhaustedError("busy", "p:r:i", retry_perm)) is False

    cyclic_exhausted = cloud_sql_exc.ResourceExhaustedError("cyclic", "p:r:i", None)
    cyclic_exhausted.raw_error = cyclic_exhausted
    assert is_transient_connector_error(cyclic_exhausted) is True

    alloydb_exc = pytest.importorskip("google.cloud.alloydbconnector.exceptions")
    assert is_transient_connector_error(alloydb_exc.ClosedConnectorError("closed")) is False
    assert is_transient_connector_error(alloydb_exc.IPTypeNotFoundError("no private ip")) is False
    refresh_transient = alloydb_exc.RefreshError("refresh timed out")
    refresh_transient.__cause__ = api_exc.DeadlineExceeded("504")
    assert is_transient_connector_error(refresh_transient) is True
    refresh_perm = alloydb_exc.RefreshError("refresh denied")
    refresh_perm.__cause__ = api_exc.PermissionDenied("denied")
    assert is_transient_connector_error(refresh_perm) is False


def test_calculate_connector_backoff_seconds_bounds() -> None:
    """Backoff returns zero for non-positive inputs and stays within jittered capped half-interval."""
    assert calculate_connector_backoff_seconds(0, 0.0, 5.0) == 0.0
    assert calculate_connector_backoff_seconds(0, 0.5, 0.0) == 0.0
    delay_0 = calculate_connector_backoff_seconds(0, 0.5, 5.0)
    assert 0.25 <= delay_0 <= 0.5
    delay_2 = calculate_connector_backoff_seconds(2, 0.5, 5.0)
    assert 1.0 <= delay_2 <= 2.0
    delay_high = calculate_connector_backoff_seconds(10, 0.5, 5.0)
    assert 2.5 <= delay_high <= 5.0


@pytest.mark.parametrize("connector_kind", ["cloud_sql", "alloydb"])
async def test_connector_retries_transient_error_and_recovers(connector_kind: str) -> None:
    """Transient connector failures retry with backoff and return the established connection."""
    fake_conn = _FakePoolConnection()
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    if api_exceptions is None:
        pytest.skip("google-api-core is not installed")
    mock_connector = MagicMock()
    side_effects = [api_exceptions.DeadlineExceeded("504"), api_exceptions.ServiceUnavailable("503"), fake_conn]

    with patch("sqlspec.adapters.asyncpg.config.asyncio.sleep", side_effect=fake_sleep):
        if connector_kind == "cloud_sql":
            mock_connector.connect_async = AsyncMock(side_effect=side_effects)
            with patch("sqlspec.adapters.asyncpg.config.CLOUD_SQL_CONNECTOR_INSTALLED", True):
                config = AsyncpgConfig(
                    connection_config={"user": "u", "password": "p", "database": "d"},
                    driver_features={
                        "enable_cloud_sql": True,
                        "cloud_sql_instance": "p:r:i",
                        "connector_max_retries": 3,
                        "connector_retry_backoff_seconds": 0.2,
                        "connector_retry_max_backoff_seconds": 1.0,
                    },
                )
                config._cloud_sql_connector = mock_connector
                wrapper: Any = _AsyncpgCloudSqlConnector(config, "u", "p", "d")
                result = await wrapper()
                assert mock_connector.connect_async.await_count == 3
        else:
            mock_connector.connect = AsyncMock(side_effect=side_effects)
            with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
                config = AsyncpgConfig(
                    connection_config={"user": "u", "password": "p", "database": "d"},
                    driver_features={
                        "enable_alloydb": True,
                        "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i",
                        "connector_max_retries": 3,
                        "connector_retry_backoff_seconds": 0.2,
                        "connector_retry_max_backoff_seconds": 1.0,
                    },
                )
                config._alloydb_connector = mock_connector
                wrapper = _AsyncpgAlloydbConnector(config, "u", "p", "d")
                result = await wrapper()
                assert mock_connector.connect.await_count == 3

    assert result is fake_conn
    assert len(sleep_delays) == 2
    assert 0.1 <= sleep_delays[0] <= 0.2
    assert 0.2 <= sleep_delays[1] <= 0.4


@pytest.mark.parametrize("max_retries", [0, 2])
async def test_alloydb_connector_exhausts_retries_and_reraises(max_retries: int) -> None:
    """Persistent transient errors stop after max_retries + 1 attempts and re-raise."""
    if api_exceptions is None:
        pytest.skip("google-api-core is not installed")
    mock_connector = MagicMock()
    err = api_exceptions.DeadlineExceeded("still timing out")
    mock_connector.connect = AsyncMock(side_effect=err)

    with patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True):
        config = AsyncpgConfig(
            connection_config={"user": "u", "password": "p", "database": "d"},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i",
                "connector_max_retries": max_retries,
                "connector_retry_backoff_seconds": 0.0,
                "connector_retry_max_backoff_seconds": 0.0,
            },
        )
        config._alloydb_connector = mock_connector
        wrapper = _AsyncpgAlloydbConnector(config, "u", "p", "d")

        with pytest.raises(api_exceptions.DeadlineExceeded, match="still timing out"):
            await wrapper()

    assert mock_connector.connect.await_count == max_retries + 1


async def test_alloydb_connector_fails_fast_on_permanent_error() -> None:
    """Permanent errors are not retried."""
    if api_exceptions is None:
        pytest.skip("google-api-core is not installed")
    mock_connector = MagicMock()
    mock_connector.connect = AsyncMock(side_effect=api_exceptions.PermissionDenied("no iam role"))

    with (
        patch("sqlspec.adapters.asyncpg.config.ALLOYDB_CONNECTOR_INSTALLED", True),
        patch("sqlspec.adapters.asyncpg.config.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
    ):
        config = AsyncpgConfig(
            connection_config={"user": "u", "password": "p", "database": "d"},
            driver_features={
                "enable_alloydb": True,
                "alloydb_instance_uri": "projects/p/locations/l/clusters/c/instances/i",
                "connector_max_retries": 3,
            },
        )
        config._alloydb_connector = mock_connector
        wrapper = _AsyncpgAlloydbConnector(config, "u", "p", "d")

        with pytest.raises(api_exceptions.PermissionDenied, match="no iam role"):
            await wrapper()

    assert mock_connector.connect.await_count == 1
    mock_sleep.assert_not_awaited()


def test_asyncpg_adapter_import_does_not_eagerly_load_google_or_aiohttp() -> None:
    """Importing sqlspec.adapters.asyncpg stays isolated from optional cloud RPC dependencies."""
    probe = (
        "import sys; "
        "import sqlspec.adapters.asyncpg; "
        "loaded = [m for m in ('google.api_core', 'google.auth', 'grpc', 'aiohttp') if m in sys.modules]; "
        "assert not loaded, f'unexpectedly imported: {loaded}'"
    )
    proc = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=False, timeout=30)
    assert proc.returncode == 0, proc.stderr
