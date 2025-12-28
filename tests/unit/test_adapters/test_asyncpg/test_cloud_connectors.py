"""Unit tests for Google Cloud SQL and AlloyDB connector integration in AsyncPG."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError

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

    yield mock_connector_class

    sys.modules.pop("google.cloud.sql", None)
    sys.modules.pop("google.cloud.sql.connector", None)


@pytest.fixture
def mock_alloydb_module():
    """Create and register mock google.cloud.alloydb module."""
    mock_connector_class = MagicMock()
    mock_module = MagicMock()
    mock_module.connector.AsyncConnector = mock_connector_class

    sys.modules["google.cloud.alloydb"] = mock_module
    sys.modules["google.cloud.alloydb.connector"] = mock_module.connector

    yield mock_connector_class

    sys.modules.pop("google.cloud.alloydb", None)
    sys.modules.pop("google.cloud.alloydb.connector", None)


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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
                    "alloydb_enable_iam_auth": True,
                },
            )

            await config._create_pool()
            get_conn_func = mock_create_pool.call_args.kwargs["connect"]
            await get_conn_func()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
