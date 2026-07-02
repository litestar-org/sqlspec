"""pymssql configuration tests."""

from typing import get_type_hints

import pytest

from sqlspec.adapters.pymssql.config import PymssqlConfig, PymssqlConnectionParams
from sqlspec.adapters.pymssql.driver import PymssqlDriver
from sqlspec.adapters.pymssql.pool import PymssqlConnectionPool


def test_connection_params_cover_common_pymssql_keywords() -> None:
    """Typed connection params should cover the common pymssql connect kwargs."""
    annotations = get_type_hints(PymssqlConnectionParams, include_extras=True)

    expected_keys = {
        "server",
        "host",
        "user",
        "password",
        "database",
        "port",
        "timeout",
        "login_timeout",
        "charset",
        "as_dict",
        "appname",
        "conn_properties",
        "autocommit",
        "tds_version",
        "pool_recycle_seconds",
        "health_check_interval",
    }

    assert expected_keys <= set(annotations)


def test_config_defaults_server_port_and_features() -> None:
    """PymssqlConfig should normalize connection defaults and driver features."""
    config = PymssqlConfig(connection_config={}, driver_features={"enable_events": True})

    assert config.connection_config["server"] == "localhost"
    assert config.connection_config["port"] == 1433
    assert config.driver_type is PymssqlDriver
    assert config.supports_transactional_ddl is True
    assert config.supports_native_arrow_export is False
    assert config.driver_features["enable_events"] is True


def test_config_create_pool_splits_pool_options_and_hook() -> None:
    """Pool lifecycle options should not be forwarded to pymssql.connect."""
    seen: list[object] = []
    config = PymssqlConfig(
        connection_config={
            "server": "sql.example.test",
            "user": "sa",
            "password": "secret",
            "database": "app",
            "pool_recycle_seconds": 5,
            "health_check_interval": 0.25,
        },
        driver_features={"on_connection_create": seen.append},
    )

    pool = config.create_pool()

    assert isinstance(pool, PymssqlConnectionPool)
    assert pool._connection_parameters == {
        "server": "sql.example.test",
        "user": "sa",
        "password": "secret",
        "database": "app",
        "port": 1433,
    }
    assert pool._recycle_seconds == 5
    assert pool._health_check_interval == 0.25
    assert "on_connection_create" not in config.driver_features


def test_config_close_pool_clears_connection_instance() -> None:
    """Closing the config pool should clear the stored pool reference."""
    config = PymssqlConfig(connection_instance=PymssqlConnectionPool({}))

    config._close_pool()

    assert config.connection_instance is None


def test_signature_namespace_exposes_public_adapter_types() -> None:
    """Config signature namespaces should include public pymssql types."""
    config = PymssqlConfig(connection_config={"server": "localhost"})

    namespace = config.get_signature_namespace()

    assert namespace["PymssqlConfig"] is PymssqlConfig
    assert namespace["PymssqlConnectionParams"] is PymssqlConnectionParams
    assert namespace["PymssqlConnectionPool"] is PymssqlConnectionPool
    assert namespace["PymssqlDriver"] is PymssqlDriver


def test_pymssql_runtime_aliases_resolve_to_installed_classes() -> None:
    """pymssql public runtime aliases should expose installed pymssql classes."""
    pymssql = pytest.importorskip("pymssql")
    from sqlspec.adapters.pymssql import PymssqlConnection as PublicPymssqlConnection
    from sqlspec.adapters.pymssql._typing import PYMSSQL_MODULE, PymssqlConnection, PymssqlRawCursor

    namespace = PymssqlConfig().get_signature_namespace()

    assert PYMSSQL_MODULE is pymssql
    assert PymssqlConnection is pymssql.Connection
    assert PublicPymssqlConnection is pymssql.Connection
    assert PymssqlRawCursor is pymssql.Cursor
    assert PymssqlConfig.connection_type is pymssql.Connection
    assert namespace["PymssqlConnection"] is pymssql.Connection
    assert namespace["PymssqlRawCursor"] is pymssql.Cursor
    assert isinstance(object(), PymssqlConnection) is False
