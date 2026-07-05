"""DuckDB configuration tests covering statement config builders."""

from typing import Any, get_type_hints
from unittest.mock import patch
from uuid import UUID

import duckdb
from typing_extensions import NotRequired

from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBPoolParams,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.core import (
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
)
from sqlspec.adapters.duckdb.driver import DuckDBDriver


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)
    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_duckdb_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the DuckDB statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    config = DuckDBConfig(driver_features={"json_serializer": serializer})
    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_duckdb_config_honors_uuid_conversion_flag() -> None:
    """Driver features should control UUID string conversion."""

    uuid_str = "550e8400-e29b-41d4-a716-446655440000"

    enabled_config = DuckDBConfig(driver_features={"enable_uuid_conversion": True})
    disabled_config = DuckDBConfig(driver_features={"enable_uuid_conversion": False})

    enabled_converter = enabled_config.statement_config.parameter_config.type_coercion_map[str]
    disabled_converter = disabled_config.statement_config.parameter_config.type_coercion_map[str]

    assert isinstance(enabled_converter(uuid_str), UUID)
    assert disabled_converter(uuid_str) == uuid_str


def test_connection_param_types_match_current_duckdb_settings() -> None:
    """Typed config keys should match DuckDB 1.5 setting types."""

    hints = get_type_hints(DuckDBConnectionParams, include_extras=True)

    assert hints["parquet_metadata_cache"] == NotRequired[bool]
    assert hints["progress_bar_time"] == NotRequired[int]


def test_pool_params_do_not_type_noop_pool_sizing_keys() -> None:
    """Only pool settings consumed by DuckDBConnectionPool should remain typed."""

    hints = get_type_hints(DuckDBPoolParams, include_extras=True)

    assert "pool_min_size" not in hints
    assert "pool_max_size" not in hints
    assert "pool_timeout" not in hints
    assert hints["pool_recycle_seconds"] == NotRequired[int]
    assert hints["health_check_interval"] == NotRequired[float]


def test_extension_config_types_repository_url() -> None:
    """DuckDB install_extension supports repository_url as a distinct kwarg."""

    hints = get_type_hints(DuckDBExtensionConfig, include_extras=True)

    assert hints["repository_url"] == NotRequired[str]


def test_secret_config_types_scope_provider_persistence_and_optional_values() -> None:
    """Provider-based DuckDB secrets do not always need inline value pairs."""

    hints = get_type_hints(DuckDBSecretConfig, include_extras=True)

    assert hints["value"] == NotRequired[dict[str, Any]]
    assert hints["provider"] == NotRequired[str]
    assert hints["scope"] == NotRequired[str]
    assert hints["persistent"] == NotRequired[bool]


def test_build_connection_config_preserves_nested_config_and_extra() -> None:
    """DuckDB connect should receive database/read_only plus one merged config dict."""

    connection_config = {
        "database": "analytics.duckdb",
        "read_only": True,
        "config": {"threads": 1, "parquet_metadata_cache": True},
        "extra": {"custom_user_agent": "sqlspec-tests"},
        "progress_bar_time": 250,
        "pool_min_size": 2,
        "pool_max_size": 4,
        "pool_timeout": 10.0,
        "pool_recycle_seconds": 60,
        "health_check_interval": 5.0,
    }

    assert build_connection_config(connection_config) == {
        "database": "analytics.duckdb",
        "read_only": True,
        "config": {
            "threads": 1,
            "parquet_metadata_cache": True,
            "custom_user_agent": "sqlspec-tests",
            "progress_bar_time": 250,
        },
    }


def test_driver_init_does_not_apply_driver_features_when_statement_config_is_none() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    driver = DuckDBDriver(connection=connection, statement_config=None, driver_features=driver_features)

    assert driver.driver_features is not driver_features
    assert driver.driver_features == driver_features


def test_driver_init_apply_driver_features_not_called_when_statement_config_provided() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    driver = DuckDBDriver(
        connection=connection, statement_config=default_statement_config, driver_features=driver_features
    )

    assert driver.statement_config is default_statement_config
    assert driver.driver_features is not driver_features
    assert driver.driver_features == driver_features


def test_driver_init_custom_json_serializer_identity_preserved_through_session() -> None:

    def custom_serializer(_: object) -> str:
        return "custom"

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_serializer}
    )
    with config.provide_session() as driver:
        serializer_in_driver = driver.statement_config.parameter_config.json_serializer
    assert serializer_in_driver is custom_serializer


def test_driver_init_apply_driver_features_not_called_per_session_via_config() -> None:

    def custom_serializer(_: object) -> str:
        return "custom"

    with patch("sqlspec.adapters.duckdb.config.apply_driver_features", wraps=apply_driver_features) as mock_apply:
        config = DuckDBConfig(
            connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_serializer}
        )
        with config.provide_session():
            pass
    assert mock_apply.call_count == 1
