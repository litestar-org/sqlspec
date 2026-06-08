"""DuckDB configuration tests covering statement config builders."""

from typing import Any
from unittest.mock import patch

import duckdb

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.core import apply_driver_features, build_statement_config, default_statement_config
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


def test_driver_init_apply_driver_features_called_once_when_statement_config_is_none() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    with patch("sqlspec.adapters.duckdb.driver.apply_driver_features", wraps=apply_driver_features) as mock_apply:
        DuckDBDriver(connection=connection, statement_config=None, driver_features=driver_features)
    assert mock_apply.call_count == 1


def test_driver_init_apply_driver_features_not_called_when_statement_config_provided() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    with patch("sqlspec.adapters.duckdb.driver.apply_driver_features", wraps=apply_driver_features) as mock_apply:
        DuckDBDriver(connection=connection, statement_config=default_statement_config, driver_features=driver_features)
    assert mock_apply.call_count == 0


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

    with patch("sqlspec.adapters.duckdb.driver.apply_driver_features", wraps=apply_driver_features) as mock_apply:
        config = DuckDBConfig(
            connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_serializer}
        )
        with config.provide_session():
            pass
    assert mock_apply.call_count == 0
