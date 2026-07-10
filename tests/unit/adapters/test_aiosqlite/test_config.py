import sqlite3
from pathlib import Path
from typing import get_args, get_type_hints

import pytest

from sqlspec.adapters.aiosqlite._typing import AiosqliteConnectionFactory
from sqlspec.adapters.aiosqlite.config import (
    AiosqliteConfig,
    AiosqliteConnectionParams,
    AiosqliteDriverFeatures,
    AiosqlitePoolParams,
)
from sqlspec.adapters.aiosqlite.core import build_connection_config
from sqlspec.exceptions import ImproperConfigurationError


class CustomConnection(sqlite3.Connection):
    """Connection subclass used to validate factory passthrough."""


def _annotation_contains(annotation: object, expected: object) -> bool:
    """Return whether an annotation tree contains the expected object."""
    if annotation is expected:
        return True
    return any(_annotation_contains(arg, expected) for arg in get_args(annotation))


def test_connection_params_accept_pathlike_and_modern_connection_options(tmp_path: Path) -> None:
    db_path = tmp_path / "typed.db"

    params: AiosqliteConnectionParams = {
        "database": db_path,
        "factory": CustomConnection,
        "iter_chunk_size": 128,
        "autocommit": True,
        "isolation_level": None,
    }

    assert params["database"] == db_path


def test_connection_params_factory_uses_adapter_alias() -> None:
    annotations = get_type_hints(AiosqliteConnectionParams, include_extras=True)

    assert _annotation_contains(annotations["factory"], AiosqliteConnectionFactory)


def test_build_connection_config_filters_pool_keys_and_gates_autocommit(tmp_path: Path) -> None:
    db_path = tmp_path / "config.db"

    connection_config = build_connection_config({
        "database": db_path,
        "timeout": 1.5,
        "isolation_level": None,
        "factory": CustomConnection,
        "iter_chunk_size": 256,
        "autocommit": True,
        "min_size": 2,
        "health_check_interval": 0.25,
        "pool_size": 4,
        "connect_timeout": 0.5,
        "idle_timeout": 1.0,
        "operation_timeout": 1.5,
        "pool_recycle_seconds": 2.0,
        "enable_optimizations": False,
        "enable_foreign_keys": True,
        "extra": {"ignored": True},
    })

    assert connection_config["database"] == db_path
    assert connection_config["timeout"] == 1.5
    assert connection_config["isolation_level"] is None
    assert connection_config["factory"] is CustomConnection
    assert connection_config["iter_chunk_size"] == 256

    assert "min_size" not in connection_config
    assert "health_check_interval" not in connection_config
    assert "pool_size" not in connection_config
    assert "connect_timeout" not in connection_config
    assert "idle_timeout" not in connection_config
    assert "operation_timeout" not in connection_config
    assert "pool_recycle_seconds" not in connection_config
    assert "enable_optimizations" not in connection_config
    assert "enable_foreign_keys" not in connection_config
    assert "extra" not in connection_config

    if hasattr(sqlite3, "LEGACY_TRANSACTION_CONTROL"):
        assert connection_config["autocommit"] is True
    else:
        assert "autocommit" not in connection_config


async def test_create_pool_routes_pool_settings_without_leaking_to_connect_kwargs(tmp_path: Path) -> None:
    config = AiosqliteConfig(
        connection_config={
            "database": tmp_path / "pool.db",
            "pool_size": 4,
            "min_size": 2,
            "health_check_interval": 0.25,
            "iter_chunk_size": 512,
            "isolation_level": None,
            "enable_optimizations": False,
            "enable_foreign_keys": True,
        }
    )

    pool = await config._create_pool()
    try:
        assert pool._pool_size == 4
        assert pool._min_size == 2
        assert pool._health_check_interval == 0.25
        assert pool._enable_optimizations is False
        assert pool._enable_foreign_keys is True
        assert pool._connection_parameters["iter_chunk_size"] == 512
        assert pool._connection_parameters["isolation_level"] is None
        assert "min_size" not in pool._connection_parameters
        assert "health_check_interval" not in pool._connection_parameters
        assert "enable_optimizations" not in pool._connection_parameters
        assert "enable_foreign_keys" not in pool._connection_parameters
    finally:
        await pool.close()


def test_config_accepts_pathlike_database_without_coercing_connection_value(tmp_path: Path) -> None:
    db_path = tmp_path / "pathlike.db"

    config = AiosqliteConfig(connection_config={"database": db_path})

    assert config.connection_config["database"] == db_path
    assert isinstance(config.connection_config["database"], Path)


def test_pool_params_declare_pragma_flags() -> None:
    annotations = get_type_hints(AiosqlitePoolParams, include_extras=True)

    assert annotations["enable_optimizations"] is not None
    assert annotations["enable_foreign_keys"] is not None


def test_driver_features_runtime_keys_declared() -> None:
    annotations = AiosqliteDriverFeatures.__annotations__

    assert {
        "custom_functions",
        "custom_collations",
        "custom_aggregates",
        "authorizer_callback",
        "trace_callback",
        "progress_handler",
        "progress_handler_interval",
        "row_factory",
        "text_factory",
        "pragmas",
        "extensions",
    } <= set(annotations)
    assert "allow_extension_loading" not in annotations


def test_runtime_setup_exposes_extensions_without_gate() -> None:
    config = AiosqliteConfig(driver_features={"extensions": ["/tmp/ext.so"]})

    assert config._runtime_setup is not None
    assert config._runtime_setup["extensions"] == ["/tmp/ext.so"]


def test_pragma_name_rejects_injection() -> None:
    with pytest.raises(ImproperConfigurationError, match="PRAGMA name"):
        AiosqliteConfig(driver_features={"pragmas": {"journal_mode; DROP TABLE x": "WAL"}})


def test_pragma_value_rejects_injection() -> None:
    with pytest.raises(ImproperConfigurationError, match="PRAGMA value"):
        AiosqliteConfig(driver_features={"pragmas": {"journal_mode": "WAL; DROP TABLE x"}})


def test_pragma_values_rendered() -> None:
    config = AiosqliteConfig(
        driver_features={"pragmas": {"cache_size": -16000, "foreign_keys": True, "journal_mode": "WAL"}}
    )

    assert config._runtime_setup is not None
    assert config._runtime_setup["pragmas"] == [
        ("cache_size", "-16000"),
        ("foreign_keys", "1"),
        ("journal_mode", "WAL"),
    ]


def test_row_factory_literal_validated() -> None:
    with pytest.raises(ImproperConfigurationError, match="row_factory"):
        AiosqliteConfig(driver_features={"row_factory": "bogus"})


def test_row_factory_non_callable_rejected() -> None:
    with pytest.raises(ImproperConfigurationError, match="row_factory"):
        AiosqliteConfig(driver_features={"row_factory": 123})


def test_row_factory_dict_literal_accepted() -> None:
    config = AiosqliteConfig(driver_features={"row_factory": "dict"})

    assert config._runtime_setup is not None
    assert config._runtime_setup["row_factory"] == "dict"


def test_custom_function_entry_requires_keys() -> None:
    with pytest.raises(ImproperConfigurationError, match="custom_functions"):
        AiosqliteConfig(driver_features={"custom_functions": [{"name": "f"}]})


def test_custom_collation_entry_requires_keys() -> None:
    with pytest.raises(ImproperConfigurationError, match="custom_collations"):
        AiosqliteConfig(driver_features={"custom_collations": [{"name": "sort_name"}]})


def test_custom_aggregate_entry_requires_keys() -> None:
    with pytest.raises(ImproperConfigurationError, match="custom_aggregates"):
        AiosqliteConfig(driver_features={"custom_aggregates": [{"name": "agg", "narg": 1}]})


def test_progress_handler_interval_must_be_positive() -> None:
    with pytest.raises(ImproperConfigurationError, match="progress_handler_interval"):
        AiosqliteConfig(driver_features={"progress_handler": lambda: None, "progress_handler_interval": 0})


def test_runtime_keys_removed_from_driver_features() -> None:
    config = AiosqliteConfig(
        driver_features={
            "custom_functions": [{"name": "double", "narg": 1, "func": lambda value: value}],
            "row_factory": "tuple",
            "pragmas": {"foreign_keys": True},
            "extensions": ["/tmp/ext.so"],
        }
    )

    assert "custom_functions" not in config.driver_features
    assert "row_factory" not in config.driver_features
    assert "pragmas" not in config.driver_features
    assert "extensions" not in config.driver_features
    assert config.driver_features["enable_custom_adapters"] is False
