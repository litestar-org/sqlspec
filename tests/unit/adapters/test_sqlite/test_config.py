"""SQLite configuration normalization tests."""

from os import PathLike
from pathlib import Path
from typing import Literal, get_args, get_origin, get_type_hints

import pytest

import sqlspec.adapters.sqlite.core as sqlite_core
from sqlspec.adapters.sqlite._typing import SqliteConnectionFactory
from sqlspec.adapters.sqlite.config import SqliteConfig, SqliteConnectionParams, SqliteDriverFeatures
from sqlspec.adapters.sqlite.core import build_connection_config
from sqlspec.exceptions import ImproperConfigurationError


def _annotation_contains(annotation: object, expected: object) -> bool:
    """Return whether an annotation tree contains the expected object."""
    if annotation is expected or annotation == expected:
        return True
    return any(_annotation_contains(arg, expected) for arg in get_args(annotation))


def _annotation_has_origin(annotation: object, expected: object) -> bool:
    """Return whether an annotation tree contains a node with the expected origin."""
    if get_origin(annotation) is expected:
        return True
    return any(_annotation_has_origin(arg, expected) for arg in get_args(annotation))


def test_build_connection_config_preserves_explicit_isolation_level_none() -> None:
    """isolation_level=None must reach sqlite3.connect() to disable implicit transactions."""
    connection_config = build_connection_config({"database": ":memory:", "isolation_level": None})

    assert connection_config["isolation_level"] is None


def test_build_connection_config_drops_other_none_values() -> None:
    """None-valued optional settings should still be omitted unless sqlite3 treats None as meaningful."""
    connection_config = build_connection_config({
        "database": ":memory:",
        "timeout": None,
        "factory": None,
        "isolation_level": None,
    })

    assert connection_config == {"database": ":memory:", "isolation_level": None}


def test_build_connection_config_merges_extra_as_driver_kwargs() -> None:
    """extra should be a normalized escape hatch for sqlite3.connect() keyword arguments."""
    connection_config = build_connection_config({
        "database": ":memory:",
        "timeout": 1.0,
        "extra": {"cached_statements": 32, "timeout": 2.0, "uri": None},
    })

    assert connection_config == {"database": ":memory:", "cached_statements": 32, "timeout": 2.0}


def test_build_connection_config_gates_autocommit_on_python_support(monkeypatch: pytest.MonkeyPatch) -> None:
    """autocommit should only be passed to sqlite3.connect() on Python versions that support it."""
    monkeypatch.setattr(sqlite_core, "SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT", False, raising=False)

    connection_config = build_connection_config({"database": ":memory:", "autocommit": True})

    assert "autocommit" not in connection_config


@pytest.mark.skipif(
    not getattr(sqlite_core, "SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT", True),
    reason="sqlite3.connect() autocommit is available on Python 3.12+",
)
def test_build_connection_config_keeps_autocommit_when_supported() -> None:
    """Supported runtimes should forward the modern sqlite3 autocommit argument."""
    connection_config = build_connection_config({"database": ":memory:", "autocommit": True})

    assert connection_config["autocommit"] is True


def test_sqlite_connection_params_describe_current_sync_config_surface() -> None:
    """TypedDict coverage should include current sqlite3 and SQLSpec sync pool settings."""
    annotations = get_type_hints(SqliteConnectionParams, include_extras=True)

    assert _annotation_has_origin(annotations["database"], PathLike)
    assert _annotation_contains(annotations["factory"], SqliteConnectionFactory)
    assert _annotation_contains(annotations["isolation_level"], type(None))
    assert _annotation_has_origin(annotations["isolation_level"], Literal)
    assert annotations["autocommit"] is not None
    assert annotations["pool_recycle_seconds"] is not None
    assert annotations["health_check_interval"] is not None
    assert annotations["enable_optimizations"] is not None
    assert annotations["extra"] is not None


def test_sqlite_config_accepts_pathlike_database(tmp_path: Path) -> None:
    """PathLike database values should be preserved for sqlite3.connect()."""
    database_path = tmp_path / "sqlspec.sqlite"
    config = SqliteConfig(connection_config={"database": database_path, "enable_optimizations": False})

    connection_config = build_connection_config(config.connection_config)

    assert connection_config["database"] is database_path
    with config.provide_session() as session:
        session.execute("CREATE TABLE pathlike_config_test (id INTEGER PRIMARY KEY)")
    config.close_pool()
    assert database_path.exists()


def test_driver_features_runtime_keys_declared() -> None:
    """SqliteDriverFeatures should expose the runtime feature keys used by the worksheet."""
    annotations = SqliteDriverFeatures.__annotations__

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
    """Configured extensions should be retained in runtime setup for later pool application."""
    config = SqliteConfig(driver_features={"extensions": ["/tmp/ext.so"]})

    assert config._runtime_setup is not None
    assert config._runtime_setup["extensions"] == ["/tmp/ext.so"]


def test_pragma_name_rejects_injection() -> None:
    """PRAGMA names must reject SQL injection characters."""
    with pytest.raises(ImproperConfigurationError, match="PRAGMA name"):
        SqliteConfig(driver_features={"pragmas": {"journal_mode; DROP TABLE x": "WAL"}})


def test_pragma_value_rejects_injection() -> None:
    """PRAGMA values must reject SQL injection characters."""
    with pytest.raises(ImproperConfigurationError, match="PRAGMA value"):
        SqliteConfig(driver_features={"pragmas": {"journal_mode": "WAL; DROP TABLE x"}})


def test_pragma_values_rendered() -> None:
    """PRAGMA values should render to SQLite-safe string payloads."""
    config = SqliteConfig(
        driver_features={"pragmas": {"cache_size": -16000, "foreign_keys": True, "journal_mode": "WAL"}}
    )

    assert config._runtime_setup is not None
    assert config._runtime_setup["pragmas"] == [
        ("cache_size", "-16000"),
        ("foreign_keys", "1"),
        ("journal_mode", "WAL"),
    ]


def test_row_factory_literal_validated() -> None:
    """Unknown row_factory literals should be rejected."""
    with pytest.raises(ImproperConfigurationError, match="row_factory"):
        SqliteConfig(driver_features={"row_factory": "bogus"})


def test_row_factory_non_callable_rejected() -> None:
    """Non-callable row_factory values should be rejected."""
    with pytest.raises(ImproperConfigurationError, match="row_factory"):
        SqliteConfig(driver_features={"row_factory": 123})


def test_row_factory_dict_literal_accepted() -> None:
    """The dict row_factory literal should be accepted and preserved in runtime setup."""
    config = SqliteConfig(driver_features={"row_factory": "dict"})

    assert config._runtime_setup is not None
    assert config._runtime_setup["row_factory"] == "dict"


def test_custom_function_entry_requires_keys() -> None:
    """Custom function entries must include the required registration keys."""
    with pytest.raises(ImproperConfigurationError, match="custom_functions"):
        SqliteConfig(driver_features={"custom_functions": [{"name": "f"}]})


def test_custom_collation_entry_requires_keys() -> None:
    """Custom collation entries must include the required registration keys."""
    with pytest.raises(ImproperConfigurationError, match="custom_collations"):
        SqliteConfig(driver_features={"custom_collations": [{"name": "sort_name"}]})


def test_custom_aggregate_entry_requires_keys() -> None:
    """Custom aggregate entries must include the required registration keys."""
    with pytest.raises(ImproperConfigurationError, match="custom_aggregates"):
        SqliteConfig(driver_features={"custom_aggregates": [{"name": "agg", "narg": 1}]})


def test_progress_handler_interval_must_be_positive() -> None:
    """Progress handler intervals must be positive integers."""
    with pytest.raises(ImproperConfigurationError, match="progress_handler_interval"):
        SqliteConfig(driver_features={"progress_handler": lambda: None, "progress_handler_interval": 0})


def test_runtime_keys_removed_from_driver_features() -> None:
    """Runtime feature keys should be removed from the stored driver_features dict."""
    config = SqliteConfig(
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
