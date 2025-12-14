"""Unit tests for ADBC config normalization helpers."""

from typing import Any, cast

from sqlspec.adapters.adbc import AdbcConfig


def _resolve_driver_name(config: AdbcConfig) -> str:
    """Call the internal driver-name resolver without triggering pyright private usage."""
    return cast("str", cast("Any", config)._resolve_driver_name())


def _get_connection_config_dict(config: AdbcConfig) -> dict[str, Any]:
    """Call the internal connection-config builder without triggering pyright private usage."""
    return cast("dict[str, Any]", cast("Any", config)._get_connection_config_dict())


def test_resolve_driver_name_alias_to_connect_path() -> None:
    """Resolve short driver aliases to concrete connect paths."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite"})
    assert _resolve_driver_name(config) == "adbc_driver_sqlite.dbapi.connect"


def test_resolve_driver_name_module_name_appends_suffix() -> None:
    """Append .dbapi.connect for bare driver module names."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite"})
    assert _resolve_driver_name(config) == "adbc_driver_sqlite.dbapi.connect"


def test_resolve_driver_name_dbapi_suffix_appends_connect() -> None:
    """Append .connect when driver_name ends in .dbapi."""
    config = AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite.dbapi"})
    assert _resolve_driver_name(config) == "adbc_driver_sqlite.dbapi.connect"


def test_resolve_driver_name_custom_dotted_path_is_left_unchanged() -> None:
    """Treat dotted driver_name values as full import paths."""
    config = AdbcConfig(connection_config={"driver_name": "my.custom.connect"})
    assert _resolve_driver_name(config) == "my.custom.connect"


def test_resolve_driver_name_custom_bare_name_appends_suffix() -> None:
    """Preserve historical behavior for bare custom driver names."""
    config = AdbcConfig(connection_config={"driver_name": "my_custom_driver"})
    assert _resolve_driver_name(config) == "my_custom_driver.dbapi.connect"


def test_resolve_driver_name_from_uri() -> None:
    """Detect driver from URI scheme when driver_name is absent."""
    config = AdbcConfig(connection_config={"uri": "postgresql://example.invalid/db"})
    assert _resolve_driver_name(config) == "adbc_driver_postgresql.dbapi.connect"


def test_connection_config_dict_strips_sqlite_scheme() -> None:
    """Strip sqlite:// from URI when using the sqlite driver."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": "sqlite:///tmp.db"})
    resolved = _get_connection_config_dict(config)
    assert resolved.get("uri") == "/tmp.db"
    assert "driver_name" not in resolved


def test_connection_config_dict_converts_duckdb_uri_to_path() -> None:
    """Convert duckdb:// URI to a path parameter for DuckDB."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "uri": "duckdb:///tmp.db"})
    resolved = _get_connection_config_dict(config)
    assert resolved.get("path") == "/tmp.db"
    assert "uri" not in resolved
    assert "driver_name" not in resolved


def test_connection_config_dict_moves_bigquery_fields_into_db_kwargs() -> None:
    """Move BigQuery configuration fields into db_kwargs."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "bigquery",
            "project_id": "test-project",
            "dataset_id": "test-dataset",
            "token": "token",
        }
    )
    resolved = _get_connection_config_dict(config)
    assert "driver_name" not in resolved
    assert "project_id" not in resolved
    assert "dataset_id" not in resolved
    assert "token" not in resolved
    assert resolved["db_kwargs"]["project_id"] == "test-project"
    assert resolved["db_kwargs"]["dataset_id"] == "test-dataset"
    assert resolved["db_kwargs"]["token"] == "token"


def test_connection_config_dict_moves_bigquery_fields_for_bq_alias() -> None:
    """Move BigQuery fields into db_kwargs when using the bq alias."""
    config = AdbcConfig(connection_config={"driver_name": "bq", "project_id": "p", "dataset_id": "d"})
    resolved = _get_connection_config_dict(config)
    assert resolved["db_kwargs"]["project_id"] == "p"
    assert resolved["db_kwargs"]["dataset_id"] == "d"


def test_connection_config_dict_flattens_db_kwargs_for_non_bigquery() -> None:
    """Flatten db_kwargs into top-level for non-BigQuery drivers."""
    config = AdbcConfig(connection_config={"driver_name": "postgres", "db_kwargs": {"foo": "bar"}})
    resolved = _get_connection_config_dict(config)
    assert "db_kwargs" not in resolved
    assert resolved["foo"] == "bar"
