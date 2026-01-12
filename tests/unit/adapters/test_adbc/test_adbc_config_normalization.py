"""Unit tests for ADBC config normalization helpers."""

from typing import Any

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.core import build_connection_config, resolve_driver_name_from_config


def _resolve_driver_name(config: AdbcConfig) -> str:
    """Resolve driver name from configuration."""
    return resolve_driver_name_from_config(config.connection_config)


def _get_connection_config_dict(config: AdbcConfig) -> dict[str, Any]:
    """Build the normalized connection configuration."""
    return build_connection_config(config.connection_config)


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


def test_resolve_driver_name_gizmosql_alias() -> None:
    """Resolve GizmoSQL aliases to the FlightSQL driver."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"


def test_resolve_driver_name_from_gizmosql_uri() -> None:
    """Detect FlightSQL driver from GizmoSQL URI schemes."""
    config = AdbcConfig(connection_config={"uri": "gizmosql://localhost:31337"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"
    config = AdbcConfig(connection_config={"uri": "gizmo://localhost:31337"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"


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


def test_gizmosql_default_dialect_is_duckdb() -> None:
    """Default GizmoSQL connections to DuckDB dialect."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    assert config.statement_config.dialect == "duckdb"


def test_gizmosql_backend_override_to_sqlite() -> None:
    """Override GizmoSQL dialect to SQLite when requested."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "gizmosql_backend": "sqlite"})
    assert config.statement_config.dialect == "sqlite"


def test_grpc_tls_uri_defaults_to_duckdb() -> None:
    """Default grpc+tls URIs to DuckDB for GizmoSQL."""
    config = AdbcConfig(connection_config={"uri": "grpc+tls://localhost:31337"})
    assert config.statement_config.dialect == "duckdb"


def test_gizmosql_parameter_style_is_qmark() -> None:
    """GizmoSQL connections should use qmark parameter style like DuckDB."""
    from sqlspec.core import ParameterStyle

    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK


def test_gizmo_alias_resolves_to_flightsql() -> None:
    """The short 'gizmo' alias should also resolve to FlightSQL driver."""
    config = AdbcConfig(connection_config={"driver_name": "gizmo"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"
    assert config.statement_config.dialect == "duckdb"


def test_flightsql_alias_backward_compatibility() -> None:
    """Existing flightsql alias should still map to SQLite dialect."""
    config = AdbcConfig(connection_config={"driver_name": "flightsql"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"
    assert config.statement_config.dialect == "sqlite"


def test_grpc_alias_backward_compatibility() -> None:
    """Existing grpc alias should still map to SQLite dialect."""
    config = AdbcConfig(connection_config={"driver_name": "grpc"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"
    assert config.statement_config.dialect == "sqlite"


def test_gizmosql_tls_skip_verify_in_config() -> None:
    """TLS skip verify parameter should be accepted in connection config."""
    config = AdbcConfig(
        connection_config={"driver_name": "gizmosql", "uri": "grpc+tls://localhost:31337", "tls_skip_verify": True}
    )
    # Verify the config accepts tls_skip_verify without error
    assert config.connection_config.get("tls_skip_verify") is True


def test_gizmosql_with_authentication() -> None:
    """GizmoSQL should accept username and password parameters."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "gizmosql",
            "uri": "grpc+tls://localhost:31337",
            "username": "test_user",
            "password": "test_password",
        }
    )
    assert config.connection_config.get("username") == "test_user"
    assert config.connection_config.get("password") == "test_password"


def test_gizmosql_backend_duckdb_explicit() -> None:
    """Explicit DuckDB backend should work."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "gizmosql_backend": "duckdb"})
    assert config.statement_config.dialect == "duckdb"


def test_gizmosql_supported_parameter_styles() -> None:
    """GizmoSQL should support qmark and numeric parameter styles."""
    from sqlspec.core import ParameterStyle

    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    # GizmoSQL (DuckDB backend) should support multiple styles
    supported = config.statement_config.parameter_config.supported_parameter_styles
    assert ParameterStyle.QMARK in supported
