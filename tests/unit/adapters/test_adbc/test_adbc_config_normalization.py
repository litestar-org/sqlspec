"""Unit tests for ADBC config normalization helpers."""

from typing import Any, get_type_hints
from unittest.mock import MagicMock

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.config import AdbcConnectionParams
from sqlspec.adapters.adbc.core import build_connection_config, resolve_driver_name_from_config


def _resolve_driver_name(config: AdbcConfig) -> str:
    """Resolve driver name from configuration."""
    return resolve_driver_name_from_config(config.connection_config)


def _get_connection_config_dict(config: AdbcConfig) -> dict[str, Any]:
    """Build the normalized connection configuration."""
    return build_connection_config(config.connection_config)


def test_adbc_config_runs_connection_create_callback(monkeypatch: Any) -> None:
    """ADBC should run the connection hook when it creates a physical connection."""
    connection = MagicMock()
    seen: list[Any] = []

    def fake_connect(**_kwargs: Any) -> Any:
        return connection

    monkeypatch.setattr("sqlspec.adapters.adbc.config.resolve_driver_connect_func", lambda *_args: fake_connect)

    config = AdbcConfig(
        connection_config={"driver_name": "sqlite"}, driver_features={"on_connection_create": seen.append}
    )

    assert config.create_connection() is connection
    assert seen == [connection]
    assert "on_connection_create" not in config.driver_features


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


def test_connection_config_dict_moves_bigquery_fields_for_bigquery_uri() -> None:
    """Move BigQuery fields into db_kwargs when the driver is inferred from the URI."""
    config = AdbcConfig(connection_config={"uri": "bigquery://", "project_id": "p", "dataset_id": "d"})
    resolved = _get_connection_config_dict(config)
    assert "project_id" not in resolved
    assert "dataset_id" not in resolved
    assert resolved["db_kwargs"]["project_id"] == "p"
    assert resolved["db_kwargs"]["dataset_id"] == "d"


def test_connection_config_dict_preserves_db_kwargs_for_non_bigquery() -> None:
    """Preserve db_kwargs for drivers whose dbapi.connect accepts nested database kwargs."""
    config = AdbcConfig(connection_config={"driver_name": "postgres", "db_kwargs": {"foo": "bar"}})
    resolved = _get_connection_config_dict(config)
    assert resolved["db_kwargs"] == {"foo": "bar"}
    assert "foo" not in resolved


def test_flightsql_lifts_username_password_into_db_kwargs() -> None:
    """Top-level username/password move into db_kwargs for GizmoSQL."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "username": "alice", "password": "secret"})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"] == {"username": "alice", "password": "secret"}
    assert "username" not in resolved
    assert "password" not in resolved


def test_flightsql_lifts_tls_skip_verify_true() -> None:
    """tls_skip_verify=True maps to the FlightSQL ADBC option key."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "tls_skip_verify": True})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "true"


def test_flightsql_lifts_tls_skip_verify_false() -> None:
    """tls_skip_verify=False maps to the FlightSQL ADBC option key."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "tls_skip_verify": False})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "false"


def test_flightsql_lifts_authorization_header() -> None:
    """authorization_header maps to the FlightSQL ADBC option key."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "authorization_header": "Bearer token"})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.authorization_header"] == "Bearer token"


def test_flightsql_explicit_db_kwargs_wins_over_top_level_shortcuts() -> None:
    """User-supplied db_kwargs entries take precedence over top-level shortcuts."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "gizmosql",
            "username": "alice",
            "tls_skip_verify": False,
            "db_kwargs": {"username": "override", "adbc.flight.sql.client_option.tls_skip_verify": "true"},
        }
    )
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["username"] == "override"
    assert resolved["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "true"


def test_flightsql_gizmosql_backend_is_stripped_from_outgoing_config() -> None:
    """gizmosql_backend drives dialect selection but does not leak to FlightSQL."""
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "gizmosql_backend": "sqlite"})
    resolved = _get_connection_config_dict(config)

    assert "gizmosql_backend" not in resolved
    assert config.statement_config.dialect == "sqlite"


def test_postgres_config_unchanged_by_flightsql_branch() -> None:
    """PostgreSQL keeps db_kwargs nested while preserving top-level connection shortcuts."""
    config = AdbcConfig(
        connection_config={"driver_name": "postgres", "username": "alice", "db_kwargs": {"sslmode": "require"}}
    )
    resolved = _get_connection_config_dict(config)

    assert resolved["username"] == "alice"
    assert resolved["db_kwargs"] == {"sslmode": "require"}


def test_duckdb_config_unchanged_by_flightsql_branch() -> None:
    """DuckDB configs are not affected by the FlightSQL branch."""
    config = AdbcConfig(connection_config={"driver_name": "duckdb", "path": "/tmp/sqlspec-test.duckdb"})
    resolved = _get_connection_config_dict(config)

    assert resolved["path"] == "/tmp/sqlspec-test.duckdb"
    assert "db_kwargs" not in resolved


def test_adbc_connection_params_include_current_driver_manager_keys() -> None:
    """Typed config should include current driver-manager keys and the legacy alias."""
    annotations = get_type_hints(AdbcConnectionParams, include_extras=True)

    assert "entrypoint" in annotations
    assert "profile" in annotations
    assert "adbc_driver_manager_entrypoint" in annotations


def test_legacy_entrypoint_alias_normalizes_to_entrypoint() -> None:
    """Legacy adbc_driver_manager_entrypoint should normalize to current entrypoint."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "postgres",
            "adbc_driver_manager_entrypoint": "PostgreSQL",
            "profile": "analytics",
        }
    )
    resolved = _get_connection_config_dict(config)

    assert resolved["entrypoint"] == "PostgreSQL"
    assert resolved["profile"] == "analytics"
    assert "adbc_driver_manager_entrypoint" not in resolved


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
