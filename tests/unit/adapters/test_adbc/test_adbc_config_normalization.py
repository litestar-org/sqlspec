"""Unit tests for ADBC config normalization helpers."""

from typing import Any, get_type_hints
from unittest.mock import MagicMock

import adbc_driver_manager.dbapi
import pytest
from adbc_driver_bigquery import DatabaseOptions

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.adbc.config import AdbcConnectionParams
from sqlspec.adapters.adbc.core import (
    build_connection_config,
    resolve_driver_connect_func,
    resolve_driver_name_from_config,
)
from sqlspec.core import SQL, ParameterStyle
from sqlspec.exceptions import SQLSpecError


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


def test_resolve_driver_name_gizmosql_alias(monkeypatch: Any) -> None:
    """Resolve GizmoSQL aliases to the FlightSQL driver when native gizmosql driver is absent."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"


def test_resolve_driver_name_from_gizmosql_uri(monkeypatch: Any) -> None:
    """Detect FlightSQL driver from GizmoSQL URI schemes when native gizmosql driver is absent."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
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
    assert resolved["db_kwargs"][DatabaseOptions.PROJECT_ID.value] == "test-project"
    assert resolved["db_kwargs"][DatabaseOptions.DATASET_ID.value] == "test-dataset"
    assert resolved["db_kwargs"][DatabaseOptions.AUTH_REFRESH_TOKEN.value] == "token"
    assert "adbc.bigquery.sql.token" not in resolved["db_kwargs"]


def test_connection_config_dict_moves_bigquery_fields_for_bq_alias() -> None:
    """Move BigQuery fields into db_kwargs when using the bq alias."""
    config = AdbcConfig(connection_config={"driver_name": "bq", "project_id": "p", "dataset_id": "d"})
    resolved = _get_connection_config_dict(config)
    assert resolved["db_kwargs"][DatabaseOptions.PROJECT_ID.value] == "p"
    assert resolved["db_kwargs"][DatabaseOptions.DATASET_ID.value] == "d"


def test_connection_config_dict_moves_bigquery_fields_for_bigquery_uri() -> None:
    """Move BigQuery fields into db_kwargs when the driver is inferred from the URI."""
    config = AdbcConfig(connection_config={"uri": "bigquery://", "project_id": "p", "dataset_id": "d"})
    resolved = _get_connection_config_dict(config)
    assert "project_id" not in resolved
    assert "dataset_id" not in resolved
    assert resolved["db_kwargs"][DatabaseOptions.PROJECT_ID.value] == "p"
    assert resolved["db_kwargs"][DatabaseOptions.DATASET_ID.value] == "d"


def test_connection_config_dict_preserves_db_kwargs_for_non_bigquery() -> None:
    """Preserve db_kwargs for drivers whose dbapi.connect accepts nested database kwargs."""
    config = AdbcConfig(connection_config={"driver_name": "postgres", "db_kwargs": {"foo": "bar"}})
    resolved = _get_connection_config_dict(config)
    assert resolved["db_kwargs"] == {"foo": "bar"}
    assert "foo" not in resolved


def test_flightsql_lifts_username_password_into_db_kwargs(monkeypatch: Any) -> None:
    """Top-level username/password move into db_kwargs on the FlightSQL fallback route."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "username": "alice", "password": "secret"})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"] == {"username": "alice", "password": "secret"}
    assert "username" not in resolved
    assert "password" not in resolved


def test_flightsql_lifts_tls_skip_verify_true(monkeypatch: Any) -> None:
    """tls_skip_verify=True maps to the FlightSQL ADBC option key on the FlightSQL fallback route."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "tls_skip_verify": True})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "true"


def test_flightsql_lifts_tls_skip_verify_false(monkeypatch: Any) -> None:
    """tls_skip_verify=False maps to the FlightSQL ADBC option key on the FlightSQL fallback route."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "tls_skip_verify": False})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "false"


def test_flightsql_lifts_authorization_header(monkeypatch: Any) -> None:
    """authorization_header maps to the FlightSQL ADBC option key."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(connection_config={"driver_name": "gizmosql", "authorization_header": "Bearer token"})
    resolved = _get_connection_config_dict(config)

    assert resolved["db_kwargs"]["adbc.flight.sql.authorization_header"] == "Bearer token"


def test_flightsql_explicit_db_kwargs_wins_over_top_level_shortcuts(monkeypatch: Any) -> None:
    """User-supplied db_kwargs entries take precedence over top-level shortcuts."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
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
            "driver_name": "/opt/lib/libadbc_driver_postgresql.so",
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
    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK


def test_gizmo_alias_resolves_to_flightsql(monkeypatch: Any) -> None:
    """The short 'gizmo' alias resolves to FlightSQL driver when native gizmosql driver is absent."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
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
    config = AdbcConfig(connection_config={"driver_name": "gizmosql"})
    supported = config.statement_config.parameter_config.supported_parameter_styles
    assert ParameterStyle.QMARK in supported


def test_bigquery_options_use_the_installed_driver_constants() -> None:
    """Every BigQuery option name must match the driver's own constant."""
    config = AdbcConfig(
        connection_config={"driver_name": "bigquery", "project_id": "p", "dataset_id": "d", "token": "t"}
    )

    db_kwargs = _get_connection_config_dict(config)["db_kwargs"]

    assert set(db_kwargs) <= {option.value for option in DatabaseOptions}


def test_shared_object_driver_routes_through_the_driver_manager() -> None:
    """A driver shared object is a filesystem path, not an importable module path."""
    config = AdbcConfig(connection_config={"driver_name": "/opt/lib/libadbc_driver_postgresql.so"})

    assert _resolve_driver_name(config) == "adbc_driver_manager.dbapi.connect"

    resolved = _get_connection_config_dict(config)

    assert resolved["driver"] == "/opt/lib/libadbc_driver_postgresql.so"
    assert "driver_name" not in resolved


def test_shared_object_driver_connect_func_is_the_driver_manager() -> None:
    """Resolving the connect function must not try to import the path as a module."""
    connect_func = resolve_driver_connect_func("/opt/lib/libadbc_driver_postgresql.dylib", None)

    assert connect_func is adbc_driver_manager.dbapi.connect


def test_entrypoint_is_stripped_for_dedicated_drivers() -> None:
    """Dedicated dbapi modules reject entrypoint, so it must not reach them."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:", "entrypoint": "foo"})

    assert "entrypoint" not in _get_connection_config_dict(config)


def test_entrypoint_is_preserved_on_the_driver_manager_route() -> None:
    """The driver manager is the only consumer of entrypoint."""
    config = AdbcConfig(
        connection_config={"driver_name": "/opt/lib/libadbc_driver_postgresql.so", "entrypoint": "PostgreSQL"}
    )

    assert _get_connection_config_dict(config)["entrypoint"] == "PostgreSQL"


def test_shared_object_driver_keeps_its_dialect() -> None:
    """Routing through the driver manager must not erase the dialect the path names."""
    config = AdbcConfig(
        connection_config={"driver_name": "/opt/lib/libadbc_driver_postgresql.so", "uri": "postgresql://host/db"}
    )

    assert config.statement_config.dialect == "postgres"


def test_shared_object_driver_keeps_its_parameter_style() -> None:
    """A PostgreSQL shared object must not be given SQLite's qmark placeholders."""
    config = AdbcConfig(connection_config={"driver_name": "/opt/lib/libadbc_driver_postgresql.so"})

    assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


def test_resolve_driver_name_from_url_and_dsn() -> None:
    """Detect driver from URL and DSN aliases when driver_name is absent."""
    url_config = AdbcConfig(connection_config={"url": "postgresql://example.invalid/db"})
    assert _resolve_driver_name(url_config) == "adbc_driver_postgresql.dbapi.connect"

    dsn_config = AdbcConfig(connection_config={"dsn": "postgresql://example.invalid/db"})
    assert _resolve_driver_name(dsn_config) == "adbc_driver_postgresql.dbapi.connect"

    conn_str_config = AdbcConfig(connection_config={"connection_string": "postgresql://example.invalid/db"})
    assert _resolve_driver_name(conn_str_config) == "adbc_driver_postgresql.dbapi.connect"


def test_connection_config_dict_resolves_aliases() -> None:
    """Normalize url, dsn, path, and database aliases into uri or path kwargs."""
    url_result = build_connection_config({"url": "postgresql://example.invalid/db"})
    assert url_result["uri"] == "postgresql://example.invalid/db"
    assert "url" not in url_result

    sqlite_db_result = build_connection_config({"driver_name": "sqlite", "database": "/tmp/app.db"})
    assert sqlite_db_result["uri"] == "/tmp/app.db"
    assert "database" not in sqlite_db_result

    sqlite_path_result = build_connection_config({"driver_name": "sqlite", "path": "/tmp/app.db"})
    assert sqlite_path_result["uri"] == "/tmp/app.db"
    assert "path" not in sqlite_path_result

    duckdb_db_result = build_connection_config({"driver_name": "duckdb", "database": "/tmp/app.duckdb"})
    assert duckdb_db_result["path"] == "/tmp/app.duckdb"
    assert "database" not in duckdb_db_result


def test_adbc_config_resolves_database_aliases() -> None:
    """AdbcConfig should normalize aliases in connection_config."""
    sqlite_config = AdbcConfig(connection_config={"driver_name": "sqlite", "database": "/tmp/app.db"})
    assert sqlite_config.connection_config["uri"] == "/tmp/app.db"
    assert "database" not in sqlite_config.connection_config

    pg_config = AdbcConfig(connection_config={"url": "postgresql://example.invalid/db"})
    assert pg_config.connection_config["uri"] == "postgresql://example.invalid/db"
    assert "url" not in pg_config.connection_config


def test_flightsql_native_shortcuts_preserve_explicit_options() -> None:
    explicit = {"adbc.flight.sql.rpc.call_header.x-token": "explicit", "adbc.flight.sql.rpc.timeout_seconds.query": "9"}
    resolved = build_connection_config({
        "driver_name": "flightsql",
        "grpc_max_message_size": 1048576,
        "query_timeout": 2.5,
        "fetch_timeout": 1.5,
        "tls_root_certs": "root",
        "mtls_cert_chain": "chain",
        "mtls_private_key": "key",
        "with_cookie_middleware": True,
        "headers": {"x-token": "shortcut", "x-trace": "trace"},
        "db_kwargs": explicit,
    })
    assert resolved["db_kwargs"] == {
        **explicit,
        "adbc.flight.sql.client_option.with_max_msg_size": "1048576",
        "adbc.flight.sql.rpc.timeout_seconds.fetch": "1.5",
        "adbc.flight.sql.client_option.tls_root_certs": "root",
        "adbc.flight.sql.client_option.mtls_cert_chain": "chain",
        "adbc.flight.sql.client_option.mtls_private_key": "key",
        "adbc.flight.sql.rpc.with_cookie_middleware": "true",
        "adbc.flight.sql.rpc.call_header.x-trace": "trace",
    }
    assert explicit == {
        "adbc.flight.sql.rpc.call_header.x-token": "explicit",
        "adbc.flight.sql.rpc.timeout_seconds.query": "9",
    }


def test_adbc_config_respects_connection_instance_without_closing() -> None:
    """connection_instance must be reused by create_connection and not closed on context exit."""
    conn = MagicMock()
    conn.adbc_get_info.return_value = {"vendor_name": "sqlite", "driver_name": "sqlite"}
    config = AdbcConfig(connection_config={"driver_name": "sqlite"}, connection_instance=conn)

    assert config.create_connection() is conn
    with config.provide_connection() as yielded:
        assert yielded is conn
    conn.close.assert_not_called()

    with config.provide_session() as session:
        assert session.connection is conn
    conn.close.assert_not_called()


def test_adbc_config_skips_extension_connection_when_all_probes_disabled(monkeypatch: Any) -> None:
    """When all PostgreSQL extension probes are disabled, _detect_extensions_if_needed must not connect."""
    config = AdbcConfig(
        connection_config={"driver_name": "postgres", "uri": "postgresql://localhost/test"},
        driver_features={"enable_pgvector": False, "enable_paradedb": False, "enable_pg_textsearch": False},
    )

    def fail_connect(_self: AdbcConfig) -> None:
        raise AssertionError("create_connection should not be called when all probes are disabled")

    monkeypatch.setattr(AdbcConfig, "create_connection", fail_connect)
    config._detect_extensions_if_needed()  # pyright: ignore[reportPrivateUsage]
    assert config._pgvector_available is False  # pyright: ignore[reportPrivateUsage]
    assert config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert config.pg_textsearch_available is False


def test_adbc_driver_standalone_applies_features_and_enforces_strict_coercion() -> None:
    """Standalone AdbcDriver populates default driver_features, aligns dialect, and honors strict_type_coercion."""
    conn = MagicMock()
    conn.adbc_get_info.return_value = {"vendor_name": "duckdb", "driver_name": "duckdb"}

    driver = AdbcDriver(conn, driver_features={"strict_type_coercion": True})
    assert driver.dialect == "duckdb"
    assert str(driver.statement_config.dialect) == "duckdb"
    assert "json_serializer" in driver.driver_features

    with pytest.raises(SQLSpecError):
        driver.prepare_driver_parameters([object()], driver.statement_config)


def test_oracle_driver_aliases_uri_and_parameter_profile() -> None:
    """Oracle aliases and oracle:// URI resolve to adbc_driver_oracle and POSITIONAL_COLON parameters."""
    for conn_cfg in (
        {"driver_name": "oracle", "uri": "oracle://scott:tiger@localhost:1521/FREEPDB1"},
        {"driver_name": "oracledb", "uri": "oracle://scott:tiger@localhost:1521/FREEPDB1"},
        {"uri": "oracle://scott:tiger@localhost:1521/FREEPDB1"},
    ):
        config = AdbcConfig(connection_config=conn_cfg)
        assert _resolve_driver_name(config) == "adbc_driver_oracle.dbapi.connect"
        assert config.statement_config.dialect == "oracle"
        assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.POSITIONAL_COLON
        assert set(config.statement_config.parameter_config.supported_execution_parameter_styles or ()) == {
            ParameterStyle.POSITIONAL_COLON
        }


def test_oracle_lifts_discrete_username_and_password_into_db_kwargs() -> None:
    """adbc_driver_oracle.dbapi.connect only accepts uri at top level; username/password move to db_kwargs."""
    config = AdbcConfig(
        connection_config={
            "driver_name": "oracle",
            "uri": "oracle://localhost:1521/FREEPDB1",
            "username": "scott",
            "password": "tiger",
        }
    )
    resolved = _get_connection_config_dict(config)
    assert resolved["uri"] == "oracle://localhost:1521/FREEPDB1"
    assert "username" not in resolved
    assert "password" not in resolved
    assert resolved["db_kwargs"] == {"username": "scott", "password": "tiger"}


def test_db2_driver_aliases_uri_and_parameter_profile() -> None:
    """Db2 aliases and db2:// URI resolve to adbc_driver_db2 and QMARK parameters."""
    for conn_cfg in (
        {"driver_name": "db2", "uri": "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;UID=db2inst1;PWD=secret;"},
        {"driver_name": "ibmdb2", "uri": "db2://db2inst1:secret@localhost:50000/testdb"},
        {"driver_name": "ibm_db", "uri": "db2://db2inst1:secret@localhost:50000/testdb"},
        {"uri": "db2://db2inst1:secret@localhost:50000/testdb"},
    ):
        config = AdbcConfig(connection_config=conn_cfg)
        assert _resolve_driver_name(config) == "adbc_driver_db2.dbapi.connect"
        assert config.statement_config.dialect == "db2"
        assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK
        assert set(config.statement_config.parameter_config.supported_execution_parameter_styles or ()) == {
            ParameterStyle.QMARK
        }


def test_mssql_driver_manager_fallback_and_tsql_statement_config(monkeypatch: Any) -> None:
    """When adbc_driver_mssql is absent, mssql routes through adbc_driver_manager with driver='mssql'."""
    monkeypatch.setattr(
        "sqlspec.adapters.adbc.core.import_optional", lambda name: None if name == "adbc_driver_mssql" else MagicMock()
    )

    for conn_cfg in (
        {"driver_name": "mssql", "uri": "sqlserver://sa:Pass@localhost:1433?database=master"},
        {"driver_name": "sqlserver", "uri": "mssql://sa:Pass@localhost:1433?database=master"},
        {"uri": "sqlserver://sa:Pass@localhost:1433?database=master"},
        {"driver": "mssql", "db_kwargs": {"uri": "sqlserver://sa:Pass@localhost:1433?database=master"}},
    ):
        config = AdbcConfig(connection_config=conn_cfg)
        assert _resolve_driver_name(config) == "adbc_driver_manager.dbapi.connect"
        assert config.statement_config.dialect == "tsql"
        assert config.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK
        assert set(config.statement_config.parameter_config.supported_execution_parameter_styles or ()) == {
            ParameterStyle.QMARK
        }
        resolved = _get_connection_config_dict(config)
        assert resolved["driver"] == "mssql"
        assert "uri" not in resolved
        assert resolved["db_kwargs"]["uri"].startswith(("sqlserver://", "mssql://"))


def test_mssql_native_package_preferred_when_installed(monkeypatch: Any) -> None:
    """When adbc_driver_mssql is installed, mssql resolves to adbc_driver_mssql.dbapi.connect."""
    monkeypatch.setattr(
        "sqlspec.adapters.adbc.core.import_optional", lambda name: MagicMock() if name == "adbc_driver_mssql" else None
    )
    config = AdbcConfig(connection_config={"driver_name": "mssql", "uri": "sqlserver://localhost:1433"})
    assert _resolve_driver_name(config) == "adbc_driver_mssql.dbapi.connect"
    assert config.statement_config.dialect == "tsql"


def test_gizmosql_prefers_native_driver_when_installed(monkeypatch: Any) -> None:
    """When adbc_driver_gizmosql is installed, gizmosql resolves to adbc_driver_gizmosql.dbapi.connect."""
    monkeypatch.setattr(
        "sqlspec.adapters.adbc.core.import_optional",
        lambda name: MagicMock() if name == "adbc_driver_gizmosql" else None,
    )
    config = AdbcConfig(
        connection_config={
            "driver_name": "gizmosql",
            "uri": "grpc+tls://localhost:31337",
            "auth_type": "external",
            "tls_skip_verify": True,
        }
    )
    assert _resolve_driver_name(config) == "adbc_driver_gizmosql.dbapi.connect"
    resolved = _get_connection_config_dict(config)
    assert resolved["auth_type"] == "external"
    assert resolved["tls_skip_verify"] is True
    assert resolved["autocommit"] is False

    explicit_autocommit = AdbcConfig(
        connection_config={"driver_name": "gizmosql", "uri": "grpc+tls://localhost:31337", "autocommit": True}
    )
    assert _get_connection_config_dict(explicit_autocommit)["autocommit"] is True


def test_gizmosql_flightsql_fallback_lifts_auth_type_into_db_kwargs(monkeypatch: Any) -> None:
    """When adbc_driver_gizmosql is absent, auth_type is lifted into db_kwargs['adbc.gizmosql.auth_type']."""
    monkeypatch.setattr("sqlspec.adapters.adbc.core.import_optional", lambda _name: None)
    config = AdbcConfig(
        connection_config={"driver_name": "gizmosql", "uri": "grpc+tls://localhost:31337", "auth_type": "external"}
    )
    assert _resolve_driver_name(config) == "adbc_driver_flightsql.dbapi.connect"
    resolved = _get_connection_config_dict(config)
    assert "auth_type" not in resolved
    assert resolved["db_kwargs"]["adbc.gizmosql.auth_type"] == "external"


def test_quack_driver_aliases_uri_and_token_normalization() -> None:
    """Quack driver resolves to adbc_driver_quack, duckdb dialect, and lifts token into db_kwargs."""
    for conn_cfg in (
        {"driver_name": "quack", "uri": "quack+tls://quack.example.com:9999", "token": "secret-jwt"},
        {"uri": "quack://localhost:9999", "token": "secret-jwt"},
        {"uri": "quack+tls://localhost:9999", "token": "secret-jwt"},
    ):
        config = AdbcConfig(connection_config=conn_cfg)
        assert _resolve_driver_name(config) == "adbc_driver_quack.dbapi.connect"
        assert config.statement_config.dialect == "duckdb"
        resolved = _get_connection_config_dict(config)
        assert "token" not in resolved
        assert resolved["db_kwargs"]["adbc.quack.token"] == "secret-jwt"


def test_db2_cli_dsn_normalizes_to_db2_uri_and_credentials() -> None:
    """IBM Db2 CLI keyword DSNs normalize to db2:// URIs, top-level credentials, and ADBC db_kwargs."""
    explicit_driver_cfg = AdbcConfig(
        connection_config={
            "driver_name": "db2",
            "uri": "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret;",
        }
    )
    resolved_explicit = _get_connection_config_dict(explicit_driver_cfg)
    assert resolved_explicit == {"uri": "db2://localhost:50000/testdb", "username": "db2inst1", "password": "secret"}

    inferred_dsn_cfg = AdbcConfig(
        connection_config={
            "dsn": (
                "DATABASE=testdb;HOSTNAME=127.0.0.1;PORT=50000;UID=db2inst1;PWD=secret;"
                "SECURITY=SSL;SSLSERVERCERTIFICATE=/etc/ssl/ca.pem;CURRENTSCHEMA=APP;CONNECTTIMEOUT=10;"
            )
        }
    )
    assert _resolve_driver_name(inferred_dsn_cfg) == "adbc_driver_db2.dbapi.connect"
    assert inferred_dsn_cfg.statement_config.dialect == "db2"
    assert inferred_dsn_cfg.statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK
    resolved_inferred = _get_connection_config_dict(inferred_dsn_cfg)
    assert resolved_inferred["uri"] == "db2://127.0.0.1:50000/testdb"
    assert resolved_inferred["username"] == "db2inst1"
    assert resolved_inferred["password"] == "secret"
    assert resolved_inferred["db_kwargs"] == {
        "adbc.db2.tls": "true",
        "adbc.db2.tls.ca_cert": "/etc/ssl/ca.pem",
        "adbc.db2.current_schema": "APP",
        "adbc.db2.connect_timeout": "10",
    }

    override_resolved = build_connection_config({
        "dsn": "DATABASE=testdb;HOSTNAME=db2.internal;UID=dsn_user;PWD=dsn_pwd;CURRENTSCHEMA=DSN_SCHEMA;",
        "username": "explicit_user",
        "db_kwargs": {"adbc.db2.current_schema": "EXPLICIT_SCHEMA"},
    })
    assert override_resolved["uri"] == "db2://db2.internal/testdb"
    assert override_resolved["username"] == "explicit_user"
    assert override_resolved["password"] == "dsn_pwd"
    assert override_resolved["db_kwargs"] == {"adbc.db2.current_schema": "EXPLICIT_SCHEMA"}

    ipv6_resolved = build_connection_config({"dsn": "DATABASE=testdb;HOSTNAME=::1;PORT=50000;UID=db2inst1;"})
    assert ipv6_resolved["uri"] == "db2://[::1]:50000/testdb"

    odbc_cfg = AdbcConfig(
        connection_config={"dsn": "DRIVER={IBM DB2 ODBC DRIVER};DATABASE=testdb;HOSTNAME=localhost;PORT=50000;"}
    )
    assert _resolve_driver_name(odbc_cfg) != "adbc_driver_db2.dbapi.connect"
    assert odbc_cfg.statement_config.dialect != "db2"


def test_adbc_driver_db2_routes_parameterized_non_query_via_executemany() -> None:
    """Db2 ADBC routes parameterized non-queries through cursor.executemany across initial, cached, and script paths."""
    cursor = MagicMock()
    cursor.description = None
    cursor.rowcount = 1
    cursor.fetch_arrow_table.return_value = MagicMock(
        column_names=["ID"],
        to_pylist=lambda: [{"ID": 1}],
        rename_columns=lambda cols: MagicMock(column_names=cols, to_pylist=lambda: [{cols[0]: 1}]),
    )
    conn = MagicMock()
    conn.adbc_get_info.return_value = {"vendor_name": "IBM Db2", "driver_name": "adbc_driver_db2"}
    conn.cursor.return_value = cursor

    driver = AdbcDriver(conn, dialect="db2")

    driver.execute("CREATE TABLE items (id INT NOT NULL PRIMARY KEY, name VARCHAR(64))")
    assert cursor.execute.call_count == 1
    assert cursor.executemany.call_count == 0

    first_insert = driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", (1, "alpha"))
    assert first_insert.rows_affected == 1
    assert cursor.executemany.call_count == 1
    assert cursor.executemany.call_args_list[0].args == ("INSERT INTO items (id, name) VALUES (?, ?)", [[1, "alpha"]])

    cached_insert = driver.execute("INSERT INTO items (id, name) VALUES (?, ?)", (2, "beta"))
    assert cached_insert.rows_affected == 1
    assert cursor.executemany.call_count == 2
    assert cursor.executemany.call_args_list[1].args == ("INSERT INTO items (id, name) VALUES (?, ?)", [[2, "beta"]])

    script_update = driver.execute_script("UPDATE items SET name = ? WHERE id = ?", ("gamma", 1))
    assert script_update.rows_affected == 1
    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args_list[1].args == ("UPDATE items SET name = 'gamma' WHERE id = 1",)
    assert cursor.executemany.call_count == 2

    direct_single_script = driver.dispatch_execute_script(
        cursor, SQL("UPDATE items SET name = ? WHERE id = ?", ("gamma", 1), statement_config=driver.statement_config)
    )
    assert direct_single_script.rowcount_override == 1
    assert cursor.executemany.call_count == 3
    assert cursor.executemany.call_args_list[2].args == ("UPDATE items SET name = ? WHERE id = ?", [["gamma", 1]])

    direct_multi_script = driver.dispatch_execute_script(
        cursor,
        SQL(
            "CREATE TABLE extra (id INT); INSERT INTO items (id, name) VALUES (?, ?)",
            (3, "delta"),
            statement_config=driver.statement_config,
        ),
    )
    assert direct_multi_script.rowcount_override == 1
    assert cursor.execute.call_count == 3
    assert cursor.executemany.call_count == 4
    assert cursor.executemany.call_args_list[3].args == ("INSERT INTO items (id, name) VALUES (?, ?)", [[3, "delta"]])

    named_delete = driver.execute("DELETE FROM items WHERE id = :item_id", {"item_id": 2})
    assert named_delete.rows_affected == 1
    assert cursor.executemany.call_count == 5
    assert cursor.executemany.call_args_list[4].args == ("DELETE FROM items WHERE id = ?", [[2]])

    driver.execute("SELECT id FROM items WHERE id = ?", (1,))
    assert cursor.execute.call_count == 4
    assert cursor.executemany.call_count == 5

    class _ArrowStreamStub:
        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            return requested_schema

    arrow_stream = _ArrowStreamStub()
    AdbcDriver._execute_db2_non_query(cursor, "INSERT INTO items (id, name) VALUES (?, ?)", arrow_stream)
    assert cursor.executemany.call_count == 6
    assert cursor.executemany.call_args_list[5].args == ("INSERT INTO items (id, name) VALUES (?, ?)", arrow_stream)


def _resolve_driver_name(config: AdbcConfig) -> str:
    """Resolve driver name from configuration."""
    return resolve_driver_name_from_config(config.connection_config)


def _get_connection_config_dict(config: AdbcConfig) -> dict[str, Any]:
    """Build the normalized connection configuration."""
    return build_connection_config(config.connection_config)


def test_snowflake_lowercase_default() -> None:
    """Snowflake dialect defaults enable_lowercase_column_names to True."""
    config = AdbcConfig(connection_config={"driver_name": "snowflake"})
    assert config.driver_features["enable_lowercase_column_names"] is True


def test_driver_features_alias_consolidation() -> None:
    """AdbcConfig consolidates legacy alias pairs to enable_*."""
    config = AdbcConfig(
        connection_config={"driver_name": "sqlite"},
        driver_features={"strict_type_coercion": True, "arrow_extension_types": False},
    )
    assert config.driver_features["enable_strict_type_coercion"] is True
    assert config.driver_features["enable_arrow_extension_types"] is False
    assert "strict_type_coercion" not in config.driver_features
    assert "arrow_extension_types" not in config.driver_features
