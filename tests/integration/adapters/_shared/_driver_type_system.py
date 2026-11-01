"""Driver-type-system registry for adapter contract tests."""

import importlib
from dataclasses import dataclass
from typing import Any

__all__ = (
    "DRIVER_FEATURE_CONSUMED_KEYS",
    "DRIVER_FEATURE_CONTRACT_ENABLED",
    "DRIVER_FEATURE_CONTRACT_GROUPS",
    "DRIVER_FEATURE_TYPED_DICTS",
    "DriverFeatureContractGroup",
    "get_driver_feature_consumed_keys",
    "get_driver_feature_keys",
)


@dataclass(frozen=True)
class DriverFeatureContractGroup:
    """Registry entry for a disabled-first contract group."""

    description: str
    enabled: bool = False
    enabled_adapters: tuple[str, ...] = ()


DRIVER_FEATURE_TYPED_DICTS: dict[str, tuple[str, str]] = {
    "adbc": ("sqlspec.adapters.adbc.config", "AdbcDriverFeatures"),
    "aiomysql": ("sqlspec.adapters.aiomysql.config", "AiomysqlDriverFeatures"),
    "aiosqlite": ("sqlspec.adapters.aiosqlite.config", "AiosqliteDriverFeatures"),
    "arrow_odbc": ("sqlspec.adapters.arrow_odbc.config", "ArrowOdbcDriverFeatures"),
    "asyncmy": ("sqlspec.adapters.asyncmy.config", "AsyncmyDriverFeatures"),
    "asyncpg": ("sqlspec.adapters.asyncpg.config", "AsyncpgDriverFeatures"),
    "bigquery": ("sqlspec.adapters.bigquery.config", "BigQueryDriverFeatures"),
    "cockroach_asyncpg": ("sqlspec.adapters.cockroach_asyncpg.config", "CockroachAsyncpgDriverFeatures"),
    "cockroach_psycopg": ("sqlspec.adapters.cockroach_psycopg.config", "CockroachPsycopgDriverFeatures"),
    "db2": ("sqlspec.adapters.db2.config", "Db2DriverFeatures"),
    "duckdb": ("sqlspec.adapters.duckdb.config", "DuckDBDriverFeatures"),
    "mssql_python": ("sqlspec.adapters.mssql_python.config", "MssqlPythonDriverFeatures"),
    "mysqlconnector": ("sqlspec.adapters.mysqlconnector.config", "MysqlConnectorDriverFeatures"),
    "oracledb": ("sqlspec.adapters.oracledb.config", "OracleDriverFeatures"),
    "psqlpy": ("sqlspec.adapters.psqlpy.config", "PsqlpyDriverFeatures"),
    "psycopg": ("sqlspec.adapters.psycopg.config", "PsycopgDriverFeatures"),
    "pymssql": ("sqlspec.adapters.pymssql.config", "PymssqlDriverFeatures"),
    "pymysql": ("sqlspec.adapters.pymysql.config", "PyMysqlDriverFeatures"),
    "spanner": ("sqlspec.adapters.spanner.config", "SpannerDriverFeatures"),
    "sqlite": ("sqlspec.adapters.sqlite.config", "SqliteDriverFeatures"),
}

_MIGRATED_DRIVER_FEATURE_ADAPTERS = (
    "adbc",
    "aiomysql",
    "aiosqlite",
    "arrow_odbc",
    "asyncmy",
    "asyncpg",
    "bigquery",
    "cockroach_asyncpg",
    "cockroach_psycopg",
    "db2",
    "duckdb",
    "mssql_python",
    "mysqlconnector",
    "oracledb",
    "psqlpy",
    "psycopg",
    "pymssql",
    "pymysql",
    "spanner",
    "sqlite",
)

DRIVER_FEATURE_CONSUMED_KEYS: dict[str, tuple[str, ...]] = dict.fromkeys(DRIVER_FEATURE_TYPED_DICTS, ())

DRIVER_FEATURE_CONSUMED_KEYS.update({
    "adbc": (
        "json_serializer",
        "enable_cast_detection",
        "enable_strict_type_coercion",
        "enable_arrow_extension_types",
        "enable_pgvector",
        "enable_paradedb",
        "enable_pg_textsearch",
        "enable_lowercase_column_names",
        "enable_events",
        "on_connection_create",
        "events_backend",
        "enable_statement_profiling",
    ),
    "aiomysql": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_local_infile_bulk_load",
    ),
    "aiosqlite": (
        "enable_custom_adapters",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "custom_functions",
        "custom_collations",
        "custom_aggregates",
        "custom_window_functions",
        "default_transaction_mode",
        "authorizer_callback",
        "trace_callback",
        "progress_handler",
        "progress_handler_interval",
        "row_factory",
        "text_factory",
        "pragmas",
        "extensions",
    ),
    "arrow_odbc": (
        "events_backend",
        "chunk_size",
        "max_bytes_per_batch",
        "max_text_size",
        "max_binary_size",
        "fetch_concurrently",
        "query_timeout_sec",
        "payload_text_encoding",
        "enable_pooling",
        "connection_string",
        "dbms_name",
        "connection_autocommit",
        "enable_lowercase_column_names",
        "json_serializer",
        "json_deserializer",
        "enable_events",
        "on_connection_create",
    ),
    "asyncmy": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_local_infile_bulk_load",
    ),
    "asyncpg": (
        "enable_pgcopy_binary",
        "json_serializer",
        "json_deserializer",
        "enable_json_codecs",
        "enable_pgvector",
        "enable_paradedb",
        "enable_pg_textsearch",
        "enable_cloud_sql",
        "cloud_sql_instance",
        "cloud_sql_enable_iam_auth",
        "cloud_sql_ip_type",
        "enable_alloydb",
        "alloydb_instance_uri",
        "enable_alloydb_iam_auth",
        "alloydb_ip_type",
        "enable_alloydb_columnar",
        "enable_events",
        "events_backend",
        "on_connection_create",
        "pooler_mode",
        "direct_dsn",
        "type_codecs",
    ),
    "bigquery": (
        "default_query_job_config",
        "json_deserializer",
        "connection_instance",
        "on_connection_create",
        "json_serializer",
        "enable_uuid_conversion",
        "enable_events",
        "events_backend",
        "job_retry_deadline",
        "job_result_timeout",
        "enable_query_and_wait",
        "request_timeout",
        "query_page_size",
        "query_max_results",
        "enable_storage_write_api",
        "storage_write_stream_type",
        "enable_native_storage",
        "native_export_connection",
    ),
    "cockroach_asyncpg": (
        "enable_native_storage",
        "native_storage_csv_options",
        "enable_auto_retry",
        "max_retries",
        "retry_delay_base_ms",
        "retry_delay_max_ms",
        "enable_retry_logging",
        "enable_follower_reads",
        "default_staleness",
        "json_serializer",
        "json_deserializer",
        "enable_json_codecs",
        "enable_pgvector",
        "on_connection_create",
        "enable_events",
        "events_backend",
    ),
    "cockroach_psycopg": (
        "enable_native_storage",
        "native_storage_csv_options",
        "enable_auto_retry",
        "max_retries",
        "retry_delay_base_ms",
        "retry_delay_max_ms",
        "enable_retry_logging",
        "enable_follower_reads",
        "default_staleness",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
    ),
    "db2": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_lowercase_column_names",
    ),
    "duckdb": (
        "extensions",
        "secrets",
        "on_connection_create",
        "json_serializer",
        "enable_uuid_conversion",
        "extension_flags",
        "enable_events",
        "events_backend",
        "enable_statement_profiling",
    ),
    "mssql_python": (
        "enable_pooling",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
    ),
    "mysqlconnector": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "cursor_options",
        "enable_local_infile_bulk_load",
    ),
    "oracledb": (
        "enable_numpy_vectors",
        "enable_lowercase_column_names",
        "enable_uuid_binary",
        "vector_return_format",
        "oracle_varchar2_byte_limit",
        "oracle_raw_byte_limit",
        "arraysize",
        "prefetchrows",
        "fetch_lobs",
        "fetch_decimals",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_direct_path_load",
        "session_context_hook",
        "enable_session_tracing",
        "dbms_cloud_credential",
    ),
    "psqlpy": (
        "enable_pgcopy_binary",
        "enable_cast_detection",
        "enable_pgvector",
        "enable_paradedb",
        "enable_pg_textsearch",
        "enable_alloydb_columnar",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "pooler_mode",
        "direct_dsn",
    ),
    "psycopg": (
        "enable_pgcopy_binary",
        "enable_pgvector",
        "enable_paradedb",
        "enable_pg_textsearch",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_alloydb",
        "alloydb_instance_uri",
        "enable_alloydb_iam_auth",
        "alloydb_ip_type",
        "enable_alloydb_columnar",
        "pooler_mode",
        "direct_dsn",
        "enable_pooling",
    ),
    "pymssql": (
        "enable_pooling",
        "enable_uuid_conversion",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
    ),
    "pymysql": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_local_infile_bulk_load",
        "enable_cloud_sql",
        "cloud_sql_instance",
        "cloud_sql_enable_iam_auth",
        "cloud_sql_ip_type",
    ),
    "spanner": (
        "on_connection_create",
        "enable_uuid_conversion",
        "json_serializer",
        "json_deserializer",
        "retry",
        "timeout",
        "request_options",
        "query_options",
        "directed_read_options",
        "session_labels",
        "enable_events",
        "events_backend",
        "enable_batch_write_api",
    ),
    "sqlite": (
        "enable_custom_adapters",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "custom_functions",
        "custom_collations",
        "custom_aggregates",
        "custom_window_functions",
        "default_transaction_mode",
        "authorizer_callback",
        "trace_callback",
        "progress_handler",
        "progress_handler_interval",
        "row_factory",
        "text_factory",
        "pragmas",
        "extensions",
    ),
})

DRIVER_FEATURE_CONTRACT_GROUPS: dict[str, DriverFeatureContractGroup] = {
    "feature_honesty": DriverFeatureContractGroup(
        description="TypedDict keys must be consumed by the owning adapter implementation.",
        enabled=True,
        enabled_adapters=_MIGRATED_DRIVER_FEATURE_ADAPTERS,
    ),
    "enable_false_semantics": DriverFeatureContractGroup(
        description="enable_*=False must be inert and not leak process-global state.",
        enabled=True,
        enabled_adapters=("aiosqlite", "sqlite"),
    ),
    "parity": DriverFeatureContractGroup(
        description="Buffered, streamed, and Arrow values must stay equal on canonical typed fixtures.",
        enabled=True,
        enabled_adapters=(
            "adbc",
            "aiomysql",
            "arrow_odbc",
            "asyncmy",
            "bigquery",
            "duckdb",
            "mysqlconnector",
            "pymysql",
            "spanner",
        ),
    ),
    "row_format": DriverFeatureContractGroup(
        description="The first materialized row shape must match the row_format tag.",
        enabled=True,
        enabled_adapters=("aiosqlite", "psycopg", "sqlite"),
    ),
}

DRIVER_FEATURE_CONTRACT_ENABLED: dict[str, bool] = {
    group_name: group.enabled for group_name, group in DRIVER_FEATURE_CONTRACT_GROUPS.items()
}


def get_driver_feature_keys(adapter: str) -> tuple[str, ...]:
    """Return the canonical DriverFeatures keys declared by an adapter TypedDict."""
    module_path, typed_dict_name = DRIVER_FEATURE_TYPED_DICTS[adapter]
    typed_dict = _load_symbol(module_path, typed_dict_name)
    annotations = getattr(typed_dict, "__annotations__", None)
    if not isinstance(annotations, dict):
        msg = f"{typed_dict_name} in {module_path} does not expose TypedDict annotations"
        raise AssertionError(msg)
    return tuple(annotations)


def get_driver_feature_consumed_keys(adapter: str) -> tuple[str, ...]:
    """Return the registry-tracked keys consumed by an adapter."""
    return DRIVER_FEATURE_CONSUMED_KEYS[adapter]


def _load_symbol(module_path: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_path)
        return getattr(module, symbol_name)
    except (AttributeError, ImportError) as exc:
        msg = f"Unable to load {symbol_name} from {module_path}: {exc}"
        raise AssertionError(msg) from exc
