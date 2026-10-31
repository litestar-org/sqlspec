"""ADBC adapter compiled helpers."""

import contextlib
import datetime as dt
import decimal
import re
import tempfile
from collections.abc import Sized
from functools import lru_cache, partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlspec.adapters.adbc.type_converter import ADBCOutputConverter, get_adbc_type_converter
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_null_pruning_transform,
    build_statement_config_from_profile,
)
from sqlspec.core.parameters import type_coercion_dispatcher
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
    _classify_timeout_or_cancellation,
    map_sqlstate_to_exception,
)
from sqlspec.typing import PGVECTOR_INSTALLED, Empty, import_optional_attr
from sqlspec.utils.module_loader import import_optional, import_string
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount, has_sqlstate
from sqlspec.utils.uuids import uuid4, uuid_from_string

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL
    from sqlspec.utils.dispatch import TypeDispatcher

__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "cleanup_duckdb_profile_path",
    "collect_rows",
    "configure_adbc_duckdb_profiling",
    "create_duckdb_profile_path",
    "create_mapped_exception",
    "detect_dialect",
    "detect_postgres_extensions",
    "driver_from_uri",
    "driver_kind_from_driver_name",
    "driver_kind_from_uri",
    "driver_profile",
    "enforce_strict_type_coercion",
    "format_identifier",
    "get_statement_config",
    "handle_postgres_rollback",
    "is_postgres_dialect",
    "is_shared_object_driver",
    "normalize_column_names",
    "normalize_connection_aliases",
    "normalize_driver_path",
    "normalize_postgres_empty_parameters",
    "normalize_script_rowcount",
    "prepare_batch_with_casts",
    "prepare_parameters_with_casts",
    "prepare_postgres_parameters",
    "prepare_postgres_uuid_bindings",
    "read_duckdb_profile_json",
    "resolve_column_names",
    "resolve_dialect_from_config",
    "resolve_dialect_from_driver_path",
    "resolve_dialect_name",
    "resolve_driver_connect_func",
    "resolve_driver_name",
    "resolve_driver_name_from_config",
    "resolve_many_rowcount",
    "resolve_parameter_casts",
    "resolve_rowcount",
)

COLUMN_CACHE_MAX_SIZE: int = 256
_IMPLICIT_UPPER_COLUMN_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
_UUID_TYPES: Final[tuple[type[Any], ...]] = tuple(build_uuid_coercions())
_STRICT_COERCION_ALLOWED_TYPES: Final[tuple[type[Any], ...]] = (
    type(None),
    bool,
    int,
    float,
    str,
    bytes,
    bytearray,
    memoryview,
    decimal.Decimal,
    dt.date,
    dt.time,
    dt.datetime,
    dt.timedelta,
    UUID,
    list,
    tuple,
    dict,
    *_UUID_TYPES,
)

DIALECT_PATTERNS: "dict[str, tuple[str, ...]]" = {
    "postgres": ("postgres", "postgresql"),
    "bigquery": ("bigquery",),
    "sqlite": ("sqlite", "flight", "flightsql"),
    "duckdb": ("duckdb", "gizmosql", "gizmo", "quack"),
    "mysql": ("mysql",),
    "snowflake": ("snowflake",),
    "oracle": ("oracle",),
    "db2": ("db2", "ibm db2"),
    "mssql": ("mssql", "sql server", "sqlserver", "microsoft sql"),
}


DIALECT_PARAMETER_STYLES: "dict[str, tuple[ParameterStyle, list[ParameterStyle]]]" = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (
        ParameterStyle.QMARK,
        [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.NAMED_COLON],
    ),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
    "oracle": (
        ParameterStyle.POSITIONAL_COLON,
        [ParameterStyle.POSITIONAL_COLON, ParameterStyle.NAMED_COLON, ParameterStyle.QMARK],
    ),
    "db2": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NAMED_COLON]),
    "mssql": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_AT]),
    "tsql": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_AT]),
}

_DRIVER_ALIASES: "dict[str, str]" = {
    "sqlite": "adbc_driver_sqlite.dbapi.connect",
    "sqlite3": "adbc_driver_sqlite.dbapi.connect",
    "duckdb": "adbc_driver_duckdb.dbapi.connect",
    "postgres": "adbc_driver_postgresql.dbapi.connect",
    "postgresql": "adbc_driver_postgresql.dbapi.connect",
    "pg": "adbc_driver_postgresql.dbapi.connect",
    "snowflake": "adbc_driver_snowflake.dbapi.connect",
    "sf": "adbc_driver_snowflake.dbapi.connect",
    "bigquery": "adbc_driver_bigquery.dbapi.connect",
    "bq": "adbc_driver_bigquery.dbapi.connect",
    "flightsql": "adbc_driver_flightsql.dbapi.connect",
    "grpc": "adbc_driver_flightsql.dbapi.connect",
    "gizmosql": "adbc_driver_flightsql.dbapi.connect",
    "gizmo": "adbc_driver_flightsql.dbapi.connect",
    "oracle": "adbc_driver_oracle.dbapi.connect",
    "oracledb": "adbc_driver_oracle.dbapi.connect",
    "db2": "adbc_driver_db2.dbapi.connect",
    "ibmdb2": "adbc_driver_db2.dbapi.connect",
    "ibm_db": "adbc_driver_db2.dbapi.connect",
    "quack": "adbc_driver_quack.dbapi.connect",
}

_URI_PREFIX_DRIVER: "tuple[tuple[str, str], ...]" = (
    ("postgresql://", "adbc_driver_postgresql.dbapi.connect"),
    ("postgres://", "adbc_driver_postgresql.dbapi.connect"),
    ("sqlite://", "adbc_driver_sqlite.dbapi.connect"),
    ("duckdb://", "adbc_driver_duckdb.dbapi.connect"),
    ("grpc://", "adbc_driver_flightsql.dbapi.connect"),
    ("grpc+tls://", "adbc_driver_flightsql.dbapi.connect"),
    ("gizmosql://", "adbc_driver_flightsql.dbapi.connect"),
    ("gizmo://", "adbc_driver_flightsql.dbapi.connect"),
    ("snowflake://", "adbc_driver_snowflake.dbapi.connect"),
    ("bigquery://", "adbc_driver_bigquery.dbapi.connect"),
    ("oracle://", "adbc_driver_oracle.dbapi.connect"),
    ("db2://", "adbc_driver_db2.dbapi.connect"),
    ("quack://", "adbc_driver_quack.dbapi.connect"),
    ("quack+tls://", "adbc_driver_quack.dbapi.connect"),
)

_DRIVER_PATH_KEYWORDS_TO_DIALECT: "tuple[tuple[str, str], ...]" = (
    ("postgresql", "postgres"),
    ("sqlite", "sqlite"),
    ("duckdb", "duckdb"),
    ("bigquery", "bigquery"),
    ("snowflake", "snowflake"),
    ("gizmosql", "duckdb"),
    ("quack", "duckdb"),
    ("oracle", "oracle"),
    ("db2", "db2"),
    ("mssql", "mssql"),
    ("sqlserver", "mssql"),
    ("flightsql", "sqlite"),
    ("grpc", "sqlite"),
)

_BIGQUERY_DB_KWARGS_OPTIONS: "tuple[tuple[str, str], ...]" = (
    ("project_id", "adbc.bigquery.sql.project_id"),
    ("dataset_id", "adbc.bigquery.sql.dataset_id"),
    ("token", "adbc.bigquery.sql.auth.refresh_token"),
)
_SHARED_OBJECT_SUFFIXES: "tuple[str, ...]" = (".so", ".dylib", ".dll")
_DRIVER_MANAGER_CONNECT: Final[str] = "adbc_driver_manager.dbapi.connect"
_GIZMOSQL_NATIVE_CONNECT: Final[str] = "adbc_driver_gizmosql.dbapi.connect"
_FLIGHTSQL_CONNECT: Final[str] = "adbc_driver_flightsql.dbapi.connect"
_MSSQL_NATIVE_CONNECT: Final[str] = "adbc_driver_mssql.dbapi.connect"
_QUACK_AUTH_KEY: Final[str] = "adbc.quack.token"
_GIZMOSQL_AUTH_TYPE_KEY: Final[str] = "adbc.gizmosql.auth_type"
_FLIGHTSQL_DB_KWARGS_FIELDS: "tuple[str, ...]" = ("username", "password")
_FLIGHTSQL_TLS_SKIP_VERIFY_KEY: Final[str] = "adbc.flight.sql.client_option.tls_skip_verify"
_FLIGHTSQL_AUTHORIZATION_HEADER_KEY: Final[str] = "adbc.flight.sql.authorization_header"
_FLIGHTSQL_WITH_MAX_MSG_SIZE_KEY: Final[str] = "adbc.flight.sql.client_option.with_max_msg_size"
_FLIGHTSQL_TIMEOUT_QUERY_KEY: Final[str] = "adbc.flight.sql.rpc.timeout_seconds.query"
_FLIGHTSQL_TIMEOUT_FETCH_KEY: Final[str] = "adbc.flight.sql.rpc.timeout_seconds.fetch"
_FLIGHTSQL_TLS_ROOT_CERTS_KEY: Final[str] = "adbc.flight.sql.client_option.tls_root_certs"
_FLIGHTSQL_MTLS_CERT_CHAIN_KEY: Final[str] = "adbc.flight.sql.client_option.mtls_cert_chain"
_FLIGHTSQL_MTLS_PRIVATE_KEY_KEY: Final[str] = "adbc.flight.sql.client_option.mtls_private_key"
_FLIGHTSQL_WITH_COOKIE_MIDDLEWARE_KEY: Final[str] = "adbc.flight.sql.rpc.with_cookie_middleware"
_FLIGHTSQL_RPC_CALL_HEADER_PREFIX: Final[str] = "adbc.flight.sql.rpc.call_header."
_SQLSTATE_CLASS_CODE_LEN = 2
_SQLSTATE_DESCRIPTIONS: dict[str, str] = {
    "23": "integrity constraint violation",
    "40": "transaction error",
    "42": "SQL syntax error",
    "08": "connection error",
    "22": "data error",
    "28": "authorization error",
    "57": "operational error",
    "02": "no data",
}
_ORA_CODE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\bORA-(\d{5})\b", re.IGNORECASE)
_ORA_INTEGRITY_START: Final[int] = 2290
_ORA_INTEGRITY_END: Final[int] = 2300
_ORA_PARSING_START: Final[int] = 900
_ORA_PARSING_END: Final[int] = 1000
_ORA_ERROR_CODE_MAP: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    1: (UniqueViolationError, "unique constraint violation"),
    60: (DeadlockError, "deadlock detected"),
    900: (SQLParsingError, "invalid SQL statement"),
    904: (SQLParsingError, "invalid identifier"),
    907: (SQLParsingError, "missing right parenthesis"),
    923: (SQLParsingError, "FROM keyword not found where expected"),
    933: (SQLParsingError, "SQL command not properly ended"),
    936: (SQLParsingError, "missing expression"),
    942: (SQLParsingError, "table or view does not exist"),
    1013: (QueryTimeoutError, "user requested cancel of current operation"),
    1017: (PermissionDeniedError, "invalid username/password"),
    1031: (PermissionDeniedError, "insufficient privileges"),
    1400: (NotNullViolationError, "not-null constraint violation"),
    1407: (NotNullViolationError, "not-null constraint violation"),
    1438: (DataError, "numeric precision overflow"),
    1722: (DataError, "invalid number"),
    1843: (DataError, "not a valid month"),
    1858: (DataError, "invalid character"),
    1861: (DataError, "literal does not match format string"),
    2290: (CheckViolationError, "check constraint violation"),
    2291: (ForeignKeyViolationError, "foreign key constraint violation"),
    2292: (ForeignKeyViolationError, "foreign key constraint violation"),
    3113: (DatabaseConnectionError, "end-of-file on communication channel"),
    3114: (DatabaseConnectionError, "not connected to ORACLE"),
    3135: (DatabaseConnectionError, "connection lost contact"),
    8177: (SerializationConflictError, "cannot serialize access"),
    12154: (DatabaseConnectionError, "TNS resolution failed"),
    12170: (QueryTimeoutError, "connect timeout"),
    12505: (DatabaseConnectionError, "listener rejected"),
    12514: (DatabaseConnectionError, "service not known"),
    12541: (DatabaseConnectionError, "no listener"),
    12899: (DataError, "value too large for column"),
}
_DB2_SQLSTATE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:SQLSTATE[=:\s]+|state[:=\s]+)([0-9A-Z]{5})\b", re.IGNORECASE
)
_DB2_SQLCODE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\bSQL(?:CODE[=:\s]*)?(-?\d{3,5})[NCW]?\b", re.IGNORECASE)
_DB2_REASON_CODE_PATTERN: Final[re.Pattern[str]] = re.compile(r'Reason code\s*"?(\d+)"?', re.IGNORECASE)
_DB2_LOCK_TIMEOUT_REASON_CODE: Final[int] = 68
_DB2_UNDEFINED_OBJECT_CODE: Final[int] = 204
_DB2_ROLLBACK_SQLSTATES: Final[frozenset[str]] = frozenset({"40001", "57033"})
_DB2_ROLLBACK_SQLCODES: Final[frozenset[int]] = frozenset({911, 913})
_DB2_SQLCODE_MAP: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    104: (SQLParsingError, "sql syntax error"),
    204: (SQLParsingError, "undefined object"),
    302: (DataError, "numeric or string value out of range"),
    407: (NotNullViolationError, "not-null constraint violation"),
    433: (DataError, "value is too long"),
    530: (ForeignKeyViolationError, "foreign key constraint violation"),
    532: (ForeignKeyViolationError, "foreign key constraint violation"),
    545: (CheckViolationError, "check constraint violation"),
    551: (PermissionDeniedError, "permission denied"),
    552: (PermissionDeniedError, "authorization error"),
    803: (UniqueViolationError, "unique constraint violation"),
    900: (DatabaseConnectionError, "connection does not exist or was severed"),
    952: (QueryTimeoutError, "statement execution interrupted or timed out"),
    1042: (DatabaseConnectionError, "unexpected system error occurred"),
    30081: (DatabaseConnectionError, "communication failure"),
}
_MSSQL_MSG_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\b(?:Msg|error|number)\s+(\d{3,5})\b", re.IGNORECASE)
_MSSQL_CONSTRAINT_ERROR_CODE: Final[int] = 547
_MSSQL_ERROR_CODE_MAP: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    53: (DatabaseConnectionError, "could not open connection to SQL Server"),
    102: (SQLParsingError, "incorrect syntax"),
    105: (SQLParsingError, "unclosed quotation mark"),
    156: (SQLParsingError, "incorrect syntax near keyword"),
    207: (SQLParsingError, "invalid column name"),
    208: (SQLParsingError, "invalid object name"),
    229: (PermissionDeniedError, "permission denied on object"),
    230: (PermissionDeniedError, "permission denied on column"),
    241: (DataError, "conversion failed when converting date/time"),
    242: (DataError, "datetime data type conversion out-of-range"),
    245: (DataError, "conversion failed when converting varchar to int"),
    262: (PermissionDeniedError, "permission denied in database"),
    515: (NotNullViolationError, "cannot insert the value NULL into column"),
    1205: (DeadlockError, "transaction was deadlocked on lock resources"),
    1222: (QueryTimeoutError, "lock request time out period exceeded"),
    2601: (UniqueViolationError, "cannot insert duplicate key row"),
    2627: (UniqueViolationError, "violation of unique or primary key constraint"),
    2628: (DataError, "string or binary data would be truncated"),
    3960: (SerializationConflictError, "snapshot isolation transaction aborted due to update conflict"),
    4060: (DatabaseConnectionError, "cannot open database requested by the login"),
    8114: (DataError, "error converting data type"),
    8115: (DataError, "arithmetic overflow error"),
    8152: (DataError, "string or binary data would be truncated"),
    18456: (PermissionDeniedError, "login failed for user"),
}


def detect_dialect(connection: Any, logger: Any | None = None, *, fallback_dialect: "str | None" = None) -> str:
    """Detect database dialect from ADBC driver information.

    Args:
        connection: ADBC connection with driver metadata.
        logger: Optional logger for diagnostics.
        fallback_dialect: Pre-resolved dialect from config. Used when introspection
            yields no match, before defaulting to ``postgres``.

    Returns:
        Detected dialect name, defaulting to ``postgres``.
    """
    try:
        driver_info = _get_adbc_driver_info(connection)
        vendor_name = driver_info.get("vendor_name", "").lower()
        vendor_version = driver_info.get("vendor_version", "").lower()
        driver_name = driver_info.get("driver_name", "").lower()

        if "gizmosql" in vendor_name or "gizmosql" in vendor_version:
            if "sqlite" in vendor_version:
                return "sqlite"
            if "duckdb" in vendor_version:
                return "duckdb"
            return "duckdb"

        for dialect, patterns in DIALECT_PATTERNS.items():
            for pattern in patterns:
                if pattern in vendor_name or pattern in driver_name:
                    if logger is not None:
                        logger.debug("Dialect detected: %s (from %s/%s)", dialect, vendor_name, driver_name)
                    return dialect
    except Exception as exc:
        if logger is not None:
            logger.debug("Dialect detection failed: %s", exc)

    if fallback_dialect:
        if logger is not None:
            logger.debug("Dialect from connection_config: %s (introspection yielded no match)", fallback_dialect)
        return fallback_dialect

    if logger is not None:
        logger.warning("Could not determine dialect from driver info. Defaulting to 'postgres'.")
    return "postgres"


def detect_postgres_extensions(
    connection: Any, *, enable_pgvector: bool = False, enable_paradedb: bool = False, enable_pg_textsearch: bool = False
) -> "tuple[bool, bool, bool]":
    """Detect pgvector, paradedb, and pg_textsearch extensions on a postgres connection.

    Queries ``pg_extension`` for the ``vector``, ``pg_search``, and ``pg_textsearch`` extensions.
    Returns cached-friendly booleans suitable for storing on the config instance.

    Args:
        connection: ADBC connection to a PostgreSQL database.
        enable_pgvector: Whether to check for the pgvector extension.
        enable_paradedb: Whether to check for the pg_search extension.
        enable_pg_textsearch: Whether to check for the pg_textsearch extension.

    Returns:
        Tuple of ``(pgvector_available, paradedb_available, pg_textsearch_available)``.
    """
    extensions: list[str] = []
    if enable_pgvector:
        extensions.append("vector")
    if enable_paradedb:
        extensions.append("pg_search")
    if enable_pg_textsearch:
        extensions.append("pg_textsearch")

    if not extensions:
        return False, False, False

    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT extname FROM pg_extension WHERE extname = ANY($1::text[])", [extensions])
            rows = cursor.fetchall()
            detected: set[str] = {row[0] for row in rows} if rows else set()
            return "vector" in detected, "pg_search" in detected, "pg_textsearch" in detected
        finally:
            cursor.close()
    except Exception:
        return False, False, False


def is_shared_object_driver(driver_name: str) -> bool:
    """Return whether a driver name points at a shared library rather than a module.

    Args:
        driver_name: Raw driver name or path from configuration.

    Returns:
        True when the value is a filesystem path to a driver shared object.
    """
    stripped = driver_name.strip()
    if not stripped:
        return False
    lowered = stripped.lower()
    return lowered.endswith(_SHARED_OBJECT_SUFFIXES) or "/" in stripped or "\\" in stripped


def normalize_driver_path(driver_name: str) -> str:
    """Normalize a driver name to an importable connect function path."""
    stripped = driver_name.strip()
    if is_shared_object_driver(stripped):
        return _DRIVER_MANAGER_CONNECT
    if stripped.endswith(".dbapi.connect"):
        return stripped
    if stripped.endswith(".dbapi"):
        return f"{stripped}.connect"
    if "." in stripped:
        return stripped
    return f"{stripped}.dbapi.connect"


def driver_from_uri(uri: str) -> "str | None":
    """Resolve a default driver connect path from a URI."""
    lowered = uri.lower()
    if lowered.startswith(("gizmosql://", "gizmo://")):
        return _resolve_gizmosql_driver_connect()
    if lowered.startswith(("mssql://", "sqlserver://")):
        return _resolve_mssql_driver_connect()
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if lowered.startswith(prefix):
            return driver_path
    if _parse_db2_cli_dsn(uri) is not None:
        return "adbc_driver_db2.dbapi.connect"
    return None


def driver_kind_from_driver_name(driver_name: str) -> "str | None":
    """Return a canonical driver kind based on driver name content."""
    lowered_name = driver_name.lower()
    if lowered_name in {"gizmosql", "gizmo"} or "gizmosql" in lowered_name:
        return "gizmosql"
    if lowered_name in {"mssql", "sqlserver", "tsql"} or "mssql" in lowered_name or "sqlserver" in lowered_name:
        return "mssql"
    if lowered_name in {"oracle", "oracledb"} or "oracle" in lowered_name:
        return "oracle"
    if lowered_name in {"db2", "ibmdb2", "ibm_db"} or "db2" in lowered_name:
        return "db2"
    if lowered_name == "quack" or "quack" in lowered_name:
        return "quack"
    resolved = _DRIVER_ALIASES.get(lowered_name, driver_name)
    lowered = resolved.lower()
    for keyword, _dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
        if keyword in lowered:
            return keyword
    return None


def driver_kind_from_uri(uri: str) -> "str | None":
    """Return a canonical driver kind based on URI scheme."""
    lowered = uri.lower()
    if lowered.startswith(("gizmosql://", "gizmo://")):
        return "gizmosql"
    if lowered.startswith(("mssql://", "sqlserver://")):
        return "mssql"
    if lowered.startswith(("quack://", "quack+tls://")):
        return "quack"
    if lowered.startswith("oracle://"):
        return "oracle"
    if lowered.startswith("db2://") or _parse_db2_cli_dsn(uri) is not None:
        return "db2"
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if lowered.startswith(prefix):
            return driver_kind_from_driver_name(driver_path)
    return None


def resolve_driver_name(driver_name: str | None, uri: str | None) -> str:
    """Resolve and normalize the driver name."""
    if isinstance(driver_name, str):
        if is_shared_object_driver(driver_name):
            return _DRIVER_MANAGER_CONNECT
        lowered_driver = driver_name.lower()
        if lowered_driver in {"gizmosql", "gizmo"}:
            return _resolve_gizmosql_driver_connect()
        if lowered_driver in {"mssql", "sqlserver", "tsql"}:
            return _resolve_mssql_driver_connect()
        alias = _DRIVER_ALIASES.get(lowered_driver)
        if alias is not None:
            return alias
        return normalize_driver_path(driver_name)

    if isinstance(uri, str):
        resolved = driver_from_uri(uri)
        if resolved is not None:
            return resolved

    return "adbc_driver_sqlite.dbapi.connect"


def resolve_driver_name_from_config(connection_config: "Mapping[str, Any]") -> str:
    """Resolve and normalize the driver name from a connection config mapping."""
    db_kwargs = connection_config.get("db_kwargs")
    nested_uri = db_kwargs.get("uri") if isinstance(db_kwargs, dict) else None
    uri = (
        connection_config.get("uri")
        or connection_config.get("url")
        or connection_config.get("dsn")
        or connection_config.get("connection_string")
        or nested_uri
    )
    driver_name = connection_config.get("driver_name") or connection_config.get("driver")
    return resolve_driver_name(driver_name, uri)


def resolve_driver_connect_func(driver_name: str | None, uri: str | None) -> "Callable[..., Any]":
    """Resolve the driver connect function.

    Raises:
        ImproperConfigurationError: If driver cannot be loaded.
    """
    driver_path = resolve_driver_name(driver_name, uri)
    try:
        connect_func = import_string(driver_path)
    except ImportError as exc:
        msg = f"Failed to import connect function from '{driver_path}'. Is the driver installed? Error: {exc}"
        raise ImproperConfigurationError(msg) from exc

    if not callable(connect_func):
        msg = f"The path '{driver_path}' did not resolve to a callable function."
        raise ImproperConfigurationError(msg)

    return cast("Callable[..., Any]", connect_func)


def resolve_dialect_from_driver_path(driver_path: str) -> str:
    """Get the SQL dialect type based on the driver path.

    Only the final path segment is considered, because a directory on the way to
    the driver can carry the name of an unrelated database.
    """
    candidate = driver_path.replace("\\", "/").rsplit("/", 1)[-1] or driver_path
    for keyword, dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
        if keyword in candidate:
            return dialect
    return "sqlite"


def resolve_dialect_from_config(connection_config: "Mapping[str, Any]") -> str:
    """Resolve dialect using ADBC connection configuration.

    GizmoSQL connections default to DuckDB unless the backend is explicitly overridden.
    """
    backend = connection_config.get("gizmosql_backend")
    if isinstance(backend, str):
        lowered_backend = backend.lower()
        if lowered_backend in {"duckdb", "sqlite"}:
            return lowered_backend

    driver_name = connection_config.get("driver_name") or connection_config.get("driver")
    if isinstance(driver_name, str):
        lowered_driver_name = driver_name.lower()
        if lowered_driver_name in {"gizmosql", "gizmo"}:
            return "duckdb"
        if lowered_driver_name in {"mssql", "sqlserver", "tsql"}:
            return "mssql"
        if lowered_driver_name in {"oracle", "oracledb"}:
            return "oracle"
        if lowered_driver_name in {"db2", "ibmdb2", "ibm_db"}:
            return "db2"
        if lowered_driver_name == "quack":
            return "duckdb"
        if is_shared_object_driver(driver_name):
            return resolve_dialect_from_driver_path(lowered_driver_name)

    db_kwargs = connection_config.get("db_kwargs")
    nested_uri = db_kwargs.get("uri") if isinstance(db_kwargs, dict) else None
    uri = (
        connection_config.get("uri")
        or connection_config.get("url")
        or connection_config.get("dsn")
        or connection_config.get("connection_string")
        or nested_uri
    )
    if isinstance(uri, str):
        lowered_uri = uri.lower()
        if lowered_uri.startswith(("gizmosql://", "gizmo://", "quack://", "quack+tls://")):
            return "duckdb"
        if lowered_uri.startswith(("mssql://", "sqlserver://")):
            return "mssql"
        if lowered_uri.startswith("oracle://"):
            return "oracle"
        if lowered_uri.startswith("db2://") or _parse_db2_cli_dsn(uri) is not None:
            return "db2"
        if driver_name is None and lowered_uri.startswith("grpc+tls://"):
            return "duckdb"

    return resolve_dialect_from_driver_path(resolve_driver_name_from_config(connection_config))


def normalize_connection_aliases(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Normalize URI and file-path aliases in an ADBC connection config mapping.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with normalized ``uri`` / ``path`` keys.
    """
    config = dict(connection_config)

    if "uri" not in config:
        uri_alias = config.pop("url", None) or config.pop("dsn", None) or config.pop("connection_string", None)
        if uri_alias is not None:
            config["uri"] = uri_alias
    else:
        config.pop("url", None)
        config.pop("dsn", None)
        config.pop("connection_string", None)

    driver_name = config.get("driver_name") or config.get("driver")
    uri = config.get("uri")
    driver_kind: str | None = None
    if isinstance(driver_name, str):
        driver_kind = driver_kind_from_driver_name(driver_name)
    if driver_kind is None and isinstance(uri, str):
        driver_kind = driver_kind_from_uri(uri)

    if (driver_kind == "sqlite" or driver_kind is None) and "uri" not in config and "db_kwargs" not in config:
        db_path = (
            config.pop("database", None)
            or config.pop("db", None)
            or config.pop("path", None)
            or config.pop("file", None)
        )
        if db_path is not None:
            config["uri"] = db_path
    elif driver_kind == "duckdb" and "path" not in config:
        duck_path = (
            config.pop("database", None)
            or config.pop("db", None)
            or config.pop("file", None)
            or config.pop("path", None)
        )
        if duck_path is not None:
            config["path"] = duck_path

    return config


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build a normalized connection configuration dictionary.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Normalized connection configuration dictionary.
    """
    config = normalize_connection_aliases(connection_config)

    driver_name = config.get("driver_name") or config.get("driver")
    db_kwargs_existing = config.get("db_kwargs")
    nested_uri = db_kwargs_existing.get("uri") if isinstance(db_kwargs_existing, dict) else None
    uri = config.get("uri") or nested_uri
    driver_kind: str | None = None
    if isinstance(driver_name, str):
        driver_kind = driver_kind_from_driver_name(driver_name)
    if driver_kind is None and isinstance(uri, str):
        driver_kind = driver_kind_from_uri(uri)
    if driver_kind is None and "uri" in config:
        driver_kind = "sqlite"

    if isinstance(uri, str) and driver_kind == "sqlite" and uri.lower().startswith("sqlite://"):
        config["uri"] = uri[9:]
    if isinstance(uri, str) and driver_kind == "duckdb" and uri.lower().startswith("duckdb://"):
        config["path"] = uri[9:]
        config.pop("uri", None)
    if isinstance(uri, str) and driver_kind == "db2" and not uri.lower().startswith("db2://"):
        _normalize_db2_cli_dsn(config, uri)

    legacy_entrypoint = config.pop("adbc_driver_manager_entrypoint", None)
    if legacy_entrypoint is not None:
        config.setdefault("entrypoint", legacy_entrypoint)

    resolved_connect_path = resolve_driver_name(
        driver_name if isinstance(driver_name, str) else None, uri if isinstance(uri, str) else None
    )
    uses_driver_manager = resolved_connect_path == _DRIVER_MANAGER_CONNECT
    if not uses_driver_manager:
        config.pop("entrypoint", None)

    _lift_driver_specific_db_kwargs(config, driver_kind, resolved_connect_path)

    config.pop("driver_name", None)
    if uses_driver_manager:
        _apply_driver_manager_config(config, driver_kind, driver_name)

    return config


def resolve_dialect_name(dialect: Any) -> str:
    """Return the normalized dialect name string."""
    if dialect is None:
        return ""
    if isinstance(dialect, str):
        return dialect.lower()
    if isinstance(dialect, type) and issubclass(dialect, sqlglot.Dialect):
        return dialect.__name__.lower()
    if isinstance(dialect, sqlglot.Dialect):
        return type(dialect).__name__.lower()
    return str(dialect)


def is_postgres_dialect(dialect_name: str) -> bool:
    """Return True when the dialect indicates PostgreSQL.

    Includes pgvector, paradedb, and pg_textsearch extension dialects.
    """
    return dialect_name in {"postgres", "postgresql", "pgvector", "paradedb", "pg_textsearch", "pgtextsearch"}


def handle_postgres_rollback(dialect: str, cursor: Any, logger: Any | None = None) -> None:
    """Clear an aborted PostgreSQL transaction after a statement failure.

    The connection-level rollback is preferred because the driver refuses to
    start a transaction on an errored connection, which a cursor needs in order
    to issue a ``ROLLBACK`` statement.

    Args:
        dialect: Active dialect identifier.
        cursor: Database cursor whose connection should be rolled back.
        logger: Optional logger for diagnostics.
    """
    if not is_postgres_dialect(dialect):
        return

    connection = getattr(cursor, "connection", None)
    if connection is not None and _try_rollback(connection.rollback):
        _log_rollback(logger)
        return

    if _try_rollback(partial(cursor.execute, "ROLLBACK")):
        _log_rollback(logger)


def normalize_postgres_empty_parameters(dialect: str, parameters: Any) -> Any:
    """Normalize empty parameter payloads for PostgreSQL drivers.

    Args:
        dialect: Active dialect identifier.
        parameters: Parameter payload in any supported shape.

    Returns:
        Normalized parameter payload.
    """
    if is_postgres_dialect(dialect) and isinstance(parameters, dict) and not parameters:
        return None
    return parameters


def prepare_postgres_parameters(
    parameters: Any,
    parameter_casts: "dict[int, str]",
    statement_config: "StatementConfig",
    *,
    dialect: str,
    json_serializer: "Callable[[Any], str]",
) -> Any:
    """Prepare Postgres parameters with cast-aware coercion."""
    postgres_compatible = normalize_postgres_empty_parameters(dialect, parameters)
    converter = get_adbc_type_converter(dialect, json_serializer=json_serializer)
    if isinstance(postgres_compatible, (list, tuple)) and hasattr(converter, "convert_sequence"):
        seq = [
            converter.convert_sequence(item, json_serializer=json_serializer)
            if isinstance(item, (list, tuple))
            else item
            for item in postgres_compatible
        ]
        postgres_compatible = tuple(seq) if isinstance(postgres_compatible, tuple) else seq
    if not parameter_casts:
        return postgres_compatible
    return prepare_parameters_with_casts(
        postgres_compatible, parameter_casts, statement_config, dialect=dialect, json_serializer=json_serializer
    )


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map ADBC errors to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. SQLSTATE codes (most reliable for ADBC drivers)
        2. Error message patterns
        3. Default SQLSpecError fallback

    Args:
        error: The ADBC exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    status_code = getattr(error, "status_code", None)
    status_name = getattr(status_code, "name", str(status_code)).upper()
    if status_name == "TIMEOUT":
        return _create_adbc_error(error, QueryTimeoutError, "query timeout")
    if status_name == "CANCELLED":
        return _create_adbc_error(error, OperationCancelledError, "operation cancelled")

    raw_message = str(error)
    error_msg = raw_message.lower()
    sqlstate_attr = error.sqlstate if has_sqlstate(error) else None
    sqlstate = sqlstate_attr.strip().upper() if isinstance(sqlstate_attr, str) and sqlstate_attr.strip() else None

    if enterprise_mapped := _map_enterprise_exception(error, raw_message, error_msg, sqlstate):
        return enterprise_mapped

    if sqlstate:
        return _map_sqlstate_exception(error, sqlstate)

    if "unique" in error_msg or "duplicate" in error_msg:
        return _create_adbc_error(error, UniqueViolationError, "unique constraint violation")
    if "foreign key" in error_msg:
        return _create_adbc_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if "not null" in error_msg or "null value" in error_msg:
        return _create_adbc_error(error, NotNullViolationError, "not-null constraint violation")
    if "check constraint" in error_msg:
        return _create_adbc_error(error, CheckViolationError, "check constraint violation")
    if "constraint" in error_msg:
        return _create_adbc_error(error, IntegrityError, "integrity constraint violation")

    if "deadlock" in error_msg:
        return _create_adbc_error(error, DeadlockError, "deadlock detected")
    if "serialization" in error_msg or "concurrent update" in error_msg:
        return _create_adbc_error(error, SerializationConflictError, "serialization failure")

    if message_class := _classify_timeout_or_cancellation(error_msg):
        return _create_adbc_error(error, message_class, "query terminated")

    if "permission" in error_msg or "denied" in error_msg or "unauthorized" in error_msg:
        return _create_adbc_error(error, PermissionDeniedError, "permission denied")

    if "syntax" in error_msg:
        return _create_adbc_error(error, SQLParsingError, "SQL parsing error")

    if "connection" in error_msg or "connect" in error_msg:
        return _create_adbc_error(error, DatabaseConnectionError, "connection error")

    return _create_adbc_error(error, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
    """Create the ADBC driver parameter profile."""

    return DriverParameterProfile(
        name="ADBC",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            dt.datetime: _identity,
            dt.date: _identity,
            dt.time: _identity,
            decimal.Decimal: _identity,
            bool: _identity,
            int: _identity,
            float: _identity,
            bytes: _identity,
            tuple: _convert_array_for_postgres_adbc,
            list: _convert_array_for_postgres_adbc,
            dict: _identity,
            **build_uuid_coercions(native=True),
        },
        extras={
            "type_coercion_overrides": {list: _convert_array_for_postgres_adbc, tuple: _convert_array_for_postgres_adbc}
        },
    )


def get_statement_config(detected_dialect: str) -> StatementConfig:
    """Create statement configuration for the specified dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        "postgres" if is_postgres_dialect(detected_dialect) else detected_dialect,
        (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    )

    if detected_dialect == "postgresql":
        sqlglot_dialect = "postgres"
    elif detected_dialect == "mssql":
        sqlglot_dialect = "tsql"
    else:
        sqlglot_dialect = detected_dialect
    profile = driver_profile

    parameter_overrides: dict[str, Any] = {
        "default_parameter_style": default_style,
        "supported_parameter_styles": set(supported_styles),
        "default_execution_parameter_style": default_style,
        "supported_execution_parameter_styles": set(supported_styles),
        "type_coercion_map": _BASE_TYPE_COERCION_MAP,
    }

    if detected_dialect == "duckdb":
        parameter_overrides["preserve_parameter_format"] = False
        parameter_overrides["supported_execution_parameter_styles"] = {ParameterStyle.QMARK, ParameterStyle.NUMERIC}
        parameter_overrides["ast_transformer"] = _transform_duckdb_copy_parameters
    elif detected_dialect == "oracle":
        parameter_overrides["preserve_parameter_format"] = False
        parameter_overrides["supported_execution_parameter_styles"] = {ParameterStyle.POSITIONAL_COLON}
    elif detected_dialect in {"db2", "mssql", "tsql"}:
        parameter_overrides["preserve_parameter_format"] = False
        parameter_overrides["supported_execution_parameter_styles"] = {ParameterStyle.QMARK}

    if is_postgres_dialect(detected_dialect):
        parameter_overrides["ast_transformer"] = build_null_pruning_transform(dialect=sqlglot_dialect)

    return build_statement_config_from_profile(
        profile, parameter_overrides=parameter_overrides, statement_overrides={"dialect": sqlglot_dialect}
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply ADBC driver feature defaults to the statement config.

    Args:
        statement_config: Base statement configuration.
        driver_features: Optional driver feature overrides.

    Returns:
        Updated statement configuration and normalized driver features.
    """
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_features = _normalize_adbc_driver_features(processed_features)
    resolved_dialect = resolve_dialect_name(statement_config.dialect).lower()

    json_serializer = cast("Callable[[Any], str] | None", processed_features.setdefault("json_serializer", to_json))
    processed_features.setdefault("enable_cast_detection", True)
    processed_features.setdefault("strict_type_coercion", False)
    processed_features.setdefault("enable_strict_type_coercion", processed_features["strict_type_coercion"])
    processed_features.setdefault("arrow_extension_types", True)
    processed_features.setdefault("enable_arrow_extension_types", processed_features["arrow_extension_types"])
    processed_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    processed_features.setdefault("enable_paradedb", True)
    processed_features.setdefault("enable_pg_textsearch", True)
    processed_features.setdefault("enable_lowercase_column_names", resolved_dialect in {"oracle", "db2"})
    processed_features.setdefault("enable_statement_profiling", False)

    if json_serializer is not None:
        statement_config = _apply_adbc_json_serializer(statement_config, json_serializer)

    return statement_config, processed_features


def create_duckdb_profile_path() -> Path:
    """Allocate a unique temporary file path for DuckDB JSON statement profiling."""
    return Path(tempfile.gettempdir()) / f"sqlspec_duckdb_profile_{uuid4().hex}.json"


def configure_adbc_duckdb_profiling(connection: Any, profile_path: Path) -> None:
    """Enable DuckDB JSON statement profiling on an ADBC DuckDB connection.

    Args:
        connection: Active ADBC DuckDB connection.
        profile_path: Temporary file path where DuckDB writes statement profiles.
    """
    escaped_path = str(profile_path).replace("'", "''")
    cursor = connection.cursor()
    try:
        cursor.execute("PRAGMA enable_profiling='json'")
        with contextlib.suppress(Exception):
            cursor.execute("SET profiling_coverage='ALL'")
        cursor.execute(f"SET profiling_output='{escaped_path}'")
    finally:
        with contextlib.suppress(Exception):
            cursor.close()


def cleanup_duckdb_profile_path(profile_path: Path | str | None) -> None:
    """Remove a temporary DuckDB JSON statement profile file if it exists."""
    if not profile_path:
        return
    with contextlib.suppress(OSError):
        Path(profile_path).unlink(missing_ok=True)


def read_duckdb_profile_json(profile_path: Path | str | None, executed_sql: str | None = None) -> dict[str, Any] | None:
    """Read and validate the DuckDB JSON profile file written for executed_sql."""
    if not profile_path:
        return None
    path_obj = Path(profile_path)
    if not path_obj.exists():
        return None
    try:
        raw_bytes = path_obj.read_bytes()
    except OSError:
        return None
    if not raw_bytes:
        return None
    try:
        parsed: Any = from_json(raw_bytes)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    query_name = parsed.get("query_name")
    if query_name is None and isinstance(parsed.get("query"), dict):
        query_name = parsed["query"].get("sql")
    if isinstance(query_name, str) and executed_sql:
        normalized_query = query_name.strip().rstrip(";").strip()
        normalized_sql = executed_sql.strip().rstrip(";").strip()
        if normalized_sql and normalized_query != normalized_sql:
            return None
    if "cpu_time" not in parsed and isinstance(parsed.get("query"), dict) and "cpu_time" in parsed["query"]:
        parsed["cpu_time"] = parsed["query"]["cpu_time"]
    if "children" not in parsed and isinstance(parsed.get("operator"), list) and parsed["operator"]:
        root_op = parsed["operator"][0]
        if isinstance(root_op, dict) and "children" in root_op:
            parsed["children"] = root_op["children"]
    return cast("dict[str, Any]", parsed)


def format_identifier(identifier: str) -> str:
    """Format and quote a potentially qualified SQL identifier for ADBC metadata queries."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"')
    return ".".join(quote_identifier(part) for part in parts)


def normalize_column_names(
    column_names: "list[str]", driver_features: "Mapping[str, Any] | None" = None
) -> "list[str]":
    """Lowercase implicit-uppercase column names when enabled in driver_features."""
    if not driver_features or not driver_features.get("enable_lowercase_column_names", False):
        return column_names
    return [name.lower() if name and _IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name) else name for name in column_names]


def enforce_strict_type_coercion(parameters: Any, *, is_many: bool = False) -> None:
    """Validate parameter values when ``strict_type_coercion`` is enabled.

    Args:
        parameters: Parameter payload to validate.
        is_many: Whether the payload represents an execute-many batch.

    Raises:
        SQLSpecError: If any parameter value is not a supported SQL/Arrow scalar or container type.
    """
    if parameters is None:
        return
    if is_many and isinstance(parameters, (list, tuple)):
        for row in parameters:
            _validate_strict_parameter_payload(row)
        return
    _validate_strict_parameter_payload(parameters)


def resolve_column_names(
    description: "list[Any] | None",
    cache: "dict[int, tuple[Any, list[str]]]",
    driver_features: "Mapping[str, Any] | None" = None,
) -> list[str]:
    """Resolve and cache ADBC cursor description column names."""
    if not description:
        return []

    cache_key = id(description)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]

    column_names = normalize_column_names([col[0] for col in description], driver_features)
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (description, column_names)
    return column_names


def collect_rows(
    fetched_data: "list[Any] | None",
    description: "list[Any] | None",
    *,
    column_names: "list[str] | None" = None,
    driver_features: "Mapping[str, Any] | None" = None,
) -> "tuple[list[Any], list[str]]":
    """Collect ADBC rows and column names.

    Returns raw data without dict conversion. The row format is detected
    by the driver and passed to ``create_execution_result`` so that
    ``SQLResult`` can handle lazy dict materialisation.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        column_names: Optional precomputed column names.
        driver_features: Optional driver feature configuration.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    resolved_column_names = (
        normalize_column_names([col[0] for col in description], driver_features)
        if column_names is None
        else normalize_column_names(column_names, driver_features)
    )
    if not fetched_data:
        return [], resolved_column_names
    return fetched_data, resolved_column_names


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an ADBC cursor.

    Args:
        cursor: ADBC cursor with optional rowcount metadata.

    Returns:
        Rowcount value or -1 when unavailable.
    """
    if not has_rowcount(cursor):
        return -1
    rowcount = cursor.rowcount
    if isinstance(rowcount, int):
        return rowcount
    return -1


def resolve_many_rowcount(cursor: Any, parameter_sets: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve execute_many rowcount with a parameter-count fallback.

    Args:
        cursor: ADBC cursor with optional rowcount metadata.
        parameter_sets: Execute-many payload.
        fallback_count: Optional precomputed execute-many payload size.

    Returns:
        Driver rowcount when available, otherwise parameter count when sized.
    """
    rowcount = resolve_rowcount(cursor)
    if rowcount != -1:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameter_sets, Sized):
        return len(parameter_sets)
    return -1


def normalize_script_rowcount(previous: int, cursor: Any) -> int:
    """Normalize script rowcount using the latest cursor value when present.

    Args:
        previous: Previously recorded rowcount.
        cursor: ADBC cursor with optional rowcount metadata.

    Returns:
        Updated rowcount value.
    """
    rowcount = resolve_rowcount(cursor)
    return rowcount if rowcount != -1 else previous


def resolve_parameter_casts(statement: "SQL") -> "dict[int, str]":
    """Return parameter cast mapping from a compiled SQL statement."""
    processed_state = statement.get_processed_state()
    if processed_state is not Empty:
        return processed_state.parameter_casts or {}
    return {}


def prepare_parameters_with_casts(
    parameters: Any,
    parameter_casts: "dict[int, str]",
    statement_config: "StatementConfig",
    *,
    dialect: str,
    json_serializer: "Callable[[Any], str]",
) -> Any:
    """Prepare parameters with cast-aware type coercion for ADBC."""
    json_encoder = statement_config.parameter_config.json_serializer or json_serializer

    if isinstance(parameters, (list, tuple)):
        converter = get_adbc_type_converter(dialect)
        type_map = statement_config.parameter_config.type_coercion_map
        dispatcher = type_coercion_dispatcher(tuple(type_map.items())) if type_map else None
        return _prepare_parameter_sequence_with_casts(
            parameters, parameter_casts, type_map, dispatcher, converter, json_encoder
        )
    return parameters


def prepare_batch_with_casts(
    parameter_sets: Any,
    parameter_casts: "dict[int, str]",
    statement_config: "StatementConfig",
    *,
    dialect: str,
    json_serializer: "Callable[[Any], str]",
) -> Any:
    """Prepare an execute-many batch while sharing converter setup across rows."""
    if not isinstance(parameter_sets, (list, tuple)):
        return normalize_postgres_empty_parameters(dialect, parameter_sets)

    postgres_compatible = [normalize_postgres_empty_parameters(dialect, row) for row in parameter_sets]
    if not parameter_casts:
        return postgres_compatible

    json_encoder = statement_config.parameter_config.json_serializer or json_serializer
    converter = get_adbc_type_converter(dialect)
    type_map = statement_config.parameter_config.type_coercion_map
    dispatcher = type_coercion_dispatcher(tuple(type_map.items())) if type_map else None
    return [
        _prepare_parameter_sequence_with_casts(row, parameter_casts, type_map, dispatcher, converter, json_encoder)
        if isinstance(row, (list, tuple))
        else row
        for row in postgres_compatible
    ]


def prepare_postgres_uuid_bindings(
    compiled_sql: str, prepared_parameters: Any, *, is_many: bool, dialect: str
) -> "tuple[str, object]":
    """Bind top-level UUID values through PostgreSQL-family ADBC drivers.

    Args:
        compiled_sql: SQL after normal statement compilation and cache rebinding.
        prepared_parameters: Current execution's driver parameters.
        is_many: Whether parameters contain an execute-many batch.
        dialect: Resolved ADBC dialect name.

    Returns:
        Rewritten SQL and current-call parameters with UUID values normalized,
        or the inputs unchanged when no placeholder needs a new UUID cast.
    """
    if not is_postgres_dialect(dialect):
        return compiled_sql, prepared_parameters
    if is_many:
        scalar_ordinals, array_ordinals = _detect_batch_uuid_ordinals(prepared_parameters)
    elif isinstance(prepared_parameters, (list, tuple)):
        scalar_ordinals, array_ordinals = _detect_uuid_ordinals(prepared_parameters)
    else:
        scalar_ordinals, array_ordinals = (), ()
    if not scalar_ordinals and not array_ordinals:
        return compiled_sql, prepared_parameters
    sqlglot_dialect = "postgres" if dialect == "postgresql" else dialect
    rewritten_sql, effective_scalars, effective_arrays = _rewrite_postgres_uuid_placeholders(
        compiled_sql, tuple(sorted(scalar_ordinals)), tuple(sorted(array_ordinals)), sqlglot_dialect
    )
    if not effective_scalars and not effective_arrays:
        return compiled_sql, prepared_parameters
    if is_many:
        converted_rows = [
            _convert_uuid_row(row, effective_scalars, effective_arrays, row_number)
            for row_number, row in enumerate(prepared_parameters, 1)
        ]
        converted_parameters = tuple(converted_rows) if isinstance(prepared_parameters, tuple) else converted_rows
    else:
        converted_parameters = _convert_uuid_row(prepared_parameters, effective_scalars, effective_arrays, 1)
    return rewritten_sql, converted_parameters


def _try_rollback(rollback: "Callable[[], Any]") -> bool:
    """Run a rollback callable and report whether it succeeded.

    Args:
        rollback: Zero-argument callable performing the rollback.

    Returns:
        True if the rollback completed without raising.
    """
    try:
        rollback()
    except Exception:
        return False
    return True


def _log_rollback(logger: "Any | None") -> None:
    """Record that a PostgreSQL rollback completed.

    Args:
        logger: Optional logger for diagnostics.
    """
    if logger is not None:
        logger.debug("PostgreSQL rollback executed after transaction failure")


def _identity(value: Any) -> Any:
    return value


def _convert_array_for_postgres_adbc(value: Any) -> Any:
    """Convert array values for PostgreSQL compatibility."""

    if isinstance(value, tuple):
        return list(value)
    return value


_BASE_TYPE_COERCION_MAP: Final[dict[type, Any]] = {
    dt.datetime: _identity,
    dt.date: _identity,
    dt.time: _identity,
    decimal.Decimal: _identity,
    bool: _identity,
    int: _identity,
    float: _identity,
    bytes: _identity,
    tuple: _convert_array_for_postgres_adbc,
    list: _convert_array_for_postgres_adbc,
    dict: _identity,
    UUID: _identity,
    **build_uuid_coercions(native=True),
}


def _parameter_ordinal(parameter: exp.Parameter) -> "int | None":
    value = parameter.this
    if not isinstance(value, exp.Literal) or value.is_string:
        return None
    try:
        return int(value.this)
    except (TypeError, ValueError):
        return None


def _direct_parameter_cast(parameter: exp.Parameter) -> "exp.Cast | None":
    current: exp.Expression = parameter
    parent = current.parent
    while isinstance(parent, exp.Paren):
        current = parent
        parent = current.parent
    if isinstance(parent, exp.Cast) and parent.this is current:
        return parent
    return None


def _is_uuid_data_type(target: Any) -> bool:
    """Report whether a cast target names the scalar ``UUID`` type."""
    if not isinstance(target, exp.DataType):
        return False
    if target.this == exp.DataType.Type.UUID:
        return True
    target_kind = target.args.get("kind")
    if isinstance(target_kind, exp.Dot):
        target_kind = target_kind.expression
    return (
        target.this == exp.DataType.Type.USERDEFINED
        and isinstance(target_kind, exp.Identifier)
        and target_kind.name.lower() == "uuid"
    )


def _is_uuid_array_data_type(target: Any) -> bool:
    """Report whether a cast target names a one-dimensional ``UUID[]`` type."""
    if not isinstance(target, exp.DataType) or target.this != exp.DataType.Type.ARRAY:
        return False
    element_types = target.expressions
    return len(element_types) == 1 and _is_uuid_data_type(element_types[0])


@lru_cache(maxsize=256)
def _rewrite_postgres_uuid_placeholders(
    sql: str, scalar_ordinals: "tuple[int, ...]", array_ordinals: "tuple[int, ...]", dialect: str
) -> "tuple[str, tuple[int, ...], tuple[int, ...]]":
    """Wrap requested UUID parameter placeholders in explicit UUID or UUID[] casts.

    Placeholders already carrying the cast their ordinal requires keep that cast
    and stay effective. Placeholders carrying any other explicit cast stay
    authoritative and are excluded from the returned effective ordinals. Results
    are memoized per ``(sql, scalar_ordinals, array_ordinals, dialect)`` so
    repeated executions of the same statement shape reuse the rewrite without
    parsing again.
    """
    try:
        expressions = cast(
            "list[exp.Expression]",
            [expression for expression in sqlglot.parse(sql, read=dialect) if expression is not None],
        )
    except ParseError as exc:
        msg = f"Failed to parse PostgreSQL ADBC SQL for UUID parameter binding: {exc}"
        raise SQLSpecError(msg) from exc
    if not expressions:
        msg = "Failed to parse PostgreSQL ADBC SQL for UUID parameter binding: SQLGlot returned no statements."
        raise SQLSpecError(msg)

    array_requested = set(array_ordinals)
    requested = set(scalar_ordinals) | array_requested
    authoritative: set[int] = set()
    for expression in expressions:
        for parameter in expression.find_all(exp.Parameter):
            ordinal = _parameter_ordinal(parameter)
            if ordinal is None or ordinal not in requested:
                continue
            cast_expression = _direct_parameter_cast(parameter)
            if cast_expression is None:
                continue
            target = cast_expression.args.get("to")
            matches_required_cast = (
                _is_uuid_array_data_type(target) if ordinal in array_requested else _is_uuid_data_type(target)
            )
            if not matches_required_cast:
                authoritative.add(ordinal)
    effective_scalars = tuple(ordinal for ordinal in scalar_ordinals if ordinal not in authoritative)
    effective_arrays = tuple(ordinal for ordinal in array_ordinals if ordinal not in authoritative)
    effective_array_set = set(effective_arrays)
    effective_set = set(effective_scalars) | effective_array_set

    def wrap_parameter(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Parameter):
            return node
        ordinal = _parameter_ordinal(node)
        if ordinal is None or ordinal not in effective_set:
            return node
        if _direct_parameter_cast(node) is not None:
            return node
        target_type = "UUID[]" if ordinal in effective_array_set else "UUID"
        return exp.Cast(this=node.copy(), to=exp.DataType.build(target_type, dialect=dialect))

    for expression in expressions:
        expression.transform(wrap_parameter, copy=False)
    rewritten = "; ".join(expression.sql(dialect=dialect, copy=False) for expression in expressions)
    return rewritten, effective_scalars, effective_arrays


def _sequence_holds_uuid(values: "list[Any] | tuple[Any, ...]") -> bool:
    """Report whether a sequence's first non-null element is a UUID."""
    for value in values:
        if value is not None:
            return isinstance(value, _UUID_TYPES)
    return False


def _detect_uuid_ordinals(parameters: "list[Any] | tuple[Any, ...]") -> "tuple[tuple[int, ...], tuple[int, ...]]":
    """Split a parameter sequence into scalar-UUID and UUID-array ordinals."""
    scalar_ordinals: list[int] = []
    array_ordinals: list[int] = []
    for ordinal, value in enumerate(parameters, 1):
        if isinstance(value, _UUID_TYPES):
            scalar_ordinals.append(ordinal)
        elif isinstance(value, (list, tuple)) and _sequence_holds_uuid(value):
            array_ordinals.append(ordinal)
    return tuple(scalar_ordinals), tuple(array_ordinals)


def _detect_batch_uuid_ordinals(parameters: Any) -> "tuple[tuple[int, ...], tuple[int, ...]]":
    if not isinstance(parameters, (list, tuple)) or not parameters:
        return (), ()
    first_row = parameters[0]
    if not isinstance(first_row, (list, tuple)):
        return (), ()
    expected_size = len(first_row)
    for row in parameters:
        if not isinstance(row, (list, tuple)) or len(row) != expected_size:
            return (), ()
    scalar_ordinals: list[int] = []
    array_ordinals: list[int] = []
    for ordinal in range(1, expected_size + 1):
        values = [row[ordinal - 1] for row in parameters]
        if any(isinstance(value, _UUID_TYPES) for value in values):
            scalar_ordinals.append(ordinal)
        elif any(isinstance(value, (list, tuple)) and _sequence_holds_uuid(value) for value in values):
            array_ordinals.append(ordinal)
    return tuple(scalar_ordinals), tuple(array_ordinals)


def _uuid_binding_error(ordinal: int, value: Any, row_number: int, element_index: "int | None" = None) -> str:
    """Build the incompatible-value message for a UUID parameter ordinal."""
    location = "" if element_index is None else f" at element {element_index}"
    return (
        f"ADBC PostgreSQL UUID parameter ordinal {ordinal} has incompatible "
        f"{type(value).__name__} value{location} in batch row {row_number}; expected a UUID object, "
        "parseable UUID string, or None."
    )


def _convert_uuid_value(value: Any, ordinal: int, row_number: int, element_index: "int | None" = None) -> Any:
    """Normalize one UUID-bound value to its canonical string form."""
    if value is None:
        return None
    if isinstance(value, _UUID_TYPES):
        return str(value)
    if not isinstance(value, str):
        raise SQLSpecError(_uuid_binding_error(ordinal, value, row_number, element_index))
    try:
        return str(uuid_from_string(value))
    except (TypeError, ValueError) as exc:
        raise SQLSpecError(_uuid_binding_error(ordinal, value, row_number, element_index)) from exc


def _null_array_element_error(ordinal: int, row_number: int, element_index: int) -> str:
    """Build the null-array-element message for a UUID array parameter ordinal."""
    return (
        f"ADBC PostgreSQL UUID parameter ordinal {ordinal} has a null value at element {element_index} "
        f"in batch row {row_number}; the PostgreSQL ADBC driver encodes null array elements as empty "
        "strings, which PostgreSQL rejects for UUID[]. Remove the null element or bind the array as text."
    )


def _convert_uuid_sequence(values: Any, ordinal: int, row_number: int) -> "list[Any]":
    """Normalize every element of a UUID array parameter."""
    converted: list[Any] = []
    for element_index, value in enumerate(values, 1):
        if value is None:
            raise SQLSpecError(_null_array_element_error(ordinal, row_number, element_index))
        converted.append(_convert_uuid_value(value, ordinal, row_number, element_index))
    return converted


def _convert_uuid_row(
    row: Any, scalar_ordinals: "tuple[int, ...]", array_ordinals: "tuple[int, ...]", row_number: int
) -> Any:
    converted = list(row)
    for ordinal in scalar_ordinals:
        converted[ordinal - 1] = _convert_uuid_value(converted[ordinal - 1], ordinal, row_number)
    for ordinal in array_ordinals:
        value = converted[ordinal - 1]
        if value is None:
            continue
        if not isinstance(value, (list, tuple)):
            raise SQLSpecError(_uuid_binding_error(ordinal, value, row_number))
        converted[ordinal - 1] = _convert_uuid_sequence(value, ordinal, row_number)
    return tuple(converted) if isinstance(row, tuple) else converted


def _create_adbc_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create an ADBC error instance without raising it."""
    msg = f"ADBC {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def _get_sqlstate_description(sqlstate: str) -> str:
    """Get a human-readable description for a SQLSTATE code class."""
    class_code = sqlstate[:_SQLSTATE_CLASS_CODE_LEN] if len(sqlstate) >= _SQLSTATE_CLASS_CODE_LEN else sqlstate
    return _SQLSTATE_DESCRIPTIONS.get(class_code, "database error")


def _map_oracle_exception(error: Any, raw_message: str) -> "SQLSpecError | None":
    match = _ORA_CODE_PATTERN.search(raw_message)
    if match is None:
        return None
    code = int(match.group(1))
    if code in _ORA_ERROR_CODE_MAP:
        exc_cls, desc = _ORA_ERROR_CODE_MAP[code]
        return _create_adbc_error(error, exc_cls, desc)
    if _ORA_INTEGRITY_START <= code < _ORA_INTEGRITY_END:
        return _create_adbc_error(error, IntegrityError, "integrity constraint violation")
    if _ORA_PARSING_START <= code < _ORA_PARSING_END:
        return _create_adbc_error(error, SQLParsingError, "SQL parsing error")
    return None


def _extract_db2_sqlcode(error: Any, raw_message: str, has_db2_marker: bool) -> "int | None":
    match = _DB2_SQLCODE_PATTERN.search(raw_message)
    if match is not None:
        return abs(int(match.group(1)))
    vendor_code = getattr(error, "vendor_code", None)
    if isinstance(vendor_code, int) and (vendor_code < 0 or has_db2_marker):
        return abs(vendor_code)
    return None


def _map_db2_exception(
    error: Any, raw_message: str, lowered_message: str, explicit_sqlstate: "str | None"
) -> "SQLSpecError | None":
    sqlstate = explicit_sqlstate
    if sqlstate is None:
        state_matches = _DB2_SQLSTATE_PATTERN.findall(raw_message)
        if state_matches:
            sqlstate = str(state_matches[-1]).upper()

    has_db2_marker = (
        "db2" in lowered_message
        or "sqlstate=" in lowered_message
        or "reason code" in lowered_message
        or _DB2_SQLCODE_PATTERN.search(raw_message) is not None
    )
    sqlcode = _extract_db2_sqlcode(error, raw_message, has_db2_marker)
    if not has_db2_marker and sqlcode is None and sqlstate not in {"57033", "42704"}:
        return None

    if sqlstate in _DB2_ROLLBACK_SQLSTATES or (sqlcode is not None and sqlcode in _DB2_ROLLBACK_SQLCODES):
        reason_match = _DB2_REASON_CODE_PATTERN.search(raw_message)
        if reason_match is not None and int(reason_match.group(1)) == _DB2_LOCK_TIMEOUT_REASON_CODE:
            return _create_adbc_error(error, QueryTimeoutError, "lock timeout")
        if sqlstate == "57033" or (sqlcode is not None and sqlcode in _DB2_ROLLBACK_SQLCODES) or has_db2_marker:
            return _create_adbc_error(error, DeadlockError, "deadlock detected")

    if sqlstate == "42704" or sqlcode == _DB2_UNDEFINED_OBJECT_CODE:
        return _create_adbc_error(error, SQLParsingError, "undefined object")
    if sqlstate == "42501" or (sqlcode is not None and sqlcode in {551, 552}):
        return _create_adbc_error(error, PermissionDeniedError, "permission denied")

    if sqlcode is not None and sqlcode in _DB2_SQLCODE_MAP:
        exc_cls, desc = _DB2_SQLCODE_MAP[sqlcode]
        return _create_adbc_error(error, exc_cls, desc)
    return None


def _map_mssql_exception(error: Any, raw_message: str, lowered_message: str) -> "SQLSpecError | None":
    has_mssql_marker = any(marker in lowered_message for marker in ("mssql", "sql server", "sqlserver", "tds:", "msg "))
    code: int | None = None
    vendor_code = getattr(error, "vendor_code", None)
    if isinstance(vendor_code, int) and vendor_code > 0 and has_mssql_marker:
        code = vendor_code
    else:
        msg_match = _MSSQL_MSG_NUMBER_PATTERN.search(raw_message)
        if msg_match is not None and has_mssql_marker:
            code = int(msg_match.group(1))

    if code is None:
        return None
    if code == _MSSQL_CONSTRAINT_ERROR_CODE:
        if "check constraint" in lowered_message:
            return _create_adbc_error(error, CheckViolationError, "check constraint violation")
        return _create_adbc_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if code in _MSSQL_ERROR_CODE_MAP:
        exc_cls, desc = _MSSQL_ERROR_CODE_MAP[code]
        return _create_adbc_error(error, exc_cls, desc)
    return None


def _map_enterprise_exception(
    error: Any, raw_message: str, lowered_message: str, sqlstate: "str | None"
) -> "SQLSpecError | None":
    if oracle_mapped := _map_oracle_exception(error, raw_message):
        return oracle_mapped
    if db2_mapped := _map_db2_exception(error, raw_message, lowered_message, sqlstate):
        return db2_mapped
    return _map_mssql_exception(error, raw_message, lowered_message)


def _map_sqlstate_exception(error: Any, sqlstate: str) -> SQLSpecError:
    if sqlstate == "23505":
        return _create_adbc_error(error, UniqueViolationError, "unique constraint violation")
    if sqlstate == "23503":
        return _create_adbc_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if sqlstate == "23502":
        return _create_adbc_error(error, NotNullViolationError, "not-null constraint violation")
    if sqlstate == "23514":
        return _create_adbc_error(error, CheckViolationError, "check constraint violation")

    if sqlstate == "40P01":
        return _create_adbc_error(error, DeadlockError, "deadlock detected")
    if sqlstate == "40001":
        return _create_adbc_error(error, SerializationConflictError, "serialization failure")

    if sqlstate == "57014":
        termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
        return _create_adbc_error(error, termination_class, "query terminated")

    if sqlstate == "42501":
        return _create_adbc_error(error, PermissionDeniedError, "insufficient privilege")
    if sqlstate == "28000":
        return _create_adbc_error(error, PermissionDeniedError, "invalid authorization")

    exc_class = map_sqlstate_to_exception(sqlstate)
    if exc_class is not None and exc_class is not SQLSpecError:
        description = _get_sqlstate_description(sqlstate)
        return _create_adbc_error(error, exc_class, description)

    return _create_adbc_error(error, SQLSpecError, "database error")


def _normalize_adbc_driver_features(processed_features: "dict[str, Any]") -> "dict[str, Any]":
    if "strict_type_coercion" in processed_features and "enable_strict_type_coercion" not in processed_features:
        processed_features["enable_strict_type_coercion"] = processed_features["strict_type_coercion"]
    if "enable_strict_type_coercion" in processed_features and "strict_type_coercion" not in processed_features:
        processed_features["strict_type_coercion"] = processed_features["enable_strict_type_coercion"]

    if "arrow_extension_types" in processed_features and "enable_arrow_extension_types" not in processed_features:
        processed_features["enable_arrow_extension_types"] = processed_features["arrow_extension_types"]
    if "enable_arrow_extension_types" in processed_features and "arrow_extension_types" not in processed_features:
        processed_features["arrow_extension_types"] = processed_features["enable_arrow_extension_types"]

    return processed_features


def _validate_strict_parameter_value(value: Any) -> None:
    if isinstance(value, (list, tuple)):
        for item in value:
            _validate_strict_parameter_value(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            _validate_strict_parameter_value(item)
        return
    if not isinstance(value, _STRICT_COERCION_ALLOWED_TYPES):
        msg = f"Unsupported parameter type for strict_type_coercion: {type(value).__name__}"
        raise SQLSpecError(msg)


def _validate_strict_parameter_payload(parameters: Any) -> None:
    if isinstance(parameters, (list, tuple)):
        for item in parameters:
            _validate_strict_parameter_value(item)
    elif isinstance(parameters, dict):
        for item in parameters.values():
            _validate_strict_parameter_value(item)
    else:
        _validate_strict_parameter_value(parameters)


def _transform_duckdb_copy_parameters(
    expression: Any, parameters: Any, _parameter_profile: Any = None, is_many: bool = False
) -> "tuple[Any, Any]":
    """Inline bound file-target placeholders in DuckDB ``COPY ... TO/FROM`` statements."""
    if is_many or not isinstance(expression, exp.Copy) or not isinstance(parameters, (list, tuple)) or not parameters:
        return expression, parameters

    files = expression.args.get("files")
    if not isinstance(files, list) or not files:
        return expression, parameters

    this_node = expression.this
    prefix_count = (
        len(list(this_node.find_all(exp.Placeholder, exp.Parameter))) if isinstance(this_node, exp.Expression) else 0
    )

    inlined_indices: set[int] = set()
    new_files: list[Any] = list(files)
    file_qmark_offset = 0
    for idx, file_expr in enumerate(new_files):
        placeholder_node: exp.Expression | None = None
        if isinstance(file_expr, (exp.Placeholder, exp.Parameter)):
            placeholder_node = file_expr
        elif isinstance(file_expr, exp.Table) and isinstance(file_expr.this, (exp.Placeholder, exp.Parameter)):
            placeholder_node = file_expr.this

        if placeholder_node is None:
            continue

        param_idx: int | None = None
        if isinstance(placeholder_node, exp.Parameter):
            ordinal = _parameter_ordinal(placeholder_node)
            if ordinal is not None:
                param_idx = ordinal - 1
        elif isinstance(placeholder_node, exp.Placeholder):
            raw_val = placeholder_node.this
            if raw_val is not None and str(raw_val).lstrip("$").isdigit():
                param_idx = int(str(raw_val).lstrip("$")) - 1

        if param_idx is None:
            param_idx = prefix_count + file_qmark_offset
            file_qmark_offset += 1

        if 0 <= param_idx < len(parameters):
            new_files[idx] = exp.Literal.string(str(parameters[param_idx]))
            inlined_indices.add(param_idx)

    if not inlined_indices:
        return expression, parameters

    transformed_copy = expression.copy()
    transformed_copy.set("files", new_files)
    remaining = [val for idx, val in enumerate(parameters) if idx not in inlined_indices]
    if not remaining:
        return transformed_copy, []
    return transformed_copy, tuple(remaining) if isinstance(parameters, tuple) else remaining


def _apply_adbc_json_serializer(
    statement_config: "StatementConfig", json_serializer: "Callable[[Any], str]"
) -> "StatementConfig":
    """Apply a JSON serializer to statement config while preserving list/tuple converters.

    Args:
        statement_config: Base statement configuration to update.
        json_serializer: JSON serializer function.

    Returns:
        Updated statement configuration.
    """
    parameter_config = statement_config.parameter_config
    previous_list_converter = parameter_config.type_coercion_map.get(list)
    previous_tuple_converter = parameter_config.type_coercion_map.get(tuple)

    updated_parameter_config = parameter_config.with_json_serializers(json_serializer)
    updated_map = dict(updated_parameter_config.type_coercion_map)

    if previous_list_converter is not None:
        updated_map[list] = previous_list_converter
    if previous_tuple_converter is not None:
        updated_map[tuple] = previous_tuple_converter

    return statement_config.replace(parameter_config=updated_parameter_config.replace(type_coercion_map=updated_map))


def _prepare_parameter_sequence_with_casts(
    parameters: "list[Any] | tuple[Any, ...]",
    parameter_casts: "dict[int, str]",
    type_map: "dict[type, Callable[[Any], Any]]",
    dispatcher: "TypeDispatcher[Callable[[Any], Any]] | None",
    converter: Any,
    json_encoder: "Callable[[Any], str]",
) -> "list[Any] | tuple[Any, ...]":
    result: list[Any] = []
    for idx, param in enumerate(parameters, start=1):
        cast_type = parameter_casts.get(idx, "").upper()
        if cast_type in {"JSON", "JSONB", "TYPE.JSON", "TYPE.JSONB"}:
            if isinstance(param, dict):
                result.append(json_encoder(param))
            else:
                result.append(param)
        elif isinstance(param, dict):
            if isinstance(converter, ADBCOutputConverter):
                result.append(converter.convert_dict(param, json_serializer=json_encoder))
            else:
                result.append(converter.convert_dict(param))
        elif isinstance(param, (list, tuple)):
            if type_map and dispatcher is not None:
                exact_converter = type_map.get(type(param))
                if exact_converter is not None:
                    param = exact_converter(param)
                else:
                    converter_func = dispatcher.get(param)
                    if converter_func is not None:
                        param = converter_func(param)
                    elif hasattr(converter, "convert_sequence"):
                        if isinstance(converter, ADBCOutputConverter):
                            param = converter.convert_sequence(param, json_serializer=json_encoder)
                        else:
                            param = converter.convert_sequence(param)
            elif hasattr(converter, "convert_sequence"):
                if isinstance(converter, ADBCOutputConverter):
                    param = converter.convert_sequence(param, json_serializer=json_encoder)
                else:
                    param = converter.convert_sequence(param)
            result.append(param)
        else:
            if type_map and dispatcher is not None:
                exact_converter = type_map.get(type(param))
                if exact_converter is not None and type(param) is not dict:
                    param = exact_converter(param)
                else:
                    converter_func = dispatcher.get(param)
                    if converter_func is not None and type(param) is not dict:
                        param = converter_func(param)
            result.append(param)
    return tuple(result) if isinstance(parameters, tuple) else result


_prepare_batch_with_casts = prepare_batch_with_casts


def _resolve_gizmosql_driver_connect() -> str:
    """Return the preferred GizmoSQL connect path based on installed packages."""
    if import_optional("adbc_driver_gizmosql") is not None:
        return _GIZMOSQL_NATIVE_CONNECT
    return _FLIGHTSQL_CONNECT


def _resolve_mssql_driver_connect() -> str:
    """Return the preferred SQL Server connect path based on installed packages."""
    if import_optional("adbc_driver_mssql") is not None:
        return _MSSQL_NATIVE_CONNECT
    return _DRIVER_MANAGER_CONNECT


def _resolve_flightsql_db_kwargs_keys() -> "tuple[str, str]":
    database_options = import_optional_attr("adbc_driver_flightsql", "DatabaseOptions")
    if database_options is None:
        return _FLIGHTSQL_TLS_SKIP_VERIFY_KEY, _FLIGHTSQL_AUTHORIZATION_HEADER_KEY
    return database_options.TLS_SKIP_VERIFY.value, database_options.AUTHORIZATION_HEADER.value


def _lift_flightsql_common_options(
    config: "dict[str, Any]", db_kwargs_dict: "dict[str, Any]", authorization_header_key: str
) -> None:
    if "authorization_header" in config:
        db_kwargs_dict.setdefault(authorization_header_key, config.pop("authorization_header"))

    if "grpc_max_message_size" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_WITH_MAX_MSG_SIZE_KEY, str(config.pop("grpc_max_message_size")))

    if "query_timeout" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_TIMEOUT_QUERY_KEY, str(config.pop("query_timeout")))

    if "fetch_timeout" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_TIMEOUT_FETCH_KEY, str(config.pop("fetch_timeout")))

    if "tls_root_certs" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_TLS_ROOT_CERTS_KEY, str(config.pop("tls_root_certs")))

    if "mtls_cert_chain" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_MTLS_CERT_CHAIN_KEY, str(config.pop("mtls_cert_chain")))

    if "mtls_private_key" in config:
        db_kwargs_dict.setdefault(_FLIGHTSQL_MTLS_PRIVATE_KEY_KEY, str(config.pop("mtls_private_key")))

    if "with_cookie_middleware" in config:
        val = config.pop("with_cookie_middleware")
        db_kwargs_dict.setdefault(
            _FLIGHTSQL_WITH_COOKIE_MIDDLEWARE_KEY, str(val).lower() if isinstance(val, bool) else str(val)
        )

    if "headers" in config:
        headers = config.pop("headers")
        if isinstance(headers, dict):
            for hdr_key, hdr_val in headers.items():
                db_kwargs_dict.setdefault(f"{_FLIGHTSQL_RPC_CALL_HEADER_PREFIX}{hdr_key}", str(hdr_val))


def _get_adbc_driver_info(connection: Any) -> "dict[Any, Any]":
    """Return per-connection ADBC driver metadata without class-level cache bleed."""
    conn_type: Any = type(connection)
    if "_get_info_cache" in conn_type.__dict__:
        conn_dict = getattr(connection, "__dict__", None)
        if isinstance(conn_dict, dict):
            cached = conn_dict.get("_get_info_cache")
            if isinstance(cached, dict):
                return cached
            for base_cls in conn_type.__mro__[1:]:
                base_method = base_cls.__dict__.get("adbc_get_info")
                if callable(base_method):
                    info = cast("dict[Any, Any]", base_method(connection))
                    conn_dict["_get_info_cache"] = info
                    return info
    return cast("dict[Any, Any]", connection.adbc_get_info())


def _lift_native_gizmosql_db_kwargs(config: "dict[str, Any]") -> None:
    db_kwargs = config.pop("db_kwargs", None)
    db_kwargs_dict: dict[str, Any] = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
    _tls_skip_verify_key, authorization_header_key = _resolve_flightsql_db_kwargs_keys()
    _lift_flightsql_common_options(config, db_kwargs_dict, authorization_header_key)
    config.pop("gizmosql_backend", None)
    config.setdefault("autocommit", False)
    if db_kwargs_dict:
        config["db_kwargs"] = db_kwargs_dict


def _lift_flightsql_db_kwargs(config: "dict[str, Any]") -> None:
    db_kwargs = config.pop("db_kwargs", None)
    db_kwargs_dict: dict[str, Any] = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
    tls_skip_verify_key, authorization_header_key = _resolve_flightsql_db_kwargs_keys()

    for field in _FLIGHTSQL_DB_KWARGS_FIELDS:
        if field in config:
            db_kwargs_dict.setdefault(field, config.pop(field))

    if "auth_type" in config:
        db_kwargs_dict.setdefault(_GIZMOSQL_AUTH_TYPE_KEY, config.pop("auth_type"))

    if "tls_skip_verify" in config:
        tls_skip_verify = config.pop("tls_skip_verify")
        if isinstance(tls_skip_verify, bool):
            tls_skip_verify = str(tls_skip_verify).lower()
        db_kwargs_dict.setdefault(tls_skip_verify_key, tls_skip_verify)

    _lift_flightsql_common_options(config, db_kwargs_dict, authorization_header_key)

    config.pop("gizmosql_backend", None)
    if db_kwargs_dict:
        config["db_kwargs"] = db_kwargs_dict


def _lift_driver_specific_db_kwargs(
    config: "dict[str, Any]", driver_kind: "str | None", resolved_connect_path: str
) -> None:
    if driver_kind == "gizmosql" and resolved_connect_path == _GIZMOSQL_NATIVE_CONNECT:
        _lift_native_gizmosql_db_kwargs(config)
        return
    if driver_kind in {"gizmosql", "flightsql"}:
        _lift_flightsql_db_kwargs(config)
        return
    if driver_kind == "bigquery":
        db_kwargs = config.get("db_kwargs")
        db_kwargs_dict: dict[str, Any] = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
        for param, option in _BIGQUERY_DB_KWARGS_OPTIONS:
            if param in config:
                db_kwargs_dict[option] = config.pop(param)
        if db_kwargs_dict:
            config["db_kwargs"] = db_kwargs_dict
        return
    if driver_kind == "quack" and "token" in config:
        db_kwargs = config.get("db_kwargs")
        db_kwargs_dict = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
        db_kwargs_dict.setdefault(_QUACK_AUTH_KEY, config.pop("token"))
        config["db_kwargs"] = db_kwargs_dict
        return
    if driver_kind == "oracle":
        db_kwargs = config.get("db_kwargs")
        db_kwargs_dict = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
        for field in ("username", "password"):
            if field in config:
                db_kwargs_dict.setdefault(field, config.pop(field))
        if db_kwargs_dict:
            config["db_kwargs"] = db_kwargs_dict


def _parse_db2_cli_dsn(uri: str) -> "dict[str, str] | None":
    """Parse a semicolon-delimited IBM Db2 CLI keyword DSN when valid."""
    if "://" in uri or ";" not in uri:
        return None
    parsed: dict[str, str] = {}
    for raw_segment in uri.split(";"):
        segment = raw_segment.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        normalized_key = key.strip().lower()
        normalized_val = value.strip()
        if normalized_key and normalized_val:
            parsed[normalized_key] = normalized_val
    if "driver" in parsed:
        return None
    database = parsed.get("database") or parsed.get("dbalias")
    host = parsed.get("hostname") or parsed.get("host")
    if not database or not host:
        return None
    return parsed


def _normalize_db2_cli_dsn(config: "dict[str, Any]", uri: str) -> None:
    """Convert an IBM Db2 CLI keyword DSN into a db2:// URI, credentials, and db_kwargs."""
    parsed = _parse_db2_cli_dsn(uri)
    if parsed is None:
        return
    database = parsed.get("database") or parsed.get("dbalias") or ""
    host = parsed.get("hostname") or parsed.get("host") or ""
    port = parsed.get("port", "")
    formatted_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    config["uri"] = f"db2://{formatted_host}:{port}/{database}" if port else f"db2://{formatted_host}/{database}"

    uid = parsed.get("uid") or parsed.get("user")
    if uid is not None:
        config.setdefault("username", uid)
    pwd = parsed.get("pwd") or parsed.get("password")
    if pwd is not None:
        config.setdefault("password", pwd)

    existing_db_kwargs = config.get("db_kwargs")
    db_kwargs_dict: dict[str, Any] = dict(existing_db_kwargs) if isinstance(existing_db_kwargs, dict) else {}
    if "uri" in db_kwargs_dict and db_kwargs_dict.get("uri") == uri:
        db_kwargs_dict.pop("uri", None)

    security = parsed.get("security", "").lower()
    if security in {"ssl", "tls"}:
        db_kwargs_dict.setdefault("adbc.db2.tls", "true")
    ssl_cert = parsed.get("sslservercertificate")
    if ssl_cert is not None:
        db_kwargs_dict.setdefault("adbc.db2.tls.ca_cert", ssl_cert)
    current_schema = parsed.get("currentschema")
    if current_schema is not None:
        db_kwargs_dict.setdefault("adbc.db2.current_schema", current_schema)
    connect_timeout = parsed.get("connecttimeout")
    if connect_timeout is not None:
        db_kwargs_dict.setdefault("adbc.db2.connect_timeout", connect_timeout)

    if db_kwargs_dict:
        config["db_kwargs"] = db_kwargs_dict
    elif isinstance(existing_db_kwargs, dict) and "uri" in existing_db_kwargs:
        config.pop("db_kwargs", None)


def _apply_driver_manager_config(config: "dict[str, Any]", driver_kind: "str | None", driver_name: Any) -> None:
    if driver_kind == "mssql":
        config["driver"] = "mssql"
    elif isinstance(driver_name, str) and "driver" not in config:
        config["driver"] = driver_name.strip()
    if any(field in config for field in ("uri", "username", "password")):
        db_kwargs = config.get("db_kwargs")
        db_kwargs_dict = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
        for field in ("uri", "username", "password"):
            if field in config:
                db_kwargs_dict.setdefault(field, config.pop(field))
        if db_kwargs_dict:
            config["db_kwargs"] = db_kwargs_dict


driver_profile: Final[DriverParameterProfile] = build_profile()
